"""test_reviewfix_c_redis — 评审修复包 C3/C4 回归。

C3：RedisEventBus.wait_for_event 历史上用恒为 None 的 self.pubsub
（initialize 只在非 None 时重绑），首个等待者在 subscribe 处
AttributeError 且异常滞留任务内（仅 "Task exception was never retrieved"
告警），future 永不 resolve 挂到超时；即便初始化了，多个等待者还共享
一条 pubsub，一个等待者的 unsubscribe 会拆掉别人的订阅。
修复后：每个等待者局部 pubsub + finally aclose，监听协程异常 set 到
future 传播给调用方。

C4：RedisNode 构造 redis.Redis 必须带连接/读写超时（Redis 抖动不再
永久挂起 flow）。

测试用 fakeredis（共享 FakeServer）模拟 Redis，全程无需真实实例。
"""

import asyncio
import unittest
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis
import fakeredis.aioredis

import plaita.event.redis as redis_mod
from plaita.event.exceptions import EventTimeoutError
from plaita.event.redis import RedisEventBus

_SHARED_SERVER = fakeredis.FakeServer()


async def _fake_from_url(url):
    """initialize() 的 from_url 打桩：共享 FakeServer 的 fakeredis 客户端。"""
    return fakeredis.aioredis.FakeRedis(server=_SHARED_SERVER)


async def _publish_from_remote_bus(event_type, **data):
    """用第二条总线实例发布事件（模拟另一进程经 pubsub 投递）。

    同一 bus 实例的 publish() 会直接 resolve waiting_futures、绕过 pubsub
    ——那掩盖不了 C3 的 pubsub 死路。跨 bus 发布才真正走监听协程的订阅。
    """
    publisher = RedisEventBus("redis://localhost:6379/9")
    with patch.object(redis_mod.aioredis, "from_url", side_effect=_fake_from_url):
        await publisher.publish(event_type, **data)


class TestWaitForEventLocalPubsub(unittest.IsolatedAsyncioTestCase):
    async def test_first_call_receives_published_event(self):
        """首次调用不再 AttributeError，跨进程事件经订阅送达等待者。"""
        bus = RedisEventBus("redis://localhost:6379/9")
        with patch.object(redis_mod.aioredis, "from_url", side_effect=_fake_from_url):
            waiter = asyncio.create_task(bus.wait_for_event("order.paid", timeout=5))
            await asyncio.sleep(0.2)  # 让监听协程先完成 subscribe
            await _publish_from_remote_bus("order.paid", amount=42)
            event = await asyncio.wait_for(waiter, timeout=5)
        self.assertEqual(event.event_type, "order.paid")
        self.assertEqual(event.data, {"amount": 42})

    async def test_no_event_still_times_out_with_event_timeout_error(self):
        bus = RedisEventBus("redis://localhost:6379/9")
        with patch.object(redis_mod.aioredis, "from_url", side_effect=_fake_from_url):
            with self.assertRaises(EventTimeoutError):
                await bus.wait_for_event("never.published", timeout=0.3)

    async def test_listener_exception_propagates_to_caller(self):
        """监听协程的异常 set 到 future，调用方拿到真实错误而非超时。"""

        class _BoomPubSub:
            async def subscribe(self, *a, **kw):
                raise ConnectionError("redis down")

            async def unsubscribe(self, *a, **kw):
                pass

            async def aclose(self):
                pass

            async def get_message(self, **kw):
                return None

        async def _bad_from_url(_url):
            client = fakeredis.aioredis.FakeRedis(server=_SHARED_SERVER)
            client.pubsub = lambda: _BoomPubSub()
            return client

        bus = RedisEventBus("redis://localhost:6379/9")
        with patch.object(redis_mod.aioredis, "from_url", side_effect=_bad_from_url):
            with self.assertRaises(ConnectionError) as cm:
                await bus.wait_for_event("a.b", timeout=5)
        self.assertIn("redis down", str(cm.exception))

    async def test_waiter_timeout_does_not_kill_other_waiters(self):
        """一个等待者超时退出（unsubscribe/aclose）不影响另一等待者收事件。"""
        bus = RedisEventBus("redis://localhost:6379/9")
        with patch.object(redis_mod.aioredis, "from_url", side_effect=_fake_from_url):
            t1 = asyncio.create_task(bus.wait_for_event("e.multi", timeout=0.3))
            t2 = asyncio.create_task(bus.wait_for_event("e.multi", timeout=5))
            await asyncio.sleep(0.6)  # t1 已超时并完成清理
            with self.assertRaises(EventTimeoutError):
                await t1
            await _publish_from_remote_bus("e.multi", n=1)
            event = await asyncio.wait_for(t2, timeout=5)
        self.assertEqual(event.data, {"n": 1})

    async def test_each_waiter_uses_its_own_pubsub(self):
        """两个并发等待者同时收到同一事件（各自局部 pubsub 的直接证据）。"""
        bus = RedisEventBus("redis://localhost:6379/9")
        with patch.object(redis_mod.aioredis, "from_url", side_effect=_fake_from_url):
            t1 = asyncio.create_task(bus.wait_for_event("e.both", timeout=5))
            t2 = asyncio.create_task(bus.wait_for_event("e.both", timeout=5))
            await asyncio.sleep(0.2)
            await _publish_from_remote_bus("e.both", v=1)
            e1 = await asyncio.wait_for(t1, timeout=5)
            e2 = await asyncio.wait_for(t2, timeout=5)
        self.assertEqual(e1.data, {"v": 1})
        self.assertEqual(e2.data, {"v": 1})


class TestRedisNodeConnectionTimeouts(unittest.TestCase):
    """C4：构造 redis.Redis 必须带连接/读写超时默认值。"""

    def test_constructor_passes_timeouts(self):
        import plaita.node.redis as redis_node_mod
        from plaita.node.redis import RedisNode

        captured = {}

        class _FakeRedis:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            def execute_command(self, *args):
                return "OK"

        node = RedisNode(id="r1", target="redis://u:p@127.0.0.1:6379/0",
                         command="GET", arguments="key")
        execution = MagicMock()
        execution.evaluate.side_effect = lambda v: v
        with patch.object(redis_node_mod.redis, "Redis", _FakeRedis):
            node.execute(execution)
        self.assertEqual(captured["socket_connect_timeout"], 5)
        self.assertEqual(captured["socket_timeout"], 30)
        self.assertTrue(captured["retry_on_timeout"])
        # 基本连接参数不回归
        self.assertEqual(captured["host"], "127.0.0.1")
        self.assertEqual(captured["port"], 6379)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
