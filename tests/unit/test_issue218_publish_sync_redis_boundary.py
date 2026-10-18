"""plaita#218 Redis 侧边界：``publish_sync`` 不排空 listener 循环上的分发任务。

``RedisEventBus.publish`` 只写存储 + ``redis.publish``，**不**建分发任务；
handler 由 pubsub listener 在它自己的循环驱动，排空归 ``close()``（#109）。
``publish_sync`` 因此必须原样返回，不得跨循环等待 listener 侧的在飞任务，
也不能把「sync 发布」误当成「handler 已同步执行」。

用 fakeredis（共享 FakeServer），无需真实实例。
"""

import asyncio
import unittest
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis
import fakeredis.aioredis

import plaita.event.redis as redis_mod
from plaita.event.core import Event
from plaita.event.redis import RedisEventBus

_SHARED_SERVER = fakeredis.FakeServer()


async def _fake_from_url(url, **kwargs):
    return fakeredis.aioredis.FakeRedis(server=_SHARED_SERVER, decode_responses=False)


def _event(event_type="issue218.redis"):
    return Event(event_type=event_type, data={})


class TestRedisPublishSyncBoundary(unittest.IsolatedAsyncioTestCase):
    async def test_publish_sync_leaves_dispatch_to_listener_loop(self):
        seen = []
        bus = None
        with patch.object(redis_mod.aioredis, "from_url", side_effect=_fake_from_url):
            try:
                bus = RedisEventBus(redis_url="redis://unused/0")

                async def handler(event):
                    seen.append(event.event_id)

                await bus.register_handler(event_type="issue218.redis", handler=handler)
                # 让 listener 任务跑起来并完成订阅（fakeredis pubsub 是异步建立）。
                for _ in range(100):
                    if any(not t.done() for t in bus.listeners):
                        break
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.05)

                event_id = bus.publish_sync(_event())

                self.assertIsInstance(event_id, str)
                self.assertEqual(len(bus._pending_tasks), 0,
                                 "Redis publish 不应在 publish 循环上建分发任务")
                self.assertEqual(seen, [],
                                 "publish_sync 不应同步运行 listener 侧 handler")
                for _ in range(200):
                    if seen:
                        break
                    await asyncio.sleep(0.01)
                self.assertEqual(seen, [event_id],
                                 "sync 发布后 listener 侧 handler 应最终执行")
            finally:
                if bus is not None:
                    await asyncio.wait_for(bus.close(), timeout=10)
