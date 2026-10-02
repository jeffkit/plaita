"""C4-2 回归：DelayService list→ZSET 两段式排程。

历史实现 BLPOP 出队即提交线程池、handle_task 内睡眠：出队后崩溃任务即丢、
未到期任务占满线程池坑位、shutdown 不回队。现改为先 ZADD 进排程 ZSET 再
出 list，到期才提交，处理走完才 ZREM。

生产者契约（flow_worker._dispatch_service_task RPUSH list）保持不变。
"""
from __future__ import annotations

import json
import time
import unittest
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.event.memory import InMemoryEventBus
from plaita.server.services.delay_service import DelayService


def _task(delay_ms=10, trigger_timestamp=None, node_id="n1", execution_id="e1"):
    cfg = {
        "type": "delay",
        "delay_ms": delay_ms,
        "node_id": node_id,
        "execution_id": execution_id,
        "flow_id": "f1",
        "event_type": "delay_trigger",
    }
    if trigger_timestamp is not None:
        cfg["trigger_timestamp"] = trigger_timestamp
    return cfg


def _wait_until(cond, timeout=5.0, interval=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return cond()


class DelayServiceTestBase(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.bus = InMemoryEventBus()
        self.queue_key = "plaita:delay:queue:c4-2"
        self.scheduled_key = f"{self.queue_key}:scheduled"
        self.config = {"max_workers": 2, "poll_interval": 0.05, "delay_queue": self.queue_key}
        self.services = []

    def tearDown(self):
        for svc in self.services:
            svc.shutdown(timeout=1)

    def make_service(self):
        svc = DelayService(self.bus, self.config, redis_client=self.redis)
        self.services.append(svc)
        return svc


class TestScheduledTrigger(DelayServiceTestBase):
    def test_due_task_is_triggered(self):
        """到期任务被触发：RPUSH → 搬运 → 到期提交 → handle_task 执行。"""
        svc = self.make_service()
        with patch.object(DelayService, "handle_task", new_callable=AsyncMock) as ht:
            ht.return_value = True
            self.redis.rpush(self.queue_key, json.dumps(_task(delay_ms=10)))
            self.assertTrue(svc.start_service())
            self.assertTrue(
                _wait_until(lambda: ht.await_count >= 1), "到期任务未被触发"
            )
            called_cfg = ht.await_args.args[-1]
            self.assertEqual(called_cfg["execution_id"], "e1")
            # 处理走完才 ZREM
            self.assertTrue(_wait_until(lambda: self.redis.zcard(self.scheduled_key) == 0))
            self.assertEqual(self.redis.llen(self.queue_key), 0)

    def test_undue_task_untouched(self):
        """未到期任务不动：不提交线程池、留在 ZSET。"""
        svc = self.make_service()
        with patch.object(DelayService, "handle_task", new_callable=AsyncMock) as ht:
            ht.return_value = True
            self.redis.rpush(
                self.queue_key, json.dumps(_task(trigger_timestamp=int(time.time() * 1000) + 60_000))
            )
            self.assertTrue(svc.start_service())
            self.assertTrue(_wait_until(lambda: self.redis.llen(self.queue_key) == 0))
            time.sleep(0.5)
            ht.assert_not_called()
            self.assertEqual(svc.get_active_task_count(), 0)
            self.assertEqual(self.redis.zcard(self.scheduled_key), 1)

    def test_ten_long_delays_do_not_hog_pool(self):
        """10 个长延迟不占线程池坑位：全部滞留 ZSET，active_tasks=0。"""
        svc = self.make_service()
        with patch.object(DelayService, "handle_task", new_callable=AsyncMock):
            for i in range(10):
                self.redis.rpush(
                    self.queue_key,
                    json.dumps(
                        _task(trigger_timestamp=int(time.time() * 1000) + 60_000, node_id=f"n{i}")
                    ),
                )
            self.assertTrue(svc.start_service())
            self.assertTrue(_wait_until(lambda: self.redis.zcard(self.scheduled_key) == 10))
            self.assertEqual(self.redis.llen(self.queue_key), 0)
            time.sleep(0.3)
            self.assertEqual(svc.get_active_task_count(), 0)

    def test_inflight_not_resubmitted(self):
        """处理中的到期任务不会被下一周期重复提交。"""
        svc = self.make_service()
        release = threading_release = __import__("threading").Event()

        async def slow_handle(cfg):
            release.wait(timeout=5)
            return True

        with patch.object(DelayService, "handle_task", side_effect=slow_handle):
            self.redis.rpush(self.queue_key, json.dumps(_task(delay_ms=1)))
            self.assertTrue(svc.start_service())
            self.assertTrue(_wait_until(lambda: svc.get_active_task_count() == 1))
            time.sleep(0.5)  # 覆盖多个轮询周期
            # handle_task 只被提交一次（active_tasks 仍是 1，无第二个并发执行）
            self.assertEqual(svc.get_active_task_count(), 1)
            release.set()
            self.assertTrue(_wait_until(lambda: self.redis.zcard(self.scheduled_key) == 0))

    def test_handled_failure_consumes_task(self):
        """handle_task 返回 False（处理过但失败，已触发错误事件）：按既有语义消费掉。"""
        svc = self.make_service()
        with patch.object(DelayService, "handle_task", new_callable=AsyncMock) as ht:
            ht.return_value = False
            self.redis.rpush(self.queue_key, json.dumps(_task(delay_ms=1)))
            self.assertTrue(svc.start_service())
            self.assertTrue(_wait_until(lambda: ht.await_count >= 1))
            self.assertTrue(_wait_until(lambda: self.redis.zcard(self.scheduled_key) == 0))


class TestCrashRecovery(DelayServiceTestBase):
    def test_survives_service_rebuild(self):
        """重建 service 实例后，ZSET 中未触发的任务仍被处理（崩溃恢复）。"""
        svc1 = self.make_service()
        member = json.dumps(_task(trigger_timestamp=int(time.time() * 1000) + 60_000))
        self.redis.rpush(self.queue_key, member)
        self.assertTrue(svc1.start_service())
        # 任务已搬进 ZSET（list 清空），未到期
        self.assertTrue(_wait_until(lambda: self.redis.zcard(self.scheduled_key) == 1))
        self.assertEqual(self.redis.llen(self.queue_key), 0)
        # 模拟崩溃：进程直接消失，ZSET 遗留（不清理）
        svc1._shutdown_event.set()
        svc1.stop_service()

        # 新实例（同一 Redis）接管
        svc2 = self.make_service()
        with patch.object(DelayService, "handle_task", new_callable=AsyncMock) as ht:
            ht.return_value = True
            self.assertTrue(svc2.start_service())
            time.sleep(0.3)
            ht.assert_not_called()  # 未到期，不触发
            # 模拟时间流逝：触发点已到
            self.redis.zadd(self.scheduled_key, {member: int(time.time() * 1000) - 1})
            self.assertTrue(_wait_until(lambda: ht.await_count >= 1), "重建后遗留任务未被处理")
            self.assertTrue(_wait_until(lambda: self.redis.zcard(self.scheduled_key) == 0))

    def test_legacy_list_migrated_into_zset(self):
        """启动后发现旧 list 队列条目：搬运进 ZSET，坏条目按既有语义丢弃。"""
        svc = self.make_service()
        good1 = json.dumps(_task(trigger_timestamp=int(time.time() * 1000) + 60_000, node_id="ok1"))
        good2 = json.dumps(_task(delay_ms=1, node_id="ok2"))
        self.redis.rpush(
            self.queue_key, good1, "{not-json", json.dumps({"node_id": "bad"}), good2
        )
        self.assertTrue(svc.start_service())
        self.assertTrue(_wait_until(lambda: self.redis.llen(self.queue_key) == 0))
        members = self.redis.zrange(self.scheduled_key, 0, -1)
        self.assertEqual(sorted(members), sorted([good1, good2]))


class TestEndToEndEvent(DelayServiceTestBase):
    def test_full_path_publishes_event(self):
        """端到端：RPUSH → 到期 → 真实 handle_task → publish 到事件频道。"""
        import asyncio

        svc = self.make_service()
        pubsub = self.redis.pubsub()
        pubsub.subscribe("plaita:events:delay_trigger")
        self.redis.rpush(self.queue_key, json.dumps(_task(delay_ms=10)))
        self.assertTrue(svc.start_service())

        received = []

        def drain():
            deadline = time.time() + 5
            while time.time() < deadline and not received:
                msg = pubsub.get_message(timeout=0.2)
                if msg and msg.get("type") == "message":
                    received.append(json.loads(msg["data"]))
            pubsub.close()

        import threading

        t = threading.Thread(target=drain, daemon=True)
        t.start()
        t.join(timeout=6)
        self.assertTrue(received, "未收到延迟触发事件")
        # 发布的是 Event 模型 dump：execution_id 在 data 内，correlation_id 顶层
        self.assertEqual(received[0]["data"]["execution_id"], "e1")
        self.assertEqual(received[0]["data"]["trigger_type"], "delay_completed")
        self.assertEqual(received[0]["correlation_id"], "e1")


if __name__ == "__main__":
    unittest.main()
