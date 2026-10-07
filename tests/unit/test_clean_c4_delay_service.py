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

    def make_service(self, **overrides):
        config = dict(self.config)
        config.update(overrides)
        svc = DelayService(self.bus, config, redis_client=self.redis)
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
            # 搬运两段式（先 ZADD 后 LREM）：zcard==10 可能落在两条管道之间，
            # list 排空是随后的必然态，须同样轮询等待。
            self.assertTrue(_wait_until(lambda: self.redis.llen(self.queue_key) == 0))
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
        """handle_task 返回 False（处理过但失败，已触发错误事件）：按既有语义消费掉。

        与 #35 的边界：**异常**路径不出排程（下轮重试），显式 False 视为
        「已处置」（错误事件已发出），照旧 ZREM。
        """
        svc = self.make_service()
        with patch.object(DelayService, "handle_task", new_callable=AsyncMock) as ht:
            ht.return_value = False
            self.redis.rpush(self.queue_key, json.dumps(_task(delay_ms=1)))
            self.assertTrue(svc.start_service())
            self.assertTrue(_wait_until(lambda: ht.await_count >= 1))
            self.assertTrue(_wait_until(lambda: self.redis.zcard(self.scheduled_key) == 0))


class TestTriggerFailureRetry(DelayServiceTestBase):
    """#35：到期触发的最后一跳失败不得出排程。

    历史实现：base_service.publish_resume_event 吞掉发布异常 → handle_task
    正常返回 → finally 一律 ZREM——失败与成功出队路径完全相同。Redis 抖动
    瞬间 = publish 没送达 + 任务已出排程，挂起执行的唯一唤醒凭据消失。
    现：异常路径留排程态退避重试，超限转显式死信键。
    """

    def test_publish_failure_keeps_task_and_next_round_retries(self):
        """验收：publish 抛异常 → 任务仍在 ZSET；恢复后下轮重试成功出排程。"""
        svc = self.make_service(
            trigger_retry_backoff_seconds=0.05, max_trigger_attempts=50
        )
        attempts = []

        def broken_publish(*args, **kwargs):
            attempts.append(args)
            raise ConnectionError("redis 瞬断")

        with patch.object(self.redis, "publish", side_effect=broken_publish):
            self.redis.rpush(self.queue_key, json.dumps(_task(delay_ms=1)))
            self.assertTrue(svc.start_service())
            self.assertTrue(
                _wait_until(lambda: len(attempts) >= 2), "触发失败后未被下一轮重试"
            )
            self.assertEqual(
                self.redis.zcard(self.scheduled_key),
                1,
                "触发失败的任务被 ZREM 出排程——唤醒凭据丢失",
            )
            member = self.redis.zrange(self.scheduled_key, 0, -1)[0]
            self.assertEqual(json.loads(member)["execution_id"], "e1")
            # 重试计数随失败递增（系统性可发现，而不只剩日志）。计数在每次
            # 失败处理里 +1，断言区间而非等值——计数线程可能正差一次未落。
            count = int(self.redis.hget(svc.retry_key, member) or 0)
            self.assertGreaterEqual(count, 1, "重试计数未登记")
            self.assertLessEqual(count, len(attempts))

        # Redis 恢复：退避到期后重试成功、任务出排程、计数清理
        self.assertTrue(
            _wait_until(lambda: self.redis.zcard(self.scheduled_key) == 0),
            "Redis 恢复后重试未成功（任务卡在排程态）",
        )
        self.assertEqual(self.redis.hlen(svc.retry_key), 0, "重试计数未随出排程清理")
        self.assertEqual(self.redis.hlen(svc.dead_letter_key), 0, "不该有死信")

    def test_retry_exhausted_moves_task_to_dead_letter(self):
        """重试超限：移出排程 ZSET，登记显式死信键（原因/次数/执行 ID）。"""
        svc = self.make_service(
            trigger_retry_backoff_seconds=0.02, max_trigger_attempts=3
        )
        with patch.object(
            self.redis, "publish", side_effect=ConnectionError("redis 瞬断")
        ):
            self.redis.rpush(self.queue_key, json.dumps(_task(delay_ms=1)))
            self.assertTrue(svc.start_service())
            self.assertTrue(
                _wait_until(lambda: self.redis.hlen(svc.dead_letter_key) == 1),
                "重试超限未转死信",
            )
            self.assertTrue(_wait_until(lambda: self.redis.zcard(self.scheduled_key) == 0))
            payload = json.loads(list(self.redis.hvals(svc.dead_letter_key))[0])
            self.assertEqual(payload["attempts"], 3)
            self.assertEqual(payload["execution_id"], "e1")
            self.assertIn("瞬断", payload["last_error"])
            self.assertEqual(self.redis.hlen(svc.retry_key), 0, "死信后重试计数未清")

        info = svc.get_pending_tasks_info()
        self.assertEqual(info["dead_letter_count"], 1, "死信未对外暴露（无系统性信号）")
        self.assertEqual(info["dead_letter_key"], svc.dead_letter_key)

    def test_handle_task_exception_keeps_task_for_retry(self):
        """非 publish 的异常同样不出排程：重试成功后才 ZREM。"""
        svc = self.make_service(
            trigger_retry_backoff_seconds=0.05, max_trigger_attempts=50
        )
        calls = []

        async def boom(cfg):
            calls.append(cfg)
            raise RuntimeError("boom")

        with patch.object(DelayService, "handle_task", side_effect=boom):
            self.redis.rpush(self.queue_key, json.dumps(_task(delay_ms=1)))
            self.assertTrue(svc.start_service())
            self.assertTrue(_wait_until(lambda: len(calls) >= 2), "异常路径未重试")
            self.assertEqual(self.redis.zcard(self.scheduled_key), 1)

        # 恢复后真实 handle_task 走完 → ZREM
        self.assertTrue(_wait_until(lambda: self.redis.zcard(self.scheduled_key) == 0))

    def test_shutdown_interrupt_still_leaves_task_in_zset(self):
        """shutdown 中断不触发重试登记（既有语义：留待下次启动）。"""
        svc = self.make_service()
        release = __import__("threading").Event()

        async def slow_handle(cfg):
            release.wait(timeout=5)
            return False

        with patch.object(DelayService, "handle_task", side_effect=slow_handle):
            self.redis.rpush(self.queue_key, json.dumps(_task(delay_ms=1)))
            self.assertTrue(svc.start_service())
            self.assertTrue(_wait_until(lambda: svc.get_active_task_count() == 1))
            svc._shutdown_event.set()
            release.set()
            self.assertTrue(_wait_until(lambda: svc.get_active_task_count() == 0))
            self.assertEqual(self.redis.zcard(self.scheduled_key), 1)
            self.assertEqual(self.redis.hlen(svc.dead_letter_key), 0)


class TestCrashRecovery(DelayServiceTestBase):
    def test_survives_service_rebuild(self):
        """重建 service 实例后，ZSET 中未触发的任务仍被处理（崩溃恢复）。"""
        svc1 = self.make_service()
        member = json.dumps(_task(trigger_timestamp=int(time.time() * 1000) + 60_000))
        self.redis.rpush(self.queue_key, member)
        self.assertTrue(svc1.start_service())
        # 任务已搬进 ZSET（list 清空），未到期
        self.assertTrue(_wait_until(lambda: self.redis.zcard(self.scheduled_key) == 1))
        self.assertTrue(_wait_until(lambda: self.redis.llen(self.queue_key) == 0))
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
        # 两个好条目都必须远未到期：本用例只验证「搬运 + 坏条目丢弃」，到期
        # 触发语义由 TestScheduledTrigger 覆盖。历史上 good2 用 delay_ms=1，
        # 搬进 ZSET 后 1ms 即到期——consumer 下个轮询周期（50ms）就可能提交
        # 执行并 ZREM，主线程断言 zrange 时好条目已消失（裸跑约 2/5 挂的
        # 时序竞态，2026-10 Track P2 修复）。
        good2 = json.dumps(_task(delay_ms=60_000, node_id="ok2"))
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
