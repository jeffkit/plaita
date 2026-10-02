"""Redis Stream task queue (at-least-once) for FlowWorker."""
from __future__ import annotations

import json
import time
import unittest
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.task_queue import (
    DEFAULT_CONSUMER_GROUP,
    RedisStreamTaskQueue,
    enqueue_task,
)


class TestRedisStreamTaskQueue(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.stream = "plaita:flow:queue:test"
        self.group = "test-group"

    def test_enqueue_read_ack_roundtrip(self):
        q_prod = RedisStreamTaskQueue(self.redis, self.stream, group_name=self.group, consumer_name="c1")
        q_cons = RedisStreamTaskQueue(self.redis, self.stream, group_name=self.group, consumer_name="c1")
        task = {"type": "start", "flow_id": "f1", "params": {}, "version": "1"}
        msg_id = q_prod.enqueue(task)
        self.assertTrue(msg_id)

        read = q_cons.read(block_ms=100)
        self.assertIsNotNone(read)
        self.assertEqual(read.body, task)
        q_cons.ack(read.message_id)

        # 已 ack 的消息不应再被同一 consumer 读到
        again = q_cons.read(block_ms=50)
        self.assertIsNone(again)

    def test_read_tolerates_connection_error(self):
        """redis 瞬断（DNS/连接失败）不炸穿主循环：退避后返回 None 继续轮询。

        2026-09 的空轮询修复只容忍了 BLOCK 到期的 TimeoutError；ConnectionError
        仍会炸穿 run() 主循环致 worker 退出（e2e-chaos-redis.sh 混沌实测）。
        """
        from redis.exceptions import ConnectionError as RedisConnectionError

        q = RedisStreamTaskQueue(self.redis, self.stream, group_name=self.group, consumer_name="c1")
        with patch.object(self.redis, "xreadgroup", side_effect=RedisConnectionError("boom")), \
             patch("plaita.server.task_queue.RECONNECT_BACKOFF_SECONDS", 0):
            self.assertIsNone(q.read(block_ms=100))

    def test_unacked_message_reclaimed_by_other_consumer(self):
        q1 = RedisStreamTaskQueue(
            self.redis,
            self.stream,
            group_name=self.group,
            consumer_name="c1",
            claim_min_idle_ms=1,
        )
        q2 = RedisStreamTaskQueue(
            self.redis,
            self.stream,
            group_name=self.group,
            consumer_name="c2",
            claim_min_idle_ms=1,
        )
        q1.enqueue({"type": "resume", "flow_id": "f", "execution_id": "e", "resume_type": "event"})
        first = q1.read(block_ms=100)
        self.assertIsNotNone(first)
        # 模拟 c1 崩溃：不 ack
        time.sleep(0.01)
        reclaimed = q2.read(block_ms=100)
        self.assertIsNotNone(reclaimed)
        self.assertEqual(reclaimed.body["type"], "resume")
        q2.ack(reclaimed.message_id)

    def test_enqueue_task_helper(self):
        mid = enqueue_task(self.redis, self.stream, {"type": "start", "flow_id": "x"})
        self.assertIn("-", mid)
        length = self.redis.xlen(self.stream)
        self.assertEqual(length, 1)

    def test_dead_letter_moves_and_acks(self):
        q = RedisStreamTaskQueue(
            self.redis,
            self.stream,
            group_name=self.group,
            consumer_name="c1",
            max_deliveries=2,
        )
        q.enqueue({"type": "start", "flow_id": "poison"})
        task = q.read(block_ms=100)
        self.assertIsNotNone(task)
        dlq_id = q.dead_letter(task, reason="test")
        self.assertTrue(dlq_id)
        self.assertEqual(q.stats()["dead_lettered"], 1)
        self.assertGreaterEqual(self.redis.xlen(q.dlq_key), 1)
        # original should be acked — no reclaim
        time.sleep(0.01)
        q2 = RedisStreamTaskQueue(
            self.redis,
            self.stream,
            group_name=self.group,
            consumer_name="c2",
            claim_min_idle_ms=1,
            max_deliveries=2,
        )
        self.assertIsNone(q2.read(block_ms=50))

    def test_stats_includes_keys(self):
        q = RedisStreamTaskQueue(self.redis, self.stream, group_name=self.group)
        q.enqueue({"type": "start", "flow_id": "f"})
        stats = q.stats()
        self.assertEqual(stats["stream_key"], self.stream)
        self.assertIn("dlq_key", stats)
        self.assertEqual(stats["enqueued"], 1)


class TestDeadLetterGuard(unittest.TestCase):
    """dead_letter 前守卫闸门（Track B 任务1，2026-10-02 评审）。

    背景：_reclaim_scan 对 idle > claim_min_idle_ms 的 pending 无条件 XCLAIM，
    不看持有者是否存活。worker A 长步（LLM/AGENTRUN 常 >5min）处理中，worker B
    抢走消息 → resume 遇 ExecutionLeaseError 不 ack 留 pending → 每 60s 被重抢
    一次、delivery_count 递增 → 达到 max_deliveries 后被 _read_once 回收循环
    直接 dead_letter（内部 XACK 原消息）→ 若 A 此刻崩溃，该执行永久失去恢复机会。

    守卫契约（与 Track A 的跨工约定，签名不得偏离）：
    ``dead_letter_guard: Optional[Callable[[StreamTask], bool]]``
    True = 允许死信；False 或抛异常 = 跳过死信（消息留 pending）。
    """

    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.stream = "plaita:flow:queue:fixb-dlq-guard"
        self.group = "g"

    def _make_overdelivered_pending(self, *, deliveries: int, max_deliveries: int):
        """造一条 times_delivered == deliveries 的 pending 消息（XCLAIM 刷次数）。"""
        writer = RedisStreamTaskQueue(
            self.redis, self.stream, group_name=self.group,
            consumer_name="c1", claim_min_idle_ms=1, max_deliveries=max_deliveries,
        )
        mid = writer.enqueue(
            {"type": "resume", "flow_id": "f", "execution_id": "e1", "resume_type": "continue"}
        )
        first = writer.read(block_ms=100)
        self.assertIsNotNone(first)
        self.assertEqual(first.message_id, mid)
        for _ in range(deliveries - 1):
            time.sleep(0.005)
            claimed = self.redis.xclaim(
                self.stream, self.group, "c2", min_idle_time=1, message_ids=[mid]
            )
            self.assertTrue(claimed)
        pending = self.redis.xpending_range(
            self.stream, self.group, min=mid, max=mid, count=1
        )
        self.assertEqual(int(pending[0]["times_delivered"]), deliveries)
        # 留出 idle 窗口（> claim_min_idle_ms=1ms），否则回收扫描会跳过该条目
        time.sleep(0.01)
        return mid

    def _read_queue(self, **kwargs):
        kwargs.setdefault("claim_min_idle_ms", 1)
        kwargs.setdefault("max_deliveries", 3)
        return RedisStreamTaskQueue(
            self.redis, self.stream, group_name=self.group, consumer_name="reader", **kwargs
        )

    def test_no_guard_overdelivered_still_dead_lettered(self):
        """默认（无守卫）行为零变化：超限 pending 在回收时照旧死信。"""
        self._make_overdelivered_pending(deliveries=3, max_deliveries=3)
        q = self._read_queue()
        self.assertIsNone(q.read(block_ms=50))
        self.assertEqual(self.redis.xlen(q.dlq_key), 1)
        self.assertEqual(q.stats()["dead_lettered"], 1)

    def test_guard_false_skips_dead_letter_and_keeps_pending(self):
        """守卫拒绝（租约仍被存活 worker 持有）：不死信、消息留 pending。"""
        mid = self._make_overdelivered_pending(deliveries=3, max_deliveries=3)
        q = self._read_queue(dead_letter_guard=lambda task: False)
        self.assertIsNone(q.read(block_ms=50))
        self.assertEqual(self.redis.xlen(q.dlq_key), 0, "守卫拒绝不得死信")
        pending = self.redis.xpending_range(
            self.stream, self.group, min=mid, max=mid, count=1
        )
        self.assertEqual(len(pending), 1, "消息应留在 pending")
        self.assertEqual(q.stats()["dlq_guard_skipped"], 1)
        self.assertEqual(q.stats()["dead_lettered"], 0)

    def test_guard_raising_skips_dead_letter(self):
        """守卫抛异常按拒绝处理：跳过死信、消息留 pending、计数。"""
        mid = self._make_overdelivered_pending(deliveries=3, max_deliveries=3)

        def boom(task):
            raise RuntimeError("lease probe failed")

        q = self._read_queue(dead_letter_guard=boom)
        self.assertIsNone(q.read(block_ms=50))
        self.assertEqual(self.redis.xlen(q.dlq_key), 0)
        pending = self.redis.xpending_range(
            self.stream, self.group, min=mid, max=mid, count=1
        )
        self.assertEqual(len(pending), 1)
        self.assertEqual(q.stats()["dlq_guard_skipped"], 1)

    def test_guard_true_allows_dead_letter(self):
        """守卫放行（True）：死信照常发生。"""
        mid = self._make_overdelivered_pending(deliveries=3, max_deliveries=3)
        seen = []

        def guard(task):
            seen.append(task.message_id)
            return True

        q = self._read_queue(dead_letter_guard=guard)
        self.assertIsNone(q.read(block_ms=50))
        self.assertEqual(self.redis.xlen(q.dlq_key), 1)
        self.assertEqual(seen, [mid], "守卫应被调用一次")
        self.assertEqual(q.stats()["dlq_guard_skipped"], 0)

    def test_guard_refusal_via_read_loop_no_hang_and_next_pending_served(self):
        """被拒消息不阻塞扫描：守卫拒绝 A 后，同批 pending 中健康消息 B 仍被服务。"""
        self._make_overdelivered_pending(deliveries=3, max_deliveries=3)  # 消息 A
        writer = RedisStreamTaskQueue(
            self.redis, self.stream, group_name=self.group,
            consumer_name="c1", claim_min_idle_ms=1, max_deliveries=3,
        )
        writer.enqueue({"type": "resume", "flow_id": "f2", "execution_id": "e2", "resume_type": "continue"})

        def refuse_a(task):
            return task.body.get("execution_id") != "e1"

        q = self._read_queue(dead_letter_guard=refuse_a)
        # 首次 read：A 被拒（留 pending），B 是新消息走 xreadgroup 分支
        first = q.read(block_ms=100)
        self.assertIsNotNone(first)
        self.assertEqual(first.body["execution_id"], "e2")
        q.ack(first.message_id)
        # 再次 read：A 仍被拒，不挂起、不返回
        time.sleep(0.005)
        self.assertIsNone(q.read(block_ms=50))
        self.assertEqual(self.redis.xlen(q.dlq_key), 0)

    def test_guard_refused_message_reclaimable_when_guard_allows_later(self):
        """被拒消息不是僵尸：守卫改放行后（如持有者已崩溃），下轮照常处置。"""
        mid = self._make_overdelivered_pending(deliveries=3, max_deliveries=3)
        allow = {"v": False}
        q = self._read_queue(dead_letter_guard=lambda task: allow["v"])
        self.assertIsNone(q.read(block_ms=50))
        self.assertEqual(self.redis.xlen(q.dlq_key), 0)
        allow["v"] = True
        time.sleep(0.005)
        self.assertIsNone(q.read(block_ms=50))
        self.assertEqual(self.redis.xlen(q.dlq_key), 1, "守卫放行后应死信")
        entries = self.redis.xpending_range(
            self.stream, self.group, min=mid, max=mid, count=1
        )
        self.assertEqual(entries, [], "死信后原消息应被 ack")

    def test_guard_refusal_increments_delivery_count_but_never_dead_letters(self):
        """副作用取证：守卫拒绝期间每次 read 重抢使 delivery_count 递增，
        但只要守卫持续拒绝就永不死信（递增有界于时间，不在单次 read 内自旋）。"""
        mid = self._make_overdelivered_pending(deliveries=3, max_deliveries=3)
        q = self._read_queue(dead_letter_guard=lambda task: False)
        for _ in range(3):
            time.sleep(0.005)
            self.assertIsNone(q.read(block_ms=20))
        pending = self.redis.xpending_range(
            self.stream, self.group, min=mid, max=mid, count=1
        )
        self.assertEqual(len(pending), 1)
        self.assertGreater(
            int(pending[0]["times_delivered"]), 3, "每次重抢递增（副作用，见回报评估）"
        )
        self.assertEqual(self.redis.xlen(q.dlq_key), 0, "拒绝期间永不死信")


class TestRedisFlowWorkerDispatch(unittest.TestCase):
    def test_dispatch_start_and_resume(self):
        from plaita.server.flow_worker import RedisFlowWorker
        from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/0",
            queue_name="q",
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            redis_client=fakeredis.FakeRedis(decode_responses=True),
            enable_registry=False,
        )
        with patch.object(worker, "start_flow") as start:
            worker._dispatch_task({"type": "start", "flow_id": "f", "params": {}, "version": "1"})
            # rebase 组合：execution_id（G1 预铸）+ dedup_key / delivery_count
            # （波次二任务③/①）三者一起透传
            start.assert_called_once_with(
                "f", {}, "1", execution_id=None, dedup_key=None, delivery_count=None)
        with patch.object(worker, "start_flow") as start:
            # BFF 预铸 id 随消息透传（P0 可见性：提交方即刻可轮询）
            worker._dispatch_task({"type": "start", "flow_id": "f", "params": {},
                                   "version": "1", "execution_id": "pre-1"})
            start.assert_called_once_with(
                "f", {}, "1", execution_id="pre-1", dedup_key=None, delivery_count=None)
        with patch.object(worker, "resume_flow") as resume:
            worker._dispatch_task(
                {
                    "type": "resume",
                    "flow_id": "f",
                    "execution_id": "e",
                    "resume_type": "event",
                    "data": {"k": 1},
                }
            )
            resume.assert_called_once_with("f", "e", "event", {"k": 1}, delivery_count=None)

    def test_dispatch_unknown_type_raises(self):
        from plaita.server.flow_worker import RedisFlowWorker
        from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/0",
            queue_name="q",
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            redis_client=fakeredis.FakeRedis(decode_responses=True),
            enable_registry=False,
        )
        with self.assertRaises(ValueError):
            worker._dispatch_task({"type": "nope"})


if __name__ == "__main__":
    unittest.main()
