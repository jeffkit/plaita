"""#43 队列残留回收：已 XACK 未 XDEL 的 Stream 条目污染 XLEN 积压读数。

背景：正常 ack 路径 = XACK（task_queue.ack）→ best-effort XDEL；两步之间
进程被杀就留下「已确认未删除」的残留条目（XDEL 是仓库唯一的删除点，所以
残留只可能来自这个窗口——跳过一次 ack 的条目留在消费组 PEL 里是 pending，
本 sweep 一概不动）。它们只增不减地计入 XLEN，把运维的「队列积压」读数读成
假阳性（2026-10-07 实测据 XLEN=3 误判「新系统未投产」，实为前一日演练残留）。

兜底：worker 启动时扫一次 + 每 DEFAULT_RESIDUE_SWEEP_INTERVAL_SECONDS 扫一次
（task_queue.sweep_acked_residue），只删「id ≤ 消费组 last-delivered-id 且不在
本组 PEL 中」的条目——未投递的合法积压与未 ack 的 pending 一概不动。
"""
from __future__ import annotations

import json
import threading
import time
import unittest
from unittest.mock import patch

import pytest

pytest.importorskip("cachetools")
pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.task_queue import (
    DEFAULT_CONSUMER_GROUP,
    PAYLOAD_FIELD,
    RedisStreamTaskQueue,
    enqueue_task,
)


def _add_residue(redis, stream, group, bodies, consumer="c1"):
    """手工构造残留：XADD → XREADGROUP（交付）→ XACK，**不** XDEL。"""
    redis.xgroup_create(stream, group, id="0", mkstream=True)
    ids = []
    for body in bodies:
        mid = enqueue_task(redis, stream, body)
        redis.xreadgroup(
            groupname=group, consumername=consumer, streams={stream: ">"}, count=1
        )
        redis.xack(stream, group, mid)
        ids.append(mid)
    return ids


class TestSweepAckedResidue(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.stream = "plaita:flow:queue:residue"
        self.group = DEFAULT_CONSUMER_GROUP
        self.q = RedisStreamTaskQueue(
            self.redis, self.stream, group_name=self.group, consumer_name="sweeper"
        )

    def _lag(self) -> int:
        return self.redis.xinfo_groups(self.stream)[0]["lag"]

    def test_acked_residue_swept_and_pending_semantics_unchanged(self):
        """验收：3 条残留 → sweep 删除；XPENDING/lag 的既有语义不变。"""
        residue = _add_residue(
            self.redis,
            self.stream,
            self.group,
            [{"type": "start", "flow_id": f"f{i}"} for i in range(3)],
        )
        self.assertEqual(self.redis.xlen(self.stream), 3)
        pending_before = self.redis.xpending_range(
            self.stream, self.group, min="-", max="+", count=100
        )
        lag_before = self._lag()
        last_delivered_before = self.redis.xinfo_groups(self.stream)[0]["last-delivered-id"]
        self.assertEqual(pending_before, [])
        self.assertEqual(lag_before, 0)

        swept = self.q.sweep_acked_residue()

        self.assertEqual(swept, 3)
        self.assertEqual(self.redis.xlen(self.stream), 0)
        # PEL 语义不变：无 pending、无悬挂 id
        self.assertEqual(
            self.redis.xpending_range(self.stream, self.group, min="-", max="+", count=100),
            pending_before,
        )
        self.assertEqual(self.redis.xpending(self.stream, self.group)["pending"], 0)
        # lag = 「未投递条目数」的既有语义不变：本 sweep 不动 last-delivered-id
        # 也不动 entries-read，故 0 条未投递仍是 0。真 Redis 8.0.2 实测 XDEL 后
        # lag 仍为 0（lag 有 0 下限）；fakeredis 的公式未夹紧（model/_stream.py：
        # lag = len(stream) - entries-read）会算成 -3，故按语义取 0 下限比较，
        # 不 assert 不可能的负值。
        groups = self.redis.xinfo_groups(self.stream)[0]
        self.assertEqual(groups["last-delivered-id"], last_delivered_before)
        self.assertEqual(max(0, self._lag()), max(0, lag_before))
        self.assertEqual(max(0, lag_before), 0)
        self.assertEqual(self.q.stats()["residue_swept"], 3)
        self.assertEqual(len(residue), 3)

    def test_pending_unacked_entry_never_swept(self):
        """已交付未 ack 的 pending 条目：一条都不能删（at-least-once 底线）。"""
        _add_residue(self.redis, self.stream, self.group, [{"type": "start", "flow_id": "acked"}])
        pending_id = enqueue_task(self.redis, self.stream, {"type": "resume", "flow_id": "p"})
        self.redis.xreadgroup(
            groupname=self.group, consumername="c1", streams={self.stream: ">"}, count=1
        )

        self.assertEqual(self.q.sweep_acked_residue(), 1)

        self.assertEqual(self.redis.xlen(self.stream), 1)
        entries = self.redis.xrange(self.stream, min="-", max="+")
        self.assertEqual([mid for mid, _ in entries], [pending_id])
        self.assertEqual(self.redis.xpending(self.stream, self.group)["pending"], 1)
        # 仍可被回收重投（残留回收没把它变成僵尸）
        self.q.claim_min_idle_ms = 1
        time.sleep(0.01)
        task = self.q.read(block_ms=100)
        self.assertIsNotNone(task)
        self.assertEqual(task.message_id, pending_id)

    def test_undelivered_entries_are_kept(self):
        """未交付条目（id > last-delivered-id）是合法积压，绝不能被删。"""
        redis, stream, group = self.redis, self.stream, self.group
        redis.xgroup_create(stream, group, id="0", mkstream=True)
        first = enqueue_task(redis, stream, {"type": "start", "flow_id": "delivered"})
        second = enqueue_task(redis, stream, {"type": "start", "flow_id": "unread-1"})
        third = enqueue_task(redis, stream, {"type": "start", "flow_id": "unread-2"})
        redis.xreadgroup(groupname=group, consumername="c1", streams={stream: ">"}, count=1)
        redis.xack(stream, group, first)  # 残留：已 ack 未 XDEL

        self.assertEqual(self.q.sweep_acked_residue(), 1)

        remaining = [mid for mid, _ in redis.xrange(stream, min="-", max="+")]
        self.assertEqual(remaining, [second, third])
        # 未投递的两条照常可消费
        task = self.q.read(block_ms=100)
        self.assertEqual(task.message_id, second)

    def test_batch_bounded_and_progress_across_calls(self):
        """单轮删除量有界（batch_size）；残留在后续轮次继续收敛。"""
        _add_residue(
            self.redis,
            self.stream,
            self.group,
            [{"type": "start", "flow_id": f"f{i}"} for i in range(3)],
        )
        deleted = [self.q.sweep_acked_residue(batch_size=1) for _ in range(4)]
        self.assertEqual(deleted, [1, 1, 1, 0])
        self.assertEqual(self.redis.xlen(self.stream), 0)
        self.assertEqual(self.q.stats()["residue_swept"], 3)

    def test_clean_queue_is_noop(self):
        """无残留 = 幂等空转（返回 0，不产生任何写入）。"""
        _add_residue(self.redis, self.stream, self.group, [{"type": "start", "flow_id": "f"}])
        self.assertEqual(self.q.sweep_acked_residue(), 1)
        self.assertEqual(self.q.sweep_acked_residue(), 0)
        self.assertEqual(self.q.stats()["residue_swept"], 1)

    def test_no_group_is_noop(self):
        """消费组尚未建立（stream 不存在）：不抛异常、返回 0。"""
        self.assertEqual(self.q.sweep_acked_residue(), 0)

    def test_best_effort_on_redis_failures(self):
        """best-effort：扫描/PEL/XDEL 任一失败都只返回 0，不炸穿消费循环。"""
        _add_residue(self.redis, self.stream, self.group, [{"type": "start", "flow_id": "f"}])
        with patch.object(self.redis, "xinfo_groups", side_effect=RuntimeError("down")):
            self.assertEqual(self.q.sweep_acked_residue(), 0)
        with patch.object(self.redis, "xpending_range", side_effect=RuntimeError("down")):
            self.assertEqual(self.q.sweep_acked_residue(), 0)
        with patch.object(self.redis, "xdel", side_effect=RuntimeError("down")):
            self.assertEqual(self.q.sweep_acked_residue(), 0)
        # 读不到 PEL / 删不掉时泄漏残留，但绝不误删
        self.assertEqual(self.redis.xlen(self.stream), 1)

    def test_payload_field_is_the_one_sweep_preserves(self):
        """回收只删整条目，不改条目内容（payload 字段名与 enqueue 一致）。"""
        redis = self.redis
        redis.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        mid = enqueue_task(redis, self.stream, {"type": "start", "flow_id": "keep"})
        redis.xreadgroup(
            groupname=self.group, consumername="c1", streams={self.stream: ">"}, count=1
        )
        entry = redis.xrange(self.stream, min="-", max="+")[0]
        self.assertEqual(json.loads(entry[1][PAYLOAD_FIELD])["flow_id"], "keep")
        # 未 ack → 不删
        self.assertEqual(self.q.sweep_acked_residue(), 0)
        self.assertEqual(redis.xrange(self.stream, min="-", max="+")[0][0], mid)


class _RecordingQueue:
    def __init__(self):
        self.sweep_calls = 0

    def ensure_group(self):
        pass

    def read(self, block_ms=1000):
        return None

    def sweep_acked_residue(self, batch_size=256):
        self.sweep_calls += 1
        return 0


def _skeleton_worker(interval):
    from plaita.server.flow_worker import RedisFlowWorker

    w = RedisFlowWorker.__new__(RedisFlowWorker)
    w._running = True
    w.read_block_ms = 1000
    w._residue_sweep_interval_seconds = interval
    w._last_residue_sweep = None
    w._residue_sweep_lock = threading.Lock()
    return w


class TestWorkerSweepSchedule(unittest.TestCase):
    def test_startup_sweeps_then_respects_interval(self):
        """启动时（首次）扫一次；间隔未到不重复扫，到期后再扫。"""
        w = _skeleton_worker(300.0)
        queue = _RecordingQueue()
        w._sweep_residue_if_due(queue)
        self.assertEqual(queue.sweep_calls, 1, "启动后第一次应立即扫")
        w._sweep_residue_if_due(queue)
        self.assertEqual(queue.sweep_calls, 1, "间隔内不得重复扫")
        w._last_residue_sweep = time.monotonic() - 301.0
        w._sweep_residue_if_due(queue)
        self.assertEqual(queue.sweep_calls, 2, "到期后应兜底再扫一次")

    def test_non_positive_interval_disables_sweep(self):
        for interval in (0.0, -1.0):
            with self.subTest(interval=interval):
                w = _skeleton_worker(interval)
                queue = _RecordingQueue()
                w._sweep_residue_if_due(queue)
                self.assertEqual(queue.sweep_calls, 0)

    def test_queue_without_sweep_method_is_skipped(self):
        """旧队列类（无 sweep_acked_residue）：跳过而不是 AttributeError 炸消费线程。"""
        w = _skeleton_worker(300.0)

        class _BareQueue:
            def ensure_group(self):
                pass

            def read(self, block_ms=1000):
                w._running = False
                return None

        queue = _BareQueue()
        w._sweep_residue_if_due(queue)  # 不抛
        w._consume_loop(queue)  # 消费循环照常跑完一轮并退出
        self.assertIsNone(w._last_residue_sweep, "无该方法时不记录扫描时间")

    def test_consume_loop_sweeps_periodically(self):
        """消费循环里按间隔兜底（并发下多线程共享一份状态，锁不阻塞对方）。"""
        w = _skeleton_worker(300.0)
        queue = _RecordingQueue()

        def stop_after_first_iteration(block_ms=1000):
            w._running = False
            return None

        queue.read = stop_after_first_iteration
        w._consume_loop(queue)
        self.assertEqual(queue.sweep_calls, 1)

        # 另一线程正在扫（锁被持有）时不重复扫
        w2 = _skeleton_worker(300.0)
        queue2 = _RecordingQueue()
        w2._residue_sweep_lock.acquire()
        try:
            w2._sweep_residue_if_due(queue2)
        finally:
            w2._residue_sweep_lock.release()
        self.assertEqual(queue2.sweep_calls, 0)


class TestRunSweepsAtStartup(unittest.TestCase):
    def test_run_sweeps_startup_residue(self):
        """端到端：worker.run() 启动即回收已有残留（本轮 #43 的实况场景）。"""
        from plaita.server.flow_worker import RedisFlowWorker
        from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

        redis = fakeredis.FakeRedis(decode_responses=True)
        stream = "plaita:flow:queue:v2"
        _add_residue(
            redis,
            stream,
            DEFAULT_CONSUMER_GROUP,
            [{"type": "start", "flow_id": f"residue-{i}"} for i in range(3)],
        )
        self.assertEqual(redis.xlen(stream), 3)
        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/0",
            queue_name=stream,
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            redis_client=redis,
            enable_registry=False,
            enable_redis_logging=False,
            read_block_ms=50,
        )
        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        deadline = time.time() + 5
        while redis.xlen(stream) and time.time() < deadline:
            time.sleep(0.02)
        worker.stop()
        thread.join(timeout=5)

        self.assertFalse(thread.is_alive(), "run() 未在 stop() 后退出")
        self.assertEqual(redis.xlen(stream), 0)
        self.assertEqual(redis.xpending(stream, DEFAULT_CONSUMER_GROUP)["pending"], 0)


class TestStatsSurface(unittest.TestCase):
    def test_stats_exposes_residue_swept(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        q = RedisStreamTaskQueue(redis, "plaita:flow:queue:stats", group_name="g")
        self.assertEqual(q.stats()["residue_swept"], 0)


if __name__ == "__main__":
    unittest.main()
