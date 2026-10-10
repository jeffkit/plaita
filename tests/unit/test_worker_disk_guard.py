"""#49 worker claim 前的本机盘预检：低盘机不再「抢单白跑」。

现象（2026-10-08 实测一夜 11 次）：多 worker 池下，低盘机器先领到任务，
flow 的 preflight 节点（跑在领取宿主上）才判 `os.statvfs(repo)` 不足 →
`retry-later`；健康的富盘机器全程空闲。且 retry-later 退避升档把瞬时低盘
放大成 ≈6h 停机。

修法：worker 在 `XREADGROUP` **之前**预检本机可用盘
（`PLAITA_WORKER_MIN_FREE_DISK_GIB` / `--min-free-disk-gib`，默认 0 = 关闭），
低于守线就不领任务、空转重探，盘回线自动恢复（无需重启）——不产生 claim，
也就不烧 delivery、不产生 retry-later 回评。
"""
from __future__ import annotations

import os
import threading
import time
import unittest
from contextlib import contextmanager
from unittest.mock import patch

import pytest

pytest.importorskip("cachetools")
pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import (
    DEFAULT_DISK_GUARD_POLL_SECONDS,
    RedisFlowWorker,
    _disk_guard_path_from_env,
    _free_disk_gib,
    _min_free_disk_gib_from_env,
)
from plaita.server.task_queue import (
    DEFAULT_CONSUMER_GROUP,
    RedisStreamTaskQueue,
    enqueue_task,
)

GIB = 1024 ** 3
THRESHOLD_ENV = "PLAITA_WORKER_MIN_FREE_DISK_GIB"
PATH_ENV = "PLAITA_WORKER_DISK_GUARD_PATH"


class _Stat:
    """os.statvfs 返回值的最小替身（只用到 f_bavail/f_frsize）。"""

    def __init__(self, free_gib: float):
        self.f_frsize = 4096
        self.f_bavail = int(free_gib * GIB / self.f_frsize)


@contextmanager
def _disk(free_gib):
    """把本机可用盘钉在 free_gib；``None`` = 探测失败（抛 OSError）。"""
    side = OSError("no such path") if free_gib is None else None
    patcher = (
        patch("plaita.server.flow_worker.os.statvfs", side_effect=side)
        if side is not None
        else patch("plaita.server.flow_worker.os.statvfs", return_value=_Stat(free_gib))
    )
    with patcher:
        yield


@contextmanager
def _per_host_disks(free_by_path):
    """按探测路径给不同宿主配不同可用盘（{'/diskA': 15.6, '/diskB': 198.0}）。"""

    def _statvfs(path, *args, **kwargs):
        return _Stat(free_by_path[path])

    with patch("plaita.server.flow_worker.os.statvfs", side_effect=_statvfs):
        yield


class TestFreeDiskProbe(unittest.TestCase):
    def test_free_disk_gib_reads_statvfs(self):
        with _disk(15.5):
            self.assertAlmostEqual(_free_disk_gib("/"), 15.5, places=3)

    def test_probe_failure_is_none(self):
        with _disk(None):
            self.assertIsNone(_free_disk_gib("/nonexistent"))

    def test_probe_without_statvfs_is_none(self):
        """无 ``os.statvfs`` 的平台（Windows）：预检不可用 → None（放行），
        不得把 AttributeError 冒到消费循环（2026-10-10 评审）。"""
        with patch(
            "plaita.server.flow_worker.os.statvfs",
            side_effect=AttributeError("module 'os' has no attribute 'statvfs'"),
        ):
            self.assertIsNone(_free_disk_gib("/"))


class TestThresholdFromEnv(unittest.TestCase):
    def setUp(self):
        for name in (THRESHOLD_ENV, PATH_ENV):
            os.environ.pop(name, None)

    def tearDown(self):
        for name in (THRESHOLD_ENV, PATH_ENV):
            os.environ.pop(name, None)

    def test_unset_and_blank_disable(self):
        self.assertEqual(_min_free_disk_gib_from_env(), 0.0)
        for raw in ("", "   "):
            with self.subTest(raw=raw):
                os.environ[THRESHOLD_ENV] = raw
                self.assertEqual(_min_free_disk_gib_from_env(), 0.0)

    def test_parses_value_and_clamps_negative(self):
        for raw, expected in (("20", 20.0), ("15.5", 15.5), ("-3", 0.0)):
            with self.subTest(raw=raw):
                os.environ[THRESHOLD_ENV] = raw
                self.assertEqual(_min_free_disk_gib_from_env(), expected)

    def test_illegal_value_disables(self):
        os.environ[THRESHOLD_ENV] = "not-a-number"
        self.assertEqual(_min_free_disk_gib_from_env(), 0.0)

    def test_path_default_is_cwd(self):
        self.assertEqual(_disk_guard_path_from_env(), ".")
        os.environ[PATH_ENV] = "/data"
        self.assertEqual(_disk_guard_path_from_env(), "/data")


def _skeleton_worker(**attrs):
    """只带消费循环所需状态的骨架 worker（不走 __init__，同既有单测口径）。"""
    w = RedisFlowWorker.__new__(RedisFlowWorker)
    w._running = True
    w.read_block_ms = 50
    w._residue_sweep_interval_seconds = 0.0  # 关闭残留 sweep，隔离本测关注点
    w._last_residue_sweep = None
    w._residue_sweep_lock = threading.Lock()
    w._active_task_count = 0
    w._active_count_lock = threading.Lock()
    w._enable_registry = False
    w.__dict__.update(attrs)
    return w


class _RecordingQueue:
    """真队列 + read 计数（claim = XREADGROUP 是否真的发生）。"""

    def __init__(self, inner):
        self.inner = inner
        self.reads = 0

    def read(self, block_ms=1000):
        self.reads += 1
        return self.inner.read(block_ms)

    def __getattr__(self, name):
        return getattr(self.inner, name)


class TestGateDecision(unittest.TestCase):
    def test_blocks_below_and_allows_at_or_above(self):
        w = _skeleton_worker(_min_free_disk_gib=20.0, _disk_guard_path="/disk")
        with _disk(15.6):
            self.assertTrue(w._disk_guard_blocks_claim(), "低于守线必须拦")
        with _disk(20.0):
            self.assertFalse(w._disk_guard_blocks_claim(), "等于守线放行（>= 判据）")
        with _disk(198.0):
            self.assertFalse(w._disk_guard_blocks_claim())

    def test_zero_threshold_disabled(self):
        w = _skeleton_worker(_min_free_disk_gib=0.0, _disk_guard_path="/disk")
        with _disk(0.1):
            self.assertFalse(w._disk_guard_blocks_claim())

    def test_missing_attribute_disabled(self):
        """__init__ 未跑过的骨架（旧路径）：不得 AttributeError 炸消费线程。"""
        w = _skeleton_worker()
        with _disk(0.1):
            self.assertFalse(w._disk_guard_blocks_claim())

    def test_probe_failure_allows(self):
        """预检自身故障不得让整池停摆：放行（宁可能白跑一次）。"""
        w = _skeleton_worker(_min_free_disk_gib=20.0, _disk_guard_path="/gone")
        with _disk(None):
            self.assertFalse(w._disk_guard_blocks_claim())

    def test_state_transition_is_edge_triggered(self):
        w = _skeleton_worker(_min_free_disk_gib=20.0, _disk_guard_path="/disk")
        with _disk(10.0):
            self.assertTrue(w._disk_guard_blocks_claim())
            self.assertTrue(w._disk_guard_active)
            self.assertTrue(w._disk_guard_blocks_claim())
            self.assertTrue(w._disk_guard_active, "持续低盘不重复置位")
        with _disk(30.0):
            self.assertFalse(w._disk_guard_blocks_claim())
            self.assertFalse(w._disk_guard_active, "盘回线应清位")


class TestConsumeLoopClaimGate(unittest.TestCase):
    """验收：构造「A 盘 < min、B 盘充足」——A 不读、B 读并完成。"""

    def _reset(self):
        self.redis = fakeredis.FakeRedis(decode_responses=True)
        self.stream = "plaita:flow:queue:diskguard"
        self.dispatched = []

    def _queue(self, consumer):
        inner = RedisStreamTaskQueue(
            self.redis,
            self.stream,
            group_name=DEFAULT_CONSUMER_GROUP,
            consumer_name=consumer,
            claim_min_idle_ms=1,
        )
        inner.ensure_group()  # 队列已投产（消费组在）：低盘期游标可否推进才可观测
        return _RecordingQueue(inner)

    def _worker(self, disk_path="/disk", **attrs):
        w = _skeleton_worker(
            _min_free_disk_gib=20.0,
            _disk_guard_path=disk_path,
            _disk_guard_poll_seconds=0.01,
            **attrs,
        )
        w._dispatch_task = lambda body, delivery_count=1: self.dispatched.append(body)
        return w

    def _run_loop_briefly(self, w, queue, seconds=0.2):
        w._running = True
        t = threading.Thread(target=w._consume_loop, args=(queue,), daemon=True)
        t.start()
        time.sleep(seconds)
        w._running = False
        t.join(timeout=5)
        self.assertFalse(t.is_alive(), "stop() 后消费循环未退出")

    def _last_delivered_id(self):
        for group in self.redis.xinfo_groups(self.stream):
            if group["name"] == DEFAULT_CONSUMER_GROUP:
                return group["last-delivered-id"]
        raise AssertionError("consumer group missing")

    def test_low_disk_worker_never_claims_then_recovers(self):
        self._reset()
        queue = self._queue("c1")
        enqueue_task(self.redis, self.stream, {"type": "start", "flow_id": "f1"})
        w = self._worker()

        with _disk(15.6):  # A 机：盘低于守线
            self._run_loop_briefly(w, queue)
        self.assertEqual(queue.reads, 0, "低盘机不得 XREADGROUP（零 claim）")
        self.assertEqual(self.dispatched, [], "低盘机不得跑任何任务")
        self.assertEqual(self.redis.xpending(self.stream, DEFAULT_CONSUMER_GROUP)["pending"], 0)
        self.assertEqual(self._last_delivered_id(), "0-0", "交付游标不得推进")
        self.assertEqual(self.redis.xlen(self.stream), 1, "任务仍留在队列")

        with _disk(198.0):  # 盘回线：无需重启，下一轮自动恢复领取
            self._run_loop_briefly(w, queue, seconds=0.3)
        self.assertGreaterEqual(queue.reads, 1)
        self.assertEqual([b["flow_id"] for b in self.dispatched], ["f1"])
        self.assertEqual(self.redis.xpending(self.stream, DEFAULT_CONSUMER_GROUP)["pending"], 0)

    def test_low_disk_host_skips_while_healthy_host_claims(self):
        """同一队列上 A（低盘）空转、B（富盘）领取并完成——池内不再算力错配。"""
        self._reset()
        queue_a = self._queue("host-a")
        queue_b = self._queue("host-b")
        enqueue_task(self.redis, self.stream, {"type": "start", "flow_id": "f2"})
        w_a = self._worker(disk_path="/diskA")
        w_b = self._worker(disk_path="/diskB")

        with _per_host_disks({"/diskA": 15.6, "/diskB": 198.0}):
            threads = []
            for w, q in ((w_a, queue_a), (w_b, queue_b)):
                w._running = True
                t = threading.Thread(target=w._consume_loop, args=(q,), daemon=True)
                t.start()
                threads.append(t)
            deadline = time.time() + 5
            while not self.dispatched and time.time() < deadline:
                time.sleep(0.02)
            for w in (w_a, w_b):
                w._running = False
            for t in threads:
                t.join(timeout=5)

        self.assertEqual([b["flow_id"] for b in self.dispatched], ["f2"], "B 应领到并完成")
        self.assertEqual(queue_a.reads, 0, "A 全程零 claim")
        self.assertGreaterEqual(queue_b.reads, 1)
        self.assertEqual(self.redis.xpending(self.stream, DEFAULT_CONSUMER_GROUP)["pending"], 0)



class TestWorkerConstruction(unittest.TestCase):
    def setUp(self):
        for name in (THRESHOLD_ENV, PATH_ENV):
            os.environ.pop(name, None)
        from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

        self._storages = (MemoryExecutionStorage(), MemoryFlowStorage())

    def tearDown(self):
        for name in (THRESHOLD_ENV, PATH_ENV):
            os.environ.pop(name, None)

    def _worker(self, **kwargs):
        return RedisFlowWorker(
            redis_url="redis://localhost:6379/0",
            queue_name="plaita:flow:queue:ctor",
            execution_storage=self._storages[0],
            flow_storage=self._storages[1],
            redis_client=fakeredis.FakeRedis(decode_responses=True),
            enable_registry=False,
            enable_redis_logging=False,
            **kwargs,
        )

    def test_defaults_disabled(self):
        w = self._worker()
        self.assertEqual(w._min_free_disk_gib, 0.0)
        self.assertEqual(w._disk_guard_path, ".")
        self.assertEqual(w._disk_guard_poll_seconds, DEFAULT_DISK_GUARD_POLL_SECONDS)

    def test_env_and_param_resolution(self):
        os.environ[THRESHOLD_ENV] = "20"
        os.environ[PATH_ENV] = "/data"
        w = self._worker()
        self.assertEqual(w._min_free_disk_gib, 20.0)
        self.assertEqual(w._disk_guard_path, "/data")
        explicit = self._worker(min_free_disk_gib=5.0, disk_guard_path="/tmp")
        self.assertEqual(explicit._min_free_disk_gib, 5.0, "构造参数优先于 env")
        self.assertEqual(explicit._disk_guard_path, "/tmp")


if __name__ == "__main__":
    unittest.main()
