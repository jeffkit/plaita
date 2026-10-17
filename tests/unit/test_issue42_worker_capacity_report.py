"""#42（W10-a）worker 侧容量上报接线：注册键 metadata + 盘闸翻转 + 指标。

`tests/unit/test_issue42_worker_capacity.py` 覆盖容量块契约与派发门（纯函数）；
本文件覆盖**接线**——`RedisFlowWorker` 真的会把它写进
``plaita:registry:flow_worker:<id>``（派发侧唯一能看到的地方）。

## 红先证

基线实测（origin/main 87b8ad0）：``RedisFlowWorker.__init__`` 写入的 metadata
只有 ``queue_name`` / ``redis_url``(+host/port/db) / ``cache_size`` / ``cache_ttl``；
``_disk_guard_blocks_claim`` 只翻本进程 ``_disk_guard_active``，**不写注册表**。
故「注册键里出现 ``worker_capacity``」在基线必然失败。

## 守卫口径

- 盘探针失败（``os.statvfs`` 抛）→ 快照里 ``free_disk_gib=None``（不是 0）；
- ``_enable_registry=False`` / 未初始化 registry / Redis 抛错 → 静默降级，
  绝不把异常冒进消费循环；
- 盘闸未启用（守线 0）→ 不变更注册表（默认零行为变化）。
"""
from __future__ import annotations

import json
import threading
import unittest
from contextlib import contextmanager
from unittest.mock import patch

import pytest

pytest.importorskip("cachetools")
pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis  # noqa: E402

from plaita.event.memory import InMemoryEventBus  # noqa: E402
from plaita.server.capacity import (  # noqa: E402
    CAPACITY_METADATA_KEY,
    capacity_from_metadata,
)
from plaita.server.flow_worker import RedisFlowWorker  # noqa: E402
from plaita.server.registry import ServiceInfo, ServiceRegistry  # noqa: E402
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage  # noqa: E402

GIB = 1024 ** 3


class _Stat:
    """``os.statvfs`` 最小替身（只被 f_bavail/f_frsize 读）。"""

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


def _worker_with_registry(redis):
    """骨架 worker（不走 ``__init__``，同既有单测口径）+ 真注册表对象。"""
    w = RedisFlowWorker.__new__(RedisFlowWorker)
    w._enable_registry = True
    w._registry = ServiceRegistry(redis, ttl=30, heartbeat_interval=10)
    w._service_info = ServiceInfo(
        instance_id="w-42",
        service_type="flow_worker",
        host="test-host/127.0.0.1",
        metadata={"queue_name": "plaita:flow:queue"},
    )
    w._min_free_disk_gib = 20.0
    w._disk_guard_path = "/disk"
    w._active_task_count = 0
    w._active_count_lock = threading.Lock()
    w.concurrency = 2
    w.__dict__["_disk_guard_active"] = False
    return w


def _registered_capacity(redis):
    raw = redis.get("plaita:registry:flow_worker:w-42")
    assert raw, "注册键必须已写入"
    data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
    return capacity_from_metadata(data.get("metadata") or {})


class TestCapacitySnapshot(unittest.TestCase):
    def test_snapshot_carries_disk_load_and_sampling_time(self):
        w = _worker_with_registry(fakeredis.FakeRedis(decode_responses=True))
        with _disk(123.5):
            snap = w.capacity_snapshot()
        self.assertAlmostEqual(snap["free_disk_gib"], 123.5, places=3)
        self.assertAlmostEqual(snap["min_free_disk_gib"], 20.0, places=3)
        self.assertEqual(snap["concurrency"], 2)
        self.assertEqual(snap["active_tasks"], 0)
        self.assertFalse(snap["disk_blocked"])
        self.assertTrue(snap["sampled_at"], "快照必须带采样时刻（新鲜度判定的唯一依据）")

    def test_probe_failure_is_null_not_zero(self):
        """探针失败 ≠ 没盘：写 null 让派发侧走「放行」，写 0 会被当低盘拒单。"""
        w = _worker_with_registry(fakeredis.FakeRedis(decode_responses=True))
        with _disk(None):
            snap = w.capacity_snapshot()
        self.assertIsNone(snap["free_disk_gib"])

    def test_snapshot_reflects_disk_guard_state(self):
        w = _worker_with_registry(fakeredis.FakeRedis(decode_responses=True))
        w.__dict__["_disk_guard_active"] = True
        with _disk(3.0):
            snap = w.capacity_snapshot()
        self.assertTrue(snap["disk_blocked"])


class TestRegistryWrite(unittest.TestCase):
    def test_report_writes_capacity_into_registry_metadata(self):
        """核心断言：派发侧唯一可见面（注册键）里出现容量块。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        w = _worker_with_registry(redis)
        with _disk(88.0):
            w._report_capacity()
        cap = _registered_capacity(redis)
        self.assertIsNotNone(cap, "注册键 metadata 必须含 worker_capacity")
        self.assertAlmostEqual(cap["free_disk_gib"], 88.0, places=3)
        self.assertEqual(cap["concurrency"], 2)

    def test_report_keeps_existing_metadata_keys(self):
        """增量不替换：queue_name 等既有键必须原样保留（运维诊断面）。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        w = _worker_with_registry(redis)
        with _disk(88.0):
            w._report_capacity()
        data = json.loads(redis.get("plaita:registry:flow_worker:w-42"))
        self.assertEqual(data["metadata"]["queue_name"], "plaita:flow:queue")

    def test_registry_disabled_is_noop(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        w = _worker_with_registry(redis)
        w._enable_registry = False
        with _disk(1.0):
            w._report_capacity()
        self.assertIsNone(redis.get("plaita:registry:flow_worker:w-42"))

    def test_uninitialized_registry_is_noop(self):
        w = RedisFlowWorker.__new__(RedisFlowWorker)
        w._enable_registry = True
        w._report_capacity()  # 不得抛（骨架 worker 无 _registry/_service_info）

    def test_redis_failure_does_not_raise(self):
        class _Boom:
            def setex(self, *a, **k):
                raise RuntimeError("redis down")

        w = _worker_with_registry(_Boom())
        with _disk(50.0):
            w._report_capacity()  # 上报失败只告警，绝不冒到消费循环


class TestDiskGuardTransitionReports(unittest.TestCase):
    def test_entering_low_disk_reports_blocked_state(self):
        """缺口核心：进入低盘态必须当轮上报（此前只在本进程空转）。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        w = _worker_with_registry(redis)
        with _disk(3.0):
            self.assertTrue(w._disk_guard_blocks_claim())
        cap = _registered_capacity(redis)
        self.assertIsNotNone(cap)
        self.assertTrue(cap["disk_blocked"], "低盘态必须上报 disk_blocked=True")
        self.assertAlmostEqual(cap["free_disk_gib"], 3.0, places=3)

    def test_recovering_disk_reports_unblocked_state(self):
        """盘回线要撤回标记——否则派发侧在心跳周期内继续拒单。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        w = _worker_with_registry(redis)
        with _disk(3.0):
            w._disk_guard_blocks_claim()
        with _disk(99.0):
            self.assertFalse(w._disk_guard_blocks_claim())
        cap = _registered_capacity(redis)
        self.assertFalse(cap["disk_blocked"])
        self.assertAlmostEqual(cap["free_disk_gib"], 99.0, places=3)

    def test_threshold_disabled_never_touches_registry(self):
        """负向对照：守线 0（默认）＝盘闸关，注册表一个字节都不该被写。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        w = _worker_with_registry(redis)
        w._min_free_disk_gib = 0.0
        with _disk(0.1):
            self.assertFalse(w._disk_guard_blocks_claim())
        self.assertIsNone(redis.get("plaita:registry:flow_worker:w-42"))

    def test_probe_failure_does_not_report(self):
        """负向对照：探不到盘时既不拦也不写（不得把故障写成容量）。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        w = _worker_with_registry(redis)
        with _disk(None):
            self.assertFalse(w._disk_guard_blocks_claim())
        self.assertIsNone(redis.get("plaita:registry:flow_worker:w-42"))


class TestRealInitReportsCapacity(unittest.TestCase):
    """走**真** ``__init__``（不是骨架 worker）：注册即带容量。

    接线若只挂在「盘闸翻转」上，worker 正常富盘启动时派发侧仍然看不见容量；
    这条守住启动路径。
    """

    def test_construction_registers_capacity_block(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        with _disk(77.0):
            worker = RedisFlowWorker(
                redis_url="redis://localhost:6379/9",
                queue_name="plaita:flow:queue:test42",
                execution_storage=MemoryExecutionStorage(),
                flow_storage=MemoryFlowStorage(),
                event_bus=InMemoryEventBus(),
                redis_client=redis,
                enable_registry=True,
                enable_redis_logging=False,
                min_free_disk_gib=20.0,
                disk_guard_path="/disk",
                concurrency=3,
            )

        instance_id = worker._service_info.instance_id
        raw = redis.get(f"plaita:registry:flow_worker:{instance_id}")
        self.assertIsNotNone(raw, "worker 构造完成即应有一条带容量的注册记录")
        data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        cap = capacity_from_metadata(data["metadata"])
        self.assertIsNotNone(cap)
        self.assertAlmostEqual(cap["free_disk_gib"], 77.0, places=3)
        self.assertEqual(cap["concurrency"], 3)
        self.assertEqual(data["metadata"]["queue_name"], "plaita:flow:queue:test42")


class TestActiveTasksReport(unittest.TestCase):
    def test_bump_active_pushes_fresh_capacity(self):
        """在跑数一变就直推：容量门判饱和靠的是新鲜快照，不能等心跳。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        w = _worker_with_registry(redis)
        with _disk(60.0):
            w._bump_active(+1)
        cap = _registered_capacity(redis)
        self.assertEqual(cap["active_tasks"], 1)
        self.assertEqual(cap["concurrency"], 2)
        with _disk(60.0):
            w._bump_active(+1)
        self.assertEqual(_registered_capacity(redis)["active_tasks"], 2)

    def test_active_count_never_negative_in_snapshot(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        w = _worker_with_registry(redis)
        with _disk(60.0):
            w._bump_active(-5)
        self.assertEqual(_registered_capacity(redis)["active_tasks"], 0)


class TestCapacityMetrics(unittest.TestCase):
    def test_metrics_text_exposes_capacity_gauges(self):
        """可观测：低盘/饱和必须能直接在看板上看见。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        w = _worker_with_registry(redis)
        w._running = True
        w._draining = threading.Event()
        w._alerter = None
        w._get_task_queue = lambda: type("Q", (), {"stats": lambda self: {"stream_key": "q"}})()
        w._resolve_consumer_name = lambda: "w-42"
        with _disk(4.0):
            text = w.metrics_text()
        self.assertIn("plaita_worker_free_disk_gib", text)
        self.assertIn("plaita_worker_disk_blocked", text)
        self.assertIn("plaita_worker_capacity_concurrency", text)
        self.assertIn('instance="w-42"', text)

    def test_metrics_text_survives_capacity_probe_failure(self):
        """容量探针炸了也不能打空整张看板（队列指标仍要出）。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        w = _worker_with_registry(redis)
        w._running = True
        w._draining = threading.Event()
        w._alerter = None
        w._get_task_queue = lambda: type("Q", (), {"stats": lambda self: {"stream_key": "q"}})()
        w._resolve_consumer_name = lambda: "w-42"
        w.capacity_snapshot = lambda: (_ for _ in ()).throw(RuntimeError("probe boom"))
        text = w.metrics_text()
        self.assertIn("plaita_worker_up", text)
        self.assertNotIn("plaita_worker_free_disk_gib", text)
