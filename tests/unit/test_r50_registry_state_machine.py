"""S18-02A registry 状态机：合法迁移 / 派发白名单 / 即时落盘 / closed 世代。

覆盖台账（duo-a2-ledger §S18-02）的验收判据：

- 纯函数：starting/draining/stopping/stopped/unknown 即使带完美容量也不可派；
  running 缺容量/陈旧容量仍放行（红线）；running + disk_blocked 拒。
- 真实（fakeredis）注册表：register 保留 draining/stopping、拒绝 stopped/closed；
  request_drain/stop 返回时键已是 draining/stopping（不等心跳）；
  注销 delete 失败留 stopped 墓碑、get 失败仍尝试 delete；
  closed 后 `_bump_active(±1)`/register/heartbeat 都不得复活键；
  draining 后容量不再刷新（容量上报仅 running）。
- ack 安全闸：非 running 一律「不保证收尾」。
"""
from __future__ import annotations

import json
import threading

import pytest

pytest.importorskip("cachetools")
pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis  # noqa: E402

from plaita.event.memory import InMemoryEventBus  # noqa: E402
from plaita.server import capacity as cap  # noqa: E402
from plaita.server.flow_worker import RedisFlowWorker  # noqa: E402
from plaita.server.registry import ServiceInfo, ServiceRegistry  # noqa: E402
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage  # noqa: E402

KEY = "plaita:registry:flow_worker:w-state"

# 负控兼容：旧生产文件没有这些常量时用字面量，让测试收集成功、断言精确红。
REASON_STARTING = getattr(cap, "REASON_STARTING", "status: starting")
REASON_STOPPING = getattr(cap, "REASON_STOPPING", "status: stopping")
REASON_STOPPED = getattr(cap, "REASON_STOPPED", "status: stopped")
REASON_STATUS_UNKNOWN = getattr(cap, "REASON_STATUS_UNKNOWN", "status: unknown")


def _fresh_block(**over):
    block = cap.build_capacity_block(
        free_disk_gib=100.0,
        min_free_disk_gib=0.0,
        disk_blocked=False,
        active_tasks=0,
        concurrency=1,
    )
    block.update(over)
    return block


def _read(redis, key=KEY):
    raw = redis.get(key)
    return None if raw is None else json.loads(raw)


def _worker_key(worker):
    return f"plaita:registry:flow_worker:{worker.instance_id}"


def _skeleton(redis, *, status="starting", instance_id="w-state"):
    """骨架 worker（不走 __init__，同既有单测口径）+ 真注册表对象。"""
    w = RedisFlowWorker.__new__(RedisFlowWorker)
    w.redis_client = redis
    w.queue_name = "test:queue"
    w._enable_registry = True
    w._registry = ServiceRegistry(redis, ttl=30, heartbeat_interval=10)
    w._service_info = ServiceInfo(
        instance_id=instance_id,
        service_type="flow_worker",
        host="h",
        status=status,
        metadata={"queue_name": "test:queue"},
    )
    w._active_task_count = 0
    w._active_count_lock = threading.Lock()
    w.concurrency = 1
    w._min_free_disk_gib = 0.0
    w._disk_guard_path = "."
    w._running = False
    w._drain_timer = None
    w._drain_started_at = None
    return w


def _full_worker(redis):
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/0",
        queue_name="test:queue",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        event_bus=InMemoryEventBus(),
        enable_registry=True,
        enable_redis_logging=False,
        redis_client=redis,
    )


@pytest.fixture
def redis():
    return fakeredis.FakeRedis(decode_responses=True)


class _RedisProxy:
    """可控失败的 Redis 代理（只覆盖 get/delete，其余委托）。"""

    def __init__(self, inner, *, get_error=None, delete_error=None):
        self.inner = inner
        self.get_error = get_error
        self.delete_error = delete_error
        self.deleted = []

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def get(self, *args, **kwargs):
        if self.get_error is not None:
            raise self.get_error
        return self.inner.get(*args, **kwargs)

    def delete(self, *args, **kwargs):
        self.deleted.append(args)
        if self.delete_error is not None:
            raise self.delete_error
        return self.inner.delete(*args, **kwargs)


class TestDispatchStatusWhitelist:
    """A46-01：status 是唯一派发白名单，且在容量判定之前。"""

    @pytest.mark.parametrize(
        "status,reason",
        [
            ("starting", REASON_STARTING),
            ("draining", cap.REASON_DRAINING),
            ("stopping", REASON_STOPPING),
            ("stopped", REASON_STOPPED),
            ("closed", REASON_STATUS_UNKNOWN),
            ("mystery", REASON_STATUS_UNKNOWN),
            ("", REASON_STATUS_UNKNOWN),
        ],
    )
    def test_non_running_is_never_dispatchable_even_with_perfect_capacity(
        self, status, reason
    ):
        cand = cap.DispatchCandidate(
            instance_id="w", status=status, capacity=_fresh_block()
        )
        assert cand.dispatchable is False
        assert cand.reject_reason() == reason
        assert cand.reject_reason().startswith("status:" if status != "draining" else "draining")

    def test_running_without_capacity_still_allowed(self):
        assert cap.DispatchCandidate(instance_id="w", status="running").dispatchable

    def test_running_with_stale_capacity_still_allowed(self):
        stale = _fresh_block(sampled_at="2000-01-01T00:00:00+00:00")
        cand = cap.DispatchCandidate(
            instance_id="w", status="running", capacity=stale
        )
        assert cand.dispatchable is True

    def test_running_disk_blocked_still_rejected(self):
        cand = cap.DispatchCandidate(
            instance_id="w", status="running",
            capacity=_fresh_block(disk_blocked=True),
        )
        assert cand.dispatchable is False
        assert cand.reject_reason() == cap.REASON_DISK

    def test_route_never_selects_non_running_even_with_best_capacity(self):
        running = cap.DispatchCandidate(
            instance_id="w-run", status="running",
            capacity=_fresh_block(free_disk_gib=1.0),
        )
        draining = cap.DispatchCandidate(
            instance_id="w-drain", status="draining",
            capacity=_fresh_block(free_disk_gib=999.0),
        )
        route = cap.decide_route([draining, running])
        assert route.target_instance_id == "w-run"
        assert [r.instance_id for r in route.rejected] == ["w-drain"]
        assert route.rejected[0].reason == cap.REASON_DRAINING


class TestRegistryTransitions:
    """合法迁移表：register 不得把 draining/stopping 复活成 running。"""

    def test_register_starting_becomes_running(self, redis):
        registry = ServiceRegistry(redis)
        info = ServiceInfo("w-state", "flow_worker", "h", status="starting")
        assert registry.register(info) is True
        assert info.status == "running"
        assert _read(redis)["status"] == "running"

    @pytest.mark.parametrize("status", ["draining", "stopping", "mystery"])
    def test_register_preserves_non_running_lifecycle_state(self, redis, status):
        registry = ServiceRegistry(redis)
        info = ServiceInfo("w-state", "flow_worker", "h", status=status)
        assert registry.register(info) is True
        assert info.status == status
        assert _read(redis)["status"] == status

    @pytest.mark.parametrize("status", ["stopped", "closed"])
    def test_register_rejects_terminal_states_without_writing(self, redis, status):
        registry = ServiceRegistry(redis)
        info = ServiceInfo("w-state", "flow_worker", "h", status=status)
        assert registry.register(info) is False
        assert redis.get(KEY) is None


class TestUnregisterTombstone:
    """A46-01 S5/S6：注销失败留 stopped 墓碑，get 失败仍尝试 delete。"""

    def test_success_deletes_key(self, redis):
        registry = ServiceRegistry(redis)
        registry.register(ServiceInfo("w-state", "flow_worker", "h"))
        assert registry.unregister("flow_worker", "w-state") is True
        assert redis.get(KEY) is None

    def test_delete_failure_leaves_stopped_tombstone(self, redis):
        registry = ServiceRegistry(redis)
        registry.register(ServiceInfo("w-state", "flow_worker", "h"))
        proxy = _RedisProxy(redis, delete_error=RuntimeError("delete failed"))
        registry.redis_client = proxy

        assert registry.unregister("flow_worker", "w-state") is False
        data = _read(redis)
        assert data["status"] == "stopped"
        assert 0 < redis.ttl(KEY) <= 5
        cand = cap.DispatchCandidate.from_service_info(
            ServiceInfo.from_dict(data)
        )
        assert cand.dispatchable is False

    def test_get_failure_still_attempts_delete(self, redis):
        registry = ServiceRegistry(redis)
        registry.register(ServiceInfo("w-state", "flow_worker", "h"))
        proxy = _RedisProxy(redis, get_error=RuntimeError("get failed"))
        registry.redis_client = proxy

        assert registry.unregister("flow_worker", "w-state") is False
        assert proxy.deleted, "get 失败后仍必须尝试 delete（旧实现直接跳出）"
        assert redis.get(KEY) is None, "delete 成功则键消失"


class TestImmediateFlush:
    """A45-03：draining/stopping 返回时键已落盘，不等心跳。"""

    def test_constructed_starting_is_not_dispatchable(self, redis):
        worker = _full_worker(redis)
        data = _read(redis, _worker_key(worker))
        assert data["status"] == "starting"
        cand = cap.DispatchCandidate.from_service_info(ServiceInfo.from_dict(data))
        assert cand.dispatchable is False
        worker.unregister_service()

    def test_request_drain_flushes_immediately(self, redis, monkeypatch):
        monkeypatch.setenv("PLAITA_WORKER_DRAIN_TIMEOUT", "0")
        worker = _full_worker(redis)
        key = _worker_key(worker)
        assert _read(redis, key)["status"] == "starting"
        worker.request_drain("test")
        data = _read(redis, key)
        assert data["status"] == "draining"
        assert cap.DispatchCandidate.from_service_info(ServiceInfo.from_dict(data)).dispatchable is False
        worker.unregister_service()

    def test_startup_drain_then_register_preserves_draining(self, redis, monkeypatch):
        """S9：SIGTERM 早于 run()，register 不得覆盖回 running。"""
        monkeypatch.setenv("PLAITA_WORKER_DRAIN_TIMEOUT", "0")
        worker = _full_worker(redis)
        key = _worker_key(worker)
        worker.request_drain("startup")
        assert worker.register_service() is True
        data = _read(redis, key)
        assert data["status"] == "draining"
        assert cap.DispatchCandidate.from_service_info(ServiceInfo.from_dict(data)).dispatchable is False
        worker.unregister_service()

    def test_stop_flushes_stopping_before_unregister(self, redis, monkeypatch):
        monkeypatch.setenv("PLAITA_WORKER_DRAIN_TIMEOUT", "0")
        worker = _full_worker(redis)
        key = _worker_key(worker)
        seen = {}

        original_unregister = worker._registry.unregister

        def spy(service_type, instance_id):
            seen["status_before_delete"] = _read(redis, key)["status"]
            return original_unregister(service_type, instance_id)

        worker._registry.unregister = spy
        worker._running = True
        worker.stop()
        assert seen["status_before_delete"] == "stopping"
        assert redis.get(key) is None


class TestClosedGeneration:
    """A46-03：注销后任何迟到写入都不得复活注册键。"""

    def test_bump_after_unregister_does_not_resurrect(self, redis):
        worker = _full_worker(redis)
        key = _worker_key(worker)
        assert worker.register_service() is True
        assert redis.get(key) is not None
        assert worker.unregister_service() is True
        assert redis.get(key) is None

        worker._bump_active(+1)
        worker._bump_active(-1)
        worker.update_registry_info(status="running", active_tasks=1)
        worker._refresh_registry_capacity()

        assert redis.get(key) is None, "注销后 ±1/更新/容量都不得复活注册键"

    def test_register_service_after_unregister_is_refused(self, redis):
        worker = _full_worker(redis)
        key = _worker_key(worker)
        worker.unregister_service()
        assert worker.register_service() is False
        assert redis.get(key) is None

    def test_capacity_is_frozen_after_drain(self, redis, monkeypatch):
        monkeypatch.setenv("PLAITA_WORKER_DRAIN_TIMEOUT", "0")
        worker = _skeleton(redis, status="running")
        assert worker.register_service() is True
        worker._bump_active(+1)
        drained_at = redis.get(KEY)

        worker.request_drain("test")
        after_drain = redis.get(KEY)
        worker._bump_active(+1)
        assert redis.get(KEY) == after_drain, "draining 后容量/active 不得再刷新"
        assert _read(redis)["status"] == "draining"
        assert drained_at is not None

    def test_flush_failure_is_counted_and_logged(self, redis, monkeypatch, caplog):
        """落盘重试仍失败：不得静默——计数器 +1，ERROR 日志。"""
        monkeypatch.setenv("PLAITA_WORKER_DRAIN_TIMEOUT", "0")
        worker = _skeleton(redis, status="running")
        monkeypatch.setattr(worker._registry, "heartbeat", lambda info: False)

        worker.request_drain("test")

        assert getattr(worker, "_registry_status_flush_failures", 0) == 1
        assert any("立即落盘失败" in rec.getMessage() for rec in caplog.records)
