"""第 47 轮存储面两格回归：A2R13（fence 世代 ABA）+ A51-03（计数键残留）。

A2R13：``delete_execution_state`` 删除 fence 键后，同 execution_id 重新
acquire 时旧实现的 ``INCR`` 从 0 回到 1——与旧 worker 世代相同。旧 worker
的迟到 save 因此命中 CAS（状态复活/覆写），旧世代 delete 也能删掉新执行。
修复把世代计数器（``fencegen``）与 fence 键分离：删除不清计数器、计数器
无 TTL，INCR 永续；fence 键仍带 7 天 TTL 只承载当前世代。

A51-03：``nofail`` / ``g1wakeups`` / ``noderefetch`` / ``noderetry``（含
节点维度子键）/ ``affinityhandover`` 按 execution_id 存 7 天。删除执行时
不清理，同 id 新执行会继承旧预算（确定性失败或 G1 唤醒已达上限时，新执行
第一次失败就被判不可救）。修复把五类键放进同一删除 pipeline / Lua。

本文件同时是负控基线：只回退生产文件时，下列用例应逐条精确红。
"""
from __future__ import annotations

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.execution_lease import ExecutionLeaseError, RedisExecutionLease
from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.fenced import (
    FencedExecutionStorage,
    reset_current_fence_token,
    set_current_fence_token,
)
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.storage.redis import RedisExecutionStorage

EXEC_ID = "exec-r47"
NS = "plaita"

# 五类计数键在测试侧独立声明（不 import 生产常量）：负控回退生产文件时
# 测试仍可收集，红必须来自行为而不是 ImportError。
COUNTER_KINDS = (
    "nofail",
    "g1wakeups",
    "noderefetch",
    "noderetry",
    "affinityhandover",
)


def _fake():
    return fakeredis.FakeStrictRedis(decode_responses=True)


def _storage(fake) -> FencedExecutionStorage:
    return FencedExecutionStorage(
        RedisExecutionStorage(client=fake, namespace=NS)
    )


def _state(execution_id: str = EXEC_ID, status: str = "running") -> ExecutionState:
    return ExecutionState(
        execution_id=execution_id,
        flow_id="f1",
        flow_version="1",
        context={"$LAST_NODE": "start", "$NODE": {}},
        status=status,
    )


def _save_with_gen(storage, gen: int, execution_id: str = EXEC_ID,
                   status: str = "running") -> bool:
    token = set_current_fence_token(gen)
    try:
        return storage.save_execution_state(execution_id, _state(execution_id, status))
    finally:
        reset_current_fence_token(token)


def _counter_key(kind: str, execution_id: str = EXEC_ID) -> str:
    return f"{NS}:execution:{kind}:{execution_id}"


def _node_retry_key(node_id: str, execution_id: str = EXEC_ID) -> str:
    return f"{NS}:execution:noderetry:{execution_id}:{node_id}"


def _seed_counters(fake, execution_id: str = EXEC_ID) -> None:
    values = {
        "nofail": 12,          # DETERMINISTIC_FAILURE_MAX
        "g1wakeups": 2,        # G1_MAX_WAKEUPS
        "noderefetch": 100,    # NODE_RETRY_TOTAL_MAX
        "noderetry": 5,        # 执行维度退化键
        "affinityhandover": 3,  # AFFINITY_HANDOVER_MAX
    }
    for kind, value in values.items():
        fake.set(_counter_key(kind, execution_id), value)
    fake.set(_node_retry_key("node-a", execution_id), 5)
    fake.set(_node_retry_key("node-b", execution_id), 4)


def _all_counter_keys(execution_id: str = EXEC_ID) -> list[str]:
    keys = [_counter_key(kind, execution_id) for kind in COUNTER_KINDS]
    keys.extend(
        [
            _node_retry_key("node-a", execution_id),
            _node_retry_key("node-b", execution_id),
        ]
    )
    return keys


def _worker(fake) -> RedisFlowWorker:
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:r47-counter-residue",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


class TestFenceGenerationAfterDelete:
    """A2R13：删除后新 acquire 的世代必须严格大于旧世代。"""

    def test_force_delete_does_not_reset_generation(self):
        fake = _fake()
        storage = _storage(fake)
        lease = RedisExecutionLease(fake)

        gen1 = lease.try_acquire_fenced(EXEC_ID, "h1", 60)
        assert gen1 == 1
        assert _save_with_gen(storage, gen1) is True

        # 控制面 force delete（无世代）：删除即吊销
        assert storage.delete_execution_state(EXEC_ID) is True

        # 同 id 新 worker acquire：pre-fix 会回到 1（== 旧世代）
        gen2 = lease.try_acquire_fenced(EXEC_ID, "h2", 60)
        assert gen2 > gen1
        # 世代计数器是持久单调锚：删除不清、无 TTL
        assert fake.get(f"{NS}:execution:fencegen:{EXEC_ID}") == str(gen2)
        assert fake.ttl(f"{NS}:execution:fencegen:{EXEC_ID}") == -1

    def test_stale_worker_save_refused_after_reacquire(self):
        fake = _fake()
        storage = _storage(fake)
        lease = RedisExecutionLease(fake)

        gen1 = lease.try_acquire_fenced(EXEC_ID, "h1", 60)
        assert _save_with_gen(storage, gen1, status="running") is True
        assert storage.delete_execution_state(EXEC_ID) is True

        gen2 = lease.try_acquire_fenced(EXEC_ID, "h2", 60)
        assert _save_with_gen(storage, gen2, status="running") is True

        # 旧 worker 的迟到 save 必须被具名拒绝，不能把新执行改回 error
        # （pre-fix：gen2 == gen1，这里不抛异常 → DID NOT RAISE）
        with pytest.raises(ExecutionLeaseError):
            _save_with_gen(storage, gen1, status="error")
        loaded = storage.load_execution_state(EXEC_ID)
        assert loaded is not None and loaded.status == "running"

    def test_stale_worker_delete_refused_after_reacquire(self):
        fake = _fake()
        storage = _storage(fake)
        lease = RedisExecutionLease(fake)

        gen1 = lease.try_acquire_fenced(EXEC_ID, "h1", 60)
        assert storage.delete_execution_state(EXEC_ID) is True
        gen2 = lease.try_acquire_fenced(EXEC_ID, "h2", 60)
        assert _save_with_gen(storage, gen2) is True

        # 旧世代的 delete 不得删掉新执行（pre-fix：gen2 == gen1，
        # 删除成功 → DID NOT RAISE，新执行被旧世代清掉）
        token = set_current_fence_token(gen1)
        try:
            with pytest.raises(ExecutionLeaseError):
                storage.delete_execution_state(EXEC_ID)
        finally:
            reset_current_fence_token(token)
        assert fake.get(f"{NS}:execution:{EXEC_ID}") is not None
        assert fake.get(f"{NS}:execution:fence:{EXEC_ID}") == str(gen2)

    def test_legacy_fence_value_seeds_generation_counter(self):
        """升级兼容：存量 fence 键有值、计数器缺失时世代不得回退。"""
        fake = _fake()
        fake.set(f"{NS}:execution:fence:{EXEC_ID}", "7", ex=120)
        lease = RedisExecutionLease(fake)

        gen = lease.try_acquire_fenced(EXEC_ID, "h1", 60)
        assert gen == 8
        assert fake.get(f"{NS}:execution:fencegen:{EXEC_ID}") == "8"


class TestDeletePurgesExecutionCounters:
    """A51-03：删除执行必须清掉五类计数键，同 id 新执行预算从零。"""

    def test_delete_purges_all_counter_families(self):
        fake = _fake()
        storage = _storage(fake)
        fake.set(f"{NS}:execution:{EXEC_ID}", "{}")
        fake.zadd(f"{NS}:execution:index", {EXEC_ID: 1.0})
        _seed_counters(fake)
        _seed_counters(fake, "exec-keep")

        assert storage.delete_execution_state(EXEC_ID) is True

        for key in _all_counter_keys():
            assert fake.exists(key) == 0, f"计数键残留: {key}"
        # 另一执行（含节点子键）的预算不受影响
        for key in _all_counter_keys("exec-keep"):
            assert fake.exists(key) == 1, f"误删他执行计数键: {key}"

    def test_new_execution_budget_starts_at_zero(self):
        fake = _fake()
        storage = _storage(fake)
        worker = _worker(fake)
        fake.set(f"{NS}:execution:{EXEC_ID}", "{}")
        _seed_counters(fake)

        # 删除前：全部处于「耗尽」状态
        assert worker._read_deterministic_failure_count(EXEC_ID) == 12
        assert worker._read_g1_wakeup_count(EXEC_ID) == 2
        assert worker._read_node_retry_counter(EXEC_ID) == 5
        assert worker._read_node_retry_counter(EXEC_ID, "node-a") == 5
        assert worker._node_retry_total_exhausted(EXEC_ID) is True

        assert storage.delete_execution_state(EXEC_ID) is True

        # 删除后：同 id 新执行从零预算开始（否则 cap=1 时全拒）
        assert worker._read_deterministic_failure_count(EXEC_ID) == 0
        assert worker._read_g1_wakeup_count(EXEC_ID) == 0
        assert worker._read_node_retry_counter(EXEC_ID) == 0
        assert worker._read_node_retry_counter(EXEC_ID, "node-a") == 0
        assert worker._node_retry_total_exhausted(EXEC_ID) is False
        assert fake.get(_counter_key("affinityhandover")) is None

    def test_fenced_delete_with_generation_purges_counters(self):
        """持世代的删除（A43-07 Lua 路径）同样清计数键。"""
        fake = _fake()
        storage = _storage(fake)
        lease = RedisExecutionLease(fake)
        gen = lease.try_acquire_fenced(EXEC_ID, "h1", 60)
        _seed_counters(fake)

        token = set_current_fence_token(gen)
        try:
            assert storage.delete_execution_state(EXEC_ID) is True
        finally:
            reset_current_fence_token(token)

        for key in _all_counter_keys():
            assert fake.exists(key) == 0, f"计数键残留: {key}"
        # 世代计数器不在删除范围（A2R13 的单调锚）
        assert fake.get(f"{NS}:execution:fencegen:{EXEC_ID}") == str(gen)
