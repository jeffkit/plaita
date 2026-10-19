"""第五十六轮 A2R25-04/05/07：恢复语义越权回归（负控基线）。

三条恢复不变式：

1. A2R25-04——删除执行后只恢复状态键，``resume`` 必须仍被墓碑拦下；
   ``run_distributed`` 零调用（恢复不撤销「删除即吊销」）。
2. A2R25-05——只恢复旧 ``state+fence``（fencegen 留新值）时，陈旧世代的
   save/delete 必须抛 ``ExecutionLeaseError``；CAS 不能只信 fence 键。
3. A2R25-07——只恢复状态键（``index:ready`` 仍在）时，读取/列表必须把
   索引成员补回来，执行不得从列表消失。

pre-fix 预期：三条全部红（resume 跑完、stale save accepted、列表为空）。
"""

from __future__ import annotations

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.execution_lease import ExecutionLeaseError, RedisExecutionLease
from plaita.server.flow_worker import FlowWorker, RedisFlowWorker
from plaita.server.tenant_context import (
    reset_current_tenant,
    set_current_tenant,
)
from plaita.storage.base import ExecutionState
from plaita.storage.fenced import (
    FencedExecutionStorage,
    reset_current_fence_token,
    set_current_fence_token,
)
from plaita.storage.memory import MemoryFlowStorage
from plaita.storage.redis import (
    RedisExecutionStorage,
    execution_fence_generation_key,
    execution_fence_key,
    execution_index_key,
    execution_lease_key,
    execution_tombstone_key,
)

EXEC = "exec-r56"
NS = "plaita"
FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}


def _state(
    status: str = "running",
    last_node: str = "start",
    execution_id: str = EXEC,
) -> ExecutionState:
    return ExecutionState(
        execution_id=execution_id,
        flow_id="f1",
        flow_version="1",
        status=status,
        context={"$LAST_NODE": last_node, "$NODE": {}},
    )


def _fake():
    return fakeredis.FakeStrictRedis(decode_responses=True)


def _save(fenced: FencedExecutionStorage, generation: int, status: str = "running") -> bool:
    token = set_current_fence_token(generation)
    try:
        return fenced.save_execution_state(EXEC, _state(status))
    finally:
        reset_current_fence_token(token)


def _delete(fenced: FencedExecutionStorage, generation: int) -> bool:
    token = set_current_fence_token(generation)
    try:
        return fenced.delete_execution_state(EXEC)
    finally:
        reset_current_fence_token(token)


class TestA2R25ResumeTombstone:
    """A2R25-04：恢复状态键不撤销墓碑，resume 不得推进。"""

    def _worker(self, fake):
        storage = RedisExecutionStorage(client=fake, namespace=NS)
        flows = MemoryFlowStorage()
        flows.save_flow(FLOW)
        worker = FlowWorker(
            storage,
            flows,
            execution_lease=RedisExecutionLease(fake),
            lease_ttl_seconds=60,
        )
        return worker, storage

    def _deleted_then_restored(self, fake, storage):
        storage.save_execution_state(EXEC, _state())
        backup = fake.get(f"{NS}:execution:{EXEC}")
        assert storage.delete_execution_state(EXEC) is True
        assert fake.get(execution_tombstone_key(EXEC)) is not None
        # 备份工具只恢复状态键：墓碑仍在。
        fake.set(f"{NS}:execution:{EXEC}", backup)

    def test_resume_refuses_restored_state_behind_tombstone(self):
        fake = _fake()
        worker, storage = self._worker(fake)
        self._deleted_then_restored(fake, storage)

        token = set_current_tenant("default", source="explicit")
        try:
            out = worker.resume_flow("f1", EXEC, "continue")
        finally:
            reset_current_tenant(token)

        assert out.get("status") == "deleted", out
        assert out.get("discarded") is True, out
        # 状态保持恢复时的 running，绝不被继续推进成 completed。
        assert storage.load_execution_state(EXEC).status == "running"

    def test_dispatch_drops_resume_carrier_for_tombstoned_execution(self, monkeypatch):
        fake = _fake()
        storage = RedisExecutionStorage(client=fake, namespace=NS)
        flows = MemoryFlowStorage()
        flows.save_flow(FLOW)
        # 派发闸在 RedisFlowWorker（_dispatch_task）；基类没有队列消费入口。
        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/15",
            queue_name="test:r56-resume-tombstone",
            execution_storage=storage,
            flow_storage=flows,
            redis_client=fake,
            enable_registry=False,
            enable_redis_logging=False,
        )
        self._deleted_then_restored(fake, storage)

        called = []
        monkeypatch.setattr(
            worker, "resume_flow", lambda *a, **kw: called.append((a, kw))
        )
        worker._dispatch_task(
            {
                "type": "resume",
                "flow_id": "f1",
                "execution_id": EXEC,
                "resume_type": "continue",
                "tenant_id": "default",
            }
        )
        assert called == [], "已删执行的 resume 载体必须在派发闸被丢弃"


class TestA2R25FenceGenerationConsistency:
    """A2R25-05：fence 键与 fencegen 不一致（部分恢复）不得授予写/删权。"""

    def _env(self):
        fake = _fake()
        fenced = FencedExecutionStorage(RedisExecutionStorage(client=fake, namespace=NS))
        lease = RedisExecutionLease(fake)
        return fake, fenced, lease

    def test_stale_generation_save_and_delete_refused_after_partial_restore(self):
        fake, fenced, lease = self._env()

        gen1 = lease.try_acquire_fenced(EXEC, "worker-1", 60)
        assert gen1 == 1
        assert _save(fenced, gen1) is True
        # 控制面删除（无世代）：清 fence/lease，保留 fencegen、写墓碑。
        assert fenced.delete_execution_state(EXEC) is True

        gen2 = lease.try_acquire_fenced(EXEC, "worker-2", 60)
        assert gen2 == 2
        assert _save(fenced, gen2) is True
        snap_state = fake.get(f"{NS}:execution:{EXEC}")
        snap_fence = fake.get(execution_fence_key(NS, EXEC))
        assert snap_fence == str(gen2)

        # 租约过期/重启后 C 接管到 gen3，再只恢复 B 的 state+fence。
        fake.delete(execution_lease_key(NS, EXEC))
        gen3 = lease.try_acquire_fenced(EXEC, "worker-3", 60)
        assert gen3 == 3
        assert _save(fenced, gen3) is True
        fake.delete(execution_lease_key(NS, EXEC))
        fake.set(f"{NS}:execution:{EXEC}", snap_state)
        fake.set(execution_fence_key(NS, EXEC), snap_fence, ex=7 * 86400)
        assert fake.get(execution_fence_generation_key(NS, EXEC)) == "3"

        # 陈旧 gen2 的 save：fence 键命中但 fencegen=3 → 拒绝，且状态不被写。
        with pytest.raises(ExecutionLeaseError):
            _save(fenced, gen2, status="completed")
        assert fake.get(f"{NS}:execution:{EXEC}") == snap_state

        # 陈旧 gen2 的 delete 同样不得删掉恢复回来的状态。
        with pytest.raises(ExecutionLeaseError):
            _delete(fenced, gen2)
        assert fake.get(f"{NS}:execution:{EXEC}") == snap_state


class TestA2R25RestoredStateVisibility:
    """A2R25-07：只恢复状态键后，读取路径补回索引成员，列表可见。"""

    def _env(self):
        fake = _fake()
        storage = RedisExecutionStorage(client=fake, namespace=NS)
        return fake, storage

    def test_restored_state_key_is_visible_after_load(self):
        fake, storage = self._env()
        storage.save_execution_state(EXEC, _state())
        storage.list_executions()  # 置 index:ready；此后一次性回填不再跑
        backup = fake.get(f"{NS}:execution:{EXEC}")
        assert storage.delete_execution_state(EXEC) is True
        fake.set(f"{NS}:execution:{EXEC}", backup)

        loaded = storage.load_execution_state(EXEC)
        assert loaded is not None and loaded.status == "running"
        assert fake.zscore(execution_index_key(NS), EXEC) is not None
        assert [s.execution_id for s in storage.list_executions()] == [EXEC]

    def test_rebuild_execution_index_recovers_all_states(self):
        fake, storage = self._env()
        storage.save_execution_state("e1", _state(execution_id="e1"))
        storage.save_execution_state("e2", _state(execution_id="e2"))
        storage.list_executions()
        fake.delete(execution_index_key(NS))  # 模拟索引丢失/只恢复状态键

        assert storage.rebuild_execution_index() == 2
        assert {s.execution_id for s in storage.list_executions()} == {"e1", "e2"}
