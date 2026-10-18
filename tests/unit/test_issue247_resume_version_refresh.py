"""plaita#247：#241 租约内重读未刷新 flow_version / flow / flow_hash。

缺陷：``resume_flow`` 在租约前用预读 state 算出 ``version``/``flow``/
``current_flow_hash``；#241 的租约内复核只把 ``state`` 换成新快照。若
load→acquire 窗口内另一载体把同一 execution 的 ``flow_version`` 从 1 改到 2，
复核看到新 state，却仍用 **V1 的 flow** 执行，并把结果写回 V2 的状态行——
执行定义与状态身份不一致；``current_flow_hash`` 同样可能误判/误放行。

修复契约（三不变量）：

1. **定义身份**：执行的 flow 定义必须来自盘上 ``flow_version``（V2），
   不得复用 acquire 前的 V1；
2. **状态身份**：落盘 ``flow_version`` / checkpoint / result 必须一致地
   来自 V2（``result == "V2"``、``$NODE.end == "V2"``）；
3. **指纹身份**：hash 校验用新版本的指纹——V2 状态携带 V2 hash 时放行；
   携带旧 V1 hash 时走既有 mismatch 路径**明确拒绝**，绝不带着旧指纹
   执行 V1。

负控：删除复核后的 version/flow/hash 重算，第一条用例红
（``result V1, expected V2 (or explicit reject)``），第二条 red 为
``FlowHashMismatchError`` 误报，第三条 red 为「未拒绝且执行了 V1」。
"""
from __future__ import annotations

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.execution_lease import RedisExecutionLease
from plaita.server.flow_worker import FlowHashMismatchError, FlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

EXEC = "exec-247"

FLOW_V1 = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "V1"},
    ],
}
FLOW_V2 = {
    "flow_id": "f1",
    "version": "2",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "V2"},
    ],
}


def _setup() -> tuple[MemoryExecutionStorage, FlowWorker]:
    fake = fakeredis.FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(FLOW_V1)
    flow_storage.save_flow(FLOW_V2)
    worker = FlowWorker(
        storage,
        flow_storage,
        execution_lease=RedisExecutionLease(fake),
        lease_ttl_seconds=60,
    )
    return storage, worker


def _seed(storage: MemoryExecutionStorage, *, version="1", flow_hash=None) -> None:
    storage.save_execution_state(
        EXEC,
        ExecutionState(
            execution_id=EXEC,
            flow_id="f1",
            flow_version=version,
            flow_hash=flow_hash,
            status="running",
            context={"$LAST_NODE": "start", "$NODE": {}},
        ),
    )


def _switch_on_acquire(
    worker: FlowWorker,
    storage: MemoryExecutionStorage,
    *,
    version="2",
    flow_hash=None,
) -> None:
    """模拟 load→acquire 窗口内另一载体改写版本/指纹。"""
    original = worker._acquire_lease

    def acquire_during_race(execution_id: str, holder: str):
        state = storage.load_execution_state(execution_id)
        state.flow_version = version
        state.flow_hash = flow_hash
        storage.save_execution_state(execution_id, state)
        return original(execution_id, holder)

    worker._acquire_lease = acquire_during_race  # type: ignore[method-assign]


def test_version_switch_executes_new_definition():
    """不变量 1+2：窗口内 1→2 ⇒ 执行 V2，状态/checkpoint/result 均为 V2。"""
    storage, worker = _setup()
    _seed(storage)
    _switch_on_acquire(worker, storage)

    result = worker.resume_flow("f1", EXEC, "continue")

    assert result["result"] == "V2", (
        f"result {result.get('result')}, expected V2 (or explicit reject)"
    )
    saved = storage.load_execution_state(EXEC)
    assert saved.status == "completed"
    assert saved.flow_version == "2"
    assert saved.context["$LAST_NODE"] == "end"
    assert saved.context["$NODE"]["end"] == "V2"


def test_version_switch_with_new_hash_passes():
    """不变量 3（放行面）：V2 状态携带 V2 指纹 ⇒ 用 V2 指纹校验并执行 V2。

    若 current_flow_hash 仍是 V1 的，这里会被既有 mismatch 路径误杀
    （stored H2 vs current H1），用例红。
    """
    storage, worker = _setup()
    h1 = worker.get_flow_definition_hash("f1", "1")
    h2 = worker.get_flow_definition_hash("f1", "2")
    assert h1 != h2
    _seed(storage, version="1", flow_hash=h1)
    _switch_on_acquire(worker, storage, version="2", flow_hash=h2)

    result = worker.resume_flow("f1", EXEC, "continue")

    assert result["result"] == "V2"
    saved = storage.load_execution_state(EXEC)
    assert saved.status == "completed"
    assert saved.flow_version == "2"
    assert saved.flow_hash == h2


def test_stale_hash_on_new_version_is_rejected():
    """不变量 3（拒绝面）：V2 状态仍带 V1 指纹 ⇒ 按新版本指纹判定 mismatch，
    明确拒绝且不执行旧 V1。

    若 current_flow_hash 未刷新（stored H1 == current H1），旧实现会放行并
    执行 V1——本用例红。
    """
    storage, worker = _setup()
    h1 = worker.get_flow_definition_hash("f1", "1")
    _seed(storage, version="1", flow_hash=h1)
    _switch_on_acquire(worker, storage, version="2", flow_hash=h1)

    with pytest.raises(FlowHashMismatchError):
        worker.resume_flow("f1", EXEC, "continue")

    saved = storage.load_execution_state(EXEC)
    assert saved.status == "error"
    assert saved.error is not None
    assert saved.error.get("error_type") == "flow_hash_mismatch"
    assert saved.flow_version == "2"
    # 明确拒绝发生在任何节点执行之前：checkpoint 仍是 start。
    assert saved.context["$LAST_NODE"] == "start"
    assert saved.context["$NODE"] == {}


def test_no_version_change_still_uses_original_definition():
    """回归：窗口内版本不变时仍用 V1，且不触发 247 的重算分支。"""
    storage, worker = _setup()
    _seed(storage)

    result = worker.resume_flow("f1", EXEC, "continue")

    assert result["result"] == "V1"
    saved = storage.load_execution_state(EXEC)
    assert saved.status == "completed"
    assert saved.flow_version == "1"
    assert saved.context["$NODE"]["end"] == "V1"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
