"""#304：定义缺失的 start/resume 载体——具名终态，不再 poison-ack 吞执行。

issue 机制（A2R49-01）：版本被删后引擎定义键消失，``resume_flow``（钉死
版本、租约/try 之前）与 ``start_flow``（首行）经 ``get_flow_definition``
抛**裸 ValueError** → run() 按畸形消息 poison ack——执行行永驻
suspended/queued、无 error 字段、无终态，``_release_quota_slot_if_terminal``
只认终态写 → start 幽灵行还占住租户配额名额。

修法（两侧并用之 worker 半边）：``get_flow_definition`` 改抛
``FlowDefinitionMissingError``（刻意继承 ValueError，既有 poison-ack 语义
不变）；start/resume 入口捕获后先把执行终态化 error——
``error.error_type="flow_definition_missing"`` 机器可判身份 + 配额名额经
``_persist_state_or_raise`` 单一收口释放——再原样上抛。终态用 error 与
flow_hash mismatch 同口径：版本恢复后仍可 ``resume_type=retry`` 唤醒。

负控（修复前全红）：断言异常**类型名**为 FlowDefinitionMissingError（修复
前裸 ValueError，名不匹配）且状态行拿到 error 终态（修复前停在
suspended/queued）——两条独立红面。
"""
from __future__ import annotations

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.execution_lease import RedisExecutionLease
from plaita.server.flow_worker import FlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.storage.redis import TERMINAL_EXECUTION_STATUSES

EXEC = "exec-304"

FLOW_V1 = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "done"},
    ],
}


def _worker_without_definitions(
    storage: MemoryExecutionStorage,
) -> FlowWorker:
    """不带任何 flow 定义的 worker（模拟版本被删/未同步的引擎存储）。"""
    fake = fakeredis.FakeRedis(decode_responses=True)
    return FlowWorker(
        storage,
        MemoryFlowStorage(),
        execution_lease=RedisExecutionLease(fake),
        lease_ttl_seconds=60,
    )


def _seed(storage: MemoryExecutionStorage, status: str = "suspended") -> None:
    storage.save_execution_state(
        EXEC,
        ExecutionState(
            execution_id=EXEC,
            flow_id="f1",
            flow_version="1",
            status=status,
            tenant_id="",
            context={"$LAST_NODE": "start", "$NODE": {}},
        ),
    )


def test_resume_missing_definition_terminalizes_error():
    """resume（event 决议路径）撞定义缺失：终态化 error，不再永驻 suspended。

    resume_type 用 event：continue/retry 对挂起执行会被 #33 挂起幂等短路
    （在取定义之前返回），踩不到本缺陷；event/cancel/timeout 决议路径绕过
    短路直达取定义——审批到达时定义已被删正是 #304 的生产形态。
    """
    storage = MemoryExecutionStorage()
    _seed(storage, status="suspended")
    worker = _worker_without_definitions(storage)

    with pytest.raises(ValueError) as exc_info:
        worker.resume_flow("f1", EXEC, "event")
    # 具名异常（修复前：裸 ValueError，类型名不匹配 → 红）
    assert type(exc_info.value).__name__ == "FlowDefinitionMissingError", (
        f"expected FlowDefinitionMissingError, got {type(exc_info.value).__name__}"
    )

    saved = storage.load_execution_state(EXEC)
    assert saved.status == "error", (
        f"执行必须得到可观测终态，实际停在 {saved.status!r}（poison-ack 吞执行）"
    )
    assert saved.error is not None
    assert saved.error.get("error_type") == "flow_definition_missing"
    assert saved.error.get("flow_version") == "1"
    assert saved.end_time


def test_resume_missing_definition_quota_slot_released():
    """终态化经单一收口 → 配额名额释放被触发（start 幽灵行的名额半边）。"""
    storage = MemoryExecutionStorage()
    _seed(storage, status="suspended")
    worker = _worker_without_definitions(storage)

    released: list[tuple[str, str]] = []
    original = worker._release_quota_slot_if_terminal

    def spy(execution_id: str, state: ExecutionState) -> None:
        if getattr(state, "status", None) in TERMINAL_EXECUTION_STATUSES:
            released.append((execution_id, state.status))
        original(execution_id, state)

    worker._release_quota_slot_if_terminal = spy  # type: ignore[method-assign]

    with pytest.raises(ValueError):
        worker.resume_flow("f1", EXEC, "event")

    assert ("exec-304", "error") in released, "终态化必须触发配额名额释放收口"


def test_start_missing_definition_terminalizes_placeholder():
    """start 撞定义缺失（预铸 id 占位行）：queued → error 具名终态。"""
    storage = MemoryExecutionStorage()
    _seed(storage, status="queued")
    worker = _worker_without_definitions(storage)

    with pytest.raises(ValueError) as exc_info:
        worker.start_flow("f1", {}, version="1", execution_id=EXEC)
    assert type(exc_info.value).__name__ == "FlowDefinitionMissingError"

    saved = storage.load_execution_state(EXEC)
    assert saved.status == "error", (
        f"queued 占位行必须终态化，实际停在 {saved.status!r}（幽灵 queued 行）"
    )
    assert saved.error is not None
    assert saved.error.get("error_type") == "flow_definition_missing"
    assert saved.end_time


def test_start_missing_definition_without_row_no_ghost():
    """未预铸 id（无状态行）的定义缺失：poison ack 即终局，无行可孤儿化。"""
    storage = MemoryExecutionStorage()
    worker = _worker_without_definitions(storage)

    with pytest.raises(ValueError):
        worker.start_flow("f1", {}, version="1")
    assert storage.list_executions() == []


def test_terminalize_never_overwrites_terminal_state():
    """幂等：已终态的行（竞窗内他人终态化/前次已写）不被覆写。"""
    storage = MemoryExecutionStorage()
    _seed(storage, status="cancelled")
    worker = _worker_without_definitions(storage)

    state = storage.load_execution_state(EXEC)
    worker._terminalize_definition_missing(
        EXEC, "f1", "1", state, phase="test"
    )
    saved = storage.load_execution_state(EXEC)
    assert saved.status == "cancelled"
    assert saved.error is None
