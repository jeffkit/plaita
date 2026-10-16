"""#80 端到端守卫（okguitar 口径 ①）：不可序列化 ⇒ 终态化 error **且真的落盘成功**。

这是那个陷阱的**反向断言**：终态化后必然要落盘，而落盘的 state 若仍带着
不可序列化的 context，存储层会**再抛同一个** `StateSerializationError` ⇒
终态永远落不下去（症状与原缺陷同形）。本文件钉住「净化后终态真的落盘」。
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from plaita.core.flow import Flow
from plaita.server.flow_worker import FlowWorker
from plaita.storage.base import ExecutionState, StateSerializationError
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

TEST_FLOW = {
    "id": "f1", "name": "t", "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end"},
    ],
}


class SerializationRaisingStorage(MemoryExecutionStorage):
    """前 `raise_times` 次 save 抛 `StateSerializationError`（模拟 Redis 后端撞上非 JSON context）。

    之后的 save 正常写入底层 dict ⇒ `load_execution_state` 可读回，用于断言
    「终态真的落盘了」。
    """

    def __init__(self, raise_times: int = 1):
        super().__init__()
        self.raise_times = raise_times
        self.calls = 0

    def save_execution_state(self, execution_id, state) -> bool:
        self.calls += 1
        if self.calls <= self.raise_times:
            raise StateSerializationError(
                execution_id, TypeError("Object of type set is not JSON serializable")
            )
        return super().save_execution_state(execution_id, state)


def _worker(storage) -> FlowWorker:
    return FlowWorker(execution_storage=storage, flow_storage=MemoryFlowStorage())


def _state(context) -> ExecutionState:
    return ExecutionState(execution_id="exec-1", flow_id="f1", flow_version="1",
                          status="running", context=context)


BAD_CTX = {"$LAST_NODE": "start", "$NODE": {"n1": {"result": {1, 2, 3}}}}  # set 不可 JSON


def _drive(storage, ctx):
    worker = _worker(storage)
    steps = [
        {"execution_id": "exec-1", "is_end": False, "is_suspend": False, "context": ctx},
        {"execution_id": "exec-1", "is_end": False, "is_suspend": False, "context": ctx},
    ]
    execution = MagicMock()
    execution.run_distributed.side_effect = steps
    worker._process_execution_result(
        Flow.model_validate(TEST_FLOW), steps[0], _state(ctx), execution=execution
    )
    return storage


def test_unserializable_terminalizes_error_and_state_actually_persists():
    """★ 口径 ①：**不得抛**（永久失败不重投），且终态 error **真的落盘**。"""
    storage = _drive(SerializationRaisingStorage(raise_times=1), BAD_CTX)

    saved = storage.load_execution_state("exec-1")
    assert saved is not None, "终态没落盘 —— 说明落盘时又抛了同一个异常（陷阱）"
    assert saved.status == "error"
    assert saved.error.get("context_sanitized") is True
    assert "#80" in str(saved.error.get("rule", ""))


def test_persisted_context_is_json_safe_and_stringified():
    """落盘的 context 必须 JSON 安全，且那个 set 已降级为 str（读回可验证）。"""
    storage = _drive(SerializationRaisingStorage(raise_times=1), BAD_CTX)
    saved = storage.load_execution_state("exec-1")

    json.dumps(saved.context)                                    # 不抛 ⇒ 真安全
    assert isinstance(saved.context["$NODE"]["n1"]["result"], str)
    assert "1" in saved.context["$NODE"]["n1"]["result"] or "{" in saved.context["$NODE"]["n1"]["result"]


def test_original_context_object_is_not_mutated():
    """净化只作用于持久化副本：原 context 里那个 set **仍在**。"""
    ctx = {"$LAST_NODE": "start", "$NODE": {"n1": {"result": {1, 2, 3}}}}
    _drive(SerializationRaisingStorage(raise_times=1), ctx)
    assert isinstance(ctx["$NODE"]["n1"]["result"], set), "原 context 被就地改了！"


def test_layer2_falls_back_to_minimal_error_state_when_persist_keeps_failing():
    """层 2：净化后**仍**抛 ⇒ 降级为不含 context 的最小 error 状态再落一次。"""
    class RaisesTwice(MemoryExecutionStorage):
        """前 **2** 次抛（第 1 次=原始非 JSON；第 2 次=**已净化**的）⇒ 逼出层 2。

        只抛 1 次是打不到层 2 的：层 1 净化后第 2 次就成功了 —— 那正是层 1
        有效的证据（我第一次就把夹具写错在此，被这条测试当场逮住）。
        """

        def __init__(self):
            super().__init__()
            self.calls = 0
            self.saved_states = []

        def save_execution_state(self, execution_id, state) -> bool:
            self.calls += 1
            if self.calls <= 2:
                raise StateSerializationError(execution_id, TypeError("boom"))
            self.saved_states.append(state.model_copy(deep=True))
            return super().save_execution_state(execution_id, state)

    storage = RaisesTwice()
    _drive(storage, BAD_CTX)

    assert storage.calls >= 2, "层 2 没被触发"
    last = storage.saved_states[-1]
    assert last.status == "error"
    assert last.context == {}, "层 2 应落一个**不含 context** 的最小状态"
