"""plaita#240：error + resume_type=retry 的空 checkpoint 必须保持 error 原状。

修复前：``resume_flow`` 的 G1 唤醒分支在验证 retry 能力（盘上 checkpoint）
**之前**就把 error 翻成 running 并落盘；策略层随后抛 ``ResumeGuardError``，
协议错误分支虽「保持原状」，但原状已经是 running——非法 ``error → running``
迁移，且 ``error`` / ``end_time`` 被清空（失败证据丢失）。

本文件按三个不变量分别钉住（空 checkpoint 的 ``{}`` / ``None`` 两种形状）：

1. ``status`` 保持 error（不得出现 running）；
2. 原 ``error`` payload 原样保留；
3. 原 ``end_time`` 原样保留。

并回归：非空 checkpoint + retry 仍推进、error + continue 仍终态短路、
running + retry 语义不变（#53）。

负控：删除/短路新增预检 ⇒ 三个不变量用例必红（status 变 running、
error/end_time 变 None）。
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.executor import FlowExecution
from plaita.server.flow_worker import RedisFlowWorker, ResumeProtocolError
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

WORKER_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

_ORIGINAL_ERROR = {
    "message": "boom",
    "code": -500,
    "error_type": "node_failure",
    "node_id": "a",
}
_ORIGINAL_END_TIME = "2026-10-02T00:00:00"


class _NoneContextStorage(MemoryExecutionStorage):
    """模拟「状态被截断为 context=None」的存量行（绕过加载期 pydantic 校验）。

    MemoryExecutionStorage 的 load 会 model_validate 存量 dict；context=None
    不是合法 dict，会先抛 ValidationError。真实 Redis 反序列化路径可能把
    None/缺省带进来，本 shim 只用于覆盖该形状的 worker 守卫。
    """

    def load_execution_state(self, execution_id):
        data = self.execution_states.get(execution_id)
        if data is None:
            return None
        payload = dict(data)
        payload["context"] = {}
        state = ExecutionState.model_validate(payload)
        state.context = None
        return state


def _make_worker(storage, flow_storage, fake_redis):
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:i240-retry",
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake_redis,
        enable_registry=False,
        enable_redis_logging=False,
        lease_ttl_seconds=60,
    )


def _setup(context):
    fake = fakeredis.FakeRedis(decode_responses=True)
    storage = _NoneContextStorage() if context is None else MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(WORKER_FLOW)
    state = ExecutionState(
        execution_id="exec-1",
        flow_id="f1",
        flow_version="1",
        status="error",
        context={},
    )
    # 模拟「状态被截断」的存量行（模型允许 None；写入端历史上不保证）。
    state.context = context
    state.error = dict(_ORIGINAL_ERROR)
    state.end_time = _ORIGINAL_END_TIME
    storage.save_execution_state("exec-1", state)
    return _make_worker(storage, flow_storage, fake), storage


@pytest.mark.parametrize("empty", [{}, None], ids=["empty-dict", "none"])
def test_retry_with_empty_checkpoint_keeps_status_error(empty):
    worker, storage = _setup(empty)
    # wraps=真实类：预检命中时不构造；变异（预检失效）时真实引擎随即抛
    # ResumeGuardError，让「status 变 running」成为可断言的精确红。
    with patch("plaita.server.flow_worker.FlowExecution", wraps=FlowExecution) as FE:
        with pytest.raises(ResumeProtocolError) as ei:
            worker.resume_flow("f1", "exec-1", "retry")
    # 先断言状态（变异时这里给出 `assert 'running' == 'error'` 的精确红）；
    # 再断言预检在创建执行体之前命中（无节点执行）。
    assert "status=error" in str(ei.value)
    assert storage.load_execution_state("exec-1").status == "error", (
        "空 checkpoint 的 retry 不得把 error 翻成 running"
    )
    FE.assert_not_called()


@pytest.mark.parametrize("empty", [{}, None], ids=["empty-dict", "none"])
def test_retry_with_empty_checkpoint_preserves_error_payload(empty):
    worker, storage = _setup(empty)
    with pytest.raises(ResumeProtocolError):
        worker.resume_flow("f1", "exec-1", "retry")
    assert storage.load_execution_state("exec-1").error == _ORIGINAL_ERROR


@pytest.mark.parametrize("empty", [{}, None], ids=["empty-dict", "none"])
def test_retry_with_empty_checkpoint_preserves_end_time(empty):
    worker, storage = _setup(empty)
    with pytest.raises(ResumeProtocolError):
        worker.resume_flow("f1", "exec-1", "retry")
    assert storage.load_execution_state("exec-1").end_time == _ORIGINAL_END_TIME


# -- 回归：合法路径不受影响 ------------------------------------------------


def _valid_context():
    return {"$LAST_NODE": "start", "$NODE": {"start": {}}}


def test_retry_with_checkpoint_still_advances():
    worker, storage = _setup(_valid_context())
    seen = {}

    def run_step(flow, **kwargs):
        state = storage.load_execution_state("exec-1")
        seen["status"] = state.status
        seen["resume_type"] = kwargs.get("resume_type")
        seen["saved_context"] = kwargs.get("saved_context")
        return {
            "execution_id": "exec-1",
            "is_end": True,
            "is_suspend": False,
            "result": "ok",
            "context": {"$LAST_NODE": "end", "$NODE": {}},
        }

    with patch("plaita.server.flow_worker.FlowExecution") as FE:
        inst = MagicMock()
        FE.return_value = inst
        inst.run_distributed.side_effect = run_step
        result = worker.resume_flow("f1", "exec-1", "retry")

    assert result.get("is_end") is True
    assert seen["status"] == "running"  # 合法 retry 仍先翻 running 落盘
    assert seen["resume_type"] == "retry"
    assert seen["saved_context"] == _valid_context()
    assert storage.load_execution_state("exec-1").status == "completed"


def test_error_plus_continue_still_terminal():
    worker, storage = _setup({})
    result = worker.resume_flow("f1", "exec-1", "continue")
    assert result["already_terminal"] is True
    assert result["status"] == "error"
    assert storage.load_execution_state("exec-1").status == "error"


def test_running_plus_retry_semantics_unchanged():
    """#53 边界：running + retry 是断点续跑（不是 G1 唤醒），不受新预检影响。"""
    worker, storage = _setup({})
    state = storage.load_execution_state("exec-1")
    state.status = "running"
    state.error = None
    state.end_time = None
    state.context = _valid_context()
    storage.save_execution_state("exec-1", state)

    with patch("plaita.server.flow_worker.FlowExecution") as FE:
        inst = MagicMock()
        FE.return_value = inst
        inst.run_distributed.return_value = {
            "execution_id": "exec-1",
            "is_end": True,
            "is_suspend": False,
            "result": "ok",
            "context": {"$LAST_NODE": "end", "$NODE": {}},
        }
        result = worker.resume_flow("f1", "exec-1", "retry")

    assert result.get("is_end") is True
    inst.run_distributed.assert_called_once()
    assert storage.load_execution_state("exec-1").status == "completed"
