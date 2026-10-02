"""G1 resume-retry 验收（keeper 迁移设计稿 §G1）。

验收口径：error 态 execution 经 resume(retry) 后从断点步进、失败节点恰好
重跑一次、已完成节点不重放。附带：execution_id 预铸种子（P0 可见性修复的
引擎侧一半）、终态短路放行矩阵（worker 层）。
"""
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.errors import ResumeError, ResumeType
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


def _flaky_flow() -> Flow:
    """start -> a1 -> flaky -> end；flaky 是断点重跑的靶节点。"""
    return Flow.model_validate({
        "runtime": "python",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "a1"},
            {"type": "assignment", "id": "a1", "output": {"step": 1}, "next": "flaky"},
            {"type": "assignment", "id": "flaky", "output": {"ok": True}, "next": "end"},
            {"type": "end", "id": "end", "output": {"done": True}},
        ],
    })


def _drive_until(execution, flow, context, **kw):
    return execution.run_distributed(flow, saved_context=context, **kw)


class TestRetrySemantics:
    def test_coerce_retry(self):
        assert ResumeType.coerce("retry") is ResumeType.RETRY

    def test_retry_steps_from_breakpoint_exactly_once(self):
        """验收主用例：失败节点恰重跑一次，已完成节点不重放。"""
        fl = _flaky_flow()
        execution = FlowExecution()
        execution.mode = "distributed"

        calls = []
        real_run_node = execution._runner.run_node

        async def spy(flow, node, **kwargs):
            calls.append(node.id)
            if node.id == "flaky" and calls.count("flaky") == 1:
                raise RuntimeError("boom")
            return await real_run_node(flow, node, **kwargs)

        execution._runner.run_node = spy

        # 步1：start+a1 成功；步2：flaky 首炸（checkpoint 里无 flaky 条目）
        result = execution.run_distributed(fl, {})
        assert calls == ["start", "a1"]
        checkpoint = result["context"]
        assert sorted(checkpoint["$NODE"].keys()) == ["a1", "start"]

        with pytest.raises(Exception):
            _drive_until(execution, fl, checkpoint, resume_type="continue")
        assert calls == ["start", "a1", "flaky"]

        # G1：error 态经 retry 放行 → 从 a1 的后继（= flaky）步进
        result = _drive_until(execution, fl, checkpoint, resume_type="retry")
        assert calls == ["start", "a1", "flaky", "flaky"]  # 恰重跑一次，a1 未重放
        assert result["context"]["$NODE"]["flaky"] == {"ok": True}

        while not result.get("is_end"):
            result = _drive_until(execution, fl, result["context"],
                                  resume_type="continue")
        assert calls[-1] == "end"
        assert result["is_end"] is True

    def test_retry_without_saved_context_rejected(self):
        # run_distributed 把策略层 ResumeError 归一化为 FlowErrorException
        # （-500 对外契约），message 保留原始语义
        from plaita.core.errors import FlowErrorException

        fl = _flaky_flow()
        execution = FlowExecution()
        execution.mode = "distributed"
        with pytest.raises(FlowErrorException, match="requires a saved_context"):
            execution.run_distributed(fl, {}, resume_type="retry")

    def test_retry_at_pending_event_node_rejected(self):
        """挂起中的 EventNode 不允许 retry 绕过（与 continue 同一守卫）。"""
        fl = Flow.model_validate({
            "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "wait"},
                {"type": "event", "id": "wait", "eventType": "approval",
                 "next": "end"},
                {"type": "end", "id": "end", "output": {"done": True}},
            ],
        })
        execution = FlowExecution()
        execution.mode = "distributed"
        result = execution.run_distributed(fl, {})
        # start 执行后到达 wait：start 不是挂起节点，第一步推进到 wait 挂起
        context = result.get("context")
        if not result.get("is_suspend"):
            result = execution.run_distributed(fl, saved_context=context,
                                               resume_type="continue")
            context = result.get("context", context)
        assert result.get("is_suspend") is True
        from plaita.core.errors import FlowErrorException
        with pytest.raises(FlowErrorException, match="pending"):
            execution.run_distributed(fl, saved_context=context,
                                      resume_type="retry")


class TestSeededExecutionId:
    """P0 可见性（§5.5 清单①）引擎侧一半：worker 先落行再执行，id 预铸喂入。"""

    def test_seed_honored(self):
        fl = _flaky_flow()
        execution = FlowExecution()
        execution.mode = "distributed"
        result = execution.run_distributed(fl, {}, execution_id="seed-123")
        assert result["execution_id"] == "seed-123"

    def test_unseeded_still_mints(self):
        fl = _flaky_flow()
        execution = FlowExecution()
        execution.mode = "distributed"
        result = execution.run_distributed(fl, {})
        assert result["execution_id"]  # 非空
        assert len(result["execution_id"]) == 32  # uuid4().hex


class TestWorkerHonorsPremintedId:
    """worker.start_flow 认 BFF 预铸 id：行先于执行落盘（含失败路径）。"""

    def _worker(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(WORKER_TEST_FLOW)
        return _make_worker(fake, storage, flow_storage), storage

    def test_preminted_id_used_for_state_row(self):
        worker, storage = self._worker()
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.return_value = {
                "execution_id": "preminted-1", "is_end": True, "is_suspend": False,
                "result": "ok", "context": {"$LAST_NODE": "end", "$NODE": {}},
            }
            worker.start_flow("f1", {}, execution_id="preminted-1")
        assert storage.load_execution_state("preminted-1").status == "completed"
        # 种子透传给引擎
        assert inst.run_distributed.call_args.kwargs.get("execution_id") == "preminted-1"

    def test_running_row_survives_start_failure(self):
        """执行即炸：running 行已在（zombie 可见、可 cancel），而非无行可查。"""
        worker, storage = self._worker()
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = RuntimeError("agent boom")
            with pytest.raises(RuntimeError):
                worker.start_flow("f1", {}, execution_id="preminted-2")
        state = storage.load_execution_state("preminted-2")
        assert state is not None
        assert state.status == "running"


# ---------- worker 层：终态短路放行矩阵 ----------

WORKER_TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}


def _make_worker(fake_redis, storage=None, flow_storage=None):
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:g1-retry",
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=flow_storage if flow_storage is not None else MemoryFlowStorage(),
        redis_client=fake_redis,
        enable_registry=False,
        enable_redis_logging=False,
        lease_ttl_seconds=60,
    )


def _save_state(storage, status, execution_id="exec-1"):
    state = ExecutionState(
        execution_id=execution_id, flow_id="f1", flow_version="1",
        status=status,
        context={"$LAST_NODE": "start", "$NODE": {"start": {}}},
    )
    if status == "error":
        state.error = {"message": "boom"}
        state.end_time = "2026-10-02T00:00:00"
    storage.save_execution_state(execution_id, state)
    return state


class TestTerminalShortCircuitMatrix:
    def _setup(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(WORKER_TEST_FLOW)
        return fake, storage, _make_worker(fake, storage, flow_storage)

    def test_error_plus_continue_still_terminal(self):
        _, storage, worker = self._setup()
        _save_state(storage, "error")
        result = worker.resume_flow("f1", "exec-1", "continue")
        assert result["already_terminal"] is True
        assert result["status"] == "error"

    def test_completed_plus_retry_still_terminal(self):
        """retry 只解锁 error；completed 幂等语义不变。"""
        _, storage, worker = self._setup()
        _save_state(storage, "completed")
        result = worker.resume_flow("f1", "exec-1", "retry")
        assert result["already_terminal"] is True

    def test_cancelled_plus_retry_still_terminal(self):
        _, storage, worker = self._setup()
        _save_state(storage, "cancelled")
        result = worker.resume_flow("f1", "exec-1", "retry")
        assert result["already_terminal"] is True

    def test_error_plus_retry_flips_to_running_then_completes(self):
        """验收主用例（worker 层）：放行 → 先翻 running 落盘 → 断点步进到终态。"""
        fake, storage, worker = self._setup()
        _save_state(storage, "error")

        seen_status_at_run = {}

        def run_step(flow, **kwargs):
            state = storage.load_execution_state("exec-1")
            seen_status_at_run["status"] = state.status
            seen_status_at_run["error"] = state.error
            seen_status_at_run["resume_type"] = kwargs.get("resume_type")
            seen_status_at_run["saved_context"] = kwargs.get("saved_context")
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

        # 引擎看到的已是 running（放行即翻转落盘），error 已清
        assert seen_status_at_run["status"] == "running"
        assert seen_status_at_run["error"] is None
        assert seen_status_at_run["resume_type"] == "retry"
        assert seen_status_at_run["saved_context"]["$LAST_NODE"] == "start"
        # 跑到终态
        assert result.get("is_end") is True
        state = storage.load_execution_state("exec-1")
        assert state.status == "completed"

    def test_error_plus_retry_survives_repeat_after_second_crash(self):
        """retry 后再崩：error → 再 retry 仍放行（at-least-once 语义闭环）。"""
        fake, storage, worker = self._setup()
        _save_state(storage, "error")

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = RuntimeError("crash again")
            with pytest.raises(RuntimeError):
                worker.resume_flow("f1", "exec-1", "retry")

        state = storage.load_execution_state("exec-1")
        assert state.status == "error"  # 错误处理器归位 error，可再 retry

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = None
            inst.run_distributed.return_value = {
                "execution_id": "exec-1", "is_end": True, "is_suspend": False,
                "result": "ok", "context": {"$LAST_NODE": "end", "$NODE": {}},
            }
            result = worker.resume_flow("f1", "exec-1", "retry")
        assert storage.load_execution_state("exec-1").status == "completed"
