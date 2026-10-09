"""#33 回归：挂起执行被 continue/retry 重复投递终态化成不可恢复 error。

基线缺陷（okguitar 提报 P1 场景 gap）：suspended 执行收到 resume_type=continue
（挂起双写窗口 crash 后重投的原消息 / start 重派竞速 / 运维误发），策略层
pending 守卫抛 ResumeError → resume_flow 通用 except 终态化 error → 消息
重投命中 already_terminal 短路被 ack → 连 resume_type=event 都进不去，
checkpoint 里的合法挂起状态永远无法决议（retry 也救不回：retry 再触发
守卫 → 再 error → 死循环）。

修复契约：
- resume_flow 入口：suspended + continue/retry → 幂等短路返回
  （already_suspended=True），不取租约、不推进、状态原样；
- 兜底（绕过入口短路的路径，如构造/竞态窗口）：策略层守卫的 ResumeError
  经 _chain_has_resume_protocol_error 判出 → 不终态化，抛 ResumeProtocolError，
  run() 对其 ack（重投只会重复命中同一守卫，烧 delivery 无意义）。

验收口径（工单原文）：构造 suspended 执行，投 continue 消息，断言状态仍
suspended 且后续 event 唤醒可成功。
"""
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.errors import FlowErrorException, ResumeError
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.server.flow_worker import RedisFlowWorker, ResumeProtocolError
from plaita.server.task_queue import RedisStreamTaskQueue, enqueue_task
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

EVENT_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "runtime": "python",
    "inputType": {"dataType": "object"},
    "nodes": [
        {"type": "start", "id": "start", "next": "wait"},
        {"type": "event", "id": "wait", "event_type": "approval", "next": "end"},
        {"type": "end", "id": "end", "output": {"done": True}},
    ],
}


def _redis_worker(fake=None, storage=None) -> tuple:
    fake = fake or fakeredis.FakeRedis(decode_responses=True)
    storage = storage or MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(EVENT_FLOW)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue33-suspended",
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    return worker, storage


def _real_suspended_execution():
    """真实引擎跑到 EventNode 挂起，返回 (execution, flow, checkpoint)。"""
    fl = Flow.model_validate(EVENT_FLOW)
    execution = FlowExecution()
    execution.mode = "distributed"
    result = execution.run_distributed(fl, {})
    if not result.get("is_suspend"):
        result = execution.run_distributed(
            fl, saved_context=result["context"], resume_type="continue")
    assert result.get("is_suspend") is True
    return execution, fl, result["context"]


class TestSuspendedIdempotentShortCircuit:
    """入口短路：suspended + continue/retry 幂等跳过，状态原样。"""

    def _suspended_state_saved(self, storage) -> str:
        """真实引擎挂起 checkpoint 落盘，返回落盘的 execution_id。

        resume 后 `_process_execution_result` 以 result.execution_id（=
        checkpoint 的 $EXECUTION_ID）定位落盘行，测试必须存同一个 id。"""
        _, _, checkpoint = _real_suspended_execution()
        eid = checkpoint.get("$EXECUTION_ID") or "exec-1"
        state = ExecutionState(
            execution_id=eid,
            flow_id="f1",
            flow_version="1",
            status="suspended",
            context=checkpoint,
        )
        storage.save_execution_state(eid, state)
        return eid

    @pytest.mark.parametrize("resume_type", ["continue", "retry"])
    def test_continue_and_retry_short_circuit_keeping_suspended(self, resume_type):
        worker, storage = _redis_worker()
        eid = self._suspended_state_saved(storage)

        result = worker.resume_flow("f1", eid, resume_type)

        assert result["already_suspended"] is True
        assert result["status"] == "suspended"
        state = storage.load_execution_state(eid)
        assert state.status == "suspended"
        assert state.error is None
        assert state.end_time is None

    def test_short_circuit_does_not_advance_or_lease(self):
        """短路发生在租约/引擎之前：不推进、不造 FlowExecution。"""
        worker, storage = _redis_worker()
        eid = self._suspended_state_saved(storage)

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            result = worker.resume_flow("f1", eid, "continue")

        FE.assert_not_called()
        assert result["already_suspended"] is True
        # 租约键未写（短路在 _acquire_lease 之前）
        assert worker.redis_client.get(f"plaita:execution:lease:{eid}") is None

    def test_repeat_delivery_loop_stays_suspended(self):
        """at-least-once 重复投递 N 次：状态钉死 suspended，不进死循环。"""
        worker, storage = _redis_worker()
        eid = self._suspended_state_saved(storage)

        for _ in range(5):
            result = worker.resume_flow("f1", eid, "continue")
            assert result["already_suspended"] is True

        assert storage.load_execution_state(eid).status == "suspended"

    def test_event_wakeup_after_continue_deliveries_completes(self):
        """验收主用例：continue 重复投递后，event 唤醒仍可成功跑完。"""
        worker, storage = _redis_worker()
        eid = self._suspended_state_saved(storage)

        # 随意投几次 continue（模拟重复投递）
        for _ in range(3):
            worker.resume_flow("f1", eid, "continue")

        result = worker.resume_flow(
            "f1", eid, "event", data={"approved": True})

        assert result.get("is_end") is True
        state = storage.load_execution_state(eid)
        assert state.status == "completed"

    def test_cancel_resume_still_resolves_suspended(self):
        """cancel 决议路径不受短路影响（只挡 continue/retry）。"""
        worker, storage = _redis_worker()
        eid = self._suspended_state_saved(storage)

        result = worker.resume_flow("f1", eid, "cancel", data={})

        assert result.get("is_end") is True
        state = storage.load_execution_state(eid)
        assert state.status == "completed"


class TestResumeProtocolErrorExemption:
    """兜底：守卫类 ResumeError 不终态化（绕过入口短路的路径）。"""

    def test_guard_resume_error_keeps_state_and_raises_protocol_error(self):
        worker, storage = _redis_worker()
        _, _, checkpoint = _real_suspended_execution()
        # 构造 running 态执行但 checkpoint 停在挂起节点（模拟双写窗口竞态：
        # 状态已翻 running 而节点再次命中 pending 守卫）
        storage.save_execution_state("exec-1", ExecutionState(
            execution_id="exec-1", flow_id="f1", status="running",
            context=checkpoint,
        ))

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            # run_distributed 归一化形态：FlowErrorException(__cause__=ResumeError)
            wrapped = FlowErrorException("pending guard hit")
            wrapped.__cause__ = ResumeError(
                "Execution is suspended at EventNode 'wait' (status=pending)")
            inst.run_distributed.side_effect = wrapped

            with pytest.raises(ResumeProtocolError):
                worker.resume_flow("f1", "exec-1", "continue")

        state = storage.load_execution_state("exec-1")
        # 不终态化：保持 running（原状），无 error 落盘
        assert state.status == "running"
        assert state.error is None
        assert state.end_time is None

    def test_engine_level_guard_error_keeps_suspended(self):
        """端到端：真实引擎守卫（无 mock）→ suspended 保持 + 协议错误上抛。"""
        worker, storage = _redis_worker()
        _, _, checkpoint = _real_suspended_execution()
        storage.save_execution_state("exec-1", ExecutionState(
            execution_id="exec-1", flow_id="f1", status="running",
            context=checkpoint,
        ))

        with pytest.raises(ResumeProtocolError):
            worker.resume_flow("f1", "exec-1", "continue")

        state = storage.load_execution_state("exec-1")
        assert state.status == "running"
        assert state.error is None

    def test_resume_protocol_error_is_not_value_error(self):
        """刻意非 ValueError 子类：防 run() 的 poison ack 分支误吞语义。"""
        assert not issubclass(ResumeProtocolError, ValueError)
        assert issubclass(ResumeProtocolError, RuntimeError)


class TestRunLoopAcksProtocolError:
    """run() 主循环：ResumeProtocolError → ack（重投只会重复命中守卫）。"""

    def _consume(self, worker, body):
        queue = RedisStreamTaskQueue(
            worker.redis_client, "test:issue33-suspended", consumer_name="w1")
        queue.ensure_group()
        enqueue_task(worker.redis_client, "test:issue33-suspended", body)
        task = queue.read(block_ms=100)
        assert task is not None
        try:
            worker._dispatch_task(task.body, delivery_count=task.delivery_count)
            queue.ack(task.message_id)
            return "acked"
        except ResumeProtocolError:
            # run() 现路径（except ResumeProtocolError 分支）：ack + note_poison
            queue.ack(task.message_id)
            queue.note_poison()
            return "acked-poison"
        except Exception as e:  # noqa: BLE001
            return f"raised:{type(e).__name__}"

    def test_suspended_continue_via_dispatch_short_circuits_to_ack(self):
        """入口短路路径：suspended+continue 经 _dispatch_task 正常返回 → ack，
        状态原样（不抛任何异常、不重投）。"""
        worker, storage = _redis_worker()
        _, _, checkpoint = _real_suspended_execution()
        eid = checkpoint.get("$EXECUTION_ID")
        storage.save_execution_state(eid, ExecutionState(
            execution_id=eid, flow_id="f1", flow_version="1", status="suspended",
            context=checkpoint,
        ))

        outcome = self._consume(worker, {
            "type": "resume", "flow_id": "f1", "execution_id": eid,
            "resume_type": "continue", "tenant_id": "default",
        })
        assert outcome == "acked"
        assert storage.load_execution_state(eid).status == "suspended"

    def test_protocol_error_message_gets_acked(self):
        """兜底路径（绕过入口短路，状态 running + 挂起 checkpoint）：
        ResumeProtocolError → run() ack，不终态化、不重投。"""
        worker, storage = _redis_worker()
        _, _, checkpoint = _real_suspended_execution()
        eid = checkpoint.get("$EXECUTION_ID")
        storage.save_execution_state(eid, ExecutionState(
            execution_id=eid, flow_id="f1", flow_version="1", status="running",
            context=checkpoint,
        ))

        outcome = self._consume(worker, {
            "type": "resume", "flow_id": "f1", "execution_id": eid,
            "resume_type": "continue", "tenant_id": "default",
        })
        assert outcome == "acked-poison"
        # 状态原样，等 event/cancel/timeout 决议
        assert storage.load_execution_state(eid).status == "running"
