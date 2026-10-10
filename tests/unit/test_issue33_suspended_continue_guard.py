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
- 兜底（绕过入口短路的路径，如构造/竞态窗口）：策略层守卫 ResumeGuardError
  经 _chain_has_resume_guard_error 判出 → 不终态化，抛 ResumeProtocolError，
  run() 对其 ack（重投只会重复命中同一守卫，烧 delivery 无意义）；
  裸 ResumeError（节点 resume() 抛错）**不**豁免，仍终态化 error。

验收口径（工单原文）：构造 suspended 执行，投 continue 消息，断言状态仍
suspended 且后续 event 唤醒可成功。
"""
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.errors import FlowErrorException, ResumeError, ResumeGuardError
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.server.flow_worker import (
    NodeFailureTerminalizedError,
    RedisFlowWorker,
    ResumeProtocolError,
)
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
    """兜底：策略层**守卫**（``ResumeGuardError``）不终态化（绕过入口短路的路径）。"""

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
            # run_distributed 归一化形态：FlowErrorException(__cause__=守卫)
            wrapped = FlowErrorException("pending guard hit")
            wrapped.__cause__ = ResumeGuardError(
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

    def test_guard_error_is_a_resume_error_subclass(self):
        """守卫用**子类**区分（不是新异常族）：既有 ``except ResumeError`` 不受影响。"""
        assert issubclass(ResumeGuardError, ResumeError)


class TestNodeResumeFailureStillTerminalizes:
    """豁免面必须窄：**节点 ``resume()`` 抛错**不是守卫，不得静默保持原状。

    2026-10-10 评审：``_chain_has_resume_protocol_error``（现
    ``_chain_has_resume_guard_error``）原判据认**任何**
    ``ResumeError``，于是 ``strategies._handle_resume`` 把 ``current_node.resume()``
    的异常包成的那个（事件数据畸形 / 节点恢复逻辑炸）也被吞成「挂起 + ack、
    无 error 记录、永无决议」的哑执行。现在只有 ``ResumeGuardError`` 豁免，
    其余 ResumeError 走现状终态化 error（可观测、可人工 retry）。
    """

    def _run_with_engine_error(self, cause: BaseException):
        worker, storage = _redis_worker()
        _, _, checkpoint = _real_suspended_execution()
        storage.save_execution_state("exec-1", ExecutionState(
            execution_id="exec-1", flow_id="f1", status="running",
            context=checkpoint,
        ))
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            # strategies._handle_resume 的真实形态：
            # ResumeError(str(e), node=...) 带 __cause__ 上抛，被 run_distributed
            # 归一化成 FlowErrorException(__cause__=ResumeError(__cause__=e))
            inner = cause
            resume_err = ResumeError(f"{inner}")
            resume_err.__cause__ = inner
            wrapped = FlowErrorException(f"恢复执行出错: {inner}")
            wrapped.__cause__ = resume_err
            inst.run_distributed.side_effect = wrapped

            with pytest.raises(NodeFailureTerminalizedError):
                worker.resume_flow("f1", "exec-1", "event", data={"approved": True})
        return storage.load_execution_state("exec-1")

    def test_malformed_event_data_terminalizes_with_error_recorded(self):
        state = self._run_with_engine_error(ValueError("bad event payload"))

        assert state.status == "error", "节点 resume 失败必须终态化（不得静默保持原状）"
        assert state.error and "bad event payload" in state.error["message"]
        assert state.end_time, "终态必须带 end_time"


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

        queue = RedisStreamTaskQueue(
            worker.redis_client, "test:issue33-suspended", consumer_name="w1")
        queue.ensure_group()
        enqueue_task(worker.redis_client, "test:issue33-suspended", {
            "type": "resume", "flow_id": "f1", "execution_id": eid,
            "resume_type": "continue", "tenant_id": "default",
        })
        task = queue.read(block_ms=100)
        assert task is not None
        try:
            worker._dispatch_task(task.body, delivery_count=task.delivery_count)
            queue.ack(task.message_id)
            raise AssertionError("守卫错误必须抛出 ResumeProtocolError")
        except ResumeProtocolError:
            queue.ack(task.message_id)
            queue.note_resume_protocol()

        stats = queue.stats()
        assert stats["resume_protocol_acked"] == 1
        # 不并进 poison 口径：消息良构，只是与执行状态对不上
        assert stats["poison_acked"] == 0
        # 状态原样，等 event/cancel/timeout 决议
        assert storage.load_execution_state(eid).status == "running"


class TestStepLoopGuardErrorKeepsSuspended:
    """#33 补漏（2026-10-10 评审）：**步循环**里的守卫同样不得终态化。

    用户可达触发面：``POST /executions/{id}/resume {"resume_type":"event"}``
    不带 ``data``（``data`` 可选；console 恢复对话框默认提交 ``{}`` 解析出的
    ``{}``）——EventNode 收到空载荷 = 没事件、节点仍 pending，紧接着步循环用
    ``resume_type="continue"`` 推进即命中策略层守卫。此前该路径在
    ``_process_execution_result`` 里把执行写成 error 终态：终态短路随即拒绝
    event/cancel/timeout，一次「没带数据的 event」就永久封死本可恢复的挂起
    执行（消息还被 ack，连重投都没有）。修复后：磁盘行保持 suspended、
    消息 ack（run() 的 ResumeProtocolError 分支），真决议仍可唤醒跑完。
    """

    def _consume_one_round(self, worker, body):
        """真消费循环跑一轮：返回 (queue, 是否已 ack)。"""
        queue = worker._get_task_queue()
        queue.ensure_group()
        enqueue_task(worker.redis_client, worker.queue_name, body)
        worker._running = True
        consumer = threading.Thread(
            target=worker._consume_loop, args=(queue,), daemon=True,
        )
        consumer.start()
        try:
            deadline = time.time() + 20
            while time.time() < deadline and queue.stats()["pending"] != 0:
                time.sleep(0.05)
            acked = queue.stats()["pending"] == 0
        finally:
            worker._running = False
            consumer.join(timeout=10)
        return queue, acked

    def _suspended_execution(self, worker, storage) -> str:
        _, _, checkpoint = _real_suspended_execution()
        eid = checkpoint.get("$EXECUTION_ID") or "exec-1"
        storage.save_execution_state(eid, ExecutionState(
            execution_id=eid, flow_id="f1", flow_version="1", status="suspended",
            context=checkpoint,
        ))
        return eid

    def test_empty_event_payload_keeps_suspended_and_acks(self):
        worker, storage = _redis_worker()
        eid = self._suspended_execution(worker, storage)

        queue, acked = self._consume_one_round(worker, {
            "type": "resume", "flow_id": "f1", "execution_id": eid,
            "resume_type": "event", "tenant_id": "default",
        })

        assert acked, "协议错误消息必须 ack（重投只会重复命中同一守卫）"
        state = storage.load_execution_state(eid)
        assert state.status == "suspended", "挂起执行不得被终态化成 error"
        assert state.error is None
        assert state.end_time is None
        assert queue.stats()["resume_protocol_acked"] == 1
        assert queue.stats()["poison_acked"] == 0

    def test_real_event_can_still_resolve_after_empty_payload(self):
        """验收口径：空载荷 event 之后，真正的 event 决议仍能跑完。"""
        worker, storage = _redis_worker()
        eid = self._suspended_execution(worker, storage)

        self._consume_one_round(worker, {
            "type": "resume", "flow_id": "f1", "execution_id": eid,
            "resume_type": "event", "tenant_id": "default",
        })

        result = worker.resume_flow("f1", eid, "event", data={"approved": True})

        assert result.get("is_end") is True
        assert storage.load_execution_state(eid).status == "completed"

    def test_bare_resume_error_in_step_loop_still_terminalizes(self):
        """豁免面必须**窄**：步循环里的裸 ``ResumeError``（节点 ``resume()``
        抛错，如事件数据畸形）仍终态化 error——否则真失败会静默成一个
        「无 error 记录、永远等不到决议」的哑执行。"""
        worker, storage = _redis_worker()
        eid = self._suspended_execution(worker, storage)

        inner = ValueError("bad event payload")
        resume_err = ResumeError(str(inner))
        resume_err.__cause__ = inner
        wrapped = FlowErrorException(f"恢复执行出错: {inner}")
        wrapped.__cause__ = resume_err

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            # 第一次（resume_flow 里的 event 恢复）不推进，第二次（步循环的
            # continue 推进）才抛节点 resume 失败——即步循环分支。
            inst.run_distributed.side_effect = [
                {"execution_id": eid, "context": {}, "is_end": False,
                 "is_suspend": False},
                wrapped,
            ]
            worker.resume_flow("f1", eid, "event", data={"approved": True})
            assert inst.run_distributed.call_count == 2, (
                "第二次调用（步循环的 continue 推进）才抛——本用例打的就是该分支"
            )

        state = storage.load_execution_state(eid)
        assert state.status == "error", "节点 resume() 自身失败必须终态化"
        assert "bad event payload" in state.error["message"]
