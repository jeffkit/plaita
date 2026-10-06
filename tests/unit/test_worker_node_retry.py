"""FlowWorker 节点级有界重试（2026-10 第二波 Track P1 任务①）。

基线缺陷：分布式路径上节点执行失败（如 LLM/HTTP 网络抖一次）→ runner 抛
``NodeExecutionError`` → ``run_distributed`` 归一化为 ``FlowErrorException``
（core/_error_normalization.py:59-69，原始异常在 ``__cause__``）→
``_process_execution_result`` 步进循环的 except 与 ``resume_flow`` 的 except
把执行终态化 error → 消息重投命中 already_terminal 短路被 ack。
**一次瞬态异常 = 废整个执行**。

修复契约：
- 可重试判据 = ``__cause__`` 链中出现 ``NodeExecutionError``（节点执行异常，
  runner.py:143 raise）；链中出现超时（``NodeTimeoutError``/``FlowTimeoutError``）
  或取消（``FlowCancelledException``）→ 不重试（确定性信号重试=再烧全款，
  对齐 v3 宿主 D4 决策）；协议/图错误（``ResumeError`` 等）→ 不重试。
- 重试载体 = at-least-once 消息重投本身：节点异常时**不终态化**（磁盘 state
  停在最后成功步的 checkpoint——strategies.py ``_execute_current_node`` 的
  ``runner.run_node`` 抛出时 context 未变）、不 ack，消息被回收（间隔
  = claim_min_idle_ms，默认 60s）后 resume 自然重跑失败节点。
- 预算 = Redis 重试计数键 ``{ns}:execution:noderetry:{id}``（INCR+EX 7d）。
  **不**用消息 delivery_count 判预算：核实发现 `_reclaim_one` 上报的是
  XCLAIM **前**的 times_delivered（task_queue.py:402，与其自身注释
  "count + 1" 相悖）、fresh 读取恒报 1，且达限消息在队列层就地死信、
  根本不会进 handler（task_queue.py:404）——按 delivery_count 判预算
  永远不会触发，只会造成「死信守卫重入队 → delivery 归 1 → 再耗尽」
  的无限循环。计数键在 worker 侧自增，预算耗尽时在处理函数内终态化
  error（现状行为），消息走 DLQ 且守卫见终态放行，循环终止。
- 预算耗尽还有一道后备：消息在队列层被就地死信时经过死信守卫，守卫读
  计数键达预算 → 终态化 error 后放行（覆盖「终态化写盘前 worker 崩溃」
  的窗口），不再重入队。
- 回滚开关 ``PLAITA_DISABLE_NODE_RETRY=1`` → 完全回到现状。
- ``run()`` 对新异常的处理仿照 ExecutionLeaseError：不 ack、不计 poison、
  留 pending 等回收。与死信守卫天然协同（非终态执行的消息不会被死信）。
"""
import json
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.errors import (
    FlowCancelledException,
    FlowErrorException,
    NodeExecutionError,
    NodeNotFoundError,
    NodeTimeoutError,
    ResumeError,
)
from plaita.server.flow_worker import (
    FlowWorker,
    NodeExecutionRetryableError,
    RedisFlowWorker,
)
from plaita.server.task_queue import StreamTask
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "a"},
        {"id": "a", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

CHECKPOINT_CONTEXT = {"$LAST_NODE": "start", "$NODE": {"start": {"status": "success"}}}


def _node_failure_exc(cause: Exception = None) -> FlowErrorException:
    """模拟分布式路径的真实异常形态：run_distributed 把节点执行异常归一化
    为 FlowErrorException，原始异常挂在 __cause__（_error_normalization.py:69）。
    """
    cause = cause or ConnectionError("llm provider blip")
    err = NodeExecutionError(f"执行节点a出错了: {type(cause).__name__}: {cause}", node="a")
    wrapped = FlowErrorException(str(err))
    wrapped.__cause__ = err
    err.__cause__ = cause
    return wrapped


def _wrapped(cause: Exception) -> FlowErrorException:
    """直接以给定异常为 __cause__ 构造 FlowErrorException（协议错误/超时用）。"""
    wrapped = FlowErrorException(str(cause))
    wrapped.__cause__ = cause
    return wrapped


def _base_worker(storage=None) -> FlowWorker:
    return FlowWorker(
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
    )


def _redis_worker(fake_redis, storage=None, flow_storage=None, **kwargs) -> RedisFlowWorker:
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:worker-node-retry",
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=flow_storage if flow_storage is not None else MemoryFlowStorage(),
        redis_client=fake_redis,
        enable_registry=False,
        enable_redis_logging=False,
        **kwargs,
    )


def _flow():
    from plaita.core.flow import Flow

    return Flow.model_validate(TEST_FLOW)


def _state(execution_id="exec-1", status="running", context=None) -> ExecutionState:
    return ExecutionState(
        execution_id=execution_id,
        flow_id="f1",
        flow_version="1",
        status=status,
        context=context if context is not None else CHECKPOINT_CONTEXT,
    )


def _resume_worker_with_running_exec(fake=None):
    """resume 链路夹具：running 执行已落盘，FlowExecution.run_distributed 可注入失败。"""
    fake = fake or fakeredis.FakeRedis(decode_responses=True)
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(TEST_FLOW)
    worker = _redis_worker(fake, storage, flow_storage)
    storage.save_execution_state("exec-1", _state())
    return worker, storage, fake


# ---------- 判别：可重试的节点执行失败 vs 协议/图错误/超时/取消 ----------


class TestRetryableClassification:
    def _resume_with_failure(self, exc):
        worker, storage, _ = _resume_worker_with_running_exec()
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = exc
            outcome = None
            try:
                worker.resume_flow("f1", "exec-1", "continue")
            except NodeExecutionRetryableError as e:
                outcome = e
            except Exception as e:  # noqa: BLE001
                outcome = e
        state = storage.load_execution_state("exec-1")
        return outcome, state

    def test_node_execution_failure_raises_retryable_and_keeps_state(self):
        """节点执行失败（__cause__=NodeExecutionError）→ 抛 NodeExecutionRetryableError，
        状态不终态化（磁盘保持 checkpoint 的 running + context）。"""
        outcome, state = self._resume_with_failure(_node_failure_exc())
        assert isinstance(outcome, NodeExecutionRetryableError)
        assert state.status == "running"
        assert state.error is None
        assert state.end_time is None
        assert state.context == CHECKPOINT_CONTEXT

    def test_protocol_error_resume_still_terminalizes(self):
        """协议错误（__cause__=ResumeError）维持现状：终态化 error。"""
        outcome, state = self._resume_with_failure(_wrapped(ResumeError("not pending")))
        assert not isinstance(outcome, NodeExecutionRetryableError)
        assert isinstance(outcome, RuntimeError)
        assert state.status == "error"

    def test_graph_error_still_terminalizes(self):
        """图结构错误（__cause__=NodeNotFoundError）维持现状：终态化 error。"""
        outcome, state = self._resume_with_failure(_wrapped(NodeNotFoundError(node_id="x")))
        assert not isinstance(outcome, NodeExecutionRetryableError)
        assert state.status == "error"

    def test_node_timeout_not_retried(self):
        """超时（__cause__=NodeTimeoutError）不重试——确定性信号重试=再烧全款（D4）。"""
        outcome, state = self._resume_with_failure(_wrapped(NodeTimeoutError("node a timeout")))
        assert not isinstance(outcome, NodeExecutionRetryableError)
        assert state.status == "error"

    def test_flow_timeout_not_retried(self):
        """流程级超时（__cause__=FlowTimeoutError）同样不重试。"""
        from plaita.core.errors import FlowTimeoutError

        outcome, state = self._resume_with_failure(_wrapped(FlowTimeoutError("flow timeout")))
        assert not isinstance(outcome, NodeExecutionRetryableError)
        assert state.status == "error"

    def test_cancelled_not_retried(self):
        """取消（__cause__=FlowCancelledException）是执行级意图，不重试。

        波次③起终态为 ``cancelled``（取消点前 checkpoint），而非 ``error``——
        取消是控制面意图不是失败；步内取消终态化语义见 ``resume_flow`` 的
        ``_chain_has_cancellation`` 分支。
        """
        outcome, state = self._resume_with_failure(_wrapped(FlowCancelledException()))
        assert not isinstance(outcome, NodeExecutionRetryableError)
        assert state.status == "cancelled"

    def test_plain_exception_without_cause_still_terminalizes(self):
        """无 __cause__ 链的裸异常（如引擎管线自身错误）维持现状终态化。"""
        outcome, state = self._resume_with_failure(ValueError("boom"))
        assert not isinstance(outcome, NodeExecutionRetryableError)
        assert state.status == "error"


# ---------- 预算：重试计数键 ----------


class TestRetryConvergenceEndToEnd:
    def test_fail_then_reclaim_then_retry_succeeds(self):
        """端到端收敛：节点第一次失败 → 消息留 pending → 队列回收重投 →
        resume 重跑失败节点成功 → 执行 completed、消息被 ack。

        用真实 RedisStreamTaskQueue（fakeredis + claim_min_idle_ms=1）验证
        重试载体（消息重投）与租约/checkpoint 协同真的闭环。
        """
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _redis_worker(fake, storage, flow_storage, claim_min_idle_ms=1)
        storage.save_execution_state("exec-1", _state())

        real_queue = worker._get_task_queue()
        from plaita.server.task_queue import enqueue_task

        enqueue_task(fake, "test:worker-node-retry", {
            "type": "resume", "flow_id": "f1", "execution_id": "exec-1",
            "resume_type": "continue", "tenant_id": "default",
        })

        outcomes = [
            _node_failure_exc(),  # 第一次投递：节点执行失败
            {"execution_id": "exec-1", "is_end": True, "context": {"done": 1}},
        ]

        def scripted_run_distributed(flow, saved_context=None, resume_type="continue", **kw):
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = scripted_run_distributed
            # 消费直到队列空（第一次失败不 ack → pending；第二次回收成功 → ack）
            for _ in range(10):
                task = real_queue.read(block_ms=10)
                if task is None:
                    break
                try:
                    worker._dispatch_task(task.body, delivery_count=task.delivery_count)
                    real_queue.ack(task.message_id)
                except NodeExecutionRetryableError:
                    pass  # run() 的语义：不 ack，留 pending 等回收
                except RuntimeError:
                    pass  # 预算耗尽的终态化路径（本用例不触达）
            # 需要等 idle >= claim_min_idle_ms 才能被回收
            import time as _time

            _time.sleep(0.05)

            for _ in range(10):
                task = real_queue.read(block_ms=10)
                if task is None:
                    break
                try:
                    worker._dispatch_task(task.body, delivery_count=task.delivery_count)
                    real_queue.ack(task.message_id)
                except NodeExecutionRetryableError:
                    pass
                except RuntimeError:
                    pass

        state = storage.load_execution_state("exec-1")
        assert state.status == "completed"
        assert fake.xpending("test:worker-node-retry", real_queue.group_name)["pending"] == 0


class TestRetryBudget:
    def _failure(self):
        return _node_failure_exc(ConnectionError("blip"))

    def test_retry_counter_key_written_with_ttl(self):
        """重试放行时写计数键 {ns}:execution:noderetry:{id}，带 TTL（7 天窗口）。"""
        worker, storage, fake = _resume_worker_with_running_exec()
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = self._failure()
            with pytest.raises(NodeExecutionRetryableError):
                worker.resume_flow("f1", "exec-1", "continue")
        assert fake.get("plaita:execution:noderetry:exec-1") == "1"
        assert 0 < fake.ttl("plaita:execution:noderetry:exec-1") <= 7 * 86400

    def test_budget_exhaustion_terminalizes_with_retry_note(self):
        """计数达预算 → 处理函数内终态化 error（现状行为），不再重试放行。

        这一条与死信守卫协同闭环：终态化后消息重投命中 already_terminal 被
        ack / 队列层死信时守卫见终态放行，不存在无限重试循环。
        """
        worker, storage, fake = _resume_worker_with_running_exec()
        fake.set("plaita:execution:noderetry:exec-1", "4")  # 已重试 4 次
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = self._failure()
            with pytest.raises(RuntimeError) as ei:
                worker.resume_flow("f1", "exec-1", "continue")
        assert not isinstance(ei.value, NodeExecutionRetryableError)
        state = storage.load_execution_state("exec-1")
        assert state.status == "error"
        assert "重试" in state.error["message"]
        assert state.error.get("node_retries") == 4

    def test_tenant_routed_counter_key(self):
        """计数键按租户 namespace 路由（与租约键同规则）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker, storage, _ = _resume_worker_with_running_exec(fake)
        storage.save_execution_state(
            "exec-t", _state(execution_id="exec-t")
        )
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = self._failure()

            from plaita.server.tenant_context import set_current_tenant

            token = set_current_tenant("acme")
            try:
                with pytest.raises(NodeExecutionRetryableError):
                    worker.resume_flow("f1", "exec-t", "continue")
            finally:
                from plaita.server.tenant_context import reset_current_tenant

                reset_current_tenant(token)
        assert fake.get("plaita:acme:execution:noderetry:exec-t") == "1"
        assert fake.get("plaita:execution:noderetry:exec-t") is None


# ---------- 回滚开关 ----------


class TestRollbackSwitch:
    def _failure(self):
        return _node_failure_exc(ConnectionError("blip"))

    def test_disable_env_restores_current_behavior(self, monkeypatch):
        """PLAITA_DISABLE_NODE_RETRY=1 → 完全回到现状（终态化 error）。"""
        monkeypatch.setenv("PLAITA_DISABLE_NODE_RETRY", "1")
        worker, storage, _ = _resume_worker_with_running_exec()
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = self._failure()
            with pytest.raises(RuntimeError) as ei:
                worker.resume_flow("f1", "exec-1", "continue")
        assert not isinstance(ei.value, NodeExecutionRetryableError)
        assert storage.load_execution_state("exec-1").status == "error"


# ---------- start 路径的步进失败同样重试 ----------


class TestStartPathRetry:
    def test_process_execution_result_step_failure_raises_retryable(self):
        """start 消息处理中的步进失败（_process_execution_result 内层 except）
        同样不终态化、抛 NodeExecutionRetryableError。"""
        worker = _base_worker()
        execution = MagicMock()

        def boom(*args, **kwargs):
            raise _node_failure_exc()

        execution.run_distributed.side_effect = boom
        result = {"execution_id": "exec-1", "is_end": False, "is_suspend": False,
                  "context": CHECKPOINT_CONTEXT}
        state = _state()
        worker.execution_storage.save_execution_state("exec-1", state)
        with pytest.raises(NodeExecutionRetryableError):
            worker._process_execution_result(
                _flow(), result, state, execution=execution
            )
        saved = worker.execution_storage.load_execution_state("exec-1")
        assert saved.status == "running"
        assert saved.error is None

    def test_start_flow_propagates_retryable_without_wrapping(self):
        """start_flow 不把 NodeExecutionRetryableError 包成 RuntimeError（run() 
        需按类型区分不 ack 语义），也不触发观测回调 finalize（执行还会继续）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _redis_worker(fake, storage, flow_storage)

        finalized = []
        handler = MagicMock()
        handler.finalize.side_effect = lambda: finalized.append(True)
        worker.callback_handlers.append(handler)

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.execution_id = "exec-new"
            inst.run_distributed.side_effect = [
                # 第一步：start 节点成功，返回非终态 lazy 输出
                {"execution_id": "exec-new", "is_end": False, "is_suspend": False,
                 "context": {"$LAST_NODE": "start", "$NODE": {}}},
                # 第二步：节点 a 失败
                _node_failure_exc(),
            ]
            with pytest.raises(NodeExecutionRetryableError):
                worker.start_flow("f1", {}, version="1", execution_id="exec-new")
        assert finalized == []
        # G1（43828aa）先行落行 + 波次二任务①：可重试失败不终态化，行停在
        # running（context 为先行落行的空 checkpoint）
        saved = storage.load_execution_state("exec-new")
        assert saved.status == "running"
        assert saved.error is None


# ---------- run() 主循环：不 ack、不 poison、留 pending ----------


class _StubQueue:
    """最小队列桩：弹出预置任务后置 worker 停机；记录 ack/dead_letter 调用。"""

    def __init__(self, worker, tasks):
        self._worker = worker
        self._tasks = list(tasks)
        self.acked = []
        self.dead_lettered = []
        self.poison = 0
        self.failed = 0
        self.lease_conflicts = 0
        self.max_deliveries = 5
        self.consumer_name = "stub"

    def read(self, block_ms=1000):
        if self._tasks:
            return self._tasks.pop(0)
        self._worker.stop()
        return None

    def ack(self, message_id):
        self.acked.append(message_id)

    def dead_letter(self, task, *, reason):
        self.dead_lettered.append((task.message_id, reason))

    def note_poison(self):
        self.poison += 1

    def note_failed(self):
        self.failed += 1

    def note_lease_conflict(self):
        self.lease_conflicts += 1

    def ensure_group(self):
        pass


class TestRunLoopRetrySemantics:
    def test_retryable_error_leaves_message_pending(self):
        """run() 对 NodeExecutionRetryableError：不 ack、不 poison、不死信，
        留 pending 等回收重投（仿 ExecutionLeaseError 语义）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        exc = NodeExecutionRetryableError("node a failed; will retry", execution_id="exec-1")
        task = StreamTask(message_id="m1", body={"type": "resume", "execution_id": "exec-1"},
                          delivery_count=2)
        stub = _StubQueue(worker, [task])
        with patch.object(worker, "_get_task_queue", return_value=stub):
            with patch.object(worker, "_dispatch_task", side_effect=exc):
                worker.run()
        assert stub.acked == []
        assert stub.dead_lettered == []
        assert stub.poison == 0
        assert stub.failed == 1

    def test_delivery_count_passed_to_dispatch(self):
        """task.delivery_count 透传进 _dispatch_task（可观测/降级预算用）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _redis_worker(fake)
        seen = []
        task = StreamTask(message_id="m1", body={"type": "resume", "execution_id": "exec-1"},
                          delivery_count=3)
        stub = _StubQueue(worker, [task])

        def fake_dispatch(body, delivery_count=None):
            seen.append(delivery_count)

        with patch.object(worker, "_get_task_queue", return_value=stub):
            with patch.object(worker, "_dispatch_task", side_effect=fake_dispatch):
                worker.run()
        assert seen == [3]


# ---------- 死信守卫后备：计数达预算 → 终态化后放行 ----------


class TestGuardBackstop:
    def _task(self, delivery_count=5):
        return StreamTask(
            message_id="m1",
            body={"type": "resume", "execution_id": "exec-1", "tenant_id": "default"},
            delivery_count=delivery_count,
        )

    def test_guard_finalizes_and_allows_when_retry_budget_exhausted(self):
        """消息在队列层被就地死信（delivery 达限不进 handler）时执行仍非终态、
        且重试计数已达预算 → 守卫终态化 error 后放行，**不**重入队（否则
        delivery 归 1 再耗尽再重入队 = 无限循环）。覆盖「预算耗尽判定后、
        终态化写盘前 worker 崩溃」的窗口。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _redis_worker(fake, storage, flow_storage)
        storage.save_execution_state("exec-1", _state(status="running"))
        fake.set("plaita:execution:noderetry:exec-1", "5")

        assert worker._dead_letter_guard(self._task()) is True
        assert fake.xlen("test:worker-node-retry") == 0  # 未重入队
        state = storage.load_execution_state("exec-1")
        assert state.status == "error"
        assert "重试" in state.error["message"]

    def test_guard_reenqueues_when_counter_below_budget(self):
        """计数未达预算的非终态执行（含崩溃抢救）维持现状：重入队恢复路径。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(TEST_FLOW)
        worker = _redis_worker(fake, storage, flow_storage)
        storage.save_execution_state("exec-1", _state(status="running"))
        fake.set("plaita:execution:noderetry:exec-1", "2")

        assert worker._dead_letter_guard(self._task()) is True
        assert fake.xlen("test:worker-node-retry") == 1
        assert storage.load_execution_state("exec-1").status == "running"

    def test_guard_terminalize_failure_keeps_message(self):
        """后备终态化写盘失败（瞬断）→ 保守跳过死信，消息留 pending。"""
        fake = fakeredis.FakeRedis(decode_responses=True)

        class FalseSaveStorage(MemoryExecutionStorage):
            def save_execution_state(self, execution_id, state) -> bool:
                super().save_execution_state(execution_id, state)
                return False

        storage = FalseSaveStorage()
        worker = _redis_worker(fake, storage)
        storage.save_execution_state("exec-1", _state(status="running"))
        fake.set("plaita:execution:noderetry:exec-1", "5")

        assert worker._dead_letter_guard(self._task()) is False
        assert fake.xlen("test:worker-node-retry") == 0
