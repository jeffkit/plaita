"""plaita#91 回归：start 载体的重投必须有 resume 语义 + 显式重投必须有退避。

基线缺陷（2026-10-09，#52 显式重投落地后暴露）：

- **无退避**：``_requeue_retryable_failure`` 是即时 XADD，没有旧「留 pending
  等 reclaim」路径的 claim_min_idle_ms 天然间隔——重投副本被立即消费、再失败、
  再重投，5 次预算毫秒级烧光（实测相邻尝试 ~1ms），瞬态故障等不到恢复。
- **start 载体无 resume 语义**：重投/重派回到 worker 的是同体 start 副本；
  dedup_key 未提供或已过期时幂等键拦不住，start_flow 按全新执行处理——新建
  context={} 的 state 落盘**覆写盘上 checkpoint**，再从首节点整条重放（已完成
  节点副作用重烧）。与 #73 的计数清零叠加时预算永不耗尽，执行 ~3ms/轮空转。

修复契约：

- ``start_flow`` 先查盘上本 id 的执行行：终态 → 幂等回报；suspended → 回报
  现状不抢跑；其余非终态 → 委托 ``resume_flow`` 从 checkpoint 续跑（租约被
  原持有者持有时抛 ExecutionLeaseError，run() 按租约冲突处理，不产生并发推进）。
  行级守卫读状态瞬断上抛 ``ExecutionStateLoadError``，不得被包成 RuntimeError
  （与 resume 同语义：留 pending 重投重读）。
- ``run()``（``_consume_loop``）对 ``NodeExecutionRetryableError`` 在显式重投
  **之前**按已放行次数指数退避（``_sleep_node_retry_backoff``）：
  ``base·2^(attempt-1)`` 封顶，``PLAITA_NODE_RETRY_BACKOFF_BASE_MS=0`` 关闭。
  睡眠切片查 ``_running``，stop()/drain 不被拖住。
"""

import time
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.core.errors import FlowErrorException, NodeExecutionError
from plaita.server.flow_worker import (
    NodeExecutionRetryableError,
    RedisFlowWorker,
)
from plaita.server.task_queue import enqueue_task
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.storage.redis import ExecutionStateLoadError


QUEUE = "test:worker-start-carrier-resume"

TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "s1"},
        {"id": "s1", "type": "assignment", "result": {"step": 1}, "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}

CHECKPOINT_CONTEXT = {
    "$LAST_NODE": "start",
    "$NODE": {"start": {"status": "success"}},
}


def _node_failure_exc() -> FlowErrorException:
    """对齐分布式真实形态：run_distributed 把节点异常归一化，原始异常挂 __cause__。"""
    err = NodeExecutionError("执行节点a出错: ConnectionError: blip", node="a")
    wrapped = FlowErrorException(str(err))
    wrapped.__cause__ = err
    return wrapped


def _state(execution_id="exec-1", status="running", context=None) -> object:
    from plaita.storage.base import ExecutionState

    return ExecutionState(
        execution_id=execution_id,
        flow_id="f1",
        flow_version="1",
        status=status,
        context=context if context is not None else CHECKPOINT_CONTEXT,
    )


def _worker(fake=None, storage=None):
    fake = fake or fakeredis.FakeRedis(decode_responses=True)
    storage = storage or MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(TEST_FLOW)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    return worker, storage, fake


def _enqueue_start(fake, execution_id="exec-1", **extra):
    body = {
        "type": "start",
        "flow_id": "f1",
        "execution_id": execution_id,
        "params": {"k": "v"},
        "tenant_id": "default",
    }
    body.update(extra)
    enqueue_task(fake, QUEUE, body)


def _dispatch_until_quiet(worker, fake, max_rounds=20):
    """按 run() 的分支语义驱动队列直到空（不碰退避睡眠；退避单测单测）。"""
    queue = worker._get_task_queue()
    queue.ensure_group()
    for _ in range(max_rounds):
        task = queue.read(block_ms=10)
        if task is None:
            return
        try:
            worker._dispatch_task(task.body, delivery_count=task.delivery_count)
            queue.ack(task.message_id)
        except NodeExecutionRetryableError:
            if not worker._requeue_retryable_failure(task, queue):
                return
        except RuntimeError:
            return


def _stop_on_empty(worker, queue):
    """消费循环驱动器：读空即停（fakeredis 的 BLOCK 立即返回，否则空转不退出）。"""
    orig_read = queue.read

    def read_until_empty(block_ms: int = 10_000):
        task = orig_read(block_ms=min(block_ms, 10))
        if task is None:
            worker._running = False
        return task

    queue.read = read_until_empty
    return queue


# ---------- start 载体的行级守卫（resume 语义） ----------


class TestStartCarrierRowGuard:
    def test_redelivered_start_resumes_from_checkpoint(self):
        """在途行 + 重投 start → 委托 resume：checkpoint 进引擎、盘上 context
        不被 context={} 覆写、不从首节点整条重放。"""
        worker, storage, fake = _worker()
        storage.save_execution_state("exec-1", _state())
        seen = []

        def scripted_run_distributed(flow, saved_context=None, resume_type="continue", **kw):
            st = storage.load_execution_state("exec-1")
            seen.append(
                {
                    "saved_context": None if saved_context is None else dict(saved_context),
                    "disk_context": None if st is None else dict(st.context),
                    "has_params": "params" in kw and kw["params"] is not None,
                }
            )
            if saved_context is None:
                # start_flow 的全新启动路径才没有 saved_context——本用例不允许出现
                return {"execution_id": "exec-1", "is_end": True, "context": {"done": 1}}
            return {"execution_id": "exec-1", "is_end": True, "context": {"done": 1}}

        _enqueue_start(fake)
        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = scripted_run_distributed
            _dispatch_until_quiet(worker, fake)

        assert len(seen) == 1
        # resume 语义：checkpoint 进引擎，而不是 params 起新
        assert seen[0]["saved_context"] == CHECKPOINT_CONTEXT
        assert not seen[0]["has_params"]
        # 盘上 checkpoint 全程未被覆写（基线缺陷：先被 context={} 落盘覆盖）
        assert seen[0]["disk_context"] == CHECKPOINT_CONTEXT
        assert storage.load_execution_state("exec-1").status == "completed"

    def test_redelivered_start_on_terminal_row_is_idempotent(self):
        """终态行 + 重投 start → 幂等回报，绝不覆写已完成的执行、不再执行。"""
        worker, storage, fake = _worker()
        done = _state(status="completed", context={"done": 1})
        done.end_time = "2026-10-09T00:00:00"
        storage.save_execution_state("exec-1", done)

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = AssertionError("终态行不得再执行")
            _enqueue_start(fake)
            _dispatch_until_quiet(worker, fake)

        inst.run_distributed.assert_not_called()
        state = storage.load_execution_state("exec-1")
        assert state.status == "completed"
        assert state.context == {"done": 1}
        assert state.end_time == "2026-10-09T00:00:00"

    def test_redelivered_start_on_suspended_row_does_not_advance(self):
        """挂起行 + 重投 start → 回报现状不推进（挂起在等外延事件/人工 resume）。"""
        worker, storage, fake = _worker()
        storage.save_execution_state("exec-1", _state(status="suspended"))

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = AssertionError("挂起行不得被 start 抢跑")
            _enqueue_start(fake)
            _dispatch_until_quiet(worker, fake)

        inst.run_distributed.assert_not_called()
        assert storage.load_execution_state("exec-1").status == "suspended"

    def test_fresh_start_without_row_unchanged(self):
        """盘上无行（真·首次启动）→ 存量全新启动语义不变（params 起新）。"""
        worker, storage, fake = _worker()
        seen = {}

        def scripted_run_distributed(flow, saved_context=None, resume_type="continue", **kw):
            seen["saved_context"] = saved_context
            seen["params"] = kw.get("params")
            return {"execution_id": "exec-1", "is_end": True, "context": {"done": 1}}

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = scripted_run_distributed
            _enqueue_start(fake)
            _dispatch_until_quiet(worker, fake)

        assert seen["saved_context"] is None
        assert seen["params"] == {"k": "v"}
        assert storage.load_execution_state("exec-1").status == "completed"

    def test_state_load_error_propagates_not_wrapped(self):
        """行级守卫读状态瞬断 → ExecutionStateLoadError 原样上抛（留 pending
        重投重读），不得被包成 RuntimeError 走误 ack/终态路径。"""
        worker, storage, fake = _worker()
        with patch.object(
            worker.execution_storage,
            "load_execution_state",
            side_effect=ExecutionStateLoadError("redis blip"),
        ):
            with pytest.raises(ExecutionStateLoadError):
                worker.start_flow("f1", {"k": "v"}, execution_id="exec-1")


# ---------- 显式重投的指数退避 ----------


class TestRetryBackoffSchedule:
    def _worker_for_backoff(self):
        return _worker()[0]

    def test_exponential_growth_from_base(self, monkeypatch):
        monkeypatch.setenv("PLAITA_NODE_RETRY_BACKOFF_BASE_MS", "500")
        monkeypatch.delenv("PLAITA_NODE_RETRY_BACKOFF_MAX_MS", raising=False)
        w = self._worker_for_backoff()
        assert w._node_retry_backoff_seconds(1) == 0.5
        assert w._node_retry_backoff_seconds(2) == 1.0
        assert w._node_retry_backoff_seconds(3) == 2.0
        assert w._node_retry_backoff_seconds(4) == 4.0

    def test_attempt_zero_or_negative_treated_as_first(self, monkeypatch):
        monkeypatch.setenv("PLAITA_NODE_RETRY_BACKOFF_BASE_MS", "500")
        w = self._worker_for_backoff()
        assert w._node_retry_backoff_seconds(0) == 0.5
        assert w._node_retry_backoff_seconds(-3) == 0.5

    def test_cap_bounds_single_sleep(self, monkeypatch):
        monkeypatch.setenv("PLAITA_NODE_RETRY_BACKOFF_BASE_MS", "500")
        monkeypatch.setenv("PLAITA_NODE_RETRY_BACKOFF_MAX_MS", "2000")
        w = self._worker_for_backoff()
        assert w._node_retry_backoff_seconds(10) == 2.0

    def test_base_zero_disables_backoff(self, monkeypatch):
        monkeypatch.setenv("PLAITA_NODE_RETRY_BACKOFF_BASE_MS", "0")
        w = self._worker_for_backoff()
        assert w._node_retry_backoff_seconds(3) == 0.0
        assert w._sleep_node_retry_backoff(3) == 0.0

    def test_invalid_env_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("PLAITA_NODE_RETRY_BACKOFF_BASE_MS", "abc")
        w = self._worker_for_backoff()
        assert w._node_retry_backoff_seconds(1) == w.NODE_RETRY_BACKOFF_BASE_MS / 1000

    def test_sleep_stops_when_worker_stopping(self, monkeypatch):
        """stop()/drain 置 _running=False 后切片睡眠立即让路，不被退避拖住。"""
        monkeypatch.setenv("PLAITA_NODE_RETRY_BACKOFF_BASE_MS", "60000")
        w = self._worker_for_backoff()
        w._running = False
        started = time.monotonic()
        slept = w._sleep_node_retry_backoff(1)
        assert time.monotonic() - started < 1.0
        assert slept == 0.0


class TestRetryBackoffInConsumeLoop:
    def test_backoff_runs_before_requeue(self, monkeypatch):
        """端到端：run() 的重投分支先退避、后重投（顺序钉死），副本消费后收敛。

        环境退避基值压到 1ms 保持用例快速；顺序用包装器记录。"""
        monkeypatch.setenv("PLAITA_NODE_RETRY_BACKOFF_BASE_MS", "1")
        worker, storage, fake = _worker()
        storage.save_execution_state("exec-1", _state())
        queue = worker._get_task_queue()
        queue.ensure_group()
        _enqueue_start(fake)

        order = []
        orig_sleep = worker._sleep_node_retry_backoff
        orig_requeue = worker._requeue_retryable_failure

        def scripted_sleep(attempt):
            order.append(("backoff", attempt))
            return orig_sleep(attempt)

        def scripted_requeue(task, q):
            order.append(("requeue",))
            return orig_requeue(task, q)

        worker._sleep_node_retry_backoff = scripted_sleep
        worker._requeue_retryable_failure = scripted_requeue

        calls = {"n": 0}

        def scripted_run_distributed(flow, saved_context=None, **kw):
            calls["n"] += 1
            if calls["n"] == 1:
                # 第一次投递：resume 自 checkpoint 的首步即失败（瞬态）
                raise _node_failure_exc()
            # 重投副本：成功收尾；循环由「读空即停」驱动器退出
            return {"execution_id": "exec-1", "is_end": True, "context": {"done": 1}}

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = scripted_run_distributed
            worker._running = True
            worker._consume_loop(_stop_on_empty(worker, queue))

        assert order == [("backoff", 1), ("requeue",)]
        assert storage.load_execution_state("exec-1").status == "completed"
        assert fake.xpending(QUEUE, queue.group_name)["pending"] == 0


# ---------- 闭环：修复后预算正常耗尽（对照基线的「永不耗尽空转」） ----------


class TestBudgetExhaustionAfterFix:
    def test_retry_budget_exhausts_and_terminalizes(self, monkeypatch):
        """resume 载体重投循环：每轮直接命中失败节点 → 计数 INCR → 达预算
        终态化 error（带 node_retries）。退避关闭以保持用例快速（退避本身
        由上组用例覆盖）。"""
        monkeypatch.setenv("PLAITA_NODE_RETRY_BACKOFF_BASE_MS", "0")
        worker, storage, fake = _worker()
        storage.save_execution_state("exec-1", _state())
        queue = worker._get_task_queue()
        queue.ensure_group()
        enqueue_task(
            fake,
            QUEUE,
            {
                "type": "resume",
                "flow_id": "f1",
                "execution_id": "exec-1",
                "resume_type": "continue",
                "tenant_id": "default",
            },
        )

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = MagicMock()
            FE.return_value = inst
            inst.run_distributed.side_effect = _node_failure_exc()
            worker._running = True
            worker._consume_loop(_stop_on_empty(worker, queue))

        state = storage.load_execution_state("exec-1")
        assert state.status == "error"
        assert state.error["node_retries"] == 4
        assert fake.get("plaita:execution:noderetry:exec-1") == "5"
