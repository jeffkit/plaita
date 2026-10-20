"""plaita#25 P2 —— 错误路径落盘：失败 run 的落盘时间线含失败节点与 run_end。

红先证（修复前在本轮基线树 `a789498` 上实跑，见 /tmp/w20-a-out/RED-PROOF.md）：
``start_flow`` 的错误路径（确定性失败，非可重试族）**不落盘**——
``run_distributed`` 抛出后只打日志 + ``finalize_observers`` + 抛 RuntimeError，
状态行永远停在 ``start`` 时落的 ``running``：

    state.status   = 'running'
    state.timeline = {"events": [run_start]}        # 只有开篇
    in-memory 事件 = [run_start, node_start:start, node_end:start,
                      node_start:boom, node_end:boom:ok=false]   # 失败节点在，
                                                                  # 但只实时发布

⇒ 数小时后复盘时，落盘时间线里**缺失败节点与 run_end**（Langfuse 之外的
第二来源落空）。

本文件守住 P2 竖切：错误路径以**唯一落盘收口**把这次尝试补进时间线
（``node_end(ok=false)`` + ``run_end(final=False, status=error)``），
且**不改状态行、不改 ack/重投语义**（落盘抛错只告警 ⇒ 消息仍不 ack 走重投）。
状态行保持非终态是既有契约（可重投/可 cancel），见
``tests/unit/test_g1_resume_retry.py::test_running_row_survives_start_failure``
——P2 只补观测面，不把 running 行改写成 error。
"""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar
from unittest.mock import patch

import pytest

pytest.importorskip("cachetools")

from plaita.event.memory import InMemoryEventBus  # noqa: E402
from plaita.node import Node, get_default_registry  # noqa: E402
from plaita.server.flow_worker import (  # noqa: E402
    FlowWorker,
    RedisFlowWorker,
    StatePersistError,
)
from plaita.server.task_queue import StreamTask  # noqa: E402
from plaita.storage.base import ExecutionState  # noqa: E402
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage  # noqa: E402

BOOM_TYPE = "w20_p2_test_boom"
BOOM_FLOW = {
    "flow_id": "w20-p2-boom",
    "name": "w20-p2-boom",
    "version": "1.0.0",
    "nodes": [
        {"id": "start", "type": "start", "next": "boom"},
        {"id": "boom", "type": BOOM_TYPE, "next": "end"},
        {"id": "end", "type": "end", "output": "success"},
    ],
}


@pytest.fixture()
def boom_node_type():
    class BoomNode(Node):
        node_type: ClassVar[str] = BOOM_TYPE

        def execute(self, execution):  # noqa: ARG002
            raise RuntimeError("w20 p2 boom")

    reg = get_default_registry()
    reg.register(BoomNode)
    yield BOOM_TYPE
    reg.unregister(BOOM_TYPE)


def _worker(flow=None, storage=None, *, redis_client=None) -> FlowWorker:
    storage = storage if storage is not None else MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(flow or BOOM_FLOW)
    if redis_client is None:
        worker = FlowWorker(
            execution_storage=storage,
            flow_storage=flow_storage,
            event_bus=InMemoryEventBus(),
        )
    else:
        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/15",
            queue_name="test:issue25-p2",
            execution_storage=storage,
            flow_storage=flow_storage,
            redis_client=redis_client,
            enable_registry=False,
            enable_redis_logging=False,
        )
    return worker


def _run_failing_start(worker, execution_id="w20p2") -> Exception:
    with pytest.raises(Exception) as ei:
        worker.start_flow(
            flow_id="w20-p2-boom", params={}, version="1.0.0",
            execution_id=execution_id,
        )
    return ei.value


def _timeline_types(state) -> list:
    return [e["type"] for e in (state.timeline or {}).get("events") or []]


class TestFailedStartPersistsTimeline:
    def test_failed_attempt_is_recorded_with_failed_node_and_run_end(self, boom_node_type):
        worker = _worker()
        _run_failing_start(worker)
        state = worker.execution_storage.load_execution_state("w20p2")

        # 状态行**刻意**保持非终态：既有契约（可重投/可 cancel/zombie 可见），
        # P2 只补观测面。见 tests/unit/test_g1_resume_retry.py 的预铸 id 守卫。
        assert state.status == "running", "P2 不得把错误路径的 running 行改写成终态"
        events = (state.timeline or {}).get("events") or []
        failed = [e for e in events
                  if e["type"] == "node_end" and e.get("node_id") == "boom"]
        assert failed, "落盘时间线必须含失败节点（修复前只有内存事件里有）"
        assert failed[0]["ok"] is False
        assert "boom" in (failed[0].get("error") or "")
        assert events[-1]["type"] == "run_end"
        assert events[-1]["status"] == "error"
        assert events[-1]["final"] is False, "非终态尝试的收口必须标 final=False"

    def test_state_row_fields_are_untouched_by_the_attempt_persist(self, boom_node_type):
        """只补观测面：context/error/end_time 一个都不许被这次落盘改写。"""
        worker = _worker()
        _run_failing_start(worker)
        state = worker.execution_storage.load_execution_state("w20p2")
        assert state.error is None
        assert state.end_time is None
        assert state.context == {}

    def test_terminal_run_end_is_marked_final(self):
        """终态收口（completed）仍走 finish_run：run_end 带 final=True。"""
        ok_flow = {
            "flow_id": "w20-p2-ok", "name": "ok", "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "end"},
                {"id": "end", "type": "end", "output": "ok"},
            ],
        }
        worker = _worker(flow=ok_flow)
        result = worker.start_flow(flow_id="w20-p2-ok", params={}, version="1.0.0")
        state = worker.execution_storage.load_execution_state(result["execution_id"])
        assert state.status == "completed"
        ends = [e for e in state.timeline["events"] if e["type"] == "run_end"]
        assert len(ends) == 1 and ends[0]["final"] is True
        assert ends[0]["status"] == "completed"

    def test_events_are_not_duplicated_by_the_error_persist(self, boom_node_type):
        """错误落盘走增量交付（drain）：不得把 start 时已落盘的 run_start 再追加。"""
        worker = _worker()
        _run_failing_start(worker)
        state = worker.execution_storage.load_execution_state("w20p2")
        events = (state.timeline or {}).get("events") or []
        assert [e["seq"] for e in events] == list(range(1, len(events) + 1))
        keys = [(e["type"], e.get("node_id")) for e in events]
        assert len(keys) == len(set(keys)), f"事件被重复落盘：{keys}"
        assert _timeline_types(state).count("run_end") == 1

    def test_collector_kept_for_later_attempts(self, boom_node_type):
        """非终态不回收采集器：同一执行的后续尝试继续往同一条流里追加。"""
        worker = _worker()
        _run_failing_start(worker)
        assert "w20p2" in worker._timelines

    def test_cancellation_path_still_wins_over_error(self, boom_node_type, monkeypatch):
        """取消是控制面意图：命中取消的路径照旧终态化 cancelled，不写 error。"""
        worker = _worker()
        monkeypatch.setattr(worker, "_cancel_requested", lambda _eid: True)
        result = worker.start_flow(
            flow_id="w20-p2-boom", params={}, version="1.0.0", execution_id="w20p2c"
        )
        assert result["status"] == "cancelled"
        state = worker.execution_storage.load_execution_state("w20p2c")
        assert state.status == "cancelled"


class TestAckSemanticsUnchanged:
    """负控：错误路径的观测落盘**不得**改写 ack/重投语义。"""

    def _attempt_save_fails_storage(self):
        class _AttemptSaveFails(MemoryExecutionStorage):
            """第一次落盘（start 的 running 行）成功，之后每次落盘都炸
            ——覆盖错误路径收口这一笔。"""

            def __init__(self):
                super().__init__()
                self.saves = 0

            def save_execution_state(self, execution_id, state) -> bool:
                self.saves += 1
                if self.saves > 1:
                    raise StatePersistError("redis blip on attempt persist")
                return super().save_execution_state(execution_id, state)

        return _AttemptSaveFails()

    def test_persist_failure_is_swallowed_and_runtimeerror_still_raised(
        self, boom_node_type
    ):
        storage = self._attempt_save_fails_storage()
        worker = _worker(storage=storage)
        exc = _run_failing_start(worker)
        # 上抛的必须是「确定性失败」的 RuntimeError，而不是落盘异常
        assert type(exc) is RuntimeError
        assert "执行流程出错" in str(exc)
        # 落盘失败 ⇒ 行保持 start 时落的那一份（非终态、没有本次尝试的收口）
        state = storage.load_execution_state("w20p2")
        assert state.status == "running"
        assert state.timeline is not None  # start 时的 run_start 锚点仍在
        assert all(e["type"] != "run_end" for e in state.timeline["events"])

    def test_persist_failure_leaves_message_unacked(self, boom_node_type):
        """落盘抛错 ⇒ 消息仍不 ack、不 poison、不死信（留 pending 等重投）。"""
        import fakeredis

        pytest.importorskip("lupa")
        redis_client = fakeredis.FakeRedis(decode_responses=True)
        storage = self._attempt_save_fails_storage()
        worker = _worker(storage=storage, redis_client=redis_client)

        task = StreamTask(
            message_id="m1",
            body={"type": "start", "flow_id": "w20-p2-boom", "params": {},
                  "execution_id": "w20p2", "tenant_id": "default",
                  # A69-04：合成载体（非数字 message_id）需带年龄证据。
                  "timestamp": datetime.now().isoformat()},
            delivery_count=1,
        )
        stub = _StubQueue(worker, [task])
        with patch.object(worker, "_get_task_queue", return_value=stub):
            worker.run()

        assert stub.acked == [], "落盘抛错绝不能让消息被 ack"
        assert stub.dead_lettered == []
        assert stub.poison == 0
        assert stub.failed == 1

    def test_retryable_node_failure_is_not_terminalized(self, boom_node_type):
        """可重试族（节点瞬态失败）照旧**不**终态化——重投语义不被观测改写。

        与 tests/unit/test_worker_node_retry.py::TestStartPathRetry 同源的回归点：
        本批新增的终态落盘只挂在非可重试的通用 except 上；步进循环里的
        ``NodeExecutionRetryableError`` 分支原样上抛（状态停在 checkpoint）。
        """
        from plaita.core.errors import FlowErrorException, NodeExecutionError
        from plaita.server.flow_worker import NodeExecutionRetryableError

        worker = _worker()
        cause = ConnectionError("llm blip")
        err = NodeExecutionError("执行节点boom出错了", node="boom")
        wrapped = FlowErrorException(str(err))
        wrapped.__cause__ = err
        err.__cause__ = cause

        with patch("plaita.server.flow_worker.FlowExecution") as FE:
            inst = FE.return_value
            inst.execution_id = "w20p2r"
            inst.run_distributed.side_effect = [
                # 第一步：start 节点成功，返回非终态 lazy 输出
                {"execution_id": "w20p2r", "is_end": False, "is_suspend": False,
                 "context": {"$LAST_NODE": "start", "$NODE": {}}},
                # 第二步（步进循环内）：节点瞬态失败
                wrapped,
            ]
            with pytest.raises(NodeExecutionRetryableError):
                worker.start_flow(
                    flow_id="w20-p2-boom", params={}, version="1.0.0",
                    execution_id="w20p2r",
                )
        state = worker.execution_storage.load_execution_state("w20p2r")
        assert state.status == "running", "可重试失败不得被终态化"
        assert state.timeline is None or all(
            e["type"] != "run_end" for e in state.timeline["events"]
        )

class TestHelperBoundaries:
    def test_error_persist_keeps_prior_events(self, boom_node_type):
        """跨进程/多段落盘：旧事件不得被错误路径的落盘抹掉。"""
        from plaita.server.execution_timeline import merge_timeline

        old = {"events": [{"seq": 1, "type": "run_start", "at_ms": 1}],
               "dropped": 0, "enqueued_at": "2026-01-01T00:00:00", "queue_wait_ms": 7}
        new = {"events": [{"seq": 1, "type": "node_end", "node_id": "boom", "ok": False}],
               "dropped": 0, "enqueued_at": None, "queue_wait_ms": None}
        merged = merge_timeline(old, new)
        assert [e["type"] for e in merged["events"]] == ["run_start", "node_end"]
        assert merged["queue_wait_ms"] == 7

    def test_helper_is_noop_without_registered_collector(self):
        """没有登记采集器（骨架 worker/内存档）：不得抛错、不得造出空时间线。"""
        worker = _worker()
        state = ExecutionState(execution_id="ghost", context={})
        worker._record_failed_start_attempt("ghost", state)
        assert state.timeline is None, "未采集 ≠ 采集到空"

    def test_helper_tolerates_missing_state(self):
        worker = _worker()
        worker._record_failed_start_attempt("ghost", None)

    def test_helper_never_flips_status(self, boom_node_type):
        """状态行是终态时也不得被改写成 error（只补观测面，不做状态迁移）。"""
        worker = _worker()
        state = ExecutionState(execution_id="done", context={}, status="completed")
        worker._record_failed_start_attempt("done", state)
        assert state.status == "completed"


class _StubQueue:
    """最小队列桩：弹出预置任务后置 worker 停机；记录 ack/dead_letter 调用。

    （与 tests/unit/test_worker_node_retry.py 的同名桩同形——该文件是
    「不 ack」语义的既有守卫，本文件只在错误路径落盘这一竖切上复用。）
    """

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
