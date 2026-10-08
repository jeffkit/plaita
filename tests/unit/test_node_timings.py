"""节点级耗时采集（NodeTimingCallback）与 worker 落盘。

背景：执行状态里原本只有整条流程的 start_time/end_time，节点维度无任何时间
信息，执行详情页因此说不出「哪个节点慢」。本用例覆盖采集器的语义边界，以及
worker 是否真的把它写进了 ExecutionState.node_timings。
"""

import pytest

pytest.importorskip("cachetools")

from plaita.event.memory import InMemoryEventBus
from plaita.server.flow_worker import FlowWorker
from plaita.server.node_timings import NodeTimingCallback
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


class _Node:
    def __init__(self, node_id: str):
        self.id = node_id


class _Clock:
    """可推进的假时钟（秒）。"""

    def __init__(self, start: float = 1_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def tick(self, seconds: float) -> None:
        self.now += seconds


class TestNodeTimingCallback:
    def test_is_a_full_flow_callback(self):
        """必须继承 FlowCallback：分发器对每个钩子逐个 getattr，缺一个就打一条
        「Error in on_flow_xxx callback ... has no attribute」告警（每个 flow 两条，
        2026-10-08 实测纯噪声）。"""
        from plaita.core.callback import FlowCallback

        assert issubclass(NodeTimingCallback, FlowCallback)
        cb = NodeTimingCallback()
        for hook in ("on_flow_start", "on_flow_end", "on_flow_suspend", "on_flow_resume"):
            getattr(cb, hook)(None)          # 基类 no-op，不得抛 AttributeError

    def test_records_duration_and_epoch_ms(self):
        clock = _Clock()
        cb = NodeTimingCallback(clock=clock)
        cb.on_node_start(None, _Node("a"))
        clock.tick(1.5)
        cb.on_node_end(None, _Node("a"))
        entry = cb.snapshot()["a"]
        assert entry["duration_ms"] == 1500
        assert entry["started_ms"] == 1_000_000
        assert entry["ended_ms"] == 1_001_500
        assert entry["attempts"] == 1
        assert entry["failed"] is False
        assert entry["started_at"] and entry["ended_at"]

    def test_loop_visits_accumulate_attempts_and_total(self):
        clock = _Clock()
        cb = NodeTimingCallback(clock=clock)
        for _ in range(3):
            cb.on_node_start(None, _Node("loop"))
            clock.tick(0.2)
            cb.on_node_end(None, _Node("loop"))
        entry = cb.snapshot()["loop"]
        assert entry["attempts"] == 3
        assert entry["duration_ms"] == 200  # 最后一次
        assert entry["total_duration_ms"] == 600

    def test_end_without_start_does_not_invent_duration(self):
        cb = NodeTimingCallback(clock=_Clock())
        cb.on_node_end(None, _Node("orphan"))
        entry = cb.snapshot()["orphan"]
        assert "duration_ms" not in entry
        assert entry["attempts"] == 1

    def test_failure_marks_entry_but_never_raises(self):
        cb = NodeTimingCallback(clock=_Clock())
        cb.on_node_start(None, _Node("bad"))
        cb.on_node_end(None, _Node("bad"), error=RuntimeError("boom"))
        assert cb.snapshot()["bad"]["failed"] is True

        # 无 id 的对象：内部异常被吞，不影响执行
        cb.on_node_start(None, object())
        cb.on_node_end(None, object())
        assert "bad" in cb.snapshot()

    def test_snapshot_is_a_copy(self):
        cb = NodeTimingCallback(clock=_Clock())
        cb.on_node_start(None, _Node("a"))
        cb.on_node_end(None, _Node("a"))
        snap = cb.snapshot()
        snap["a"]["duration_ms"] = 999
        assert cb.snapshot()["a"]["duration_ms"] != 999


class TestExecutionStateCarriesTimings:
    def test_defaults_to_none_for_old_states(self):
        assert ExecutionState(execution_id="e", context={}).node_timings is None

    def test_model_dump_roundtrip(self):
        state = ExecutionState(
            execution_id="e",
            context={},
            node_timings={"a": {"duration_ms": 5, "attempts": 1}},
        )
        dumped = state.model_dump()
        assert dumped["node_timings"] == {"a": {"duration_ms": 5, "attempts": 1}}
        assert ExecutionState(**dumped).node_timings == {"a": {"duration_ms": 5, "attempts": 1}}


class TestWorkerPersistsTimings:
    def _flow(self):
        return {
            "flow_id": "timing-flow",
            "name": "计时流程",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "assign1"},
                {"id": "assign1", "type": "assignment", "output": {"step": 1}, "next": "assign2"},
                {"id": "assign2", "type": "assignment", "output": {"step": 2}, "next": "end"},
                {"id": "end", "type": "end", "output": "success"},
            ],
        }

    def test_start_flow_records_node_timings(self):
        execution_storage = MemoryExecutionStorage()
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(self._flow())
        worker = FlowWorker(
            execution_storage=execution_storage,
            flow_storage=flow_storage,
            event_bus=InMemoryEventBus(),
        )
        result = worker.start_flow(flow_id="timing-flow", params={}, version="1.0.0")
        execution_id = result["execution_id"]

        state = execution_storage.load_execution_state(execution_id)
        assert state is not None
        timings = state.node_timings
        assert timings, "落盘的执行状态必须带节点耗时"
        for node_id in ("start", "assign1", "assign2", "end"):
            assert node_id in timings, f"{node_id} 缺少耗时记录"
            entry = timings[node_id]
            assert entry["duration_ms"] >= 0
            assert entry["ended_ms"] >= entry["started_ms"]
            assert entry["attempts"] == 1

        # 终态落盘后回收采集器，避免长跑 worker 泄漏
        assert execution_id not in worker._node_timings

    def test_worker_without_execution_keeps_state_untouched(self):
        worker = FlowWorker(
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            event_bus=InMemoryEventBus(),
        )
        state = ExecutionState(execution_id="ghost", context={})
        worker._collect_node_timings("ghost", state)
        assert state.node_timings is None
