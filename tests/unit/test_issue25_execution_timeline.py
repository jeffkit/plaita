"""plaita#25 —— run 级事件时间线：最小持久化 + 唯一发布者 + 排队时长可答。

红先证（修复前在 origin/main(87b8ad0) 工作树上实跑，见 /tmp/w9-a-out/RED-PROOF.md）：
1. ``ExecutionState`` 无 ``timeline`` 字段——``"timeline" in ExecutionState.model_fields``
   为 False；worker 跑完整条 flow 后落盘状态里没有任何事件；
2. ``plaita:execution:events:{id}`` 全仓只有**订阅方**（console SSE）与**删除方**
   （delete_execution），零发布者 ⇒ 集群档 SSE 除 initial_state 外永远静默；
3. 失败节点（abort 策略）不触发 ``on_node_end`` ⇒ node_timings / Langfuse span /
   时间线三处同时缺该节点（runner.py 直接 raise）；
4. 「排队多久」无处计算：console 入队写 ``timestamp``，worker 从不读它。

本文件把上面 1/3/4 钉成守卫，并给发布者加**负控**（发布失败/关闭/无 redis
都不得影响执行结果）。
"""

from __future__ import annotations

from typing import ClassVar

import pytest

pytest.importorskip("cachetools")

from plaita.core.executor import ExecutionMode, FlowExecution  # noqa: E402
from plaita.event.memory import InMemoryEventBus  # noqa: E402
from plaita.server.execution_timeline import (  # noqa: E402
    CHANNEL_PREFIX,
    ExecutionTimelineCallback,
    merge_timeline,
)
from plaita.server.flow_worker import FlowWorker  # noqa: E402
from plaita.storage.base import ExecutionState  # noqa: E402
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage  # noqa: E402


class _Node:
    def __init__(self, node_id):
        self.id = node_id


class _Clock:
    """可推进的假时钟（秒）。"""

    def __init__(self, start: float = 1_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def tick(self, seconds: float) -> None:
        self.now += seconds


class _RecordingRedis:
    """只记 publish 的最小假客户端（worker 侧只需 publish）。"""

    def __init__(self, raise_on_publish: bool = False):
        self.published = []
        self.raise_on_publish = raise_on_publish

    def publish(self, channel, payload):
        if self.raise_on_publish:
            raise RuntimeError("redis down")
        self.published.append((channel, payload))
        return 1


def _flow():
    return {
        "flow_id": "timeline-flow",
        "name": "时间线流程",
        "version": "1.0.0",
        "nodes": [
            {"id": "start", "type": "start", "next": "assign1"},
            {"id": "assign1", "type": "assignment", "output": {"step": 1}, "next": "end"},
            {"id": "end", "type": "end", "output": "success"},
        ],
    }


def _worker(redis_client=None):
    execution_storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(_flow())
    worker = FlowWorker(
        execution_storage=execution_storage,
        flow_storage=flow_storage,
        event_bus=InMemoryEventBus(),
    )
    if redis_client is not None:
        worker.redis_client = redis_client
    return worker, execution_storage


def _types(snapshot):
    return [e["type"] for e in snapshot["events"]]


class TestStateSchema:
    """老状态零回归：timeline 可选，缺省 None（与 node_timings 同款）。"""

    def test_timeline_defaults_to_none(self):
        assert ExecutionState(execution_id="e", context={}).timeline is None

    def test_old_state_dict_without_timeline_loads(self):
        dumped = ExecutionState(execution_id="e", context={}).model_dump()
        dumped.pop("timeline", None)
        assert ExecutionState(**dumped).timeline is None

    def test_timeline_round_trips(self):
        tl = {"events": [{"seq": 1, "type": "run_start"}], "dropped": 0,
              "queue_wait_ms": 12}
        state = ExecutionState(execution_id="e", context={}, timeline=tl)
        assert ExecutionState(**state.model_dump()).timeline == tl


class TestExecutionTimelineCallback:
    def test_is_a_full_flow_callback(self):
        """必须继承 FlowCallback：分发器逐个 getattr，缺钩子会打噪声告警。"""
        from plaita.core.callback import FlowCallback

        assert issubclass(ExecutionTimelineCallback, FlowCallback)
        cb = ExecutionTimelineCallback()
        for hook in ("on_flow_start", "on_flow_end", "on_flow_suspend", "on_flow_resume"):
            getattr(cb, hook)(None)

    def test_records_node_events_with_timestamps_and_duration(self):
        clock = _Clock()
        cb = ExecutionTimelineCallback(clock=clock)
        cb.start_run()
        cb.on_node_start(None, _Node("a"))
        clock.tick(2.5)
        cb.on_node_end(None, _Node("a"))
        snap = cb.snapshot()
        assert _types(snap) == ["run_start", "node_start", "node_end"]
        end = snap["events"][-1]
        assert end["node_id"] == "a"
        assert end["duration_ms"] == 2500
        assert end["ok"] is True
        assert end["at_ms"] - snap["events"][1]["at_ms"] == 2500
        # seq 单调递增（事件流可排序，不依赖同毫秒稳定性）
        assert [e["seq"] for e in snap["events"]] == [1, 2, 3]

    def test_failed_node_event_carries_error_and_not_ok(self):
        cb = ExecutionTimelineCallback(clock=_Clock())
        cb.start_run()
        cb.on_node_start(None, _Node("boom"))
        cb.on_node_end(None, _Node("boom"), None, {"code": -500, "message": "kaboom"},
                       exception=RuntimeError("kaboom"))
        end = cb.snapshot()["events"][-1]
        assert end["ok"] is False
        assert "kaboom" in end["error"]

    def test_failed_node_error_text_is_scrubbed(self):
        """A2R49-02：时间线事件 ``error`` = ``str(exception)`` 是**读面**——

        落盘（``/timeline`` 端点）与实时发布（SSE 频道）共用同一份事件记录，
        构造处必须先过 scrub（与同模块 logger 侧 ``#151`` 同判据），诊断语义
        （异常类型、消息主体）保留。
        """
        cb = ExecutionTimelineCallback(clock=_Clock())
        cb.start_run()
        cb.on_node_start(None, _Node("call"))
        cb.on_node_end(
            None, _Node("call"), None, None,
            exception=RuntimeError(
                "ConnectionError: Max retries exceeded with url: "
                "/api/run?token=sk-live-timeline-needle"
            ),
        )
        end = cb.snapshot()["events"][-1]
        assert end["ok"] is False
        assert "sk-live-timeline-needle" not in end["error"]
        assert "Max retries exceeded" in end["error"]

    def test_failed_node_error_text_without_credentials_unchanged(self):
        """负控：非凭据文本一字不动（scrub 只抹凭据分量）。"""
        cb = ExecutionTimelineCallback(clock=_Clock())
        cb.start_run()
        cb.on_node_end(None, _Node("boom"), None, {"code": -500, "message": "kaboom"},
                       exception=RuntimeError("kaboom"))
        assert cb.snapshot()["events"][-1]["error"] == "kaboom"

    def test_failed_node_error_none_stays_none(self):
        """成功节点没有 error：scrub 不得把 None 变成空串以外的形状。"""
        cb = ExecutionTimelineCallback(clock=_Clock())
        cb.start_run()
        cb.on_node_end(None, _Node("ok-node"))
        assert cb.snapshot()["events"][-1]["error"] is None

    def test_queue_wait_answered_from_enqueued_at(self):
        cb = ExecutionTimelineCallback(clock=_Clock(start=1_000.0),
                                       enqueued_at="1970-01-01T00:16:40+00:00")
        cb.start_run()
        snap = cb.snapshot()
        assert snap["enqueued_at"] == "1970-01-01T00:16:40+00:00"
        assert snap["queue_wait_ms"] == 0
        assert "enqueued" in _types(snap)

    def test_queue_wait_none_when_enqueue_time_unknown(self):
        """「不知道排队多久」与「排队 0ms」必须可区分（未采集 ≠ 0）。"""
        cb = ExecutionTimelineCallback(clock=_Clock())
        cb.start_run()
        snap = cb.snapshot()
        assert snap["queue_wait_ms"] is None
        assert snap["enqueued_at"] is None

    def test_run_end_recorded_once(self):
        cb = ExecutionTimelineCallback(clock=_Clock())
        cb.start_run()
        cb.start_run()          # 幂等
        cb.finish_run("completed")
        cb.finish_run("error")  # 只有第一次收口
        snap = cb.snapshot()
        assert _types(snap).count("run_start") == 1
        assert _types(snap).count("run_end") == 1
        assert snap["events"][-1]["status"] == "completed"

    def test_bounded_keeps_newest_and_counts_dropped(self):
        cb = ExecutionTimelineCallback(clock=_Clock(), max_events=3)
        cb.start_run()
        for i in range(5):
            cb.on_node_start(None, _Node(f"n{i}"))
            cb.on_node_end(None, _Node(f"n{i}"))
        snap = cb.snapshot()
        assert len(snap["events"]) == 3
        assert snap["dropped"] == 8      # run_start + 5×(start+end) = 11 条，留 3 条
        assert snap["events"][-1]["node_id"] == "n4"

    def test_broken_node_object_never_raises(self):
        """观测回调绝不得把执行带崩（node.id 自身可能就是要报的错）。"""

        class _Bad:
            @property
            def id(self):
                raise RuntimeError("no id")

        cb = ExecutionTimelineCallback(clock=_Clock())
        cb.start_run()
        cb.on_node_start(None, _Bad())
        cb.on_node_end(None, _Bad())

    def test_publisher_receives_every_event(self):
        seen = []
        cb = ExecutionTimelineCallback(clock=_Clock(), publisher=seen.append)
        cb.start_run()
        cb.on_node_start(None, _Node("a"))
        cb.on_node_end(None, _Node("a"))
        assert [e["type"] for e in seen] == ["run_start", "node_start", "node_end"]

    def test_publisher_failure_never_breaks_callback(self):
        def _boom(_event):
            raise RuntimeError("publish down")

        cb = ExecutionTimelineCallback(clock=_Clock(), publisher=_boom)
        cb.start_run()
        cb.on_node_start(None, _Node("a"))
        cb.on_node_end(None, _Node("a"))
        assert _types(cb.snapshot()) == ["run_start", "node_start", "node_end"]


class TestMergeTimeline:
    """resume 跨进程：旧事件与排队时长必须保留（节点耗时同款合并语义）。"""

    def test_old_events_and_queue_wait_survive(self):
        old = {"events": [{"seq": 1, "type": "run_start", "at_ms": 1_000}],
               "dropped": 0, "enqueued_at": "2026-01-01T00:00:00", "queue_wait_ms": 7}
        new = {"events": [{"seq": 1, "type": "node_start", "node_id": "a", "at_ms": 2_000}],
               "dropped": 0, "enqueued_at": None, "queue_wait_ms": None}
        merged = merge_timeline(old, new)
        assert [e["type"] for e in merged["events"]] == ["run_start", "node_start"]
        assert [e["seq"] for e in merged["events"]] == [1, 2]
        assert merged["queue_wait_ms"] == 7
        assert merged["enqueued_at"] == "2026-01-01T00:00:00"

    def test_merge_without_old_returns_new(self):
        new = {"events": [{"seq": 1, "type": "run_start"}], "dropped": 0}
        assert merge_timeline(None, new)["events"] == new["events"]


class TestWorkerPersistsTimeline:
    def test_start_flow_persists_timeline_and_queue_wait(self):
        worker, storage = _worker()
        result = worker.start_flow(
            flow_id="timeline-flow", params={}, version="1.0.0",
            enqueued_at="2026-01-01T00:00:00+00:00",
        )
        state = storage.load_execution_state(result["execution_id"])
        tl = state.timeline
        assert tl is not None, "落盘的执行状态必须带事件时间线"
        types = [e["type"] for e in tl["events"]]
        assert types[0] == "enqueued", "入队时间已知时必须留下 enqueued 锚点"
        assert types[1] == "run_start"
        assert types[-1] == "run_end"
        for node_id in ("start", "assign1", "end"):
            assert types.count("node_start") == types.count("node_end") == 3
            assert any(e.get("node_id") == node_id and e["type"] == "node_end"
                       for e in tl["events"]), f"{node_id} 缺 node_end 事件"
        assert tl["queue_wait_ms"] is not None and tl["queue_wait_ms"] > 0
        assert tl["events"][-1]["status"] == "completed"
        # 终态落盘后回收采集器，避免长跑 worker 泄漏
        assert result["execution_id"] not in worker._timelines

    def test_worker_without_execution_keeps_state_untouched(self):
        worker, _ = _worker()
        state = ExecutionState(execution_id="ghost", context={})
        worker._collect_execution_timeline("ghost", state)
        assert state.timeline is None

    def test_incremental_persists_do_not_duplicate_events(self):
        """每个节点边界都会落盘：增量交付（drain）不得把已落盘事件重复追加。

        反例（本条修掉的真缺陷）：宿主用**全量** snapshot 去 merge，落盘 N 次
        就重复 N 遍——事件流立刻变成噪声，seq 也不再单调。
        """
        worker, storage = _worker()
        result = worker.start_flow(flow_id="timeline-flow", params={}, version="1.0.0")
        tl = storage.load_execution_state(result["execution_id"]).timeline
        seqs = [e["seq"] for e in tl["events"]]
        assert seqs == list(range(1, len(seqs) + 1)), f"seq 必须严格递增：{seqs}"
        keys = [(e["type"], e.get("node_id")) for e in tl["events"]]
        assert len(keys) == len(set(keys)), f"事件被重复落盘：{keys}"
        assert [k for k in keys].count(("node_end", "assign1")) == 1


class TestSinglePublisher:
    """SSE 频道从此有唯一发布者：worker（执行推进的唯一在场方）。"""

    def test_worker_publishes_events_to_sse_channel(self):
        import json as _json

        redis = _RecordingRedis()
        worker, storage = _worker(redis_client=redis)
        result = worker.start_flow(flow_id="timeline-flow", params={}, version="1.0.0")
        eid = result["execution_id"]

        assert redis.published, "时间线事件必须发布到 SSE 频道（修复前零发布者）"
        channels = {c for c, _ in redis.published}
        assert channels == {f"{CHANNEL_PREFIX}{eid}"}
        payloads = [_json.loads(p) for _, p in redis.published]
        assert all(p["execution_id"] == eid for p in payloads)
        kinds = [p["type"] for p in payloads]
        assert "run_start" in kinds and "node_end" in kinds

    def test_publish_disabled_by_env_is_silent_but_execution_succeeds(self, monkeypatch):
        redis = _RecordingRedis()
        monkeypatch.setenv("PLAITA_EXECUTION_EVENTS_PUBLISH", "0")
        worker, storage = _worker(redis_client=redis)
        result = worker.start_flow(flow_id="timeline-flow", params={}, version="1.0.0")
        assert redis.published == []
        state = storage.load_execution_state(result["execution_id"])
        assert state.status == "completed"
        assert state.timeline is not None, "关发布不得关持久化"

    def test_publish_failure_never_breaks_execution(self):
        """负控：Redis 抖动时发布失败，执行结果与落盘必须照常。"""
        redis = _RecordingRedis(raise_on_publish=True)
        worker, storage = _worker(redis_client=redis)
        result = worker.start_flow(flow_id="timeline-flow", params={}, version="1.0.0")
        state = storage.load_execution_state(result["execution_id"])
        assert state.status == "completed"
        assert state.timeline is not None

    def test_no_redis_client_is_noop(self):
        """负控：内存档（无 redis）不得因为没有客户端而失败。"""
        worker, storage = _worker()
        result = worker.start_flow(flow_id="timeline-flow", params={}, version="1.0.0")
        assert storage.load_execution_state(result["execution_id"]).status == "completed"


class TestFailedNodeStillReported:
    """runner 的 abort 路径直接 raise，on_node_end 从未触发 ⇒ 失败节点在
    node_timings / 时间线 / Langfuse 三处同时缺失（#25 Summary 第 4 条）。"""

    @pytest.fixture()
    def boom_node(self):
        from plaita.node import Node, get_default_registry

        class BoomNode(Node):
            node_type: ClassVar[str] = "w9a_test_boom"

            def execute(self, execution):  # noqa: ARG002
                raise RuntimeError("w9a boom")

        reg = get_default_registry()
        reg.register(BoomNode)
        yield "w9a_test_boom"
        reg.unregister("w9a_test_boom")

    def test_engine_fires_node_end_with_error_on_abort(self, boom_node):
        from plaita.core.flow import Flow

        flow = Flow.model_validate({
            "flow_id": "boom-flow",
            "name": "boom",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "boom"},
                {"id": "boom", "type": boom_node, "next": "end"},
                {"id": "end", "type": "end", "output": "success"},
            ],
        })
        cb = ExecutionTimelineCallback(clock=_Clock())
        execution = FlowExecution(callback_handlers=[cb])
        execution.mode = ExecutionMode.DISTRIBUTED
        with pytest.raises(Exception):
            execution.run_distributed(flow, params={})
        ends = [e for e in cb.snapshot()["events"]
                if e["type"] == "node_end" and e.get("node_id") == "boom"]
        assert ends, "abort 失败节点必须补发 on_node_end（否则时间线里它凭空消失）"
        assert ends[0]["ok"] is False

    def test_worker_publishes_failed_node_event_in_real_time(self, boom_node):
        """失败节点的**实时事件**必须先到 SSE——这正是「出事时除 Langfuse 外
        没有第二来源」的补位。

        （落盘面由 #25 P2 补齐：错误路径现在也把这次尝试补进
        ``state.timeline``；见 tests/unit/test_issue25_p2_error_path_timeline.py
        与 /tmp/w20-a-out/RED-PROOF.md。）"""
        import json as _json

        redis = _RecordingRedis()
        worker, storage = _worker(redis_client=redis)
        flow_storage = worker.flow_storage
        flow_storage.save_flow({
            "flow_id": "boom-flow",
            "name": "boom",
            "version": "1.0.0",
            "nodes": [
                {"id": "start", "type": "start", "next": "boom"},
                {"id": "boom", "type": boom_node, "next": "end"},
                {"id": "end", "type": "end", "output": "success"},
            ],
        })
        with pytest.raises(Exception):
            worker.start_flow(flow_id="boom-flow", params={}, version="1.0.0")
        payloads = [_json.loads(p) for _, p in redis.published]
        failed = [p for p in payloads
                  if p["type"] == "node_end" and p.get("node_id") == "boom"]
        assert failed and failed[0]["ok"] is False
