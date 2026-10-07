"""执行事件时间线（ExecutionEventRecorder + worker 接线 + 排队时长）。

背景：节点维度的时间线过去只有 Langfuse（失败节点还缺），「这个节点排队多久、
跑了多久」在系统里没有答案。本用例覆盖采集器的载荷形状与旁路语义、ExecutionState
的排队字段，以及 RedisFlowWorker 是否真的把事件写进了 per-execution Stream。
"""

import json

import pytest

pytest.importorskip("cachetools")
fakeredis = pytest.importorskip("fakeredis")

from plaita.core.errors import NodeExecutionError
from plaita.event.memory import InMemoryEventBus
from plaita.server.execution_events import (
    TIMELINE_EVENT_TYPES,
    ExecutionEventRecorder,
    execution_events_key,
    queue_wait_ms,
)
from plaita.server.flow_worker import FlowWorker, RedisFlowWorker
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


class _Node:
    def __init__(self, node_id: str, name: str = ""):
        self.id = node_id
        self.name = name


class _Clock:
    def __init__(self, start: float = 1_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def tick(self, seconds: float) -> None:
        self.now += seconds


class _FakeRedis:
    """只记录 xadd/expire/publish 的最小替身（载荷形状断言用）。"""

    def __init__(self, fail_xadd: bool = False):
        self.entries = []
        self.expires = []
        self.published = []
        self.fail_xadd = fail_xadd

    def xadd(self, key, fields, maxlen=None, approximate=True):
        if self.fail_xadd:
            raise RuntimeError("redis down")
        stream_id = f"1700000000000-{len(self.entries) + 1}"
        self.entries.append((key, stream_id, fields, maxlen))
        return stream_id

    def expire(self, key, ttl):
        self.expires.append((key, ttl))

    def publish(self, channel, payload):
        self.published.append((channel, payload))

    def events(self):
        return [json.loads(f["data"]) for _key, _sid, f, _ml in self.entries]


class TestQueueWaitMs:
    def test_computes_milliseconds(self):
        assert queue_wait_ms("2026-10-07T10:00:00", "2026-10-07T10:00:02.500000") == 2500

    @pytest.mark.parametrize("queued,started", [
        (None, "2026-10-07T10:00:00"),
        ("2026-10-07T10:00:00", None),
        ("not-a-time", "2026-10-07T10:00:00"),
        ("2026-10-07T10:00:00", "not-a-time"),
    ])
    def test_unknown_or_unparsable_is_none(self, queued, started):
        assert queue_wait_ms(queued, started) is None

    def test_clock_skew_is_clamped_to_zero(self):
        """跨机时钟回拨 → 负值无意义，取 0（不抛、不记负数）。"""
        assert queue_wait_ms("2026-10-07T10:00:05", "2026-10-07T10:00:00") == 0

    def test_timezone_offset_is_not_counted_as_wait(self):
        """入队侧与 worker 侧时区不同（偏移已写进时间戳）→ 按同一时轴差分。

        两串字面时间差 8 小时 3 秒，实际只差 3 秒；naive 相减会把整段 TZ 偏移
        算成排队时长（29999700ms 的假等待）。
        """
        assert (
            queue_wait_ms("2026-10-07T10:00:00+00:00", "2026-10-07T18:00:03+08:00")
            == 3000
        )

    def test_aware_queued_at_against_naive_start(self):
        """混用也成立：老 worker 的 naive 起始时间按读取方时区解释，仍可差分。"""
        assert queue_wait_ms("2026-10-07T10:00:00+00:00", "2026-10-07T10:00:02") is not None


class TestExecutionEventRecorder:
    def test_key_matches_console_channel(self):
        assert execution_events_key("e1") == "plaita:execution:events:e1"

    def test_node_lifecycle_payload_shape(self):
        clock = _Clock()
        redis = _FakeRedis()
        rec = ExecutionEventRecorder(redis, "e1", clock=clock)

        rec.on_node_start(None, _Node("a", "节点A"))
        clock.tick(0.25)
        rec.on_node_end(None, _Node("a", "节点A"))

        events = redis.events()
        assert [e["event"] for e in events] == ["node_start", "node_end"]
        started, ended = events
        assert started["execution_id"] == "e1"
        assert started["node_id"] == "a"
        assert started["ts_ms"] == 1_000_000
        assert ended["node_name"] == "节点A"
        assert ended["duration_ms"] == 250
        assert ended["status"] == "success"
        assert ended["error"] is None
        # 每次追加都刷新 TTL（终态执行没人来删这条 Stream）
        assert redis.expires and redis.expires[0][0] == "plaita:execution:events:e1"

    def test_failed_node_end_carries_error_and_type(self):
        redis = _FakeRedis()
        rec = ExecutionEventRecorder(redis, "e1", clock=_Clock())
        exc = NodeExecutionError("执行节点a出错了: boom", code=-520)
        rec.on_node_start(None, _Node("a"))
        rec.on_node_end(None, _Node("a"), None, {"code": -520, "message": "boom"}, exception=exc)

        ended = redis.events()[-1]
        assert ended["status"] == "error"
        assert ended["error"] == {"code": -520, "message": "boom", "type": "NodeExecutionError"}

    def test_node_end_without_start_has_no_duration(self):
        redis = _FakeRedis()
        rec = ExecutionEventRecorder(redis, "e1", clock=_Clock())
        rec.on_node_end(None, _Node("orphan"))
        assert redis.events()[-1]["duration_ms"] is None

    def test_flow_start_carries_queue_wait(self):
        redis = _FakeRedis()
        rec = ExecutionEventRecorder(redis, "e1", clock=_Clock())
        rec.record_flow_start(
            queued_at="2026-10-07T09:59:58", queue_wait_ms=2000,
            started_at="2026-10-07T10:00:00",
        )
        first = redis.events()[0]
        assert first["event"] == "flow_start"
        assert first["queued_at"] == "2026-10-07T09:59:58"
        assert first["queue_wait_ms"] == 2000

    def test_emitted_event_types_match_the_shared_timeline_table(self):
        """事件类型表是两端共用的事实来源：采集器发出的每种事件都必须在表内
        （console 按 ``TIMELINE_EVENT_TYPES`` 把它推成 SSE ``timeline``；表漂移
        会让时间线载荷落进 ``update``，覆盖详情页的执行信息）。"""
        redis = _FakeRedis()
        rec = ExecutionEventRecorder(redis, "e1", clock=_Clock())
        rec.record_flow_start(started_at="2026-10-07T10:00:00")
        rec.on_node_start(None, _Node("a"))
        rec.on_node_end(None, _Node("a"))

        assert {e["event"] for e in redis.events()} == set(TIMELINE_EVENT_TYPES)

    def test_publish_copy_carries_stream_id(self):
        """实时副本带 stream_id：SSE 据此对「重放段」去重。"""
        redis = _FakeRedis()
        rec = ExecutionEventRecorder(redis, "e1", clock=_Clock())
        rec.on_node_start(None, _Node("a"))

        channel, raw = redis.published[0]
        assert channel == "plaita:execution:events:e1"
        published = json.loads(raw)
        assert published["stream_id"] == redis.entries[0][1]
        assert published == {**redis.events()[0], "stream_id": redis.entries[0][1]}

    def test_stream_is_bounded(self):
        redis = _FakeRedis()
        rec = ExecutionEventRecorder(redis, "e1", maxlen=7, clock=_Clock())
        rec.on_node_start(None, _Node("a"))
        assert redis.entries[0][3] == 7

    def test_redis_failure_is_swallowed(self):
        """观测旁路：写失败只告警，绝不把异常抛进流程执行。"""
        redis = _FakeRedis(fail_xadd=True)
        rec = ExecutionEventRecorder(redis, "e1", clock=_Clock())
        rec.on_node_start(None, _Node("a"))
        rec.on_node_end(None, _Node("a"))
        assert redis.entries == [] and redis.published == []


def _flow():
    return {
        "flow_id": "event-flow",
        "name": "时间线流程",
        "version": "1.0.0",
        "nodes": [
            {"id": "start", "type": "start", "next": "assign1"},
            {"id": "assign1", "type": "assignment", "output": {"step": 1}, "next": "end"},
            {"id": "end", "type": "end", "output": "success"},
        ],
    }


def _redis_worker(fake_redis, flow_storage):
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:execution-events",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=flow_storage,
        event_bus=InMemoryEventBus(),
        redis_client=fake_redis,
        enable_registry=False,
        enable_redis_logging=False,
    )


class TestWorkerWiring:
    def test_base_worker_has_no_recorder(self):
        worker = FlowWorker(
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            event_bus=InMemoryEventBus(),
        )
        assert worker._make_event_recorder("e1") is None
        assert all(
            not isinstance(h, ExecutionEventRecorder) for h in worker._handlers_for("e1")
        )

    def test_redis_worker_streams_node_events_and_queue_wait(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        flow_storage = MemoryFlowStorage()
        flow_storage.save_flow(_flow())
        worker = _redis_worker(fake, flow_storage)

        result = worker.start_flow(
            "event-flow", {}, version="1.0.0",
            execution_id="exec-events",
            queued_at="2026-10-07T10:00:00",
        )
        assert result["execution_id"] == "exec-events"

        entries = fake.xrange(execution_events_key("exec-events"))
        events = [json.loads(fields["data"]) for _sid, fields in entries]
        kinds = [e["event"] for e in events]
        assert kinds[0] == "flow_start"
        for node_id in ("start", "assign1", "end"):
            assert "node_start" in kinds and "node_end" in kinds
            ended = next(
                e for e in events if e["event"] == "node_end" and e["node_id"] == node_id
            )
            assert ended["status"] == "success"
            assert ended["duration_ms"] >= 0
        # 时间线里每个 node_end 都有对应的 node_start（不再只有 start 没有 end）
        started_ids = sorted(e["node_id"] for e in events if e["event"] == "node_start")
        ended_ids = sorted(e["node_id"] for e in events if e["event"] == "node_end")
        assert started_ids == ended_ids

        # 排队时长：worker 认账时间 - 消息里的入队时间，随行落盘
        state = worker.execution_storage.load_execution_state("exec-events")
        assert state.queued_at == "2026-10-07T10:00:00"
        assert state.queue_wait_ms == queue_wait_ms("2026-10-07T10:00:00", state.start_time)
        assert state.queue_wait_ms is not None
        assert events[0]["queue_wait_ms"] == state.queue_wait_ms

        # 实时副本与 Stream 同频道（SSE 双段的同一把 key）：发布能被订到
        channel = execution_events_key("exec-live")
        pubsub = fake.pubsub()
        pubsub.subscribe(channel)
        worker.start_flow(
            "event-flow", {}, version="1.0.0",
            execution_id="exec-live", queued_at="2026-10-07T10:00:00",
        )
        live = []
        empty_reads = 0
        while empty_reads < 2:
            message = pubsub.get_message(timeout=0.5)
            if message is None:
                empty_reads += 1
                continue
            if message.get("type") != "message":
                continue
            live.append(json.loads(message["data"]))
        assert [e["event"] for e in live][:2] == ["flow_start", "node_start"]
        assert all(e["execution_id"] == "exec-live" for e in live)
        assert all(e["stream_id"] for e in live)  # 去重句柄

        # 终态落盘后回收采集器，避免长跑 worker 泄漏
        assert "exec-events" not in worker._event_recorders

    def test_state_without_queue_fields_roundtrips(self):
        state = ExecutionState(execution_id="e", context={})
        assert state.queued_at is None and state.queue_wait_ms is None
        dumped = state.model_dump()
        assert ExecutionState(**dumped).queue_wait_ms is None
