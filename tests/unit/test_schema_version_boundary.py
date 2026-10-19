"""版本边界统一走死信守卫 + 重投继承源信封版本（台账 r42/r44）。

## 三个缺陷

- **A42-05**：``_read_once`` 对超 ``max_schema_version`` 的消息直接
  ``dead_letter``，绕过 ``dead_letter_if_allowed`` 单一入口；守卫一次都没被
  调用（探针 ``guard_calls=0, DLQ=1, pending=0``）。
- **A44-01**：``_reclaim_one`` 拿到 XCLAIM 后不校验信封版本。同一条 v2 消息，
  旧 build 首投 → 死信；消费者在 schema 检查前崩溃、别人 reclaim → **按旧语义
  真正执行**。
- **A44-02**：``enqueue_task`` 无条件盖当前 build 的 ``TASK_SCHEMA_VERSION``。
  worker 的重投/交接/归一化副本因此在新 build 上变成 v2，混跑池里的旧实例抢到
  后按超版本死信——唯一恢复载体被自己人写死。

## 修复契约

1. 首投与回收对超版本信封**同判**：都经 ``dead_letter_if_allowed``，守卫拒绝
   时消息留 pending（等新 build 回收），并计 ``schema_rejected``；
2. 重投/交接/归一化副本继承源消息的 ``schema_version``（``None`` = 新生产消息
   才写当前写版本）；
3. DLQ 信封固定带 ``schema_version``（审计/重放不靠猜）；
4. 畸形 payload 的第二旁路（``_dead_letter_invalid``）同样经守卫入口。

## 负控（还原修复后必须精确红）

- 还原 A42-05（直接 ``dead_letter``）：``TestFirstDeliverySchemaReject`` 的
  allow/deny 两条 —— ``guard_calls`` 为空、deny 时 DLQ 1 ≠ 0、pending 0 ≠ 1；
- 还原 A44-01（删除 ``_reclaim_one`` 的版本检查）：
  ``test_reclaimed_over_version_message_is_not_served`` 返回 task（旧 build
  真执行形态）；
- 还原 A44-02（``**kwargs`` 去掉）：所有 ``TestRequeueInheritance`` 的
  kwargs 断言为空（副本落当前版本）。
"""
import json
import threading

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server import task_queue as tq
from plaita.server import flow_worker as fw
from plaita.server.flow_worker import ResumeProtocolError, RedisFlowWorker
from plaita.server.task_queue import (
    PAYLOAD_FIELD,
    SCHEMA_FIELD,
    RedisStreamTaskQueue,
    StreamTask,
    enqueue_task,
)
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

STREAM = "test:schema-boundary"
GROUP = "g"

BODY = {
    "type": "resume",
    "flow_id": "f1",
    "execution_id": "exec-1",
    "resume_type": "continue",
    "tenant_id": "default",
}


def _queue(redis, **kwargs):
    kwargs.setdefault("group_name", GROUP)
    kwargs.setdefault("consumer_name", "reader")
    kwargs.setdefault("claim_min_idle_ms", 1)
    return RedisStreamTaskQueue(redis, STREAM, **kwargs)


def _seed_pending(redis, *, version="2", body=None):
    """投一条消息进 PEL 但不 ack —— 模拟消费者在 schema 检查前崩溃。"""
    seed = _queue(redis, consumer_name="crashed", max_schema_version=99)
    seed.ensure_group()
    fields = {PAYLOAD_FIELD: json.dumps(body or BODY)}
    if version is not None:
        fields[SCHEMA_FIELD] = version
    mid = redis.xadd(STREAM, fields)
    redis.xreadgroup(GROUP, "crashed", {STREAM: ">"}, count=1)
    return mid


def _dlq_envelopes(redis, queue):
    return [
        json.loads(fields[PAYLOAD_FIELD])
        for _mid, fields in redis.xrange(queue.dlq_key)
    ]


def _pending_count(redis):
    summary = redis.xpending(STREAM, GROUP)
    return int(summary["pending"] if isinstance(summary, dict) else summary[0])


def _envelope_version(fields):
    raw = fields.get(SCHEMA_FIELD, fields.get(SCHEMA_FIELD.encode()))
    return raw.decode() if isinstance(raw, bytes) else raw


class TestFirstDeliverySchemaReject:
    """A42-05：首投的超版本消息必须询问守卫再决定死信。"""

    def test_guard_allowed_still_dead_letters_with_schema_envelope(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        calls = []

        def guard(task):
            calls.append(task.message_id)
            return True

        q = _queue(redis, max_schema_version=1, dead_letter_guard=guard)
        q.ensure_group()
        mid = redis.xadd(
            STREAM, {PAYLOAD_FIELD: json.dumps(BODY), SCHEMA_FIELD: "2"}
        )

        assert q.read(block_ms=50) is None

        assert calls == [mid], "A42-05：schema 拒收前必须调用守卫（单一入口）"
        envelopes = _dlq_envelopes(redis, q)
        assert len(envelopes) == 1
        assert envelopes[0]["schema_version"] == 2, "DLQ 信封必须保留源版本"
        assert "unsupported schema_version=2" in envelopes[0]["reason"]
        assert _pending_count(redis) == 0, "守卫放行后原消息应 ack 出 PEL"
        assert q.stats()["schema_rejected"] == 1
        assert q.stats()["dead_lettered"] == 1
        assert q.stats()["dlq_guard_skipped"] == 0

    def test_guard_refusal_keeps_over_version_message_pending(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        calls = []

        def refuse(task):
            calls.append(task.message_id)
            return False

        q = _queue(redis, max_schema_version=1, dead_letter_guard=refuse)
        q.ensure_group()
        mid = redis.xadd(
            STREAM, {PAYLOAD_FIELD: json.dumps(BODY), SCHEMA_FIELD: "2"}
        )

        assert q.read(block_ms=50) is None

        assert calls == [mid]
        assert redis.xlen(q.dlq_key) == 0, "守卫拒绝不得死信"
        assert _pending_count(redis) == 1, "消息留 pending 等新 build 回收"
        assert q.stats()["schema_rejected"] == 1
        assert q.stats()["dlq_guard_skipped"] == 1
        assert q.stats()["dead_lettered"] == 0


class TestReclaimSchemaBoundary:
    """A44-01：崩溃窗口后的 reclaim 与首投同判，不按旧语义执行新消息。"""

    def test_reclaimed_over_version_message_is_not_served(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        mid = _seed_pending(redis, version="2")
        calls = []

        def guard(task):
            calls.append(task.message_id)
            return True

        q = _queue(
            redis, max_schema_version=1, claim_min_idle_ms=0,
            dead_letter_guard=guard,
        )

        assert q.read(block_ms=50) is None, (
            "A44-01：回收路径不得把超版本消息返回给消费循环（旧 build 真执行）"
        )

        assert calls == [mid], "回收路径与首投同判：也必须询问守卫"
        envelopes = _dlq_envelopes(redis, q)
        assert len(envelopes) == 1
        assert envelopes[0]["schema_version"] == 2
        assert "unsupported schema_version=2" in envelopes[0]["reason"]
        assert _pending_count(redis) == 0
        assert q.stats()["schema_rejected"] == 1
        assert q.stats()["dead_lettered"] == 1

    def test_reclaim_guard_refusal_keeps_message_pending(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        mid = _seed_pending(redis, version="2")

        q = _queue(
            redis, max_schema_version=1, claim_min_idle_ms=0,
            dead_letter_guard=lambda task: False,
        )

        assert q.read(block_ms=50) is None
        assert redis.xlen(q.dlq_key) == 0
        assert _pending_count(redis) == 1
        assert q.stats()["schema_rejected"] == 1
        assert q.stats()["dlq_guard_skipped"] == 1

    def test_reclaim_same_version_is_still_served(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        mid = _seed_pending(redis, version="1")
        calls = []

        q = _queue(
            redis, max_schema_version=1, claim_min_idle_ms=0,
            dead_letter_guard=lambda task: calls.append(task.message_id) or True,
        )

        task = q.read(block_ms=50)

        assert task is not None and task.message_id == mid
        assert task.schema_version == 1
        assert q.stats().get("schema_rejected", 0) == 0
        assert calls == [], "版本兼容的回收不应进入死信判定"
        q.ack(task.message_id)

    def test_reclaim_missing_envelope_is_legacy_v1_and_served(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        mid = _seed_pending(redis, version=None)

        q = _queue(redis, max_schema_version=1, claim_min_idle_ms=0)

        task = q.read(block_ms=50)
        assert task is not None and task.message_id == mid
        assert task.schema_version == tq.LEGACY_TASK_SCHEMA_VERSION == 1


class TestInvalidPayloadGuarded:
    """B44-05：畸形 payload 的第二旁路同样走守卫（DLQ 保留信封版本）。"""

    def test_invalid_payload_consults_guard(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        calls = []

        def guard(task):
            calls.append((task.message_id, task.schema_version))
            return True

        q = _queue(redis, dead_letter_guard=guard)
        q.ensure_group()
        mid = redis.xadd(
            STREAM, {PAYLOAD_FIELD: "{not-json", SCHEMA_FIELD: "1"}
        )

        assert q.read(block_ms=50) is None

        assert calls == [(mid, 1)], "畸形 payload 死信前也必须询问守卫"
        envelopes = _dlq_envelopes(redis, q)
        assert len(envelopes) == 1
        assert envelopes[0]["schema_version"] == 1
        assert envelopes[0]["raw_fields"][PAYLOAD_FIELD] == "{not-json"
        assert q.stats()["invalid_payload"] == 1
        assert q.stats()["dead_lettered"] == 1

    def test_invalid_payload_guard_refusal_keeps_pending(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        q = _queue(redis, dead_letter_guard=lambda task: False)
        q.ensure_group()
        redis.xadd(STREAM, {PAYLOAD_FIELD: "{not-json"})

        assert q.read(block_ms=50) is None

        assert redis.xlen(q.dlq_key) == 0
        assert _pending_count(redis) == 1
        assert q.stats()["invalid_payload"] == 1
        assert q.stats()["dlq_guard_skipped"] == 1


class TestEnvelopeWriter:
    """A44-02 写入口：新消息写当前版本，重投副本可显式继承源版本。"""

    def test_enqueue_task_defaults_to_current_and_accepts_source_version(self, monkeypatch):
        monkeypatch.setattr(tq, "TASK_SCHEMA_VERSION", 2)
        redis = fakeredis.FakeRedis(decode_responses=True)

        enqueue_task(redis, STREAM, BODY)
        enqueue_task(redis, STREAM, BODY, schema_version=1)

        versions = [_envelope_version(fields) for _mid, fields in redis.xrange(STREAM)]
        assert versions == ["2", "1"]

    def test_queue_enqueue_passes_source_version_through(self, monkeypatch):
        monkeypatch.setattr(tq, "TASK_SCHEMA_VERSION", 2)
        redis = fakeredis.FakeRedis(decode_responses=True)
        q = _queue(redis)

        q.enqueue(BODY)
        q.enqueue(BODY, schema_version=1)

        versions = [_envelope_version(fields) for _mid, fields in redis.xrange(STREAM)]
        assert versions == ["2", "1"]


class _CaptureQueue:
    def __init__(self):
        self.enqueued = []
        self.acked = []

    def enqueue(self, body, **kwargs):
        self.enqueued.append((dict(body), kwargs))
        return "9-0"

    def ack(self, message_id):
        self.acked.append(message_id)


def _task(schema_version=1, body=None, mid="1-0"):
    return StreamTask(
        message_id=mid,
        body=dict(body or BODY),
        delivery_count=2,
        schema_version=schema_version,
    )


def _worker(redis, storage=None):
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=STREAM,
        execution_storage=storage if storage is not None else MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        redis_client=redis,
        enable_registry=False,
        enable_redis_logging=False,
        claim_min_idle_ms=1,
        read_block_ms=100,
    )


def _running_state(execution_id="exec-1"):
    return ExecutionState(
        execution_id=execution_id,
        flow_id="f1",
        flow_version="v1",
        status="running",
        context={"$LAST_NODE": "start", "$NODE": {"start": {}}},
    )


class TestRequeueInheritance:
    """A44-02：worker 的每条重投/交接/归一化副本继承源信封版本。"""

    @pytest.fixture(autouse=True)
    def _new_build_writer(self, monkeypatch):
        """模拟「本进程是新 build」：不继承源版本的副本会落 v2。"""
        monkeypatch.setattr(tq, "TASK_SCHEMA_VERSION", 2)

    def test_retryable_requeue(self):
        worker = RedisFlowWorker.__new__(RedisFlowWorker)
        worker.execution_storage = MemoryExecutionStorage()
        queue = _CaptureQueue()

        assert worker._requeue_retryable_failure(_task(), queue) is True

        body, kwargs = queue.enqueued[0]
        assert body["type"] == "resume"
        assert kwargs == {"schema_version": 1}
        assert queue.acked == ["1-0"]

    def test_quota_defer_requeue(self):
        worker = RedisFlowWorker.__new__(RedisFlowWorker)
        worker.TENANT_QUOTA_DEFER_MAX = 5
        queue = _CaptureQueue()

        assert worker._defer_over_quota_start(_task(), queue, RuntimeError("quota")) is True

        body, kwargs = queue.enqueued[0]
        assert body["quota_defers"] == 1
        assert kwargs == {"schema_version": 1}

    def test_affinity_handover_requeue(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        worker = RedisFlowWorker.__new__(RedisFlowWorker)
        worker.redis_client = redis
        worker.queue_name = STREAM
        queue = _CaptureQueue()

        task = _task(body={
            "type": "start", "flow_id": "f1", "execution_id": "exec-h",
            "params": {"repo": "/nonexistent/path"},
        })
        worker._handover_non_affine(task, queue, RuntimeError("no affinity"))

        entries = redis.xrange(STREAM)
        assert len(entries) == 1
        assert _envelope_version(entries[0][1]) == "1"
        assert queue.acked == ["1-0"]

    def test_start_dedup_requeue(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(redis, storage)
        storage.save_execution_state("exec-1", _running_state())

        result = worker._handle_start_dedup_hit(
            "f1", "exec-1", params={"order_id": "O-1"}, schema_version=1
        )

        assert result is not None and result["resume_requeued"] is True
        entries = redis.xrange(STREAM)
        assert len(entries) == 1
        assert _envelope_version(entries[0][1]) == "1"
        requeued = json.loads(entries[0][1][PAYLOAD_FIELD])
        assert requeued["type"] == "resume"
        assert requeued["resume_type"] == "continue"
        assert requeued["params"] == {"order_id": "O-1"}

    def test_dead_letter_guard_recovery_requeue(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        worker = _worker(redis, storage)
        storage.save_execution_state("exec-1", _running_state())

        task = StreamTask(
            message_id="1-0",
            body={
                "type": "resume", "flow_id": "f1", "execution_id": "exec-1",
                "resume_type": "continue", "tenant_id": "default",
            },
            delivery_count=5,
            schema_version=1,
        )

        assert worker._dead_letter_guard(task) is True

        entries = redis.xrange(STREAM)
        assert len(entries) == 1
        assert _envelope_version(entries[0][1]) == "1"

    def test_resume_protocol_normalization_requeue(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        flows = MemoryFlowStorage()
        flows.save_flow({
            "flow_id": "f1",
            "version": "v1",
            "nodes": [
                {"id": "start", "type": "start", "next": "end"},
                {"id": "end", "type": "end"},
            ],
        })
        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/15",
            queue_name=STREAM,
            execution_storage=storage,
            flow_storage=flows,
            redis_client=redis,
            enable_registry=False,
            enable_redis_logging=False,
        )
        storage.save_execution_state("exec-1", _running_state())
        queue = _CaptureQueue()

        task = StreamTask(
            message_id="1-0",
            body={
                "type": "resume", "flow_id": "f1", "execution_id": "exec-1",
                "resume_type": "event", "tenant_id": "default",
            },
            delivery_count=2,
            schema_version=1,
        )

        assert worker._handle_resume_protocol_error(
            task, queue, ResumeProtocolError("stranded", execution_id="exec-1")
        ) is True

        body, kwargs = queue.enqueued[0]
        assert body["resume_type"] == "continue"
        assert kwargs == {"schema_version": 1}
        assert queue.acked == ["1-0"]


class TestConsumeLoopPlumbing:
    """派发侧把源版本交给 `_dispatch_task`（start 幂等命中的载体转换用）。"""

    class _LoopHost:
        _consume_loop = RedisFlowWorker._consume_loop
        read_block_ms = 1

        def __init__(self):
            self._running = True
            self._drain_event = threading.Event()
            self.seen = []

        def _disk_guard_blocks_claim(self):
            return False

        def _wait_disk_guard(self):
            pass

        def _sweep_residue_if_due(self, queue):
            pass

        def _bump_active(self, delta):
            pass

        def _dispatch_task(self, body, delivery_count=None, schema_version=None):
            self.seen.append((body["execution_id"], delivery_count, schema_version))
            self._running = False

    def test_source_schema_version_reaches_dispatch(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        q = _queue(redis)
        q.ensure_group()
        q.enqueue(BODY, schema_version=1)

        host = self._LoopHost()
        host._consume_loop(q)

        assert host.seen == [("exec-1", 1, 1)]
