"""plaita#186：畸形队列消息必须进 DLQ，而不是炸穿 worker 消费循环。

基线缺陷：``_task_from_fields`` 对「payload 非 JSON / JSON 非对象」直接
``raise ValueError``；该解析点在 ``queue.read()`` 内、位于 FlowWorker 消费
循环的内层 try **之外**，于是任意一条坏消息（生产者 bug / 手工 XADD /
升级期旧格式）都会让 worker 进程退出。消息未被 ack，留在 PEL 里由 reclaim
再次解析 ⇒ 崩溃/重启循环。

修复契约：在**解析点**把不可解析消息转成 DLQ 信封（``reason=invalid_payload``
+ 原始字段 + 截断标记）并 ack，``read()``/``_reclaim_one()`` 都不得抛出解析
异常；正常消息照常消费。
"""
from __future__ import annotations

import json
import threading

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import (
    INVALID_PAYLOAD_MAX_CHARS,
    PAYLOAD_FIELD,
    RedisStreamTaskQueue,
)

STREAM = "plaita:flow:queue:t186"
GROUP = "g"


def _queue(redis, **kwargs):
    kwargs.setdefault("claim_min_idle_ms", 60_000)
    return RedisStreamTaskQueue(redis, STREAM, group_name=GROUP, consumer_name="c1", **kwargs)


def _dlq_envelopes(redis, q):
    return [json.loads(fields[PAYLOAD_FIELD]) for _id, fields in redis.xrange(q.dlq_key)]


class _LoopWorker:
    """最小 consume-loop 宿主：只实现 ``_consume_loop`` 需要的接口。"""

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
        self.seen.append(body)
        # 正常消息处理完就停循环，避免依赖 block 超时。
        self._running = False


class TestInvalidPayloadReadPath:
    def test_non_json_payload_is_dead_lettered_not_raised(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        q = _queue(redis)
        q.ensure_group()
        bad_id = redis.xadd(STREAM, {PAYLOAD_FIELD: "{not-json"})

        assert q.read(block_ms=1) is None, "解析失败必须被队列层吃掉，不能抛给 worker"

        envelopes = _dlq_envelopes(redis, q)
        assert len(envelopes) == 1
        env = envelopes[0]
        assert env["reason"].startswith("invalid_payload")
        assert env["source_stream"] == STREAM
        assert env["source_id"] == bad_id
        assert env["raw_fields"][PAYLOAD_FIELD] == "{not-json"
        assert env["truncated"] is False
        # 已 ack（XACK + XDEL）：既不在 PEL，也不在 stream 里
        assert int(redis.xpending(STREAM, GROUP)["pending"]) == 0
        assert redis.xlen(STREAM) == 0
        assert q.stats()["invalid_payload"] == 1

    def test_non_object_json_payload_is_dead_lettered(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        q = _queue(redis)
        q.ensure_group()
        redis.xadd(STREAM, {PAYLOAD_FIELD: "[1, 2, 3]"})

        assert q.read(block_ms=1) is None
        env = _dlq_envelopes(redis, q)[0]
        assert "must be a JSON object" in env["reason"]

    def test_oversized_payload_is_truncated_with_marker(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        q = _queue(redis)
        q.ensure_group()
        raw = "x" * (INVALID_PAYLOAD_MAX_CHARS + 700)
        redis.xadd(STREAM, {PAYLOAD_FIELD: raw})

        assert q.read(block_ms=1) is None
        env = _dlq_envelopes(redis, q)[0]
        assert env["truncated"] is True
        assert env["payload"] == raw[:INVALID_PAYLOAD_MAX_CHARS]
        assert len(env["raw_fields"][PAYLOAD_FIELD]) <= INVALID_PAYLOAD_MAX_CHARS

    def test_normal_message_after_bad_one_is_served(self):
        redis = fakeredis.FakeRedis(decode_responses=True)
        q = _queue(redis)
        q.ensure_group()
        redis.xadd(STREAM, {PAYLOAD_FIELD: "{not-json"})
        normal = {"type": "start", "flow_id": "f1"}
        q.enqueue(normal)

        assert q.read(block_ms=1) is None  # 坏消息先被处置
        task = q.read(block_ms=1)
        assert task is not None and task.body == normal
        q.ack(task.message_id)
        assert redis.xlen(STREAM) == 0

    def test_consume_loop_survives_bad_message_and_processes_next(self):
        """验收本体：一条坏消息不得让消费循环退出，后续正常消息照常处理。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        q = _queue(redis)
        q.ensure_group()
        redis.xadd(STREAM, {PAYLOAD_FIELD: "{not-json"})
        normal = {"type": "start", "flow_id": "f1"}
        q.enqueue(normal)

        worker = _LoopWorker()
        RedisFlowWorker._consume_loop(worker, q)  # 修复前：这里抛 ValueError

        assert worker.seen == [normal], "坏消息之后的正常消息没被处理"
        assert len(_dlq_envelopes(redis, q)) == 1
        assert redis.xlen(STREAM) == 0


class TestInvalidPayloadReclaimPath:
    def test_reclaim_dead_letters_bad_message_and_continues(self):
        """坏消息已在别人 PEL 里（idle 超阈）时，reclaim 也不得抛。"""
        redis = fakeredis.FakeRedis(decode_responses=True)
        writer = RedisStreamTaskQueue(
            redis, STREAM, group_name=GROUP, consumer_name="dead", claim_min_idle_ms=0
        )
        writer.ensure_group()
        bad_id = redis.xadd(STREAM, {PAYLOAD_FIELD: "[1,2,3]"})
        redis.xreadgroup(GROUP, "dead", {STREAM: ">"}, count=1)

        reader = RedisStreamTaskQueue(
            redis, STREAM, group_name=GROUP, consumer_name="c1", claim_min_idle_ms=0
        )
        assert reader._reclaim_one() is None, "坏消息必须就地进 DLQ，不得抛回消费循环"
        assert redis.xlen(reader.dlq_key) == 1
        assert int(redis.xpending(STREAM, GROUP)["pending"]) == 0
        env = _dlq_envelopes(redis, reader)[0]
        assert env["source_id"] == bad_id
