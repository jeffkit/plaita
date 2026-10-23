"""A53-S2（duo-a 第五十三轮）：DLQ replay 幂等 + schema 继承 + 审计。

## 缺陷

baseline 的 runbook「回灌」就是裸 ``enqueue_task``：

- 同一个 ``dlq_id`` 可无限回灌；无 ``execution_id`` 的旧 start 载体回灌两次 =
  两条真实执行（r53 探针：``executions=2 statuses=['completed','completed']``）；
- ``enqueue_task`` 默认写当前 build 的 schema 版本：v2 载体在 v1 build 上按 v1
  语义硬跑；继承 v2 又会被消费侧拒收再进 DLQ（A53-03）；
- 没有任何一次性标记/审计，运维无法区分「已受理」与「重复回灌」。

## 修复契约（本文件钉住）

1. ``(dlq_key, dlq_id)`` 一次性标记：第二次 replay 返回 ``already_replayed``，
   队列只 +1；
2. 无 ``execution_id`` 的 start 铸造确定性 id（uuid5），并补 ``dedup_key``；
   force 重灌同一死信仍收敛到同一条执行；
3. ``schema_version`` 继承源信封；缺失 = LEGACY(1)；超本 build → 入队前
   ``schema_too_new``；
4. 每次尝试写 ``plaita:dlq:replay:audit``，DLQ 原条目保留。

## 负控（还原修复后必须精确红）

- 去掉 ``SET NX`` 标记：``test_second_replay_is_named_rejection`` /
  ``test_double_replay_yields_single_execution`` 看到队列 2 条、执行 2 条；
- 去掉确定性 id 铸造：无 id 载体消息缺 ``execution_id``，双投即两条执行；
- 回报当前 build 版本：``test_schema_inherited_and_missing_uses_legacy`` 红。
"""
from __future__ import annotations

import json

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.dlq_replay import (
    REPLAY_AUDIT_STREAM,
    deterministic_execution_id,
    marker_key,
    replay_dead_letter,
)
from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import enqueue_task
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

DLQ = "plaita:flow:queue:v2:dlq"
TARGET = "plaita:flow:queue:v2"

TEST_FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}


def _fake():
    return fakeredis.FakeRedis(decode_responses=True)


def _seed(
    redis,
    *,
    execution_id="exec-1",
    schema_version=1,
    tenant="acme",
    task_type="start",
    dead_lettered_at=1700000000.0,
    truncated=None,
    payload_override=None,
):
    payload = {
        "type": task_type,
        "flow_id": "f1",
        "version": "1",
        "tenant_id": tenant,
        "params": {"x": 1},
    }
    if execution_id is not None:
        payload["execution_id"] = execution_id
    envelope = {
        "reason": "max_deliveries=5",
        "source_stream": TARGET,
        "source_id": "1699-0",
        "delivery_count": 5,
        "dead_lettered_at": dead_lettered_at,
        "payload": payload if payload_override is None else payload_override,
    }
    if schema_version is not None:
        envelope["schema_version"] = schema_version
    if truncated is not None:
        envelope["truncated"] = truncated
    return redis.xadd(DLQ, {"payload": json.dumps(envelope)})


def _replay(redis, dlq_id, **kwargs):
    return replay_dead_letter(
        redis,
        dlq_key=DLQ,
        dlq_id=dlq_id,
        target_stream=TARGET,
        actor="operator",
        **kwargs,
    )


def _target_messages(redis):
    return [
        (msg_id, json.loads(fields["payload"]), fields)
        for msg_id, fields in redis.xrange(TARGET)
    ]


def _audit_rows(redis):
    return [fields for _, fields in redis.xrange(REPLAY_AUDIT_STREAM)]


def _worker(redis):
    storage = MemoryExecutionStorage()
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow(TEST_FLOW)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/0",
        queue_name=TARGET,
        execution_storage=storage,
        flow_storage=flow_storage,
        redis_client=redis,
        enable_registry=False,
        enable_redis_logging=False,
    )
    return worker, storage


class TestReplayIdempotency:
    def test_replay_once_enqueues_and_audits(self):
        redis = _fake()
        dlq_id = _seed(redis)
        result = _replay(redis, dlq_id)
        assert result.replayed is True and result.reason == "ok"
        assert result.execution_id == "exec-1"
        assert result.tenant_id == "acme"
        assert result.schema_version == 1
        messages = _target_messages(redis)
        assert len(messages) == 1
        assert messages[0][1]["execution_id"] == "exec-1"
        rows = _audit_rows(redis)
        assert len(rows) == 1 and rows[0]["result"] == "ok"
        assert rows[0]["actor"] == "operator"
        assert rows[0]["dlq_id"] == dlq_id
        assert rows[0]["target_message_id"] == result.target_message_id
        # DLQ 条目保留（证据不删）。
        assert redis.xlen(DLQ) == 1

    def test_second_replay_is_named_rejection(self):
        redis = _fake()
        dlq_id = _seed(redis)
        first = _replay(redis, dlq_id)
        second = _replay(redis, dlq_id)
        assert second.replayed is False
        assert second.reason == "already_replayed"
        assert second.target_message_id == first.target_message_id
        assert len(_target_messages(redis)) == 1, "第二次 replay 不得再入队"
        results = [row["result"] for row in _audit_rows(redis)]
        assert results == ["ok", "already_replayed"]
        assert redis.exists(marker_key(DLQ, dlq_id)) == 1

    def test_force_replay_reuses_same_execution_id(self):
        redis = _fake()
        dlq_id = _seed(redis, execution_id=None)
        first = _replay(redis, dlq_id)
        forced = _replay(redis, dlq_id, force=True)
        assert first.replayed and forced.replayed
        assert forced.execution_id == first.execution_id
        messages = _target_messages(redis)
        assert len(messages) == 2
        assert {msg[1]["execution_id"] for msg in messages} == {first.execution_id}
        marker = json.loads(redis.get(marker_key(DLQ, dlq_id)))
        assert marker["target_message_id"] == forced.target_message_id
        assert [row["result"] for row in _audit_rows(redis)] == ["ok", "ok"]


class TestLegacyCarrierExecutionId:
    def test_no_execution_id_mints_deterministic_id_and_dedup_key(self):
        redis = _fake()
        dlq_id = _seed(redis, execution_id=None)
        expected = deterministic_execution_id(DLQ, dlq_id)
        result = _replay(redis, dlq_id)
        assert result.replayed is True
        assert result.execution_id == expected
        body = _target_messages(redis)[0][1]
        assert body["execution_id"] == expected
        assert body["dedup_key"] == f"dlq-replay:{expected}"
        # 同一死信重复铸造必须是同一个 id（确定性的定义）。
        assert deterministic_execution_id(DLQ, dlq_id) == expected

    def test_explicit_id_fills_missing_id(self):
        redis = _fake()
        dlq_id = _seed(redis, execution_id=None)
        result = _replay(redis, dlq_id, execution_id="manual-exec")
        assert result.replayed is True
        assert result.execution_id == "manual-exec"
        body = _target_messages(redis)[0][1]
        assert body["execution_id"] == "manual-exec"
        # 显式补 id 是操作者输入，不补 dedup_key（避免把它伪装成确定性载体）。
        assert "dedup_key" not in body

    def test_explicit_id_conflicting_with_payload_is_rejected(self):
        redis = _fake()
        dlq_id = _seed(redis, execution_id="exec-1")
        result = _replay(redis, dlq_id, execution_id="exec-2")
        assert result.replayed is False
        assert result.reason == "execution_id_mismatch"
        assert _target_messages(redis) == []

    def test_resume_without_execution_id_is_not_replayable(self):
        redis = _fake()
        dlq_id = _seed(redis, execution_id=None, task_type="resume")
        result = _replay(redis, dlq_id)
        assert result.replayed is False
        assert result.reason == "not_replayable"
        assert "no execution_id" in (result.detail or "")
        assert _target_messages(redis) == []


class TestSchemaVersionInheritance:
    def test_schema_inherited_and_missing_uses_legacy(self):
        redis = _fake()
        v2_id = _seed(redis, schema_version=2)
        legacy_id = _seed(redis, schema_version=None)
        assert _replay(redis, v2_id, max_schema_version=2).schema_version == 2
        assert _replay(redis, legacy_id).schema_version == 1
        messages = _target_messages(redis)
        assert [fields["schema_version"] for _, _, fields in messages] == ["2", "1"]

    def test_schema_too_new_is_rejected_before_enqueue(self):
        redis = _fake()
        dlq_id = _seed(redis, schema_version=2)
        result = _replay(redis, dlq_id, max_schema_version=1)
        assert result.replayed is False
        assert result.reason == "schema_too_new"
        assert result.schema_version == 2
        assert _target_messages(redis) == []
        assert redis.exists(marker_key(DLQ, dlq_id)) == 0, "拒绝不得占用 replay 标记"
        assert _audit_rows(redis)[0]["result"] == "schema_too_new"


class TestUnreplayableShapes:
    def test_non_dict_payload_is_rejected(self):
        redis = _fake()
        dlq_id = _seed(redis, payload_override="truncated raw payload")
        result = _replay(redis, dlq_id)
        assert result.replayed is False and result.reason == "not_replayable"
        assert _target_messages(redis) == []

    def test_truncated_payload_is_rejected(self):
        redis = _fake()
        dlq_id = _seed(redis, truncated=True)
        result = _replay(redis, dlq_id)
        assert result.replayed is False and result.reason == "not_replayable"

    def test_missing_dlq_entry_is_rejected(self):
        redis = _fake()
        result = _replay(redis, "999-0")
        assert result.replayed is False and result.reason == "not_replayable"
        assert _target_messages(redis) == []

    def test_non_json_envelope_is_rejected(self):
        redis = _fake()
        dlq_id = redis.xadd(DLQ, {"payload": "not-json {broken"})
        result = _replay(redis, dlq_id)
        assert result.replayed is False and result.reason == "not_replayable"

    def test_execution_tombstone_is_rejected(self):
        redis = _fake()
        dlq_id = _seed(redis, execution_id="deleted-exec")
        redis.set("plaita:exec_tombstone:deleted-exec", "{}")
        result = _replay(redis, dlq_id)
        assert result.replayed is False and result.reason == "execution_deleted"
        assert _target_messages(redis) == []


class TestTenantAttribution:
    def test_missing_tenant_is_rejected_without_explicit_tenant(self):
        redis = _fake()
        dlq_id = _seed(redis, tenant=None)
        result = _replay(redis, dlq_id)
        assert result.replayed is False and result.reason == "not_replayable"
        assert "tenant_id" in (result.detail or "")

    def test_missing_tenant_can_be_supplied_explicitly(self):
        redis = _fake()
        dlq_id = _seed(redis, tenant=None)
        result = _replay(redis, dlq_id, tenant_id="acme")
        assert result.replayed is True and result.tenant_id == "acme"
        assert _target_messages(redis)[0][1]["tenant_id"] == "acme"

    def test_explicit_tenant_conflict_is_rejected(self):
        redis = _fake()
        dlq_id = _seed(redis, tenant="acme")
        result = _replay(redis, dlq_id, tenant_id="beta")
        assert result.replayed is False and result.reason == "tenant_mismatch"
        assert _target_messages(redis) == []


class TestSingleExecutionConvergence:
    def test_double_replay_yields_single_execution(self):
        """无 id 载体：双 replay + force 重灌，最终只有一条 completed 执行。"""
        redis = _fake()
        dlq_id = _seed(redis, execution_id=None)
        first = _replay(redis, dlq_id)
        second = _replay(redis, dlq_id)
        assert second.reason == "already_replayed"
        # force 再灌一条同 id 副本（模拟标记过期后的运维重试）。
        assert _replay(redis, dlq_id, force=True).execution_id == first.execution_id
        messages = _target_messages(redis)
        assert len(messages) == 2, "第三次 replay（非 force）不得再入队"

        worker, storage = _worker(redis)
        bodies = [body for _, body, _ in messages]
        out1 = worker.start_flow(
            bodies[0]["flow_id"],
            bodies[0].get("params"),
            version=bodies[0].get("version"),
            execution_id=bodies[0]["execution_id"],
            dedup_key=bodies[0].get("dedup_key"),
        )
        out2 = worker.start_flow(
            bodies[1]["flow_id"],
            bodies[1].get("params"),
            version=bodies[1].get("version"),
            execution_id=bodies[1]["execution_id"],
            dedup_key=bodies[1].get("dedup_key"),
        )
        assert out1["execution_id"] == out2["execution_id"] == first.execution_id
        assert out2.get("already_terminal") is True
        state = storage.load_execution_state(first.execution_id)
        assert state is not None and state.status == "completed"
        # 盘上只有一个执行 id（同一 id 的两次投递收敛）。
        assert set(storage.execution_states) == {first.execution_id}

    def test_baseline_manual_enqueue_twice_would_duplicate(self):
        """负控形态：裸 enqueue_task 两次（无 id 载体）产生两条独立消息。"""
        redis = _fake()
        body = {"type": "start", "flow_id": "f1", "params": {}, "tenant_id": "acme"}
        enqueue_task(redis, TARGET, dict(body), schema_version=1)
        enqueue_task(redis, TARGET, dict(body), schema_version=1)
        messages = _target_messages(redis)
        assert len(messages) == 2
        assert all("execution_id" not in msg[1] for msg in messages)
