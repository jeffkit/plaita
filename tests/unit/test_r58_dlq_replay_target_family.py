"""r58 a/P2：DLQ replay 必须校验目标 stream 的 config_family。

缺陷：replay 把源 family F1 的死信灌进目标 family F2 的 stream，仍返回
``replayed=True/ok`` 并写 marker + 目标 Stream；目标 worker 的 family gate
拒收并留 pending，没有执行——「replayed ok」是假成功。

修复契约：在源 ``config_envelope`` 构建后、marker/enqueue 前解析目标
stream 的生效 family（默认按目标 stream + 生效 group/DLQ；自定义部署可显式
传 ``target_config_family``）。不一致 ⇒ ``target_family_mismatch``：不写
marker、目标 XLEN 不变、源 DLQ 保留、审计记 rejection。legacy 源默认拒绝；
管理员显式 ``allow_family_mismatch=True`` 才可跨 family（仍 WARNING/审计）。
"""
from __future__ import annotations

import json

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")

import fakeredis

from plaita.runtime_config import envelope_for_stream
from plaita.server.dlq_replay import (
    REPLAY_AUDIT_STREAM,
    marker_key,
    replay_dead_letter,
)
from plaita.server.task_queue import RedisStreamTaskQueue

SOURCE = "r58:source-queue"
TARGET = "r58:target-queue"
SOURCE_DLQ = SOURCE + ":dlq"
TENANT = "default"


def _family(stream: str) -> str:
    return envelope_for_stream(stream, role="console")["config_family"]


def _seed(redis, *, family: str | None, execution_id: str = "e-r58"):
    envelope = {
        "reason": "max_deliveries=5",
        "source_stream": SOURCE,
        "source_id": "1-0",
        "delivery_count": 5,
        "dead_lettered_at": 1700000000.0,
        "schema_version": 1,
        "payload": {
            "type": "start",
            "flow_id": "f1",
            "version": "1",
            "tenant_id": TENANT,
            "execution_id": execution_id,
            "params": {"x": 1},
        },
    }
    if family is not None:
        envelope["config_family"] = family
    return redis.xadd(SOURCE_DLQ, {"payload": json.dumps(envelope)})


def _audit_rows(redis):
    return [fields for _id, fields in redis.xrange(REPLAY_AUDIT_STREAM)]


def _queue_read(redis, stream: str, family: str):
    queue = RedisStreamTaskQueue(
        redis,
        stream,
        group_name="g",
        consumer_name="c",
        claim_min_idle_ms=0,
        config_family=family,
    )
    queue.ensure_group()
    return queue, queue.read(block_ms=1)


def test_cross_family_replay_rejected_before_marker():
    redis = fakeredis.FakeRedis(decode_responses=True)
    source_family = _family(SOURCE)
    target_family = _family(TARGET)
    assert source_family != target_family
    dlq_id = _seed(redis, family=source_family)

    res = replay_dead_letter(
        redis,
        dlq_key=SOURCE_DLQ,
        dlq_id=dlq_id,
        target_stream=TARGET,
        actor="operator",
    )

    assert res.replayed is False
    assert res.reason == "target_family_mismatch"
    assert source_family in (res.detail or "")
    assert target_family in (res.detail or "")
    assert redis.xlen(TARGET) == 0
    assert not redis.exists(marker_key(SOURCE_DLQ, dlq_id))
    assert redis.xlen(SOURCE_DLQ) == 1
    audit = _audit_rows(redis)
    assert audit and audit[-1]["result"] == "target_family_mismatch"

    queue, got = _queue_read(redis, TARGET, target_family)
    assert got is None
    assert queue.stats()["config_family_mismatch"] == 0
    assert redis.xpending_range(TARGET, "g", "-", "+", 10) == []

    # 正控：同一份死信回灌到同 family 的源 stream 成功（拒绝未烧 marker）。
    positive = replay_dead_letter(
        redis,
        dlq_key=SOURCE_DLQ,
        dlq_id=dlq_id,
        target_stream=SOURCE,
        actor="operator",
    )
    assert positive.replayed is True
    assert positive.reason == "ok"
    assert redis.xlen(SOURCE) == 1
    target_queue, got = _queue_read(redis, SOURCE, source_family)
    assert got is not None
    assert got.config_family == source_family
    assert _audit_rows(redis)[-1]["result"] == "ok"


def test_same_family_replay_succeeds_without_override():
    redis = fakeredis.FakeRedis(decode_responses=True)
    family = _family(TARGET)
    dlq_id = _seed(redis, family=family)

    res = replay_dead_letter(
        redis,
        dlq_key=SOURCE_DLQ,
        dlq_id=dlq_id,
        target_stream=TARGET,
        actor="operator",
    )

    assert res.replayed is True, res
    assert redis.xlen(TARGET) == 1
    assert redis.exists(marker_key(SOURCE_DLQ, dlq_id))
    assert _audit_rows(redis)[-1]["result"] == "ok"


def test_legacy_source_rejected_by_default_and_allows_explicit_override():
    redis = fakeredis.FakeRedis(decode_responses=True)
    target_family = _family(TARGET)
    dlq_id = _seed(redis, family=None)  # legacy / missing

    rejected = replay_dead_letter(
        redis,
        dlq_key=SOURCE_DLQ,
        dlq_id=dlq_id,
        target_stream=TARGET,
        actor="operator",
    )
    assert rejected.replayed is False
    assert rejected.reason == "target_family_mismatch"
    assert redis.xlen(TARGET) == 0
    assert not redis.exists(marker_key(SOURCE_DLQ, dlq_id))

    overridden = replay_dead_letter(
        redis,
        dlq_key=SOURCE_DLQ,
        dlq_id=dlq_id,
        target_stream=TARGET,
        actor="operator",
        allow_family_mismatch=True,
    )
    assert overridden.replayed is True
    assert redis.xlen(TARGET) == 1
    assert redis.exists(marker_key(SOURCE_DLQ, dlq_id))
    assert _audit_rows(redis)[-1]["result"] == "ok"


def test_malformed_explicit_target_family_is_unknown_and_not_consumed():
    redis = fakeredis.FakeRedis(decode_responses=True)
    dlq_id = _seed(redis, family=_family(SOURCE))

    res = replay_dead_letter(
        redis,
        dlq_key=SOURCE_DLQ,
        dlq_id=dlq_id,
        target_stream=TARGET,
        actor="operator",
        target_config_family="not-a-family",
    )
    assert res.replayed is False
    assert res.reason == "target_family_unknown"
    assert redis.xlen(TARGET) == 0
    assert not redis.exists(marker_key(SOURCE_DLQ, dlq_id))
    assert redis.xlen(SOURCE_DLQ) == 1
    assert _audit_rows(redis)[-1]["result"] == "target_family_unknown"


def test_explicit_target_family_is_used_for_custom_group_dlq_deployments():
    """自定义 group/DLQ 时 console 无法推断 family：显式声明后按声明校验。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    source_family = _family(SOURCE)
    dlq_id = _seed(redis, family=source_family)

    res = replay_dead_letter(
        redis,
        dlq_key=SOURCE_DLQ,
        dlq_id=dlq_id,
        target_stream=TARGET,  # 默认解析得到另一 family
        actor="operator",
        target_config_family=source_family,
    )
    assert res.replayed is True
    assert redis.xlen(TARGET) == 1
    assert redis.exists(marker_key(SOURCE_DLQ, dlq_id))
