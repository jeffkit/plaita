"""A53-01（duo-a 第五十三轮）：DLQ 按租户排空（purge_tenant_dlq）。

``delete_tenant`` 只排空任务 Stream 时，平台级 DLQ 里的前代 payload 原样留存，
同名重建后新租户可读/可回灌（#264）。``purge_tenant_dlq`` 只删信封内
``payload.tenant_id`` 明确归属的条目；畸形/缺字段跳过并 WARNING，非 Stream
键同样跳过；Redis 错误上抛（失败 loud）。

负控：去掉 ``_dlq_entry_tenant`` 的显式租户判据（按 default 回退）→
``test_only_explicit_tenant_entries_are_deleted`` 误删他租户/不可归属条目而红。
"""
from __future__ import annotations

import json
import logging

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.task_queue import purge_tenant_dlq

STREAM = "plaita:flow:queue:trim-test"
DLQ = f"{STREAM}:dlq"


def _fake():
    return fakeredis.FakeRedis(decode_responses=True)


class TestPurgeTenantDlq:
    def _seed(self, redis, *, tenant=None, payload_override=None):
        envelope = {
            "reason": "max_deliveries=5",
            "source_stream": STREAM,
            "source_id": "1-0",
            "delivery_count": 5,
            "dead_lettered_at": 1700000000.0,
            "schema_version": 1,
            "payload": payload_override
            if payload_override is not None
            else {"type": "start", "flow_id": "f1", "tenant_id": tenant},
        }
        if tenant is None and payload_override is None:
            envelope["payload"].pop("tenant_id", None)
        return redis.xadd(DLQ, {"payload": json.dumps(envelope)})

    def test_only_explicit_tenant_entries_are_deleted(self, caplog):
        redis = _fake()
        acme = self._seed(redis, tenant="acme")
        acme2 = self._seed(redis, tenant="acme")
        beta = self._seed(redis, tenant="beta")
        no_tenant = self._seed(redis)
        raw_payload = self._seed(redis, payload_override="truncated raw")
        malformed = redis.xadd(DLQ, {"payload": "not-json {broken"})
        caplog.set_level(logging.WARNING, logger="plaita.server.task_queue")

        deleted = purge_tenant_dlq(redis, DLQ, "acme")
        assert deleted == 2
        remaining = {entry_id for entry_id, _ in redis.xrange(DLQ)}
        assert acme not in remaining and acme2 not in remaining
        assert remaining == {beta, no_tenant, raw_payload, malformed}
        # 无法归属条目必须 WARNING 计数（不得静默跳过）。
        assert any("无法归属" in r.message for r in caplog.records)

    def test_other_tenants_never_deleted(self):
        redis = _fake()
        beta = self._seed(redis, tenant="beta")
        assert purge_tenant_dlq(redis, DLQ, "acme") == 0
        assert {entry_id for entry_id, _ in redis.xrange(DLQ)} == {beta}

    def test_non_stream_key_is_skipped_with_warning(self, caplog):
        redis = _fake()
        redis.set("ops:deadletters:dlq", "not-a-stream")
        caplog.set_level(logging.WARNING, logger="plaita.server.task_queue")
        assert purge_tenant_dlq(redis, "ops:deadletters:dlq", "acme") == 0
        assert redis.get("ops:deadletters:dlq") == "not-a-stream"
        assert any("而非 Stream" in r.message for r in caplog.records)

    def test_bytes_fields_are_decoded(self):
        redis = fakeredis.FakeRedis(decode_responses=False)
        entry_id = redis.xadd(
            DLQ,
            {
                b"payload": json.dumps(
                    {"payload": {"type": "start", "tenant_id": "acme"}}
                ).encode()
            },
        )
        assert purge_tenant_dlq(redis, DLQ, "acme") == 1
        assert redis.xrange(DLQ) == []
        assert entry_id

    def test_empty_stream_and_missing_tenant(self):
        redis = _fake()
        assert purge_tenant_dlq(redis, DLQ, "acme") == 0
        assert purge_tenant_dlq(redis, DLQ, "") == 0
