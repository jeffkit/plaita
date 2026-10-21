"""B108-01 (r62): start-dedup park 超时有界清扫。

park 保留原 id 依赖同 key/同 due 重试或 worker 终态自然收敛；无人重试时
queued 行/map/token/配额没有最终清收。ParkSweeper 只扫 park 标记，超时
（默认 claiming TTL）后：行 queued/缺失 ⇒ 墓碑+删行+清认领+回配额；
running/终态 ⇒ 只删标记（执行已接管）；未超时不动。
"""
from __future__ import annotations

import time

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")

import fakeredis

from plaita.server import start_dedup as sd
from plaita.server.start_dedup_sweeper import (
    park_sweep_metrics,
    reset_park_sweep_metrics,
    sweep_parks,
)
from plaita.server.tenant_quota import (
    publish_tenant_quota,
    tenant_running_count,
    try_admit_tenant_start,
)
from plaita.storage.base import ExecutionState
from plaita.storage.redis import RedisExecutionStorage
from plaita.tenant_context import tenant_namespace

TENANT = "acme"
KEY = "park-key"
NS = tenant_namespace(TENANT)


def _redis():
    redis = fakeredis.FakeRedis(decode_responses=True)
    publish_tenant_quota(redis, TENANT, 5)
    return redis


def _park(redis, *, status="queued", parked_at=None, row=True, key=KEY, eid="a" * 32):
    claim = sd.claim(redis, TENANT, key, eid)
    assert claim.claimed and claim.token
    storage = RedisExecutionStorage(client=redis, namespace=NS)
    if row:
        storage.save_execution_state(
            eid,
            ExecutionState(
                execution_id=eid, flow_id="f", status=status, context={},
                tenant_id=TENANT,
            ),
        )
    assert try_admit_tenant_start(redis, TENANT, eid)
    sd.mark_parked(
        redis, TENANT, key, eid,
        parked_at=time.time() if parked_at is None else parked_at,
    )
    return eid


def test_expired_park_is_cleaned_and_quota_returned():
    reset_park_sweep_metrics()
    redis = _redis()
    eid = _park(redis, parked_at=time.time() - 120)
    counts = sweep_parks(redis, now=time.time(), timeout_seconds=60)
    assert counts["cleaned"] == 1
    assert sd.read_mapped(redis, TENANT, KEY) is None
    assert redis.get(sd.token_key(TENANT, KEY)) is None
    assert redis.get(sd.owner_key(TENANT, KEY)) is None
    assert redis.get(sd.execution_row_key(TENANT, eid)) is None
    assert redis.exists(sd.tombstone_key(eid))
    assert redis.get(sd.park_key(TENANT, KEY)) is None
    assert tenant_running_count(redis, TENANT) == 0
    assert park_sweep_metrics()["start_dedup_park_sweep_cleaned_total"] == 1


def test_fresh_park_is_not_touched():
    reset_park_sweep_metrics()
    redis = _redis()
    now = time.time()
    eid = _park(redis, parked_at=now)
    counts = sweep_parks(redis, now=now + 10, timeout_seconds=60)
    assert counts["skipped"] == 1 and counts["cleaned"] == 0
    assert sd.read_mapped(redis, TENANT, KEY) == eid
    assert redis.get(sd.execution_row_key(TENANT, eid)) is not None
    assert tenant_running_count(redis, TENANT) == 1


def test_running_or_terminal_park_only_clears_marker():
    reset_park_sweep_metrics()
    redis = _redis()
    eid = _park(redis, status="running", parked_at=time.time() - 120)
    counts = sweep_parks(redis, now=time.time(), timeout_seconds=60)
    assert counts["skipped"] == 1
    assert sd.read_mapped(redis, TENANT, KEY) == eid
    assert redis.get(sd.execution_row_key(TENANT, eid)) is not None
    assert redis.get(sd.park_key(TENANT, KEY)) is None
    assert tenant_running_count(redis, TENANT) == 1

    eid2 = _park(
        redis, status="completed", parked_at=time.time() - 120,
        key="park-key-2", eid="b" * 32,
    )
    counts2 = sweep_parks(redis, now=time.time(), timeout_seconds=60)
    assert counts2["skipped"] == 1
    assert sd.read_mapped(redis, TENANT, "park-key-2") == eid2


def test_row_missing_park_clears_claim_and_quota():
    reset_park_sweep_metrics()
    redis = _redis()
    eid = _park(redis, row=False, parked_at=time.time() - 120)
    counts = sweep_parks(redis, now=time.time(), timeout_seconds=60)
    assert counts["cleaned"] == 1
    assert sd.read_mapped(redis, TENANT, KEY) is None
    assert redis.get(sd.token_key(TENANT, KEY)) is None
    assert tenant_running_count(redis, TENANT) == 0
    assert redis.exists(sd.tombstone_key(eid))
