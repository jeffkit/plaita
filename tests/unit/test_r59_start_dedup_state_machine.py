"""r59：start dedup token 状态机 + schedule 预认领/配额收口（#286/#287 底座）。

Lua 原语（plaita/server/start_dedup.py）负责 claim/guard/reserve/mark/release
的原子转换；本文件钉住 token/row/墓碑复核、旧格式迁移、反向碰撞与 schedule
同 due 收敛/配额回滚。console 侧的端到端交错见
test_issue286_287_owner_state_machine.py。
"""
from __future__ import annotations

import json

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")

import fakeredis

from plaita.server import start_dedup as sd
from plaita.server.services import schedule_service
from plaita.server.tenant_quota import (
    publish_tenant_quota,
    release_tenant_start,
    tenant_running_count,
    try_admit_tenant_start,
)

TENANT = "acme"
QUEUE = "plaita:flow:queue"


def _redis():
    return fakeredis.FakeRedis(decode_responses=True)


def _queue_ids(redis):
    out = []
    for _id, fields in redis.xrange(QUEUE):
        raw = fields.get("payload")
        msg = json.loads(raw)
        if msg.get("type") == "start":
            out.append(msg["execution_id"])
    return out


def test_token_transitions_and_release_guards():
    redis = _redis()
    eid = "a" * 32
    claim = sd.claim(redis, TENANT, "k", eid)
    assert claim.claimed and claim.token

    assert sd.guard(redis, TENANT, "k", eid, claim.token)
    assert sd.reserve_enqueue(redis, TENANT, "k", eid, claim.token)
    redis.set(sd.execution_row_key(TENANT, eid), "{}")
    assert sd.mark_enqueued(redis, TENANT, "k", eid, claim.token)

    # 无 token 的孤儿释放：token 在位 / enqueued ⇒ 拒绝。
    assert sd.release(redis, TENANT, "k", eid, require_row_absent=True) is False
    # 认领者自己的释放：enqueued 终态 ⇒ 拒绝（不能把已入队认领删掉）。
    assert sd.release(redis, TENANT, "k", eid, token=claim.token) is False
    assert sd.read_mapped(redis, TENANT, "k") == eid

    # 墓碑后允许无 token 清映射（#285 force DELETE 回收）。
    redis.set(sd.tombstone_key(eid), "1")
    assert sd.release(redis, TENANT, "k", eid, require_row_absent=True) is True
    assert sd.read_mapped(redis, TENANT, "k") is None


def test_stale_release_after_owner_renewed_or_enqueued_cannot_delete():
    """#286：B 的旧 release 决定在 A 续期/入队后必须原子失败。"""
    redis = _redis()
    eid = "b" * 32
    claim = sd.claim(redis, TENANT, "k", eid)
    assert claim.claimed and claim.token
    assert sd.guard(redis, TENANT, "k", eid, claim.token)  # A 续期
    redis.set(sd.execution_row_key(TENANT, eid), "{}")     # A 落行
    assert sd.release(redis, TENANT, "k", eid, require_row_absent=True) is False
    assert sd.read_mapped(redis, TENANT, "k") == eid

    assert sd.reserve_enqueue(redis, TENANT, "k", eid, claim.token)
    assert sd.mark_enqueued(redis, TENANT, "k", eid, claim.token)
    assert sd.release(redis, TENANT, "k", eid, require_row_absent=True) is False
    owner = sd.read_owner(redis, TENANT, "k")
    assert owner.state == sd.STATE_ENQUEUED and owner.execution_id == eid


def test_legacy_claiming_owner_is_adopted_not_stolen():
    """#287：旧 `<map>:owner=claiming:A` 在新代码下是 live claim，不换 id。"""
    redis = _redis()
    old = "c" * 32
    redis.set(sd.map_key(TENANT, "mig"), old, ex=3600)
    redis.set(sd.legacy_owner_key(TENANT, "mig"), f"claiming:{old}", ex=60)

    claim = sd.claim(redis, TENANT, "mig", "d" * 32)
    assert claim.claimed is False
    assert claim.mapped == old
    assert claim.legacy is True and claim.token
    owner = sd.read_owner(redis, TENANT, "mig")
    assert owner.state == sd.STATE_CLAIMING and owner.execution_id == old
    # 新 token 在位：无 token 孤儿释放必须失败。
    assert sd.release(redis, TENANT, "mig", old, require_row_absent=True) is False


def test_legacy_enqueued_owner_is_authoritative():
    redis = _redis()
    old = "e" * 32
    redis.set(sd.map_key(TENANT, "mig2"), old, ex=3600)
    redis.set(sd.execution_row_key(TENANT, old), "{}")
    redis.set(sd.legacy_owner_key(TENANT, "mig2"), f"enqueued:{old}", ex=3600)

    claim = sd.claim(redis, TENANT, "mig2", "f" * 32)
    assert claim.claimed is False and claim.mapped == old
    owner = sd.read_owner(redis, TENANT, "mig2")
    assert owner.state == sd.STATE_ENQUEUED and owner.legacy is True


def test_reverse_collision_migrates_owner_then_claims():
    """`start-dedup:foo:owner` 既是 foo 的旧 owner 又是 foo:owner 的 map。"""
    redis = _redis()
    foo_id = "1" * 32
    redis.set(sd.map_key(TENANT, "foo"), foo_id, ex=3600)
    redis.set(sd.legacy_owner_key(TENANT, "foo"), f"claiming:{foo_id}", ex=60)
    assert redis.get(sd.map_key(TENANT, "foo:owner")) == f"claiming:{foo_id}"

    claim = sd.claim(redis, TENANT, "foo:owner", "2" * 32)
    assert claim.claimed is True and claim.token
    assert sd.read_mapped(redis, TENANT, "foo") == foo_id
    foo_owner = sd.read_owner(redis, TENANT, "foo")
    assert foo_owner.state == sd.STATE_CLAIMING and foo_owner.execution_id == foo_id
    assert sd.read_mapped(redis, TENANT, "foo:owner") == "2" * 32


def test_guard_rejects_wrong_token_for_same_id():
    redis = _redis()
    eid = "5" * 32
    claim = sd.claim(redis, TENANT, "k", eid)
    assert claim.claimed and claim.token
    assert sd.guard(redis, TENANT, "k", eid, "0" * 32) is False
    assert sd.guard(redis, TENANT, "k", eid, claim.token) is True


def test_mark_enqueued_rejects_wrong_token_same_id():
    redis = _redis()
    eid = "6" * 32
    claim = sd.claim(redis, TENANT, "k", eid)
    assert claim.claimed and claim.token
    assert sd.reserve_enqueue(redis, TENANT, "k", eid, claim.token)
    redis.set(sd.execution_row_key(TENANT, eid), "{}")
    assert sd.mark_enqueued(redis, TENANT, "k", eid, "0" * 32) is False
    assert sd.mark_enqueued(redis, TENANT, "k", eid, claim.token) is True


def test_owner_token_never_overwritten_by_stale_id():
    redis = _redis()
    eid = "3" * 32
    claim = sd.claim(redis, TENANT, "k", eid)
    assert claim.claimed and claim.token
    assert sd.reserve_enqueue(redis, TENANT, "k", eid, claim.token)
    assert sd.mark_enqueued(redis, TENANT, "k", eid, claim.token)
    assert sd.mark_enqueued(redis, TENANT, "k", "4" * 32, claim.token) is False
    owner = sd.read_owner(redis, TENANT, "k")
    assert owner.execution_id == eid and owner.state == sd.STATE_ENQUEUED


def _schedule(**overrides):
    base = {
        "schedule_id": "s-r59",
        "tenant_id": TENANT,
        "name": "n",
        "flow_id": "f",
        "cron": "* * * * *",
        "params": {"x": 1},
        "enabled": True,
        "next_run_at": "1700000000000",
    }
    base.update(overrides)
    return base


def test_schedule_same_due_single_id_single_carrier_and_quota():
    redis = _redis()
    publish_tenant_quota(redis, TENANT, 5)
    sched = _schedule()

    first = schedule_service.fire_schedule(redis, sched, QUEUE, due_at=1700000000000)
    second = schedule_service.fire_schedule(redis, sched, QUEUE, due_at=1700000000000)

    ids = _queue_ids(redis)
    assert len(ids) == 1, "同一 due 重触发不得再入队/铸新 id"
    assert first is not None
    assert second is None, "重触发不入队（fire_record 记既有 execution_id）"
    fires = [json.loads(r) for r in redis.lrange("plaita:schedule:fires:s-r59", 0, -1)]
    assert fires[0]["enqueue"] == "dedup"
    assert fires[0]["execution_id"] == ids[0]
    assert fires[1]["execution_id"] == ids[0]
    assert tenant_running_count(redis, TENANT) == 1
    owner = sd.read_owner(redis, TENANT, "sched:s-r59:due:1700000000000")
    assert owner.state == sd.STATE_ENQUEUED and owner.execution_id == ids[0]


def test_schedule_quota_full_releases_claim_for_next_due():
    redis = _redis()
    publish_tenant_quota(redis, TENANT, 1)
    assert try_admit_tenant_start(redis, TENANT, "occupier" * 4)
    sched = _schedule()

    assert schedule_service.fire_schedule(
        redis, sched, QUEUE, due_at=1700000000000
    ) is None
    key = "sched:s-r59:due:1700000000000"
    assert sd.read_mapped(redis, TENANT, key) is None, "配额失败必须释放预认领"
    assert redis.get(sd.token_key(TENANT, key)) is None
    assert tenant_running_count(redis, TENANT) == 1

    release_tenant_start(redis, TENANT, "occupier" * 4)
    again = schedule_service.fire_schedule(
        redis, sched, QUEUE, due_at=1700000000000
    )
    assert again is not None
    assert _queue_ids(redis) == [sd.read_mapped(redis, TENANT, key)]
    assert tenant_running_count(redis, TENANT) == 1


def test_schedule_enqueue_failure_releases_claim_and_quota(monkeypatch):
    redis = _redis()
    publish_tenant_quota(redis, TENANT, 5)
    sched = _schedule()
    key = "sched:s-r59:due:1700000000000"

    def boom(*args, **kwargs):
        raise RuntimeError("enqueue down")

    monkeypatch.setattr(schedule_service, "enqueue_task", boom)
    assert schedule_service.fire_schedule(
        redis, sched, QUEUE, due_at=1700000000000
    ) is None
    assert sd.read_mapped(redis, TENANT, key) is None
    assert redis.get(sd.token_key(TENANT, key)) is None
    assert tenant_running_count(redis, TENANT) == 0
    assert _queue_ids(redis) == []

    monkeypatch.undo()
    retry = schedule_service.fire_schedule(
        redis, sched, QUEUE, due_at=1700000000000
    )
    assert retry is not None
    assert len(_queue_ids(redis)) == 1
    assert tenant_running_count(redis, TENANT) == 1
