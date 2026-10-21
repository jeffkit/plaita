"""#288：schedule 触发必须与 console 同一条 reservation 状态机路径。

r59 不变量 6：XADD 前必有 `reserve_enqueue`。`fire_schedule` 原先
claim→配额→占位行→直接 enqueue_task，claim 在慢占位行窗口过期后仍 XADD，
mark 失败只 XDEL carrier，留下 queued 行 + map 7 天；同 due 再触发走孤儿
分支返回 None，不 adopt 不 release。

本文件钉住：
1. reservation 失败 ⇒ 不 XADD，配额/占位行/映射逐级回滚；
2. 慢占位行跨 claiming TTL ⇒ guard 同 token 续期后 reserve，仍成功；
3. 同 due 孤儿 ⇒ discard 行+释放映射后重试新 id，不静默吞掉；
4. mark 失败 ⇒ XDEL carrier + discard 行（墓碑）+ 释放映射 + 配额回滚；
5. `_MARK_ENQUEUED_LUA` 拒绝 claiming→enqueued（强制 reservation）。
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
    tenant_running_count,
)

TENANT = "acme"
QUEUE = "plaita:flow:queue"
DUE = 1700000000000
KEY = f"sched:s288:due:{DUE}"


def _redis():
    redis = fakeredis.FakeRedis(decode_responses=True)
    publish_tenant_quota(redis, TENANT, 5)
    return redis


def _schedule():
    return {
        "schedule_id": "s288",
        "tenant_id": TENANT,
        "name": "n",
        "flow_id": "f",
        "cron": "* * * * *",
        "params": {},
        "enabled": True,
        "next_run_at": str(DUE),
    }


def _fire(redis):
    return schedule_service.fire_schedule(
        redis, _schedule(), QUEUE, trigger_kind="cron", due_at=DUE
    )


def _queue_ids(redis):
    out = []
    for _id, fields in redis.xrange(QUEUE):
        msg = json.loads(fields["payload"])
        if msg.get("type") == "start":
            out.append(msg["execution_id"])
    return out


def _state(redis):
    return {
        "map": sd.read_mapped(redis, TENANT, KEY),
        "owner": sd.read_owner(redis, TENANT, KEY),
        "token": redis.get(sd.token_key(TENANT, KEY)),
        "quota": tenant_running_count(redis, TENANT),
        "queue": _queue_ids(redis),
    }


def _rows(redis):
    return [
        k for k in redis.keys(f"plaita:{TENANT}:execution:*")
        if "index" not in str(k)
    ]


def test_mark_enqueued_requires_reservation():
    """不变量 6：claiming 直接 mark 必须被拒（只能 enqueueing→enqueued）。"""
    redis = _redis()
    eid = "a" * 32
    claim = sd.claim(redis, TENANT, "k", eid)
    assert claim.claimed and claim.token
    redis.set(sd.execution_row_key(TENANT, eid), "{}")
    assert sd.mark_enqueued(redis, TENANT, "k", eid, claim.token) is False
    assert sd.reserve_enqueue(redis, TENANT, "k", eid, claim.token)
    assert sd.mark_enqueued(redis, TENANT, "k", eid, claim.token) is True


def test_schedule_reservation_failure_never_xadds(monkeypatch):
    redis = _redis()
    monkeypatch.setattr(sd, "reserve_enqueue", lambda *a, **k: False)
    assert _fire(redis) is None
    st = _state(redis)
    assert st["queue"] == [], "reservation 失败不得 XADD"
    assert st["map"] is None and st["token"] is None
    assert st["quota"] == 0
    assert _rows(redis) == [], "失败路径不得残留 queued 行"


def test_schedule_slow_placeholder_expiry_renews_then_reserves(monkeypatch):
    """A1 r106 最小复现：占位行后 owner/token 过期仍必须走 reservation 成功。"""
    redis = _redis()
    orig = schedule_service._write_queued_placeholder

    def slow_placeholder(redis_client, tenant_id, execution_id, *args, **kwargs):
        ok = orig(redis_client, tenant_id, execution_id, *args, **kwargs)
        redis_client.delete(
            sd.owner_key(tenant_id, KEY), sd.token_key(tenant_id, KEY)
        )
        return ok

    monkeypatch.setattr(
        schedule_service, "_write_queued_placeholder", slow_placeholder
    )
    msg_id = _fire(redis)
    assert msg_id is not None
    st = _state(redis)
    assert st["queue"] == [st["map"]]
    assert st["owner"].state == sd.STATE_ENQUEUED
    assert st["owner"].execution_id == st["map"]
    assert st["quota"] == 1
    assert len(_rows(redis)) == 1


def test_schedule_orphan_same_due_cleans_and_retries():
    redis = _redis()
    old = "b" * 32
    redis.set(sd.map_key(TENANT, KEY), old, ex=7 * 86400)
    redis.set(
        sd.execution_row_key(TENANT, old),
        json.dumps({"execution_id": old, "status": "queued"}),
    )

    msg_id = _fire(redis)
    assert msg_id is not None, "孤儿应清理后重试，而不是静默吞掉"
    st = _state(redis)
    assert st["map"] and st["map"] != old
    assert st["queue"] == [st["map"]]
    assert st["quota"] == 1
    assert redis.get(sd.execution_row_key(TENANT, old)) is None
    assert redis.exists(sd.tombstone_key(old))


def test_schedule_mark_failure_discards_row_and_map(monkeypatch):
    redis = _redis()
    monkeypatch.setattr(sd, "mark_enqueued", lambda *a, **k: False)
    assert _fire(redis) is None
    st = _state(redis)
    assert st["queue"] == [], "失去认领的 carrier 必须 XDEL"
    assert st["map"] is None and st["token"] is None
    assert st["quota"] == 0
    assert _rows(redis) == [], "mark 失败必须 discard queued 行"
