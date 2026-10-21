"""#289 (r61) schedule：mark 失败在 carrier 已被消费后不得 discard 铸新 id。

A carrier XADD 后、mark 前被 worker 消费并完成；token/owner 过期使 mark 失败。
旧 r60 路径 discard 行+墓碑+XDEL+释放 map，同 due 再 fire 铸 B（A 已完成 +
B 待执行）。新路径先原地恢复 mark；map 仍属 A 且无墓碑时 park（保留载体/
占位行/映射），同 due 收敛 A；只有映射换主/墓碑才清理。孤儿 row 存在时用
同 id 恢复而不是新 id。
"""
from __future__ import annotations

import json
import threading

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")

import fakeredis

from plaita.server import start_dedup as sd
from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.services import schedule_service
from plaita.server.tenant_quota import (
    publish_tenant_quota,
    tenant_running_count,
)
from plaita.storage.memory import MemoryFlowStorage
from plaita.storage.redis import RedisExecutionStorage
from plaita.tenant_context import set_current_tenant

TENANT = "default"
QUEUE = "q"
DUE = 1700000000000
KEY = f"sched:s289:due:{DUE}"
FLOW = {
    "flow_id": "f",
    "version": "1",
    "nodes": [
        {"id": "s", "type": "start", "next": "e"},
        {"id": "e", "type": "end", "output": "ok"},
    ],
}


def _redis():
    redis = fakeredis.FakeRedis(decode_responses=True)
    publish_tenant_quota(redis, TENANT, 10)
    return redis


def _schedule():
    return {
        "schedule_id": "s289",
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
    return [
        json.loads(fields["payload"])["execution_id"]
        for _id, fields in redis.xrange(QUEUE)
        if json.loads(fields["payload"]).get("type") == "start"
    ]


def _execute_a(redis, execution_id):
    fs = MemoryFlowStorage()
    fs.save_flow(FLOW)
    storage = RedisExecutionStorage(client=redis, namespace="plaita")
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=storage,
        flow_storage=fs,
        redis_client=redis,
        enable_registry=False,
        enable_redis_logging=False,
        lease_ttl_seconds=60,
    )
    set_current_tenant(TENANT, source="explicit")
    worker.start_flow(
        "f", {}, version="1", execution_id=execution_id, dedup_key=KEY
    )
    return storage


def test_schedule_mark_failure_after_consumed_converges_original_id(monkeypatch):
    redis = _redis()
    entered = threading.Event()
    release = threading.Event()
    first = {"done": False, "a": None}
    orig = sd.mark_enqueued

    def gated(redis_client, tenant, key, execution_id, token, **kwargs):
        if not first["done"]:
            first["done"] = True
            first["a"] = execution_id
            entered.set()
            assert release.wait(12)
        return orig(redis_client, tenant, key, execution_id, token, **kwargs)

    monkeypatch.setattr(sd, "mark_enqueued", gated)
    results = {}

    def do_fire():
        set_current_tenant(TENANT, source="explicit")
        results["first"] = _fire(redis)

    thread = threading.Thread(target=do_fire)
    thread.start()
    assert entered.wait(8)
    a_id = first["a"]
    storage = _execute_a(redis, a_id)
    assert storage.load_execution_state(a_id).status == "completed"

    redis.delete(sd.token_key(TENANT, KEY), sd.owner_key(TENANT, KEY))
    release.set()
    thread.join(12)

    assert results["first"] is not None
    assert sd.read_mapped(redis, TENANT, KEY) == a_id
    assert sd.read_owner(redis, TENANT, KEY).state == sd.STATE_ENQUEUED
    assert not redis.exists(sd.tombstone_key(a_id))
    assert redis.exists(sd.execution_row_key(TENANT, a_id))
    assert set(_queue_ids(redis)) == {a_id}, "只允许 A 的载体，不得铸 B"

    second = _fire(redis)
    assert second is None, "同 due 命中已入队 A，不再入队"
    assert set(_queue_ids(redis)) == {a_id}
    assert sd.read_mapped(redis, TENANT, KEY) == a_id


def test_schedule_mark_failure_parks_then_recovers_same_id(monkeypatch):
    redis = _redis()
    orig_mark = sd.mark_enqueued
    orig_recover = sd.recover_after_mark_failure
    monkeypatch.setattr(sd, "mark_enqueued", lambda *a, **k: False)
    monkeypatch.setattr(sd, "recover_after_mark_failure", lambda *a, **k: False)
    assert _fire(redis) is None

    a_id = sd.read_mapped(redis, TENANT, KEY)
    assert a_id
    assert redis.exists(sd.execution_row_key(TENANT, a_id)), "park 不得 discard 行"
    assert not redis.exists(sd.tombstone_key(a_id))
    assert tenant_running_count(redis, TENANT) == 1, "park 不得回滚配额"
    assert set(_queue_ids(redis)) == {a_id}

    # 模拟 claiming TTL 过期：同 due 的孤儿分支用同 id 恢复。
    redis.delete(sd.token_key(TENANT, KEY), sd.owner_key(TENANT, KEY))
    monkeypatch.setattr(sd, "mark_enqueued", orig_mark)
    monkeypatch.setattr(sd, "recover_after_mark_failure", orig_recover)
    recovered = _fire(redis)
    assert recovered is not None
    assert sd.read_mapped(redis, TENANT, KEY) == a_id
    assert sd.read_owner(redis, TENANT, KEY).state == sd.STATE_ENQUEUED
    assert set(_queue_ids(redis)) == {a_id}
