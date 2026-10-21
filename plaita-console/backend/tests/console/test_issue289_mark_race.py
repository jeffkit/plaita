"""#289 (r61): mark_enqueued 失败时 carrier 可能已被消费——park-retry，不换 id。

console 路径：A 在 XADD 后、mark 前暂停；worker 消费并完成 A；随后 token/owner
过期使 mark 失败。旧实现 discard/墓碑/XDEL + 409，同 key 重试铸 B（A 已完成
+ B 待执行）。新实现先原地恢复 mark（guard→reserve→mark）；map 仍属本 id 且
无墓碑时只 park，重试收敛 A；只有映射换主/墓碑才清收。
"""
from __future__ import annotations

import json
import sys
import threading
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402
from plaita.server.flow_worker import RedisFlowWorker  # noqa: E402
from plaita.server import start_dedup as sd  # noqa: E402
from plaita.server.tenant_quota import (  # noqa: E402
    publish_tenant_quota,
    tenant_running_count,
)
from plaita.storage.base import ExecutionState  # noqa: E402
from plaita.storage.memory import MemoryFlowStorage  # noqa: E402
from plaita.storage.redis import RedisExecutionStorage  # noqa: E402
from plaita.tenant_context import (  # noqa: E402
    DEFAULT_TENANT_ID,
    TENANT_SOURCE_EXPLICIT,
    reset_current_tenant,
    set_current_tenant,
)

ADMIN_KEY = "cluster-admin-key-r61"
QUEUE = "plaita:flow:queue"
TENANT = "default"

FLOW = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "s", "type": "start", "next": "e"},
        {"id": "e", "type": "end", "output": "ok"},
    ],
}


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path}/r61.db")
    redis = FakeRedis(decode_responses=True)
    publish_tenant_quota(redis, TENANT, 10)
    return redis


def _client(redis):
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    app.include_router(
        executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _post(client, key="mark-race"):
    return client.post(
        "/api/executions",
        json={"flow_id": "f1", "params": {}, "dedup_key": key},
        headers={"X-Admin-API-Key": ADMIN_KEY},
    )


def _queue_ids(redis):
    return [
        json.loads(entry[1]["payload"])["execution_id"]
        for entry in redis.xrange(QUEUE)
        if json.loads(entry[1]["payload"]).get("type") == "start"
    ]


def _execute_a(redis, a_id, key="mark-race"):
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
    token = set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)
    try:
        worker.start_flow(
            "f1", {}, version="1", execution_id=a_id, dedup_key=key
        )
    finally:
        reset_current_tenant(token)
    return storage


def test_mark_failure_after_carrier_consumed_converges_original_id(env, monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    first = {"done": False, "a": None}
    orig = executions_api._set_start_dedup_owner

    def gated(redis, tenant, key, execution_id, **kwargs):
        if not first["done"]:
            first["done"] = True
            first["a"] = execution_id
            entered.set()
            assert release.wait(12)
        return orig(redis, tenant, key, execution_id, **kwargs)

    monkeypatch.setattr(executions_api, "_set_start_dedup_owner", gated)
    results = {}

    def do_a():
        token = set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)
        try:
            results["A"] = _post(_client(env))
        finally:
            reset_current_tenant(token)

    thread = threading.Thread(target=do_a)
    thread.start()
    assert entered.wait(8)
    a_id = first["a"]
    storage = _execute_a(env, a_id)
    assert storage.load_execution_state(a_id).status == "completed"

    # mark 前 token/owner 消失（模拟 TTL 过期/接管），A carrier 已执行。
    env.delete(
        executions_api._start_dedup_token_key(TENANT, "mark-race"),
        executions_api._start_dedup_owner_key(TENANT, "mark-race"),
    )
    release.set()
    thread.join(12)

    resp_a = results["A"]
    assert resp_a.status_code == 200, resp_a.text
    assert resp_a.json()["execution_id"] == a_id
    assert resp_a.json()["status"] == "completed"
    assert env.exists(sd.execution_row_key(TENANT, a_id))
    assert not env.exists(sd.tombstone_key(a_id))
    assert sd.read_mapped(env, TENANT, "mark-race") == a_id
    # 直接调用 worker.start_flow 不消费 Stream；park 保留 A 的 carrier，
    # 但不得出现第二个 id。
    assert set(_queue_ids(env)) == {a_id}

    retry = _post(_client(env))
    assert retry.status_code == 200, retry.text
    assert retry.json()["execution_id"] == a_id, "同 key 重试必须收敛 A"
    assert set(_queue_ids(env)) == {a_id}, "不得铸 B"
    # A 已 completed → worker 终态释放名额（配额 0 是正确的）。
    assert tenant_running_count(env, TENANT) == 0


def test_mark_failure_parks_and_retry_converges_when_unrecoverable(env, monkeypatch):
    orig_mark = executions_api._set_start_dedup_owner
    orig_recover = sd.recover_after_mark_failure
    monkeypatch.setattr(executions_api, "_set_start_dedup_owner", lambda *a, **k: False)
    monkeypatch.setattr(sd, "recover_after_mark_failure", lambda *a, **k: False)
    first = _post(_client(env))
    assert first.status_code == 409, first.text
    detail = first.json()["detail"]
    assert detail["error"] == "execution_start_in_progress"
    assert first.headers.get("Retry-After") == "1"

    a_id = sd.read_mapped(env, TENANT, "mark-race")
    assert a_id
    assert env.exists(sd.execution_row_key(TENANT, a_id)), "park 不得 discard 占位行"
    assert not env.exists(sd.tombstone_key(a_id))
    assert tenant_running_count(env, TENANT) == 1

    # 模拟 claiming TTL 过期后重试：同 key 走 recovery，收敛原 id。
    env.delete(
        sd.token_key(TENANT, "mark-race"),
        sd.owner_key(TENANT, "mark-race"),
    )
    monkeypatch.setattr(executions_api, "_set_start_dedup_owner", orig_mark)
    monkeypatch.setattr(sd, "recover_after_mark_failure", orig_recover)
    retry = _post(_client(env))
    assert retry.status_code == 200, retry.text
    assert retry.json()["execution_id"] == a_id
    assert set(_queue_ids(env)) == {a_id}, "恢复只补 A 的载体（可重复同 id），不铸 B"


def test_worker_terminal_hit_discards_carrier(env):
    a_id = "a" * 32
    env.set(sd.map_key(TENANT, "k"), a_id)
    storage = RedisExecutionStorage(client=env, namespace="plaita")
    storage.save_execution_state(
        a_id,
        ExecutionState(
            execution_id=a_id, flow_id="f1", status="completed",
            context={"$LAST_NODE": "e"}, tenant_id=DEFAULT_TENANT_ID,
        ),
    )
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=storage,
        flow_storage=MemoryFlowStorage(),
        redis_client=env,
        enable_registry=False,
        enable_redis_logging=False,
    )
    token = set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)
    try:
        out = worker._handle_start_dedup_hit(
            "f1", a_id, carrier_execution_id="b" * 32, dedup_key="k"
        )
    finally:
        reset_current_tenant(token)
    assert out["already_terminal"] is True
    assert out["discarded"] is True
    assert out["execution_id"] == a_id

    # B108-02：独立计数器 + 审计记录（执行详情可见）。
    assert worker._dedup_discarded_total == 1
    metrics_text = worker.metrics_text()
    assert "plaita_worker_dedup_discarded_total" in metrics_text
    assert "} 1" in metrics_text.split("plaita_worker_dedup_discarded_total", 1)[1]
    audit = json.loads(
        env.get(f"plaita:execution:dedup-discard:{a_id}")
    )
    assert audit["execution_id"] == a_id
    assert audit["carrier_execution_id"] == "b" * 32
    assert audit["dedup_key"] == "k"
    assert audit["status"] == "completed"

    got = _client(env).get(
        f"/api/executions/{a_id}", headers={"X-Admin-API-Key": ADMIN_KEY}
    )
    assert got.status_code == 200, got.text
    assert got.json()["dedup_discarded"]["reason"] == "already_terminal"
    assert got.json()["dedup_discarded"]["carrier_execution_id"] == "b" * 32
