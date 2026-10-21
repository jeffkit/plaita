"""#280 (r55): dedup pre-claim/placeholder race + queued-cancel atomicity.

Concurrent same-``dedup_key`` POSTs: A's ``SET NX`` succeeds but its queued row
is not written yet; B saw "mapping exists, row missing" and deleted A's claim,
re-claimed with its own id, and both POSTs enqueued distinct start carriers.
The loser id stayed queued forever and consumed quota.

Fix: the claim Lua writes ``SET NX`` + an owner marker atomically; B seeing a
live ``claiming:<id>`` owner never steals, waits briefly for the row, and
returns the same id. Owner expiry (crash) still allows orphan reclaim. A queued
row with no owner (crashed between row write and enqueue) is re-enqueued with
the same id instead of returning permanent queued. CANCEL queued now removes
the quota member in the same Lua as the CAS; re-CANCEL on an already-cancelled
row compensates legacy quota leaks.
"""
from __future__ import annotations

import json
import sys
import threading
import time
from datetime import datetime
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
from plaita.server.tenant_quota import (  # noqa: E402
    publish_tenant_quota,
    tenant_running_count,
    try_admit_tenant_start,
)
from plaita.tenant_context import (  # noqa: E402
    DEFAULT_TENANT_ID,
    TENANT_SOURCE_EXPLICIT,
    reset_current_tenant,
    set_current_tenant,
)

ADMIN_KEY = "cluster-admin-key-280"
QUEUE = "plaita:flow:queue"
TENANT = "default"


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
    flow_store.init_engine(f"sqlite:///{tmp_path}/issue280.db")
    redis = FakeRedis(decode_responses=True)
    publish_tenant_quota(redis, TENANT, 10)
    return redis


def _client(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    app.include_router(
        executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _post_start(client, **overrides):
    body = {"flow_id": "f1", "params": {"x": 1}}
    body.update(overrides)
    return client.post("/api/executions", json=body, headers=_headers())


def _queued_ids(redis):
    return [
        json.loads(entry[1]["payload"])["execution_id"]
        for entry in redis.xrange(QUEUE)
        if json.loads(entry[1]["payload"]).get("type") == "start"
    ]


def test_concurrent_same_key_returns_one_id_and_one_carrier(env, monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    real_placeholder = executions_api._write_queued_placeholder

    def blocked_placeholder(*args, **kwargs):
        if not entered.is_set():
            entered.set()
            assert release.wait(5), "A 未在超时内完成占位行写入"
        return real_placeholder(*args, **kwargs)

    monkeypatch.setattr(
        executions_api, "_write_queued_placeholder", blocked_placeholder
    )
    results: dict[str, object] = {}

    def post(name):
        token = set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)
        try:
            results[name] = _post_start(_client(env), dedup_key="race-key")
        finally:
            reset_current_tenant(token)

    thread_a = threading.Thread(target=post, args=("A",))
    thread_a.start()
    assert entered.wait(3), "A 未进入占位行写窗口"

    thread_b = threading.Thread(target=post, args=("B",))
    thread_b.start()
    thread_b.join(8)
    release.set()
    thread_a.join(8)

    assert results["A"].status_code == 200, results["A"].text
    # plaita#285：载体尚未落盘时 B 不再返回（可能成为 ghost 的）200，而是
    # 明确 409 in-progress；用同一 dedup_key 重试后收敛到 A。
    assert results["B"].status_code == 409, results["B"].text
    assert results["B"].json()["detail"]["error"] == "execution_start_in_progress"
    id_a = results["A"].json()["execution_id"]
    retry = _post_start(_client(env), dedup_key="race-key")
    assert retry.status_code == 200, retry.text
    assert retry.json()["execution_id"] == id_a
    assert _queued_ids(env) == [id_a], "同 dedup_key 只能有一条 start 载体"
    assert tenant_running_count(env, TENANT) == 1


def test_expired_owner_allows_orphan_reclaim(env):
    client = _client(env)
    old_id = "a" * 32
    claimed, mapped = executions_api._claim_start_dedup_at_post(
        env, TENANT, "orphan-key", old_id
    )
    assert claimed and mapped is None
    # 模拟 owner 崩溃/过期（行从未写入）：B 可释放孤儿映射并换新 id。
    env.delete(executions_api._start_dedup_owner_key(TENANT, "orphan-key"))

    resp = _post_start(client, dedup_key="orphan-key")
    assert resp.status_code == 200, resp.text
    new_id = resp.json()["execution_id"]
    assert new_id != old_id
    assert _queued_ids(env) == [new_id]


def test_queued_row_without_carrier_is_requeued_with_same_id(env):
    client = _client(env)
    old_id = "b" * 32
    claimed, _ = executions_api._claim_start_dedup_at_post(
        env, TENANT, "crash-key", old_id
    )
    assert claimed
    assert executions_api._write_queued_placeholder(
        env, TENANT, old_id, "f1", None, datetime.now().isoformat()
    )
    # 崩溃在「占位行落盘 → 入队」之间：owner 消失，行仍在。
    env.delete(executions_api._start_dedup_owner_key(TENANT, "crash-key"))

    resp = _post_start(client, dedup_key="crash-key")
    assert resp.status_code == 200, resp.text
    assert resp.json()["execution_id"] == old_id
    assert _queued_ids(env) == [old_id], "崩溃窗口应补入队同一 id，而不是永久 queued"


def test_cancel_queued_releases_quota_atomically(env):
    client = _client(env)
    eid = _post_start(client).json()["execution_id"]
    assert tenant_running_count(env, TENANT) == 1

    resp = client.post(f"/api/executions/{eid}/cancel", headers=_headers())
    assert resp.status_code == 200, resp.text
    assert tenant_running_count(env, TENANT) == 0, "CAS 置 cancelled 必须同脚本释放配额"
    got = client.get(f"/api/executions/{eid}", headers=_headers())
    assert got.status_code == 200, got.text
    assert got.json()["status"] == "cancelled"


def test_recancel_cancelled_row_releases_legacy_quota(env):
    client = _client(env)
    eid = _post_start(client).json()["execution_id"]
    assert client.post(f"/api/executions/{eid}/cancel", headers=_headers()).status_code == 200
    # 模拟旧版「CAS 后崩溃」留下的配额占用；再 CANCEL 命中终态短路也要补释放。
    assert try_admit_tenant_start(env, TENANT, eid)
    assert tenant_running_count(env, TENANT) == 1

    resp = client.post(f"/api/executions/{eid}/cancel", headers=_headers())
    assert resp.status_code == 200, resp.text
    assert tenant_running_count(env, TENANT) == 0
