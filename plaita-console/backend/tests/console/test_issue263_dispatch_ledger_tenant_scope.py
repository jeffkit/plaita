"""plaita#263（P1 security 同族读面）：``/api/dispatch/ledger`` 的租户隔离。

缺口：派发台账条目含 ``tenant_id`` / ``actor`` / ``issue_id`` / ``execution_id``
与队列对账，但端点此前对**任何**已认证身份返回全量 ``XRANGE``——租户 viewer
可读他租户的派发事实（谁派了哪个执行、因何路由、队列积压量）。

判据：
1. 非平台身份只读本租户：rows / ledger / queue_length / parity 同时收窄
   （缺 ``tenant_id`` 的历史条目不可见，fail-closed）；
2. 非平台身份显式传他租户 ``tenant_id`` ⇒ 403 + ``dispatch.ledger.denied``
   审计（不得静默忽略冒充过滤）；
3. 平台管理员 / 平台 API-Key 默认全量，可显式按租户收窄。

负控（必红）：删除端点里的租户过滤（直接 ``ledger_rows(redis)``）⇒
``test_viewer_only_sees_own_tenant`` 读到他租户行而精确红。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from api import capacity as capacity_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import audit as audit_svc  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

from plaita.server.dispatch_ledger import (  # noqa: E402
    build_ledger_entry,
    record_dispatch,
)
from plaita.server.task_queue import enqueue_task  # noqa: E402

ROOT_PW = "root-password-1"
VIEWER_PW = "viewer-password-1"
TENANT_A = "tenant-a"
TENANT_B = "tenant-b"
ADMIN_KEY = "issue263-ledger-key"
QUEUE = "plaita:flow:queue"


class Env:
    def __init__(self, client, redis):
        self.client = client
        self.redis = redis


def _settings(admin_key: str = ADMIN_KEY):
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = admin_key
    s.allow_insecure_admin = False
    return s


def _seed(redis):
    # 台账：tenant-a ×2、tenant-b ×3、无租户历史条目 ×1。
    for tenant, issue in (
        (TENANT_A, "a-1"), (TENANT_A, "a-2"),
        (TENANT_B, "b-1"), (TENANT_B, "b-2"), (TENANT_B, "b-3"),
    ):
        entry = build_ledger_entry(
            issue_id=issue, queue=QUEUE, target_instance_id=None,
            reason="route:none", tenant_id=tenant, actor=f"{tenant}-editor",
        )
        record_dispatch(redis, entry)
    legacy = build_ledger_entry(
        issue_id="legacy", queue=QUEUE, target_instance_id=None,
        reason="route:none", tenant_id=None, actor="legacy",
    )
    record_dispatch(redis, legacy)

    # 队列：tenant-a ×2、tenant-b ×3、无租户 ×1。
    for tenant, exec_id in (
        (TENANT_A, "a-1"), (TENANT_A, "a-2"),
        (TENANT_B, "b-1"), (TENANT_B, "b-2"), (TENANT_B, "b-3"),
    ):
        enqueue_task(redis, QUEUE, {
            "type": "start", "flow_id": "f", "execution_id": exec_id,
            "tenant_id": tenant, "params": {},
        })
    enqueue_task(redis, QUEUE, {
        "type": "start", "flow_id": "f", "execution_id": "legacy", "params": {},
    })


@pytest.fixture()
def env(tmp_path, monkeypatch) -> Env:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/issue263l.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())

    flow_store.init_engine(f"sqlite:///{tmp_path}/issue263l.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")
    tenants_svc.create_tenant(store, TENANT_B, "Tenant B")

    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    users_svc.create_user(store, "viewer-a", VIEWER_PW, "viewer")
    users_svc.add_member(store, TENANT_A, "viewer-a", "viewer")

    redis = FakeRedis(decode_responses=True)
    _seed(redis)

    app = FastAPI()
    app.state.store = store
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        capacity_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return Env(TestClient(app), redis)


def _login(client: TestClient, username: str, password: str) -> dict:
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def test_viewer_only_sees_own_tenant(env: Env):
    """负控：去掉租户过滤 ⇒ 本条读到 b-* 行而精确红。"""
    headers = _login(env.client, "viewer-a", VIEWER_PW)
    resp = env.client.get("/api/dispatch/ledger", headers=headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["tenant_id"] == TENANT_A
    issue_ids = {row["issue_id"] for row in body["rows"]}
    assert issue_ids == {"a-1", "a-2"}, issue_ids
    assert all(row["tenant_id"] == TENANT_A for row in body["rows"])
    assert body["ledger"] == 2
    assert body["queue_length"] == 2
    assert body["parity"] is True
    assert body["error"] is None


def test_viewer_cross_tenant_query_is_denied_and_audited(env: Env):
    headers = _login(env.client, "viewer-a", VIEWER_PW)
    resp = env.client.get(
        "/api/dispatch/ledger", params={"tenant_id": TENANT_B}, headers=headers,
    )
    assert resp.status_code == 403, resp.text
    assert resp.json().get("detail")
    assert TENANT_B not in resp.text

    rows = audit_svc.list_audit(action="dispatch.ledger.denied", limit=50)
    assert any(
        row["actor"] == "viewer-a"
        and row["detail"].get("requested_tenant") == TENANT_B
        and row["detail"].get("auth_tenant") == TENANT_A
        for row in rows
    ), rows


def test_viewer_own_tenant_query_ok(env: Env):
    """显式传本租户不是跨租户：仍 200（避免「无条件 403」冒充隔离）。"""
    headers = _login(env.client, "viewer-a", VIEWER_PW)
    resp = env.client.get(
        "/api/dispatch/ledger", params={"tenant_id": TENANT_A}, headers=headers,
    )
    assert resp.status_code == 200, resp.text
    assert {row["issue_id"] for row in resp.json()["rows"]} == {"a-1", "a-2"}


def test_platform_admin_sees_all_and_can_narrow(env: Env):
    root = _login(env.client, "root", ROOT_PW)

    full = env.client.get("/api/dispatch/ledger", headers=root)
    assert full.status_code == 200, full.text
    body = full.json()
    assert body["tenant_id"] is None
    assert {row["issue_id"] for row in body["rows"]} == {
        "a-1", "a-2", "b-1", "b-2", "b-3", "legacy",
    }
    assert body["ledger"] == 6
    assert body["queue_length"] == 6
    assert body["parity"] is True

    narrowed = env.client.get(
        "/api/dispatch/ledger", params={"tenant_id": TENANT_B}, headers=root,
    )
    assert narrowed.status_code == 200, narrowed.text
    assert narrowed.json()["tenant_id"] == TENANT_B
    assert {row["issue_id"] for row in narrowed.json()["rows"]} == {
        "b-1", "b-2", "b-3",
    }
    assert narrowed.json()["ledger"] == 3
    assert narrowed.json()["queue_length"] == 3


def test_api_key_is_platform_full_view(env: Env):
    resp = env.client.get(
        "/api/dispatch/ledger", headers={"X-Admin-API-Key": ADMIN_KEY},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["tenant_id"] is None
    assert resp.json()["ledger"] == 6
