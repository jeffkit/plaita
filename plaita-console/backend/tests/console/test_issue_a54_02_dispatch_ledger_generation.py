"""A54-02（duo-a 第五十四轮）：派发台账跨世代可见。

基线 ``5ccaf6a`` 的 ``GET /api/dispatch/ledger`` 既不取 ``tenant_scope`` 也不
按世代过滤，台账条目也没有世代字段；同名租户重建后旧条目的 ``tenant_id``
重新命中新租户 ⇒ 新租户 viewer 读到前代 ``actor/issue_id/execution_id``。

本文件钉住：
1. 租户视图只返回**当前世代**的本租户条目（他租户、旧世代、无世代旧行都不可见）；
2. 同名重建后旧世代条目对新租户为空（平台审计仍可全量读——保留而不物理删）；
3. 租户视图的队列对账显式不可用（全局计数不泄露给租户）；
4. 派发入口把当前世代写进台账（``dispatch_one`` → ``build_ledger_entry``）。

负控（验收时手工执行）：
- 去掉世代比较（只比 ``tenant_id``）⇒ 第 2 组重建后读到旧行，精确红；
- 去掉租户比较 ⇒ 他租户行出现在租户视图，精确红。
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
from plaita.server.dispatch_ledger import (  # noqa: E402
    build_ledger_entry,
    read_ledger,
    record_dispatch,
)
from services import dispatcher as dispatcher_svc  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

TENANT_A = "tenant-a"
TENANT_B = "tenant-b"
VIEWER_PW = "viewer-password-1"
ROOT_PW = "root-password-1"
QUEUE = "plaita:flow:queue"
TENANT_SCOPED_PARITY_DETAIL = "租户视图：队列对账仅平台身份可用"


class Env:
    def __init__(self, client: TestClient, redis: FakeRedis, store):
        self.client = client
        self.redis = redis
        self.store = store


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ""
    s.allow_insecure_admin = False
    return s


def _build_env(tmp_path, monkeypatch) -> Env:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/a54-02.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())

    flow_store.init_engine(f"sqlite:///{tmp_path}/a54-02.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")
    tenants_svc.create_tenant(store, TENANT_B, "Tenant B")
    users_svc.create_user(store, "viewer-a", VIEWER_PW, "viewer")
    users_svc.add_member(store, TENANT_A, "viewer-a", "viewer")
    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")

    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.store = store
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        capacity_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return Env(TestClient(app), redis, store)


@pytest.fixture()
def env(tmp_path, monkeypatch) -> Env:
    return _build_env(tmp_path, monkeypatch)


def _login(client: TestClient, username: str, password: str) -> dict:
    r = client.post(
        "/api/auth/login", json={"username": username, "password": password}
    )
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _seed_ledger(env: Env):
    gen_a = tenants_svc.tenant_generation(env.store, TENANT_A)
    gen_b = tenants_svc.tenant_generation(env.store, TENANT_B)
    assert gen_a and gen_b
    rows = [
        ("iss-a-current", TENANT_A, gen_a, "alice"),
        ("iss-b-current", TENANT_B, gen_b, "bob"),
        # 修复前写入的旧行没有 tenant_generation：租户视图必须 fail-closed。
        ("iss-a-legacy", TENANT_A, None, "legacy"),
        ("iss-a-generation-old", TENANT_A, "2000-01-01T00:00:00", "oldgen"),
    ]
    for issue_id, tenant, generation, actor in rows:
        assert record_dispatch(
            env.redis,
            build_ledger_entry(
                issue_id=issue_id,
                queue=QUEUE,
                tenant_id=tenant,
                tenant_generation=generation,
                actor=actor,
            ),
        )
    return gen_a


def _issue_ids(response) -> list:
    assert response.status_code == 200, response.text
    return [row["issue_id"] for row in response.json()["rows"]]


def test_tenant_view_shows_only_current_generation(env: Env):
    _seed_ledger(env)
    headers = _login(env.client, "viewer-a", VIEWER_PW)
    r = env.client.get("/api/dispatch/ledger", headers=headers)
    assert _issue_ids(r) == ["iss-a-current"]
    body = r.json()
    assert body["ledger"] == 1
    assert body["queue_length"] is None
    assert body["parity"] is False
    assert body["error"] == TENANT_SCOPED_PARITY_DETAIL


def test_platform_view_still_sees_every_generation(env: Env):
    _seed_ledger(env)
    headers = _login(env.client, "root", ROOT_PW)
    r = env.client.get("/api/dispatch/ledger", headers=headers)
    assert set(_issue_ids(r)) == {
        "iss-a-current",
        "iss-b-current",
        "iss-a-legacy",
        "iss-a-generation-old",
    }
    assert r.json()["ledger"] == 4


def test_recreated_tenant_cannot_read_previous_generation(env: Env):
    _seed_ledger(env)
    assert tenants_svc.delete_tenant(
        env.store, TENANT_A, redis_client=env.redis
    ) is True
    tenants_svc.create_tenant(env.store, TENANT_A, "Tenant A again", redis_client=env.redis)
    users_svc.add_member(env.store, TENANT_A, "viewer-a", "viewer")

    headers = _login(env.client, "viewer-a", VIEWER_PW)
    r = env.client.get("/api/dispatch/ledger", headers=headers)
    assert _issue_ids(r) == []
    assert r.json()["ledger"] == 0

    # 平台审计保留：旧条目仍在台账里，只是租户读面看不到。
    assert len(read_ledger(env.redis)) == 4


def test_dispatch_one_records_tenant_generation(env: Env):
    generation = tenants_svc.tenant_generation(env.store, TENANT_A)
    result = dispatcher_svc.dispatch_one(
        env.redis,
        "iss-new",
        queue_name=QUEUE,
        tenant_id=TENANT_A,
        tenant_generation=generation,
        actor="viewer-a",
        dry_run=False,
        shadow=False,
    )
    assert result["enqueued"] is True, result
    rows = read_ledger(env.redis, tenant_id=TENANT_A, tenant_generation=generation)
    assert [row["issue_id"] for row in rows] == ["iss-new"]
    assert rows[0]["tenant_generation"] == generation
    assert rows[0]["actor"] == "viewer-a"
