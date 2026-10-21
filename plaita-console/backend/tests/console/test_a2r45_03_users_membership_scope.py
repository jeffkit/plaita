"""A2R45-03（duo-a2-ledger 第四十五轮，#260 残面）：租户 admin 的 ``GET /api/users``
不得回传成员的**跨租户** memberships。

缺口（基线 ``95fe2ad``）：``users_svc.list_users(store, tenant_id=…)`` 只按租户筛
**用户**（#260 读面收窄），每个用户的 ``memberships`` 仍是全表——同时属于
tenant-a / tenant-b 的用户出现在 tenant-b admin 的列表里时，其 tenant-a 的成员
关系（租户 id + 角色）一并泄露；tenant-b admin 由此可枚举他租户存在性与成员
角色（#260 只堵了用户名枚举这一半）。

钉住：
1. 租户 admin 读面：每个用户的 ``memberships`` 只含**本租户**条目（服务层与
   API 两面）；
2. 对称：admin-a 看到的 shared 用户只有 tenant-a 条目，admin-b 只有 tenant-b；
3. 平台管理员读面不变：全量用户、全量 memberships。

负控：仅还原 ``users_svc.list_users`` 的 memberships 收窄 ⇒ 用例 1/2 精确红
（复现 shared 用户带回 tenant-a 条目），用例 3 保持绿。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

ROOT_PW = "root-password-1"
ADMIN_A_PW = "admin-a-password-1"
ADMIN_B_PW = "admin-b-password-1"
SHARED_PW = "shared-password-1"
ONLY_A_PW = "only-a-password-1"

TENANT_A = "tenant-a"
TENANT_B = "tenant-b"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ""
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/a2r45_03.db")
    monkeypatch.setattr("config.get_settings", _settings)
    monkeypatch.setattr("auth.get_settings", _settings)

    flow_store.init_engine(f"sqlite:///{tmp_path}/a2r45_03.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")
    tenants_svc.create_tenant(store, TENANT_B, "Tenant B")

    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    users_svc.create_user(store, "admin-a", ADMIN_A_PW, "admin")
    users_svc.add_member(store, TENANT_A, "admin-a", "admin")
    users_svc.create_user(store, "admin-b", ADMIN_B_PW, "admin")
    users_svc.add_member(store, TENANT_B, "admin-b", "admin")
    # shared：同时是 tenant-a 的 editor 与 tenant-b 的 viewer——跨租户泄露的载体
    users_svc.create_user(store, "shared", SHARED_PW, "viewer")
    users_svc.add_member(store, TENANT_A, "shared", "editor")
    users_svc.add_member(store, TENANT_B, "shared", "viewer")
    users_svc.create_user(store, "only-a", ONLY_A_PW, "viewer")
    users_svc.add_member(store, TENANT_A, "only-a", "viewer")

    app = FastAPI()
    app.state.store = store
    app.state.local_mode = True
    app.include_router(auth_users.router, prefix="/api")
    return TestClient(app), store


def _login(client: TestClient, username: str, password: str) -> dict:
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, (username, r.status_code, r.text)
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _listing(client: TestClient, headers: dict) -> dict:
    r = client.get("/api/users", headers=headers)
    assert r.status_code == 200, r.text
    return {u["username"]: u for u in r.json()["users"]}


def _tenants_in(users: dict) -> set:
    return {m["tenant_id"] for u in users.values() for m in u["memberships"]}


# ---------------------------------------------------------------- 1/2. 租户 admin 读面


def test_tenant_admin_b_does_not_see_tenant_a_memberships(env):
    client, _ = env
    users = _listing(client, _login(client, "admin-b", ADMIN_B_PW))
    assert set(users) == {"admin-b", "shared"}, set(users)
    # shared 的 tenant-a 成员关系（editor）不得出现在 tenant-b admin 的视图里
    assert users["shared"]["memberships"] == [{"tenant_id": TENANT_B, "role": "viewer"}]
    assert _tenants_in(users) == {TENANT_B}


def test_tenant_admin_a_sees_only_tenant_a_memberships(env):
    client, _ = env
    users = _listing(client, _login(client, "admin-a", ADMIN_A_PW))
    assert set(users) == {"admin-a", "shared", "only-a"}, set(users)
    assert users["shared"]["memberships"] == [{"tenant_id": TENANT_A, "role": "editor"}]
    assert _tenants_in(users) == {TENANT_A}


def test_service_layer_scopes_memberships_with_tenant_filter(env):
    """服务层同口径：带 tenant_id 时 memberships 只含该租户（API 之外的调用方同受益）。"""
    _, store = env
    rows = {u["username"]: u for u in users_svc.list_users(store, tenant_id=TENANT_B)}
    assert set(rows) == {"admin-b", "shared"}
    assert rows["shared"]["memberships"] == [{"tenant_id": TENANT_B, "role": "viewer"}]
    assert _tenants_in(rows) == {TENANT_B}


# ---------------------------------------------------------------- 3. 平台管理员读面不变


def test_platform_admin_still_sees_all_memberships(env):
    client, store = env
    users = _listing(client, _login(client, "root", ROOT_PW))
    assert {"root", "admin-a", "admin-b", "shared", "only-a"} <= set(users)
    shared = sorted(users["shared"]["memberships"], key=lambda m: m["tenant_id"])
    assert shared == [
        {"tenant_id": TENANT_A, "role": "editor"},
        {"tenant_id": TENANT_B, "role": "viewer"},
    ]
    # 服务层 tenant_id=None 亦全量
    rows = {u["username"]: u for u in users_svc.list_users(store)}
    assert len(rows["shared"]["memberships"]) == 2
