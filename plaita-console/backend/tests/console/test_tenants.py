"""多租户隔离与租户管理端到端单测。

覆盖：租户 CRUD（平台管理员专属）、成员管理、数据隔离（A/B 租户互不可见）、
会话租户切换、X-Tenant-ID 规则、契约接口按租户密钥验签 + 全局密钥兼容、
存量库迁移回填。
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from sqlalchemy import create_engine, text
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from api import flows as flows_api  # noqa: E402
from api import tenants as tenants_api  # noqa: E402
from api import flow_version as flow_version_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402
from services import tenants_svc  # noqa: E402
from services import users_svc  # noqa: E402

GOOD_DEF = json.dumps({"nodes": [
    {"type": "start", "id": "start", "next": "end"},
    {"type": "end", "id": "end", "resultType": "success", "output": "ok"},
]})

ADMIN_KEY = "test-admin-key-1"


def _settings(env="dev", admin_key=ADMIN_KEY):
    from config import Settings
    s = Settings()
    s.console_env = env
    s.admin_api_key = admin_key
    s.allow_insecure_admin = False
    return s


def _build_app(tmp_path, monkeypatch) -> FastAPI:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/tenants.db")
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path}/tenants.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "root", "root-password-1", "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")

    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        flows_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(
        tenants_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(flow_version_api.router, prefix="/api")
    return app


@pytest.fixture()
def client(tmp_path, monkeypatch):
    return TestClient(_build_app(tmp_path, monkeypatch))


def _login(client, username, password):
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return r.json()


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


# ---- 租户 CRUD（平台管理员专属） ----

def test_tenant_crud_by_platform_admin(client):
    info = _login(client, "root", "root-password-1")
    headers = _auth(info["token"])

    r = client.post("/api/tenants", json={"id": "acme", "name": "Acme Inc"},
                    headers=headers)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["id"] == "acme"
    # 契约密钥仅创建响应返回一次
    assert body["contract_secret_id"] and body["contract_secret_key"]

    # 列表/详情不再回显 secret_key
    listing = client.get("/api/tenants", headers=headers).json()["tenants"]
    assert {t["id"] for t in listing} >= {"default", "acme"}
    assert all("contract_secret_key" not in t for t in listing)

    # 非法 slug
    assert client.post("/api/tenants", json={"id": "Bad_Slug"}, headers=headers).status_code == 409
    # 重复
    assert client.post("/api/tenants", json={"id": "acme"}, headers=headers).status_code == 409


def test_tenant_endpoints_reject_non_platform_admin(client):
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "plain", "plain-password-1", "admin")
    info = _login(client, "plain", "plain-password-1")
    headers = _auth(info["token"])
    assert client.get("/api/tenants", headers=headers).status_code == 403
    assert client.post("/api/tenants", json={"id": "x-tenant"},
                       headers=headers).status_code == 403


def test_delete_tenant_with_flows_rejected(client):
    info = _login(client, "root", "root-password-1")
    headers = _auth(info["token"])
    client.post("/api/tenants", json={"id": "busy"}, headers=headers)
    flow_store.get_flow_store().create_flow("busy-flow", tenant_id="busy")
    r = client.delete("/api/tenants/busy", headers=headers)
    assert r.status_code == 409
    assert client.delete("/api/tenants/default", headers=headers).status_code == 200


# ---- 成员管理 ----

def test_member_management_flow(client):
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "alice", "alice-password-1", "viewer")
    info = _login(client, "root", "root-password-1")
    headers = _auth(info["token"])
    client.post("/api/tenants", json={"id": "acme"}, headers=headers)

    r = client.post("/api/tenants/acme/members",
                    json={"username": "alice", "role": "editor"}, headers=headers)
    assert r.status_code == 200, r.text
    members = client.get("/api/tenants/acme/members", headers=headers).json()["members"]
    assert {"username": "alice", "role": "editor", "tenant_id": "acme"} in members

    # 改角色
    r = client.put("/api/tenants/acme/members/alice",
                   json={"username": "alice", "role": "admin"}, headers=headers)
    assert r.status_code == 200
    # 移除
    assert client.delete("/api/tenants/acme/members/alice",
                         headers=headers).status_code == 200
    assert client.get("/api/tenants/acme/members", headers=headers).json()["members"] == []


# ---- 数据隔离 ----

def test_flow_isolation_between_tenants(client):
    """A 租户建的流程，B 租户列表看不到、详情取不到。"""
    info = _login(client, "root", "root-password-1")
    headers = _auth(info["token"])
    client.post("/api/tenants", json={"id": "acme"}, headers=headers)
    users_svc.add_member(flow_store.get_flow_store(), "acme", "root", "editor")

    # 平台管理员切到 acme 建 flow
    r = client.post("/api/flows", json={"flow_id": "acme-flow"},
                    headers={**headers, "X-Tenant-ID": "acme"})
    assert r.status_code == 200, r.text
    assert r.json()["tenant_id"] == "acme"

    # 平台管理员会话被钉在 default：看不到 acme 的
    flow_store.get_flow_store().create_flow("default-flow-a", tenant_id="default")
    pinned = client.get("/api/flows", headers=headers).json()
    assert pinned["flows"] and all(f["tenant_id"] == "default" for f in pinned["flows"])

    # 平台全量视角走 API-Key（tenant_id=None）
    all_flows = client.get(
        "/api/flows", headers={"X-Admin-API-Key": ADMIN_KEY}
    ).json()["flows"]
    assert {f["tenant_id"] for f in all_flows} >= {"default", "acme"}

    # 限定 acme 只看到 acme 的
    acme_only = client.get("/api/flows", headers={**headers, "X-Tenant-ID": "acme"}).json()
    assert all(f["tenant_id"] == "acme" for f in acme_only["flows"])

    # 非 acme 租户视角取不到 acme-flow（切回 default 后访问 404）
    switch = client.post("/api/auth/switch-tenant", json={"tenant_id": "default"},
                         headers=headers)
    assert switch.status_code == 200
    r = client.get("/api/flows/acme-flow", headers=headers)
    assert r.status_code == 404


def test_same_flow_id_allowed_across_tenants(client):
    """不同租户可以建同名流程（租户内唯一）。"""
    info = _login(client, "root", "root-password-1")
    headers = _auth(info["token"])
    client.post("/api/tenants", json={"id": "acme"}, headers=headers)
    users_svc.add_member(flow_store.get_flow_store(), "acme", "root", "editor")
    store = flow_store.get_flow_store()
    store.create_flow("shared-name", tenant_id="default")
    r = client.post("/api/flows", json={"flow_id": "shared-name"},
                    headers={**headers, "X-Tenant-ID": "acme"})
    assert r.status_code == 200, r.text
    # 同租户再建同名 → 409
    r = client.post("/api/flows", json={"flow_id": "shared-name"},
                    headers={**headers, "X-Tenant-ID": "acme"})
    assert r.status_code == 409


# ---- 会话租户切换 ----

def test_switch_tenant_session_flow(client):
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "bob", "bob-password-1", "viewer")
    users_svc.add_member(store, "default", "bob", "viewer")
    info = _login(client, "root", "root-password-1")
    headers = _auth(info["token"])
    client.post("/api/tenants", json={"id": "acme"}, headers=headers)
    users_svc.add_member(store, "acme", "bob", "editor")

    bob = _login(client, "bob", "bob-password-1")
    # bob 只有 acme 一个成员资格 → 登录后活跃租户即 acme
    assert bob["active_tenant"] == "acme"
    assert {"tenant_id": "acme", "role": "editor"} in bob["memberships"]

    # 切回 default（bob 在 default 有成员资格——bootstrap 回填）
    r = client.post("/api/auth/switch-tenant", json={"tenant_id": "default"},
                    headers=_auth(bob["token"]))
    assert r.status_code == 200
    assert r.json()["tenant_id"] == "default"

    # 非成员租户切换被拒
    client.post("/api/tenants", json={"id": "other"}, headers=headers)
    r = client.post("/api/auth/switch-tenant", json={"tenant_id": "other"},
                    headers=_auth(bob["token"]))
    assert r.status_code == 400

    # 被移出后旧会话被吊销（成员变更即吊销），重新登录回落 default
    users_svc.remove_member(store, "acme", "bob")
    r = client.get("/api/auth/me", headers=_auth(bob["token"]))
    assert r.status_code == 401
    bob2 = _login(client, "bob", "bob-password-1")
    assert bob2["active_tenant"] == "default"


# ---- X-Tenant-ID 权限规则 ----

def test_x_tenant_header_rules(client):
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "carol", "carol-password-1", "viewer")
    info = _login(client, "root", "root-password-1")
    headers = _auth(info["token"])
    client.post("/api/tenants", json={"id": "acme"}, headers=headers)
    users_svc.add_member(store, "acme", "carol", "viewer")

    # 普通用户带 X-Tenant-ID → 403
    carol = _login(client, "carol", "carol-password-1")
    r = client.get("/api/flows", headers={**_auth(carol["token"]), "X-Tenant-ID": "acme"})
    assert r.status_code == 403

    # 平台管理员带头 OK；api-key 也是平台级
    assert client.get("/api/flows",
                      headers={**headers, "X-Tenant-ID": "acme"}).status_code == 200
    assert client.get("/api/flows",
                      headers={"X-Admin-API-Key": ADMIN_KEY, "X-Tenant-ID": "acme"}).status_code == 200


# ---- 契约接口按租户发密钥 ----

def _sig(secret_key: str, secret_id: str) -> str:
    """plaita.client.generate_signature(key, id, validity, sign_time) 的测试封装。"""
    import time

    from plaita.client import generate_signature
    return generate_signature(secret_key, secret_id, 600, int(time.time()))

def _publish_flow(client, headers, flow_id, tenant_header=None):
    h = {**headers, **({"X-Tenant-ID": tenant_header} if tenant_header else {})}
    assert client.post("/api/flows", json={"flow_id": flow_id}, headers=h).status_code == 200
    r = client.put(f"/api/flows/{flow_id}/versions/1.0.0",
                   json={"definition": GOOD_DEF, "layout": "{}", "created_by": "t"}, headers=h)
    assert r.status_code == 200, r.text
    assert client.post(f"/api/flows/{flow_id}/publish", json={"version": "1.0.0"},
                       headers=h).status_code == 200


def test_contract_per_tenant_secret_and_global_compat(client, monkeypatch):
    from plaita.client import generate_signature

    info = _login(client, "root", "root-password-1")
    headers = _auth(info["token"])
    created = client.post("/api/tenants", json={"id": "acme"}, headers=headers).json()
    users_svc.add_member(flow_store.get_flow_store(), "acme", "root", "editor")
    _publish_flow(client, headers, "acme-flow", tenant_header="acme")

    url = "/api/flowVersion/semver/detail"

    # 租户密钥：只能拉本租户流程
    sig = _sig(created["contract_secret_key"], created["contract_secret_id"])
    r = client.post(url, headers={"authorization": sig},
                    data={"flowId": "acme-flow", "version": "1.0.0"})
    assert r.status_code == 200 and r.json()["code"] == 0
    assert json.loads(r.json()["data"]["flow"])["nodes"][0]["id"] == "start"

    # 租户密钥拉别的租户（default）的流程 → 不存在
    _publish_flow(client, headers, "default-flow")
    r = client.post(url, headers={"authorization": sig},
                    data={"flowId": "default-flow", "version": "1.0.0"})
    assert r.json()["code"] == 2

    # 全局密钥（平台上下文）：两者都能拉。get_settings() 每次读环境变量，
    # 设置 env 即可，无需替换配置对象。
    monkeypatch.setenv("PLAITA_CONSOLE_SECRET_ID", "global-id")
    monkeypatch.setenv("PLAITA_CONSOLE_SECRET_KEY", "global-key")
    gsig = _sig("global-key", "global-id")
    for fid in ("acme-flow", "default-flow"):
        r = client.post(url, headers={"authorization": gsig},
                        data={"flowId": fid, "version": "1.0.0"})
        assert r.status_code == 200 and r.json()["code"] == 0, (fid, r.text)

    # 轮换后旧密钥失效
    rotated = client.post("/api/tenants/acme/rotate-secret", headers=headers).json()
    r = client.post(url, headers={"authorization": sig},
                    data={"flowId": "acme-flow", "version": "1.0.0"})
    assert r.status_code == 401
    sig2 = _sig(rotated["contract_secret_key"], rotated["contract_secret_id"])
    r = client.post(url, headers={"authorization": sig2},
                    data={"flowId": "acme-flow", "version": "1.0.0"})
    assert r.json()["code"] == 0


# ---- 存量库迁移 ----

def test_legacy_db_migration_backfills_default_tenant(tmp_path):
    """旧 schema（无 tenant_id、flow_id 全局唯一）的库 → init_engine + bootstrap
    后：列补齐、数据归 default、全局 admin 提平台管理员、旧同名流程可跨租户共存。"""
    import sqlalchemy

    db = tmp_path / "legacy.db"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as c:
        c.execute(text(
            "CREATE TABLE flows (id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " flow_id VARCHAR(128) NOT NULL UNIQUE, author VARCHAR(128) NOT NULL,"
            " desc TEXT NOT NULL, created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL)"
        ))
        c.execute(text(
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " username VARCHAR(64) NOT NULL UNIQUE, password_hash VARCHAR(256) NOT NULL,"
            " role VARCHAR(16) NOT NULL, disabled BOOLEAN NOT NULL, created_at DATETIME NOT NULL)"
        ))
        c.execute(text(
            "INSERT INTO flows (flow_id, author, desc, created_at, updated_at)"
            " VALUES ('legacy-flow', 'a', '', '2026-01-01 00:00:00', '2026-01-01 00:00:00')"
        ))
        c.execute(text(
            "INSERT INTO users (username, password_hash, role, disabled, created_at)"
            " VALUES ('admin', 'x', 'admin', 0, '2026-01-01 00:00:00')"
        ))

    flow_store.init_engine(f"sqlite:///{db}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()

    flows = store.list_flows("default")
    assert [f.flow_id for f in flows] == ["legacy-flow"]

    with store._session_local() as session:
        from services.flow_store import User, TenantMember
        admin = session.query(User).filter(User.username == "admin").first()
        assert admin.platform_admin is True
        m = session.query(TenantMember).filter(
            TenantMember.username == "admin", TenantMember.tenant_id == "default"
        ).first()
        assert m is not None and m.role == "admin"

    # 迁移后同租户同名拒绝、跨租户同名允许
    with pytest.raises(ValueError):
        store.create_flow("legacy-flow", tenant_id="default")
    store.create_flow("legacy-flow", tenant_id="acme")
