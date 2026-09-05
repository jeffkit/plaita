"""RBAC / 审计 / 环境晋升 端到端单测。

用裸 FastAPI 应用模拟依赖注入状态（app.state.redis/store/local_mode），
覆盖：登录签发、角色方法矩阵、admin 前缀限制、API key 兼容、
审计落库、晋升包导出/导入/指纹校验、生产环境删除保护。
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from api import flows as flows_api  # noqa: E402
from api import credentials as credentials_api  # noqa: E402
from api import audit as audit_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import audit as audit_svc  # noqa: E402
from services import deployments as deployments_svc  # noqa: E402
from services import examples as examples_svc  # noqa: E402
from services import flow_store  # noqa: E402
from services import users_svc  # noqa: E402

GOOD_DEF = json.dumps({"nodes": [
    {"type": "start", "id": "start", "next": "end"},
    {"type": "end", "id": "end", "resultType": "success", "output": "ok"},
]})


def _build_app(tmp_path, monkeypatch, env="dev", admin_key: str = "", with_user: bool = True) -> FastAPI:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/rbac.db")
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setattr("config.get_settings", lambda: _settings(env, admin_key))
    if "plaita-console" in str(BACKEND_DIR):
        monkeypatch.setattr("auth.get_settings", lambda: _settings(env, admin_key))
    else:
        monkeypatch.setattr("auth.get_settings", lambda: _settings(env, admin_key))

    flow_store.init_engine(f"sqlite:///{tmp_path}/rbac.db")
    store = flow_store.get_flow_store()
    if with_user:
        users_svc.create_user(store, "admin", "admin-password-1", "admin")

    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")

    app.include_router(
        flows_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(
        credentials_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(
        audit_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )

    @app.get("/api/secure", dependencies=[Depends(require_auth)])
    def secure(identity: dict = Depends(require_auth)):
        return identity

    return app


def _settings(env="dev", admin_key=""):
    from config import Settings
    s = Settings()
    s.console_env = env
    s.admin_api_key = admin_key
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def client(tmp_path, monkeypatch):
    app = _build_app(tmp_path, monkeypatch)
    examples_svc.seed_example_flows(flow_store.get_flow_store())
    return TestClient(app)


@pytest.fixture()
def client_with_users(tmp_path, monkeypatch):
    app = _build_app(tmp_path, monkeypatch, with_user=False)
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "admin", "admin-password-1", "admin")
    users_svc.create_user(store, "editor1", "editor-password-1", "editor")
    users_svc.create_user(store, "viewer1", "viewer-password-1", "viewer")
    examples_svc.seed_example_flows(store)
    return TestClient(app), store


def _login(client, username, password):
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200
    return r.json()


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


# ---- 登录与会话 ----

def test_login_ok_and_me(client_with_users):
    client, _ = client_with_users
    info = _login(client, "editor1", "editor-password-1")
    assert info["role"] == "editor"
    me = client.get("/api/auth/me", headers=_auth(info["token"])).json()
    assert me == {"actor": "editor1", "role": "editor"}


def test_login_wrong_password(client_with_users):
    client, _ = client_with_users
    r = client.post("/api/auth/login", json={"username": "editor1", "password": "wrong"})
    assert r.status_code == 401


def test_role_change_revokes_old_role(client_with_users):
    client, store = client_with_users
    info = _login(client, "viewer1", "viewer-password-1")
    users_svc.set_role(store, "viewer1", "editor")
    # 安全设计：改角色即撤销旧会话（强制重新登录）
    assert client.get("/api/auth/me", headers=_auth(info["token"])).status_code == 401
    info2 = _login(client, "viewer1", "viewer-password-1")
    assert info2["role"] == "editor"


# ---- 角色方法矩阵 ----

def test_viewer_blocked_from_mutations(client_with_users):
    client, _ = client_with_users
    info = _login(client, "viewer1", "viewer-password-1")
    h = _auth(info["token"])
    assert client.get("/api/flows", headers=h).status_code == 200
    r = client.post("/api/credentials", headers=h,
                    json={"name": "x", "type": "generic", "data": {"a": 1}})
    assert r.status_code == 403  # admin 前缀 + 写方法


def test_editor_can_write_but_not_admin_prefix(client_with_users):
    client, _ = client_with_users
    info = _login(client, "editor1", "editor-password-1")
    h = _auth(info["token"])
    # 保存版本（editor 可）
    r = client.put("/api/flows/hello-plaita/versions/1.1.0", headers=h,
                   json={"definition": GOOD_DEF, "layout": "", "created_by": "editor1"})
    assert r.status_code == 200
    # admin 前缀（editor 不可）
    assert client.get("/api/users", headers=h).status_code == 403
    assert client.get("/api/audit", headers=h).status_code == 403


def test_unauthenticated_rejected(client):
    r = client.get("/api/secure")
    assert r.status_code == 401


def test_api_key_maps_to_admin(tmp_path, monkeypatch):
    app = _build_app(tmp_path, monkeypatch, admin_key="svc-key-1")
    client = TestClient(app)
    r = client.get("/api/secure", headers={"X-Admin-API-Key": "svc-key-1"})
    assert r.status_code == 200
    assert r.json() == {"actor": "api-key", "role": "admin"}


def test_prod_delete_requires_admin(tmp_path, monkeypatch):
    app = _build_app(tmp_path, monkeypatch, env="prod")
    examples_svc.seed_example_flows(flow_store.get_flow_store())
    users_svc.create_user(flow_store.get_flow_store(), "editor1", "editor-password-1", "editor")
    client = TestClient(app)
    info = _login(client, "editor1", "editor-password-1")
    r = client.delete("/api/flows/http-echo", headers=_auth(info["token"]))
    assert r.status_code == 403
    assert "生产环境" in r.json()["detail"]


# ---- 审计 ----

def test_audit_records_publish_and_credential(client_with_users, tmp_path):
    client, _ = client_with_users
    admin = _login(client, "admin", "admin-password-1")
    h = _auth(admin["token"])

    client.put("/api/flows/hello-plaita/versions/1.1.0", headers=h,
               json={"definition": GOOD_DEF, "layout": "", "created_by": "admin"})
    pr = client.post(f"/api/flows/hello-plaita/publish", headers=h,
                json={"version": "1.1.0"})
    print("PUBLISH-RESP", pr.status_code, pr.text[:200])
    client.post("/api/credentials", headers=h,
                json={"name": "c1", "type": "generic", "data": {"url": "https://x"}})

    logs = client.get("/api/audit", headers=h).json()["logs"]
    actions = [l["action"] for l in logs]
    assert "flow.save_version" in actions
    assert "flow.publish" in actions
    assert "credential.save" in actions
    # 审计不落机密：credential.save 的 detail 只有 type
    cred_log = next(l for l in logs if l["action"] == "credential.save")
    assert "data" not in (cred_log["detail"] or {})


# ---- 环境晋升 ----

def test_promotion_export_import_fingerprint(client_with_users, tmp_path, monkeypatch):
    client, store = client_with_users
    admin = _login(client, "admin", "admin-password-1")
    h = _auth(admin["token"])

    client.put("/api/flows/hello-plaita/versions/2.0.0", headers=h,
               json={"definition": GOOD_DEF, "layout": "", "created_by": "admin"})

    pkg = client.get("/api/flows/hello-plaita/versions/2.0.0/export", headers=h).json()
    assert pkg["kind"] == "plaita-promotion"
    assert pkg["definition_hash"]

    # 篡改定义 → 指纹校验拒绝
    bad = {**pkg, "definition": {"nodes": []}}
    r = client.post("/api/flows/import-version", headers=h, json={"package": bad})
    assert r.status_code == 400
    assert "指纹" in r.json()["detail"]

    # 原包导入为新版本草稿
    r = client.post("/api/flows/import-version", headers=h,
                    json={"package": pkg, "new_version": "2.0.1"})
    assert r.status_code == 200
    assert r.json()["status"] == "draft"

    # 发布导入版本 → 部署记录（本地模式 publish 跳过引擎同步）
    pr = client.post("/api/flows/hello-plaita/publish", headers=h, json={"version": "2.0.1"})
    assert pr.status_code == 200, pr.text

    deps = client.get("/api/deployments", headers=h).json()["deployments"]
    assert any(d["flow_id"] == "hello-plaita" and d["version"] == "2.0.1" for d in deps)


# ---- 首次启动向导 ----

def test_setup_wizard_flow(tmp_path, monkeypatch):
    """users 为空 → needs_setup=true → setup 创建 admin 并直接签发会话。"""
    monkeypatch.delenv("PLAITA_CONSOLE_ADMIN_PASSWORD", raising=False)
    app = _build_app(tmp_path, monkeypatch, with_user=False)
    client = TestClient(app)

    assert client.get("/api/auth/setup-status").json()["needs_setup"] is True

    # setup 创建 admin 并直接返回会话
    r = client.post("/api/auth/setup", json={"username": "boss", "password": "init-password-1"})
    assert r.status_code == 200
    body = r.json()
    assert body["username"] == "boss" and body["role"] == "admin"

    # 已初始化后：needs_setup=false，重复 setup 被拒
    assert client.get("/api/auth/setup-status").json()["needs_setup"] is False
    r = client.post("/api/auth/setup", json={"username": "evil", "password": "init-password-1"})
    assert r.status_code == 409

    # 用向导设的密码登录
    r = client.post("/api/auth/login", json={"username": "boss", "password": "init-password-1"})
    assert r.status_code == 200


def test_headless_bootstrap_still_works(tmp_path, monkeypatch):
    """无人值守：设置了 PLAITA_CONSOLE_ADMIN_PASSWORD → 启动即自动建 admin。"""
    monkeypatch.setenv("PLAITA_CONSOLE_ADMIN_PASSWORD", "headless-pw-1")
    app = _build_app(tmp_path, monkeypatch, with_user=False)
    store = flow_store.get_flow_store()
    assert users_svc.ensure_bootstrap_user(store) == "headless-pw-1"
    assert users_svc.login(store, "admin", "headless-pw-1") is not None
    # 已有用户 → 向导关闭
    client = TestClient(app)
    assert client.get("/api/auth/setup-status").json()["needs_setup"] is False
