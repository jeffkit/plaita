"""管理面鉴权单测（RBAC 语义）。

- API Key 命中 PLAITA_CONSOLE_ADMIN_API_KEY → admin（服务账号兼容）
- 会话 token（/api/auth/login 签发）→ 按 users 表角色授权
- 无任何凭据 → 401；ALLOW_INSECURE_ADMIN=true → 一律放行（仅本地开发）
"""
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from auth import require_auth  # noqa: E402
from services import flow_store, users_svc  # noqa: E402


def _build_app(tmp_path, monkeypatch, admin_key: str = "", insecure: bool = False,
               with_user: bool = True) -> FastAPI:
    monkeypatch.setenv("PLAITA_CONSOLE_ADMIN_API_KEY", admin_key)
    monkeypatch.setenv("PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN", "true" if insecure else "false")
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/auth.db")
    flow_store.init_engine(f"sqlite:///{tmp_path}/auth.db")
    store = flow_store.get_flow_store()
    if with_user:
        users_svc.create_user(store, "admin", "admin-password-1", "admin")

    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = False
    app.state.store = store

    @app.get("/api/secure", dependencies=[Depends(require_auth)])
    def secure(identity: dict = Depends(require_auth)):
        return {"ok": True, "role": identity["role"]}

    return app


def test_missing_key_401(tmp_path, monkeypatch):
    app = _build_app(tmp_path, monkeypatch, with_user=False)
    c = TestClient(app)
    assert c.get("/api/secure").status_code == 401


def test_wrong_key_401(tmp_path, monkeypatch):
    app = _build_app(tmp_path, monkeypatch, admin_key="test-admin-key")
    c = TestClient(app)
    assert c.get("/api/secure", headers={"X-Admin-API-Key": "wrong"}).status_code == 401


def test_valid_header_200(tmp_path, monkeypatch):
    app = _build_app(tmp_path, monkeypatch, admin_key="test-admin-key")
    c = TestClient(app)
    r = c.get("/api/secure", headers={"X-Admin-API-Key": "test-admin-key"})
    assert r.status_code == 200
    assert r.json() == {"ok": True, "role": "admin"}


def test_bearer_200(tmp_path, monkeypatch):
    app = _build_app(tmp_path, monkeypatch, admin_key="test-admin-key")
    c = TestClient(app)
    r = c.get("/api/secure", headers={"Authorization": "Bearer test-admin-key"})
    assert r.status_code == 200


def test_unauth_401_even_without_key(tmp_path, monkeypatch):
    """新语义：无 key 时走会话认证；无会话 → 401（不再是 503）。"""
    app = _build_app(tmp_path, monkeypatch, with_user=False)
    c = TestClient(app)
    assert c.get("/api/secure").status_code == 401


def test_insecure_opt_in_allows(tmp_path, monkeypatch):
    app = _build_app(tmp_path, monkeypatch, insecure=True, with_user=False)
    c = TestClient(app)
    assert c.get("/api/secure").status_code == 200


def test_session_token_grants_role(tmp_path, monkeypatch):
    app = _build_app(tmp_path, monkeypatch, with_user=True)
    c = TestClient(app)
    info = users_svc.login(flow_store.get_flow_store(), "admin", "admin-password-1")
    assert info is not None
    r = c.get("/api/secure", headers={"Authorization": f"Bearer {info['token']}"})
    assert r.status_code == 200
    assert r.json()["role"] == "admin"
