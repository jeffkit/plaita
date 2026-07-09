"""管理面 Admin API Key 鉴权单测。"""
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from auth import require_admin_auth  # noqa: E402


@pytest.fixture()
def app_with_admin(monkeypatch) -> FastAPI:
    monkeypatch.setenv("PLAITA_CONSOLE_ADMIN_API_KEY", "test-admin-key")
    monkeypatch.delenv("PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN", raising=False)
    app = FastAPI()

    @app.get("/api/secure", dependencies=[Depends(require_admin_auth)])
    def secure():
        return {"ok": True}

    return app


def test_missing_key_401(app_with_admin: FastAPI):
    c = TestClient(app_with_admin)
    assert c.get("/api/secure").status_code == 401


def test_wrong_key_401(app_with_admin: FastAPI):
    c = TestClient(app_with_admin)
    r = c.get("/api/secure", headers={"X-Admin-API-Key": "wrong"})
    assert r.status_code == 401


def test_valid_header_200(app_with_admin: FastAPI):
    c = TestClient(app_with_admin)
    r = c.get("/api/secure", headers={"X-Admin-API-Key": "test-admin-key"})
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_bearer_200(app_with_admin: FastAPI):
    c = TestClient(app_with_admin)
    r = c.get("/api/secure", headers={"Authorization": "Bearer test-admin-key"})
    assert r.status_code == 200


def test_empty_key_fail_closed_503(monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_ADMIN_API_KEY", "")
    monkeypatch.setenv("PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN", "false")
    app = FastAPI()

    @app.get("/api/secure", dependencies=[Depends(require_admin_auth)])
    def secure():
        return {"ok": True}

    c = TestClient(app)
    assert c.get("/api/secure").status_code == 503


def test_insecure_opt_in_allows(monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_ADMIN_API_KEY", "")
    monkeypatch.setenv("PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN", "true")
    app = FastAPI()

    @app.get("/api/secure", dependencies=[Depends(require_admin_auth)])
    def secure():
        return {"ok": True}

    c = TestClient(app)
    assert c.get("/api/secure").status_code == 200
