"""
HMAC 对外契约接口单测：用 plaita.client.generate_signature 产真实签名做对称验证。
"""
import json
import sys
from pathlib import Path
from time import time

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import flow_version as fv  # noqa: E402
from services import flow_store  # noqa: E402
from plaita.client import generate_signature  # noqa: E402

SECRET_ID = "test-id"
SECRET_KEY = "test-secret-key"


def _echo_def(flow_id: str = "echo") -> str:
    return json.dumps(
        {
            "flow_id": flow_id,
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success"},
            ],
        }
    )


@pytest.fixture()
def client(tmp_path, monkeypatch) -> TestClient:
    monkeypatch.setenv("PLAITA_CONSOLE_SECRET_ID", SECRET_ID)
    monkeypatch.setenv("PLAITA_CONSOLE_SECRET_KEY", SECRET_KEY)
    flow_store.init_engine(f"sqlite:///{tmp_path / 'fv.db'}")
    app = FastAPI()
    app.include_router(fv.router, prefix="/api")
    return TestClient(app)


def _auth(validity: int = 3, secret_id: str = SECRET_ID, secret_key: str = SECRET_KEY) -> str:
    return generate_signature(secret_key, secret_id, validity, int(time()))


def test_valid_signature_returns_published_flow(client: TestClient):
    # 准备已发布 flow
    flow_store.get_flow_store().create_flow("echo")
    flow_store.get_flow_store().save_flow_definition(
        "echo", "1.0.0", _echo_def(), "{}"
    )
    flow_store.get_flow_store().publish_version("echo", "1.0.0")

    r = client.post(
        "/api/flowVersion/semver/detail",
        headers={"Authorization": _auth()},
        data={"flowId": "echo", "version": "1.0.0"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["code"] == 0
    assert json.loads(body["data"]["flow"])["flow_id"] == "echo"


def test_missing_auth_401(client: TestClient):
    r = client.post(
        "/api/flowVersion/semver/detail",
        data={"flowId": "echo", "version": "1.0.0"},
    )
    assert r.status_code == 401


def test_wrong_secret_401(client: TestClient):
    r = client.post(
        "/api/flowVersion/semver/detail",
        headers={"Authorization": _auth(secret_key="wrong")},
        data={"flowId": "echo", "version": "1.0.0"},
    )
    assert r.status_code == 401


def test_expired_signature_401(client: TestClient):
    # sign-time 设为很久以前，已过期
    from plaita.client import generate_signature as gs
    auth = gs(SECRET_KEY, SECRET_ID, 3, int(time()) - 100)
    r = client.post(
        "/api/flowVersion/semver/detail",
        headers={"Authorization": auth},
        data={"flowId": "echo", "version": "1.0.0"},
    )
    assert r.status_code == 401


def test_nonexistent_flow_returns_nonzero_code(client: TestClient):
    r = client.post(
        "/api/flowVersion/semver/detail",
        headers={"Authorization": _auth()},
        data={"flowId": "ghost", "version": "1.0.0"},
    )
    assert r.status_code == 200
    assert r.json()["code"] != 0


def test_unpublished_version_returns_nonzero_code(client: TestClient):
    flow_store.get_flow_store().create_flow("echo")
    flow_store.get_flow_store().save_flow_definition("echo", "0.0.1", _echo_def(), "{}")
    # 未发布
    r = client.post(
        "/api/flowVersion/semver/detail",
        headers={"Authorization": _auth()},
        data={"flowId": "echo", "version": "0.0.1"},
    )
    assert r.status_code == 200
    assert r.json()["code"] != 0
