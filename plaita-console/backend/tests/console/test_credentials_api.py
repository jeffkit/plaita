"""凭据管理 API 单测：加密落库、导出引擎可读文件、列表不含明文、删除。"""
import json
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import credentials as credentials_api  # noqa: E402
from services import credentials_svc, flow_store  # noqa: E402


@pytest.fixture()
def client(tmp_path, monkeypatch) -> TestClient:
    db_file = tmp_path / "cred.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    flow_store.init_engine(f"sqlite:///{db_file}")
    app = FastAPI()
    app.include_router(credentials_api.router, prefix="/api")
    return TestClient(app)


def test_save_list_delete_roundtrip(client: TestClient, tmp_path):
    r = client.post("/api/credentials", json={
        "name": "feishu-bot",
        "type": "webhook",
        "data": {"url": "https://example.com/hook", "secret": "s3cret"},
        "desc": "飞书机器人",
    })
    assert r.status_code == 200
    assert r.json()["success"] is True

    # 列表不含明文
    items = client.get("/api/credentials").json()["credentials"]
    assert len(items) == 1
    assert items[0]["name"] == "feishu-bot"
    assert "s3cret" not in json.dumps(items)

    # 详情返回明文（管理员编辑回填用）
    detail = client.get("/api/credentials/feishu-bot").json()
    assert detail["data"]["secret"] == "s3cret"

    # 引擎可读导出文件存在且密文不落明文
    cred_file = Path(tmp_path / "creds.json")
    assert cred_file.is_file()
    raw = cred_file.read_text()
    assert "s3cret" not in raw

    # 引擎侧解密读取
    monkey_patch_env(client, tmp_path)
    from plaita.credentials import get_credential
    assert get_credential("feishu-bot")["url"] == "https://example.com/hook"

    # 删除
    assert client.delete("/api/credentials/feishu-bot").status_code == 200
    assert client.get("/api/credentials/feishu-bot").status_code == 404
    assert json.loads(cred_file.read_text()) == {}


def monkey_patch_env(client, tmp_path):
    import os
    os.environ["PLAITA_CREDENTIALS_FILE"] = str(tmp_path / "creds.json")
    os.environ["PLAITA_CREDENTIALS_KEY_FILE"] = str(tmp_path / "creds.key")


def test_invalid_data_rejected(client: TestClient):
    r = client.post("/api/credentials", json={"name": "bad", "type": "generic", "data": {}})
    assert r.status_code == 400
