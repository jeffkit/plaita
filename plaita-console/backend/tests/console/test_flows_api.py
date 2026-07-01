"""
流程 CRUD + 版本 + 发布 API 单测。
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import flows as flows_api  # noqa: E402
from services import flow_store  # noqa: E402


def _echo_def(flow_id: str = "echo") -> str:
    return json.dumps(
        {
            "flow_id": flow_id,
            "desc": "echo",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success"},
            ],
        }
    )


@pytest.fixture()
def client(tmp_path) -> TestClient:
    flow_store.init_engine(f"sqlite:///{tmp_path / 'flows.db'}")
    app = FastAPI()
    app.include_router(flows_api.router, prefix="/api")
    return TestClient(app)


def test_create_list_get_delete(client: TestClient):
    r = client.post("/api/flows", json={"flow_id": "echo", "desc": "回声"})
    assert r.status_code == 200, r.text
    assert r.json()["flow_id"] == "echo"

    # 重复创建 409
    assert client.post("/api/flows", json={"flow_id": "echo"}).status_code == 409

    # 列表
    r = client.get("/api/flows")
    assert r.status_code == 200
    assert any(f["flow_id"] == "echo" for f in r.json()["flows"])

    # 详情
    r = client.get("/api/flows/echo")
    assert r.status_code == 200
    assert r.json()["versions"] == []

    # 不存在 404
    assert client.get("/api/flows/nope").status_code == 404

    # 删除
    assert client.delete("/api/flows/echo").status_code == 200
    assert client.get("/api/flows/echo").status_code == 404


def test_save_version_validates_and_returns(client: TestClient):
    client.post("/api/flows", json={"flow_id": "echo"})
    r = client.put(
        "/api/flows/echo/versions/0.0.1",
        json={"definition": _echo_def(), "layout": '{"start": {"x": 0, "y": 0}}'},
    )
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "draft"
    assert json.loads(r.json()["definition"])["flow_id"] == "echo"

    # 回读
    got = client.get("/api/flows/echo/versions/0.0.1").json()
    assert got["version"] == "0.0.1"
    assert json.loads(got["layout"])["start"]["x"] == 0


def test_save_invalid_definition_422(client: TestClient):
    client.post("/api/flows", json={"flow_id": "echo"})
    # 含未注册节点类型的非法 Flow
    bad = json.dumps({"flow_id": "echo", "nodes": [{"type": "no_such_type", "id": "x"}]})
    r = client.put("/api/flows/echo/versions/0.0.1", json={"definition": bad})
    assert r.status_code == 422


def test_save_non_json_definition_422(client: TestClient):
    client.post("/api/flows", json={"flow_id": "echo"})
    r = client.put("/api/flows/echo/versions/0.0.1", json={"definition": "not-json{"})
    assert r.status_code == 422


def test_save_bad_semver_422(client: TestClient):
    client.post("/api/flows", json={"flow_id": "echo"})
    r = client.put("/api/flows/echo/versions/v1", json={"definition": _echo_def()})
    assert r.status_code == 422


def test_save_unknown_flow_404(client: TestClient):
    r = client.put("/api/flows/ghost/versions/0.0.1", json={"definition": _echo_def()})
    assert r.status_code == 404


def test_publish_then_cannot_overwrite(client: TestClient):
    client.post("/api/flows", json={"flow_id": "echo"})
    client.put("/api/flows/echo/versions/1.0.0", json={"definition": _echo_def()})
    r = client.post("/api/flows/echo/publish", json={"version": "1.0.0"})
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "published"

    # 已发布版本不可覆盖
    r = client.put("/api/flows/echo/versions/1.0.0", json={"definition": _echo_def()})
    assert r.status_code == 409


def test_publish_nonexistent_404(client: TestClient):
    client.post("/api/flows", json={"flow_id": "echo"})
    assert client.post("/api/flows/echo/publish", json={"version": "9.9.9"}).status_code == 404


def test_delete_version(client: TestClient):
    client.post("/api/flows", json={"flow_id": "echo"})
    client.put("/api/flows/echo/versions/0.0.1", json={"definition": _echo_def()})
    assert client.delete("/api/flows/echo/versions/0.0.1").status_code == 200
    assert client.get("/api/flows/echo/versions/0.0.1").status_code == 404
