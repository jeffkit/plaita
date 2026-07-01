"""
节点管理 API 单测：列出内置节点、注册自定义、冲突校验、删除、内置不可删。
"""
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import nodes as nodes_api  # noqa: E402
from services import flow_store  # noqa: E402


@pytest.fixture()
def client(tmp_path) -> TestClient:
    db_file = tmp_path / "nodes.db"
    flow_store.init_engine(f"sqlite:///{db_file}")
    app = FastAPI()
    app.include_router(nodes_api.router, prefix="/api")
    return TestClient(app)


def test_list_nodes_includes_builtins(client: TestClient):
    r = client.get("/api/nodes")
    assert r.status_code == 200
    body = r.json()
    types = {n["node_type"] for n in body["nodes"]}
    assert {"start", "end", "http", "if", "loop"}.issubset(types)
    assert body["total"] == len(body["nodes"])
    # 内置项标记正确
    http_node = next(n for n in body["nodes"] if n["node_type"] == "http")
    assert http_node["is_builtin"] is True
    assert http_node["node_name"]


def test_register_custom_then_list(client: TestClient):
    payload = {
        "node_type": "myNode",
        "node_name": "我的节点",
        "category": "自定义",
        "schema_json": '{"properties": {"x": {"type": "string"}}}',
    }
    r = client.post("/api/nodes", json=payload)
    assert r.status_code == 200, r.text
    assert r.json()["node_type"] == "myNode"
    assert r.json()["is_builtin"] is False

    # 列表中可见
    r2 = client.get("/api/nodes")
    types = {n["node_type"] for n in r2.json()["nodes"]}
    assert "myNode" in types


def test_register_conflicts_with_builtin(client: TestClient):
    r = client.post("/api/nodes", json={"node_type": "http", "node_name": "x"})
    assert r.status_code == 400


def test_register_invalid_schema_json(client: TestClient):
    r = client.post(
        "/api/nodes",
        json={"node_type": "bad", "schema_json": "not-json{"},
    )
    assert r.status_code == 400


def test_delete_custom(client: TestClient):
    client.post("/api/nodes", json={"node_type": "tmp", "node_name": "t"})
    r = client.delete("/api/nodes/tmp")
    assert r.status_code == 200
    assert r.json()["success"] is True


def test_delete_builtin_forbidden(client: TestClient):
    r = client.delete("/api/nodes/http")
    assert r.status_code == 400


def test_delete_nonexistent_404(client: TestClient):
    r = client.delete("/api/nodes/nope")
    assert r.status_code == 404


def test_register_upsert_updates(client: TestClient):
    client.post("/api/nodes", json={"node_type": "up", "node_name": "v1"})
    client.post("/api/nodes", json={"node_type": "up", "node_name": "v2"})
    r = client.get("/api/nodes")
    up = next(n for n in r.json()["nodes"] if n["node_type"] == "up")
    assert up["node_name"] == "v2"
