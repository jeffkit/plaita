"""
自定义属性类型 + 凭据模板 API 单测（2026-09 节点管理重设计 C2/C3）。
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
from api import property_types as property_types_api  # noqa: E402
from api import credential_templates as credential_templates_api  # noqa: E402
from services import flow_store  # noqa: E402


@pytest.fixture()
def client(tmp_path) -> TestClient:
    db_file = tmp_path / "ptypes.db"
    flow_store.init_engine(f"sqlite:///{db_file}")
    app = FastAPI()
    app.include_router(nodes_api.router, prefix="/api")
    app.include_router(property_types_api.router, prefix="/api")
    app.include_router(credential_templates_api.router, prefix="/api")
    return TestClient(app)


def test_property_type_crud_roundtrip(client: TestClient):
    # 注册
    r = client.post(
        "/api/property-types",
        json={"name": "email", "base_type": "string", "desc": "邮箱地址"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["name"] == "email"

    # 列表可见
    r = client.get("/api/property-types")
    names = [t["name"] for t in r.json()["types"]]
    assert "email" in names

    # upsert 更新（同 name）
    r = client.post(
        "/api/property-types",
        json={"name": "email", "base_type": "string", "enum_options": ["a@x.com"], "desc": "限定枚举"},
    )
    assert r.status_code == 200
    assert r.json()["enum_options"] == ["a@x.com"]
    assert len(client.get("/api/property-types").json()["types"]) == 1

    # 删除
    assert client.delete("/api/property-types/email").json()["success"] is True
    assert client.delete("/api/property-types/email").status_code == 404


def test_property_type_rejects_non_builtin_base(client: TestClient):
    """base_type 必须是运行时内置类型——自定义类型只是命名别名，运行时
    Property.match 不认识未知类型名，绝不能把自定义名再当 base 存进去。"""
    r = client.post("/api/property-types", json={"name": "bad", "base_type": "email"})
    assert r.status_code == 400
    r = client.post("/api/property-types", json={"name": "", "base_type": "string"})
    assert r.status_code == 400


def test_builtin_nodes_expose_source_location(client: TestClient):
    """内置节点透出 Python 代码位置（模块路径 + 类名）；自定义节点为空。"""
    r = client.get("/api/nodes")
    by_type = {n["node_type"]: n for n in r.json()["nodes"]}
    http = by_type["http"]
    assert http["is_builtin"] is True
    assert http["source_module"].startswith("plaita")
    assert http["source_class"], "内置节点应有类名"

    r = client.post(
        "/api/nodes",
        json={"node_type": "my_meta_node", "node_name": "元数据节点", "schema_json": "{}"},
    )
    assert r.status_code == 200
    custom = r.json()
    assert custom["is_builtin"] is False
    assert custom["source_module"] == "" and custom["source_class"] == ""


def test_credential_templates_readonly(client: TestClient):
    r = client.get("/api/credential-templates")
    assert r.status_code == 200
    body = r.json()
    types = [t["type"] for t in body["templates"]]
    assert "bearer" in types and "database" in types
    for t in body["templates"]:
        for f in t["fields"]:
            assert set(f) >= {"key", "label", "input_type", "required", "secret"}
