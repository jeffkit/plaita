"""
版本 codeflow 源码视图 API 单测（GET /flows/{id}/versions/{ver}/source）。

钉三层行为：
1. authoritative——definition.metadata.source 原样透传（plaita build
   --embed-source / mediaflow publish_console 注入的仓内权威源码）；
2. decompiled——无内嵌源码时 emit_source 反编译兜底（语义等效非权威）；
3. unavailable——不可表达的构造如实回 reason，不 500。
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


def _authored_def(flow_id: str = "authored") -> str:
    d = json.loads(_echo_def(flow_id))
    d["metadata"] = {"source": "def x(INPUT):\n    return INPUT.name\n",
                     "source_format": "plaita@flow"}
    return json.dumps(d)


@pytest.fixture()
def client(tmp_path) -> TestClient:
    flow_store.init_engine(f"sqlite:///{tmp_path / 'flows.db'}")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = flow_store.get_flow_store()
    app.include_router(flows_api.router, prefix="/api")
    return TestClient(app)


def _put_version(client: TestClient, flow_id: str, definition: str, version: str = "0.0.1"):
    client.post(f"/api/flows", json={"flow_id": flow_id})
    r = client.put(f"/api/flows/{flow_id}/versions/{version}", json={"definition": definition})
    assert r.status_code == 200, r.text


def test_authoritative_from_metadata(client: TestClient):
    _put_version(client, "authored", _authored_def())
    r = client.get("/api/flows/authored/versions/0.0.1/source")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["source_kind"] == "authoritative"
    assert body["source"] == "def x(INPUT):\n    return INPUT.name\n"


def test_decompiled_fallback_without_metadata(client: TestClient):
    _put_version(client, "echo", _echo_def())
    r = client.get("/api/flows/echo/versions/0.0.1/source")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["source_kind"] == "decompiled"
    # 反编译产物是可读 @flow 源码且可回编译（语义等价；slug 派生 id 允许
    # 变化——_emit 契约即「归一化 IR 相等」，end 的 slug id 往返后变 ret_name）
    assert "return" in body["source"]
    from plaita.dsl.codeflow import compile_source

    back = compile_source(body["source"], flow_id="echo")
    assert sorted(n["type"] for n in back["nodes"]) == ["end", "start"]
    end = [n for n in back["nodes"] if n["type"] == "end"][0]
    assert end["output"] == "$INPUT.name"


def test_unavailable_reports_reason_not_500(client: TestClient):
    # 非 object 的 inputType 是 emit_source 明确不支持的构造（EmitError）
    d = json.loads(_echo_def("weird"))
    d["inputType"] = {"dataType": "string"}
    _put_version(client, "weird", json.dumps(d))
    r = client.get("/api/flows/weird/versions/0.0.1/source")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["source_kind"] == "unavailable"
    assert body["source"] is None
    assert "emit_source" in body.get("reason", "")


def test_404_for_missing_version(client: TestClient):
    client.post("/api/flows", json={"flow_id": "echo"})
    r = client.get("/api/flows/echo/versions/9.9.9/source")
    assert r.status_code == 404
