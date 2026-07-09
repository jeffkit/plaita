"""
dry-run API 单测：echo flow、含 if 分支的 flow、非法定义。
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

from api import dryrun as dryrun_api  # noqa: E402
from services import flow_store  # noqa: E402


@pytest.fixture()
def client(tmp_path) -> TestClient:
    flow_store.init_engine(f"sqlite:///{tmp_path / 'dry.db'}")
    app = FastAPI()
    app.include_router(dryrun_api.router, prefix="/api")
    return TestClient(app)


def _echo_flow() -> str:
    return json.dumps(
        {
            "flow_id": "echo",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success"},
            ],
        }
    )


def _if_flow() -> str:
    return json.dumps(
        {
            "flow_id": "if_flow",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "check"},
                {
                    "type": "if",
                    "id": "check",
                    "condition": {"field": "$INPUT.age", "operator": "gte", "value": 18},
                    "next": "adult",
                    "else_next": "minor",
                },
                {"type": "end", "id": "adult", "output": "adult", "resultType": "success"},
                {"type": "end", "id": "minor", "output": "minor", "resultType": "success"},
            ],
        }
    )


def test_dry_run_echo(client: TestClient):
    r = client.post("/api/flows/dry-run", json={"flowJson": _echo_flow(), "input": {"name": "kongjie"}})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["error"] is None
    assert body["result"] == "kongjie"
    ids = [n["id"] for n in body["nodes"]]
    assert ids == ["start", "end"]
    assert all(n["status"] == "success" for n in body["nodes"])


def test_dry_run_if_branch(client: TestClient):
    r = client.post("/api/flows/dry-run", json={"flowJson": _if_flow(), "input": {"age": 20}})
    body = r.json()
    assert body["result"] == "adult"

    r2 = client.post("/api/flows/dry-run", json={"flowJson": _if_flow(), "input": {"age": 10}})
    assert r2.json()["result"] == "minor"


def test_dry_run_invalid_json(client: TestClient):
    r = client.post("/api/flows/dry-run", json={"flowJson": "not-json{"})
    assert r.status_code == 200
    assert r.json()["error"] is not None
    assert r.json()["nodes"] == []


def test_dry_run_invalid_flow(client: TestClient):
    bad = json.dumps({"flow_id": "x", "nodes": [{"type": "no_such_type", "id": "n"}]})
    r = client.post("/api/flows/dry-run", json={"flowJson": bad})
    assert r.status_code == 200
    assert r.json()["error"] is not None


def test_dry_run_blocks_code_node(client: TestClient):
    """code 节点不得在 console backend 进程内执行（RCE 闸门）。"""
    flow = json.dumps(
        {
            "flow_id": "evil",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "c"},
                {
                    "type": "code",
                    "id": "c",
                    "language": "python",
                    "code": "print(1)",
                    "next": "end",
                },
                {"type": "end", "id": "end", "output": "x", "resultType": "success"},
            ],
        }
    )
    r = client.post("/api/flows/dry-run", json={"flowJson": flow, "input": {}})
    assert r.status_code == 200
    body = r.json()
    assert body["result"] is None
    assert body["nodes"] == []
    assert "危险节点" in body["error"]
    assert "code" in body["error"]
