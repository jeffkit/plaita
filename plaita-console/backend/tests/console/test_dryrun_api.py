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


def test_dry_run_pinned_skips_real_execution(client: TestClient):
    """pin 住 http 节点后，试跑不再真实发请求，输出等于固定值。"""
    flow = {
        "nodes": [
            {"type": "start", "id": "start", "next": "fetch"},
            {"type": "http", "id": "fetch", "method": "GET",
             "url": "http://127.0.0.1:1/unreachable", "next": "end"},
            {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.fetch"},
        ]
    }
    r = client.post(
        "/api/flows/dry-run",
        json={"flowJson": json.dumps(flow), "input": {},
              "pinned": {"fetch": {"status": 200, "data": {"hello": "pinned"}}}},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["error"] is None
    by_id = {n["id"]: n for n in body["nodes"]}
    assert by_id["fetch"]["type"] == "mock"          # 被替换为 mock，未真实请求
    assert by_id["fetch"]["output"] == {"status": 200, "data": {"hello": "pinned"}}
    assert body["result"] == {"status": 200, "data": {"hello": "pinned"}}


def test_dry_run_only_node_executes_single_node(client: TestClient):
    """only_node：其余节点 mock 化，目标节点真实执行，下游无副作用。"""
    flow = {
        "nodes": [
            {"type": "start", "id": "start", "next": "a"},
            {"type": "assignment", "id": "a", "output": {"v": 1}, "next": "b"},
            {"type": "assignment", "id": "b", "output": {"v": 2}, "next": "end"},
            {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.b"},
        ]
    }
    r = client.post(
        "/api/flows/dry-run",
        json={"flowJson": json.dumps(flow), "input": {},
              "pinned": {"a": {"v": 99}}, "onlyNode": "b"},
    )
    assert r.status_code == 200
    body = r.json()
    by_id = {n["id"]: n for n in body["nodes"]}
    assert by_id["a"]["type"] == "mock"                      # 上游被 pin → mock
    assert by_id["a"]["output"] == {"v": 99}
    assert by_id["b"]["type"] == "assignment"                # 目标真实执行
    assert by_id["b"]["output"] == {"v": 2}
    assert by_id["end"]["type"] == "mock"                    # 下游无副作用


class TestDryrunScanParallelBranches:
    """安全回归（2026-09 评审 P0-2）：嵌套在 parallel.branches[].flow 里的
    危险节点必须被闸门扫描到——历史上只沿固定键下钻，分支流完全漏扫。"""

    def test_code_node_in_parallel_branch_is_blocked(self):
        from services.dryrun import _collect_blocked_nodes

        flow = {
            "nodes": [
                {"type": "start", "id": "s", "next": "p"},
                {"type": "parallel", "id": "p", "branches": [
                    {"name": "b0", "flow": {"nodes": [
                        {"type": "code", "id": "evil", "next": "e2",
                         "code": "def run(i): import os; return os.popen('id').read()"},
                        {"type": "end", "id": "e2"},
                    ]}},
                ], "next": "end"},
                {"type": "end", "id": "end"},
            ]
        }
        blocked = _collect_blocked_nodes(flow)
        assert any("code" in b for b in blocked), blocked

    def test_code_node_in_child_flow_still_blocked(self):
        from services.dryrun import _collect_blocked_nodes

        flow = {
            "nodes": [
                {"type": "child", "id": "c",
                 "childFlow": {"nodes": [{"type": "code", "id": "evil", "code": "def run(i): return 1"}]}},
            ]
        }
        blocked = _collect_blocked_nodes(flow)
        assert any("code" in b for b in blocked), blocked
