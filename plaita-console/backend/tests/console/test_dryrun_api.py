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


def test_dry_run_hierarchy_inline_child(client: TestClient):
    """子图层级：inline child 内的节点 depth=1、flow_path 两级，根层节点 depth=0。"""
    flow = json.dumps(
        {
            "flow_id": "main",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "c1"},
                {
                    "type": "child",
                    "id": "c1",
                    "childFlow": {
                        "nodes": [
                            {"type": "start", "id": "cs", "next": "ca"},
                            {"type": "assignment", "id": "ca", "output": {"v": 1}, "next": "ce"},
                            {"type": "end", "id": "ce", "resultType": "success", "output": "$NODE.ca"},
                        ]
                    },
                    "next": "end",
                },
                {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.c1"},
            ],
        }
    )
    r = client.post("/api/flows/dry-run", json={"flowJson": flow, "input": {}})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["error"] is None
    assert body["result"] == {"v": 1}
    by_id = {n["id"]: n for n in body["nodes"]}
    # 根层
    for nid in ("start", "c1", "end"):
        assert by_id[nid]["depth"] == 0, (nid, by_id[nid])
        assert by_id[nid]["flow_path"] == ["main"]
        assert by_id[nid]["flow_id"] == "main"
    # 子流程层：标签取启动节点名，flow_id 为空（内联子流程无 id）
    for nid in ("cs", "ca"):
        assert by_id[nid]["depth"] == 1, (nid, by_id[nid])
        assert by_id[nid]["flow_path"] == ["main", "c1"]
        assert by_id[nid]["flow_id"] is None


def test_dry_run_hierarchy_parallel_branches(client: TestClient):
    """子图层级：parallel 分支 flow 在 thread 池 worker 里启动，两个兄弟分支
    必须归到共同父（depth=1），而不是经爬链失败互相嵌套（depth=2）。"""
    flow = json.dumps(
        {
            "flow_id": "par",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "p"},
                {
                    "type": "parallel",
                    "id": "p",
                    "branches": [
                        {
                            "name": "b1",
                            "flow": {"nodes": [
                                {"type": "start", "id": "b1s", "next": "b1a"},
                                {"type": "assignment", "id": "b1a", "output": {"w": "b1"}, "next": "b1e"},
                                {"type": "end", "id": "b1e", "resultType": "success", "output": "$NODE.b1a"},
                            ]},
                        },
                        {
                            "name": "b2",
                            "flow": {"nodes": [
                                {"type": "start", "id": "b2s", "next": "b2a"},
                                {"type": "assignment", "id": "b2a", "output": {"w": "b2"}, "next": "b2e"},
                                {"type": "end", "id": "b2e", "resultType": "success", "output": "$NODE.b2a"},
                            ]},
                        },
                    ],
                    "next": "end",
                },
                {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.p"},
            ],
        }
    )
    r = client.post("/api/flows/dry-run", json={"flowJson": flow, "input": {}})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["error"] is None, body["error"]
    by_id = {n["id"]: n for n in body["nodes"]}
    for nid in ("start", "p", "end"):
        assert by_id[nid]["depth"] == 0, (nid, by_id[nid])
        assert by_id[nid]["flow_path"] == ["par"]
    # 兄弟分支同层：都归到 parallel 节点之下的 depth=1
    for nid in ("b1s", "b1a", "b2s", "b2a"):
        assert by_id[nid]["depth"] == 1, (nid, by_id[nid])
        assert by_id[nid]["flow_path"] == ["par", "p"], (nid, by_id[nid])
        assert by_id[nid]["flow_id"] is None
    # 分支真实执行了（thread 池不丢结果）
    assert by_id["b1a"]["output"] == {"w": "b1"}
    assert by_id["b2a"]["output"] == {"w": "b2"}


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
