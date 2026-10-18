"""plaita#190：试跑闸门改为「内置节点能力白名单」，http 节点不得真实出网。

历史缺陷（#32 残面）：
- 闸门只有名黑名单 ``{code,python,javascript,js}``——漏了内置 ``http``；
- 内置 HTTP 节点不读 ``dry_run``，``handle_request_async`` 正常拿 session 发请求。

于是 editor 试跑一条含 http 节点的 flow，console backend 进程会真实出网
（进程内 SSRF：云元数据 / 内网管理面），响应体回显到试跑面板。

本组断言：
1. 含 http 节点的 flow 被拒绝，且点名节点路径；本地探针服务器**零请求**；
2. HTTP 节点自身在 ``dry_run`` 下短路（纵深防御：不经 console 闸门也不出网）；
3. 其它内置副作用节点（redis / event）同样被默认拒绝；
4. 嵌套（childFlow / parallel 分支）里的 http 也被扫到；
5. 纯控制流节点（start/if/end）试跑结果与现状一致；
6. 经注册表动态注册的业务节点仍可试跑（按 ``dry_run`` 契约自守，#32 不回退）；
7. 未知类型仍走 Flow 校验错误（不被白名单提前改写语义）。
"""
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, ClassVar

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import dryrun as dryrun_api  # noqa: E402
from services import dryrun as dryrun_svc  # noqa: E402
from services import flow_store  # noqa: E402

from plaita.core.flow import Flow  # noqa: E402
from plaita.node import Node, get_default_registry  # noqa: E402


class _ProbeHandler(BaseHTTPRequestHandler):
    hits: list = []

    def do_GET(self):  # noqa: N802 — http.server 接口
        type(self).hits.append(self.path)
        body = json.dumps({"secret": "internal-metadata"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        return


@pytest.fixture()
def http_probe():
    _ProbeHandler.hits = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ProbeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture()
def client(tmp_path) -> TestClient:
    flow_store.init_engine(f"sqlite:///{tmp_path / 'issue190.db'}")
    app = FastAPI()
    app.include_router(dryrun_api.router, prefix="/api")
    return TestClient(app)


def _http_flow(url: str, *, node_id: str = "fetch") -> dict:
    return {
        "flow_id": "issue190-http",
        "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": node_id},
            {"type": "http", "id": node_id, "method": "GET", "url": url, "next": "end"},
            {"type": "end", "id": "end", "output": "$NODE." + node_id, "resultType": "success"},
        ],
    }


def _dry(client: TestClient, flow: dict) -> dict:
    r = client.post("/api/flows/dry-run", json={"flowJson": json.dumps(flow), "input": {}})
    assert r.status_code == 200, r.text
    return r.json()


class TestHttpNodeBlocked:
    def test_http_flow_rejected_and_probe_gets_zero_requests(self, client, http_probe):
        body = _dry(client, _http_flow(http_probe))
        assert body["result"] is None
        assert body["nodes"] == []
        assert "nodes[fetch].type=http" in body["error"]
        assert _ProbeHandler.hits == [], "试跑不得让 http 节点真实出网"

    def test_http_inside_child_flow_is_scanned(self, client, http_probe):
        flow = {
            "flow_id": "issue190-nested",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "c"},
                {
                    "type": "child",
                    "id": "c",
                    "input": {},
                    "childFlow": {"nodes": _http_flow(http_probe, node_id="inner_http")["nodes"]},
                    "next": "end",
                },
                {"type": "end", "id": "end", "output": "ok", "resultType": "success"},
            ],
        }
        body = _dry(client, flow)
        assert body["result"] is None
        assert "inner_http" in body["error"] and "http" in body["error"]
        assert _ProbeHandler.hits == []

    def test_http_inside_parallel_branch_is_scanned(self, client, http_probe):
        flow = {
            "flow_id": "issue190-parallel",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "p"},
                {
                    "type": "parallel",
                    "id": "p",
                    "branches": [
                        {"name": "main", "flow": {"nodes": _http_flow(http_probe, node_id="branch_http")["nodes"]}},
                    ],
                    "next": "end",
                },
                {"type": "end", "id": "end", "output": "ok", "resultType": "success"},
            ],
        }
        body = _dry(client, flow)
        assert body["result"] is None
        assert "branch_http" in body["error"] and "http" in body["error"]
        assert _ProbeHandler.hits == []


class TestHttpNodeSelfGuard:
    def test_direct_execution_with_dry_run_flag_does_not_connect(self, http_probe):
        """纵深：不经 console 闸门、但执行器带 dry_run=True 时节点自己短路。"""
        flow = Flow.model_validate(_http_flow(http_probe))
        out = dryrun_svc._DryRunExecution.run(flow, params={})
        assert out["dry_run"] is True and out["skipped"] == "http"
        assert out["method"] == "GET"
        # URL 经共享渲染器清洗（fail-closed 时回落 <unprintable-url>），不承诺明文
        assert isinstance(out["url"], str) and out["url"]
        assert _ProbeHandler.hits == []


class TestOtherSideEffectNodesBlocked:
    def test_redis_node_rejected(self, client):
        """redis 节点不是默认内置（需装 redis extra 并手动注册）——注册后仍拒绝。"""
        redis_node_mod = pytest.importorskip("plaita.node.redis")
        reg = get_default_registry()
        reg.register(redis_node_mod.RedisNode)
        try:
            flow = {
                "flow_id": "issue190-redis",
                "inputType": {"dataType": "object"},
                "nodes": [
                    {"type": "start", "id": "start", "next": "r"},
                    {
                        "type": "redis",
                        "id": "r",
                        "target": "redis://u:p@127.0.0.1:6379/0",
                        "command": "SET",
                        "arguments": "k v",
                        "next": "end",
                    },
                    {"type": "end", "id": "end", "output": "ok", "resultType": "success"},
                ],
            }
            body = _dry(client, flow)
            assert body["result"] is None
            assert "nodes[r].type=redis" in body["error"]
        finally:
            reg.unregister("redis")


    def test_event_node_rejected(self, client):
        flow = {
            "flow_id": "issue190-event",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "ev"},
                {"type": "event", "id": "ev", "event_type": "approval", "next": "end"},
                {"type": "end", "id": "end", "output": "ok", "resultType": "success"},
            ],
        }
        body = _dry(client, flow)
        assert body["result"] is None
        assert "nodes[ev].type=event" in body["error"]

    def test_code_node_still_rejected_with_legacy_wording(self, client):
        flow = {
            "flow_id": "issue190-code",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "c"},
                {"type": "code", "id": "c", "language": "python", "code": "print(1)", "next": "end"},
                {"type": "end", "id": "end", "output": "x", "resultType": "success"},
            ],
        }
        body = _dry(client, flow)
        assert body["result"] is None
        assert "危险节点" in body["error"] and "nodes[c].type=code" in body["error"]


class TestSafeFlowsUnchanged:
    def test_control_flow_still_runs(self, client):
        flow = {
            "flow_id": "issue190-safe",
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
        r = client.post("/api/flows/dry-run", json={"flowJson": json.dumps(flow), "input": {"age": 20}})
        body = r.json()
        assert body["error"] is None, body["error"]
        assert body["result"] == "adult"

    def test_unknown_node_type_still_fails_flow_validation(self, client):
        flow = {
            "flow_id": "issue190-unknown",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "n"},
                {"type": "no_such_type", "id": "n", "next": "end"},
                {"type": "end", "id": "end", "output": "x", "resultType": "success"},
            ],
        }
        body = _dry(client, flow)
        assert body["result"] is None
        assert "Flow 校验失败" in body["error"]


_PROBE_TYPE = "issue190_dynamic_probe"


class _DynamicProbe(Node):
    """动态注册业务节点替身：按 #32 契约读 dry_run，不许被白名单误伤。"""

    node_type: ClassVar[str] = _PROBE_TYPE
    node_name: ClassVar[str] = "issue190 动态业务节点"

    def execute(self, execution: Any) -> dict:
        dry = bool(execution.get_global_variable("dry_run", False))
        return {"dry_run": dry, "executed": not dry}


def test_dynamic_registered_business_node_still_dry_runs(client):
    reg = get_default_registry()
    reg.register(_DynamicProbe)
    try:
        flow = {
            "flow_id": "issue190-dynamic",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "probe"},
                {"type": _PROBE_TYPE, "id": "probe", "next": "end"},
                {"type": "end", "id": "end", "output": "$NODE.probe", "resultType": "success"},
            ],
        }
        body = _dry(client, flow)
        assert body["error"] is None, body["error"]
        assert body["result"] == {"dry_run": True, "executed": False}
    finally:
        reg.unregister(_PROBE_TYPE)


def test_capability_markers_are_explicit_for_builtin_side_effect_nodes():
    """能力声明是判据本体：内置副作用节点必须显式 False，安全节点显式 True。"""
    from plaita.node.event_node import EventNode
    from plaita.node.http import HTTP
    from plaita.node.redis import RedisNode

    for cls in (HTTP, RedisNode, EventNode):
        assert getattr(cls, "dry_run_safe", None) is False, cls.__name__
    reg = get_default_registry()
    for node_type in ("start", "end", "if", "switch", "assignment", "loop", "mock"):
        cls = reg.get(node_type)
        assert cls is not None, node_type
        assert getattr(cls, "dry_run_safe", None) is True, node_type
