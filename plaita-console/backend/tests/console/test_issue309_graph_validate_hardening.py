"""#309 graph validate 两面加固 + A2R51-01 reference 一致性（r80 B1）。

三面（issue jeffkit/plaita#309，探针 /tmp/r124_probe_graph_validate.py
RED×4/GREEN×2 + A2 r51 台账 duo-a2-ledger「第五十一轮」A2R51-01）：

① 缺形节点零报错剔除 → 具名 ``graph.malformed_node``（error 级 → 422）：
   非 dict / data 非 dict / parentId 半标 / isContainerChild 半标 /
   宿主不在图——历史上全部 valid=true 且折叠即丢（照此保存节点无声消失）。
② 展开容器子边 unknown_source 误报：子↔子内部连线已双写进 owner 的
   childFlow、折叠不消费，validate 不得拒（前端 flowConverter jsonToGraph
   把子边推进同一 edges 数组——编辑器自身可产生的形态）。
③ A2R51-01 reference 组合（修复取台账方向①，validate 层形状真相）：
   - 缺 flow_id 且无 childFlow → ``reference_target_unspecified``（error）；
   - python 池 reference 零可执行形状（named-ref 无注入器 + inline 被
     ReferenceFlow.setup_child_flow 覆写丢弃，child.py:106/:127）→
     ``reference_unexecutable_on_runtime``（warning，可观测不阻断——
     manifest 死条目摘除/引擎注入器为方向②③遗留）。

负控：本文件在未修复树上 pre-run 全红
（docs/proofs/data/r80-issue309-pre-red.txt），修复后全绿（post-green.txt）。
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import flows as flows_api  # noqa: E402
from api import graphs as graphs_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402

ADMIN_KEY = "issue309-admin-key"
GO_QUEUE = "plaita:flow:queue:go"
DEFAULT_QUEUE = "plaita:flow:queue"

GO_SUPPORTED_WITH_REFERENCE = [
    "assignment", "code", "end", "http", "parallel", "start", "switch",
    "reference",
]


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def redis(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    monkeypatch.setenv("PLAITA_CONSOLE_GO_TASK_QUEUE", GO_QUEUE)
    flow_store.init_engine(f"sqlite:///{tmp_path}/issue309.db")
    r = FakeRedis(decode_responses=True)
    _register_worker(r, "go-1", engine="plaita-go",
                     supported=GO_SUPPORTED_WITH_REFERENCE)
    _register_worker(r, "py-1", engine="python")
    return r


def _register_worker(redis, instance_id, *, engine, supported=None):
    from plaita.server.capability import (
        build_capability_manifest,
        python_worker_manifest,
    )

    metadata = {"queue_name": GO_QUEUE if engine == "plaita-go" else DEFAULT_QUEUE}
    manifest = (
        build_capability_manifest(supported or [], engine=engine)
        if engine != "python"
        else python_worker_manifest()
    )
    metadata["capability_manifest"] = manifest
    redis.setex(
        f"plaita:registry:flow_worker:{instance_id}", 30,
        json.dumps({
            "instance_id": instance_id,
            "service_type": "flow_worker",
            "host": "localhost",
            "status": "running",
            "metadata": metadata,
            "active_tasks": 0,
        }),
    )


def _client(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    app.include_router(
        flows_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(
        graphs_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _node(nid, ntype, fields=None, **extra):
    n = {"id": nid, "type": "plaitaNode",
         "position": {"x": 0, "y": 0},
         "data": {"type": ntype, "name": nid, "fields": fields or {}}}
    n.update(extra)
    return n


def _edge(src, dst, handle=None):
    e = {"id": f"e-{src}-{dst}", "source": src, "target": dst,
         "type": "smoothstep"}
    if handle is not None:
        e["sourceHandle"] = handle
    return e


def _validate(client, graph, runtime=None, headers=None):
    body = {"graph": graph}
    if runtime is not None:
        body["runtime"] = runtime
    return client.post("/api/graphs/validate", json=body,
                       headers=headers or _headers())


def _error_codes(resp):
    body = resp.json()
    errors = body["errors"] if resp.status_code == 200 else body["detail"]["errors"]
    return [e["code"] for e in errors]


def _errors_by_code(resp, code):
    body = resp.json()
    errors = body["errors"] if resp.status_code == 200 else body["detail"]["errors"]
    return [e for e in errors if e["code"] == code]


def _container_child(nid, ntype, owner):
    """flowConverter jsonToGraph 形状的容器子节点（标记在 data 层，非 fields）。"""
    n = _node(nid, ntype)
    n["parentId"] = owner
    n["extent"] = "parent"
    n["data"]["isContainerChild"] = True
    n["data"]["ownerId"] = owner
    return n


def _plain_nodes():
    return [_node("s", "start"),
            _node("e", "end", {"output": "$INPUT", "resultType": "success"})]


# ---------------------------------------------------------------------------
# ① 缺形节点：零报错剔除 → 具名 graph.malformed_node（r124 RED-1/2/3 翻绿）
# ---------------------------------------------------------------------------

class TestMalformedNodeShapeErrors:
    def test_missing_data_key_422(self, redis):
        # r124 RED-1：节点缺 data 键 → 历史 valid=true 且 ghost 从 definition 消失
        client = _client(redis)
        g = {"nodes": _plain_nodes()
             + [{"id": "ghost", "type": "plaitaNode", "position": {"x": 2, "y": 0}}],
              "edges": [_edge("s", "e")]}
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 422, resp.text
        hits = _errors_by_code(resp, "graph.malformed_node")
        assert hits and hits[0]["node_id"] == "ghost"
        assert hits[0]["type"] == "node"
        assert hits[0]["severity"] == "error"

    def test_data_none_422(self, redis):
        # r124 RED-2 前半：data=None 同面
        client = _client(redis)
        g = {"nodes": _plain_nodes()
             + [{"id": "ghost2", "type": "plaitaNode",
                 "position": {"x": 2, "y": 0}, "data": None}],
              "edges": [_edge("s", "e")]}
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 422, resp.text
        hits = _errors_by_code(resp, "graph.malformed_node")
        assert hits and hits[0]["node_id"] == "ghost2"

    def test_container_child_without_parent_422(self, redis):
        # r124 RED-3：data.isContainerChild 为真但无 parentId（无折叠宿主）
        client = _client(redis)
        ghost4 = _node("ghost4", "end")
        ghost4["data"]["isContainerChild"] = True
        ghost4["data"]["ownerId"] = "nowhere"
        g = {"nodes": _plain_nodes() + [ghost4],
             "edges": [_edge("s", "e")]}
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 422, resp.text
        hits = _errors_by_code(resp, "graph.malformed_node")
        assert hits and hits[0]["node_id"] == "ghost4"

    def test_parent_without_child_marker_422(self, redis):
        # r124 RED-2 后半：parentId 指向不存在容器且无 isContainerChild 半标
        client = _client(redis)
        g = {"nodes": _plain_nodes()
             + [_node("ghost3", "start", parentId="no-such-container")],
              "edges": [_edge("s", "e")]}
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 422, resp.text
        hits = _errors_by_code(resp, "graph.malformed_node")
        assert hits and hits[0]["node_id"] == "ghost3"

    def test_non_dict_node_422(self, redis):
        client = _client(redis)
        g = {"nodes": _plain_nodes() + ["not-a-node"],
             "edges": [_edge("s", "e")]}
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 422, resp.text
        hits = _errors_by_code(resp, "graph.malformed_node")
        assert hits  # 无 id 可挂，条目在即可

    def test_child_owner_missing_422(self, redis):
        # 双标记齐全但宿主不在图中（陈旧 ownerId）：折叠找不到双写目标
        client = _client(redis)
        g = {"nodes": _plain_nodes() + [_container_child("c1", "start", "ghostbox")],
              "edges": [_edge("s", "e")]}
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 422, resp.text
        hits = _errors_by_code(resp, "graph.malformed_node")
        assert hits and hits[0]["node_id"] == "c1"

    def test_well_formed_isolated_node_still_orphan(self, redis):
        # GREEN 控（r124 GREEN-1）：形好孤立节点仍走 graph.orphan，不误报 malformed
        client = _client(redis)
        g = {"nodes": _plain_nodes() + [_node("lonely", "end")],
             "edges": [_edge("s", "e")]}
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 422, resp.text
        assert "graph.orphan" in _error_codes(resp)
        assert "graph.malformed_node" not in _error_codes(resp)


# ---------------------------------------------------------------------------
# ② 展开容器子边：unknown_source 误报消除（r124 RED-4 翻绿）
# ---------------------------------------------------------------------------

def _expanded_container_graph():
    """r124 RED-4 形态：flowConverter jsonToGraph 的展开态序列化。

    子节点带 parentId+isContainerChild（data 层）+ownerId；子边留在同一
    edges 数组；容器 fields.childFlow 已双写（折叠语义权威）。
    """
    box = _node("box", "loop", {
        "over": "$INPUT.items",
        "childFlow": {
            "flow_id": "child",
            "nodes": [
                {"type": "start", "id": "c1", "next": "c2"},
                {"type": "end", "id": "c2", "output": "$INPUT",
                 "resultType": "success"},
            ],
        },
    })
    return {"nodes": _plain_nodes() + [box]
            + [_container_child("c1", "start", "box"),
               _container_child("c2", "end", "box")],
            "edges": [_edge("s", "box"), _edge("box", "e"),
                      _edge("c1", "c2")]}


class TestExpandedContainerChildEdges:
    def test_expanded_container_graph_valid(self, redis):
        # r124 RED-4：历史 422 edge.unknown_source（c1 不在顶层 id 全集）
        client = _client(redis)
        resp = _validate(client, _expanded_container_graph(), runtime="python")
        assert resp.status_code == 200, resp.text
        assert resp.json()["valid"] is True, resp.text
        # 折叠语义不回归：子节点不进顶层 definition
        ids = [n["id"] for n in resp.json()["definition"]["nodes"]]
        assert "c1" not in ids and "c2" not in ids and "box" in ids

    def test_collapsed_container_graph_valid(self, redis):
        # r124 GREEN-2 守恒：同逻辑图收起态（无子节点/子边）照常通过
        client = _client(redis)
        box = _node("box", "loop", {
            "over": "$INPUT.items",
            "childFlow": {
                "flow_id": "child",
                "nodes": [
                    {"type": "start", "id": "c1", "next": "c2"},
                    {"type": "end", "id": "c2", "output": "$INPUT",
                     "resultType": "success"},
                ],
            },
        })
        g = {"nodes": _plain_nodes() + [box],
             "edges": [_edge("s", "box"), _edge("box", "e")]}
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 200, resp.text

    def test_edge_into_container_child_rejected(self, redis):
        # 顶层 → 子节点边：折叠会产出悬空 next（运行期 NodeNotFoundError），
        # 保持拒绝，但病因改准（target 是容器子节点而非「不存在」）
        client = _client(redis)
        g = _expanded_container_graph()
        g["edges"].append(_edge("s", "c1"))
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 422, resp.text
        hits = _errors_by_code(resp, "edge.unknown_target")
        assert hits and "容器" in hits[0]["message"]

    def test_edge_from_child_to_top_rejected(self, redis):
        # 子 → 顶层边：折叠不消费子节点出边（接线已丢失），保持可观测
        client = _client(redis)
        g = _expanded_container_graph()
        g["edges"].append(_edge("c1", "e"))
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 422, resp.text
        assert "edge.unknown_source" in _error_codes(resp)


# ---------------------------------------------------------------------------
# ③ A2R51-01：reference 形状×池一致性
# ---------------------------------------------------------------------------

def _reference_graph(fields):
    return {"nodes": [
        _node("s", "start"),
        _node("ref", "reference", fields),
        _node("e", "end", {"output": "$INPUT", "resultType": "success"}),
    ], "edges": [_edge("s", "ref"), _edge("ref", "e")]}


class TestReferenceShapeRuntimeConsistency:
    def test_reference_without_any_target_422(self, redis):
        # A2 r51 R51-A：缺 flow_id 的 reference 历史 200 valid=true 零 error
        client = _client(redis)
        resp = _validate(client, _reference_graph({}), runtime="python")
        assert resp.status_code == 422, resp.text
        hits = _errors_by_code(resp, "reference_target_unspecified")
        assert hits and hits[0]["node_id"] == "ref"
        assert hits[0]["field"] == "flow_id"

    def test_inline_childflow_reference_not_unspecified(self, redis):
        # inline childFlow 是合法目标形状：不得报 unspecified
        # （go 池 manifest 含 reference → gate 放行；池可执行性另由②warning 面）
        client = _client(redis)
        graph = _reference_graph({"childFlow": {
            "flow_id": "inlined",
            "nodes": [
                {"type": "start", "id": "s2", "next": "e2"},
                {"type": "end", "id": "e2", "output": "$INPUT",
                 "resultType": "success"},
            ],
        }})
        resp = _validate(client, graph, runtime="go")
        assert resp.status_code == 200, resp.text
        assert "reference_target_unspecified" not in _error_codes(resp)

    def test_named_reference_python_pool_warning(self, redis):
        # python 池对 reference 零可执行形状：valid 不翻（warning 级可观测，
        # 台账方向①验收格），但 errors[] 必须带池真相
        flow_store.get_flow_store().create_flow("lib-flow", tenant_id="default")
        client = _client(redis)
        resp = _validate(client, _reference_graph({"flow_id": "lib-flow"}),
                         runtime="python")
        assert resp.status_code == 200, resp.text
        assert resp.json()["valid"] is True
        hits = _errors_by_code(resp, "reference_unexecutable_on_runtime")
        assert hits and hits[0]["severity"] == "warning"
        assert hits[0]["node_id"] == "ref"

    def test_named_reference_go_pool_no_warning(self, redis):
        # go 池已转正 reference（plaita-go ir.go:99）：不发池警告
        flow_store.get_flow_store().create_flow("lib-flow", tenant_id="default")
        client = _client(redis)
        resp = _validate(client, _reference_graph({"flow_id": "lib-flow"}),
                         runtime="go")
        assert resp.status_code == 200, resp.text
        assert "reference_unexecutable_on_runtime" not in _error_codes(resp)

    def test_save_still_accepts_targetless_reference(self, redis):
        # 不对称留证（A2 R51-A save 格）：save 路径不受本轮影响照常 200——
        # validate 422 拦在编辑器保存前，save/publish 面收敛是方向②③的事
        client = _client(redis)
        definition = {
            "flow_id": "ref-no-target",
            "desc": "reference without any target",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "s", "next": "ref"},
                {"type": "reference", "id": "ref", "next": "e"},
                {"type": "end", "id": "e", "output": "$INPUT",
                 "resultType": "success"},
            ],
        }
        r = client.post("/api/flows", json={"flow_id": "ref-no-target"},
                        headers=_headers())
        assert r.status_code in (200, 201), r.text
        rv = client.put("/api/flows/ref-no-target/versions/1.0.0",
                        json={"definition": json.dumps(definition),
                              "created_by": "t"},
                        headers=_headers())
        assert rv.status_code == 200, rv.text
