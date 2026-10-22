"""#305 Phase 1：POST /api/graphs/validate（编辑器 GraphDTO 结构化校验）。

负控矩阵（先红后绿；pre = 基线 worktree 只拷本文件，端点不存在 → 404 红）：

1. **GraphDTO 契约**：合法图 200/valid/零 error；viewport 只收不判；容器子
   节点跳过；edges 折叠规则与前端 flowToJson 一致；assignment 补默认 outputType。
2. **四类错误各 ≥2 具名**：
   - node：缺 start / 多 start / 孤儿 / 重复 id / 非 end 终端 / 环；
   - field：未知节点类型（parse_node）/ assignment 缺 output（node.validate）/
     end 非法 resultType（pydantic）/ inputType 非法（flow 级）；
   - edge：悬空 target / handle 不在分支名 / 线性双出边 / if 未知 handle /
     if 双真分支 / if 缺假分支；end 出边 = warning 不阻断；
   - capability：map vs go 池 / while vs go 池；池无清单 → fail-open + warning；
     本地档无 redis 跳过。
3. **双向一致性矩阵（≥6 行）**：validate.valid ⇒ save+publish 必成功；
   publish/save 拒绝 ⇒ validate 报同因（capability 按 #300 detail 同键对账，
   field 按异常文本同因对账）。validate 更严（结构/连线面）为设计内不对称，
   不对称行同时钉住「save/publish 仍 200」。
4. **多租户隔离**：reference 引用目标按请求租户解析，不跨租户借用；错误
   文案不泄露他租户存在性。
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

ADMIN_KEY = "issue305-admin-key"
GO_QUEUE = "plaita:flow:queue:go"
DEFAULT_QUEUE = "plaita:flow:queue"

GO_SUPPORTED = ["assignment", "code", "end", "http", "parallel", "start", "switch"]
GO_UNSUPPORTED = ["calculate", "case", "filter", "find", "map", "mock", "reference",
                  "reduce", "while"]


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
    flow_store.init_engine(f"sqlite:///{tmp_path}/issue305.db")
    r = FakeRedis(decode_responses=True)
    _register_worker(r, "go-1", engine="plaita-go", supported=GO_SUPPORTED)
    _register_worker(r, "py-1", engine="python")
    return r


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


def _register_worker(redis, instance_id, *, engine, supported=None,
                     with_manifest=True):
    from plaita.server.capability import build_capability_manifest, python_worker_manifest

    metadata = {"queue_name": GO_QUEUE if engine == "plaita-go" else DEFAULT_QUEUE}
    if with_manifest:
        manifest = (
            build_capability_manifest(
                supported or [], engine=engine,
                unsupported_node_types=GO_UNSUPPORTED if engine == "plaita-go" else (),
            )
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


# ---- GraphDTO 构造（编辑器序列化形状，见 services/graph_validation.py 契约）----

# if 节点 fields：condition 是结构化 Condition（field/operator/value），不是表达式串
_IF_COND = {"condition": {"field": "INPUT.ok", "operator": "eq", "value": True}}


def _node(id, type, fields=None, name=None, **extra):
    n = {"id": id, "type": "plaitaNode",
         "position": {"x": 0, "y": 0},
         "data": {"type": type, "name": name or id, "fields": fields or {}}}
    n.update(extra)
    return n


def _edge(src, dst, handle=None):
    e = {"id": f"e-{src}-{dst}", "source": src, "target": dst,
         "type": "smoothstep"}
    if handle is not None:
        e["sourceHandle"] = handle
    return e


def _graph(nodes, edges, meta=None):
    g = {"nodes": nodes, "edges": edges,
         "viewport": {"x": 12, "y": -3, "zoom": 0.75}}
    if meta is not None:
        g["meta"] = meta
    return g


def _plain_graph():
    """start → assignment → end（合法基线图，go/python 池都支持）。"""
    return _graph(
        [
            _node("start", "start"),
            _node("a", "assignment", {"output": "$INPUT.name"}),
            _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
        ],
        [_edge("start", "a", "true"), _edge("a", "end", "true")],
        meta={"flow_id": "issue305", "desc": "plain"},
    )


def _map_graph():
    """含 map 节点（go 池不支持、python 池支持）。"""
    return _graph(
        [
            _node("start", "start"),
            _node("m", "map", {
                "over": "$INPUT.items",
                "childFlow": {
                    "flow_id": "child",
                    "nodes": [
                        {"type": "start", "id": "s", "next": "e"},
                        {"type": "end", "id": "e", "output": "$INPUT",
                         "resultType": "success"},
                    ],
                },
            }),
            _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
        ],
        [_edge("start", "m", "true"), _edge("m", "end", "true")],
    )


def _validate(client, graph, runtime=None, headers=None):
    body = {"graph": graph}
    if runtime is not None:
        body["runtime"] = runtime
    return client.post("/api/graphs/validate", json=body,
                       headers=headers or _headers())


def _error_codes(resp):
    detail = resp.json()["detail"]
    return [e["code"] for e in detail["errors"]]


def _errors_by_code(resp, code):
    detail = resp.json()["detail"]
    return [e for e in detail["errors"] if e["code"] == code]


def _save(client, flow_id, definition, runtime=None):
    body = {"definition": json.dumps(definition), "created_by": "t"}
    if runtime is not None:
        body["runtime"] = runtime
    return client.put(f"/api/flows/{flow_id}/versions/1.0.0", json=body,
                      headers=_headers())


def _publish(client, flow_id):
    return client.post(f"/api/flows/{flow_id}/publish",
                       json={"version": "1.0.0"}, headers=_headers())


# ---------------------------------------------------------------------------
# 1) GraphDTO 契约
# ---------------------------------------------------------------------------

class TestGraphDtoContract:
    def test_valid_graph_200_zero_errors(self, redis):
        client = _client(redis)
        resp = _validate(client, _plain_graph(), runtime="python")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["valid"] is True
        assert body["errors"] == []

    def test_viewport_accepted_not_validated(self, redis):
        # viewport/position 是画布态，只收不判：任意值不影响结论
        client = _client(redis)
        g = _plain_graph()
        g["viewport"] = {"x": 99999, "y": -99999, "zoom": 3.14}
        g["nodes"][0]["position"] = {"x": -12345, "y": 54321}
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 200, resp.text

    def test_edge_folding_matches_flow_to_json(self, redis):
        client = _client(redis)
        g = _graph(
            [
                _node("s", "start"),
                _node("cond", "if", _IF_COND),
                _node("t", "assignment", {"output": "1"}),
                _node("f", "assignment", {"output": "2"}),
                _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
            ],
            [
                _edge("s", "cond", "true"),
                _edge("cond", "t", "true"),     # 真分支 → next
                _edge("cond", "f", "false"),    # 假分支 → else_next
                _edge("t", "end", "true"),
                _edge("f", "end", "true"),
            ],
        )
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 200, resp.text
        nodes = {n["id"]: n for n in resp.json()["definition"]["nodes"]}
        assert nodes["cond"]["next"] == "t"
        assert nodes["cond"]["else_next"] == "f"

    def test_switch_branch_handle_folding(self, redis):
        client = _client(redis)
        g = _graph(
            [
                _node("s", "start"),
                _node("sw", "switch", {
                    "expression": "$INPUT.n",
                    "branches": [
                        {"name": "big", "isDefault": True},
                        {"name": "small"},
                    ],
                }),
                _node("e1", "end", {"output": "1", "resultType": "success"}),
                _node("e2", "end", {"output": "2", "resultType": "success"}),
            ],
            [
                _edge("s", "sw", "true"),
                _edge("sw", "e1", "big"),
                _edge("sw", "e2", "small"),
            ],
        )
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 200, resp.text
        sw = next(n for n in resp.json()["definition"]["nodes"] if n["id"] == "sw")
        by_name = {b["name"]: b for b in sw["branches"]}
        assert by_name["big"]["next"] == "e1"
        assert by_name["small"]["next"] == "e2"

    def test_assignment_default_output_type_injected(self, redis):
        client = _client(redis)
        g = _plain_graph()
        g["nodes"][1]["data"]["fields"].pop("output")
        g["nodes"][1]["data"]["fields"]["upstream_output"] = [
            {"value": "$INPUT.name"}]
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 200, resp.text
        a = next(n for n in resp.json()["definition"]["nodes"] if n["id"] == "a")
        assert a["outputType"] == {"dataType": "object"}

    def test_container_children_skipped(self, redis):
        # 容器展开态子节点（parentId/isContainerChild）已双写进 owner 的
        # childFlow，转换时必须跳过、也不因「不可达」误报
        client = _client(redis)
        g = _plain_graph()
        g["nodes"].append(_node("c1", "assignment", {"output": "1"},
                                parentId="a", isContainerChild=True))
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 200, resp.text
        ids = [n["id"] for n in resp.json()["definition"]["nodes"]]
        assert "c1" not in ids

    def test_graph_shape_guards_422(self, redis):
        client = _client(redis)
        resp = _validate(client, {"nodes": "not-a-list", "edges": []})
        assert resp.status_code == 422
        assert _error_codes(resp) == ["graph_dto.invalid"]
        # 非 dict 的 graph 在请求模型层（pydantic）就被 422
        resp = client.post("/api/graphs/validate",
                           json={"graph": "not-a-dict"}, headers=_headers())
        assert resp.status_code == 422

    def test_invalid_runtime_422(self, redis):
        client = _client(redis)
        resp = _validate(client, _plain_graph(), runtime="wasm")
        assert resp.status_code == 422
        assert resp.json()["detail"]["code"] == "invalid_runtime"


# ---------------------------------------------------------------------------
# 2) 四类错误 · 具名负控
# ---------------------------------------------------------------------------

class TestNodeErrors:
    def test_missing_start(self, redis):
        client = _client(redis)
        g = _graph([_node("a", "assignment", {"output": "1"}),
                    _node("end", "end", {"output": "$INPUT"})],
                   [_edge("a", "end")])
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "graph.no_start")
        assert hits and hits[0]["type"] == "node"

    def test_extra_start(self, redis):
        client = _client(redis)
        g = _plain_graph()
        g["nodes"].append(_node("start2", "start"))
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "graph.extra_start")
        assert hits and hits[0]["node_id"] == "start2"

    def test_orphan_node(self, redis):
        client = _client(redis)
        g = _plain_graph()
        g["nodes"].append(_node("lonely", "http",
                                {"url": "https://example.com", "method": "GET"}))
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "graph.orphan")
        assert hits and hits[0]["node_id"] == "lonely"

    def test_duplicate_id(self, redis):
        client = _client(redis)
        g = _plain_graph()
        g["nodes"].append(_node("a", "assignment", {"output": "2"}))
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "node.duplicate_id")
        assert hits and hits[0]["node_id"] == "a"

    def test_dead_end_non_end(self, redis):
        client = _client(redis)
        g = _graph([_node("s", "start"),
                    _node("a", "assignment", {"output": "1"})],
                   [_edge("s", "a")])
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "graph.dead_end_non_end")
        assert hits and hits[0]["node_id"] == "a"

    def test_cycle_without_timeout_error(self, redis):
        # 环出口必须走分支节点：if 真分支回边、假分支前进（合法环形态）
        client = _client(redis)
        g = _graph(
            [
                _node("s", "start"),
                _node("a", "assignment", {"output": "1"}),
                _node("cond", "if", _IF_COND),
                _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
            ],
            [
                _edge("s", "a"), _edge("a", "cond"),
                _edge("cond", "a", "false"),  # 回边 → 环
                _edge("cond", "end", "true"),
            ],
        )
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "graph.cycle")
        assert hits and hits[0]["severity"] == "error"
        assert hits[0]["node_id"] in {"a", "cond"}

    def test_cycle_with_timeout_warning(self, redis):
        client = _client(redis)
        g = _graph(
            [
                _node("s", "start"),
                _node("a", "assignment", {"output": "1"}),
                _node("cond", "if", _IF_COND),
                _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
            ],
            [
                _edge("s", "a"), _edge("a", "cond"),
                _edge("cond", "a", "false"),
                _edge("cond", "end", "true"),
            ],
            meta={"timeout": "30s"},
        )
        resp = _validate(client, g)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["valid"] is True
        cycles = [e for e in body["errors"] if e["code"] == "graph.cycle"]
        assert cycles and cycles[0]["severity"] == "warning"

    def test_diamond_convergence_no_false_cycle(self, redis):
        # plaita#105 回归锚：菱形汇聚不是环
        client = _client(redis)
        g = _graph(
            [
                _node("s", "start"),
                _node("cond", "if", _IF_COND),
                _node("l", "assignment", {"output": "1"}),
                _node("r", "assignment", {"output": "2"}),
                _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
            ],
            [
                _edge("s", "cond"),
                _edge("cond", "l", "true"), _edge("cond", "r", "false"),
                _edge("l", "end"), _edge("r", "end"),
            ],
        )
        resp = _validate(client, g)
        assert resp.status_code == 200, resp.text
        assert all(e["code"] != "graph.cycle" for e in resp.json()["errors"])

    def test_childflow_missing_start(self, redis):
        # 子图与根图同判据：childFlow 缺 start 也是错误
        client = _client(redis)
        g = _graph(
            [
                _node("s", "start"),
                _node("w", "while", {"childFlow": {
                    "nodes": [_node("ce", "end", {"output": "$INPUT"})],
                }}),
                _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
            ],
            [_edge("s", "w"), _edge("w", "end")],
        )
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "graph.no_start")
        assert hits and "子流程" in hits[0]["message"]


class TestFieldErrors:
    def test_unknown_node_type(self, redis):
        client = _client(redis)
        g = _plain_graph()
        g["nodes"][1]["data"]["type"] = "llm_call"
        resp = _validate(client, g)
        assert resp.status_code == 422
        # 权威 catch-all（保存路径同一实现）+ 逐节点定位双报
        catch_all = _errors_by_code(resp, "invalid_definition")
        granular = _errors_by_code(resp, "node_schema")
        assert catch_all and "unRecognized node type" in catch_all[0]["message"]
        assert granular and granular[0]["node_id"] == "a"
        assert "unRecognized node type" in granular[0]["message"]

    def test_assignment_missing_output_and_upstream(self, redis):
        # assignment 缺 output 且缺 upstream_output：引擎 node.validate() 判死
        client = _client(redis)
        g = _plain_graph()
        g["nodes"][1]["data"]["fields"] = {}
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "node_validate")
        assert hits and hits[0]["node_id"] == "a"
        assert "output" in hits[0]["message"]

    def test_end_bad_error_payload(self, redis):
        # end.resultType 非法值会被引擎静默归一为 success（warning），不在此列；
        # 真正的 pydantic 拒绝面：resultType=error 时 error 载荷必须是对象
        client = _client(redis)
        g = _plain_graph()
        g["nodes"][2]["data"]["fields"] = {"resultType": "error", "error": "boom"}
        resp = _validate(client, g)
        assert resp.status_code == 422
        granular = _errors_by_code(resp, "node_schema")
        assert granular and granular[0]["node_id"] == "end"

    def test_flow_level_bad_input_type(self, redis):
        client = _client(redis)
        g = _plain_graph()
        g["meta"] = {"inputType": {"dataType": 123}}
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "invalid_definition")
        assert hits and hits[0]["message"]


class TestEdgeErrors:
    def test_dangling_target(self, redis):
        client = _client(redis)
        g = _plain_graph()
        g["edges"].append(_edge("a", "ghost", "true"))
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "edge.unknown_target")
        assert hits and hits[0]["node_id"] == "a"

    def test_switch_handle_not_a_branch(self, redis):
        client = _client(redis)
        g = _graph(
            [
                _node("s", "start"),
                _node("sw", "switch", {
                    "expression": "$INPUT.n",
                    "branches": [{"name": "big", "isDefault": True}],
                }),
                _node("e1", "end", {"output": "1", "resultType": "success"}),
                _node("lost", "end", {"output": "2", "resultType": "success"}),
            ],
            [_edge("s", "sw"), _edge("sw", "e1", "big"),
             _edge("sw", "lost", "nope")],
        )
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "edge.handle_not_a_branch")
        assert hits and hits[0]["node_id"] == "sw"

    def test_linear_two_out_edges_ambiguous(self, redis):
        client = _client(redis)
        g = _plain_graph()
        g["edges"].append(_edge("a", "start", "true"))
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "edge.ambiguous_multiple")
        assert hits and hits[0]["node_id"] == "a"

    def test_if_unknown_handle(self, redis):
        client = _client(redis)
        g = _graph(
            [
                _node("s", "start"),
                _node("cond", "if", _IF_COND),
                _node("t", "assignment", {"output": "1"}),
                _node("f", "assignment", {"output": "2"}),
                _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
            ],
            [
                _edge("s", "cond"), _edge("cond", "t", "true"),
                _edge("cond", "f", "false"), _edge("t", "end"),
                _edge("f", "end"),
                _edge("cond", "end", "maybe"),  # 非法 handle
            ],
        )
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "edge.unknown_handle")
        assert hits and hits[0]["node_id"] == "cond"

    def test_if_duplicate_true_branch(self, redis):
        client = _client(redis)
        g = _graph(
            [
                _node("s", "start"),
                _node("cond", "if", _IF_COND),
                _node("t1", "assignment", {"output": "1"}),
                _node("t2", "assignment", {"output": "2"}),
                _node("f", "assignment", {"output": "3"}),
                _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
            ],
            [
                _edge("s", "cond"), _edge("cond", "t1", "true"),
                _edge("cond", "t2", "true"), _edge("cond", "f", "false"),
                _edge("t1", "end"), _edge("t2", "end"), _edge("f", "end"),
            ],
        )
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "edge.duplicate_branch")
        assert hits and hits[0]["node_id"] == "cond"

    def test_if_missing_false_branch(self, redis):
        client = _client(redis)
        g = _graph(
            [
                _node("s", "start"),
                _node("cond", "if", _IF_COND),
                _node("t", "assignment", {"output": "1"}),
                _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
            ],
            [_edge("s", "cond"), _edge("cond", "t", "true"), _edge("t", "end")],
        )
        resp = _validate(client, g)
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "edge.if_branch_missing")
        assert hits and hits[0]["field"] == "else_next"

    def test_end_out_edge_warning_not_blocking(self, redis):
        client = _client(redis)
        g = _plain_graph()
        g["edges"].append(_edge("end", "a"))
        resp = _validate(client, g, runtime="python")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["valid"] is True
        warns = [e for e in body["errors"] if e["code"] == "edge.end_outgoing"]
        assert warns and warns[0]["severity"] == "warning"


class TestCapabilityErrors:
    def test_map_vs_go_pool(self, redis):
        client = _client(redis)
        resp = _validate(client, _map_graph(), runtime="go")
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "capability.unsupported_node_type")
        assert hits and hits[0]["node_id"] == "m"
        assert hits[0]["type"] == "capability"
        assert hits[0]["unsupported"] == ["map"]
        assert "start" in hits[0]["supported"]

    def test_while_vs_go_pool(self, redis):
        client = _client(redis)
        g = _graph(
            [
                _node("s", "start"),
                _node("w", "while", {"childFlow": {
                    "flow_id": "loop-body",
                    "nodes": [
                        {"type": "start", "id": "cs", "next": "ce"},
                        {"type": "end", "id": "ce", "output": "$INPUT",
                         "resultType": "success"},
                    ],
                }}),
                _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
            ],
            [_edge("s", "w"), _edge("w", "end")],
        )
        resp = _validate(client, g, runtime="go")
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "capability.unsupported_node_type")
        assert hits and hits[0]["node_id"] == "w"
        assert hits[0]["unsupported"] == ["while"]

    def test_python_pool_takes_map(self, redis):
        client = _client(redis)
        resp = _validate(client, _map_graph(), runtime="python")
        assert resp.status_code == 200, resp.text
        assert resp.json()["valid"] is True

    def test_pool_manifest_missing_fail_open_warning(self, redis):
        # 池内全是没有清单的旧 build worker：与 #300 publish 门同口径
        # fail-open 放行，但响应内必须带 warning（降级可观测）
        redis.delete("plaita:registry:flow_worker:go-1")
        _register_worker(redis, "go-old", engine="plaita-go", with_manifest=False)
        client = _client(redis)
        resp = _validate(client, _map_graph(), runtime="go")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["valid"] is True
        warns = [e for e in body["errors"]
                 if e["code"] == "capability.pool_manifest_missing"]
        assert warns and warns[0]["severity"] == "warning"

    def test_local_mode_without_redis_skips_gate(self, tmp_path, monkeypatch):
        monkeypatch.setattr("config.get_settings", lambda: _settings())
        monkeypatch.setattr("auth.get_settings", lambda: _settings())
        flow_store.init_engine(f"sqlite:///{tmp_path}/issue305-local.db")
        app = FastAPI()
        app.state.redis = None  # 本地单机档
        app.state.local_mode = True
        app.state.store = flow_store.get_flow_store()
        app.include_router(graphs_api.router, prefix="/api",
                           dependencies=[Depends(require_auth)])
        client = TestClient(app)
        resp = _validate(client, _map_graph(), runtime="go")
        assert resp.status_code == 200, resp.text


# ---------------------------------------------------------------------------
# 3) 双向一致性矩阵（validate ⇄ save/publish，同一内部实现）
# ---------------------------------------------------------------------------

class TestConsistencyMatrix:
    """V⇒P：validate.valid ⇒ save+publish 必成功。
    P⇒V：save/publish 拒绝 ⇒ validate 报同因。
    validate 更严（结构/连线面）为设计内不对称——不对称行同时钉住
    「save/publish 仍 200」（这些图发布得了，但运行期才炸，编辑器在保存前拦）。
    """

    def _row(self, client, name, graph, runtime, *,
             expect_valid, flow_id, expect_publish_status=200,
             cause_code=None, cause_needle=None):
        flow_store.get_flow_store().create_flow(flow_id, tenant_id="default")
        resp = _validate(client, graph, runtime=runtime)
        # ---- validate 侧判定 ----
        if expect_valid:
            assert resp.status_code == 200, f"[{name}] {resp.text}"
            assert resp.json()["valid"] is True, f"[{name}] {resp.text}"
        else:
            assert resp.status_code == 422, f"[{name}] {resp.text}"
            if cause_code is not None:
                assert cause_code in _error_codes(resp), \
                    f"[{name}] 缺 {cause_code}: {_error_codes(resp)}"

        # ---- 用 validate 转出的 definition 走 save+publish（转换契约可发布）----
        definition = resp.json().get("definition") if resp.status_code == 200 \
            else resp.json()["detail"]["definition"]
        save = _save(client, flow_id, definition, runtime=runtime)
        if save.status_code != 200:
            # save 拒绝 ⇒ validate 必已报同因（P⇒V，field 面）
            assert save.status_code == 422, f"[{name}] {save.text}"
            assert not expect_valid, f"[{name}] validate 过但 save 422"
            msg = save.json()["detail"]["message"]
            if cause_needle is not None:
                assert cause_needle in msg, f"[{name}] save 因: {msg}"
            invalid = [e for e in resp.json()["detail"]["errors"]
                       if e["code"] == "invalid_definition"]
            assert invalid and cause_needle in invalid[0]["message"], \
                f"[{name}] validate 未报同因: {invalid}"
            return
        pub = _publish(client, flow_id)
        assert pub.status_code == expect_publish_status, \
            f"[{name}] publish {pub.status_code}: {pub.text}"

    def test_matrix(self, redis):
        client = _client(redis)

        # 行 1：plain@go —— V⇒P（validate 过 → 发布成功）
        self._row(client, "plain@go", _plain_graph(), "go",
                  expect_valid=True, flow_id="m1-plain-go")

        # 行 2：map@python —— V⇒P（python 池收 map）
        self._row(client, "map@python", _map_graph(), "python",
                  expect_valid=True, flow_id="m2-map-py")

        # 行 3：map@go —— P⇒V（publish 422 unsupported_node_types ⇔ validate
        # capability 错误同键对账）
        resp = _validate(client, _map_graph(), runtime="go")
        assert resp.status_code == 422
        cap = _errors_by_code(resp, "capability.unsupported_node_type")
        assert cap and cap[0]["unsupported"] == ["map"]
        flow_store.get_flow_store().create_flow("m3-map-go", tenant_id="default")
        save = _save(client, "m3-map-go", resp.json()["detail"]["definition"],
                     runtime="go")
        assert save.status_code == 200
        pub = _publish(client, "m3-map-go")
        assert pub.status_code == 422, pub.text
        detail = pub.json()["detail"]
        assert detail["error"] == "unsupported_node_types"
        assert detail["unsupported"] == cap[0]["unsupported"]
        assert detail["runtime"] == cap[0]["runtime"]
        assert sorted(detail["supported"]) == sorted(cap[0]["supported"])

        # 行 4：unknown-type@python —— P⇒V（save 422 invalid_definition ⇔
        # validate field 错误同因对账）
        g = _plain_graph()
        g["nodes"][1]["data"]["type"] = "llm_call"
        self._row(client, "unknown-type@python", g, "python",
                  expect_valid=False, flow_id="m4-unknown",
                  cause_code="invalid_definition",
                  cause_needle="unRecognized node type")

        # 行 5（不对称）：悬空连线 —— save/publish 都过（引擎运行期才
        # NodeNotFoundError），validate 在保存前拦
        g = _plain_graph()
        g["edges"].append(_edge("a", "ghost"))
        self._row(client, "dangling@python", g, "python",
                  expect_valid=False, flow_id="m5-dangling",
                  cause_code="edge.unknown_target", expect_publish_status=200)

        # 行 6（不对称）：if 缺假分支 —— 同上
        g = _graph(
            [
                _node("s", "start"),
                _node("cond", "if", _IF_COND),
                _node("t", "assignment", {"output": "1"}),
                _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
            ],
            [_edge("s", "cond"), _edge("cond", "t", "true"), _edge("t", "end")],
        )
        self._row(client, "if-no-else@python", g, "python",
                  expect_valid=False, flow_id="m6-if-else",
                  cause_code="edge.if_branch_missing", expect_publish_status=200)

        # 行 7（不对称）：孤儿节点 —— 发布得了但节点静默丢失，validate 拦
        g = _plain_graph()
        g["nodes"].append(_node("lonely", "http",
                                {"url": "https://example.com", "method": "GET"}))
        self._row(client, "orphan@python", g, "python",
                  expect_valid=False, flow_id="m7-orphan",
                  cause_code="graph.orphan", expect_publish_status=200)

    def test_no_manifest_pool_matrix_fallback_alignment(self, redis):
        # 兜底对齐：池无清单时 publish 放行 ⇔ validate 放行 + warning
        redis.delete("plaita:registry:flow_worker:go-1")
        _register_worker(redis, "go-old", engine="plaita-go", with_manifest=False)
        client = _client(redis)
        resp = _validate(client, _map_graph(), runtime="go")
        assert resp.status_code == 200
        flow_store.get_flow_store().create_flow("m8-fallback", tenant_id="default")
        save = _save(client, "m8-fallback", resp.json()["definition"], runtime="go")
        assert save.status_code == 200
        pub = _publish(client, "m8-fallback")
        assert pub.status_code == 200, pub.text  # #300 fail-open 同判


# ---------------------------------------------------------------------------
# 4) 多租户隔离
# ---------------------------------------------------------------------------

class TestTenantIsolation:
    def _create_flow(self, flow_id, tenant_id):
        store = flow_store.get_flow_store()
        store.create_flow(flow_id, tenant_id=tenant_id)

    def _reference_graph(self, flow_id):
        return _graph(
            [
                _node("s", "start"),
                _node("ref", "reference", {"flow_id": flow_id}),
                _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
            ],
            [_edge("s", "ref"), _edge("ref", "end")],
        )

    def test_reference_resolves_within_own_tenant(self, redis):
        self._create_flow("lib-flow", "tenant-a")
        client = _client(redis)
        resp = _validate(client, self._reference_graph("lib-flow"),
                         runtime="python",
                         headers={**_headers(), "X-Tenant-ID": "tenant-a"})
        assert resp.status_code == 200, resp.text
        assert resp.json()["valid"] is True

    def test_reference_does_not_borrow_other_tenant_flow(self, redis):
        # 同名 flow 只存在于 tenant-a：tenant-b 的 graph 引用它必须报缺失，
        # 不跨租户解析（错误文案不泄露 tenant-a 的存在性）
        self._create_flow("lib-flow", "tenant-a")
        client = _client(redis)
        resp = _validate(client, self._reference_graph("lib-flow"),
                         runtime="python",
                         headers={**_headers(), "X-Tenant-ID": "tenant-b"})
        assert resp.status_code == 422
        hits = _errors_by_code(resp, "reference_target_missing")
        assert hits and hits[0]["node_id"] == "ref"
        assert hits[0]["field"] == "flow_id"
        assert "tenant-a" not in hits[0]["message"]

    def test_missing_reference_reports_missing(self, redis):
        client = _client(redis)
        resp = _validate(client, self._reference_graph("no-such-flow"))
        assert resp.status_code == 422
        assert "reference_target_missing" in _error_codes(resp)


# ---------------------------------------------------------------------------
# 5) 鉴权
# ---------------------------------------------------------------------------

class TestAuth:
    def test_requires_auth(self, redis):
        client = _client(redis)
        resp = client.post("/api/graphs/validate",
                           json={"graph": _plain_graph()})
        assert resp.status_code == 401, resp.text
