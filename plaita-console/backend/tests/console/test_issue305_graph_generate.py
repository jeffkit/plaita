"""#305 Phase 2：POST /api/graphs/generate（意图 → GraphDTO 草稿）。

负控矩阵（先红后绿；pre = 基线 worktree 只拷本文件——服务模块不存在收集期
ImportError，端点不存在 404 红）：

1. **契约**：合法生成 200/valid/graph/definition/generation 元数据（模型/
   耗时/轮次/token）；纯函数式不落库（流程表零写入）；空 prompt / 非法
   runtime 具名 422；请求体未知字段 422（StrictRequestModel）。
2. **generate→validate→回喂闭环**（ai_flow 先例）：首轮草稿不过 validate
   →错误回喂自纠（attempts>1 收敛）；始终不合法 → 200 valid=false 且
   errors[] 原样带回（validate 的错误项逐字段保真：code/node_id/field/
   severity/runtime/unsupported/supported）；模型输出非 JSON 回喂重试；
   warning 不阻断（valid=true 随 errors 带回）。
3. **A128-01 自检**（豁免类标记信任缝，generate 是首个非编辑器 GraphDTO
   产源）：投影子节点内容未双写进 owner.childFlow → 具名 500
   ``generation.childflow_not_double_written``、响应体不出草稿；owner 根本
   没有 childFlow → 不触发自检（validate 会具名 child_flow_missing，非静默
   缝）；编辑器同构的忠实展开态（``owner::id`` 命名空间 + fields 剥拓扑键）
   正控通过。
4. **租户与鉴权**（与 Phase 1 validate 同口径）：无凭据 401；viewer 写闸
   403（editor 及以上）；reference 引用按请求租户解析（不跨租户借用、文案
   不泄露他租户存在性）；hints（偏好类型/可引用子流程清单）确实注入模型
   prompt；LLM client 注入式打桩——真 client 构造入口全程零触达，client
   构造失败具名 503。
"""
from __future__ import annotations

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

from api import graphs as graphs_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store, users_svc  # noqa: E402
from services import graph_generation  # noqa: E402

ADMIN_KEY = "issue305-gen-admin-key"
GO_QUEUE = "plaita:flow:queue:go"
DEFAULT_QUEUE = "plaita:flow:queue"

GO_SUPPORTED = ["assignment", "code", "end", "http", "parallel", "reference",
                "start", "switch"]
GO_UNSUPPORTED = ["calculate", "case", "filter", "find", "map", "mock",
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
    flow_store.init_engine(f"sqlite:///{tmp_path}/issue305gen.db")
    # 真模型构造入口必须全程零触达：误触达（出网/读凭证）即炸
    def _no_real_client():
        raise AssertionError("测试不得构造真实 LLM client（必须注入式打桩）")

    monkeypatch.setattr(graph_generation, "default_model", _no_real_client)
    r = FakeRedis(decode_responses=True)
    _register_worker(r, "go-1", engine="plaita-go", supported=GO_SUPPORTED)
    _register_worker(r, "py-1", engine="python")
    return r


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


def _client(redis) -> TestClient:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = flow_store.get_flow_store()
    app.include_router(
        graphs_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


# ---- 模型打桩：注入式 complete(task)->text ----

def _stub_model(responses, model_name="stub-graph-model", usage=None):
    """按序返回响应的打桩 client；记录收到的 task 文本供 prompt 断言。"""
    state = {"calls": [], "i": 0}
    if usage is None:
        usage = {"input_tokens": 11, "output_tokens": 22, "total_tokens": 33}

    def complete(task: str) -> str:
        state["calls"].append(task)
        i = min(state["i"], len(responses) - 1)
        state["i"] += 1
        return responses[i]

    return complete, model_name, usage, state


def _install_stub(monkeypatch, responses, model_name="stub-graph-model"):
    complete, model_name, usage, state = _stub_model(responses, model_name)

    def _factory():
        return complete, model_name, usage

    monkeypatch.setattr(graph_generation, "default_model", _factory)
    return state


# ---- GraphDTO 构造（与 test_issue305_graph_validate.py 同形状）----

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


def _plain_graph_json():
    """start → assignment → end（合法基线图）。"""
    return {
        "nodes": [
            _node("start", "start"),
            _node("a", "assignment", {"output": "$INPUT.name"}),
            _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
        ],
        "edges": [_edge("start", "a"), _edge("a", "end")],
        "meta": {"flow_id": "gen-plain", "desc": "生成的草稿"},
    }


def _generate(client, prompt="把输入原样返回", runtime=None, hints=None,
              headers=None):
    body = {"prompt": prompt}
    if runtime is not None:
        body["runtime"] = runtime
    if hints is not None:
        body["hints"] = hints
    return client.post("/api/graphs/generate", json=body,
                       headers=headers or _headers())


def _detail_errors(resp):
    return resp.json()["detail"]["errors"]


def _error_codes(items):
    return [e.get("code") for e in items]


# ---------------------------------------------------------------------------
# 1) 契约
# ---------------------------------------------------------------------------

class TestContract:
    def test_valid_generation_200(self, redis, monkeypatch):
        state = _install_stub(monkeypatch, [json.dumps(_plain_graph_json())])
        resp = _generate(_client(redis))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["valid"] is True
        assert body["graph"] == _plain_graph_json()
        # definition 是折叠镜像：可直接走保存面（节点类型逐一对得上）
        assert [n["type"] for n in body["definition"]["nodes"]] == [
            "start", "assignment", "end",
        ]
        gen = body["generation"]
        assert gen["model"] == "stub-graph-model"
        assert gen["attempts"] == 1
        assert isinstance(gen["duration_ms"], int) and gen["duration_ms"] >= 0
        assert gen["usage"] == {"input_tokens": 11, "output_tokens": 22,
                                "total_tokens": 33}
        assert state["calls"], "模型必须被调用恰好一轮"
        assert len(state["calls"]) == 1

    def test_empty_prompt_422(self, redis, monkeypatch):
        _install_stub(monkeypatch, [])
        client = _client(redis)
        for prompt in ("", "   \n  "):
            resp = _generate(client, prompt=prompt)
            assert resp.status_code == 422, resp.text
            assert resp.json()["detail"]["code"] == "empty_prompt"

    def test_invalid_runtime_422(self, redis, monkeypatch):
        _install_stub(monkeypatch, [])
        resp = _generate(_client(redis), runtime="ruby")
        assert resp.status_code == 422, resp.text
        assert resp.json()["detail"]["code"] == "invalid_runtime"

    def test_unknown_request_field_422(self, redis, monkeypatch):
        _install_stub(monkeypatch, [])
        resp = _client(redis).post(
            "/api/graphs/generate",
            json={"prompt": "x", "temperature": 0.7},
            headers=_headers(),
        )
        assert resp.status_code == 422, resp.text

    def test_generation_is_pure_function_no_flow_written(self, redis, monkeypatch):
        """纯函数式端点：生成成功也不落库（保存仍走宿主既有 save）。"""
        _install_stub(monkeypatch, [json.dumps(_plain_graph_json())])
        store = flow_store.get_flow_store()
        resp = _generate(_client(redis))
        assert resp.status_code == 200 and resp.json()["valid"] is True
        assert store.list_flows() == []  # 不落库：保存仍走宿主既有 save

    def test_default_model_not_touched_when_stubbed(self, redis, monkeypatch):
        """打桩替换的就是真 client 构造入口——本用例防「注入缝失配」。"""
        state = _install_stub(monkeypatch, [json.dumps(_plain_graph_json())])
        resp = _generate(_client(redis))
        assert resp.status_code == 200
        assert len(state["calls"]) == 1


# ---------------------------------------------------------------------------
# 2) generate→validate→回喂闭环
# ---------------------------------------------------------------------------

def _no_start_graph_json():
    g = _plain_graph_json()
    g["nodes"] = [n for n in g["nodes"] if n["data"]["type"] != "start"]
    g["edges"] = [e for e in g["edges"] if e["source"] != "start"]
    return g


class TestGenerateValidateLoop:
    def test_self_correction_second_attempt_converges(self, redis, monkeypatch):
        state = _install_stub(monkeypatch, [
            json.dumps(_no_start_graph_json()),   # 首轮缺 start
            json.dumps(_plain_graph_json()),      # 回喂后自纠
        ])
        resp = _generate(_client(redis))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["valid"] is True
        assert body["generation"]["attempts"] == 2
        assert len(state["calls"]) == 2
        # 第二轮 prompt 携带首轮 validate 错误（回喂闭环）
        assert "graph.no_start" in state["calls"][1]

    def test_invalid_after_retries_errors_returned_verbatim(self, redis, monkeypatch):
        _install_stub(monkeypatch, [json.dumps(_no_start_graph_json())])
        resp = _generate(_client(redis))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["valid"] is False
        codes = _error_codes(body["errors"])
        assert "graph.no_start" in codes
        first = body["errors"][0]
        # validate 错误项逐字段保真（type/code/message/severity/node_id）
        assert first["type"] == "node"
        assert first["severity"] == "error"
        assert "message" in first
        assert body["graph"] is not None  # 最后一次草稿随响应带回供排查

    def test_capability_error_shape_preserved(self, redis, monkeypatch):
        g = _plain_graph_json()
        g["nodes"].insert(1, _node("m", "map", {
            "over": "$INPUT.items",
            "childFlow": {
                "flow_id": "child",
                "nodes": [
                    {"type": "start", "id": "s", "next": "e"},
                    {"type": "end", "id": "e", "output": "$INPUT",
                     "resultType": "success"},
                ],
            },
        }))
        g["edges"] = [_edge("start", "m"), _edge("m", "a"), _edge("a", "end")]
        _install_stub(monkeypatch, [json.dumps(g)])
        resp = _generate(_client(redis), runtime="go")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["valid"] is False
        hits = [e for e in body["errors"]
                if e["code"] == "capability.unsupported_node_type"]
        assert hits, body["errors"]
        assert hits[0]["node_id"] == "m"
        assert "map" in hits[0]["unsupported"]
        assert hits[0]["runtime"] == "go"

    def test_unparseable_output_retried_then_converges(self, redis, monkeypatch):
        state = _install_stub(monkeypatch, [
            "抱歉，我无法生成这张图。",       # 非 JSON
            f"好的：\n```json\n{json.dumps(_plain_graph_json())}\n```",
        ])
        resp = _generate(_client(redis))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["valid"] is True
        assert body["generation"]["attempts"] == 2
        assert "JSON" in state["calls"][1]

    def test_permanently_unparseable_reports_generation_error(self, redis, monkeypatch):
        _install_stub(monkeypatch, ["不是 JSON 的回复"])
        resp = _generate(_client(redis))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["valid"] is False
        assert body["graph"] is None
        assert "generation.unparseable_output" in _error_codes(body["errors"])

    def test_warning_does_not_block(self, redis, monkeypatch):
        g = _plain_graph_json()
        # end 出边=死边 warning（终点用既有节点 a，避免造出孤儿 error）
        g["edges"].append(_edge("end", "a"))
        _install_stub(monkeypatch, [json.dumps(g)])
        resp = _generate(_client(redis))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["valid"] is True
        assert "edge.end_outgoing" in _error_codes(body["errors"])


# ---------------------------------------------------------------------------
# 3) A128-01 自检（豁免类标记信任缝）
# ---------------------------------------------------------------------------

def _childflow():
    return {
        "flow_id": "child",
        "nodes": [
            {"type": "start", "id": "s", "next": "w"},
            {"type": "assignment", "id": "w", "name": "加工",
             "output": "$INPUT.x", "outputType": {"dataType": "object"},
             "next": "e"},
            {"type": "end", "id": "e", "output": "$INPUT",
             "resultType": "success"},
        ],
    }


def _faithful_expanded_map_graph(child_flow=None):
    """编辑器同构的展开态 map：子节点 ``m::id`` 命名空间投影 + owner 双写。

    与 flowConverter jsonToFlow 展开形态一致：projectId 带 ``owner::`` 前缀、
    parentId 指向宿主画布 id、data.isContainerChild/ownerId 双标记、
    data.fields 为 IR 字段平铺（剥拓扑键）。默认内容与 childFlow 忠实一致。
    """
    cf = child_flow if child_flow is not None else _childflow()
    return {
        "nodes": [
            _node("start", "start"),
            _node("m", "map", {"over": "$INPUT.items", "childFlow": cf}),
            _node("end", "end", {"output": "$INPUT", "resultType": "success"}),
            # --- 展开态投影（编辑器形态）---
            {"id": "m::s", "type": "plaitaNode",
             "position": {"x": 20, "y": 56}, "parentId": "m",
             "data": {"type": "start", "name": "m::s", "fields": {},
                      "isContainerChild": True, "ownerId": "m"}},
            {"id": "m::w", "type": "plaitaNode",
             "position": {"x": 20, "y": 128}, "parentId": "m",
             "data": {"type": "assignment", "name": "加工",
                      "fields": {"output": "$INPUT.x",
                                 "outputType": {"dataType": "object"}},
                      "isContainerChild": True, "ownerId": "m"}},
            {"id": "m::e", "type": "plaitaNode",
             "position": {"x": 20, "y": 200}, "parentId": "m",
             "data": {"type": "end", "name": "m::e",
                      "fields": {"output": "$INPUT", "resultType": "success"},
                      "isContainerChild": True, "ownerId": "m"}},
        ],
        "edges": [
            _edge("start", "m"), _edge("m", "end"),
            _edge("m::s", "m::w"), _edge("m::w", "m::e"),  # 子边（折叠不消费）
        ],
    }


class TestA12801SelfCheck:
    def test_fake_projection_named_500_no_draft(self, redis, monkeypatch):
        """手造全豁免子节点（内容未双写）→ 具名 500，不出草稿。"""
        g = _plain_graph_json()
        g["nodes"].append({
            "id": "ghost", "type": "plaitaNode",
            "position": {"x": 0, "y": 0}, "parentId": "a",
            "data": {"type": "end", "name": "ghost",
                     "fields": {"output": "x", "resultType": "success"},
                     "isContainerChild": True, "ownerId": "a"},
        })
        _install_stub(monkeypatch, [json.dumps(g)])
        resp = _generate(_client(redis))
        assert resp.status_code == 500, resp.text
        detail = resp.json()["detail"]
        assert detail["code"] == "generation.childflow_not_double_written"
        assert "ghost" in json.dumps(detail)
        assert "graph" not in detail  # 不出草稿

    def test_twin_missing_from_childflow_named_500(self, redis, monkeypatch):
        """childFlow 在、但投影内容不在其中（A128-01 静默缝本体）。"""
        cf = _childflow()
        cf["nodes"] = [n for n in cf["nodes"] if n["id"] != "w"]  # 丢加工节点
        cf["nodes"][0]["next"] = "e"
        _install_stub(monkeypatch, [json.dumps(_faithful_expanded_map_graph(cf))])
        resp = _generate(_client(redis))
        assert resp.status_code == 500, resp.text
        detail = resp.json()["detail"]
        assert detail["code"] == "generation.childflow_not_double_written"
        assert "m::w" in json.dumps(detail)

    def test_twin_field_divergence_named_500(self, redis, monkeypatch):
        """投影显示的字段值与双写副本不一致（所见非所存）。"""
        cf = _childflow()
        g = _faithful_expanded_map_graph(cf)
        for n in g["nodes"]:
            if n.get("id") == "m::w":
                n["data"]["fields"]["output"] = "$INPUT.evil"  # 投影说一套
        _install_stub(monkeypatch, [json.dumps(g)])
        resp = _generate(_client(redis))
        assert resp.status_code == 500, resp.text
        assert "generation.childflow_not_double_written" == \
            resp.json()["detail"]["code"]

    def test_non_container_host_with_projection_named_500(self, redis, monkeypatch):
        """宿主是非容器节点（assignment）：不可能双写，投影必是伪造。"""
        g = _plain_graph_json()
        g["nodes"].append({
            "id": "a::x", "type": "plaitaNode",
            "position": {"x": 0, "y": 0}, "parentId": "a",
            "data": {"type": "end", "name": "a::x",
                     "fields": {"output": "x", "resultType": "success"},
                     "isContainerChild": True, "ownerId": "a"},
        })
        _install_stub(monkeypatch, [json.dumps(g)])
        resp = _generate(_client(redis))
        assert resp.status_code == 500, resp.text
        assert "generation.childflow_not_double_written" == \
            resp.json()["detail"]["code"]

    def test_faithful_expanded_form_passes(self, redis, monkeypatch):
        """正控：编辑器同构的忠实展开态通过（自检不误报）。"""
        _install_stub(monkeypatch, [json.dumps(_faithful_expanded_map_graph())])
        resp = _generate(_client(redis))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["valid"] is True
        assert body["generation"]["attempts"] == 1
        # 折叠定义里 map 的 childFlow 完整（投影未产生也不丢失内容）
        m = next(n for n in body["definition"]["nodes"] if n.get("id") == "m")
        assert [n["id"] for n in m["childFlow"]["nodes"]] == ["s", "w", "e"]

    def test_owner_without_childflow_not_selfcheck_domain(self, redis, monkeypatch):
        """owner 无 childFlow + 投影：validate 具名拦（非静默缝）→ 走
        回喂/带回面而非 500。"""
        g = _plain_graph_json()
        g["nodes"][1] = _node("m", "map", {"over": "$INPUT.items"})
        g["nodes"].append({
            "id": "m::s", "type": "plaitaNode",
            "position": {"x": 0, "y": 0}, "parentId": "m",
            "data": {"type": "start", "name": "m::s", "fields": {},
                     "isContainerChild": True, "ownerId": "m"},
        })
        g["edges"] = [_edge("start", "m"), _edge("m", "end")]
        _install_stub(monkeypatch, [json.dumps(g)])
        resp = _generate(_client(redis))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["valid"] is False
        assert "node.child_flow_missing" in _error_codes(body["errors"])


# ---------------------------------------------------------------------------
# 4) 租户与鉴权（与 Phase 1 validate 同口径）
# ---------------------------------------------------------------------------

class TestTenantAndAuth:
    def test_requires_auth(self, redis, monkeypatch):
        _install_stub(monkeypatch, [])
        resp = _client(redis).post("/api/graphs/generate",
                                   json={"prompt": "x"})
        assert resp.status_code == 401, resp.text

    def test_viewer_forbidden_by_editor_gate(self, tmp_path, monkeypatch):
        """viewer 写闸 403（editor 及以上）——鉴权与 Phase 1 validate 同口径。"""
        from api import auth_users

        monkeypatch.setenv("PLAITA_CONSOLE_DB_URL",
                           f"sqlite:///{tmp_path}/gen305.db")
        monkeypatch.setattr("config.get_settings", lambda: _settings())
        monkeypatch.setattr("auth.get_settings", lambda: _settings())
        flow_store.init_engine(f"sqlite:///{tmp_path}/gen305.db")
        flow_store.ensure_tenant_bootstrap()
        store = flow_store.get_flow_store()
        users_svc.create_user(
            store, "viewer1", "viewer-password-1", "viewer",
            memberships=[{"tenant_id": "default", "role": "viewer"}],
        )
        app = FastAPI()
        app.state.redis = FakeRedis(decode_responses=True)
        app.state.local_mode = True
        app.state.store = store
        app.include_router(auth_users.router, prefix="/api")
        app.include_router(
            graphs_api.router, prefix="/api",
            dependencies=[Depends(require_auth)],
        )
        client = TestClient(app)
        r = client.post("/api/auth/login",
                        json={"username": "viewer1",
                              "password": "viewer-password-1"})
        assert r.status_code == 200, r.text
        token = r.json()["token"]
        resp = client.post("/api/graphs/generate", json={"prompt": "x"},
                           headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 403, resp.text
        assert "editor" in resp.json()["detail"]

    def test_reference_resolves_within_request_tenant(self, redis, monkeypatch):
        def _ref_graph(flow_id):
            return {
                "nodes": [
                    _node("start", "start"),
                    _node("ref", "reference", {"flow_id": flow_id}),
                    _node("end", "end", {"output": "$INPUT",
                                         "resultType": "success"}),
                ],
                "edges": [_edge("start", "ref"), _edge("ref", "end")],
            }

        store = flow_store.get_flow_store()
        store.create_flow("lib-flow", tenant_id="tenant-a")
        state = _install_stub(monkeypatch, [json.dumps(_ref_graph("lib-flow"))])
        client = _client(redis)
        ok = _generate(client, runtime="go",
                       headers={**_headers(), "X-Tenant-ID": "tenant-a"})
        assert ok.status_code == 200, ok.text
        assert ok.json()["valid"] is True
        # 同一草稿换租户：引用目标不跨租户解析 → errors 原样带回 valid=false
        other = _generate(client, runtime="go",
                          headers={**_headers(), "X-Tenant-ID": "tenant-b"})
        assert other.status_code == 200, other.text
        body = other.json()
        assert body["valid"] is False
        hits = [e for e in body["errors"]
                if e["code"] == "reference_target_missing"]
        assert hits and hits[0]["node_id"] == "ref"
        assert "tenant-a" not in hits[0]["message"]

    def test_hints_injected_into_model_prompt(self, redis, monkeypatch):
        state = _install_stub(monkeypatch, [json.dumps(_plain_graph_json())])
        resp = _generate(
            _client(redis),
            hints={
                "preferred_node_types": ["map", "http"],
                "reference_flows": [
                    {"flow_id": "lib-1", "desc": "取数"},
                    {"flow_id": "lib-2", "desc": "写回"},
                ],
            },
        )
        assert resp.status_code == 200, resp.text
        task = state["calls"][0]
        assert "map" in task and "http" in task
        assert "lib-1" in task and "取数" in task
        assert "lib-2" in task and "写回" in task
        assert "reference" in task  # 可引用子流程走 reference 节点的指引在

    def test_client_construction_failure_named_503(self, redis, monkeypatch):
        def _broken():
            raise RuntimeError("flowcast providers.json 缺少 provider")

        monkeypatch.setattr(graph_generation, "default_model", _broken)
        resp = _generate(_client(redis))
        assert resp.status_code == 503, resp.text
        assert resp.json()["detail"]["code"] == "generation_client_unavailable"
