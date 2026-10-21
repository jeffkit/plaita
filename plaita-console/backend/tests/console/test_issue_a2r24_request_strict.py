"""A2R24-10 请求体未知字段 422 回归（B2 第四十九轮；B1 第四十九轮全端点扫尾）。

拼错 ``params`` / ``base_updated_at`` 不能再被 Pydantic 默认的
``extra="ignore"`` 静默吞掉：前者导致流程以空参数启动，后者直接绕过乐观锁
覆盖草稿。除 ``DispatchOneRequest``（#262 服务端权威字段，显式 ignore）外，
全部请求模型必须继承 ``StrictRequestModel``。

「全端点」还包括**没有 body 参数**的写端点（cancel / trigger / logout /
rotate-secret / 各 DELETE）：此前 Starlette 把请求体读走后丢弃，传
``{"reason": ...}`` 同样 200。它们挂 ``EmptyRequestModel``——空体 / ``{}``
保持 200，任何字段 422。仅 AG-UI 透传（copilot）与 form 契约
（flowVersion/semver/detail）豁免。
"""
import importlib
import json
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import FastAPI
from pydantic import BaseModel, ValidationError
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users as auth_users_api  # noqa: E402
from api import capacity as capacity_api  # noqa: E402
from api import cluster as cluster_api  # noqa: E402
from api import credentials as credentials_api  # noqa: E402
from api import dryrun as dryrun_api  # noqa: E402
from api import events as events_api  # noqa: E402
from api import executions as executions_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from api import nodes as nodes_api  # noqa: E402
from api import property_types as property_types_api  # noqa: E402
from api import queues as queues_api  # noqa: E402
from api import schedules as schedules_api  # noqa: E402
from api import services as services_api  # noqa: E402
from api import supervisor as supervisor_api  # noqa: E402
from api import tenants as tenants_api  # noqa: E402
from auth import require_auth  # noqa: E402
from plaita.server.services.schedule_service import SCHEDULES_KEY  # noqa: E402
from request_models import EmptyRequestModel, StrictRequestModel  # noqa: E402
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


def _body_forbids_extra(schema: dict, schemas: dict) -> bool:
    """``additionalProperties: false``，含 Optional 空体模型的 anyOf 形状。"""
    if "$ref" in schema:
        name = schema["$ref"].rsplit("/", 1)[-1]
        return _body_forbids_extra(schemas[name], schemas)
    branches = schema.get("anyOf") or schema.get("oneOf")
    if branches:
        return all(
            _body_forbids_extra(branch, schemas)
            for branch in branches
            if branch.get("type") != "null"
        )
    return schema.get("additionalProperties") is False


@pytest.fixture()
def flow_client(tmp_path):
    flow_store.init_engine(f"sqlite:///{tmp_path / 'flows.db'}")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = flow_store.get_flow_store()
    app.include_router(flows_api.router, prefix="/api")
    return TestClient(app)


@pytest.fixture()
def cluster_execution_client():
    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(executions_api.router, prefix="/api")
    return TestClient(app), redis


@pytest.fixture()
def cluster_schedule_client():
    redis = FakeRedis(decode_responses=True)
    redis.hset(
        SCHEDULES_KEY,
        "s1",
        json.dumps({
            "schedule_id": "s1",
            "tenant_id": "default",
            "name": "n",
            "flow_id": "f",
            "version": "1.0.0",
            "cron": "* * * * *",
            "params": {},
            "enabled": True,
            "created_at": "t0",
            "updated_at": "t0",
            "next_run_at": "0",
        }),
    )
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(schedules_api.router, prefix="/api")
    return TestClient(app), redis


@pytest.fixture()
def auth_client(tmp_path):
    flow_store.init_engine(f"sqlite:///{tmp_path / 'auth.db'}")
    app = FastAPI()
    app.dependency_overrides[require_auth] = lambda: {
        "actor": "root",
        "username": "root",
        "role": "admin",
        "tenant_id": "default",
        "platform_admin": True,
    }
    app.include_router(auth_users_api.router, prefix="/api")
    return TestClient(app)


@pytest.fixture()
def nodes_client(tmp_path):
    flow_store.init_engine(f"sqlite:///{tmp_path / 'nodes.db'}")
    app = FastAPI()
    app.include_router(nodes_api.router, prefix="/api")
    return TestClient(app)


@pytest.fixture()
def credential_client(tmp_path, monkeypatch):
    db_file = tmp_path / "cred.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    app = FastAPI()
    app.include_router(credentials_api.router, prefix="/api")
    return TestClient(app)


class TestStrictRequestModels:
    def test_typo_params_returns_422(self, cluster_execution_client):
        client, _redis = cluster_execution_client
        r = client.post(
            "/api/executions", json={"flow_id": "hello", "paramz": {"name": "x"}}
        )
        assert r.status_code == 422, r.text
        assert "params" in r.text or "extra" in r.text

    def test_typo_base_updated_at_returns_422(self, flow_client):
        client = flow_client
        assert client.post("/api/flows", json={"flow_id": "echo"}).status_code == 200
        r = client.put(
            "/api/flows/echo/versions/0.0.1",
            json={
                "definition": _echo_def(),
                # 拼错乐观锁基准：旧行为静默 200 覆盖，现在必须 422。
                "base_updated_at_typo": "2000-01-01T00:00:00",
            },
        )
        assert r.status_code == 422, r.text

    def test_correct_base_updated_at_still_accepted(self, flow_client):
        client = flow_client
        assert client.post("/api/flows", json={"flow_id": "echo"}).status_code == 200
        saved = client.put(
            "/api/flows/echo/versions/0.0.1", json={"definition": _echo_def()}
        )
        assert saved.status_code == 200
        base = saved.json()["updated_at"]
        assert base
        # 正确的乐观锁字段仍走原来的冲突检查路径（不是被 422 误伤）。
        r = client.put(
            "/api/flows/echo/versions/0.0.1",
            json={"definition": _echo_def(), "base_updated_at": base},
        )
        assert r.status_code == 200, r.text

    def test_dispatch_one_keeps_documented_ignore(self):
        # #262：shadow 是服务端权威字段，请求体同名字段被显式忽略。
        req = capacity_api.DispatchOneRequest(issue_id="issue-1", shadow=True)
        assert "shadow" not in req.model_dump()

    def test_all_request_models_forbid_unknown_fields(self):
        modules = [
            auth_users_api,
            capacity_api,
            cluster_api,
            credentials_api,
            dryrun_api,
            events_api,
            executions_api,
            flows_api,
            nodes_api,
            property_types_api,
            queues_api,
            schedules_api,
            services_api,
            supervisor_api,
            tenants_api,
        ]
        checked = 0
        for module in modules:
            for name, cls in vars(module).items():
                if not name.endswith("Request") or not isinstance(cls, type):
                    continue
                # 只检查本模块定义的 Pydantic 模型（排除 `Request` 导入别名）。
                if cls.__module__ != module.__name__ or not issubclass(cls, BaseModel):
                    continue
                if name == "DispatchOneRequest":
                    # 唯一显式例外：请求体字段不生效（见模型注释）。
                    assert cls.model_config.get("extra") == "ignore"
                    continue
                assert issubclass(cls, StrictRequestModel), f"{module.__name__}.{name}"
                checked += 1
        # 防止模块改名/移动后本测试静默空转。
        assert checked >= 30

    def test_empty_request_model_rejects_every_field(self):
        assert EmptyRequestModel.model_config.get("extra") == "forbid"
        assert EmptyRequestModel.model_fields == {}
        assert EmptyRequestModel().model_dump() == {}
        with pytest.raises(ValidationError):
            EmptyRequestModel(reason="x")

    def test_cancel_rejects_unknown_body(self, cluster_execution_client):
        """执行控制类：``cancel`` 此前没有 body 参数，``reason`` 被静默丢弃。"""
        client, redis = cluster_execution_client
        redis.set(
            "plaita:execution:e1",
            json.dumps({"flow_id": "f", "status": "running"}),
        )
        r = client.post("/api/executions/e1/cancel", json={"reason": "operator"})
        assert r.status_code == 422, r.text
        assert "extra_forbidden" in r.text
        # 422 发生在 handler 之前：取消意图键未写入（拒绝无副作用）。
        assert redis.get("plaita:execution:cancel:e1") is None

    def test_logout_rejects_unknown_body(self, auth_client):
        """会话控制类：``logout`` 的 ``all_sessions`` 等字段不再被静默忽略。"""
        client = auth_client
        r = client.post(
            "/api/auth/logout",
            json={"all_sessions": True},
            headers={"Authorization": "Bearer test-token"},
        )
        assert r.status_code == 422, r.text
        assert "extra_forbidden" in r.text

    def test_schedule_trigger_rejects_unknown_body(self, cluster_schedule_client):
        """调度控制类：``trigger`` 此前静默吞掉 ``params`` 等字段。"""
        client, redis = cluster_schedule_client
        r = client.post("/api/schedules/s1/trigger", json={"params": {"x": 1}})
        assert r.status_code == 422, r.text
        assert "extra_forbidden" in r.text
        assert redis.xlen(executions_api._task_queue_name()) == 0

    def test_flow_delete_rejects_unknown_body(self, flow_client):
        """流程生命周期（DELETE）：请求体里的 ``force`` 不再被静默忽略。"""
        client = flow_client
        assert client.post("/api/flows", json={"flow_id": "echo"}).status_code == 200
        r = client.request("DELETE", "/api/flows/echo", json={"force": True})
        assert r.status_code == 422, r.text
        assert "extra_forbidden" in r.text
        assert client.get("/api/flows/echo").status_code == 200

    def test_node_delete_rejects_unknown_body(self, nodes_client):
        """节点生命周期（DELETE）：未知字段 422 且节点未删除。"""
        client = nodes_client
        assert client.post("/api/nodes", json={"node_type": "tmp"}).status_code == 200
        r = client.request("DELETE", "/api/nodes/tmp", json={"force": True})
        assert r.status_code == 422, r.text
        assert "extra_forbidden" in r.text
        types = {n["node_type"] for n in client.get("/api/nodes").json()["nodes"]}
        assert "tmp" in types

    def test_credential_delete_rejects_unknown_body(self, credential_client):
        """凭据生命周期（DELETE）：未知字段 422 且凭据仍可读。"""
        client = credential_client
        assert client.post("/api/credentials", json={
            "name": "c1", "type": "generic", "data": {"token": "x"},
        }).status_code == 200
        r = client.request("DELETE", "/api/credentials/c1", json={"force": True})
        assert r.status_code == 422, r.text
        assert "extra_forbidden" in r.text
        assert client.get("/api/credentials/c1").status_code == 200

    def test_empty_body_and_empty_object_still_accepted(self, flow_client):
        """向后兼容：无 body / ``{}`` / ``null`` 与收紧前同形（不误伤客户端）。"""
        client = flow_client
        for flow_id in ("a", "b", "c"):
            assert client.post(
                "/api/flows", json={"flow_id": flow_id}
            ).status_code == 200
        assert client.delete("/api/flows/a").status_code == 200
        assert client.request("DELETE", "/api/flows/b", json={}).status_code == 200
        r = client.request(
            "DELETE", "/api/flows/c",
            content=b"null", headers={"content-type": "application/json"},
        )
        assert r.status_code == 200, r.text

    def test_every_json_request_body_forbids_unknown_fields(self):
        """全端点扫尾：OpenAPI 中每个 JSON requestBody 都必须是 forbid。

        A2R24-10 的端面口径是「所有请求模型」；上一测试按类名（``*Request``）
        扫，本测试反过来从 OpenAPI 的 requestBody 出发，覆盖匿名/内联模型和
        将来新增的模块，避免只改了类名或漏了路由就静默放过。

        同时要求**每个写端点都声明请求体**：有字段的走 ``StrictRequestModel``，
        无字段的走 ``EmptyRequestModel``。否则 Starlette 会把请求体读走后丢弃
        （旧 ``cancel``/``trigger``/DELETE 的 200 形态）。

        唯一 ignore 例外 ``POST /api/dispatch/one``（#262：``shadow``/``dry_run``
        是服务端权威字段，请求体同名字段按文档忽略）；无 body 例外只有 AG-UI
        透传与 form 契约两个端点。
        """
        modules = [
            "auth_users",
            "audit",
            "capacity",
            "cluster",
            "copilot",
            "credential_templates",
            "credentials",
            "dryrun",
            "events",
            "executions",
            "flow_version",
            "flows",
            "logs",
            "metrics",
            "nodes",
            "property_types",
            "queues",
            "schedules",
            "services",
            "supervisor",
            "tenants",
        ]
        app = FastAPI()
        for name in modules:
            app.include_router(
                importlib.import_module(f"api.{name}").router, prefix="/api"
            )
        spec = app.openapi()
        schemas = spec.get("components", {}).get("schemas", {})
        checked = 0
        ignored_body_exemptions = set()
        no_body_exemptions = set()
        for path, ops in spec["paths"].items():
            for method, op in ops.items():
                if method not in ("post", "put", "delete", "patch"):
                    continue
                request_body = op.get("requestBody")
                if not request_body:
                    # 没有 body 的写端点也必须显式建模（EmptyRequestModel），
                    # 否则 Starlette 会把任意请求体读走后丢弃。
                    no_body_exemptions.add((method.upper(), path))
                    continue
                for content_type, media in request_body.get("content", {}).items():
                    if not content_type.startswith("application/json"):
                        continue
                    schema = media.get("schema", {})
                    ref = schema.get("$ref")
                    model_name = ref.rsplit("/", 1)[-1] if ref else None
                    if path == "/api/dispatch/one":
                        assert model_name == "DispatchOneRequest"
                        ignored_body_exemptions.add(path)
                        continue
                    assert _body_forbids_extra(schema, schemas), (
                        f"{method.upper()} {path} 的 {model_name} 未 forbid 未知字段"
                    )
                    checked += 1
        # 66 个 JSON body operation（含 27 个 EmptyRequestModel 的 anyOf 形状）；
        # 防止路由改名后扫尾静默空转。
        assert checked >= 66, checked
        assert ignored_body_exemptions == {"/api/dispatch/one"}
        # 仅有的两个非模型请求面：AG-UI 透传与 form 契约（见 request_models.py）。
        assert no_body_exemptions == {
            ("POST", "/api/copilot"),
            ("POST", "/api/flowVersion/semver/detail"),
        }
