"""A2R24-10 请求体未知字段 422 回归（B2 第四十九轮）。

拼错 ``params`` / ``base_updated_at`` 不能再被 Pydantic 默认的
``extra="ignore"`` 静默吞掉：前者导致流程以空参数启动，后者直接绕过乐观锁
覆盖草稿。除 ``DispatchOneRequest``（#262 服务端权威字段，显式 ignore）外，
全部请求模型必须继承 ``StrictRequestModel``。
"""
import importlib
import json
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import FastAPI
from pydantic import BaseModel
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
from request_models import StrictRequestModel  # noqa: E402
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

    def test_every_json_request_body_forbids_unknown_fields(self):
        """全端点扫尾：OpenAPI 中每个 JSON requestBody 都必须是 forbid。

        A2R24-10 的端面口径是「所有请求模型」；上一测试按类名（``*Request``）
        扫，本测试反过来从 OpenAPI 的 requestBody 出发，覆盖匿名/内联模型和
        将来新增的模块，避免只改了类名或漏了路由就静默放过。

        唯一例外 ``POST /api/dispatch/one``（#262：``shadow``/``dry_run`` 是
        服务端权威字段，请求体同名字段按文档忽略，见 ``DispatchOneRequest``）。
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
        exemptions = set()
        for path, ops in spec["paths"].items():
            for method, op in ops.items():
                if method not in ("get", "post", "put", "delete", "patch"):
                    continue
                request_body = op.get("requestBody")
                if not request_body:
                    continue
                for content_type, media in request_body.get("content", {}).items():
                    if not content_type.startswith("application/json"):
                        continue
                    schema = media.get("schema", {})
                    ref = schema.get("$ref")
                    model_name = ref.rsplit("/", 1)[-1] if ref else None
                    target = schemas.get(model_name, schema) if model_name else schema
                    if path == "/api/dispatch/one":
                        assert model_name == "DispatchOneRequest"
                        exemptions.add(path)
                        continue
                    assert target.get("additionalProperties") is False, (
                        f"{method.upper()} {path} 的 {model_name} 未 forbid 未知字段"
                    )
                    checked += 1
        # 40 个 JSON body operation（38 个模型，StopService/CreateUser 各复用）；
        # 防止路由改名后扫尾静默空转。
        assert checked >= 38, checked
        assert exemptions == {"/api/dispatch/one"}
