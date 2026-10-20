"""A2R24 投影一致性回归（B2 第四十九轮）。

覆盖：

① list/detail 投影字段一致性：flow（tenant_id/created_at/updated_at）、
   credential（updated_at）、queue（queue_type）；每类资源一条断言。
② ``ExecutionInfo.flow_name``：集群档 Lua 投影/构造器传入的值不再被
   Pydantic ``extra="ignore"`` 静默丢弃；本地档字段存在（null=未提供）。

请求侧严格校验（A2R24-10）见 ``test_issue_a2r24_request_strict.py``。
"""
import json
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import credentials as credentials_api  # noqa: E402
from api import executions as executions_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from api import queues as queues_api  # noqa: E402
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


@pytest.fixture()
def queue_client():
    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(queues_api.router, prefix="/api")
    return TestClient(app), redis


@pytest.fixture()
def cluster_execution_client():
    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(executions_api.router, prefix="/api")
    return TestClient(app), redis


@pytest.fixture()
def local_execution_client(tmp_path):
    flow_store.init_engine(f"sqlite:///{tmp_path / 'local.db'}")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = flow_store.get_flow_store()
    app.include_router(executions_api.router, prefix="/api")
    return TestClient(app)


class TestProjectionParity:
    def test_flow_detail_has_list_fields(self, flow_client):
        client = flow_client
        assert client.post("/api/flows", json={"flow_id": "echo", "desc": "回声"}).status_code == 200
        assert client.put(
            "/api/flows/echo/versions/0.0.1", json={"definition": _echo_def()}
        ).status_code == 200

        list_row = client.get("/api/flows").json()["flows"][0]
        detail = client.get("/api/flows/echo").json()

        # 逐资源钉子（A2R24-07）：detail ⊇ list 的实体字段。
        assert set(list_row) <= set(detail)
        for key in ("flow_id", "tenant_id", "author", "desc", "created_at", "updated_at"):
            assert detail[key] == list_row[key], key
        assert detail["versions"][0]["version"] == "0.0.1"
        assert set(detail["versions"][0]) == {
            "version", "status", "created_at", "published_at", "updated_at"
        }

    def test_credential_detail_has_list_fields(self, credential_client):
        client = credential_client
        assert client.post(
            "/api/credentials",
            json={
                "name": "feishu-bot",
                "type": "webhook",
                "data": {"url": "https://example.com/hook", "secret": "s3cret"},
                "desc": "飞书机器人",
            },
        ).status_code == 200

        list_row = client.get("/api/credentials").json()["credentials"][0]
        detail = client.get("/api/credentials/feishu-bot").json()

        assert set(list_row) <= set(detail)
        assert detail["updated_at"] == list_row["updated_at"]
        assert detail["data"]["secret"] == "s3cret"

    def test_queue_detail_queue_type_matches_list(self, queue_client):
        client, redis = queue_client
        redis.xadd(
            "plaita:flow:queue",
            {"payload": json.dumps({"type": "start", "tenant_id": "default", "flow_id": "f1"})},
        )
        redis.rpush(
            "plaita:delay:queue",
            json.dumps({"type": "start", "tenant_id": "default", "flow_id": "f1"}),
        )

        list_rows = {row["name"]: row for row in client.get("/api/queues").json()["queues"]}

        # 已存在的 Stream / list 键：详情与列表同口径。
        for name in ("plaita:flow:queue", "plaita:delay:queue"):
            detail = client.get(f"/api/queues/{name}").json()
            assert detail["queue_type"] == list_rows[name]["queue_type"]
        assert list_rows["plaita:flow:queue"]["queue_type"] == "stream"
        assert list_rows["plaita:delay:queue"]["queue_type"] == "list"

        # 登记但尚未创建的 Stream 键：列表零值行与详情都报 stream。
        detail = client.get("/api/queues/plaita:flow:queue:v2").json()
        assert detail["queue_type"] == "stream"
        assert detail["queue_type"] == list_rows["plaita:flow:queue:v2"]["queue_type"]


class TestExecutionFlowName:
    def test_cluster_list_and_detail_include_flow_name(self, cluster_execution_client):
        client, redis = cluster_execution_client
        redis.set(
            "plaita:execution:exec-1",
            json.dumps(
                {
                    "execution_id": "exec-1",
                    "flow_id": "greeting",
                    "flow_name": "greeting-flow",
                    "flow_version": "1.0.0",
                    "status": "completed",
                    "start_time": "2026-10-01T00:00:00",
                    "end_time": "2026-10-01T00:00:01",
                }
            ),
        )

        rows = client.get("/api/executions").json()["executions"]
        assert len(rows) == 1
        assert rows[0]["flow_name"] == "greeting-flow"

        detail = client.get("/api/executions/exec-1").json()
        assert detail["flow_name"] == "greeting-flow"

    def test_local_list_declares_flow_name(self, local_execution_client):
        client = local_execution_client
        flow_store.insert_local_execution(
            execution_id="local-1", flow_id="greeting", flow_version="1.0.0"
        )

        rows = client.get("/api/executions").json()["executions"]
        assert len(rows) == 1
        # 本地档没有 flow_name 列：字段必须存在且为 null（不是被静默丢弃）。
        assert "flow_name" in rows[0]
        assert rows[0]["flow_name"] is None
