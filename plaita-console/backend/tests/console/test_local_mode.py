"""本地单机模式（无 Redis）端到端单测。

覆盖：示例流程种子、进程内执行、节点级 trace、状态终结、
本地模式下的执行 CRUD，以及集群接口的 503 提示。
"""
import json
import sys
import time
from pathlib import Path

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import executions as executions_api  # noqa: E402
from api import queues as queues_api  # noqa: E402
from services import examples, flow_store  # noqa: E402
from services import examples as examples_svc  # noqa: E402


@pytest.fixture()
def app(tmp_path) -> FastAPI:
    db_file = tmp_path / "local.db"
    flow_store.init_engine(f"sqlite:///{db_file}")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.include_router(executions_api.router, prefix="/api")
    app.include_router(queues_api.router, prefix="/api")
    return app


@pytest.fixture()
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def _wait_completed(client: TestClient, execution_id: str, timeout=10.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = client.get(f"/api/executions/{execution_id}").json()
        if body["status"] in ("completed", "failed", "cancelled"):
            return body
        time.sleep(0.1)
    raise AssertionError(f"执行未在 {timeout}s 内终结: {body}")


def test_seed_example_flows(app: FastAPI):
    created = examples.seed_example_flows()
    assert created == 3
    # 幂等：第二次不再写入
    assert examples.seed_example_flows() == 0
    flow_ids = {f.flow_id for f in flow_store.get_flow_store().list_flows()}
    assert {"hello-plaita", "list-map", "http-echo"} == flow_ids


def test_local_run_hello_plaita(app: FastAPI, client: TestClient):
    examples.seed_example_flows()
    r = client.post("/api/executions", json={"flow_id": "hello-plaita"})
    assert r.status_code == 200
    execution_id = r.json()["execution_id"]

    body = _wait_completed(client, execution_id)
    assert body["status"] == "completed"
    assert body["output"]["message"] == "你好，Plaita！"
    assert [n["status"] for n in body["nodes"]] == ["success", "success", "success"]
    assert body["nodes"][1]["output"]["message"] == "你好，Plaita！"


def test_local_run_list_map_with_child_trace(app: FastAPI, client: TestClient):
    examples.seed_example_flows()
    r = client.post("/api/executions", json={"flow_id": "list-map"})
    assert r.status_code == 200
    body = _wait_completed(client, r.json()["execution_id"])
    assert body["status"] == "completed"
    assert body["output"] == [
        {"item": 1, "index": 0},
        {"item": 2, "index": 1},
        {"item": 3, "index": 2},
    ]
    # 子流程节点也进 trace
    child_ids = [n["id"] for n in body["nodes"] if n["id"].startswith("c-")]
    assert len(child_ids) == 9  # 3 项 × (c-start/c-echo/c-end)


def test_local_run_unpublished_rejected(app: FastAPI, client: TestClient):
    flow_store.get_flow_store().ensure_flow("draft-flow")
    flow_store.get_flow_store().save_flow_definition(
        "draft-flow", "0.1.0", '{"nodes": []}', status="draft"
    )
    r = client.post("/api/executions", json={"flow_id": "draft-flow"})
    assert r.status_code == 400
    assert "已发布版本" in r.json()["detail"]


def test_local_delete_execution(app: FastAPI, client: TestClient):
    examples.seed_example_flows()
    r = client.post("/api/executions", json={"flow_id": "hello-plaita"})
    execution_id = r.json()["execution_id"]
    _wait_completed(client, execution_id)

    assert client.delete(f"/api/executions/{execution_id}").status_code == 200
    assert client.get(f"/api/executions/{execution_id}").status_code == 404


def test_cluster_api_returns_503_in_local_mode(client: TestClient):
    r = client.get("/api/queues")
    assert r.status_code == 503
    assert "本地单机模式" in r.json()["detail"]


# ---- 挂起-恢复（本地分布式执行） ----

APPROVAL_DEF = json.dumps({"nodes": [
    {"type": "start", "id": "start", "next": "wait"},
    {"type": "event", "id": "wait", "event_type": "approval", "next": "end"},
    {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.wait"},
]})


def test_approval_suspend_then_resume(app: FastAPI, client: TestClient):
    """本地分布式执行：审批事件挂起 → /resume 恢复 → 完成。"""
    store = flow_store.get_flow_store()
    store.ensure_flow("approval-demo")
    store.save_flow_definition("approval-demo", "1.0.0", APPROVAL_DEF, status="draft")
    store.publish_version("approval-demo", "1.0.0")

    r = client.post("/api/executions", json={"flow_id": "approval-demo"})
    assert r.status_code == 200
    eid = r.json()["execution_id"]

    # 等待挂起
    deadline = time.time() + 10
    while time.time() < deadline:
        body = client.get(f"/api/executions/{eid}").json()
        if body["status"] == "suspended":
            break
        time.sleep(0.2)
    assert body["status"] == "suspended", body

    # 恢复（审批通过）
    r = client.post(f"/api/executions/{eid}/resume",
                    json={"resume_type": "event", "data": {"approved": True}})
    assert r.status_code == 200

    deadline = time.time() + 10
    while time.time() < deadline:
        body = client.get(f"/api/executions/{eid}").json()
        if body["status"] in ("completed", "failed"):
            break
        time.sleep(0.2)
    assert body["status"] == "completed"
    assert body["output"]["event_data"] == {"approved": True}
    # checkpoint 已持久化
    assert body["context"]


def test_resume_rejects_non_suspended(app: FastAPI, client: TestClient):
    examples_svc.seed_example_flows()
    r = client.post("/api/executions", json={"flow_id": "hello-plaita"})
    eid = r.json()["execution_id"]
    deadline = time.time() + 10
    while time.time() < deadline:
        body = client.get(f"/api/executions/{eid}").json()
        if body["status"] in ("completed", "failed", "cancelled"):
            break
        time.sleep(0.2)
    r = client.post(f"/api/executions/{eid}/resume",
                    json={"resume_type": "event", "data": {}})
    assert r.status_code == 400
    assert "挂起" in r.json()["detail"]
