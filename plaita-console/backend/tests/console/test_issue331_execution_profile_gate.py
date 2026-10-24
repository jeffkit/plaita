"""#331 执行要求闸（console 面）：publish / start × 目标执行档 profile。

负控（先红后绿；引擎侧执行入口四路 / 递归闭包 / runner 兜底 /
矩阵差分在 plaita ``tests/unit/test_issue331_execution_profile.py``）：

1. **publish 格**：本地单机档（``redis=None`` → ``local`` profile：进程内事件
   总线可挂起/恢复，**无**持久队列+延迟排程）发布含 ``delay`` 的 flow → 422
   结构化拒（``error=suspend_not_executable``、``missing=["queue"]``、
   ``profile=local``），版本保持 draft（publish 与 import-version 经
   ``_publish_gates`` 同闸）。
2. **start 格**：本地档 start 一个直置 published 的 delay flow（直置以隔离
   被测面）→ 422 且**不产生执行行**（绝不出现「suspended 行永不唤醒」）。
3. 正控：approval flow（``event_bus + suspend:root ⊆ local``）本地 start
   正常挂起（suspended 行）——闸只拒 queue 族，不误伤挂起面。
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
from api import flows as flows_api  # noqa: E402
from services import flow_store  # noqa: E402


@pytest.fixture()
def app(tmp_path) -> FastAPI:
    flow_store.init_engine(f"sqlite:///{tmp_path}/issue331.db")
    app = FastAPI()
    app.state.redis = None  # 本地单机档：profile=local
    app.state.local_mode = True
    app.include_router(flows_api.router, prefix="/api")
    app.include_router(executions_api.router, prefix="/api")
    return app


@pytest.fixture()
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def _delay_def(flow_id="delayed"):
    return json.dumps({
        "flow_id": flow_id,
        "nodes": [
            {"type": "start", "id": "start", "next": "d"},
            {"type": "delay", "id": "d", "delay_seconds": 30, "next": "end"},
            {"type": "end", "id": "end", "output": "x"},
        ],
    })


def _approval_def(flow_id="approved"):
    return json.dumps({
        "flow_id": flow_id,
        "nodes": [
            {"type": "start", "id": "start", "next": "ap"},
            {"type": "approval", "id": "ap", "event_type": "ap_decision",
             "approval_title": "t", "approval_content": "c",
             "approvers": ["u1"], "next": "end"},
            {"type": "end", "id": "end", "output": "x"},
        ],
    })


def _wait_suspended(client: TestClient, execution_id: str, timeout=10.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = client.get(f"/api/executions/{execution_id}").json()
        if body["status"] in ("suspended", "completed", "failed", "cancelled"):
            return body
        time.sleep(0.1)
    raise AssertionError(f"执行未在 {timeout}s 内到态: {body}")


def test_local_publish_delay_flow_rejected_422(app, client):
    """publish 格：local 档缺 ``queue``——delay flow 发布即结构化拒，版本保持
    draft（改动前：本地档发布放行，start 后 delay 挂起永不唤醒）。"""
    store = flow_store.get_flow_store()
    store.ensure_flow("delayed", tenant_id="default")
    store.save_flow_definition("delayed", "1.0.0", _delay_def(), status="draft",
                               tenant_id="default")
    r = client.post("/api/flows/delayed/publish", json={"version": "1.0.0"})
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    assert detail["error"] == "suspend_not_executable"
    assert detail["profile"] == "local"
    assert detail["missing"] == ["queue"]
    assert detail["required"] == ["event_bus", "queue", "suspend:root"]
    assert detail["node_id"] == "d"
    row = store.get_version("delayed", "1.0.0", tenant_id="default")
    assert row.status == "draft"


def test_local_start_delay_flow_rejected_422_no_execution_row(app, client):
    """start 格：直置 published（隔离 publish 闸）后本地 start → 422，且不产生
    执行行/后台线程（改动前：start 受理，suspended 行永不唤醒）。"""
    store = flow_store.get_flow_store()
    store.ensure_flow("delayed", tenant_id="default")
    store.save_flow_definition("delayed", "1.0.0", _delay_def(), status="published",
                               tenant_id="default")
    r = client.post("/api/executions", json={"flow_id": "delayed"})
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    assert detail["error"] == "suspend_not_executable"
    assert detail["missing"] == ["queue"]
    assert detail["profile"] == "local"
    assert list_local_execution_ids() == []


def list_local_execution_ids():
    from services import local_executor

    return [e["execution_id"] for e in local_executor.list_local_executions()]


def test_local_start_approval_flow_suspends_positive_control(app, client):
    """正控：approval（event_bus+suspend:root ⊆ local）本地 start 正常挂起——
    闸只拒 queue 族，挂起面不受牵连（ADR-001 §3.4 local 后端语义）。"""
    store = flow_store.get_flow_store()
    store.ensure_flow("approved", tenant_id="default")
    store.save_flow_definition("approved", "1.0.0", _approval_def(), status="published",
                               tenant_id="default")
    r = client.post("/api/executions", json={"flow_id": "approved"})
    assert r.status_code == 200, r.text
    body = _wait_suspended(client, r.json()["execution_id"])
    assert body["status"] == "suspended"
