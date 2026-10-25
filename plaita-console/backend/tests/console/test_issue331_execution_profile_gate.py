"""#331 执行要求闸（console 面）：publish / start × 目标执行环境。

负控（先红后绿；引擎侧执行入口四路 / 递归闭包 / runner 兜底 /
矩阵差分在 plaita ``tests/unit/test_issue331_execution_profile.py``）。
台阶化 2026-10-25：``local`` 归一化为 ``managed``（托管·无队列服务）；错误体
``required``/``missing`` 均为台阶名：

1. **publish 格**：本地单机档发布含 ``delay`` 的 flow → 422
   ``queue_service_absent``（wake_service 点名，先于台阶闸；publish 与
   import-version 经 ``_publish_gates`` 同闸）。
2. **start 格**：本地档 start 一个直置 published 的 delay flow（直置以隔离
   被测面）→ 422 且**不产生执行行**（绝不出现「suspended 行永不唤醒」）。
3. **睡死坑修复钉**：approval 声明 ``wake_service="approval"``（审批走队列 +
   服务登记/决策转发，服务不在场则待办永不创建）——本地档 start 即 422。
4. 正控：event flow（``managed ⊆ managed``）本地 start 正常挂起（suspended
   行）——闸只拒队列服务族，不误伤事件挂起面。
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


def _event_def(flow_id="waiter"):
    return json.dumps({
        "flow_id": flow_id,
        "nodes": [
            {"type": "start", "id": "start", "next": "w"},
            {"type": "event", "id": "w", "event_type": "go", "next": "end"},
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
    """publish 格：本地档点名（静态，两级模型）——delay 声明 wake_service，
    本地档无唤醒服务 → 422 queue_service_absent，版本保持 draft。"""
    store = flow_store.get_flow_store()
    store.ensure_flow("delayed", tenant_id="default")
    store.save_flow_definition("delayed", "1.0.0", _delay_def(), status="draft",
                               tenant_id="default")
    r = client.post("/api/flows/delayed/publish", json={"version": "1.0.0"})
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    assert detail["code"] == "queue_service_absent"
    assert detail["wake_services"] == ["delay"]
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
    assert detail["code"] == "queue_service_absent"
    assert detail["wake_services"] == ["delay"]
    assert list_local_execution_ids() == []


def list_local_execution_ids():
    from services import local_executor

    return [e["execution_id"] for e in local_executor.list_local_executions()]


def test_local_start_approval_flow_rejected_422_sleep_death_fix(app, client):
    """睡死坑修复钉（两级模型）：approval 声明 wake_service="approval"——
    本地档点名静态拒，start 即 422（改动前正控：被放行、挂起、无人唤醒）。"""
    store = flow_store.get_flow_store()
    store.ensure_flow("approved", tenant_id="default")
    store.save_flow_definition("approved", "1.0.0", _approval_def(), status="published",
                               tenant_id="default")
    r = client.post("/api/executions", json={"flow_id": "approved"})
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    assert detail["code"] == "queue_service_absent"
    assert detail["wake_services"] == ["approval"]
    assert list_local_execution_ids() == []


def test_local_start_event_flow_suspends_positive_control(app, client):
    """正控：event flow（managed ⊆ managed）本地 start 正常挂起——闸只拒队列
    服务族，事件挂起面不受牵连（进程内事件总线 + 手工 resume 可恢复）。"""
    store = flow_store.get_flow_store()
    store.ensure_flow("waiter", tenant_id="default")
    store.save_flow_definition("waiter", "1.0.0", _event_def(), status="published",
                               tenant_id="default")
    r = client.post("/api/executions", json={"flow_id": "waiter"})
    assert r.status_code == 200, r.text
    body = _wait_suspended(client, r.json()["execution_id"])
    assert body["status"] == "suspended"
