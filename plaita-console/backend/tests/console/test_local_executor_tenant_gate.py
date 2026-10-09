"""停用租户闸（#27）本地档补口：local_executor 的 start/resume。

集群档由 FlowWorker `_dispatch_task` 与 `fire_schedule` 把闸（见
tests/unit/test_tenant_status_gate.py）；本地档没有 worker——执行就在
console 进程内（``local_executor``），闸必须落在本地执行器入口，否则
「停用租户」在本地单机模式下仍是运行面 no-op（start 直接跑、挂起 run
直接 resume）。
"""
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import flow_store as fs  # noqa: E402
from services import local_executor as le  # noqa: E402
from services import tenants_svc  # noqa: E402

DEF = __import__("json").dumps(
    {
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "resultType": "success", "output": "ok"},
        ]
    }
)


def _publish(store, flow_id="hello", tenant_id="acme"):
    store.ensure_flow(flow_id, tenant_id=tenant_id)
    store.save_flow_definition(flow_id, "1.0.0", DEF, status="draft", tenant_id=tenant_id)
    store.publish_version(flow_id, "1.0.0", tenant_id=tenant_id)


def _suspended_execution(store, tenant_id="acme", execution_id="exec-tn-dis"):
    store.ensure_flow("hello", tenant_id=tenant_id)
    store.save_flow_definition("hello", "1.0.0", DEF, status="draft", tenant_id=tenant_id)
    store.publish_version("hello", "1.0.0", tenant_id=tenant_id)
    fs.insert_local_execution(
        execution_id=execution_id,
        flow_id="hello",
        flow_version="1.0.0",
        status="suspended",
        tenant_id=tenant_id,
    )
    # insert_local_execution 不收 context：挂起 checkpoint 直接落列
    import sqlalchemy

    engine = fs.get_init_engine()
    with engine.begin() as conn:
        conn.execute(
            sqlalchemy.text(
                "UPDATE local_executions SET context_json = :c WHERE execution_id = :e"
            ),
            {"c": __import__("json").dumps({"step": 1}), "e": execution_id},
        )
    return execution_id


@pytest.fixture()
def store(tmp_path):
    fs.init_engine(f"sqlite:///{tmp_path}/tenant-gate-local.db")
    flow_store = fs.get_flow_store()
    tenants_svc.create_tenant(flow_store, "acme")
    return flow_store


def test_start_local_execution_blocked_for_disabled_tenant(store):
    _publish(store)
    tenants_svc.set_tenant_status(store, "acme", "disabled")
    with pytest.raises(ValueError, match="已停用"):
        le.start_local_execution(store, "hello", "1.0.0", {}, tenant_id="acme")


def test_start_local_execution_allowed_for_active_tenant(store):
    _publish(store)
    info = le.start_local_execution(store, "hello", "1.0.0", {}, tenant_id="acme")
    assert info["status"] in ("running", "completed")


def test_resume_local_execution_blocked_for_disabled_tenant(store):
    eid = _suspended_execution(store)
    tenants_svc.set_tenant_status(store, "acme", "disabled")
    assert le.resume_local_execution(store, eid, "event", {}, tenant_id="acme") is False


def test_resume_uses_execution_owner_tenant_not_caller_context(store):
    """闸按执行归属租户判（同 worker 消息携带 tenant_id 口径）：
    调用方即使换租户上下文，也恢复不了停用租户的挂起执行。"""
    eid = _suspended_execution(store, tenant_id="acme", execution_id="exec-owner-tn")
    tenants_svc.set_tenant_status(store, "acme", "disabled")
    # 调用方上下文是别的（active）租户，仍被归属租户闸拦下
    assert le.resume_local_execution(
        store, eid, "event", {}, tenant_id="default"
    ) is False


def test_resume_local_execution_allowed_after_reenable(store):
    eid = _suspended_execution(store)
    tenants_svc.set_tenant_status(store, "acme", "disabled")
    assert le.resume_local_execution(store, eid, "event", {}, tenant_id="acme") is False
    tenants_svc.set_tenant_status(store, "acme", "active")
    assert le.resume_local_execution(store, eid, "event", {}, tenant_id="acme") is True
