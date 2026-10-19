"""A51-02 / A52-S2：请求级租户世代绑定（round 52 最小改造清单）。

r38 的世代令牌在写路径**首次读租户**时才捕获：请求已在认证时以旧租户成员
身份通过，若 delete+同名 create 在它到达服务首次读之前完成，首次读看到的
是新租户，捕获新令牌，复核自然通过——旧租户 admin 的在途 ``add_member``
把同伙写成重建租户的 admin，``save_credential`` 植入凭据，flow/schedule/
local execution 写路径更是完全没有复核。

本文件钉住：
1. ``resolve_session`` 单条 JOIN 返回认证快照里的 ``tenant_generation``；
2. 请求绑定旧世代 → delete+create 后，add_member/save_credential/flow save/
   schedule/local execution 全部失败、重建租户零残留；
3. 合法路径（绑定的世代与当前一致）不回归；
4. HTTP 挂载：async 依赖把世代放进 ContextVar，sync handler（threadpool）
   仍能读到（在途请求跨 ABA 必须 409、不落行）。

pre-fix 预期：1 无法取字段；2 全部成功（r52 探针实测 add_member/save ok）；
3/4 通过。
"""
from __future__ import annotations

import json
import sys
import threading
from pathlib import Path

import pytest
from sqlalchemy import select

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from fastapi import Depends, FastAPI  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

from models.flow import Credential, Tenant, TenantMember  # noqa: E402
from services import credentials_svc, flow_store, local_scheduler, tenants_svc, users_svc  # noqa: E402
from services.tenant_write_guard import (  # noqa: E402
    TenantGenerationMismatch,
    bind_tenant_generation,
    reset_tenant_generation,
)

TENANT = "acme"
TADMIN_PW = "tadmin-password-1"
BOB_PW = "bob-password-1"


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "r52-bind.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT, "Acme")
    users_svc.create_user(store, "tadmin", TADMIN_PW, "admin")
    users_svc.create_user(store, "bob", BOB_PW, "viewer")
    users_svc.add_member(store, TENANT, "tadmin", "admin")
    return {"store": store, "tmp": tmp_path}


def _login(store) -> str:
    session = users_svc.login(store, "tadmin", TADMIN_PW)
    assert session is not None
    return session["token"]


def _generation(store):
    token = _login(store)
    resolved = users_svc.resolve_session(store, token)
    assert resolved is not None
    return resolved["tenant_generation"]


def _old_request_binding(store):
    """模拟「认证完成、handler 尚未执行」的旧租户请求。"""
    generation = _generation(store)
    assert generation is not None
    return bind_tenant_generation(TENANT, generation)


def _delete_recreate(store):
    assert tenants_svc.delete_tenant(store, TENANT) is True
    tenants_svc.create_tenant(store, TENANT, "Acme recreated")


def _members(store):
    with store._session_local() as session:
        return session.scalars(
            select(TenantMember).where(TenantMember.tenant_id == TENANT)
        ).all()


def _credentials(store):
    with store._session_local() as session:
        return session.scalars(
            select(Credential).where(Credential.tenant_id == TENANT)
        ).all()


def test_resolve_session_returns_single_snapshot_generation(env):
    store = env["store"]
    old_token = _login(store)
    generation = users_svc.resolve_session(store, old_token)["tenant_generation"]
    with store._session_local() as session:
        row = session.get(Tenant, TENANT)
        assert generation == (row.created_at, row.contract_secret_id)

    # 删除 + 同名重建：旧会话解析后不再拥有活跃租户/角色（#254）。
    _delete_recreate(store)
    resolved = users_svc.resolve_session(store, old_token)
    assert resolved is not None
    assert resolved["tenant_id"] is None
    assert resolved["role"] is None
    assert resolved["tenant_generation"] is None


def test_inflight_add_member_after_recreate_is_rejected(env):
    store = env["store"]
    binding = _old_request_binding(store)
    try:
        _delete_recreate(store)
        with pytest.raises((users_svc.UserError, TenantGenerationMismatch)):
            users_svc.add_member(store, TENANT, "bob", "admin")
    finally:
        reset_tenant_generation(binding)

    assert _members(store) == []
    session = users_svc.login(store, "bob", BOB_PW)
    assert session is not None and session["role"] is None


def test_inflight_save_credential_after_recreate_is_rejected(env):
    store = env["store"]
    binding = _old_request_binding(store)
    try:
        _delete_recreate(store)
        with pytest.raises(ValueError):
            credentials_svc.save_credential(
                "planted-webhook",
                "generic",
                {"url": "https://attacker.example/hook"},
                tenant_id=TENANT,
            )
    finally:
        reset_tenant_generation(binding)

    assert _credentials(store) == []
    assert not credentials_svc.credentials_file(TENANT).exists()


def test_inflight_flow_save_after_recreate_is_rejected(env):
    store = env["store"]
    binding = _old_request_binding(store)
    try:
        _delete_recreate(store)
        with pytest.raises(TenantGenerationMismatch):
            store.save_flow_definition(
                "f-late",
                "0.0.1",
                json.dumps(
                    {
                        "flow_id": "f-late",
                        "nodes": [
                            {"id": "start", "type": "start", "next": "end"},
                            {"id": "end", "type": "end"},
                        ],
                    }
                ),
                tenant_id=TENANT,
            )
    finally:
        reset_tenant_generation(binding)

    assert store.get_flow_record("f-late", tenant_id=TENANT) is None
    assert store.list_versions("f-late", tenant_id=TENANT) == []


def test_inflight_schedule_and_local_execution_after_recreate_are_rejected(env):
    store = env["store"]
    binding = _old_request_binding(store)
    try:
        _delete_recreate(store)
        with pytest.raises(TenantGenerationMismatch):
            local_scheduler.create_schedule(
                store,
                {
                    "schedule_id": "sched-late",
                    "tenant_id": TENANT,
                    "name": "late",
                    "flow_id": "f1",
                    "cron": "* * * * *",
                },
            )
        with pytest.raises(TenantGenerationMismatch):
            flow_store.insert_local_execution(
                execution_id="exec-late",
                flow_id="f1",
                flow_version="0.0.1",
                tenant_id=TENANT,
            )
    finally:
        reset_tenant_generation(binding)

    assert local_scheduler.list_schedules(store, tenant_id=TENANT) == []


def test_legal_request_with_current_generation_still_writes(env):
    store = env["store"]
    binding = bind_tenant_generation(TENANT, _generation(store))
    try:
        users_svc.add_member(store, TENANT, "bob", "viewer")
        credentials_svc.save_credential(
            "legal-webhook", "generic", {"url": "https://example.test/hook"},
            tenant_id=TENANT,
        )
        store.save_flow_definition(
            "f-legal",
            "0.0.1",
            json.dumps(
                {
                    "flow_id": "f-legal",
                    "nodes": [
                        {"id": "start", "type": "start", "next": "end"},
                        {"id": "end", "type": "end"},
                    ],
                }
            ),
            tenant_id=TENANT,
        )
        local_scheduler.create_schedule(
            store,
            {
                "schedule_id": "sched-legal",
                "tenant_id": TENANT,
                "name": "legal",
                "flow_id": "f-legal",
                "cron": "* * * * *",
            },
        )
    finally:
        reset_tenant_generation(binding)

    assert [m.username for m in _members(store)] == ["tadmin", "bob"]
    assert [c.name for c in _credentials(store)] == ["legal-webhook"]
    assert store.get_flow_record("f-legal", tenant_id=TENANT) is not None
    assert [s["schedule_id"] for s in local_scheduler.list_schedules(store, tenant_id=TENANT)] == [
        "sched-legal"
    ]


def test_http_mount_binds_generation_for_sync_handler(env, monkeypatch):
    """生产挂载形态：Depends(require_auth)+Depends(tenant_generation_context)。

    在途请求在 handler 内暂停，主线程 delete+create；请求的 ContextVar 仍
    是认证时的旧世代 → 写入被拒（flows 路由把 ValueError 转 409），重建
    租户零流程。
    """
    import auth as auth_mod
    from api import flows as flows_api

    store = env["store"]
    app = FastAPI()
    app.state.store = store
    app.state.local_mode = True
    app.state.redis = None
    app.include_router(
        flows_api.router,
        prefix="/api",
        dependencies=[
            Depends(auth_mod.require_auth),
            Depends(auth_mod.tenant_generation_context),
        ],
    )
    client = TestClient(app)
    headers = {"Authorization": f"Bearer {_login(store)}"}

    entered, release = threading.Event(), threading.Event()
    original = flow_store.FlowStore.create_flow

    def paused_create_flow(self, *args, **kwargs):
        entered.set()
        assert release.wait(10.0), "probe deadlock: release never signalled"
        return original(self, *args, **kwargs)

    monkeypatch.setattr(flow_store.FlowStore, "create_flow", paused_create_flow)
    outcome = {}

    def request():
        outcome["response"] = client.post(
            "/api/flows", json={"flow_id": "f-inflight"}, headers=headers
        )

    worker = threading.Thread(target=request)
    worker.start()
    assert entered.wait(5.0), "request never reached the flow write"
    _delete_recreate(store)
    release.set()
    worker.join(10.0)
    assert not worker.is_alive()

    response = outcome["response"]
    assert response.status_code == 409, response.text
    assert store.get_flow_record("f-inflight", tenant_id=TENANT) is None
