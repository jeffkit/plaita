"""A54-03/04/05（duo-a 第五十四轮）：DB 资产跨世代可见。

基线 ``5ccaf6a`` 的 ``delete_tenant`` 不清理 ``node_descriptors``、
``property_types``、``copilot_threads``、``deployments``；``audit_logs``
按合规要求保留但读面只按 ``tenant_id`` 过滤。同名重建后：
- viewer 经 ``/api/nodes``、``/api/property-types`` 读前代自定义节点 schema；
- viewer 经 ``/api/copilot/threads`` 读前代会话标题/消息数；
- viewer 经 ``/api/deployments`` 读前代发布历史；
- 租户 admin 经 ``/api/audit`` 读前代审计 detail。

本文件钉住：
1. 四类租户资产随删除事务清理（他租户行保留）；
2. 审计**不物理删除**（平台合规证据），但租户 admin 读面按当前世代
   （``tenants.created_at``）收窄：旧世代不可见、新世代可见；
3. 平台身份仍可读全量审计（跨世代保留）。

负控（验收时手工执行）：
- 去掉 ``_purge_tenant_db_assets`` 调用 ⇒ 第 1 组四类资产断言精确红；
- 去掉 ``api/audit.py`` 的 ``since`` 世代过滤 ⇒ 旧审计在新租户下可见，精确红。
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from sqlalchemy import select
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import audit as audit_api  # noqa: E402
from api import auth_users  # noqa: E402
from api import copilot as copilot_api  # noqa: E402
from api import nodes as nodes_api  # noqa: E402
from api import property_types as property_types_api  # noqa: E402
from auth import require_auth  # noqa: E402
from models.flow import (  # noqa: E402
    AuditLog,
    CopilotThread,
    Deployment,
    NodeDescriptor,
    PropertyType,
)
from services import flow_store, tenants_svc, users_svc  # noqa: E402

TENANT = "acme"
OTHER = "beta"
VIEWER_PW = "viewer-password-1"
TADMIN_PW = "tadmin-password-1"
ROOT_PW = "root-password-1"
FLOW_ID = "flow-old"
ADMIN_KEY = "a54-assets-admin-key"


class Env:
    def __init__(self, client: TestClient, redis: FakeRedis, store):
        self.client = client
        self.redis = redis
        self.store = store


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


def _seed_assets(store, tenant_id: str, suffix: str) -> None:
    with store._session_local() as session:
        session.add(NodeDescriptor(
            tenant_id=tenant_id,
            node_type=f"old_custom_{suffix}",
            node_name=f"Old {suffix}",
            category="custom",
            schema_json='{"secret": "OLD-NODE-SCHEMA"}',
            is_builtin=False,
        ))
        session.add(PropertyType(
            tenant_id=tenant_id,
            name=f"old_money_{suffix}",
            base_type="string",
            enum_json="[]",
            default_json='"OLD-PROP-DESC"',
            desc="OLD-PROP-DESC",
        ))
        session.add(CopilotThread(
            tenant_id=tenant_id,
            thread_id=f"thread-{suffix}",
            flow_id=FLOW_ID,
            version="1.0.0",
            title="OLD-COPILOT-TITLE",
            message_count=3,
        ))
        session.add(Deployment(
            tenant_id=tenant_id,
            flow_id=FLOW_ID,
            version="1.0.0",
            environment="prod",
            actor=f"actor-{suffix}",
            definition_hash="deadbeef",
        ))
        session.add(AuditLog(
            ts=datetime.utcnow(),
            tenant_id=tenant_id,
            actor="old-actor",
            action=f"old.action.{suffix}",
            resource="credential",
            resource_id="old-cred",
            detail_json='{"OLD-AUDIT-DETAIL": true}',
            ip="127.0.0.1",
        ))
        session.commit()


def _build_env(tmp_path, monkeypatch) -> Env:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/a54-assets.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())

    flow_store.init_engine(f"sqlite:///{tmp_path}/a54-assets.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT, "Acme")
    tenants_svc.create_tenant(store, OTHER, "Beta")
    users_svc.create_user(store, "viewer-a", VIEWER_PW, "viewer")
    users_svc.add_member(store, TENANT, "viewer-a", "viewer")
    users_svc.create_user(store, "tadmin", TADMIN_PW, "admin")
    users_svc.add_member(store, TENANT, "tadmin", "admin")
    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    _seed_assets(store, TENANT, "acme")
    _seed_assets(store, OTHER, "beta")

    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.store = store
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(auth_users.router, prefix="/api")
    for router in (
        nodes_api.router,
        property_types_api.router,
        copilot_api.router,
        audit_api.router,
    ):
        app.include_router(router, prefix="/api", dependencies=[Depends(require_auth)])
    return Env(TestClient(app), redis, store)


@pytest.fixture()
def env(tmp_path, monkeypatch) -> Env:
    return _build_env(tmp_path, monkeypatch)


def _login(client: TestClient, username: str, password: str) -> dict:
    r = client.post(
        "/api/auth/login", json={"username": username, "password": password}
    )
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _delete_and_recreate(env: Env) -> None:
    assert tenants_svc.delete_tenant(
        env.store, TENANT, redis_client=env.redis
    ) is True
    tenants_svc.create_tenant(
        env.store, TENANT, "Acme again", redis_client=env.redis
    )
    users_svc.add_member(env.store, TENANT, "viewer-a", "viewer")
    users_svc.add_member(env.store, TENANT, "tadmin", "admin")


def _asset_names(env: Env) -> dict:
    with env.store._session_local() as session:
        return {
            "nodes": {
                r.node_type
                for r in session.scalars(select(NodeDescriptor)).all()
            },
            "props": {
                r.name for r in session.scalars(select(PropertyType)).all()
            },
            "threads": {
                r.thread_id for r in session.scalars(select(CopilotThread)).all()
            },
            "deployments": {
                r.flow_id for r in session.scalars(select(Deployment)).all()
            },
            "audit_actions": {
                r.action for r in session.scalars(select(AuditLog)).all()
            },
        }


def test_pre_delete_assets_are_visible(env: Env):
    headers = _login(env.client, "viewer-a", VIEWER_PW)
    assert "old_custom_acme" in env.client.get("/api/nodes", headers=headers).text
    assert "old_money_acme" in env.client.get(
        "/api/property-types", headers=headers
    ).text
    assert "thread-acme" in env.client.get(
        f"/api/copilot/threads?flow_id={FLOW_ID}", headers=headers
    ).text
    assert "flow-old" in env.client.get("/api/deployments", headers=headers).text

    admin = _login(env.client, "tadmin", TADMIN_PW)
    assert "old.action.acme" in env.client.get("/api/audit", headers=admin).text


def test_db_assets_do_not_survive_tenant_recreation(env: Env):
    _delete_and_recreate(env)
    headers = _login(env.client, "viewer-a", VIEWER_PW)

    nodes = env.client.get("/api/nodes", headers=headers)
    assert nodes.status_code == 200
    assert "old_custom_acme" not in nodes.text

    props = env.client.get("/api/property-types", headers=headers)
    assert props.status_code == 200
    assert props.json()["types"] == []

    threads = env.client.get(
        f"/api/copilot/threads?flow_id={FLOW_ID}", headers=headers
    )
    assert threads.status_code == 200
    assert threads.json()["threads"] == []

    deployments = env.client.get("/api/deployments", headers=headers)
    assert deployments.status_code == 200
    assert deployments.json()["deployments"] == []


def test_other_tenant_assets_are_preserved(env: Env):
    _delete_and_recreate(env)
    names = _asset_names(env)
    assert "old_custom_beta" in names["nodes"]
    assert "old_money_beta" in names["props"]
    assert "thread-beta" in names["threads"]
    assert "flow-old" in names["deployments"]


def test_audit_is_retained_but_read_surface_is_generation_scoped(env: Env):
    old_created = tenants_svc.tenant_generation(env.store, TENANT)
    _delete_and_recreate(env)
    new_created = tenants_svc.tenant_generation(env.store, TENANT)
    assert new_created and new_created != old_created

    # 新世代写入一条审计：租户 admin 必须能看到它（收窄不是清空）。
    with env.store._session_local() as session:
        session.add(AuditLog(
            ts=datetime.utcnow(),
            tenant_id=TENANT,
            actor="new-actor",
            action="new.action.acme",
            resource="flow",
            resource_id="new-flow",
            detail_json="{}",
            ip="127.0.0.1",
        ))
        session.commit()

    admin = _login(env.client, "tadmin", TADMIN_PW)
    visible = env.client.get("/api/audit", headers=admin).text
    assert "old.action.acme" not in visible
    assert "new.action.acme" in visible

    # 合规保留：物理行仍在；平台身份可全量读（跨世代）。
    assert "old.action.acme" in _asset_names(env)["audit_actions"]
    platform_view = env.client.get(
        "/api/audit", headers={"X-Admin-API-Key": ADMIN_KEY}
    ).text
    assert "old.action.acme" in platform_view
    assert "old.action.beta" in platform_view
