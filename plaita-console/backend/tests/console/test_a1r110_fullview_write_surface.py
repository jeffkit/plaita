"""A1 r110 a/P2：平台管理员「全量视角」（switch-tenant null）不得按 id 盲写他租户资源。

**裁定**（B2 r69）：``docs-site/docs/guide/console.md`` 与 ``auth.py``（A2R17-01）
明示「会话型平台管理员的全量视角是**只读**的（403）」，写路径由
``tenant_scope(request, required=True)`` 把关；仓内**没有**「平台管理员=全权」的
授权口径。因此按**最小权限收口**：平台管理员跨租户写必须显式声明租户
（``X-Tenant-ID`` 或 switch-tenant 到具体租户），全量视角下的盲改一律
``FULL_VIEW_WRITE_REQUIRES_TENANT_DETAIL`` 具名 403，且在任何查找之前拒绝
（不做存在性 oracle）。

缺口（基线 ``74616b8``，A1 r110 实弹 ``/tmp/r110_fullview_schedule_probe.py``）：
各资源的 **create** 端点已是 ``required=True``，但 by-id **写**端点仍用非 required
的 ``tenant_scope(request)``——全量视角解析出 ``tenant=None``，service 层
``tenant_id=None`` 即「不过滤」，于是 root 全量视角能按 id 改/删/触发任意租户
的资源，审计还把租户记成空串。涉及 12 个端点：

- schedules ×5：PUT / DELETE / enable / disable / trigger
- nodes DELETE · property-types DELETE · credentials DELETE
- executions ×3：cancel / DELETE / resume（集群档经 ``_find_execution`` 全量 SCAN）
- events subscriptions DELETE（``_require_owned_subscription`` 对 None 直接放行）

本文件挂载与生产一致（``require_auth + tenant_generation_context``），集群档
（FakeRedis）+ 真 SQLite store，资源由 tenant-b 的 admin 经 API 创建：

1. 全量视角 root 对 12 个端点逐个 403（detail 精确）、资源零变化、队列零消息；
2. root 显式 ``X-Tenant-ID: tenant-b`` ⇒ 写成功，审计 ``tenant_id == tenant-b``
   （生效租户入审计）；
3. 无状态平台 API-Key 不带租户头 ⇒ 沿写路径契约落 default，他租户资源 404
   （不被盲改，与 create 端点同口径）；
4. 全量视角**读**面不回归（GET 仍 200）。

负控：仅把这些端点的 ``required=True`` 还原 ⇒ 第 1 组精确红（复现 200 + 资源
被改）、第 3 组红（无头 API-Key 同样 ``tenant=None`` 不过滤 ⇒ 盲写 200），
第 2、4 组保持绿。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from api import credentials as credentials_api  # noqa: E402
from api import events as events_api  # noqa: E402
from api import executions as executions_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from api import nodes as nodes_api  # noqa: E402
from api import property_types as property_types_api  # noqa: E402
from api import schedules as schedules_api  # noqa: E402
from auth import (  # noqa: E402
    FULL_VIEW_WRITE_REQUIRES_TENANT_DETAIL,
    require_auth,
    tenant_generation_context,
)
from services import audit as audit_svc  # noqa: E402
from services import credentials_svc, flow_store, node_registry_svc, tenants_svc, users_svc  # noqa: E402

ADMIN_KEY = "a1r110-admin-key"
ROOT_PW = "root-password-1"
ADMIN_B_PW = "admin-b-password-1"
TENANT_B = "tenant-b"
FLOW_B = "flow-b"
NODE_B = "custom-node-b"
PTYPE_B = "ptype-b"
CRED_B = "cred-b"
SUB_B = "sub-b"
EXEC_RUNNING = "exec-running-b"
EXEC_DONE = "exec-done-b"
EXEC_SUSPENDED = "exec-suspended-b"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


def _echo_def(flow_id: str) -> str:
    return json.dumps({
        "flow_id": flow_id, "desc": "echo", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "output": "$INPUT.q", "resultType": "success"},
        ],
    })


def _seed_execution(redis: FakeRedis, execution_id: str, status: str) -> None:
    ns = f"plaita:{TENANT_B}"
    redis.set(f"{ns}:execution:{execution_id}", json.dumps({
        "execution_id": execution_id, "flow_id": FLOW_B, "flow_version": "1.0.0",
        "status": status, "tenant_id": TENANT_B, "start_time": "2026-10-22T00:00:00",
        "context": {},
    }))
    redis.zadd(f"{ns}:execution:index", {execution_id: 1.0})
    redis.set(f"{ns}:execution:fence:{execution_id}", "1")
    redis.set(f"{ns}:execution:lease:{execution_id}", "worker:1")


class _Env:
    def __init__(self, client: TestClient, redis: FakeRedis, store, schedule_id: str):
        self.client = client
        self.redis = redis
        self.store = store
        self.schedule_id = schedule_id
        self.queue = executions_api._task_queue_name()

    def login(self, username: str, password: str) -> dict:
        r = self.client.post("/api/auth/login", json={"username": username, "password": password})
        assert r.status_code == 200, (username, r.status_code, r.text)
        return {"Authorization": f"Bearer {r.json()['token']}"}

    def full_view_root(self) -> dict:
        root = self.login("root", ROOT_PW)
        r = self.client.post("/api/auth/switch-tenant", json={"tenant_id": None}, headers=root)
        assert r.status_code == 200, r.text
        me = self.client.get("/api/auth/me", headers=root).json()
        assert me.get("tenant_id") is None and me.get("platform_admin"), me
        return root

    @property
    def key(self) -> dict:
        return {"X-Admin-API-Key": ADMIN_KEY}

    # ---- 资源快照 ----
    def schedule(self) -> dict:
        raw = self.redis.hget(schedules_api.SCHEDULES_KEY, self.schedule_id)
        return json.loads(raw) if raw else None

    def node_types(self) -> set:
        return {d.node_type for d in node_registry_svc.list_descriptors(self.store, tenant_id=TENANT_B)}

    def ptype_names(self) -> set:
        return {p.name for p in node_registry_svc.list_property_types(self.store, tenant_id=TENANT_B)}

    def cred_names(self) -> set:
        return {c["name"] for c in credentials_svc.list_credentials(tenant_id=TENANT_B)}

    def exec_status(self, execution_id: str):
        raw = self.redis.get(f"plaita:{TENANT_B}:execution:{execution_id}")
        return json.loads(raw)["status"] if raw else None

    def cancel_flag(self, execution_id: str):
        return self.redis.get(f"plaita:{TENANT_B}:execution:cancel:{execution_id}")

    def sub_exists(self) -> bool:
        return self.redis.exists(f"plaita:subscription:data:{SUB_B}") == 1

    def queue_len(self) -> int:
        return self.redis.xlen(self.queue) if self.redis.exists(self.queue) else 0


@pytest.fixture()
def env(tmp_path, monkeypatch) -> _Env:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/a1r110.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", _settings)
    monkeypatch.setattr("auth.get_settings", _settings)

    flow_store.init_engine(f"sqlite:///{tmp_path}/a1r110.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_B, "Tenant B")
    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    users_svc.create_user(store, "admin-b", ADMIN_B_PW, "admin")
    users_svc.add_member(store, TENANT_B, "admin-b", "admin")

    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.store = store
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(auth_users.router, prefix="/api")
    admin_deps = [Depends(require_auth), Depends(tenant_generation_context)]
    for module in (flows_api, schedules_api, nodes_api, property_types_api,
                   credentials_api, executions_api, events_api):
        app.include_router(module.router, prefix="/api", dependencies=admin_deps)
    client = TestClient(app, raise_server_exceptions=False)

    # tenant-b 的 admin 经 API 创建本租户资源（真写路径，含 #239 写守卫）
    r = client.post("/api/auth/login", json={"username": "admin-b", "password": ADMIN_B_PW})
    admin_b = {"Authorization": f"Bearer {r.json()['token']}"}
    r = client.post("/api/flows", json={"flow_id": FLOW_B, "desc": "b"}, headers=admin_b)
    assert r.status_code == 200, r.text
    r = client.put(f"/api/flows/{FLOW_B}/versions/1.0.0",
                   json={"definition": _echo_def(FLOW_B)}, headers=admin_b)
    assert r.status_code == 200, r.text
    r = client.post(f"/api/flows/{FLOW_B}/publish", json={"version": "1.0.0"}, headers=admin_b)
    assert r.status_code == 200, r.text
    r = client.post("/api/schedules", json={
        "name": "original-name", "flow_id": FLOW_B, "cron": "0 * * * *", "enabled": True,
    }, headers=admin_b)
    assert r.status_code == 200, r.text
    schedule_id = r.json()["schedule_id"]
    r = client.post("/api/nodes", json={
        "node_type": NODE_B, "node_name": "B", "category": "custom", "schema_json": "{}",
    }, headers=admin_b)
    assert r.status_code == 200, r.text
    r = client.post("/api/property-types", json={"name": PTYPE_B, "base_type": "string"},
                    headers=admin_b)
    assert r.status_code == 200, r.text
    r = client.post("/api/credentials", json={
        "name": CRED_B, "type": "generic", "data": {"token": "b-secret"},
    }, headers=admin_b)
    assert r.status_code == 200, r.text

    for execution_id, status in ((EXEC_RUNNING, "running"), (EXEC_DONE, "completed"),
                                 (EXEC_SUSPENDED, "suspended")):
        _seed_execution(redis, execution_id, status)
    redis.set(f"plaita:subscription:data:{SUB_B}", json.dumps({
        "subscription_id": SUB_B, "event_type": "approval", "correlation_id": EXEC_SUSPENDED,
        "flow_id": FLOW_B, "created_at": 1.0, "tenant_id": TENANT_B,
    }))
    return _Env(client, redis, store, schedule_id)


def _write_calls(env: _Env):
    """12 个 by-id 写端点：(名字, 发请求的函数, 资源未变断言)。"""
    sid = env.schedule_id
    c = env.client
    return [
        ("schedules.update",
         lambda h: c.put(f"/api/schedules/{sid}", json={"name": "mutated-by-full-view"}, headers=h),
         lambda: env.schedule()["name"] == "original-name"),
        ("schedules.disable",
         lambda h: c.post(f"/api/schedules/{sid}/disable", headers=h),
         lambda: env.schedule()["enabled"] is True),
        ("schedules.enable",
         lambda h: c.post(f"/api/schedules/{sid}/enable", headers=h),
         lambda: env.schedule()["enabled"] is True),
        ("schedules.trigger",
         lambda h: c.post(f"/api/schedules/{sid}/trigger", headers=h),
         lambda: env.queue_len() == 0),
        ("schedules.delete",
         lambda h: c.delete(f"/api/schedules/{sid}", headers=h),
         lambda: env.schedule() is not None),
        ("nodes.delete",
         lambda h: c.delete(f"/api/nodes/{NODE_B}", headers=h),
         lambda: NODE_B in env.node_types()),
        ("property_types.delete",
         lambda h: c.delete(f"/api/property-types/{PTYPE_B}", headers=h),
         lambda: PTYPE_B in env.ptype_names()),
        ("credentials.delete",
         lambda h: c.delete(f"/api/credentials/{CRED_B}", headers=h),
         lambda: CRED_B in env.cred_names()),
        ("executions.cancel",
         lambda h: c.post(f"/api/executions/{EXEC_RUNNING}/cancel", headers=h),
         lambda: env.exec_status(EXEC_RUNNING) == "running"
         and env.cancel_flag(EXEC_RUNNING) is None and env.queue_len() == 0),
        ("executions.delete",
         lambda h: c.delete(f"/api/executions/{EXEC_DONE}", headers=h),
         lambda: env.exec_status(EXEC_DONE) == "completed"),
        ("executions.resume",
         lambda h: c.post(f"/api/executions/{EXEC_SUSPENDED}/resume",
                          json={"resume_type": "continue"}, headers=h),
         lambda: env.exec_status(EXEC_SUSPENDED) == "suspended" and env.queue_len() == 0),
        ("events.subscription_delete",
         lambda h: c.delete(f"/api/events/subscriptions/{SUB_B}", headers=h),
         lambda: env.sub_exists()),
    ]


# ---------------------------------------------------------------- 1. 全量视角盲写 ⇒ 具名 403、零变化


def test_full_view_blind_writes_are_403_and_touch_nothing(env):
    root = env.full_view_root()
    calls = _write_calls(env)
    assert [n for n, _, _ in calls] == _NAMES, "端点清单与逐端点用例失配"
    failures = []
    for name, call, untouched in calls:
        r = call(root)
        detail = None
        try:
            detail = r.json().get("detail")
        except Exception:  # noqa: BLE001
            pass
        if r.status_code != 403 or detail != FULL_VIEW_WRITE_REQUIRES_TENANT_DETAIL:
            failures.append((name, r.status_code, r.text[:160]))
        if not untouched():
            failures.append((name, "resource mutated"))
    assert failures == [], failures


_NAMES = [
    "schedules.update", "schedules.disable", "schedules.enable", "schedules.trigger",
    "schedules.delete", "nodes.delete", "property_types.delete", "credentials.delete",
    "executions.cancel", "executions.delete", "executions.resume", "events.subscription_delete",
]


@pytest.mark.parametrize("name", _NAMES)
def test_each_full_view_write_is_named_403(env, name):
    """逐端点版本（定位用）：同一断言拆成 12 个用例，哪一个红一眼可见。"""
    root = env.full_view_root()
    call, untouched = next((c, u) for n, c, u in _write_calls(env) if n == name)
    r = call(root)
    assert r.status_code == 403, (name, r.status_code, r.text)
    assert r.json()["detail"] == FULL_VIEW_WRITE_REQUIRES_TENANT_DETAIL, (name, r.text)
    assert untouched(), f"{name}: 全量视角写已落地"


def test_full_view_write_rejection_does_not_leak_existence(env):
    """403 在任何查找之前发生：对不存在的 id 也是同一 403（无存在性 oracle）。"""
    root = env.full_view_root()
    for path in (
        "/api/schedules/no-such-schedule",
        "/api/executions/no-such-exec/cancel",
        "/api/events/subscriptions/no-such-sub",
    ):
        method = "delete" if "subscriptions" in path or path.startswith("/api/schedules") else "post"
        r = getattr(env.client, method)(path, headers=root)
        assert r.status_code == 403, (path, r.status_code, r.text)
        assert r.json()["detail"] == FULL_VIEW_WRITE_REQUIRES_TENANT_DETAIL


# ---------------------------------------------------------------- 2. 显式声明租户 ⇒ 可写，审计记生效租户


def test_platform_admin_with_explicit_tenant_header_can_write_and_audit_records_tenant(env):
    root = env.full_view_root()
    declared = {**root, "X-Tenant-ID": TENANT_B}
    sid = env.schedule_id
    c = env.client

    r = c.put(f"/api/schedules/{sid}", json={"name": "renamed-by-root"}, headers=declared)
    assert r.status_code == 200, r.text
    assert env.schedule()["name"] == "renamed-by-root"
    r = c.post(f"/api/schedules/{sid}/disable", headers=declared)
    assert r.status_code == 200, r.text
    assert env.schedule()["enabled"] is False
    r = c.post(f"/api/schedules/{sid}/enable", headers=declared)
    assert r.status_code == 200, r.text
    r = c.post(f"/api/schedules/{sid}/trigger", headers=declared)
    assert r.status_code == 200, r.text
    assert env.queue_len() == 1
    r = c.post(f"/api/executions/{EXEC_RUNNING}/cancel", headers=declared)
    assert r.status_code == 200, r.text
    assert env.cancel_flag(EXEC_RUNNING) is not None
    r = c.post(f"/api/executions/{EXEC_SUSPENDED}/resume",
               json={"resume_type": "continue"}, headers=declared)
    assert r.status_code == 200, r.text
    r = c.delete(f"/api/executions/{EXEC_DONE}", headers=declared)
    assert r.status_code == 200, r.text
    assert env.exec_status(EXEC_DONE) is None
    r = c.delete(f"/api/events/subscriptions/{SUB_B}", headers=declared)
    assert r.status_code == 200, r.text
    assert not env.sub_exists()
    r = c.delete(f"/api/nodes/{NODE_B}", headers=declared)
    assert r.status_code == 200, r.text
    r = c.delete(f"/api/property-types/{PTYPE_B}", headers=declared)
    assert r.status_code == 200, r.text
    r = c.delete(f"/api/credentials/{CRED_B}", headers=declared)
    assert r.status_code == 200, r.text
    r = c.delete(f"/api/schedules/{sid}", headers=declared)
    assert r.status_code == 200, r.text
    assert env.schedule() is None

    # 审计：每类写操作都记**生效租户** tenant-b（不再是空串 / None）
    for action in ("schedule.update", "schedule.disable", "schedule.enable", "schedule.trigger",
                   "schedule.delete", "execution.cancel", "execution.resume", "execution.delete",
                   "event.subscription_delete", "node.delete", "property_type.delete",
                   "credential.delete"):
        rows = audit_svc.list_audit(action=action, limit=50)
        assert rows, f"缺少审计行: {action}"
        assert all(row["tenant_id"] == TENANT_B for row in rows if row["actor"] == "root"), \
            (action, [(row["actor"], row["tenant_id"]) for row in rows])


# ---------------------------------------------------------------- 3. 无状态平台 key 不带租户头 ⇒ default 作用域，他租户 404


def test_stateless_platform_key_without_header_scopes_to_default_and_cannot_blind_write(env):
    failures = []
    for name, call, untouched in _write_calls(env):
        r = call(env.key)
        if r.status_code != 404:
            failures.append((name, r.status_code, r.text[:160]))
        if not untouched():
            failures.append((name, "resource mutated"))
    assert failures == [], failures


# ---------------------------------------------------------------- 4. 全量视角读面不回归


def test_full_view_reads_still_work(env):
    root = env.full_view_root()
    r = env.client.get(f"/api/schedules/{env.schedule_id}", headers=root)
    assert r.status_code == 200, r.text
    assert r.json()["name"] == "original-name"
    r = env.client.get(f"/api/executions/{EXEC_RUNNING}", headers=root)
    assert r.status_code == 200, r.text
    r = env.client.get(f"/api/events/subscriptions/{SUB_B}", headers=root)
    assert r.status_code == 200, r.text
