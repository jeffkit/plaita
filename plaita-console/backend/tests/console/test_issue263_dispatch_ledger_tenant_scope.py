"""plaita#263（P1 security 同族读面）：``/api/dispatch/ledger`` 的租户隔离。

A54-02 取代说明：本文件最初按 r40 的「租户可显式按 tenant_id 收窄 / 跨租户
403 审计 / 租户视图返回队列对账」写；r41 A54-02 把端点改为**世代过滤**契约
（dispatch-ledger 三文件冲突取 main 侧，见 ``b18cb94``）。本文件随之改钉
现行契约，安全不变量不变：

1. 非平台身份只读**当前世代 + 本租户**的台账行；无 ``tenant_generation`` 的
   历史行与无 ``tenant_id`` 的 legacy 行一律不可见（fail-closed）；
2. 跨租户查询参数不能扩权：viewer 传 ``tenant_id=tenant-b`` 仍只拿到本租户
   当前世代行（端点已不提供按租户收窄入口，未知参数不改变作用域）；
3. 租户视图的队列对账显式不可用（全局计数不泄露给租户，不伪造成 0/相等）；
4. 平台管理员 / 平台 API-Key 全量可见（审计面）。

负控（必红）：删除端点里的租户/世代过滤（直接 ``ledger_rows(redis)``）⇒
``test_viewer_only_sees_own_tenant`` 读到 b-*/legacy 行而精确红。
"""
from __future__ import annotations

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
from api import capacity as capacity_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

from plaita.server.dispatch_ledger import (  # noqa: E402
    build_ledger_entry,
    record_dispatch,
)
from plaita.server.task_queue import enqueue_task  # noqa: E402

ROOT_PW = "root-password-1"
VIEWER_PW = "viewer-password-1"
TENANT_A = "tenant-a"
TENANT_B = "tenant-b"
ADMIN_KEY = "issue263-ledger-key"
QUEUE = "plaita:flow:queue"
TENANT_SCOPED_PARITY_DETAIL = "租户视图：队列对账仅平台身份可用"


class Env:
    def __init__(self, client, redis, store):
        self.client = client
        self.redis = redis
        self.store = store


def _settings(admin_key: str = ADMIN_KEY):
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = admin_key
    s.allow_insecure_admin = False
    return s


def _seed(redis, store):
    gen_a = tenants_svc.tenant_generation(store, TENANT_A)
    gen_b = tenants_svc.tenant_generation(store, TENANT_B)
    assert gen_a and gen_b

    # 台账：tenant-a 当前世代 ×2、tenant-b 当前世代 ×3、无世代的历史行 ×1、
    # 无租户的 legacy 行 ×1。后两类对租户视图必须 fail-closed。
    entries = [
        ("a-1", TENANT_A, gen_a),
        ("a-2", TENANT_A, gen_a),
        ("b-1", TENANT_B, gen_b),
        ("b-2", TENANT_B, gen_b),
        ("b-3", TENANT_B, gen_b),
        ("a-legacy", TENANT_A, None),
        ("legacy", None, None),
    ]
    for issue, tenant, generation in entries:
        record_dispatch(
            redis,
            build_ledger_entry(
                issue_id=issue,
                queue=QUEUE,
                target_instance_id=None,
                reason="route:none",
                tenant_id=tenant,
                tenant_generation=generation,
                actor=f"{tenant}-editor" if tenant else "legacy",
            ),
        )

    # 队列与台账条数一致（7 == 7），让平台视图的 parity 断言有判别力。
    for issue, tenant in (
        ("a-1", TENANT_A),
        ("a-2", TENANT_A),
        ("b-1", TENANT_B),
        ("b-2", TENANT_B),
        ("b-3", TENANT_B),
        ("a-legacy", TENANT_A),
        ("legacy", None),
    ):
        task = {
            "type": "start",
            "flow_id": "f",
            "execution_id": issue,
            "params": {},
        }
        if tenant:
            task["tenant_id"] = tenant
        enqueue_task(redis, QUEUE, task)


@pytest.fixture()
def env(tmp_path, monkeypatch) -> Env:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/issue263l.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())

    flow_store.init_engine(f"sqlite:///{tmp_path}/issue263l.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")
    tenants_svc.create_tenant(store, TENANT_B, "Tenant B")

    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    users_svc.create_user(store, "viewer-a", VIEWER_PW, "viewer")
    users_svc.add_member(store, TENANT_A, "viewer-a", "viewer")

    redis = FakeRedis(decode_responses=True)
    _seed(redis, store)

    app = FastAPI()
    app.state.store = store
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        capacity_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return Env(TestClient(app), redis, store)


def _login(client: TestClient, username: str, password: str) -> dict:
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _issue_ids(resp) -> set[str]:
    assert resp.status_code == 200, resp.text
    return {row["issue_id"] for row in resp.json()["rows"]}


def test_viewer_only_sees_own_tenant(env: Env):
    """负控：去掉租户/世代过滤 ⇒ 本条读到 b-*/legacy 行而精确红。"""
    headers = _login(env.client, "viewer-a", VIEWER_PW)
    resp = env.client.get("/api/dispatch/ledger", headers=headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert _issue_ids(resp) == {"a-1", "a-2"}
    assert all(row["tenant_id"] == TENANT_A for row in body["rows"])
    assert body["ledger"] == 2
    # 租户视图不泄露全局队列计数，也不伪造成 0/相等。
    assert body["queue_length"] is None
    assert body["parity"] is False
    assert body["error"] == TENANT_SCOPED_PARITY_DETAIL
    # A54-02 契约：平台收窄字段不进入租户响应。
    assert "tenant_id" not in body


def test_viewer_cross_tenant_param_cannot_widen_scope(env: Env):
    """跨租户参数不改变作用域：viewer 仍只读本租户当前世代行。"""
    headers = _login(env.client, "viewer-a", VIEWER_PW)
    resp = env.client.get(
        "/api/dispatch/ledger", params={"tenant_id": TENANT_B}, headers=headers,
    )
    assert resp.status_code == 200, resp.text
    assert _issue_ids(resp) == {"a-1", "a-2"}
    assert TENANT_B not in resp.text


def test_viewer_own_tenant_query_ok(env: Env):
    """显式传本租户不是跨租户：仍 200，作用域不变。"""
    headers = _login(env.client, "viewer-a", VIEWER_PW)
    resp = env.client.get(
        "/api/dispatch/ledger", params={"tenant_id": TENANT_A}, headers=headers,
    )
    assert resp.status_code == 200, resp.text
    assert _issue_ids(resp) == {"a-1", "a-2"}


def test_platform_admin_sees_all(env: Env):
    root = _login(env.client, "root", ROOT_PW)

    full = env.client.get("/api/dispatch/ledger", headers=root)
    assert full.status_code == 200, full.text
    body = full.json()
    assert _issue_ids(full) == {
        "a-1", "a-2", "b-1", "b-2", "b-3", "a-legacy", "legacy",
    }
    assert body["ledger"] == 7
    assert body["queue_length"] == 7
    assert body["parity"] is True
    assert body["error"] is None
    assert "tenant_id" not in body


def test_api_key_is_platform_full_view(env: Env):
    resp = env.client.get(
        "/api/dispatch/ledger", headers={"X-Admin-API-Key": ADMIN_KEY},
    )
    assert resp.status_code == 200, resp.text
    assert _issue_ids(resp) == {
        "a-1", "a-2", "b-1", "b-2", "b-3", "a-legacy", "legacy",
    }
    assert resp.json()["ledger"] == 7
    assert "tenant_id" not in resp.json()
