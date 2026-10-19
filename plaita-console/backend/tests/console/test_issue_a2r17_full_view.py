"""A2R17-01：平台管理员「全量视角」服务端权威 + 只读。

修复前形态：TenantSwitcher 的全量选项只写 localStorage，服务端会话仍钉在
旧租户——UI 标签「全量」与请求作用域错位（读/写都还在旧租户）。本文件钉住：

1. ``POST /api/auth/switch-tenant {"tenant_id": null}`` 对平台管理员是
   显式清活跃租户；之后无头读请求（不带 X-Tenant-ID）确实跨租户。
2. 全量视角只读：租户作用域写路径（``tenant_scope(required=True)``）返回
   403 具名引导「先选择具体租户」，不再静默回退 default。
3. 选择具体租户后写路径恢复，落该租户。
4. 非平台管理员不能切换到全量（400，活跃租户不变）；缺省/空串不解释成
   全量（422，防止客户端漏传时静默清租户）。
5. 同批 A2 r22：editor 保存版本时超限 ``retryTimes`` / ``maxRedirects`` /
   ``requestTimeout`` 构造期 422，合法边界值 200。

负控（只回退实现文件、保留测试）：null 清租户 422/写回退 default、读只见
单租户、超限定义保存成功——对应断言精确红。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import auth as auth_mod  # noqa: E402
from api import auth_users, flows  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

ADMIN_KEY = "a2r17-admin-key"
TENANT_A = "tenant-a"
TENANT_B = "tenant-b"

ROOT_PW = "root-password-1"
TENANT_ADMIN_PW = "tenant-admin-password-1"
EDITOR_PW = "editor-password-1"

#: 负控还原修复时常量不存在——回退字面量，让负控表现为断言失败而非
#: import 错误。
FULL_VIEW_WRITE_DETAIL = getattr(
    auth_mod,
    "FULL_VIEW_WRITE_REQUIRES_TENANT_DETAIL",
    "全量视角为只读，写操作请先选择具体租户",
)


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/a2r17.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setattr("config.get_settings", _settings)
    monkeypatch.setattr("auth.get_settings", _settings)

    flow_store.init_engine(f"sqlite:///{tmp_path}/a2r17.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()

    tenants_svc.create_tenant(store, TENANT_A, "A")
    tenants_svc.create_tenant(store, TENANT_B, "B")
    store.create_flow("flow-a", tenant_id=TENANT_A)
    store.create_flow("flow-b", tenant_id=TENANT_B)

    users_svc.create_user(
        store, "root", ROOT_PW, "admin", platform_admin=True,
        memberships=[
            {"tenant_id": TENANT_A, "role": "admin"},
            {"tenant_id": TENANT_B, "role": "admin"},
        ],
    )
    users_svc.create_user(
        store, "tenantadmin", TENANT_ADMIN_PW, "admin",
        memberships=[{"tenant_id": TENANT_A, "role": "admin"}],
    )
    users_svc.create_user(
        store, "editor", EDITOR_PW, "editor",
        memberships=[{"tenant_id": TENANT_A, "role": "editor"}],
    )

    app = FastAPI()
    app.state.store = store
    app.state.redis = None
    app.state.local_mode = True
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        flows.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return SimpleNamespace(client=TestClient(app), store=store)


def _login(env, username: str, password: str):
    r = env.client.post(
        "/api/auth/login", json={"username": username, "password": password}
    )
    assert r.status_code == 200, r.text
    info = r.json()
    return info, {"Authorization": f"Bearer {info['token']}"}


def _switch(env, headers, tenant_id):
    return env.client.post(
        "/api/auth/switch-tenant", json={"tenant_id": tenant_id}, headers=headers
    )


def _flow_ids(env, headers) -> set[str]:
    r = env.client.get("/api/flows", headers=headers)
    assert r.status_code == 200, r.text
    return {f["flow_id"] for f in r.json()["flows"]}


# ---------------------------------------------------------------- 全量视角


def test_platform_admin_full_view_is_cross_tenant_read(env):
    info, headers = _login(env, "root", ROOT_PW)
    assert info["platform_admin"] is True
    # 登录默认钉在其中一个成员租户
    assert _flow_ids(env, headers) in ({"flow-a"}, {"flow-b"})

    r = _switch(env, headers, None)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["tenant_id"] is None
    assert body["platform_admin"] is True
    assert body["role"] == "admin"

    # 无头请求（不带 X-Tenant-ID）确实落到服务端会话的全量视角
    assert _flow_ids(env, headers) == {"flow-a", "flow-b"}
    me = env.client.get("/api/auth/me", headers=headers)
    assert me.status_code == 200, me.text
    assert me.json()["tenant_id"] is None

    # 具体租户视角仍可回去
    r = _switch(env, headers, TENANT_B)
    assert r.status_code == 200, r.text
    assert r.json()["tenant_id"] == TENANT_B
    assert _flow_ids(env, headers) == {"flow-b"}


def test_full_view_write_is_named_rejected_then_works_after_pick(env):
    _, headers = _login(env, "root", ROOT_PW)
    assert _switch(env, headers, None).status_code == 200

    r = env.client.post("/api/flows", json={"flow_id": "full-view-write"}, headers=headers)
    assert r.status_code == 403, (
        f"全量视角写路径必须拒绝（不能回退 default），实得 {r.status_code}: {r.text[:300]}"
    )
    assert r.json().get("detail") == FULL_VIEW_WRITE_DETAIL
    assert env.store.get_flow_record("full-view-write") is None

    # 引导路径：选择具体租户后同一写操作恢复，落该租户
    assert _switch(env, headers, TENANT_A).status_code == 200
    r = env.client.post("/api/flows", json={"flow_id": "after-pick"}, headers=headers)
    assert r.status_code == 200, r.text
    assert r.json()["tenant_id"] == TENANT_A


def test_non_platform_user_cannot_clear_active_tenant(env):
    for username, password in (
        ("editor", EDITOR_PW),
        ("tenantadmin", TENANT_ADMIN_PW),
    ):
        info, headers = _login(env, username, password)
        assert info["active_tenant"] == TENANT_A
        r = _switch(env, headers, None)
        assert r.status_code == 400, (
            f"{username} 非平台身份不得切全量，实得 {r.status_code}: {r.text[:300]}"
        )
        assert "仅平台管理员" in r.json()["detail"]
        me = env.client.get("/api/auth/me", headers=headers).json()
        assert me["tenant_id"] == TENANT_A, "非法切换必须保持原活跃租户"

    # 普通 editor 的写路径不受影响
    _, headers = _login(env, "editor", EDITOR_PW)
    r = env.client.post("/api/flows", json={"flow_id": "editor-write"}, headers=headers)
    assert r.status_code == 200, r.text
    assert r.json()["tenant_id"] == TENANT_A


@pytest.mark.parametrize("payload", [{}, {"tenant_id": ""}])
def test_missing_or_empty_tenant_is_not_treated_as_full_view(env, payload):
    _, headers = _login(env, "root", ROOT_PW)
    before = env.client.get("/api/auth/me", headers=headers).json()["tenant_id"]
    r = env.client.post("/api/auth/switch-tenant", json=payload, headers=headers)
    assert r.status_code == 422, (
        f"缺省/空串不得静默清租户，实得 {r.status_code}: {r.text[:300]}"
    )
    after = env.client.get("/api/auth/me", headers=headers).json()["tenant_id"]
    assert after == before


# ------------------------------------------------- 同批 A2 r22：构造期上限


def _definition(retry_times: int = 3, **http_kwargs) -> str:
    return json.dumps({
        "flow_id": "flow-a",
        "nodes": [
            {"id": "s", "type": "start", "next": "h"},
            {
                "id": "h",
                "type": "http",
                "url": "https://example.com",
                "errorHandler": {"strategy": "abort", "retryTimes": retry_times},
                "next": "e",
                **http_kwargs,
            },
            {"id": "e", "type": "end"},
        ],
    })


def test_editor_save_version_rejects_over_cap_values(env):
    _, headers = _login(env, "editor", EDITOR_PW)

    cases = [
        ({"retry_times": 10**9}, "retryTimes"),
        ({"maxRedirects": 10**9}, "maxRedirects"),
        ({"requestTimeout": 10**9}, "requestTimeout"),
    ]
    for kwargs, needle in cases:
        r = env.client.put(
            "/api/flows/flow-a/versions/1.0.0",
            json={"definition": _definition(**kwargs)},
            headers=headers,
        )
        assert r.status_code == 422, (
            f"{needle} 超限必须在保存/发布面 422，实得 {r.status_code}: {r.text[:300]}"
        )
        assert needle in r.text

    # 合法边界值不回归
    r = env.client.put(
        "/api/flows/flow-a/versions/1.0.0",
        json={
            "definition": _definition(
                retry_times=10, maxRedirects=20, requestTimeout=600
            )
        },
        headers=headers,
    )
    assert r.status_code == 200, r.text
