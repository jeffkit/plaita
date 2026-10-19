"""plaita#254（P1 security）：无活跃租户不得被当成平台全量视角。

根因链：
1. ``resolve_session`` 发现活跃租户成员关系已消失 → ``active_tenant=None``；
2. ``_resolve_role`` 在无租户上下文时回退 ``users.role`` 遗留全局角色；
3. ``tenant_scope()`` 把 ``tenant_id=None`` 解释为「平台全量」/写路径回退
   ``default``——从不检查 ``platform_admin`` / ``auth_source``。

触发条件是常规运维动作：平台管理员移除成员后，用户重新登录即获得全量视角。
本组用**真实鉴权链路**（登录会话 + 真实 handler）钉住四类越权：

- 被移出租户的 viewer 重登后跨租户**读** flow / schedule；
- 被移出租户的 editor 重登后跨租户**删** schedule / execution；
- 被移出租户的 editor 重登后把内容**写进 default**（required=True 回退）；
- 遗留全局 ``users.role=admin``（``platform_admin=False``、无成员关系）
  读全部租户凭据**列表与明文**。

同时钉住不回归：API-Key / platform_admin 的平台全量视角、平台级写路径
回退 default、普通租户成员的本租户读写、租户管理员的凭据明细归属校验。

负控（还原修复后必红）：把 ``tenant_scope`` 的放行分支、``_resolve_role``
的 ``users.role`` 回退、``require_auth`` 的无租户闸恢复原状，本文件
``test_offboarded_*`` / ``test_legacy_global_admin_*`` **精确红**（断言里
出现 tenant-b 条目 / 删除成功 / 明文泄露）。
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from api import credentials as credentials_api  # noqa: E402
from api import executions, flows, schedules, tenants  # noqa: E402
from auth import require_auth  # noqa: E402
import auth as auth_mod  # noqa: E402
from services import (  # noqa: E402
    credentials_svc,
    flow_store,
    local_scheduler,
    tenants_svc,
    users_svc,
)

#: 具名拒绝文案：从产品源码取（负控还原修复时该常量不存在，回退到字面量，
#: 使负控表现为**断言失败**而不是 import 错误）。
NO_ACTIVE_TENANT_DETAIL = getattr(
    auth_mod, "NO_ACTIVE_TENANT_DETAIL", "当前会话无活跃租户，请先切换租户或联系管理员"
)
from plaita.tenant_context import (  # noqa: E402
    DEFAULT_TENANT_ID,
    TENANT_SOURCE_EXPLICIT,
    set_current_tenant,
)

ADMIN_KEY = "issue254-admin-key"
TENANT_A = "tenant-a"
TENANT_B = "tenant-b"

VIEWER_PW = "vicky-password-1"
EDITOR_PW = "alice-password-1"
LEGACY_ADMIN_PW = "bob-password-1"
TENANT_ADMIN_PW = "carol-password-1"
ROOT_PW = "root-password-1"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/issue254.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", _settings)
    monkeypatch.setattr("auth.get_settings", _settings)

    # 复刻 console 进程入口（main.create_app）：显式声明 default 租户，
    # 否则 #239 fail-closed 会拒绝平台级写路径（与本议题无关）。
    set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)

    flow_store.init_engine(f"sqlite:///{tmp_path}/issue254.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()

    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, DEFAULT_TENANT_ID, "root", "admin")
    tenants_svc.create_tenant(store, TENANT_A, "A")
    tenants_svc.create_tenant(store, TENANT_B, "B")

    store.create_flow("flow-a", tenant_id=TENANT_A)
    store.create_flow("flow-b", tenant_id=TENANT_B)
    for tid in (TENANT_A, TENANT_B):
        local_scheduler.create_schedule(store, {
            "schedule_id": f"sched-{tid}", "tenant_id": tid,
            "name": f"job-{tid}", "flow_id": f"flow-{tid[-1]}",
            "cron": "* * * * *", "params": {"secret_marker": f"PARAM-{tid}"},
            "enabled": True,
        })
        flow_store.insert_local_execution(
            f"exec-{tid}", f"flow-{tid[-1]}", "1.0.0", status="completed",
            input_json='{"secret": "input-%s"}' % tid, tenant_id=tid,
        )
        credentials_svc.save_credential(
            f"cred-{tid}", "api", {"token": f"SECRET-{tid}"}, tenant_id=tid
        )

    # 普通用户都挂在 tenant-a；bob 是遗留全局 admin，无任何成员关系。
    users_svc.create_user(
        store, "vicky", VIEWER_PW, "viewer",
        memberships=[{"tenant_id": TENANT_A, "role": "viewer"}],
    )
    users_svc.create_user(
        store, "alice", EDITOR_PW, "editor",
        memberships=[{"tenant_id": TENANT_A, "role": "editor"}],
    )
    users_svc.create_user(
        store, "carol", TENANT_ADMIN_PW, "admin",
        memberships=[{"tenant_id": TENANT_A, "role": "admin"}],
    )
    users_svc.create_user(store, "bob", LEGACY_ADMIN_PW, "admin")

    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    for router in (
        flows.router,
        schedules.router,
        executions.router,
        credentials_api.router,
        tenants.router,
    ):
        app.include_router(
            router, prefix="/api", dependencies=[Depends(require_auth)]
        )
    return SimpleNamespace(client=TestClient(app), store=store)


def _login(env, username: str, password: str):
    r = env.client.post(
        "/api/auth/login", json={"username": username, "password": password}
    )
    assert r.status_code == 200, r.text
    info = r.json()
    return info, {"Authorization": f"Bearer {info['token']}"}


def _remove_member(env, tenant_id: str, username: str) -> None:
    """平台管理员经真实 API 移除成员（会撤销该用户全部会话）。"""
    _, root_h = _login(env, "root", ROOT_PW)
    r = env.client.delete(
        f"/api/tenants/{tenant_id}/members/{username}", headers=root_h
    )
    assert r.status_code == 200, r.text


def _assert_named_no_tenant_403(r) -> None:
    assert r.status_code == 403, (
        f"无活跃租户的普通会话必须 fail-closed 403，实得 {r.status_code}: {r.text[:300]}"
    )
    assert r.json().get("detail") == NO_ACTIVE_TENANT_DETAIL, (
        f"拒绝文案必须是具名的「无活跃租户」，实得 {r.json()!r}"
    )


# ---------------------------------------------------------------- 负控 1：读


def test_offboarded_viewer_cannot_read_cross_tenant(env):
    info, headers = _login(env, "vicky", VIEWER_PW)
    assert info["role"] == "viewer" and info["active_tenant"] == TENANT_A

    before = env.client.get("/api/flows", headers=headers)
    assert before.status_code == 200, before.text
    assert {f["tenant_id"] for f in before.json()["flows"]} == {TENANT_A}

    _remove_member(env, TENANT_A, "vicky")
    after_info, after_headers = _login(env, "vicky", VIEWER_PW)
    assert after_info["active_tenant"] is None
    assert after_info["platform_admin"] is False

    for path in ("/api/flows", "/api/schedules"):
        r = env.client.get(path, headers=after_headers)
        _assert_named_no_tenant_403(r)
        assert TENANT_B not in r.text, f"{path} 泄露了其他租户条目: {r.text[:300]}"

    # #254：无租户上下文不再回退 users.role 遗留角色
    assert after_info["role"] is None, (
        f"被移出租户的 viewer 重登后仍拿到遗留角色: {after_info!r}"
    )


# ---------------------------------------------------------------- 负控 2：删


def test_offboarded_editor_cannot_delete_cross_tenant_rows(env):
    info, headers = _login(env, "alice", EDITOR_PW)
    assert info["role"] == "editor" and info["active_tenant"] == TENANT_A

    _remove_member(env, TENANT_A, "alice")
    after_info, after_headers = _login(env, "alice", EDITOR_PW)
    assert after_info["active_tenant"] is None

    r_sched = env.client.delete(
        "/api/schedules/sched-tenant-b", headers=after_headers
    )
    r_exec = env.client.delete(
        "/api/executions/exec-tenant-b", headers=after_headers
    )
    # 先看破坏是否发生（负控还原修复时这里给出「行已消失」的直接证据）
    assert local_scheduler.get_schedule(env.store, "sched-tenant-b") is not None, (
        "跨租户删除成功：tenant-b 调度行已消失"
    )
    assert flow_store.get_local_execution("exec-tenant-b") is not None, (
        "跨租户删除成功：tenant-b 执行行已消失"
    )
    _assert_named_no_tenant_403(r_sched)
    _assert_named_no_tenant_403(r_exec)
    assert after_info["role"] is None


# ---------------------------------------------------------------- 负控 3：写 default


def test_offboarded_editor_cannot_write_into_default(env):
    _login(env, "alice", EDITOR_PW)
    _remove_member(env, TENANT_A, "alice")
    _, headers = _login(env, "alice", EDITOR_PW)

    r = env.client.post(
        "/api/flows", json={"flow_id": "alice-evil"}, headers=headers
    )
    _assert_named_no_tenant_403(r)
    assert env.store.get_flow_record("alice-evil", tenant_id=None) is None, (
        "写路径回退 default：被移出租户的 editor 在 default 建出了流程"
    )


# ---------------------------------------------------------------- 负控 4：凭据明文


def test_legacy_global_admin_cannot_read_credentials(env):
    info, headers = _login(env, "bob", LEGACY_ADMIN_PW)
    assert info["platform_admin"] is False
    assert info["active_tenant"] is None

    r_list = env.client.get("/api/credentials", headers=headers)
    r_detail = env.client.get("/api/credentials/cred-tenant-a", headers=headers)
    # 先看明文是否泄露（负控还原修复时这里给出直接证据）
    assert "SECRET-tenant-a" not in r_detail.text, "遗留 admin 读到了跨租户凭据明文"
    assert "cred-tenant-b" not in r_list.text, "遗留 admin 列出了他租户凭据"

    _assert_named_no_tenant_403(r_list)
    assert "cred-tenant-a" not in r_list.text and "cred-tenant-b" not in r_list.text
    _assert_named_no_tenant_403(r_detail)
    assert "SECRET-tenant-a" not in r_detail.text
    assert info["role"] is None, (
        f"遗留 users.role=admin 仍被回退为管理面角色: {info!r}"
    )


# ---------------------------------------------------------------- 正控：平台不回归


def test_platform_admin_and_api_key_keep_full_view(env):
    # API-Key 是可证明的平台级身份：读全量、写回退 default（E2E 兼容）
    hk = {"X-Admin-API-Key": ADMIN_KEY}
    flows_resp = env.client.get("/api/flows", headers=hk)
    assert flows_resp.status_code == 200, flows_resp.text
    assert {f["tenant_id"] for f in flows_resp.json()["flows"]} == {TENANT_A, TENANT_B}

    creds_resp = env.client.get("/api/credentials", headers=hk)
    assert creds_resp.status_code == 200, creds_resp.text
    assert {c["name"] for c in creds_resp.json()["credentials"]} == {
        "cred-tenant-a", "cred-tenant-b"
    }
    detail = env.client.get("/api/credentials/cred-tenant-b", headers=hk)
    assert detail.status_code == 200, detail.text
    assert detail.json()["data"]["token"] == "SECRET-tenant-b"

    write = env.client.post(
        "/api/flows", json={"flow_id": "api-key-flow"}, headers=hk
    )
    assert write.status_code == 200, write.text
    assert write.json()["tenant_id"] == DEFAULT_TENANT_ID, (
        "平台级身份的写路径必须保持回退 default（API-key/E2E 集成兼容）"
    )

    # 平台管理员会话：显式 X-Tenant-ID 可读指定租户凭据明文
    root_info, root_h = _login(env, "root", ROOT_PW)
    assert root_info["platform_admin"] is True
    detail = env.client.get(
        "/api/credentials/cred-tenant-b",
        headers={**root_h, "X-Tenant-ID": TENANT_B},
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["tenant_id"] == TENANT_B


def test_tenant_member_paths_unchanged(env):
    info, headers = _login(env, "alice", EDITOR_PW)
    assert info["role"] == "editor" and info["active_tenant"] == TENANT_A

    flows_resp = env.client.get("/api/flows", headers=headers)
    assert flows_resp.status_code == 200, flows_resp.text
    assert {f["tenant_id"] for f in flows_resp.json()["flows"]} == {TENANT_A}

    # 跨租户按 id 取：仍 404（不是全量视角）
    assert env.client.get("/api/flows/flow-b", headers=headers).status_code == 404


# ---------------------------------------------------------------- 正控：凭据归属


def test_tenant_admin_credential_detail_is_tenant_owned(env):
    """租户管理员（非平台）只能读本租户凭据明文；跨租户 404。"""
    info, headers = _login(env, "carol", TENANT_ADMIN_PW)
    assert info["role"] == "admin" and info["platform_admin"] is False
    assert info["active_tenant"] == TENANT_A

    listing = env.client.get("/api/credentials", headers=headers)
    assert listing.status_code == 200, listing.text
    assert {c["name"] for c in listing.json()["credentials"]} == {"cred-tenant-a"}

    own = env.client.get("/api/credentials/cred-tenant-a", headers=headers)
    assert own.status_code == 200, own.text
    assert own.json()["data"]["token"] == "SECRET-tenant-a"

    other = env.client.get("/api/credentials/cred-tenant-b", headers=headers)
    assert other.status_code == 404, (
        f"租户管理员读到了他租户凭据明细: {other.status_code} {other.text[:200]}"
    )
    assert "SECRET-tenant-b" not in other.text

    # 删除同样按租户归属：他租户同名凭据不得被删
    deleted = env.client.delete("/api/credentials/cred-tenant-b", headers=headers)
    assert deleted.status_code == 404, deleted.text
    assert credentials_svc.get_credential_record(
        "cred-tenant-b", tenant_id=TENANT_B
    ) is not None, "租户管理员删除了他租户凭据"
