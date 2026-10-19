"""plaita#262（P1 security）：``/api/dispatch/one`` 的服务端权威字段。

缺口（基线 ``c0bffe0``）：
- ``DispatchOneRequest.shadow`` / ``dry_run`` 直接进 ``dispatch_one`` ⇒ 租户
  editor 传 ``shadow=false`` + ``dry_run=false`` 即可绕过影子纪律真发单；
- ``tenant_id`` 直接取请求体 ⇒ editor 可把任务投进任意租户（跨租户入队）；
- 台账只有 ``decided_by``（派发方主机），没有认证主体 actor。

修复后的契约：
1. ``shadow`` 只取 ``PLAITA_CONSOLE_DISPATCH_SHADOW``；请求体同名字段被剔除
   （``extra="ignore"``）；
2. ``dry_run=false`` 仅平台身份可写；租户 editor 传 false 被忽略并强制干跑
   （WARNING 留痕）；
3. ``tenant_id`` 取认证上下文；只有可证明的平台身份可代传其它租户，否则
   403 + ``dispatch.one.denied`` 审计；
4. 台账写 ``actor`` 与生效 ``tenant_id``；消息体租户与台账同源。

负控（验收时手工执行）：还原 ``capacity.dispatch_one`` 的
shadow/dry_run/tenant 三处服务端裁决 ⇒ 前三条负向用例回到 pre-fix 形态
（editor 真入队 / 跨租户入队）而精确红；正控保持绿。
"""
from __future__ import annotations

import json
import logging
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
from services import audit as audit_svc  # noqa: E402
from services import dispatcher as dispatcher_svc  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

ROOT_PW = "root-password-1"
EDITOR_PW = "editor-password-1"
TENANT_A = "tenant-a"
TENANT_B = "tenant-b"
ADMIN_KEY = "issue262-dispatch-key"
QUEUE = "plaita:flow:queue:canary"
# 与 api.capacity.DISPATCH_TENANT_DENIED_DETAIL 同文；此处用字面量是为了让
# 负控（还原修复前源码）时测试文件仍可导入，从而得到精确红而非 collection error。
DISPATCH_TENANT_DENIED_DETAIL = "仅平台管理员可指定派发租户"


class Env:
    def __init__(self, client: TestClient, redis: FakeRedis):
        self.client = client
        self.redis = redis


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


def _build_env(tmp_path, monkeypatch) -> Env:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/issue262.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())

    flow_store.init_engine(f"sqlite:///{tmp_path}/issue262.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")
    tenants_svc.create_tenant(store, TENANT_B, "Tenant B")

    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    # editor 钉在 tenant-a，platform_admin=False —— 跨租户/真发单攻击者身份
    users_svc.create_user(store, "editor", EDITOR_PW, "editor")
    users_svc.add_member(store, TENANT_A, "editor", "editor")

    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.store = store
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        capacity_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return Env(TestClient(app), redis)


@pytest.fixture()
def env(tmp_path, monkeypatch) -> Env:
    return _build_env(tmp_path, monkeypatch)


def _login(client: TestClient, username: str, password: str) -> dict:
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _api_key_headers(tenant: str | None = None) -> dict:
    headers = {"X-Admin-API-Key": ADMIN_KEY}
    if tenant:
        headers["X-Tenant-ID"] = tenant
    return headers


def _message(redis: FakeRedis, queue: str = QUEUE, index: int = 0) -> dict:
    raw = redis.xrange(queue)[index][1]["payload"]
    return json.loads(raw)


def _denied_audits():
    return audit_svc.list_audit(action="dispatch.one.denied", limit=500)


# ---------------------------------------------------------------- 负向：editor


def test_editor_cannot_force_shadow_off_or_real_dispatch(env: Env, monkeypatch):
    """editor 传 shadow=false + dry_run=false：两者都被服务端裁决挡下。

    shadow 由 env（默认开）决定；dry_run 被强制为真 ⇒ 零写入。
    负控：还原 shadow/dry_run 服务端裁决后，pre-fix 会入队 1 条。
    """
    monkeypatch.delenv("PLAITA_CONSOLE_DISPATCH_SHADOW", raising=False)
    editor = _login(env.client, "editor", EDITOR_PW)
    before = set(env.redis.keys("*"))

    resp = env.client.post(
        "/api/dispatch/one",
        json={
            "issue_id": "i-shadow", "queue_name": QUEUE,
            "shadow": False, "dry_run": False,
        },
        headers=editor,
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["shadow"] is True, "请求体 shadow 必须被剔除（服务端 env 决定）"
    assert body["dry_run"] is True, "非平台身份的 dry_run=false 必须被忽略"
    assert body["decision"] == "shadow"
    assert body["enqueued"] is False and body["ledger_recorded"] is False
    assert set(env.redis.keys("*")) == before, "editor 不得产生任何队列/台账写入"


def test_editor_cross_tenant_dispatch_denied_and_audited(env: Env, monkeypatch):
    """editor 传 tenant-b：403 + 具名拒绝 + denied 审计，零副作用。

    负控：还原 tenant 裁决后，pre-fix 会用 tenant-b 入队（跨租户注入）。
    """
    monkeypatch.setenv("PLAITA_CONSOLE_DISPATCH_SHADOW", "0")
    editor = _login(env.client, "editor", EDITOR_PW)
    before = set(env.redis.keys("*"))

    resp = env.client.post(
        "/api/dispatch/one",
        json={
            "issue_id": "i-cross", "queue_name": QUEUE,
            "tenant_id": TENANT_B, "dry_run": False,
        },
        headers=editor,
    )

    assert resp.status_code == 403, resp.text
    assert resp.json().get("detail") == DISPATCH_TENANT_DENIED_DETAIL, resp.json()
    assert set(env.redis.keys("*")) == before
    assert env.redis.xlen(QUEUE) == 0

    rows = _denied_audits()
    assert any(
        row["actor"] == "editor"
        and row["detail"].get("denied") is True
        and row["detail"].get("reason") == "tenant_delegation_required"
        and row["detail"].get("auth_tenant") == TENANT_A
        and row["detail"].get("requested_tenant") == TENANT_B
        for row in rows
    ), rows


def test_editor_dry_run_false_forced_to_dry_run_with_warning(
    env: Env, monkeypatch, caplog,
):
    """同租户 + dry_run=false：忽略该提示、强制干跑，并留 WARNING（可观测）。"""
    monkeypatch.setenv("PLAITA_CONSOLE_DISPATCH_SHADOW", "0")
    editor = _login(env.client, "editor", EDITOR_PW)
    before = set(env.redis.keys("*"))

    with caplog.at_level(logging.WARNING, logger="api.capacity"):
        resp = env.client.post(
            "/api/dispatch/one",
            json={
                "issue_id": "i-forced", "queue_name": QUEUE,
                "tenant_id": TENANT_A, "dry_run": False,
            },
            headers=editor,
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["shadow"] is False, "shadow 跟随服务端配置（此处已显式关闭）"
    assert body["dry_run"] is True and body["decision"] == "dry_run"
    assert body["enqueued"] is False and body["ledger_recorded"] is False
    assert set(env.redis.keys("*")) == before
    assert "强制降为干跑" in caplog.text, caplog.text


# ---------------------------------------------------------------- 正向：平台身份


def test_api_key_real_dispatch_records_actor_and_tenant(env: Env, monkeypatch):
    """API-Key + X-Tenant-ID：真发单；消息体与台账同源（actor=api-key）。"""
    monkeypatch.setenv("PLAITA_CONSOLE_DISPATCH_SHADOW", "0")
    headers = _api_key_headers(TENANT_B)

    resp = env.client.post(
        "/api/dispatch/one",
        json={"issue_id": "i-key", "queue_name": QUEUE, "dry_run": False},
        headers=headers,
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["enqueued"] is True and body["ledger_recorded"] is True

    message = _message(env.redis)
    assert message["tenant_id"] == TENANT_B
    assert message["execution_id"] == "i-key"

    rows = dispatcher_svc.ledger_rows(env.redis)
    assert len(rows) == 1
    row = rows[0]
    assert row["actor"] == "api-key", "台账必须记录认证主体"
    assert row["tenant_id"] == TENANT_B, "台账租户必须与消息体同源"
    assert row["issue_id"] == "i-key" and row["enqueued"] is True


def test_platform_admin_session_can_delegate_tenant(env: Env, monkeypatch):
    """平台管理员 session 可显式代传其它租户（#239 语义：仅显式平台身份）。"""
    monkeypatch.setenv("PLAITA_CONSOLE_DISPATCH_SHADOW", "0")
    root = _login(env.client, "root", ROOT_PW)

    resp = env.client.post(
        "/api/dispatch/one",
        json={
            "issue_id": "i-delegate", "queue_name": QUEUE,
            "tenant_id": TENANT_B, "dry_run": False,
        },
        headers=root,
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["enqueued"] is True
    assert _message(env.redis)["tenant_id"] == TENANT_B
    row = dispatcher_svc.ledger_rows(env.redis)[0]
    assert row["actor"] == "root"
    assert row["tenant_id"] == TENANT_B


def test_platform_admin_cannot_override_shadow_via_body(env: Env, monkeypatch):
    """shadow 是服务端配置：连平台管理员传 shadow=false 也不生效。"""
    monkeypatch.delenv("PLAITA_CONSOLE_DISPATCH_SHADOW", raising=False)
    root = _login(env.client, "root", ROOT_PW)
    before = set(env.redis.keys("*"))

    resp = env.client.post(
        "/api/dispatch/one",
        json={
            "issue_id": "i-root-shadow", "queue_name": QUEUE,
            "shadow": False, "dry_run": False,
        },
        headers=root,
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["shadow"] is True
    assert body["enqueued"] is False
    assert set(env.redis.keys("*")) == before
