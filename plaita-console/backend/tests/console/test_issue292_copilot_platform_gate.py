"""#292（P1 security）：``POST /api/copilot`` 宿主 Agent 大脑的平台身份闸。

缺口（基线 ``b2906c2``）：
- ``copilot_run`` 只依赖 router 级 ``require_auth``（editor+ 即过），
  ``_ADMIN_PREFIXES`` 不含 ``/api/copilot``；
- ``PLAITA_CONSOLE_COPILOT_BRAIN=claude`` 时，租户 editor 的 prompt 被交给
  本机 ``claude`` CLI（``-p <prompt> --max-turns 30``），子进程 ``env`` 继承
  console 全量环境（admin key / DB / Redis / 凭据路径），stdout 经 SSE 回传；
  ``brain=recursive`` 同理把任意 body 以平台 API key 反代外部 Agent。
- 同仓 ``POST /api/flows/ai-generate/stream`` 对「宿主上跑编码 Agent」已有
  ``ensure_platform_admin`` 判例（pn#20）：宿主 Agent 是平台能力，不是租户
  编辑功能。

修法：``copilot_run`` 对 ``brain != "flow_agent"``（凡 spawn 宿主 Agent 的
brain：claude / recursive）调 ``ensure_platform_admin``，与 ai-generate 同
口径；``flow_agent``（纯 validate/dry-run 工具，无宿主 Agent）保持租户可用。

负控：仅去掉 ``copilot_run`` 里的 ``ensure_platform_admin`` 调用 ⇒ 租户
editor claude/recursive 用例精确红（200 + 大脑被调用 = pre-fix 形态）。
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users, copilot as copilot_api  # noqa: E402
from auth import PLATFORM_ADMIN_REQUIRED_DETAIL, require_auth, tenant_generation_context  # noqa: E402
from services import audit as audit_svc, claude_brain, flow_agent, flow_store, tenants_svc, users_svc  # noqa: E402

ROOT_PW = "root-password-1"
TADMIN_PW = "tadmin-password-1"
EDITOR_PW = "editor-password-1"
TENANT_A = "tenant-a"

BODY = {
    "threadId": "t-292",
    "runId": "r-292",
    "messages": [{"role": "user", "content": "帮我把回声流程改成两跳"}],
    "context": [],
}


def _settings(admin_key: str = "", recursive_url: str = ""):
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = admin_key
    s.allow_insecure_admin = False
    s.copilot_brain = ""
    s.recursive_agui_url = recursive_url
    return s


def _build_app(tmp_path, monkeypatch, admin_key: str = "", recursive_url: str = "") -> FastAPI:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/issue292.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.delenv("PLAITA_CONSOLE_COPILOT_BRAIN", raising=False)

    def _patched_settings(ak=admin_key, ru=recursive_url):
        return _settings(ak, ru)

    monkeypatch.setattr("config.get_settings", _patched_settings)
    monkeypatch.setattr("auth.get_settings", _patched_settings)
    # copilot 模块的 get_settings 是 import 期绑定，须按模块对象补丁
    monkeypatch.setattr(copilot_api, "get_settings", _patched_settings)

    flow_store.init_engine(f"sqlite:///{tmp_path}/issue292.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")

    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    users_svc.create_user(store, "tadmin", TADMIN_PW, "admin")
    users_svc.add_member(store, TENANT_A, "tadmin", "admin")
    users_svc.create_user(store, "editor1", EDITOR_PW, "editor")
    users_svc.add_member(store, TENANT_A, "editor1", "editor")

    app = FastAPI()
    app.state.store = store
    app.state.local_mode = True
    app.state.redis = None
    app.include_router(auth_users.router, prefix="/api")
    # 与 main._mount_admin 相同的生产依赖（require_auth + 租户世代上下文）
    app.include_router(
        copilot_api.router,
        prefix="/api",
        dependencies=[Depends(require_auth), Depends(tenant_generation_context)],
    )
    return app


@pytest.fixture()
def client(tmp_path, monkeypatch) -> TestClient:
    return TestClient(_build_app(tmp_path, monkeypatch))


@pytest.fixture()
def claude_calls(monkeypatch):
    """替换 claude 大脑：记录调用、不 spawn 真实 CLI；available 恒真。"""
    calls: List[Dict[str, Any]] = []

    async def _fake_stream(body, context_note, tenant_id=""):
        calls.append({"body": body, "context_note": context_note, "tenant_id": tenant_id})
        yield b'data: {"type": "RunFinished", "threadId": "t-292"}\n\n'

    monkeypatch.setattr(claude_brain, "claude_available", lambda: True)
    monkeypatch.setattr(claude_brain, "claude_agui_stream", _fake_stream)
    return calls


def _login(client: TestClient, username: str, password: str) -> str:
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return r.json()["token"]


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _post_copilot(client: TestClient, headers: dict, body=None):
    payload = dict(BODY) if body is None else body
    return client.post(
        "/api/copilot",
        json=payload,
        headers={"x-flow-id": "flow-292", **headers},
    )


# ---- 负控（修复后应绿；pre-fix 为 200 + 大脑被调用）----

def test_editor_claude_brain_403_named_and_zero_start(client, claude_calls, monkeypatch):
    """租户 editor + brain=claude → 403 具名拒绝，claude 大脑零启动、零留痕。"""
    monkeypatch.setenv("PLAITA_CONSOLE_COPILOT_BRAIN", "claude")
    headers = _auth(_login(client, "editor1", EDITOR_PW))

    r = _post_copilot(client, headers)

    assert r.status_code == 403, r.text
    assert r.json().get("detail") == PLATFORM_ADMIN_REQUIRED_DETAIL
    assert claude_calls == []
    # 闸先于会话留痕：被拒请求不得建立 thread 关联
    assert flow_store.get_flow_store().list_copilot_threads("flow-292", tenant_id=None) == []
    denied = audit_svc.list_audit(action="copilot.run.denied", limit=50)
    assert denied and denied[0]["detail"]["denied"] is True
    assert denied[0]["detail"]["reason"] == "platform_admin_required"


def test_editor_claude_brain_403_before_body_parse(client, claude_calls, monkeypatch):
    """闸先于请求体解析：editor + claude + 非法 JSON → 403 而非 400。"""
    monkeypatch.setenv("PLAITA_CONSOLE_COPILOT_BRAIN", "claude")
    headers = _auth(_login(client, "editor1", EDITOR_PW))

    r = client.post(
        "/api/copilot", content=b"{not-json",
        headers={"x-flow-id": "flow-292",
                 "content-type": "application/json", **headers},
    )

    assert r.status_code == 403, r.text
    assert claude_calls == []


def test_editor_recursive_brain_403(tmp_path, monkeypatch):
    """租户 editor + brain=recursive → 403；反代面（httpx）零构造。"""
    client = TestClient(
        _build_app(tmp_path, monkeypatch, recursive_url="http://127.0.0.1:9/agui")
    )
    headers = _auth(_login(client, "editor1", EDITOR_PW))
    monkeypatch.setenv("PLAITA_CONSOLE_COPILOT_BRAIN", "recursive")

    r = _post_copilot(client, headers)

    assert r.status_code == 403, r.text
    assert r.json().get("detail") == PLATFORM_ADMIN_REQUIRED_DETAIL


def test_tenant_admin_claude_brain_403_too(client, claude_calls, monkeypatch):
    """租户 admin 是租户内管理权，不是平台身份 → 403。"""
    monkeypatch.setenv("PLAITA_CONSOLE_COPILOT_BRAIN", "claude")
    headers = _auth(_login(client, "tadmin", TADMIN_PW))

    r = _post_copilot(client, headers)

    assert r.status_code == 403, r.text
    assert claude_calls == []


# ---- 正控（平台身份放行 + flow_agent 租户保持可用）----

def test_platform_admin_claude_brain_passes(client, claude_calls, monkeypatch):
    """平台管理员（会话）+ brain=claude → 200 SSE，大脑收到租户上下文。"""
    monkeypatch.setenv("PLAITA_CONSOLE_COPILOT_BRAIN", "claude")
    headers = _auth(_login(client, "root", ROOT_PW))

    r = _post_copilot(client, headers)

    assert r.status_code == 200, r.text
    assert "RunFinished" in r.text
    assert len(claude_calls) == 1


def test_api_key_claude_brain_passes(tmp_path, monkeypatch, claude_calls):
    """平台级 API-Key 是平台身份 → 200。"""
    client = TestClient(
        _build_app(tmp_path, monkeypatch, admin_key="platform-key-292")
    )
    monkeypatch.setenv("PLAITA_CONSOLE_COPILOT_BRAIN", "claude")

    r = _post_copilot(client, {"X-Admin-API-Key": "platform-key-292"})

    assert r.status_code == 200, r.text
    assert len(claude_calls) == 1


def test_editor_flow_agent_brain_reaches_dispatch(client, monkeypatch):
    """正控：纯 validate/dry-run 的 flow_agent 对租户 editor 保持可用——
    闸不拦（403 才是闸形）；此处以可判定的 503（agent 构造失败）证明 editor
    已通过闸到达大脑分发。"""
    def _boom():
        raise RuntimeError("no provider credentials in test")

    monkeypatch.setattr(flow_agent, "get_flow_agent", _boom)

    headers = _auth(_login(client, "editor1", EDITOR_PW))

    r = _post_copilot(client, headers)

    assert r.status_code == 503, r.text
    assert "flow_agent 构造失败" in r.json()["detail"]
