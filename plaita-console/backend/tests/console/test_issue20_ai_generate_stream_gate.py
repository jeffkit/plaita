"""pn#20（P1 security）：``/api/flows/ai-generate/stream`` 平台身份闸 + 工作区隔离。

缺口：
- 路由只挂 ``require_auth``，``_authorize`` 对 POST 只要求 editor+——租户
  editor/admin 都能触发后端在宿主上跑编码 Agent（``--permission-mode auto``，
  可读宿主凭据与其它租户产物）；
- 工作区固定为 ``<tmp>/plaita-console-agent-ws``，所有租户/执行共享同一目录
  （跨租户读到彼此的过程产物，且 run-lock 互斥）。

修法：
- 端点补 ``ensure_platform_admin``（对齐 ``/api/users`` 族）：租户 editor 与
  租户 admin 一律 403 具名拒绝并进审计；平台管理员 / API-Key 放行。
- ``ai_flow._workspace(tenant_id)`` 每次执行生成
  ``<root>/<tenant>/<run-id>`` 独立目录，租户 id 经过单层路径清洗。

负控：仅还原端点里的 ``ensure_platform_admin`` 调用 ⇒ 租户 editor 用例精确红
（返回 200 且生成器被调用 = pre-fix 的 Popen 形态）。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users, flows  # noqa: E402
from auth import PLATFORM_ADMIN_REQUIRED_DETAIL, require_auth  # noqa: E402
from services import ai_flow, audit as audit_svc, flow_store, tenants_svc, users_svc  # noqa: E402

ROOT_PW = "root-password-1"
TADMIN_PW = "tadmin-password-1"
EDITOR_PW = "editor-password-1"
TENANT_A = "tenant-a"


def _settings(admin_key: str = ""):
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = admin_key
    s.allow_insecure_admin = False
    return s


def _build_app(tmp_path, monkeypatch, admin_key: str = "") -> FastAPI:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/issue20.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", lambda: _settings(admin_key))
    monkeypatch.setattr("auth.get_settings", lambda: _settings(admin_key))

    flow_store.init_engine(f"sqlite:///{tmp_path}/issue20.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")

    # root：平台管理员（platform_admin=True），活跃租户 default
    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    # tenant-a 的租户 admin / editor：role 足够过 require_auth，但不是平台身份
    users_svc.create_user(store, "tadmin", TADMIN_PW, "admin")
    users_svc.add_member(store, TENANT_A, "tadmin", "admin")
    users_svc.create_user(store, "editor1", EDITOR_PW, "editor")
    users_svc.add_member(store, TENANT_A, "editor1", "editor")

    app = FastAPI()
    app.state.store = store
    app.state.local_mode = True
    app.state.redis = None
    app.include_router(auth_users.router, prefix="/api")
    # 与 main.create_app 相同：管理面 router 统一挂 require_auth
    app.include_router(flows.router, prefix="/api", dependencies=[Depends(require_auth)])
    return app


@pytest.fixture()
def client(tmp_path, monkeypatch) -> TestClient:
    return TestClient(_build_app(tmp_path, monkeypatch))


@pytest.fixture()
def generator_calls(monkeypatch):
    """替换真实 agent 生成器：记录调用，不启动 Popen。"""
    calls: list[tuple[str, dict]] = []

    def _fake_generate(requirement, **kwargs):
        calls.append((requirement, kwargs))
        yield {"type": "finished", "ok": True, "source": "@flow(\"x\")", "ir": None}

    monkeypatch.setattr(ai_flow, "generate_flow_events", _fake_generate)
    return calls


def _login(client: TestClient, username: str, password: str) -> str:
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return r.json()["token"]


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _post_stream(client: TestClient, headers: dict):
    return client.post("/api/flows/ai-generate/stream",
                       json={"prompt": "生成一个回声流程"}, headers=headers)


def test_tenant_editor_denied_before_agent_start(client, generator_calls):
    """负向：租户 editor → 403 具名拒绝，agent 生成器零调用。"""
    headers = _auth(_login(client, "editor1", EDITOR_PW))

    r = _post_stream(client, headers)

    assert r.status_code == 403, r.text
    assert r.json().get("detail") == PLATFORM_ADMIN_REQUIRED_DETAIL
    assert generator_calls == []
    denied = audit_svc.list_audit(action="flows.ai_generate.stream.denied", limit=50)
    assert denied and denied[0]["detail"]["denied"] is True
    assert denied[0]["detail"]["reason"] == "platform_admin_required"


def test_tenant_admin_denied_too(client, generator_calls):
    """租户 admin 的 role 是 admin，但仍不是平台身份 → 403。"""
    headers = _auth(_login(client, "tadmin", TADMIN_PW))

    r = _post_stream(client, headers)

    assert r.status_code == 403, r.text
    assert r.json().get("detail") == PLATFORM_ADMIN_REQUIRED_DETAIL
    assert generator_calls == []


def test_platform_admin_allowed(client, generator_calls):
    """正向：平台管理员放行；工作区命名空间跟随其活跃租户（default）。"""
    headers = _auth(_login(client, "root", ROOT_PW))

    r = _post_stream(client, headers)

    assert r.status_code == 200, r.text
    assert "data: " in r.text
    assert len(generator_calls) == 1
    assert generator_calls[0][1] == {"tenant_id": "default"}


def test_platform_admin_tenant_header_scopes_workspace(client, generator_calls):
    """正向：平台管理员带 X-Tenant-ID → 工作区按该租户命名空间隔离。"""
    headers = _auth(_login(client, "root", ROOT_PW))
    headers["X-Tenant-ID"] = TENANT_A

    r = _post_stream(client, headers)

    assert r.status_code == 200, r.text
    assert generator_calls[0][1] == {"tenant_id": TENANT_A}


def test_api_key_allowed(tmp_path, monkeypatch, generator_calls):
    """正向：平台级 API-Key 是平台身份，照常可用。"""
    client = TestClient(_build_app(tmp_path, monkeypatch, admin_key="platform-key-1"))

    r = _post_stream(client, {"X-Admin-API-Key": "platform-key-1"})

    assert r.status_code == 200, r.text
    assert len(generator_calls) == 1


def test_workspace_isolated_per_tenant_and_execution(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_AI_WORKSPACE", str(tmp_path / "ws-root"))

    a1 = ai_flow._workspace("tenant-a")
    a2 = ai_flow._workspace("tenant-a")
    b1 = ai_flow._workspace("tenant-b")

    # 同租户的两次执行也不共享目录（并发 agent 不再互抢 run-lock/文件）
    assert a1 != a2
    assert a1 != b1
    assert a1.parent.name == "tenant-a"
    assert b1.parent.name == "tenant-b"
    assert a1.parent.parent == b1.parent.parent == tmp_path / "ws-root"
    assert all(p.is_dir() for p in (a1, a2, b1))


def test_workspace_sanitizes_tenant_path_traversal(tmp_path, monkeypatch):
    root = tmp_path / "ws-root"
    monkeypatch.setenv("PLAITA_CONSOLE_AI_WORKSPACE", str(root))

    for raw in ("../../etc", "a/b", "..", ""):
        ws = ai_flow._workspace(raw)
        assert ws.is_relative_to(root), (raw, ws)
        assert ws.parent.parent == root
