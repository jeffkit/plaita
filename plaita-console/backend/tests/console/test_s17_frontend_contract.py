"""第四十二轮 B2：#266 + S17-01/02/03 契约回归。

覆盖：
1. **#266**：前端源码/文档不再出现浏览器管理密钥通道；若本地有构建产物，
   同时扫描 bundle。
2. **S17-01**：登录期内存 token 直接 `POST /api/auth/switch-tenant`；
   不切换时读写落登录钉住的租户（pre-fix 形态），切换后读写落目标租户；
   非成员 400 不改变活跃租户；无头 401。
3. **S17-02**：管理面/ Copilot 共用 `require_auth`：无头 401、会话 Bearer 200。
4. **S17-03 3C**：SPA 兜底不得把未知 `/api/*` 变成 200 HTML。

前端接线（apiFetch 注入、Login 显式 token、SSE 终态守卫）由
``frontend/scripts/check-api-transport.mjs`` 与 ``check-admin-key.mjs`` 钉住，
CI 的 "Console frontend contracts" 步骤执行。负控：还原 pre-fix 前端形态 →
上述脚本红；本文件第 2 组断言在「不调 switch」时精确复现落 A。
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

import main as main_module  # noqa: E402
from api import auth_users  # noqa: E402
from api import flows as flows_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402
from services import tenants_svc  # noqa: E402
from services import users_svc  # noqa: E402

ADMIN_KEY = "s17-admin-key"
MULTI_PW = "multi-password-1"
TENANT_A = "tenant-a"
TENANT_B = "tenant-b"
TENANT_C = "tenant-c"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def app(tmp_path, monkeypatch) -> FastAPI:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/s17.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())

    flow_store.init_engine(f"sqlite:///{tmp_path}/s17.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    for tenant_id in (TENANT_A, TENANT_B, TENANT_C):
        tenants_svc.create_tenant(store, tenant_id, tenant_id.upper())
    users_svc.create_user(store, "multi", MULTI_PW, "editor")
    users_svc.add_member(store, TENANT_A, "multi", "editor")
    users_svc.add_member(store, TENANT_B, "multi", "editor")
    store.create_flow("flow-a", author="seed", desc="", tenant_id=TENANT_A)
    store.create_flow("flow-b", author="seed", desc="", tenant_id=TENANT_B)

    app = FastAPI()
    app.state.store = store
    app.state.redis = None
    app.state.local_mode = True
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        flows_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )

    @app.get("/api/probe-auth")
    def probe_auth(identity: dict = Depends(require_auth)):
        return {"actor": identity["actor"], "role": identity["role"]}

    return app


@pytest.fixture()
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def _login(client: TestClient) -> dict:
    r = client.post("/api/auth/login", json={"username": "multi", "password": MULTI_PW})
    assert r.status_code == 200, r.text
    return r.json()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _flow_ids(client: TestClient, token: str) -> set[str]:
    r = client.get("/api/flows", headers=_auth(token))
    assert r.status_code == 200, r.text
    return {f["flow_id"] for f in r.json()["flows"]}


def _pin_tenant(client: TestClient, token: str, tenant_id: str) -> None:
    r = client.post(
        "/api/auth/switch-tenant",
        json={"tenant_id": tenant_id},
        headers=_auth(token),
    )
    assert r.status_code == 200, r.text


# ---- S17-01 ----


def test_switch_tenant_with_memory_token_changes_read_and_write_scope(client):
    login = _login(client)
    token = login["token"]
    assert {m["tenant_id"] for m in login["memberships"]} == {TENANT_A, TENANT_B}
    # 登录可能钉到任一成员租户；先把基线钉到 A（对应多租户选择页的「未选」形态）
    _pin_tenant(client, token, TENANT_A)

    # pre-fix 形态（选择租户不发请求）：读 A、写 A
    assert _flow_ids(client, token) == {"flow-a"}
    r = client.post(
        "/api/flows",
        json={"flow_id": "written-before-pick", "desc": ""},
        headers=_auth(token),
    )
    assert r.status_code == 200, r.text
    assert r.json()["tenant_id"] == TENANT_A

    # 修复形态：用登录返回的内存 token 直接切换（不依赖任何 localStorage）
    r = client.post(
        "/api/auth/switch-tenant",
        json={"tenant_id": TENANT_B},
        headers=_auth(token),
    )
    assert r.status_code == 200, r.text
    assert r.json()["tenant_id"] == TENANT_B
    assert r.json()["role"] == "editor"

    assert _flow_ids(client, token) == {"flow-b"}
    r = client.post(
        "/api/flows",
        json={"flow_id": "written-after-pick", "desc": ""},
        headers=_auth(token),
    )
    assert r.status_code == 200, r.text
    assert r.json()["tenant_id"] == TENANT_B


def test_switch_tenant_rejects_bad_cases_without_moving_active_tenant(client):
    login = _login(client)
    token = login["token"]
    _pin_tenant(client, token, TENANT_B)

    # 无头：401（登录期绝不能靠 localStorage 里还没有的 token）
    r = client.post("/api/auth/switch-tenant", json={"tenant_id": TENANT_A})
    assert r.status_code == 401, r.text

    # 非成员：400，活跃租户不变
    r = client.post(
        "/api/auth/switch-tenant",
        json={"tenant_id": TENANT_C},
        headers=_auth(token),
    )
    assert r.status_code == 400
    assert "不是租户" in r.json()["detail"]
    assert _flow_ids(client, token) == {"flow-b"}


# ---- S17-02 ----


def test_auth_gate_for_copilot_and_admin_surfaces(client):
    # 无头 → 401（Copilot HttpAgent 旧形态）
    assert client.get("/api/probe-auth").status_code == 401

    # 会话 Bearer → 200（apiFetch 注入后的形态）
    token = _login(client)["token"]
    r = client.get("/api/probe-auth", headers=_auth(token))
    assert r.status_code == 200, r.text
    assert r.json()["actor"] == "multi"

    # 错误 Bearer → 401
    assert client.get("/api/probe-auth", headers=_auth("bogus")).status_code == 401

    # 服务端/CLI 的 X-Admin-API-Key 路径保留（不属于浏览器）
    r = client.get("/api/probe-auth", headers={"X-Admin-API-Key": ADMIN_KEY})
    assert r.status_code == 200, r.text


# ---- S17-03 3C ----


def test_spa_fallback_returns_json_404_for_unknown_api_path(tmp_path):
    (tmp_path / "index.html").write_text("<!doctype html><html><body>app</body></html>")
    (tmp_path / "assets").mkdir()
    (tmp_path / "assets" / "index.js").write_text("console.log('app')")

    app = FastAPI()
    app.mount("/", main_module.make_spa_static_files(tmp_path), name="web")
    client = TestClient(app)

    r = client.get("/api/does-not-exist")
    assert r.status_code == 404, r.text
    assert r.headers["content-type"].startswith("application/json")
    assert "接口不存在" in r.json()["detail"]

    assert client.get("/api").status_code == 404
    # 非 /api 深链仍然回 index.html（BrowserRouter）
    r = client.get("/executions/abc")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]


# ---- #266 源码/文档回归 ----


FRONTEND = BACKEND_DIR.parent / "frontend"
FORBIDDEN_SOURCE = (
    "VITE_PLAITA_ADMIN_API_KEY",
    "plaita_admin_api_key",
    "getAdminApiKey",
    "X-Admin-API-Key",
)


def test_issue266_frontend_source_has_no_admin_key_channel():
    offenders = []
    for path in (FRONTEND / "src").rglob("*"):
        if not path.is_file() or path.suffix not in {".ts", ".tsx", ".js", ".jsx"}:
            continue
        text = path.read_text(encoding="utf-8")
        for token in FORBIDDEN_SOURCE:
            if token in text:
                offenders.append(f"{path.relative_to(FRONTEND)}: {token}")
    assert offenders == [], "前端源码仍含管理密钥通道：" + ", ".join(offenders)

    readme = (BACKEND_DIR.parent / "README.md").read_text(encoding="utf-8")
    for token in ("VITE_PLAITA_ADMIN_API_KEY", "plaita_admin_api_key", "getAdminApiKey"):
        assert token not in readme, f"README 仍含 {token}"


def test_issue266_built_bundle_has_no_admin_key_material():
    assets = FRONTEND / "dist" / "assets"
    if not assets.is_dir():
        pytest.skip("本地无前端构建产物（CI 由 check-admin-key.mjs --dist 覆盖）")
    offenders = []
    for path in assets.glob("*.js"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        for token in FORBIDDEN_SOURCE:
            if token in text:
                offenders.append(f"{path.name}: {token}")
    assert offenders == [], "构建产物仍含管理密钥材料：" + ", ".join(offenders)
