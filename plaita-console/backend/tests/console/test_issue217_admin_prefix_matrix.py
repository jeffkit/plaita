"""plaita#217 A3/A4：``_ADMIN_PREFIXES`` 里 ``/api/credentials`` 与 ``/api/cluster``
两行**可被整行删除**而既有 6 个 console 测试文件仍全绿。

缺口形态（issue 正文 A3/A4）：

* ``auth._authorize`` 对 ``_ADMIN_PREFIXES`` 做 ``path.startswith(p)``，这是
  ``/api/credentials`` / ``/api/cluster`` 两个管理面的**唯一**角色闸——两个 router
  内的端点只有路由级 ``require_auth``，没有自己的 ``role == "admin"`` 判断；
* 既有用例里 editor/viewer 对这两个前缀的断言要么不存在，要么命中的是**别的**闸
  （如 viewer 写方法闸，detail 是「editor 及以上」），所以把前缀行删掉后仍是
  「403 + 某文案」，或者根本没人打过这些路由 ⇒ 回归全绿、editor 直接
  获得凭据/集群管理面（RBAC 绕过）。

本文件用**真实 router**（与 ``main.create_app`` 相同的挂载方式：
``include_router(..., dependencies=[Depends(require_auth)])``）钉住：

1. editor 对两个前缀的读/写方法一律 403，且 detail 是 **admin 闸**文案
   （``该操作需要 admin 角色``）——不是 viewer 写闸的 editor 文案；
2. 正控：admin 打同样路由 200（避免「无条件 403」也能让本文件变绿）；
3. 正控：同一 editor 会话打非 admin 前缀（``GET /api/auth/me``）仍是 200
   （证明 403 来自前缀闸，而不是坏会话/坏角色）。

负控（未入库，验收时手工执行，见 ``/tmp/w42-out/neg-*.log``）：

* 从 ``auth._ADMIN_PREFIXES`` 删除 ``"/api/credentials"`` ⇒
  ``test_editor_forbidden_on_credentials_admin_prefix`` 的 4 条参数化用例全红
  （editor 读到 200 或走到 handler 的 404/422），其余用例保持绿；
* 删除 ``"/api/cluster"`` ⇒ ``test_editor_forbidden_on_cluster_admin_prefix``
  的 4 条全红。
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

from api import auth_users  # noqa: E402
from api import cluster as cluster_api  # noqa: E402
from api import credentials as credentials_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store, users_svc  # noqa: E402

ADMIN_GATE_DETAIL = "该操作需要 admin 角色"

ADMIN_PASSWORD = "admin-password-1"
EDITOR_PASSWORD = "editor-password-1"
VIEWER_PASSWORD = "viewer-password-1"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ""
    s.allow_insecure_admin = False
    return s


def _build_app(tmp_path, monkeypatch) -> FastAPI:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/rbac217.db")
    # 凭据落点全部钉在 tmp_path：负控里 editor 真被放行时也不会写到家目录/仓库。
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())

    flow_store.init_engine(f"sqlite:///{tmp_path}/rbac217.db")
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "admin", ADMIN_PASSWORD, "admin")
    users_svc.create_user(store, "editor1", EDITOR_PASSWORD, "editor")
    users_svc.create_user(store, "viewer1", VIEWER_PASSWORD, "viewer")

    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    # 与 main.create_app 的挂载方式一致：集中 require_auth。
    for router in (credentials_api.router, cluster_api.router):
        app.include_router(router, prefix="/api", dependencies=[Depends(require_auth)])
    return app


@pytest.fixture()
def client(tmp_path, monkeypatch) -> TestClient:
    return TestClient(_build_app(tmp_path, monkeypatch))


def _login(client: TestClient, username: str, password: str) -> dict:
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


# A3：``/api/credentials`` 管理面。credentials.py 只有 GET(列表/详情)/POST/DELETE
# 三类路由（**没有 PUT**）；四类都覆盖，负控删除前缀行后各自走到 handler：
# 列表 200、详情 404、POST 200（tmp_path 落盘）、DELETE 404。
CREDENTIALS_CASES = [
    pytest.param("GET", "/api/credentials", None, id="get-list"),
    pytest.param("GET", "/api/credentials/w42-missing", None, id="get-detail"),
    pytest.param(
        "POST", "/api/credentials",
        {"name": "w42-probe", "type": "generic", "data": {"note": "w42"}},
        id="post-save",
    ),
    pytest.param("DELETE", "/api/credentials/w42-missing", None, id="delete"),
]

# A4：``/api/cluster`` 管理面。读（config / 静态模板）与写（mode / infra check）
# 各取两条；负控删除前缀行后各自走到 handler：200 / 200 / 400 / 404。
CLUSTER_CASES = [
    pytest.param("GET", "/api/cluster/config", None, id="get-config"),
    pytest.param(
        "GET", "/api/cluster/infrastructure-templates", None, id="get-templates",
    ),
    pytest.param(
        "POST", "/api/cluster/mode", {"mode": "w42-invalid"}, id="post-mode",
    ),
    pytest.param(
        "POST", "/api/cluster/infrastructure/w42-missing/check", None,
        id="post-infra-check",
    ),
]


def _assert_admin_gate(client: TestClient, headers: dict, method: str, path: str, body):
    r = client.request(method, path, headers=headers, json=body)
    assert r.status_code == 403, (
        f"editor 未被 admin 前缀闸拒绝：{method} {path} → {r.status_code} {r.text}"
    )
    assert r.json().get("detail") == ADMIN_GATE_DETAIL, (
        f"{method} {path} 命中的不是 admin 闸：{r.json()!r}"
    )


@pytest.mark.parametrize("method,path,body", CREDENTIALS_CASES)
def test_editor_forbidden_on_credentials_admin_prefix(client, method, path, body):
    """A3：editor 打 ``/api/credentials`` 的任一方法 → 403 + admin 闸文案。

    负控：删除 ``_ADMIN_PREFIXES`` 的 ``"/api/credentials"`` 行后本条必红。
    """
    _assert_admin_gate(client, _login(client, "editor1", EDITOR_PASSWORD), method, path, body)


@pytest.mark.parametrize("method,path,body", CLUSTER_CASES)
def test_editor_forbidden_on_cluster_admin_prefix(client, method, path, body):
    """A4：editor 打 ``/api/cluster`` 的任一方法 → 403 + admin 闸文案。

    负控：删除 ``_ADMIN_PREFIXES`` 的 ``"/api/cluster"`` 行后本条必红。
    """
    _assert_admin_gate(client, _login(client, "editor1", EDITOR_PASSWORD), method, path, body)


def test_viewer_still_forbidden_on_credentials_prefix(client):
    """对照：viewer 也必须 403（且是 admin 闸，不是 viewer 写闸）。"""
    viewer = _login(client, "viewer1", VIEWER_PASSWORD)
    r = client.get("/api/credentials", headers=viewer)
    assert r.status_code == 403, r.text
    assert r.json().get("detail") == ADMIN_GATE_DETAIL, r.json()


def test_viewer_still_forbidden_on_cluster_prefix(client):
    """对照：viewer 也必须 403（且是 admin 闸，不是 viewer 写闸）。"""
    viewer = _login(client, "viewer1", VIEWER_PASSWORD)
    r = client.get("/api/cluster/config", headers=viewer)
    assert r.status_code == 403, r.text
    assert r.json().get("detail") == ADMIN_GATE_DETAIL, r.json()


def test_admin_still_allowed_on_both_prefixes(client):
    """正控：admin 打同样路由 200——「无条件 403」不能冒充本文件通过。"""
    admin = _login(client, "admin", ADMIN_PASSWORD)
    r = client.get("/api/credentials", headers=admin)
    assert r.status_code == 200, r.text
    r = client.get("/api/cluster/config", headers=admin)
    assert r.status_code == 200, r.text


def test_editor_session_is_valid_outside_admin_prefixes(client):
    """正控：同一 editor 会话在非 admin 前缀仍是 200（403 来自前缀闸，不是坏会话）。"""
    editor = _login(client, "editor1", EDITOR_PASSWORD)
    r = client.get("/api/auth/me", headers=editor)
    assert r.status_code == 200, r.text
    assert r.json()["role"] == "editor", r.json()
