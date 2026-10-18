"""plaita#214：viewer 对**非 admin 写路由**的角色闸回归。

`auth._authorize` 里有两道独立的闸：

1. admin 前缀闸（`/api/users`、`/api/credentials`、`/api/audit`、`/api/cluster`、
   `/api/tenants`）——非 admin 一律 403「该操作需要 admin 角色」；
2. 写方法闸（非 GET/HEAD/OPTIONS）——viewer 一律 403
   「该操作需要 editor 及以上角色」。

历史缺口：既有 `test_viewer_blocked_from_mutations` 只打 `POST /api/credentials`，
它先命中第 1 道闸，**从未走到第 2 道**。把第 2 道闸的 `raise` 改成 `return`，
现有测试仍全绿（RBAC 绕过无回归防护）。

本文件用真实路由（flows / schedules / dry-run / supervisor）钉住第 2 道闸：
viewer 的写请求必须 403 且 detail 是 **editor 及以上**（不是 admin），
证明命中的是非 admin 前缀的写分支；同时保留正控（viewer GET 200、editor 写
放行、admin 前缀仍走第 1 道闸）。

负控（未入库，验收时手工执行）：把 `auth.py` 中 viewer 写分支的
``raise HTTPException(... "该操作需要 editor 及以上角色")`` 改成 ``return``，
本文件中所有 ``test_viewer_write_...`` 参数化用例必须红（路由落到真正的业务
handler，典型是 200/404/500，不再是该 403 detail），其余正控保持绿。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from api import credentials as credentials_api  # noqa: E402
from api import dryrun as dryrun_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from api import schedules as schedules_api  # noqa: E402
from api import supervisor as supervisor_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import examples as examples_svc  # noqa: E402
from services import flow_store, users_svc  # noqa: E402

GOOD_DEF = json.dumps({"nodes": [
    {"type": "start", "id": "start", "next": "end"},
    {"type": "end", "id": "end", "resultType": "success", "output": "ok"},
]})

EDITOR_GATE_DETAIL = "该操作需要 editor 及以上角色"
ADMIN_GATE_DETAIL = "该操作需要 admin 角色"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ""
    s.allow_insecure_admin = False
    return s


def _build_app(tmp_path, monkeypatch) -> FastAPI:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/rbac214.db")
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())

    flow_store.init_engine(f"sqlite:///{tmp_path}/rbac214.db")
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "admin", "admin-password-1", "admin")
    users_svc.create_user(store, "editor1", "editor-password-1", "editor")
    users_svc.create_user(store, "viewer1", "viewer-password-1", "viewer")
    examples_svc.seed_example_flows(store)

    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    # admin_deps 与 main.create_app 的挂载方式一致：集中 require_auth。
    for router in (
        flows_api.router,
        schedules_api.router,
        dryrun_api.router,
        supervisor_api.router,
        credentials_api.router,
    ):
        app.include_router(router, prefix="/api", dependencies=[Depends(require_auth)])
    return app


@pytest.fixture()
def client_with_users(tmp_path, monkeypatch):
    return TestClient(_build_app(tmp_path, monkeypatch))


def _login(client: TestClient, username: str, password: str) -> dict:
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return r.json()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# 非 admin 前缀的写路由矩阵。viewer 必须在 _authorize 的写方法闸被拒，
# 且 detail 是 editor 闸——证明不是被 admin 前缀闸提前拦下。
VIEWER_WRITE_CASES = [
    pytest.param(
        "PUT", "/api/flows/hello-plaita/versions/1.1.0",
        {"definition": GOOD_DEF, "layout": "", "created_by": "viewer1"},
        id="put-flow-version",
    ),
    pytest.param(
        "POST", "/api/flows/hello-plaita/publish", {"version": "1.1.0"},
        id="publish-flow",
    ),
    pytest.param(
        "DELETE", "/api/flows/hello-plaita", None, id="delete-flow",
    ),
    pytest.param(
        "POST", "/api/schedules",
        {"name": "n", "flow_id": "hello-plaita", "cron": "* * * * *", "enabled": True},
        id="create-schedule",
    ),
    pytest.param(
        "POST", "/api/flows/dry-run",
        {"flowJson": GOOD_DEF, "input": {}}, id="dry-run",
    ),
    pytest.param(
        "POST", "/api/flows/hello-plaita/supervisor/iterate",
        {"dataset": "cases", "proposer": "static"}, id="supervisor-iterate",
    ),
]


@pytest.mark.parametrize("method,path,body", VIEWER_WRITE_CASES)
def test_viewer_write_forbidden_by_editor_gate(client_with_users, method, path, body):
    """viewer 写非 admin 路由 → 403 + editor 闸文案（#214 的回归钉）。"""
    viewer = _login(client_with_users, "viewer1", "viewer-password-1")
    r = client_with_users.request(method, path, headers=_auth(viewer["token"]), json=body)
    assert r.status_code == 403, f"{method} {path} 未被 viewer 写闸拒绝: {r.status_code} {r.text}"
    assert r.json().get("detail") == EDITOR_GATE_DETAIL, (
        f"{method} {path} 命中的不是 editor 写闸: {r.json()!r}"
    )


def test_viewer_read_still_allowed(client_with_users):
    """写闸收紧不能误伤读：viewer GET 仍 200。"""
    viewer = _login(client_with_users, "viewer1", "viewer-password-1")
    r = client_with_users.get("/api/flows", headers=_auth(viewer["token"]))
    assert r.status_code == 200, r.text


def test_editor_write_still_allowed(client_with_users):
    """正控：editor 通过同一写闸（flow 版本保存 → 200）。"""
    editor = _login(client_with_users, "editor1", "editor-password-1")
    r = client_with_users.put(
        "/api/flows/hello-plaita/versions/1.1.0",
        headers=_auth(editor["token"]),
        json={"definition": GOOD_DEF, "layout": "", "created_by": "editor1"},
    )
    assert r.status_code == 200, r.text


def test_admin_prefix_still_uses_admin_gate(client_with_users):
    """对照：admin 前缀路由的错误文案仍是 admin 闸，未被本测试混淆。"""
    viewer = _login(client_with_users, "viewer1", "viewer-password-1")
    r = client_with_users.post(
        "/api/credentials", headers=_auth(viewer["token"]),
        json={"name": "x", "type": "generic", "data": {"a": 1}},
    )
    assert r.status_code == 403, r.text
    assert r.json().get("detail") == ADMIN_GATE_DETAIL
