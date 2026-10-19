"""plaita#260：租户 admin 不是平台身份——全局账号/全局服务生命周期收 platform_admin。

缺口（基线 ``1818a3d``）：
- ``_ADMIN_PREFIXES`` 含 ``/api/users``，但只按**租户角色** ``role == "admin"``
  放行；租户 admin 的 role 恰好是 admin ⇒ 过闸。
- ``POST /api/users/{u}/password`` / ``DELETE /api/users/{u}`` 只挂
  ``require_auth``，svc 按用户名全局操作 ⇒ 租户 admin 重置 root 密码后
  以 ``platform_admin=True`` 登录，或直接删掉平台账号。
- ``POST /api/services/{id}/stop``（A42-02）不在 admin 前缀，租户 editor
  即可停任意全局服务实例。

本文件钉住四件事：
1. 负向：租户 admin 的全局账号变更（改密/删号/建号/改角色）一律 403
   具名拒绝，且拒绝尝试进审计（``*.denied`` + ``denied=True``）。
2. 正向：平台管理员管理用户、租户 admin 管理**本租户**成员、任意角色
   自助改密三条合法流仍可用。
3. 读面：租户 admin 的 ``GET /api/users`` 只返回本租户成员（平台管理员
   仍是全量，行为不变）。
4. A42-02：全局服务 stop 只允许平台管理员，拒绝时 Redis 零副作用。

负控（验收时手工执行）：仅还原 ``auth.ensure_platform_admin`` 的拒绝分支
（保留本文件）⇒ 改密/删号/建号+改角色/停服务四条负向用例精确红
（复现 200 接管与 200 停服），其余正向/读面/成员管理用例保持绿。
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
from api import services as services_api  # noqa: E402
from api import tenants as tenants_api  # noqa: E402
from auth import PLATFORM_ADMIN_REQUIRED_DETAIL, require_auth  # noqa: E402
from services import audit as audit_svc  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

ROOT_PW = "root-password-1"
TADMIN_PW = "tadmin-password-1"
EDITOR_PW = "editor-password-1"
VIEWER_PW = "viewer-password-1"
OUTSIDER_PW = "outsider-password-1"

TENANT_A = "tenant-a"
TENANT_B = "tenant-b"

TENANT_MEMBER_ADMIN_DETAIL = "仅平台管理员或本租户管理员可管理租户成员"
PLATFORM_ACCOUNT_PROTECTED_DETAIL = "仅平台管理员可操作平台管理员账号"


class FakeRedis:
    """最小 Redis 替身：只实现 stop 路径用到的 keys/publish。"""

    def __init__(self, keys=None):
        self._keys = list(keys or [])
        self.published = []

    def keys(self, pattern):  # noqa: ARG002 - 测试只关心是否发布 stop
        return list(self._keys)

    def publish(self, channel, message):
        self.published.append((channel, message))
        return 1


def _settings(admin_key: str = ""):
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = admin_key
    s.allow_insecure_admin = False
    return s


def _build_app(tmp_path, monkeypatch, admin_key: str = "") -> FastAPI:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/issue260.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", lambda: _settings(admin_key))
    monkeypatch.setattr("auth.get_settings", lambda: _settings(admin_key))

    flow_store.init_engine(f"sqlite:///{tmp_path}/issue260.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")
    tenants_svc.create_tenant(store, TENANT_B, "Tenant B")

    # root：平台管理员，只在 default 租户
    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    # tadmin：tenant-a 的 admin，platform_admin=False —— 攻击者身份
    users_svc.create_user(store, "tadmin", TADMIN_PW, "admin")
    users_svc.add_member(store, TENANT_A, "tadmin", "admin")
    # tenant-a 的其他成员（用来验证租户 admin 的读面与成员管理正控）
    users_svc.create_user(store, "editor1", EDITOR_PW, "editor")
    users_svc.add_member(store, TENANT_A, "editor1", "editor")
    users_svc.create_user(store, "member1", VIEWER_PW, "viewer")
    users_svc.add_member(store, TENANT_A, "member1", "viewer")
    # 另一个租户的成员：租户 admin 不得触达
    users_svc.create_user(store, "outsider", OUTSIDER_PW, "viewer")
    users_svc.add_member(store, TENANT_B, "outsider", "viewer")

    app = FastAPI()
    app.state.store = store
    app.state.local_mode = True
    app.state.redis = FakeRedis(
        keys=[b"plaita:registry:flow_worker:inst-global"],
    )
    app.include_router(auth_users.router, prefix="/api")
    # 与 main.create_app 相同：管理面 router 统一挂 require_auth
    for router in (tenants_api.router, services_api.router):
        app.include_router(router, prefix="/api", dependencies=[Depends(require_auth)])
    return app


@pytest.fixture()
def client(tmp_path, monkeypatch) -> TestClient:
    return TestClient(_build_app(tmp_path, monkeypatch))


def _login(client: TestClient, username: str, password: str) -> str:
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return r.json()["token"]


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _denied_audits(action: str):
    return audit_svc.list_audit(action=action, limit=500)


def _assert_denied(resp, detail: str) -> None:
    assert resp.status_code == 403, resp.text
    assert resp.json().get("detail") == detail, resp.json()


# ---------------------------------------------------------------- 负向：全局账号


def test_tenant_admin_cannot_reset_platform_admin_password(client):
    """租户 admin 重置 root 密码 → 403；root 原密码可登录、旧会话不被吊销。

    负控：还原 ``auth.ensure_platform_admin`` 拒绝分支后本条红——
    pre-fix 返回 200，随后 root 用新密码登录得到 ``platform_admin=True``。
    """
    root_token = _login(client, "root", ROOT_PW)
    tadmin = _auth(_login(client, "tadmin", TADMIN_PW))

    r = client.post(
        "/api/users/root/password", json={"password": "owned-root-pw-1"},
        headers=tadmin,
    )
    _assert_denied(r, PLATFORM_ADMIN_REQUIRED_DETAIL)

    # 原密码仍可登录、仍是平台管理员（密码未被改写）
    root_login = client.post(
        "/api/auth/login", json={"username": "root", "password": ROOT_PW}
    )
    assert root_login.status_code == 200, root_login.text
    assert root_login.json()["platform_admin"] is True
    # 旧会话未被吊销（set_password 的 _revoke_user_sessions 未触发）
    assert client.get("/api/auth/me", headers=_auth(root_token)).status_code == 200
    # 接管密码不可用
    assert client.post(
        "/api/auth/login", json={"username": "root", "password": "owned-root-pw-1"}
    ).status_code == 401

    denied = _denied_audits("user.set_password.denied")
    assert any(
        row["actor"] == "tadmin" and row["resource_id"] == "root"
        and row["detail"].get("denied") is True
        for row in denied
    ), denied


def test_tenant_admin_cannot_delete_platform_admin(client):
    """租户 admin 删除平台账号 → 403；账号仍在。

    目标用**非末位**平台管理员 root2：负控只还原平台身份闸时，
    pre-fix 会 200 删掉（可精确复现接管），不会被「最后一个平台管理员」
    兜底保护混淆。
    """
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "root2", "root2-password-1", "admin",
                          platform_admin=True)
    users_svc.add_member(store, "default", "root2", "admin")

    tadmin = _auth(_login(client, "tadmin", TADMIN_PW))
    r = client.delete("/api/users/root2", headers=tadmin)
    _assert_denied(r, PLATFORM_ADMIN_REQUIRED_DETAIL)

    assert client.post(
        "/api/auth/login", json={"username": "root2", "password": "root2-password-1"}
    ).status_code == 200
    denied = _denied_audits("user.delete.denied")
    assert any(
        row["actor"] == "tadmin" and row["resource_id"] == "root2"
        and row["detail"].get("denied") is True
        for row in denied
    ), denied


def test_tenant_admin_cannot_create_user_or_change_global_role(client):
    """租户 admin 的创建/改角色同属全局账号操作 → 403 + 审计。"""
    tadmin = _auth(_login(client, "tadmin", TADMIN_PW))

    r = client.post(
        "/api/users",
        json={"username": "planted", "password": "planted-pw-1", "role": "admin"},
        headers=tadmin,
    )
    _assert_denied(r, PLATFORM_ADMIN_REQUIRED_DETAIL)

    r = client.post("/api/users/root/role", json={"role": "viewer"}, headers=tadmin)
    _assert_denied(r, PLATFORM_ADMIN_REQUIRED_DETAIL)

    # 两次拒绝都进审计（denied=True）
    for action in ("user.create.denied", "user.set_role.denied"):
        rows = _denied_audits(action)
        assert any(row["actor"] == "tadmin" and row["detail"].get("denied") is True
                   for row in rows), (action, rows)

    # root 的遗留全局角色未被改写
    users = {u["username"]: u for u in users_svc.list_users(flow_store.get_flow_store())}
    assert users["root"]["role"] == "admin"
    assert "planted" not in users


def test_tenant_admin_user_list_is_scoped_to_own_tenant(client):
    """读面收敛：租户 admin 只看到本租户成员，不能枚举平台账号。"""
    tadmin = _auth(_login(client, "tadmin", TADMIN_PW))
    r = client.get("/api/users", headers=tadmin)
    assert r.status_code == 200, r.text
    usernames = {u["username"] for u in r.json()["users"]}
    assert usernames == {"tadmin", "editor1", "member1"}, usernames
    assert "root" not in usernames and "outsider" not in usernames


# ---------------------------------------------------------------- 正向：合法流


def test_platform_admin_still_manages_users(client):
    """平台管理员：列表全量、创建、改角色、改密、删除（含非末位平台管理员）。"""
    root = _auth(_login(client, "root", ROOT_PW))

    listing = client.get("/api/users", headers=root)
    assert listing.status_code == 200, listing.text
    assert {"root", "tadmin", "outsider"} <= {
        u["username"] for u in listing.json()["users"]
    }

    r = client.post(
        "/api/users",
        json={"username": "alice", "password": "alice-pw-1", "role": "viewer",
              "memberships": [{"tenant_id": TENANT_A, "role": "editor"}]},
        headers=root,
    )
    assert r.status_code == 200, r.text

    assert client.post("/api/users/alice/role", json={"role": "editor"},
                       headers=root).status_code == 200
    assert client.post("/api/users/alice/password", json={"password": "alice-pw-2"},
                       headers=root).status_code == 200
    assert client.post("/api/auth/login",
                       json={"username": "alice", "password": "alice-pw-2"}
                       ).status_code == 200
    assert client.delete("/api/users/alice", headers=root).status_code == 200

    # 非末位平台管理员可删除；最后一个平台管理员有保护
    assert client.post(
        "/api/users",
        json={"username": "root2", "password": "root2-pw-1", "role": "admin",
              "platform_admin": True},
        headers=root,
    ).status_code == 200
    assert client.delete("/api/users/root2", headers=root).status_code == 200
    r = client.delete("/api/users/root", headers=root)
    assert r.status_code == 400, r.text
    assert "最后一个平台管理员" in r.json()["detail"]


def test_api_key_still_manages_users(tmp_path, monkeypatch):
    """正控：平台级 API-Key（platform_admin=True）不受影响。"""
    client = TestClient(_build_app(tmp_path, monkeypatch, admin_key="svc-key-1"))
    headers = {"X-Admin-API-Key": "svc-key-1"}

    r = client.post(
        "/api/users",
        json={"username": "svc-user", "password": "svc-user-pw-1", "role": "viewer"},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    assert client.get("/api/users", headers=headers).status_code == 200
    assert client.delete("/api/users/svc-user", headers=headers).status_code == 200


@pytest.mark.parametrize("username,password", [("editor1", EDITOR_PW),
                                               ("member1", VIEWER_PW)])
def test_any_role_can_change_own_password(client, username, password):
    """自助改密对 editor/viewer 也成立（/api/users/{self}/password）。"""
    token = _auth(_login(client, username, password))
    new_pw = f"{username}-new-pw-1"
    r = client.post(f"/api/users/{username}/password",
                    json={"password": new_pw}, headers=token)
    assert r.status_code == 200, r.text

    # 改密会吊销旧会话，须用新密码重新登录
    assert client.get("/api/auth/me", headers=token).status_code == 401
    assert client.post(
        "/api/auth/login", json={"username": username, "password": new_pw}
    ).status_code == 200


def test_tenant_admin_can_change_own_password(client):
    token = _auth(_login(client, "tadmin", TADMIN_PW))
    r = client.post("/api/users/tadmin/password",
                    json={"password": "tadmin-new-pw-1"}, headers=token)
    assert r.status_code == 200, r.text


def test_non_admin_cannot_change_other_users_password(client):
    """editor 改别人密码仍被 admin 前缀闸拒绝（不是自助路径）。"""
    editor = _auth(_login(client, "editor1", EDITOR_PW))
    r = client.post("/api/users/member1/password",
                    json={"password": "hijacked-pw-1"}, headers=editor)
    assert r.status_code == 403, r.text
    assert r.json()["detail"] == "该操作需要 admin 角色"


def test_tenant_admin_manages_own_tenant_members(client):
    """租户 admin 经 /api/tenants/{tid}/members/* 管理本租户成员（正控）。"""
    tadmin = _auth(_login(client, "tadmin", TADMIN_PW))
    store = flow_store.get_flow_store()
    users_svc.create_user(store, "newbie", "newbie-pw-1", "viewer")

    r = client.get(f"/api/tenants/{TENANT_A}/members", headers=tadmin)
    assert r.status_code == 200, r.text
    assert {m["username"] for m in r.json()["members"]} >= {"tadmin", "editor1"}

    assert client.post(
        f"/api/tenants/{TENANT_A}/members",
        json={"username": "newbie", "role": "viewer"}, headers=tadmin,
    ).status_code == 200
    assert client.put(
        f"/api/tenants/{TENANT_A}/members/newbie",
        json={"username": "newbie", "role": "editor"}, headers=tadmin,
    ).status_code == 200
    assert client.delete(
        f"/api/tenants/{TENANT_A}/members/newbie", headers=tadmin
    ).status_code == 200

    # 平台管理员对成员管理的正控不受影响
    root = _auth(_login(client, "root", ROOT_PW))
    assert client.get(f"/api/tenants/{TENANT_A}/members",
                      headers=root).status_code == 200


def test_tenant_admin_cannot_manage_other_tenant_members(client):
    """跨租户成员管理：即使是 admin 角色也 403（本租户限定）+ 审计。"""
    tadmin = _auth(_login(client, "tadmin", TADMIN_PW))
    r = client.get(f"/api/tenants/{TENANT_B}/members", headers=tadmin)
    _assert_denied(r, TENANT_MEMBER_ADMIN_DETAIL)

    r = client.post(
        f"/api/tenants/{TENANT_B}/members",
        json={"username": "tadmin", "role": "admin"}, headers=tadmin,
    )
    _assert_denied(r, TENANT_MEMBER_ADMIN_DETAIL)

    rows = _denied_audits("tenant.list_members.denied")
    assert any(row["actor"] == "tadmin" and row["detail"].get("denied") is True
               for row in rows), rows


def test_tenant_admin_cannot_touch_platform_account_via_members(client):
    """租户 admin 不得把平台账号加入/移出自己的租户（防接管旁路）。"""
    tadmin = _auth(_login(client, "tadmin", TADMIN_PW))

    r = client.post(
        f"/api/tenants/{TENANT_A}/members",
        json={"username": "root", "role": "viewer"}, headers=tadmin,
    )
    _assert_denied(r, PLATFORM_ACCOUNT_PROTECTED_DETAIL)

    r = client.delete(f"/api/tenants/{TENANT_A}/members/root", headers=tadmin)
    _assert_denied(r, PLATFORM_ACCOUNT_PROTECTED_DETAIL)

    rows = _denied_audits("tenant.add_member.denied")
    assert any(row["actor"] == "tadmin" and row["detail"].get("denied") is True
               for row in rows), rows


# ---------------------------------------------------------------- A42-02


def test_tenant_editor_and_admin_cannot_stop_global_service(client):
    """A42-02：租户 editor（及租户 admin）停全局服务 → 403，Redis 零发布。"""
    for username, password in (("editor1", EDITOR_PW), ("tadmin", TADMIN_PW)):
        headers = _auth(_login(client, username, password))
        r = client.post("/api/services/inst-global/stop",
                        json={"graceful": False}, headers=headers)
        _assert_denied(r, PLATFORM_ADMIN_REQUIRED_DETAIL)

    assert client.get("/api/auth/me", headers=headers).status_code == 200
    assert client.app.state.redis.published == []

    rows = _denied_audits("service.stop.denied")
    assert {row["actor"] for row in rows} >= {"editor1", "tadmin"}, rows
    assert all(row["detail"].get("denied") is True for row in rows)


def test_platform_admin_can_stop_global_service(client):
    """正控：平台管理员 stop 仍发布控制指令（行为不变）。"""
    root = _auth(_login(client, "root", ROOT_PW))
    r = client.post("/api/services/inst-global/stop",
                    json={"graceful": False}, headers=root)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "sent"

    published = client.app.state.redis.published
    assert len(published) == 1
    channel, message = published[0]
    assert channel == "plaita:control:inst-global"
    assert '"command": "stop"' in message
    assert '"graceful": false' in message
