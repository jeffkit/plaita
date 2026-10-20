"""RBAC / 审计 / 环境晋升 端到端单测。

用裸 FastAPI 应用模拟依赖注入状态（app.state.redis/store/local_mode），
覆盖：登录签发、角色方法矩阵、admin 前缀限制、API key 兼容、
审计落库、晋升包导出/导入/指纹校验、生产环境删除保护。
"""
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
from api import flows as flows_api  # noqa: E402
from api import credentials as credentials_api  # noqa: E402
from api import audit as audit_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import audit as audit_svc  # noqa: E402
from services import deployments as deployments_svc  # noqa: E402
from services import examples as examples_svc  # noqa: E402
from services import flow_store  # noqa: E402
from services import users_svc  # noqa: E402

GOOD_DEF = json.dumps({"nodes": [
    {"type": "start", "id": "start", "next": "end"},
    {"type": "end", "id": "end", "resultType": "success", "output": "ok"},
]})


def _build_app(tmp_path, monkeypatch, env="dev", admin_key: str = "", with_user: bool = True) -> FastAPI:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/rbac.db")
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setattr("config.get_settings", lambda: _settings(env, admin_key))
    if "plaita-console" in str(BACKEND_DIR):
        monkeypatch.setattr("auth.get_settings", lambda: _settings(env, admin_key))
    else:
        monkeypatch.setattr("auth.get_settings", lambda: _settings(env, admin_key))

    flow_store.init_engine(f"sqlite:///{tmp_path}/rbac.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    if with_user:
        users_svc.create_user(
            store, "admin", "admin-password-1", "admin",
            memberships=[{"tenant_id": "default", "role": "admin"}],
        )

    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")

    app.include_router(
        flows_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(
        credentials_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(
        audit_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )

    @app.get("/api/secure", dependencies=[Depends(require_auth)])
    def secure(identity: dict = Depends(require_auth)):
        return identity

    return app


def _settings(env="dev", admin_key=""):
    from config import Settings
    s = Settings()
    s.console_env = env
    s.admin_api_key = admin_key
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def client(tmp_path, monkeypatch):
    app = _build_app(tmp_path, monkeypatch)
    examples_svc.seed_example_flows(flow_store.get_flow_store())
    return TestClient(app)


@pytest.fixture()
def client_with_users(tmp_path, monkeypatch):
    app = _build_app(tmp_path, monkeypatch, with_user=False)
    store = flow_store.get_flow_store()
    users_svc.create_user(
        store, "admin", "admin-password-1", "admin",
        memberships=[{"tenant_id": "default", "role": "admin"}],
    )
    users_svc.create_user(
        store, "editor1", "editor-password-1", "editor",
        memberships=[{"tenant_id": "default", "role": "editor"}],
    )
    users_svc.create_user(
        store, "viewer1", "viewer-password-1", "viewer",
        memberships=[{"tenant_id": "default", "role": "viewer"}],
    )
    examples_svc.seed_example_flows(store)
    return TestClient(app), store


def _login(client, username, password):
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200
    return r.json()


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


# ---- 登录与会话 ----

def test_login_ok_and_me(client_with_users):
    client, _ = client_with_users
    info = _login(client, "editor1", "editor-password-1")
    assert info["role"] == "editor"
    me = client.get("/api/auth/me", headers=_auth(info["token"])).json()
    assert me["actor"] == "editor1" and me["role"] == "editor"
    # #254：普通用户钉在成员租户上，只有平台身份才有无租户的全量视角
    assert me["tenant_id"] == "default" and me["platform_admin"] is False


def test_login_wrong_password(client_with_users):
    client, _ = client_with_users
    r = client.post("/api/auth/login", json={"username": "editor1", "password": "wrong"})
    assert r.status_code == 401


def test_role_change_revokes_old_role(client_with_users):
    client, store = client_with_users
    info = _login(client, "viewer1", "viewer-password-1")
    # #254：权限角色以 tenant_members 为准（users.role 遗留回退已移除）
    users_svc.set_member_role(store, "default", "viewer1", "editor")
    # 安全设计：改角色即撤销旧会话（强制重新登录）
    assert client.get("/api/auth/me", headers=_auth(info["token"])).status_code == 401
    info2 = _login(client, "viewer1", "viewer-password-1")
    assert info2["role"] == "editor"


# ---- 角色方法矩阵 ----

def test_viewer_blocked_from_mutations(client_with_users):
    client, _ = client_with_users
    info = _login(client, "viewer1", "viewer-password-1")
    h = _auth(info["token"])
    assert client.get("/api/flows", headers=h).status_code == 200
    r = client.post("/api/credentials", headers=h,
                    json={"name": "x", "type": "generic", "data": {"a": 1}})
    assert r.status_code == 403  # admin 前缀 + 写方法


def test_editor_can_write_but_not_admin_prefix(client_with_users):
    client, _ = client_with_users
    info = _login(client, "editor1", "editor-password-1")
    h = _auth(info["token"])
    # 保存版本（editor 可）
    r = client.put("/api/flows/hello-plaita/versions/1.1.0", headers=h,
                   json={"definition": GOOD_DEF, "layout": "", "created_by": "editor1"})
    assert r.status_code == 200
    # admin 前缀（editor 不可）
    assert client.get("/api/users", headers=h).status_code == 403
    assert client.get("/api/audit", headers=h).status_code == 403


def test_unauthenticated_rejected(client):
    r = client.get("/api/secure")
    assert r.status_code == 401


def test_api_key_maps_to_admin(tmp_path, monkeypatch):
    app = _build_app(tmp_path, monkeypatch, admin_key="svc-key-1")
    client = TestClient(app)
    r = client.get("/api/secure", headers={"X-Admin-API-Key": "svc-key-1"})
    assert r.status_code == 200
    body = r.json()
    assert body["actor"] == "api-key" and body["role"] == "admin"
    assert body["platform_admin"] is True and body["tenant_id"] is None


def test_prod_delete_requires_admin(tmp_path, monkeypatch):
    app = _build_app(tmp_path, monkeypatch, env="prod")
    examples_svc.seed_example_flows(flow_store.get_flow_store())
    users_svc.create_user(
        flow_store.get_flow_store(), "editor1", "editor-password-1", "editor",
        memberships=[{"tenant_id": "default", "role": "editor"}],
    )
    client = TestClient(app)
    info = _login(client, "editor1", "editor-password-1")
    r = client.delete("/api/flows/http-echo", headers=_auth(info["token"]))
    assert r.status_code == 403
    assert "生产环境" in r.json()["detail"]


# ---- 审计 ----

def test_audit_records_publish_and_credential(client_with_users, tmp_path):
    client, _ = client_with_users
    admin = _login(client, "admin", "admin-password-1")
    h = _auth(admin["token"])

    client.put("/api/flows/hello-plaita/versions/1.1.0", headers=h,
               json={"definition": GOOD_DEF, "layout": "", "created_by": "admin"})
    pr = client.post(f"/api/flows/hello-plaita/publish", headers=h,
                json={"version": "1.1.0"})
    print("PUBLISH-RESP", pr.status_code, pr.text[:200])
    client.post("/api/credentials", headers=h,
                json={"name": "c1", "type": "generic", "data": {"url": "https://x"}})

    logs = client.get("/api/audit", headers=h).json()["logs"]
    actions = [l["action"] for l in logs]
    assert "flow.save_version" in actions
    assert "flow.publish" in actions
    assert "credential.save" in actions
    # 审计不落机密：credential.save 的 detail 只有 type
    cred_log = next(l for l in logs if l["action"] == "credential.save")
    assert "data" not in (cred_log["detail"] or {})


# ---- 环境晋升 ----

def test_promotion_export_import_fingerprint(client_with_users, tmp_path, monkeypatch):
    client, store = client_with_users
    admin = _login(client, "admin", "admin-password-1")
    h = _auth(admin["token"])

    client.put("/api/flows/hello-plaita/versions/2.0.0", headers=h,
               json={"definition": GOOD_DEF, "layout": "", "created_by": "admin"})

    pkg = client.get("/api/flows/hello-plaita/versions/2.0.0/export", headers=h).json()
    assert pkg["kind"] == "plaita-promotion"
    assert pkg["definition_hash"]

    # 篡改定义 → 指纹校验拒绝
    bad = {**pkg, "definition": {"nodes": []}}
    r = client.post("/api/flows/import-version", headers=h, json={"package": bad})
    assert r.status_code == 400
    assert "指纹" in r.json()["detail"]["message"]

    # 原包导入为新版本草稿
    r = client.post("/api/flows/import-version", headers=h,
                    json={"package": pkg, "new_version": "2.0.1"})
    assert r.status_code == 200
    assert r.json()["status"] == "draft"

    # 发布导入版本 → 部署记录（本地模式 publish 跳过引擎同步）
    pr = client.post("/api/flows/hello-plaita/publish", headers=h, json={"version": "2.0.1"})
    assert pr.status_code == 200, pr.text

    deps = client.get("/api/deployments", headers=h).json()["deployments"]
    assert any(d["flow_id"] == "hello-plaita" and d["version"] == "2.0.1" for d in deps)


# ---- A2R25-01/02/03：晋升包往返与导入校验 ----

_UI_DEF = json.dumps(
    {
        "desc": "中文描述：UI 保存形态（indent=2）",
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "resultType": "success", "output": "完成"},
        ],
    },
    ensure_ascii=False,
    indent=2,
)

_ASCII_ESCAPED_DEF = json.dumps(
    {
        "desc": "中文描述：ASCII 转义形态",
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "resultType": "success", "output": "完成"},
        ],
    },
    ensure_ascii=True,
    indent=2,
)


def _promotion_setup(client, headers):
    """建源版本并导出包；返回 (package, flow_id)。"""
    created = client.post("/api/flows", headers=headers, json={"flow_id": "promo-src"})
    assert created.status_code == 200, created.text
    r = client.put(
        "/api/flows/promo-src/versions/1.0.0",
        headers=headers,
        json={"definition": GOOD_DEF, "layout": "", "created_by": "admin"},
    )
    assert r.status_code == 200, r.text
    pkg = client.get(
        "/api/flows/promo-src/versions/1.0.0/export", headers=headers
    ).json()
    return pkg, "promo-src"


@pytest.mark.parametrize("definition", [_UI_DEF, _ASCII_ESCAPED_DEF],
                         ids=["ui-indent2-non-ascii", "ascii-escaped"])
def test_promotion_roundtrip_ui_definition(client_with_users, definition):
    """A2R25-01 主路径：UI 的 JSON.stringify(def, null, 2) 保存后必须可往返。

    旧实现对「原始 definition 字节」算指纹、导入时对重排后的紧凑串重算，
    缩进/非 ASCII 转义都会让正常包 400「指纹校验失败」。这里两种形态都必须
    导入成功（负控：去掉正典序列化即红）。
    """
    client, store = client_with_users
    admin = _login(client, "admin", "admin-password-1")
    h = _auth(admin["token"])

    created = client.post("/api/flows", headers=h, json={"flow_id": "ui-flow"})
    assert created.status_code == 200, created.text
    r = client.put(
        "/api/flows/ui-flow/versions/1.0.0",
        headers=h,
        json={"definition": definition, "layout": "", "created_by": "admin"},
    )
    assert r.status_code == 200, r.text
    pkg = client.get(
        "/api/flows/ui-flow/versions/1.0.0/export", headers=h
    ).json()
    # 新包同时带旧字段与正典字段；旧 console 仍可识别 definition_hash。
    assert pkg["definition_hash"]
    assert pkg["definition_hash_v2"]
    assert pkg["definition_hash_version"] == 2

    r = client.post(
        "/api/flows/import-version",
        headers=h,
        json={"package": pkg, "new_version": "1.0.1"},
    )
    assert r.status_code == 200, r.text
    imported = store.get_version("ui-flow", "1.0.1")
    assert imported is not None
    assert json.loads(imported.definition)["desc"] == json.loads(definition)["desc"]


def test_import_rejects_invalid_version_and_path_traversal(client_with_users):
    """A2R25-03：非 semver / 路径穿越的 version、flow_id 必须在落库前 422。"""
    client, store = client_with_users
    admin = _login(client, "admin", "admin-password-1")
    h = _auth(admin["token"])
    pkg, flow_id = _promotion_setup(client, h)

    for bad_version in ("not-a-version", "1.0.0/../../x", "../escape", ""):
        r = client.post(
            "/api/flows/import-version",
            headers=h,
            json={"package": pkg, "new_version": bad_version},
        )
        assert r.status_code == 422, (bad_version, r.status_code, r.text)
        if bad_version:
            assert "semver" in r.json()["detail"]["message"]
        assert store.get_version(flow_id, bad_version) is None

    for bad_flow_id in ("../escape", "a/b", "a\\b", ".", ".."):
        bad_pkg = {**pkg, "flow_id": bad_flow_id}
        r = client.post(
            "/api/flows/import-version", headers=h, json={"package": bad_pkg}
        )
        assert r.status_code == 422, (bad_flow_id, r.status_code, r.text)
        assert "flow_id" in r.json()["detail"]["message"]
        assert store.get_flow_record(bad_flow_id) is None


def test_import_publish_validates_flow_definition(client_with_users):
    """A2R25-03：指纹正确也不能 publish 一个 schema 非法的 Flow。"""
    client, store = client_with_users
    admin = _login(client, "admin", "admin-password-1")
    h = _auth(admin["token"])
    pkg, flow_id = _promotion_setup(client, h)

    bad_definition = {"nodes": [{"type": "does-not-exist", "id": "x"}]}
    bad_pkg = {
        **pkg,
        "definition": bad_definition,
        # 攻击者可重算无密钥哈希；校验必须落在 Flow schema 上。
        "definition_hash": deployments_svc.definition_hash(
            json.dumps(bad_definition, ensure_ascii=False)
        ),
        "definition_hash_v2": deployments_svc.canonical_definition_hash(
            bad_definition
        ),
    }
    r = client.post(
        "/api/flows/import-version",
        headers=h,
        json={"package": bad_pkg, "new_version": "9.9.9", "publish": True},
    )
    assert r.status_code == 422, r.text
    assert "Flow 校验失败" in r.json()["detail"]["message"]
    # 零落库：未创建版本，更没有 published 行。
    assert store.get_version(flow_id, "9.9.9") is None


def test_import_over_draft_requires_force_and_audits(client_with_users):
    """A2R25-02：草稿覆盖默认 409；force=true 才写，并审计旧/新定义哈希。"""
    client, store = client_with_users
    admin = _login(client, "admin", "admin-password-1")
    h = _auth(admin["token"])
    pkg, flow_id = _promotion_setup(client, h)

    original_layout = json.dumps({"local": True})
    old_definition = json.dumps({"nodes": [
        {"type": "start", "id": "start", "next": "end"},
        {"type": "end", "id": "end", "resultType": "success", "output": "old"},
    ]})
    r = client.put(
        f"/api/flows/{flow_id}/versions/2.0.0",
        headers=h,
        json={"definition": old_definition, "layout": original_layout},
    )
    assert r.status_code == 200, r.text

    r = client.post(
        "/api/flows/import-version",
        headers=h,
        json={"package": pkg, "new_version": "2.0.0"},
    )
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert detail["conflict"] == "draft_exists"
    assert detail["previous_definition_hash"]
    # 未 force：既有草稿逐字节未动。
    kept = store.get_version(flow_id, "2.0.0")
    assert kept is not None
    assert kept.layout == original_layout
    assert kept.definition == old_definition

    r = client.post(
        "/api/flows/import-version",
        headers=h,
        json={"package": pkg, "new_version": "2.0.0", "force": True},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["overwritten"] is True
    assert body["previous_status"] == "draft"
    assert body["previous_definition_hash"]
    assert body["definition_hash"]
    assert body["previous_definition_hash"] != body["definition_hash"]
    assert store.get_version(flow_id, "2.0.0").layout == ""

    logs = client.get("/api/audit", headers=h).json()["logs"]
    row = next(
        log for log in logs
        if log["action"] == "flow.import" and log["resource_id"] == f"{flow_id}@2.0.0"
    )
    assert row["detail"]["force"] is True
    assert row["detail"]["overwritten"] is True
    assert row["detail"]["previous_definition_hash"] == body["previous_definition_hash"]
    assert row["detail"]["definition_hash"] == body["definition_hash"]


def test_import_over_published_is_named_conflict(client_with_users):
    """A2R25-02：已发布版本不可被导入静默覆盖（具名 409）。"""
    client, store = client_with_users
    admin = _login(client, "admin", "admin-password-1")
    h = _auth(admin["token"])
    pkg, flow_id = _promotion_setup(client, h)

    client.put(
        f"/api/flows/{flow_id}/versions/3.0.0",
        headers=h,
        json={"definition": GOOD_DEF, "layout": ""},
    )
    pr = client.post(
        f"/api/flows/{flow_id}/publish", headers=h, json={"version": "3.0.0"}
    )
    assert pr.status_code == 200, pr.text

    r = client.post(
        "/api/flows/import-version",
        headers=h,
        json={"package": pkg, "new_version": "3.0.0", "force": True},
    )
    assert r.status_code == 409, r.text
    assert r.json()["detail"]["conflict"] == "published_immutable"
    assert store.get_version(flow_id, "3.0.0").status == "published"


def test_import_malformed_package_is_named_4xx(client_with_users):
    """A2R25-12：缺字段的畸形包不得 500（旧实现 KeyError → text/plain 500）。"""
    client, _ = client_with_users
    admin = _login(client, "admin", "admin-password-1")
    h = _auth(admin["token"])

    cases = [
        ({"kind": "plaita-promotion"}, 422),
        ({"kind": "plaita-promotion", "flow_id": "x"}, 422),
        ({"kind": "plaita-promotion", "flow_id": "x", "version": "1.0.0"}, 422),
        ({"kind": "not-promotion", "flow_id": "x"}, 400),
    ]
    for package, expected in cases:
        r = client.post("/api/flows/import-version", headers=h,
                        json={"package": package})
        assert r.status_code == expected, (package, r.status_code, r.text)
        assert r.headers["content-type"].startswith("application/json")


# ---- 首次启动向导 ----

def test_setup_wizard_flow(tmp_path, monkeypatch):
    """users 为空 → needs_setup=true → setup 创建 admin 并直接签发会话。"""
    monkeypatch.delenv("PLAITA_CONSOLE_ADMIN_PASSWORD", raising=False)
    app = _build_app(tmp_path, monkeypatch, with_user=False)
    client = TestClient(app)

    assert client.get("/api/auth/setup-status").json()["needs_setup"] is True

    # setup 创建 admin 并直接返回会话
    r = client.post("/api/auth/setup", json={"username": "boss", "password": "init-password-1"})
    assert r.status_code == 200
    body = r.json()
    assert body["username"] == "boss" and body["role"] == "admin"

    # 已初始化后：needs_setup=false，重复 setup 被拒
    assert client.get("/api/auth/setup-status").json()["needs_setup"] is False
    r = client.post("/api/auth/setup", json={"username": "evil", "password": "init-password-1"})
    assert r.status_code == 409

    # 用向导设的密码登录
    r = client.post("/api/auth/login", json={"username": "boss", "password": "init-password-1"})
    assert r.status_code == 200


def test_headless_bootstrap_still_works(tmp_path, monkeypatch):
    """无人值守：设置了 PLAITA_CONSOLE_ADMIN_PASSWORD → 启动即自动建 admin。"""
    monkeypatch.setenv("PLAITA_CONSOLE_ADMIN_PASSWORD", "headless-pw-1")
    app = _build_app(tmp_path, monkeypatch, with_user=False)
    store = flow_store.get_flow_store()
    assert users_svc.ensure_bootstrap_user(store) == "headless-pw-1"
    assert users_svc.login(store, "admin", "headless-pw-1") is not None
    # 已有用户 → 向导关闭
    client = TestClient(app)
    assert client.get("/api/auth/setup-status").json()["needs_setup"] is False


# ---------------------------------------------------------------- plaita#214
# viewer **写**保护的回归（非 admin 前缀路由）
#
# 缺口：现有 `test_viewer_blocked_from_mutations` 只 POST `/api/credentials`，
# 而该前缀在 `_ADMIN_PREFIXES` 里 ⇒ 先命中 **admin 前缀闸**（403 "该操作需要
# admin 角色"）⇒ **从未走到 viewer 写分支**（auth.py 的 `if role == "viewer": raise ...`）。
# 负控实证：把那行 `raise` 改成 `return`，现有 19 条仍全绿、直接探针
# `_authorize(viewer, POST /api/flows/x/versions/1)` 不再抛 ⇒ **鉴权绕过级回归隐形**。
#
# 本组用例的关键不是"返回 403"（任何闸都能给 403），而是**断言我们打中的是哪个闸**：
# 必须拿到 viewer 分支的文案「该操作需要 editor 及以上角色」，
# **不能**是 admin 前缀的「该操作需要 admin 角色」。

#: 从产品源码取 `_ADMIN_PREFIXES`（不硬编码：前缀表变了这里跟着变，
#: 而"本用例的路径必须落在前缀表之外"这个前置条件永远成立）
def _admin_prefixes():
    import auth as _auth_mod
    return tuple(_auth_mod._ADMIN_PREFIXES)


_ADMIN_PREFIXES_OF_TEST = _admin_prefixes()

_VIEWER_WRITE_DETAIL = "该操作需要 editor 及以上角色"
_ADMIN_PREFIX_DETAIL = "该操作需要 admin 角色"


def _mk_request(method: str, path: str):
    """最小 Starlette Request —— 只为 `_authorize` 读 method/url.path。"""
    from starlette.requests import Request

    return Request({"type": "http", "method": method, "path": path,
                    "headers": [], "query_string": b""})


@pytest.mark.parametrize("path", [
    "/api/flows/x/versions/1",      # 非 admin 前缀的写路由（issue 探针路径）
    "/api/flows",                   # 集合写
    "/api/schedules/x",
    "/api/services/x",
])
def test_viewer_write_blocked_by_viewer_branch_not_admin_prefix(path):
    """viewer 的写操作必须被 **viewer 分支** 拒绝（而不是被 admin 前缀闸兜住）。

    两闸都返回 403 ⇒ 只断言状态码无法区分。这里断言**文案**，从而钉死"打中了哪一支"。
    """
    from auth import _authorize
    from fastapi import HTTPException

    req = _mk_request("POST", path)
    assert not any(path.startswith(p) for p in _ADMIN_PREFIXES_OF_TEST), (
        f"{path} 落在 admin 前缀内 ⇒ 本用例测不到 viewer 分支")

    with pytest.raises(HTTPException) as exc:
        _authorize(req, "viewer")
    assert exc.value.status_code == 403
    assert exc.value.detail == _VIEWER_WRITE_DETAIL, (
        f"打中错闸：期望 viewer 分支（{_VIEWER_WRITE_DETAIL}），"
        f"实得 {exc.value.detail!r}。若为 admin 前缀文案，说明该路径落在 _ADMIN_PREFIXES 内，"
        "本用例退化为重复覆盖")


def test_viewer_read_still_allowed():
    """同一路径 GET 仍放行（viewer+ 读）——确认上面的拦截来自**写方法**，不是路径。"""
    from auth import _authorize

    _authorize(_mk_request("GET", "/api/flows/x/versions/1"), "viewer")


def test_editor_write_allowed_on_non_admin_prefix():
    """editor 在非 admin 前缀上可写 —— 证明 viewer 分支只拦 viewer，不误伤 editor。"""
    from auth import _authorize

    _authorize(_mk_request("POST", "/api/flows/x/versions/1"), "editor")


def test_admin_prefix_still_requires_admin():
    """对照：admin 前缀仍由前缀闸拒绝 editor（两闸并存，未互相遮蔽）。"""
    from auth import _authorize
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc:
        _authorize(_mk_request("POST", "/api/credentials"), "editor")
    assert exc.value.detail == _ADMIN_PREFIX_DETAIL
