"""plaita#290：``POST /api/flows/{id}/supervisor/iterate`` 必须按调用方租户校验 flow
归属，回环客户端必须钉在调用方租户。

缺口（基线 ``95fe2ad``，A2 r45 全栈实弹 ``/tmp/a2r45/probe_f1_supervisor_xtenant.py``）：
- 路由只有 editor 闸：无 ``tenant_scope``、无 flow 归属校验、不在 ``_ADMIN_PREFIXES``；
- ``_local_client`` 以平台 admin key 构造回环 ``ConsoleClient`` 且**不带**
  ``X-Tenant-ID`` ⇒ 回环读面是平台全量（``tenant_scope()`` → None）、写面回退 default。

于是任意租户 editor 可借平台身份：跨租户取出 flow 定义并在 console 进程内 dry-run
（响应回传 baseline 版本与分数；flow 不存在 502 ⇒ 存在性 oracle），并向 default
租户同 id 的 flow 追加攻击者候选的 draft 版本。

本文件按 issue 验收口径钉住。挂载与生产一致（``require_auth +
tenant_generation_context``）；**真 Supervisor + 真 ConsoleClient + 真 auth /
flow_store / dry-run**，只把回环 httpx transport 委托给进程内 TestClient；写面
用例给 ``StaticProposer`` 一个候选代替 LLM 产出：

1. 跨租户 / default 租户 / 不存在的 flow 一律 404（与 ``GET /api/flows/{id}``
   同口径且不可区分 ⇒ oracle 关闭），目标 flow 版本谱系不变，回环零请求
   （无 dry-run、无写）；
2. 本租户 flow 照常 200，候选草稿落在本租户，回环**每个**请求都带调用方租户
   ``X-Tenant-ID``；
3. 平台 API-Key 带 ``X-Tenant-ID`` 可跨租户；不带则沿写路径契约落 default；
   会话型平台管理员全量视角（只读）403；
4. viewer 403 / 未认证 401 不回归。

负控：仅还原 ``api/supervisor.py`` 的归属校验与回环租户 ⇒ 第 1–3 组精确红
（复现 200 + 版本 +1 / 502 oracle / 回环缺租户头），第 4 组保持绿。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx
import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


def _find_plaita_ai_src() -> Path | None:
    """在同工作区找 plaita-ai 源码（含 plaita_ai/supervisor.py 的目录）。"""
    for base in (BACKEND_DIR.parent.parent, BACKEND_DIR.parent):
        candidate = base / "plaita-ai"
        if (candidate / "plaita_ai" / "supervisor.py").is_file():
            return candidate
    return None


try:
    import plaita_ai.console_client as plaita_console_client
    import plaita_ai.supervisor as plaita_supervisor
except ImportError:
    _src = _find_plaita_ai_src()
    if _src is None:
        pytest.skip("plaita-ai 源码不在本机工作区，跳过 #290 supervisor 租户面测试",
                    allow_module_level=True)
    sys.path.insert(0, str(_src))
    try:
        import plaita_ai.console_client as plaita_console_client
        import plaita_ai.supervisor as plaita_supervisor
    except ImportError as exc:
        pytest.skip(f"plaita-ai 不可导入（{exc}），跳过 #290 supervisor 租户面测试",
                    allow_module_level=True)

from fakeredis import FakeRedis  # noqa: E402

from api import auth_users  # noqa: E402
from api import dryrun as dryrun_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from api import supervisor as supervisor_api  # noqa: E402
from api import tenants as tenants_api  # noqa: E402
from auth import (  # noqa: E402
    FULL_VIEW_WRITE_REQUIRES_TENANT_DETAIL,
    require_auth,
    tenant_generation_context,
)
from services import flow_store, tenants_svc, users_svc  # noqa: E402

ADMIN_KEY = "issue290-admin-key"
ROOT_PW = "root-password-1"
EDITOR_A_PW = "editor-a-password-1"
VIEWER_A_PW = "viewer-a-password-1"
ADMIN_B_PW = "admin-b-password-1"
TENANT_A = "tenant-a"
TENANT_B = "tenant-b"

SECRET_FLOW = "secret-flow"      # tenant-b 的 flow
PLATFORM_FLOW = "platform-flow"  # default（平台）租户的 flow
OWN_FLOW = "own-flow"            # tenant-a 自己的 flow
ATTACK_DESC = "mutated-by-tenant-a"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


def _echo_def(flow_id: str, desc: str = "echo") -> str:
    return json.dumps({
        "flow_id": flow_id, "desc": desc, "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "output": "$INPUT.q", "resultType": "success"},
        ],
    })


class _LoopbackViaTestClient(httpx.BaseTransport):
    """把回环 httpx 调用委托给进程内 TestClient：真 ASGI app、真 auth / 租户代码。

    每个请求记 ``(method, path, X-Tenant-ID)``——用来断言「跨租户零回环」与
    「本租户回环逐请求带租户头」。
    """

    def __init__(self, client: TestClient, log: list):
        self._client = client
        self.log = log

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        headers = dict(request.headers)
        self.log.append((request.method, request.url.path, headers.get("x-tenant-id")))
        resp = self._client.request(
            request.method, str(request.url), headers=headers, content=request.read(),
        )
        return httpx.Response(resp.status_code, headers=resp.headers, content=resp.content)


class _Env:
    def __init__(self, client: TestClient, loopback_log: list):
        self.client = client
        self.loopback_log = loopback_log

    def login(self, username: str, password: str) -> dict:
        r = self.client.post("/api/auth/login", json={"username": username, "password": password})
        assert r.status_code == 200, (username, r.status_code, r.text)
        return {"Authorization": f"Bearer {r.json()['token']}"}

    @property
    def key(self) -> dict:
        return {"X-Admin-API-Key": ADMIN_KEY}

    def versions(self, flow_id: str, headers: dict) -> set:
        r = self.client.get(f"/api/flows/{flow_id}", headers=headers)
        assert r.status_code == 200, (flow_id, r.status_code, r.text)
        return {(v["version"], v["status"]) for v in r.json()["versions"]}

    def iterate(self, flow_id: str, headers: dict | None, **body):
        payload = {"dataset": "demo.json", "proposer": "static", **body}
        return self.client.post(
            f"/api/flows/{flow_id}/supervisor/iterate", json=payload, headers=headers or {},
        )


def _make_flow(env: _Env, headers: dict, flow_id: str) -> None:
    c = env.client
    r = c.post("/api/flows", json={"flow_id": flow_id, "desc": "x"}, headers=headers)
    assert r.status_code == 200, r.text
    r = c.put(f"/api/flows/{flow_id}/versions/1.0.0",
              json={"definition": _echo_def(flow_id)}, headers=headers)
    assert r.status_code == 200, r.text
    r = c.post(f"/api/flows/{flow_id}/publish", json={"version": "1.0.0"}, headers=headers)
    assert r.status_code == 200, r.text


@pytest.fixture()
def env(tmp_path, monkeypatch) -> _Env:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/issue290.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", _settings)
    monkeypatch.setattr("auth.get_settings", _settings)
    # 回环客户端需要 admin key；proposer env 显式清空，避免吃本机配置
    monkeypatch.setenv("PLAITA_CONSOLE_ADMIN_API_KEY", ADMIN_KEY)
    for name in ("PLAITA_AI_PROPOSER_BASE_URL", "PLAITA_AI_PROPOSER_MODEL",
                 "PLAITA_AI_PROPOSER_API_KEY"):
        monkeypatch.delenv(name, raising=False)

    ds_root = tmp_path / "datasets"
    ds_root.mkdir()
    (ds_root / "demo.json").write_text(json.dumps({
        "cases": [{"id": "c1", "input": {"q": "hello-from-a"}, "expect": {"contains": "hello"}}]
    }), encoding="utf-8")
    monkeypatch.setenv("PLAITA_SUPERVISOR_DATASET_DIR", str(ds_root))

    flow_store.init_engine(f"sqlite:///{tmp_path}/issue290.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")
    tenants_svc.create_tenant(store, TENANT_B, "Tenant B")
    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    users_svc.create_user(store, "editor-a", EDITOR_A_PW, "editor")
    users_svc.add_member(store, TENANT_A, "editor-a", "editor")
    users_svc.create_user(store, "viewer-a", VIEWER_A_PW, "viewer")
    users_svc.add_member(store, TENANT_A, "viewer-a", "viewer")
    users_svc.create_user(store, "admin-b", ADMIN_B_PW, "admin")
    users_svc.add_member(store, TENANT_B, "admin-b", "admin")

    app = FastAPI()
    app.state.store = store
    app.state.local_mode = True
    app.state.redis = FakeRedis()
    app.include_router(auth_users.router, prefix="/api")
    # 与 main.create_app 相同：管理面 router 统一挂 require_auth + 世代绑定
    admin_deps = [Depends(require_auth), Depends(tenant_generation_context)]
    for module in (flows_api, dryrun_api, supervisor_api, tenants_api):
        app.include_router(module.router, prefix="/api", dependencies=admin_deps)
    client = TestClient(app, raise_server_exceptions=False)

    # 真 ConsoleClient，只换 transport（admin key / X-Tenant-ID 由被测代码决定）
    loopback_log: list = []
    real_init = plaita_console_client.ConsoleClient.__init__

    def _patched_init(self, config, transport=None):
        real_init(self, config, transport=_LoopbackViaTestClient(client, loopback_log))

    monkeypatch.setattr(plaita_console_client.ConsoleClient, "__init__", _patched_init)

    env = _Env(client, loopback_log)
    _make_flow(env, env.login("admin-b", ADMIN_B_PW), SECRET_FLOW)
    _make_flow(env, {**env.key, "X-Tenant-ID": "default"}, PLATFORM_FLOW)
    _make_flow(env, env.login("editor-a", EDITOR_A_PW), OWN_FLOW)
    loopback_log.clear()
    return env


@pytest.fixture()
def one_candidate(monkeypatch):
    """给 static proposer 一个候选（代替 LLM 产出）——打开写面（save_version）。"""
    real_static = plaita_supervisor.StaticProposer

    class _OneCandidate(real_static):
        def __init__(self):
            super().__init__(candidates=[{
                "definition": _echo_def("x", desc=ATTACK_DESC),
                "rationale": "attacker candidate",
            }])

    monkeypatch.setattr(plaita_supervisor, "StaticProposer", _OneCandidate)
    return _OneCandidate


# ---------------------------------------------------------------- 1. 跨租户 / default / 不存在 ⇒ 404 + 零回环


def test_cross_tenant_iterate_is_404_and_touches_nothing(env, one_candidate):
    """tenant-a editor 对 tenant-b flow：404、版本不变、无 dry-run / 无写（回环零请求）。"""
    editor_a = env.login("editor-a", EDITOR_A_PW)
    admin_b = env.login("admin-b", ADMIN_B_PW)
    assert env.versions(SECRET_FLOW, admin_b) == {("1.0.0", "published")}

    r = env.iterate(SECRET_FLOW, editor_a)
    assert r.status_code == 404, r.text
    assert "流程不存在" in str(r.json()["detail"])
    assert env.versions(SECRET_FLOW, admin_b) == {("1.0.0", "published")}
    assert env.loopback_log == [], env.loopback_log


def test_default_tenant_flow_cannot_receive_foreign_candidate_draft(env, one_candidate):
    """tenant-a editor 对 default 租户 flow：404；default 版本谱系不变（无攻击者 draft）。"""
    editor_a = env.login("editor-a", EDITOR_A_PW)
    root_default = {**env.key, "X-Tenant-ID": "default"}
    assert env.versions(PLATFORM_FLOW, root_default) == {("1.0.0", "published")}

    r = env.iterate(PLATFORM_FLOW, editor_a)
    assert r.status_code == 404, r.text
    assert env.versions(PLATFORM_FLOW, root_default) == {("1.0.0", "published")}
    assert env.loopback_log == [], env.loopback_log


def test_nonexistent_flow_is_404_indistinguishable_from_cross_tenant(env):
    """存在性 oracle 关闭：不存在 / 他租户存在，状态码与 detail 完全一致（不再 502）。"""
    editor_a = env.login("editor-a", EDITOR_A_PW)
    missing = env.iterate("no-such-flow", editor_a)
    foreign = env.iterate(SECRET_FLOW, editor_a)
    assert missing.status_code == foreign.status_code == 404, (missing.text, foreign.text)

    def _norm(resp, flow_id):
        return str(resp.json()["detail"]).replace(flow_id, "<id>")

    assert _norm(missing, "no-such-flow") == _norm(foreign, SECRET_FLOW)
    assert env.loopback_log == []


# ---------------------------------------------------------------- 2. 本租户正控


def test_same_tenant_iterate_succeeds_and_stays_in_tenant(env, one_candidate):
    """tenant-a editor 对本租户 flow：200、候选草稿落 tenant-a、回环逐请求带 X-Tenant-ID。"""
    editor_a = env.login("editor-a", EDITOR_A_PW)
    r = env.iterate(OWN_FLOW, editor_a)
    assert r.status_code == 200, r.text
    it0 = r.json()["iterations"][0]
    assert it0["baseline_version"] == "1.0.0"
    assert it0["candidate_version"] == "1.0.1"
    assert it0.get("promoted") is not True  # promote_gate=manual：发布永远由人

    # 候选落在本租户（tenant-a 视角可见、定义就是候选内容）
    assert env.versions(OWN_FLOW, editor_a) == {("1.0.0", "published"), ("1.0.1", "draft")}
    v = env.client.get(f"/api/flows/{OWN_FLOW}/versions/1.0.1", headers=editor_a)
    assert v.status_code == 200, v.text
    assert json.loads(v.json()["definition"])["desc"] == ATTACK_DESC
    # default 租户没有被凭空写出同名 flow
    r = env.client.get(f"/api/flows/{OWN_FLOW}", headers={**env.key, "X-Tenant-ID": "default"})
    assert r.status_code == 404, r.text

    # 回环：get_flow / dry-run / save_version 三类请求都发生了，且每个都钉在调用方租户
    paths = {path for _, path, _ in env.loopback_log}
    assert f"/api/flows/{OWN_FLOW}" in paths
    assert "/api/flows/dry-run" in paths
    assert f"/api/flows/{OWN_FLOW}/versions/1.0.1" in paths
    assert all(tenant == TENANT_A for _, _, tenant in env.loopback_log), env.loopback_log


# ---------------------------------------------------------------- 3. 平台身份


def test_platform_key_with_tenant_header_can_iterate_that_tenant(env):
    """平台 API-Key + X-Tenant-ID=tenant-b：可对 tenant-b flow 迭代（基线评测），回环钉 tenant-b。"""
    r = env.iterate(SECRET_FLOW, {**env.key, "X-Tenant-ID": TENANT_B})
    assert r.status_code == 200, r.text
    it0 = r.json()["iterations"][0]
    assert it0["baseline_version"] == "1.0.0"
    assert it0["status"] == "no_proposal"  # static 无候选 ⇒ 只跑基线
    assert env.loopback_log, "基线评测必须经回环发生"
    assert all(tenant == TENANT_B for _, _, tenant in env.loopback_log), env.loopback_log


def test_platform_key_without_tenant_header_scopes_to_default(env):
    """无状态平台凭证不带租户头：沿写路径契约落 default——default 的 flow 可迭代，他租户 404。"""
    r = env.iterate(PLATFORM_FLOW, env.key)
    assert r.status_code == 200, r.text
    assert env.loopback_log, "基线评测必须经回环发生"
    assert all(tenant == "default" for _, _, tenant in env.loopback_log), env.loopback_log

    env.loopback_log.clear()
    r = env.iterate(SECRET_FLOW, env.key)
    assert r.status_code == 404, r.text
    assert env.loopback_log == []


def test_platform_admin_session_full_view_is_read_only(env):
    """会话型平台管理员全量视角（switch-tenant null）：iterate 是写路径 ⇒ 403，零回环。"""
    root = env.login("root", ROOT_PW)
    r = env.client.post("/api/auth/switch-tenant", json={"tenant_id": None}, headers=root)
    assert r.status_code == 200, r.text

    r = env.iterate(SECRET_FLOW, root)
    assert r.status_code == 403, r.text
    assert r.json()["detail"] == FULL_VIEW_WRITE_REQUIRES_TENANT_DETAIL
    assert env.loopback_log == []


# ---------------------------------------------------------------- 4. 角色闸不回归


def test_viewer_and_anonymous_are_still_rejected(env):
    viewer_a = env.login("viewer-a", VIEWER_A_PW)
    r = env.iterate(OWN_FLOW, viewer_a)
    assert r.status_code == 403, r.text
    assert "editor" in str(r.json()["detail"])
    r = env.iterate(OWN_FLOW, None)
    assert r.status_code == 401, r.text
    assert env.loopback_log == []
