"""plaita#291：supervisor 回环目标不得取自请求 ``Host`` 头。

缺口（基线 ``74616b8``，A1 r110 实弹 ``/tmp/r110_host_loopback_probe.py``）：
``_local_client`` 用 ``request.base_url``（ASGI scheme + 请求 ``Host``）当 console
自身的回环地址，且 app 上没有 ``TrustedHostMiddleware``——任意租户 editor 带
``Host: 127.0.0.1:<攻击者端口>`` 调**自有 flow** 的 iterate，console 就以平台
admin key（``X-Admin-API-Key``）向攻击者监听器发出 ``GET /api/flows/{id}``。
#290 的「回环钉租户头」修法与之正交：key 照送，只是多了 ``X-Tenant-ID``。

修法：回环目标钉**固定内部源**——由 console 自身监听配置推导
（``PLAITA_CONSOLE_HOST`` 通配则取回环地址，否则取绑定地址；端口取
``PLAITA_CONSOLE_PORT``），或显式 ``PLAITA_CONSOLE_LOOPBACK_URL``；``_local_client``
不再接触 ``request``，结构上无法回退到 Host 头。

本文件**双监听器实弹**：真 TCP、真 ``ConsoleClient``（不换 transport）、真 editor
会话 + 本租户 flow——
- 攻击者监听器 **A**：其端口写进请求 ``Host``；
- 自身监听器 **S**：扮演 console 的固定内部源（``settings.port == S``）。

断言：A **零请求**；回环唯一落点是 S，且回环请求自带的 ``Host`` 头 ==
``127.0.0.1:S``（与入站 Host 无关）——这就是「admin key 回环请求的 Host 一致性
断言」；回环携带 admin key + 调用方租户头（#290 不回归）。

负控：仅还原 ``base_url=str(request.base_url)`` ⇒ 实弹组精确红（A 收到 admin
key / S 零请求）、结构组红（``_local_client`` 重新持有 ``request``）；正控组
（transport 委托 TestClient 的 200）在两种实现下都绿，证明修法不伤正常路径。
"""
from __future__ import annotations

import inspect
import json
import logging
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


def _find_plaita_ai_src() -> Path | None:
    for base in (BACKEND_DIR.parent.parent, BACKEND_DIR.parent):
        candidate = base / "plaita-ai"
        if (candidate / "plaita_ai" / "supervisor.py").is_file():
            return candidate
    return None


try:
    import plaita_ai.console_client as plaita_console_client
except ImportError:
    _src = _find_plaita_ai_src()
    if _src is None:
        pytest.skip("plaita-ai 源码不在本机工作区，跳过 #291 supervisor 回环目标测试",
                    allow_module_level=True)
    sys.path.insert(0, str(_src))
    try:
        import plaita_ai.console_client as plaita_console_client
    except ImportError as exc:
        pytest.skip(f"plaita-ai 不可导入（{exc}），跳过 #291 supervisor 回环目标测试",
                    allow_module_level=True)

from fakeredis import FakeRedis  # noqa: E402

from api import auth_users  # noqa: E402
from api import dryrun as dryrun_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from api import supervisor as supervisor_api  # noqa: E402
from auth import require_auth, tenant_generation_context  # noqa: E402
from config import Settings  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

ADMIN_KEY = "issue291-admin-key-SECRET"
EDITOR_A_PW = "editor-a-password-1"
TENANT_A = "tenant-a"
OWN_FLOW = "own-flow"


class _Listener:
    """本机 HTTP 监听器：记录每个请求的落点特征，一律回 404 JSON。"""

    def __init__(self) -> None:
        self.captured: list[dict] = []
        captured = self.captured

        class _Handler(BaseHTTPRequestHandler):
            def _record(self) -> None:
                captured.append({
                    "method": self.command,
                    "path": self.path,
                    "host": self.headers.get("Host"),
                    "admin_key": self.headers.get("X-Admin-API-Key"),
                    "tenant": self.headers.get("X-Tenant-ID"),
                })
                body = b'{"detail":"listener"}'
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = do_POST = do_PUT = do_DELETE = _record

            def log_message(self, *args) -> None:  # noqa: D401
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.port: int = self._server.server_address[1]
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    @property
    def netloc(self) -> str:
        return f"127.0.0.1:{self.port}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def _echo_def(flow_id: str) -> str:
    return json.dumps({
        "flow_id": flow_id, "desc": "echo", "inputType": {"dataType": "object"},
        "nodes": [
            {"type": "start", "id": "start", "next": "end"},
            {"type": "end", "id": "end", "output": "$INPUT.q", "resultType": "success"},
        ],
    })


class _Env:
    def __init__(self, client: TestClient, attacker: _Listener, selfsrc: _Listener,
                 settings: Settings):
        self.client = client
        self.attacker = attacker
        self.selfsrc = selfsrc
        self.settings = settings

    def login(self, username: str, password: str) -> dict:
        r = self.client.post("/api/auth/login", json={"username": username, "password": password})
        assert r.status_code == 200, (username, r.status_code, r.text)
        return {"Authorization": f"Bearer {r.json()['token']}"}

    def iterate(self, headers: dict, host: str | None = None, flow_id: str = OWN_FLOW):
        hdrs = dict(headers)
        if host is not None:
            hdrs["Host"] = host
        return self.client.post(
            f"/api/flows/{flow_id}/supervisor/iterate",
            json={"dataset": "demo.json", "proposer": "static", "max_iterations": 1},
            headers=hdrs,
        )


@pytest.fixture()
def env(tmp_path, monkeypatch) -> _Env:
    attacker = _Listener()
    selfsrc = _Listener()

    # console 自身监听配置：模拟 docker/e2e 的对外监听 0.0.0.0——派生回环必须
    # 落到 127.0.0.1:{port}，端口取自 settings（即 PLAITA_CONSOLE_PORT）。
    settings = Settings()
    settings.console_env = "dev"
    settings.admin_api_key = ADMIN_KEY
    settings.allow_insecure_admin = False
    settings.host = "0.0.0.0"
    settings.port = selfsrc.port
    monkeypatch.setattr("config.get_settings", lambda: settings)
    monkeypatch.setattr("auth.get_settings", lambda: settings)
    monkeypatch.setenv("PLAITA_CONSOLE_ADMIN_API_KEY", ADMIN_KEY)
    monkeypatch.delenv("PLAITA_CONSOLE_LOOPBACK_URL", raising=False)
    for name in ("PLAITA_AI_PROPOSER_BASE_URL", "PLAITA_AI_PROPOSER_MODEL",
                 "PLAITA_AI_PROPOSER_API_KEY"):
        monkeypatch.delenv(name, raising=False)

    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/issue291.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)

    ds_root = tmp_path / "datasets"
    ds_root.mkdir()
    (ds_root / "demo.json").write_text(json.dumps({
        "cases": [{"id": "c1", "input": {"q": "hello-from-a"}, "expect": {"contains": "hello"}}]
    }), encoding="utf-8")
    monkeypatch.setenv("PLAITA_SUPERVISOR_DATASET_DIR", str(ds_root))

    flow_store.init_engine(f"sqlite:///{tmp_path}/issue291.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")
    users_svc.create_user(store, "editor-a", EDITOR_A_PW, "editor")
    users_svc.add_member(store, TENANT_A, "editor-a", "editor")

    app = FastAPI()
    app.state.store = store
    app.state.local_mode = True
    app.state.redis = FakeRedis()
    app.include_router(auth_users.router, prefix="/api")
    admin_deps = [Depends(require_auth), Depends(tenant_generation_context)]
    for module in (flows_api, dryrun_api, supervisor_api):
        app.include_router(module.router, prefix="/api", dependencies=admin_deps)
    client = TestClient(app, raise_server_exceptions=False)

    env = _Env(client, attacker, selfsrc, settings)
    editor_a = env.login("editor-a", EDITOR_A_PW)
    r = client.post("/api/flows", json={"flow_id": OWN_FLOW, "desc": "x"}, headers=editor_a)
    assert r.status_code == 200, r.text
    r = client.put(f"/api/flows/{OWN_FLOW}/versions/1.0.0",
                   json={"definition": _echo_def(OWN_FLOW)}, headers=editor_a)
    assert r.status_code == 200, r.text
    r = client.post(f"/api/flows/{OWN_FLOW}/publish", json={"version": "1.0.0"}, headers=editor_a)
    assert r.status_code == 200, r.text
    attacker.captured.clear()
    selfsrc.captured.clear()
    yield env
    attacker.close()
    selfsrc.close()


# ---------------------------------------------------------------- 1. 实弹：攻击者 Host 零字节、回环落固定源


@pytest.mark.parametrize("host_template", ["127.0.0.1:{port}", "localhost:{port}"])
def test_attacker_host_header_gets_zero_bytes_and_loopback_lands_on_fixed_source(env, host_template):
    """editor 带攻击者 Host 调自有 flow iterate：A 零请求；唯一落点 S，Host 头 == S。"""
    editor_a = env.login("editor-a", EDITOR_A_PW)
    attacker_host = host_template.format(port=env.attacker.port)

    r = env.iterate(editor_a, host=attacker_host)

    # 攻击者监听器零字节：没有任何请求、更没有 admin key
    assert env.attacker.captured == [], env.attacker.captured
    # 回环真的发生，且唯一落点是固定内部源 S（S 回 404 ⇒ 迭代失败；A2R45-02
    # 起 run_loop 兜底：会话不崩，200 + budget_exhausted + failed_iteration）
    body = r.json()
    assert r.status_code == 200 and body["final_status"] == "budget_exhausted", r.text
    it0 = body["iterations"][0]
    assert it0["status"] == "failed_iteration" and "ConsoleClientError" in it0["reason"], it0
    assert env.selfsrc.captured, "回环必须落到固定内部源"
    first = env.selfsrc.captured[0]
    assert first["method"] == "GET" and first["path"] == f"/api/flows/{OWN_FLOW}", first
    # Host 一致性：回环请求自带 Host == 固定源 netloc，与入站 Host 无关
    assert first["host"] == env.selfsrc.netloc, (first, attacker_host)
    assert all(c["host"] == env.selfsrc.netloc for c in env.selfsrc.captured), env.selfsrc.captured
    # #290 不回归：平台 key + 调用方租户头仍在
    assert first["admin_key"] == ADMIN_KEY
    assert first["tenant"] == TENANT_A


def test_loopback_url_override_takes_precedence_over_derived(env, monkeypatch):
    """显式 PLAITA_CONSOLE_LOOPBACK_URL 优先于监听配置推导——仍与请求 Host 无关。"""
    override = _Listener()
    try:
        monkeypatch.setenv("PLAITA_CONSOLE_LOOPBACK_URL", f"http://{override.netloc}/")
        editor_a = env.login("editor-a", EDITOR_A_PW)
        r = env.iterate(editor_a, host=f"127.0.0.1:{env.attacker.port}")
        body = r.json()
        assert r.status_code == 200 and body["final_status"] == "budget_exhausted", r.text
        assert env.attacker.captured == []
        assert env.selfsrc.captured == []
        assert override.captured and override.captured[0]["host"] == override.netloc, override.captured
        assert override.captured[0]["admin_key"] == ADMIN_KEY
    finally:
        override.close()


# ---------------------------------------------------------------- 2. 结构：_local_client 看不到 request


def test_local_client_cannot_see_the_request():
    """回环客户端构造函数不持有 request：结构上无法再从 Host 头拼目标。"""
    params = inspect.signature(supervisor_api._local_client).parameters
    assert "request" not in params, list(params)
    assert "tenant_id" in params, list(params)


# ---------------------------------------------------------------- 3. 推导规则（纯函数）


@pytest.mark.parametrize("host,port,expected", [
    ("127.0.0.1", 8080, "http://127.0.0.1:8080"),
    ("0.0.0.0", 9000, "http://127.0.0.1:9000"),
    ("", 8080, "http://127.0.0.1:8080"),
    ("::", 8080, "http://[::1]:8080"),
    ("::1", 8080, "http://[::1]:8080"),
    ("localhost", 8080, "http://localhost:8080"),
    ("10.0.0.5", 8080, "http://10.0.0.5:8080"),
])
def test_loopback_base_url_derives_from_bind_config(monkeypatch, host, port, expected):
    """通配监听取对应回环地址；具体绑定地址原样；IPv6 字面量加方括号。"""
    s = Settings()
    s.host = host
    s.port = port
    monkeypatch.setattr("config.get_settings", lambda: s)
    monkeypatch.delenv("PLAITA_CONSOLE_LOOPBACK_URL", raising=False)
    assert supervisor_api._loopback_base_url() == expected


def test_off_box_override_is_observable(monkeypatch, caplog):
    """覆盖值指向非本机地址：照用（部署方权威），但 WARNING 留痕——降级必须可观测。"""
    s = Settings()
    monkeypatch.setattr("config.get_settings", lambda: s)
    monkeypatch.setenv("PLAITA_CONSOLE_LOOPBACK_URL", "http://console.internal:8080/")
    with caplog.at_level(logging.WARNING, logger="api.supervisor"):
        assert supervisor_api._loopback_base_url() == "http://console.internal:8080"
    assert any("console.internal" in rec.getMessage() and rec.levelno == logging.WARNING
               for rec in caplog.records), [r.getMessage() for r in caplog.records]

    caplog.clear()
    monkeypatch.setenv("PLAITA_CONSOLE_LOOPBACK_URL", "http://127.0.0.1:9999")
    with caplog.at_level(logging.WARNING, logger="api.supervisor"):
        assert supervisor_api._loopback_base_url() == "http://127.0.0.1:9999"
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING], \
        [r.getMessage() for r in caplog.records]


@pytest.mark.parametrize("bad", ["ftp://127.0.0.1:8080", "127.0.0.1:8080", "http://", "http:///path"])
def test_malformed_override_is_rejected_loudly(monkeypatch, bad):
    """非法覆盖值不静默回退到推导值：503 具名拒绝（回环配置错误）。"""
    from fastapi import HTTPException

    s = Settings()
    monkeypatch.setattr("config.get_settings", lambda: s)
    monkeypatch.setenv("PLAITA_CONSOLE_LOOPBACK_URL", bad)
    with pytest.raises(HTTPException) as ei:
        supervisor_api._loopback_base_url()
    assert ei.value.status_code == 503
    assert "PLAITA_CONSOLE_LOOPBACK_URL" in str(ei.value.detail)


# ---------------------------------------------------------------- 4. 正控：正常路径仍 200，回环 netloc == 固定源


class _LoopbackViaTestClient(httpx.BaseTransport):
    """把回环委托给进程内 TestClient（真 auth / 租户代码），并记录每个请求的 netloc。"""

    def __init__(self, client: TestClient, log: list):
        self._client = client
        self.log = log

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.log.append(request.url.netloc.decode())
        resp = self._client.request(
            request.method, str(request.url), headers=dict(request.headers),
            content=request.read(),
        )
        return httpx.Response(resp.status_code, headers=resp.headers, content=resp.content)


def test_normal_iterate_still_succeeds_and_every_loopback_request_targets_fixed_source(env, monkeypatch):
    """攻击者 Host 下走正常全链路（transport 委托）：200，且每个回环请求 netloc == 固定源。"""
    log: list = []
    real_init = plaita_console_client.ConsoleClient.__init__

    def _patched_init(self, config, transport=None):
        real_init(self, config, transport=_LoopbackViaTestClient(env.client, log))

    monkeypatch.setattr(plaita_console_client.ConsoleClient, "__init__", _patched_init)

    editor_a = env.login("editor-a", EDITOR_A_PW)
    r = env.iterate(editor_a, host=f"127.0.0.1:{env.attacker.port}")
    assert r.status_code == 200, r.text
    assert r.json()["iterations"][0]["baseline_version"] == "1.0.0"
    assert log, "基线评测必须经回环发生"
    assert set(log) == {env.selfsrc.netloc}, log
    assert env.attacker.captured == []
