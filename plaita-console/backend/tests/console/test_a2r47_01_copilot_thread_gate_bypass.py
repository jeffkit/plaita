"""A2R47-01（duo-a2-ledger 第四十七轮，P2）：copilot thread 归属闸双绕收口。

缺口（基线 ``daadc00``，r70 修后残面）：
- **①头缺席整跳**：端点闸写作 ``if flow_id and threadId:``——``x-flow-id``
  缺席时归属 upsert（连同跨租户 404 闸）整个跳过，请求照常进入脑分发。
  threadId 是 recursive 侧的**全局**会话钥匙（``blake3(thread_id)`` 无租户
  段），闸的作用域不能由一个客户端可控、非必填的头决定。
- **②check-then-insert 竞窗**：``thread_id`` 全局 unique + 服务层两级
  SELECT 后 INSERT——真 SQLite + Barrier 双线程并发首用，败者双双通过检查
  后撞全局唯一约束，抛 ``IntegrityError`` 而非 ``LookupError``；端点
  ``except Exception`` 吞成 warning+200：顺序语义的「thread 不存在 404」
  在并发下静默消失；同租户双插变体败者的 ``message_count+1`` 随回滚丢失。

修法：
- 闸 fail-closed：带 ``threadId`` 的请求缺 ``x-flow-id`` → 具名 422
  （``flow_id_required``，error contract），先于脑分发与任何留痕；
- 服务层首插撞唯一约束（``_is_unique_violation``）时**回查归属**：同租户
  胜者行就地采纳（更新臂 + bump，与顺序语义等价；返回胜者 thread_id），
  他租户抛 ``LookupError``（端点 404，不接管历史）；
- 端点移除 ``except Exception`` 吞——upsert 意外失败向上抛（生产由 error
  contract 归一为具名 500），不得静默 200 继续跑大脑。

负控（pre 精确红 → post 收敛）：
- 还原端点闸为 ``if flow_id and ...`` ⇒ 头缺席用例红（过闸进脑）；
- 还原服务层 IntegrityError 转译 ⇒ 并发用例红（IntegrityError 漏出）、
  同租户计数用例红（败者计数丢失）；
- 端点加回 ``except Exception`` ⇒ 吞异常探针红（过闸 503 而非上抛 500）。
"""
from __future__ import annotations

import threading
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from sqlalchemy.exc import IntegrityError
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users, copilot as copilot_api  # noqa: E402
from auth import require_auth, tenant_generation_context  # noqa: E402
from error_contract import install_error_handlers  # noqa: E402
from services import (  # noqa: E402
    claude_brain,
    flow_agent,
    flow_store,
    tenants_svc,
    users_svc,
)

EDITOR_A_PW = "editor-a-password-1"
EDITOR_B_PW = "editor-b-password-1"
ROOT_PW = "root-password-1"
TENANT_A = "tenant-a"
TENANT_B = "tenant-b"
FLOW_A = "flow-a-r47"
FLOW_B = "flow-b-r47"

BODY = {
    "threadId": "t-r47",
    "runId": "run-r47",
    "messages": [{"role": "user", "content": "hi"}],
    "context": [],
}


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ""
    s.allow_insecure_admin = False
    s.copilot_brain = ""
    s.recursive_agui_url = ""
    return s


def _build_app(tmp_path, monkeypatch) -> FastAPI:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/r47.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.delenv("PLAITA_CONSOLE_COPILOT_BRAIN", raising=False)

    def _patched_settings():
        return _settings()

    monkeypatch.setattr("config.get_settings", _patched_settings)
    monkeypatch.setattr("auth.get_settings", _patched_settings)
    # copilot 模块的 get_settings 是 import 期绑定，须按模块对象补丁
    monkeypatch.setattr(copilot_api, "get_settings", _patched_settings)

    flow_store.init_engine(f"sqlite:///{tmp_path}/r47.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")
    tenants_svc.create_tenant(store, TENANT_B, "Tenant B")

    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    users_svc.create_user(store, "editor-a", EDITOR_A_PW, "editor")
    users_svc.add_member(store, TENANT_A, "editor-a", "editor")
    users_svc.create_user(store, "editor-b", EDITOR_B_PW, "editor")
    users_svc.add_member(store, TENANT_B, "editor-b", "editor")

    app = FastAPI()
    app.state.store = store
    app.state.local_mode = True
    app.state.redis = None
    install_error_handlers(app)
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
def agent_dispatch_probe(monkeypatch):
    """flow_agent 分发探针：记录是否被调用，随后以 503 可判定失败收场。"""
    calls = []

    def _boom():
        calls.append(True)
        raise RuntimeError("no provider credentials in test")

    monkeypatch.setattr(flow_agent, "get_flow_agent", _boom)
    return calls


@pytest.fixture()
def claude_calls(monkeypatch):
    calls = []

    async def _fake_stream(body, context_note, tenant_id=""):
        calls.append({"body": body, "tenant_id": tenant_id})
        yield b'data: {"type": "RunFinished"}\n\n'

    monkeypatch.setattr(claude_brain, "claude_available", lambda: True)
    monkeypatch.setattr(claude_brain, "claude_agui_stream", _fake_stream)
    return calls


def _login(client: TestClient, username: str, password: str) -> str:
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return r.json()["token"]


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _post(client: TestClient, headers: dict, flow_id=None, body=None):
    payload = dict(BODY) if body is None else body
    headers = dict(headers)
    if flow_id is not None:
        headers["x-flow-id"] = flow_id
    return client.post("/api/copilot", json=payload, headers=headers)


# ---- ①闸 fail-closed：头缺席即具名 422，先于脑分发、零留痕 ----


class TestGateFailClosed:
    def test_thread_without_flow_id_header_422_named(self, client, agent_dispatch_probe):
        """带 threadId 但无 x-flow-id → 具名 422；不进脑分发、不建会话行。"""
        headers = _auth(_login(client, "editor-a", EDITOR_A_PW))

        r = _post(client, headers)

        assert r.status_code == 422, r.text
        assert r.json()["detail"]["code"] == "flow_id_required"
        assert "x-flow-id" in r.json()["detail"]["message"]
        assert agent_dispatch_probe == []
        store = flow_store.get_flow_store()
        assert store.list_copilot_threads(FLOW_A, tenant_id=None) == []
        assert store.list_copilot_threads(FLOW_B, tenant_id=None) == []

    def test_claude_brain_thread_without_header_422_zero_start(self, client, claude_calls, monkeypatch):
        """平台管理员已过平台闸（claude 允许），头缺席仍 422、大脑零启动。"""
        monkeypatch.setenv("PLAITA_CONSOLE_COPILOT_BRAIN", "claude")
        headers = _auth(_login(client, "root", ROOT_PW))

        r = _post(client, headers)

        assert r.status_code == 422, r.text
        assert claude_calls == []

    def test_blank_flow_id_header_422(self, client, agent_dispatch_probe):
        """空白 x-flow-id 视同缺席（strip 后 fail-closed）。"""
        headers = _auth(_login(client, "editor-a", EDITOR_A_PW))

        r = _post(client, headers, flow_id="   ")

        assert r.status_code == 422, r.text
        assert agent_dispatch_probe == []

    def test_no_thread_id_request_still_reaches_brain(self, client, claude_calls, monkeypatch):
        """正控：无 threadId 的请求不受闸影响（无可关联的会话）。"""
        monkeypatch.setenv("PLAITA_CONSOLE_COPILOT_BRAIN", "claude")
        headers = _auth(_login(client, "root", ROOT_PW))
        body = {"runId": "r", "messages": [{"role": "user", "content": "hi"}], "context": []}

        r = _post(client, headers, flow_id=FLOW_A, body=body)

        assert r.status_code == 200, r.text
        assert len(claude_calls) == 1
        assert flow_store.get_flow_store().list_copilot_threads(FLOW_A, tenant_id=None) == []

    def test_gate_passes_with_header_and_registers_thread(self, client, agent_dispatch_probe):
        """正控：带头请求照常过闸（桩 503 = 已达分发）并落会话行。"""
        headers = _auth(_login(client, "editor-a", EDITOR_A_PW))

        r = _post(client, headers, flow_id=FLOW_A)

        assert r.status_code == 503, r.text
        rows = flow_store.get_flow_store().list_copilot_threads(FLOW_A, tenant_id=None)
        assert len(rows) == 1 and rows[0]["thread_id"] == "t-r47"


# ---- ②的掩盖面：禁止 except Exception 吞 upsert 失败 ----


class TestNoSwallow:
    def test_upsert_failure_not_swallowed_into_brain_dispatch(
        self, tmp_path, monkeypatch, agent_dispatch_probe
    ):
        """upsert 意外失败必须上抛（error contract 归一具名 500），不得吞成
        warning 后 200/503 继续跑大脑。"""
        def _boom(self, **kwargs):
            raise IntegrityError("INSERT INTO copilot_threads ...", {}, OSError("db gone"))

        # get_flow_store() 每次返回新实例，须在类上patch
        monkeypatch.setattr(flow_store.FlowStore, "upsert_copilot_thread", _boom)
        # raise_server_exceptions=False：看 error contract 处理器的具名 500，
        # 而不是让 TestClient 把异常原样重抛
        client = TestClient(_build_app(tmp_path, monkeypatch), raise_server_exceptions=False)
        headers = _auth(_login(client, "editor-a", EDITOR_A_PW))

        r = _post(client, headers, flow_id=FLOW_A)

        assert r.status_code == 500, r.text
        assert r.json()["detail"]["code"] == "internal_error"
        assert agent_dispatch_probe == []


# ---- ②竞窗：并发首用回查归属（真 SQLite + Barrier）----


class TestConcurrentFirstUse:
    def test_cross_tenant_race_loser_gets_lookup_error_not_integrity_error(self, client):
        """两租户并发首用同新 thread_id：恰一胜者，败者按归属得 LookupError
        （端点 404 语义），不得漏 IntegrityError（pre 被端点吞成 200）。"""
        store = flow_store.get_flow_store()
        for g in range(12):
            thread_id = f"race-x-{g}"
            flow_a, flow_b = f"flow-{g}-a", f"flow-{g}-b"  # 每组独立 flow，行数断言不跨组累积
            barrier = threading.Barrier(2)
            outcomes: dict[str, str] = {}

            def _run(tenant: str, flow: str):
                try:
                    barrier.wait(timeout=10)
                    store.upsert_copilot_thread(
                        thread_id, flow, bump_message=True, tenant_id=tenant
                    )
                    outcomes[tenant] = "ok"
                except LookupError:
                    outcomes[tenant] = "lookup"
                except IntegrityError as exc:  # pragma: no cover - pre-fix 形态
                    outcomes[tenant] = f"integrity:{exc}"

            threads = [
                threading.Thread(target=_run, args=(t, f))
                for t, f in ((TENANT_A, flow_a), (TENANT_B, flow_b))
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(30)

            assert sorted(outcomes) == [TENANT_A, TENANT_B], (g, outcomes)
            assert sorted(outcomes.values()) == ["lookup", "ok"], (g, outcomes)
            winner = next(t for t, v in outcomes.items() if v == "ok")
            rows = store.list_copilot_threads(
                flow_a if winner == TENANT_A else flow_b, tenant_id=None
            )
            assert len(rows) == 1 and rows[0]["message_count"] == 1

    def test_same_tenant_race_adopts_winner_row_and_keeps_both_counts(self, client):
        """同租户并发首用：败者回查采纳胜者行（更新臂重放），两笔 bump 都在，
        与顺序语义等价（pre 败者 IntegrityError 吞掉、计数丢失）。"""
        store = flow_store.get_flow_store()
        thread_id = "race-same-0"
        barrier = threading.Barrier(2)
        errors: list[Exception] = []

        def _run():
            try:
                barrier.wait(timeout=10)
                store.upsert_copilot_thread(
                    thread_id, FLOW_A, bump_message=True, tenant_id=TENANT_A
                )
            except Exception as exc:  # noqa: BLE001 - 测试收集
                errors.append(exc)

        threads = [threading.Thread(target=_run) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)

        assert errors == [], errors
        rows = store.list_copilot_threads(FLOW_A, tenant_id=None)
        assert len(rows) == 1, rows
        assert rows[0]["message_count"] == 2, rows
        assert rows[0]["flow_id"] == FLOW_A

    def test_upsert_returns_authoritative_thread_id(self, client):
        """契约：upsert 返回权威 thread_id（竞窗采纳胜者行后仍返回该 id）。"""
        store = flow_store.get_flow_store()
        assert store.upsert_copilot_thread("ret-1", FLOW_A, tenant_id=TENANT_A) == "ret-1"
        assert store.upsert_copilot_thread(
            "ret-1", FLOW_A, bump_message=True, tenant_id=TENANT_A
        ) == "ret-1"
