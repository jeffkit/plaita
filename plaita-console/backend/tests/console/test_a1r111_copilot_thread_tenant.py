"""r70 b/P2（duo-a-ledger 第一百一十一轮）：CopilotThread 跨租户劫持收口。

缺口（基线 ``b2906c2``）：
- ``upsert_copilot_thread`` 只按 ``thread_id`` 查行（全局唯一键），
  ``POST /api/copilot`` 的 ``threadId`` 客户端可控——tenant-a 用 tenant-b 的
  thread id 发起会话即把该行「接管」：``flow_id`` 改写为 A 的 flow、
  ``message_count`` 继续累加、租户归属易主，B 的会话历史关联消失；
- ``claude_brain._SESSIONS`` 只以 threadId 为键：知道他租户 threadId 即可
  ``--resume`` 其 claude 会话（读到对方上下文）。

修法：
- thread 归属租户闸：upsert 按 ``(tenant_id, thread_id)`` 复合键查行；
  thread 已被其他租户占用时抛 ``LookupError``，端点转 404（不接管历史、
  不以 200 继续跑大脑）；
- ``_SESSIONS`` 键加租户段 ``(tenant_id, thread_id)``：跨租户 threadId 查
  不到会话 ⇒ 不可能 ``--resume`` 他租户会话。

负控：仅还原 upsert 的复合键查询（回到只按 thread_id）/ 还原 _SESSIONS
键（回到裸 threadId）⇒ 对应用例精确红。
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users, copilot as copilot_api  # noqa: E402
from auth import require_auth, tenant_generation_context  # noqa: E402
from services import claude_brain, flow_agent, flow_store, tenants_svc, users_svc  # noqa: E402

EDITOR_A_PW = "editor-a-password-1"
EDITOR_B_PW = "editor-b-password-1"
TENANT_A = "tenant-a"
TENANT_B = "tenant-b"
VICTIM_THREAD = "victim-thread-r111"
B_FLOW = "flow-b-r111"
A_FLOW = "flow-a-r111"


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
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/r111.db")
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

    flow_store.init_engine(f"sqlite:///{tmp_path}/r111.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")
    tenants_svc.create_tenant(store, TENANT_B, "Tenant B")

    users_svc.create_user(store, "editor-a", EDITOR_A_PW, "editor")
    users_svc.add_member(store, TENANT_A, "editor-a", "editor")
    users_svc.create_user(store, "editor-b", EDITOR_B_PW, "editor")
    users_svc.add_member(store, TENANT_B, "editor-b", "editor")

    app = FastAPI()
    app.state.store = store
    app.state.local_mode = True
    app.state.redis = None
    app.include_router(auth_users.router, prefix="/api")
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
    """flow_agent 分发探针：记录是否被调用，随后以 503 可判定失败收场
    （get_flow_agent 抛错 → 端点 503「flow_agent 构造失败」）。"""
    calls = []

    def _boom():
        calls.append(True)
        raise RuntimeError("no provider credentials in test")

    monkeypatch.setattr(flow_agent, "get_flow_agent", _boom)
    return calls


def _login(client: TestClient, username: str, password: str) -> str:
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return r.json()["token"]


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _post_copilot(client: TestClient, headers: dict, thread_id: str, flow_id: str):
    return client.post(
        "/api/copilot",
        json={
            "threadId": thread_id,
            "runId": f"run-{thread_id}",
            "messages": [{"role": "user", "content": "hi"}],
            "context": [],
        },
        headers={"x-flow-id": flow_id, **headers},
    )


def _victim_row():
    rows = flow_store.get_flow_store().list_copilot_threads(B_FLOW, tenant_id=None)
    assert len(rows) <= 1
    return rows[0] if rows else None


# ---- 存储层：upsert 按 (tenant, thread) 复合键 ----

class TestUpsertTenantBinding:
    def test_same_tenant_reupsert_updates_same_row(self, client):
        """正控：同租户重复 upsert 是同一行的更新（计数累加、flow 可更新）。"""
        store = flow_store.get_flow_store()
        store.upsert_copilot_thread(VICTIM_THREAD, B_FLOW, tenant_id=TENANT_B)
        store.upsert_copilot_thread(
            VICTIM_THREAD, B_FLOW, bump_message=True, tenant_id=TENANT_B
        )

        row = _victim_row()
        assert row is not None
        assert row["message_count"] == 1
        # 同 thread_id 全库仅一行
        all_rows = (
            flow_store.get_flow_store()
            .list_copilot_threads(B_FLOW, tenant_id=None)
        )
        assert len(all_rows) == 1

    def test_cross_tenant_upsert_refused_and_row_untouched(self, client):
        """tenant-a 拿 tenant-b 的 thread upsert → LookupError，B 行分毫不动。"""
        store = flow_store.get_flow_store()
        store.upsert_copilot_thread(
            VICTIM_THREAD, B_FLOW, title="B 的会话", tenant_id=TENANT_B
        )
        store.upsert_copilot_thread(VICTIM_THREAD, B_FLOW, bump_message=True, tenant_id=TENANT_B)
        before = _victim_row()

        with pytest.raises(LookupError):
            store.upsert_copilot_thread(
                VICTIM_THREAD, A_FLOW, bump_message=True, tenant_id=TENANT_A
            )

        after = _victim_row()
        assert after == before
        assert after["flow_id"] == B_FLOW


# ---- 端点层：跨租户 thread 404，大脑零启动 ----

class TestCopilotEndpointTenantGate:
    def test_cross_tenant_thread_404_and_brain_zero_run(self, client, agent_dispatch_probe):
        """tenant-b 先建会话，tenant-a 拿同一 threadId 发起 → 404、B 行不动。"""
        b_headers = _auth(_login(client, "editor-b", EDITOR_B_PW))
        r = _post_copilot(client, b_headers, VICTIM_THREAD, B_FLOW)
        assert r.status_code == 503, r.text  # 正控：B 自己可用（桩 503 = 已过闸）
        before = _victim_row()
        assert before is not None and before["flow_id"] == B_FLOW

        a_headers = _auth(_login(client, "editor-a", EDITOR_A_PW))
        r = _post_copilot(client, a_headers, VICTIM_THREAD, A_FLOW)

        assert r.status_code == 404, r.text
        # 404 先于大脑分发：探针只记录了 B 自己那一次合法调用
        assert len(agent_dispatch_probe) == 1
        after = _victim_row()
        assert after == before  # 接管未发生

    def test_new_thread_still_creates_and_reuses(self, client, agent_dispatch_probe):
        """正控：本租户新 thread 照常建行（503=桩），二次使用是同一行累加。"""
        a_headers = _auth(_login(client, "editor-a", EDITOR_A_PW))

        r1 = _post_copilot(client, a_headers, "thread-a-1", A_FLOW)
        assert r1.status_code == 503, r1.text
        r2 = _post_copilot(client, a_headers, "thread-a-1", A_FLOW)
        assert r2.status_code == 503, r2.text

        rows = flow_store.get_flow_store().list_copilot_threads(A_FLOW, tenant_id=TENANT_A)
        assert len(rows) == 1
        assert rows[0]["message_count"] == 2  # 每请求 bump 一次（建行请求也计）
        assert rows[0]["flow_id"] == A_FLOW
        # tenant-b 视角看不到 A 的会话
        assert flow_store.get_flow_store().list_copilot_threads(A_FLOW, tenant_id=TENANT_B) == []


# ---- claude 会话：_SESSIONS 键加租户段 ----

def _fake_cli(tmp_path: Path, argv_file: str, session_id: str) -> Path:
    path = tmp_path / "claude"
    path.write_text(
        "#!/bin/sh\n"
        f'printf \'%s\\n\' "$@" > "{tmp_path / argv_file}"\n'
        "printf '%s\\n' "
        f"'{{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"{session_id}\"}}'\n"
        "printf '%s\\n' "
        "'{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"ok\"}'\n"
        "exit 0\n"
    )
    path.chmod(0o755)
    return path


def _read_argv(tmp_path: Path, argv_file: str) -> list:
    return (tmp_path / argv_file).read_text().splitlines()


def _parse(frame: bytes) -> dict:
    text = frame.decode()
    assert text.startswith("data: ") and text.endswith("\n\n"), text
    return json.loads(text[len("data: "):].strip())


def _collect(body: dict, tenant_id=None):
    async def _run():
        if tenant_id is None:
            gen = claude_brain.claude_agui_stream(body, "hi")
        else:
            gen = claude_brain.claude_agui_stream(body, "hi", tenant_id=tenant_id)
        return [_parse(frame) async for frame in gen]

    return asyncio.run(_run())


@pytest.fixture()
def clean_sessions(monkeypatch):
    monkeypatch.setattr(claude_brain, "_SESSIONS", {})
    return claude_brain._SESSIONS


class TestClaudeSessionTenantSegment:
    def test_known_thread_id_cannot_resume_foreign_session(self, tmp_path, monkeypatch, clean_sessions):
        """裸 threadId 命中的旧键形态必须失效：他租户会话不可 --resume。"""
        cli = _fake_cli(tmp_path, "argv1.txt", "s-new")
        monkeypatch.setenv("PLAITA_CONSOLE_CLAUDE_CLI", str(cli))
        # 攻击者知道的 threadId（对应他租户已有会话 s-foreign）
        clean_sessions[VICTIM_THREAD] = "s-foreign"

        events = _collect({"threadId": VICTIM_THREAD, "messages": [{"role": "user", "content": "hi"}]})

        assert events[-1]["type"] == "RunFinished"
        argv = _read_argv(tmp_path, "argv1.txt")
        assert "--resume" not in argv
        assert "s-foreign" not in argv

    def test_same_tenant_resume_still_works(self, tmp_path, monkeypatch, clean_sessions):
        """正控：同租户同 thread 照常 --resume 既有会话。"""
        cli = _fake_cli(tmp_path, "argv2.txt", "s-second")
        monkeypatch.setenv("PLAITA_CONSOLE_CLAUDE_CLI", str(cli))
        clean_sessions[(TENANT_A, VICTIM_THREAD)] = "s-first"

        events = _collect(
            {"threadId": VICTIM_THREAD, "messages": [{"role": "user", "content": "hi"}]},
            tenant_id=TENANT_A,
        )

        assert events[-1]["type"] == "RunFinished"
        argv = _read_argv(tmp_path, "argv2.txt")
        assert "--resume" in argv
        assert argv[argv.index("--resume") + 1] == "s-first"

    def test_same_thread_id_across_tenants_isolated(self, tmp_path, monkeypatch, clean_sessions):
        """负控：同 threadId 不同租户是两个会话——tenant-a 拿 tenant-b 的
        threadId 开新会话，不得复用（--resume）tenant-b 的 session。"""
        cli = _fake_cli(tmp_path, "argv3.txt", "s-a-new")
        monkeypatch.setenv("PLAITA_CONSOLE_CLAUDE_CLI", str(cli))
        clean_sessions[(TENANT_B, VICTIM_THREAD)] = "s-b-owned"

        events = _collect(
            {"threadId": VICTIM_THREAD, "messages": [{"role": "user", "content": "hi"}]},
            tenant_id=TENANT_A,
        )

        assert events[-1]["type"] == "RunFinished"
        argv = _read_argv(tmp_path, "argv3.txt")
        assert "--resume" not in argv
        assert "s-b-owned" not in argv
        # B 的会话映射未被覆盖
        assert clean_sessions[(TENANT_B, VICTIM_THREAD)] == "s-b-owned"
        # A 落的是自己的新会话
        assert clean_sessions[(TENANT_A, VICTIM_THREAD)] == "s-a-new"
