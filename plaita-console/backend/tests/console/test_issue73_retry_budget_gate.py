"""plaita#73 操作面：retry 唤醒预算达限必须**用户可见**（409），不得静默哑弹。

worker 对 `resume_type=retry` 有有界唤醒（G1 唤醒 ≤ `G1_MAX_WAKEUPS`、确定性
失败连续 ≤ `DETERMINISTIC_FAILURE_MAX`），达限时 `resume_flow` **幂等拒绝**
（`already_terminal` + `*_exhausted`，不抛异常、不改状态），只在 worker 日志留
一行。BFF 若照常返回「已受理」，操作台的「从断点重试」就是静默哑弹——点了没
反应、也没有任何提示（2026-10-10 评审）。

本组断言：
1. error + retry 且计数达限 → 409，detail 带 reason / counter_key 与解封动作；
2. 未达限 → 照常入队（闸不得误伤正常重试）；
3. 计数键按调用方租户 namespace 读（跨租户计数不得互相影响）；
4. 非 error 态（挂起）不受此闸影响——那里的幂等短路语义另算。
"""
import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from api import executions as executions_api  # noqa: E402
from auth import require_auth  # noqa: E402
from plaita.server.flow_worker import FlowWorker  # noqa: E402
from services import flow_store  # noqa: E402
from services import tenants_svc  # noqa: E402
from services import users_svc  # noqa: E402

ADMIN_KEY = "issue73-admin-key"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/r73.db")
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path}/r73.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, "acme", "Acme")
    # resume 是写操作：需要 editor 及以上角色（viewer 会 403，与预算闸无关）
    users_svc.create_user(
        store,
        "editor_acme",
        "editor-password-1",
        "editor",
        memberships=[{"tenant_id": "acme", "role": "editor"}],
    )
    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app), redis


def _editor_headers(client):
    r = client.post(
        "/api/auth/login",
        json={"username": "editor_acme", "password": "editor-password-1"},
    )
    assert r.status_code == 200, r.text
    info = r.json()
    assert info["role"] == "editor" and info["active_tenant"] == "acme"
    return {"Authorization": f"Bearer {info['token']}"}


def _seed_error_execution(redis, eid="exec-1", tenant="acme"):
    key = f"plaita:{tenant}:execution:{eid}"
    redis.set(
        key,
        json.dumps(
            {
                "execution_id": eid,
                "flow_id": "f1",
                "flow_version": "1",
                "status": "error",
                "context": {"$LAST_NODE": "start", "$NODE": {"start": {}}},
                "error": {"message": "boom"},
            }
        ),
    )
    return key


class TestRetryBudgetGate:
    def test_g1_wakeups_exhausted_returns_409(self, env):
        client, redis = env
        eid = "exec-g1"
        _seed_error_execution(redis, eid)
        key = f"plaita:acme:execution:g1wakeups:{eid}"
        redis.set(key, str(FlowWorker.G1_MAX_WAKEUPS))

        r = client.post(
            f"/api/executions/{eid}/resume",
            json={"resume_type": "retry"},
            headers=_editor_headers(client),
        )

        assert r.status_code == 409, r.text
        detail = r.json()["detail"]
        assert detail["reason"] == "g1_wakeups_exhausted"
        assert detail["counter_key"] == key, "必须给出人工解封要删的键"
        assert key in detail["message"]

    def test_deterministic_failure_exhausted_returns_409(self, env):
        client, redis = env
        eid = "exec-nofail"
        _seed_error_execution(redis, eid)
        key = f"plaita:acme:execution:nofail:{eid}"
        redis.set(key, str(FlowWorker.DETERMINISTIC_FAILURE_MAX))

        r = client.post(
            f"/api/executions/{eid}/resume",
            json={"resume_type": "retry"},
            headers=_editor_headers(client),
        )

        assert r.status_code == 409, r.text
        detail = r.json()["detail"]
        assert detail["reason"] == "deterministic_failure_exhausted"
        assert detail["counter_key"] == key

    def test_below_cap_still_accepted(self, env):
        """闸不得误伤正常重试：未达限照常入队。"""
        client, redis = env
        eid = "exec-ok"
        _seed_error_execution(redis, eid)
        redis.set(f"plaita:acme:execution:g1wakeups:{eid}", "1")

        r = client.post(
            f"/api/executions/{eid}/resume",
            json={"resume_type": "retry"},
            headers=_editor_headers(client),
        )

        assert r.status_code == 200, r.text
        assert r.json()["status"] == "resuming"
        assert redis.xlen("plaita:flow:queue") >= 1

    def test_counter_is_read_in_caller_namespace(self, env):
        """计数键按调用方租户读：default 前缀上的计数不得拦住 acme 的重试。"""
        client, redis = env
        eid = "exec-scope"
        _seed_error_execution(redis, eid)
        redis.set(f"plaita:execution:g1wakeups:{eid}", str(FlowWorker.G1_MAX_WAKEUPS))

        r = client.post(
            f"/api/executions/{eid}/resume",
            json={"resume_type": "retry"},
            headers=_editor_headers(client),
        )

        assert r.status_code == 200, r.text

    def test_suspended_execution_not_gated(self, env):
        """非 error 态：闸不介入（挂起/运行中的幂等语义由 worker 自理）。"""
        client, redis = env
        eid = "exec-suspended"
        key = _seed_error_execution(redis, eid)
        data = json.loads(redis.get(key))
        data["status"] = "suspended"
        redis.set(key, json.dumps(data))
        redis.set(
            f"plaita:acme:execution:g1wakeups:{eid}", str(FlowWorker.G1_MAX_WAKEUPS)
        )

        r = client.post(
            f"/api/executions/{eid}/resume",
            json={"resume_type": "event", "data": {"approved": True}},
            headers=_editor_headers(client),
        )

        assert r.status_code == 200, r.text
