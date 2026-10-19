"""A54-01（duo-a 第五十四轮）：集群日志 Stream 是租户删除的清理盲区。

缺口（基线 ``5ccaf6a``）：``delete_tenant`` 的 ``_purge_tenant_redis`` 只清
租户命名空间、调度、事件、订阅、配额与任务队列，**不清**平台级
``plaita:logs:*``。日志键不含租户段，租户边界只在条目 ``tenant_id`` 字段；
同名重建后旧租户 slug 重新命中新租户 ⇒ 新租户 viewer 经 ``/api/logs``、
``/api/logs/instance/{id}``、``/api/logs/stats`` 读到前代 message。

本文件钉住：
1. 删除前 viewer 能读到本租户旧日志（pre-fix 形态可复现）；
2. 删除后 → 同名重建 → 新租户 viewer 三个读面均为 0（旧 message 不出现）；
3. 清理按条目收窄：他租户日志原样保留（不误删共享 Stream）；
4. 缺 ``tenant_id`` 的旧条目按 ``default`` 归类（与读面同口径）。

负控（验收时手工执行）：仅移除 ``_purge_tenant_redis`` 里的
``counts["logs"] = _purge_tenant_logs(...)`` 调用（保留本文件）⇒
第 2 组断言精确红（重建后仍读到 ``OLD-LOG-SECRET``），第 1/3/4 组保持绿。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users, logs as logs_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

TENANT = "acme"
OTHER = "beta"
VIEWER_PW = "viewer-password-1"
OTHER_VIEWER_PW = "other-viewer-password-1"
LOG_KEY_1 = "plaita:logs:flow-worker:worker-global-1"
LOG_KEY_2 = "plaita:logs:flow-worker:worker-global-2"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ""
    s.allow_insecure_admin = False
    return s


def _entry(tenant: str, message: str, level: str = "ERROR") -> dict:
    return {
        "timestamp": "2026-10-19T00:00:00",
        "level": level,
        "message": message,
        "service_type": "flow-worker",
        "instance_id": "worker-global-1",
        "tenant_id": tenant,
        "logger": "plaita.server.flow_worker",
        "module": "flow_worker",
        "line": 1,
        "context": "{}",
        "exception": "",
    }


def _seed_logs(redis) -> None:
    redis.xadd(LOG_KEY_1, _entry(TENANT, "node failed: OLD-LOG-SECRET"))
    redis.xadd(LOG_KEY_1, _entry(OTHER, "BETA-LOG-OK", level="INFO"))
    redis.xadd(LOG_KEY_2, _entry(TENANT, "OLD-LOG-SECRET-2"))
    redis.xadd(LOG_KEY_2, _entry(OTHER, "BETA-LOG-OK-2"))
    # 旧条目没有 tenant_id：读面按 default 归类，删除 default 时才清。
    redis.xadd(LOG_KEY_2, {
        "timestamp": "2026-10-19T00:00:00",
        "level": "INFO",
        "message": "LEGACY-NO-TENANT",
        "service_type": "flow-worker",
        "instance_id": "worker-global-1",
        "context": "{}",
    })


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "a54-01.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())

    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    fake = FakeRedis(decode_responses=True)
    tenants_svc.create_tenant(store, TENANT, "Acme", redis_client=fake)
    tenants_svc.create_tenant(store, OTHER, "Beta", redis_client=fake)
    users_svc.create_user(store, "viewer1", VIEWER_PW, "viewer")
    users_svc.add_member(store, TENANT, "viewer1", "viewer")
    users_svc.create_user(store, "viewer2", OTHER_VIEWER_PW, "viewer")
    users_svc.add_member(store, OTHER, "viewer2", "viewer")
    _seed_logs(fake)

    app = FastAPI()
    app.state.store = store
    app.state.local_mode = False
    app.state.redis = fake
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        logs_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return {
        "store": store,
        "redis": fake,
        "client": TestClient(app),
    }


def _login(client: TestClient, username: str, password: str) -> str:
    r = client.post(
        "/api/auth/login", json={"username": username, "password": password}
    )
    assert r.status_code == 200, r.text
    return r.json()["token"]


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _messages(response) -> list:
    assert response.status_code == 200, response.text
    return [entry["message"] for entry in response.json()["logs"]]


def _delete_and_recreate(store, redis) -> None:
    assert tenants_svc.delete_tenant(store, TENANT, redis_client=redis) is True
    tenants_svc.create_tenant(store, TENANT, "Acme again", redis_client=redis)
    users_svc.add_member(store, TENANT, "viewer1", "viewer")


def test_pre_delete_viewer_reads_own_logs(env):
    """pre-fix 形态：删除前同名租户 viewer 确实能读到旧 message。"""
    token = _login(env["client"], "viewer1", VIEWER_PW)
    r = env["client"].get("/api/logs", headers=_auth(token))
    assert "node failed: OLD-LOG-SECRET" in _messages(r)


def test_recreated_tenant_viewer_cannot_read_previous_generation(env):
    _delete_and_recreate(env["store"], env["redis"])

    token = _login(env["client"], "viewer1", VIEWER_PW)
    for path in (
        "/api/logs",
        "/api/logs/instance/worker-global-1",
        "/api/logs/instance/worker-global-2",
    ):
        r = env["client"].get(path, headers=_auth(token))
        assert r.status_code == 200, r.text
        assert "OLD-LOG-SECRET" not in r.text
        assert r.json()["total"] == 0, (path, r.text)

    stats = env["client"].get("/api/logs/stats", headers=_auth(token))
    assert stats.status_code == 200, stats.text
    assert stats.json()["total_logs"] == 0
    assert stats.json()["stats"] == []


def test_other_tenant_logs_are_preserved(env):
    _delete_and_recreate(env["store"], env["redis"])

    token = _login(env["client"], "viewer2", OTHER_VIEWER_PW)
    messages = _messages(env["client"].get("/api/logs", headers=_auth(token)))
    assert "BETA-LOG-OK" in messages
    assert "BETA-LOG-OK-2" in messages
    assert env["redis"].xlen(LOG_KEY_1) == 1
    assert env["redis"].xlen(LOG_KEY_2) == 2  # beta + legacy no-tenant


def test_missing_tenant_id_is_default_not_matched_for_non_default(env):
    _delete_and_recreate(env["store"], env["redis"])
    rows = env["redis"].xrange(LOG_KEY_2)
    assert any(fields.get("message") == "LEGACY-NO-TENANT" for _, fields in rows)


def test_log_purge_counts_matched_entries(env):
    deleted = tenants_svc._purge_tenant_logs(env["redis"], TENANT)
    assert deleted == 2
    # key1 原有 acme + beta：acme 已清，beta 保留。
    assert env["redis"].xlen(LOG_KEY_1) == 1
