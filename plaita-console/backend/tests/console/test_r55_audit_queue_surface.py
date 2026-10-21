"""A55-S2/S3/S4 余量：DLQ/queue 面的拒绝限频、归属身份与读面纵深。

背景（duo-a 第五十五轮；B1 第三十八轮「余量 S2-S4」）：B2 r44 已落通用审计
基建，但 queue 面仍有三处缺口：

- S2：``ensure_platform_admin`` 的拒绝行走 ``record``（每请求一行），租户
  admin/editor 反复回灌 403 可按请求数撑大审计表（A55-S2 要求窗口合并）；
- S3：``dlq.replay`` 审计行取 ``request.state.tenant_id``（平台/API-key 无
  header 时为 ``""``），而资源实际落在死信条目的租户；``detail`` 也没有
  ``target_tenant``，租户视角看不到本租户死信被谁回灌；
- S4：``list_audit`` 服务层未钳 ``limit``，绕过 FastAPI 的内部调用在 SQLite
  上用 ``limit=-1`` 物化全表（API 层 ``ge=1`` 只挡住 HTTP 面）。

负控（验收时手工执行，每条对应下面的用例）：
1. ``ensure_platform_admin`` 改回 ``record`` ⇒ 5 次 403 由 1 行变 5 行；
2. replay 审计去掉显式 ``tenant_id``/``target_tenant`` ⇒ 归属用例红；
3. 4xx 早退不写审计 ⇒ ``dlq_not_found``/``target_not_allowed`` 用例红；
4. 服务层去掉 ``max(1, min(...))`` ⇒ 1205 行断言红；
5. API 层去掉 ``Query(ge=1)`` ⇒ ``limit=-1`` 由 422 变 200（服务层钳制仍
   保证不物化全表，但契约已破坏）。
"""
from __future__ import annotations

import hashlib
import json
import sys
import uuid
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI, Request
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import audit as audit_api  # noqa: E402
from api import auth_users  # noqa: E402
from api import queues as queues_api  # noqa: E402
from api import tenants as tenants_api  # noqa: E402
from auth import require_auth  # noqa: E402
from models.flow import AuditLog  # noqa: E402
from services import audit as audit_svc  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

ADMIN_KEY = "r38-audit-queue-key"
TENANT = "acme"
DLQ_KEY = "plaita:flow:queue:v2:dlq"
TARGET = "plaita:flow:queue:v2"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    db_file = tmp_path / "r38-audit-queue.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{db_file}")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT, "Acme")
    tenants_svc.create_tenant(store, "other", "Other")
    users_svc.create_user(store, "root", "root-password-1", "admin",
                          platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    users_svc.create_user(store, "tadmin", "tadmin-password-1", "admin")
    users_svc.add_member(store, TENANT, "tadmin", "admin")
    users_svc.create_user(store, "editor1", "editor-password-1", "editor")
    users_svc.add_member(store, TENANT, "editor1", "editor")
    users_svc.create_user(store, "v_acme", "viewer-password-1", "viewer")
    users_svc.add_member(store, TENANT, "v_acme", "viewer")

    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = store

    @app.middleware("http")
    async def _request_id(request: Request, call_next):
        request.state.request_id = uuid.uuid4().hex
        response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.request_id
        return response

    app.include_router(auth_users.router, prefix="/api")
    for router in (audit_api.router, queues_api.router, tenants_api.router):
        app.include_router(router, prefix="/api",
                           dependencies=[Depends(require_auth)])
    return TestClient(app), redis, store


def _login(client, username, password):
    r = client.post("/api/auth/login",
                    json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _seed(redis, tenant=TENANT):
    from plaita.runtime_config import envelope_for_stream

    envelope = {
        "reason": "max_deliveries=5",
        "source_stream": TARGET,
        "source_id": "1699-0",
        "delivery_count": 5,
        "dead_lettered_at": 1700000000.0,
        "schema_version": 1,
        # r58：源/目标 family 一致的正控。
        "config_family": envelope_for_stream(TARGET, role="console")["config_family"],
        "payload": {
            "type": "start",
            "flow_id": "f1",
            "version": "1",
            "tenant_id": tenant,
            "execution_id": "exec-r38-1",
            "params": {"x": 1},
        },
    }
    return redis.xadd(DLQ_KEY, {"payload": json.dumps(envelope)})


def _rows(action):
    return audit_svc.list_audit(action=action, limit=audit_svc.AUDIT_LIMIT_MAX)


def _replay(client, dlq_id, *, headers, body=None):
    return client.post(
        f"/api/queues/dlq/{dlq_id}/replay",
        json=body or {"dlq_key": DLQ_KEY},
        headers=headers,
    )


# ------------------------------------------------------------------ S2 拒绝限频
def test_platform_gate_denials_coalesce_on_dlq_replay(env):
    """租户 admin/editor 反复回灌只留一行拒绝审计（60s 窗口）。"""
    client, redis, _store = env
    dlq_id = _seed(redis)
    tadmin = _login(client, "tadmin", "tadmin-password-1")
    for _ in range(5):
        assert _replay(client, dlq_id, headers=tadmin).status_code == 403

    rows = _rows("dlq.replay.denied")
    assert len(rows) == 1, rows
    row = rows[0]
    assert row["actor"] == "tadmin"
    assert row["resource"] == "queue"
    assert row["resource_id"] == dlq_id
    assert row["detail"]["denied"] is True
    assert row["detail"]["reason"] == "platform_admin_required"
    assert row["detail"]["method"] == "POST"
    assert row["detail"]["path"] == f"/api/queues/dlq/{dlq_id}/replay"
    assert row["auth_source"] == "session"
    assert row["key_fingerprint"] == ""
    # 拒绝不得写入任何回灌副作用。
    assert redis.xlen(TARGET) == 0


def test_editor_platform_gate_and_viewer_role_gate_are_throttled(env):
    """editor 落在平台闸、viewer 落在角色闸；两条拒绝路径都窗口合并。"""
    client, redis, _store = env
    dlq_id = _seed(redis)
    editor = _login(client, "editor1", "editor-password-1")
    for _ in range(3):
        assert _replay(client, dlq_id, headers=editor).status_code == 403
    editor_rows = _rows("dlq.replay.denied")
    assert len(editor_rows) == 1, editor_rows
    assert editor_rows[0]["actor"] == "editor1"
    assert editor_rows[0]["detail"]["reason"] == "platform_admin_required"

    viewer = _login(client, "v_acme", "viewer-password-1")
    for _ in range(3):
        assert _replay(client, dlq_id, headers=viewer).status_code == 403
    viewer_rows = _rows("queue.denied")
    assert len(viewer_rows) == 1, viewer_rows
    assert viewer_rows[0]["actor"] == "v_acme"
    assert viewer_rows[0]["detail"]["reason"] == "role_required"
    assert viewer_rows[0]["detail"]["required_role"] == "editor"


def test_queue_401_and_failed_login_are_audited_and_throttled(env):
    """401 与失败登录在 queue 读面/登录面留痕；密码/token 不得入审计。"""
    client, _redis, _store = env
    for _ in range(3):
        assert client.get("/api/queues/dlq").status_code == 401
    denied = _rows("auth.denied")
    assert len(denied) == 1, denied
    assert denied[0]["actor"] == "anonymous"
    assert denied[0]["detail"]["reason"] == "unauthenticated"
    assert denied[0]["detail"]["path"] == "/api/queues/dlq"

    for _ in range(3):
        resp = client.post("/api/auth/login",
                           json={"username": "root",
                                 "password": "wrong-password-1"})
        assert resp.status_code == 401
    failed = _rows("auth.login_failed")
    assert len(failed) == 1, failed
    assert failed[0]["detail"]["reason"] == "bad_credentials"
    assert "wrong-password-1" not in json.dumps(failed[0]["detail"])


def test_tenant_member_gate_denials_coalesce(env):
    """租户 admin 跨租户管理成员：重复拒绝只留一行（与平台闸同口径）。"""
    client, _redis, _store = env
    tadmin = _login(client, "tadmin", "tadmin-password-1")
    for _ in range(3):
        resp = client.post(
            "/api/tenants/other/members",
            json={"username": "v_acme", "role": "viewer"},
            headers=tadmin,
        )
        assert resp.status_code == 403, resp.text
    rows = _rows("tenant.add_member.denied")
    assert len(rows) == 1, rows
    assert rows[0]["actor"] == "tadmin"
    assert rows[0]["resource_id"] == "other/v_acme"
    assert rows[0]["detail"]["denied"] is True
    assert rows[0]["detail"]["reason"]


# -------------------------------------------------------- S3 归属与身份字段
def test_replay_audit_attributes_entry_tenant_and_api_key_identity(env):
    """API-key 无 header 回灌 acme 死信：审计行落在 acme，并带 key 指纹。"""
    client, redis, _store = env
    dlq_id = _seed(redis)
    resp = _replay(client, dlq_id, headers={"X-Admin-API-Key": ADMIN_KEY})
    assert resp.status_code == 200, resp.text
    assert resp.json()["replayed"] is True

    rows = _rows("dlq.replay")
    assert len(rows) == 1, rows
    row = rows[0]
    assert row["tenant_id"] == TENANT          # 资源生效租户，不再是 ""
    assert row["detail"]["tenant_id"] == TENANT
    assert row["detail"]["target_tenant"] == TENANT
    assert row["actor"] == "api-key"
    assert row["auth_source"] == "api-key"
    assert row["key_fingerprint"] == hashlib.sha256(
        ADMIN_KEY.encode("utf-8")).hexdigest()[:16]
    assert row["key_fingerprint"] != ADMIN_KEY
    assert row["request_id"] == resp.headers["X-Request-ID"]
    assert row["role_snapshot"] == "admin"


def test_replay_audit_target_tenant_beats_caller_header(env):
    """平台 API-key 带 X-Tenant-ID=default 回灌 acme 死信：审计归属以条目为准。"""
    client, redis, _store = env
    dlq_id = _seed(redis)
    resp = _replay(
        client,
        dlq_id,
        headers={"X-Admin-API-Key": ADMIN_KEY, "X-Tenant-ID": "default"},
    )
    assert resp.status_code == 200, resp.text
    row = _rows("dlq.replay")[0]
    assert row["tenant_id"] == TENANT
    assert row["detail"]["target_tenant"] == TENANT


def test_replay_4xx_attempts_leave_audit_rows(env):
    """定位失败/目标未登记也是「一次回灌尝试」：4xx 前必须留痕。"""
    client, redis, _store = env
    admin = {"X-Admin-API-Key": ADMIN_KEY}

    r404 = _replay(client, "999-0", headers=admin)
    assert r404.status_code == 404, r404.text
    missing = _rows("dlq.replay")
    assert len(missing) == 1, missing
    assert missing[0]["detail"]["reason"] == "dlq_not_found"
    assert missing[0]["resource_id"] == f"{DLQ_KEY}/999-0"
    assert missing[0]["tenant_id"] == ""       # 平台调用者无 header

    dlq_id = _seed(redis)
    r422 = _replay(
        client, dlq_id, headers=admin,
        body={"dlq_key": DLQ_KEY, "target_stream": "attacker:stream"},
    )
    assert r422.status_code == 422, r422.text
    rows = _rows("dlq.replay")
    assert len(rows) == 2, rows
    rejected = next(r for r in rows if r["detail"]["reason"] == "target_not_allowed")
    assert rejected["detail"]["target_stream"] == "attacker:stream"
    assert rejected["resource_id"] == f"{DLQ_KEY}/{dlq_id}"
    assert redis.xlen(TARGET) == 0


# ------------------------------------------------------------- S4 读面纵深
def test_audit_service_clamps_limit_for_internal_callers(env):
    """绕过 HTTP 的内部调用也不能用 limit=-1 在 SQLite 物化全表。"""
    _client, _redis, store = env
    with store._session_local() as session:
        session.add_all([
            AuditLog(tenant_id="default", actor="seed", action="seed.row",
                     resource="seed", resource_id=str(i))
            for i in range(1205)
        ])
        session.commit()

    assert len(audit_svc.list_audit(limit=-1)) == 1
    assert len(audit_svc.list_audit(limit=0)) == 1
    assert len(audit_svc.list_audit(limit=5000)) == audit_svc.AUDIT_LIMIT_MAX


def test_audit_api_lower_bound_and_read_trail(env):
    """API 层 limit 下限仍是 422；读审计记录生效上限与过滤条件。"""
    client, _redis, store = env
    root = _login(client, "root", "root-password-1")
    with store._session_local() as session:
        session.add_all([
            AuditLog(tenant_id="default", actor="seed", action="seed.row",
                     resource="seed", resource_id=str(i))
            for i in range(1205)
        ])
        session.commit()

    assert client.get("/api/audit?limit=-1", headers=root).status_code == 422
    assert client.get("/api/audit?limit=0", headers=root).status_code == 422
    body = client.get("/api/audit?limit=5000", headers=root).json()
    assert len(body["logs"]) == audit_svc.AUDIT_LIMIT_MAX

    reads = [r for r in _rows("audit.read")
             if r["detail"].get("limit") == audit_svc.AUDIT_LIMIT_MAX]
    assert reads, "读审计必须记录生效上限"
    assert reads[0]["detail"]["rows"] == audit_svc.AUDIT_LIMIT_MAX


def test_tenant_admin_read_trail_stays_in_current_generation(env):
    """租户 admin 的 audit.read 落在本租户，且不会看到平台全量行。"""
    client, _redis, store = env
    tadmin = _login(client, "tadmin", "tadmin-password-1")
    with store._session_local() as session:
        session.add_all([
            AuditLog(tenant_id=TENANT, actor="seed", action="seed.row",
                     resource="seed", resource_id=str(i))
            for i in range(3)
        ])
        session.add(AuditLog(tenant_id="", actor="platform", action="seed.global",
                             resource="seed", resource_id="g"))
        session.commit()

    body = client.get("/api/audit?limit=100", headers=tadmin).json()
    assert body["logs"], body
    assert all(row["tenant_id"] == TENANT for row in body["logs"])
    reads = _rows("audit.read")
    assert reads and reads[0]["tenant_id"] == TENANT
