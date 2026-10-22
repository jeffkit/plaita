"""A55-S1~S5：审计覆盖 / denied 审计 / 归属与身份 / 读面硬化 / metrics 有界化。

对应 duo-a-ledger 第五十五轮（r55）：
- S1：schedules/nodes/property-types/events/flows/cluster/service.stop 等成功路径留痕；
- S2：viewer 角色 403、401、失败登录、租户 admin 触平台面 → ``*.denied``/``auth.*``；
- S3：API-key 写 default 时审计 tenant=default，并带 auth_source/key_fingerprint/
  request_id/role_snapshot；
- S4：``limit=-1`` 422、正上限钳到 1000、``before_id`` 游标、读审计留痕；
- S5：event_type 拒绝 ``:dlq``；metrics 不再 KEYS ``*:dlq``，只扫描已登记队列族，
  触界输出 ``plaita_metrics_truncated``。

负控约定：删除对应 ``_audit``/``record_denied`` 调用或把 ``scan_keys`` 换回
``redis.keys`` ⇒ 对应用例精确红（验收时手工执行）。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import audit as audit_api  # noqa: E402
from api import auth_users  # noqa: E402
from api import capacity as capacity_api  # noqa: E402
from api import copilot as copilot_api  # noqa: E402
from api import credentials as credentials_api  # noqa: E402
from api import dryrun as dryrun_api  # noqa: E402
from api import events as events_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from api import metrics as metrics_api  # noqa: E402
from api import nodes as nodes_api  # noqa: E402
from api import tenants as tenants_api  # noqa: E402
from api import property_types as property_types_api  # noqa: E402
from api import queues as queues_api  # noqa: E402
from api import schedules as schedules_api  # noqa: E402
from api import services as services_api  # noqa: E402
from api import supervisor as supervisor_api  # noqa: E402
from auth import require_auth  # noqa: E402
from plaita.tenant_context import (  # noqa: E402
    DEFAULT_TENANT_ID,
    TENANT_SOURCE_EXPLICIT,
    set_current_tenant,
)
from services import audit as audit_svc  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

ROOT_PW = "root-password-1"
TADMIN_PW = "tadmin-password-1"
VIEWER_PW = "viewer-password-1"
TENANT_A = "tenant-a"
ADMIN_KEY = "r55-audit-key"


def _settings(admin_key: str = ADMIN_KEY):
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = admin_key
    s.allow_insecure_admin = False
    return s


class Env:
    def __init__(self, client: TestClient, redis, store):
        self.client = client
        self.redis = redis
        self.store = store


@pytest.fixture()
def env(tmp_path, monkeypatch) -> Env:
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/r55.db")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    audit_svc.reset_denied_throttle()
    set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)

    flow_store.init_engine(f"sqlite:///{tmp_path}/r55.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Tenant A")
    users_svc.create_user(store, "root", ROOT_PW, "admin", platform_admin=True)
    users_svc.add_member(store, "default", "root", "admin")
    users_svc.create_user(store, "tadmin", TADMIN_PW, "admin")
    users_svc.add_member(store, TENANT_A, "tadmin", "admin")
    users_svc.create_user(store, "viewer1", VIEWER_PW, "viewer")
    users_svc.add_member(store, TENANT_A, "viewer1", "viewer")

    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.store = store
    app.state.local_mode = True
    app.state.redis = redis
    app.include_router(auth_users.router, prefix="/api")
    for router in (
        audit_api.router,
        capacity_api.router,
        copilot_api.router,
        credentials_api.router,
        dryrun_api.router,
        events_api.router,
        flows_api.router,
        metrics_api.router,
        nodes_api.router,
        property_types_api.router,
        queues_api.router,
        schedules_api.router,
        services_api.router,
        supervisor_api.router,
        tenants_api.router,
    ):
        app.include_router(router, prefix="/api", dependencies=[Depends(require_auth)])
    return Env(TestClient(app), redis, store)


def _login(client: TestClient, username: str, password: str) -> dict:
    r = client.post("/api/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _rows(action: str):
    return audit_svc.list_audit(action=action, limit=1000)


def _assert_audit(action: str, *, resource_id: str | None = None,
                  tenant_id: str | None = None, actor: str | None = None) -> dict:
    rows = _rows(action)
    assert rows, f"缺少审计行: {action}"
    for row in rows:
        if resource_id is not None and row["resource_id"] != resource_id:
            continue
        if tenant_id is not None and row["tenant_id"] != tenant_id:
            continue
        if actor is not None and row["actor"] != actor:
            continue
        return row
    raise AssertionError(f"审计行不匹配: {action} rows={rows}")


# ------------------------------------------------------------------ S1 覆盖
def test_schedule_crud_trigger_are_audited(env: Env, monkeypatch):
    root = _login(env.client, "root", ROOT_PW)
    env.store.create_flow("flow-sched", tenant_id="default")

    resp = env.client.post(
        "/api/schedules",
        json={"name": "s1", "flow_id": "flow-sched", "cron": "* * * * *"},
        headers=root,
    )
    assert resp.status_code == 200, resp.text
    sid = resp.json()["schedule_id"]
    _assert_audit("schedule.create", resource_id=sid, tenant_id="default", actor="root")

    resp = env.client.put(
        f"/api/schedules/{sid}", json={"cron": "*/2 * * * *"}, headers=root)
    assert resp.status_code == 200, resp.text
    _assert_audit("schedule.update", resource_id=sid, tenant_id="default")

    assert env.client.post(f"/api/schedules/{sid}/disable", headers=root).status_code == 200
    _assert_audit("schedule.disable", resource_id=sid, tenant_id="default")
    assert env.client.post(f"/api/schedules/{sid}/enable", headers=root).status_code == 200
    _assert_audit("schedule.enable", resource_id=sid, tenant_id="default")

    from services import local_scheduler
    monkeypatch.setattr(local_scheduler, "trigger_now", lambda store, schedule: "exec-r55")
    resp = env.client.post(f"/api/schedules/{sid}/trigger", headers=root)
    assert resp.status_code == 200, resp.text
    row = _assert_audit("schedule.trigger", resource_id=sid, tenant_id="default")
    assert row["detail"]["execution_id"] == "exec-r55"

    assert env.client.delete(f"/api/schedules/{sid}", headers=root).status_code == 200
    _assert_audit("schedule.delete", resource_id=sid, tenant_id="default")


def test_nodes_properties_events_flows_are_audited(env: Env):
    root = _login(env.client, "root", ROOT_PW)

    assert env.client.post(
        "/api/nodes",
        json={"node_type": "custom_r55", "node_name": "R55", "category": "test",
              "schema_json": "{}"},
        headers=root,
    ).status_code == 200
    _assert_audit("node.register", resource_id="custom_r55", tenant_id="default")
    assert env.client.delete("/api/nodes/custom_r55", headers=root).status_code == 200
    _assert_audit("node.delete", resource_id="custom_r55", tenant_id="default")

    assert env.client.post(
        "/api/property-types",
        json={"name": "pct_r55", "base_type": "string"},
        headers=root,
    ).status_code == 200
    _assert_audit("property_type.upsert", resource_id="pct_r55", tenant_id="default")
    assert env.client.delete("/api/property-types/pct_r55", headers=root).status_code == 200
    _assert_audit("property_type.delete", resource_id="pct_r55", tenant_id="default")

    resp = env.client.post(
        "/api/events/publish",
        json={"event_type": "approval", "correlation_id": "corr-r55"},
        headers=root,
    )
    assert resp.status_code == 200, resp.text
    event_id = resp.json()["event_id"]

    row = _assert_audit("event.publish", resource_id=event_id, tenant_id="default")
    assert row["detail"]["event_type"] == "approval"
    assert row["detail"].get("data") is None

    env.redis.set(
        "plaita:subscription:data:sub-r55",
        json.dumps({
            "subscription_id": "sub-r55", "event_type": "approval",
            "flow_id": "f-r55", "tenant_id": "default",
        }),
    )
    assert env.client.delete(
        "/api/events/subscriptions/sub-r55", headers=root
    ).status_code == 200
    _assert_audit("event.subscription_delete", resource_id="sub-r55", tenant_id="default")

    assert env.client.post(
        "/api/flows", json={"flow_id": "flow-r55"}, headers=root
    ).status_code == 200
    _assert_audit("flow.create", resource_id="flow-r55", tenant_id="default")

    # 直接落一个版本，只验证 delete_version 的审计（定义校验不在本单范围）。
    env.store.save_flow_definition(
        flow_id="flow-r55", version="1.0.0", definition="{}", layout="{}",
        status="draft", created_by="root", tenant_id="default",
    )
    assert env.client.delete(
        "/api/flows/flow-r55/versions/1.0.0", headers=root
    ).status_code == 200
    row = _assert_audit("flow.delete_version", resource_id="flow-r55@1.0.0",
                        tenant_id="default")
    assert row["detail"]["prior_status"] == "draft"


def test_service_stop_and_credential_read_are_audited(env: Env):
    root = _login(env.client, "root", ROOT_PW)
    env.redis.set("plaita:registry:flow_worker:inst-r55", "{}")
    resp = env.client.post(
        "/api/services/inst-r55/stop", json={"graceful": False}, headers=root)
    assert resp.status_code == 200, resp.text
    row = _assert_audit("service.stop", resource_id="inst-r55", tenant_id="")
    assert row["detail"]["graceful"] is False

    assert env.client.post(
        "/api/credentials",
        json={"name": "cred-r55", "type": "generic", "data": {"token": "s3cret"}},
        headers=root,
    ).status_code == 200
    assert env.client.get("/api/credentials/cred-r55", headers=root).status_code == 200
    row = _assert_audit("credential.read", resource_id="cred-r55", tenant_id="default")
    assert "s3cret" not in json.dumps(row["detail"])
    assert "data" not in row["detail"]


def test_dispatch_dry_run_and_logout_are_audited(env: Env, monkeypatch):
    root = _login(env.client, "root", ROOT_PW)

    resp = env.client.post(
        "/api/dispatch/one",
        json={"issue_id": "issue-r55", "dry_run": True},
        headers=root,
    )
    assert resp.status_code == 200, resp.text
    row = _assert_audit("dispatch.one", resource_id="issue-r55", tenant_id="default")
    assert row["detail"]["dry_run"] is True

    monkeypatch.setattr(
        dryrun_api.dryrun_svc, "dry_run",
        lambda *args, **kwargs: {"result": None, "nodes": [], "error": None},
    )
    resp = env.client.post(
        "/api/flows/dry-run",
        json={"flowJson": json.dumps(
            {"flow_id": "f-dry", "nodes": [{"id": "start", "type": "start"}]})},
        headers=root,
    )
    assert resp.status_code == 200, resp.text
    row = _assert_audit("flow.dry_run", resource_id="f-dry", tenant_id="default")
    assert row["detail"]["nodes"] == 1

    assert env.client.post("/api/auth/logout", headers=root).status_code == 200
    row = _assert_audit("auth.logout", actor="root", tenant_id="default")
    assert row["actor"] == "root"


def test_copilot_and_supervisor_are_audited(env: Env, monkeypatch):
    root = _login(env.client, "root", ROOT_PW)

    class _CopilotSettings:
        copilot_brain = "recursive"
        recursive_agui_url = "http://127.0.0.1:1/agui"
        recursive_agui_api_key = ""

    monkeypatch.setattr(copilot_api, "get_settings", lambda: _CopilotSettings())
    resp = env.client.post(
        "/api/copilot",
        json={"threadId": "thread-r55",
              "messages": [{"role": "user", "content": "PROMPT-SECRET"}]},
        headers={**root, "x-flow-id": "flow-r55"},
    )
    assert resp.status_code == 200, resp.text
    row = _assert_audit("copilot.run", resource_id="thread-r55", tenant_id="default")
    assert row["detail"]["brain"] == "recursive"
    assert "PROMPT-SECRET" not in json.dumps(row["detail"])

    class _FakeSupervisor:
        def __init__(self, *args, **kwargs):  # noqa: ARG002
            pass

        def run_loop(self, flow_id, dataset):
            return {"ticket": "promotion-r55"}

    monkeypatch.setattr(
        supervisor_api, "_load_plaita_ai",
        lambda: (lambda path: {"cases": []}, lambda: object(), lambda: object(),
                 lambda: object(), _FakeSupervisor, lambda **kwargs: object()),
    )
    # plaita#290：回环客户端现带调用方租户；入口归属校验要求 flow 真实存在。
    # plaita#291：回环客户端不再持有 request（目标钉固定内部源）。
    monkeypatch.setattr(supervisor_api, "_local_client", lambda tenant_id: object())
    monkeypatch.setattr(
        supervisor_api, "_resolve_dataset_path", lambda name: Path("/tmp/r55-ds"))
    env.store.create_flow("flow-r55", tenant_id="default")
    resp = env.client.post(
        "/api/flows/flow-r55/supervisor/iterate",
        json={"dataset": "ds-r55"}, headers=root,
    )
    assert resp.status_code == 200, resp.text
    row = _assert_audit("supervisor.iterate", resource_id="flow-r55",
                        tenant_id="default")
    assert row["detail"]["dataset"] == "ds-r55"


# ------------------------------------------------------------------ S2 denied
def test_setup_is_audited(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/r55-setup.db")
    flow_store.init_engine(f"sqlite:///{tmp_path}/r55-setup.db")
    flow_store.ensure_tenant_bootstrap()
    app = FastAPI()
    app.state.store = flow_store.get_flow_store()
    app.include_router(auth_users.router, prefix="/api")

    resp = TestClient(app).post(
        "/api/auth/setup",
        json={"username": "boss", "password": "init-password-1"},
    )
    assert resp.status_code == 200, resp.text
    row = _assert_audit("auth.setup", actor="boss", tenant_id="default")
    assert row["detail"]["role"] == "admin"


def test_denied_and_auth_failures_are_audited_and_throttled(env: Env):
    viewer = _login(env.client, "viewer1", VIEWER_PW)
    _assert_audit("auth.login", actor="viewer1", tenant_id="tenant-a")
    for _ in range(5):
        assert env.client.post("/api/nodes", json={"node_type": "x"},
                               headers=viewer).status_code == 403
    rows = _rows("node.denied")
    assert len(rows) == 1, rows  # 60s 窗口内合并，未认证/低权流量不能按请求数膨胀
    assert rows[0]["actor"] == "viewer1"
    assert rows[0]["detail"]["reason"] == "role_required"
    assert rows[0]["detail"]["required_role"] == "editor"

    assert env.client.get("/api/flows").status_code == 401
    row = _assert_audit("auth.denied", actor="anonymous")
    assert row["detail"]["reason"] == "unauthenticated"

    resp = env.client.post("/api/auth/login",
                           json={"username": "root", "password": "wrong-password"})
    assert resp.status_code == 401
    row = _assert_audit("auth.login_failed", actor="root")
    assert row["detail"]["reason"] == "bad_credentials"
    assert "wrong-password" not in json.dumps(row["detail"])

    tadmin = _login(env.client, "tadmin", TADMIN_PW)
    assert env.client.get("/api/tenants", headers=tadmin).status_code == 403
    row = _assert_audit("tenant.list.denied", actor="tadmin")
    assert row["detail"]["reason"] == "platform_admin_required"


# ------------------------------------------------------------------ S3 归属/身份
def test_api_key_write_default_audits_effective_tenant_and_identity(
    tmp_path, monkeypatch,
):
    db = f"sqlite:///{tmp_path}/r55-main.db"
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", db)
    monkeypatch.setenv("PLAITA_CONSOLE_ADMIN_API_KEY", ADMIN_KEY)
    monkeypatch.setenv("PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN", "false")
    # 立即失败的 Redis：console 进本地单机模式，不依赖外部服务。
    monkeypatch.setenv("PLAITA_CONSOLE_REDIS_URL", "redis://127.0.0.1:1/0")
    monkeypatch.setenv("PLAITA_CREDENTIALS_FILE", str(tmp_path / "creds.json"))
    monkeypatch.setenv("PLAITA_CREDENTIALS_KEY_FILE", str(tmp_path / "creds.key"))
    monkeypatch.delenv("PLAITA_CREDENTIALS_KEY", raising=False)
    audit_svc.reset_denied_throttle()

    from main import create_app

    headers = {"X-Admin-API-Key": ADMIN_KEY}
    with TestClient(create_app()) as client:
        resp = client.post(
            "/api/credentials",
            json={"name": "cred-api-r55", "type": "generic",
                  "data": {"token": "api-secret"}},
            headers=headers,
        )
        assert resp.status_code == 200, resp.text
        request_id = resp.headers.get("X-Request-ID")
        assert request_id, "响应必须回传 X-Request-ID"

        resp = client.get("/api/audit?action=credential.save", headers=headers)
        assert resp.status_code == 200, resp.text
        rows = resp.json()["logs"]
        row = next(r for r in rows if r["resource_id"] == "cred-api-r55")
        assert row["tenant_id"] == "default"       # 生效租户，不再是 ""
        assert row["actor"] == "api-key"
        assert row["auth_source"] == "api-key"
        assert len(row["key_fingerprint"]) == 16
        assert row["key_fingerprint"] != ADMIN_KEY
        assert row["request_id"] == request_id
        assert row["role_snapshot"] == "admin"


# ------------------------------------------------------------------ S4 读面
def test_audit_read_surface_limit_cursor_and_trail(env: Env):
    root = _login(env.client, "root", ROOT_PW)
    from models.flow import AuditLog

    with env.store._session_local() as session:
        session.add_all([
            AuditLog(tenant_id="default", actor="seed", action="seed.row",
                     resource="seed", resource_id=str(i))
            for i in range(1205)
        ])
        session.commit()

    assert env.client.get("/api/audit?limit=-1", headers=root).status_code == 422
    body = env.client.get("/api/audit?limit=5000", headers=root).json()
    assert len(body["logs"]) == 1000

    first = env.client.get("/api/audit?limit=10", headers=root).json()["logs"]
    second = env.client.get(
        f"/api/audit?limit=10&before_id={first[-1]['id']}", headers=root
    ).json()["logs"]
    assert len(first) == len(second) == 10
    assert not ({r["id"] for r in first} & {r["id"] for r in second})
    assert all(a["id"] > b["id"] for a, b in zip(first, first[1:]))
    assert second[0]["id"] < first[-1]["id"]

    reads = _rows("audit.read")
    assert reads, "读取审计本身必须留痕"
    assert reads[0]["detail"]["limit"] == 10


# ------------------------------------------------------------------ S5 metrics
class NoKeysRedis(FakeRedis):
    """metrics/DLQ 读面不得再调用 KEYS（#202 / A55-S5）。"""

    def keys(self, *args, **kwargs):  # noqa: ARG002
        raise AssertionError("读面仍在使用无界 KEYS")


def test_event_type_rejects_dlq_suffix(env: Env):
    root = _login(env.client, "root", ROOT_PW)
    resp = env.client.post(
        "/api/events/publish", json={"event_type": "x:dlq"}, headers=root)
    assert resp.status_code == 422, resp.text
    assert "dlq" in resp.text.lower()

    resp = env.client.post(
        "/api/events/publish", json={"event_type": "bad type"}, headers=root)
    assert resp.status_code == 422


def test_metrics_scans_only_registered_queue_family_and_is_bounded(env: Env):
    root = _login(env.client, "root", ROOT_PW)
    redis = NoKeysRedis(decode_responses=True)
    env.client.app.state.redis = redis
    redis.zadd("plaita:event:type:inject-a:dlq", {"evt": 1})
    for i in range(250):
        redis.set(f"plaita:redis_queue:q{i}", "x")

    body = env.client.get("/api/metrics", headers=root).text
    assert "plaita:event:type:inject-a:dlq" not in body
    assert "plaita_metrics_truncated" in body

    dlq = env.client.get("/api/queues/dlq", headers=root).json()
    assert all("plaita:event:" not in s["name"] for s in dlq["streams"])


# --------------------------------------------------- r55 S1-S5 落地面残面（r56）
def test_ai_generate_success_path_is_audited(env: Env, monkeypatch):
    """S1 残面：r55 的 ai-generate 只有 denied 审计，成功生成要留痕。

    A55-01 把它归在「仅拒绝审计」的 17 个端点里，S1 的修复清单只列了
    26 个「完全无审计」+ cluster 19，于是它是唯一剩下的 denial-only 写面。
    """
    root = _login(env.client, "root", ROOT_PW)
    from services import ai_flow

    monkeypatch.setattr(
        ai_flow, "generate_flow_events",
        lambda prompt, tenant_id=None: iter([{"type": "run_started"}]),
    )
    resp = env.client.post(
        "/api/flows/ai-generate/stream",
        json={"prompt": "PROMPT-SECRET", "agent": "glm-52"},
        headers=root,
    )
    assert resp.status_code == 200, resp.text
    row = _assert_audit("flow.ai_generate", actor="root")
    assert row["detail"]["agent"] == "glm-52"
    assert row["tenant_id"] == "default"
    assert row["detail"]["tenant_id"] == "default"
    assert "PROMPT-SECRET" not in json.dumps(row["detail"])


def test_tenant_member_ops_audit_target_tenant(env: Env):
    """S3 残面：成员关系的生效租户是**目标租户**，不是平台 actor 的 ""。"""
    root = _login(env.client, "root", ROOT_PW)
    users_svc.create_user(env.store, "viewer2", "viewer2-password", "viewer")

    resp = env.client.post(
        f"/api/tenants/{TENANT_A}/members",
        json={"username": "viewer2", "role": "viewer"},
        headers=root,
    )
    assert resp.status_code == 200, resp.text
    row = _assert_audit("tenant.add_member",
                        resource_id=f"{TENANT_A}/viewer2")
    assert row["tenant_id"] == TENANT_A
    assert row["detail"]["target_tenant"] == TENANT_A

    resp = env.client.put(
        f"/api/tenants/{TENANT_A}/members/viewer2",
        json={"username": "viewer2", "role": "editor"},
        headers=root,
    )
    assert resp.status_code == 200, resp.text
    row = _assert_audit("tenant.set_member_role",
                        resource_id=f"{TENANT_A}/viewer2")
    assert row["tenant_id"] == TENANT_A
    assert row["detail"]["target_tenant"] == TENANT_A

    resp = env.client.delete(
        f"/api/tenants/{TENANT_A}/members/viewer2", headers=root)
    assert resp.status_code == 200, resp.text
    row = _assert_audit("tenant.remove_member",
                        resource_id=f"{TENANT_A}/viewer2")
    assert row["tenant_id"] == TENANT_A
    assert row["detail"]["target_tenant"] == TENANT_A


def test_login_failure_reasons_are_separately_throttled(env: Env, monkeypatch):
    """S2 残面：合并键必须含 reason——锁定尝试不能被口令错误行吞掉。"""
    from services import login_guard

    resp = env.client.post(
        "/api/auth/login",
        json={"username": "root", "password": "wrong-password"},
    )
    assert resp.status_code == 401
    first = _assert_audit("auth.login_failed", actor="root")
    assert first["detail"]["reason"] == "bad_credentials"

    monkeypatch.setattr(login_guard, "retry_after", lambda ip, username: 30)
    resp = env.client.post(
        "/api/auth/login",
        json={"username": "root", "password": "wrong-password"},
    )
    assert resp.status_code == 429
    reasons = {
        row["detail"]["reason"] for row in _rows("auth.login_failed")
        if row["actor"] == "root"
    }
    assert reasons == {"bad_credentials", "locked"}


def test_platform_global_ops_audit_empty_tenant_with_target(env: Env):
    """S3 验收：平台全局操作 tenant=""，目标租户落在 detail.target_tenant。"""
    root = _login(env.client, "root", ROOT_PW)
    resp = env.client.post(
        "/api/tenants", json={"id": "tenant-b", "name": "Tenant B"}, headers=root)
    assert resp.status_code == 200, resp.text
    row = _assert_audit("tenant.create", resource_id="tenant-b")
    assert row["tenant_id"] == ""
    assert row["detail"]["target_tenant"] == "tenant-b"


def test_api_key_write_with_tenant_header_audits_that_tenant(env: Env):
    """S3 验收：API-key 带 X-Tenant-ID=acme 时审计落 acme，不回退 default。"""
    headers = {"X-Admin-API-Key": ADMIN_KEY, "X-Tenant-ID": TENANT_A}
    resp = env.client.post(
        "/api/nodes",
        json={"node_type": "custom-r55-header", "node_name": "Header"},
        headers=headers,
    )
    assert resp.status_code == 200, resp.text
    row = _assert_audit("node.register", resource_id="custom-r55-header")
    assert row["tenant_id"] == TENANT_A
    assert row["auth_source"] == "api-key"


def test_denied_audit_growth_is_bounded_under_1000_bad_requests(env: Env):
    """S2 验收：1000 次同键坏请求只落一行，合并计数不丢。"""
    import types

    state = types.SimpleNamespace(
        actor="viewer1", tenant_id=TENANT_A, auth_source="session",
        key_fingerprint="", request_id="", role="viewer",
    )
    request = types.SimpleNamespace(
        state=state,
        client=types.SimpleNamespace(host="10.0.0.9"),
        method="POST",
        url=types.SimpleNamespace(path="/api/nodes"),
    )
    for _ in range(1000):
        audit_svc.record_denied(
            request, "node.denied", "node", detail={"denied": True},
            actor="viewer1", tenant_id=TENANT_A, key_suffix="role_required",
        )
    assert len(_rows("node.denied")) == 1

    # 窗口到期后的下一行必须带 suppressed=999（不是静默丢弃）
    audit_svc.record_denied(
        request, "node.denied", "node", detail={"denied": True},
        actor="viewer1", tenant_id=TENANT_A, key_suffix="role_required",
        window_seconds=0,
    )
    rows = _rows("node.denied")
    assert len(rows) == 2
    assert max(row["detail"].get("suppressed", 0) for row in rows) == 999
