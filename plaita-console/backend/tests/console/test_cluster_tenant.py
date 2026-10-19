"""集群档多租户单测（fakeredis）。

覆盖：执行消息带租户、执行状态按租户 namespace 存取、平台视角跨租户扫描、
engine_sync 租户键、调度列表过滤、fire_schedule 消息租户。
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

from api import executions as executions_api  # noqa: E402
from api import schedules as schedules_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402
from services import engine_sync  # noqa: E402

ADMIN_KEY = "cluster-admin-key"


def _settings():
    from config import Settings
    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path}/cluster.db")
    redis = FakeRedis(decode_responses=True)
    return redis


def _build_app(redis) -> FastAPI:
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False  # 集群档分支
    app.state.store = flow_store.get_flow_store()
    app.include_router(
        executions_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    app.include_router(
        schedules_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return app


def _client(redis) -> TestClient:
    return TestClient(_build_app(redis))


def _headers(tenant: str = None):
    h = {"X-Admin-API-Key": ADMIN_KEY}
    if tenant:
        h["X-Tenant-ID"] = tenant
    return h


def _queued_messages(redis) -> list:
    out = []
    for _id, fields in redis.xrange("plaita:flow:queue"):
        payload = fields.get("payload")
        if isinstance(payload, bytes):
            payload = payload.decode()
        out.append(json.loads(payload))
    return out


def _seed_execution(redis, tenant: str, execution_id: str, status="running",
                    flow_id="demo-flow"):
    key = f"plaita:{tenant}:execution:{execution_id}" if tenant != "default" \
        else f"plaita:execution:{execution_id}"
    redis.set(key, json.dumps({
        "execution_id": execution_id,
        "flow_id": flow_id,
        "status": status,
        "start_time": "2026-09-09T00:00:00",
        "tenant_id": tenant,
    }))


class TestClusterExecutionTenant:
    def test_start_message_carries_tenant(self, env):
        client = _client(env)
        r = client.post("/api/executions",
                        json={"flow_id": "f1", "params": {}},
                        headers=_headers("acme"))
        assert r.status_code == 200, r.text
        msgs = _queued_messages(env)
        assert msgs and msgs[-1]["tenant_id"] == "acme"

    def test_start_message_defaults_when_platform_view(self, env):
        client = _client(env)
        r = client.post("/api/executions", json={"flow_id": "f1", "params": {}},
                        headers=_headers())
        assert r.status_code == 200
        msgs = _queued_messages(env)
        assert msgs and msgs[-1]["tenant_id"] == "default"

    def test_list_scoped_by_tenant_namespace(self, env):
        _seed_execution(env, "acme", "e-acme")
        _seed_execution(env, "default", "e-default")
        client = _client(env)

        acme = client.get("/api/executions", headers=_headers("acme")).json()
        assert [e["execution_id"] for e in acme["executions"]] == ["e-acme"]

        platform = client.get("/api/executions", headers=_headers()).json()
        ids = {e["execution_id"] for e in platform["executions"]}
        assert ids == {"e-acme", "e-default"}
        tenants = {e["execution_id"]: e["tenant_id"] for e in platform["executions"]}
        assert tenants == {"e-acme": "acme", "e-default": "default"}

    def test_platform_get_finds_tenant_execution_by_scan(self, env):
        _seed_execution(env, "acme", "e-acme")
        client = _client(env)
        r = client.get("/api/executions/e-acme", headers=_headers())
        assert r.status_code == 200
        assert r.json()["tenant_id"] == "acme"

    def test_tenant_view_cannot_see_other_tenant_execution(self, env):
        _seed_execution(env, "acme", "e-acme")
        client = _client(env)
        r = client.get("/api/executions/e-acme", headers=_headers("other"))
        assert r.status_code == 404

    def test_delete_removes_tenant_key(self, env):
        # 删除只对终态开放（A43-01）；本用例验租户 namespace 归属
        _seed_execution(env, "acme", "e-acme", status="completed")
        client = _client(env)
        r = client.delete("/api/executions/e-acme", headers=_headers("acme"))
        assert r.status_code == 200
        assert env.exists("plaita:acme:execution:e-acme") == 0
        # default 键不受影响
        _seed_execution(env, "default", "e-def", status="completed")
        r = client.delete("/api/executions/e-def", headers=_headers())
        assert r.status_code == 200
        assert env.exists("plaita:execution:e-def") == 0


class TestEngineSyncTenant:
    def test_sync_writes_tenant_namespace(self, env):
        assert engine_sync.sync_flow_to_engine(
            env, "f1", "1.0.0", '{"nodes": []}', tenant_id="acme"
        ) is True
        assert env.exists("plaita:acme:flow:f1:1.0.0") == 1
        assert env.exists("plaita:acme:flow_versions:f1") == 1

        engine_sync.sync_flow_to_engine(env, "f1", "1.0.0", '{"nodes": []}')
        assert env.exists("plaita:flow:f1:1.0.0") == 1  # default = 历史前缀

    def test_remove_uses_tenant_namespace(self, env):
        engine_sync.sync_flow_to_engine(env, "f1", "1.0.0", "{}", tenant_id="acme")
        engine_sync.remove_flow_version_from_engine(env, "f1", "1.0.0", tenant_id="acme")
        assert env.exists("plaita:acme:flow:f1:1.0.0") == 0
        engine_sync.remove_flow_from_engine(env, "f1", tenant_id="acme")


class TestClusterScheduleTenant:
    def test_list_filters_by_tenant(self, env):
        env.hset("plaita:schedules", "s-acme", json.dumps({
            "schedule_id": "s-acme", "name": "acme调度", "flow_id": "f",
            "cron": "* * * * *", "enabled": True, "tenant_id": "acme",
        }))
        env.hset("plaita:schedules", "s-def", json.dumps({
            "schedule_id": "s-def", "name": "默认调度", "flow_id": "f",
            "cron": "* * * * *", "enabled": True,
        }))
        client = _client(env)

        acme = client.get("/api/schedules", headers=_headers("acme")).json()
        assert [s["schedule_id"] for s in acme["schedules"]] == ["s-acme"]

        platform = client.get("/api/schedules", headers=_headers()).json()
        assert platform["total"] == 2

    def test_fire_schedule_message_carries_tenant(self, env):
        from plaita.server.services.schedule_service import fire_schedule

        msg_id = fire_schedule(env, {
            "schedule_id": "s1", "name": "n", "flow_id": "f",
            "cron": "* * * * *", "params": {}, "tenant_id": "acme",
        }, "plaita:flow:queue")
        assert msg_id is not None
        msgs = _queued_messages(env)
        assert msgs and msgs[-1]["tenant_id"] == "acme"

    def test_fire_schedule_skips_disabled_tenant(self, env):
        from plaita.server.services.schedule_service import fire_schedule
        from plaita.server.tenant_status import publish_tenant_status

        schedule = {
            "schedule_id": "s1", "name": "n", "flow_id": "f",
            "cron": "* * * * *", "params": {}, "tenant_id": "acme",
        }
        publish_tenant_status(env, "acme", "disabled")
        assert fire_schedule(env, schedule, "plaita:flow:queue") is None
        assert _queued_messages(env) == []

        publish_tenant_status(env, "acme", "active")
        assert fire_schedule(env, schedule, "plaita:flow:queue") is not None
        assert len(_queued_messages(env)) == 1

    def test_manual_trigger_rejected_for_disabled_tenant(self, env):
        from plaita.server.tenant_status import publish_tenant_status

        env.hset("plaita:schedules", "s-acme", json.dumps({
            "schedule_id": "s-acme", "name": "acme调度", "flow_id": "f",
            "cron": "* * * * *", "enabled": True, "tenant_id": "acme",
        }))
        publish_tenant_status(env, "acme", "disabled")
        client = _client(env)
        r = client.post("/api/schedules/s-acme/trigger", headers=_headers("acme"))
        assert r.status_code == 403, r.text
        assert _queued_messages(env) == []

    def test_single_endpoints_reject_cross_tenant(self, env):
        """#63：集群档单条端点必须做租户归属校验。

        acme 视角（X-Tenant-ID=acme）访问 default 条目（无 tenant_id 字段）：
        GET/PUT/DELETE/enable/disable/trigger/history 全部 404 且零副作用；
        同租户条目照常 200；平台视角（不带 X-Tenant-ID）不受影响。
        """
        env.hset("plaita:schedules", "s-acme", json.dumps({
            "schedule_id": "s-acme", "name": "acme调度", "flow_id": "f",
            "cron": "* * * * *", "enabled": True, "tenant_id": "acme",
            "params": {},
        }))
        env.hset("plaita:schedules", "s-def", json.dumps({
            "schedule_id": "s-def", "name": "default调度", "flow_id": "f",
            "cron": "* * * * *", "enabled": True, "params": {"secret": "x"},
        }))
        client = _client(env)

        # 跨租户读/停用/启用/触发/历史 → 404（先测非破坏性端点）
        for path in ["/api/schedules/s-def", "/api/schedules/s-def/history"]:
            r = client.get(path, headers=_headers("acme"))
            assert r.status_code == 404, f"GET {path} -> {r.status_code}: {r.text}"
        for path in ["/api/schedules/s-def/enable", "/api/schedules/s-def/disable",
                     "/api/schedules/s-def/trigger"]:
            r = client.post(path, json={}, headers=_headers("acme"))
            assert r.status_code == 404, f"POST {path} -> {r.status_code}: {r.text}"
        assert env.hexists("plaita:schedules", "s-def") == 1  # 未被改写/停用
        assert _queued_messages(env) == []  # 未被越权触发入队

        # 跨租户 PUT/DELETE：404 且条目原样保留（DELETE 不清触发历史键）
        r = client.put("/api/schedules/s-def", json={"params": {"evil": True}},
                       headers=_headers("acme"))
        assert r.status_code == 404, r.text
        assert json.loads(env.hget("plaita:schedules", "s-def"))["params"] == {"secret": "x"}
        r = client.delete("/api/schedules/s-def", headers=_headers("acme"))
        assert r.status_code == 404, r.text
        assert env.hexists("plaita:schedules", "s-def") == 1
        assert env.exists("plaita:schedule:fires:s-def") == 0

        # 同租户：读/改照常
        assert client.get("/api/schedules/s-acme",
                          headers=_headers("acme")).status_code == 200
        r = client.put("/api/schedules/s-acme", json={"params": {"k": "v"}},
                       headers=_headers("acme"))
        assert r.status_code == 200, r.text
        assert r.json()["params"] == {"k": "v"}

        # 平台视角：仍可访问 default 条目（tenant_scope=None 不过滤）
        assert client.get("/api/schedules/s-def", headers=_headers()).status_code == 200
