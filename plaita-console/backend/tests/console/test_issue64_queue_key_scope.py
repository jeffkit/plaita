"""#64 队列详情端点（GET /api/queues/{queue_name:path}）资源范围。

此前 queue_name 被原样当 Redis 键读取：无白名单、无租户过滤、count 无上限，
最低权限 viewer 即可跨租户 / 跨命名空间读取任意 list / stream 键。本组断言：
1. 未登记键 → 404（不泄露键是否存在）
2. 共享 Stream 按调用方租户过滤；平台全量视角（api-key）不过滤
3. count 封顶 200、负 start 拒绝
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
from api import queues as queues_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402
from services import tenants_svc  # noqa: E402
from services import users_svc  # noqa: E402

ADMIN_KEY = "issue64-admin-key"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/q64.db")
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path}/q64.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, "acme", "Acme")
    users_svc.create_user(
        store,
        "viewer_acme",
        "viewer-password-1",
        "viewer",
        memberships=[{"tenant_id": "acme", "role": "viewer"}],
    )
    redis = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = store
    app.include_router(auth_users.router, prefix="/api")
    app.include_router(
        queues_api.router, prefix="/api", dependencies=[Depends(require_auth)]
    )
    return TestClient(app), redis


def _viewer_headers(client):
    r = client.post(
        "/api/auth/login",
        json={"username": "viewer_acme", "password": "viewer-password-1"},
    )
    assert r.status_code == 200, r.text
    info = r.json()
    assert info["role"] == "viewer" and info["active_tenant"] == "acme"
    return {"Authorization": f"Bearer {info['token']}"}


def _admin_headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _seed_stream(redis, key="plaita:flow:queue", tenant="default", flow_id="f1"):
    return redis.xadd(
        key,
        {
            "payload": json.dumps(
                {
                    "type": "start",
                    "flow_id": flow_id,
                    "params": {"db_url": "postgres://u:p@h/db"},
                    "tenant_id": tenant,
                }
            )
        },
    )


class TestKeyWhitelist:
    def test_unregistered_key_404(self, env):
        client, redis = env
        redis.rpush("plaita:some:arbitrary:secret", json.dumps({"raw": "leaked"}))
        r = client.get(
            "/api/queues/plaita:some:arbitrary:secret", headers=_viewer_headers(client)
        )
        assert r.status_code == 404

    def test_non_plaita_namespace_404(self, env):
        client, redis = env
        redis.rpush("some-service:internal:list", "x")
        r = client.get(
            "/api/queues/some-service:internal:list", headers=_viewer_headers(client)
        )
        assert r.status_code == 404

    def test_registered_literal_key_ok(self, env):
        client, redis = env
        _seed_stream(redis, tenant="acme")
        r = client.get("/api/queues/plaita:flow:queue", headers=_viewer_headers(client))
        assert r.status_code == 200

    def test_registered_wildcard_prefix_ok(self, env):
        client, redis = env
        redis.rpush(
            "plaita:redis_queue:abc",
            json.dumps({"tenant_id": "acme", "task": 1}),
        )
        r = client.get(
            "/api/queues/plaita:redis_queue:abc", headers=_viewer_headers(client)
        )
        assert r.status_code == 200 and r.json()["length"] == 1


class TestTenantFilter:
    def test_viewer_only_sees_own_tenant_stream_entries(self, env):
        client, redis = env
        _seed_stream(redis, tenant="default", flow_id="victim-flow")
        _seed_stream(redis, tenant="acme", flow_id="mine")
        body = client.get(
            "/api/queues/plaita:flow:queue", headers=_viewer_headers(client)
        ).json()
        assert [t["data"]["flow_id"] for t in body["tasks"]] == ["mine"]

    def test_viewer_only_sees_own_tenant_list_entries(self, env):
        client, redis = env
        redis.rpush("plaita:delay:queue", json.dumps({"tenant_id": "default"}))
        redis.rpush("plaita:delay:queue", json.dumps({"tenant_id": "acme"}))
        body = client.get(
            "/api/queues/plaita:delay:queue", headers=_viewer_headers(client)
        ).json()
        assert [t["data"]["tenant_id"] for t in body["tasks"]] == ["acme"]

    def test_platform_view_sees_all_tenants(self, env):
        client, redis = env
        _seed_stream(redis, tenant="default")
        _seed_stream(redis, tenant="acme")
        body = client.get(
            "/api/queues/plaita:flow:queue", headers=_admin_headers()
        ).json()
        assert len(body["tasks"]) == 2


class TestCountBounds:
    def test_count_capped_at_200(self, env):
        client, redis = env
        for _ in range(300):
            redis.rpush(
                "plaita:delay:queue", json.dumps({"tenant_id": "acme"})
            )
        body = client.get(
            "/api/queues/plaita:delay:queue?count=100000",
            headers=_viewer_headers(client),
        ).json()
        assert len(body["tasks"]) <= 200

    def test_negative_start_rejected(self, env):
        client, redis = env
        redis.rpush("plaita:delay:queue", json.dumps({"tenant_id": "acme"}))
        r = client.get(
            "/api/queues/plaita:delay:queue?start=-5", headers=_viewer_headers(client)
        )
        assert r.status_code == 422
