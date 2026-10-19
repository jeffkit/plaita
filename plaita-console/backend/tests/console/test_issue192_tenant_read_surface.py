"""plaita#192：console 多租户读面残面（logs instance / SSE / stats、events、
订阅 GET/DELETE、DLQ）。

#63/#64 波次只修了被点名的端点；同族的这些读面仍无租户过滤：viewer 可跨租户
读日志/事件/死信，editor 可删他租户订阅（挂起执行永久失去唤醒）。本组用
**真实鉴权链路**（登录后的 viewer/editor + API-Key 平台视角）逐端点断言：

- 租户 A 视角只见 A 的日志 / 事件 / 死信 / 订阅；
- 对 B 的订阅 GET/DELETE 均 404，且记录未被删除；
- 平台管理员（API-Key，tenant_scope=None）仍看到全量；
- 已修的订阅列表回归保持；
- SSE：A 的流不出现 B 的日志条目（B 条目作负控）。
"""
import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import fakeredis
from fakeredis import FakeRedis
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import auth_users  # noqa: E402
from api import events as events_api  # noqa: E402
from api import logs as logs_api  # noqa: E402
from api import queues as queues_api  # noqa: E402
from auth import require_auth  # noqa: E402
from services import flow_store  # noqa: E402
from services import tenants_svc  # noqa: E402
from services import users_svc  # noqa: E402

ADMIN_KEY = "issue192-admin-key"
TENANT_A = "acme"
TENANT_B = "globex"
DLQ_KEY = "plaita:flow:queue:dlq"


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("PLAITA_CONSOLE_DB_URL", f"sqlite:///{tmp_path}/issue192.db")
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    flow_store.init_engine(f"sqlite:///{tmp_path}/issue192.db")
    flow_store.ensure_tenant_bootstrap()
    store = flow_store.get_flow_store()
    tenants_svc.create_tenant(store, TENANT_A, "Acme")
    tenants_svc.create_tenant(store, TENANT_B, "Globex")
    users_svc.create_user(
        store,
        "viewer_acme",
        "viewer-password-1",
        "viewer",
        memberships=[{"tenant_id": TENANT_A, "role": "viewer"}],
    )
    users_svc.create_user(
        store,
        "editor_acme",
        "editor-password-1",
        "editor",
        memberships=[{"tenant_id": TENANT_A, "role": "editor"}],
    )
    # #259：SSE 走 redis.asyncio pubsub；sync/async 共享同一 FakeServer。
    server = fakeredis.FakeServer()
    redis = FakeRedis(server=server, decode_responses=True)
    async_redis = fakeredis.aioredis.FakeRedis(
        server=server, decode_responses=True)
    _seed(redis)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.state.store = store
    app.state.execution_sse_aioredis = async_redis
    app.include_router(auth_users.router, prefix="/api")
    for module in (events_api, logs_api, queues_api):
        app.include_router(
            module.router, prefix="/api", dependencies=[Depends(require_auth)]
        )
    return TestClient(app), redis, async_redis


def _login(client: TestClient, username: str, password: str, tenant: str) -> dict:
    r = client.post(
        "/api/auth/login", json={"username": username, "password": password}
    )
    assert r.status_code == 200, r.text
    info = r.json()
    if info.get("active_tenant") != tenant:
        r = client.post(
            "/api/auth/switch-tenant",
            json={"tenant_id": tenant},
            headers={"Authorization": f"Bearer {info['token']}"},
        )
        assert r.status_code == 200, r.text
        info = r.json()
    return {"Authorization": f"Bearer {info['token']}"}


def _viewer_a(client: TestClient) -> dict:
    return _login(client, "viewer_acme", "viewer-password-1", TENANT_A)


def _editor_a(client: TestClient) -> dict:
    return _login(client, "editor_acme", "editor-password-1", TENANT_A)


def _admin() -> dict:
    return {"X-Admin-API-Key": ADMIN_KEY}


# ---------------------------------------------------------------- seed


def _log_entry(tenant: str, instance: str, message: str, level: str = "ERROR") -> dict:
    return {
        "timestamp": "2026-10-18T09:00:00",
        "level": level,
        "service_type": "flow_worker",
        "instance_id": instance,
        "tenant_id": tenant,
        "message": message,
        "context": "{}",
    }


def _event(event_id: str, tenant: str) -> dict:
    return {
        "event_id": event_id,
        "event_type": "approval",
        "data": {"approved": True},
        "timestamp": 1.0,
        "correlation_id": f"exec-{tenant}",
        "source": "test",
        "tenant_id": tenant,
    }


def _subscription(sub_id: str, tenant: str) -> dict:
    return {
        "subscription_id": sub_id,
        "event_type": "approval",
        "correlation_id": f"exec-{tenant}",
        "flow_id": f"flow-{tenant}",
        "created_at": 1.0,
        "tenant_id": tenant,
    }


def _dlq_payload(tenant: str, secret: str) -> dict:
    return {
        "payload": json.dumps(
            {
                "reason": "retry_exhausted",
                "source_stream": "plaita:flow:queue:v2",
                "source_id": "1-0",
                "delivery_count": 9,
                "dead_lettered_at": 1.0,
                "payload": {
                    "tenant_id": tenant,
                    "flow_id": f"flow-{tenant}",
                    "secret": secret,
                },
            }
        )
    }


def _seed(redis: FakeRedis) -> None:
    redis.xadd(
        "plaita:logs:flow_worker:inst-b",
        _log_entry(TENANT_B, "inst-b", "globex-secret-line"),
    )
    redis.xadd(
        "plaita:logs:flow_worker:inst-a",
        _log_entry(TENANT_A, "inst-a", "acme-line"),
    )
    redis.set("plaita:event:evt-b", json.dumps(_event("evt-b", TENANT_B)))
    redis.set("plaita:event:evt-a", json.dumps(_event("evt-a", TENANT_A)))
    redis.set(
        "plaita:subscription:data:sub-b", json.dumps(_subscription("sub-b", TENANT_B))
    )
    redis.set(
        "plaita:subscription:data:sub-a", json.dumps(_subscription("sub-a", TENANT_A))
    )
    redis.sadd("plaita:subscription:type:approval", "sub-b")
    redis.xadd(DLQ_KEY, _dlq_payload(TENANT_B, "globex-dlq-secret"))
    redis.xadd(DLQ_KEY, _dlq_payload(TENANT_A, "acme-dlq-line"))


# ---------------------------------------------------------------- logs


class TestLogsTenantScope:
    def test_instance_logs_only_own_tenant(self, env):
        client, _redis, _async_redis = env
        body = client.get(
            "/api/logs/instance/inst-b", headers=_viewer_a(client)
        ).json()
        assert [row["message"] for row in body["logs"]] == []

        admin_body = client.get("/api/logs/instance/inst-b", headers=_admin()).json()
        assert [row["message"] for row in admin_body["logs"]] == [
            "globex-secret-line"
        ]

    def test_instance_logs_own_tenant_still_visible(self, env):
        client, _redis, _async_redis = env
        body = client.get(
            "/api/logs/instance/inst-a", headers=_viewer_a(client)
        ).json()
        assert [row["message"] for row in body["logs"]] == ["acme-line"]

    def test_stats_only_own_tenant(self, env):
        client, _redis, _async_redis = env
        body = client.get("/api/logs/stats", headers=_viewer_a(client)).json()
        assert body["total_logs"] == 1, "租户视角不得把平台级 xlen 当租户总量"

        admin_body = client.get("/api/logs/stats", headers=_admin()).json()
        assert admin_body["total_logs"] == 2

    def test_list_logs_regression(self, env):
        client, _redis, _async_redis = env
        rows = client.get("/api/logs", headers=_viewer_a(client)).json()["logs"]
        assert [row["message"] for row in rows] == ["acme-line"]


class _StubRequest:
    """stream_logs 只用 request.is_disconnected 与 request.state.tenant_id。"""

    def __init__(self, tenant, async_client):
        self.app = SimpleNamespace(
            state=SimpleNamespace(
                local_mode=False, execution_sse_aioredis=async_client
            )
        )
        # #254：tenant_id=None 只有在可证明的平台级身份下才等于全量视角
        self.state = SimpleNamespace(
            tenant_id=tenant, role="viewer", platform_admin=tenant is None
        )

    async def is_disconnected(self):
        await asyncio.sleep(0)
        return False


def _drive_sse(
    fake: FakeRedis, async_fake, tenant, timeout: float = 8.0
) -> list:
    async def _run():
        resp = await logs_api.stream_logs(
            request=_StubRequest(tenant, async_fake),
            service_type=None,
            instance_id=None,
            level=None,
            redis=fake,
        )
        agen = resp.body_iterator
        events = []

        async def consume():
            while True:
                ev = await agen.__anext__()
                if ev.get("event") == "log":
                    events.append(json.loads(ev["data"]))
                    if events[-1].get("tenant_id") == TENANT_A:
                        return

        task = asyncio.create_task(consume())
        await asyncio.sleep(0.3)  # psubscribe 生效
        fake.publish(
            "plaita:logs:stream:flow_worker:inst-b",
            json.dumps(_log_entry(TENANT_B, "inst-b", "globex-stream-secret")),
        )
        await asyncio.sleep(0.1)
        fake.publish(
            "plaita:logs:stream:flow_worker:inst-a",
            json.dumps(_log_entry(TENANT_A, "inst-a", "acme-stream-line")),
        )
        await asyncio.wait_for(task, timeout=timeout)
        await agen.aclose()
        return events

    return asyncio.run(_run())


def test_sse_does_not_emit_other_tenant_logs(env):
    _client, fake, async_fake = env
    events = _drive_sse(fake, async_fake, TENANT_A)
    assert [e["tenant_id"] for e in events] == [TENANT_A], "B 的日志条目不得出现在 A 的流里"
    assert all(e["message"] == "acme-stream-line" for e in events)


def test_sse_platform_view_gets_all(env):
    _client, fake, async_fake = env
    events = _drive_sse(fake, async_fake, None)
    assert {e["tenant_id"] for e in events} == {TENANT_A, TENANT_B}


# ---------------------------------------------------------------- events / subscriptions


class TestEventsTenantScope:
    def test_event_list_only_own_events(self, env):
        client, _redis, _async_redis = env
        body = client.get("/api/events", headers=_viewer_a(client)).json()
        assert [e["event_id"] for e in body["events"]] == ["evt-a"]

        admin_body = client.get("/api/events", headers=_admin()).json()
        assert {e["event_id"] for e in admin_body["events"]} == {"evt-a", "evt-b"}

    def test_subscription_list_regression(self, env):
        client, _redis, _async_redis = env
        body = client.get("/api/events/subscriptions", headers=_viewer_a(client)).json()
        assert [s["subscription_id"] for s in body["subscriptions"]] == ["sub-a"]

    def test_cross_tenant_subscription_get_is_404(self, env):
        client, _redis, _async_redis = env
        r = client.get("/api/events/subscriptions/sub-b", headers=_viewer_a(client))
        assert r.status_code == 404
        # 不暴露存在性：平台视角能读到同一条
        admin = client.get("/api/events/subscriptions/sub-b", headers=_admin())
        assert admin.status_code == 200 and admin.json()["subscription_id"] == "sub-b"

    def test_cross_tenant_subscription_delete_is_404_and_does_not_delete(self, env):
        client, redis, _async_redis = env
        r = client.delete("/api/events/subscriptions/sub-b", headers=_editor_a(client))
        assert r.status_code == 404
        assert redis.exists("plaita:subscription:data:sub-b") == 1, "跨租户不得删除订阅"
        # 平台视角仍可删除（保持运维口径）
        admin = client.delete("/api/events/subscriptions/sub-b", headers=_admin())
        assert admin.status_code == 200
        assert redis.exists("plaita:subscription:data:sub-b") == 0

    def test_own_subscription_get_and_delete(self, env):
        client, redis, _async_redis = env
        assert (
            client.get(
                "/api/events/subscriptions/sub-a", headers=_viewer_a(client)
            ).status_code
            == 200
        )
        r = client.delete("/api/events/subscriptions/sub-a", headers=_editor_a(client))
        assert r.status_code == 200
        assert redis.exists("plaita:subscription:data:sub-a") == 0


# ---------------------------------------------------------------- queues / DLQ


class TestDlqTenantScope:
    def test_dlq_only_own_tenant_entries(self, env):
        client, _redis, _async_redis = env
        body = client.get("/api/queues/dlq", headers=_viewer_a(client)).json()
        payloads = [
            entry["payload"]
            for stream in body["streams"]
            for entry in stream["entries"]
        ]
        assert [p["secret"] for p in payloads] == ["acme-dlq-line"]

    def test_dlq_platform_view_sees_all(self, env):
        client, _redis, _async_redis = env
        body = client.get("/api/queues/dlq", headers=_admin()).json()
        payloads = [
            entry["payload"]
            for stream in body["streams"]
            for entry in stream["entries"]
        ]
        assert {p["secret"] for p in payloads} == {
            "acme-dlq-line",
            "globex-dlq-secret",
        }

    def test_dlq_tenant_paging_past_other_tenant_entries(self, env):
        """别租户条目刷屏时也要补满本租户的 count（逐页回退，非只看首页）。"""
        client, redis, _async_redis = env
        for i in range(30):
            redis.xadd(DLQ_KEY, _dlq_payload(TENANT_B, f"globex-noise-{i}"))
        for i in range(5):
            redis.xadd(DLQ_KEY, _dlq_payload(TENANT_A, f"acme-tail-{i}"))
        body = client.get(
            "/api/queues/dlq", params={"count": 5}, headers=_viewer_a(client)
        ).json()
        payloads = [
            entry["payload"]
            for stream in body["streams"]
            for entry in stream["entries"]
        ]
        assert [p["secret"] for p in payloads] == [
            f"acme-tail-{i}" for i in range(5)
        ]


def test_queue_detail_tenant_filter_regression(env):
    """#64 已修端点的回归：共享队列条目按租户过滤不回退。"""
    client, redis, _async_redis = env
    for tenant in (TENANT_A, TENANT_B):
        redis.xadd(
            "plaita:flow:queue",
            {
                "payload": json.dumps(
                    {"type": "start", "flow_id": f"flow-{tenant}", "tenant_id": tenant}
                )
            },
        )
    body = client.get("/api/queues/plaita:flow:queue", headers=_viewer_a(client)).json()
    assert [t["data"]["flow_id"] for t in body["tasks"]] == [f"flow-{TENANT_A}"]
