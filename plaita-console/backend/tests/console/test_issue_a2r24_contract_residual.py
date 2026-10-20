"""A2R24 契约族余项回归（B1 第四十七轮：02/05/07/08/09）。

覆盖：

* **A2R24-02**：``/api/queues/{name}`` 的 Stream ``start`` 是**索引**（5 条
  ``start=2&count=2`` 必须返回 n=2,3 且 index=2,3），并可用 ``start_id``
  排他游标继续；``start`` 越界 422。
* **A2R24-05**：``/events``、``/logs``、``/logs/instance`` 的 ``total`` 是
  匹配总数、``returned`` 是本页条数、``truncated`` 表示还有更多、``next_cursor``
  可翻完全部历史；``limit=0``/负数 422；``order=bogus`` 422。
* **A2R24-07**：tenant 详情补 ``member_count``；晋升包 ``tenant_id`` 取实体
  租户；平台全量视角同名 flow 跨租户时 409（不再静默 ``.first()``）。
* **A2R24-08**：``/schedules/{id}/trigger`` 本地/集群同一字段集
  （``success/mode/execution_id/msg_id``）。
* **A2R24-09**：两个 SSE 路由的 OpenAPI 200 媒体类型是 ``text/event-stream``。

负控（clean pre-fix 行）：
- queue：``start=2`` 返回 n=0,1 且 index=0,1 → 红；
- events：8 条 ``limit=3`` → ``total=3`` 且无 ``truncated/next_cursor`` → 红；
- logs：5 条 ``limit=2`` → ``total=2``、无法翻页 → 红；
- ``limit=0`` 旧行为 1 条/空 + degraded（200）→ 红；
- trigger：本地档只有 ``execution_id``、集群档只有 ``msg_id`` → 红；
- SSE：OpenAPI 200 是 ``application/json`` → 红；
- tenant 详情无 ``member_count``、导出 ``tenant_id=""``、同名 flow 静默选择 → 红。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import fakeredis
import pytest
from fastapi import FastAPI, Request
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import events as events_api  # noqa: E402
from api import executions as executions_api  # noqa: E402
from api import flows as flows_api  # noqa: E402
from api import logs as logs_api  # noqa: E402
from api import queues as queues_api  # noqa: E402
from api import schedules as schedules_api  # noqa: E402
from plaita.event.redis import event_record_key  # noqa: E402
from plaita.server.services.schedule_service import SCHEDULES_KEY  # noqa: E402
from services import flow_store, tenants_svc, users_svc  # noqa: E402

QUEUE = "plaita:flow:queue"


def _echo_def(flow_id: str = "shared") -> str:
    return json.dumps(
        {
            "flow_id": flow_id,
            "desc": "echo",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "end"},
                {"type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success"},
            ],
        }
    )


def _tenant_middleware(app: FastAPI) -> None:
    """按 ``X-Tenant-ID`` 填 request.state.tenant_id（模拟 require_auth 的绑定）。"""

    @app.middleware("http")
    async def _bind_tenant(request: Request, call_next):
        header_tenant = request.headers.get("X-Tenant-ID")
        if header_tenant:
            request.state.tenant_id = header_tenant
        return await call_next(request)


@pytest.fixture()
def queue_client():
    redis = fakeredis.FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(queues_api.router, prefix="/api")
    return TestClient(app), redis


@pytest.fixture()
def events_client():
    redis = fakeredis.FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(events_api.router, prefix="/api")
    return TestClient(app), redis


@pytest.fixture()
def logs_client():
    redis = fakeredis.FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(logs_api.router, prefix="/api")
    return TestClient(app), redis


@pytest.fixture()
def local_logs_client(tmp_path):
    flow_store.init_engine(f"sqlite:///{tmp_path / 'logs.db'}")
    app = FastAPI()
    app.state.redis = None
    app.state.local_mode = True
    app.state.store = flow_store.get_flow_store()
    app.include_router(logs_api.router, prefix="/api")
    return TestClient(app)


@pytest.fixture()
def schedule_client():
    redis = fakeredis.FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    app.include_router(schedules_api.router, prefix="/api")
    return TestClient(app), redis


class TestQueueStreamPagination:
    def test_stream_start_is_index_not_stream_id(self, queue_client):
        client, redis = queue_client
        ids = []
        for i in range(5):
            ids.append(
                redis.xadd(
                    QUEUE,
                    {"payload": json.dumps({"n": i, "tenant_id": "default"})},
                )
            )

        first = client.get(f"/api/queues/{QUEUE}?start=0&count=2").json()
        second = client.get(f"/api/queues/{QUEUE}?start=2&count=2").json()
        third = client.get(f"/api/queues/{QUEUE}?start=4&count=2").json()

        assert [task["data"]["n"] for task in second["tasks"]] == [2, 3]
        assert [task["index"] for task in second["tasks"]] == [2, 3]
        assert first["tasks"][0]["data"]["n"] == 0
        assert third["tasks"][0]["data"]["n"] == 4
        assert second["truncated"] is True
        assert third["truncated"] is False
        assert second["next_start_id"] == ids[3]

        # 深队列走排他游标：与索引分页同一页内容（index 是批内序号）。
        cursor_page = client.get(
            f"/api/queues/{QUEUE}?start_id={second['next_start_id']}&count=2"
        ).json()
        assert [task["data"]["n"] for task in cursor_page["tasks"]] == [4]
        assert cursor_page["truncated"] is False

    def test_start_bounds_and_invalid_cursor(self, queue_client):
        client, redis = queue_client
        redis.rpush("plaita:delay:queue", json.dumps({"n": 0}))
        assert client.get(
            f"/api/queues/{QUEUE}?start=10001"
        ).status_code == 422
        assert client.get(
            f"/api/queues/{QUEUE}?start_id=not-a-stream-id"
        ).status_code == 422

    def test_list_pagination_keeps_index_semantics(self, queue_client):
        client, redis = queue_client
        for i in range(5):
            redis.rpush("plaita:delay:queue", json.dumps({"n": i}))
        body = client.get("/api/queues/plaita:delay:queue?start=2&count=2").json()
        assert [task["data"]["n"] for task in body["tasks"]] == [2, 3]
        assert [task["index"] for task in body["tasks"]] == [2, 3]
        assert body["truncated"] is True
        assert body["next_start_id"] is None


class TestEventsPagination:
    def _seed(self, redis, count: int = 8) -> None:
        for i in range(count):
            redis.set(
                event_record_key(f"e{i}"),
                json.dumps(
                    {
                        "event_id": f"e{i}",
                        "event_type": "contract.type",
                        "timestamp": 1_700_000_000.0 + i,
                        "data": {"n": i},
                    }
                ),
            )

    def test_total_truncated_and_cursor_reaches_all(self, events_client):
        client, redis = events_client
        self._seed(redis)

        page = client.get("/api/events?limit=3").json()
        assert page["total"] == 8
        assert page["returned"] == 3
        assert page["truncated"] is True
        assert page["next_cursor"]

        seen = [event["event_id"] for event in page["events"]]
        cursor = page["next_cursor"]
        for _ in range(10):
            if not cursor:
                break
            follow = client.get(f"/api/events?limit=3&cursor={cursor}").json()
            seen.extend(event["event_id"] for event in follow["events"])
            cursor = follow["next_cursor"]
        assert len(seen) == 8
        assert len(set(seen)) == 8
        assert cursor is None

    def test_limit_zero_and_bad_cursor_rejected(self, events_client):
        client, _redis = events_client
        assert client.get("/api/events?limit=0").status_code == 422
        assert client.get("/api/events?limit=-1").status_code == 422
        assert client.get("/api/events?cursor=bogus").status_code == 422


class TestLogsPagination:
    def _seed(self, redis, count: int = 5) -> None:
        for i in range(count):
            redis.xadd(
                "plaita:logs:svc:inst1",
                {
                    "timestamp": f"2026-10-21T00:00:0{i}",
                    "level": "INFO",
                    "service_type": "svc",
                    "instance_id": "inst1",
                    "message": f"m{i}",
                },
            )

    def test_logs_total_and_cursor_reaches_all(self, logs_client):
        client, redis = logs_client
        self._seed(redis)

        page = client.get("/api/logs?limit=2").json()
        assert page["returned"] == 2
        assert page["total"] > 2, "total 不能再是「本页条数」"
        assert page["truncated"] is True
        assert page["next_cursor"]

        seen = [row["message"] for row in page["logs"]]
        cursor = page["next_cursor"]
        for _ in range(10):
            if not cursor:
                break
            follow = client.get(f"/api/logs?limit=2&cursor={cursor}").json()
            seen.extend(row["message"] for row in follow["logs"])
            cursor = follow["next_cursor"]
        assert sorted(seen) == ["m0", "m1", "m2", "m3", "m4"]
        assert cursor is None

    def test_logs_limit_zero_rejected(self, logs_client):
        client, _redis = logs_client
        assert client.get("/api/logs?limit=0").status_code == 422
        assert client.get("/api/logs?limit=-1").status_code == 422

    def test_instance_logs_order_validation_and_both_directions(self, logs_client):
        client, redis = logs_client
        self._seed(redis)

        assert client.get(
            "/api/logs/instance/inst1?order=bogus"
        ).status_code == 422

        def _collect(order: str) -> list:
            seen: list = []
            cursor = None
            for _ in range(10):
                url = f"/api/logs/instance/inst1?limit=2&order={order}"
                if cursor:
                    url += f"&cursor={cursor}"
                body = client.get(url).json()
                seen.extend(row["message"] for row in body["logs"])
                cursor = body["next_cursor"]
                if not cursor:
                    break
            return seen

        assert _collect("desc") == ["m4", "m3", "m2", "m1", "m0"]
        assert _collect("asc") == ["m0", "m1", "m2", "m3", "m4"]

    def test_local_logs_cursor_pagination(self, local_logs_client):
        for i in range(5):
            flow_store.insert_local_log("exec-1", "INFO", "test", f"m{i}")

        seen: list = []
        cursor = None
        for _ in range(10):
            url = "/api/logs?limit=2"
            if cursor:
                url += f"&cursor={cursor}"
            body = local_logs_client.get(url).json()
            seen.extend(row["message"] for row in body["logs"])
            cursor = body["next_cursor"]
            if not cursor:
                break
        assert sorted(seen) == ["m0", "m1", "m2", "m3", "m4"]


class TestTriggerResponseShape:
    def test_local_trigger_returns_unified_shape(self, monkeypatch):
        class _Local:
            def get_schedule(self, store, schedule_id, tenant_id=None):
                return {"schedule_id": schedule_id, "tenant_id": "default"}

            def trigger_now(self, store, schedule):
                return "exec-local-1"

        monkeypatch.setattr(schedules_api, "_get_local", lambda request: _Local())
        monkeypatch.setattr(schedules_api, "_tenant_is_active", lambda request, tenant: True)
        app = FastAPI()
        app.state.redis = None
        app.state.local_mode = True
        app.include_router(schedules_api.router, prefix="/api")
        client = TestClient(app)

        body = client.post("/api/schedules/s1/trigger").json()
        assert set(body) == {"success", "mode", "execution_id", "msg_id"}
        assert body["mode"] == "local"
        assert body["execution_id"] == "exec-local-1"
        assert body["msg_id"] is None

    def test_cluster_trigger_returns_unified_shape(self, schedule_client):
        client, redis = schedule_client
        redis.hset(
            SCHEDULES_KEY,
            "s1",
            json.dumps(
                {
                    "schedule_id": "s1",
                    "tenant_id": "default",
                    "name": "n",
                    "flow_id": "f",
                    "version": "1.0.0",
                    "cron": "* * * * *",
                    "params": {},
                    "enabled": True,
                    "created_at": "t0",
                    "updated_at": "t0",
                    "next_run_at": "0",
                }
            ),
        )

        body = client.post("/api/schedules/s1/trigger").json()
        assert set(body) == {"success", "mode", "execution_id", "msg_id"}
        assert body["mode"] == "cluster"
        assert body["execution_id"] is None
        assert body["msg_id"]


class TestSseOpenApiMediaType:
    def test_streaming_routes_declare_event_stream(self):
        app = FastAPI()
        app.state.redis = None
        app.state.local_mode = False
        app.include_router(logs_api.router, prefix="/api")
        app.include_router(flows_api.router, prefix="/api")
        app.include_router(executions_api.router, prefix="/api")
        schema = app.openapi()

        for path, method in (
            ("/api/logs/stream", "get"),
            ("/api/flows/ai-generate/stream", "post"),
        ):
            content = schema["paths"][path][method]["responses"]["200"]["content"]
            # 不能再同时声明 application/json（生成的客户端会按 JSON 解析 SSE）。
            assert set(content) == {"text/event-stream"}, (path, content)

        ticket = schema["components"]["schemas"].get("StreamTicketResponse")
        assert ticket and {"ticket", "expires_in"} <= set(ticket["properties"])


class TestProjectionResiduals:
    def test_tenant_detail_has_member_count(self, tmp_path):
        flow_store.init_engine(f"sqlite:///{tmp_path / 'tenants.db'}")
        flow_store.ensure_tenant_bootstrap()
        store = flow_store.get_flow_store()
        tenants_svc.create_tenant(store, "acme", "Acme")
        users_svc.create_user(store, "bob", "s3cret-pass", "viewer")
        users_svc.add_member(store, "acme", "bob", "viewer")

        detail = tenants_svc.get_tenant(store, "acme")
        listed = {row["id"]: row for row in tenants_svc.list_tenants(store)}
        assert detail["member_count"] == 1
        assert detail["member_count"] == listed["acme"]["member_count"]

    @pytest.fixture()
    def platform_flow_client(self, tmp_path):
        flow_store.init_engine(f"sqlite:///{tmp_path / 'flows.db'}")
        store = flow_store.get_flow_store()
        for tenant in ("default", "acme"):
            store.ensure_flow("shared", tenant_id=tenant)
            store.save_flow_definition(
                "shared", "1.0.0", _echo_def(), tenant_id=tenant
            )
        app = FastAPI()
        app.state.redis = None
        app.state.local_mode = True
        app.state.store = store
        _tenant_middleware(app)
        app.include_router(flows_api.router, prefix="/api")
        return TestClient(app)

    def test_ambiguous_flow_id_is_explicit_409(self, platform_flow_client):
        client = platform_flow_client
        for url in (
            "/api/flows/shared",
            "/api/flows/shared/versions/1.0.0",
            "/api/flows/shared/versions/1.0.0/export",
        ):
            response = client.get(url)
            assert response.status_code == 409, (url, response.text)
            detail = response.json()["detail"]
            assert detail["code"] == "ambiguous_flow_id"
            assert detail["available_tenants"] == ["acme", "default"]

    def test_export_package_carries_entity_tenant(self, platform_flow_client):
        client = platform_flow_client
        response = client.get(
            "/api/flows/shared/versions/1.0.0/export",
            headers={"X-Tenant-ID": "acme"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["tenant_id"] == "acme"

        detail = client.get(
            "/api/flows/shared", headers={"X-Tenant-ID": "acme"}
        ).json()
        assert detail["tenant_id"] == "acme"

    def test_platform_export_fills_source_tenant(self, tmp_path):
        """平台视角（无 X-Tenant-ID）导出单租户版本：tenant_id 取实体租户。

        pre-fix 取调用方 ``tenant_scope``（None → ``""``），晋升包丢失源租户。
        """
        flow_store.init_engine(f"sqlite:///{tmp_path / 'single.db'}")
        store = flow_store.get_flow_store()
        store.ensure_flow("solo", tenant_id="default")
        store.save_flow_definition("solo", "1.0.0", _echo_def("solo"), tenant_id="default")

        app = FastAPI()
        app.state.redis = None
        app.state.local_mode = True
        app.state.store = store
        app.include_router(flows_api.router, prefix="/api")
        client = TestClient(app)

        response = client.get("/api/flows/solo/versions/1.0.0/export")
        assert response.status_code == 200, response.text
        assert response.json()["tenant_id"] == "default"
