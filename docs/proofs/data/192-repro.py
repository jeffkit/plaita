#!/usr/bin/env python3
"""plaita#192 repro / before-after probe：console 多租户读面残面。

逐端点探针（租户 acme 的 viewer / editor vs 平台全量视角）：

* ``GET /api/logs/instance/{id}`` —— 他租户实例日志
* ``GET /api/logs/stats`` —— 全平台日志统计
* ``GET /api/logs/stream``（SSE）—— 全平台日志频道
* ``GET /api/events`` —— 全平台事件
* ``GET/DELETE /api/events/subscriptions/{id}`` —— 他租户订阅（含删除）
* ``GET /api/queues/dlq`` —— 他租户死信 payload

修复前：这些端点均无租户过滤（#63/#64 波次只覆盖了被点名的端点）。
修复后：逐端点按条目/记录/频道的 ``tenant_id`` 收窄；平台视角（无租户
上下文）保持全量。

运行（仓库根）::

    PYTHONPATH=.:plaita-console/backend \
        /Users/jeff.jie/.venvs/plaita/bin/python docs/proofs/data/192-repro.py

退出码 0 = 安全；1 = 仍有跨租户读/删（红）。
"""
from __future__ import annotations

import asyncio
import json
import sys
from types import SimpleNamespace

from fastapi import Depends, FastAPI, Request
from fakeredis import FakeRedis
from starlette.testclient import TestClient

from api import events as events_api
from api import logs as logs_api
from api import queues as queues_api

QUEUE_DLQ = "plaita:flow:queue:dlq"


def _log_entry(tenant: str, instance: str, message: str) -> dict:
    return {
        "timestamp": "2026-10-18T09:00:00",
        "level": "ERROR",
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
        # 受信顶层租户（#191 修复后由发布端写入）
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


def _seed(fake: FakeRedis) -> None:
    fake.xadd(
        "plaita:logs:flow_worker:inst-b",
        _log_entry("globex", "inst-b", "globex-secret-line"),
    )
    fake.xadd(
        "plaita:logs:flow_worker:inst-a",
        _log_entry("acme", "inst-a", "acme-line"),
    )
    fake.set(f"plaita:event:{'evt-b'}", json.dumps(_event("evt-b", "globex")))
    fake.set(f"plaita:event:{'evt-a'}", json.dumps(_event("evt-a", "acme")))
    fake.set(
        "plaita:subscription:data:sub-b",
        json.dumps(_subscription("sub-b", "globex")),
    )
    fake.set(
        "plaita:subscription:data:sub-a",
        json.dumps(_subscription("sub-a", "acme")),
    )
    fake.sadd("plaita:subscription:type:approval", "sub-b")
    fake.xadd(
        QUEUE_DLQ,
        {
            "payload": json.dumps(
                {
                    "reason": "retry_exhausted",
                    "source_stream": "plaita:flow:queue:v2",
                    "source_id": "1-0",
                    "delivery_count": 9,
                    "dead_lettered_at": 1.0,
                    "payload": {
                        "tenant_id": "globex",
                        "flow_id": "flow-b",
                        "secret": "globex-dlq-secret",
                    },
                }
            )
        },
    )


def _client(fake: FakeRedis) -> TestClient:
    app = FastAPI()
    app.state.redis = fake
    app.state.local_mode = False

    async def _auth(request: Request):
        tenant = request.headers.get("X-Test-Tenant")
        request.state.tenant_id = tenant or None
        request.state.role = "viewer"
        request.state.actor = "probe"
        request.state.platform_admin = tenant is None
        return {"tenant_id": tenant or None}

    app.include_router(events_api.router, prefix="/api", dependencies=[Depends(_auth)])
    app.include_router(logs_api.router, prefix="/api", dependencies=[Depends(_auth)])
    app.include_router(queues_api.router, prefix="/api", dependencies=[Depends(_auth)])
    return TestClient(app)


class _StubRequest:
    """stream_logs 只用 request.is_disconnected 与 request.state.tenant_id。"""

    def __init__(self, tenant: str | None):
        self.app = SimpleNamespace(state=SimpleNamespace(local_mode=False))
        self.state = SimpleNamespace(tenant_id=tenant, role="viewer")

    async def is_disconnected(self):
        await asyncio.sleep(0)
        return False


def _drive_sse(fake: FakeRedis, tenant: str | None, timeout: float = 8.0) -> list:
    """起 stream_logs → 依次发 globex、acme 消息 → 收集事件（见到 acme 即停）。"""

    async def _run():
        resp = await logs_api.stream_logs(
            request=_StubRequest(tenant),
            service_type=None,
            instance_id=None,
            level=None,
            redis=fake,
        )
        agen = resp.body_iterator
        events: list = []

        async def consume():
            while True:
                ev = await agen.__anext__()
                if ev.get("event") == "log":
                    events.append(json.loads(ev["data"]))
                    if events[-1].get("tenant_id") == "acme":
                        return

        task = asyncio.create_task(consume())
        await asyncio.sleep(0.3)  # psubscribe 生效
        fake.publish(
            "plaita:logs:stream:flow_worker:inst-b",
            json.dumps(_log_entry("globex", "inst-b", "globex-stream-secret")),
        )
        await asyncio.sleep(0.1)
        fake.publish(
            "plaita:logs:stream:flow_worker:inst-a",
            json.dumps(_log_entry("acme", "inst-a", "acme-stream-line")),
        )
        await asyncio.wait_for(task, timeout=timeout)
        await agen.aclose()
        return events

    return asyncio.run(_run())


def run() -> dict:
    fake = FakeRedis(decode_responses=True)
    _seed(fake)
    client = _client(fake)
    ta = {"X-Test-Tenant": "acme"}
    admin = {}

    inst = client.get("/api/logs/instance/inst-b", headers=ta).json()
    stats = client.get("/api/logs/stats", headers=ta).json()
    events = client.get("/api/events", headers=ta).json()
    admin_events = client.get("/api/events", headers=admin).json()
    sub_get = client.get("/api/events/subscriptions/sub-b", headers=ta)
    sub_get_admin = client.get("/api/events/subscriptions/sub-b", headers=admin)
    dlq = client.get("/api/queues/dlq", headers=ta).json()
    dlq_admin = client.get("/api/queues/dlq", headers=admin).json()
    sse_events = _drive_sse(fake, "acme")

    # DELETE 放最后：修复前会把 globex 订阅删掉
    sub_del = client.delete("/api/events/subscriptions/sub-b", headers=ta)
    sub_b_still_exists = bool(fake.exists("plaita:subscription:data:sub-b"))

    return {
        "python": sys.version.split()[0],
        "logs_instance": {
            "messages": [row["message"] for row in inst.get("logs", [])],
            "leaked": any(row["message"] == "globex-secret-line" for row in inst.get("logs", [])),
        },
        "logs_stats": {
            "total_logs": stats.get("total_logs"),
            "levels": sorted({s["level"] for s in stats.get("stats", [])}),
            "leaked": (stats.get("total_logs") or 0) > 1,
        },
        "sse": {
            "tenant_ids": [e.get("tenant_id") for e in sse_events],
            "leaked": any(e.get("tenant_id") == "globex" for e in sse_events),
        },
        "events": {
            "acme_view_ids": [e["event_id"] for e in events.get("events", [])],
            "admin_view_ids": [e["event_id"] for e in admin_events.get("events", [])],
            "leaked": any(e["event_id"] == "evt-b" for e in events.get("events", [])),
        },
        "subscription_get": {
            "status": sub_get.status_code,
            "admin_status": sub_get_admin.status_code,
            "leaked": sub_get.status_code == 200,
        },
        "subscription_delete": {
            "status": sub_del.status_code,
            "still_exists": sub_b_still_exists,
            "leaked": sub_del.status_code == 200,
        },
        "dlq": {
            "acme_payloads": [
                entry.get("payload", {})
                for stream in dlq.get("streams", [])
                for entry in stream.get("entries", [])
            ],
            "admin_entry_count": sum(
                len(stream.get("entries", [])) for stream in dlq_admin.get("streams", [])
            ),
            "leaked": any(
                entry.get("payload", {}).get("tenant_id") == "globex"
                for stream in dlq.get("streams", [])
                for entry in stream.get("entries", [])
            ),
        },
        "verdict": (
            "SAFE（逐端点按租户收窄，平台视角仍全量）"
            if not any(
                section["leaked"]
                for section in (
                    {"leaked": any(row["message"] == "globex-secret-line" for row in inst.get("logs", []))},
                    {"leaked": (stats.get("total_logs") or 0) > 1},
                    {"leaked": any(e.get("tenant_id") == "globex" for e in sse_events)},
                    {"leaked": any(e["event_id"] == "evt-b" for e in events.get("events", []))},
                    {"leaked": sub_get.status_code == 200},
                    {"leaked": sub_del.status_code == 200 or not sub_b_still_exists},
                    {
                        "leaked": any(
                            entry.get("payload", {}).get("tenant_id") == "globex"
                            for stream in dlq.get("streams", [])
                            for entry in stream.get("entries", [])
                        )
                    },
                )
            )
            else "VULNERABLE（仍有跨租户读/删）"
        ),
    }


if __name__ == "__main__":
    report = run()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    raise SystemExit(0 if report["verdict"].startswith("SAFE") else 1)
