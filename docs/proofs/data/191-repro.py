#!/usr/bin/env python3
"""plaita#191 repro / before-after probe：事件发布/消费的租户边界。

被测面：
1. ``plaita-console/backend/api/events.py::publish_event`` —— 发布端是否把
   发布者租户写成**服务端字段**（而不是信任客户端 ``data.tenant_id``）；
2. ``plaita/server/event_filter.py::EventFilter.handle_event`` —— 消费端是否
   只读受信顶层租户、并按租户路由执行状态 + 校验订阅租户。

修复前：发布端点不取 tenant_scope，事件租户完全由 ``data.tenant_id`` 自报；
EventFilter 直接用它 ``set_current_tenant`` 去加载执行状态。租户 A 的 editor
只要知道租户 B 的 execution_id，就能给 B 的挂起执行注入 resume（审批放行）。

运行（仓库根）::

    PYTHONPATH=.:plaita-console/backend \
        /Users/jeff.jie/.venvs/plaita/bin/python docs/proofs/data/191-repro.py

退出码 0 = 安全；1 = 跨租户发布/注入链仍通（红）。
"""
from __future__ import annotations

import asyncio
import json
import sys

from fastapi import Depends, FastAPI, Request
from fakeredis import FakeRedis
from starlette.testclient import TestClient

from api import events as events_api
from plaita.event.core import Event, EventSubscription
from plaita.event.memory import InMemoryEventSubscriptionStorage
from plaita.server.event_filter import EventFilter
from plaita.server.task_queue import PAYLOAD_FIELD
from plaita.server.tenant_context import current_tenant
from plaita.storage.base import ExecutionState

QUEUE = "plaita:flow:queue"


# ── 1. 发布端：租户必须由服务端绑定 ──────────────────────────────────────

def _publish_probe() -> dict:
    fake = FakeRedis(decode_responses=True)
    app = FastAPI()
    app.state.redis = fake

    async def _auth(request: Request):
        # 复刻 require_auth 的租户落点：调用方是租户 acme 的 editor
        request.state.tenant_id = "acme"
        request.state.role = "editor"
        request.state.actor = "editor@acme"
        request.state.platform_admin = False
        return {"tenant_id": "acme"}

    app.include_router(events_api.router, prefix="/api", dependencies=[Depends(_auth)])
    client = TestClient(app)
    r = client.post(
        "/api/events/publish",
        json={
            "event_type": "approval",
            # 攻击载荷：客户端在 data 里自报受害租户 globex
            "data": {"tenant_id": "globex", "approved": True},
            "correlation_id": "exec-b",
        },
    )
    body = r.json()
    event_id = body.get("event_id")
    stored = json.loads(fake.get(f"plaita:event:{event_id}") or "{}")
    return {
        "status": r.status_code,
        "trusted_tenant": stored.get("tenant_id"),
        "data_tenant": (stored.get("data") or {}).get("tenant_id"),
        "stored": stored,
    }


# ── 2. 消费端：EventFilter 只认受信顶层租户 ─────────────────────────────

class _TenantRoutingExecutionStorage:
    """复刻 TenantRoutingExecutionStorage：按 current_tenant() 分命名空间。"""

    def __init__(self, by_tenant):
        self._by_tenant = by_tenant

    def load_execution_state(self, execution_id):
        tenant = current_tenant() or "default"
        return self._by_tenant.get(tenant, {}).get(execution_id)


def _state(execution_id: str, flow_id: str, tenant: str) -> ExecutionState:
    return ExecutionState(
        execution_id=execution_id,
        flow_id=flow_id,
        status="suspended",
        context={},
        tenant_id=tenant,
    )


def _queue_tasks(redis_client) -> list:
    tasks = []
    for _mid, fields in redis_client.xrange(QUEUE, min="-", max="+"):
        raw = fields.get(PAYLOAD_FIELD) or fields.get(PAYLOAD_FIELD.encode())
        if isinstance(raw, bytes):
            raw = raw.decode()
        tasks.append(json.loads(raw))
    return tasks


def _event_filter_probe() -> dict:
    redis_client = FakeRedis(decode_responses=True)
    execution_storage = _TenantRoutingExecutionStorage(
        {
            "globex": {"exec-b": _state("exec-b", "flow-b", "globex")},
        }
    )
    subscription_storage = InMemoryEventSubscriptionStorage()

    async def _setup():
        await subscription_storage.store_subscription(
            EventSubscription(
                event_type="approval",
                correlation_id="exec-b",
                flow_id="flow-b",
                tenant_id="globex",
            )
        )

    asyncio.run(_setup())
    event_filter = EventFilter(
        execution_storage=execution_storage,
        subscription_storage=subscription_storage,
        redis_client=redis_client,
        event_bus=None,
        queue_name=QUEUE,
        enable_subscription_timeout_checker=False,
    )

    # 2a. 伪造事件：无受信顶层租户，只在 data 里自报 globex（跨租户注入）
    spoof = Event(
        event_type="approval",
        data={"tenant_id": "globex", "approved": True},
        correlation_id="exec-b",
        source="attacker",
    )
    asyncio.run(event_filter.handle_event(spoof))
    spoof_tasks = _queue_tasks(redis_client)

    # 2b. 正向对照：受信顶层租户 = globex（发布端绑定后的正常事件）
    legit = Event(
        event_type="approval",
        data={"approved": True},
        correlation_id="exec-b",
        source="plaita-console",
        tenant_id="globex",
    )
    asyncio.run(event_filter.handle_event(legit))
    all_tasks = _queue_tasks(redis_client)

    return {
        "spoof_enqueued": len(spoof_tasks),
        "spoof_task_tenants": [t.get("tenant_id") for t in spoof_tasks],
        "legit_enqueued": len(all_tasks) - len(spoof_tasks),
        "legit_task_tenants": [t.get("tenant_id") for t in all_tasks[len(spoof_tasks):]],
    }


def run() -> dict:
    publish = _publish_probe()
    consume = _event_filter_probe()

    publish_safe = (
        publish["trusted_tenant"] == "acme" and publish["data_tenant"] is None
    )
    consume_safe = (
        consume["spoof_enqueued"] == 0
        and consume["legit_enqueued"] == 1
        and consume["legit_task_tenants"] == ["globex"]
    )
    return {
        "python": sys.version.split()[0],
        "publish": publish,
        "consume": consume,
        "publish_safe": publish_safe,
        "consume_safe": consume_safe,
        "verdict": (
            "SAFE（发布端绑定租户 + 消费端只认受信租户）"
            if publish_safe and consume_safe
            else "VULNERABLE（跨租户发布/注入链仍通）"
        ),
    }


if __name__ == "__main__":
    report = run()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    raise SystemExit(0 if report["verdict"].startswith("SAFE") else 1)
