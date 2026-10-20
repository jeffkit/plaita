"""plaita#191：事件发布/消费的租户边界（受信顶层 tenant_id）。

历史缺陷：
- console ``POST /api/events/publish`` 不绑定发布者租户，事件租户完全由
  ``data.tenant_id`` 自报；事件键/频道也是全局的；
- ``EventFilter.handle_event`` 直接 ``set_current_tenant(data["tenant_id"])``
  去加载执行状态，并用事件自报租户入队 resume。

攻击：租户 A 的 editor 知道租户 B 的 execution_id 后，发一条
``correlation_id=B 的 execution`` + ``data.tenant_id=B`` 的事件，就能给 B 的
挂起执行注入 resume（审批放行）。

本组断言：
1. 发布端用 ``tenant_scope(required=True)`` 绑定受信租户；客户端 ``data``
   里的 tenant_id 被剥离；
2. 消费端只读受信顶层字段，``data.tenant_id`` 的自报值不再能切租户；
3. 状态租户与事件租户不一致（非租户路由存储也成立）→ 丢弃；
4. 订阅租户与事件租户不一致 → 不入队；
5. 正向对照：受信租户事件正常入队，resume 任务携带该租户；
6. 服务内部 resume 事件（delay/approval/http_callback 共用入口）写受信租户；
7. 事件回扫器逐事件按其受信租户回放——两个租户各入队一次（#79/#85/#153
   补偿链不因租户改造漏扫）。
"""
import asyncio
import json
import sys
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI, Request
from fakeredis import FakeRedis
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import events as events_api  # noqa: E402

from plaita.event.core import Event, EventSubscription  # noqa: E402
from plaita.event.memory import InMemoryEventSubscriptionStorage  # noqa: E402
from plaita.event.redis import RedisEventStorage  # noqa: E402
from plaita.server.event_filter import EventFilter  # noqa: E402
from plaita.server.event_reconcile import EventReconciler  # noqa: E402
from plaita.server.services.base_service import BaseExtendedService  # noqa: E402
from plaita.server.task_queue import PAYLOAD_FIELD  # noqa: E402
from plaita.server.tenant_context import current_tenant  # noqa: E402
from plaita.storage.base import ExecutionState  # noqa: E402
from plaita.storage.memory import MemoryExecutionStorage  # noqa: E402

QUEUE = "plaita:flow:queue"
TENANT_A = "acme"
TENANT_B = "globex"


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _state(execution_id: str, flow_id: str, tenant: str) -> ExecutionState:
    return ExecutionState(
        execution_id=execution_id,
        flow_id=flow_id,
        status="suspended",
        context={},
        tenant_id=tenant,
    )


class _TenantRoutingExecutionStorage:
    """复刻 TenantRoutingExecutionStorage：按 current_tenant() 分命名空间。"""

    def __init__(self, by_tenant):
        self._by_tenant = by_tenant

    def load_execution_state(self, execution_id):
        tenant = current_tenant() or "default"
        return self._by_tenant.get(tenant, {}).get(execution_id)


def _queue_tasks(redis_client, queue: str = QUEUE):
    tasks = []
    for _mid, fields in redis_client.xrange(queue, min="-", max="+"):
        raw = fields.get(PAYLOAD_FIELD) or fields.get(PAYLOAD_FIELD.encode())
        if isinstance(raw, bytes):
            raw = raw.decode()
        tasks.append(json.loads(raw))
    return tasks


def _make_filter(execution_storage, subscriptions, redis_client):
    subscription_storage = InMemoryEventSubscriptionStorage()

    async def _setup():
        for sub in subscriptions:
            await subscription_storage.store_subscription(sub)

    _run(_setup())
    return EventFilter(
        execution_storage=execution_storage,
        subscription_storage=subscription_storage,
        redis_client=redis_client,
        event_bus=None,
        queue_name=QUEUE,
        enable_subscription_timeout_checker=False,
    )


def _bound_subscription(execution_id: str, flow_id: str, tenant: str) -> EventSubscription:
    return EventSubscription(
        event_type="approval",
        correlation_id=execution_id,
        flow_id=flow_id,
        tenant_id=tenant,
    )


# ---------------------------------------------------------------- 发布端


def _publish_client(redis_client: FakeRedis, tenant: str) -> TestClient:
    app = FastAPI()
    app.state.redis = redis_client

    async def _auth(request: Request):
        request.state.tenant_id = tenant
        request.state.role = "editor"
        request.state.actor = f"editor@{tenant}"
        request.state.platform_admin = False
        return {"tenant_id": tenant}

    app.include_router(events_api.router, prefix="/api", dependencies=[Depends(_auth)])
    return TestClient(app)


def test_publish_binds_publisher_tenant_and_strips_client_data_tenant():
    fake = FakeRedis(decode_responses=True)
    client = _publish_client(fake, TENANT_A)
    r = client.post(
        "/api/events/publish",
        json={
            "event_type": "approval",
            "data": {"tenant_id": TENANT_B, "approved": True},
            "correlation_id": "exec-b",
        },
    )
    assert r.status_code == 200, r.text
    event_id = r.json()["event_id"]
    stored = json.loads(fake.get(f"plaita:event:events:{event_id}"))
    assert stored["tenant_id"] == TENANT_A, "发布者租户必须由服务端绑定"
    assert "tenant_id" not in stored["data"], "客户端自报租户必须被剥离"
    assert stored["data"]["approved"] is True
    assert stored["source"] == "plaita-console"


def test_publish_uses_request_tenant_even_if_data_claims_other():
    fake = FakeRedis(decode_responses=True)
    client = _publish_client(fake, TENANT_B)
    r = client.post(
        "/api/events/publish",
        json={"event_type": "approval", "data": {"tenant_id": TENANT_A}},
    )
    stored = json.loads(fake.get(f"plaita:event:events:{r.json()['event_id']}"))
    assert stored["tenant_id"] == TENANT_B


# ---------------------------------------------------------------- 消费端


class TestEventFilterTenantIsolation:
    def test_spoofed_data_tenant_does_not_resume_other_tenant(self):
        redis_client = FakeRedis(decode_responses=True)
        storage = _TenantRoutingExecutionStorage(
            {TENANT_B: {"exec-b": _state("exec-b", "flow-b", TENANT_B)}}
        )
        event_filter = _make_filter(
            storage, [_bound_subscription("exec-b", "flow-b", TENANT_B)], redis_client
        )
        spoof = Event(
            event_type="approval",
            data={"tenant_id": TENANT_B, "approved": True},
            correlation_id="exec-b",
            source="attacker",
        )
        _run(event_filter.handle_event(spoof))
        assert _queue_tasks(redis_client) == [], "data.tenant_id 不得用于切租户"

    def test_trusted_tenant_event_resumes(self):
        redis_client = FakeRedis(decode_responses=True)
        storage = _TenantRoutingExecutionStorage(
            {TENANT_B: {"exec-b": _state("exec-b", "flow-b", TENANT_B)}}
        )
        event_filter = _make_filter(
            storage, [_bound_subscription("exec-b", "flow-b", TENANT_B)], redis_client
        )
        legit = Event(
            event_type="approval",
            data={"approved": True},
            correlation_id="exec-b",
            source="plaita-console",
            tenant_id=TENANT_B,
        )
        _run(event_filter.handle_event(legit))
        tasks = _queue_tasks(redis_client)
        assert len(tasks) == 1
        assert tasks[0]["tenant_id"] == TENANT_B
        assert tasks[0]["execution_id"] == "exec-b"
        assert tasks[0]["resume_type"] == "event"

    def test_state_tenant_mismatch_dropped_without_tenant_routing(self):
        """非租户路由存储（内存）也不得跨租户推进：强校验状态自报租户。"""
        redis_client = FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-b", _state("exec-b", "flow-b", TENANT_B))
        event_filter = _make_filter(
            storage, [_bound_subscription("exec-b", "flow-b", TENANT_B)], redis_client
        )
        cross = Event(
            event_type="approval",
            data={"tenant_id": TENANT_B},
            correlation_id="exec-b",
            tenant_id=TENANT_A,  # 受信字段是 A，但状态属于 B
        )
        _run(event_filter.handle_event(cross))
        assert _queue_tasks(redis_client) == []

    def test_subscription_tenant_mismatch_dropped(self):
        redis_client = FakeRedis(decode_responses=True)
        storage = _TenantRoutingExecutionStorage(
            {TENANT_B: {"exec-b": _state("exec-b", "flow-b", TENANT_B)}}
        )
        # 状态租户与事件租户一致，但订阅记录属于另一租户
        event_filter = _make_filter(
            storage, [_bound_subscription("exec-b", "flow-b", TENANT_A)], redis_client
        )
        event = Event(
            event_type="approval",
            # 受信字段与 data 都指向 B：修复前 data 自报即可加载状态并入队；
            # 修复后只用受信字段，且订阅租户（A）不匹配 → 丢弃。
            data={"tenant_id": TENANT_B},
            correlation_id="exec-b",
            tenant_id=TENANT_B,
        )
        _run(event_filter.handle_event(event))
        assert _queue_tasks(redis_client) == []


# ---------------------------------------------------------------- 服务内部 resume 事件


class _RecordingService(BaseExtendedService):
    def get_service_type(self) -> str:
        return "w37-test"

    def start_service(self) -> bool:
        return True

    def stop_service(self) -> bool:
        return True

    async def handle_task(self, task_config):
        return True


def test_service_resume_event_carries_trusted_tenant():
    fake = FakeRedis(decode_responses=True)
    svc = _RecordingService(event_bus=None, redis_client=fake, enable_registry=False)
    pubsub = fake.pubsub()
    pubsub.subscribe("plaita:events:approval")
    _run(
        svc.publish_resume_event(
            "approval",
            {
                "execution_id": "exec-b",
                "flow_id": "flow-b",
                "tenant_id": TENANT_B,
                "approved": True,
            },
        )
    )
    message = None
    for _ in range(100):
        message = pubsub.get_message(timeout=0.02)
        if message and message.get("type") == "message":
            break
    assert message is not None, "resume 事件未发布到 plaita:events:approval"
    payload = json.loads(message["data"])
    assert payload["tenant_id"] == TENANT_B, "服务内部 resume 事件必须写受信租户"
    assert payload["data"]["tenant_id"] == TENANT_B  # 对外 data 形状不变
    svc.thread_pool.shutdown(wait=False)


# ---------------------------------------------------------------- 回扫补偿


def test_reconciler_replays_both_tenants_once():
    import fakeredis

    server = fakeredis.FakeServer()
    sync_redis = fakeredis.FakeRedis(server=server, decode_responses=True)
    event_storage = RedisEventStorage(redis_url="redis://localhost:6379/15", ttl=3600)
    event_storage.redis = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)

    storage = _TenantRoutingExecutionStorage(
        {
            TENANT_A: {"exec-a": _state("exec-a", "flow-a", TENANT_A)},
            TENANT_B: {"exec-b": _state("exec-b", "flow-b", TENANT_B)},
        }
    )
    event_filter = _make_filter(
        storage,
        [
            _bound_subscription("exec-a", "flow-a", TENANT_A),
            _bound_subscription("exec-b", "flow-b", TENANT_B),
        ],
        sync_redis,
    )

    async def _seed():
        await event_storage.store_event(
            Event(
                event_type="approval",
                data={},
                correlation_id="exec-a",
                tenant_id=TENANT_A,
            )
        )
        await event_storage.store_event(
            Event(
                event_type="approval",
                data={},
                correlation_id="exec-b",
                tenant_id=TENANT_B,
            )
        )

    _run(_seed())
    reconciler = EventReconciler(
        event_storage,
        event_filter,
        redis_client=sync_redis,
        backfill_seconds=3600.0,
        batch_size=50,
    )
    replayed = _run(reconciler.scan_once())
    assert replayed == 2
    tasks = _queue_tasks(sync_redis)
    assert sorted(t["execution_id"] for t in tasks) == ["exec-a", "exec-b"]
    assert {t["execution_id"]: t["tenant_id"] for t in tasks} == {
        "exec-a": TENANT_A,
        "exec-b": TENANT_B,
    }
