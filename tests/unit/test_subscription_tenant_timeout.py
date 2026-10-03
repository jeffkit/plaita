"""P3（2026-10-02 评审遗留 #6）：订阅超时恢复在非 default 租户下失效。

缺陷链：挂起订阅不携带租户（租户只随事件数据传递，超时路径没有事件载体）
→ ``EventFilter._on_subscription_timeout`` 按 default 命名空间加载执行状态
→ 非 default 租户的挂起执行超时后加载不到 state → 跳过 → 超时 resume 机制
整体失效。

修复语义（本文件钉死）：
1. 挂起点 ``_subscribe_event`` 把当前租户写进订阅（``tenant_id`` 字段，
   default 租户保持历史参数集零变化）；
2. 消费点 ``_on_subscription_timeout`` 按订阅 ``tenant_id`` set ContextVar
   后再加载执行状态，timeout resume 任务的 ``tenant_id`` 同源；
3. 存量订阅（无 tenant_id 字段，升级前写入）回退 default 语义零回归；
   非 default 租户的存量订阅仍无法定位租户——升级窗口的已知残留（见
   ``test_legacy_subscription_of_nondefault_tenant_still_skips``）。
"""
import asyncio
import json
import time
import unittest
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis
import fakeredis.aioredis

import plaita.event.redis as redis_mod

from plaita.core.context import ExecutionContext
from plaita.core.executor import _subscribe_event
from plaita.event.core import EventSubscription
from plaita.event.memory import (
    InMemoryEventBus,
    InMemoryEventSubscriptionStorage,
)
from plaita.event.redis import RedisEventSubscriptionStorage
from plaita.node.event_node import EventNode, EventNodeStatus
from plaita.server.event_filter import EventFilter
from plaita.server.tenant_context import (
    TenantRoutingExecutionStorage,
    reset_current_tenant,
    set_current_tenant,
)
from plaita.storage.base import ExecutionState


TIMEOUT_DEDUP_PREFIX = "plaita:event_filter:timeout:"


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class _RecordingAsyncBus:
    """记录 register_subscription 收到的参数集。"""

    def __init__(self):
        self.calls = []
        self.subscription_id = "sub-p3-rec"

    async def register_subscription(self, **params):
        self.calls.append(params)
        return self.subscription_id


class _FakeFlow:
    flow_id = "flow-p3"


# ─── 1. 挂起点：租户写进订阅 ─────────────────────────────────


class TestSubscribeEventCarriesTenant:
    def _subscribe(self, node, bus):
        context = ExecutionContext(event_bus=bus)
        node_state = {
            "event_type": node.event_type,
            "status": EventNodeStatus.PENDING.value,
        }
        return run(_subscribe_event(node, _FakeFlow(), node_state, context))

    def test_non_default_tenant_written_into_subscription(self):
        bus = _RecordingAsyncBus()
        node = EventNode(id="evt", event_type="approval", subscription_timeout=30)
        token = set_current_tenant("acme")
        try:
            assert self._subscribe(node, bus) is True
        finally:
            reset_current_tenant(token)
        assert bus.calls[0]["tenant_id"] == "acme"
        assert bus.calls[0]["timeout"] == 30

    def test_default_tenant_keeps_historical_param_set(self):
        """default/未 set 租户不带 tenant_id 键——存量简化 bus 替身
        （不接受新形参）零破坏，与波次④ timeout 的兼容红线同款。"""
        bus = _RecordingAsyncBus()
        node = EventNode(id="evt", event_type="approval")
        assert self._subscribe(node, bus) is True
        assert "tenant_id" not in bus.calls[0]
        assert "timeout" not in bus.calls[0]

    def test_empty_tenant_context_treated_as_default(self):
        """租户为空串也归入 default 参数集（set_current_tenant 的归一化
        语义），不往订阅塞无效租户。"""
        bus = _RecordingAsyncBus()
        event_node = EventNode(id="evt", event_type="approval")
        token = set_current_tenant("")
        try:
            assert self._subscribe(event_node, bus) is True
        finally:
            reset_current_tenant(token)
        assert "tenant_id" not in bus.calls[0]

    def test_memory_bus_roundtrip_carries_tenant(self):
        """内存 bus 的 register_subscription 接受并落存 tenant_id。"""
        bus = InMemoryEventBus()
        token = set_current_tenant("acme")
        try:
            sid = run(bus.register_subscription(
                event_type="approval",
                correlation_id="exec-mem-1",
                flow_id="flow-p3",
                node_id="evt",
                tenant_id="acme",
            ))
        finally:
            reset_current_tenant(token)
        sub = run(bus.subscription_storage.get_subscription(sid))
        assert sub.tenant_id == "acme"


# ─── 2. 消费点：超时恢复按订阅租户加载（核心红→绿） ──────────


class TestTimeoutResumeTenantRouting(unittest.IsolatedAsyncioTestCase):
    """真实生产栈：TenantRoutingExecutionStorage（Redis 租户路由）+
    InMemoryEventSubscriptionStorage + fakeredis。执行状态落在租户
    命名空间，其余命名空间为空——按错误租户加载必然 miss。"""

    QUEUE = "test:p3-queue"

    def _save_state(self, execution_storage, tenant, execution_id, flow_id):
        state = ExecutionState(
            execution_id=execution_id,
            flow_id=flow_id,
            tenant_id=tenant,
            status="suspended",
            context={},
        )
        token = set_current_tenant(tenant)
        try:
            execution_storage.save_execution_state(execution_id, state)
        finally:
            reset_current_tenant(token)

    async def _make_env(self, tenant, subscription_tenant,
                        execution_id="exec-p3-1", flow_id="flow-p3-acme"):
        fake_redis = fakeredis.FakeRedis(decode_responses=True)
        execution_storage = TenantRoutingExecutionStorage(client=fake_redis)
        self._save_state(execution_storage, tenant, execution_id, flow_id)

        subscription_storage = InMemoryEventSubscriptionStorage()
        sub_kwargs = dict(
            event_type="approval",
            correlation_id=execution_id,
            flow_id=flow_id,
            timeout=1.0,
            created_at=time.time() - 10,
        )
        if subscription_tenant is not None:
            sub_kwargs["tenant_id"] = subscription_tenant
        sub = EventSubscription(**sub_kwargs)
        await subscription_storage.store_subscription(sub)

        event_filter = EventFilter(
            execution_storage=execution_storage,
            subscription_storage=subscription_storage,
            redis_client=fake_redis,
            event_bus=AsyncMock(),
            queue_name=self.QUEUE,
            enable_subscription_timeout_checker=True,
        )
        return event_filter, sub, fake_redis

    @staticmethod
    def _stream_payloads(redis_client, stream_key):
        entries = redis_client.xrange(stream_key, min="-", max="+")
        tasks = []
        for _mid, fields in entries:
            raw = fields.get("payload") or fields.get(b"payload")
            if isinstance(raw, bytes):
                raw = raw.decode()
            tasks.append(json.loads(raw))
        return tasks

    async def test_nondefault_tenant_timeout_enqueues_resume(self):
        """租户 acme 的挂起执行超时：按 acme 加载 state → 入队 timeout
        resume，任务带 tenant_id=acme（修复前按 default 加载不到 → 跳过）。"""
        event_filter, sub, fake_redis = await self._make_env(
            tenant="acme", subscription_tenant="acme",
        )
        await event_filter._timeout_checker._check_timeouts()

        tasks = self._stream_payloads(fake_redis, self.QUEUE)
        assert len(tasks) == 1
        task = tasks[0]
        assert task["type"] == "resume"
        assert task["resume_type"] == "timeout"
        assert task["execution_id"] == "exec-p3-1"
        assert task["flow_id"] == "flow-p3-acme"
        assert task["tenant_id"] == "acme"
        assert task["data"]["subscription_id"] == sub.subscription_id
        assert fake_redis.exists(TIMEOUT_DEDUP_PREFIX + sub.subscription_id) == 1

    async def test_default_tenant_subscription_unaffected(self):
        """default 租户订阅（tenant_id=None）：语义与历史一致，照常入队。"""
        event_filter, _sub, fake_redis = await self._make_env(
            tenant="default", subscription_tenant=None,
        )
        await event_filter._timeout_checker._check_timeouts()
        tasks = self._stream_payloads(fake_redis, self.QUEUE)
        assert len(tasks) == 1
        assert tasks[0]["tenant_id"] == "default"
        assert tasks[0]["flow_id"] == "flow-p3-acme"

    async def test_legacy_subscription_falls_back_to_default(self):
        """存量订阅（无 tenant_id 字段，升级前写入的 JSON round-trip 形状）
        回退 default 语义：default 租户的执行照常超时恢复，零回归。"""
        event_filter, _sub, fake_redis = await self._make_env(
            tenant="default", subscription_tenant=None,
        )
        await event_filter._timeout_checker._check_timeouts()
        tasks = self._stream_payloads(fake_redis, self.QUEUE)
        assert len(tasks) == 1
        assert tasks[0]["tenant_id"] == "default"

    async def test_legacy_subscription_of_nondefault_tenant_still_skips(self):
        """已知残留（钉住而非修复）：升级前写入的订阅不携带租户，其
        非 default 执行状态无法定位——跳过且保留订阅（不误删、不误入队）。
        存量库迁移说明见 event/sqlalchemy.py 与本修复提交说明。"""
        event_filter, _sub, fake_redis = await self._make_env(
            tenant="acme", subscription_tenant=None,
        )
        await event_filter._timeout_checker._check_timeouts()
        assert self._stream_payloads(fake_redis, self.QUEUE) == []
        remaining = await event_filter.subscription_storage.list_subscriptions()
        assert len(remaining) == 1

    async def test_same_execution_id_across_tenants_routed_correctly(self):
        """同 execution_id 双租户共存：各租户订阅超时各自路由到正确租户
        的执行状态（修复前 acme 订阅会错误加载 default 命名空间的同号
        执行——flow 串台）。"""
        fake_redis = fakeredis.FakeRedis(decode_responses=True)
        execution_storage = TenantRoutingExecutionStorage(client=fake_redis)
        self._save_state(execution_storage, "acme", "exec-p3-x", "flow-acme")
        self._save_state(execution_storage, "default", "exec-p3-x", "flow-default")

        subscription_storage = InMemoryEventSubscriptionStorage()
        sub_acme = EventSubscription(
            event_type="approval", correlation_id="exec-p3-x",
            flow_id="flow-acme", timeout=1.0, created_at=time.time() - 10,
            tenant_id="acme",
        )
        sub_default = EventSubscription(
            event_type="approval", correlation_id="exec-p3-x",
            flow_id="flow-default", timeout=1.0, created_at=time.time() - 10,
        )
        await subscription_storage.store_subscription(sub_acme)
        await subscription_storage.store_subscription(sub_default)

        event_filter = EventFilter(
            execution_storage=execution_storage,
            subscription_storage=subscription_storage,
            redis_client=fake_redis,
            event_bus=AsyncMock(),
            queue_name=self.QUEUE,
            enable_subscription_timeout_checker=True,
        )
        await event_filter._timeout_checker._check_timeouts()

        tasks = self._stream_payloads(fake_redis, self.QUEUE)
        assert len(tasks) == 2
        by_flow = {t["flow_id"]: t for t in tasks}
        assert by_flow["flow-acme"]["tenant_id"] == "acme"
        assert by_flow["flow-default"]["tenant_id"] == "default"


# ─── 3. redis 后端 round-trip ────────────────────────────────

_SHARED_SERVER = fakeredis.FakeServer()


async def _fake_from_url(url):
    return fakeredis.aioredis.FakeRedis(server=_SHARED_SERVER)


def _patch_from_url():
    return patch.object(redis_mod.aioredis, "from_url", side_effect=_fake_from_url)


class TestRedisSubscriptionTenantRoundTrip(unittest.IsolatedAsyncioTestCase):
    KEY_PREFIX = "plaita:subscription:"

    def _storage(self):
        return RedisEventSubscriptionStorage(
            "redis://localhost:6379/9", key_prefix=self.KEY_PREFIX)

    async def test_tenant_id_survives_roundtrip(self):
        storage = self._storage()
        sub = EventSubscription(
            event_type="approval", correlation_id="exec-rt-1", tenant_id="acme",
        )
        with _patch_from_url():
            await storage.store_subscription(sub)
            got = await storage.get_subscription(sub.subscription_id)
        assert got.tenant_id == "acme"

    async def test_legacy_json_without_tenant_loads_none(self):
        """升级前写入的订阅 JSON（无 tenant_id 键）读回 tenant_id=None
        （消费侧回退 default 的数据基础）。"""
        storage = self._storage()
        sub = EventSubscription(event_type="approval", correlation_id="exec-rt-2")
        legacy_json = json.loads(sub.model_dump_json())
        legacy_json.pop("tenant_id")  # 还原为升级前形状：JSON 里没有该键
        with _patch_from_url():
            await storage.store_subscription(sub)
            raw_key = f"{self.KEY_PREFIX}data:{sub.subscription_id}"
            await storage.redis.set(raw_key, json.dumps(legacy_json))
            got = await storage.get_subscription(sub.subscription_id)
        assert got.tenant_id is None


# ─── 4. sqlalchemy 后端（experimental；无 aiosqlite 环境自动跳） ──


class TestSqlalchemySubscriptionTenantColumn(unittest.TestCase):
    def test_model_has_nullable_tenant_column(self):
        pytest.importorskip("sqlalchemy")
        from plaita.event.sqlalchemy import EventSubscriptionModel

        col = EventSubscriptionModel.__table__.columns["tenant_id"]
        assert col.nullable is True

    def test_tenant_roundtrip(self):
        pytest.importorskip("aiosqlite")
        from sqlalchemy.ext.asyncio import create_async_engine

        from plaita.event.sqlalchemy import SqlalchemyEventSubscriptionStorage

        async def _test():
            engine = create_async_engine("sqlite+aiosqlite://")
            # 订阅存储无自动建表（events 侧的 create_tables opt-in 不覆盖
            # subscriptions 表）——测试自建，否则首个写操作即 no such table
            from plaita.event.sqlalchemy import Base

            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            storage = SqlalchemyEventSubscriptionStorage(engine)
            sub = EventSubscription(
                event_type="approval", correlation_id="exec-sq-1", tenant_id="acme",
            )
            await storage.store_subscription(sub)
            got = await storage.get_subscription(sub.subscription_id)
            assert got.tenant_id == "acme"
            await engine.dispose()

        run(_test())
