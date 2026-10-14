"""test_issue109_110_event_redis_parity — #109/#110：Redis 后端与 memory 后端对等性。

#109（分发任务生命周期）
    ``RedisEventBus._process_message`` 的 fire-and-forget 分发任务必须有强引用
    （``_pending_tasks`` / ``_track_task``，与 ``InMemoryEventBus`` 同一机制），
    且 ``close()`` 返回后不得存在「inflight 且无处理记录」的第三态：有限等待，
    超时逐个取消并打 WARNING（含 ``event_id`` / ``handler_id``）。

#110（订阅查询索引）
    ``list_subscriptions(correlation_id=..., event_type=...)`` 必须走 correlation
    索引（代价与该 correlation 的订阅数成正比，而非该 event_type 的全部订阅数），
    返回集合与 ``list_subscriptions(correlation_id=...)`` **以及 memory/sqlalchemy
    后端的同条件查询**一致；``correlation_id`` 为空时既有单条件路径行为不变。

Redis 部分用 fakeredis（共享 FakeServer），无需真实实例。
"""

import asyncio
import logging
import time
import unittest
import uuid
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis
import fakeredis.aioredis

import plaita.event.redis as redis_mod
from plaita.event.core import Event, EventSubscription
from plaita.event.memory import InMemoryEventBus
from plaita.event.redis import RedisEventBus, RedisEventSubscriptionStorage

_SHARED_SERVER = fakeredis.FakeServer()


async def _fake_from_url(url):
    return fakeredis.aioredis.FakeRedis(server=_SHARED_SERVER, decode_responses=False)


def _patch_from_url():
    return patch.object(redis_mod.aioredis, "from_url", side_effect=_fake_from_url)


class _Tracker:
    def __init__(self):
        self.marked = []

    async def is_event_processed(self, event_id, handler_id):
        return False

    async def mark_event_processed(self, event_id, handler_id):
        self.marked.append((event_id, handler_id))

    async def record_processing_attempt(self, *a, **k):
        pass

    async def close(self):
        pass


class _Stub:
    async def close(self):
        pass

    async def aclose(self):
        pass


class _Catch(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


def _make_bus(handler):
    """只探任务生命周期：stub 掉 Redis/存储，不触碰网络（#109 验收条件 1）。"""
    bus = RedisEventBus(redis_url="redis://unused/0")
    bus.processing_tracker = _Tracker()
    bus.event_storage = _Stub()
    bus.subscription_storage = _Stub()
    bus.pubsub = _Stub()
    bus.redis = _Stub()
    # retry_policy=None ⇒ 走 _process_event 那条裸 create_task 分支（生产 EventFilter 形态）
    bus.handlers["h1"] = (handler, "*", None, None)
    return bus


def _msg(event):
    return {"type": "message", "data": event.model_dump_json().encode()}


class Test109DispatchTaskLifetime(unittest.IsolatedAsyncioTestCase):
    """#109 验收条件 1/2：强引用存在；close() 无第三态。"""

    async def test_dispatch_task_is_tracked_then_released(self):
        release = asyncio.Event()
        finished = []

        async def handler(event):
            await release.wait()
            finished.append(event.event_id)

        bus = _make_bus(handler)
        event = Event(event_type="t.demo", data={})
        await bus._process_message(_msg(event), "h1")
        self.assertEqual(len(bus._pending_tasks), 1,
                         "分发任务未被 _pending_tasks 持有（#109）")
        release.set()
        for _ in range(50):
            if not bus._pending_tasks:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(len(bus._pending_tasks), 0,
                         "完成回调未把任务移出 _pending_tasks")
        self.assertEqual(len(finished), 1)
        self.assertEqual(bus.processing_tracker.marked,
                         [(event.event_id, "h1")])

    async def test_close_waits_for_short_dispatch_and_marks(self):
        finished = []

        async def handler(event):
            await asyncio.sleep(0.05)
            finished.append(event.event_id)

        bus = _make_bus(handler)
        event = Event(event_type="t.demo", data={})
        await bus._process_message(_msg(event), "h1")
        await bus.close()
        self.assertEqual(len(finished), 1, "close() 未等待在飞分发任务（#109）")
        self.assertEqual(bus.processing_tracker.marked,
                         [(event.event_id, "h1")])
        self.assertEqual(len(bus._pending_tasks), 0)

    async def test_close_timeout_cancels_and_names_event_and_handler(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def handler(event):
            entered.set()
            await release.wait()

        bus = _make_bus(handler)
        event = Event(event_type="t.demo", data={})
        catch = _Catch()
        logging.getLogger("plaita.event.redis").addHandler(catch)
        try:
            await bus._process_message(_msg(event), "h1")
            await entered.wait()
            with patch.object(redis_mod, "DISPATCH_DRAIN_TIMEOUT_SECONDS", 0.1):
                await asyncio.wait_for(bus.close(), timeout=10)
        finally:
            logging.getLogger("plaita.event.redis").removeHandler(catch)
            release.set()

        # 不存在「inflight 且无记录」的第三态
        inflight = [t for t in asyncio.all_tasks()
                    if t is not asyncio.current_task() and not t.done()
                    and "_process_event" in t.get_coro().__qualname__]
        self.assertEqual(inflight, [], "close() 返回后仍有在飞分发任务（#109 第三态）")
        warnings = [m for m in catch.lines if "关停" in m]
        self.assertEqual(len(warnings), 1, f"超时取消未点名: {catch.lines}")
        self.assertIn(event.event_id, warnings[0])
        self.assertIn("h1", warnings[0])
        self.assertEqual(len(bus._pending_tasks), 0)


    async def test_dispatch_after_close_starts_is_refused_loudly(self):
        """#109 收尾围栏：close() 开始后到达的分发必须被拒且点名，不得静默创建。"""
        finished = []

        async def handler(event):
            finished.append(event.event_id)

        bus = _make_bus(handler)
        catch = _Catch()
        logging.getLogger("plaita.event.redis").addHandler(catch)
        try:
            await bus.close()                       # 进入关停态
            event = Event(event_type="t.demo", data={})
            await bus._process_message(_msg(event), "h1")
        finally:
            logging.getLogger("plaita.event.redis").removeHandler(catch)
        refused = [m for m in catch.lines if "拒绝新的分发" in m]
        self.assertEqual(len(refused), 1, f"关停后分发未被拒绝: {catch.lines}")
        self.assertIn(event.event_id, refused[0])
        self.assertIn("h1", refused[0])
        self.assertEqual(finished, [], "关停后仍执行了 handler")
        self.assertEqual(len(bus._pending_tasks), 0)


class Test109CancelWindowAndSelfClose(unittest.IsolatedAsyncioTestCase):
    """#109/F2 + 验收者补的形状：取消窗口点名、自 close 具名拒绝、retry 分支登记。"""

    async def test_cancel_window_names_event_instead_of_silent_loss(self):
        entered = asyncio.Event()

        class BlockingTracker:
            async def is_event_processed(self, e, h):
                entered.set()
                await asyncio.Event().wait()      # 可阻塞的首个 await（真实 Redis 往返）
            async def mark_event_processed(self, e, h):
                pass
            async def record_processing_attempt(self, *a, **k):
                pass
            async def close(self):
                pass

        bus = _make_bus(lambda e: None)
        bus.processing_tracker = BlockingTracker()
        event = Event(event_type="t.demo", data={})
        # 监听器内联 await _process_message 的形态（close() 会 cancel listeners）
        listener = asyncio.create_task(
            bus._process_message(_msg(event), "h1"))
        bus.listeners.append(listener)
        await entered.wait()
        catch = _Catch()
        logging.getLogger("plaita.event.redis").addHandler(catch)
        try:
            await asyncio.wait_for(bus.close(), timeout=10)
            for _ in range(100):
                if listener.done():
                    break
                await asyncio.sleep(0.01)
        finally:
            logging.getLogger("plaita.event.redis").removeHandler(catch)
        named = [m for m in catch.lines
                 if event.event_id in m and "h1" in m and "取消" in m]
        self.assertEqual(len(named), 1,
                         f"取消窗口既未完成也未点名（F2）: {catch.lines}")
        self.assertTrue(listener.cancelled())

    async def test_close_from_own_handler_is_named_refusal(self):
        holder = {}

        async def handler(event):
            await holder["b"].close()

        bus = _make_bus(handler)
        holder["b"] = bus
        catch = _Catch()
        logging.getLogger("plaita.event.redis").addHandler(catch)
        try:
            await bus._process_message(_msg(Event(event_type="t.demo", data={})), "h1")
            for _ in range(200):
                if not bus._pending_tasks:
                    break
                await asyncio.sleep(0.01)
        finally:
            logging.getLogger("plaita.event.redis").removeHandler(catch)
        self.assertEqual(len(bus._pending_tasks), 0)
        hits = [m for m in catch.lines if "不能从它自己的分发 handler 内调用" in m]
        self.assertEqual(len(hits), 1,
                         f"handler 内 close() 未被具名拒绝: {catch.lines}")

    async def test_retry_branch_task_is_tracked_and_drained(self):
        from plaita.event.core import RetryPolicy
        release = asyncio.Event()
        finished = []

        async def handler(event):
            await release.wait()
            finished.append(event.event_id)

        bus = _make_bus(handler)
        bus.handlers["h1"] = (handler, "*", None, RetryPolicy(max_retries=0))
        event = Event(event_type="t.demo", data={})
        await bus._process_message(_msg(event), "h1")
        self.assertEqual(len(bus._pending_tasks), 1,
                         "retry 分支的分发任务未被持有（F3②）")
        release.set()
        await bus.close()
        self.assertEqual(len(finished), 1)
        self.assertEqual(len(bus._pending_tasks), 0)


class Test110EventTypeMustBeNonEmpty(unittest.IsolatedAsyncioTestCase):
    """#110/F1：空 event_type 在三后端语义分叉 ⇒ 模型层直接判非法。"""

    def test_event_node_rejects_empty_event_type(self):
        from pydantic import ValidationError
        from plaita.node.event_node import EventNode
        with self.assertRaises(ValidationError) as ctx:
            EventNode(id="e", event_type="")
        self.assertIn("event_type", str(ctx.exception))
        self.assertIn("不能为空", str(ctx.exception))
        with self.assertRaises(ValidationError):        # camelCase 别名同样拒绝
            EventNode(id="e", eventType="")
        self.assertEqual(EventNode(id="e", event_type="*").event_type, "*")

    def test_subscription_rejects_empty_event_type(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            EventSubscription(event_type="")
        self.assertEqual(EventSubscription(event_type="*").event_type, "*")

    async def test_empty_type_subscription_cannot_enter_storage(self):
        """作者/编译入口已拒；存储写入口同样拒（不会留下分叉形状）。"""
        from pydantic import ValidationError
        storage = RedisEventSubscriptionStorage(
            "redis://localhost:6379/9", f"t{uuid.uuid4().hex[:8]}:")
        with _patch_from_url():
            await storage.initialize()
            with self.assertRaises(ValidationError):
                await storage.store_subscription(EventSubscription(
                    event_type="", correlation_id="CID"))
            await storage.close()


class Test110CorrelationIndexPriority(unittest.IsolatedAsyncioTestCase):
    """#110 验收条件 1–4：索引选择、代价界、集合等价、既有路径不变。"""

    def setUp(self):
        # 共享 fakeredis server 跨用例：每个用例独立 keyspace，避免 type 索引串味
        self.prefix = f"t{uuid.uuid4().hex[:10]}:"

    async def _store(self, storage, n_type, correlation_id=None, subscription_id=None,
                     event_type="shared.type"):
        return await storage.store_subscription(EventSubscription(
            event_type=event_type,
            correlation_id=correlation_id,
            subscription_id=subscription_id or f"sub-{correlation_id}",
        ))

    async def test_correlation_index_cost_is_bounded_by_matches(self):
        storage = RedisEventSubscriptionStorage("redis://localhost:6379/9", self.prefix)
        with _patch_from_url():
            await storage.initialize()
            for i in range(100):
                await self._store(storage, 100, correlation_id=f"c-{i:03d}")
            await self._store(storage, 100, correlation_id="CID",
                              subscription_id="sub-target")
            gets, smembers = [], []
            real_get, real_smembers = storage.redis.get, storage.redis.smembers

            async def counting_get(key):
                gets.append(key)
                return await real_get(key)

            async def counting_smembers(key):
                smembers.append(key)
                return await real_smembers(key)

            storage.redis.get = counting_get
            storage.redis.smembers = counting_smembers
            subs = await storage.list_subscriptions(
                event_type="shared.type", correlation_id="CID")
            await storage.close()

        self.assertEqual([s.subscription_id for s in subs], ["sub-target"])
        data_gets = [k for k in gets if "data:" in str(k)]
        self.assertEqual(len(data_gets), 1,
                         f"按 event_type 取全量再逐条反序列化（{len(data_gets)} 次 GET）")
        self.assertTrue(any("correlation:CID" in str(k) for k in smembers),
                        f"未走 correlation 索引: {smembers}")
        self.assertFalse(any("type:shared.type" in str(k) for k in smembers),
                         f"仍走 type 索引: {smembers}")

    async def test_equivalent_to_correlation_only_query(self):
        storage = RedisEventSubscriptionStorage("redis://localhost:6379/9", self.prefix)
        with _patch_from_url():
            await storage.initialize()
            for i in range(20):
                await self._store(storage, 20, correlation_id=f"x-{i:02d}")
            both = await storage.list_subscriptions(
                event_type="shared.type", correlation_id="x-07")
            only = await storage.list_subscriptions(correlation_id="x-07")
            await storage.close()
        self.assertEqual(sorted(s.subscription_id for s in both),
                         sorted(s.subscription_id for s in only),
                         "两条件查询与 correlation 单条件查询集合不一致（#110）")
        self.assertEqual([s.subscription_id for s in both], ["sub-x-07"])

    async def test_correlation_less_subscription_excluded_when_filter_given(self):
        """给了 correlation_id 就必须相等（与 memory/sqlalchemy 的 list 口径一致）。"""
        storage = RedisEventSubscriptionStorage("redis://localhost:6379/9", self.prefix)
        with _patch_from_url():
            await storage.initialize()
            await storage.store_subscription(EventSubscription(
                event_type="shared.type", correlation_id=None,
                subscription_id="sub-wildcard"))
            await self._store(storage, 1, correlation_id="CID",
                              subscription_id="sub-target")
            got = await storage.list_subscriptions(
                event_type="shared.type", correlation_id="CID")
            await storage.close()
        self.assertEqual([s.subscription_id for s in got], ["sub-target"],
                         "correlation_id 为空的订阅被错误纳入（#110 口径）")

    async def test_type_only_path_unchanged(self):
        storage = RedisEventSubscriptionStorage("redis://localhost:6379/9", self.prefix)
        with _patch_from_url():
            await storage.initialize()
            for i in range(5):
                await self._store(storage, 5, correlation_id=f"t-{i}")
            await self._store(storage, 5, correlation_id=None,
                              subscription_id="sub-no-corr")
            got = await storage.list_subscriptions(event_type="shared.type")
            other = await storage.list_subscriptions(event_type="other.type")
            await storage.close()
        self.assertEqual(len(got), 6, "event_type 单条件路径行为改变")
        self.assertEqual(other, [])

    async def test_global_scan_keeps_colon_containing_ids(self):
        """#110/F3：全量 SCAN 路径按已知前缀剥离 id；split(':')[-1] 会静默丢订阅。"""
        storage = RedisEventSubscriptionStorage(
            "redis://localhost:6379/9", self.prefix)
        with _patch_from_url():
            await storage.initialize()
            await storage.store_subscription(EventSubscription(
                event_type="shared.type", subscription_id=f"{self.prefix}a:b:c"))
            await storage.store_subscription(EventSubscription(
                event_type="shared.type", subscription_id=f"{self.prefix}plain"))
            got = await storage.list_subscriptions()
            await storage.close()
        self.assertEqual(
            sorted(s.subscription_id for s in got),
            sorted([f"{self.prefix}a:b:c", f"{self.prefix}plain"]),
            "全量 SCAN 路径丢了含 ':' 的订阅 id（#110/F3）")

    async def test_cross_backend_parity_with_memory(self):
        """同一批订阅下 find_matching_subscriptions 的集合在 memory/redis 一致。"""
        event = Event(event_type="shared.type", data={}, correlation_id="CID")
        memory = InMemoryEventBus()
        await memory.subscription_storage.store_subscription(EventSubscription(
            event_type="shared.type", correlation_id="CID", subscription_id="m-hit"))
        await memory.subscription_storage.store_subscription(EventSubscription(
            event_type="shared.type", correlation_id="OTHER", subscription_id="m-other"))
        await memory.subscription_storage.store_subscription(EventSubscription(
            event_type="shared.type", correlation_id=None, subscription_id="m-wild"))
        await memory.subscription_storage.store_subscription(EventSubscription(
            event_type="other.type", correlation_id="CID", subscription_id="m-wrong-type"))

        storage = RedisEventSubscriptionStorage("redis://localhost:6379/9", self.prefix)
        with _patch_from_url():
            await storage.initialize()
            for sid, corr, etype in (("m-hit", "CID", "shared.type"),
                                     ("m-other", "OTHER", "shared.type"),
                                     ("m-wild", None, "shared.type"),
                                     ("m-wrong-type", "CID", "other.type")):
                await storage.store_subscription(EventSubscription(
                    event_type=etype, correlation_id=corr, subscription_id=sid))
            redis_matches = await storage.find_matching_subscriptions(event, {})
            await storage.close()
        memory_matches = await memory.subscription_storage.find_matching_subscriptions(event, {})
        self.assertEqual(sorted(s.subscription_id for s in redis_matches),
                         sorted(s.subscription_id for s in memory_matches),
                         "memory/redis 的 find_matching_subscriptions 不一致（#110 对齐）")
        self.assertEqual([s.subscription_id for s in redis_matches], ["m-hit"])
