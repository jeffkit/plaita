"""#109/#110 r3：验收者四项缺陷的回归面。

覆盖（每条对应验收者 finding 的一格）：

#109/D1  自 close 守卫的**嵌套形状**：``handler`` ``await create_task(nested())``、
        两级嵌套、``gather``、``shield``、spawned-not-awaited ⇒ 具名拒绝；
        外部 close 仍能干净排空（pending==0、无 RecursionError、无悬置任务）。
#109/D2  ``event_filter.main_async`` 收尾按**能力**关停：memory/sqlalchemy 无
        ``close()`` ⇒ 不再打假 WARNING「事件总线关停失败」；redis 路径不变。
#110/D3  ``RedisEventStorage.list_events``：全量分支按已知前缀剥离 event_type
        （含 ':' 的类型不再截断）；两个分支都把 ``zrangebyscore`` 的 bytes
        member 归一化为 str（生产 ``decode_responses=False`` 下不再整条返回空）；
        经生产调用方 ``EventReconciler.scan_once`` 覆盖。
        ``registry.get_service_types`` 同 split(':') 家族收口。
#109/D4  ``EventSubscription`` 空类型给具名中文原因；sqlalchemy 历史脏行跳过并
        具名点名（同批合法订阅不再被整批丢弃）；``model_construct`` 绕过校验后
        的三后端分叉**按实测计数钉住**（声明口径）。
"""

import asyncio
import logging
import tempfile
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
from plaita.event.redis import RedisEventBus, RedisEventStorage

_SHARED_SERVER = fakeredis.FakeServer()


async def _fake_from_url(url):
    # 与生产 ``aioredis.from_url(url)`` 默认一致：decode_responses=False
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
    bus = RedisEventBus(redis_url="redis://unused/0")
    bus.processing_tracker = _Tracker()
    bus.event_storage = _Stub()
    bus.subscription_storage = _Stub()
    bus.pubsub = _Stub()
    bus.redis = _Stub()
    bus.handlers["h1"] = (handler, "*", None, None)
    return bus


def _msg(event):
    return {"type": "message", "data": event.model_dump_json().encode()}


async def _drain_test_tasks():
    for _ in range(300):
        live = [t for t in asyncio.all_tasks()
                if t is not asyncio.current_task() and not t.done()]
        if not live:
            return []
        await asyncio.sleep(0.01)
    return [t.get_coro().__qualname__ for t in asyncio.all_tasks()
            if t is not asyncio.current_task() and not t.done()]


class Test109NestedSelfCloseGuard(unittest.IsolatedAsyncioTestCase):
    """验收者 V2 形状：task-wait cycle 不得再产生 RecursionError / 悬置任务。"""

    async def _run_shape(self, holder, handler):
        bus = _make_bus(handler)
        holder["bus"] = bus        # 必须在 dispatch 前可见（闭包在 handler 内读取）
        event = Event(event_type="t.demo", data={})
        await bus._process_message(_msg(event), "h1")
        live = await _drain_test_tasks()
        return bus, live

    async def test_nested_awaited_task_close_is_named_refusal(self):
        holder = {}
        refusal = []

        async def nested():
            try:
                await holder["bus"].close()
            except BaseException as e:      # noqa: BLE001 - 断言拒绝类型/文案
                refusal.append(e)

        async def handler(event):
            await asyncio.create_task(nested())

        bus, live = await self._run_shape(holder, handler)
        self.assertEqual(len(refusal), 1, "嵌套 close() 未被拒绝（V2 病理）")
        self.assertIsInstance(refusal[0], RuntimeError)
        self.assertIn("不能从它自己的分发 handler 内调用", str(refusal[0]))
        self.assertEqual(len(bus._pending_tasks), 0, "close 拒绝后仍有登记任务")
        self.assertFalse(bus._closing, "拒绝必须发生在任何关停副作用之前")
        self.assertEqual(live, [], f"close 后仍有悬置任务: {live}")
        # 外部（非分发任务图内）close 仍能干净返回
        await bus.close()
        self.assertEqual(len(bus._pending_tasks), 0)

    async def test_two_level_nesting_close_is_named_refusal(self):
        holder = {}
        refusal = []

        async def inner():
            try:
                await holder["bus"].close()
            except BaseException as e:      # noqa: BLE001
                refusal.append(e)

        async def mid():
            await asyncio.create_task(inner())

        async def handler(event):
            await asyncio.create_task(mid())

        bus, live = await self._run_shape(holder, handler)
        self.assertEqual(len(refusal), 1, f"两级嵌套未拒绝: {refusal}")
        self.assertIn("不能从它自己的分发 handler 内调用", str(refusal[0]))
        self.assertEqual(len(bus._pending_tasks), 0)
        self.assertEqual(live, [], f"close 后仍有悬置任务: {live}")

    async def test_gather_close_is_named_refusal(self):
        holder = {}
        refusal = []

        async def nested():
            try:
                await holder["bus"].close()
            except BaseException as e:      # noqa: BLE001
                refusal.append(e)

        async def handler(event):
            await asyncio.gather(nested())

        bus, live = await self._run_shape(holder, handler)
        self.assertEqual(len(refusal), 1, f"gather 形状未拒绝: {refusal}")
        self.assertEqual(len(bus._pending_tasks), 0)
        self.assertEqual(live, [])

    async def test_shield_close_is_named_refusal(self):
        """r2 的 bypass-but-named：shield 起的任务也继承分发 context ⇒ 拒绝。"""
        holder = {}
        refusal = []

        async def handler(event):
            try:
                await asyncio.shield(holder["bus"].close())
            except BaseException as e:      # noqa: BLE001
                refusal.append(e)

        bus, live = await self._run_shape(holder, handler)
        self.assertEqual(len(refusal), 1, f"shield 形状未拒绝: {refusal}")
        self.assertIn("不能从它自己的分发 handler 内调用", str(refusal[0]))
        self.assertFalse(bus._closing)
        self.assertEqual(len(bus._pending_tasks), 0)
        self.assertEqual(live, [])

    async def test_spawned_detached_task_close_is_named_refusal(self):
        """spawned-not-awaited 的子任务同样继承 context ⇒ 具名拒绝（保守口径）。"""
        holder = {}
        refusal = []

        async def nested():
            try:
                await holder["bus"].close()
            except BaseException as e:      # noqa: BLE001
                refusal.append(e)

        async def handler(event):
            asyncio.create_task(nested())

        bus, live = await self._run_shape(holder, handler)
        self.assertEqual(len(refusal), 1, f"detached 子任务未拒绝: {refusal}")
        self.assertEqual(len(bus._pending_tasks), 0)
        self.assertEqual(live, [])

    async def test_direct_handler_close_same_named_error(self):
        """r2 的直接形状必须仍是**同一条**具名错误。"""
        holder = {}
        refusal = []

        async def handler(event):
            try:
                await holder["bus"].close()
            except BaseException as e:      # noqa: BLE001
                refusal.append(e)

        bus, live = await self._run_shape(holder, handler)
        self.assertEqual(len(refusal), 1)
        self.assertIn("不能从它自己的分发 handler 内调用", str(refusal[0]))
        self.assertEqual(len(bus._pending_tasks), 0)
        self.assertEqual(live, [])

    async def test_external_close_still_drains_cleanly(self):
        """分发之外的 close 不受守卫影响：等完在飞、pending 归零。"""
        finished = []

        async def handler(event):
            await asyncio.sleep(0.05)
            finished.append(event.event_id)

        bus = _make_bus(handler)
        event = Event(event_type="t.demo", data={})
        await bus._process_message(_msg(event), "h1")
        await bus.close()
        self.assertEqual(finished, [event.event_id])
        self.assertEqual(len(bus._pending_tasks), 0)
        self.assertEqual(bus.processing_tracker.marked, [(event.event_id, "h1")])

    async def test_handler_of_bus_a_may_close_bus_b(self):
        """标记按**总线实例**作用域：A 的分发里关停另一个总线不得误拒。"""
        other = _make_bus(lambda event: None)
        seen = []

        async def handler(event):
            await other.close()
            seen.append(True)

        bus, live = await self._run_shape({}, handler)
        self.assertEqual(seen, [True], "A 的 handler 关停 B 被误拒（标记未按实例作用域）")
        self.assertEqual(len(other._pending_tasks), 0)
        self.assertEqual(len(bus._pending_tasks), 0)
        self.assertEqual(live, [])


class Test109EventFilterShutdownCapability(unittest.IsolatedAsyncioTestCase):
    """D2：按能力关停，memory/sqlalchemy 无 close() 时不得打假 WARNING。"""

    async def test_memory_bus_shutdown_logs_no_false_warning(self):
        from plaita.event.memory import InMemoryEventBus
        import plaita.server.event_filter as ef

        bus = InMemoryEventBus()
        self.assertFalse(hasattr(bus, "close"),
                         "InMemoryEventBus 若新增 close()，本测试口径需同步")
        catch = _Catch()
        logger = logging.getLogger("plaita.server.event_filter")
        logger.addHandler(catch)
        try:
            await ef._close_event_bus(bus)
        finally:
            logger.removeHandler(catch)
        self.assertEqual([m for m in catch.lines if "关停失败" in m], [],
                         f"memory 关停打了假 WARNING: {catch.lines}")

    async def test_sqlalchemy_bus_has_no_close_and_is_skipped(self):
        from plaita.event.sqlalchemy import SqlalchemyEventBus
        import plaita.server.event_filter as ef

        bus = SqlalchemyEventBus.__new__(SqlalchemyEventBus)   # 不建引擎
        self.assertFalse(hasattr(bus, "close"))
        catch = _Catch()
        logger = logging.getLogger("plaita.server.event_filter")
        logger.addHandler(catch)
        try:
            await ef._close_event_bus(bus)
        finally:
            logger.removeHandler(catch)
        self.assertEqual([m for m in catch.lines if "关停失败" in m], [])

    async def test_redis_like_bus_close_is_called_once(self):
        import plaita.server.event_filter as ef

        calls = []

        class Bus:
            async def close(self):
                calls.append("closed")

        await ef._close_event_bus(Bus())
        self.assertEqual(calls, ["closed"])

    async def test_main_async_memory_end_to_end_no_false_warning(self):
        import argparse
        import plaita.server.event_filter as ef

        class StubFilter:
            async def start(self, event_type=None):
                return None

        catch = _Catch()
        logger = logging.getLogger("plaita.server.event_filter")
        logger.addHandler(catch)
        try:
            args = argparse.Namespace(
                redis_url="redis://unused/0", queue_name="q",
                database_url="sqlite:///x.db",
                execution_storage_type="memory",
                subscription_storage_type="memory",
                event_bus_type="memory", event_type="")
            with patch.object(ef.EventFilter, "create_event_filter",
                              return_value=StubFilter()):
                await ef.main_async(args)
        finally:
            logger.removeHandler(catch)
        self.assertEqual([m for m in catch.lines if "关停失败" in m], [],
                         f"--event-bus-type=memory 收尾打了假 WARNING: {catch.lines}")


class Test110ListEventsProductionConnection(unittest.IsolatedAsyncioTestCase):
    """D3：生产连接配置（decode_responses=False）下 list_events 必须非空。"""

    def setUp(self):
        self.prefix = f"le{uuid.uuid4().hex[:8]}:"

    async def _store_two(self, storage):
        await storage.store_event(Event(event_type="order:created", data={}))
        await storage.store_event(Event(event_type="plain", data={}))

    async def test_explicit_type_branch_bytes_safe(self):
        storage = RedisEventStorage("redis://localhost:6379/9", self.prefix)
        with _patch_from_url():
            await storage.initialize()
            await self._store_two(storage)
            got = await storage.list_events("order:created")
            await storage.close()
        self.assertEqual([e.event_type for e in got], ["order:created"],
                         "显式类型分支在 bytes member 下返回空（生产 decode_responses=False）")

    async def test_all_types_branch_keeps_colon_event_type(self):
        storage = RedisEventStorage("redis://localhost:6379/9", self.prefix)
        with _patch_from_url():
            await storage.initialize()
            await self._store_two(storage)
            got = await storage.list_events()
            await storage.close()
        self.assertEqual(sorted(e.event_type for e in got),
                         ["order:created", "plain"],
                         "全量分支用 split(':')[-1] 截断了含 ':' 的 event_type")

    async def test_reconciler_scan_once_replays_colon_events(self):
        """经生产调用方（无 event_type ⇒ 全量分支）覆盖。"""
        from plaita.server.event_reconcile import EventReconciler

        class Filter:
            def __init__(self):
                self.seen = []

            async def handle_event(self, event):
                self.seen.append(event.event_type)

        storage = RedisEventStorage("redis://localhost:6379/9", self.prefix)
        filt = Filter()
        with _patch_from_url():
            await storage.initialize()
            await self._store_two(storage)
            reconciler = EventReconciler(
                storage, filt, redis_client=None,
                backfill_seconds=3600.0, batch_size=10)
            replayed = await reconciler.scan_once()
            await storage.close()
        self.assertEqual(replayed, 2, "EventReconciler.scan_once 在真实连接配置下回放 0 条")
        self.assertEqual(sorted(filt.seen), ["order:created", "plain"])


class Test110RegistryServiceTypeParse(unittest.TestCase):
    """D3 家族：registry.get_service_types 不再按 parts[2] 截断。"""

    class _Redis:
        def __init__(self, keys):
            self._keys = keys

        def keys(self, pattern):
            return list(self._keys)

    def test_colon_service_type_not_truncated(self):
        from plaita.server.registry import ServiceRegistry

        keys = [
            b"plaita:registry:a:b:host-1234",     # service_type 自身含 ':'
            b"plaita:registry:flow_worker:host-5678",
        ]
        reg = ServiceRegistry(redis_client=self._Redis(keys))
        self.assertEqual(sorted(reg.get_service_types()),
                         ["a:b", "flow_worker"])


class Test110SubscriptionNamedReason(unittest.IsolatedAsyncioTestCase):
    """D4a：EventSubscription 空类型与 EventNode 同一条具名中文原因。"""

    def test_empty_type_names_reason(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError) as ctx:
            EventSubscription(event_type="")
        self.assertIn("event_type 不能为空", str(ctx.exception))
        self.assertIn("如需通配请显式写", str(ctx.exception))

    def test_whitespace_only_type_rejected(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError):
            EventSubscription(event_type="   ")

    async def test_storage_write_entry_names_reason(self):
        from plaita.event.redis import RedisEventSubscriptionStorage
        from pydantic import ValidationError
        storage = RedisEventSubscriptionStorage(
            "redis://localhost:6379/9", f"nr{uuid.uuid4().hex[:8]}:")
        with _patch_from_url():
            await storage.initialize()
            with self.assertRaises(ValidationError) as ctx:
                await storage.store_subscription(
                    EventSubscription(event_type="", correlation_id="CID"))
            await storage.close()
        self.assertIn("event_type 不能为空", str(ctx.exception))


class Test110SqlalchemyDirtyRowSkipped(unittest.IsolatedAsyncioTestCase):
    """D4c：历史脏行跳过并具名点名，同批合法订阅不得被整批丢弃。"""

    async def asyncSetUp(self):
        pytest.importorskip("sqlalchemy")
        pytest.importorskip("aiosqlite")
        from sqlalchemy.ext.asyncio import create_async_engine
        from plaita.event.sqlalchemy import Base

        self.tmp = tempfile.mkdtemp(prefix="dirty-row-")
        self.engine = create_async_engine(f"sqlite+aiosqlite:///{self.tmp}/t.db")
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def _seed_dirty(self):
        from sqlalchemy import text
        async with self.engine.begin() as conn:
            # r2 之前 event_type 允许空：直接写库模拟存量脏行（绕过 pydantic）
            await conn.execute(text(
                "INSERT INTO event_subscriptions "
                "(id, event_type, created_at, correlation_id) "
                "VALUES ('bad-empty', '', 0.0, 'CID')"))

    async def test_dirty_row_skipped_and_good_row_kept_both_shapes(self):
        from plaita.event.sqlalchemy import SqlalchemyEventSubscriptionStorage

        storage = SqlalchemyEventSubscriptionStorage(self.engine)
        await storage.store_subscription(EventSubscription(
            event_type="ok.type", correlation_id="CID",
            subscription_id="good-1"))
        await self._seed_dirty()
        catch = _Catch()
        logger = logging.getLogger("plaita.event.sqlalchemy")
        logger.addHandler(catch)
        try:
            all_rows = await storage.list_subscriptions()
            corr_rows = await storage.list_subscriptions(correlation_id="CID")
        finally:
            logger.removeHandler(catch)
        self.assertEqual([s.subscription_id for s in all_rows], ["good-1"],
                         "脏行让整条 list_subscriptions() 失败/丢批")
        self.assertEqual([s.subscription_id for s in corr_rows], ["good-1"],
                         "脏行让 correlation 查询丢批")
        named = [m for m in catch.lines if "跳过非法订阅行" in m]
        self.assertEqual(len(named), 2, f"脏行未逐次具名点名: {catch.lines}")
        self.assertTrue(all("bad-empty" in m for m in named))


class Test110ModelConstructDivergenceDeclared(unittest.IsolatedAsyncioTestCase):
    """D4b：``model_construct`` 绕过校验后的三后端分叉——按实测计数钉住并声明。

    声明口径（不是「uniform」）：memory 静默排除；Redis 在 correlation 索引命中
    该行时抛 ``EventError``（loud，无名）；sqlalchemy 跳过并具名点名。
    本测试只钉住**当前真实行为**，防止分叉被未来改动悄悄改变而不留记录。
    """

    async def test_divergence_counts(self):
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.exceptions import EventError
        from plaita.event.redis import RedisEventSubscriptionStorage
        from plaita.event.sqlalchemy import (
            Base, SqlalchemyEventSubscriptionStorage,
        )
        from sqlalchemy.ext.asyncio import create_async_engine

        rogue = EventSubscription.model_construct(
            subscription_id="rogue", event_type="", correlation_id="CID",
            filter_condition=None, flow_id=None, node_id=None,
            created_at=0.0, timeout=None, tenant_id=None, processed_events=set())

        # memory：静默排除（0 命中，无日志）
        memory = InMemoryEventBus()
        await memory.subscription_storage.store_subscription(rogue)
        mem_rows = await memory.subscription_storage.list_subscriptions(
            event_type="ok.type")
        self.assertEqual(mem_rows, [])

        # redis：correlation 索引命中 ⇒ get_subscription 反序列化抛 EventError（loud）
        storage = RedisEventSubscriptionStorage(
            "redis://localhost:6379/9", f"mc{uuid.uuid4().hex[:8]}:")
        with _patch_from_url():
            await storage.initialize()
            await storage.store_subscription(rogue)
            with self.assertRaises(EventError):
                await storage.list_subscriptions(correlation_id="CID")
            await storage.close()

        # sqlalchemy：跳过 + 具名点名（0 命中）
        tmp = tempfile.mkdtemp(prefix="mc-dirty-")
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp}/t.db")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        sqla = SqlalchemyEventSubscriptionStorage(engine)
        await sqla.store_subscription(rogue)
        catch = _Catch()
        logger = logging.getLogger("plaita.event.sqlalchemy")
        logger.addHandler(catch)
        try:
            sqla_rows = await sqla.list_subscriptions()
        finally:
            logger.removeHandler(catch)
        await engine.dispose()
        self.assertEqual(sqla_rows, [])
        self.assertEqual(len([m for m in catch.lines if "跳过非法订阅行" in m]), 1)


if __name__ == "__main__":
    unittest.main()
