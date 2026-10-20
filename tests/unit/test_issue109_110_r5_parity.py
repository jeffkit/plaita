"""#109/#110 r5：结构性自关停围栏（完成唤醒边遍历 + 预算 fail-closed）与 #110 合取语义。

本文件钉住 r5 的两处结构性决定：

#109/F1（自关停围栏的 r5 口径）
  判据 = ``close()` 的调用任务沿「事件循环的完成唤醒边」可达的人里是否含本实例
  登记的在飞分发任务**（方向：close 完成 ⇒ 被唤醒的人；命中 ⇒ close 会把自己
  纳入 drain）。本文件的断言谓词（逐格独立、不看实现返回值回填）：

      REFUSED := 具名 RuntimeError ∧ ``bus._closing is False``
                 ∧ 未进入 ``_drain_pending_tasks`` ∧ listener 未被取消
                 ∧ ``_pending_tasks`` 不被清掉

  * 新边族格：X1 ``TaskGroup.create_task(..., context=Context())``、
    X2 ``functools.partial(waiter.set_result)``（future 在 ``func.__self__``）、
    X3 ``asyncio.as_completed``（Queue ``_getters`` 中介）——已在
    ``test_issue109_110_r3_parity.py::test_empty_context_new_edge_families_refused``
    与 ``..._net_refuses_before_drain_all_kinds`` 钉住；这里补**预算**两格。
  * **近预算格**（``chain=100``）：唤醒锥完全枚举 ⇒ 命中 ⇒ 具名自关停拒绝。
  * **超预算格**（``chain=_DISPATCH_GRAPH_MAX_NODES``，r6 预算 65536）：
    预算内枚举不完 ⇒ **fail-closed 拒绝**
    （``_SELF_CLOSE_BUDGET_REFUSAL``，文案含预算与已访问节点数）——r4 的
    「节点上限用满即当作未命中」会把这一格变成**放行**（验收者 X4 实测
    pads=1500 时 12/12 漏检），本格是那次退化的直接死法。
  * **允许半边**：≥600 个在飞分发 + **外部** close 仍必须放行（``refusal is None``、
    600 条超时点名 WARNING、**超时任务不被取消**——r7 起 ``close()`` 返回时它们仍在
    ``_pending_tasks`` 里，由完成回调回收）；遍历代价只与 close 任务自己的唤醒锥
    有关（实测 ``visited`` 为个位数），与在飞分发条数无关。

#110（``list_subscriptions`` 的合取语义）
  r3 及以前 redis 的 correlation 分支只按索引取行、不再校验 ``event_type`` ⇒
  ``list_subscriptions(event_type='shared.type', correlation_id='CID')`` 在真实
  Redis 上返回 2 行（同 correlation 的 ``other.type`` 行也被返回），memory /
  sqlalchemy 返回 1 行，与 docstring 的「同一集合」声明矛盾。r5 决定：索引只选
  候选集，给出的每个条件都生效（AND），三后端一致；索引选择与 ``data:`` GET 代价
  不变（仍与该 correlation 的候选数成正比）。
"""

import asyncio
import contextvars
import functools
import logging
import tempfile
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
from plaita.event.redis import (
    RedisEventBus,
    RedisEventSubscriptionStorage,
    _DISPATCH_GRAPH_MAX_NODES,
    _SELF_CLOSE_BUDGET_REFUSAL,
    _SELF_CLOSE_REFUSAL,
    _close_graph_verdict,
)

_SERVER = fakeredis.FakeServer()
_SELF_CLOSE_MARK = "不能从它自己的分发 handler 内调用"
_BUDGET_MARK = "证明预算内未闭合"


async def _fake_from_url(url):
    return fakeredis.aioredis.FakeRedis(server=_SERVER, decode_responses=False)


def _patch_from_url():
    return patch.object(redis_mod.aioredis, "from_url", side_effect=_fake_from_url)


class _Tracker:
    async def is_event_processed(self, e, h):
        return False

    async def mark_event_processed(self, e, h):
        pass

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


def _inner(err):
    if isinstance(err, BaseExceptionGroup):
        return err.exceptions[0]
    return err


class Test109R5WakeGraphBudget(unittest.IsolatedAsyncioTestCase):
    """唤醒锥的纯函数格：clean / in_graph / budget 三分，预算耗尽必须 fail-closed。"""

    def test_chain_below_budget_is_clean_without_pending(self):
        loop = asyncio.new_event_loop()
        try:
            root = loop.create_future()
            cur = root
            for _ in range(50):
                nxt = loop.create_future()
                cur.add_done_callback(nxt.set_result)
                cur = nxt
            verdict, visited, _reason = _close_graph_verdict(root, {}, budget=512)
        finally:
            loop.close()
        self.assertEqual(verdict, "clean", "无在飞分发时链路应可完全枚举")
        self.assertEqual(visited, 51, "干净格的访问数=链长+1（root）")

    def test_chain_over_budget_is_fail_closed(self):
        loop = asyncio.new_event_loop()
        try:
            root = loop.create_future()
            cur = root
            for _ in range(2000):
                nxt = loop.create_future()
                cur.add_done_callback(nxt.set_result)
                cur = nxt
            verdict, visited, _reason = _close_graph_verdict(root, {}, budget=512)
        finally:
            loop.close()
        self.assertEqual(verdict, "budget",
                         "预算内枚举不完必须 fail-closed，不得退化成 clean/放行")
        self.assertEqual(visited, 512, "预算格必须报出预算用尽的访问数")
        self.assertIn("{budget}", _SELF_CLOSE_BUDGET_REFUSAL)
        self.assertIn("{visited}", _SELF_CLOSE_BUDGET_REFUSAL)

    async def test_chain_tail_awaited_by_pending_is_in_graph(self):
        """链路末端被「在飞分发任务」await ⇒ in_graph（且不必用满预算）。"""
        loop = asyncio.get_running_loop()
        root = loop.create_future()
        cur = root
        for _ in range(50):
            nxt = loop.create_future()
            cur.add_done_callback(nxt.set_result)
            cur = nxt

        async def waiter():
            await cur

        pending_task = loop.create_task(waiter())
        await asyncio.sleep(0)                      # 让 waiter 挂到 cur 上
        try:
            verdict, visited, _reason = _close_graph_verdict(
                root, {pending_task: ("e", "h")}, budget=512)
        finally:
            pending_task.cancel()
            try:
                await pending_task
            except BaseException:                   # noqa: BLE001
                pass
        self.assertEqual(verdict, "in_graph")
        self.assertLess(visited, 512)
        self.assertIn(_SELF_CLOSE_MARK, _SELF_CLOSE_REFUSAL)

    async def _run_chain_shape(self, chain):
        holder, refusal, drain_calls = {}, [], []

        async def handler(event):
            loop = asyncio.get_running_loop()
            bus = holder["bus"]
            task = loop.create_task(bus.close(), context=contextvars.Context())
            fut = loop.create_future()
            task.add_done_callback(fut.set_result)
            for _ in range(chain):
                nxt = loop.create_future()
                fut.add_done_callback(nxt.set_result)
                fut = nxt
            try:
                await fut
            except BaseException as e:               # noqa: BLE001
                refusal.append(e)
            if task.done() and not task.cancelled() and task.exception():
                refusal.append(task.exception())
            if not refusal:
                refusal.append(AssertionError(
                    "close() 未被拒绝：链式 self-close 被放行"))

        async def fake_drain():
            drain_calls.append("drain")

        bus = _make_bus(handler)
        holder["bus"] = bus
        listener = asyncio.ensure_future(asyncio.Event().wait())
        bus.listeners.append(listener)
        with patch.object(bus, "_drain_pending_tasks", new=fake_drain):
            await bus._process_message(
                _msg(Event(event_type="t.demo", data={})), "h1")
            # 等待窗口随链规模伸缩（#184：CI 实测 65536 节点链传播需 21.1s，
            # 固定 4s 窗口是假红根源）；条件满足仍即刻返回，判据一字未改。
            deadline = time.monotonic() + max(60.0, chain * 0.002)
            while time.monotonic() < deadline:
                if not bus._pending_tasks and refusal:
                    break
                await asyncio.sleep(0.01)
            listener_cancelled = listener.done()
            listener.cancel()
        return bus, refusal, drain_calls, listener_cancelled

    async def test_near_budget_chain_is_named_self_close_refusal(self):
        bus, refusal, drain_calls, listener_cancelled = (
            await self._run_chain_shape(100))
        err = _inner(refusal[0])
        self.assertIsInstance(err, RuntimeError, f"{refusal!r}")
        self.assertIn(_SELF_CLOSE_MARK, str(err), "近预算格应命中自关停判据")
        self.assertNotIn(_BUDGET_MARK, str(err))
        self.assertFalse(bus._closing)
        self.assertEqual(drain_calls, [])
        self.assertFalse(listener_cancelled)
        self.assertEqual(len(bus._pending_tasks), 0)

    async def test_over_budget_chain_is_named_budget_refusal(self):
        bus, refusal, drain_calls, listener_cancelled = (
            await self._run_chain_shape(_DISPATCH_GRAPH_MAX_NODES))
        err = _inner(refusal[0])
        self.assertIsInstance(err, RuntimeError, f"{refusal!r}")
        self.assertIn(_BUDGET_MARK, str(err),
                      "超预算必须 fail-closed，不得放行（r4 X4 缺陷）")
        self.assertIn(str(_DISPATCH_GRAPH_MAX_NODES), str(err),
                      "预算拒绝必须报出预算")
        self.assertFalse(bus._closing, "预算拒绝也必须发生在副作用之前")
        self.assertEqual(drain_calls, [])
        self.assertFalse(listener_cancelled)
        self.assertEqual(len(bus._pending_tasks), 0)


class Test109R5AllowHalf(unittest.IsolatedAsyncioTestCase):
    """允许半边：合法外部关停不得被新判据误拒。"""

    async def test_600_inflight_external_close_still_allowed(self):
        """600 在飞 + **外部** close：放行；r7 起超时**不取消**它们，点名后放手跑完。"""
        release = asyncio.Event()
        n = 600
        finished = []

        async def handler(event):
            await release.wait()
            finished.append(event.event_id)

        bus = _make_bus(handler)
        catch = _Catch()
        logger = logging.getLogger("plaita.event.redis")
        logger.addHandler(catch)
        try:
            for _ in range(n):
                await bus._process_message(
                    _msg(Event(event_type="t.demo", data={})), "h1")
            await asyncio.sleep(0.05)
            in_flight = len(bus._pending_tasks)
            verdict, visited, _reason = _close_graph_verdict(
                asyncio.current_task(), bus._pending_tasks)
            with patch.object(redis_mod, "DISPATCH_DRAIN_TIMEOUT_SECONDS", 0.05):
                await bus.close()
            self.assertEqual(bus._teardown_runs, 1, "teardown 必须恰好一次")
            warns = [m for m in catch.lines if "分发任务未在" in m]
            self.assertEqual(len(warns), n,
                             "600 个超时分发必须逐个点名（无第三态）")
            for m in warns:
                self.assertIn("event_id=", m)
                self.assertIn("handler_id=h1", m)
            self.assertEqual(len(bus._pending_tasks), n,
                             "r7：超时任务不得被取消/清空登记（留给完成回调）")
        finally:
            logger.removeHandler(catch)
            release.set()
        self.assertEqual(in_flight, n, "在飞分发未全部登记")
        self.assertEqual(verdict, "clean", "外部关停不得被判为图内关停")
        self.assertLess(visited, 16,
                        "外部关停的唤醒锥必须是 O(1)，与在飞分发条数无关")
        self.assertTrue(bus._closing, "合法外部关停必须真的执行")
        for _ in range(400):
            if not bus._pending_tasks:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(len(finished), n,
                         "被点名的在飞分发必须能全部跑完（未被 cancel）")
        self.assertEqual(len(bus._pending_tasks), 0)

    async def test_sibling_set_in_wait_does_not_inflate_cone(self):
        """``asyncio.wait`` 闭包里的兄弟集合不得被当作唤醒边（否则预算被撑爆）。

        ``_wait`` 的 ``_on_completion`` 闭包同时引用 ``waiter``（真唤醒目标）与
        ``todo``（兄弟簿记集合，含被等待者）。若展开 ``todo``，700 个兄弟会把唤醒锥
        推过 512 节点预算 ⇒ 合法关停被误拒。这里让 699 个兄弟已完成、1 个挂住，
        使 ``asyncio.wait`` 停在自己的 ``waiter`` 上，再从被等待的 target 量唤醒锥。
        """
        loop = asyncio.get_running_loop()
        held = loop.create_future()
        done_siblings = [loop.create_future() for _ in range(699)]
        for s in done_siblings:
            s.set_result(None)
        target = loop.create_future()

        async def waiter():
            await asyncio.wait({target, *done_siblings, held})

        w = loop.create_task(waiter())
        await asyncio.sleep(0)                      # 让 _on_completion 注册到 target
        verdict, visited, _reason = _close_graph_verdict(target, {})
        held.set_result(None)
        target.set_result(None)                     # 收尾：wait 是 ALL_COMPLETED
        await w
        self.assertEqual(verdict, "clean",
                         "兄弟集合被当成唤醒边 ⇒ 合法关停会被误判/撑爆预算")
        self.assertLess(visited, 16, f"唤醒锥被兄弟集合撑大：visited={visited}")

    async def test_sibling_set_e2e_close_still_allowed(self):
        """同一形状的端到端格：700 兄弟 + 外部 close ⇒ 放行（非预算拒绝）。"""
        loop = asyncio.get_running_loop()
        bus = _make_bus(lambda e: None)
        siblings = [loop.create_future() for _ in range(700)]
        for s in siblings:
            s.set_result(None)
        close_task = loop.create_task(bus.close())
        await asyncio.wait({close_task, *siblings})
        self.assertIsNone(close_task.exception(),
                          "兄弟集合撑爆预算 ⇒ 合法外部关停被误拒")
        self.assertTrue(bus._closing)
        self.assertEqual(len(bus._pending_tasks), 0)

    async def test_cross_bus_close_allowed(self):
        holder = {}

        async def handler_a(event):
            await holder["b"].close()

        bus_a = _make_bus(handler_a)
        bus_b = _make_bus(lambda e: None)
        holder["b"] = bus_b
        await bus_a._process_message(
            _msg(Event(event_type="t.demo", data={})), "h1")
        for _ in range(200):
            if bus_b._closing:
                break
            await asyncio.sleep(0.01)
        self.assertTrue(bus_b._closing, "A 的 handler 关停 B 不得被误拒")
        self.assertEqual(len(bus_b._pending_tasks), 0)

    async def test_close_with_no_pending_allowed(self):
        bus = _make_bus(lambda e: None)
        await bus.close()
        self.assertTrue(bus._closing)
        self.assertEqual(len(bus._pending_tasks), 0)

    async def test_cross_loop_close_without_inflight_allowed(self):
        bus = _make_bus(lambda e: None)
        outcome = {}

        def run():
            try:
                asyncio.run(bus.close())
                outcome["r"] = "returned"
            except BaseException as e:               # noqa: BLE001
                outcome["r"] = f"{type(e).__name__}: {e}"

        await asyncio.get_running_loop().run_in_executor(None, run)
        self.assertEqual(outcome["r"], "returned",
                         "无在飞分发时跨循环 close 不拦（T2 边界）")


class Test110ListSubscriptionsConjunction(unittest.IsolatedAsyncioTestCase):
    """#110：``list_subscriptions`` 三后端合取语义（验收者反例的直接死法）。"""

    def setUp(self):
        self.prefix = f"r5{uuid.uuid4().hex[:10]}:"

    async def _store_row(self, storage, sid, etype, corr):
        await storage.store_subscription(EventSubscription(
            event_type=etype, correlation_id=corr, subscription_id=sid))

    async def test_two_condition_listing_excludes_other_event_type(self):
        """``(event_type='shared.type', correlation_id='CID')`` 只回 1 行，三后端一致。"""
        from plaita.event.memory import InMemoryEventBus
        from plaita.event.sqlalchemy import (
            Base, SqlalchemyEventSubscriptionStorage,
        )
        from sqlalchemy.ext.asyncio import create_async_engine

        rows = (("sub-hit", "shared.type", "CID"),
                ("sub-wrong-type", "other.type", "CID"),
                ("sub-other-corr", "shared.type", "OTHER"))

        memory = InMemoryEventBus()
        for sid, etype, corr in rows:
            await self._store_row(memory.subscription_storage, sid, etype, corr)
        mem = await memory.subscription_storage.list_subscriptions(
            event_type="shared.type", correlation_id="CID")

        storage = RedisEventSubscriptionStorage(
            "redis://localhost:6379/9", self.prefix)
        mget_keys = []
        with _patch_from_url():
            await storage.initialize()
            for sid, etype, corr in rows:
                await self._store_row(storage, sid, etype, corr)
            real_mget = storage.redis.mget

            async def counting_mget(*keys):
                mget_keys.extend(keys)
                return await real_mget(*keys)

            storage.redis.mget = counting_mget
            red = await storage.list_subscriptions(
                event_type="shared.type", correlation_id="CID")
            await storage.close()

        tmp = tempfile.mkdtemp(prefix="r5-corr-")
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp}/t.db")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        sqla_storage = SqlalchemyEventSubscriptionStorage(engine)
        for sid, etype, corr in rows:
            await self._store_row(sqla_storage, sid, etype, corr)
        sql = await sqla_storage.list_subscriptions(
            event_type="shared.type", correlation_id="CID")
        await engine.dispose()

        self.assertEqual([s.subscription_id for s in mem], ["sub-hit"])
        self.assertEqual([s.subscription_id for s in red], ["sub-hit"],
                         "redis correlation 分支漏了 event_type 过滤（#110 反例）")
        self.assertEqual([s.subscription_id for s in sql], ["sub-hit"])
        # A69-03：逐条 GET 改为一次 MGET；候选数断言不变（hit + wrong-type）。
        data_keys = [k for k in mget_keys if "data:" in str(k)]
        self.assertEqual(len(data_keys), 2,
                         "correlation 候选仍是 2（hit + wrong-type），"
                         "索引不回落 type 索引、不额外取数")

    async def test_single_condition_paths_unchanged(self):
        memory = None
        from plaita.event.memory import InMemoryEventBus
        memory = InMemoryEventBus()
        for sid, etype, corr in (("a", "shared.type", "CID"),
                                 ("b", "other.type", "CID"),
                                 ("c", "shared.type", None)):
            await self._store_row(memory.subscription_storage, sid, etype, corr)
        corr_only = await memory.subscription_storage.list_subscriptions(
            correlation_id="CID")
        type_only = await memory.subscription_storage.list_subscriptions(
            event_type="shared.type")
        self.assertEqual(sorted(s.subscription_id for s in corr_only), ["a", "b"],
                         "单条件 correlation 语义不变")
        self.assertEqual(sorted(s.subscription_id for s in type_only), ["a", "c"],
                         "单条件 event_type 语义不变")


if __name__ == "__main__":
    unittest.main()
