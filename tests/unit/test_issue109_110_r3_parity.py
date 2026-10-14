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
import contextvars
import functools
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


async def _settle_registry(bus, timeout=3.0):
    """确定性等 ``_pending_tasks`` 的完成回调落地（r8/F2 修掉的瞬态断言）。

    ``Task.add_done_callback`` 的回调由 ``loop.call_soon`` 排程：任务 ``done()`` 之后、
    回调执行之前存在一个窗口——``asyncio.all_tasks()`` 已不再列出该任务，而
    ``_pending_tasks`` 仍持有它。r7 在 handler 里加 ``await asyncio.sleep(0.05)``
    只是把窗口变小（验收者实测 4/600 fresh-loop、5/3000 single-loop 仍失败）。
    这里改为等待**可观察的终态**：让出事件循环 tick 直到登记表真的清空，断言因此
    不再对瞬态取值。
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while bus._pending_tasks and loop.time() < deadline:
        await asyncio.sleep(0)                    # 先把已排程的完成回调放行
        if bus._pending_tasks:
            await asyncio.sleep(0.001)
    return not bus._pending_tasks


class Test109NestedSelfCloseGuard(unittest.IsolatedAsyncioTestCase):
    """验收者 V2 形状：task-wait cycle 不得再产生 RecursionError / 悬置任务。"""

    async def _run_shape(self, holder, handler):
        bus = _make_bus(handler)
        holder["bus"] = bus        # 必须在 dispatch 前可见（闭包在 handler 内读取）
        event = Event(event_type="t.demo", data={})
        await bus._process_message(_msg(event), "h1")
        live = await _drain_test_tasks()
        await _settle_registry(bus)   # r8/F2：断言前先把登记表的完成回调跑完
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
        """spawned-not-awaited 的子任务同样继承 context；**分发仍在飞时** ⇒ 具名拒绝。

        r7：判据只在确有在飞分发时才有意义，所以 handler 用 ``await asyncio.sleep``
        把分发保持在飞，让 marker 成为该形状的唯一判据。

        r8/F2：``_run_shape`` 现在等待 ``_pending_tasks`` 的完成回调真正落地
        （``_settle_registry``）后再断言——r7 的 ``sleep(0.05)`` 只是把
        「任务 ``done()`` 但登记回调未跑」的窗口变小，验收者实测 4/600 仍失败。
        """
        holder = {}
        refusal = []

        async def nested():
            try:
                await holder["bus"].close()
            except BaseException as e:      # noqa: BLE001
                refusal.append(e)

        async def handler(event):
            asyncio.create_task(nested())
            await asyncio.sleep(0.05)       # 分发仍在飞（pending 非空）

        bus, live = await self._run_shape(holder, handler)
        self.assertEqual(len(refusal), 1, f"detached 子任务未拒绝: {refusal}")
        self.assertIn("不能从它自己的分发 handler 内调用", str(refusal[0]))
        self.assertFalse(bus._closing)
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


class Test109R3ResidualHoles(unittest.IsolatedAsyncioTestCase):
    """r3 验收者 F1/F2/F3：显式空 context、线程/executor、两处保守过度拒绝。

    - F1（``context=contextvars.Context()`` 换掉 context ⇒ 不继承 marker）与
      F2（线程 / executor 各自独立 context ⇒ marker 不跨线程）**能修则修**：
      F1 由 r5 的**完成唤醒边遍历**兜底兜住（不依赖 contextvars；判据=「close
      任务完成后会被唤醒的人里含本实例在飞分发任务」，覆盖绑定方法 ``__self__`` /
      ``functools.partial`` 递归 / 函数 ``__closure__`` / asyncio 唤醒队列属性）；
      F2 由「有在飞分发时跨事件循环 close 具名拒绝」兜住。拒绝都发生在**任何关停
      副作用之前**（``_closing`` 保持 ``False``）。
    - F3 两处（``call_soon`` 分离的 close、分发结束后才关停的 detached 任务）r3 曾
      是**保守过度拒绝**；**r7 起放行**：判据只在「确有在飞分发任务」时才有意义，
      没有可保护对象就不拒绝——两者退化成「晚一点的关停」，安全性由 ``close()`` 的
      关停顺序（owner context 调度 teardown、不取消在飞分发、监听最后取消）保证。
    """

    async def _settle(self):
        for _ in range(300):
            live = [t for t in asyncio.all_tasks()
                    if t is not asyncio.current_task() and not t.done()]
            if not live:
                return []
            await asyncio.sleep(0.01)
        return [t.get_coro().__qualname__ for t in asyncio.all_tasks()
                if t is not asyncio.current_task() and not t.done()]

    def _assert_named_refusal(self, refusal_list, bus, live, *, context=""):
        self.assertEqual(len(refusal_list), 1, f"{context}: 未具名拒绝: {refusal_list}")
        err = refusal_list[0]
        # TaskGroup 形状把子任务异常包成 ExceptionGroup（r5/X1），解一层再断言。
        if isinstance(err, BaseExceptionGroup):
            err = err.exceptions[0]
        self.assertIsInstance(err, RuntimeError, context)
        self.assertIn("不能从它自己的分发 handler 内调用", str(err), context)
        self.assertFalse(bus._closing,
                         f"{context}: 拒绝必须发生在任何关停副作用之前")
        self.assertEqual(len(bus._pending_tasks), 0, context)
        self.assertEqual(live, [], f"{context}: close 后仍有悬置任务: {live}")

    async def _run_empty_context_shape(self, kind):
        """验收者 F1 的原始形状 + 同族的其它 asyncio 等待构造。"""
        holder, refusal = {}, []

        async def handler(event):
            ctx = contextvars.Context()          # 显式空 context：绕开 marker
            loop = asyncio.get_running_loop()
            bus = holder["bus"]
            try:
                if kind == "direct":
                    await loop.create_task(bus.close(), context=ctx)
                elif kind == "wait":
                    task = loop.create_task(bus.close(), context=ctx)
                    await asyncio.wait({task})
                    refusal.append(task.exception())
                elif kind == "gather":
                    await asyncio.gather(loop.create_task(bus.close(), context=ctx))
                elif kind == "shield":
                    await asyncio.shield(loop.create_task(bus.close(), context=ctx))
                elif kind == "wait_for":
                    await asyncio.wait_for(
                        loop.create_task(bus.close(), context=ctx), 2)
                elif kind == "nested":
                    async def mid():
                        await loop.create_task(bus.close(), context=ctx)
                    await loop.create_task(mid(), context=ctx)
                elif kind == "taskgroup":
                    # 验收者 X1（决定性）：``TaskGroup._on_task_done`` 是 group 的
                    # 绑定方法，真正被 await 的是 group 的 ``_on_completed_fut``。
                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(bus.close(), context=ctx)
                elif kind == "partial_funcself":
                    # 验收者 X2：future 在 ``partial.func.__self__``。
                    waiter = loop.create_future()
                    task = loop.create_task(bus.close(), context=ctx)
                    task.add_done_callback(functools.partial(waiter.set_result))
                    await waiter
                    refusal.append(task.exception())
                elif kind == "as_completed":
                    # 验收者 X3：Queue 中介的完成边（``_on_completion`` 闭包 →
                    # ``done`` Queue → ``_getters`` 里的 getter → 消费任务）。
                    # 3.11 的 ``as_completed`` 是同步生成器（yield coroutine）。
                    task = loop.create_task(bus.close(), context=ctx)
                    try:
                        for fut in asyncio.as_completed([task]):
                            await fut
                    except BaseException as e:   # noqa: BLE001
                        refusal.append(e)
                    else:
                        refusal.append(task.exception())
            except BaseException as e:           # noqa: BLE001 - 断言拒绝类型/文案
                refusal.append(e)

        bus = _make_bus(handler)
        holder["bus"] = bus
        await bus._process_message(
            _msg(Event(event_type="t.demo", data={})), "h1")
        live = await self._settle()
        return bus, refusal, live

    async def test_empty_context_direct_close_is_named_refusal(self):
        """F1 原形状：``create_task(bus.close(), context=Context())`` 必须具名拒绝。"""
        bus, refusal, live = await self._run_empty_context_shape("direct")
        self._assert_named_refusal(refusal, bus, live, context="F1/direct")

    async def test_empty_context_wait_gather_shield_refused(self):
        """等待图兜底：wait / gather / shield 的空 context 同族构造也拒绝。"""
        for kind in ("wait", "gather", "shield"):
            with self.subTest(kind=kind):
                bus, refusal, live = await self._run_empty_context_shape(kind)
                self._assert_named_refusal(refusal, bus, live, context=f"F1/{kind}")

    async def test_empty_context_wait_for_and_nested_refused(self):
        bus, refusal, live = await self._run_empty_context_shape("wait_for")
        self._assert_named_refusal(refusal, bus, live, context="F1/wait_for")
        bus, refusal, live = await self._run_empty_context_shape("nested")
        self._assert_named_refusal(refusal, bus, live, context="F1/nested")

    async def test_empty_context_new_edge_families_refused(self):
        """r5/X1–X3 新边族（验收者四项反例的决定性形状）。

        每条都用**独立谓词**断言：具名 ``RuntimeError`` ∧ ``_closing is False``
        ∧ 未进 drain ∧ listener 未被取消 ∧ 无悬置任务。不用 subTest：让变异
        重放器能直接解析失败（``SUBFAILED`` 不在重放器语法内）。
        """
        for kind in ("taskgroup", "partial_funcself", "as_completed"):
            bus, refusal, drain_calls, listener_cancelled, live = (
                await self._run_new_edge_shape(kind))
            ctx = f"F1-r5/{kind}"
            self.assertEqual(len(refusal), 1, f"{ctx}: 未具名拒绝: {refusal}")
            err = refusal[0]
            if isinstance(err, BaseExceptionGroup):
                err = err.exceptions[0]
            self.assertIsInstance(err, RuntimeError, f"{ctx}: {refusal!r}")
            self.assertIn("不能从它自己的分发 handler 内调用", str(err), ctx)
            self.assertFalse(bus._closing,
                             f"{ctx}: 拒绝必须发生在任何关停副作用之前")
            self.assertEqual(drain_calls, [], f"{ctx}: close() 进了 drain")
            self.assertFalse(listener_cancelled, f"{ctx}: 拒绝前取消了监听任务")
            self.assertEqual(len(bus._pending_tasks), 0, ctx)
            self.assertEqual(live, [], f"{ctx}: close 后仍有悬置任务: {live}")

    async def _run_new_edge_shape(self, kind):
        """带**真听众任务**的新边族形状：``listener.done()`` 可区分「拒绝」与
        「已关停」（close 会同步 cancel listener 并 clear）。"""
        holder, refusal, drain_calls = {}, [], []

        async def handler(event):
            ctx = contextvars.Context()
            loop = asyncio.get_running_loop()
            bus = holder["bus"]
            try:
                if kind == "taskgroup":
                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(bus.close(), context=ctx)
                elif kind == "partial_funcself":
                    waiter = loop.create_future()
                    task = loop.create_task(bus.close(), context=ctx)
                    task.add_done_callback(functools.partial(waiter.set_result))
                    await waiter
                    refusal.append(task.exception())
                elif kind == "as_completed":
                    task = loop.create_task(bus.close(), context=ctx)
                    try:
                        for fut in asyncio.as_completed([task]):
                            await fut
                    except BaseException as e:       # noqa: BLE001
                        refusal.append(e)
                    else:
                        refusal.append(task.exception())
                else:
                    raise AssertionError(kind)
            except BaseException as e:               # noqa: BLE001
                refusal.append(e)

        async def fake_drain():
            drain_calls.append("drain")

        bus = _make_bus(handler)
        holder["bus"] = bus
        listener = asyncio.ensure_future(asyncio.Event().wait())
        bus.listeners.append(listener)
        with patch.object(bus, "_drain_pending_tasks", new=fake_drain):
            await bus._process_message(
                _msg(Event(event_type="t.demo", data={})), "h1")
            for _ in range(300):
                if not bus._pending_tasks and refusal:
                    break
                await asyncio.sleep(0.01)
            listener_cancelled = listener.done()
            listener.cancel()
            live = await self._settle()
        return bus, refusal, drain_calls, listener_cancelled, live

    async def test_empty_context_net_refuses_before_drain_all_kinds(self):
        """围栏必须发生在副作用之前：进入 ``_drain_pending_tasks`` 即失败。

        drain 被替换为记录器，因此即使围栏失灵也不会 hang——本测试是
        ``m109-r3-redis-dispatch-graph-net-off`` 与
        ``m109-r3-redis-net-callback-edges-off`` 两个变异点的**可断言死法**
        （对九种等待构造都跑一遍，含 r5 新增的 taskgroup / partial_funcself /
        as_completed 三族）。
        """
        for kind in ("direct", "wait", "gather", "shield", "wait_for", "nested",
                     "taskgroup", "partial_funcself", "as_completed"):
            with self.subTest(kind=kind):
                holder, refusal, drain_calls = {}, [], []

                async def handler(event, _kind=kind):
                    ctx = contextvars.Context()
                    loop = asyncio.get_running_loop()
                    bus = holder["bus"]
                    try:
                        if _kind == "direct":
                            await loop.create_task(bus.close(), context=ctx)
                        elif _kind == "wait":
                            task = loop.create_task(bus.close(), context=ctx)
                            await asyncio.wait({task})
                            refusal.append(task.exception())
                        elif _kind == "gather":
                            await asyncio.gather(
                                loop.create_task(bus.close(), context=ctx))
                        elif _kind == "shield":
                            await asyncio.shield(
                                loop.create_task(bus.close(), context=ctx))
                        elif _kind == "wait_for":
                            await asyncio.wait_for(
                                loop.create_task(bus.close(), context=ctx), 2)
                        elif _kind == "nested":
                            async def mid():
                                await loop.create_task(bus.close(), context=ctx)
                            await loop.create_task(mid(), context=ctx)
                        elif _kind == "taskgroup":
                            async with asyncio.TaskGroup() as tg:
                                tg.create_task(bus.close(), context=ctx)
                        elif _kind == "partial_funcself":
                            waiter = loop.create_future()
                            task = loop.create_task(bus.close(), context=ctx)
                            task.add_done_callback(
                                functools.partial(waiter.set_result))
                            await waiter
                            refusal.append(task.exception())
                        elif _kind == "as_completed":
                            task = loop.create_task(bus.close(), context=ctx)
                            try:
                                for fut in asyncio.as_completed([task]):
                                    await fut
                            except BaseException as e:   # noqa: BLE001
                                refusal.append(e)
                            else:
                                refusal.append(task.exception())
                    except BaseException as e:       # noqa: BLE001
                        refusal.append(e)

                async def fake_drain(_calls=drain_calls):
                    _calls.append("drain")

                bus = _make_bus(handler)
                holder["bus"] = bus
                with patch.object(bus, "_drain_pending_tasks", new=fake_drain):
                    await bus._process_message(
                        _msg(Event(event_type="t.demo", data={})), "h1")
                    live = await self._settle()
                self.assertEqual(drain_calls, [],
                                 f"{kind}: close() 在具名拒绝之前进入了 drain")
                self._assert_named_refusal(
                    refusal, bus, live, context=f"F1-net/{kind}")

    async def test_cross_loop_close_with_inflight_is_named_refusal(self):
        """F2：线程里 ``asyncio.run(bus.close())``，主循环仍有在飞分发 ⇒ 具名拒绝。"""
        entered, release, outcome = asyncio.Event(), asyncio.Event(), {}

        async def handler(event):
            entered.set()
            await release.wait()

        bus = _make_bus(handler)
        await bus._process_message(
            _msg(Event(event_type="t.demo", data={})), "h1")
        await entered.wait()

        def close_in_foreign_loop():
            try:
                asyncio.run(bus.close())
                outcome["result"] = "returned"
            except BaseException as e:           # noqa: BLE001
                outcome["result"] = f"{type(e).__name__}: {e}"

        await asyncio.get_running_loop().run_in_executor(
            None, close_in_foreign_loop)
        self.assertIn("另一个事件循环", outcome.get("result", ""),
                      f"跨循环关停未具名拒绝: {outcome}")
        self.assertFalse(bus._closing,
                         "跨循环拒绝必须发生在任何关停副作用之前")
        self.assertEqual(len(bus._pending_tasks), 1,
                         "拒绝后主循环的在飞分发不得被清掉")
        release.set()
        live = await self._settle()
        self.assertEqual(len(bus._pending_tasks), 0)
        self.assertEqual(live, [])

    async def test_cross_loop_close_refused_before_drain(self):
        """跨循环围栏必须在副作用之前：进入 drain 即失败。

        drain 被替换为记录器，因此即使围栏失灵也不会 hang——本测试是
        ``m109-r3-redis-cross-loop-close-fence-off`` 变异点的**可断言死法**。
        """
        entered, release, outcome = asyncio.Event(), asyncio.Event(), {}
        drain_calls = []

        async def handler(event):
            entered.set()
            await release.wait()

        async def fake_drain():
            drain_calls.append("drain")

        bus = _make_bus(handler)
        bus._drain_pending_tasks = fake_drain
        await bus._process_message(
            _msg(Event(event_type="t.demo", data={})), "h1")
        await entered.wait()

        def close_in_foreign_loop():
            try:
                asyncio.run(bus.close())
                outcome["result"] = "returned"
            except BaseException as e:           # noqa: BLE001
                outcome["result"] = f"{type(e).__name__}: {e}"

        await asyncio.get_running_loop().run_in_executor(
            None, close_in_foreign_loop)
        release.set()
        live = await self._settle()
        self.assertEqual(drain_calls, [],
                         "close() 在跨循环具名拒绝之前进入了 drain")
        self.assertIn("另一个事件循环", outcome.get("result", ""),
                      f"跨循环关停未具名拒绝: {outcome}")
        self.assertFalse(bus._closing)
        self.assertEqual(live, [])

    async def test_call_soon_detached_close_after_dispatch_is_now_allowed(self):
        """F3(a)（r7 放行）：``call_soon`` 分离的 close（handler 已返回、pending==0）。

        r3 把它当「明知的保守过度拒绝」声明钉住；r7 的口径是**判据只在有在飞分发时
        才有意义**——没有可保护对象就放行，这类形状退化成「晚一点的关停」，安全。
        """
        holder = {}

        async def handler(event):
            async def late():
                try:
                    await holder["bus"].close()
                except BaseException as e:       # noqa: BLE001
                    holder["refusal"] = e
                holder["ran"] = True

            asyncio.get_running_loop().call_soon(
                lambda: asyncio.ensure_future(late()))

        bus = _make_bus(handler)
        holder["bus"] = bus
        await bus._process_message(
            _msg(Event(event_type="t.demo", data={})), "h1")
        live = await self._settle()
        self.assertTrue(holder.get("ran"), "detached close 未跑")
        self.assertNotIn("refusal", holder, f"r7：pending==0 时不该拒绝: {holder}")
        self.assertTrue(bus._closing, "关停请求必须被受理")
        self.assertEqual(bus._teardown_runs, 1, "teardown 必须恰好一次")
        self.assertEqual(live, [])

    async def test_detached_task_after_dispatch_is_now_allowed(self):
        """F3(b)（r7 放行）：分发结束后 detached 任务里的 close（marker 还在）。

        marker 只是**提前拒绝**的线索，不是安全机制；此时 pending==0，纳不进任何
        在飞分发，放行后 teardown 立即完成。L1 的独立检验力由
        ``Test109R6LineageMarker``（分发**仍在飞**时 marker 是唯一判据）钉住。
        """
        holder = {}

        async def late():
            await asyncio.sleep(0.2)             # 此时分发早已结束（pending==0）
            try:
                await holder["bus"].close()
            except BaseException as e:           # noqa: BLE001
                holder["refusal"] = e
            holder["ran"] = True

        async def handler(event):
            asyncio.create_task(late())

        bus = _make_bus(handler)
        holder["bus"] = bus
        await bus._process_message(
            _msg(Event(event_type="t.demo", data={})), "h1")
        for _ in range(200):
            if holder.get("ran"):
                break
            await asyncio.sleep(0.02)
        live = await self._settle()
        self.assertTrue(holder.get("ran"), "detached close 未跑")
        self.assertNotIn("refusal", holder, f"r7：pending==0 时不该拒绝: {holder}")
        self.assertTrue(bus._closing)
        self.assertEqual(bus._teardown_runs, 1)
        self.assertEqual(len(bus._pending_tasks), 0)
        self.assertEqual(live, [])

    def test_contextvar_name_matches_docs(self):
        """声明↔实现：ContextVar 名字必须与文档一致（r3 验收者第 1 处过度声明）。"""
        import plaita.event.redis as redis_module
        self.assertEqual(redis_module._IN_DISPATCH_BUS.name,
                         "plaita_redis_in_dispatch_bus")

    async def test_shield_refusal_log_scope_declared(self):
        """声明↔实现：shield 拒绝**在 handler 内被接住**时不得有 handler 失败日志。

        （r3 验收者第 3 处过度声明：文档曾称该拒绝「经 ``_process_event`` 的
        handler 失败日志」可见；作者自己的 shield 测试在 handler 内接住，
        那条日志从未触发。这里把两种口径都钉住。）
        """
        holder = {}

        async def caught_handler(event):
            try:
                await asyncio.shield(holder["bus"].close())
            except BaseException:                # noqa: BLE001 - 故意接住
                pass

        catch = _Catch()
        logger = logging.getLogger("plaita.event.redis")
        logger.addHandler(catch)
        try:
            bus = _make_bus(caught_handler)
            holder["bus"] = bus
            await bus._process_message(
                _msg(Event(event_type="t.demo", data={})), "h1")
            live = await self._settle()
        finally:
            logger.removeHandler(catch)
        self.assertEqual(live, [])
        self.assertEqual(
            [m for m in catch.lines if "处理事件" in m and "失败" in m], [],
            f"handler 内接住的 shield 拒绝不得触发 handler 失败日志: {catch.lines}")

        async def uncaught_handler(event):
            await asyncio.shield(holder["bus"].close())

        catch2 = _Catch()
        logger.addHandler(catch2)
        try:
            bus2 = _make_bus(uncaught_handler)
            holder["bus"] = bus2
            await bus2._process_message(
                _msg(Event(event_type="t.demo", data={})), "h1")
            live2 = await self._settle()
        finally:
            logger.removeHandler(catch2)
        self.assertEqual(live2, [])
        self.assertEqual(
            len([m for m in catch2.lines if "处理事件" in m and "失败" in m]), 1,
            f"未接住的 shield 拒绝应经 handler 失败日志可见一次: {catch2.lines}")


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
    """D4a：EventSubscription 空类型给具名中文原因。

    口径修正（r3 验收者第 2 处过度声明）：两处**共享前缀**
    ``event_type 不能为空：``，但**正文不同**——``EventNode`` 说「event 节点必须…」、
    ``EventSubscription`` 说「订阅必须…」。它们是**两个** validator，不是「同一条原因」。
    """

    def test_empty_type_names_reason(self):
        from pydantic import ValidationError
        with self.assertRaises(ValidationError) as ctx:
            EventSubscription(event_type="")
        self.assertIn("event_type 不能为空", str(ctx.exception))
        self.assertIn("如需通配请显式写", str(ctx.exception))

    def test_prefix_shared_but_body_differs_from_event_node(self):
        """钉住「同前缀、不同正文」，防止文档再写成「同一条原因」。"""
        from pydantic import ValidationError
        from plaita.node.event_node import EventNode

        with self.assertRaises(ValidationError) as sub_ctx:
            EventSubscription(event_type="")
        with self.assertRaises(ValidationError) as node_ctx:
            EventNode(event_type="")
        sub_msg, node_msg = str(sub_ctx.exception), str(node_ctx.exception)
        prefix = "event_type 不能为空："
        self.assertIn(prefix, sub_msg)
        self.assertIn(prefix, node_msg)
        self.assertIn("订阅必须写明要等待的事件类型", sub_msg)
        self.assertIn("event 节点必须写明要等待的事件类型", node_msg)
        self.assertNotEqual(sub_msg, node_msg)

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

    声明口径（**不是「uniform」**，也**不是「memory 一律静默排除」**——
    r3 验收者第 4 处过度声明）：memory 仅在 ``event_type=…`` 这条 list 条件下
    静默排除（0 命中）；``correlation_id="CID"`` 与全量 SCAN 会**静默返回该行**
    （1 命中、0 日志）。Redis 在 correlation 索引命中该行时抛 ``EventError``
    （loud、无名），``event_type=…`` 落空静默空；sqlalchemy 跳过并具名点名。
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

        # memory：event_type 条件静默排除（0 命中）；correlation / 全量**返回该行**
        memory = InMemoryEventBus()
        await memory.subscription_storage.store_subscription(rogue)
        mem_rows = await memory.subscription_storage.list_subscriptions(
            event_type="ok.type")
        self.assertEqual(mem_rows, [])
        mem_corr = await memory.subscription_storage.list_subscriptions(
            correlation_id="CID")
        self.assertEqual([s.subscription_id for s in mem_corr], ["rogue"],
                         "memory correlation 路径的声明漂移：应为 1 命中、静默返回")
        mem_all = await memory.subscription_storage.list_subscriptions()
        self.assertEqual([s.subscription_id for s in mem_all], ["rogue"],
                         "memory 全量 SCAN 的声明漂移：应为 1 命中、静默返回")

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
