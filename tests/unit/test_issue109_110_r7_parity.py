"""#109 r7：**按构造**消除自关停危险（关停顺序 + 归属），判据降级为提前拒绝。

r6 被验收者以两个决定性反例判负：

* **CE-1**：两层未知持有者 ``Outer(Gate(f))`` 让 L2 的一层 ``vars()`` 扫描落空 ⇒
  ``close()`` 被放行 ⇒ drain 取消正在 ``await close()`` 的任务（自取消 P0）。
* **CE-2**：在 dispatch 之外创建、被某个分发 handler 等待的 close 任务，唤醒边是
  **纯 asyncio 簿记**（``create_task`` 的 ``call_soon`` 先于 awaiter 注册
  ``__wakeup``）⇒ L2 的首 tick 快照结构性看不到 ⇒ 放行 ⇒ ``Task.cancel`` 沿
  ``_fut_waiter`` 环无界递归 ``RecursionError`` + 「Task was destroyed but it is
  pending」。

r7 不再改判据，改**关停的构造**：

1. ``close()`` 是总体、非阻塞、幂等的关停**请求**：受理时同步置 ``_closing``，在
   **总线构造 context**（``_owner_context``）里创建 teardown 任务，返回同一个 future；
   ``close()`` 用 ``asyncio.shield`` 等它 ⇒ teardown 永远不是调用者的 context 后代。
2. teardown **不取消**在飞分发（超时只具名 WARNING 后放手），监听任务**最后**才逐个
   cancel ⇒ 既不会「取消一个正在等自己的任务」，也不会沿 ``gather`` 的
   ``_GatheringFuture.cancel`` 递归。

本文件对 §3 列出的**每一种调用者形状**断言**症状**（不依赖 ``close_graph_verdict``
的内部结论）：

* 有界（不 hang）；无 ``RecursionError``；无「Task was destroyed but it is pending」/
  「exception was never retrieved」（由本循环的 exception handler 捕获）；
* handler 不被 ``_closing`` 取消（记录 ``CancelledError``）；
* 总线**恰好关停一次**（``_teardown_runs == 1``、五个后端组件各关闭一次），重复
  ``close()`` 幂等；
* 判据可以提前拒绝（那就用外部 close 收尾）；拒绝/放行都不影响上述症状。
"""

import asyncio
import contextvars
import gc
import threading
import unittest
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import plaita.event._selfclose_guard as guard_mod
import plaita.event.redis as redis_mod
from plaita.event.core import Event
from plaita.event.redis import RedisEventBus, _IN_DISPATCH_BUS, _close_graph_verdict

_SELF_CLOSE_MARK = "不能从它自己的分发 handler 内调用"
_INCOMPLETE_MARK = "error_kind=wake-graph-incomplete"
# 每个形状的关停等待压到 0.15s：形状安全性与超时长度无关。
_SHAPE_DRAIN_TIMEOUT = 0.15


class _Counter:
    """计数关闭次数的后端组件（证明「恰好关停一次」）。"""

    def __init__(self, method="close"):
        self.closes = 0
        self._method = method

    async def close(self):
        self.closes += 1

    async def aclose(self):
        self.closes += 1

    async def is_event_processed(self, e, h):
        return False

    async def mark_event_processed(self, e, h):
        pass

    async def record_processing_attempt(self, *a, **k):
        pass


class _Watch:
    """捕获本事件循环的 exception handler 消息（RecursionError / destroyed-pending）。"""

    _KEYS = ("RecursionError", "destroyed but it is pending",
             "never retrieved")

    def __init__(self):
        self.messages = []
        self.loop = asyncio.get_running_loop()
        self._prev = self.loop.get_exception_handler()
        self.loop.set_exception_handler(self._handle)

    def _handle(self, loop, context):
        message = str(context.get("message", ""))
        exc = context.get("exception")
        if exc is not None:
            message = f"{message} | {type(exc).__name__}: {exc}"
        self.messages.append(message)
        if self._prev is not None:
            self._prev(loop, context)

    def bad(self):
        gc.collect()
        return [m for m in self.messages if any(k in m for k in self._KEYS)]


def _make_bus(handler):
    bus = RedisEventBus(redis_url="redis://unused/0")
    bus.processing_tracker = _Counter()
    bus.event_storage = _Counter()
    bus.subscription_storage = _Counter()
    bus.pubsub = _Counter()
    bus.redis = _Counter()
    bus.handlers["h1"] = (handler, "*", None, None)
    return bus


def _components(bus):
    return [bus.pubsub, bus.redis, bus.event_storage, bus.subscription_storage,
            bus.processing_tracker]


def _msg(event):
    return {"type": "message", "data": event.model_dump_json().encode()}


def _event():
    return Event(event_type="t.demo", data={})


async def _wait_until(predicate, timeout=3.0, step=0.01):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(step)
    return predicate()


async def _settle_tasks(timeout=3.0):
    return await _wait_until(
        lambda: not [t for t in asyncio.all_tasks()
                     if t is not asyncio.current_task() and not t.done()],
        timeout)


async def _close_external(bus, timeout=5.0):
    """外部（非分发图内）收尾：首次受理或幂等，等 teardown 完成。"""
    await asyncio.wait_for(bus.close(), timeout=timeout)


def _is_named_refusal(err):
    text = str(err)
    return _SELF_CLOSE_MARK in text or _INCOMPLETE_MARK in text


class Gate:
    """自定义 awaitable：future 直接挂在实例属性 ``_f``（L2 一层扫描能看到）。"""

    def __init__(self, loop):
        self._f = loop.create_future()

    def __await__(self):
        return self._f.__await__()


class Outer:
    """**两层**未知持有者：L2 只扫一层 ``__dict__``，看不到内层的已武装 future。"""

    def __init__(self, inner):
        self.inner = inner

    def release(self):
        if not self.inner._f.done():
            self.inner._f.set_result(None)


class SlotGate:
    """残差 R1 载体：future 藏在 ``__slots__``（无 ``__dict__``）。"""

    __slots__ = ("f",)


class Test109R7EndToEndShapes(unittest.IsolatedAsyncioTestCase):
    """§3 的调用者形状逐个端到端：只断言症状。"""

    def setUp(self):
        self._saved_timeout = redis_mod.DISPATCH_DRAIN_TIMEOUT_SECONDS
        redis_mod.DISPATCH_DRAIN_TIMEOUT_SECONDS = _SHAPE_DRAIN_TIMEOUT

    def tearDown(self):
        redis_mod.DISPATCH_DRAIN_TIMEOUT_SECONDS = self._saved_timeout

    def _assert_e2e(self, bus, watch, obs, comps):
        bad = watch.bad()
        self.assertEqual(bad, [],
                         f"loop/GC 报告症状（递归/destroyed-pending/未取回异常）: {bad}")
        self.assertTrue(bus._closing, "总线未进入关停态")
        self.assertEqual(bus._teardown_runs, 1, "总线必须恰好关停一次")
        self.assertFalse(obs.get("handler_cancelled", False),
                         "handler 被 _closing 连坐取消")
        self.assertNotIn("handler_error", obs,
                         f"handler 出现非拒绝异常: {obs.get('handler_error')}")
        for comp in comps:
            self.assertEqual(comp.closes, 1, "后端组件必须恰好关闭一次")
        self.assertIsNone(bus.pubsub)
        self.assertIsNone(bus.redis)

    # ---- 形状①：handler 直接调用（并等待）close() -------------------------
    async def test_shape_handler_calling_and_awaiting_close(self):
        obs, holder = {"handler_cancelled": False, "refusal": None}, {}

        async def handler(event):
            try:
                await holder["bus"].close()
            except asyncio.CancelledError:
                obs["handler_cancelled"] = True
                raise
            except BaseException as e:            # noqa: BLE001
                obs["refusal"] = e

        bus = _make_bus(handler)
        holder["bus"] = bus
        comps = _components(bus)
        watch = _Watch()
        await bus._process_message(_msg(_event()), "h1")
        self.assertTrue(await _settle_tasks(), "handler 未在界内结束（hang）")
        self.assertIsInstance(obs["refusal"], RuntimeError,
                              "最常见标记形状应被提前拒绝")
        self.assertIn(_SELF_CLOSE_MARK, str(obs["refusal"]))
        self.assertFalse(bus._closing, "拒绝必须发生在任何关停副作用之前")
        await _close_external(bus)                # 外部收尾：总线仍然恰好关停一次
        self.assertTrue(await _settle_tasks())
        self._assert_e2e(bus, watch, obs, comps)

    # ---- 形状②：handler 的后代用 context=Context()（L1 失灵，一层门） -----
    async def test_shape_handler_descendant_with_empty_context(self):
        obs, holder = {"handler_cancelled": False, "refusal": None}, {}

        async def handler(event):
            loop = asyncio.get_running_loop()
            gate = Gate(loop)

            async def closer():
                try:
                    await holder["bus"].close()
                except asyncio.CancelledError:
                    obs["sub_cancelled"] = True
                    raise
                except BaseException as e:        # noqa: BLE001
                    obs["refusal"] = e

            task = asyncio.create_task(closer(), context=contextvars.Context())

            def _done(t):
                if not gate._f.done():
                    gate._f.set_result(None)

            task.add_done_callback(_done)
            try:
                await gate
            except asyncio.CancelledError:
                obs["handler_cancelled"] = True
                raise

        bus = _make_bus(handler)
        holder["bus"] = bus
        comps = _components(bus)
        watch = _Watch()
        await bus._process_message(_msg(_event()), "h1")
        self.assertTrue(await _settle_tasks(), "形状未在界内结束（hang）")
        self.assertIsInstance(obs["refusal"], RuntimeError, obs)
        self.assertTrue(_is_named_refusal(obs["refusal"]), str(obs["refusal"]))
        await _close_external(bus)
        self.assertTrue(await _settle_tasks())
        self._assert_e2e(bus, watch, obs, comps)

    # ---- 形状③：CE-1 两层未知持有者 Outer(Gate(f)) ------------------------
    async def test_shape_two_layer_unknown_holder_ce1(self):
        """CE-1：判据漏判（放行）也必须安全——不取消、无递归、有界、恰好一次。"""
        obs, holder = {"handler_cancelled": False, "refusal": None,
                       "handler_returned": False}, {}

        async def handler(event):
            loop = asyncio.get_running_loop()
            outer = Outer(Gate(loop))

            async def closer():
                try:
                    await holder["bus"].close()
                except asyncio.CancelledError:
                    obs["sub_cancelled"] = True
                    raise
                except BaseException as e:        # noqa: BLE001
                    obs["refusal"] = e

            task = asyncio.create_task(closer(), context=contextvars.Context())
            task.add_done_callback(lambda t: outer.release())
            try:
                await outer.inner
                obs["handler_returned"] = True
            except asyncio.CancelledError:
                obs["handler_cancelled"] = True
                raise

        bus = _make_bus(handler)
        holder["bus"] = bus
        comps = _components(bus)
        watch = _Watch()
        await bus._process_message(_msg(_event()), "h1")
        self.assertTrue(await _settle_tasks(), "CE-1 未在界内结束（hang）")
        self.assertIsNone(obs["refusal"], "CE-1 是判据漏判形状（本用例即证明漏判无碍）")
        self.assertTrue(obs["handler_returned"], "在飞分发必须能跑完（不得被取消）")
        self.assertTrue(bus._closing)
        self.assertEqual(bus._teardown_runs, 1)
        await _close_external(bus)                # 幂等
        self.assertEqual(bus._teardown_runs, 1)
        self._assert_e2e(bus, watch, obs, comps)

    # ---- 形状④：线程 / executor + run_coroutine_threadsafe + wrap_future ---
    async def test_shape_thread_run_coroutine_threadsafe_wrap_future(self):
        obs, holder = {"handler_cancelled": False}, {}
        release = asyncio.Event()

        async def handler(event):
            try:
                await release.wait()
            except asyncio.CancelledError:
                obs["handler_cancelled"] = True
                raise

        bus = _make_bus(handler)
        holder["bus"] = bus
        comps = _components(bus)
        watch = _Watch()
        loop = asyncio.get_running_loop()
        await bus._process_message(_msg(_event()), "h1")
        await asyncio.sleep(0.02)                 # 让分发进入 handler（在飞）

        submitted = {}

        def worker():                             # 另一个线程投递关停请求
            submitted["future"] = asyncio.run_coroutine_threadsafe(
                holder["bus"].close(), loop)

        thread = threading.Thread(target=worker)
        thread.start()
        await asyncio.wait_for(loop.run_in_executor(None, thread.join), timeout=5)
        wrapped = asyncio.wrap_future(submitted["future"])
        await asyncio.wait_for(wrapped, timeout=5)
        obs["wrapped_exc"] = wrapped.exception()
        release.set()
        self.assertTrue(await _settle_tasks(), "线程形状未在界内结束（hang）")
        self._assert_e2e(bus, watch, obs, comps)

    # ---- 形状⑤：CE-2 TOCTOU（close 任务在 dispatch 之外创建、被 handler 等待）
    async def test_shape_ce2_close_task_created_outside_dispatch(self):
        obs, holder = {"handler_cancelled": False, "handler_returned": False,
                       "refusal": None}, {}

        async def handler(event):
            try:
                await holder["close_task"]
                obs["handler_returned"] = True
            except asyncio.CancelledError:
                obs["handler_cancelled"] = True
                raise
            except BaseException as e:            # noqa: BLE001
                obs["refusal"] = e

        bus = _make_bus(handler)
        holder["bus"] = bus
        comps = _components(bus)
        watch = _Watch()
        close_task = asyncio.create_task(bus.close())   # 在 dispatch 之外创建
        holder["close_task"] = close_task
        await bus._process_message(_msg(_event()), "h1")
        try:
            await asyncio.wait_for(close_task, timeout=5)
        except RuntimeError as e:                 # 判据若命中：提前拒绝也算合格
            obs["refusal"] = e
        self.assertTrue(await _settle_tasks(), "CE-2 未在界内结束（hang）")
        self.assertFalse(close_task.cancelled(), "close 任务不得被取消")
        self.assertTrue(obs["handler_returned"], "在飞分发必须能跑完（不得被取消）")
        await _close_external(bus)
        self.assertEqual(bus._teardown_runs, 1)
        self._assert_e2e(bus, watch, obs, comps)

    # ---- 形状⑥：__slots__ 持有者（残差 R1） -------------------------------
    async def test_shape_slots_holder(self):
        obs, holder = {"handler_cancelled": False, "refusal": None}, {}

        async def handler(event):
            loop = asyncio.get_running_loop()
            gate = SlotGate()
            gate.f = loop.create_future()

            async def closer():
                try:
                    await holder["bus"].close()
                except asyncio.CancelledError:
                    obs["sub_cancelled"] = True
                    raise
                except BaseException as e:        # noqa: BLE001
                    obs["refusal"] = e

            task = asyncio.create_task(closer(), context=contextvars.Context())
            task.add_done_callback(lambda t: gate.f.set_result(None))
            try:
                await gate.f
            except asyncio.CancelledError:
                obs["handler_cancelled"] = True
                raise

        bus = _make_bus(handler)
        holder["bus"] = bus
        comps = _components(bus)
        watch = _Watch()
        await bus._process_message(_msg(_event()), "h1")
        self.assertTrue(await _settle_tasks(), "slots 形状未在界内结束（hang）")
        await _close_external(bus)
        self._assert_e2e(bus, watch, obs, comps)

    # ---- 形状⑦：合法 TaskGroup，仅 close 任务（零在飞分发） ---------------
    async def test_shape_taskgroup_with_only_close_task_works(self):
        """验收者报告的**未声明过度拒绝**：必须可用（r7 起不再拒绝）。"""
        bus = _make_bus(lambda e: None)
        comps = _components(bus)
        watch = _Watch()

        async def caller():
            async with asyncio.TaskGroup() as tg:
                tg.create_task(bus.close())
                await asyncio.sleep(0)

        await asyncio.wait_for(caller(), timeout=5)
        self.assertTrue(bus._closing)
        self.assertEqual(bus._teardown_runs, 1)
        await _close_external(bus)
        self.assertEqual(bus._teardown_runs, 1)
        self.assertEqual(watch.bad(), [])
        for comp in comps:
            self.assertEqual(comp.closes, 1)

    # ---- 形状⑧：嵌套 TaskGroup，仅 close 任务（零在飞分发） ---------------
    async def test_shape_nested_taskgroups_with_only_close_task_works(self):
        bus = _make_bus(lambda e: None)
        comps = _components(bus)
        watch = _Watch()

        async def caller():
            async with asyncio.TaskGroup() as g2:
                async with asyncio.TaskGroup() as g1:
                    g1.create_task(bus.close())
                    await asyncio.sleep(0)
                await asyncio.sleep(0)

        await asyncio.wait_for(caller(), timeout=5)
        self.assertTrue(bus._closing)
        self.assertEqual(bus._teardown_runs, 1)
        await _close_external(bus)
        self.assertEqual(bus._teardown_runs, 1)
        self.assertEqual(watch.bad(), [])
        for comp in comps:
            self.assertEqual(comp.closes, 1)

    # ---- 形状⑨：在飞分发 + handler 同时等 close（不取消的硬约束） ---------
    async def test_shape_handler_awaits_close_while_undetected_caller_finishes(self):
        """handler 自己也 ``await bus.close()`` 但判据因换 context 漏判：

        这是最接近 P0 的形状（close 的调用者与等待者都是分发图内的任务）。
        r7 的答案：teardown 有界等待后**放手**，close future 一 resolve，handler 立刻
        跑完——没有被取消、没有递归、没有悬置。
        """
        obs, holder = {"handler_cancelled": False, "handler_returned": False,
                       "refusal": None}, {}

        async def handler(event):
            loop = asyncio.get_running_loop()
            outer = Outer(Gate(loop))

            async def closer():                   # 空 context：L1 失灵；两层：L2 失灵
                try:
                    await holder["bus"].close()
                except asyncio.CancelledError:
                    obs["sub_cancelled"] = True
                    raise
                except BaseException as e:        # noqa: BLE001
                    obs["refusal"] = e

            task = asyncio.create_task(closer(), context=contextvars.Context())
            task.add_done_callback(lambda t: outer.release())
            try:
                await outer.inner
                obs["handler_returned"] = True
            except asyncio.CancelledError:
                obs["handler_cancelled"] = True
                raise

        bus = _make_bus(handler)
        holder["bus"] = bus
        comps = _components(bus)
        watch = _Watch()
        await bus._process_message(_msg(_event()), "h1")
        self.assertTrue(await _settle_tasks(), "形状未在界内结束（hang）")
        self.assertTrue(obs["handler_returned"])
        self.assertFalse(obs["handler_cancelled"])
        await _close_external(bus)
        self._assert_e2e(bus, watch, obs, comps)


class Test109R7RequestSemantics(unittest.IsolatedAsyncioTestCase):
    """``close()`` 的请求语义：非阻塞、幂等、teardown 归属总线构造 context。"""

    async def test_request_close_is_idempotent_and_non_blocking(self):
        bus = _make_bus(lambda e: None)
        comps = _components(bus)
        first = bus.request_close()
        second = bus.request_close()
        self.assertIs(first, second, "重复请求必须返回同一个 future")
        self.assertFalse(first.done(), "request_close() 必须是非阻塞的")
        self.assertTrue(bus._closing)
        await asyncio.wait_for(asyncio.shield(first), timeout=5)
        self.assertEqual(bus._teardown_runs, 1)
        await _close_external(bus)
        self.assertEqual(bus._teardown_runs, 1)
        for comp in comps:
            self.assertEqual(comp.closes, 1)

    async def test_teardown_context_is_bus_owner_not_caller(self):
        """teardown **运行时**看到的是总线构造时的 context；调用者的 marker 不进入。"""
        bus = _make_bus(lambda e: None)
        comps = _components(bus)
        observed = {}
        original_teardown = bus._teardown

        async def wrapped_teardown():                 # 记录 teardown 实际跑在哪个 context
            observed["marker"] = _IN_DISPATCH_BUS.get()
            await original_teardown()

        bus._teardown = wrapped_teardown             # 实例属性：不绑定 self
        caller_context = contextvars.copy_context()
        caller_context.run(_IN_DISPATCH_BUS.set, bus)     # 伪造调用者带 marker

        async def caller():
            bus.request_close()
            observed["caller_marker"] = _IN_DISPATCH_BUS.get()

        await asyncio.get_running_loop().create_task(caller(), context=caller_context)
        await asyncio.wait_for(asyncio.shield(bus._close_future), timeout=5)
        self.assertIs(observed["caller_marker"], bus,
                      "调用者 context 里的 marker 应保持可见（未被改写）")
        self.assertIsNone(observed["marker"],
                          "teardown 的 context 不得继承调用者的 dispatch marker")
        await _close_external(bus)
        for comp in comps:
            self.assertEqual(comp.closes, 1)


class Test109R7NamedExceptionConvergence(unittest.IsolatedAsyncioTestCase):
    """验收者缺陷：非 ``AttributeError`` 描述符不得**裸**逃出 ``close_graph_verdict``。"""

    @staticmethod
    def _cone_verdict(obj):
        loop = asyncio.get_running_loop()
        root = loop.create_future()

        def make_cb():
            held = obj

            def cb(_fut):
                return held
            return cb

        root.add_done_callback(make_cb())
        verdict, visited, reason = _close_graph_verdict(root, {})
        root.cancel()
        return verdict, visited, reason

    async def test_exploding_known_attribute_is_named_incomplete(self):
        class ExplodingQueue(asyncio.Queue):
            @property
            def _getters(self):
                raise ValueError("boom-getters")

            @_getters.setter
            def _getters(self, value):
                pass

        verdict, _v, reason = self._cone_verdict(ExplodingQueue())
        self.assertEqual(verdict, "incomplete", verdict)
        self.assertIn("ValueError", reason or "")

    async def test_exploding_dict_is_named_incomplete(self):
        class ExplodingDict:
            @property
            def __dict__(self):
                raise ValueError("boom-dict")

        verdict, _v, reason = self._cone_verdict(ExplodingDict())
        self.assertEqual(verdict, "incomplete", verdict)
        self.assertIn("ValueError", reason or "")


class Test109R7SelfTestNoPoison(unittest.TestCase):
    """验收者缺陷：无运行循环时 ``ensure_self_test()`` 不得毒化本进程。"""

    def setUp(self):
        guard_mod.reset_self_test()

    def tearDown(self):
        guard_mod.reset_self_test()

    def test_loopless_call_is_not_cached_and_close_still_works(self):
        status = guard_mod.ensure_self_test()          # 没有 running loop
        self.assertIsNotNone(status, "无循环时应返回「未运行」文案")
        self.assertIn("未运行自检", status)
        ran, cached = guard_mod.self_test_status()
        self.assertFalse(ran, "无循环的结果**不得**被缓存")
        self.assertIsNone(cached)

        # 端到端反证 DoS：带**在飞分发**的 close() 必须在真循环里跑自检并成功。
        release = asyncio.Event()

        async def handler(event):
            await release.wait()

        bus = _make_bus(handler)

        async def scenario():
            event = _event()
            await bus._process_message(_msg(event), "h1")
            with patch.object(redis_mod, "DISPATCH_DRAIN_TIMEOUT_SECONDS", 0.1):
                await asyncio.wait_for(bus.close(), timeout=5)
            release.set()
            await _settle_tasks()
            return bus

        asyncio.run(scenario())
        self.assertTrue(bus._closing)
        self.assertEqual(bus._teardown_runs, 1)
        ran, cached = guard_mod.self_test_status()
        self.assertTrue(ran, "循环内 close() 必须跑真实自检并缓存")
        self.assertIsNone(cached)


if __name__ == "__main__":
    unittest.main()
