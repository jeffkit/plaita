"""#109/#110 r11：把「收养了但入口没跑」从**声明**变成**结构**（A）+ 敌对 Task 子类（B）。

r10 的残留（本文件逐条钉住的**修前**症状，全部带 2 s 有界看门狗，绝不靠 timeout 通过）：

* **A/入口没跑**：``isinstance(x, asyncio.Task)`` 对任何真 Task 都为真，但「真 Task」不等于
  「它跑我们的 ``_teardown_entry``」。工厂交回一个包着**别的** coroutine 的 Task 时
  （r11 实测；同类还有「返回前就被 cancel」「建在**另一个**事件循环上」），r10 会静默收养：
  入口从未 await、``_teardown`` 从未跑过 ⇒ ``_close_future`` **永不结算**，两次
  ``close()`` 都永久挂起（``(True, PENDING, True, 0)``、五个组件 ``[0×5]``，外加
  ``RuntimeWarning: coroutine ... _teardown_entry was never awaited``）。
* **B/敌对 Task 子类**：覆写 ``add_done_callback`` 让它抛异常的子类同样通过
  ``isinstance`` ⇒ ``request_close`` 对返回值的使用**裸抛且不回滚**（无 ``_closing``
  恢复、无凭据清理）。

r11 的修法（见 ``plaita/event/redis.py`` 的 ``request_close`` / ``_spawn_teardown`` /
``_on_teardown_done`` docstring）：

1. ``_teardown_entry`` 第一行置「已进入」标记（绑在该次受理的 holder 上，holder 绑定在
   任务自己身上）；收养的 Task 结束时若标记未置位 ⇒ 具名 ``teardown-never-entered``
   （``_teardown_runs`` 如实为 0、入口 coroutine 若从未启动则 close 掉）。判据是
   **「任务结束」这一事件**，不是计时。
2. 跨循环的 Task、以及覆写了 ``add_done_callback``/``done``/``cancelled``/``exception``
   的敌对 Task：``_spawn_teardown`` 结构性拒收（reason 在 ``__cause__``、整体回滚、
   尽力中和）。
3. 合法包装协程（``await <我们的 coro>``）跑到入口那一行即置位 ⇒ 照常完成，不受影响。

C 的三处更正也钉在这里：收养瞬间 ``_teardown_task`` 是**真的在飞 Task**（不是 ``None``，
第三元素 ≠ None），收养路径存在**零重锚**的 5 条 request_close 变异（见变异表/报告）。
"""

import asyncio
import gc
import sys
import unittest
import warnings
from typing import Any, Dict, List, Optional, Set

import pytest

pytest.importorskip("redis")

from plaita.event.redis import (          # noqa: E402
    RedisEventBus,
    TEARDOWN_FOREIGN_LOOP_REASON,
    TEARDOWN_HOSTILE_TASK_REASON,
    TEARDOWN_NEVER_ENTERED_KIND,
    _task_uses_stdlib_methods,
)

WATCHDOG = 2.0
SETTLE_TICKS = 3


# --------------------------------------------------------------------------
# 总线与观测
# --------------------------------------------------------------------------
class _Counter:
    """计数关闭次数的后端组件（``aclose`` 优先，redis-py ≥5 / fakeredis 形状）。"""

    def __init__(self):
        self.closes = 0

    async def aclose(self):
        self.closes += 1

    async def close(self):
        self.closes += 1

    async def is_event_processed(self, e, h):
        return False

    async def mark_event_processed(self, e, h):
        pass

    async def record_processing_attempt(self, *a, **k):
        pass


def _make_bus():
    bus = RedisEventBus(redis_url="redis://unused/0")
    comps = [_Counter() for _ in range(5)]
    (bus.pubsub, bus.redis, bus.event_storage,
     bus.subscription_storage, bus.processing_tracker) = comps
    bus.handlers["h1"] = (lambda e: None, "*", None, None)
    return bus, comps


def _components(bus):
    return [bus.pubsub, bus.redis, bus.event_storage,
            bus.subscription_storage, bus.processing_tracker]


def _future_state(bus) -> str:
    fut = bus._close_future
    if fut is None:
        return "None"
    if fut.cancelled():
        return "cancelled"
    if fut.done():
        return "done-ok" if fut.exception() is None else "done-exc"
    return "PENDING"


def _state(bus) -> tuple:
    """``(_closing, _close_future, _teardown_task 是否为真在飞句柄, _teardown_runs)``。"""
    return (bus._closing, _future_state(bus),
            bus._teardown_task is not None, bus._teardown_runs)


def _new_task(coro, loop, kwargs):
    """按版本构造 ``Task``（3.11+ 有 ``context=``，3.10 没有）。"""
    if sys.version_info >= (3, 11):
        return asyncio.Task(coro, loop=loop, context=kwargs.get("context"))
    return asyncio.Task(coro, loop=loop)


class _Record:
    def __init__(self, outcome, exc, bus, comps, sink, adopted, adopt_state, orphans,
                 warnings_, reports):
        self.outcome = outcome
        self.exc = exc
        self.bus = bus
        self.comps = comps
        self.sink = sink
        self.adopted = adopted
        self.adopt_state = adopt_state
        self.orphans = orphans
        self.runtime_warnings = warnings_
        self.reports = reports

    @property
    def named_kind(self) -> Optional[str]:
        if isinstance(self.exc, RuntimeError):
            text = str(self.exc)
            for kind in (TEARDOWN_NEVER_ENTERED_KIND, "teardown-spawn-failed",
                         "teardown-incomplete", "cross-loop-close"):
                if kind in text:
                    return kind
        return None

    @property
    def cause_text(self) -> str:
        return str(getattr(self.exc, "__cause__", "") or "")

    @property
    def closes(self):
        return [c.closes for c in self.comps]

    @property
    def runs(self):
        return self.bus._teardown_runs

    @property
    def state(self):
        return _state(self.bus)

    @property
    def orphan_count(self):
        return len(self.orphans)

    def leaked_reports(self):
        return [str(r.get("message", "")) for r in self.reports
                if "never retrieved" in str(r.get("message", ""))
                or "was destroyed but it is pending" in str(r.get("message", ""))]


async def _driver(bus, factory, sink):
    """``close()`` 的核心（guard 之后就是 shield(request_close())）+ 收养瞬间快照。

    不直接 ``await bus.close()``，因为 ``_teardown_task`` 只在**受理那一刻**是「在飞的
    收养句柄」——C2 要钉的正是这个瞬间（修前它被误报成 ``None``）。
    """
    loop = asyncio.get_running_loop()
    saved = loop.get_task_factory()
    me = asyncio.current_task()
    before = set(asyncio.all_tasks())
    loop.set_task_factory(factory)
    try:
        try:
            future = bus.request_close()
            sink["adopted"] = bus._teardown_task
            sink["adopt_state"] = _state(bus)
            await asyncio.shield(future)
            return ("returned", None)
        except BaseException as exc:                 # noqa: BLE001 - 断言用
            return ("raised", exc)
    finally:
        loop.set_task_factory(saved)                 # 先摘下工厂，避免收尾又造敌对任务
        for _ in range(SETTLE_TICKS):
            await asyncio.sleep(0)                   # 让被中和的任务落地（cancel 要一次 step）
        sink["orphans"] = [t for t in asyncio.all_tasks()
                           if t is not me and t not in before]
        loop.set_task_factory(saved)


async def run_shape(factory, *, bus=None, bound=WATCHDOG):
    """带看门狗跑一次 close#1；返回精确状态 + 关闭计数 + 孤儿/未取用证据。"""
    loop = asyncio.get_running_loop()
    reports = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _l, ctx: reports.append(ctx))
    if bus is None:
        bus, comps = _make_bus()
    else:
        comps = _components(bus)
    sink: Dict[str, Any] = {}
    driver = asyncio.create_task(_driver(bus, factory, sink))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            got = await asyncio.wait_for(asyncio.shield(driver), timeout=bound)
        except asyncio.TimeoutError:
            got = ("HANG/TIMEOUT", None)
        outcome, exc = got
        raw = list(sink.get("orphans", []))
        del driver
        gc.collect()
        await asyncio.sleep(0)
        gc.collect()
    loop.set_exception_handler(previous_handler)
    runtime = [w for w in caught if issubclass(w.category, RuntimeWarning)]
    return _Record(outcome, exc, bus, comps, sink, sink.get("adopted"),
                   sink.get("adopt_state"), raw, runtime, reports)


async def settle():
    for _ in range(SETTLE_TICKS):
        await asyncio.sleep(0)


async def _noop():
    return None


async def _hang_forever():
    await asyncio.Event().wait()


# --------------------------------------------------------------------------
# A 的形状：工厂
# --------------------------------------------------------------------------
def _factory_other_coro_task(loop, coro, **kwargs):
    """交回真 Task，但它跑的是**别的** coroutine（我们的入口从未被 await）。"""
    async def other():
        return "not-teardown"
    return _new_task(other(), loop, kwargs)


def _factory_other_coro_task_raises(loop, coro, **kwargs):
    """同上，但那个别的 coroutine 抛异常（必须被取用，不得 never retrieved）。"""
    async def other():
        raise ValueError("other-coro-boom")
    return _new_task(other(), loop, kwargs)


def _factory_cancel_before_return(loop, coro, **kwargs):
    """建了包我们 coroutine 的 Task，但在返回前把它 cancel 掉。"""
    task = _new_task(coro, loop, kwargs)
    task.cancel()
    return task


_FOREIGN_LOOPS: List[asyncio.AbstractEventLoop] = []


def _factory_foreign_loop(loop, coro, **kwargs):
    """把我们 coroutine 的 Task 建在**另一个**事件循环上。"""
    other = asyncio.new_event_loop()
    _FOREIGN_LOOPS.append(other)
    return asyncio.Task(coro, loop=other)


def _factory_wrapper_enters(loop, coro, **kwargs):
    """合法包装：包装协程里 ``await`` 我们的入口 coroutine（必须照常完成）。"""
    async def wrapper():
        return await coro
    return _new_task(wrapper(), loop, kwargs)


class _HostileAddDoneCallback(asyncio.Task):
    """敌对子类：覆写 ``add_done_callback`` 让它抛（r10 在这里裸抛且不回滚）。"""

    def add_done_callback(self, callback, *, context=None):
        raise RuntimeError("hostile add_done_callback")


def _hostile_factory(loop, coro, **kwargs):
    if sys.version_info >= (3, 11):
        return _HostileAddDoneCallback(coro, loop=loop, context=kwargs.get("context"))
    return _HostileAddDoneCallback(coro, loop=loop)


def _factory_instance_shadow(loop, coro, **kwargs):
    """实例级影子属性（类上没覆写，但实例上塞了一个会抛的 ``add_done_callback``）。"""
    task = _new_task(coro, loop, kwargs)

    def _boom(callback, *, context=None):
        raise RuntimeError("instance-level hostile add_done_callback")

    task.add_done_callback = _boom
    return task


# r10 五行（不得回归）
def _factory_genuine_throw(loop, coro, **kwargs):
    raise RuntimeError("factory boom")


def _factory_create_then_raise(loop, coro, **kwargs):
    if sys.version_info >= (3, 11):
        asyncio.Task(coro, loop=loop, context=kwargs.get("context"))
    else:
        asyncio.Task(coro, loop=loop)
    raise RuntimeError("factory boom")


def _factory_done_future(loop, coro, **kwargs):
    fut = loop.create_future()
    fut.set_result(None)
    return fut


class _AwaitableWithoutCallback:
    def __init__(self, loop):
        self._loop = loop

    def __await__(self):
        fut = self._loop.create_future()
        fut.set_result(None)
        return fut.__await__()


def _factory_awaitable_without_callback(loop, coro, **kwargs):
    return _AwaitableWithoutCallback(loop)


def _factory_returns_coro(loop, coro, **kwargs):
    return coro


# --------------------------------------------------------------------------
# 1) A1：真 Task 但不跑入口 —— 具名结算，绝不悬着
# --------------------------------------------------------------------------
class Test109R11NeverEnteredAdoptedTask(unittest.IsolatedAsyncioTestCase):
    async def test_task_wrapping_other_coro_is_named_not_hung(self):
        """修前：``(True, PENDING, True, 0)`` + ``[0×5]`` + 两次 close 永久超时。"""
        rec = await run_shape(_factory_other_coro_task)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT",
                            "绝不能永久挂起（看门狗 2 s 必须不触发）")
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(
            rec.adopt_state, (True, "PENDING", True, 0),
            "C2 更正：收养瞬间第三个元素是**真的在飞 Task**（不是 None）")
        self.assertEqual(rec.state, (True, "None", False, 0),
                         "结束后凭据清空以便重试；一次都没跑过 ⇒ runs 必须如实为 0")
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.orphan_count, 0)
        self.assertEqual(rec.runtime_warnings, [],
                         "不得出现 'coroutine ... _teardown_entry was never awaited'")
        self.assertEqual(rec.leaked_reports(), [])
        # close#2：同一个坏工厂下也必须**有界 + 具名**，绝不挂起、绝不关任何组件。
        rec2 = await run_shape(_factory_other_coro_task, bus=rec.bus)
        self.assertNotEqual(rec2.outcome, "HANG/TIMEOUT",
                            "修前第二次 close() 同样永久超时；修后有界具名")
        self.assertEqual(rec2.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec2.closes, [0, 0, 0, 0, 0], "一次都没跑过，不得关组件")
        self.assertEqual(rec2.runs, 0)
        # 换回健康工厂：重试真的关停一次、且不双关。
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])

    async def test_other_coro_exception_is_retrieved(self):
        """不跑入口的那个 Task 自己抛异常时，异常必须被取用 + 留档。"""
        rec = await run_shape(_factory_other_coro_task_raises)
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertIsInstance(rec.bus._teardown_task_error, ValueError,
                              "被收养 Task 的异常必须被取用（否则 GC 打 never retrieved）")
        self.assertEqual(rec.bus._teardown_error_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec.leaked_reports(), [])
        self.assertEqual(rec.orphan_count, 0)

    async def test_wrapper_that_awaits_entry_still_completes(self):
        """合法包装协程仍照常完成：判据是「入口有没有跑」，不是 coro 对象身份。"""
        rec = await run_shape(_factory_wrapper_enters)
        self.assertEqual(rec.outcome, "returned")
        self.assertEqual(rec.adopt_state, (True, "PENDING", True, 0))
        self.assertEqual(rec.state, (True, "done-ok", True, 1))
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])
        self.assertEqual(rec.orphan_count, 0)
        self.assertEqual(rec.runtime_warnings, [])
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1], "幂等：第二次不得再关")


# --------------------------------------------------------------------------
# 2) A3：返回时已 done / cancelled —— 显式具名 + runs 如实
# --------------------------------------------------------------------------
class Test109R11AlreadyDoneOrCancelled(unittest.IsolatedAsyncioTestCase):
    async def test_already_done_task_is_named(self):
        done = asyncio.ensure_future(_noop())
        await asyncio.wait_for(done, timeout=WATCHDOG)
        self.assertTrue(done.done())

        def factory(loop, coro, **kwargs):
            return done

        rec = await run_shape(factory)
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec.adopt_state, (True, "None", False, 0),
                         "返回时已 done ⇒ 走 request_close 的同步 `if done()` 分支："
                         "判决在受理调用**返回前**就落地（凭据已清），死句柄不回填")
        self.assertEqual(rec.state, (True, "None", False, 0))
        self.assertEqual(rec.runs, 0, "一次都没跑过 ⇒ 不得谎报 runs")
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.orphan_count, 0)
        self.assertEqual(rec.leaked_reports(), [])
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])

    async def test_cancelled_and_done_task_is_named(self):
        task = asyncio.ensure_future(_hang_forever())
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=WATCHDOG)

        def factory(loop, coro, **kwargs):
            return task

        rec = await run_shape(factory)
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec.runs, 0)
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.bus._teardown_task_error, None,
                         "取消不是「某个异常」：error 侧必须整体为 None")
        self.assertEqual(rec.bus._teardown_error_kind, TEARDOWN_NEVER_ENTERED_KIND)
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])

    async def test_cancelled_before_return_midflight_is_named(self):
        """工厂建 Task 后立刻 cancel（返回时可能还没 step）——两条分支都必须具名。"""
        rec = await run_shape(_factory_cancel_before_return)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec.runs, 0)
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.orphan_count, 0)
        self.assertEqual(rec.runtime_warnings, [],
                         "被 cancel 的入口 coroutine 不得打 'was never awaited'")
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])


# --------------------------------------------------------------------------
# 3) A2：跨事件循环的 Task —— 具名拒收 + 整体回滚
# --------------------------------------------------------------------------
class Test109R11ForeignLoopTask(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _FOREIGN_LOOPS.clear()
        self.addCleanup(self._drain_foreign_loops)

    def _drain_foreign_loops(self):
        """在测试协程之外（此时没有别的循环在跑）把被中止的外循环任务落地。"""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            for other in _FOREIGN_LOOPS:
                seen = []
                other.set_exception_handler(lambda _l, ctx: seen.append(ctx))
                other.run_until_complete(asyncio.sleep(0))
                other.close()
                self.assertEqual(seen, [], f"外循环上有异常报告: {seen}")
            gc.collect()
        runtime = [w for w in caught if issubclass(w.category, RuntimeWarning)]
        self.assertEqual(runtime, [], "被拒的外循环任务中和后不得留下 never-awaited")
        _FOREIGN_LOOPS.clear()

    async def test_foreign_loop_task_is_refused_named_and_bounded(self):
        rec = await run_shape(_factory_foreign_loop)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT",
                            "跨循环收养会让 close future 永远悬着；必须拒收")
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, "teardown-spawn-failed")
        self.assertIn(TEARDOWN_FOREIGN_LOOP_REASON, rec.cause_text,
                      "reason 必须点名跨循环（机器可读）")
        self.assertEqual(rec.state, (False, "None", False, 0),
                         "整体回滚：_closing 回受理前、凭据清空、runs 0")
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.orphan_count, 0, "本循环上不得留下孤儿")
        # 健康重试：真的关停一次（跨循环任务已被 cancel，不会二次关停）。
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])


# --------------------------------------------------------------------------
# 4) B：敌对 Task 子类 —— 拒收 + 回滚（不再裸抛）
# --------------------------------------------------------------------------
class Test109R11HostileTaskSubclass(unittest.IsolatedAsyncioTestCase):
    async def test_add_done_callback_override_is_refused_with_rollback(self):
        rec = await run_shape(_hostile_factory)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, "teardown-spawn-failed",
                         "不得是裸 RuntimeError：必须是具名家族")
        self.assertIn(TEARDOWN_HOSTILE_TASK_REASON, rec.cause_text)
        self.assertEqual(rec.state, (False, "None", False, 0),
                         "修前：裸抛且不回滚；修后：整体回滚，总线仍可用")
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.orphan_count, 0)
        self.assertEqual(rec.runtime_warnings, [])
        self.assertEqual(rec.leaked_reports(), [])
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1], "中和成功：重试恰好关一次")

    async def test_instance_level_shadow_is_refused(self):
        rec = await run_shape(_factory_instance_shadow)
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, "teardown-spawn-failed")
        self.assertIn(TEARDOWN_HOSTILE_TASK_REASON, rec.cause_text)
        self.assertEqual(rec.state, (False, "None", False, 0))
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.orphan_count, 0)

    async def test_predicate_rejects_every_tampered_method(self):
        """``_task_uses_stdlib_methods``：类覆写四项任一 + 实例影子 ⇒ 一律 False。"""
        loop = asyncio.get_running_loop()

        class _Plain(asyncio.Task):
            pass

        class _Tampered(asyncio.Task):
            tampered: Set[str] = set()

            def add_done_callback(self, callback, *, context=None):
                if "add_done_callback" in type(self).tampered:
                    raise RuntimeError("hostile")
                return super().add_done_callback(callback, context=context)

            def done(self):
                if "done" in type(self).tampered:
                    raise RuntimeError("hostile")
                return super().done()

            def cancelled(self):
                if "cancelled" in type(self).tampered:
                    raise RuntimeError("hostile")
                return super().cancelled()

            def exception(self):
                if "exception" in type(self).tampered:
                    raise RuntimeError("hostile")
                return super().exception()

        async def _work():
            return None

        created = []
        try:
            plain = asyncio.ensure_future(_work())
            created.append(plain)
            self.assertTrue(_task_uses_stdlib_methods(plain), "默认 Task 必须通过")
            subclass = _Plain(_work(), loop=loop)
            created.append(subclass)
            self.assertTrue(_task_uses_stdlib_methods(subclass),
                            "没覆写方法的 Task 子类必须通过（uvloop 等合法子类）")
            for name in ("add_done_callback", "done", "cancelled", "exception"):
                task = _Tampered(_work(), loop=loop)
                created.append(task)
                _Tampered.tampered = {name}
                try:
                    self.assertFalse(_task_uses_stdlib_methods(task),
                                     f"覆写 {name} 必须判 False")
                finally:
                    _Tampered.tampered = set()
            shadowed = _Plain(_work(), loop=loop)
            created.append(shadowed)
            shadowed.foo = 1                     # C Task 允许任意属性
            shadowed.add_done_callback = lambda cb, **k: None
            self.assertFalse(_task_uses_stdlib_methods(shadowed),
                             "实例级影子属性必须判 False（C Task 不暴露 __dict__，"
                             "靠绑定比较）")
        finally:
            for task in created:
                task.cancel()
            await settle()
            self.assertEqual([t for t in created if not t.done()], [],
                             "测试自建的 Task 必须全部落地")


# --------------------------------------------------------------------------
# 5) r10 五行回归：有界 + 精确结局（与 test_issue109_110_r10_parity.py 同判据）
# --------------------------------------------------------------------------
class Test109R11R10RowsUnchanged(unittest.IsolatedAsyncioTestCase):
    async def test_genuine_throw_rolls_back(self):
        rec = await run_shape(_factory_genuine_throw)
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, "teardown-spawn-failed")
        self.assertEqual(rec.state, (False, "None", False, 0))
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])

    async def test_create_then_raise_is_adopted_no_orphan(self):
        rec = await run_shape(_factory_create_then_raise)
        self.assertEqual(rec.outcome, "returned")
        self.assertEqual(rec.adopt_state, (True, "PENDING", True, 0),
                         "C2：非 eager 收养的那次，_teardown_task 是真的在飞 Task")
        self.assertEqual(rec.state, (True, "done-ok", True, 1))
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])
        self.assertEqual(rec.orphan_count, 0)
        self.assertEqual(rec.runtime_warnings, [])
        self.assertEqual(rec.leaked_reports(), [])

    async def _assert_non_task_all_or_nothing(self, factory, label):
        rec = await run_shape(factory)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT", f"{label}: 不得挂起")
        self.assertEqual(rec.orphan_count, 0, f"{label}: 不得留孤儿")
        self.assertEqual(rec.runtime_warnings, [], f"{label}: 不得 never awaited")
        if rec.outcome == "returned":
            self.assertEqual(rec.state, (True, "done-ok", True, 1))
            self.assertEqual(rec.closes, [1, 1, 1, 1, 1])
        else:
            self.assertEqual(rec.named_kind, "teardown-spawn-failed")
            self.assertEqual(rec.state, (False, "None", False, 0))
            self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1], f"{label}: 不得双关")

    async def test_done_future_is_wrapped(self):
        await self._assert_non_task_all_or_nothing(_factory_done_future, "done-future")

    async def test_awaitable_without_callback_is_wrapped(self):
        await self._assert_non_task_all_or_nothing(
            _factory_awaitable_without_callback, "awaitable")

    async def test_bare_coro_is_wrapped(self):
        await self._assert_non_task_all_or_nothing(_factory_returns_coro, "bare-coro")


if __name__ == "__main__":
    unittest.main()
