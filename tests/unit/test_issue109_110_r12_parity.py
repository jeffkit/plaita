"""#109/#110 r12：把「入口跑过」的证据从**可写标记**换成**事件**（A）+ 判据不再问对象自己（B）。

r11 的验收是 **FAIL**，两个反例都属于「一句工厂代码即可证伪所声称的机制」：

* **A/伪造证据**：r11 把「已进入」标记放在交给工厂的 coroutine 的 frame 里（``holder``
  参数）。工厂拿到的就是这个 coroutine，而 ``coro.cr_frame.f_locals`` 在**未启动**时就
  已含 ``{'holder': {...}, 'self': <RedisEventBus>}``（3.10–3.13 实测；**自由变量也在
  ``f_locals`` 里**）。于是

  ```python
  coro.cr_frame.f_locals["holder"]["entered"] = True      # 伪造「入口跑过」
  return asyncio.Task(some_other_coro(), loop=loop)       # 真 Task，但从不跑我们的
  ```

  收尾判定被绕过 ⇒ **两次 ``close()`` 永久挂起**（``(True, PENDING, True, 0)``、五个组件
  ``[0×5]``、``runs=0``、无具名错误）。把标记挪到 ``self`` 字段或闭包自由变量都不是修法
  （两者都在 frame 里可达）。
* **B/判据被击败**：``_task_uses_stdlib_methods`` 问的是**对象自己**（``__self__`` /
  ``__func__`` / ``__eq__`` / 逐次动态取属性），于是 ①实例影子（``__self__`` 指向本任务、
  无 ``__func__``、``__eq__`` 恒真的假方法）与 ②类级 TOCTOU 描述符（首次实例访问给真
  stdlib 绑定方法、第二次抛）都能通过 ⇒ 具名拒收被绕过，之后 ``request_close`` 对返回值
  的使用**裸抛且不回滚**（``(True, PENDING, False, 0)``）；影子再叠加「交回跑别的
  coroutine 的 Task」⇒ 第二次 ``close()`` 拿到陈旧的 PENDING future ⇒ **挂起**。

r12 的修法（见 ``plaita/event/redis.py`` 的 ``_TeardownRound`` / ``_watch_round`` /
``_task_uses_stdlib_methods`` docstring）：

1. **A**：受理的 Task 结束时，本轮 ``close future`` 是否**仍未**结算——只有我们自己的
   关停路径会结算它。判据是**事件**，不是可写证据位；本轮凭据（``_TeardownRound``）只活在
   ``_spawn_teardown`` 的闭包与任务属性里，**不在** coroutine 的 frame 里。
   入口 ``_teardown_entry()`` 因此**无参、无标记、不结算**。
2. **B**：``inspect.getattr_static``（类表 + 实例表，**不触发描述符/``__getattribute__``**）
   必须**就是** stdlib 本体，再做一次真绑定并要求
   ``type(bound) is type(baseline.__get__(task, cls))`` 且 ``bound.__self__ is task``。

本文件的负控用**有界 2 s 看门狗**跑，绝不靠 timeout 通过；断言具名结局 + 精确状态元组 +
组件关闭次数 + 无孤儿 / 无 ``never awaited`` / 无 ``never retrieved`` / 无双关。
"""

import asyncio
import gc
import sys
import unittest
import warnings
from typing import Any, Dict, List, Set

import pytest

pytest.importorskip("redis")

from plaita.event.redis import (          # noqa: E402
    RedisEventBus,
    TEARDOWN_HOSTILE_TASK_REASON,
    TEARDOWN_NEVER_ENTERED_KIND,
    _TeardownRound,
    _task_uses_stdlib_methods,
)

WATCHDOG = 2.0
SETTLE_TICKS = 3
_SHADOW_NAMES = ("add_done_callback", "done", "cancelled", "exception", "get_loop")


# --------------------------------------------------------------------------
# 总线与观测（与 r11 同口径，便于逐字对照）
# --------------------------------------------------------------------------
class _Counter:
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
    return (bus._closing, _future_state(bus),
            bus._teardown_task is not None, bus._teardown_runs)


def _new_task(coro, loop, kwargs):
    if sys.version_info >= (3, 11):
        return asyncio.Task(coro, loop=loop, context=kwargs.get("context"))
    return asyncio.Task(coro, loop=loop)


def _is_our_entry(coro):
    """只对 teardown 入口出手（同一个循环里还会创建别的任务）。"""
    return getattr(getattr(coro, "cr_code", None), "co_name", "") == "_teardown_entry"


class _Record:
    def __init__(self, outcome, exc, bus, comps, sink, warnings_, reports):
        self.outcome = outcome
        self.exc = exc
        self.bus = bus
        self.comps = comps
        self.sink = sink
        self.runtime_warnings = warnings_
        self.reports = reports

    @property
    def named_kind(self):
        if isinstance(self.exc, RuntimeError):
            text = str(self.exc)
            for kind in (TEARDOWN_NEVER_ENTERED_KIND, "teardown-spawn-failed",
                         "teardown-incomplete", "cross-loop-close"):
                if kind in text:
                    return kind
        return None

    @property
    def cause_text(self):
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
    def adopt_state(self):
        return self.sink.get("adopt_state")

    @property
    def orphan_count(self):
        return len(self.sink.get("orphans", []))

    def leaked_reports(self):
        return [str(r.get("message", "")) for r in self.reports
                if "never retrieved" in str(r.get("message", ""))
                or "was destroyed but it is pending" in str(r.get("message", ""))]


async def _driver(bus, factory, sink):
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
        loop.set_task_factory(saved)
        for _ in range(SETTLE_TICKS):
            await asyncio.sleep(0)
        sink["orphans"] = [t for t in asyncio.all_tasks()
                           if t is not me and t not in before]
        loop.set_task_factory(saved)


async def run_shape(factory, *, bus=None, bound=WATCHDOG):
    loop = asyncio.get_running_loop()
    reports = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _l, ctx: reports.append(ctx))
    if bus is None:
        bus, comps = _make_bus()
    else:
        comps = [bus.pubsub, bus.redis, bus.event_storage,
                 bus.subscription_storage, bus.processing_tracker]
    sink: Dict[str, Any] = {}
    driver = asyncio.create_task(_driver(bus, factory, sink))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            got = await asyncio.wait_for(asyncio.shield(driver), timeout=bound)
        except asyncio.TimeoutError:
            got = ("HANG/TIMEOUT", None)
        outcome, exc = got
        del driver
        gc.collect()
        await asyncio.sleep(0)
        gc.collect()
    loop.set_exception_handler(previous_handler)
    runtime = [w for w in caught if issubclass(w.category, RuntimeWarning)]
    return _Record(outcome, exc, bus, comps, sink, runtime, reports)


async def settle():
    for _ in range(SETTLE_TICKS):
        await asyncio.sleep(0)


# --------------------------------------------------------------------------
# A 的负控：工厂伪造 frame 可达的一切，再交回一个跑别的 coroutine 的真 Task
# --------------------------------------------------------------------------
def _forge_everything(locs: Dict[str, Any]) -> None:
    """枚举 frame 可达的一切并**尽力伪造**「入口跑过」的痕迹（负控的动作）。"""
    for value in locs.values():
        if isinstance(value, dict):
            value["entered"] = True          # r11 的伪造点
            value["task"] = "forged"
            value["future"] = None
            if "coro" in value:
                value["coro"] = None
        elif isinstance(value, list):
            value.append(True)
        for attr in ("_teardown_runs", "_teardown_error_kind", "_teardown_task_error",
                     "_closing", "_teardown_entry_task"):
            if hasattr(value, attr):
                try:
                    setattr(value, attr, True if attr == "_closing" else 0)
                except Exception:             # noqa: BLE001 - 伪造尽力而为
                    pass


_FORGED_FRAMES: List[Dict[str, Any]] = []


def _factory_forge_and_run_other(loop, coro, **kwargs):
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    frame = coro.cr_frame
    _FORGED_FRAMES.append({
        "f_locals": sorted(frame.f_locals.keys()),
        "co_freevars": list(coro.cr_code.co_freevars),
    })
    _forge_everything(frame.f_locals)

    async def other():
        return "not-teardown"
    return _new_task(other(), loop, kwargs)


class Test109R12ForgeChannelIsGone(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _FORGED_FRAMES.clear()

    async def test_forging_every_reachable_object_is_named_not_hung(self):
        """核心负控：伪造 frame 里的一切 ⇒ 仍具名 ``teardown-never-entered``、不挂起。"""
        rec = await run_shape(_factory_forge_and_run_other)
        self.assertEqual(
            len(_FORGED_FRAMES), 1, "工厂必须真的拿到入口 coroutine 并枚举过它的 frame")
        self.assertEqual(
            _FORGED_FRAMES[0]["f_locals"], ["self"],
            "r12 的入口 frame 里只剩 self——没有 holder、没有 future、没有标记位可伪造")
        self.assertEqual(
            _FORGED_FRAMES[0]["co_freevars"], [],
            "入口也不许把本轮凭据放进闭包自由变量（自由变量同样出现在 f_locals）")
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT",
                            "伪造不再能压住具名结局（看门狗 2 s 必须不触发）")
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec.adopt_state, (True, "PENDING", True, 0),
                         "受理瞬间仍是真在飞的收养 Task（r11/C2 口径不变）")
        self.assertEqual(rec.state, (True, "None", False, 0),
                         "结束后凭据清空以便重试；一次都没跑过 ⇒ runs 如实为 0")
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.orphan_count, 0)
        self.assertEqual(rec.runtime_warnings, [],
                         "不得出现 'coroutine ... _teardown_entry was never awaited'")
        self.assertEqual(rec.leaked_reports(), [])
        # close#2：同一个伪造工厂下同样有界 + 具名，绝不挂起、绝不关任何组件。
        rec2 = await run_shape(_factory_forge_and_run_other, bus=rec.bus)
        self.assertNotEqual(rec2.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec2.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec2.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec2.runs, 0)
        # 换回健康工厂：重试真的关停一次、且不双关。
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])

    async def test_entry_takes_nothing_and_settles_nothing(self):
        """直接枚举：入口 coroutine 的 frame / 自由变量里没有任何可用于结算的对象。"""
        bus, comps = _make_bus()
        coro = bus._teardown_entry()
        try:
            frame = coro.cr_frame
            self.assertEqual(sorted(frame.f_locals.keys()), ["self"],
                             "入口只收 self（总线）；没有任何 holder/凭据参数")
            self.assertEqual(coro.cr_code.co_freevars, (),
                             "入口不闭包任何东西 ⇒ co_freevars 为空")
            self.assertEqual(coro.cr_code.co_varnames, ("self",),
                             "入口只有一个形参槽 self（句柄写在 self 上，不进 frame）")
            self.assertEqual(coro.cr_code.co_nlocals, 1,
                             "入口没有任何局部变量槽")
            for banned in ("entered", "future", "holder", "judged"):
                self.assertNotIn(banned, frame.f_locals)
        finally:
            coro.close()

    async def test_forged_bus_counters_do_not_suppress_the_named_outcome(self):
        """R-12a 的边界：``self`` 上的字段确实可被伪造，但**具名结局不因此消失、也不挂起**。"""
        def factory(loop, coro, **kwargs):
            if not _is_our_entry(coro):
                return _new_task(coro, loop, kwargs)
            bus = coro.cr_frame.f_locals["self"]
            bus._teardown_runs = 99            # 伪造「跑过 99 次」
            bus._teardown_error_kind = None

            async def other():
                return "not-teardown"
            return _new_task(other(), loop, kwargs)

        rec = await run_shape(factory)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND,
                         "伪造总线字段压不住具名结局")
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0], "组件一个都没关")
        self.assertEqual(rec.runs, 99, "R-12a：总线字段本身可写（这就是被 bound 的残差）")

    async def test_forged_entry_handle_is_not_adopted(self):
        """R-12b：伪造 ``self._teardown_entry_task`` 为「永不结束的任务」⇒ 不被收养。"""
        forged_holder: List["asyncio.Task"] = []

        async def _never():
            await asyncio.Event().wait()

        def factory(loop, coro, **kwargs):
            if not _is_our_entry(coro):
                return _new_task(coro, loop, kwargs)
            bus = coro.cr_frame.f_locals["self"]
            forged = asyncio.ensure_future(_never())
            forged_holder.append(forged)
            bus._teardown_entry_task = forged        # 伪造句柄
            raise RuntimeError("factory-boom-after-forge")

        try:
            rec = await run_shape(factory)
            self.assertNotEqual(rec.outcome, "HANG/TIMEOUT",
                                "伪造句柄不得让 close 长等到那个永不结束的任务")
            self.assertEqual(rec.outcome, "raised")
            self.assertEqual(rec.named_kind, "teardown-spawn-failed")
            self.assertEqual(rec.state, (False, "None", False, 0),
                             "整个受理回滚；伪造句柄未被采纳为在飞 teardown")
            self.assertIsNone(rec.sink.get("adopted"),
                              "受理瞬间 _teardown_task 必须仍是 None")
            self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
            await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
            self.assertEqual(rec.bus._teardown_runs, 1)
            self.assertEqual(rec.closes, [1, 1, 1, 1, 1])
        finally:
            for task in forged_holder:
                task.cancel()
            await settle()

    async def test_direct_future_settlement_is_false_success_not_a_hang(self):
        """R-12a 的**精确边界**：直接结算 ``self._close_future`` ⇒ 假成功，但绝不是挂起。"""
        def factory(loop, coro, **kwargs):
            if not _is_our_entry(coro):
                return _new_task(coro, loop, kwargs)
            bus = coro.cr_frame.f_locals["self"]
            bus._close_future.set_result(None)     # 绕过一切判据的直接结算

            async def other():
                return "not-teardown"
            return _new_task(other(), loop, kwargs)

        rec = await run_shape(factory)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT",
                            "bound 是「假成功」而不是「挂起」——这是可陈述的残差")
        self.assertEqual(rec.outcome, "returned")
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0],
                         "假成功：调用方看到关停完成，五个组件其实一个都没关")
        self.assertEqual(rec.runs, 0)
        self.assertEqual(rec.orphan_count, 0)

    async def test_round_credential_is_not_reachable_from_the_frame(self):
        """结构性断言：``_TeardownRound`` 不落在 coroutine 的 frame/自由变量里。"""
        bus, comps = _make_bus()
        seen = {}
        loop = asyncio.get_running_loop()
        saved = loop.get_task_factory()

        def factory(lp, coro, **kwargs):
            if not _is_our_entry(coro):
                return _new_task(coro, lp, kwargs)
            locs = coro.cr_frame.f_locals
            seen["round_in_frame"] = any(
                isinstance(v, _TeardownRound) for v in locs.values())
            seen["round_in_freevars"] = any(
                isinstance(coro.cr_frame.f_locals.get(n), _TeardownRound)
                for n in coro.cr_code.co_freevars)
            return _new_task(coro, lp, kwargs)

        loop.set_task_factory(factory)
        try:
            await asyncio.wait_for(bus.close(), timeout=WATCHDOG)
        finally:
            loop.set_task_factory(saved)
        self.assertEqual(bus._teardown_runs, 1)
        self.assertFalse(seen.get("round_in_frame", True),
                         "本轮凭据不许出现在 cr_frame.f_locals")
        self.assertFalse(seen.get("round_in_freevars", True),
                         "本轮凭据不许出现在 co_freevars")


# --------------------------------------------------------------------------
# B 的负控：判据不再问对象自己
# --------------------------------------------------------------------------
class _FakeBound:
    """伪造的「绑定方法」：有 ``__self__``、没有 ``__func__``、``__eq__`` 恒真。"""

    def __init__(self, task, name):
        self.__self__ = task
        self.name = name

    def __eq__(self, other):
        return True

    def __ne__(self, other):
        return False

    def __hash__(self):
        return 0

    def __call__(self, *a, **k):
        if self.name == "add_done_callback":
            raise RuntimeError("shadow-add_done_callback-boom")
        # 其余方法委托真实现：伪造品只在**结构**上像，不制造额外噪声。
        return getattr(asyncio.Task, self.name)(self.__self__, *a, **k)


def _shadow(task, names=_SHADOW_NAMES):
    for name in names:
        setattr(task, name, _FakeBound(task, name))
    return task


def _factory_instance_shadow(loop, coro, **kwargs):
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    return _shadow(_new_task(coro, loop, kwargs))


def _factory_shadow_other_coro(loop, coro, **kwargs):
    """组合反例：影子 + 交回**跑别的 coroutine** 的真 Task（r11 上第二次 close 挂起）。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)

    async def other():
        return "not-teardown"
    return _shadow(_new_task(other(), loop, kwargs))


class _TOCTOU:
    """类级描述符：类访问给 baseline，实例第 2 次访问抛（第 1 次给真绑定方法）。"""

    def __init__(self, name, baseline):
        self.name = name
        self.baseline = baseline
        self.calls = 0

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self.baseline
        self.calls += 1
        if self.calls == 1:
            return self.baseline.__get__(obj, objtype)
        raise RuntimeError("descriptor-toctou-boom")


#: 只挂在 add_done_callback 上：done/get_loop 若也 TOCTOU，``asyncio.all_tasks()``
#: （判定**之后**由 harness 调用）会被弄崩，掩盖真正要看的东西。r11 的失败路径正是
#: 「判据实例访问第 1 次（真）→ request_close 第 2 次（抛）」，一个方法足矣。
_TOCTOU_NAMES = ("add_done_callback",)
_TOCTOU_STATE: Dict[str, _TOCTOU] = {}


def _make_toctou_class():
    attrs = {}
    for name in _TOCTOU_NAMES:
        d = _TOCTOU(name, getattr(asyncio.Task, name))
        _TOCTOU_STATE[name] = d
        attrs[name] = d
    return type("_ToctouTask", (asyncio.Task,), attrs)


_ToctouTask = _make_toctou_class()


def _factory_toctou(loop, coro, **kwargs):
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    for d in _TOCTOU_STATE.values():
        d.calls = 0
    return _new_task_toctou(coro, loop, kwargs)


def _new_task_toctou(coro, loop, kwargs):
    if sys.version_info >= (3, 11):
        return _ToctouTask(coro, loop=loop, context=kwargs.get("context"))
    return _ToctouTask(coro, loop=loop)


class Test109R12PredicateDoesNotTrustTheObject(unittest.IsolatedAsyncioTestCase):
    async def test_instance_shadow_with_eq_is_refused_named_and_rolled_back(self):
        """反例①：实例影子 + ``__eq__`` 恒真 ⇒ 判据 False、具名拒收 + 整体回滚。"""
        loop = asyncio.get_running_loop()
        shadow = _shadow(asyncio.ensure_future(self._noop()))
        try:
            self.assertFalse(_task_uses_stdlib_methods(shadow),
                             "伪造品没有 __func__ 且 __eq__ 恒真：不得再被放行")
        finally:
            shadow.cancel()
            await settle()
        rec = await run_shape(_factory_instance_shadow)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, "teardown-spawn-failed")
        self.assertIn(TEARDOWN_HOSTILE_TASK_REASON, rec.cause_text)
        self.assertEqual(rec.state, (False, "None", False, 0), "整体回滚")
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.orphan_count, 0)
        self.assertEqual(rec.runtime_warnings, [])
        self.assertEqual(rec.leaked_reports(), [])
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1], "中和成功：重试恰好关一次")

    async def test_toctou_descriptor_is_refused_named_and_rolled_back(self):
        """反例②：类级 TOCTOU 描述符 ⇒ 静态表在**任何动态访问之前**就把它逐出。"""
        loop = asyncio.get_running_loop()
        for d in _TOCTOU_STATE.values():
            d.calls = 0
        task = _new_task_toctou(self._noop(), loop, {})
        try:
            self.assertFalse(_task_uses_stdlib_methods(task),
                             "类表上不是 stdlib 本体（是描述符）⇒ 必须判 False")
            self.assertEqual(
                {n: d.calls for n, d in _TOCTOU_STATE.items()},
                {n: 0 for n in _TOCTOU_NAMES},
                "静态表拒绝必须发生在**任何**实例访问之前（否则 TOCTOU 仍可能得手）")
        finally:
            task.cancel()
            await settle()
        rec = await run_shape(_factory_toctou)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, "teardown-spawn-failed")
        self.assertIn(TEARDOWN_HOSTILE_TASK_REASON, rec.cause_text)
        self.assertEqual(rec.state, (False, "None", False, 0))
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.orphan_count, 0)
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])

    async def test_shadow_plus_other_coro_no_longer_hangs_the_second_close(self):
        """组合反例：影子 + 跑别的 coroutine ⇒ r11 上 close#2 ==== 永久挂起。"""
        rec = await run_shape(_factory_shadow_other_coro)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT", "第一次 close 必须具名有界")
        self.assertEqual(rec.named_kind, "teardown-spawn-failed")
        self.assertIn(TEARDOWN_HOSTILE_TASK_REASON, rec.cause_text)
        self.assertEqual(rec.state, (False, "None", False, 0), "整体回滚 ⇒ 可重试")
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        rec2 = await run_shape(_factory_shadow_other_coro, bus=rec.bus)
        self.assertNotEqual(rec2.outcome, "HANG/TIMEOUT",
                            "r11：close#2 拿到陈旧的 PENDING future ⇒ 永久挂起")
        self.assertEqual(rec2.named_kind, "teardown-spawn-failed")
        self.assertEqual(rec2.closes, [0, 0, 0, 0, 0])
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])

    async def _noop(self):
        return None

    async def test_legitimate_cases_are_not_refused(self):
        """合法形状必须**照常通过**：默认 Task / 纯子类 / 带属性 / ``__slots__`` /
        ``__getattr__`` / uvloop 的 task 类型。"""
        loop = asyncio.get_running_loop()

        class _Plain(asyncio.Task):
            pass

        class _WithAttrs(asyncio.Task):
            pass

        class _Slotted(asyncio.Task):
            __slots__ = ()

        class _Getattr(asyncio.Task):
            def __getattr__(self, name):
                raise AttributeError(name)

        created = []
        try:
            default = asyncio.ensure_future(self._noop())
            created.append(default)
            self.assertTrue(_task_uses_stdlib_methods(default), "默认 Task 必须通过")

            plain = _Plain(self._noop(), loop=loop)
            created.append(plain)
            self.assertTrue(_task_uses_stdlib_methods(plain), "纯子类必须通过")

            attrs = _WithAttrs(self._noop(), loop=loop)
            created.append(attrs)
            attrs.extra = 1
            attrs.another = "x"
            self.assertTrue(_task_uses_stdlib_methods(attrs),
                            "带额外实例属性的子类必须通过")

            slotted = _Slotted(self._noop(), loop=loop)
            created.append(slotted)
            self.assertTrue(_task_uses_stdlib_methods(slotted),
                            "__slots__=() 的子类必须通过（连 __dict__ 都没有）")

            ga = _Getattr(self._noop(), loop=loop)
            created.append(ga)
            self.assertTrue(_task_uses_stdlib_methods(ga),
                            "覆写 __getattr__（只在缺失时触发）必须通过")
        finally:
            for task in created:
                task.cancel()
            await settle()
            self.assertEqual([t for t in created if not t.done()], [])

    def test_uvloop_task_type_is_accepted_if_available(self):
        """uvloop 的 task 类型（若装了 uvloop）必须照常通过。"""
        uvloop = pytest.importorskip("uvloop")
        adv = uvloop.new_event_loop()

        async def main():
            task = asyncio.ensure_future(asyncio.sleep(0))
            try:
                self.assertTrue(_task_uses_stdlib_methods(task),
                                f"uvloop 的 task 类型 {type(task)!r} 不得被拒")
                self.assertIs(type(task), asyncio.Task)   # 记录实际类型
            finally:
                await task

        try:
            adv.run_until_complete(main())
        finally:
            adv.close()

    def test_fake_bound_equality_no_longer_matters(self):
        """判据不得再把「对象自己说相等」当作证据（结构性断言）。"""
        import inspect as _inspect
        loop = asyncio.new_event_loop()
        try:
            task = asyncio.Task(self._noop_coro(), loop=loop)
            shadow = _shadow(task, names=("add_done_callback",))
            self.assertEqual(shadow.add_done_callback,  # __eq__ 恒真
                             asyncio.Task.add_done_callback.__get__(shadow, type(shadow)))
            self.assertFalse(_task_uses_stdlib_methods(shadow))
            self.assertIsNotNone(
                _inspect.getattr_static(shadow, "add_done_callback"),
                "静态表能看见实例影子（不触发 __getattribute__）")
            task.cancel()
            loop.run_until_complete(asyncio.sleep(0))
        finally:
            loop.close()

    async def _noop_coro(self):
        return None


if __name__ == "__main__":
    unittest.main()
