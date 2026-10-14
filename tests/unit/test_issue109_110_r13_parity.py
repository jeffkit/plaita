"""#109/#110 r13：**删掉每一个可写证据位**（A）+ **不再问对象、直接调 C 函数**（B）。

r12 的独立验收是 **FAIL**，两个反例都属于「一句工厂代码即可证伪所声称的机制」：

* **A/可写证据位仍可达**：r12 把受理凭据 ``_TeardownRound``（含 ``judged`` 与 ``future``）
  ``setattr`` 到**工厂交回的那个 Task** 上，于是三条通道都成立（3.11/3.13 实测，2 s 有界
  看门狗，全部以 ``(True, PENDING, True, 0)``、``[0×5]``、``runs=0``、无具名错误 收场，
  与 r11 症状逐位相同）：

    * **S1** 栈帧窃取：``sys._getframe`` 往上走到 ``_spawn_teardown`` 的活帧 ⇒
      ``f_locals['round_']`` ⇒ ``round_.judged = True``（看门狗 no-op）；
    * **S2** 任务属性窃取：工厂留着返回的 Task，``loop.call_soon`` 里
      ``getattr(t, _TEARDOWN_ROUND_ATTR)`` ⇒ 同一个凭据；
    * **S3** 敌对 ``__setattr__``：引擎自己把凭据递给它；
    * **S4** 把 ``round_.future`` 换成诱饵 ⇒ 看门狗结算诱饵、真 close future 永远悬着。
* **B/判据仍可被击败**：``_asyncio.Task`` 用 managed dict，``setattr(t, name, ...)`` 成功而
  ``object.__getattribute__(t, "__dict__")`` / ``inspect.getattr_static(t, name)`` 都看不见
  ⇒ 「实例静态表」结构性瞎掉。把**另一个真 stdlib 绑定方法**
  （``asyncio.Task.remove_done_callback.__get__(t, type(t))``）塞进实例属性：类型相同、
  ``__self__`` 是本任务 ⇒ 判据放行 ⇒ 引擎的 ``task.add_done_callback(cb)`` 注册了个寂寞
  ⇒ **两次 close 都永久挂起**。

r13 的修法（见 ``plaita/event/redis.py`` 的 ``_TeardownRound`` / ``_bind_round`` /
``_watch_round`` / ``_purge_instance_method_shadows`` / ``_overrides_instance_getattribute``）：

1. **A**：删除 ``judged``（幂等只看 ``future.done()``）；凭据**不再是 Task 属性**，只活在
   ``_watch`` 的闭包 + 按 ``id(task)`` 的 ``_TEARDOWN_ROUNDS``（只服务留档）；看门狗闭包
   **直接捕获真 future**；受理时捕获的真 future 在判定点钉回 ``self._close_future``。
   假成功（工厂直接结算 future）由**具名留档** ``teardown-settlement-tampered`` + 响亮
   WARNING 可见（r12 是静默的）。
   不变量：*没有任何一个工厂能写的字段会影响判决；判决只由闭包里的 ``future.done()``
   决定，且工厂从它交回的对象上读不到任何一个本轮判据字段。*
   **S1 形状（frame 可达 / 闭包 cell / 模块全局）明示 out of contract、不声称已关闭。**
2. **B**：引擎对受理 Task 的**每一次**操作都改成类上的 C 函数
   （``asyncio.Task.add_done_callback(task, cb)`` / ``get_loop`` / ``done`` / ``cancelled`` /
   ``exception`` / ``cancel`` / ``get_coro``），实例与类属性查找全被绕过；另加两道闸——
   类表拒收覆写 ``__getattribute__``、``object.__delattr__`` 探测并清除实例方法影子。

本文件的负控用**有界 2 s 看门狗**，绝不靠 timeout 通过；断言具名结局 + 精确状态元组 +
组件关闭次数 + 无孤儿 / 无 ``never awaited`` / 无 ``never retrieved`` / 无双关。
"""

import asyncio
import gc
import inspect
import sys
import unittest
import warnings
from typing import Any, Dict, List

import pytest

pytest.importorskip("redis")

from plaita.event import redis as R  # noqa: E402
from plaita.event.redis import (          # noqa: E402
    RedisEventBus,
    TEARDOWN_HOSTILE_TASK_REASON,
    TEARDOWN_NEVER_ENTERED_KIND,
    TEARDOWN_SETTLEMENT_TAMPERED_KIND,
    _TEARDOWN_ROUNDS,
    _TEARDOWN_USED_TASK_METHODS,
    _TeardownRound,
    _task_uses_stdlib_methods,
)

WATCHDOG = 2.0
SETTLE_TICKS = 3
_ROUND_ATTR = "_plaita_teardown_round"     # r12 的任务属性名（r13 必须不再写它）
_SHADOW_NAMES = tuple(_TEARDOWN_USED_TASK_METHODS)


# --------------------------------------------------------------------------
# 总线与观测（与 r11/r12 同口径，便于逐字对照）
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
            for kind in (TEARDOWN_SETTLEMENT_TAMPERED_KIND,
                         TEARDOWN_NEVER_ENTERED_KIND, "teardown-spawn-failed",
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
    def kind_field(self):
        return self.bus._teardown_error_kind

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
# A：删掉每一个可写证据位
# --------------------------------------------------------------------------
#: 工厂在活帧里找到的 ``round_``（S1/S4 的通道）以及它看到的字段
_STOLEN: List[Dict[str, Any]] = []


def _walk_round():
    """工厂侧：沿栈帧找 ``round_`` 局部（r12 的 S1/S4 通道）。"""
    frame = sys._getframe(1)
    while frame is not None:
        found = frame.f_locals.get("round_")
        if found is not None:
            return frame, found
        frame = frame.f_back
    return None, None


def _factory_s1_forge_everything(loop, coro, **kwargs):
    """S1：栈帧窃取 + 把 ``round_`` 上**每一个**字段都伪造一遍。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    frame, round_ = _walk_round()
    entry: Dict[str, Any] = {"frame": None, "fields": [], "mutated": []}
    if round_ is not None:
        entry["frame"] = frame.f_code.co_name
        entry["fields"] = sorted(n for n in ("judged", "never_entered", "tampered",
                                             "future", "task", "coro", "task_ref")
                                 if hasattr(round_, n))
        for name in entry["fields"]:
            if name == "coro":
                continue
            try:
                setattr(round_, name, True)
                entry["mutated"].append(name)
            except BaseException:                    # noqa: BLE001 - 伪造尽力而为
                pass
        if hasattr(round_, "future"):
            round_.future = loop.create_future()
    _STOLEN.append(entry)

    async def other():
        return "not-teardown"
    return _new_task(other(), loop, kwargs)


def _factory_s2_task_attr(loop, coro, **kwargs):
    """S2：工厂留着返回的 Task，``call_soon`` 里读回本轮凭据并改写。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    gate = loop.create_future()

    async def other():
        await gate
        return "not-teardown"

    task = _new_task(other(), loop, kwargs)
    seen: Dict[str, Any] = {}

    def steal():
        found = getattr(task, _ROUND_ATTR, None)
        seen["round"] = found
        if found is not None:
            for name in ("judged", "never_entered", "tampered"):
                if hasattr(found, name):
                    setattr(found, name, True)
            if hasattr(found, "future"):
                found.future = loop.create_future()
        if not gate.done():
            gate.set_result(None)

    loop.call_soon(steal)
    _STOLEN.append(seen)
    return task


def _spy_setattr_class():
    class SpySetattr(asyncio.Task):
        names: List[str] = []

        def __setattr__(self, name, value):
            object.__setattr__(self, name, value)
            type(self).names.append(name)
    return SpySetattr


def _factory_s3_setattr_spy(loop, coro, **kwargs):
    """S3：敌对 ``__setattr__`` —— 引擎若还往 Task 上写凭据，它就会看到。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    cls = _spy_setattr_class()
    gate = loop.create_future()

    async def other():
        await gate
        return "not-teardown"

    task = cls(other(), loop=loop)
    loop.call_soon(lambda: gate.done() or gate.set_result(None))
    task._spy_cls = cls
    return task


def _factory_s4_round_future_decoy(loop, coro, **kwargs):
    """S4：把 ``round_.future`` 换成诱饵（r12 的挂起通道）。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    _, round_ = _walk_round()
    info: Dict[str, Any] = {"slots": getattr(type(round_), "__slots__", None)
                            if round_ is not None else None}
    if round_ is not None and hasattr(round_, "future"):
        round_.future = loop.create_future()
        info["swapped"] = True
    else:
        info["swapped"] = False
    _STOLEN.append(info)

    async def other():
        return "not-teardown"
    return _new_task(other(), loop, kwargs)


def _factory_s4b_bus_field_decoy(loop, coro, **kwargs):
    """S4b：改写**总线字段** ``self._close_future``（受理之后再改一次）。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    bus = coro.cr_frame.f_locals["self"]
    decoy = loop.create_future()
    bus._close_future = decoy
    gate = loop.create_future()

    async def other():
        await gate
        return "not-teardown"

    task = _new_task(other(), loop, kwargs)

    def reseat():
        bus._close_future = decoy          # 判定之前再换一次（越过受理点的一次性钉回）
        if not gate.done():
            gate.set_result(None)

    loop.call_soon(reseat)
    task._decoy = decoy
    return task


def _factory_s5_direct_settle(loop, coro, **kwargs):
    """S5：工厂直接结算 ``self._close_future``（r12 的静默假成功）。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    bus = coro.cr_frame.f_locals["self"]
    bus._close_future.set_result(None)

    async def other():
        return "not-teardown"
    return _new_task(other(), loop, kwargs)


def _raise_setattr_class():
    class RaiseSetattr(asyncio.Task):
        def __setattr__(self, name, value):
            if name == _ROUND_ATTR:
                raise RuntimeError("hostile-setattr")
            object.__setattr__(self, name, value)
    return RaiseSetattr


def _factory_s6_setattr_raise(loop, coro, **kwargs):
    """S6：敌对 ``__setattr__`` 对凭据属性直接抛。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    cls = _raise_setattr_class()

    async def other():
        return "not-teardown"
    return cls(other(), loop=loop)


def _factory_s9_future_method_shadow(loop, coro, **kwargs):
    """S9（r13/B 补钉）：把 close future 的**结算**方法换成会抛的影子。

    只影子 ``set_result`` / ``set_exception``——``done``/``cancelled``/``exception`` 留给
    观测工具（``_future_state``）与 ``asyncio.shield``，否则崩的是探针而不是被测机制。
    """
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    bus = coro.cr_frame.f_locals["self"]
    future = bus._close_future

    def boom(*a, **k):
        raise RuntimeError("shadowed-future-method")

    for name in ("set_result", "set_exception"):
        setattr(future, name, boom)

    async def other():
        return "not-teardown"
    return _new_task(other(), loop, kwargs)


class Test109R13NoWritableEvidenceBit(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _STOLEN.clear()

    async def test_round_is_not_a_task_attribute_and_has_no_settlement_fields(self):
        """结构性：凭据不再是 Task 属性，且没有 ``future`` / ``judged`` / ``task`` 字段。"""
        bus, comps = _make_bus()
        seen: Dict[str, Any] = {}
        loop = asyncio.get_running_loop()
        saved = loop.get_task_factory()
        # 登记表是模块全局；同进程里别的套件可能留下属于**它们**的条目（那些总线/任务不在
        # 本用例里结束）。这里只断言本用例**没有新增**遗留。
        before = set(_TEARDOWN_ROUNDS)

        def factory(lp, coro, **kwargs):
            if not _is_our_entry(coro):
                return _new_task(coro, lp, kwargs)
            task = _new_task(coro, lp, kwargs)
            seen["task"] = task
            return task

        loop.set_task_factory(factory)
        try:
            await asyncio.wait_for(bus.close(), timeout=WATCHDOG)
        finally:
            loop.set_task_factory(saved)
        task = seen["task"]
        self.assertEqual(bus._teardown_runs, 1)
        self.assertEqual(comps[0].closes, 1)
        self.assertFalse(hasattr(task, _ROUND_ATTR),
                         "r13 绝不再把本轮凭据写到**工厂交回的对象**上（S2/S3 的通道）")
        self.assertIsNone(getattr(task, _ROUND_ATTR, None))
        for banned in ("future", "judged", "task"):
            self.assertNotIn(banned, _TeardownRound.__slots__,
                             f"凭据不许再有 {banned} 字段（S1/S4 的伪造对象）")
        self.assertNotIn("__dict__", _TeardownRound.__slots__)
        self.assertIn("task_ref", _TeardownRound.__slots__,
                      "对 Task 只许弱引用（登记表不替 Task 续命）")
        self.assertEqual(set(_TEARDOWN_ROUNDS) - before, set(),
                         "完成后登记表里属于本轮的条目必须被同一个完成事件清空（不泄漏）")
        await asyncio.wait_for(bus.close(), timeout=WATCHDOG)   # 幂等

    async def test_s1_frame_walking_is_out_of_contract_but_cannot_change_verdict(self):
        """S1（**明示 out of contract**）：栈帧仍能走到 ``round_``，但改不动判决。

        进程内对手能读活帧/闭包/模块全局——语言内无法防御（见 ``redis.py`` 的 R-13a），
        所以 r13 不声称「不可达」。本用例钉住 r13 真正保证的东西：**无论它往 ``round_``
        写什么**，判决仍由闭包里的 ``future.done()`` 给出，结局具名、有界、组件一个不关。
        """
        rec = await run_shape(_factory_s1_forge_everything)
        self.assertEqual(len(_STOLEN), 1, "工厂必须真的走到栈帧并找到 round_")
        stolen = _STOLEN[0]
        self.assertEqual(stolen["frame"], "_spawn_teardown",
                         "S1 的通道（活帧里的 round_）确实存在——这是声明的不在契约内")
        self.assertNotIn("future", stolen["fields"])
        self.assertNotIn("judged", stolen["fields"])
        self.assertIn("never_entered", stolen["mutated"])
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT",
                            "伪造 frame 可达的每一个字段都压不住判决（2 s 看门狗必须不触发）")
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
        rec2 = await run_shape(_factory_s1_forge_everything, bus=rec.bus)
        self.assertNotEqual(rec2.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec2.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec2.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec2.runs, 0)
        # 换回健康工厂：重试真的关停一次、且不双关。
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])

    async def test_s2_task_attribute_channel_is_gone(self):
        """S2：工厂留着返回的 Task 也读不回本轮凭据 ⇒ 具名、有界、组件不关。"""
        rec = await run_shape(_factory_s2_task_attr)
        self.assertEqual(len(_STOLEN), 1)
        self.assertIsNone(_STOLEN[0]["round"],
                          "r13 的受理 Task 上没有本轮凭据属性（r12 上这里能读到 round_）")
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec.state, (True, "None", False, 0))
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.runs, 0)
        self.assertEqual(rec.runtime_warnings, [])
        self.assertEqual(rec.leaked_reports(), [])

    async def test_s3_hostile_setattr_never_receives_the_round(self):
        """S3：敌对 ``__setattr__`` 事件里**没有**引擎写的凭据属性。"""
        rec = await run_shape(_factory_s3_setattr_spy)
        adopted = rec.sink.get("adopted")
        self.assertIsNotNone(adopted, "该形状下受理本身是成功的（不是拒收路径）")
        names = getattr(type(adopted), "names", None)
        self.assertIsNotNone(names)
        self.assertNotIn(_ROUND_ATTR, names,
                         "引擎不许再把凭据 setattr 到工厂交回的对象上（r12 的 S3）")
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.runs, 0)
        self.assertEqual(rec.runtime_warnings, [])

    async def test_s4_round_future_decoy_channel_is_gone(self):
        """S4：``round_`` 上**没有** ``future`` 字段 ⇒ 换诱饵的通道不存在。"""
        rec = await run_shape(_factory_s4_round_future_decoy)
        self.assertEqual(len(_STOLEN), 1)
        self.assertFalse(_STOLEN[0]["swapped"], "r13 的凭据里没有 future 字段")
        self.assertNotIn("future", _STOLEN[0]["slots"])
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT",
                            "r12 上这条是 HANG（看门狗结算诱饵、真 future 悬着）")
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec.state, (True, "None", False, 0))
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.runs, 0)

    async def test_s4b_watchdog_settles_the_closure_future_not_a_bus_field(self):
        """S4b：工厂把 ``self._close_future`` 换成诱饵也改不了结算对象。

        看门狗闭包捕获的是受理时的真 future，因此真 future 仍拿到具名结局、两次 close
        都**有界**；受理点的一次性钉回让第一轮结束后幂等凭据被清空，第二轮能重新受理。
        """
        rec = await run_shape(_factory_s4b_bus_field_decoy)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.runs, 0)
        rec2 = await run_shape(_factory_s4b_bus_field_decoy, bus=rec.bus)
        self.assertNotEqual(rec2.outcome, "HANG/TIMEOUT",
                            "诱饵不许把幂等凭据钉死成永不结算（r13 的钉回）")
        self.assertEqual(rec2.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec2.closes, [0, 0, 0, 0, 0])

    async def test_s5_direct_settlement_is_named_tampered_not_silent(self):
        """S5：假成功仍然可能（R-12a），但**必须有具名留档 + 响亮 WARNING**。"""
        with self.assertLogs("plaita.event.redis", level="WARNING") as captured:
            rec = await run_shape(_factory_s5_direct_settle)
        self.assertEqual(rec.outcome, "returned",
                         "直接结算仍是假成功（结算只能发生一次、无法撤回）")
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0],
                         "假成功：调用方看到关停完成，五个组件其实一个都没关")
        self.assertEqual(rec.runs, 0)
        self.assertEqual(rec.kind_field, TEARDOWN_SETTLEMENT_TAMPERED_KIND,
                         "r13 的 tamper 信号：future 已结算而本轮 teardown 从未跑")
        self.assertTrue(any(TEARDOWN_SETTLEMENT_TAMPERED_KIND in line
                            for line in captured.output),
                        "tamper 必须留下响亮日志（r12 这条是静默的）")
        self.assertEqual(rec.orphan_count, 0)
        self.assertEqual(rec.runtime_warnings, [],
                         "本轮入口 coroutine 从未启动 ⇒ 必须被 close 掉，不留 never-awaited")
        self.assertEqual(rec.leaked_reports(), [])

    async def test_s6_raising_setattr_neither_rolls_back_nor_leaks(self):
        """S6：敌对 ``__setattr__`` 对凭据属性抛 —— 引擎不再写它，因此形状照常受理。"""
        rec = await run_shape(_factory_s6_setattr_raise)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND,
                         "r12 上这条是「spawn 失败 + 回滚 + never-awaited 泄漏」；"
                         "r13 不再触发它，按正常「入口没跑」具名")
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.runs, 0)
        self.assertEqual(rec.runtime_warnings, [],
                         "r12 这里漏一个 'coroutine ... was never awaited'")
        self.assertEqual(rec.leaked_reports(), [])
        # 回滚/重试语义仍成立：换回健康路径真的关停一次。
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])
        self.assertEqual(rec.state, (True, "done-ok", True, 1))

    async def test_s7_s8_benign_shapes_unchanged(self):
        """S7（``__slots__=()``）/ S8（两轮交回同一个 Task）口径与 r12 一致。"""
        class Slotted(asyncio.Task):
            __slots__ = ()

        def factory_other(loop, coro, **kwargs):
            if not _is_our_entry(coro):
                return _new_task(coro, loop, kwargs)

            async def other():
                return "not-teardown"
            return Slotted(other(), loop=loop)

        def factory_own(loop, coro, **kwargs):
            if not _is_our_entry(coro):
                return _new_task(coro, loop, kwargs)

            async def wrap():
                await coro
            return Slotted(wrap(), loop=loop)

        rec = await run_shape(factory_other)
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        rec2 = await run_shape(factory_own)
        self.assertEqual(rec2.outcome, "returned")
        self.assertEqual(rec2.closes, [1, 1, 1, 1, 1])
        self.assertEqual(rec2.runs, 1)

        holder: Dict[str, Any] = {}

        def factory_same(loop, coro, **kwargs):
            if not _is_our_entry(coro):
                return _new_task(coro, loop, kwargs)
            if "t" not in holder:
                async def other():
                    return "not-teardown"
                holder["t"] = _new_task(other(), loop, kwargs)
            return holder["t"]

        rec3 = await run_shape(factory_same)
        self.assertEqual(rec3.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        rec4 = await run_shape(factory_same, bus=rec3.bus)
        self.assertEqual(rec4.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec4.closes, [0, 0, 0, 0, 0])

    async def test_s9_future_method_shadows_cannot_block_the_named_settlement(self):
        """S9（r13/B）：close future 对工厂可达 ⇒ 结算也必须走 ``asyncio.Future`` 的 C 函数。

        工厂把 ``self._close_future.set_exception`` / ``set_result`` 换成会抛的影子；若结算
        走实例属性查找，看门狗里那句 ``future.set_exception(...)`` 就会抛出去 ⇒ 真 future
        永不结算 ⇒ 两次 close 长等。C 函数让它照常具名、有界。
        """
        rec = await run_shape(_factory_s9_future_method_shadow)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT",
                            "结算不许经实例属性查找（影子方法会吞掉具名失败）")
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(rec.state, (True, "None", False, 0))
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.runs, 0)
        self.assertEqual(rec.orphan_count, 0)
        self.assertEqual(rec.leaked_reports(), [])
        # close#2：同一个影子工厂下同样有界、同样具名。
        rec2 = await run_shape(_factory_s9_future_method_shadow, bus=rec.bus)
        self.assertNotEqual(rec2.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec2.named_kind, TEARDOWN_NEVER_ENTERED_KIND)

    async def test_registry_is_cleaned_on_named_failure_and_on_success(self):
        """登记表与 ``id`` 复用：受理结束（成功或具名失败）后条目必须消失。"""
        before = set(_TEARDOWN_ROUNDS)
        rec = await run_shape(_factory_s2_task_attr)
        self.assertEqual(rec.named_kind, TEARDOWN_NEVER_ENTERED_KIND)
        await settle()
        self.assertEqual(set(_TEARDOWN_ROUNDS) - before, set(),
                         "具名失败路径同样要清表")
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(set(_TEARDOWN_ROUNDS) - before, set())


@unittest.skipUnless(hasattr(asyncio, "eager_task_factory"),
                     "需要 3.12+ 的 asyncio.eager_task_factory")
class Test109R13EagerOrdering(unittest.IsolatedAsyncioTestCase):
    """eager 同步路径：判决（以及 tamper 信号）必须早于 ``request_close`` 返回。"""

    async def test_healthy_eager_completion_settles_before_return(self):
        bus, comps = _make_bus()
        loop = asyncio.get_running_loop()
        saved = loop.get_task_factory()
        loop.set_task_factory(asyncio.eager_task_factory)
        try:
            future = bus.request_close()
            self.assertTrue(future.done(), "eager 下 teardown 在 spawn 内同步跑完")
            self.assertIsNone(future.exception())
            self.assertEqual(bus._teardown_runs, 1)
            self.assertEqual(_state(bus), (True, "done-ok", False, 1))
            self.assertEqual([c.closes for c in comps], [1, 1, 1, 1, 1])
        finally:
            loop.set_task_factory(saved)

    def _hostile_factory(self):
        def factory(loop, coro, **kwargs):
            if not _is_our_entry(coro):
                return asyncio.eager_task_factory(loop, coro, **kwargs)

            async def other():
                return "not-teardown"
            return asyncio.eager_task_factory(loop, other(), **kwargs)
        return factory

    async def test_hostile_eager_completion_is_named_before_return(self):
        bus, comps = _make_bus()
        loop = asyncio.get_running_loop()
        saved = loop.get_task_factory()
        loop.set_task_factory(self._hostile_factory())
        try:
            future = bus.request_close()
            self.assertTrue(future.done(),
                            "r11/A3：返回时已 done 的 Task 走同一条同步判据")
            self.assertIn(TEARDOWN_NEVER_ENTERED_KIND, str(future.exception()))
            self.assertEqual(bus._teardown_error_kind, TEARDOWN_NEVER_ENTERED_KIND)
            self.assertEqual(_state(bus), (True, "None", False, 0))
            self.assertEqual([c.closes for c in comps], [0, 0, 0, 0, 0])
        finally:
            loop.set_task_factory(saved)

    async def test_hostile_eager_direct_settle_is_tampered_before_return(self):
        def factory(loop, coro, **kwargs):
            if not _is_our_entry(coro):
                return asyncio.eager_task_factory(loop, coro, **kwargs)
            bus = coro.cr_frame.f_locals["self"]
            bus._close_future.set_result(None)

            async def other():
                return "not-teardown"
            return asyncio.eager_task_factory(loop, other(), **kwargs)

        bus, comps = _make_bus()
        loop = asyncio.get_running_loop()
        saved = loop.get_task_factory()
        loop.set_task_factory(factory)
        try:
            future = bus.request_close()
            self.assertTrue(future.done())
            self.assertEqual(bus._teardown_runs, 0)
            self.assertEqual(bus._teardown_error_kind,
                             TEARDOWN_SETTLEMENT_TAMPERED_KIND,
                             "tamper 信号同样必须早于 request_close 返回")
        finally:
            loop.set_task_factory(saved)


# --------------------------------------------------------------------------
# B：不再问对象；对 Task 的每一次操作都走类上的 C 函数
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
        return getattr(asyncio.Task, self.name)(self.__self__, *a, **k)


def _shadow(task, names=_SHADOW_NAMES):
    for name in names:
        setattr(task, name, _FakeBound(task, name))
    return task


def _factory_alias(loop, coro, **kwargs):
    """B 的决定性反例：实例属性塞一个**真** stdlib 绑定方法（``remove_done_callback``）。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    task = _new_task(coro, loop, kwargs)
    setattr(task, "add_done_callback",
            asyncio.Task.remove_done_callback.__get__(task, type(task)))
    return task


def _factory_alias_other_coro(loop, coro, **kwargs):
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)

    async def other():
        return "not-teardown"
    task = _new_task(other(), loop, kwargs)
    setattr(task, "add_done_callback",
            asyncio.Task.remove_done_callback.__get__(task, type(task)))
    return task


def _factory_instance_shadow(loop, coro, **kwargs):
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    return _shadow(_new_task(coro, loop, kwargs))


def _factory_shadow_other_coro(loop, coro, **kwargs):
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


_TOCTOU_STATE: Dict[str, _TOCTOU] = {}


def _make_toctou_class():
    attrs = {}
    for name in ("add_done_callback",):
        descriptor = _TOCTOU(name, getattr(asyncio.Task, name))
        _TOCTOU_STATE[name] = descriptor
        attrs[name] = descriptor
    return type("_ToctouTask", (asyncio.Task,), attrs)


_ToctouTask = _make_toctou_class()


def _factory_toctou(loop, coro, **kwargs):
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    for descriptor in _TOCTOU_STATE.values():
        descriptor.calls = 0
    if sys.version_info >= (3, 11):
        return _ToctouTask(coro, loop=loop, context=kwargs.get("context"))
    return _ToctouTask(coro, loop=loop)


class _StatefulGetattribute(asyncio.Task):
    """R-12c：对 ``add_done_callback`` 第 2 次起抛（有状态 ``__getattribute__``）。"""

    def __getattribute__(self, name):
        if name == "add_done_callback":
            counts = object.__getattribute__(self, "_ga_counts")
            counts[name] = counts.get(name, 0) + 1
            if counts[name] > 1:
                raise RuntimeError(f"stateful-ga-boom:{name}")
        return object.__getattribute__(self, name)

    def __init__(self, *a, **k):
        object.__setattr__(self, "_ga_counts", {})
        asyncio.Task.__init__(self, *a, **k)


def _factory_stateful_ga(loop, coro, **kwargs):
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)

    async def other():
        return "not-teardown"
    return _StatefulGetattribute(other(), loop=loop)


class _HostileMethods(asyncio.Task):
    """把引擎会用到的方法全部**计数后委托**（#109 r13/B 的结构性钉）。

    计数即探针：引擎若经实例/类属性查找去用受理 Task，对应计数就会非零。委托保证
    非引擎路径（loop 的 ``all_tasks()`` / 测试收尾的 ``cancel``）照常工作，不会把
    观测工具自己弄崩（r12 验收者的 ``StatefulGA`` 就栽在这里）。
    """

    seen: Dict[str, int] = {}

    def _hit(self, name):
        type(self).seen[name] = type(self).seen.get(name, 0) + 1

    def add_done_callback(self, callback, *, context=None):
        self._hit("add_done_callback")
        return asyncio.Task.add_done_callback(self, callback, context=context)

    def done(self):
        self._hit("done")
        return asyncio.Task.done(self)

    def cancelled(self):
        self._hit("cancelled")
        return asyncio.Task.cancelled(self)

    def exception(self):
        self._hit("exception")
        return asyncio.Task.exception(self)

    def get_loop(self):
        self._hit("get_loop")
        return asyncio.Task.get_loop(self)

    def get_coro(self):
        self._hit("get_coro")
        return asyncio.Task.get_coro(self)

    def cancel(self, *a, **k):
        self._hit("cancel")
        return asyncio.Task.cancel(self, *a, **k)


def _factory_hostile_methods(loop, coro, **kwargs):
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    _HostileMethods.seen.clear()
    return _HostileMethods(coro, loop=loop)


class Test109R13DirectCCalls(unittest.IsolatedAsyncioTestCase):
    async def test_remove_done_callback_alias_is_refused_named(self):
        """验收者的决定性反例：实例属性放**真** stdlib 绑定方法。

        r12：判据放行 ⇒ 引擎的 ``add_done_callback`` 变成 ``remove_done_callback``
        （注册了个寂寞）⇒ 两次 close 都永久挂起。r13：实例影子被
        ``object.__delattr__`` 探测到 ⇒ 具名拒收 + 整体回滚。
        """
        rec = await run_shape(_factory_alias)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT",
                            "r12 上这条（跑别的 coroutine 时）是 HANG ×2")
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, "teardown-spawn-failed")
        self.assertIn(TEARDOWN_HOSTILE_TASK_REASON, rec.cause_text)
        self.assertEqual(rec.state, (False, "None", False, 0), "整体回滚 ⇒ 可重试")
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.orphan_count, 0)
        self.assertEqual(rec.runtime_warnings, [])
        self.assertEqual(rec.leaked_reports(), [])
        # 同一形状 + 跑别的 coroutine（r12 的挂起变体）：同样具名、两次 close 都有界。
        rec2 = await run_shape(_factory_alias_other_coro)
        self.assertNotEqual(rec2.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec2.named_kind, "teardown-spawn-failed")
        self.assertIn(TEARDOWN_HOSTILE_TASK_REASON, rec2.cause_text)
        rec3 = await run_shape(_factory_alias_other_coro, bus=rec2.bus)
        self.assertNotEqual(rec3.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec3.closes, [0, 0, 0, 0, 0])
        # 健康重试：中和成功 ⇒ 恰好关一次。
        await asyncio.wait_for(rec.bus.close(), timeout=WATCHDOG)
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])

    async def test_r11_r12_variants_are_still_refused(self):
        """r11 的实例影子（``__eq__`` 恒真）+ r12 的类级 TOCTOU 都必须仍被拒。"""
        rec = await run_shape(_factory_instance_shadow)
        self.assertEqual(rec.named_kind, "teardown-spawn-failed")
        self.assertIn(TEARDOWN_HOSTILE_TASK_REASON, rec.cause_text)
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        self.assertEqual(rec.runtime_warnings, [])
        rec2 = await run_shape(_factory_shadow_other_coro)
        self.assertNotEqual(rec2.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec2.named_kind, "teardown-spawn-failed")
        loop = asyncio.get_running_loop()
        for descriptor in _TOCTOU_STATE.values():
            descriptor.calls = 0
        task = _ToctouTask(self._noop(), loop=loop)
        try:
            self.assertFalse(_task_uses_stdlib_methods(task))
            self.assertEqual({n: d.calls for n, d in _TOCTOU_STATE.items()},
                             {n: 0 for n in _TOCTOU_STATE},
                             "静态表必须在**任何**实例访问之前拒收（TOCTOU 无处下手）")
        finally:
            task.cancel()
            await settle()
        rec3 = await run_shape(_factory_toctou)
        self.assertEqual(rec3.named_kind, "teardown-spawn-failed")
        self.assertEqual({n: d.calls for n, d in _TOCTOU_STATE.items()},
                         {n: 0 for n in _TOCTOU_STATE})

    async def test_stateful_getattribute_is_refused_from_the_class_table(self):
        """R-12c：覆写 ``__getattribute__`` 的子类在**类表**上被拒，动态访问 0 次。"""
        rec = await run_shape(_factory_stateful_ga)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, "teardown-spawn-failed")
        self.assertIn(TEARDOWN_HOSTILE_TASK_REASON, rec.cause_text)
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        adopted = rec.sink.get("adopted")
        self.assertIsNone(adopted, "拒收路径不登记在飞 teardown")

    async def test_reject_and_neutralize_bypass_every_task_override(self):
        """结构性：拒收/中和一个覆写了全部相关方法的 Task 时，一次都不碰那些覆写。

        r11/B 的失败模式正是「裸抛且不回滚」——``_reject_spawned_task`` 里若有任何一处
        属性查找，这个形状就会走成裸抛。r13 全部改成 C 函数调用，所以计数必须为 0。
        """
        rec = await run_shape(_factory_hostile_methods)
        self.assertNotEqual(rec.outcome, "HANG/TIMEOUT")
        self.assertEqual(rec.outcome, "raised")
        self.assertEqual(rec.named_kind, "teardown-spawn-failed")
        self.assertIn(TEARDOWN_HOSTILE_TASK_REASON, rec.cause_text)
        self.assertEqual(rec.state, (False, "None", False, 0), "整体回滚")
        self.assertEqual(rec.closes, [0, 0, 0, 0, 0])
        # 拒收/中和路径上引擎会用到 ``get_coro`` / ``add_done_callback`` / ``cancel``：
        # 这三个计数必须为 0。``get_loop`` / ``done`` 的计数来自**观测工具自己**
        # （``asyncio.all_tasks()`` 的 ``_get_loop(t)`` + ``not t.done()``），与被测机制无关，
        # 所以这里只允许出现这两个名字。
        for name in ("get_coro", "add_done_callback", "cancel"):
            self.assertEqual(_HostileMethods.seen.get(name, 0), 0,
                             f"引擎不许经属性查找调用 {name}（实测：{_HostileMethods.seen}）")
        self.assertLessEqual(set(_HostileMethods.seen), {"get_loop", "done"},
                             f"engine-side attribute lookups leaked: {_HostileMethods.seen}")
        self.assertEqual(rec.orphan_count, 0)
        self.assertEqual(rec.leaked_reports(), [])
        # 交叉钉：同一个 Task 的覆写确实**会**计数，所以上面的断言不是空的证据。
        loop = asyncio.get_running_loop()
        _HostileMethods.seen.clear()
        probe = _HostileMethods(self._noop(), loop=loop)
        try:
            self.assertFalse(_task_uses_stdlib_methods(probe),
                             "类表判据必须拒收覆写（r11/B 的静态层）")
            self.assertEqual(_HostileMethods.seen, {},
                             "静态判据一次都不许对实例取属性（TOCTOU/有状态访问）")
            probe.done()
            self.assertEqual(_HostileMethods.seen, {"done": 1})
        finally:
            _HostileMethods.seen.clear()
            asyncio.Task.cancel(probe)
            await settle()

    async def test_bind_round_registers_through_the_c_function_not_the_attribute(self):
        """直接钉 ``_bind_round`` 的注册路径：实例属性上的**真** stdlib 别名不得被使用。

        ``_accept_spawned_task`` 会把这种影子形 Task 拒收，所以这里绕过它、直接驱动
        ``_bind_round``——这正是 r12 的 B 反例本体（``remove_done_callback`` 绑定方法被当成
        ``add_done_callback`` 用，注册了个寂寞）。r13 的注册走
        ``asyncio.Task.add_done_callback(task, _watch)``，因此别名一次都不该被调到，
        而且看门狗必须真的在任务结束时结算本轮 future。
        """
        loop = asyncio.get_running_loop()
        bus, comps = _make_bus()
        future = loop.create_future()
        bus._close_future = future
        bus._closing = True
        round_ = _TeardownRound()
        task = asyncio.Task(self._noop(), loop=loop)
        alias_calls: List[Any] = []
        real_remove = asyncio.Task.remove_done_callback.__get__(task, type(task))

        def alias(callback):
            alias_calls.append(callback)
            return real_remove(callback)

        setattr(task, "add_done_callback", alias)
        try:
            self.assertIs(bus._bind_round(task, round_, future), task)
            self.assertEqual(alias_calls, [],
                             "注册不许走实例属性（否则别名会吞掉看门狗 ⇒ 长等）")
            # 注意：**std**lib 的 shield/await 在影子 Task 上也会走属性查找（所以这里不 await
            # 这个 task，只推进循环）——被测的是引擎自己的注册路径。
            for _ in range(SETTLE_TICKS + 2):
                await asyncio.sleep(0)
            self.assertTrue(asyncio.Task.done(task))
            self.assertTrue(future.done(),
                            "C 函数注册的看门狗必须真的跑起来并结算本轮 future")
            self.assertIn(TEARDOWN_NEVER_ENTERED_KIND, str(future.exception()))
            self.assertEqual(bus._teardown_error_kind, TEARDOWN_NEVER_ENTERED_KIND)
        finally:
            _TEARDOWN_ROUNDS.pop(id(task), None)
            if not future.done():
                future.cancel()
            else:
                future.exception()
            bus._close_future = None
            bus._closing = False

    async def test_legitimate_shapes_are_not_refused(self):
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

            getattr_task = _Getattr(self._noop(), loop=loop)
            created.append(getattr_task)
            self.assertTrue(_task_uses_stdlib_methods(getattr_task),
                            "覆写 __getattr__（只在缺失时触发）必须通过")
            # 这两道新闸也必须放行它们
            self.assertFalse(R._overrides_instance_getattribute(default))
            self.assertIsNone(R._purge_instance_method_shadows(plain))
            self.assertIsNone(R._purge_instance_method_shadows(slotted))
            self.assertIsNone(R._purge_instance_method_shadows(attrs))
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
                self.assertFalse(R._overrides_instance_getattribute(task))
                self.assertIsNone(R._purge_instance_method_shadows(task))
            finally:
                await task

        try:
            adv.run_until_complete(main())
        finally:
            adv.close()

    async def test_instance_shadow_probe_detects_c_task_managed_dict(self):
        """``object.__delattr__`` 是唯一能看见 C Task managed dict 影子的探针。"""
        loop = asyncio.get_running_loop()
        task = asyncio.Task(self._noop(), loop=loop)
        try:
            self.assertIsNone(R._purge_instance_method_shadows(task),
                              "没有影子就该判 None（删不存在的属性抛 AttributeError）")
            task.add_done_callback = asyncio.Task.remove_done_callback.__get__(
                task, type(task))
            self.assertIs(
                inspect.getattr_static(task, "add_done_callback"),
                asyncio.Task.add_done_callback,
                "复现验收者的前提：getattr_static 对 C Task 的实例影子是瞎的")
            self.assertEqual(R._purge_instance_method_shadows(task),
                             "add_done_callback",
                             "r13 的探针必须看得见并清除它")
            self.assertIsNot(getattr(task, "add_done_callback", None), None)
        finally:
            task.cancel()
            await settle()

    async def _noop(self):
        return None


if __name__ == "__main__":
    unittest.main()
