"""#109/#110 r14：``close()`` 的**完成路径**不再经过任何实例属性查找（N8），并重钉 N1/N1b/N2/N4/N5。

r13 的独立验收是 **FAIL**：四条**明写的界**是假的，其中 N8 是与 r12-B 同级的**决定性挂起**。

* **N8（决定性）**：``close()`` 用 ``asyncio.shield(self._close_future)`` 等关停完成，而
  ``shield`` 内部做的是**动态属性查找**（3.10–3.13 源码一致）——``inner.get_loop()`` 与
  ``inner.add_done_callback(...)``。close future 对 task factory 可达（``self`` 就在交给工厂
  的 coroutine 的 ``cr_frame.f_locals`` 里），于是：
    * ``bus._close_future.add_done_callback = lambda *a, **k: None`` ⇒ 完成回调从未注册
      ⇒ 真 future 已被我们结算、**调用方永远收不到** ⇒ ``close()`` **挂起**；
    * 影子 ``get_loop`` ⇒ ``AttributeError: 'str' object has no attribute 'create_future'``；
    * 影子 ``add_done_callback`` 抛 ⇒ 同样一个与具名结局无关的裸异常；
    * 对照：影子 ``done`` 无害（看门狗走 C 函数）——**C 转换在它覆盖到的地方有效**。
  r14 的修法：``close()`` 不再 shield，改 ``_await_close_future``——只用
  ``asyncio.Future`` 类上的 C 函数，等一个**本函数局部创建**的 waiter（工厂在 spawn 窗口
  够不到它）。``request_close`` / ``_self_close_refusal`` 也改走**类**（绕过实例影子）。
* **N1/N1b**：tamper 信号要求「受理的 Task 确实结束 + ``_teardown_runs == 0`` + 本轮第一次
  判定还没发生」。工厂改写 ``_teardown_runs`` 或交回**永不结束**的 Task 都能让它不出现
  ——「让静默不再可能」是**假的**，r14 改成具名、可测的**条件界**。
* **N2**：r13 只在「future 仍未结算」分支钉回幂等凭据；把 ``self._close_future`` 换成
  **pending 诱饵**后第二次 ``close()`` **挂起**。r14 在受理点与判定点都钉回，并把已结算
  结局**镜像**到诱饵上（放行已经等在诱饵上的等待者）。
* **N3/N4/N5**：``round_.tampered`` 经模块全局 ``_TEARDOWN_ROUNDS`` 一行可改 ⇒ 抑制
  WARNING（N4）/ 在健康关停上造成假阳性（N5）。r14 把 WARNING 的判定门移进 ``_watch`` 的
  闭包（``judged``），伪造 round 字段不再能抑制日志；kind 字段仍可伪造，**如实声明**。
"""

import asyncio
import contextlib
import inspect
import logging
import re
import sys
import unittest
from typing import Any, Dict, List, Optional

import pytest

pytest.importorskip("redis")

from plaita.event.redis import (          # noqa: E402
    TEARDOWN_INCOMPLETE_KIND,
    TEARDOWN_NEVER_ENTERED_KIND,
    TEARDOWN_SETTLEMENT_TAMPERED_KIND,
    RedisEventBus,
    _ACCEPTED_CLOSE_FUTURES,
    _TEARDOWN_ROUNDS,
    _await_close_future,
)

from tests.unit.test_issue109_110_r13_parity import (  # noqa: E402
    WATCHDOG,
    _is_our_entry,
    _make_bus,
    _new_task,
    settle,
)

_ALL_KINDS = (TEARDOWN_SETTLEMENT_TAMPERED_KIND, TEARDOWN_NEVER_ENTERED_KIND,
              TEARDOWN_INCOMPLETE_KIND, "teardown-spawn-failed", "cross-loop-close")

#: 测试侧的见证（工厂真的把影子/诱饵/伪造装到了**本轮真对象**上）
_APPLIED: List[Dict[str, Any]] = []


def _named_kind(exc) -> Optional[str]:
    if isinstance(exc, RuntimeError):
        for kind in _ALL_KINDS:
            if kind in str(exc):
                return kind
    return None


def _executable_source(func) -> str:
    """函数源码去掉 docstring（结构性断言只看**可执行体**）。"""
    return re.sub(r'"""..*?"""', "", inspect.getsource(func), count=1, flags=re.S)


async def close_once(bus, bound: float = WATCHDOG):
    """真 ``bus.close()`` + **有界**看门狗。N8/N2 的决定性路径是 ``close()``（不是 request_close）。"""
    try:
        await asyncio.wait_for(bus.close(), bound)
        return "returned", None
    except asyncio.TimeoutError:
        return "HANG/TIMEOUT", None
    except BaseException as exc:                 # noqa: BLE001 - 断言用
        return "raised", exc


class _LogCapture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records: List[logging.LogRecord] = []

    def emit(self, record):
        self.records.append(record)


@contextlib.contextmanager
def captured_warnings():
    logger = logging.getLogger("plaita.event.redis")
    handler = _LogCapture()
    previous = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    try:
        yield handler.records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)


def _warn_count(records) -> int:
    return sum(1 for r in records if r.levelno >= logging.WARNING)


async def _with_factory(bus, factory, closers=2):
    """装 factory，跑 ``closers`` 次真 ``close()``（每次 2 s 有界），每次之间让出若干 tick。"""
    loop = asyncio.get_running_loop()
    saved = loop.get_task_factory()
    results = []
    loop.set_task_factory(factory)
    try:
        for index in range(closers):
            results.append(await close_once(bus))
            if index + 1 < closers:
                await settle()
    finally:
        loop.set_task_factory(saved)
        await settle()
    return results


# --------------------------------------------------------------------------
# N8：工厂给真 close future 塞实例影子（一条普通实例属性）
# --------------------------------------------------------------------------
def _credential(coro):
    return coro.cr_frame.f_locals["self"]._close_future


def _n8_factory(variant):
    def factory(loop, coro, **kwargs):
        if not _is_our_entry(coro):
            return _new_task(coro, loop, kwargs)
        future = _credential(coro)
        if variant == "noop":
            future.add_done_callback = lambda *a, **k: None
        elif variant == "raise":
            def boom(*a, **k):
                raise RuntimeError("shadow-add-done-callback")
            future.add_done_callback = boom
        elif variant == "get_loop":
            future.get_loop = lambda: "not-a-loop"
        elif variant == "done":
            future.done = lambda: False
        else:                                    # pragma: no cover - 测试自身防呆
            raise AssertionError(variant)
        _APPLIED.append({"variant": variant, "future": future})
        return _new_task(coro, loop, kwargs)
    return factory


class Test109R14CloseCompletionImmune(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _APPLIED.clear()

    async def _run(self, variant):
        bus, comps = _make_bus()
        # 两次 close()：第一次走「受理」分支（request_close + waiter），第二次走**幂等**
        # 分支（loop 检查 + 同一个 future）——N8 的三个影子对两条路径都要无效。
        (out1, exc1), (out2, exc2) = await _with_factory(
            bus, _n8_factory(variant), closers=2)
        return bus, comps, out1, exc1, out2, exc2

    async def _assert_named_and_bounded(self, variant):
        bus, comps, out1, exc1, out2, exc2 = await self._run(variant)
        self.assertEqual(
            out1, "returned",
            f"N8/{variant}：真 future 已被我们自己的关停路径结算，调用方必须收到"
            f"（r13 的 shield 路径下这里 HANG/TIMEOUT）")
        self.assertIsNone(exc1)
        self.assertEqual(
            out2, "returned",
            f"N8/{variant}：幂等分支（`asyncio.Future.get_loop` + 引擎自建 waiter）"
            f"同样必须绕开实例影子（否则影子 `get_loop` 会把跨循环检查判成命中）")
        self.assertIsNone(exc2)
        self.assertEqual(bus._teardown_runs, 1, "第二次是幂等早返回，不得再跑一次 teardown")
        self.assertEqual([c.closes for c in comps], [1, 1, 1, 1, 1])
        self.assertEqual(len(_APPLIED), 1, "工厂必须真的把影子装上去了")
        self.assertEqual(_APPLIED[0]["variant"], variant)
        self.assertIs(_APPLIED[0]["future"], bus._close_future,
                      "影子必须装在**本轮幂等凭据**上（否则测的不是 N8）")
        return bus, comps

    async def test_n8_noop_add_done_callback_shadow_is_bounded(self):
        """N8 本体：``add_done_callback`` 影子（吞掉 shield 的完成回调）⇒ 不许挂起。"""
        await self._assert_named_and_bounded("noop")

    async def test_n8_raising_add_done_callback_shadow_is_bounded(self):
        """N8 变体：``add_done_callback`` 影子会抛 ⇒ 不许逃出与具名结局无关的裸异常。"""
        await self._assert_named_and_bounded("raise")

    async def test_n8_get_loop_shadow_is_bounded(self):
        """N8 变体：影子 ``get_loop`` 返回非 loop ⇒ 不许 ``AttributeError`` 逃出。"""
        await self._assert_named_and_bounded("get_loop")

    async def test_n8_done_shadow_control_is_harmless(self):
        """对照：影子 ``done`` 无害（看门狗走 C 函数）——C 转换在它覆盖到的地方有效。"""
        await self._assert_named_and_bounded("done")

    async def test_completion_waiter_uses_only_class_level_c_functions(self):
        """结构性：``close()`` / waiter 的**可执行体**里不许再有对凭据的实例属性查找或 shield。"""
        close_src = _executable_source(RedisEventBus.close)
        waiter_src = _executable_source(_await_close_future)
        self.assertIsNone(
            re.search(r"asyncio\.shield\s*\(", close_src),
            "N8：close() 不许再**调用** shield（它按 inner.get_loop/"
            "inner.add_done_callback 做动态查找）")
        for banned in ("self._close_future.", "self.request_close(",
                       "self._self_close_refusal("):
            self.assertNotIn(banned, close_src,
                             f"close() 仍在工厂可达的 self 上做属性查找：{banned}")
        self.assertIn("asyncio.Future.get_loop", close_src)
        self.assertIn("RedisEventBus.request_close", close_src)
        for banned in ("future.get_loop(", "future.add_done_callback(",
                       "inner.get_loop(", "inner.add_done_callback("):
            self.assertNotIn(banned, waiter_src,
                             f"waiter 仍在被等的 future 上做属性查找：{banned}")
        self.assertIn("asyncio.Future.add_done_callback(future, _forward)", waiter_src)

    async def test_shadowed_request_close_and_refusal_are_never_consulted(self):
        """枚举/影子测试：``self.request_close`` 与 ``self._self_close_refusal`` 都改走类。"""
        bus, comps = _make_bus()
        loop = asyncio.get_running_loop()
        calls: List[str] = []

        def hostile_request_close():
            calls.append("request_close")
            return loop.create_future()          # pending 诱饵：被用上就挂起

        def hostile_refusal(current):
            calls.append("refusal")
            return RuntimeError("shadow-self-close-refusal")

        bus.request_close = hostile_request_close
        bus._self_close_refusal = hostile_refusal
        outcome, exc = await close_once(bus)
        self.assertEqual(outcome, "returned")
        self.assertIsNone(exc)
        self.assertEqual(calls, [], "close() 不许走实例属性查找（否则影子直接改结局）")
        self.assertEqual([c.closes for c in comps], [1, 1, 1, 1, 1])

    async def test_poisoned_pending_tasks_fence_only_fails_bounded(self):
        """枚举：跨循环围栏上的 ``task.done()`` / ``task.get_loop()`` 不在完成路径上。

        影子最多让这道围栏误判（bounded 裸抛或跳过），不能让 close future 悬置、也不能改判决。
        """
        bus, comps = _make_bus()
        running = asyncio.get_running_loop()

        class _Poison:
            def done(self):
                return False

            def get_loop(self):
                return running                    # 围栏被跳过 ⇒ 交给 drain 去失败

        bus._pending_tasks = {_Poison(): ("e", "h")}
        outcome, exc = await close_once(bus)
        self.assertNotEqual(outcome, "HANG/TIMEOUT",
                            "跨循环围栏上的实例影子只能有界失败/跳过（不许挂起）")
        self.assertIn(outcome, ("returned", "raised"))
        if outcome == "raised":
            self.assertEqual(_named_kind(exc), TEARDOWN_INCOMPLETE_KIND,
                             "非 Task 的分发登记必须在关停序列里被接住并具名，不许裸抛")


# --------------------------------------------------------------------------
# N2：settle 真 future + 换成 pending 诱饵 ⇒ 第二次 close() 必须有界
# --------------------------------------------------------------------------
def _n2_spawn_decoy_factory(loop, coro, **kwargs):
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    bus = coro.cr_frame.f_locals["self"]
    real = bus._close_future
    asyncio.Future.set_result(real, None)        # 直接结算（假成功，S5 同形）
    decoy = loop.create_future()
    bus._close_future = decoy                    # spawn 窗口里换成 pending 诱饵
    _APPLIED.append({"real": real, "decoy": decoy})

    async def other():
        return "not-teardown"
    return _new_task(other(), loop, kwargs)


def _n2_reseat_factory(loop, coro, **kwargs):
    """受理点钉回**之后**经 ``call_soon`` 再换一次诱饵（越过一次性钉回）。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    bus = coro.cr_frame.f_locals["self"]
    real = bus._close_future
    asyncio.Future.set_result(real, None)
    decoy = loop.create_future()
    gate = loop.create_future()
    _APPLIED.append({"real": real, "decoy": decoy})

    async def other():
        await gate
        return "not-teardown"

    task = _new_task(other(), loop, kwargs)

    def reseat():
        bus._close_future = decoy
        if not gate.done():
            gate.set_result(None)

    loop.call_soon(reseat)
    return task


def _healthy_swap_factory(loop, coro, **kwargs):
    """健康关停 + 工厂在 spawn 窗口把总线字段换成 pending 诱饵（真 future 只在请求方局部）。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    bus = coro.cr_frame.f_locals["self"]
    decoy = loop.create_future()
    bus._close_future = decoy
    _APPLIED.append({"decoy": decoy})
    return _new_task(coro, loop, kwargs)


def _healthy_swap_eager_factory(loop, coro, **kwargs):
    """eager：teardown 在 ``create_task`` 里**同步**跑完（结算打到诱饵上，早于受理点钉回）。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    bus = coro.cr_frame.f_locals["self"]
    decoy = loop.create_future()
    bus._close_future = decoy
    _APPLIED.append({"decoy": decoy})
    return asyncio.eager_task_factory(loop, coro, **kwargs)


class Test109R14CredentialRepin(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _APPLIED.clear()

    async def test_n2_spawn_decoy_second_close_is_bounded(self):
        """N2 本体：结算真 future + 换成 pending 诱饵 ⇒ 第二次 close() 不许挂起。"""
        bus, comps = _make_bus()
        with captured_warnings() as records:
            (out1, exc1), (out2, exc2) = await _with_factory(
                bus, _n2_spawn_decoy_factory, closers=2)
        decoy = _APPLIED[0]["decoy"]
        self.assertEqual(out1, "returned")
        self.assertIsNone(exc1)
        self.assertNotEqual(out2, "HANG/TIMEOUT",
                            "诱饵不许把幂等凭据钉死成永不结算（r13 在这里挂起）")
        self.assertEqual(out2, "returned")
        self.assertIsNone(exc2)
        self.assertIsNot(bus._close_future, decoy, "受理点/判定点必须钉回真 future")
        self.assertIs(bus._close_future, _APPLIED[0]["real"])
        self.assertEqual(bus._teardown_runs, 0, "工厂交回的不是我们的入口 ⇒ 一次都没跑")
        self.assertEqual([c.closes for c in comps], [0, 0, 0, 0, 0])
        self.assertGreaterEqual(_warn_count(records), 1, "假成功必须留下 tamper WARNING")

    async def test_n2_decoy_reseated_after_bind_second_close_is_bounded(self):
        """N2 变体：受理点钉回之后再经 ``call_soon`` 换一次诱饵 ⇒ 判定点仍须钉回。"""
        bus, comps = _make_bus()
        with captured_warnings() as records:
            (out1, exc1), (out2, exc2) = await _with_factory(
                bus, _n2_reseat_factory, closers=2)
        decoy = _APPLIED[0]["decoy"]
        self.assertEqual((out1, exc1), ("returned", None))
        self.assertNotEqual(out2, "HANG/TIMEOUT")
        self.assertEqual(out2, "returned")
        self.assertIsNone(exc2)
        self.assertIs(bus._close_future, _APPLIED[0]["real"],
                      "判定点把诱饵钉回受理时捕获的真 future")
        self.assertEqual(bus._teardown_runs, 0)
        self.assertEqual([c.closes for c in comps], [0, 0, 0, 0, 0])
        self.assertGreaterEqual(_warn_count(records), 1)

    async def test_n2_spawn_decoy_is_repaired_before_close_returns(self):
        """受理点的钉回必须**在第一次 close() 返回之前**落地（否则第二次仍可能等诱饵）。"""
        bus, comps = _make_bus()
        loop = asyncio.get_running_loop()
        saved = loop.get_task_factory()
        loop.set_task_factory(_n2_spawn_decoy_factory)

        async def scenario():
            await bus.close()                    # 直接 await：真 future 已结算 ⇒ 不让出
            return bus._close_future

        try:
            field = await asyncio.wait_for(scenario(), WATCHDOG)
        finally:
            loop.set_task_factory(saved)
            await settle()
        self.assertIs(field, _APPLIED[0]["real"],
                      "受理点必须在 close() 返回前把诱饵钉回受理时捕获的真 future")
        self.assertIsNot(field, _APPLIED[0]["decoy"])

    async def test_n2_waiter_already_on_the_decoy_is_released(self):
        """诱饵已被第二个 close() 拿去等待 ⇒ 判定点必须把结局**镜像**到诱饵上放行它。"""
        bus, comps = _make_bus()
        loop = asyncio.get_running_loop()
        saved = loop.get_task_factory()
        loop.set_task_factory(_n2_reseat_factory)

        async def scenario():
            await bus.close()                        # close#1（真 future 已结算 ⇒ 不让出）
            second = asyncio.create_task(bus.close())  # 立刻的第二个等待者
            await second                              # 没被镜像放行就会悬着
            return second

        try:
            second = await asyncio.wait_for(scenario(), WATCHDOG)
        finally:
            loop.set_task_factory(saved)
            await settle()
        self.assertTrue(second.done())
        self.assertIsNone(second.exception(), "镜像必须把真 future 的结局交给等待者")
        self.assertIs(bus._close_future, _APPLIED[0]["real"])

    async def test_n2_healthy_swap_at_bind_keeps_the_true_success(self):
        """健康关停 + spawn 窗口换诱饵 ⇒ 受理点钉回后结算的是真 future：判决仍是成功。"""
        bus, comps = _make_bus()
        with captured_warnings() as records:
            (out1, exc1), = await _with_factory(bus, _healthy_swap_factory, closers=1)
        self.assertEqual(out1, "returned",
                         "无关停字段可改判决：组件真的关过一次 ⇒ 不许报 never-entered")
        self.assertIsNone(exc1)
        self.assertEqual(bus._teardown_runs, 1)
        self.assertEqual([c.closes for c in comps], [1, 1, 1, 1, 1])
        self.assertGreaterEqual(_warn_count(records), 1,
                                "受理窗口内凭据被改写必须留痕（reason=teardown-credential-repin）")
        self.assertIsNot(bus._close_future, _APPLIED[0]["decoy"])

    @unittest.skipUnless(hasattr(asyncio, "eager_task_factory"),
                         "需要 3.12+ 的 asyncio.eager_task_factory")
    async def test_n2_eager_healthy_swap_is_conservative_not_silent(self):
        """eager：结算早于受理点钉回 ⇒ 具名 ``teardown-never-entered``（**保守误报**）。

        这是 r14 明示的残差 R-14a：「入口跑没跑」没有任何不可伪造的信号（``_teardown_runs``
        是工厂可写字段，用它就会像 r12 的负控那样压住具名结局），所以这里只保证
        **有界 + 具名 + 组件真的关过一次 + 永不静默成功**。
        """
        bus, comps = _make_bus()
        with captured_warnings() as records:
            (out1, exc1), = await _with_factory(
                bus, _healthy_swap_eager_factory, closers=1)
        self.assertEqual(out1, "raised")
        self.assertEqual(_named_kind(exc1), TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(bus._teardown_runs, 1, "入口其实跑过（组件确实关了一次）")
        self.assertEqual([c.closes for c in comps], [1, 1, 1, 1, 1])
        self.assertGreaterEqual(_warn_count(records), 1)
        self.assertNotEqual(out1, "returned",
                            "宁可保守报失败，也不许静默成功")


# --------------------------------------------------------------------------
# N1/N1b/N4/N5：tamper 信号的确切触发条件（可抑制、不可信）
# --------------------------------------------------------------------------
def _n1_forge_runs_factory(loop, coro, **kwargs):
    """N1：结算真 future + 把工厂可写的 ``_teardown_runs`` 写成 1 + 关掉入口 coro。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    bus = coro.cr_frame.f_locals["self"]
    asyncio.Future.set_result(bus._close_future, None)
    bus._teardown_runs = 1
    coro.close()                                 # 防 "was never awaited"

    async def other():
        return "not-teardown"
    return _new_task(other(), loop, kwargs)


def _n1b_never_ending_factory(loop, coro, **kwargs):
    """N1b：结算真 future + 交回**永不结束**的 Task ⇒ 判定事件永不发生。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    bus = coro.cr_frame.f_locals["self"]
    asyncio.Future.set_result(bus._close_future, None)

    async def forever():
        await asyncio.Event().wait()

    task = _new_task(forever(), loop, kwargs)
    _APPLIED.append({"task": task})
    return task


def _n4_forge_round_tampered_factory(loop, coro, **kwargs):
    """N4：结算真 future + 经模块全局 ``_TEARDOWN_ROUNDS[id(task)]`` 预置 ``tampered=True``。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    bus = coro.cr_frame.f_locals["self"]
    asyncio.Future.set_result(bus._close_future, None)
    holder: Dict[str, Any] = {}

    async def other():
        await asyncio.sleep(0)                   # 让 _bind_round 先登记
        round_ = _TEARDOWN_ROUNDS.get(id(holder["task"]))
        if round_ is not None:
            round_.tampered = True

    task = _new_task(other(), loop, kwargs)
    holder["task"] = task
    return task


def _n5_forge_round_tampered_on_healthy_close(loop, coro, **kwargs):
    """N5：**健康**关停 + 伪造 ``round_.tampered`` ⇒ 假阳性 ``teardown-settlement-tampered``。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    holder: Dict[str, Any] = {}

    async def wrapper():
        await asyncio.sleep(0)                   # 让 _bind_round 先登记
        round_ = _TEARDOWN_ROUNDS.get(id(holder["task"]))
        if round_ is not None:
            round_.tampered = True
        await coro

    task = _new_task(wrapper(), loop, kwargs)
    holder["task"] = task
    return task


def _s5_direct_settle_factory(loop, coro, **kwargs):
    """S5 对照：工厂直接结算真 future（不碰任何字段）⇒ 信号**必须**出现。"""
    if not _is_our_entry(coro):
        return _new_task(coro, loop, kwargs)
    bus = coro.cr_frame.f_locals["self"]
    asyncio.Future.set_result(bus._close_future, None)

    async def other():
        return "not-teardown"
    return _new_task(other(), loop, kwargs)


class Test109R14TamperSignalBounds(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _APPLIED.clear()

    async def test_s5_signal_fires_when_the_conditions_hold(self):
        """正方向：直接结算真 future（runs==0、任务结束、第一次判定未发生）⇒ 具名 + WARNING。"""
        bus, comps = _make_bus()
        with captured_warnings() as records:
            (out1, exc1), = await _with_factory(
                bus, _s5_direct_settle_factory, closers=1)
        self.assertEqual(out1, "returned")
        self.assertIsNone(exc1)
        self.assertEqual(bus._teardown_error_kind, TEARDOWN_SETTLEMENT_TAMPERED_KIND)
        self.assertEqual(bus._teardown_runs, 0)
        self.assertEqual([c.closes for c in comps], [0, 0, 0, 0, 0])
        self.assertEqual(_warn_count(records), 1, "条件成立时 WARNING 恰好一次")

    async def test_n1_forged_teardown_runs_suppresses_the_signal(self):
        """N1（**明示的界**）：工厂把 ``_teardown_runs`` 写成 1 ⇒ 信号不出现（静默假成功）。"""
        bus, comps = _make_bus()
        with captured_warnings() as records:
            (out1, exc1), = await _with_factory(
                bus, _n1_forge_runs_factory, closers=1)
        self.assertEqual((out1, exc1), ("returned", None))
        self.assertEqual(bus._teardown_runs, 1)
        self.assertIsNone(bus._teardown_error_kind,
                          "文档写的是「条件界」：这个形状下**没有**信号")
        self.assertEqual(_warn_count(records), 0)
        self.assertEqual([c.closes for c in comps], [0, 0, 0, 0, 0])

    async def test_n1b_never_ending_task_never_fires_the_signal(self):
        """N1b（**明示的界**）：交回永不结束的 Task ⇒ 判定事件永不发生 ⇒ 无信号。"""
        bus, comps = _make_bus()
        with captured_warnings() as records:
            (out1, exc1), = await _with_factory(
                bus, _n1b_never_ending_factory, closers=1)
        self.assertEqual((out1, exc1), ("returned", None))
        self.assertIsNone(bus._teardown_error_kind)
        self.assertEqual(_warn_count(records), 0,
                         "信号只在「受理的 Task 结束」时触发；永不结束 ⇒ 永不触发（R-1 形状）")
        self.assertEqual(bus._teardown_runs, 0)
        self.assertEqual([c.closes for c in comps], [0, 0, 0, 0, 0])
        # 清理：那个永不结束的任务还在飞（R-1 明示 out of contract）。
        leaked = _APPLIED[0]["task"]
        asyncio.Task.cancel(leaked)
        await settle()

    async def test_n4_forging_round_tampered_cannot_suppress_the_warning(self):
        """N4 修复：伪造 ``round_.tampered``（模块全局可达）不再能抑制 WARNING。"""
        bus, comps = _make_bus()
        with captured_warnings() as records:
            (out1, exc1), = await _with_factory(
                bus, _n4_forge_round_tampered_factory, closers=1)
        self.assertEqual((out1, exc1), ("returned", None))
        self.assertEqual(bus._teardown_runs, 0)
        self.assertEqual([c.closes for c in comps], [0, 0, 0, 0, 0])
        self.assertGreaterEqual(
            _warn_count(records), 1,
            "判定门是 _watch 闭包里的 judged（不是工厂可预置的 round_.tampered）")
        self.assertEqual(bus._teardown_error_kind, TEARDOWN_SETTLEMENT_TAMPERED_KIND)

    async def test_n5_forging_round_tampered_on_a_healthy_close_is_a_false_positive(self):
        """N5（**明示的界**）：健康关停上伪造 ``round_.tampered`` ⇒ 假阳性 kind。"""
        bus, comps = _make_bus()
        with captured_warnings() as records:
            (out1, exc1), = await _with_factory(
                bus, _n5_forge_round_tampered_on_healthy_close, closers=1)
        self.assertEqual((out1, exc1), ("returned", None), "关停本身是成功的")
        self.assertEqual(bus._teardown_runs, 1)
        self.assertEqual([c.closes for c in comps], [1, 1, 1, 1, 1])
        self.assertEqual(
            bus._teardown_error_kind, TEARDOWN_SETTLEMENT_TAMPERED_KIND,
            "留档 kind 仍读 round_.tampered ⇒ 工厂可造出**假阳性**（文档如实声明，不声称可信）")
        self.assertEqual(_warn_count(records), 0,
                         "健康路径不触发 tamper WARNING（阳性只在 kind 上）")


# --------------------------------------------------------------------------
# 登记表与影子集合的自愈 / 边界
# --------------------------------------------------------------------------
class Test109R14RegistryHygiene(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _APPLIED.clear()

    async def test_decoy_never_enters_the_accepted_credential_set(self):
        bus, comps = _make_bus()
        before = set(map(id, _ACCEPTED_CLOSE_FUTURES))
        (out1, exc1), = await _with_factory(bus, _n2_spawn_decoy_factory, closers=1)
        await settle()
        self.assertEqual(out1, "returned")
        decoy = _APPLIED[0]["decoy"]
        self.assertNotIn(decoy, _ACCEPTED_CLOSE_FUTURES,
                         "诱饵不许被登记成「本引擎受理过的凭据」")
        self.assertIs(bus._close_future, _APPLIED[0]["real"])
        new = [f for f in _ACCEPTED_CLOSE_FUTURES if id(f) not in before]
        self.assertEqual(new, [_APPLIED[0]["real"]])

    async def test_registry_and_credential_cleanup_after_bounded_shapes(self):
        before = set(_TEARDOWN_ROUNDS)
        bus, comps = _make_bus()
        with captured_warnings():
            (out1, _), = await _with_factory(bus, _n2_spawn_decoy_factory, closers=1)
        await settle()
        self.assertEqual(out1, "returned")
        self.assertEqual(set(_TEARDOWN_ROUNDS) - before, set(),
                         "受理结束（含工厂形状）后登记表必须清空")
