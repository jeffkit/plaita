"""#109/#110 r15：诱饵判据改**身份**（不问对象自己）+ 判定路径异常边界 + 回滚判据更正。

r14 的独立验收报出一条**决定性的规则级回归**（N9），r14 因此判 FAIL：

* **N9（决定性）**：r14 新加的 ``_is_factory_decoy(field, future)`` 用
  ``field not in _ACCEPTED_CLOSE_FUTURES``（``WeakSet`` 成员判定）实现，而那是**对工厂可写
  的字段**求 ``hash()``（哈希撞上时还会 ``__eq__``）。一个 ``__hash__ = None`` 的
  ``asyncio.Future`` 子类（``_EvilFuture``，形状与本单自己的诱饵用例完全相同）让
  ``_watch_round``（**done 回调**、判定的落点）抛 ``TypeError``；asyncio 把回调异常交给
  ``call_exception_handler`` 吞掉 ⇒ **既没钉回、也没结算** ⇒ 第一次 ``close()`` 永久挂起
  （3.10–3.13 一致：`kind=None`、无 WARNING、无具名结局、五个组件已各关一次）。
  hashbind（在 spawn 窗口内**同步**换字段）时同一个异常改从**受理点**逃出 ⇒ 冻结体
  ``request_close`` 整体回滚 ⇒ 组件真的关了而 ``_closing`` 被复位（r9 的假回滚形状重开，
  且与 docs 里「回滚只发生在没有 teardown 在跑时」矛盾）。
* **r15 的修法**：① 判据只看**身份**（``id()`` / ``is``，身份索引
  ``_ACCEPTED_CLOSE_FUTURE_REFS``）且对**任何**字段值都不抛；② ``_watch_round`` 整段包异常
  边界，判定与结算**先**落地，宿主钩子（钉回/镜像/入口 coro 收尾）异常只留痕；③
  ``_bind_round`` 受理之后**绝不抛** ⇒ 回滚只在「teardown coroutine 从未被任何 Task 托管」
  时发生。

本文件把这三条逐条钉住（身份判据 / 有界具名 / 不回滚 / 无孤儿 / 无 never-awaited / 异常不
逃进事件循环），并钉住 docs 的每一条声称。另折叠 r14 验收终稿的两条必修项：

* **必修项 A**：``_await_close_future`` 的 waiter 原来是
  ``asyncio.get_running_loop().create_future()``——``loop`` 正是 task factory 的**第一个
  实参**，实例级 ``create_future`` 影子能把 waiter 换成「永不落地的 awaitable」⇒ 第一次
  ``close()`` 不再返回（3.10–3.13 实测；loop exc 记 ``descriptor 'cancelled' ... doesn't
  apply to a 'coroutine' object``）。r15 改成 ``asyncio.Future()``（类调用）⇒ 完成路径不读
  loop 实例的任何属性；loop 的可信性声明与 R-13a 同级（进程内对手能改 loop，但完成路径不
  依赖它的属性）。
* **必修项 B**：R-12a' 的理由句收窄为「②③ 必须由 spawn 交回**已结束的非 Task** 驱动 ⇒
  冻结体下无法收口」，不再写「任何收口都会把那些用例变成空测」（① 本可改走 task factory）。
"""

import asyncio
import gc
import hashlib
import inspect
import pathlib
import re
import sys
import traceback
import unittest
import warnings
import weakref
from typing import Any, Dict, List, Optional, Tuple
from unittest import mock

import pytest

pytest.importorskip("redis")

import plaita.event.redis as redis_module          # noqa: E402
from plaita.event.redis import (                   # noqa: E402
    TEARDOWN_NEVER_ENTERED_KIND,
    TEARDOWN_SETTLEMENT_TAMPERED_KIND,
    RedisEventBus,
    _ACCEPTED_CLOSE_FUTURE_REFS,
    _ACCEPTED_CLOSE_FUTURES,
    _accept_close_future,
    _is_factory_decoy,
)

from tests.unit.test_issue109_110_r13_parity import (  # noqa: E402
    WATCHDOG,
    _is_our_entry,
    _make_bus,
    _new_task,
)

_NAMED_KINDS = (TEARDOWN_NEVER_ENTERED_KIND, TEARDOWN_SETTLEMENT_TAMPERED_KIND,
                "teardown-spawn-failed", "teardown-incomplete", "cross-loop-close")

#: 判据/边界是否对**工厂可写的字段**调用过它的宿主钩子（``__hash__`` / ``__eq__``）
_HOOK_CALLS: List[str] = []


# --------------------------------------------------------------------------
# 诱饵形状（全部是「同一个 round 自己的测试」的形状：真 Future 子类 / 非 Future / None）
# --------------------------------------------------------------------------
class _EvilFuture(asyncio.Future):
    """r14 验收者的 N9 形状：真 ``Future`` 子类，只是 ``__hash__ = None``。"""

    __hash__ = None


class _HashRaises(asyncio.Future):
    def __hash__(self):
        _HOOK_CALLS.append("hash")
        raise RuntimeError("hash-boom")


class _EqRaises(asyncio.Future):
    """``__hash__`` 与真凭据相同（强行撞桶）且 ``__eq__`` 会抛。"""

    def __init__(self, *args, collide_with=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._collide = collide_with

    def __hash__(self):
        _HOOK_CALLS.append("hash")
        return hash(self._collide)

    def __eq__(self, other):                      # noqa: D105
        _HOOK_CALLS.append("eq")
        raise RuntimeError("eq-boom")


class _ClassSpoof:
    """不是 Future，但谎报 ``__class__``（r12/B 同类形状；只有身份判据挡得住）。"""

    @property
    def __class__(self):                          # noqa: A003
        return asyncio.Future

    def __hash__(self):
        _HOOK_CALLS.append("hash")
        return object.__hash__(self)


def _make_decoy(kind: str, bus, loop, real):
    if kind == "evil_hash_none":
        return _EvilFuture()
    if kind == "hash_raises":
        return _HashRaises()
    if kind == "eq_raises":
        return _EqRaises(collide_with=real)
    if kind == "class_spoof":
        return _ClassSpoof()
    if kind == "non_future":
        return object()
    if kind == "none":
        return None
    if kind == "plain_pending":
        return loop.create_future()
    raise AssertionError(kind)                    # pragma: no cover - 测试自身防呆


def _decoy_factory(mode: str, kind: str, state: Dict[str, Any]):
    """``spawnwin``=受理点之后经 ``call_soon`` 换 / ``direct``=工厂内同步换 / ``reseat``=受理后换。"""

    def factory(loop, coro, **kwargs):
        if not _is_our_entry(coro):
            return _new_task(coro, loop, kwargs)
        bus = coro.cr_frame.f_locals["self"]
        state["bus"] = bus
        state["real"] = bus._close_future

        def swap():
            decoy = _make_decoy(kind, bus, loop, state["real"])
            state["decoy"] = decoy
            bus._close_future = decoy
            gate = state.get("gate")
            if gate is not None and not gate.done():
                gate.set_result(None)

        if mode in ("spawnwin", "reseat"):
            loop.call_soon(swap)
        else:
            swap()
        if mode == "reseat":
            gate = loop.create_future()
            state["gate"] = gate

            async def other():
                await gate
                return "not-teardown"

            return _new_task(other(), loop, kwargs)
        return _new_task(coro, loop, kwargs)

    return factory


async def _settle(ticks: int = 6) -> None:
    for _ in range(ticks):
        await asyncio.sleep(0)


async def _close_once(bus, bound: float = WATCHDOG) -> Tuple[str, Optional[BaseException]]:
    try:
        await asyncio.wait_for(bus.close(), bound)
        return "returned", None
    except asyncio.TimeoutError:
        return "HANG/TIMEOUT", None
    except BaseException as exc:                  # noqa: BLE001 - 断言用
        return "raised", exc


def _named_kind(exc) -> Optional[str]:
    if isinstance(exc, RuntimeError):
        for kind in _NAMED_KINDS:
            if kind in str(exc):
                return kind
    return None


class _DecoyRecord:
    def __init__(self, **kw):
        self.__dict__.update(kw)


async def _run_decoy(mode: str, kind: str, closers: int = 2) -> _DecoyRecord:
    """跑一次诱饵形状：真 ``close()``（有界）+ 观测 outcomes/comps/runs/字段/孤儿/日志。"""
    bus, comps = _make_bus()
    loop = asyncio.get_running_loop()
    state: Dict[str, Any] = {"mode": mode, "kind": kind}
    _HOOK_CALLS.clear()
    reports: List[str] = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(
        lambda _l, ctx: reports.append(
            f"{ctx.get('message', '')!r}::{ctx.get('exception')!r}"))
    saved = loop.get_task_factory()
    me = asyncio.current_task()
    before = set(asyncio.all_tasks())
    loop.set_task_factory(_decoy_factory(mode, kind, state))
    outcomes: List[Tuple[str, Optional[BaseException]]] = []
    closing_after_1 = None
    field_after_1 = None
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            for index in range(closers):
                outcomes.append(await _close_once(bus))
                if index == 0:
                    closing_after_1 = bus._closing
                    field_after_1 = bus._close_future
                if index + 1 < closers:
                    await _settle()
            await _settle(8)
        finally:
            loop.set_task_factory(saved)
            await _settle()
            gc.collect()
            await asyncio.sleep(0)
            gc.collect()
    loop.set_exception_handler(previous_handler)
    orphans = [t for t in asyncio.all_tasks() if t is not me and t not in before]
    never = [str(w.message) for w in caught
             if "never awaited" in str(w.message) or "never retrieved" in str(w.message)]
    return _DecoyRecord(
        bus=bus, comps=comps, state=state, outcomes=outcomes,
        closing_after_1=closing_after_1, field_after_1=field_after_1,
        orphans=orphans, reports=reports, never=never,
        hooks=list(_HOOK_CALLS),
        closes=[c.closes for c in comps],
    )


# --------------------------------------------------------------------------
# ① 判据：身份（不问对象自己）
# --------------------------------------------------------------------------
class Test109R15CriterionIsIdentityOnly(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _HOOK_CALLS.clear()

    async def test_is_factory_decoy_is_total_for_every_hostile_field(self):
        """总函数：``None`` / 自身 / 非 Future / ``__hash__=None`` / 会抛的 hash+eq / 谎报
        ``__class__`` —— 每一种都必须返回 ``bool``，一个异常都不许抛。"""
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        fields = [None, future, loop.create_future(), 0, "x", object(),
                  _EvilFuture(), _HashRaises(), _EqRaises(collide_with=future),
                  _ClassSpoof()]
        for field in fields:
            got = _is_factory_decoy(field, future)
            self.assertIsInstance(got, bool, f"字段 {field!r} 上判据必须返回 bool")
        self.assertFalse(_is_factory_decoy(None, future), "None 是「允许重试」的正常状态")
        self.assertFalse(_is_factory_decoy(future, future), "本轮真 future 不是诱饵")
        self.assertTrue(_is_factory_decoy(loop.create_future(), future))

    async def test_criterion_never_calls_the_field_hooks(self):
        """判据**一次都不许**调用字段的 ``__hash__``/``__eq__``（r12/B 的同一口径）。"""
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        for field in (_EvilFuture(), _HashRaises(), _EqRaises(collide_with=future),
                      _ClassSpoof()):
            _is_factory_decoy(field, future)
        self.assertEqual(
            _HOOK_CALLS, [],
            "判据问了对象自己（r14 的 WeakSet 成员判定就是这条通路：hash 先抛 / 撞桶再 eq）")

    async def test_an_accepted_credential_whose_hash_raises_is_still_recognised(self):
        """受理凭据按**身份**被认出：即使它的 ``__hash__`` 会抛，判据也不碰它。"""
        loop = asyncio.get_running_loop()
        credential = _HashRaises()
        _accept_close_future(credential)          # 登记（留档那一步会 hash 一次，见模块注释）
        _HOOK_CALLS.clear()
        self.assertFalse(
            _is_factory_decoy(credential, loop.create_future()),
            "身份索引里在册 ⇒ 不是诱饵（不许覆盖更新那一轮的凭据）")
        self.assertEqual(_HOOK_CALLS, [], "判据不许对在册凭据求 hash/eq")
        ref = _ACCEPTED_CLOSE_FUTURE_REFS.get(id(credential))
        self.assertIsNotNone(ref)
        self.assertIs(ref(), credential, "索引是 id → 弱引用，取值要复核身份")

    async def test_identity_index_is_keyed_by_id_not_by_the_object(self):
        """索引键是 ``id()``（int）：按对象查会走它的 ``__hash__``，按 id 查不会。"""
        loop = asyncio.get_running_loop()
        credential = loop.create_future()
        _accept_close_future(credential)
        self.assertIn(id(credential), _ACCEPTED_CLOSE_FUTURE_REFS)
        self.assertIs(_ACCEPTED_CLOSE_FUTURE_REFS[id(credential)](), credential)

    async def test_dead_entries_are_pruned_at_the_next_registration(self):
        """弱引用自清 + 登记时顺手清死条目：索引上界 = 活着的受理 + 尚未清的死条目。"""
        loop = asyncio.get_running_loop()
        dead = loop.create_future()
        _accept_close_future(dead)
        key = id(dead)
        del dead
        gc.collect()
        self.assertIsNone(_ACCEPTED_CLOSE_FUTURE_REFS[key](), "弱引用已随凭据失效")
        _accept_close_future(loop.create_future())    # 登记时顺手清理
        self.assertNotIn(key, _ACCEPTED_CLOSE_FUTURE_REFS)


# --------------------------------------------------------------------------
# ② 形状矩阵：每一种诱饵都必须**具名 + 有界**，且不调用字段钩子
# --------------------------------------------------------------------------
class Test109R15DecoyShapesAreNamedAndBounded(unittest.IsolatedAsyncioTestCase):
    async def test_evil_future_decoy_in_the_spawn_window_is_named_and_bounded(self):
        """N9 本体（spawn 窗口换 ``__hash__ = None`` 的诱饵）：两次 ``close()`` 都有界具名。"""
        rec = await _run_decoy("spawnwin", "evil_hash_none")
        (out1, exc1), (out2, exc2) = rec.outcomes
        self.assertEqual(
            out1, "raised",
            "r14 在这里 HANG/TIMEOUT（done 回调里的 TypeError 被 asyncio 吞掉 ⇒ 不钉回、不结算）")
        self.assertEqual(_named_kind(exc1), TEARDOWN_NEVER_ENTERED_KIND)
        self.assertNotEqual(out2, "HANG/TIMEOUT", "第二次必须继承已落地的具名结局")
        self.assertEqual(_named_kind(exc2), TEARDOWN_NEVER_ENTERED_KIND)
        self.assertIs(rec.bus._close_future, rec.state["real"], "判定点必须钉回真 future")
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1],
                         "组件其实关过一次（保守误报，R-14a；但绝不静默成功）")
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertTrue(rec.closing_after_1, "受理不许被回滚")
        self.assertEqual(rec.reports, [], "不许有异常逃进事件循环异常处理器")
        self.assertEqual(rec.hooks, [], "判据不许调用字段的 __hash__/__eq__")
        self.assertEqual(rec.never, [])
        self.assertEqual(rec.orphans, [])

    async def test_evil_future_decoy_swapped_directly_at_the_spawn_window(self):
        """hashbind：工厂在 spawn 窗口**同步**换掉字段 ⇒ 受理点钉回 ⇒ 不许回滚、只关一次。"""
        rec = await _run_decoy("direct", "evil_hash_none")
        (out1, exc1), (out2, exc2) = rec.outcomes
        self.assertEqual(
            out1, "returned",
            "r14 在这里 raised(teardown-spawn-failed) + 整体回滚（组件已关 ⇒ 二次 close 双关）")
        self.assertIsNone(exc1)
        self.assertEqual((out2, exc2), ("returned", None))
        self.assertTrue(rec.closing_after_1, "受理一旦发生就绝不回滚（回滚判据的唯一入口见下）")
        self.assertIs(rec.bus._close_future, rec.state["real"])
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1], "五个组件恰好各关一次（无孤儿 teardown）")
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertEqual(rec.reports, [])
        self.assertEqual(rec.hooks, [])
        self.assertEqual(rec.never, [])
        self.assertEqual(rec.orphans, [])

    async def test_raising_hash_decoy_is_named_and_bounded_without_hashing(self):
        """``__hash__`` 会抛的诱饵：判据不许碰它（r14 实测 hooks=1 + 挂起）。"""
        rec = await _run_decoy("spawnwin", "hash_raises")
        (out1, exc1), _ = rec.outcomes
        self.assertEqual(out1, "raised")
        self.assertEqual(_named_kind(exc1), TEARDOWN_NEVER_ENTERED_KIND)
        self.assertIs(rec.bus._close_future, rec.state["real"])
        self.assertEqual(rec.hooks, [], "判据在身份索引上，不需要也不许问 __hash__")
        self.assertEqual(rec.reports, [])
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])

    async def test_raising_eq_decoy_is_named_and_bounded_without_comparing(self):
        """``__eq__`` 会抛、且哈希与真凭据**撞桶**的诱饵：判据一次都不许比较。"""
        rec = await _run_decoy("spawnwin", "eq_raises")
        (out1, exc1), _ = rec.outcomes
        self.assertEqual(out1, "raised")
        self.assertEqual(_named_kind(exc1), TEARDOWN_NEVER_ENTERED_KIND)
        self.assertIs(rec.bus._close_future, rec.state["real"])
        self.assertEqual(rec.hooks, [], "r14 实测 hash+eq 各一次（WeakSet 成员判定）")
        self.assertEqual(rec.reports, [])
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])

    async def test_class_spoofed_decoy_is_named_and_bounded(self):
        """谎报 ``__class__`` 的非 Future 诱饵：识别靠身份，镜像那步的 ``TypeError`` 被边界接住。"""
        rec = await _run_decoy("spawnwin", "class_spoof")
        (out1, exc1), _ = rec.outcomes
        self.assertEqual(out1, "raised")
        self.assertEqual(_named_kind(exc1), TEARDOWN_NEVER_ENTERED_KIND)
        self.assertIs(rec.bus._close_future, rec.state["real"], "钉回不受 __class__ 谎报影响")
        self.assertEqual(
            rec.reports, [],
            "镜像时 asyncio.Future.done(<非 Future>) 抛 TypeError ⇒ 必须被判定边界接住")
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])
        self.assertEqual(rec.never, [])

    async def test_non_future_decoy_is_named_and_bounded(self):
        """普通非 Future 对象当字段：同上（具名 + 有界 + 不逃进事件循环）。"""
        rec = await _run_decoy("spawnwin", "non_future")
        (out1, exc1), _ = rec.outcomes
        self.assertEqual(out1, "raised")
        self.assertEqual(_named_kind(exc1), TEARDOWN_NEVER_ENTERED_KIND)
        self.assertIs(rec.bus._close_future, rec.state["real"])
        self.assertEqual(rec.reports, [])
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])

    async def test_none_field_is_the_retry_state_not_a_decoy(self):
        """对照：``None`` **不是**诱饵（失败路径把凭据清成 ``None`` 正是「允许重试」）。"""
        rec = await _run_decoy("spawnwin", "none")
        (out1, exc1), _ = rec.outcomes
        self.assertEqual(out1, "raised")
        self.assertEqual(_named_kind(exc1), TEARDOWN_NEVER_ENTERED_KIND)
        self.assertIsNone(rec.bus._close_future, "None 不许被从「允许重试」复活成真 future")
        self.assertEqual(rec.reports, [])
        self.assertEqual(rec.never, [])

    async def test_decoy_reseated_after_the_acceptance_point_is_still_repinned(self):
        """判定点（不是受理点）也必须钉回：``reseat`` 形状 + ``_EvilFuture``。"""
        rec = await _run_decoy("reseat", "evil_hash_none")
        (out1, exc1), _ = rec.outcomes
        self.assertEqual(out1, "raised")
        self.assertEqual(_named_kind(exc1), TEARDOWN_NEVER_ENTERED_KIND)
        self.assertIs(rec.bus._close_future, rec.state["real"],
                      "判定点把受理后换上的诱饵钉回受理时捕获的真 future")
        self.assertEqual(
            rec.never, [],
            "r14 在这里还留下 'coroutine ... was never awaited'（判定在收尾前就炸了）")
        self.assertEqual(rec.reports, [])
        self.assertEqual(rec.never, [])


# --------------------------------------------------------------------------
# ③ 判定路径的异常边界：钩子异常既不吞判决、也不逃进事件循环
# --------------------------------------------------------------------------
class Test109R15WatchdogBoundary(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _HOOK_CALLS.clear()

    async def test_mirror_step_failure_does_not_escape_the_done_callback(self):
        """镜像步骤对非 Future 诱饵会抛 ``TypeError`` ⇒ 边界接住：判决落地、事件循环干净。"""
        rec = await _run_decoy("reseat", "class_spoof")
        (out1, exc1), (out2, exc2) = rec.outcomes
        self.assertEqual(_named_kind(exc1), TEARDOWN_NEVER_ENTERED_KIND)
        self.assertEqual(_named_kind(exc2), TEARDOWN_NEVER_ENTERED_KIND)
        self.assertIs(rec.bus._close_future, rec.state["real"], "钉回先于镜像，仍要生效")
        self.assertEqual(
            rec.reports, [],
            "去掉判定边界后这里的 TypeError 会逃出 _watch_round，被 asyncio 记进异常处理器")
        self.assertEqual(rec.never, [])

    async def test_emergency_settle_lands_the_verdict_even_if_the_judge_body_raises(self):
        """直接钉住兜底：判定本体抛异常时，闭包里的真 future **绝不许**留成悬置。"""
        bus, comps = _make_bus()
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        round_ = redis_module._TeardownRound()

        def boom(*args, **kwargs):
            raise RuntimeError("judge-body-boom")

        with mock.patch.object(RedisEventBus, "_judge_round", boom):
            bus._watch_round(object(), round_, future, [False])   # 不许抛
        self.assertTrue(asyncio.Future.done(future),
                        "判定路径出异常也必须把真 future 具名结算（否则 done 回调里就是挂起）")
        exc = asyncio.Future.exception(future)
        self.assertIsInstance(exc, RuntimeError)
        self.assertIn(TEARDOWN_NEVER_ENTERED_KIND, str(exc))
        self.assertTrue(round_.never_entered)
        self.assertEqual(bus._teardown_error_kind, TEARDOWN_NEVER_ENTERED_KIND)

    async def test_late_old_round_judgment_never_clobbers_the_retry_credential(self):
        """失败重试：第 1 轮晚到的判定**不许**覆盖第 2 轮（重试轮）落盘的凭据。"""
        bus, comps = _make_bus()
        loop = asyncio.get_running_loop()
        state: Dict[str, Any] = {}
        seen = {"rounds": 0}

        def factory(loop, coro, **kwargs):
            if not _is_our_entry(coro):
                return _new_task(coro, loop, kwargs)
            seen["rounds"] += 1
            if seen["rounds"] == 1:
                bus_ = coro.cr_frame.f_locals["self"]
                state["real1"] = bus_._close_future
                gate = loop.create_future()
                state["gate"] = gate

                def clear():
                    bus_._close_future = None      # 工厂把凭据清成 None ⇒ 允许重试

                loop.call_soon(clear)

                async def other():
                    await gate
                    return "not-teardown"

                return _new_task(other(), loop, kwargs)
            return _new_task(coro, loop, kwargs)

        saved = loop.get_task_factory()
        loop.set_task_factory(factory)
        outcome1: Any = None
        outcome2: Any = None
        try:
            first = asyncio.create_task(bus.close())          # 第 1 轮（真入口不跑）
            await _settle()
            self.assertIsNone(bus._close_future, "工厂清字段后应处于「允许重试」")
            second = asyncio.create_task(bus.close())         # 第 2 轮（真 teardown）
            await _settle()
            real2 = bus._close_future
            self.assertIsNotNone(real2)
            self.assertIsNot(real2, state["real1"], "第 2 轮是**新**凭据")
            state["gate"].set_result(None)                    # 第 1 轮的 Task 结束 ⇒ 晚到判定
            await _settle(8)
            self.assertIs(
                bus._close_future, real2,
                "晚到的旧一轮判定把重试轮的凭据钉回了自己那一轮（身份索引必须挡住这条）")
            got = await asyncio.wait_for(
                asyncio.gather(first, second, return_exceptions=True), WATCHDOG)
            outcome1, outcome2 = got[0], got[1]
        finally:
            loop.set_task_factory(saved)
            await _settle()
        self.assertIsInstance(outcome1, RuntimeError,
                              "第 1 轮必须拿到具名结局，而不是悬置")
        self.assertEqual(_named_kind(outcome1), TEARDOWN_NEVER_ENTERED_KIND)
        self.assertIsNone(outcome2, "第 2 轮（重试轮）正常关停")
        self.assertEqual(seen["rounds"], 2)

    async def test_second_close_after_a_boundary_catch_is_bounded(self):
        """边界接住镜像异常之后，第二次 ``close()`` 仍然有界（不许退化成挂起）。"""
        rec = await _run_decoy("spawnwin", "non_future")
        outcomes = rec.outcomes
        self.assertEqual(len(outcomes), 2)
        self.assertNotEqual(outcomes[1][0], "HANG/TIMEOUT")
        self.assertEqual(_named_kind(outcomes[1][1]), TEARDOWN_NEVER_ENTERED_KIND)


# --------------------------------------------------------------------------
# ④ 回滚判据：只在「teardown 从未被任何 Task 托管」时回滚
# --------------------------------------------------------------------------
class Test109R15RollbackCriterion(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _HOOK_CALLS.clear()

    async def test_a_raising_criterion_hook_does_not_roll_back_the_accepted_round(self):
        """受理点上的判据钩子抛异常 ⇒ 只留痕（判决由看门狗兜底），**绝不**回滚。"""
        def boom(*args, **kwargs):
            _HOOK_CALLS.append("criterion")
            raise RuntimeError("criterion-hook-boom")

        with mock.patch("plaita.event.redis._is_factory_decoy", boom):
            rec = await _run_decoy("direct", "plain_pending", closers=1)
        (outcome, exc), = rec.outcomes
        self.assertNotEqual(outcome, "HANG/TIMEOUT")
        self.assertEqual(
            outcome, "raised",
            "判据在判定点炸了也要有具名结局（兜底结算），不许变成挂起")
        self.assertEqual(_named_kind(exc), TEARDOWN_NEVER_ENTERED_KIND)
        self.assertTrue(
            rec.closing_after_1,
            "受理一旦发生就不许回滚（r14 hashbind：异常从受理点逃出 ⇒ 冻结体复位 _closing，"
            "组件其实已经关了）")
        self.assertIsNotNone(rec.field_after_1, "不许被回滚成 None（那会让重试再关一遍组件）")
        self.assertEqual(rec.bus._teardown_runs, 1)
        self.assertEqual(rec.closes, [1, 1, 1, 1, 1])

    async def test_bind_round_never_raises_after_acceptance(self):
        """结构性：``_bind_round`` 的可执行体里，登记/钉回/挂看门狗各自都有异常边界。"""
        src = inspect.getsource(RedisEventBus._bind_round)
        body = re.sub(r'"""..*?"""', "", src, count=1, flags=re.S)
        self.assertNotIn("raise RuntimeError", body,
                         "受理之后不许再有 raise（那会穿过冻结体变成整体回滚）")
        self.assertGreaterEqual(
            body.count("except BaseException"), 3,
            "登记 / 钉回 / 读完成状态 都必须各自留痕（P0-2 门禁不允许静默吞咽）")

    async def test_decoy_shapes_leave_no_orphan_task_or_unawaited_coroutine(self):
        """直接换字段 + 受理后换诱饵：无孤儿 Task、无 never-awaited/never-retrieved。"""
        for mode, kind in (("direct", "evil_hash_none"), ("reseat", "evil_hash_none")):
            with self.subTest(mode=mode, kind=kind):
                rec = await _run_decoy(mode, kind)
                self.assertEqual(rec.orphans, [], "不许留下无人取用的孤儿 teardown Task")
                self.assertEqual(rec.never, [], "不许留下 never awaited / never retrieved")
                self.assertEqual(rec.reports, [], "不许有异常逃进事件循环异常处理器")

    async def test_frozen_request_close_body_is_still_byte_identical(self):
        """docs 的「冻结体」声称有测试钉住：``request_close`` 去 docstring 后 2770 B / 47 L。"""
        lines = pathlib.Path(redis_module.__file__).read_text().splitlines(True)
        start = next(k for k, line in enumerate(lines)
                     if re.match(r"\s*def request_close\(", line))
        end = next((j for j in range(start + 1, len(lines))
                    if lines[j].strip()
                    and len(lines[j]) - len(lines[j].lstrip()) <= 4
                    and re.match(r"\s*(def |async def |@)", lines[j])), len(lines))
        out: List[str] = []
        k = start
        while k < end:
            if lines[k].strip().startswith('"""'):
                k += 1
                while k < end and '"""' not in lines[k]:
                    k += 1
                k += 1
            else:
                out.append(lines[k])
                k += 1
        body = "".join(out)
        self.assertEqual(len(body.encode()), 2770)
        self.assertEqual(len(body.splitlines()), 47)
        self.assertEqual(
            hashlib.sha256(body.encode()).hexdigest(),
            "8b38c2bd044c563d4ea83facd4cab2ae62c9181c0309acf6c6cff2b2e83dbe7a",
            "request_close 的可执行体是冻结的（11 个 revision 逐字节相同）")


# --------------------------------------------------------------------------
# ⑤ 完成路径不许读 loop 实例的属性（r14 验收者的必修项 A）
# --------------------------------------------------------------------------
def _loop_shadow_factory(calls: List[str], hostile: bool):
    """工厂把 ``loop.create_future`` 换成影子（记录调用者 / 返回永不落地的 awaitable）。

    ``loop`` 正是 task factory 的**第一个实参** ⇒ 这是工厂可写面的一部分。
    """

    def factory(loop, coro, **kwargs):
        if not _is_our_entry(coro):
            return _new_task(coro, loop, kwargs)
        original = loop.create_future
        pending = original()          # 装影子前创建的真 future（避免影子递归）

        def shadow():
            calls.append(traceback.extract_stack()[-2].filename)

            async def never_settles():
                await pending

            return never_settles() if hostile else original()

        loop.create_future = shadow
        return _new_task(coro, loop, kwargs)

    return factory


async def _close_with_timer(bus, bound: float = WATCHDOG):
    """有界 ``close()``：用 ``loop.call_later`` 计时（**不用** ``wait_for``）。

    ``asyncio.wait_for`` 自己会 ``loop.create_future()``，在这个「loop 被影子」的用例里会污染
    计数（甚至被影子打坏），所以这里用定时器 + ``task.cancel()`` 收场。
    """
    loop = asyncio.get_running_loop()
    task = asyncio.ensure_future(bus.close())
    fired: List[bool] = []
    handle = loop.call_later(bound, lambda: (fired.append(True), task.cancel()))
    try:
        await task
        return ("HANG/TIMEOUT", None) if fired else ("returned", None)
    except asyncio.CancelledError:
        return ("HANG/TIMEOUT", None) if fired else ("raised", asyncio.CancelledError())
    except BaseException as exc:                  # noqa: BLE001 - 断言用
        return "raised", exc
    finally:
        handle.cancel()


class Test109R15CompletionPathIsLoopAttributeFree(unittest.IsolatedAsyncioTestCase):
    """必修项 A：``_await_close_future`` 的 waiter 一旦从 loop 实例上取，就不再是有界的。"""

    async def _run(self, hostile: bool):
        bus, comps = _make_bus()
        loop = asyncio.get_running_loop()
        calls: List[str] = []
        saved_factory = loop.get_task_factory()
        saved_create_future = loop.create_future
        loop.set_task_factory(_loop_shadow_factory(calls, hostile))
        try:
            outcomes = [await _close_with_timer(bus), await _close_with_timer(bus)]
        finally:
            loop.set_task_factory(saved_factory)
            loop.create_future = saved_create_future
            await _settle()
        engine_calls = [path for path in calls if "plaita/event/redis.py" in path]
        return bus, comps, engine_calls, outcomes

    async def test_completion_path_never_reads_the_loop_instance(self):
        """计数影子确认：``close()`` / waiter 这条路上**一次都不**碰 loop 的 ``create_future``。"""
        bus, comps, engine_calls, outcomes = await self._run(hostile=False)
        self.assertEqual(
            engine_calls, [],
            "r14 的 `asyncio.get_running_loop().create_future()` 会在这里留下 1 次"
            "（调用者落在 plaita/event/redis.py；loop 是 task factory 的第一个实参 ⇒ 工厂可写）")
        self.assertEqual(outcomes[0], ("returned", None))
        self.assertEqual(outcomes[1], ("returned", None))
        self.assertEqual([c.closes for c in comps], [1, 1, 1, 1, 1])
        self.assertEqual(bus._teardown_runs, 1)

    async def test_shadowed_loop_create_future_cannot_unbound_the_completion_path(self):
        """影子换成「永不落地的 awaitable」也必须有界：waiter 根本不走 loop 属性。"""
        bus, comps, engine_calls, outcomes = await self._run(hostile=True)
        self.assertEqual(engine_calls, [], "waiter 必须由 `asyncio.Future()` 类调用创建")
        self.assertEqual(
            outcomes[0], ("returned", None),
            "r14 上这里不返回（影子 waiter 永不落地 ⇒ 只能靠测试定时器收场）")
        self.assertEqual(outcomes[1], ("returned", None))
        self.assertEqual([c.closes for c in comps], [1, 1, 1, 1, 1])

    async def test_waiter_is_created_by_a_class_call_not_a_loop_attribute(self):
        """结构性：waiter 必须是 ``asyncio.Future()``（类调用），不许走 loop 实例属性。"""
        src = re.sub(r'"""..*?"""', "", inspect.getsource(redis_module._await_close_future),
                     count=1, flags=re.S)
        self.assertIn("waiter = asyncio.Future()", src)
        self.assertNotIn(
            "get_running_loop().create_future()", src,
            "waiter 从 loop 实例上取 ⇒ 工厂（它的第一个实参就是 loop）能把它换成永不落地的"
            "awaitable ⇒ close() 不再有界（r14 验收者必修项 A）")
        self.assertIn("asyncio.Future.add_done_callback(future, _forward)", src)

    async def test_pending_dispatch_plus_loop_shadow_is_declared_unbounded(self):
        """**明示残差 R-15a（不声称有界）**：有在飞分发时，drain 那一步的 stdlib
        ``asyncio.wait`` 自己会 ``loop.create_future()``（3.10–3.13 实测调用 2 次）；影子返回
        **非 Future** 时，``_wait`` 的 ``_on_completion`` **回调**里抛 ``AttributeError``
        （被 asyncio 吞掉）⇒ ``asyncio.wait`` 永不返回 ⇒ ``close()`` 长等。

        这与 **R-13a 同族**（``loop`` 是 task factory 的实参、进程内对手可达；teardown 必须在
        环上跑，任何「在环上等待/定时」都要用 loop 的实例方法）——引擎侧无法用一个 ``except``
        兜住**回调**里的异常。本用例把「该形状挂起」钉住：谁把 R-15a 写成「已收口/有界」就会红。
        """
        bus, comps = _make_bus()
        loop = asyncio.get_running_loop()
        slow = asyncio.create_task(asyncio.sleep(0.05))
        bus._pending_tasks[slow] = ("e1", "h1")
        calls: List[str] = []
        saved_factory = loop.get_task_factory()
        saved_create_future = loop.create_future
        loop.set_task_factory(_loop_shadow_factory(calls, hostile=True))
        try:
            outcome, exc = await _close_with_timer(bus, 1.0)
        finally:
            loop.set_task_factory(saved_factory)
            loop.create_future = saved_create_future
            asyncio.Task.cancel(slow)
            if bus._teardown_task is not None:
                asyncio.Task.cancel(bus._teardown_task)
            await _settle()
        self.assertEqual(
            outcome, "HANG/TIMEOUT",
            "R-15a（明示、不声称有界）：影子让 stdlib asyncio.wait 的回调炸掉 ⇒ drain 长等")
        self.assertEqual(
            [c.closes for c in comps], [0, 0, 0, 0, 0],
            "长等发生在 drain（第 ① 步）⇒ 组件释放还没开始（如实记录，不粉饰）")


# --------------------------------------------------------------------------
# ⑥ 留档与索引卫生
# --------------------------------------------------------------------------
class Test109R15RegistryHygiene(unittest.IsolatedAsyncioTestCase):
    async def test_decoy_never_enters_either_the_index_or_the_bookkeeping_set(self):
        rec = await _run_decoy("direct", "evil_hash_none", closers=1)
        self.assertEqual(rec.outcomes[0][0], "returned")
        decoy = rec.state["decoy"]
        self.assertNotIn(
            id(decoy), _ACCEPTED_CLOSE_FUTURE_REFS,
            "诱饵不许进身份索引（进去了判定点就不敢钉回）")
        self.assertNotIn(
            id(decoy), {id(item) for item in _ACCEPTED_CLOSE_FUTURES},
            "诱饵不许进留档弱集合（注意：这条断言自己也只能按 id 查——"
            "``decoy in _ACCEPTED_CLOSE_FUTURES`` 会立刻踩上 N9 的同一个 hash 陷阱）")
        real = rec.state["real"]
        self.assertIn(id(real), _ACCEPTED_CLOSE_FUTURE_REFS)
        self.assertIsInstance(_ACCEPTED_CLOSE_FUTURE_REFS[id(real)], weakref.ref,
                              "索引值是弱引用（凭据被回收即自动失效，不续命、不需清理）")
        before = sys.getrefcount(real)
        _accept_close_future(real)                # 再登记一次也不许多出一份强引用
        self.assertEqual(sys.getrefcount(real), before,
                         "登记只持弱引用：索引不给凭据续命")

    async def test_registry_is_bounded_after_hostile_shapes(self):
        """跑完一串敌对形状后：索引里不留**新**的活条目（弱引用自清），登记表不涨。"""
        before = len(redis_module._TEARDOWN_ROUNDS)
        alive_before = {key for key, ref in _ACCEPTED_CLOSE_FUTURE_REFS.items()
                        if ref() is not None}
        for mode, kind in (("spawnwin", "evil_hash_none"), ("direct", "hash_raises"),
                           ("reseat", "eq_raises")):
            await _run_decoy(mode, kind)
        await _settle()
        gc.collect()
        alive_after = {key for key, ref in _ACCEPTED_CLOSE_FUTURE_REFS.items()
                       if ref() is not None}
        self.assertLessEqual(
            alive_after, alive_before,
            "本轮跑出的凭据必须被回收（索引只持弱引用，不给凭据续命）")
        self.assertEqual(len(redis_module._TEARDOWN_ROUNDS), before,
                         "受理结束后登记表必须回到原样")
