"""#109 r6：自关停围栏换架构（dispatch lineage 主判据 + fail-closed 唤醒锥副判据）。

r5（「完成唤醒边遍历」）被独立验收者以决定性反例判负，系统性结论是：判据仍由
**私有属性名白名单**定义，缺名即静默跳过 ⇒ 按构造 fail-open。r6 不再往白名单里
加形状，改成两层：

L1 **dispatch lineage**（``_self_close_refusal`` 前半段）——分发 handler 运行时置
   ``contextvars.ContextVar`` 标记，``create_task`` 复制 context ⇒ handler 内/其后代
   任务内的 close 被具名拒绝；**不点名任何私有属性**。显式换 context 会绕过 L1。
L2 **fail-closed 完成唤醒锥**——缺属性 / 重命名 / ``TaskGroup._on_completed_fut``
   懒创建（None 而 ``_tasks`` 非空）/ 未知持有者 / 深度或节点预算耗尽**一律具名
   拒绝**（``error_kind=wake-graph-incomplete``），绝不退化成 clean。
   首次调用先跑 stdlib 属性自检，未过则本进程 close() 全拒。

本文件钉住（每条都断言**具名拒绝 ∧ 零关停副作用**：``_closing is False``、
listener 未取消、未进 ``_drain_pending_tasks``）：

  * 验收者的**决定性反例**：``TaskGroup`` + ``await asyncio.sleep(0)`` + 显式空
    context（``_on_completed_fut`` 懒创建窗口）；
  * 两级 TaskGroup、``asyncio.Barrier``（``_cond._waiters``）、
    ``StreamReader._waiter``、自定义 awaitable（``self._f``）、``as_completed``；
  * **属性缺失表**：逐个已知私有属性在实例上删掉 ⇒ 判据从 clean 变成具名 incomplete；
  * stdlib 自检失败 ⇒ close() 全拒（把「未来 Python 改名」从 fail-open 变 fail-closed）；
  * L1 的独立检验力（只有 marker 能拦下的 delayed-detached 形状）；
  * 预算边界**精确**（65536 节点放行 / 65537 拒绝）且合法 600/10000 层 await 链
    **不被误拒**（验收者证伪的 r5 声明）。

版本与诚实边界见 ``docs-site/docs/distributed/event-system.md``；
跨版本实测见 ``scripts/selfclose_109_110_versions.py``（3.10/3.11/3.12/3.13）。
"""

import asyncio
import contextvars
import os
import sys
import unittest
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis
import fakeredis.aioredis

import plaita.event._selfclose_guard as guard_mod
from plaita.event.core import Event, EventSubscription
from plaita.event.redis import (
    RedisEventBus,
    RedisEventSubscriptionStorage,
    _DISPATCH_GRAPH_MAX_NODES,
    _SELF_CLOSE_BUDGET_REFUSAL,
    _SELF_CLOSE_REFUSAL,
    _SELF_CLOSE_UNSUPPORTED_REFUSAL,
    _WAKE_GRAPH_INCOMPLETE_REFUSAL,
    _close_graph_verdict,
)

_SERVER = fakeredis.FakeServer()
_SELF_CLOSE_MARK = "不能从它自己的分发 handler 内调用"
_INCOMPLETE_MARK = "error_kind=wake-graph-incomplete"
_REAL_REDIS_URL = os.environ.get("PLAITA_TEST_REDIS_URL")


async def _fake_from_url(url):
    return fakeredis.aioredis.FakeRedis(server=_SERVER, decode_responses=False)


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
    while isinstance(err, BaseExceptionGroup) and err.exceptions:
        err = err.exceptions[0]
    return err


def _assert_named_refusal(case, err):
    assert isinstance(err, RuntimeError), f"{case}: 非具名拒绝: {err!r}"
    text = str(err)
    assert (_SELF_CLOSE_MARK in text) or (_INCOMPLETE_MARK in text), \
        f"{case}: 拒绝文案未点名判据: {text}"


class Test109R6VerifierCounterexamples(unittest.IsolatedAsyncioTestCase):
    """验收者反例逐个变成「具名拒绝 + 零副作用」，且不再让 asyncio.run 悬置。"""

    async def _run_shape(self, kind):
        holder, observed = {}, {}

        async def handler(event):                 # D = 在飞分发任务
            ctx = contextvars.Context()           # 显式空 context：L1 失灵，考 L2
            loop = asyncio.get_running_loop()
            bus = holder["bus"]
            refusal = []
            task = None
            try:
                if kind == "taskgroup_sleep0":
                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(bus.close(), context=ctx)
                        await asyncio.sleep(0)    # <<< 验收者唯一新增的一行
                elif kind == "two_level_taskgroup":
                    async with asyncio.TaskGroup() as g2:
                        async with asyncio.TaskGroup() as g1:
                            g1.create_task(bus.close(), context=ctx)
                            await asyncio.sleep(0)
                        await asyncio.sleep(0)
                elif kind == "barrier":
                    barrier = asyncio.Barrier(2)
                    task = loop.create_task(bus.close(), context=ctx)
                    task.add_done_callback(
                        lambda t: asyncio.ensure_future(barrier.abort()))
                    try:
                        await barrier.wait()
                    except BaseException:         # noqa: BLE001
                        pass
                elif kind == "streamreader":
                    reader = asyncio.StreamReader(limit=2 ** 16)
                    task = loop.create_task(bus.close(), context=ctx)
                    task.add_done_callback(lambda t: reader.feed_eof())
                    await reader.read(1)
                elif kind == "custom_awaitable":
                    gate = _Gate()
                    task = loop.create_task(bus.close(), context=ctx)
                    task.add_done_callback(lambda t: gate._f.set_result(None))
                    await gate
                elif kind == "as_completed":
                    task = loop.create_task(bus.close(), context=ctx)
                    for fut in asyncio.as_completed([task]):
                        await fut
                else:
                    raise AssertionError(kind)
            except BaseException as e:            # noqa: BLE001
                refusal.append(e)
            if task is not None and task.done() and not task.cancelled() \
                    and task.exception() is not None:
                refusal.append(task.exception())   # fire-and-forget 的 close 任务
            observed["refusal"] = refusal
            observed["closing"] = bus._closing
            observed["pending"] = len(bus._pending_tasks)
            observed["listener_done"] = holder["listener"].done()
            observed["drain"] = list(holder["drain_calls"])

        async def fake_drain():
            holder["drain_calls"].append("drain")

        holder["drain_calls"] = []
        bus = _make_bus(handler)
        holder["bus"] = bus
        listener = asyncio.ensure_future(asyncio.Event().wait())
        holder["listener"] = listener
        bus.listeners.append(listener)
        with patch.object(bus, "_drain_pending_tasks", new=fake_drain):
            await bus._process_message(
                _msg(Event(event_type="t.demo", data={})), "h1")
            for _ in range(400):
                if observed.get("refusal") and not bus._pending_tasks:
                    break
                await asyncio.sleep(0.01)
            listener.cancel()
        return bus, observed

    async def _assert_shape_refused(self, kind):
        bus, observed = await self._run_shape(kind)
        refusal = observed.get("refusal", [])
        assert refusal, f"{kind}: close() 未被拒绝（放行 = 验收者反例复现）"
        err = _inner(refusal[0])
        _assert_named_refusal(f"r6/{kind}", err)
        assert observed["closing"] is False, f"{kind}: 拒绝前已置 _closing"
        assert observed["drain"] == [], f"{kind}: 拒绝前已进 drain"
        assert observed["listener_done"] is False, f"{kind}: 拒绝前已取消 listener"
        assert observed["pending"] == 1, \
            f"{kind}: 拒绝时不该动 _pending_tasks（应仍持有在飞任务 D）"
        assert bus._closing is False
        return err

    async def test_taskgroup_sleep0_context_swap_is_named_refusal(self):
        """验收者决定性反例：TaskGroup + await sleep(0)（懒创建窗口）。"""
        err = await self._assert_shape_refused("taskgroup_sleep0")
        assert _INCOMPLETE_MARK in str(err), \
            f"该反例应由「懒创建窗口」fail-closed 拦下: {err}"

    async def test_two_level_taskgroup_is_named_refusal(self):
        await self._assert_shape_refused("two_level_taskgroup")

    async def test_barrier_wake_is_named_refusal(self):
        err = await self._assert_shape_refused("barrier")
        assert _SELF_CLOSE_MARK in str(err), \
            f"Barrier._cond._waiters 应能证明出在飞分发任务: {err}"

    async def test_streamreader_waiter_wake_is_named_refusal(self):
        err = await self._assert_shape_refused("streamreader")
        assert _SELF_CLOSE_MARK in str(err), \
            f"StreamReader._waiter（单数）应能证明出在飞分发任务: {err}"

    async def test_custom_awaitable_instance_attr_is_named_refusal(self):
        """自定义 awaitable 把 future 藏在 ``self._f``：未知持有者 ⇒ fail-closed。"""
        err = await self._assert_shape_refused("custom_awaitable")
        assert _INCOMPLETE_MARK in str(err), f"未知持有者应 fail-closed: {err}"

    async def test_as_completed_pinned_on_running_cpython(self):
        """``as_completed`` 形状：本仓库运行时 3.11/3.12 由 Queue 中介边证明出 in_graph；
        3.13 的 ``_AsCompletedIterator``（未知持有者持 ``_done`` Queue）由 fail-closed
        拦下（见 scripts/selfclose_109_110_versions.py 的跨版本实测）。两者都是具名拒绝。
        """
        err = await self._assert_shape_refused("as_completed")
        if sys.version_info >= (3, 13):
            assert _INCOMPLETE_MARK in str(err), \
                f"3.13 的 _AsCompletedIterator 应走未知持有者 fail-closed: {err}"
        else:
            assert _SELF_CLOSE_MARK in str(err), \
                f"3.11/3.12 的 as_completed 应经 Queue._getters 证明出图内: {err}"


class _Gate:
    """自定义 awaitable：future 藏在实例属性 ``self._f``（不扫 ``__dict__`` 就漏）。"""

    def __init__(self):
        self._f = asyncio.get_running_loop().create_future()

    def __await__(self):
        return self._f.__await__()


def _counterexample_via_asyncio_run():
    """在**全新事件循环**（asyncio.run）里跑验收者反例；r4/r5 会悬置不返回。"""
    async def main():
        holder, observed = {}, {}

        async def handler(event):
            ctx = contextvars.Context()
            bus = holder["bus"]
            refusal = []
            try:
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(bus.close(), context=ctx)
                    await asyncio.sleep(0)
            except BaseException as e:            # noqa: BLE001
                refusal.append(e)
            observed["refusal"] = refusal
            observed["closing"] = bus._closing
            observed["pending"] = len(bus._pending_tasks)
            observed["listener_done"] = holder["listener"].done()
            observed["drain"] = list(holder["drain_calls"])

        async def fake_drain():
            holder["drain_calls"].append("drain")

        holder["drain_calls"] = []
        bus = _make_bus(handler)
        holder["bus"] = bus
        listener = asyncio.ensure_future(asyncio.Event().wait())
        holder["listener"] = listener
        bus.listeners.append(listener)
        with patch.object(bus, "_drain_pending_tasks", new=fake_drain):
            await bus._process_message(
                _msg(Event(event_type="t.demo", data={})), "h1")
            for _ in range(400):
                if observed.get("refusal") and not bus._pending_tasks:
                    break
                await asyncio.sleep(0.01)
            listener.cancel()
        return observed

    return asyncio.run(main())


class Test109R6AsyncioRunTeardown(unittest.IsolatedAsyncioTestCase):
    async def test_counterexample_asyncio_run_returns(self):
        loop = asyncio.get_running_loop()
        try:
            observed = await asyncio.wait_for(
                loop.run_in_executor(None, _counterexample_via_asyncio_run),
                timeout=30)
        except asyncio.TimeoutError:
            self.fail("asyncio.run 未返回（r4/r5 的 teardown 悬置复现）")
        err = _inner(observed["refusal"][0])
        _assert_named_refusal("r6/asyncio.run", err)
        assert observed["closing"] is False
        assert observed["drain"] == []
        assert observed["listener_done"] is False


class Test109R6AttributeAbsence(unittest.IsolatedAsyncioTestCase):
    """验收者的属性缺失表：把每个已知私有属性删掉 ⇒ 判据必须是具名 incomplete。"""

    @staticmethod
    def _cone_verdict(obj):
        """造一个「完成回调闭包直接引用 obj」的唤醒锥，返回 verdict。"""
        loop = asyncio.get_running_loop()
        root = loop.create_future()

        def make_cb():
            held = obj                          # 真闭包 cell（不是默认参数）

            def cb(_fut):
                return held
            return cb

        root.add_done_callback(make_cb())
        verdict, visited, reason = _close_graph_verdict(root, {})
        root.cancel()
        return verdict, visited, reason

    def _cases(self):
        return [
            ("Queue._getters", asyncio.Queue(), "_getters"),
            ("Queue._putters", asyncio.Queue(), "_putters"),
            ("Event._waiters", asyncio.Event(), "_waiters"),
            ("Condition._waiters", asyncio.Condition(), "_waiters"),
            ("Semaphore._waiters", asyncio.Semaphore(), "_waiters"),
            ("Lock._waiters", asyncio.Lock(), "_waiters"),
            ("Barrier._cond", asyncio.Barrier(2), "_cond"),
            ("StreamReader._waiter", asyncio.StreamReader(limit=2 ** 16), "_waiter"),
            ("TaskGroup._tasks", asyncio.TaskGroup(), "_tasks"),
            ("TaskGroup._on_completed_fut", asyncio.TaskGroup(),
             "_on_completed_fut"),
        ]

    async def test_present_attributes_are_enumerated_clean(self):
        for label, obj, attr in self._cases():
            with self.subTest(label=label):
                assert attr in vars(obj), f"{label} 不在实例 __dict__"
                verdict, _v, _r = self._cone_verdict(obj)
                assert verdict == "clean", f"{label}: 未删属性时应可枚举: {verdict}"

    async def test_deleted_attributes_are_named_incomplete(self):
        for label, obj, attr in self._cases():
            with self.subTest(label=label):
                saved = vars(obj).pop(attr)
                try:
                    verdict, _v, reason = self._cone_verdict(obj)
                finally:
                    vars(obj)[attr] = saved
                assert verdict == "incomplete", \
                    f"{label}: 属性缺失必须 fail-closed，实际 {verdict}"
                assert reason, f"{label}: incomplete 必须带具名原因"
                assert _INCOMPLETE_MARK in _WAKE_GRAPH_INCOMPLETE_REFUSAL

    async def test_taskgroup_lazy_window_is_incomplete(self):
        """``_on_completed_fut is None`` 而 ``_tasks`` 非空 ⇒ incomplete（懒创建）。"""
        loop = asyncio.get_running_loop()
        tg = asyncio.TaskGroup()
        await tg.__aenter__()
        gate = loop.create_future()

        async def child():
            await gate

        task = tg.create_task(child())
        try:
            root = loop.create_future()

            def make_cb():
                held = tg

                def cb(_fut):
                    return held
                return cb

            root.add_done_callback(make_cb())
            verdict, _v, reason = _close_graph_verdict(root, {})
            root.cancel()
            assert verdict == "incomplete", verdict
            assert "懒创建" in reason, reason
        finally:
            gate.set_result(None)
            try:
                await tg.__aexit__(None, None, None)
            except BaseException:                 # noqa: BLE001
                pass
            if not task.done():
                task.cancel()

    async def test_guard_source_has_no_silent_getattr_default(self):
        """结构性钉住：判据模块的**可执行代码**不得再有 ``getattr(x, attr, None)``
        静默跳过（AST 口径，避免把 docstring 里引用的旧写法误判）。"""
        import ast
        path = guard_mod.__file__
        with open(path, encoding="utf-8") as fh:
            tree = ast.parse(fh.read(), filename=path)
        offenders = []
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                    and node.func.id == "getattr"):
                continue
            if len(node.args) >= 3 and isinstance(node.args[2], ast.Constant) \
                    and node.args[2].value is None:
                offenders.append(node.lineno)
        assert offenders == [], f"仍有静默跳过（getattr 默认 None）: 行 {offenders}"


class Test109R6SelfTestFailClosed(unittest.IsolatedAsyncioTestCase):
    """stdlib 自检失败 ⇒ close() 全拒（未来 Python 改名不再静默放行）。"""

    async def test_renamed_attribute_makes_close_refuse_everything(self):
        bus = _make_bus(lambda e: None)
        guard_mod._SELFTEST_EXTRA_ATTRS["Queue"] = ("_putters_renamed_away",)
        guard_mod.reset_self_test()
        try:
            with self.assertRaises(RuntimeError) as ctx:
                await bus.close()
        finally:
            guard_mod._SELFTEST_EXTRA_ATTRS.pop("Queue", None)
            guard_mod.reset_self_test()
        text = str(ctx.exception)
        assert _INCOMPLETE_MARK in text, text
        assert "自检未通过" in text, text
        assert "_putters_renamed_away" in text, text
        assert bus._closing is False, "自检拒绝必须在任何关停副作用之前"

    async def test_self_test_passes_on_repo_runtime(self):
        guard_mod.reset_self_test()
        status = guard_mod.ensure_self_test()
        assert status is None, f"本运行时的 asyncio 簿记属性自检未过: {status}"


class Test109R6LineageMarker(unittest.IsolatedAsyncioTestCase):
    """L1（dispatch lineage）的独立检验力 + 允许半边不回归。"""

    async def test_delayed_detached_close_refused_by_marker_only(self):
        """handler 里 create_task 出的 delayed 任务：分发已结束、无 await 边可走，
        只剩 marker 能拦（L2 从该任务出发的唤醒锥是空的 ⇒ clean）。"""
        holder = {}

        async def late():
            await asyncio.sleep(0.15)             # 此时分发早已结束（pending==0）
            try:
                await holder["bus"].close()
                holder["refusal"] = None
            except BaseException as e:            # noqa: BLE001
                holder["refusal"] = e

        async def handler(event):
            asyncio.create_task(late())           # 不 await：没有 L2 可见的完成边

        bus = _make_bus(handler)
        holder["bus"] = bus
        await bus._process_message(
            _msg(Event(event_type="t.demo", data={})), "h1")
        await asyncio.sleep(0.4)
        assert isinstance(holder.get("refusal"), RuntimeError), \
            f"L1 marker 未拦下 delayed detached close: {holder}"
        assert _SELF_CLOSE_MARK in str(holder["refusal"])
        assert bus._closing is False
        assert len(bus._pending_tasks) == 0

    async def test_external_close_still_allowed_after_marker_layer(self):
        bus = _make_bus(lambda e: None)
        await bus.close()
        assert bus._closing is True
        assert len(bus._pending_tasks) == 0


class Test109R6BudgetBoundary(unittest.IsolatedAsyncioTestCase):
    """预算 fail-closed 且边界**精确**、合法深链不误拒（r5 的 512 声明被证伪）。"""

    @staticmethod
    def _chain_verdict(loop, length, budget):
        root = loop.create_future()
        cur = root
        for _ in range(length):
            nxt = loop.create_future()
            cur.add_done_callback(nxt.set_result)
            cur = nxt
        verdict, visited, reason = _close_graph_verdict(root, {}, budget=budget)
        root.cancel()
        return verdict, visited, reason

    def test_small_budget_boundary_is_exact(self):
        loop = asyncio.new_event_loop()
        try:
            assert self._chain_verdict(loop, 9, 10)[:2] == ("clean", 10)
            assert self._chain_verdict(loop, 10, 10)[:2] == ("budget", 10)
        finally:
            loop.close()

    def test_documented_budget_boundary_65536(self):
        """文档口径：唤醒锥 65536 节点放行、65537 节点具名拒绝。"""
        loop = asyncio.new_event_loop()
        try:
            verdict, visited, _r = self._chain_verdict(
                loop, _DISPATCH_GRAPH_MAX_NODES - 1, _DISPATCH_GRAPH_MAX_NODES)
            assert (verdict, visited) == ("clean", _DISPATCH_GRAPH_MAX_NODES)
            verdict, visited, _r = self._chain_verdict(
                loop, _DISPATCH_GRAPH_MAX_NODES, _DISPATCH_GRAPH_MAX_NODES)
            assert (verdict, visited) == ("budget", _DISPATCH_GRAPH_MAX_NODES)
        finally:
            loop.close()

    async def test_legit_600_and_10000_deep_await_chain_not_refused(self):
        """验收者实测「512 预算下 ≥510 层合法 await 链被拒」——现在必须 clean。"""
        for depth in (600, 10000):
            with self.subTest(depth=depth):
                loop = asyncio.get_running_loop()
                gate = loop.create_future()

                async def leaf():
                    await gate
                    return _close_graph_verdict(asyncio.current_task(), {})

                async def worker(child):
                    return await child

                task = loop.create_task(leaf())
                chain = [task]
                for _ in range(depth):
                    task = loop.create_task(worker(task))
                    chain.append(task)
                for _ in range(depth + 2):
                    await asyncio.sleep(0)
                gate.set_result(None)
                verdict, visited, _r = await chain[-1]
                assert verdict == "clean", f"depth={depth}: {verdict}"
                assert visited >= depth, f"depth={depth}: 链路未展开 visited={visited}"


class _SlotGate:
    """声明残差 R1 的最小载体：future 藏在 ``__slots__`` 里（无 ``__dict__``）。"""

    __slots__ = ("f",)


class Test109R6HonestBoundary(unittest.IsolatedAsyncioTestCase):
    """文档声明的残差 R1：L2 的未知持有者只扫一层 ``__dict__``，slots 对象看不到。

    本用例钉住的是**声明过的边界**（当前判 clean 放行），不是 completeness 声明：
    若哪天改成拒绝，必须同步改这里的断言与文档的残差 R1 段。
    """

    async def test_slots_hidden_wake_edge_is_declared_residual_not_covered(self):
        loop = asyncio.get_running_loop()
        gate = _SlotGate()
        gate.f = loop.create_future()
        root = loop.create_future()

        def make_cb():
            held = gate                      # 闭包 cell 直接是 slots 对象

            def cb(_fut):
                return held
            return cb

        root.add_done_callback(make_cb())
        verdict, _visited, _reason = _close_graph_verdict(root, {})
        root.cancel()
        gate.f.cancel()
        assert verdict == "clean", (
            "若这里变成拒绝，说明 R1 已被覆盖——请同步更新文档的残差 R1 段与用例名")

    async def test_refusal_messages_state_capability_boundary(self):
        """要求：拒绝文案的 rationale 里要写明判据**不能**证明什么（不宣称完备）。"""
        for label, msg in (
                ("self-close", _SELF_CLOSE_REFUSAL),
                ("budget", _SELF_CLOSE_BUDGET_REFUSAL),
                ("incomplete", _WAKE_GRAPH_INCOMPLETE_REFUSAL),
                ("unsupported-runtime", _SELF_CLOSE_UNSUPPORTED_REFUSAL)):
            assert "不宣称完备" in msg, (label, msg)
            assert "best-effort" in msg, (label, msg)
            assert "R1" in msg, (label, msg)


@unittest.skipUnless(_REAL_REDIS_URL,
                     "需要 PLAITA_TEST_REDIS_URL 指向自起的真实 Redis（free port）")
class Test110RealRedisConjunction(unittest.IsolatedAsyncioTestCase):
    """#110 三后端合取语义 + 代价数字，在**真实 Redis**（自起）上复算。

    运行：
      PLAITA_TEST_REDIS_URL=redis://127.0.0.1:<free-port>/0 \\
        python -m pytest -q tests/unit/test_issue109_110_r6_parity.py::Test110RealRedisConjunction
    """

    async def test_two_condition_and_cost_reconciled_on_real_redis(self):
        import uuid
        prefix = f"r6real{uuid.uuid4().hex[:8]}:"
        storage = RedisEventSubscriptionStorage(_REAL_REDIS_URL, prefix)
        await storage.initialize()
        try:
            rows = (("sub-hit", "shared.type", "CID"),
                    ("sub-wrong-type", "other.type", "CID"),
                    ("sub-other-corr", "shared.type", "OTHER"))
            for sid, etype, corr in rows:
                await storage.store_subscription(EventSubscription(
                    event_type=etype, correlation_id=corr, subscription_id=sid))
            real_get = storage.redis.get
            gets = []

            async def counting_get(key):
                gets.append(str(key))
                return await real_get(key)

            storage.redis.get = counting_get

            async def probe(**kw):
                gets.clear()
                out = await storage.list_subscriptions(**kw)
                data = [k for k in gets if "data:" in k]
                return sorted(s.subscription_id for s in out), len(data)

            both_ids, both_gets = await probe(
                event_type="shared.type", correlation_id="CID")
            corr_ids, corr_gets = await probe(correlation_id="CID")
            type_ids, type_gets = await probe(event_type="shared.type")

            assert both_ids == ["sub-hit"], f"AND 语义失败: {both_ids}"
            assert corr_ids == ["sub-hit", "sub-wrong-type"], corr_ids
            assert type_ids == ["sub-hit", "sub-other-corr"], type_ids
            # 单条路径的 data: GET = correlation 候选数 = 2；三条路径累计 = 6
            assert (both_gets, corr_gets, type_gets) == (2, 2, 2), \
                (both_gets, corr_gets, type_gets)
            assert both_gets + corr_gets + type_gets == 6, "旧文档的 6 = 三路累计口径"

            ev = Event(event_type="shared.type", data={}, correlation_id="CID")
            matched = await storage.find_matching_subscriptions(ev, {})
            assert sorted(s.subscription_id for s in matched) == ["sub-hit"]
        finally:
            keys = [k async for k in storage.redis.scan_iter(match=f"{prefix}*")]
            if keys:
                await storage.redis.delete(*keys)
            await storage.close()


if __name__ == "__main__":
    unittest.main()
