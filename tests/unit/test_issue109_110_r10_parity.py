"""#109/#110 r10：task-factory「全有或全无」受理的**真值**判据（r9 判负项的回归面）。

r9 的规则是「spawn 失败 ⇒ 整体回滚」，但它把**spawn 抛异常**直接等同于「什么都没发生」。
工厂完全可以**先把 coroutine 变成 Task 再抛**（非 eager：``t = asyncio.Task(coro); raise``；
eager：先让 ``asyncio.eager_task_factory`` 同步跑完再抛）。r9 于是在这种形状下：

* **非 eager**：回滚成「未受理、总线仍可用」，却留下一个**无人取用**的孤儿 Task——它后来
  死在 ``RuntimeError('cannot reuse already awaited coroutine')``（r9 无条件
  ``coro.close()`` 把已被 Task 拥有的 coroutine 关掉了），且 ``Task exception was never
  retrieved``。
* **eager**：组件**已经被关掉**（``runs=1``、五个组件各关一次），``_closing`` 却被回滚成
  ``False``；第二次 ``close()`` 把三个存储**再关一遍**（``closes=[1,1,2,2,2]``）。
* 工厂返回**已完成的 ``Future``** / **没有 ``add_done_callback`` 的 awaitable** 时：前者
  ``_close_future`` 永远 PENDING、两次 close 都超时并打 ``coroutine ... was never awaited``；
  后者抛**裸 AttributeError**、不回滚、第二次 close 永久挂起。

r10 的规则（名字不变）：**受理是全有或全无——总线要么真的在关停（有一个可跟踪、异常会被
取用的 teardown），要么原封未动、仍可用；任何路径都不许留下无主的 pending
``_close_future``。** 判定下沉到 ``_spawn_teardown``：**已启动就收养，从未托管才回滚**；
非 Task 的工厂返回值被绕开工厂接管。

本文件用 **2 s 有界看门狗**钉住：具名 error_kind（适用处）、
``_closing`` / ``_close_future`` / ``_teardown_task`` / ``_teardown_runs`` 的**精确值**、
五个组件的关闭次数、**无孤儿 / 无未取用异常**、**无双关**。

另含两处小项：错误留档对的一致性（取消 ⇒ ``('cancelled', None)``）与幂等分支的
``error_kind=cross-loop-close``。
"""

import asyncio
import gc
import unittest
import warnings

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

from plaita.event.redis import RedisEventBus

WATCHDOG = 2.0


class _Counter:
    """计数关闭次数的后端组件；``aclose`` 优先（redis-py ≥5 / fakeredis 形状）。"""

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


def _make_bus(handler=None):
    bus = RedisEventBus(redis_url="redis://unused/0")
    comps = [_Counter() for _ in range(5)]
    (bus.pubsub, bus.redis, bus.event_storage,
     bus.subscription_storage, bus.processing_tracker) = comps
    bus.handlers["h1"] = (handler or (lambda e: None), "*", None, None)
    return bus, comps


# --------------------------------------------------------------------------
# 工厂形状
# --------------------------------------------------------------------------
def _factory_raises_without_touching(loop, coro, **kwargs):
    """从未托管 coroutine：唯一合法的「回滚」形状。"""
    raise RuntimeError("factory boom")


def _factory_create_then_raise(loop, coro, **kwargs):
    """非 eager：先建 Task（coroutine 已归它所有）再抛。"""
    asyncio.Task(coro, loop=loop, context=kwargs.get("context"))
    raise RuntimeError("factory boom")


def _factory_eager_then_raise(loop, coro, **kwargs):
    """eager：先跑完（或跑到挂起点）再抛。"""
    asyncio.eager_task_factory(loop, coro, **kwargs)
    raise RuntimeError("factory boom")


def _factory_done_future(loop, coro, **kwargs):
    """返回**已完成**的 Future：跟踪不了，且 coroutine 从此没人 await。"""
    fut = loop.create_future()
    fut.set_result(None)
    return fut


class _AwaitableWithoutCallback:
    """awaitable，但**没有** ``add_done_callback``（r9 在此抛裸 AttributeError）。"""

    def __init__(self, loop):
        self._loop = loop

    def __await__(self):
        fut = self._loop.create_future()
        fut.set_result(None)
        return fut.__await__()


def _factory_awaitable_without_callback(loop, coro, **kwargs):
    return _AwaitableWithoutCallback(loop)


def _factory_returns_coro(loop, coro, **kwargs):
    """把 coroutine 本身交回来（awaitable，同样没有 add_done_callback）。"""
    return coro


async def _drive_close1(bus, factory, sink):
    """在给定工厂下调用 close() #1；**在 finally 里**快照未被总线跟踪的任务。"""
    loop = asyncio.get_running_loop()
    saved = loop.get_task_factory()
    me = asyncio.current_task()
    before = set(asyncio.all_tasks())
    loop.set_task_factory(factory)
    try:
        try:
            await bus.close()
            return ("returned", None)
        except BaseException as exc:                  # noqa: BLE001 - 断言用
            return ("raised", exc)
    finally:
        sink["orphans"] = [t for t in asyncio.all_tasks()
                           if t is not me and t not in before]
        loop.set_task_factory(saved)


def _close_future_state(bus):
    fut = bus._close_future
    if fut is None:
        return "None"
    if fut.cancelled():
        return "cancelled"
    if fut.done():
        return "done-ok" if fut.exception() is None else "done-exc"
    return "PENDING"


class _Shape:
    """一次工厂形状的观测记录（精确状态 + 关闭计数 + 孤儿/未取用证据）。"""

    def __init__(self, outcome, exc, bus, comps, orphan_count, orphan_states, reports):
        self.outcome = outcome
        self.exc = exc
        self.bus = bus
        self.comps = comps
        self.orphan_count = orphan_count
        self.orphan_states = orphan_states
        self.reports = reports

    @property
    def named_kind(self):
        if isinstance(self.exc, RuntimeError):
            text = str(self.exc)
            for kind in ("teardown-spawn-failed", "teardown-incomplete",
                         "cross-loop-close"):
                if kind in text:
                    return kind
        return None

    @property
    def closes(self):
        return [c.closes for c in self.comps]

    def leaked_reports(self):
        return [str(r.get("message", "")) for r in self.reports
                if "never retrieved" in str(r.get("message", ""))]


def _orphan_state(task):
    """看状态但**不**调用 ``task.exception()``——那会把异常标记成已取用，掩盖证据。"""
    if task.cancelled():
        return "cancelled"
    if not task.done():
        return "PENDING"
    exc = getattr(task, "_exception", None)
    return "done-ok" if exc is None else "done-exc:" + type(exc).__name__


async def run_shape(factory, *, bus=None, bound=WATCHDOG):
    """跑一次 close() #1，并把孤儿喂给 GC（never-retrieved 在 ``Task.__del__`` 里报）。

    异常处理器必须**一直装着**到孤儿被回收，否则 ``leaked_reports()`` 只会「看着绿」。
    """
    loop = asyncio.get_running_loop()
    reports = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _l, ctx: reports.append(ctx))
    if bus is None:
        bus, comps = _make_bus()
    else:
        comps = [bus.pubsub, bus.redis, bus.event_storage,
                 bus.subscription_storage, bus.processing_tracker]
    sink = {}
    driver = asyncio.create_task(_drive_close1(bus, factory, sink))
    try:
        got = await asyncio.wait_for(asyncio.shield(driver), timeout=bound)
        outcome, exc = got
        raw = [t for t in sink.get("orphans", []) if t is not bus._teardown_task]
        await settle()                                # 孤儿跑完（若它能跑）
        states = [_orphan_state(t) for t in raw]
        count = len(raw)
        # 丢掉全部强引用，让孤儿 Task 在处理器还装着的时候被 GC 收走。
        del raw, sink, driver
        gc.collect()
        await asyncio.sleep(0)
        gc.collect()
    finally:
        loop.set_exception_handler(previous_handler)
    return _Shape(outcome, exc, bus, comps, count, states, reports)


async def settle():
    """让已排队的 teardown 步骤跑完一拍。"""
    await asyncio.sleep(0)
    await asyncio.sleep(0)


# --------------------------------------------------------------------------
# 1) 正常形状（无 Task 被创建）→ 具名回滚（r9 已验证，v10 不得回归）
# --------------------------------------------------------------------------
class Test109R10GenuineSpawnFailure(unittest.IsolatedAsyncioTestCase):
    async def test_raising_factory_without_task_rolls_back_and_retries(self):
        shape = await run_shape(_factory_raises_without_touching)
        self.assertEqual(shape.outcome, "raised")
        self.assertEqual(shape.named_kind, "teardown-spawn-failed")
        self.assertFalse(shape.bus._closing, "回滚必须恢复 _closing（受理前的 False）")
        self.assertIsNone(shape.bus._close_future)
        self.assertIsNone(shape.bus._teardown_task)
        self.assertEqual(shape.bus._teardown_runs, 0)
        self.assertEqual(shape.closes, [0, 0, 0, 0, 0])
        self.assertEqual(shape.orphan_count, 0)
        # 健康重试：真的跑完一次，五个组件各关一次。
        await asyncio.wait_for(shape.bus.close(), timeout=WATCHDOG)
        self.assertEqual(shape.bus._teardown_runs, 1)
        self.assertEqual(shape.closes, [1, 1, 1, 1, 1])
        self.assertEqual(shape.leaked_reports(), [])

    async def test_rollback_restores_pre_call_closing_true(self):
        """``_closing`` 必须回到**受理前**的值——包括受理前已是 True 的情形。"""
        bus, comps = _make_bus()
        bus._closing = True                       # 已有一轮关停被受理过（半开状态）
        shape = await run_shape(_factory_raises_without_touching, bus=bus)
        self.assertEqual(shape.outcome, "raised")
        self.assertEqual(shape.named_kind, "teardown-spawn-failed")
        self.assertTrue(bus._closing,
                        "回滚必须恢复受理前的 True，而不是硬写成 False")
        self.assertIsNone(bus._close_future)
        self.assertEqual(bus._teardown_runs, 0)
        self.assertEqual(shape.closes, [0, 0, 0, 0, 0])


# --------------------------------------------------------------------------
# 2) 工厂创建了 Task 再抛：必须**收养**（r9 的核心判负项）
# --------------------------------------------------------------------------
class Test109R10AdoptStartedTeardown(unittest.IsolatedAsyncioTestCase):
    async def test_non_eager_create_then_raise_is_adopted_no_orphan(self):
        """非 eager：Task 已存在 ⇒ 收养 ⇒ 无孤儿、异常被取用、无双关。"""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            shape = await run_shape(_factory_create_then_raise)
            await settle()
        self.assertEqual(
            shape.outcome, "returned",
            "工厂建了 Task 再抛时 teardown 是我们的：必须受理并返回，不得回滚成"
            "「未受理」（r9 在此抛 teardown-spawn-failed 并留下孤儿）")
        self.assertTrue(shape.bus._closing)
        self.assertEqual(_close_future_state(shape.bus), "done-ok")
        self.assertEqual(shape.bus._teardown_runs, 1)
        self.assertEqual(shape.closes, [1, 1, 1, 1, 1],
                         "收养的那次 teardown 必须把五个组件各关一次")
        self.assertEqual(shape.orphan_count, 0,
                         "不得留下任何未被 _teardown_task 跟踪的孤儿任务")
        self.assertEqual(
            [w for w in caught if issubclass(w.category, RuntimeWarning)], [],
            "不得出现 'coroutine ... was never awaited' / 'cannot reuse ...'")
        # 第二次 close()：幂等，绝不双关。
        await asyncio.wait_for(shape.bus.close(), timeout=WATCHDOG)
        self.assertEqual(shape.bus._teardown_runs, 1)
        self.assertEqual(shape.closes, [1, 1, 1, 1, 1], "第二次 close() 不得再关任何组件")
        self.assertEqual(shape.leaked_reports(), [],
                         "孤儿 Task 的异常必须被取用（0 次 never retrieved）")

    @unittest.skipUnless(hasattr(asyncio, "eager_task_factory"),
                         "需要 3.12+ 的 asyncio.eager_task_factory")
    async def test_eager_then_raise_is_adopted_no_double_close(self):
        """eager：teardown 已同步跑完 ⇒ 仍算「已受理」，第二次 close() 不得双关。"""
        shape = await run_shape(_factory_eager_then_raise)
        await settle()
        self.assertEqual(
            shape.outcome, "returned",
            "eager 同步跑完后工厂再抛：不得回滚成「总线仍可用」（r9 在此回滚，"
            "导致第二次 close() 把三个存储再关一遍）")
        self.assertTrue(shape.bus._closing)
        self.assertEqual(_close_future_state(shape.bus), "done-ok")
        self.assertEqual(shape.bus._teardown_runs, 1)
        self.assertEqual(shape.closes, [1, 1, 1, 1, 1])
        self.assertIsNone(
            shape.bus._teardown_task,
            "r9 死句柄口径：eager 同步跑完的 done 句柄不得回填成「在飞 teardown」")
        await asyncio.wait_for(shape.bus.close(), timeout=WATCHDOG)
        self.assertEqual(shape.bus._teardown_runs, 1)
        self.assertEqual(shape.closes, [1, 1, 1, 1, 1],
                         "双关回归：第二次 close() 后三个存储仍必须是 1")
        self.assertEqual(shape.leaked_reports(), [])

    @unittest.skipUnless(hasattr(asyncio, "eager_task_factory"),
                         "需要 3.12+ 的 asyncio.eager_task_factory")
    async def test_eager_sync_failure_still_retrieves_task_exception(self):
        """eager 同步跑完 + 步骤抛异常 + 工厂再抛：Task 异常仍必须被取用并留档。"""
        bus, comps = _make_bus()

        async def boom():
            raise ValueError("boom-step")

        bus._drain_pending_tasks = boom
        shape = await run_shape(_factory_eager_then_raise, bus=bus)
        await settle()
        self.assertEqual(shape.outcome, "raised")
        self.assertEqual(shape.named_kind, "teardown-incomplete")
        self.assertIsInstance(shape.bus._teardown_task_error, ValueError,
                              "收养的 done Task 异常必须被取用留档（r9 在此无人取用）")
        self.assertEqual(shape.bus._teardown_error_kind, "ValueError")
        self.assertIsNone(shape.bus._close_future,
                          "关停未完成 ⇒ 清空幂等凭据，允许重试")
        self.assertEqual(shape.bus._teardown_runs, 1)
        self.assertEqual(shape.orphan_count, 0)
        self.assertEqual(shape.leaked_reports(), [],
                         "不得出现 'Task exception was never retrieved'")


# --------------------------------------------------------------------------
# 3) 非 Task 的工厂返回值：绕开工厂接管（不得留悬着的 close future）
# --------------------------------------------------------------------------
class Test109R10NonTaskSpawnReturn(unittest.IsolatedAsyncioTestCase):
    async def _assert_wrapped_and_completed(self, factory, label):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            shape = await run_shape(factory)
            await settle()
        self.assertEqual(shape.outcome, "returned",
                         f"{label}：必须被接手并真的跑完关停（r9 让 close future 永悬）")
        self.assertTrue(shape.bus._closing)
        self.assertEqual(_close_future_state(shape.bus), "done-ok",
                         f"{label}：_close_future 不许停在 PENDING")
        self.assertEqual(shape.bus._teardown_runs, 1,
                         f"{label}：接管后必须**真的**跑过一次 teardown（不得谎报）")
        self.assertEqual(shape.closes, [1, 1, 1, 1, 1])
        self.assertEqual(shape.orphan_count, 0)
        self.assertEqual(
            [w for w in caught if issubclass(w.category, RuntimeWarning)], [],
            f"{label}：不得出现 'coroutine ... was never awaited'")
        await asyncio.wait_for(shape.bus.close(), timeout=WATCHDOG)
        self.assertEqual(shape.bus._teardown_runs, 1)
        self.assertEqual(shape.closes, [1, 1, 1, 1, 1], f"{label}：不得双关")

    async def test_done_future_return_is_wrapped(self):
        await self._assert_wrapped_and_completed(_factory_done_future, "已完成的 Future")

    async def test_awaitable_without_add_done_callback_is_wrapped(self):
        await self._assert_wrapped_and_completed(
            _factory_awaitable_without_callback, "无 add_done_callback 的 awaitable")

    async def test_factory_returning_bare_coro_is_wrapped(self):
        await self._assert_wrapped_and_completed(
            _factory_returns_coro, "工厂把 coroutine 交回来")


# --------------------------------------------------------------------------
# 4) 结构不变量：spawn 只交真 Task 或抛具名错；任何形状都不留无主 pending future
# --------------------------------------------------------------------------
class Test109R10SpawnContract(unittest.IsolatedAsyncioTestCase):
    ALL_SAFE_FACTORIES = [
        ("genuine-throw", _factory_raises_without_touching),
        ("create-then-raise", _factory_create_then_raise),
        ("done-future", _factory_done_future),
        ("awaitable-no-callback", _factory_awaitable_without_callback),
        ("bare-coro", _factory_returns_coro),
    ]

    async def test_no_shape_leaves_ownerless_pending_close_future(self):
        """「受理 ⇒ 有一个会被结算的 teardown」或「回滚 ⇒ 没有 pending future」。"""
        for label, factory in self.ALL_SAFE_FACTORIES:
            with self.subTest(shape=label):
                bus, comps = _make_bus()
                shape = await run_shape(factory, bus=bus)
                await settle()
                if shape.outcome == "returned":
                    self.assertNotEqual(
                        _close_future_state(bus), "PENDING",
                        f"{label}：受理必须带回一个可结算的 teardown，"
                        "不许留下无主的 pending _close_future")
                    self.assertEqual(bus._teardown_runs, 1)
                else:
                    self.assertEqual(shape.named_kind, "teardown-spawn-failed")
                    self.assertIsNone(bus._close_future)
                    self.assertEqual(bus._teardown_runs, 0)
                # 收尾，避免跨 subTest 干扰。
                try:
                    await asyncio.wait_for(bus.close(), timeout=WATCHDOG)
                except BaseException:                   # noqa: BLE001 - 已断言的形状
                    pass

    async def test_spawn_returns_only_real_tasks_or_raises(self):
        """``_spawn_teardown`` 的返回类型契约（item 1 的等价保证）。

        返回值的**使用**（``add_done_callback`` / ``done()``）在 request_close 里；
        只要本方法只可能交出真 ``asyncio.Task`` 或抛具名错误，那些使用对任意 factory
        行为都不会失败——会失败的部分全在本方法内部并走同一条回滚。
        """
        loop = asyncio.get_running_loop()
        for label, factory in self.ALL_SAFE_FACTORIES:
            with self.subTest(shape=label):
                bus, comps = _make_bus()
                saved = loop.get_task_factory()
                loop.set_task_factory(factory)
                try:
                    try:
                        spawned = bus._spawn_teardown(loop)
                    except BaseException as exc:        # noqa: BLE001 - 断言用
                        self.assertIn("factory boom", str(exc))
                    else:
                        self.assertIsInstance(
                            spawned, asyncio.Task,
                            f"{label}：_spawn_teardown 只许交真 Task（否则 "
                            "request_close 对返回值的 add_done_callback 会裸抛）")
                        spawned.cancel()
                        try:
                            await spawned
                        except (asyncio.CancelledError, RuntimeError):
                            pass
                finally:
                    loop.set_task_factory(saved)
                    bus._close_future = None
                    bus._closing = False


# --------------------------------------------------------------------------
# 5) 错误留档对一致（r10 item 9）
# --------------------------------------------------------------------------
class Test109R10ErrorPairConsistency(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_clears_previous_error(self):
        """取消 ⇒ ``('cancelled', None)``；不得残留**同一总线**上一轮的 ValueError。"""
        bus, comps = _make_bus()

        async def boom():
            raise ValueError("boom-step")

        entered = asyncio.Event()

        class HangingFirstCall:
            def __init__(self):
                self.calls = 0

            async def close(self):
                self.calls += 1
                if self.calls == 1:
                    entered.set()
                    await asyncio.Event().wait()

        bus.subscription_storage = HangingFirstCall()

        # 第一轮：drain 抛 ValueError ⇒ 留档 (ValueError, ValueError)。
        bus._drain_pending_tasks = boom
        with self.assertRaises(RuntimeError):
            await asyncio.wait_for(bus.close(), timeout=WATCHDOG)
        await settle()
        self.assertEqual(bus._teardown_error_kind, "ValueError")
        self.assertIsInstance(bus._teardown_task_error, ValueError)

        # 第二轮（**同一条总线**）：drain 正常、存储挂起 ⇒ 取消 teardown。
        # 只有同一实例上残留着上一轮异常时，才能检验取消是否把它一并清掉。
        del bus._drain_pending_tasks
        closer = asyncio.create_task(bus.close())
        await asyncio.wait_for(entered.wait(), timeout=WATCHDOG)
        bus._teardown_task.cancel()
        with self.assertRaises(RuntimeError):
            await asyncio.wait_for(closer, timeout=WATCHDOG)
        await settle()
        self.assertEqual(bus._teardown_error_kind, "cancelled")
        self.assertIsNone(bus._teardown_task_error,
                          "取消不是「某个异常」：error 必须清掉，否则 kind/error 不一致")
        await asyncio.wait_for(bus.close(), timeout=WATCHDOG)   # 收尾

    async def test_retry_success_clears_previous_error(self):
        """重试成功后留档描述的是**最近一次**收尾 ⇒ 对整体清空。"""
        bus, comps = _make_bus()

        async def boom():
            raise ValueError("boom-step")

        bus._drain_pending_tasks = boom
        with self.assertRaises(RuntimeError):
            await asyncio.wait_for(bus.close(), timeout=WATCHDOG)
        await settle()
        self.assertEqual(bus._teardown_error_kind, "ValueError")

        del bus._drain_pending_tasks
        await asyncio.wait_for(bus.close(), timeout=WATCHDOG)
        await settle()
        self.assertIsNone(bus._teardown_error_kind)
        self.assertIsNone(bus._teardown_task_error)


# --------------------------------------------------------------------------
# 6) 幂等分支的跨循环具名（r10 item 10）
# --------------------------------------------------------------------------
class Test109R10CrossLoopKind(unittest.IsolatedAsyncioTestCase):
    async def test_idempotent_branch_on_foreign_loop_is_named(self):
        bus, comps = _make_bus()
        other = asyncio.new_event_loop()
        try:
            bus._close_future = other.create_future()   # 受理发生在别的循环
            bus._closing = True
            with self.assertRaises(RuntimeError) as ctx:
                await asyncio.wait_for(bus.close(), timeout=WATCHDOG)
            self.assertIn("error_kind=cross-loop-close", str(ctx.exception),
                          "与在飞跨循环分支同一家族：幂等早返回的跨循环拒绝也要具名")
        finally:
            bus._close_future = None
            bus._closing = False
            other.close()


if __name__ == "__main__":
    unittest.main()
