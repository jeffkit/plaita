"""#109/#110 r8：验收者对 r7 的 F1/G2/G3/G4 + F2 与四个 vacuous 属性的回归面。

r7 的架构（非阻塞、幂等的 ``close()`` 请求 + teardown 在总线自己的 context 里跑）
不变；r8 修的是它周围被独立验收者判负的点：

* **F1（rule-level）**：``request_close`` 里 ``teardown_task = spawn()`` 早于
  ``self._close_future = future``。``loop.set_task_factory(asyncio.eager_task_factory)``
  （3.12+ 的**公开**配置项）会让 ``create_task`` 同步跑 coroutine；空闲总线（无在飞
  分发、无监听、后端 close 不 await）一路跑到 ``_teardown`` 的 ``finally``，此时
  ``_close_future is None`` ⇒ 稍后赋值的 future **永不 resolve** ⇒ ``close()`` 永久
  挂起。本文件既钉「spawn 之前属性已就位」的顺序，也用同步 spawn 复现同一时序；
  并在 3.12+ 上**真的**装 ``asyncio.eager_task_factory`` 端到端跑。
* **G2**：``self.pubsub.aclose`` 的属性访问在 ``_close_one`` 的 try 之外 ⇒ redis-py
  4.x（无 ``aclose``）下 ``AttributeError`` 逃出 teardown，调用方却看到「成功」而五个
  组件一个都没关。现在按能力探测（``aclose`` → ``close`` → ``disconnect``）且解析在
  try 内。
* **G3**：teardown 被取消 / 步骤抛异常时 r7 的 finally 仍 ``set_result(None)`` ⇒ 报
  成功 + 幂等早返回 ⇒ 总线永不释放。现在具名 ``error_kind=teardown-incomplete`` 且
  清空幂等凭据以便重试。
* **G4**：无运行循环的 ``request_close()`` 给出具名
  ``error_kind=no-running-loop``，不再裸 ``RuntimeError``。

另钉四个 **r8 前只有「超时判红」、没有可归因断言** 的属性（见
``scripts/mutation_replay/mutations/`` 的 ``m109-r8-*``）：监听取消必须发生在 drain
**之后**、close future 必须被 finally 结算、调用者取消不得经 ``shield`` 落到
teardown / close future 上、监听取消不得走 ``asyncio.gather`` 的
``_GatheringFuture.cancel`` 路径。

**口径更正（r9/Finding 4）**：r8 报告里曾写作「这四个属性去掉实现后套件仍绿」——
**不实**。r9 复测：在 r8 **之前**的树（``7cf3674``）上做等价变异——把 r7
``_teardown`` 的 ``finally: future.set_result(None)`` 换成 ``pass``（注意：r8 的
``m109-r8-teardown-finally-resolve-off`` 定义原文只存在于 r8 之后的代码里，**不能**
直接套到 r8 之前的树上）——5 个 issue 套件里 **4 个在 240 s 界内仍未结束**
（``rc=124``，真挂死：``event_redis`` / ``r3`` / ``r5`` / ``r6``），剩下 ``r7``
套件 57 s 跑完也是 ``12 failed, 2 passed``；**没有一个是「绿」**。抓到它的是
**timeout**，不是断言。r8 的真实价值是把这种「超时红」变成两条**可归因**的
``FAILED``。对照：同一棵 pre-r8 树不施加变异时 5 个套件全部 ``rc=0``。该口径同时
写在 docs ``docs-site/docs/distributed/event-system.md`` 的关停小节。
"""

import asyncio
import logging
import unittest
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import plaita.event.redis as redis_mod
from plaita.event.core import Event
from plaita.event.redis import RedisEventBus


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


class _Catch(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


def _make_bus(handler=None):
    bus = RedisEventBus(redis_url="redis://unused/0")
    comps = [_Counter() for _ in range(5)]
    (bus.pubsub, bus.redis, bus.event_storage,
     bus.subscription_storage, bus.processing_tracker) = comps
    bus.handlers["h1"] = (handler or (lambda e: None), "*", None, None)
    return bus, comps


def _msg(event):
    return {"type": "message", "data": event.model_dump_json().encode()}


def _event():
    return Event(event_type="t.demo", data={})


def _catch_lines():
    catch = _Catch()
    logging.getLogger("plaita.event.redis").addHandler(catch)
    return catch


# --------------------------------------------------------------------------
# F1：spawn 之前所有属性先就位（eager task factory 的同步完成时序）
# --------------------------------------------------------------------------
class Test109R8F1AssignmentOrder(unittest.IsolatedAsyncioTestCase):
    """F1：`_spawn_teardown` 被调用时，teardown 会读的属性必须已经赋值。"""

    async def test_close_future_and_closing_set_before_teardown_spawn(self):
        bus, comps = _make_bus()
        seen = {}
        original = bus._spawn_teardown

        def spy(loop):
            seen["close_future"] = bus._close_future
            seen["closing"] = bus._closing
            seen["teardown_task"] = bus._teardown_task
            seen["loop"] = loop
            return original(loop)

        bus._spawn_teardown = spy
        future = bus.request_close()
        self.assertIs(seen["close_future"], future,
                      "F1：spawn 时 teardown 看到的 _close_future 必须已是本次的 future")
        self.assertTrue(seen["closing"],
                        "F1：spawn 时 _closing 必须已置位（先于 teardown 的第一个 tick）")
        self.assertIsNone(seen["teardown_task"],
                          "F1：_teardown_task 先占位，spawn 返回后回填真实句柄")
        await asyncio.wait_for(asyncio.shield(future), timeout=2)
        for comp in comps:
            self.assertEqual(comp.closes, 1)

    async def test_synchronous_teardown_completion_still_settles_future(self):
        """等价时序：teardown 在 spawn 内**同步跑完**（eager factory + 空闲总线）。

        r7 顺序下 ``finally`` 读到 ``None``，future 永不 settle ⇒ 本用例在
        ``wait_for`` 上超时（红）；r8 顺序下立即返回。
        """
        bus, comps = _make_bus()

        def sync_spawn(loop):
            # eager_task_factory 对空闲总线的效果：coroutine 在 create_task 内
            # 一路跑到 StopIteration。
            coro = bus._teardown()
            try:
                coro.send(None)
            except StopIteration:
                pass
            else:                                # pragma: no cover - 前提被破坏
                coro.close()
                raise AssertionError("空闲总线的 teardown 未同步完成")
            return loop.create_future()

        bus._spawn_teardown = sync_spawn
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual(bus._teardown_runs, 1)
        self.assertIsNotNone(bus._close_future)
        self.assertTrue(bus._close_future.done(),
                        "F1：同步完成的 teardown 必须把 close future 结算掉")
        self.assertFalse(bus._close_future.cancelled())
        for comp in comps:
            self.assertEqual(comp.closes, 1)


@unittest.skipUnless(hasattr(asyncio, "eager_task_factory"),
                     "需要 3.12+ 的 asyncio.eager_task_factory")
class Test109R8F1EagerTaskFactory(unittest.IsolatedAsyncioTestCase):
    """3.12+：真的装 ``asyncio.eager_task_factory``，close() 必须及时返回。"""

    async def test_eager_factory_idle_bus_close_returns_promptly(self):
        bus, comps = _make_bus()
        asyncio.get_running_loop().set_task_factory(asyncio.eager_task_factory)
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual(bus._teardown_runs, 1)
        self.assertTrue(bus._close_future.done())
        for comp in comps:
            self.assertEqual(comp.closes, 1)

    async def test_eager_factory_inflight_dispatch_close_returns_promptly(self):
        release = asyncio.Event()

        async def handler(event):
            await release.wait()

        bus, comps = _make_bus(handler)
        loop = asyncio.get_running_loop()
        loop.set_task_factory(asyncio.eager_task_factory)
        await bus._process_message(_msg(_event()), "h1")
        loop.call_later(0.05, release.set)       # 在飞分发很快落地
        await asyncio.wait_for(bus.close(), timeout=3)
        self.assertEqual(bus._teardown_runs, 1)
        self.assertTrue(bus._close_future.done())
        for comp in comps:
            self.assertEqual(comp.closes, 1)


# --------------------------------------------------------------------------
# G2：按能力释放（含 redis-py 4.x 的 close / disconnect 回退）
# --------------------------------------------------------------------------
class Test109R8ReleaseByCapability(unittest.IsolatedAsyncioTestCase):
    async def test_aclose_preferred_over_close_and_disconnect(self):
        class Both:
            def __init__(self):
                self.acloses = 0
                self.closes = 0
                self.disconnects = 0

            async def aclose(self):
                self.acloses += 1

            async def close(self):
                self.closes += 1

            async def disconnect(self):
                self.disconnects += 1

        bus, comps = _make_bus()
        both = Both()
        bus.redis = both
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual((both.acloses, both.closes, both.disconnects), (1, 0, 0))

    async def test_backend_without_aclose_is_released_via_close(self):
        """redis-py 4.x 形状：只有 ``close(close_connection_pool=...)``，没有 ``aclose``。

        r8 的这一格只钉「``close`` 被调用过」（自造 fake + 方法名），r9/Finding 2
        把它改成钉**池真的被关**：4.5.5 的 ``Redis.close(close_connection_pool=None)``
        不传这个关键字就回落到 ``auto_close_connection_pool``，而 ``from_url()`` 造出
        的客户端实测是 ``False`` ⇒ 连接数 1 → 1（静默不释放）。所以这里断言
        ``close_connection_pool is True`` **且**池的 ``disconnect`` 被走到。
        """

        class _Pool:
            def __init__(self):
                self.disconnects = 0

            async def disconnect(self):
                self.disconnects += 1

        class LegacyRedis:
            """4.5.5 ``redis.asyncio.Redis`` 的最小形状。"""

            def __init__(self):
                self.pool = _Pool()
                self.closes = 0
                self.seen_kwargs = []

            async def close(self, close_connection_pool=None):
                self.closes += 1
                self.seen_kwargs.append(close_connection_pool)
                if close_connection_pool:
                    await self.pool.disconnect()

        bus, comps = _make_bus()
        legacy = LegacyRedis()
        bus.pubsub = legacy
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual(legacy.closes, 1,
                         "G2：没有 aclose 的后端必须经 close 回退被释放（不是静默跳过）")
        self.assertEqual(legacy.seen_kwargs, [True],
                         "G2/r9：4.x 的 close 必须带 close_connection_pool=True——"
                         "裸 close() 在 from_url 客户端上不关池（实测连接数 1→1）")
        self.assertEqual(legacy.pool.disconnects, 1,
                         "G2/r9：钉的是**池被关**，不是「方法名被调用过」")
        self.assertIsNone(bus.pubsub)
        for comp in comps[1:]:                    # comps[0] 是被替换掉的 pubsub
            self.assertEqual(comp.closes, 1)

    async def test_four_x_shaped_backend_without_pool_kwarg_still_closed(self):
        """签名**不接受** ``close_connection_pool`` 的 4.x 形状后端（如 4.5.5 的
        ``PubSub.close(self)``）：不得因为「塞关键字」而 ``TypeError`` 漏放。"""

        class PubSubShaped:
            def __init__(self):
                self.closes = 0

            async def close(self):                # 4.5.5 PubSub.close(self)
                self.closes += 1

        bus, comps = _make_bus()
        shaped = PubSubShaped()
        bus.pubsub = shaped
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual(shaped.closes, 1,
                         "G2/r9：签名不接受关键字时不得硬塞 close_connection_pool")

    async def test_backend_with_only_disconnect_is_released(self):
        class DisconnectOnly:
            def __init__(self):
                self.disconnects = 0

            async def disconnect(self):
                self.disconnects += 1

        bus, comps = _make_bus()
        only = DisconnectOnly()
        bus.redis = only
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual(only.disconnects, 1)
        self.assertIsNone(bus.redis)

    async def test_backend_without_any_closer_warns_named_and_continues(self):
        """三个关闭方法都缺失 ⇒ 具名 WARNING，且其余组件仍然释放。"""

        class NoCloser:
            pass

        bus, comps = _make_bus()
        bus.redis = NoCloser()
        catch = _catch_lines()
        try:
            await asyncio.wait_for(bus.close(), timeout=2)
        finally:
            logging.getLogger("plaita.event.redis").removeHandler(catch)
        named = [m for m in catch.lines if "释放 redis 失败" in m]
        self.assertEqual(len(named), 1, f"G2：必须给具名 WARNING，实得 {catch.lines}")
        self.assertIn("aclose", named[0])
        self.assertIn("NoCloser", named[0])
        self.assertIsNone(bus.redis)
        for comp in [comps[0]] + comps[2:]:       # comps[1] 是被替换掉的 redis
            self.assertEqual(comp.closes, 1,
                             "G2：单个组件失败不得中断其余组件的释放")


# --------------------------------------------------------------------------
# G3：teardown 未完成不得报成功，且可重试
# --------------------------------------------------------------------------
class Test109R8TeardownIncomplete(unittest.IsolatedAsyncioTestCase):
    async def test_cancelled_teardown_is_reported_and_retryable(self):
        started = asyncio.Event()

        class HangingRedis:
            def __init__(self):
                self.closes = 0

            async def aclose(self):
                started.set()
                await asyncio.Event().wait()      # 永远挂起：等被取消
                self.closes += 1                   # pragma: no cover

        bus, comps = _make_bus()
        hanging = HangingRedis()
        bus.pubsub = hanging
        closer = asyncio.create_task(bus.close())
        await asyncio.wait_for(started.wait(), timeout=2)
        self.assertIsNotNone(bus._teardown_task, "teardown 任务句柄必须可见")

        bus._teardown_task.cancel()               # supervisor / 批量 all_tasks() 取消
        with self.assertRaises(RuntimeError) as ctx:
            await asyncio.wait_for(closer, timeout=2)
        self.assertIn("teardown-incomplete", str(ctx.exception),
                      "G3：被取消的 teardown 不得对调用方报成功")
        self.assertIsNone(bus._close_future,
                          "G3：失败后必须清空幂等凭据，让后续 close() 真正重试")
        self.assertEqual(bus._teardown_runs, 1)

        # 重试：必须真的再跑一次 teardown（而不是对已失败的 future 早返回）。
        bus.pubsub = _Counter()
        retry_comps = [bus.pubsub] + comps[1:]
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual(bus._teardown_runs, 2, "G3：后续 close() 必须重新尝试关停")
        self.assertTrue(bus._close_future.done())
        for comp in retry_comps:
            self.assertEqual(comp.closes, 1)

    async def test_teardown_step_error_is_reported_not_success(self):
        bus, comps = _make_bus()

        async def boom():
            raise RuntimeError("boom-step")

        bus._drain_pending_tasks = boom
        with self.assertRaises(RuntimeError) as ctx:
            await asyncio.wait_for(bus.close(), timeout=2)
        self.assertIn("teardown-incomplete", str(ctx.exception))


# --------------------------------------------------------------------------
# G4：无运行循环的 request_close() 具名
# --------------------------------------------------------------------------
class Test109R8RequestCloseLoopless(unittest.TestCase):
    def test_request_close_without_running_loop_is_named(self):
        bus = RedisEventBus(redis_url="redis://unused/0")
        with self.assertRaises(RuntimeError) as ctx:
            bus.request_close()
        text = str(ctx.exception)
        self.assertIn("no-running-loop", text)
        self.assertIn("事件循环", text)
        self.assertFalse(bus._closing, "G4：拒绝必须发生在任何关停副作用之前")
        self.assertIsNone(bus._close_future)


# --------------------------------------------------------------------------
# 四个此前只有「超时判红」（无可归因断言）的属性
# --------------------------------------------------------------------------
class Test109R8VacuousProperties(unittest.IsolatedAsyncioTestCase):
    async def test_listeners_cancelled_after_drain_not_before(self):
        """属性①：监听取消必须发生在 drain **之后**（r7 的顺序契约）。"""
        order = []

        async def handler(event):
            await asyncio.sleep(0.05)
            order.append("dispatch_done")

        bus, comps = _make_bus(handler)

        async def listener():
            try:
                await asyncio.Event().wait()
            finally:
                order.append("listener_cancelled")

        listener_task = asyncio.create_task(listener())
        bus.listeners.append(listener_task)
        await asyncio.sleep(0)                    # 让 listener 真正挂起
        await bus._process_message(_msg(_event()), "h1")
        await asyncio.wait_for(bus.close(), timeout=5)
        self.assertEqual(order, ["dispatch_done", "listener_cancelled"],
                         "属性①：监听取消跑到 drain 之前（或在飞分发被取消）")

    async def test_close_future_is_settled_on_success(self):
        """属性②（成功路径）：finally 必须结算 close future，不得悬置。"""
        bus, comps = _make_bus()
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertIsNotNone(bus._close_future)
        self.assertTrue(bus._close_future.done(),
                        "属性②：teardown 正常结束后 close future 仍未结算（close() 会挂死）")

    async def test_caller_cancellation_does_not_cancel_teardown(self):
        """属性③：``asyncio.shield`` ⇒ 调用者被取消不落到 close future / teardown 上。"""
        started = asyncio.Event()
        gate = asyncio.Event()

        class SlowRedis:
            def __init__(self):
                self.closes = 0

            async def aclose(self):
                started.set()
                await gate.wait()
                self.closes += 1

        bus, comps = _make_bus()
        slow = SlowRedis()
        bus.pubsub = slow
        closer = asyncio.create_task(bus.close())
        await asyncio.wait_for(started.wait(), timeout=2)
        closer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closer
        self.assertFalse(bus._close_future.cancelled(),
                         "属性③：调用者的取消经 shield 之外落到了 close future 上")
        gate.set()
        await asyncio.wait_for(asyncio.shield(bus._close_future), timeout=2)
        self.assertEqual(slow.closes, 1)
        self.assertEqual(bus._teardown_runs, 1)
        # 幂等重入必须正常返回（不是对已取消 future 的早返回）。
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual(bus._teardown_runs, 1)

    async def test_second_waiter_cancellation_does_not_cancel_shared_future(self):
        """属性③（幂等分支）：并发等待者被取消，不得取消共享的 close future。"""
        started = asyncio.Event()
        gate = asyncio.Event()

        class SlowRedis:
            def __init__(self):
                self.closes = 0

            async def aclose(self):
                started.set()
                await gate.wait()
                self.closes += 1

        bus, comps = _make_bus()
        slow = SlowRedis()
        bus.pubsub = slow
        first = asyncio.create_task(bus.close())
        await asyncio.wait_for(started.wait(), timeout=2)
        second = asyncio.create_task(bus.close())     # 幂等分支
        await asyncio.sleep(0)
        second.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await second
        self.assertFalse(bus._close_future.cancelled(),
                         "属性③：第二个等待者的取消落到了共享 close future 上")
        gate.set()
        await asyncio.wait_for(first, timeout=2)
        self.assertEqual(slow.closes, 1)
        self.assertEqual(bus._teardown_runs, 1)

    async def test_listener_stop_does_not_use_gather(self):
        """属性④：监听取消不得走 ``asyncio.gather``（``_GatheringFuture.cancel`` 递归源）。"""
        bus, comps = _make_bus()

        async def listener():
            try:
                await asyncio.Event().wait()
            finally:
                pass

        listener_task = asyncio.create_task(listener())
        bus.listeners.append(listener_task)
        await asyncio.sleep(0)

        calls = []
        real_gather = asyncio.gather

        def spy(*args, **kwargs):
            calls.append(len(args))
            return real_gather(*args, **kwargs)

        with patch.object(asyncio, "gather", spy):
            await asyncio.wait_for(bus.close(), timeout=5)
        self.assertEqual(
            calls, [],
            "属性④：关停序列调用了 asyncio.gather（CE-2 的 _GatheringFuture.cancel 递归源）")
        self.assertTrue(listener_task.cancelled() or listener_task.done())


if __name__ == "__main__":
    unittest.main()
