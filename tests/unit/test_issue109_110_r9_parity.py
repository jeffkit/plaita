"""#109/#110 r9：独立验收者对 r8 的四条判负（+ 一条口径更正）的回归面。

r8 的架构不动，本文件钉四件事：

* **Finding 1（rule-level，r8 自己引入的挂起）**：``request_close()`` 先落凭据再
  spawn；``_spawn_teardown`` 抛异常（自定义/抛异常的 task factory、已关闭的循环）
  时凭据留在「已受理」而任务不存在 ⇒ close future 永不 resolve ⇒ **第二次 close()
  永久挂起**。修法：受理全有或全无（失败回滚 + 具名 ``teardown-spawn-failed``）；
  并且只把**仍在飞**的 teardown 回填进 ``_teardown_task``（eager factory 同步跑完
  时是 done 句柄，回填会让监督者误判）。
* **Finding 2**：4.5.5 实测 ``Redis.close()`` 不关池（连接数 1 → 1），
  ``close(close_connection_pool=True)`` 才关（0）；且 4.5.5 的 ``Redis``/``PubSub``
  **没有** ``disconnect``（r8 的第三兜底在 redis-py 路径上是死分支）。这里用
  **安装版真实客户端 + 真实 ConnectionPool + 记录型 Connection** 钉「池真的被拆」，
  不只钉方法名。
* **Finding 3**：teardown **Task** 的异常（步骤抛异常）必须被取用并留档，否则 GC 时
  打印 "Task exception was never retrieved"（r8 只给 close **Future** 挂了取用回调）。
* **Finding 4**：重试会**再次调用**三个存储的 ``close()``（声明的口径，不是漏网）。

另附 r9 的口径更正（见 ``test_issue109_110_r8_parity.py`` 与 docs）：r8 前的
``m109-r8-teardown-finally-resolve-off`` 不是「套件仍绿」，而是 5 个 issue 套件里
4 个**挂死**（rc=124，超时抓到）；r8 把它变成两条可归因 ``FAILED``。
"""

import asyncio
import gc
import unittest
from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import redis.asyncio as aioredis
from redis.asyncio.connection import Connection, ConnectionPool

import plaita.event.redis as redis_mod
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


def _make_bus(handler=None):
    bus = RedisEventBus(redis_url="redis://unused/0")
    comps = [_Counter() for _ in range(5)]
    (bus.pubsub, bus.redis, bus.event_storage,
     bus.subscription_storage, bus.processing_tracker) = comps
    bus.handlers["h1"] = (handler or (lambda e: None), "*", None, None)
    return bus, comps


# --------------------------------------------------------------------------
# Finding 1：spawn 失败必须整体回滚（r8 的挂起路径）
# --------------------------------------------------------------------------
class Test109R9SpawnFailureRollback(unittest.IsolatedAsyncioTestCase):
    async def test_throwing_task_factory_rolls_back_and_retry_completes(self):
        """抛异常的 task factory ⇒ 首次 close() 抛具名错误 + 状态回滚 + 重试成功。"""
        bus, comps = _make_bus()
        loop = asyncio.get_running_loop()
        saved_factory = loop.get_task_factory()

        def throwing_factory(loop_, coro, **kwargs):
            # 真实的抛异常 factory：不接管 coroutine，直接失败（自定义 factory 的
            # 常见形状；已关闭循环的 create_task 也是「抛异常而非返回任务」）。
            raise RuntimeError("factory boom")

        async def drive():
            try:
                await bus.close()
            except BaseException as exc:          # noqa: BLE001 - 断言用
                return exc
            return None                            # pragma: no cover

        driver = asyncio.create_task(drive())      # 健康 factory 下建 driver
        loop.set_task_factory(throwing_factory)    # request_close 的 create_task 走它
        try:
            outcome = await asyncio.wait_for(driver, timeout=2)
        finally:
            loop.set_task_factory(saved_factory)

        self.assertIsInstance(outcome, RuntimeError,
                              f"F1：spawn 失败必须抛具名 RuntimeError，实得 {outcome!r}")
        self.assertIn("teardown-spawn-failed", str(outcome),
                      "F1：失败必须是**具名**错误（error_kind 家族）")
        self.assertFalse(bus._closing,
                         "F1：回滚必须把 _closing 恢复为受理前的 False"
                         "（否则第二次 close() 走幂等分支挂死）")
        self.assertIsNone(bus._close_future,
                          "F1：回滚必须清空 close future（否则重试早返回永不 resolve 的 future）")
        self.assertIsNone(bus._teardown_task, "F1：回滚必须清空 teardown 任务句柄")
        self.assertEqual(bus._teardown_runs, 0, "F1：失败的 spawn 不得算作跑过一次 teardown")

        # 健康循环上重试：必须**真的**跑完关停（不是早返回一个死 future）。
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual(bus._teardown_runs, 1, "F1：重试必须真正受理并跑完关停")
        self.assertIsNotNone(bus._close_future)
        self.assertTrue(bus._close_future.done())
        self.assertFalse(bus._close_future.cancelled())
        for comp in comps:
            self.assertEqual(comp.closes, 1,
                             "F1：重试成功路径必须把五个组件各释放一次")

    async def test_second_close_after_spawn_failure_does_not_hang(self):
        """把「第二次 close() 挂死」直接写成**带界的**断言（2s 看门狗）。"""
        bus, comps = _make_bus()
        loop = asyncio.get_running_loop()
        saved_factory = loop.get_task_factory()

        def throwing_factory(loop_, coro, **kwargs):
            raise RuntimeError("factory boom")

        async def drive():
            try:
                await bus.close()
            except BaseException:                  # noqa: BLE001
                return "raised"
            return "returned"                      # pragma: no cover

        driver = asyncio.create_task(drive())
        loop.set_task_factory(throwing_factory)
        try:
            first = await asyncio.wait_for(driver, timeout=2)
        finally:
            loop.set_task_factory(saved_factory)
        self.assertEqual(first, "raised")

        # 第二次：r8 下这里是 `await shield(永不 resolve 的 future)` ⇒ 2s 超时（红）。
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual(bus._teardown_runs, 1)

    async def test_synchronous_failure_does_not_backfill_dead_teardown_task(self):
        """eager 形状的**同步失败**：不得把已结束的句柄回填成「在飞 teardown」。

        r8 无条件 ``self._teardown_task = teardown_task``；同步跑完（成功或失败）时
        这是 done 句柄，监督者按 ``bus._teardown_task is not None`` / ``.cancel()``
        跟它会被误导。现在只回填**未完成**的任务。
        """
        bus, comps = _make_bus()
        original_spawn = bus._spawn_teardown

        async def boom():
            raise RuntimeError("boom-step")

        bus._drain_pending_tasks = boom

        def sync_fail_spawn(loop):
            coro = bus._teardown()
            try:
                coro.send(None)
            except RuntimeError:                   # 步骤同步抛异常
                pass
            else:                                  # pragma: no cover - 前提被破坏
                coro.close()
                raise AssertionError("步骤未同步抛异常")
            done = loop.create_future()             # 已结束的句柄（等价 eager 同步跑完）
            done.set_result(None)
            return done

        bus._spawn_teardown = sync_fail_spawn
        with self.assertRaises(RuntimeError) as ctx:
            await asyncio.wait_for(bus.close(), timeout=2)
        self.assertIn("teardown-incomplete", str(ctx.exception))
        self.assertIsNone(bus._close_future, "失败后必须清空幂等凭据以便重试")
        self.assertIsNone(
            bus._teardown_task,
            "r9/Finding 1 wart：不得把**已结束**的 teardown 句柄回填成在飞任务")

        # 可重试：恢复真实 spawn 与 drain 后再关一次。
        bus._spawn_teardown = original_spawn
        del bus._drain_pending_tasks
        bus.pubsub = _Counter()
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual(bus._teardown_runs, 2, "同步失败后必须能重试")


# --------------------------------------------------------------------------
# Finding 2：安装版真实释放（连接池真的被拆）
# --------------------------------------------------------------------------
class _TouchedConnection(Connection):
    """记录 ``disconnect`` 的真实 Connection（池拆连接时必经）。"""

    disconnects = 0

    async def disconnect(self, *args, **kwargs):
        type(self).disconnects += 1
        return await super().disconnect(*args, **kwargs)


class Test109R9ReleaseActuallyClosesPool(unittest.IsolatedAsyncioTestCase):
    async def test_installed_redis_client_releases_connection_pool(self):
        """**安装版**真实客户端 + 真实连接池：``bus.close()`` 必须拆掉池。

        - ``Redis(connection_pool=pool)`` ⇒ ``auto_close_connection_pool=False``
          （5.x+ 与 4.x 一致）⇒ 裸 ``aclose()`` / 裸 ``close()`` 都**不关池**；
        - 断言落在「池里的真实 Connection 被 disconnect 了一次」，不是方法名。
        """
        pool = ConnectionPool.from_url(
            "redis://127.0.0.1:6399/0", connection_class=_TouchedConnection)
        client = aioredis.Redis(connection_pool=pool)
        self.assertFalse(
            client.auto_close_connection_pool,
            "前提：外部注入 connection_pool 的客户端 auto_close_connection_pool 为 False"
            "（裸 close/aclose 因此不关池）")
        pool._available_connections.append(pool.make_connection())

        _TouchedConnection.disconnects = 0
        bus, comps = _make_bus()
        bus.redis = client
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual(
            _TouchedConnection.disconnects, 1,
            "r9/Finding 2：安装版真实客户端必须真的拆掉连接池"
            "（必须显式传 close_connection_pool=True；只调方法名不算）")
        self.assertIsNone(bus.redis)

    async def test_aclose_with_pool_kwarg_receives_true(self):
        """5.x 形状：``aclose(close_connection_pool=...)`` 必须收到 ``True``。"""

        class Pool:
            def __init__(self):
                self.closed = 0

            async def aclose(self):
                self.closed += 1

        class ModernRedis:
            def __init__(self):
                self.pool = Pool()
                self.seen = []

            async def aclose(self, close_connection_pool=None):
                self.seen.append(close_connection_pool)
                if close_connection_pool:
                    await self.pool.aclose()

        bus, comps = _make_bus()
        modern = ModernRedis()
        bus.redis = modern
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual(modern.seen, [True],
                         "r9/Finding 2：aclose 签名接受时必须传 close_connection_pool=True")
        self.assertEqual(modern.pool.closed, 1)

    async def test_signature_unavailable_falls_back_to_bare_call(self):
        """签名不可知（C 实现 / 装饰器遮蔽）⇒ 按**不支持**处理，裸调用、绝不赌。"""

        class Opaque:
            def __init__(self):
                self.calls = 0

            async def aclose(self, *args, **kwargs):
                self.calls += 1

        bus, comps = _make_bus()
        opaque = Opaque()
        bus.redis = opaque
        with patch.object(redis_mod.inspect, "signature",
                          side_effect=ValueError("no signature")):
            await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual(opaque.calls, 1)

    def test_disconnect_fallback_is_dead_on_installed_redis_py(self):
        """4.5.5 实测证据钉在 suite 里：``Redis``/``PubSub`` 都没有 ``disconnect``。

        r8 的 docs 把 ``disconnect`` 写成「redis-py ≥4.2 的 aclose 前身」——错。
        4.5.5（``uv run --with redis==4.5.5`` 实装）与安装版 8.x 都没有该属性：
        ``_resolve_closer`` 的第三兜底在 redis-py 路径上是**死分支**，只有别的后端
        才可能命中。若某天 redis-py 真的补上，这条会红，提醒改 docs。
        """
        from redis.asyncio.client import PubSub

        self.assertFalse(hasattr(aioredis.Redis, "disconnect"),
                         "redis.asyncio.Redis 不该有 disconnect（r9 实测 4.5.5/8.x 均无）")
        self.assertFalse(hasattr(PubSub, "disconnect"),
                         "redis.asyncio.client.PubSub 不该有 disconnect（r9 实测均无）")


# --------------------------------------------------------------------------
# Finding 3：teardown Task 的异常必须被取用（0/N）
# --------------------------------------------------------------------------
class Test109R9NeverRetrievedTaskException(unittest.IsolatedAsyncioTestCase):
    async def test_failure_path_retrieves_teardown_task_exception(self):
        """失败路径反复跑 N 次：``Task exception was never retrieved`` 必须 0 次。"""
        n = 40
        loop = asyncio.get_running_loop()
        reports = []
        previous_handler = loop.get_exception_handler()
        loop.set_exception_handler(
            lambda _loop, ctx: reports.append(ctx))

        async def boom():
            raise RuntimeError("boom-step")

        try:
            for _ in range(n):
                bus, comps = _make_bus()
                bus._drain_pending_tasks = boom
                with self.assertRaises(RuntimeError) as ctx:
                    await asyncio.wait_for(bus.close(), timeout=2)
                self.assertIn("teardown-incomplete", str(ctx.exception))
                # done 回调在 Task 结束的同一个 tick 内落地；让出一拍再断言留档。
                await asyncio.sleep(0)
                self.assertIsInstance(bus._teardown_task_error, RuntimeError,
                                      "F3：步骤异常必须被取用并留档到 _teardown_task_error")
                self.assertEqual(bus._teardown_error_kind, "RuntimeError")
                bus._teardown_task_error = None
                del bus
                gc.collect()
            await asyncio.sleep(0)
            gc.collect()
        finally:
            loop.set_exception_handler(previous_handler)

        leaked = [r for r in reports
                  if "never retrieved" in str(r.get("message", ""))]
        self.assertEqual(
            leaked, [],
            f"F3：{n} 次失败路径里出现 {len(leaked)} 次 "
            f"'Task exception was never retrieved'（应为 0）：{leaked[:2]}")


# --------------------------------------------------------------------------
# Finding 4：重试的重复释放口径（声明 + 钉住）
# --------------------------------------------------------------------------
class Test109R9RetryReleaseSemantics(unittest.IsolatedAsyncioTestCase):
    async def test_retry_recloses_storage_components_deliberately(self):
        """重试会**再次调用**三个存储的 ``close()``（1→2）——这是**声明过的**口径。

        原因：``pubsub``/``redis`` 槽位释放后置 ``None``，重试天然跳过；三个存储槽位
        保留对象引用，真实实现的 ``if self.redis:`` 幂等闸让第二次是 no-op——而当
        straggler 在两次尝试之间把存储的客户端**复活**时，恰恰是这次重试才关得掉它。
        本用例把计数钉死，避免「静默行为」。
        """
        entered = asyncio.Event()

        class OnceHangingStorage:
            def __init__(self):
                self.closes = 0

            async def close(self):
                self.closes += 1
                if self.closes == 1:
                    entered.set()
                    await asyncio.Event().wait()   # 仅第一次挂起：等被取消

        bus, comps = _make_bus()
        storage = _Counter()
        hanging = OnceHangingStorage()
        tracker = _Counter()
        bus.event_storage = storage
        bus.subscription_storage = hanging
        bus.processing_tracker = tracker

        closer = asyncio.create_task(bus.close())
        await asyncio.wait_for(entered.wait(), timeout=2)
        bus._teardown_task.cancel()                # 取消在 subscription_storage 处
        with self.assertRaises(RuntimeError) as ctx:
            await asyncio.wait_for(closer, timeout=2)
        self.assertIn("teardown-incomplete", str(ctx.exception))
        self.assertEqual((storage.closes, hanging.closes, tracker.closes), (1, 1, 0))

        # 重试：event_storage / subscription_storage 被**再关一次**（声明的口径），
        # processing_tracker 第一次没轮到、这次补上；pubsub/redis 已置 None 不重复。
        await asyncio.wait_for(bus.close(), timeout=2)
        self.assertEqual(bus._teardown_runs, 2)
        self.assertEqual(
            (storage.closes, hanging.closes, tracker.closes), (2, 2, 1),
            "r9/Finding 4：重试会再关三个存储槽位——这是声明口径，不是漏网")
        self.assertEqual((comps[0].closes, comps[1].closes), (1, 1),
                         "pubsub/redis 槽位置 None 后不得重复释放")


if __name__ == "__main__":
    unittest.main()
