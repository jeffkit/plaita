"""plaita#218：``publish_sync`` 在关闭私有循环前有界排空分发任务。

缺陷面（基线 1a7efa1）：``EventBus.publish_sync`` 无 loop 分支走
``asyncio.run(self.publish(...))``；``InMemoryEventBus.publish`` 只把
``_dispatch_event`` 建成 fire-and-forget task 就返回，``asyncio.run`` 随即
关闭循环并取消 pending task——慢 async handler 永远不执行，且无报错
（事件已入库、``_pending_tasks`` 已空）。

修法：``publish_sync`` 改走 ``_publish_and_drain``，在循环关闭前调用后端
钩子 ``_drain_publish_sync_tasks``；InMemory 覆盖为「有界等待本循环上的
在飞分发任务」——超时不取消（与 Redis ``close()`` 的 #109 r7 口径一致）。

边界（issue 正文）：
- 不改 async ``publish`` 的 fire-and-forget 语义（本文件有正控）；
- Redis 后端的 handler 由 pubsub listener 在独立循环驱动，``publish`` 本身
  不建分发任务，故 ``publish_sync`` 不排空它（本文件有边界用例）。
"""

import asyncio
import time
import unittest

from plaita.event.core import Event, RetryPolicy
from plaita.event.memory import InMemoryEventBus


def _event(event_type="issue218.test", **data):
    return Event(event_type=event_type, data=data)


class TestPublishSyncDrainsInMemoryDispatch(unittest.TestCase):
    """publish_sync（无运行中 loop）在返回前等待本次发布的分发任务。"""

    def test_slow_async_handler_runs_before_publish_sync_returns(self):
        """issue 验收条件 1：handler 内 await sleep(0.1)，返回后 seen==1。"""
        bus = InMemoryEventBus()
        seen = []

        async def handler(event):
            await asyncio.sleep(0.1)
            seen.append(event.event_id)

        asyncio.run(bus.register_handler(event_type="issue218.slow", handler=handler))
        event = _event("issue218.slow", n=1)
        event_id = bus.publish_sync(event)

        self.assertEqual(seen, [event_id],
                         "publish_sync 返回时慢 async handler 未执行（#218 回归）")
        self.assertEqual(bus._pending_tasks, set(),
                         "publish_sync 返回时分发任务登记表应为空")

    def test_retry_policy_handler_runs_before_publish_sync_returns(self):
        """重试分支的分发任务同样被登记并排空（同一 bug 面）。"""
        bus = InMemoryEventBus()
        seen = []

        async def handler(event):
            await asyncio.sleep(0.05)
            seen.append(event.event_id)

        asyncio.run(bus.register_handler(
            event_type="issue218.retry",
            handler=handler,
            retry_policy=RetryPolicy(max_retries=1, initial_delay=0.01),
        ))
        event_id = bus.publish_sync(_event("issue218.retry"))

        self.assertEqual(seen, [event_id],
                         "retry 分支的分发任务未被 publish_sync 排空")
        self.assertEqual(bus._pending_tasks, set())

    def test_never_returning_handler_returns_within_configured_timeout(self):
        """issue 验收条件 4：handler 永不返回时在配置上限内返回，不挂死。"""
        bus = InMemoryEventBus()
        bus.publish_sync_drain_timeout = 0.1
        entered = []

        async def handler(event):
            entered.append(event.event_id)
            await asyncio.Event().wait()      # 永不返回

        asyncio.run(bus.register_handler(event_type="issue218.hang", handler=handler))
        started = time.monotonic()
        event_id = bus.publish_sync(_event("issue218.hang"))
        elapsed = time.monotonic() - started

        self.assertIsInstance(event_id, str)
        self.assertEqual(entered, [event_id], "handler 未被启动")
        # 绝对预算纪律（#165）：0.1s 上限 + 宽松 3s 余量，只钉「不挂死」。
        self.assertLess(elapsed, 3.0,
                        f"publish_sync 未在配置上限内返回（{elapsed:.2f}s）")
        self.assertEqual(bus._pending_tasks, set(),
                         "循环收尾后登记表应清空")


class TestPublishSyncDrainsFromRunningLoop(unittest.IsolatedAsyncioTestCase):
    """running-loop 分支（worker 线程 + 新循环）同样排空。"""

    async def test_slow_async_handler_runs_when_bridge_uses_worker_loop(self):
        bus = InMemoryEventBus()
        seen = []

        async def handler(event):
            await asyncio.sleep(0.1)
            seen.append(event.event_id)

        await bus.register_handler(event_type="issue218.bridge", handler=handler)
        event_id = bus.publish_sync(_event("issue218.bridge"))

        self.assertEqual(seen, [event_id],
                         "running-loop 桥接分支未排空分发任务")
        self.assertEqual(bus._pending_tasks, set())


class TestAsyncPublishStaysFireAndForget(unittest.IsolatedAsyncioTestCase):
    """正控：async publish 不在返回前等待 handler（边界：不改其语义）。"""

    async def test_async_publish_returns_before_slow_handler_finishes(self):
        bus = InMemoryEventBus()
        seen = []

        async def handler(event):
            await asyncio.sleep(0.1)
            seen.append(event.event_id)

        await bus.register_handler(event_type="issue218.async", handler=handler)
        event_id = await bus.publish(_event("issue218.async"), prevent_duplicate_consumption=False)

        self.assertEqual(seen, [], "async publish 不应等待 handler（fire-and-forget）")
        for _ in range(100):
            if seen:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(seen, [event_id], "async publish 的 handler 最终仍应执行")

