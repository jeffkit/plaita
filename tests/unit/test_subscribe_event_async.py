"""A6 复现: EventNode 订阅应在 async 上下文里直接 await, 而非走 sync→async 桥接。

`DistributedStrategy._execute_current_node` 是协程, 它调用 `_subscribe_event`
完成事件订阅。当 `event_bus.register_subscription` 是协程时, 当前实现通过
`_run_async_from_sync` 把协程丢到线程池 + 新 event loop 里跑, 既浪费又有跨
loop 风险。正确做法是把 `_subscribe_event` 改成 async, 直接 await。
"""

import asyncio
import threading
import unittest

from plaita.core.context import ExecutionContext
from plaita.core.executor import _subscribe_event
from plaita.node.event_node import EventNode


class _FakeFlow:
    flow_id = "flow-a6"


class _AsyncEventBus:
    """记录 register_subscription 在哪个线程/loop 执行的伪事件总线。"""

    def __init__(self):
        self.calls = []
        self.subscription_id = "sub-1"

    async def register_subscription(self, **params):
        self.calls.append(
            {
                "thread": threading.get_ident(),
                "loop": asyncio.get_running_loop(),
                "params": params,
            }
        )
        return self.subscription_id


class TestSubscribeEventAsync(unittest.IsolatedAsyncioTestCase):
    async def test_subscribe_event_runs_in_caller_loop(self):
        bus = _AsyncEventBus()
        context = ExecutionContext(event_bus=bus)
        node = EventNode(id="evt", event_type="user.login", event_filter={})
        node_state = {"event_type": "user.login", "status": "pending"}

        caller_thread = threading.get_ident()
        caller_loop = asyncio.get_running_loop()

        ok = await _subscribe_event(node, _FakeFlow(), node_state, context)

        self.assertTrue(ok)
        self.assertEqual(len(bus.calls), 1)
        # 关键: 协程应在调用方线程 + 同一个 event loop 上执行, 而不是被丢到线程池
        self.assertEqual(bus.calls[0]["thread"], caller_thread)
        self.assertIs(bus.calls[0]["loop"], caller_loop)
        # subscription_id 应回写到 node_state 并更新到 context
        self.assertEqual(node_state.get("subscription_id"), "sub-1")


if __name__ == "__main__":
    unittest.main()
