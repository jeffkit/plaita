"""A5 复现: 带超时的同步节点不应阻塞 asyncio 事件循环。

``NodeRunner._run_sync_node`` 在有超时时使用 ``threading.Thread`` + ``thread.join``,
这会**阻塞事件循环线程**, 让并发的协程无法推进。正确做法是用
``loop.run_in_executor`` 把同步节点丢到线程池, 用 ``asyncio.wait_for`` 控超时,
这样事件循环在节点执行期间仍然可以调度其他协程。

本测试并发运行:
  - 一个 ticker 协程, 每 50ms 记录一次时间, 共 10 次(理想 0.5s)
  - 一个流程, 其同步节点 sleep 0.5s, 节点超时 400ms

若事件循环被阻塞, ticker 会被拖到 ~0.85s 才完成; 若用 run_in_executor + wait_for,
ticker 应在 ~0.5s 内完成。
"""

import asyncio
import threading
import time
import unittest
from typing import ClassVar

from plaita.core import types
from plaita.core.errors import FlowExecutionException
from plaita.core.executor import FlowExecution
from plaita.io import Property
from plaita.node import End, Node, Start


class _BlockingSleepNode(Node):
    node_type: ClassVar[str] = "blocking_sleep_test"
    node_name: ClassVar[str] = "阻塞睡眠"
    seconds: float = 0.0

    def run(self, execution):
        time.sleep(self.seconds)
        return f"slept {self.seconds}s"


class _CooperativeSleepNode(Node):
    """会 poll cancel token 的协作式节点。"""

    node_type: ClassVar[str] = "coop_sleep_test"
    node_name: ClassVar[str] = "协作睡眠"
    seconds: float = 0.0

    def run(self, execution):
        token = getattr(execution, "cancel_event", None)
        end = time.monotonic() + self.seconds
        while time.monotonic() < end:
            if token is not None and token.is_set():
                return "cancelled"
            time.sleep(0.02)
        return "done"


def _build_flow(node_cls, seconds, node_timeout):
    from plaita.core.flow import Flow
    return Flow(
        flow_id="a5-blocking",
        version="1.0",
        runtime="python",
        output_type=Property(data_type=types.STRING, name="r"),
        nodes=[
            Start(id="start", next="slow"),
            node_cls(id="slow", next="end", seconds=seconds, timeout=node_timeout),
            End(id="end", **{"resultType": "success", "output": "$NODE.slow"}),
        ],
    )


class TestSyncNodeTimeoutDoesNotBlockLoop(unittest.IsolatedAsyncioTestCase):
    async def test_event_loop_not_blocked_during_sync_node_timeout(self):
        flow = _build_flow(_BlockingSleepNode, seconds=0.5, node_timeout="400")

        async def ticker():
            stamps = []
            for _ in range(10):
                await asyncio.sleep(0.05)
                stamps.append(time.monotonic())
            return stamps

        async def run_flow():
            try:
                await FlowExecution().arun_compatible(flow, False)
                return "completed"
            except FlowExecutionException:
                return "timeout"

        t0 = time.monotonic()
        stamps, _ = await asyncio.gather(ticker(), run_flow())
        elapsed = time.monotonic() - t0

        # ticker 10 个 50ms 应在 ~0.5s 内完成; 阻塞实现会拖到 ~0.85s+
        self.assertLess(
            elapsed,
            0.75,
            f"事件循环被同步节点超时阻塞: ticker 用了 {elapsed:.2f}s, stamps 间隔异常",
        )
        # 相邻 stamp 之间不应出现 >0.2s 的"卡顿"间隙
        gaps = [stamps[i + 1] - stamps[i] for i in range(len(stamps) - 1)]
        self.assertLess(max(gaps), 0.2, f"存在明显卡顿间隙: {gaps}")


class TestSyncNodeCooperativeCancellation(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_token_set_on_timeout_and_observable_by_node(self):
        flow = _build_flow(_CooperativeSleepNode, seconds=2.0, node_timeout="200")

        captured = {}

        class _CtxProbe(FlowExecution):
            # 借壳: 跑完后检查 context 上是否暴露了 cancel_event
            async def arun_compatible(self, flow, lazy, *args, **kwargs):
                try:
                    return await super().arun_compatible(flow, lazy, *args, **kwargs)
                finally:
                    captured["cancel_event"] = getattr(self._ctx, "cancel_event", None)

        try:
            await _CtxProbe().arun_compatible(flow, False)
        except FlowExecutionException:
            pass

        token = captured.get("cancel_event")
        self.assertIsNotNone(token, "ExecutionContext 未暴露 cancel_event, 节点无法协作取消")
        self.assertTrue(token.is_set(), "超时后 cancel_event 未被 set")


if __name__ == "__main__":
    unittest.main()
