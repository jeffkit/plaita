"""B2 复现: FlowExecution.run(..., timeout=...) 在 normal 模式下不应被丢弃。

调用方传入的 ``timeout`` 应与 ``flow.timeout`` 取更严格者生效。
当前实现里 ``execute`` 收了 timeout 却没往下传, ``_prepare_strategy``
只读 ``flow.timeout``, 导致无 flow 级超时的流程即使调用方给了 timeout
也不会超时。
"""

import time
import unittest

from plaita.core import types
from plaita.core.errors import FlowErrorType, FlowExecutionException
from plaita.core.executor import FlowExecution
from plaita.io import Property
from plaita.node import End, Node, Start
from typing import ClassVar


class _SleepNode(Node):
    node_type: ClassVar[str] = "sleep_test"
    node_name: ClassVar[str] = "睡眠"
    seconds: float = 0.0

    def run(self, execution):
        time.sleep(self.seconds)
        return f"slept {self.seconds}s"


class TestCallTimeoutNotDropped(unittest.TestCase):
    def _build_flow(self, seconds: float) -> object:
        from plaita.core.flow import Flow
        return Flow(
            flow_id="timeout-call",
            version="1.0",
            runtime="python",
            # 注意: 故意不给 flow.timeout
            output_type=Property(data_type=types.STRING, name="r"),
            nodes=[
                Start(id="start", next="slow"),
                _SleepNode(id="slow", next="end", seconds=seconds),
                End(id="end", **{"resultType": "success", "output": "$NODE.slow"}),
            ],
        )

    def test_call_timeout_triggers_when_no_flow_timeout(self):
        flow = self._build_flow(seconds=0.4)
        # 调用方给 100ms 超时, 节点要 400ms, 应触发 FLOW_ERROR 超时
        with self.assertRaises(FlowExecutionException) as ctx:
            FlowExecution.run(flow, params={}, timeout=100)
        self.assertEqual(ctx.exception.error_type, FlowErrorType.FLOW_ERROR)
        self.assertEqual(ctx.exception.code, -1)

    def test_call_timeout_still_respected_when_flow_timeout_looser(self):
        from plaita.core.flow import Flow
        flow = Flow(
            flow_id="timeout-merge",
            version="1.0",
            runtime="python",
            timeout="5000",  # flow 给 5s
            output_type=Property(data_type=types.STRING, name="r"),
            nodes=[
                Start(id="start", next="slow"),
                _SleepNode(id="slow", next="end", seconds=0.4),
                End(id="end", **{"resultType": "success", "output": "$NODE.slow"}),
            ],
        )
        # 调用方给 100ms, 比 flow 的 5s 更严, 应以调用方为准触发超时
        with self.assertRaises(FlowExecutionException) as ctx:
            FlowExecution.run(flow, params={}, timeout=100)
        self.assertEqual(ctx.exception.error_type, FlowErrorType.FLOW_ERROR)


if __name__ == "__main__":
    unittest.main()
