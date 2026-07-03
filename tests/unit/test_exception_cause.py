"""B3 复现: 异常包装应保留原始异常的因果链与信息。

`FlowExecution` 的 normal/distributed 归一化（`plaita.core._error_normalization` 的
`finish_normal` / `raise_distributed_error` / `run_distributed`）把任意非
`FlowExecutionException` 异常压成 ``FlowExecutionException(-500, str(e), FLOW_ERROR)``,
但 ``raise ...`` 没有 ``from e``, 丢失 ``__cause__``, 调试时无法看到原始栈。
节点级错误处理同理。
"""

import unittest
from typing import ClassVar

from plaita.core import types
from plaita.core.errors import FlowErrorType, FlowExecutionException, NodeException
from plaita.core.executor import FlowExecution
from plaita.io import Property
from plaita.node import End, Node, Start


class _BoomNode(Node):
    node_type: ClassVar[str] = "boom_test"
    node_name: ClassVar[str] = "爆炸"

    def run(self, execution):
        raise ValueError("kaboom: original detail")


def _build_flow():
    from plaita.core.flow import Flow
    return Flow(
        flow_id="b3-cause",
        version="1.0",
        runtime="python",
        output_type=Property(data_type=types.STRING, name="r"),
        nodes=[
            Start(id="start", next="boom"),
            _BoomNode(id="boom", next="end"),
            End(id="end", **{"resultType": "success", "output": "$NODE.boom"}),
        ],
    )


class TestExceptionCausePreserved(unittest.TestCase):
    def test_flow_level_wrap_preserves_cause(self):
        flow = _build_flow()
        with self.assertRaises(FlowExecutionException) as ctx:
            flow.run()
        exc = ctx.exception
        # 原始异常应作为 __cause__ 保留, 便于排查
        self.assertIsNotNone(exc.__cause__, "FlowExecutionException 丢失了 __cause__")
        self.assertIsInstance(exc.__cause__, ValueError)
        self.assertIn("kaboom", str(exc.__cause__))

    def test_node_exception_message_preserved(self):
        from plaita.core.flow import Flow

        class _NodeExcNode(Node):
            node_type: ClassVar[str] = "node_exc_test"
            node_name: ClassVar[str] = "节点异常"

            def run(self, execution):
                raise NodeException(-4101, "biz error detail")

        flow = Flow(
            flow_id="b3-nodeexc",
            version="1.0",
            runtime="python",
            output_type=Property(data_type=types.STRING, name="r"),
            nodes=[
                Start(id="start", next="ne"),
                _NodeExcNode(id="ne", next="end"),
                End(id="end", **{"resultType": "success", "output": "$NODE.ne"}),
            ],
        )
        with self.assertRaises(FlowExecutionException) as ctx:
            flow.run()
        exc = ctx.exception
        self.assertIsNotNone(exc.__cause__)
        self.assertIn("biz error detail", str(exc))


if __name__ == "__main__":
    unittest.main()
