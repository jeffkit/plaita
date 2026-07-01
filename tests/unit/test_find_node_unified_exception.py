"""B4 复现: find_node_by_id 找不到节点时应抛 FlowExecutionException(NODE_NOT_FOUND),
而非裸 ValueError, 与引擎其余错误语义统一。
"""

import unittest

from plaita.core import types
from plaita.core.errors import FlowErrorType, FlowExecutionException
from plaita.core.flow import Flow
from plaita.io import Property
from plaita.node import End, Start


def _flow_with_dangling_next() -> Flow:
    return Flow(
        flow_id="b4-dangling",
        version="1.0",
        runtime="python",
        output_type=Property(data_type=types.STRING, name="r"),
        nodes=[
            Start(id="start", next="missing_node"),
            End(id="end", **{"resultType": "success", "output": "ok"}),
        ],
    )


class TestFindNodeByIdUnifiedException(unittest.TestCase):
    def test_find_node_by_id_missing_raises_flow_exception(self):
        flow = _flow_with_dangling_next()
        with self.assertRaises(FlowExecutionException) as ctx:
            flow.find_node_by_id("nope")
        self.assertEqual(ctx.exception.error_type, FlowErrorType.NODE_NOT_FOUND)

    def test_dangling_next_raises_flow_exception_on_run(self):
        flow = _flow_with_dangling_next()
        with self.assertRaises(FlowExecutionException) as ctx:
            flow.run()
        self.assertEqual(ctx.exception.error_type, FlowErrorType.NODE_NOT_FOUND)


if __name__ == "__main__":
    unittest.main()
