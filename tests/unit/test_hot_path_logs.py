"""B8 复现: 执行热路径的逐节点日志不应是 INFO 级别。

当前 runner/executor/flow 在每个节点跳转都打 INFO 日志("result: ...",
"set last node id: ...", "next_node: ...", "finding next node ..."), 生产环境
噪声大且可能打印大体量 result。应降为 DEBUG。
"""

import logging
import unittest

from plaita.core import types
from plaita.core.flow import Flow
from plaita.io import Property
from plaita.node import Assignment, End, Start


def _flow() -> Flow:
    return Flow(
        flow_id="b8-logs",
        version="1.0",
        runtime="python",
        output_type=Property(data_type=types.STRING, name="r"),
        nodes=[
            Start(id="start", next="assign"),
            Assignment(id="assign", next="end", output={"x": 1}),
            End(id="end", **{"resultType": "success", "output": "$NODE.assign"}),
        ],
    )


class TestHotPathLogsAreDebug(unittest.TestCase):
    def test_no_per_node_info_logs(self):
        with self.assertLogs("plaita", level="DEBUG") as cm:
            _flow().run()

        info_records = [r for r in cm.records if r.levelno == logging.INFO]
        noisy = [
            r for r in info_records
            if any(s in r.getMessage() for s in ("next_node", "result:", "set last node id", "finding next node", "branch:"))
        ]
        self.assertEqual(noisy, [], f"热路径仍打印 INFO 级别逐节点日志: {[r.getMessage() for r in noisy]}")

    def test_debug_level_still_carries_diagnostics(self):
        with self.assertLogs("plaita", level="DEBUG") as cm:
            _flow().run()
        debug_msgs = [r.getMessage() for r in cm.records if r.levelno == logging.DEBUG]
        self.assertTrue(any("next_node" in m or "finding next node" in m or "result:" in m for m in debug_msgs),
                        f"DEBUG 级别缺少诊断日志: {debug_msgs}")


if __name__ == "__main__":
    unittest.main()
