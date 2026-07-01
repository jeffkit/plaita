"""B5 复现: Flow 应缓存节点索引, start_node/find_node_by_id 不应每次线性扫描。

正确性要求(原行为保持):
- 显式 Start 节点时, start_node 返回该 Start;
- 无显式 Start 时, start_node 返回"没有被任何节点引用为 next/branch.next"的节点;
- find_node_by_id 命中返回节点, 未命中抛错。

性能要求: 对大流程反复 find_node_by_id 应为 O(1); 这里用一个 1000 节点流程
验证 1000 次查找在合理时间内完成, 并校验索引存在且与 nodes 一致。
"""

import time
import unittest

from plaita.core.flow import Flow
from plaita.node import Assignment, End, Start


def _linear_flow(n: int) -> Flow:
    nodes = [Start(id="start", next="n0")]
    for i in range(n - 1):
        nodes.append(Assignment(id=f"n{i}", next=f"n{i+1}", output={"i": i}))
    nodes.append(Assignment(id=f"n{n-1}", next="end", output={"i": n - 1}))
    nodes.append(End(id="end", **{"resultType": "success", "output": "ok"}))
    return Flow(flow_id="b5-linear", version="1.0", runtime="python", nodes=nodes)


class TestFlowNodeIndex(unittest.TestCase):
    def test_find_node_by_id_uses_index(self):
        flow = _linear_flow(50)
        idx = flow._ensure_index()
        self.assertEqual(len(idx), len(flow.nodes))
        for n in flow.nodes:
            self.assertIs(idx[n.id], n)
        # 触发后私有索引也应已填充
        self.assertEqual(len(flow._node_index), len(flow.nodes))

    def test_find_node_by_id_correctness(self):
        flow = _linear_flow(20)
        node = flow.find_node_by_id("n10")
        self.assertEqual(node.id, "n10")
        self.assertIs(flow.find_node_by_id("start"), flow.nodes[0])

    def test_find_node_by_id_missing_raises(self):
        flow = _linear_flow(5)
        with self.assertRaises(Exception):
            flow.find_node_by_id("nope")

    def test_start_node_explicit(self):
        flow = _linear_flow(3)
        self.assertEqual(flow.start_node.id, "start")

    def test_start_node_inferred_when_no_start_node(self):
        # 没有 start 类型节点时, 应推断入度为 0 的节点
        flow = Flow(
            flow_id="b5-inferred",
            version="1.0",
            runtime="python",
            nodes=[
                Assignment(id="root", next="end", output={"x": 1}),
                End(id="end", **{"resultType": "success", "output": "ok"}),
            ],
        )
        self.assertEqual(flow.start_node.id, "root")

    def test_find_node_by_id_o1_for_large_flow(self):
        flow = _linear_flow(1000)
        t0 = time.monotonic()
        for n in flow.nodes:
            _ = flow.find_node_by_id(n.id)
        elapsed = time.monotonic() - t0
        # O(1) 索引: 1000 次查找应远低于 0.1s; 线性扫描的 O(n^2) 会高得多
        self.assertLess(elapsed, 0.1, f"find_node_by_id 似乎未走索引: {elapsed:.3f}s")


if __name__ == "__main__":
    unittest.main()
