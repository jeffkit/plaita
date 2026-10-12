"""plaita#105：解析期环检测必须判「回边」，不能把汇聚 DAG 误报成死循环。

原实现用「节点是否被访问过」当环判据：任何汇聚点（入度 > 1）都会二次入栈，
于是合法菱形 DAG、两条分支共用同一 End 每次都打
``contains a cycle in next/branches; ... will loop forever``。同时告警文案宣称
「without a flow-level timeout」，判据却不读 ``self.timeout``——照提示设了超时
也照打。

本文件钉住：菱形 / 汇聚 End 不告警、真环告警且消息含环上节点 id、设了 flow 级
timeout 不再告警（文案给出的处置办法真的生效）。
"""

import json
import logging
import unittest
from contextlib import contextmanager

from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow


@contextmanager
def _cycle_warnings():
    """收集 ``plaita`` logger 上含 ``contains a cycle`` 的告警消息。"""
    messages = []
    handler = logging.Handler()
    handler.emit = lambda record: (
        messages.append(record.getMessage())
        if "contains a cycle" in record.getMessage() else None
    )
    target = logging.getLogger("plaita")
    target.addHandler(handler)
    previous_level = target.level
    target.setLevel(logging.WARNING)
    try:
        yield messages
    finally:
        target.removeHandler(handler)
        target.setLevel(previous_level)


def _build(nodes, flow_id="f", timeout=""):
    payload = {"flow_id": flow_id, "nodes": nodes}
    if timeout:
        payload["timeout"] = timeout
    return Flow.model_validate(json.loads(json.dumps(payload)))


DIAMOND = [
    {"id": "s", "type": "start", "next": "dec"},
    {"id": "dec", "type": "if", "next": "a", "else_next": "b"},
    {"id": "a", "type": "mock", "value": "A", "next": "merge"},
    {"id": "b", "type": "mock", "value": "B", "next": "merge"},
    {"id": "merge", "type": "mock", "value": "M", "next": "e"},
    {"id": "e", "type": "end", "resultType": "success", "output": "$NODE.merge"},
]

CONVERGING_END = [
    {"id": "s", "type": "start", "next": "dec"},
    {"id": "dec", "type": "if", "next": "a", "else_next": "b"},
    {"id": "a", "type": "mock", "value": "A", "next": "e"},
    {"id": "b", "type": "mock", "value": "B", "next": "e"},
    {"id": "e", "type": "end", "resultType": "success", "output": "M"},
]

TRUE_CYCLE = [
    {"id": "s", "type": "start", "next": "a"},
    {"id": "a", "type": "mock", "value": "A", "next": "b"},
    {"id": "b", "type": "mock", "value": "B", "next": "a"},
    {"id": "e", "type": "end", "resultType": "success", "output": "never"},
]

SELF_LOOP = [
    {"id": "s", "type": "start", "next": "a"},
    {"id": "a", "type": "mock", "value": "A", "next": "a"},
    {"id": "e", "type": "end", "resultType": "success", "output": "never"},
]

UNREACHABLE_CYCLE = [
    {"id": "s", "type": "start", "next": "e"},
    {"id": "e", "type": "end", "resultType": "success", "output": "ok"},
    {"id": "x", "type": "mock", "value": "X", "next": "y"},
    {"id": "y", "type": "mock", "value": "Y", "next": "x"},
]


class TestCycleDetectionIsNotConvergenceDetection(unittest.TestCase):
    def test_diamond_dag_does_not_warn(self):
        with _cycle_warnings() as messages:
            _build(DIAMOND, flow_id="diamond")
        self.assertEqual(messages, [], "菱形 DAG（汇聚点 merge）不是环，不得告警")

    def test_converging_end_does_not_warn(self):
        with _cycle_warnings() as messages:
            _build(CONVERGING_END, flow_id="converging-end")
        self.assertEqual(messages, [], "两条分支共用同一 End 不是环，不得告警")

    def test_diamond_dag_still_executes(self):
        """误报之外的对照组：同一菱形 DAG 实际执行到 End，结果 = M。"""
        flow = _build(DIAMOND, flow_id="diamond-run")
        with _cycle_warnings() as messages:
            result = FlowExecution.run(flow, params={})
        self.assertEqual(messages, [])
        self.assertEqual(result, "M")

    def test_true_cycle_warns_with_cycle_path(self):
        with _cycle_warnings() as messages:
            _build(TRUE_CYCLE, flow_id="true-cycle")
        self.assertEqual(len(messages), 1, "a -> b -> a 是真环，应有一条告警")
        message = messages[0]
        self.assertIn("true-cycle", message)
        self.assertIn("a -> b -> a", message, "告警消息应含环上节点 id")
        # 文案本体也钉住（防止「改判据没改文案」式的静默漂移）
        self.assertIn("contains a cycle in next/branches", message)
        self.assertIn("without a flow-level timeout", message)

    def test_self_loop_warns(self):
        with _cycle_warnings() as messages:
            _build(SELF_LOOP, flow_id="self-loop")
        self.assertEqual(len(messages), 1)
        self.assertIn("a -> a", messages[0])

    def test_flow_timeout_suppresses_cycle_warning(self):
        """照文案设 flow 级超时后，不再是「will loop forever」，不应再告警。"""
        with _cycle_warnings() as messages:
            _build(TRUE_CYCLE, flow_id="true-cycle-timeout", timeout="PT5S")
        self.assertEqual(messages, [])

    def test_diamond_with_flow_timeout_does_not_warn(self):
        with _cycle_warnings() as messages:
            _build(DIAMOND, flow_id="diamond-timeout", timeout="PT5S")
        self.assertEqual(messages, [])

    def test_cycle_not_including_start_reports_exact_path(self):
        """环不在 start 上时，路径只含环上节点（不得把 DFS 前缀一并回显）。"""
        nodes = [
            {"id": "s", "type": "start", "next": "a"},
            {"id": "a", "type": "mock", "value": "A", "next": "b"},
            {"id": "b", "type": "mock", "value": "B", "next": "a"},
            {"id": "e", "type": "end", "resultType": "success", "output": "never"},
        ]
        with _cycle_warnings() as messages:
            _build(nodes, flow_id="mid-cycle")
        self.assertEqual(len(messages), 1)
        self.assertIn("a -> b -> a", messages[0])
        self.assertNotIn("s ->", messages[0], "环路径不得包含环外的 DFS 前缀 s")

    def test_cycle_on_second_branch_is_found_after_first_branch_exhausts(self):
        """先走完的无环分支不得让 DFS 提前收工：另一分支上的真环仍要报。

        钉住「回溯到调用栈上层继续」——把 ``continue`` 改成 ``break``（第一
        条分支的叶子出栈即终止整个 DFS）会漏报这条环。
        """
        nodes = [
            {"id": "s", "type": "start", "next": "dec"},
            {"id": "dec", "type": "if", "next": "a", "else_next": "c"},
            {"id": "a", "type": "mock", "value": "A", "next": "end"},
            {"id": "c", "type": "mock", "value": "C", "next": "c"},
            {"id": "end", "type": "end", "resultType": "success", "output": "done"},
        ]
        with _cycle_warnings() as messages:
            _build(nodes, flow_id="second-branch-cycle")
        self.assertEqual(len(messages), 1, "第二条分支上的 c -> c 必须被检出")
        self.assertIn("c -> c", messages[0])

    def test_missing_adjacency_entries_are_tolerated(self):
        """邻接表缺项（悬空 next / 空图）按无后继处理，不得抛异常。"""
        from plaita.core.flow import _resolve_cycle

        self.assertEqual(_resolve_cycle({}, "missing-root"), [])
        self.assertEqual(_resolve_cycle({"s": ["ghost"]}, "s"), [])

    def test_unreachable_cycle_still_not_reported(self):
        """#105 边界：不可达子图的真环仍不报（行为扩张留给维护者定夺）。

        若将来采纳「不可达子图的真环也报」，请翻转本断言并同步 issue 边界。
        """
        with _cycle_warnings() as messages:
            _build(UNREACHABLE_CYCLE, flow_id="unreachable")
        self.assertEqual(messages, [])


if __name__ == "__main__":
    unittest.main()
