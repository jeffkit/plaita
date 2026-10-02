"""review-fix B1 回归：非 End 节点缺 next 不再静默「成功」。

历史行为：普通（非分支）节点没有 next（或漏接 End）时，调度循环以
``reached_end=False`` 退出，把整个 ``$NODE`` 状态表当流程结果返回——
流程"成功"，结果是内部中间态。与分支未命中（``_handle_unmatched_branch``）
同源的姊妹路径，review-fix B1 起默认抛错；节点显式配置
``errorHandler.strategy=continue/continue-with`` 时保留遗留逃生口。
"""
from __future__ import annotations

import unittest

from plaita.core.errors import FlowExecutionException
from plaita.core.flow import Flow


def _flow_def(assignment_extra=None, extra_nodes=None):
    """start → assignment(可配 errorHandler/next) [→ 其他节点]。"""
    node_a = {
        "id": "a", "type": "assignment", "name": "assign",
        "output": "assigned-value", "outputType": {"data_type": "string"},
    }
    if assignment_extra:
        node_a.update(assignment_extra)
    nodes = [
        {"id": "s", "type": "start", "name": "start", "next": "a"},
        node_a,
    ]
    nodes.extend(extra_nodes or [])
    return {"flow_id": "b1-regression", "nodes": nodes}


_END_NODE = {"id": "e", "type": "end", "name": "end",
             "resultType": "success", "output": "$NODE.a"}


class TestNonBranchNodeMissingNext(unittest.TestCase):
    """无 next 的普通节点：默认报错、continue 逃生口、显式 End 不受影响。"""

    def test_default_raises(self):
        """默认 errorHandler（abort）：缺 next 的非 End 节点必须抛错。"""
        flow = Flow.model_validate(_flow_def())
        with self.assertRaises(FlowExecutionException) as cm:
            flow.run({"v": 1})
        msg = str(cm.exception)
        self.assertIn("'a'", msg)
        self.assertIn("next", msg)

    def test_error_handler_continue_returns_node_state(self):
        """errorHandler.strategy=continue：走遗留逃生口，流程以 $NODE 状态表收尾。"""
        flow = Flow.model_validate(
            _flow_def(assignment_extra={"errorHandler": {"strategy": "continue"}})
        )
        result = flow.run({"v": 1})
        # 遗留语义：$NODE 状态表作为流程结果
        self.assertIsInstance(result, dict)
        self.assertIn("s", result)
        self.assertIn("a", result)
        self.assertEqual(result["a"], "assigned-value")

    def test_explicit_end_flow_unaffected(self):
        """正常显式 End flow 行为不变：End 节点输出即流程结果。"""
        flow = Flow.model_validate(
            _flow_def(assignment_extra={"next": "e"}, extra_nodes=[dict(_END_NODE)])
        )
        self.assertEqual(flow.run({"v": 1}), "assigned-value")

    def test_end_node_without_next_still_fine(self):
        """End 节点本身没有 next 是正常形态，不受守卫影响。"""
        flow = Flow.model_validate({
            "flow_id": "b1-end-only",
            "nodes": [
                {"id": "s", "type": "start", "name": "start", "next": "e"},
                {"id": "e", "type": "end", "name": "end",
                 "resultType": "success", "output": "done"},
            ],
        })
        self.assertEqual(flow.run({}), "done")


class TestGeneratorModeMissingNext(unittest.TestCase):
    """generator（lazy）模式同样拒绝静默收尾。"""

    def test_generator_default_raises(self):
        from plaita.core.executor import FlowExecution

        flow = Flow.model_validate(_flow_def())
        execution = FlowExecution()
        gen = execution.run_compatible(flow, True, {"v": 1})
        with self.assertRaises(FlowExecutionException):
            list(gen)

    def test_generator_continue_yields_synthetic_end(self):
        from plaita.core.executor import FlowExecution

        flow = Flow.model_validate(
            _flow_def(assignment_extra={"errorHandler": {"strategy": "continue"}})
        )
        execution = FlowExecution()
        outputs = list(execution.run_compatible(flow, True, {"v": 1}))
        self.assertTrue(any(o.get("is_end") for o in outputs))


class TestDistributedModeMissingNext(unittest.TestCase):
    """distributed 模式两条推进路径同样拒绝合成假 End 步。"""

    def test_start_only_flow_first_step_raises(self):
        """首步守卫（_start_new_flow）：Start 无后继 → 报错而非假 End 步。"""
        from plaita.core.errors import FlowErrorException
        from plaita.core.executor import FlowExecution

        flow = Flow.model_validate({
            "flow_id": "b1-start-only",
            "nodes": [{"id": "s", "type": "start", "name": "start"}],
        })
        execution = FlowExecution()
        # run_distributed 契约：任何异常归一化为 FlowErrorException 抛出
        with self.assertRaises(FlowErrorException) as cm:
            execution.run_distributed(flow, {"v": 1})
        self.assertIn("next", str(cm.exception))

    def test_advance_step_missing_next_raises(self):
        """推进步守卫（_get_next_from_last）：停在缺 next 节点后推进 → 报错。"""
        from plaita.core.errors import FlowErrorException
        from plaita.core.executor import FlowExecution

        flow = Flow.model_validate(
            _flow_def(assignment_extra={"next": "e"}, extra_nodes=[dict(_END_NODE)])
        )
        execution = FlowExecution()
        first = execution.run_distributed(flow, {"v": 1})
        self.assertFalse(first.get("is_end"))
        saved = first["context"]

        # 拔掉 assignment 的 next，模拟"后继被删"后从 checkpoint 推进
        flow.find_node_by_id("a").next = None
        with self.assertRaises(FlowErrorException) as cm:
            execution.run_distributed(flow, saved_context=saved)
        self.assertIn("next", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
