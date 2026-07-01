"""B6: distributed 模式分支推进应统一走 flow.next_node, 行为钉死测试。

原来 DistributedStrategy._get_next_from_last 自行实现了一套分支推进逻辑
(``find_node_by_id(node_results[last_node_id])``), 与 ``flow.next_node`` 的
``_get_branch_target`` 语义靠约定巧合一致, 易漂移。本测试钉住分布式模式下
分支节点按 branch 结果正确推进的预期行为, 作为统一实现后的回归保护。
"""

import unittest

from plaita.core import types
from plaita.core.callback import FlowCallback
from plaita.core.errors import FlowExecutionException
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.io import Property
from plaita.node import Assignment, Bool, End, Start


def _branch_flow() -> Flow:
    # start -> if(condition=true分支) -> left | right -> end
    return Flow(
        flow_id="b6-branch",
        version="1.0",
        runtime="python",
        global_context={"pick": "left"},
        output_type=Property(data_type=types.STRING, name="r"),
        nodes=[
            Start(id="start", next="decide"),
            Bool(
                id="decide",
                next="left",
                else_next="right",
                condition={"field": "$GLOBAL.pick", "operator": "eq", "value": "left"},
            ),
            Assignment(id="left", next="end", output="L"),
            Assignment(id="right", next="end", output="R"),
            End(id="end", **{"resultType": "success", "output": "$NODE.left"}),
        ],
    )


class _Recorder(FlowCallback):
    def __init__(self):
        self.nodes = []

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs):
        self.nodes.append((node.id, result))


class TestDistributedBranchAdvance(unittest.TestCase):
    def test_branch_advances_to_correct_path(self):
        flow = _branch_flow()
        rec = _Recorder()
        execution = FlowExecution(callback_handlers=[rec])
        execution.mode = "distributed"

        # 第一步: start 在 _start_new_flow 内执行, decide 在 _execute_current_node 执行并返回
        result = execution.run_distributed(flow)
        self.assertEqual(result.get("id"), "decide")

        # 第二步: 从 decide 推进到 left
        ctx = result.get("context")
        result = execution.run_distributed(flow, saved_context=ctx, resume_type="continue")
        self.assertEqual(result.get("id"), "left")

        # 第三步: 从 left 推进到 end
        ctx = result.get("context")
        result = execution.run_distributed(flow, saved_context=ctx, resume_type="continue")
        self.assertEqual(result.get("id"), "end")
        self.assertTrue(result.get("is_end"))

    def test_branch_else_path(self):
        flow = Flow(
            flow_id="b6-else",
            version="1.0",
            runtime="python",
            global_context={"pick": "right"},
            output_type=Property(data_type=types.STRING, name="r"),
            nodes=[
                Start(id="start", next="decide"),
                Bool(
                    id="decide",
                    next="left",
                    else_next="right",
                    condition={"field": "$GLOBAL.pick", "operator": "eq", "value": "left"},
                ),
                Assignment(id="left", next="end", output="L"),
                Assignment(id="right", next="end", output="R"),
                End(id="end", **{"resultType": "success", "output": "$NODE.right"}),
            ],
        )
        execution = FlowExecution()
        execution.mode = "distributed"
        result = execution.run_distributed(flow)
        self.assertEqual(result.get("id"), "decide")
        ctx = result.get("context")
        result = execution.run_distributed(flow, saved_context=ctx, resume_type="continue")
        self.assertEqual(result.get("id"), "right")
        ctx = result.get("context")
        result = execution.run_distributed(flow, saved_context=ctx, resume_type="continue")
        self.assertEqual(result.get("id"), "end")
        self.assertTrue(result.get("is_end"))


if __name__ == "__main__":
    unittest.main()
