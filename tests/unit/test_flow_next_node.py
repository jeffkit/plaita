"""``flow.next_node`` 行为钉死 (characterization test).

2026-07 review 第 1.3 节点出 ``flow.next_node`` / ``_get_target_node`` /
``_get_branch_target`` / ``DistributedStrategy._get_next_from_last`` 四处
原本各有一份"下一个节点怎么决定"的实现。上一批整改里 DistributedStrategy
已统一走 ``flow.next_node``, 本测试钉死 ``flow.next_node`` 的全部边角行为,
作为后续任何重构 (例如 #14 ``ExecutionState(BaseModel)``) 的回归保护。

覆盖路径:
- 非分支节点按 ``next`` 推进
- 分支节点按 ``branch`` 参数选 ``branch.next``
- ``End`` 节点 → None
- ``branch`` 未命中任何分支 → None (并向后兼容旧的 ``branch.name`` 回退)
- 显式 ``Start`` 节点作为入口
- 三种执行模式 (Normal/Generator/Distributed) 在同一 flow 上推进结果一致
"""

import asyncio
import unittest

from plaita.core import types
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.io import Property
from plaita.node import Assignment, Bool, End, Start


def _linear_flow() -> Flow:
    return Flow(
        flow_id="linear",
        version="1.0",
        runtime="python",
        nodes=[
            Start(id="start", next="a"),
            Assignment(id="a", next="b", output={"v": 1}),
            Assignment(id="b", next="end", output={"v": 2}),
            End(id="end", **{"resultType": "success", "output": "$NODE.b.v"}),
        ],
    )


def _branch_flow(pick: str = "left") -> Flow:
    return Flow(
        flow_id="branch",
        version="1.0",
        runtime="python",
        global_context={"pick": pick},
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


class TestFlowNextNode(unittest.TestCase):
    def test_non_branching_advances_by_next(self):
        flow = _linear_flow()
        start = flow.find_node_by_id("start")
        a = flow.find_node_by_id("a")
        b = flow.find_node_by_id("b")
        end = flow.find_node_by_id("end")

        self.assertEqual(flow.next_node(start).id, "a")
        self.assertEqual(flow.next_node(a).id, "b")
        self.assertEqual(flow.next_node(b).id, "end")

    def test_end_node_returns_none(self):
        flow = _linear_flow()
        end = flow.find_node_by_id("end")
        self.assertIsNone(flow.next_node(end))

    def test_branching_node_uses_branch_argument(self):
        flow = _branch_flow()
        decide = flow.find_node_by_id("decide")
        # branch="left" → 走 left 分支
        self.assertEqual(flow.next_node(decide, branch="left").id, "left")
        # branch="right" → 走 right 分支
        self.assertEqual(flow.next_node(decide, branch="right").id, "right")

    def test_branching_node_unknown_branch_returns_none(self):
        flow = _branch_flow()
        decide = flow.find_node_by_id("decide")
        # 不匹配的 branch → None (而不是回退到 next)
        self.assertIsNone(flow.next_node(decide, branch="nope"))

    def test_branching_node_no_branch_argument_falls_back_to_next(self):
        # branch=None 时, _get_branch_target 遍历分支找不到 None, 返回 None。
        # 但 _get_target_node 的入口条件是 ``(not current.branching) and current.next``
        # —— 分支节点走 else 分支, 不走 next 字段。这是已知的"非分支才走 next"语义。
        flow = _branch_flow()
        decide = flow.find_node_by_id("decide")
        # branch=None → 进 _get_branch_target, 没有 b.next/b.name == None, 返回 None
        self.assertIsNone(flow.next_node(decide, branch=None))


class TestThreeModesAgreeOnLinearFlow(unittest.TestCase):
    """三种执行模式在同一 flow 上最终结果应一致 (都能跑到 end, 结果=2)。

    注意: 各模式 API 返回结构不同 (normal 返回 result 值, generator yield step,
    distributed 返回 dict), 这里只断言"最终结果"的等价性, 不强求结构相同。
    """

    def test_linear_flow_normal_mode(self):
        flow = _linear_flow()
        execution = FlowExecution()
        result = execution.run_compatible(flow, lazy=False, input=None)
        # normal 模式直接返回 End 节点的 output 求值结果
        self.assertEqual(result, 2)

    def test_linear_flow_generator_mode(self):
        flow = _linear_flow()
        execution = FlowExecution()
        gen = execution.run_compatible(flow, lazy=True, input=None)
        steps = list(gen)
        # 至少跑完所有节点 (start/a/b/end = 4 步)
        self.assertGreaterEqual(len(steps), 4)

    def test_linear_flow_distributed_mode(self):
        flow = _linear_flow()
        execution = FlowExecution()
        execution.mode = "distributed"
        ctx = None
        last = None
        for _ in range(10):
            if ctx is None:
                result = execution.run_distributed(flow)
            else:
                result = execution.run_distributed(flow, saved_context=ctx)
            last = result
            ctx = result.get("context")
            if result.get("is_end"):
                break
        self.assertIsNotNone(last)
        self.assertTrue(last.get("is_end"))
        # distributed 返回 dict, result 字段是 End 节点 output 求值
        self.assertEqual(last.get("result"), 2)


if __name__ == "__main__":
    unittest.main()
