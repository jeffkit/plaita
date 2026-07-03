"""``_get_branch_target`` 兜底契约显式化 (任务 #6).

2026-07 review 指出 ``b.next or b.name`` 是 Switch/Logic 的设计语义, 但作为
``flow._get_branch_target`` 与 ``Switch.execute`` 的隐式约定, 新增 branching 节点
类型时作者容易忘记设 ``next`` 而靠巧合的 name 回退掩住 bug。本测试钉死:

- Switch (``branch_name_as_target = True``): ``next`` 未声明时回退到 ``name``。
- 新增 branching 节点 (默认 ``branch_name_as_target = False``): ``next`` 未声明
  时 ``resolve_branch_target`` 返回 None, 不静默跳到 name 同名节点。
- ``branch.next`` 显式声明时, 无论是否 opt-in, 都用 ``next``。
"""

from __future__ import annotations

import unittest
from typing import ClassVar

from plaita.core.flow import Flow
from plaita.io import Property, types
from plaita.node import End, Start
from plaita.node.basic import Node
from plaita.node.decide import Branch, Switch, resolve_branch_target


class _Passthrough(Node):
    """占用一个节点 id, execute 原样返回 input, 用于测 Switch name 回退落到
    以 branch.name 命名的节点。"""

    node_type: ClassVar[str] = "passthrough"
    node_name: ClassVar[str] = "透传"

    def execute(self, execution):
        return execution.evaluate("$INPUT")


class _CustomBranching(Node):
    """模拟"新增的 branching 节点类型"——未声明 ``branch_name_as_target``,
    默认不应走 name 回退。"""

    branching: ClassVar[bool] = True
    node_type: ClassVar[str] = "custom_branch"
    node_name: ClassVar[str] = "自定义分支"
    branches: list = []
    next: str = None

    def execute(self, execution):
        return resolve_branch_target(self, self.branches[0]) if self.branches else None


class TestResolveBranchTarget(unittest.TestCase):
    def test_explicit_next_wins(self):
        b = Branch(name="n", next="explicit_target")
        self.assertEqual(resolve_branch_target(Switch(id="s"), b), "explicit_target")

    def test_switch_falls_back_to_name(self):
        b = Branch(name="branch_a", next=None)
        self.assertEqual(resolve_branch_target(Switch(id="s"), b), "branch_a")

    def test_custom_node_does_not_fall_back_to_name(self):
        b = Branch(name="some_node", next=None)
        node = _CustomBranching(id="cb", branches=[b])
        self.assertIsNone(resolve_branch_target(node, b))


class TestFlowGetBranchTargetContract(unittest.TestCase):
    def test_custom_branching_no_name_fallback_in_flow(self):
        b = Branch(name="left", next=None)  # 故意不声明 next
        custom = _CustomBranching(id="decide", branches=[b])
        flow = Flow(
            flow_id="custom-branch-flow",
            version="1",
            runtime="python",
            nodes=[
                Start(id="start", next="decide"),
                custom,
                # 没有 "left" 节点; name 回退若生效会 NodeNotFoundError。
                End(id="end", **{"resultType": "success", "output": "$INPUT"}),
            ],
        )
        decide = flow.find_node_by_id("decide")
        # branch="left": 未 opt-in 节点 → resolve_branch_target 返回 None,
        # _get_branch_target 找不到匹配 → None, 而不是跳到不存在的 "left"。
        self.assertIsNone(flow.next_node(decide, branch="left"))

    def test_switch_name_fallback_still_works_in_flow(self):
        flow = Flow(
            flow_id="switch-name-fallback",
            version="1",
            runtime="python",
            output_type=Property(data_type=types.STRING, name="r"),
            nodes=[
                Start(id="start", next="decide"),
                Switch(
                    id="decide",
                    branches=[Branch(name="left", is_default=True)],
                ),
                _Passthrough(id="left", next="end"),
                End(id="end", **{"resultType": "success", "output": "ok"}),
            ],
        )
        decide = flow.find_node_by_id("decide")
        nxt = flow.next_node(decide, branch="left")
        self.assertIsNotNone(nxt)
        self.assertEqual(nxt.id, "left")


if __name__ == "__main__":
    unittest.main()
