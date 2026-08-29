from unittest import TestCase

from plaita.core import types
from plaita.core.flow import Flow
from plaita.io import Property
from plaita.node import End, Start, decide
from plaita.node.loop import While


class WhileTestCase(TestCase):
    """While 条件循环节点：继续条件 / max_iterations 上限保护 / item 注入。"""

    def setUp(self) -> None:
        # 子流程：输出 $INPUT.index（当前轮次），便于断言执行轮数
        self.child_flow = Flow(
            flow_id="child-while",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                children={
                    "item": Property(data_type=types.ANY),
                    "index": Property(data_type=types.INTEGER),
                },
            ),
            output_type=Property(data_type=types.INTEGER),
            nodes=[
                Start(id="child-start", next="end"),
                End(id="end", **{"resultType": "success", "output": "$INPUT.index"}),
            ],
        )

    def create_flow(self, while_node: While):
        flow = Flow(
            flow_id="while",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT),
        )
        flow.nodes = [
            Start(id="start", next="while"),
            while_node,
            End(id="end", **{"resultType": "success", "output": "$NODE.while"}),
        ]
        return flow

    def test_condition_stop(self):
        """$LOOP-INDEX < 3 为继续条件：应恰好执行 3 轮，输出最后一轮 index=2。"""
        node = While(
            id="while",
            child_flow=self.child_flow,
            condition=decide.Condition(
                field="$LOOP-INDEX", operator=decide.CONDITION_OP_LT, value=3
            ),
            next="end",
        )
        result = self.create_flow(node).run({})
        self.assertEqual(2, result)

    def test_max_iterations_guard(self):
        """恒真条件下达到 max_iterations 上限应强制停止。"""
        node = While(
            id="while",
            child_flow=self.child_flow,
            condition=decide.Condition(
                field="$LOOP-INDEX", operator=decide.CONDITION_OP_GT, value=-1
            ),
            max_iterations=2,
            next="end",
        )
        result = self.create_flow(node).run({})
        self.assertEqual(1, result)  # 第 2 轮 index=1 后停止

    def test_item_injection(self):
        """子流程 $INPUT.item 应为上一轮结果：计数器场景累加到 3。

        首轮 $LOOP-ITEM 为 None（先判断后执行的标准 while 语义），因此继续条件
        用 or 组合「item 为空 或 未达 3」：Condition.match 对 None 恒 False（引擎
        保护语义），靠 eq None 分支放行首轮。
        """
        child = Flow(
            flow_id="child-while",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                children={"item": Property(data_type=types.ANY), "index": Property(data_type=types.INTEGER)},
            ),
            output_type=Property(data_type=types.INTEGER),
            nodes=[
                Start(id="child-start", next="end"),
                # 上一轮结果 None（falsy）时 $F.or 返回 0，否则累加
                End(
                    id="end",
                    **{
                        "resultType": "success",
                        "output": "$F.add($F.or($INPUT.item, 0), 1)",
                    },
                ),
            ],
        )
        node = While(
            id="while",
            child_flow=child,
            condition=decide.ConditionGroup(
                relation="or",
                conditions=[
                    decide.Condition(field="$LOOP-ITEM", operator="eq", value=None),
                    decide.Condition(field="$LOOP-ITEM", operator="lt", value=3),
                ],
            ),
            next="end",
        )
        result = self.create_flow(node).run({})
        self.assertEqual(3, result)
