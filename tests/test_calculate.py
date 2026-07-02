from unittest import TestCase

from plaita.core.flow import Flow
from plaita.core import types
from plaita.io import Property
from plaita.node import End, Start
from plaita.node.calculate import Calculate


class CalculateTestCase(TestCase):

    def test_add(self):
        flow = Flow(
            flow_id="plus-one",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT, is_required=True),
            output_type=Property(data_type=types.INTEGER),
            nodes=[
                Start(id="start", next="cal"),
                Calculate(
                    id="cal",
                    expression={"function_name": "add", "params": {"left": "$INPUT.x", "right": 1}},
                    next="end",
                ),
                End(id="end", **{"resultType": "success", "output": "$NODE.cal"}),
            ],
        )
        self.assertEqual(2, flow.run({"x": 1}))

    def test_greeting(self):
        flow = Flow(
            flow_id="hello-world",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT, is_required=True),
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="start", next="cal"),
                Calculate(
                    id="cal",
                    expression={"function_name": "concat", "params": {"left": "hello ", "right": "$INPUT.name"}},
                    next="end",
                ),
                End(id="end", **{"resultType": "success", "output": "$NODE.cal"}),
            ],
        )
        self.assertEqual("hello KongJie", flow.run({"name": "KongJie"}))

    def test_embed(self):
        flow = Flow(
            flow_id="plus-twice",
            version="1",
            runtime="python",
            input_type=Property(data_type=types.OBJECT, is_required=True),
            output_type=Property(data_type=types.INTEGER),
            nodes=[
                Start(id="start", next="cal"),
                Calculate(
                    id="cal",
                    expression={
                        "function_name": "add",
                        "params": {
                            "left": "$INPUT.x",
                            "right": {"function_name": "multiply", "params": {"left": "$INPUT.x", "right": 10}},
                        },
                    },
                    next="end",
                ),
                End(id="end", **{"resultType": "success", "output": "$NODE.cal"}),
            ],
        )
        self.assertEqual(11, flow.run({"x": 1}))
