from unittest import TestCase

from plaita.core import types
from plaita.core.flow import Flow
from plaita.io import Property
from plaita.node import End, InlineFlow, Start


class InlineFlowTestCase(TestCase):

    def test_inline(self):
        flow = Flow(
            flow_id="test-inline",
            version="1",
            runtime="python",
            output_type=Property(data_type=types.STRING),
            nodes=[
                Start(id="start", next="inline"),
                InlineFlow(
                    id="inline",
                    child_flow=Flow(
                        flow_id="inline-flow",
                        version="1",
                        runtime="python",
                        input_type=Property(data_type=types.STRING, name="name", is_required=True),
                        output_type=Property(data_type=types.STRING, is_required=True),
                        nodes=[
                            Start(id="start-inline", next="end-inline"),
                            End(id="end-inline", **{"resultType": "success", "output": "$INPUT"}),
                        ],
                    ),
                    input="KongJie",
                    next="end",
                ),
                End(id="end", **{"resultType": "success", "output": "$NODE.inline"}),
            ],
        )
        self.assertEqual("KongJie", flow.run())

    def test_inline_parent_input(self):
        flow = Flow(
            flow_id="test-inline",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                children={
                    "name": Property(data_type=types.STRING, is_required=True),
                    "age": Property(data_type=types.INTEGER),
                },
            ),
            output_type=Property(data_type=types.STRING),
        )
        nodes = [
            Start(id="start", next="inline"),
            InlineFlow(
                id="inline",
                child_flow=Flow(
                    flow_id="inline-flow",
                    version="1",
                    runtime="python",
                    output_type=Property(data_type=types.STRING, is_required=True),
                    nodes=[
                        Start(id="start-inline", next="end-inline"),
                        End(id="end-inline", **{"resultType": "success", "output": "$PARENT.$INPUT.name"}),
                    ],
                ),
                next="end",
            ),
            End(id="end", **{"resultType": "success", "output": "$NODE.inline"}),
        ]
        flow.nodes = nodes
        self.assertEqual("KongJie", flow.run(**{"name": "KongJie", "age": 28}))
