import unittest

from plaita.flow import Flow, types
from plaita.io import Property
from plaita.node import End, Start
from plaita.node.code import CodeNode, run_python


class CodeNodeTestCase(unittest.TestCase):

    def test_run_python(self):
        self.assertEqual(5, run_python("def run(a):\n    return a - 1", a=6))
        self.assertEqual(5, run_python("def run(a):\n    return a + 2", a=3))

    def test_run_python_with_mul_line_code(self):
        mul_line_code = """
def run(a):
    b = 2
    return a + b
"""
        self.assertEqual(5, run_python(mul_line_code, a=3))

    def test_run_python_with_wrong_argument(self):
        # wrong argument
        with self.assertRaises(ValueError):
            run_python("def run(a):\n    return a + 2", b=3)

    def test_run_python_with_imports(self):
        # with import case
        self.assertEqual(5, run_python("import math\ndef run(a):\n    return math.ceil(a)", a=4.5))

    def test_run_python_with_extra_class(self):
        # with extra class
        self.assertEqual(
            6,
            run_python(
                """
import math
import sys

class A:
    def __init__(self, a):
        self.a = a

def run(b):
    print(sys.path)
    return math.ceil(A(1.7).a + b)
""",
                b=4,
            ),
        )

    def create_flow(self):
        flow = Flow(
            flow_id="code-run",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                is_required=True,
                children={
                    "language": Property(data_type=types.STRING, is_required=True),
                    "code": Property(data_type=types.STRING, is_required=True),
                    "input": Property(data_type=types.ANY, is_required=True),
                },
            ),
            output_type=Property(data_type=types.ANY, is_required=True),
        )
        nodes = [
            Start(id="start", next="code-run"),
            CodeNode(
                id="code-run",
                flow=flow,
                language="$INPUT.language",
                code="$INPUT.code",
                input="$INPUT.input",
                next="end",
            ),
            End(id="end", flow=flow, **{"resultType": "success", "output": "$NODE.code-run"}),
        ]
        flow.nodes = nodes
        return flow

    def test_set(self):
        self.assertEqual(
            5,
            self.create_flow().run(
                language="js",
                code="function run(a) { return a - 1; }; ",
                input="6",
            ),
        )

        self.assertEqual(
            5,
            self.create_flow().run(
                language="js",
                code="function run(a) { return a + 2; }; ",
                input=3,
            ),
        )

    def test_python_code(self):
        self.assertEqual(
            5,
            self.create_flow().run(
                language="python",
                code="def run(a):\n    return a - 1",
                input=6,
            ),
        )

        self.assertEqual(
            5,
            self.create_flow().run(
                language="python",
                code="def run(a):\n    return a + 2",
                input=3,
            ),
        )
