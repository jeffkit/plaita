import unittest

try:
    import execjs as _execjs
    _EXECJS_AVAILABLE = True
except ImportError:
    _execjs = None
    _EXECJS_AVAILABLE = False

try:
    import RestrictedPython as _rp  # noqa: F401
    _RESTRICTED_AVAILABLE = True
except ImportError:
    _RESTRICTED_AVAILABLE = False

try:
    import subprocess as _sp
    _docker_check = _sp.run(
        ["docker", "images", "python:3.12-slim", "--format", "{{.Repository}}"],
        capture_output=True, timeout=5,
    )
    _DOCKER_AVAILABLE = (
        _docker_check.returncode == 0
        and b"python" in _docker_check.stdout
    )
except Exception:
    _DOCKER_AVAILABLE = False

from plaita.core.flow import Flow
from plaita.core import types
from plaita.io import Property
from plaita.node import End, Start, register_code_node
from plaita.node.code import (
    CodeNode, run_python, run_python_restricted,
    run_python_subprocess, run_python_docker,
)


# CodeNode is not in the default registry; opt in for this test module.
register_code_node()


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

    @unittest.skipUnless(_EXECJS_AVAILABLE, "PyExecJS not installed (pip install plaita[code])")
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

    def _create_unsafe_flow(self):
        """Same as create_flow but with sandbox_backend='unsafe'."""
        flow = Flow(
            flow_id="code-unsafe",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                children={
                    "language": Property(data_type=types.STRING),
                    "code": Property(data_type=types.STRING),
                    "input": Property(data_type=types.ANY),
                },
            ),
            output_type=Property(data_type=types.ANY),
        )
        nodes = [
            Start(id="start", next="code-run"),
            CodeNode(
                id="code-run",
                flow=flow,
                language="$INPUT.language",
                code="$INPUT.code",
                input="$INPUT.input",
                sandbox_backend="unsafe",
                next="end",
            ),
            End(id="end", flow=flow, **{"resultType": "success", "output": "$NODE.code-run"}),
        ]
        flow.nodes = nodes
        return flow

    @unittest.skipUnless(_RESTRICTED_AVAILABLE, "RestrictedPython not installed")
    def test_sandbox_restricted_blocks_os(self):
        """sandbox_backend='restricted' must refuse to import os."""
        with self.assertRaises(Exception) as ctx:
            run_python_restricted(
                "import os\ndef run(a):\n    return os.listdir('/')",
                input_value=None,
            )
        self.assertIn("not allowed", str(ctx.exception).lower())

    @unittest.skipUnless(_RESTRICTED_AVAILABLE, "RestrictedPython not installed")
    def test_sandbox_restricted_blocks_open(self):
        """sandbox_backend='restricted' must not expose open() builtin."""
        with self.assertRaises(Exception):
            run_python_restricted(
                "def run(a):\n    return open('/etc/passwd').read()",
                input_value=None,
            )

    @unittest.skipUnless(_RESTRICTED_AVAILABLE, "RestrictedPython not installed")
    def test_sandbox_restricted_allows_math(self):
        """sandbox_backend='restricted' should allow importing math."""
        result = run_python_restricted(
            "import math\ndef run(a):\n    return math.ceil(a)",
            input_value=4.2,
        )
        self.assertEqual(result, 5)

    @unittest.skipUnless(_RESTRICTED_AVAILABLE, "RestrictedPython not installed")
    def test_sandbox_restricted_list_comprehension(self):
        """sandbox_backend='restricted' should support list comprehensions."""
        result = run_python_restricted(
            "def run(items):\n    return [x * 2 for x in items]",
            input_value=[1, 2, 3],
        )
        self.assertEqual(result, [2, 4, 6])

    @unittest.skipUnless(_RESTRICTED_AVAILABLE, "RestrictedPython not installed")
    def test_sandbox_restricted_default_via_node(self):
        """CodeNode defaults to sandbox_backend='restricted'."""
        result = self.create_flow().run(
            language="python",
            code="def run(a):\n    return a * 2",
            input=7,
        )
        self.assertEqual(result, 14)

    def test_sandbox_unsafe_via_node(self):
        """sandbox_backend='unsafe' falls back to raw exec (all modules allowed)."""
        result = self._create_unsafe_flow().run(
            language="python",
            code="import math\ndef run(a):\n    return math.floor(a)",
            input=4.9,
        )
        self.assertEqual(result, 4)

    # ------------------------------------------------------------------
    # subprocess backend
    # ------------------------------------------------------------------

    def test_subprocess_basic(self):
        """subprocess backend executes simple code correctly."""
        result = run_python_subprocess("def run(a):\n    return a * 3", 4)
        self.assertEqual(result, 12)

    def test_subprocess_dict_return(self):
        """subprocess backend handles dict return (curly-brace safety)."""
        result = run_python_subprocess(
            'def run(a):\n    return {"value": a, "doubled": a * 2}', 5
        )
        self.assertEqual(result, {"value": 5, "doubled": 10})

    def test_subprocess_import_math(self):
        """subprocess backend allows arbitrary imports (math)."""
        result = run_python_subprocess(
            "import math\ndef run(a):\n    return math.ceil(a)", 3.1
        )
        self.assertEqual(result, 4)

    def test_subprocess_list_input(self):
        """subprocess backend handles list input via JSON serialization."""
        result = run_python_subprocess(
            "def run(items):\n    return [x * 2 for x in items]", [1, 2, 3]
        )
        self.assertEqual(result, [2, 4, 6])

    def test_subprocess_user_exception(self):
        """subprocess backend propagates user-code exceptions as RuntimeError."""
        with self.assertRaises(RuntimeError) as ctx:
            run_python_subprocess(
                "def run(a):\n    raise ValueError('bad input')", None
            )
        self.assertIn("ValueError", str(ctx.exception))
        self.assertIn("bad input", str(ctx.exception))

    def _create_subprocess_flow(self):
        """Flow with sandbox_backend='subprocess'."""
        flow = Flow(
            flow_id="code-subprocess",
            version="1",
            runtime="python",
            input_type=Property(
                data_type=types.OBJECT,
                children={
                    "language": Property(data_type=types.STRING),
                    "code": Property(data_type=types.STRING),
                    "input": Property(data_type=types.ANY),
                },
            ),
            output_type=Property(data_type=types.ANY),
        )
        nodes = [
            Start(id="start", next="code-run"),
            CodeNode(
                id="code-run",
                flow=flow,
                language="$INPUT.language",
                code="$INPUT.code",
                input="$INPUT.input",
                sandbox_backend="subprocess",
                next="end",
            ),
            End(id="end", flow=flow, **{"resultType": "success", "output": "$NODE.code-run"}),
        ]
        flow.nodes = nodes
        return flow

    def test_subprocess_via_node(self):
        """CodeNode with sandbox_backend='subprocess' works end-to-end."""
        result = self._create_subprocess_flow().run(
            language="python",
            code="def run(a):\n    return a + 100",
            input=7,
        )
        self.assertEqual(result, 107)

    # ------------------------------------------------------------------
    # docker backend
    # ------------------------------------------------------------------

    @unittest.skipUnless(_DOCKER_AVAILABLE, "python:3.12-slim image not available locally")
    def test_docker_basic(self):
        """docker backend executes simple code correctly."""
        result = run_python_docker("def run(a):\n    return a + 1", 41)
        self.assertEqual(result, 42)

    @unittest.skipUnless(_DOCKER_AVAILABLE, "python:3.12-slim image not available locally")
    def test_docker_dict_return(self):
        """docker backend handles dict return and JSON roundtrip."""
        result = run_python_docker(
            'def run(a):\n    return {"val": a}', 99
        )
        self.assertEqual(result, {"val": 99})

    @unittest.skipUnless(_DOCKER_AVAILABLE, "python:3.12-slim image not available locally")
    def test_docker_import_math(self):
        """docker backend allows importing math (available in python:3.12-slim)."""
        result = run_python_docker(
            "import math\ndef run(a):\n    return math.floor(a)", 7.9
        )
        self.assertEqual(result, 7)

    @unittest.skipUnless(_DOCKER_AVAILABLE, "python:3.12-slim image not available locally")
    def test_docker_blocks_network(self):
        """docker backend should raise on network access (--network none)."""
        with self.assertRaises(RuntimeError):
            run_python_docker(
                "import urllib.request\n"
                "def run(a):\n"
                "    urllib.request.urlopen('http://example.com', timeout=2)\n"
                "    return 'ok'",
                None,
            )

    @unittest.skipUnless(_DOCKER_AVAILABLE, "python:3.12-slim image not available locally")
    def test_docker_user_exception(self):
        """docker backend propagates user-code exceptions as RuntimeError."""
        with self.assertRaises(RuntimeError) as ctx:
            run_python_docker(
                "def run(a):\n    raise KeyError('missing')", None
            )
        self.assertIn("KeyError", str(ctx.exception))
        self.assertIn("missing", str(ctx.exception))
