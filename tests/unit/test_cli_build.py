"""``python -m plaita build`` CLI 测试。

钉：默认产物路径、--check 三态（一致/落后/缺产物）、--format ir 与 canonical
的形态差异（child_flow vs childFlow）、--register 声明式节点注册、裸调用
打印版本。
"""

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

from plaita.cli import main

_FLOW_SRC = '''
def hello(INPUT):
    name = F.upper(INPUT.name)
    return F.concat("hi ", name)
'''

_WHILE_SRC = '''
def drain(INPUT):
    while item.n > 0:
        return {"n": item.n - 1}
    return "done"
'''

_REGISTER_HELPER = tempfile.mkdtemp(prefix="plaita-cli-test-mods-")

with open(Path(_REGISTER_HELPER) / "fake_nodes.py", "w", encoding="utf-8") as _f:
    _f.write("CALLS = []\n\n\ndef register_all():\n    CALLS.append(1)\n")

sys.path.insert(0, _REGISTER_HELPER)


class TestCliBuild(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="plaita-cli-build-")
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name)

    def _write_source(self, name: str, text: str) -> Path:
        src = self.dir / name
        src.write_text(text, encoding="utf-8")
        return src

    def test_build_default_out_path_and_summary(self):
        src = self._write_source("hello_flow.py", _FLOW_SRC)
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = main(["build", str(src)])
        self.assertEqual(rc, 0, err.getvalue())
        artifact = self.dir / "hello_flow.plaita.json"
        self.assertTrue(artifact.is_file())
        doc = json.loads(artifact.read_text(encoding="utf-8"))
        self.assertEqual(doc["flow_id"], "hello")
        self.assertIn("3 nodes", out.getvalue())

    def test_build_o_flag(self):
        src = self._write_source("hello_flow.py", _FLOW_SRC)
        out_path = self.dir / "custom" / "h.flow.json"
        out_path.parent.mkdir()
        rc = main(["build", str(src), "-o", str(out_path)])
        self.assertEqual(rc, 0)
        self.assertTrue(out_path.is_file())

    def test_check_in_sync_and_stale_and_missing(self):
        src = self._write_source("hello_flow.py", _FLOW_SRC)
        self.assertEqual(main(["build", str(src)]), 0)
        # 一致 → 0
        self.assertEqual(main(["build", str(src), "--check"]), 0)
        # 产物落后（手改产物模拟）→ 1，stderr 带 diff 头
        artifact = self.dir / "hello_flow.plaita.json"
        artifact.write_text('{"nodes": []}\n', encoding="utf-8")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = main(["build", str(src), "--check"])
        self.assertEqual(rc, 1)
        self.assertIn("compiled", err.getvalue())
        self.assertIn("产物落后源码", err.getvalue())
        # 产物不存在 → 1
        artifact.unlink()
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(main(["build", str(src), "--check"]), 1)

    def test_format_ir_keeps_snake_child_flow(self):
        src = self._write_source("drain_flow.py", _WHILE_SRC)
        self.assertEqual(main(
            ["build", str(src), "--format", "ir",
             "-o", str(self.dir / "ir.json")]), 0)
        ir_doc = json.loads((self.dir / "ir.json").read_text(encoding="utf-8"))
        while_node = [n for n in ir_doc["nodes"] if n["type"] == "while"][0]
        self.assertIn("child_flow", while_node)
        # canonical（默认）→ childFlow
        self.assertEqual(main(
            ["build", str(src), "-o", str(self.dir / "canon.json")]), 0)
        canon_doc = json.loads((self.dir / "canon.json").read_text(encoding="utf-8"))
        while_node = [n for n in canon_doc["nodes"] if n["type"] == "while"][0]
        self.assertIn("childFlow", while_node)
        self.assertNotIn("child_flow", while_node)

    def test_register_module_called_before_compile(self):
        import fake_nodes  # noqa: F401  # _REGISTER_HELPER 已入 sys.path

        src = self._write_source("hello_flow.py", _FLOW_SRC)
        before = len(fake_nodes.CALLS)
        rc = main(["build", str(src), "--register", "fake_nodes"])
        self.assertEqual(rc, 0)
        self.assertEqual(len(fake_nodes.CALLS), before + 1)

    def test_missing_source_is_exit_2(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = main(["build", str(self.dir / "nope.py")])
        self.assertEqual(rc, 2)
        self.assertIn("源码不存在", err.getvalue())

    def test_bare_invocation_prints_version(self):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = main([])
        self.assertEqual(rc, 0)
        self.assertTrue(out.getvalue().strip())


if __name__ == "__main__":
    unittest.main()
