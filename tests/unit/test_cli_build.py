"""``python -m plaita build`` CLI 测试。

钉：默认产物路径、--check 三态（一致/落后/缺产物）、--format ir 与 canonical
的形态差异（child_flow vs childFlow）、--register 声明式节点注册、裸调用
打印版本；--code-backend 白名单走权威 resolver（plaita#115：env 分隔符
空白/冒号/分号与逗号同口径、未配置取默认档、非法后端一行报错 rc=2）。
"""

import contextlib
import io
import json
import os
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

    def _set_env(self, name: str, value):
        """设/清 env 并在用例结束后恢复原值（防跨用例污染）。"""
        old = os.environ.get(name)

        def _restore():
            if old is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = old

        self.addCleanup(_restore)
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value

    def _restore_sandbox_globals(self):
        """``register_code_node`` 会改写 code 模块级白名单/默认档，跑完恢复。"""
        import plaita.node.code as code_mod

        saved_allowed = code_mod._ALLOWED_SANDBOX_BACKENDS
        saved_default = code_mod._DEFAULT_SANDBOX_BACKEND

        def _restore():
            code_mod._ALLOWED_SANDBOX_BACKENDS = saved_allowed
            code_mod._DEFAULT_SANDBOX_BACKEND = saved_default

        self.addCleanup(_restore)

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

    def test_embed_source_writes_metadata(self):
        src = self._write_source("hello_flow.py", _FLOW_SRC)
        out_path = self.dir / "embed.json"
        rc = main(["build", str(src), "-o", str(out_path), "--embed-source"])
        self.assertEqual(rc, 0)
        doc = json.loads(out_path.read_text(encoding="utf-8"))
        md = doc["metadata"]
        self.assertEqual(md["source"], _FLOW_SRC)
        self.assertEqual(md["source_format"], "plaita@flow")
        # --check 与嵌入源码兼容：重编译同字节
        self.assertEqual(main(
            ["build", str(src), "-o", str(out_path), "--embed-source",
             "--check"]), 0)

    def test_no_embed_by_default(self):
        src = self._write_source("hello_flow.py", _FLOW_SRC)
        out_path = self.dir / "plain.json"
        self.assertEqual(main(["build", str(src), "-o", str(out_path)]), 0)
        doc = json.loads(out_path.read_text(encoding="utf-8"))
        self.assertNotIn("metadata", doc)

    def test_embed_source_merges_declared_metadata(self):
        src = self._write_source(
            "meta_flow.py",
            '@flow(metadata={"team": "keeper"})\ndef m(INPUT):\n    return 1\n')
        out_path = self.dir / "meta.json"
        self.assertEqual(main(
            ["build", str(src), "-o", str(out_path), "--embed-source"]), 0)
        md = json.loads(out_path.read_text(encoding="utf-8"))["metadata"]
        self.assertEqual(md["team"], "keeper")
        self.assertEqual(md["source_format"], "plaita@flow")

    def test_missing_source_is_exit_2(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = main(["build", str(self.dir / "nope.py")])
        self.assertEqual(rc, 2)
        self.assertIn("源码不存在", err.getvalue())

    def test_code_backend_env_separators_match_resolver(self):
        """plaita#115：env 空白/冒号/分号写法与逗号同解析（权威 resolver 口径）。"""
        self._restore_sandbox_globals()
        src = self._write_source("hello_flow.py", _FLOW_SRC)
        artifacts = []
        for i, env_value in enumerate(("docker subprocess", "docker:subprocess",
                                       "docker;subprocess", "docker,subprocess")):
            self._set_env("PLAITA_SANDBOX_ALLOWED_BACKENDS", env_value)
            out_path = self.dir / f"sep_{i}.json"
            rc = main(["build", str(src), "--code-backend", "subprocess",
                       "-o", str(out_path)])
            self.assertEqual(rc, 0, f"env={env_value!r} 应与 worker/console 同口径")
            self.assertTrue(out_path.is_file())
            artifacts.append(out_path.read_text(encoding="utf-8"))
        # 验收：非逗号写法与逗号写法产物逐字节一致
        self.assertEqual(len(set(artifacts)), 1)

    def test_code_backend_env_unset_uses_resolver_default(self):
        """plaita#115：env 未配置 → 默认档 (docker,) ∪ 生效后端，与 worker 同源。"""
        import plaita.node.code as code_mod

        self._restore_sandbox_globals()
        self._set_env("PLAITA_SANDBOX_ALLOWED_BACKENDS", None)
        src = self._write_source("hello_flow.py", _FLOW_SRC)
        out_path = self.dir / "unset.json"
        rc = main(["build", str(src), "--code-backend", "subprocess",
                   "-o", str(out_path)])
        self.assertEqual(rc, 0)
        self.assertEqual(code_mod._ALLOWED_SANDBOX_BACKENDS,
                         frozenset({"docker", "subprocess"}))

    def test_code_backend_env_invalid_backend_clean_error(self):
        """plaita#115：env 含非法后端 → rc=2 一行报错，文案来自 resolver 口径。"""
        self._restore_sandbox_globals()
        self._set_env("PLAITA_SANDBOX_ALLOWED_BACKENDS", "docker bogus")
        src = self._write_source("hello_flow.py", _FLOW_SRC)
        out_path = self.dir / "bad.json"
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = main(["build", str(src), "--code-backend", "subprocess",
                       "-o", str(out_path)])
        self.assertEqual(rc, 2)
        self.assertIn("PLAITA_SANDBOX_ALLOWED_BACKENDS", err.getvalue())
        self.assertIn("unknown sandbox backend", err.getvalue())
        self.assertNotIn("register_code_node:", err.getvalue())
        self.assertNotIn("Traceback", err.getvalue())
        self.assertFalse(out_path.exists())

    def test_bare_invocation_prints_version(self):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = main([])
        self.assertEqual(rc, 0)
        self.assertTrue(out.getvalue().strip())


if __name__ == "__main__":
    unittest.main()
