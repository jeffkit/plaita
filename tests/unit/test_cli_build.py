"""``python -m plaita build`` CLI 测试。

钉：默认产物路径、--check 三态（一致/落后/缺产物）、--format ir 与 canonical
的形态差异（child_flow vs childFlow）、--register 声明式节点注册、裸调用
打印版本。
"""

import contextlib
import io
import json
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

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

    def test_build_out_equals_source_refused(self):
        """产物路径与源码解析后相同（含 /./ 与 sub/../ 写法）时拒绝落盘，源码逐字节不变。"""
        src = self._write_source("hello_flow.py", _FLOW_SRC)
        before = src.read_bytes()
        sub = self.dir / "sub"
        sub.mkdir()
        # 注意：/./ 与 sub/.. 必须留在**字符串**里，Path 会就地折叠它们
        spellings = [
            str(src),
            f"{self.dir}/./{src.name}",
            str(sub / ".." / src.name),
        ]
        for spelling in spellings:
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = main(["build", spelling, "-o", spelling])
            self.assertEqual(rc, 2, f"{spelling}: {err.getvalue()}")
            self.assertIn("拒绝覆盖权威源码", err.getvalue())
            self.assertEqual(src.read_bytes(), before, "源码内容不得被产物覆盖")
            self.assertEqual(err.getvalue().count("拒绝覆盖权威源码"), 1)

    def test_build_out_symlink_to_source_refused(self):
        """符号链接指向源码时同样拦下（resolve() 归一）。"""
        src = self._write_source("hello_flow.py", _FLOW_SRC)
        link = self.dir / "alias.py"
        try:
            link.symlink_to(src)
        except (OSError, NotImplementedError):
            self.skipTest("本平台不支持符号链接")
        before = src.read_bytes()
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = main(["build", str(src), "-o", str(link)])
        self.assertEqual(rc, 2, err.getvalue())
        self.assertIn("拒绝覆盖权威源码", err.getvalue())
        self.assertEqual(src.read_bytes(), before)

    def test_build_o_creates_missing_parent(self):
        """-o 的父目录缺失按需创建（不再抛裸 FileNotFoundError）。"""
        src = self._write_source("hello_flow.py", _FLOW_SRC)
        out_path = self.dir / "custom" / "h.flow.json"
        self.assertFalse(out_path.parent.exists())
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = main(["build", str(src), "-o", str(out_path)])
        self.assertEqual(rc, 0, err.getvalue())
        self.assertTrue(out_path.is_file())
        self.assertNotIn("Traceback", err.getvalue())
        self.assertEqual(json.loads(out_path.read_text(encoding="utf-8"))["flow_id"], "hello")

    def test_build_atomic_write_keeps_old_artifact_on_failure(self):
        """原子落盘：替换步骤失败时旧产物逐字节完好，且不留临时文件。

        用 mock 让 ``os.replace`` 失败来模拟落盘中段崩溃；若实现退化成
        ``write_text`` 直写，本用例会因「没有失败」而变红（有鉴别力）。
        """
        from unittest import mock

        src = self._write_source("hello_flow.py", _FLOW_SRC)
        out_path = self.dir / "custom" / "h.flow.json"
        self.assertEqual(main(["build", str(src), "-o", str(out_path)]), 0)
        previous = out_path.read_text(encoding="utf-8")

        # 改动源码 ⇒ 重编译产物必然与旧产物不同
        src.write_text(_FLOW_SRC.replace("hi ", "hey "), encoding="utf-8")
        with mock.patch("plaita.cli.os.replace", side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                main(["build", str(src), "-o", str(out_path)])

        self.assertEqual(out_path.read_text(encoding="utf-8"), previous,
                         "替换失败后旧产物必须保持完整（不得半截/被截断）")
        residue = [p.name for p in out_path.parent.iterdir() if p.name.endswith(".tmp")]
        self.assertEqual(residue, [], "失败后不得留下临时文件")

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

    def test_check_without_flag_on_embedded_artifact_passes(self):
        """plaita#90 验收 1：嵌入产物 + 无 flag ``--check`` 不得假红。

        修复前：重编译基准不含 metadata.source，committed 嵌入产物必然
        exit=1 且 diff 出现 ``-  "metadata": {``，提示照做即删数据。
        """
        src = self._write_source("hello_flow.py", _FLOW_SRC)
        out_path = self.dir / "embed.json"
        self.assertEqual(
            main(["build", str(src), "-o", str(out_path), "--embed-source"]), 0)
        committed = out_path.read_text(encoding="utf-8")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = main(["build", str(src), "-o", str(out_path), "--check"])
        self.assertEqual(rc, 0, err.getvalue())
        self.assertNotIn("-  \"metadata\": {", err.getvalue())
        # --check 不落盘：产物逐字节保持
        self.assertEqual(out_path.read_text(encoding="utf-8"), committed)
        md = json.loads(committed)["metadata"]
        self.assertEqual(md["source"], _FLOW_SRC)
        self.assertEqual(md["source_format"], "plaita@flow")

    def test_plain_rebuild_preserves_embedded_metadata(self):
        """plaita#90 验收 2：嵌入产物被无 flag build「按提示同步」后 metadata 仍在且未变。"""
        src = self._write_source("hello_flow.py", _FLOW_SRC)
        out_path = self.dir / "embed.json"
        self.assertEqual(
            main(["build", str(src), "-o", str(out_path), "--embed-source"]), 0)
        before = out_path.read_text(encoding="utf-8")
        self.assertEqual(main(["build", str(src), "-o", str(out_path)]), 0)
        after = out_path.read_text(encoding="utf-8")
        self.assertEqual(before, after, "同步必须字节级保留已提交的嵌入产物")
        doc = json.loads(after)
        self.assertEqual(doc["metadata"]["source"], _FLOW_SRC)
        self.assertEqual(doc["metadata"]["source_format"], "plaita@flow")
        # 同步后带 flag 的 check 也一致（修复前此方向会反转成假红）
        self.assertEqual(main(
            ["build", str(src), "-o", str(out_path), "--embed-source",
             "--check"]), 0)

    def test_stale_embedded_artifact_check_diff_keeps_metadata(self):
        """plaita#90：源码真落后时无 flag check 仍报红，但 diff 不得指向删 metadata。"""
        src = self._write_source("hello_flow.py", _FLOW_SRC)
        out_path = self.dir / "embed.json"
        self.assertEqual(
            main(["build", str(src), "-o", str(out_path), "--embed-source"]), 0)
        src.write_text(_FLOW_SRC.replace("hi ", "hey "), encoding="utf-8")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = main(["build", str(src), "-o", str(out_path), "--check"])
        self.assertEqual(rc, 1)
        self.assertIn("产物落后源码", err.getvalue())
        self.assertNotIn("-  \"metadata\": {", err.getvalue())
        # 按提示重跑同步：metadata.source 跟进到新源码原文，而不是被删除
        self.assertEqual(main(["build", str(src), "-o", str(out_path)]), 0)
        md = json.loads(out_path.read_text(encoding="utf-8"))["metadata"]
        self.assertEqual(md["source"], _FLOW_SRC.replace("hi ", "hey "))

    def test_committed_metadata_requires_source_key(self):
        """plaita#90 守卫边界：committed 只有非 source 的 metadata 时不得误继承嵌入。

        ``@flow(metadata=...)`` 声明的 metadata 不带 --embed-source 也会进产物
        （无 source 键）；此形态必须维持原口径——不得因「有 metadata」就误判
        为已嵌入而注入 source（误注入会让 check 立即红、重建产物漂移）。
        """
        src = self._write_source(
            "meta_flow.py",
            '@flow(metadata={"team": "keeper"})\ndef m(INPUT):\n    return 1\n')
        out_path = self.dir / "meta.json"
        self.assertEqual(main(["build", str(src), "-o", str(out_path)]), 0)
        doc = json.loads(out_path.read_text(encoding="utf-8"))
        self.assertEqual(doc["metadata"], {"team": "keeper"})
        # 无 flag check：重编译同 metadata ⇒ 本就一致（误注入 source 会假红）
        self.assertEqual(main(
            ["build", str(src), "-o", str(out_path), "--check"]), 0)
        # 无 flag 重建：同样不得注入 source
        self.assertEqual(main(["build", str(src), "-o", str(out_path)]), 0)
        doc = json.loads(out_path.read_text(encoding="utf-8"))
        self.assertEqual(doc["metadata"], {"team": "keeper"})

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

_CODE_PLAIN_SRC = '''
def hello(INPUT):
    a = 1
    return a
'''

_CODE_UNSAFE_SRC = '''
def hello(INPUT):
    a = CODE(code="print(1)", lang="python", sandbox_backend="unsafe")
    return a
'''

_CODE_JS_SRC = '''
def hello(INPUT):
    a = CODE(code="print(1)", lang="js")
    return a
'''


class TestCliSandboxAllowlist(unittest.TestCase):
    """plaita#114/#115：CLI 的白名单口径与落盘门必须与 worker/console 同源。

    #115：白名单曾自己按逗号切分 env，空白/冒号/分号写法被当成一个后端名（未捕获
    ValueError）；#114：落盘门只过 ``validate_flow_ir``，node-model 级约束（沙箱
    后端 / 语言白名单）整段空转，``sandbox_backend="unsafe"`` 的产物照样 rc=0 落盘。
    """

    def setUp(self):
        import plaita.node.code as code_mod

        self._mod = code_mod
        self._saved = (
            code_mod._ALLOWED_SANDBOX_BACKENDS,
            code_mod._DEFAULT_SANDBOX_BACKEND,
            getattr(code_mod, "_ALLOWED_LANGUAGES", None),
        )
        self._tmp = tempfile.TemporaryDirectory(prefix="plaita-cli-sandbox-")
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name)
        for key in ("PLAITA_SANDBOX_ALLOWED_BACKENDS",
                    "PLAITA_SANDBOX_ALLOWED_LANGUAGES",
                    "PLAITA_CODE_BACKEND"):
            os.environ.pop(key, None)
            self.addCleanup(os.environ.pop, key, None)

    def tearDown(self):
        (self._mod._ALLOWED_SANDBOX_BACKENDS,
         self._mod._DEFAULT_SANDBOX_BACKEND,
         saved_langs) = self._saved
        if saved_langs is not None:
            self._mod._ALLOWED_LANGUAGES = saved_langs

    def _write(self, text: str, name: str) -> Path:
        path = self.dir / name
        path.write_text(text, encoding="utf-8")
        return path

    def test_code_backend_env_separators_accepted_and_byte_identical(self):
        """逗号/空白/冒号/分号四种分隔写法都要 rc=0，且产物逐字节一致（#115）。"""
        src = self._write(_CODE_PLAIN_SRC, "hello.py")
        artifacts = []
        for spelling in ("docker,subprocess", "docker subprocess",
                         "docker:subprocess", "docker;subprocess"):
            out = self.dir / f"out_{re.sub(r'[^a-z]', '_', spelling)}.json"
            with mock.patch.dict(
                    os.environ, {"PLAITA_SANDBOX_ALLOWED_BACKENDS": spelling}, clear=False):
                err = io.StringIO()
                with contextlib.redirect_stderr(err):
                    rc = main(["build", str(src), "--code-backend", "subprocess",
                               "-o", str(out)])
            self.assertEqual(rc, 0, f"{spelling}: {err.getvalue()}")
            self.assertTrue(out.is_file(), spelling)
            json.loads(out.read_text(encoding="utf-8"))
            artifacts.append(out.read_bytes())
        self.assertEqual(len(set(artifacts)), 1, "四种分隔写法必须产出逐字节一致的产物")

    def test_code_backend_env_invalid_name_reports_resolver_wording(self):
        """非法后端名要来自权威解析器口径，而不是 register_code_node 的报错（#115）。"""
        src = self._write(_CODE_PLAIN_SRC, "hello.py")
        with mock.patch.dict(
                os.environ, {"PLAITA_SANDBOX_ALLOWED_BACKENDS": "docker bogus"}, clear=False):
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = main(["build", str(src), "--code-backend", "subprocess",
                           "-o", str(self.dir / "o.json")])
        self.assertEqual(rc, 2, err.getvalue())
        self.assertIn("PLAITA_SANDBOX_ALLOWED_BACKENDS", err.getvalue())
        self.assertNotIn("register_code_node: unknown sandbox backend", err.getvalue())

    def test_build_rejects_sandbox_backend_outside_allowlist(self):
        """白名单外的 sandbox_backend 必须被拒且不落盘（#114）；两种 format 同深。"""
        src = self._write(_CODE_UNSAFE_SRC, "unsafe_flow.py")
        for extra in ([], ["--format", "ir"]):
            tag = "ir" if extra else "canonical"
            out = self.dir / f"unsafe_{tag}.json"
            with mock.patch.dict(
                    os.environ, {"PLAITA_SANDBOX_ALLOWED_BACKENDS": "subprocess"}, clear=False):
                err = io.StringIO()
                with contextlib.redirect_stderr(err):
                    rc = main(["build", str(src), "--code-backend", "subprocess",
                               "-o", str(out)] + extra)
            self.assertNotEqual(rc, 0, f"{tag}: 白名单外的 sandbox_backend 必须被拒")
            self.assertFalse(out.exists(), f"{tag}: 拒绝时不得落盘")
            self.assertIn("not allowed by the operator", err.getvalue(), tag)

    def test_build_rejects_language_outside_allowlist(self):
        """语言白名单同断言（#114 验收 3）：js + 白名单 python ⇒ 拒。"""
        src = self._write(_CODE_JS_SRC, "js_flow.py")
        out = self.dir / "js.json"
        with mock.patch.dict(
                os.environ, {"PLAITA_SANDBOX_ALLOWED_LANGUAGES": "python"}, clear=False):
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = main(["build", str(src), "--code-backend", "subprocess", "-o", str(out)])
        self.assertNotEqual(rc, 0, err.getvalue())
        self.assertFalse(out.exists())
        self.assertIn("not allowed by the operator", err.getvalue())

    def test_build_accepts_allowlisted_backend_and_artifact_validates(self):
        """反向用例（防误伤）：白名单内、源码不声明 sandbox_backend ⇒ rc=0 且产物可过模型校验。"""
        from plaita.core.flow import Flow

        src = self._write(_CODE_PLAIN_SRC, "hello.py")
        out = self.dir / "ok.json"
        with mock.patch.dict(
                os.environ, {"PLAITA_SANDBOX_ALLOWED_BACKENDS": "subprocess"}, clear=False):
            self.assertEqual(
                main(["build", str(src), "--code-backend", "subprocess", "-o", str(out)]), 0)
        Flow.model_validate(json.loads(out.read_text(encoding="utf-8")))
