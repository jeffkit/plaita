"""公共子进程环境白名单（``plaita.subprocess_env``）契约测试。

2026-09 安全评审 P1 的跨仓前提：code 节点的 env 白名单抽到公共层后，
gate / capture / agent 直跑（plaita-nodes）能 import 同一套策略。这里钉死
三件事——宿主白名单外不泄露、extra 合并顺序、截断头尾保留。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from unittest import TestCase
from unittest.mock import patch

import plaita.node.code as code_module
from plaita.subprocess_env import (
    SUBPROCESS_ENV_ALLOWLIST,
    build_subprocess_env,
    clip_output,
)


class TestBuildSubprocessEnv(TestCase):
    def test_allowlisted_host_keys_are_copied(self):
        with patch.dict(os.environ, {"PATH": "/usr/bin"}, clear=False):
            env = build_subprocess_env()
        self.assertEqual(env["PATH"], "/usr/bin")

    def test_non_allowlisted_host_keys_are_dropped(self):
        """白名单外的宿主变量（API key / DB URL / 凭据密钥）绝不进子进程。"""
        with patch.dict(os.environ, {"PLAITA_TEST_SECRET_DO_NOT_LEAK": "s3cr3t"},
                        clear=False):
            env = build_subprocess_env()
        self.assertNotIn("PLAITA_TEST_SECRET_DO_NOT_LEAK", env)

    def test_module_and_call_extra_merge_with_call_wins(self):
        with patch.dict(os.environ, {"PATH": "/usr/bin"}, clear=False):
            with patch.dict(code_module.SUBPROCESS_ENV_EXTRA,
                            {"RECURSIVE_BIN": "recursive", "SAME": "module"}):
                env = build_subprocess_env(extra={"SAME": "call", "NUM": 7})
        self.assertEqual(env["RECURSIVE_BIN"], "recursive")
        self.assertEqual(env["SAME"], "call")
        self.assertEqual(env["NUM"], "7")  # 子进程 env 只接受 str，统一转换

    def test_missing_allowlisted_key_is_skipped(self):
        """宿主没有的键不塞空串——避免子进程看到 ``HOME=""`` 这类假值。"""
        with patch.dict(os.environ, {}, clear=True):
            with patch.dict(code_module.SUBPROCESS_ENV_EXTRA, {}, clear=True):
                env = build_subprocess_env()
        self.assertEqual(env, {})

    def test_allowlist_has_no_credential_shaped_keys(self):
        for key in SUBPROCESS_ENV_ALLOWLIST:
            self.assertNotIn("KEY", key)
            self.assertNotIn("TOKEN", key)
            self.assertNotIn("SECRET", key)
            self.assertNotIn("PASSWORD", key)


class TestPublicContractForNodes(TestCase):
    def test_rebuilt_env_does_not_leak_host_secret_to_child(self):
        """端到端：按公开层重建的 env 交给子进程，宿主凭据不可见。

        这正是 plaita-nodes 的 gate/capture 要走的路径（替换 os.environ.copy()）。
        """
        with patch.dict(os.environ, {"PLAITA_TEST_SECRET_DO_NOT_LEAK": "s3cr3t"},
                        clear=False):
            proc = subprocess.run(
                [sys.executable, "-c",
                 "import os,json;print(json.dumps(sorted(os.environ)))"],
                env=build_subprocess_env(), capture_output=True, text=True, timeout=30,
            )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertNotIn("PLAITA_TEST_SECRET_DO_NOT_LEAK", json.loads(proc.stdout))


class TestClippedOutput(TestCase):
    def test_under_cap_is_untouched(self):
        self.assertEqual(clip_output("hi", 10), "hi")

    def test_exactly_cap_is_untouched(self):
        text = "x" * 10
        self.assertEqual(clip_output(text, 10), text)

    def test_over_cap_keeps_head_and_tail_with_marker(self):
        text = "H" * 100 + "T" * 100
        out = clip_output(text, 40)
        self.assertTrue(out.startswith("H" * 10))   # 头 1/4
        self.assertTrue(out.endswith("T" * 30))     # 尾 3/4：失败摘要/traceback 在这
        self.assertIn("…[省略 160 字符]…", out)

    def test_non_positive_cap_disables_clipping(self):
        self.assertEqual(clip_output("abc", 0), "abc")
        self.assertEqual(clip_output("abc", -1), "abc")


class TestCodeNodeUsesSharedLayer(TestCase):
    def test_code_node_reexports_are_the_shared_objects(self):
        from plaita import subprocess_env as shared
        self.assertIs(code_module.SUBPROCESS_ENV_ALLOWLIST,
                      shared.SUBPROCESS_ENV_ALLOWLIST)
        self.assertIs(code_module.SUBPROCESS_ENV_EXTRA, shared.SUBPROCESS_ENV_EXTRA)

    def test_module_extra_reaches_sandboxed_subprocess(self):
        """code 节点的模块级 extra 仍生效（re-export 是同一 dict，非拷贝）。"""
        from plaita.node.code import run_python_subprocess
        with patch.dict(code_module.SUBPROCESS_ENV_EXTRA,
                        {"PLAITA_TEST_INJECTED": "yes"}):
            result = run_python_subprocess(
                "def run(i):\n"
                "    import os\n"
                "    return {'injected': os.environ.get('PLAITA_TEST_INJECTED')}\n",
                {},
            )
        self.assertEqual(result["injected"], "yes")
