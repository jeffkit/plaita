"""test_loop_policy — PLAITA_LOOP=uvloop 可选事件循环加速。

进程级 opt-in 在 ``plaita/__init__.py`` import 时生效，因此用子进程测试
（env + 假 uvloop 模块注入 PYTHONPATH，真 uvloop 不进 CI 依赖）：

* 未设 env / 设了未知值：行为不变（default loop）；
* PLAITA_LOOP=uvloop 且可导入：policy 被替换（本测试以假模块的 install()
  被调用为证据，不真装 uvloop——Windows CI 安全）；
* PLAITA_LOOP=uvloop 但模块缺失：仅 debug 留痕，import 不炸、行为不变。
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path
from unittest import TestCase

_REPO_ROOT = Path(__file__).resolve().parents[2]

PROBE = textwrap.dedent("""
    import asyncio
    import plaita
    loop = asyncio.new_event_loop()
    print(type(loop).__module__)
    loop.close()
""")

FAKE_UVLOOP = textwrap.dedent("""
    import asyncio
    installed = False
    class EventLoop(asyncio.SelectorEventLoop):
        pass
    def install():
        global installed
        installed = True
        asyncio.set_event_loop_policy(_FakePolicy())
    class _FakePolicy(asyncio.DefaultEventLoopPolicy):
        def new_event_loop(self):
            return EventLoop()
""")


def _run_probe(env_extra: dict, with_fake_uvloop: bool) -> str:
    env = os.environ.copy()
    env.update(env_extra)
    env["PYTHONPATH"] = str(_REPO_ROOT)
    if with_fake_uvloop:
        tmp = tempfile.mkdtemp(prefix="fake-uvloop-")
        (Path(tmp) / "uvloop.py").write_text(FAKE_UVLOOP, encoding="utf-8")
        env["PYTHONPATH"] = f"{tmp}{os.pathsep}{_REPO_ROOT}"
    r = subprocess.run([sys.executable, "-c", PROBE], env=env,
                       capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        raise AssertionError(f"probe failed: {r.stderr[-500:]}")
    return r.stdout.strip()


class TestLoopPolicy(TestCase):
    def test_default_policy_without_env(self):
        self.assertEqual(_run_probe({}, with_fake_uvloop=False), "asyncio.unix_events")

    def test_unknown_env_ignored(self):
        self.assertEqual(_run_probe({"PLAITA_LOOP": "bogus"}, with_fake_uvloop=False),
                         "asyncio.unix_events")

    def test_uvloop_env_installs_policy(self):
        """env 命中 + 假模块：policy 被替换（新 loop 来自假模块）。"""
        self.assertIn("uvloop", _run_probe({"PLAITA_LOOP": "uvloop"},
                                           with_fake_uvloop=True))

    def test_uvloop_env_without_module_is_noop(self):
        """env 命中但模块缺失：import 不炸、行为不变。"""
        self.assertEqual(_run_probe({"PLAITA_LOOP": "uvloop"}, with_fake_uvloop=False),
                         "asyncio.unix_events")
        # 未知值同理忽略，已在 test_unknown_env_ignored 覆盖


if __name__ == "__main__":  # pragma: no cover
    import unittest

    unittest.main()
