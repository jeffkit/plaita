"""Ensure LangChain remains an optional dependency for plaita_ai.tools."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


def test_core_tools_api_with_base_tool_disabled(monkeypatch):
    """模拟未安装 langchain：_BaseTool=None 时核心注册仍可用。"""
    import plaita_ai.agent.fot.tools as fot_tools
    import plaita_ai.tools.langchain as lc_mod

    monkeypatch.setattr(fot_tools, "_BaseTool", None)
    monkeypatch.setattr(lc_mod, "_BaseTool", None)
    monkeypatch.setattr(lc_mod, "_BaseToolkit", None)

    from plaita_ai.agent.fot.tools import ToolNode, register_tool_node, tool
    from plaita_ai.tools import HttpToolSource

    assert HttpToolSource is not None

    @tool
    def ping() -> str:
        """ping"""
        return "pong"

    ToolNode.clear()
    specs = register_tool_node(ping)
    assert specs[0].name == "ping"
    assert ToolNode.get_tool("ping")() == "pong"

    with pytest.raises(ImportError, match="langchain-core"):
        lc_mod.register_langchain_toolkit([])


def test_tools_import_without_langchain_subprocess():
    """子进程阻断 langchain 后仍可 import 核心 tools API（不污染本进程模块缓存）。"""
    plaita_ai_root = Path(__file__).resolve().parents[1]
    repo_root = plaita_ai_root.parent
    code = """
import builtins, sys
real = builtins.__import__
def blocked(name, globals=None, locals=None, fromlist=(), level=0):
    if name == "langchain" or name.startswith("langchain.") or name == "langchain_core" or name.startswith("langchain_core."):
        raise ImportError("blocked " + name)
    return real(name, globals, locals, fromlist, level)
builtins.__import__ = blocked
for k in list(sys.modules):
    if k == "langchain" or k.startswith("langchain.") or k == "langchain_core" or k.startswith("langchain_core."):
        del sys.modules[k]

from plaita_ai.tools import HttpToolSource, register_source, load_tool_bundle
from plaita_ai.agent.fot.tools import ToolNode, register_tool_node, tool
assert HttpToolSource is not None

@tool
def ping() -> str:
    "ping"
    return "pong"

ToolNode.clear()
assert register_tool_node(ping)[0].name == "ping"

from plaita_ai.tools.langchain import register_langchain_toolkit
try:
    register_langchain_toolkit([])
except ImportError as e:
    assert "langchain-core" in str(e)
else:
    raise SystemExit("expected ImportError")
print("ok")
"""
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(plaita_ai_root), str(repo_root)]),
    }
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(plaita_ai_root),
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "ok" in proc.stdout
