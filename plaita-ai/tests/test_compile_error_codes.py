"""CompileError code/fix — stable error codes on the MCP error envelope (plaita#351 P2-2).

``errors[]`` rows gain sparse ``code`` (stable machine key) and ``fix`` (repair
hint) fields alongside the legacy ``{line, message}`` — consumers that only
read line/message see an unchanged shape; unmapped errors carry neither new
key.
"""
from __future__ import annotations

import json
import os
import sys
import types

_HERE = os.path.dirname(__file__)
_PLAITA_AI = os.path.dirname(_HERE)
if _PLAITA_AI not in sys.path:
    sys.path.insert(0, _PLAITA_AI)

from plaita_ai.flow_runner import compile_flow


def _ensure_importable_server():
    """真实 mcp 优先；未安装时注入最小 stub（同 test_mcp_integration 约定）。"""
    try:
        import mcp.server.fastmcp  # noqa: F401
        return
    except ImportError:
        pass

    class _MockFastMCP:
        def __init__(self, name, **kwargs):
            self.name = name
            self.instructions = kwargs.get("instructions", "")

        def tool(self, *args, **kwargs):
            def decorator(fn):
                return fn
            return args[0] if args and callable(args[0]) else decorator

        def run(self):
            pass

    mcp_pkg = types.ModuleType("mcp")
    mcp_server = types.ModuleType("mcp.server")
    mcp_fastmcp = types.ModuleType("mcp.server.fastmcp")
    mcp_fastmcp.FastMCP = _MockFastMCP
    mcp_pkg.server = mcp_server
    mcp_server.fastmcp = mcp_fastmcp
    sys.modules.setdefault("mcp", mcp_pkg)
    sys.modules.setdefault("mcp.server", mcp_server)
    sys.modules.setdefault("mcp.server.fastmcp", mcp_fastmcp)
    for _mod in list(sys.modules.keys()):
        if _mod.startswith("plaita_ai.mcp"):
            del sys.modules[_mod]


def test_fstring_error_carries_code_and_fix():
    result = compile_flow('@flow("t")\ndef t(INPUT):\n    return f"hi {INPUT.name}"\n')
    assert not result.ok
    row = result.errors[0].to_dict()
    assert row["code"] == "fstring_unsupported"
    assert "F.concat" in row["fix"]
    assert row["line"] == 3  # 既有 line 面不变


def test_documented_anti_patterns_map_to_stable_codes():
    cases = {
        "被多次赋值": "duplicate_assignment",
        "的赋值右侧引用了它自己": "self_reference_assignment",
        "迭代对象必须是": "for_iter_unsupported",
        "下标只支持整数常量": "subscript_unsupported",
        "caps 字段不支持动态展开": "dynamic_kwargs_unsupported",
        "未注册的自定义节点": "unregistered_node",
    }
    sources = {
        "被多次赋值": '@flow("t")\ndef t(INPUT):\n    if INPUT.a:\n        r = 1\n        return r\n    r = 2\n    return r\n',
        "的赋值右侧引用了它自己": '@flow("t")\ndef t(INPUT):\n    i = 0\n    while i < 3:\n        i = i + 1\n    return i\n',
        "迭代对象必须是": '@flow("t")\ndef t(INPUT):\n    for i in range(3):\n        return i\n    return None\n',
        "下标只支持整数常量": '@flow("t")\ndef t(INPUT):\n    n = INPUT["k"]\n    return n\n',
        "caps 字段不支持动态展开": '@flow("t")\ndef t(INPUT):\n    extra = {"timeout": "PT5S"}\n    r = HTTP.get(url="http://x", **extra)\n    return r\n',
        "未注册的自定义节点": '@flow("t")\ndef t(INPUT):\n    out = FOO(x=1)\n    return out\n',
    }
    for needle, code in cases.items():
        result = compile_flow(sources[needle])
        assert not result.ok, needle
        error = result.errors[0]
        assert needle in error.message, needle
        assert error.code == code, f"{needle}: {error.code}"
        assert error.fix, needle


def test_unmapped_error_keeps_legacy_shape():
    """无映射的报错不产出 code/fix 键——旧 {line, message} 消费者逐字节兼容。"""
    result = compile_flow("def broken(:\n")  # 语法错误，不在映射表
    assert not result.ok
    row = result.errors[0].to_dict()
    assert set(row) == {"line", "message"}


def test_mcp_flow_compile_envelope_includes_code():
    _ensure_importable_server()
    from plaita_ai.mcp.server import flow_compile

    payload = json.loads(flow_compile('@flow("t")\ndef t(INPUT):\n    return f"x {INPUT.a}"\n'))
    assert payload["ok"] is False
    assert payload["errors"][0]["code"] == "fstring_unsupported"
    assert payload["errors"][0]["fix"]
