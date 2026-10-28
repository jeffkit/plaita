"""flow_describe_node — node field schema introspection (plaita#351 P0-2).

Business node fields (AGENTRUN/HITL/connectors) were previously unqueryable:
flow_list_nodes carries only tier/suspends, so AI authors guessed field names
and hit build-time pydantic errors with no source line. These tests pin the
new introspection surface: pydantic schema + minimal example kwargs + notes
(reserved fields / expression-capable fields), for builtin, custom and
unknown node types.
"""
from __future__ import annotations

import json
import os
import sys
import types
from typing import Any, ClassVar, Optional

_HERE = os.path.dirname(__file__)
_PLAITA_AI = os.path.dirname(_HERE)
if _PLAITA_AI not in sys.path:
    sys.path.insert(0, _PLAITA_AI)

from plaita.node import get_default_registry

from plaita_ai.flow_runner import describe_node_type


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

try:  # plaita-nodes 仅本机/插件 job 安装；主 plaita-ai CI 无插件
    import plaita_nodes  # noqa: F401

    HAS_PLAITA_NODES = True
except ImportError:
    HAS_PLAITA_NODES = False


def test_builtin_http_schema_example_and_notes():
    d = describe_node_type("http")
    assert d["ok"] is True
    assert d["placeholder"] == "HTTP"
    assert d["tier"] in ("inline", "managed", "managed:queue")
    props = d["schema"]["properties"]
    assert "url" in props
    # 最小可用 kwargs = 必填且非基类字段
    assert d["example"] == {"url": "<url>"}
    # 保留字段说明 + 表达式字段（body/headers 接 $INPUT.x）
    assert set(d["notes"]["reserved_fields"]) == {"id", "timeout", "on_error"}
    assert {"body", "headers"} <= set(d["notes"]["expression_fields"])
    assert "next" not in d["notes"]["expression_fields"]  # 拓扑字段不是表达式字段


def test_unknown_type_returns_available_list():
    d = describe_node_type("definitely_not_a_node_type")
    assert d["ok"] is False
    assert "未知节点类型" in d["error"]
    assert isinstance(d["available"], list) and "http" in d["available"]


def test_custom_node_schema_and_expression_fields():
    class LLMNode:
        pass

    from plaita import Node as BaseNode

    class ProbeNode(BaseNode):
        node_type: ClassVar[str] = "probe_describe_351"
        prompt: Optional[str] = None
        payload: Optional[Any] = None
        top_k: int = 3
        mode: str = "fast"

        def execute(self, execution): ...

    registry = get_default_registry()
    registry.register(ProbeNode)
    d = describe_node_type("probe_describe_351")
    assert d["ok"] is True
    props = d["schema"]["properties"]
    assert {"prompt", "payload", "top_k", "mode"} <= set(props)
    # Optional[Any]/Optional[str] 字段识别为可接表达式；强类型不进清单
    assert {"prompt", "payload"} <= set(d["notes"]["expression_fields"])
    assert "top_k" not in d["notes"]["expression_fields"]
    assert "mode" not in d["notes"]["expression_fields"]


def test_mcp_tool_flow_describe_node_json_envelope():
    _ensure_importable_server()
    from plaita_ai.mcp.server import flow_describe_node

    payload = json.loads(flow_describe_node("http"))
    assert payload["ok"] is True and payload["node_type"] == "http"
    bad = json.loads(flow_describe_node("no_such_node"))
    assert bad["ok"] is False and "available" in bad


def test_mcp_flow_list_nodes_docstring_points_at_describe():
    _ensure_importable_server()
    from plaita_ai.mcp import server

    assert "flow_describe_node" in server.flow_list_nodes.__doc__


def test_agentrun_fields_queryable():
    """验收条目：agentrun 能列出 agent/prompt/workspace/repo/timeout_secs 等字段。"""
    if not HAS_PLAITA_NODES:
        import pytest

        pytest.skip("plaita-nodes not installed (main plaita-ai CI has no plugins)")
    d = describe_node_type("agentrun")
    assert d["ok"] is True
    props = d["schema"]["properties"]
    assert {"agent", "prompt", "workspace", "repo", "timeout_secs"} <= set(props)
