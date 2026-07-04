"""Flow-of-Thought (FoT) agent — LangChain 1.x + plaita @flow.

核心工具注册 API (tool / ToolNode / list_tools / register_tools_from_module)
无需 LangChain，直接从本包导入即可使用。FoTAgent 依赖 LangChain，在可选
try/except 块中导入。
"""

from plaita_ai.agent.fot.tools import (
    ToolNode,
    ToolSchema,
    ToolSpec,
    list_tools,
    register_tool_node,
    register_tools_from_module,
    tool,
)

__all__ = [
    "ToolNode",
    "ToolSchema",
    "ToolSpec",
    "list_tools",
    "register_tool_node",
    "register_tools_from_module",
    "tool",
]

try:
    from plaita_ai.agent.fot.agent import FoTAgent, FoTResult
    __all__ += ["FoTAgent", "FoTResult"]
except ImportError:  # pragma: no cover — LangChain optional
    pass
