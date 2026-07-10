"""plaita_ai.tools — 数据源工具统一抽象（代码轨 + 配置轨）。

用法::

    # 代码轨
    from plaita_ai.tools import HttpToolSource, register_source

    register_source(HttpToolSource(
        name="get_user",
        description="查询用户",
        url="https://api.example.com/users/{user_id}",
        params={"user_id": {"type": "string", "required": True}},
    ))

    # 配置轨
    from plaita_ai.tools import load_tool_bundle
    load_tool_bundle("tools.yaml", "resources.yaml")

LangChain 适配（``register_langchain_tool`` 等）为 **可选**：
需安装 ``plaita-ai[agent]`` / ``langchain-core``，通过 ``__getattr__`` 惰性导出，
未安装时不影响本包其余 API。
"""

from __future__ import annotations

from typing import Any

from plaita_ai.tools.addressing import (
    apply_addressing,
    clear_addressing,
    list_addressing,
    register_addressing,
)
from plaita_ai.tools.bootstrap import (
    ENV_RESOURCES,
    ENV_TOOLS,
    load_tools_from_env,
    validate_tool_bundle,
)
from plaita_ai.tools.registry import (
    config_to_source,
    load_tool_bundle,
    register_source,
    register_sources,
    schema_from_source,
)
from plaita_ai.tools.resources import (
    clear_resources,
    register_datasource,
    register_vectorstore,
)
from plaita_ai.tools.source import (
    SOURCE_TYPES,
    BaseToolSource,
    HttpToolSource,
    NativeToolSource,
    ParamDef,
    SqlToolSource,
    ToolContext,
    VectorToolSource,
    build_tool_context,
)

# LangChain 相关符号惰性导出，避免 import plaita_ai.tools 时假设 langchain 存在
_LANGCHAIN_EXPORTS = frozenset({
    "adapt_langchain_tool",
    "register_langchain_tool",
    "register_langchain_toolkit",
})

__all__ = [
    "SOURCE_TYPES",
    "ENV_RESOURCES",
    "ENV_TOOLS",
    "BaseToolSource",
    "HttpToolSource",
    "NativeToolSource",
    "SqlToolSource",
    "VectorToolSource",
    "ParamDef",
    "ToolContext",
    "adapt_langchain_tool",
    "apply_addressing",
    "build_tool_context",
    "clear_addressing",
    "clear_resources",
    "config_to_source",
    "list_addressing",
    "load_tool_bundle",
    "load_tools_from_env",
    "register_addressing",
    "register_datasource",
    "register_langchain_tool",
    "register_langchain_toolkit",
    "register_source",
    "register_sources",
    "register_vectorstore",
    "schema_from_source",
    "validate_tool_bundle",
]


def __getattr__(name: str) -> Any:
    if name in _LANGCHAIN_EXPORTS:
        from plaita_ai.tools import langchain as _lc

        return getattr(_lc, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
