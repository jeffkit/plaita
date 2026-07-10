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
    load_tool_bundle("tools.yaml")
"""

from plaita_ai.tools.registry import (
    config_to_source,
    load_tool_bundle,
    register_source,
    register_sources,
    schema_from_source,
)
from plaita_ai.tools.source import (
    SOURCE_TYPES,
    BaseToolSource,
    HttpToolSource,
    NativeToolSource,
    ParamDef,
    ToolContext,
    build_tool_context,
)

__all__ = [
    "SOURCE_TYPES",
    "BaseToolSource",
    "HttpToolSource",
    "NativeToolSource",
    "ParamDef",
    "ToolContext",
    "build_tool_context",
    "config_to_source",
    "load_tool_bundle",
    "register_source",
    "register_sources",
    "schema_from_source",
]
