"""LangChain BaseTool / BaseToolkit → plaita ToolNode 适配器（可选）。

**不**作为 plaita / plaita-ai 的必需依赖。仅在已安装 ``langchain-core``
（或 ``plaita-ai[agent]``）时可用；未安装时本模块仍可 import，但调用
注册 API 会抛出明确的 ``ImportError``。

不做成 NativeToolSource：toolkit 需要实例化与凭证，适合代码轨。

用法::

    from plaita_ai.tools.langchain import (
        register_langchain_tool,
        register_langchain_toolkit,
    )

    register_langchain_toolkit(
        FileManagementToolkit(root_dir=\"/tmp\"),
        prefix=\"fs_\",
        include=[\"read_file\", \"write_file\"],
    )
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Sequence

from plaita_ai.agent.fot.tools import (
    ParamSchema,
    ToolSchema,
    ToolSpec,
    _NO_DEFAULT,
    _TOOL_MARKER_ATTR,
    register_tool_node,
)

logger = logging.getLogger(__name__)

try:
    from langchain_core.tools import BaseTool as _BaseTool
except ImportError:  # pragma: no cover
    _BaseTool = None  # type: ignore[misc, assignment]

try:
    from langchain_core.tools import BaseToolkit as _BaseToolkit
except ImportError:  # pragma: no cover
    _BaseToolkit = None  # type: ignore[misc, assignment]


def _require_langchain() -> None:
    if _BaseTool is None:
        raise ImportError(
            "LangChain 适配需要 langchain-core（可选依赖，非 plaita 必需）。"
            "安装: pip install plaita-ai[agent] 或 pip install langchain-core"
        )


def _is_base_tool(obj: Any) -> bool:
    return _BaseTool is not None and isinstance(obj, _BaseTool)


def langchain_tool_to_callable(tool: Any) -> Callable[..., Any]:
    """把 BaseTool 变成 ``**kwargs`` 可调用对象。

    优先使用 ``tool.func``；若无（典型 toolkit 子类只实现 ``_run``），
    则包装为 ``tool.invoke(kwargs)``。
    """
    _require_langchain()
    if not _is_base_tool(tool):
        raise TypeError(f"期望 langchain BaseTool，得到 {type(tool).__name__}")

    name = (tool.name or "tool").replace("-", "_").replace(" ", "_")
    desc = tool.description or ""

    func = getattr(tool, "func", None)
    if callable(func):
        try:
            if not (getattr(func, "__doc__", None) or "").strip() and desc:
                func.__doc__ = desc
        except Exception:
            pass
        return func

    def _call(**kwargs: Any) -> Any:
        return tool.invoke(kwargs)

    _call.__name__ = name
    _call.__doc__ = desc or f"LangChain tool {tool.name}"
    return _call


def _json_schema_for_tool(tool: Any) -> Dict[str, Any]:
    """提取工具入参 JSON Schema（尽量不含 Injected* 参数）。"""
    args = getattr(tool, "args", None) or {}
    required: List[str] = []
    try:
        model = tool.get_input_schema()
        full = model.model_json_schema()
        required = list(full.get("required") or [])
        if not args:
            props = dict(full.get("properties") or {})
            args = _strip_injected_properties(model, props)
            required = [r for r in required if r in args]
    except Exception:
        pass
    return {"type": "object", "properties": dict(args), "required": required}


def _strip_injected_properties(model: Any, props: Dict[str, Any]) -> Dict[str, Any]:
    """去掉带 InjectedToolArg / InjectedToolCallId metadata 的字段。"""
    fields = getattr(model, "model_fields", None) or {}
    out: Dict[str, Any] = {}
    for name, spec in props.items():
        finfo = fields.get(name)
        meta = list(getattr(finfo, "metadata", None) or []) if finfo is not None else []
        if any(_is_injected_marker(m) for m in meta):
            continue
        out[name] = spec
    return out


def _is_injected_marker(obj: Any) -> bool:
    try:
        from langchain_core.tools import InjectedToolArg, InjectedToolCallId

        if obj is InjectedToolArg or obj is InjectedToolCallId:
            return True
        if isinstance(obj, type) and issubclass(obj, InjectedToolArg):
            return True
    except ImportError:
        pass
    except Exception:
        pass
    cls = obj if isinstance(obj, type) else type(obj)
    return "InjectedTool" in getattr(cls, "__name__", "")


def schema_from_langchain_tool(
    tool: Any,
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
) -> ToolSchema:
    """从 BaseTool.args / input schema 生成 ToolSchema。"""
    _require_langchain()
    tool_name = name or tool.name or "tool"
    desc = description if description is not None else (tool.description or "无描述")
    js = _json_schema_for_tool(tool)
    props = js.get("properties") or {}
    required_set = set(js.get("required") or [])

    params: List[ParamSchema] = []
    for pname, pspec in props.items():
        if not isinstance(pspec, dict):
            pspec = {"type": "any"}
        ptype = pspec.get("type") or "any"
        if isinstance(ptype, list):
            ptype = next((t for t in ptype if t != "null"), ptype[0] if ptype else "any")
        has_default = "default" in pspec
        required = pname in required_set and not has_default
        default = pspec.get("default", _NO_DEFAULT) if has_default or not required else _NO_DEFAULT
        if not required and not has_default:
            default = None
        params.append(
            ParamSchema(
                name=pname,
                type=str(ptype),
                required=required,
                default=default,
                description=str(pspec.get("description") or pspec.get("title") or ""),
                py_annotation="",
            )
        )
    return ToolSchema(
        name=tool_name,
        description=desc or "无描述",
        params=params,
        has_auth_context=False,
        has_tool_context=False,
    )


def adapt_langchain_tool(
    tool: Any,
    *,
    name: Optional[str] = None,
    prefix: str = "",
    description: Optional[str] = None,
) -> tuple[str, Callable[..., Any], ToolSchema]:
    """返回 ``(name, callable, schema)``，供注册或 normalize 使用。"""
    _require_langchain()
    if not _is_base_tool(tool):
        raise TypeError(f"期望 langchain BaseTool，得到 {type(tool).__name__}")

    base_name = name or tool.name or "tool"
    tool_name = f"{prefix}{base_name}" if prefix else base_name
    func = langchain_tool_to_callable(tool)
    schema = schema_from_langchain_tool(
        tool, name=tool_name, description=description
    )
    setattr(func, _TOOL_MARKER_ATTR, schema)
    return tool_name, func, schema


def register_langchain_tool(
    tool: Any,
    *,
    name: Optional[str] = None,
    prefix: str = "",
    description: Optional[str] = None,
) -> ToolSpec:
    """注册单个 LangChain BaseTool 为 plaita ToolNode。"""
    _name, func, _schema = adapt_langchain_tool(
        tool, name=name, prefix=prefix, description=description
    )
    return register_tool_node(func)[0]


def register_langchain_toolkit(
    toolkit: Any,
    *,
    prefix: str = "",
    include: Optional[Sequence[str]] = None,
    exclude: Optional[Sequence[str]] = None,
) -> List[ToolSpec]:
    """注册 BaseToolkit 或 BaseTool 列表。

    Args:
        toolkit: ``BaseToolkit``（有 ``get_tools()``）或 ``Sequence[BaseTool]``。
        prefix: 加到每个工具名前，避免与业务工具重名。
        include: 白名单（按原始 ``tool.name``）；``None`` 表示全部。
        exclude: 黑名单（按原始 ``tool.name``）。
    """
    _require_langchain()
    tools = _expand_toolkit(toolkit)
    include_set = set(include) if include is not None else None
    exclude_set = set(exclude or ())

    specs: List[ToolSpec] = []
    for t in tools:
        if not _is_base_tool(t):
            raise TypeError(
                f"toolkit 成员需为 langchain BaseTool，得到 {type(t).__name__}"
            )
        tname = getattr(t, "name", None) or ""
        if include_set is not None and tname not in include_set:
            continue
        if tname in exclude_set:
            continue
        specs.append(register_langchain_tool(t, prefix=prefix))
    logger.info(
        "registered %d langchain tool(s) from toolkit (prefix=%r)",
        len(specs),
        prefix,
    )
    return specs


def _expand_toolkit(toolkit: Any) -> List[Any]:
    # 优先鸭子类型，不强制 BaseToolkit 类（避免版本/包路径差异）
    if isinstance(toolkit, (list, tuple)):
        return list(toolkit)
    if hasattr(toolkit, "get_tools") and callable(toolkit.get_tools):
        return list(toolkit.get_tools())
    if _BaseToolkit is not None and isinstance(toolkit, _BaseToolkit):
        return list(toolkit.get_tools())
    raise TypeError(
        "toolkit 需为 BaseToolkit、带 get_tools() 的对象，或 BaseTool 序列；"
        f"得到 {type(toolkit).__name__}"
    )
