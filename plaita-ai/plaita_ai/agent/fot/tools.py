"""Bridge LangChain tools / Python callables to plaita ToolNode."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Dict, Iterable, List, Optional, Sequence, Union

from langchain_core.tools import BaseTool
from plaita import Node
from plaita.node import get_default_registry

ToolLike = Union[Callable[..., Any], BaseTool]


@dataclass
class ToolSpec:
    name: str
    description: str
    signature: str


class ToolNode(Node):
    """Minimal tool node for FoT-generated @flow (TOOL placeholder)."""

    node_type: ClassVar[str] = "tool"
    node_name: ClassVar[str] = "工具"

    action: Optional[str] = None
    params: Optional[Dict[str, Any]] = None

    _tools: ClassVar[Dict[str, Callable[..., Any]]] = {}

    @classmethod
    def register(cls, name: str, func: Optional[Callable[..., Any]] = None) -> Callable[..., Any]:
        if func is None:

            def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
                cls._tools[name] = f
                return f

            return decorator
        cls._tools[name] = func
        return func

    @classmethod
    def get_tool(cls, name: str) -> Optional[Callable[..., Any]]:
        return cls._tools.get(name)

    def execute(self, execution: Any) -> Any:
        name = self.action
        if not name:
            raise ValueError("tool 节点缺少 action 字段")
        func = self.get_tool(name)
        if func is None:
            resolved = execution.evaluate(name)
            if isinstance(resolved, str):
                name = resolved
                func = self.get_tool(name)
        if func is None:
            raise KeyError(f"未注册的工具: {name!r}（已知：{list(self._tools)}）")
        params = execution.evaluate(self.params) if self.params else {}
        params = params or {}
        return func(**params)


def _callable_name(func: Callable[..., Any]) -> str:
    return getattr(func, "__name__", "tool")


def _tool_description(func: Callable[..., Any]) -> str:
    doc = inspect.getdoc(func) or ""
    return doc.split("\n")[0].strip() or "无描述"


def _tool_signature(func: Callable[..., Any]) -> str:
    try:
        return str(inspect.signature(func))
    except (TypeError, ValueError):
        return "(...)"


def normalize_tool(tool: ToolLike) -> tuple[str, Callable[..., Any], str]:
    if isinstance(tool, BaseTool):
        name = tool.name or _callable_name(tool.func or (lambda: None))
        func = tool.func
        if func is None:
            raise ValueError(f"LangChain tool {name!r} 没有可调用 func")
        desc = tool.description or _tool_description(func)
        return name, func, desc
    name = _callable_name(tool)
    return name, tool, _tool_description(tool)


def register_tool_node(*tools: ToolLike) -> List[ToolSpec]:
    """Register tools on ToolNode + default NodeRegistry; return prompt specs."""
    registry = get_default_registry()
    registry.register(ToolNode)

    specs: List[ToolSpec] = []
    for tool in tools:
        name, func, desc = normalize_tool(tool)
        ToolNode.register(name, func)
        specs.append(
            ToolSpec(
                name=name,
                description=desc,
                signature=f"{name}{_tool_signature(func)}",
            )
        )
    return specs


def tools_prompt_section(specs: Iterable[ToolSpec]) -> str:
    rows = []
    for spec in specs:
        rows.append(
            f"- TOOL(action=\"{spec.name}\", params={{...}})  # {spec.description}\n"
            f"  签名: {spec.signature}"
        )
    return "\n".join(rows)


def ensure_tool_node_registered() -> None:
    get_default_registry().register(ToolNode)
