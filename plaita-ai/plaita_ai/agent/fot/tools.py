"""Bridge LangChain tools / Python callables to plaita ToolNode."""

from __future__ import annotations

import inspect
import logging
import warnings
from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Dict, Iterable, List, Optional, Sequence, Union

from langchain_core.tools import BaseTool
from plaita import Node
from plaita.node import get_default_registry

logger = logging.getLogger(__name__)

ToolLike = Union[Callable[..., Any], BaseTool]


@dataclass
class ToolSpec:
    name: str
    description: str
    signature: str


class ToolNode(Node):
    """Minimal tool node for FoT-generated @flow (TOOL placeholder).

    Note: ``_tools`` is a **process-level class variable** shared across all
    agent instances.  Registering two different functions under the same name
    (e.g. from two ``FoTAgent`` instances with overlapping tool names) will
    silently overwrite the earlier one.  Call ``ToolNode.clear()`` between
    isolated test cases, or use distinct tool names across agents.
    """

    node_type: ClassVar[str] = "tool"
    node_name: ClassVar[str] = "工具"

    action: Optional[str] = None
    params: Optional[Dict[str, Any]] = None

    _tools: ClassVar[Dict[str, Callable[..., Any]]] = {}

    @classmethod
    def register(cls, name: str, func: Optional[Callable[..., Any]] = None) -> Callable[..., Any]:
        if func is None:

            def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
                cls._register_one(name, f)
                return f

            return decorator
        cls._register_one(name, func)
        return func

    @classmethod
    def _register_one(cls, name: str, func: Callable[..., Any]) -> None:
        existing = cls._tools.get(name)
        if existing is not None and existing is not func:
            warnings.warn(
                f"ToolNode: 工具 {name!r} 已注册（{existing}），将被新函数（{func}）覆盖。"
                " 如果这不是预期行为，请检查是否有多个 Agent 实例注册了同名工具。"
                " 可调用 ToolNode.clear() 在测试或隔离场景中重置注册表。",
                UserWarning,
                stacklevel=3,
            )
            logger.warning(
                "ToolNode: overwriting tool %r (%s → %s)", name, existing, func
            )
        cls._tools[name] = func

    @classmethod
    def get_tool(cls, name: str) -> Optional[Callable[..., Any]]:
        return cls._tools.get(name)

    @classmethod
    def list_tools(cls) -> List[str]:
        """Return the names of all currently registered tools."""
        return list(cls._tools)

    @classmethod
    def clear(cls) -> None:
        """Remove all registered tools.

        Useful in test teardown or when creating isolated agent environments.
        Note: this does NOT unregister ``ToolNode`` itself from the
        ``NodeRegistry``; call ``get_default_registry().unregister('tool')``
        separately if needed.
        """
        cls._tools.clear()

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
