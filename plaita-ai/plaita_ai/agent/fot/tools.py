"""Bridge LangChain tools / Python callables to plaita ToolNode.

新增能力（2026-07）：
- ``@tool`` 装饰器：自动从 type hints + docstring 生成工具 Schema
- ``register_tools_from_module()``：扫描模块批量注册带 ``@tool`` 标记或
  public callable 的函数
- ``list_tools()``：枚举所有已注册工具及其 Schema，供 AI 发现
- ``auth_context`` 透传：工具函数声明了 ``auth_context`` 参数时，
  ``ToolNode.execute`` 从 flow 全局上下文（``$GLOBAL.auth_context``）
  自动注入，工具函数自行校验/使用
"""

from __future__ import annotations

import inspect
import logging
import re
import warnings
from dataclasses import dataclass, field
from typing import (
    Any, Callable, ClassVar, Dict, Iterable, List, Optional, Union,
    get_type_hints,
)

try:
    from langchain_core.tools import BaseTool as _BaseTool
except ImportError:
    _BaseTool = None  # type: ignore[misc,assignment]

from plaita import Node
from plaita.node import get_default_registry

logger = logging.getLogger(__name__)

ToolLike = Union[Callable[..., Any], Any]  # Callable or LangChain BaseTool

# Sentinel for "parameter has no default value".
# We CANNOT use inspect.Parameter.empty here because Python 3.9's
# inspect module treats that sentinel as "no default" when validating
# dataclass __init__ signatures, causing ValueError at class definition time.
_NO_DEFAULT: Any = object()


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

_PY_TYPE_TO_JSON: Dict[type, str] = {
    str: "string",
    int: "integer",
    float: "number",
    bool: "boolean",
    list: "array",
    dict: "object",
    bytes: "string",
}


def _py_type_name(annotation: Any) -> str:
    """Convert a Python type annotation to a JSON-schema-style type name."""
    if annotation is inspect.Parameter.empty:
        return "any"
    origin = getattr(annotation, "__origin__", None)
    if origin is not None:
        import typing as _typing
        if origin is _typing.Union:
            args = [a for a in annotation.__args__ if a is not type(None)]
            return _py_type_name(args[0]) if args else "any"
        return _py_type_name(origin)
    return _PY_TYPE_TO_JSON.get(annotation, "any")


def _parse_docstring_args(docstring: str) -> Dict[str, str]:
    """Extract param descriptions from Google/NumPy/reStructuredText docstrings."""
    result: Dict[str, str] = {}
    if not docstring:
        return result
    # Google style: "    param_name: description"
    google_re = re.compile(r"^\s{4,}(\w+):\s*(.+)$", re.MULTILINE)
    for m in google_re.finditer(docstring):
        result[m.group(1)] = m.group(2).strip()
    # reST style: ":param name: description"
    rst_re = re.compile(r":param\s+(\w+):\s*(.+?)(?=\n\s*:|$)", re.MULTILINE | re.DOTALL)
    for m in rst_re.finditer(docstring):
        result[m.group(1)] = m.group(2).strip().replace("\n", " ")
    return result


_JSON_TYPE_TO_PY: Dict[str, str] = {
    "string": "str",
    "integer": "int",
    "number": "float",
    "boolean": "bool",
    "array": "list",
    "object": "dict",
    "any": "Any",
}


# ---------------------------------------------------------------------------
# Complex type introspection helpers
# ---------------------------------------------------------------------------

_BUILTIN_TYPES = frozenset({
    str, int, float, bool, bytes, list, dict, tuple, set,
    type(None), Any,
})


def _is_complex_type(annotation: Any) -> bool:
    """Return True if *annotation* is a user-defined complex type.

    Detects: dataclasses, TypedDict, Pydantic BaseModel.
    Excludes: builtins (str, int, …), typing generics (List, Dict, …).
    """
    if annotation is None or annotation is inspect.Parameter.empty:
        return False
    if annotation in _BUILTIN_TYPES:
        return False
    if not isinstance(annotation, type):
        return False
    # Skip typing constructs that may be instances of type (e.g. NewType)
    if annotation.__module__ in ("typing", "typing_extensions"):
        return False
    return True


def _get_complex_type_fields(cls: type) -> Optional[Dict[str, str]]:
    """Return {field_name: annotation_str} for a complex type, or None."""
    import dataclasses as _dc
    # dataclass
    if _dc.is_dataclass(cls):
        try:
            hints = get_type_hints(cls)
        except Exception:
            hints = {f.name: f.type for f in _dc.fields(cls)}
        return {name: _annotation_str(t) for name, t in hints.items()}
    # TypedDict: __annotations__ populated at class level, __total__ exists
    if hasattr(cls, "__annotations__") and hasattr(cls, "__total__"):
        try:
            hints = get_type_hints(cls)
        except Exception:
            hints = cls.__annotations__
        return {name: _annotation_str(t) for name, t in hints.items()}
    # Pydantic v2 BaseModel
    model_fields = getattr(cls, "model_fields", None)
    if model_fields is not None:
        result: Dict[str, str] = {}
        for fname, finfo in model_fields.items():
            ann = getattr(finfo, "annotation", None)
            result[fname] = _annotation_str(ann) if ann is not None else "Any"
        return result
    # Pydantic v1 BaseModel
    schema_fn = getattr(cls, "schema", None)
    if schema_fn is not None and callable(schema_fn):
        try:
            schema = schema_fn()
            props = schema.get("properties", {})
            return {k: v.get("type", "Any") for k, v in props.items()}
        except Exception:
            pass
    # Plain class with __annotations__
    if hasattr(cls, "__annotations__"):
        try:
            hints = get_type_hints(cls)
        except Exception:
            hints = cls.__annotations__
        if hints:
            return {name: _annotation_str(t) for name, t in hints.items()}
    return None


def _collect_complex_types(
    annotation: Any,
    collected: Optional[Dict[str, type]] = None,
) -> Dict[str, type]:
    """Recursively collect all complex types referenced by *annotation*."""
    if collected is None:
        collected = {}
    if annotation is None or annotation is inspect.Parameter.empty:
        return collected
    origin = getattr(annotation, "__origin__", None)
    args = getattr(annotation, "__args__", None)
    if origin is not None and args:
        for arg in args:
            _collect_complex_types(arg, collected)
        return collected
    if _is_complex_type(annotation):
        name = annotation.__name__
        if name not in collected:
            collected[name] = annotation
            # Recursively collect types referenced in fields
            fields = _get_complex_type_fields(annotation)
            if fields:
                import typing as _typing
                for _fname, _ftype_str in fields.items():
                    # We can't easily recurse from string; instead get from hints
                    pass
            try:
                hints = get_type_hints(annotation) if hasattr(annotation, "__annotations__") else {}
            except Exception:
                hints = {}
            for sub_hint in hints.values():
                _collect_complex_types(sub_hint, collected)
    return collected


def _complex_type_stub(cls: type) -> str:
    """Generate a Python class stub for a complex type."""
    fields = _get_complex_type_fields(cls)
    if not fields:
        return f"class {cls.__name__}: ..."
    lines = [f"class {cls.__name__}:"]
    for fname, ftype in fields.items():
        lines.append(f"    {fname}: {ftype}")
    return "\n".join(lines)


@dataclass
class ParamSchema:
    name: str
    type: str = "any"           # JSON-schema type name (string/integer/boolean/…)
    required: bool = True
    default: Any = _NO_DEFAULT
    description: str = ""
    py_annotation: str = ""     # Original Python annotation string (e.g. "List[str]")
    annotation_raw: Any = field(default=None, repr=False, compare=False)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"type": self.type, "required": self.required}
        if self.description:
            d["description"] = self.description
        if self.default is not _NO_DEFAULT:
            d["default"] = self.default
        return d

    def py_type(self) -> str:
        """Return a Python-style type name suitable for code signatures."""
        if self.py_annotation:
            return self.py_annotation
        return _JSON_TYPE_TO_PY.get(self.type, self.type)


@dataclass
class ToolSchema:
    name: str
    description: str = ""
    params: List[ParamSchema] = field(default_factory=list)
    has_auth_context: bool = False
    # 若工具声明 ``context`` 参数，运行时注入 ToolContext（无业务域语义）
    has_tool_context: bool = False
    # Python return annotation string, e.g. "dict", "str", "List[Order]"
    return_annotation: str = ""
    # Raw return annotation object for complex type introspection
    return_raw: Any = field(default=None, repr=False, compare=False)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            "params": {p.name: p.to_dict() for p in self.params},
        }
        if self.return_annotation:
            d["return_type"] = self.return_annotation
        return d

    def code_signature(self) -> str:
        """Return a Python-style function signature string.

        Example output::

            def get_user(user_id: str, include_deleted: bool = False) -> dict:
                \"\"\"查询用户信息，返回 {name, age, level}\"\"\"

        If the tool name contains characters invalid in a Python identifier
        (e.g. hyphens like ``get-user``), a comment noting the action name is
        prepended and the stub uses the sanitised identifier as the function
        name.

        For complex return/param types, use ``full_code_block()`` instead,
        which also includes referenced type definitions.
        """
        parts = []
        for p in self.params:
            ptype = p.py_type()
            if ptype and ptype != "Any":
                part = f"{p.name}: {ptype}"
            else:
                part = p.name
            if not p.required and p.default is not _NO_DEFAULT:
                part += f" = {p.default!r}"
            elif not p.required:
                part += " = None"
            parts.append(part)

        params_str = ", ".join(parts)
        return_str = f" -> {self.return_annotation}" if self.return_annotation else ""

        # Tool names may contain hyphens (valid action names, invalid Python identifiers)
        py_name = self.name.replace("-", "_")
        header = ""
        if py_name != self.name:
            header = f'# TOOL(action="{self.name}", ...)\n'

        sig = f"{header}def {py_name}({params_str}){return_str}:"
        if self.description:
            doc = self.description.replace('"', '\\"')
            sig += f'\n    """{doc}"""'
        return sig

    def full_code_block(self) -> str:
        """Return function signature + all referenced complex type definitions.

        Example output::

            class UserInfo:
                name: str
                age: int
                level: str

            # TOOL(action="get-user", ...)
            def get_user(user_id: str) -> UserInfo:
                \"\"\"查询用户信息\"\"\"

        Supports dataclasses, TypedDict, and Pydantic BaseModel as return or
        param types.  Complex types are deduplicated and emitted in dependency
        order (leaf types first).
        """
        # Collect all referenced complex types from params and return annotation
        complex_types: Dict[str, type] = {}
        for p in self.params:
            if p.annotation_raw is not None:
                _collect_complex_types(p.annotation_raw, complex_types)
        if self.return_raw is not None:
            _collect_complex_types(self.return_raw, complex_types)

        blocks: List[str] = []
        for cls in complex_types.values():
            blocks.append(_complex_type_stub(cls))

        blocks.append(self.code_signature())
        return "\n\n".join(blocks)

    def prompt_line(self) -> str:
        """One-line summary for LLM prompts (generic TOOL syntax, backward compat)."""
        sig = ", ".join(
            f"{p.name}: {p.type}" + ("?" if not p.required else "")
            for p in self.params
        )
        ret = f" -> {self.return_annotation}" if self.return_annotation else ""
        return (
            f"TOOL(action=\"{self.name}\", params={{{sig}}}){ret}  # {self.description}"
        )

    def dsl_stub(self, placeholder: Optional[str] = None) -> str:
        """Return a @flow DSL node-call stub showing the UPPERCASE placeholder.

        Example output::

            # action: "get-user"  (when name has hyphens)
            GET_USER(user_id: str, include_deleted: bool = False) -> UserInfo
                \"\"\"查询用户信息\"\"\"

        The *placeholder* argument lets the caller override the default
        ``name.upper()`` (e.g. to match an already-registered node_type).
        """
        if placeholder is None:
            placeholder = self.name.replace("-", "_").replace(" ", "_").upper()

        parts = []
        for p in self.params:
            ptype = p.py_type()
            if ptype and ptype != "Any":
                part = f"{p.name}: {ptype}"
            else:
                part = p.name
            if not p.required and p.default is not _NO_DEFAULT:
                part += f" = {p.default!r}"
            elif not p.required:
                part += " = None"
            parts.append(part)

        params_str = ", ".join(parts)
        return_str = f" -> {self.return_annotation}" if self.return_annotation else ""
        header = ""
        if self.name != self.name.replace("-", "_"):
            header = f'# action: "{self.name}"\n'
        sig = f"{header}{placeholder}({params_str}){return_str}"
        if self.description:
            doc = self.description.replace('"', '\\"')
            sig += f'\n    """{doc}"""'
        return sig


def _annotation_str(annotation: Any) -> str:
    """Return a readable Python type annotation string.

    Examples:
        str              -> "str"
        int              -> "int"
        dict             -> "dict"
        List[str]        -> "List[str]"
        Optional[int]    -> "Optional[int]"
        Dict[str, Any]   -> "Dict[str, Any]"
    """
    if annotation is inspect.Parameter.empty or annotation is None:
        return ""
    # Check for generic aliases FIRST (they have __origin__ AND misleading __name__).
    # e.g. Optional[str].__name__ == "Optional" (loses the parameter).
    origin = getattr(annotation, "__origin__", None)
    args = getattr(annotation, "__args__", None)
    if origin is not None:
        import typing as _typing
        # Optional[X] is Union[X, None]
        if origin is _typing.Union and args:
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return f"Optional[{_annotation_str(non_none[0])}]"
            return "Union[" + ", ".join(_annotation_str(a) for a in args) + "]"
        # List[X] / Dict[K, V] / Tuple[…] etc.
        # origin is the underlying builtin (list, dict, tuple) or typing form.
        origin_name = getattr(origin, "__name__", None) or repr(origin).replace("typing.", "")
        # Capitalise list → List, dict → Dict to match idiomatic annotation style.
        _CAPS = {"list": "List", "dict": "Dict", "tuple": "Tuple", "set": "Set",
                 "frozenset": "FrozenSet", "type": "Type"}
        origin_name = _CAPS.get(origin_name, origin_name)
        if args:
            args_str = ", ".join(_annotation_str(a) for a in args)
            return f"{origin_name}[{args_str}]"
        return origin_name
    # Plain built-in types (str, int, dict, …)
    name = getattr(annotation, "__name__", None)
    if name:
        return name
    # Fallback: repr with typing. stripped
    r = repr(annotation).replace("typing.", "")
    return r


def _build_schema(func: Callable[..., Any], name: str, description: str) -> ToolSchema:
    """Introspect a callable and build a ToolSchema from type hints + docstring."""
    docstring = inspect.getdoc(func) or ""
    if not description:
        description = docstring.split("\n")[0].strip() or "无描述"
    param_docs = _parse_docstring_args(docstring)
    try:
        hints = get_type_hints(func)
    except Exception:
        hints = {}

    # Capture return type annotation
    return_hint = hints.get("return", inspect.Parameter.empty)
    return_annotation = _annotation_str(return_hint) if return_hint is not inspect.Parameter.empty else ""

    params: List[ParamSchema] = []
    has_auth_context = False
    has_tool_context = False
    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError):
        return ToolSchema(name=name, description=description, return_annotation=return_annotation)

    for pname, param in sig.parameters.items():
        if pname in ("self", "cls"):
            continue
        if pname == "auth_context":
            has_auth_context = True
            continue  # excluded from user-facing schema; injected by runtime
        if pname == "context":
            has_tool_context = True
            continue  # ToolContext; injected by runtime
        annotation = hints.get(pname, param.annotation)
        type_name = _py_type_name(annotation)
        py_anno = _annotation_str(annotation) if annotation is not inspect.Parameter.empty else ""
        required = param.default is inspect.Parameter.empty
        default = param.default if not required else _NO_DEFAULT
        params.append(ParamSchema(
            name=pname,
            type=type_name,
            required=required,
            default=default,
            description=param_docs.get(pname, ""),
            py_annotation=py_anno,
            annotation_raw=annotation if annotation is not inspect.Parameter.empty else None,  # noqa: E501
        ))
    return ToolSchema(
        name=name,
        description=description,
        params=params,
        has_auth_context=has_auth_context,
        has_tool_context=has_tool_context,
        return_annotation=return_annotation,
        return_raw=return_hint if return_hint is not inspect.Parameter.empty else None,
    )


# ---------------------------------------------------------------------------
# @tool decorator
# ---------------------------------------------------------------------------

_TOOL_MARKER_ATTR = "__plaita_tool__"


def tool(
    func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    description: str = "",
) -> Any:
    """Decorator to mark a function as a plaita tool with optional overrides.

    Usage::

        @tool
        def get_order(order_id: str) -> dict:
            \"\"\"查询订单信息\"\"\"
            ...

        @tool(name="search-product", description="搜索商品目录")
        def search_product(keyword: str, page: int = 1) -> list:
            ...

    Decorated functions are discovered by ``register_tools_from_module()``
    and their schema is auto-generated from type hints + docstring.
    The decorator is a no-op at call time — the function behaves normally.
    """
    def _decorate(f: Callable[..., Any]) -> Callable[..., Any]:
        tool_name = name or f.__name__
        schema = _build_schema(f, tool_name, description)
        setattr(f, _TOOL_MARKER_ATTR, schema)
        return f

    if func is not None:
        return _decorate(func)
    return _decorate


# ---------------------------------------------------------------------------
# ToolSpec (backward compat + display)
# ---------------------------------------------------------------------------

@dataclass
class ToolSpec:
    name: str
    description: str
    signature: str
    schema: Optional[ToolSchema] = None
    node_type: str = ""             # node_type registered in NodeRegistry (e.g. "get_user")
    placeholder: str = ""           # uppercase @flow DSL placeholder (e.g. "GET_USER")


# ---------------------------------------------------------------------------
# Dynamic per-tool Node class factory
# ---------------------------------------------------------------------------

# Registry of dynamically-created node classes; used by ToolNode.clear()
_DYNAMIC_NODE_CLASSES: Dict[str, type] = {}


def make_tool_node_class(
    func: Callable[..., Any],
    name: str,
    schema: ToolSchema,
) -> type:
    """Create a Node subclass specific to *func* / *name*.

    The resulting class has:
    - ``node_type = name`` (snake_case, hyphens replaced by underscores)
    - One ``Optional[Any]`` field per parameter in *schema* (values can be
      flow expressions like ``$INPUT.user_id``)
    - An ``execute()`` method that evaluates each field via the execution
      context and calls *func* with the results

    Registered in the global ``_DYNAMIC_NODE_CLASSES`` dict so
    ``ToolNode.clear()`` can remove them from the NodeRegistry.
    """
    node_type = name.replace("-", "_").replace(" ", "_").lower()
    class_name = "".join(w.title() for w in node_type.split("_")) + "ToolNode"

    # Build class body: ClassVars + one Optional[Any] field per param
    annotations: Dict[str, Any] = {
        "node_type": ClassVar[str],
        "node_name": ClassVar[str],
    }
    defaults: Dict[str, Any] = {
        "node_type": node_type,
        "node_name": name,
    }
    for p in schema.params:
        annotations[p.name] = Optional[Any]
        defaults[p.name] = None

    defaults["__annotations__"] = annotations

    # Capture func and schema in closures for execute()
    _func = func
    _schema = schema

    def execute(self, execution: Any) -> Any:
        params: Dict[str, Any] = {}
        for p in _schema.params:
            val = getattr(self, p.name, None)
            if val is not None:
                params[p.name] = execution.evaluate(val)
        if _schema.has_auth_context or _func_has_auth_context(_func):
            auth_ctx = execution.get_global_variable("auth_context")
            if auth_ctx is not None:
                params["auth_context"] = auth_ctx
        if _schema.has_tool_context or _func_has_tool_context(_func):
            params["context"] = _build_tool_context(execution)
        return _func(**params)

    defaults["execute"] = execute

    # Dynamically create the Pydantic-based Node subclass
    NodeClass = type(class_name, (Node,), defaults)
    _DYNAMIC_NODE_CLASSES[node_type] = NodeClass
    return NodeClass


# ---------------------------------------------------------------------------
# ToolNode (generic fallback — kept for backward compatibility)
# ---------------------------------------------------------------------------

class ToolNode(Node):
    """Minimal tool node for FoT-generated @flow (TOOL placeholder).

    Note: ``_tools`` is a **process-level class variable** shared across all
    agent instances.  Registering two different functions under the same name
    (e.g. from two ``FoTAgent`` instances with overlapping tool names) will
    silently overwrite the earlier one.  Call ``ToolNode.clear()`` between
    isolated test cases, or use distinct tool names across agents.

    ``auth_context`` injection: if the registered tool function declares an
    ``auth_context`` keyword parameter, ``execute`` reads
    ``$GLOBAL.auth_context`` from the flow's global context and passes it
    automatically. The tool function is responsible for validating the value.

    ``context`` injection: if the tool declares a ``context`` parameter,
    ``execute`` builds a generic ``ToolContext`` (trace/caller/auth/baggage)
    from ``$GLOBAL`` and passes it. Business fields belong in ``baggage``.
    """

    node_type: ClassVar[str] = "tool"
    node_name: ClassVar[str] = "工具"

    action: Optional[str] = None
    params: Optional[Dict[str, Any]] = None

    _tools: ClassVar[Dict[str, Callable[..., Any]]] = {}
    _schemas: ClassVar[Dict[str, ToolSchema]] = {}

    @classmethod
    def register(
        cls,
        name: str,
        func: Optional[Callable[..., Any]] = None,
        *,
        schema: Optional[ToolSchema] = None,
    ) -> Callable[..., Any]:
        if func is None:
            def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
                cls._register_one(name, f, schema=schema)
                return f
            return decorator
        cls._register_one(name, func, schema=schema)
        return func

    @classmethod
    def _register_one(
        cls,
        name: str,
        func: Callable[..., Any],
        *,
        schema: Optional[ToolSchema] = None,
    ) -> None:
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
        cls._schemas[name] = schema or _build_schema(func, name, "")

    @classmethod
    def get_tool(cls, name: str) -> Optional[Callable[..., Any]]:
        return cls._tools.get(name)

    @classmethod
    def get_schema(cls, name: str) -> Optional[ToolSchema]:
        return cls._schemas.get(name)

    @classmethod
    def list_tools(cls) -> List[ToolSchema]:
        """Return a list of all registered tool schemas (sorted by name)."""
        return [cls._schemas[n] for n in sorted(cls._schemas)]

    @classmethod
    def list_tool_names(cls) -> List[str]:
        """Return the names of all currently registered tools."""
        return list(cls._tools)

    @classmethod
    def clear(cls) -> None:
        """Remove all registered tools, schemas and dynamic node classes.

        Useful in test teardown or when creating isolated agent environments.
        Also unregisters any per-tool node classes from the NodeRegistry.
        Note: does NOT unregister the generic ``ToolNode`` (node_type="tool")
        itself.
        """
        registry = get_default_registry()
        for node_type in list(_DYNAMIC_NODE_CLASSES.keys()):
            try:
                registry.unregister(node_type)
            except Exception:
                pass
            _DYNAMIC_NODE_CLASSES.pop(node_type, None)
        cls._tools.clear()
        cls._schemas.clear()

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

        # auth_context / ToolContext injection from $GLOBAL.
        schema = self.get_schema(name)
        needs_auth = (
            (schema is not None and schema.has_auth_context)
            or _func_has_auth_context(func)
        )
        if needs_auth:
            auth_ctx = execution.get_global_variable("auth_context")
            if auth_ctx is not None:
                params = {**params, "auth_context": auth_ctx}

        needs_ctx = (
            (schema is not None and getattr(schema, "has_tool_context", False))
            or _func_has_tool_context(func)
        )
        if needs_ctx:
            params = {**params, "context": _build_tool_context(execution)}

        return func(**params)


def _func_has_auth_context(func: Callable[..., Any]) -> bool:
    """Return True if ``func`` declares an ``auth_context`` parameter."""
    try:
        sig = inspect.signature(func)
        return "auth_context" in sig.parameters
    except (TypeError, ValueError):
        return False


def _func_has_tool_context(func: Callable[..., Any]) -> bool:
    """Return True if ``func`` declares a ``context`` parameter."""
    try:
        sig = inspect.signature(func)
        return "context" in sig.parameters
    except (TypeError, ValueError):
        return False


def _build_tool_context(execution: Any) -> Any:
    """Build ToolContext from execution globals (lazy import to avoid cycles)."""
    try:
        from plaita_ai.tools.source.base import build_tool_context
        return build_tool_context(execution)
    except ImportError:  # pragma: no cover
        # tools 包不可用时退化为简单命名空间
        def _get(key: str, default: Any = None) -> Any:
            try:
                return execution.get_global_variable(key, default)
            except Exception:
                return default
        return {
            "trace_id": _get("trace_id"),
            "request_id": _get("request_id"),
            "caller": _get("caller"),
            "flow_id": _get("flow_id"),
            "auth": _get("auth_context"),
            "baggage": _get("baggage") or {},
        }


# ---------------------------------------------------------------------------
# register_tools_from_module
# ---------------------------------------------------------------------------

def register_tools_from_module(
    module: Any,
    *,
    prefix: str = "",
    auto_discover: bool = True,
) -> List[ToolSpec]:
    """Scan *module* and register all eligible functions as ToolNode tools.

    Eligibility rules (evaluated in order):
    1. Has ``@tool`` decorator (``__plaita_tool__`` attribute) → always included.
    2. ``auto_discover=True``: public functions (no leading ``_``) with a
       non-empty docstring are also included.

    ``prefix`` is prepended to the tool name (e.g. ``prefix="order_"``
    registers ``get_order`` as ``order_get_order``).

    Returns the list of registered ``ToolSpec`` objects.
    """
    registry = get_default_registry()
    registry.register(ToolNode)

    specs: List[ToolSpec] = []
    for attr_name in dir(module):
        if attr_name.startswith("_"):
            continue
        obj = getattr(module, attr_name, None)
        if not callable(obj) or isinstance(obj, type):
            continue
        # Skip objects not defined in this module (imported symbols)
        if hasattr(obj, "__module__") and obj.__module__ != getattr(module, "__name__", None):
            continue

        schema: Optional[ToolSchema] = getattr(obj, _TOOL_MARKER_ATTR, None)
        if schema is None:
            if not auto_discover:
                continue
            doc = inspect.getdoc(obj)
            if not doc:
                continue
            schema = _build_schema(obj, attr_name, "")

        tool_name = f"{prefix}{schema.name}"
        if tool_name != schema.name:
            schema = ToolSchema(
                name=tool_name,
                description=schema.description,
                params=schema.params,
                has_auth_context=schema.has_auth_context,
            )
        # Backward-compat generic ToolNode
        ToolNode.register(tool_name, obj, schema=schema)
        # Per-tool node class
        node_cls = make_tool_node_class(obj, tool_name, schema)
        registry.register(node_cls)

        node_type = node_cls.node_type  # type: ignore[attr-defined]
        specs.append(ToolSpec(
            name=tool_name,
            description=schema.description,
            signature=f"{tool_name}({', '.join(p.name for p in schema.params)})",
            schema=schema,
            node_type=node_type,
            placeholder=node_type.upper(),
        ))
    return specs


# ---------------------------------------------------------------------------
# list_tools
# ---------------------------------------------------------------------------

def list_tools(as_code: bool = True) -> List[Any]:
    """Return all registered tool schemas.

    Args:
        as_code: When True (default), return a list of strings where:
            - Index 0 (if any complex types exist) is a block of all
              unique complex type definitions referenced by any tool.
            - Remaining entries are @flow DSL node call stubs, each
              showing the UPPERCASE placeholder with typed parameters.
            Complex types are deduplicated across all tools.
            When False, return JSON-serialisable dicts (backward compat).

    Used by ``flow_list_tools`` MCP tool and planner prompts so the AI can
    discover what actions are available before writing a @flow.
    """
    schemas = ToolNode.list_tools()
    if not as_code:
        return [s.to_dict() for s in schemas]

    # Collect all complex types from all schemas (deduplicated by class name)
    all_complex: Dict[str, type] = {}
    for s in schemas:
        for p in s.params:
            if p.annotation_raw is not None:
                _collect_complex_types(p.annotation_raw, all_complex)
        if s.return_raw is not None:
            _collect_complex_types(s.return_raw, all_complex)

    result: List[str] = []
    if all_complex:
        type_stubs = "\n\n".join(_complex_type_stub(cls) for cls in all_complex.values())
        result.append(type_stubs)

    for s in schemas:
        # Show UPPERCASE placeholder (how to call in @flow DSL)
        node_type = s.name.replace("-", "_").replace(" ", "_").lower()
        placeholder = node_type.upper()
        result.append(s.dsl_stub(placeholder))
    return result


# ---------------------------------------------------------------------------
# normalize_tool (backward compat)
# ---------------------------------------------------------------------------

def normalize_tool(tool_input: ToolLike) -> tuple[str, Callable[..., Any], str]:
    if _BaseTool is not None and isinstance(tool_input, _BaseTool):
        name = tool_input.name or "tool"
        desc = tool_input.description or ""
        func = getattr(tool_input, "func", None)
        if not callable(func):
            # Toolkit 常见形态：只实现 _run，无 .func → 走 invoke
            tool = tool_input

            def _call(**kwargs: Any) -> Any:
                return tool.invoke(kwargs)

            _call.__name__ = name.replace("-", "_").replace(" ", "_")
            _call.__doc__ = desc or f"LangChain tool {name}"
            func = _call
        if not desc:
            desc = _tool_description(func)
        return name, func, desc
    name = _callable_name(tool_input)
    return name, tool_input, _tool_description(tool_input)


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


# ---------------------------------------------------------------------------
# register_tool_node (backward compat)
# ---------------------------------------------------------------------------

def register_tool_node(*tools: ToolLike) -> List[ToolSpec]:
    """Register tools as individual Node subclasses in the NodeRegistry.

    Each tool function becomes its own node type:
    - ``node_type = tool_name`` (snake_case)
    - Placeholder in @flow DSL = ``TOOL_NAME`` (UPPERCASE)
    - Fields match the function's parameters

    For functions decorated with ``@tool``, the tool's name and description
    override the defaults derived from ``__name__`` and docstring.

    Also registers a generic ``ToolNode`` fallback (node_type="tool") for
    backward compatibility with ``TOOL(action="...", params={...})`` syntax.
    """
    registry = get_default_registry()
    registry.register(ToolNode)  # Generic fallback

    specs: List[ToolSpec] = []
    for t in tools:
        # Resolve name, func, schema
        existing_schema: Optional[ToolSchema] = getattr(t, _TOOL_MARKER_ATTR, None)
        if existing_schema is not None:
            name = existing_schema.name
            func = t
            schema = existing_schema
        elif _BaseTool is not None and isinstance(t, _BaseTool):
            # LangChain BaseTool（含无 .func 的 toolkit 工具）
            try:
                from plaita_ai.tools.langchain import adapt_langchain_tool

                name, func, schema = adapt_langchain_tool(t)
            except ImportError:
                name, func, desc = normalize_tool(t)
                schema = _build_schema(func, name, desc)
        else:
            name, func, desc = normalize_tool(t)
            schema = _build_schema(func, name, desc)

        # 1. Keep backward-compat generic ToolNode registration
        ToolNode.register(name, func, schema=schema)

        # 2. Create and register a per-tool Node class
        node_cls = make_tool_node_class(func, name, schema)
        registry.register(node_cls)

        node_type = node_cls.node_type          # type: ignore[attr-defined]
        placeholder = node_type.upper()

        specs.append(ToolSpec(
            name=name,
            description=schema.description,
            signature=f"{name}{_tool_signature(func)}",
            schema=schema,
            node_type=node_type,
            placeholder=placeholder,
        ))
    return specs


# ---------------------------------------------------------------------------
# tools_prompt_section (backward compat)
# ---------------------------------------------------------------------------

def tools_prompt_section(specs: Iterable[ToolSpec]) -> str:
    rows = []
    for spec in specs:
        if spec.schema:
            rows.append(f"- {spec.schema.prompt_line()}")
        else:
            rows.append(
                f"- TOOL(action=\"{spec.name}\", params={{...}})  # {spec.description}\n"
                f"  签名: {spec.signature}"
            )
    return "\n".join(rows)


# ---------------------------------------------------------------------------
# ensure_tool_node_registered (backward compat)
# ---------------------------------------------------------------------------

def ensure_tool_node_registered() -> None:
    get_default_registry().register(ToolNode)
