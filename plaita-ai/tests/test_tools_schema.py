"""Tests for the new tools.py functionality (no langchain required).

Covers:
- @tool decorator
- _annotation_str (incl. generics and Optional)
- ToolSchema.code_signature / dsl_stub / full_code_block
- _build_schema return type capture
- make_tool_node_class — dynamic Node creation and execution
- register_tool_node — per-tool node registration in NodeRegistry
- register_tools_from_module
- list_tools code format (complex types, de-dup)
- auth_context injection in ToolNode.execute
- ToolNode.clear (also removes dynamic nodes)
- _collect_complex_types / _complex_type_stub
"""

from __future__ import annotations

import sys
import os
import unittest
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TypedDict

# Ensure plaita-ai is on sys.path for imports
_HERE = os.path.dirname(__file__)
_PLAITA_AI = os.path.dirname(_HERE)
if _PLAITA_AI not in sys.path:
    sys.path.insert(0, _PLAITA_AI)

from plaita_ai.agent.fot.tools import (
    _annotation_str,
    _build_schema,
    _collect_complex_types,
    _complex_type_stub,
    _TOOL_MARKER_ATTR,
    list_tools,
    make_tool_node_class,
    ParamSchema,
    register_tool_node,
    register_tools_from_module,
    ToolNode,
    ToolSchema,
    tool,
)
from plaita.node import get_default_registry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@dataclass
class UserInfo:
    name: str
    age: int
    level: str


@dataclass
class OrderLine:
    sku: str
    qty: int


@dataclass
class Order:
    order_id: str
    user: UserInfo
    lines: List[OrderLine]


class SearchResult(TypedDict):
    items: List[dict]
    total: int


class _ToolTestBase(unittest.TestCase):
    def setUp(self):
        ToolNode.clear()

    def tearDown(self):
        ToolNode.clear()


# ---------------------------------------------------------------------------
# _annotation_str
# ---------------------------------------------------------------------------

class TestAnnotationStr(unittest.TestCase):
    def test_plain_types(self):
        self.assertEqual(_annotation_str(str), "str")
        self.assertEqual(_annotation_str(int), "int")
        self.assertEqual(_annotation_str(float), "float")
        self.assertEqual(_annotation_str(bool), "bool")
        self.assertEqual(_annotation_str(dict), "dict")
        self.assertEqual(_annotation_str(list), "list")

    def test_list_generic(self):
        self.assertEqual(_annotation_str(List[str]), "List[str]")
        self.assertEqual(_annotation_str(List[dict]), "List[dict]")

    def test_optional(self):
        self.assertEqual(_annotation_str(Optional[str]), "Optional[str]")
        self.assertEqual(_annotation_str(Optional[int]), "Optional[int]")

    def test_dict_generic(self):
        self.assertEqual(_annotation_str(Dict[str, Any]), "Dict[str, Any]")

    def test_nested_generic(self):
        self.assertEqual(_annotation_str(List[Dict[str, int]]), "List[Dict[str, int]]")

    def test_empty_annotation(self):
        import inspect
        self.assertEqual(_annotation_str(inspect.Parameter.empty), "")

    def test_none_annotation(self):
        self.assertEqual(_annotation_str(None), "")

    def test_custom_class(self):
        self.assertEqual(_annotation_str(UserInfo), "UserInfo")


# ---------------------------------------------------------------------------
# @tool decorator
# ---------------------------------------------------------------------------

class TestToolDecorator(_ToolTestBase):
    def test_basic_decorator(self):
        @tool
        def simple_func(x: str) -> str:
            """Simple tool."""
            return x

        schema: ToolSchema = getattr(simple_func, _TOOL_MARKER_ATTR)
        self.assertEqual(schema.name, "simple_func")
        self.assertEqual(schema.description, "Simple tool.")
        self.assertEqual(len(schema.params), 1)
        self.assertEqual(schema.params[0].name, "x")
        self.assertEqual(schema.params[0].type, "string")

    def test_decorator_with_custom_name(self):
        @tool(name="my-tool", description="Custom desc")
        def func(a: int) -> bool:
            """Docstring."""
            return True

        schema: ToolSchema = getattr(func, _TOOL_MARKER_ATTR)
        self.assertEqual(schema.name, "my-tool")
        self.assertEqual(schema.description, "Custom desc")

    def test_decorator_captures_return_type(self):
        @tool
        def get_user(user_id: str) -> UserInfo:
            """Get user."""
            return UserInfo("Alice", 30, "vip")

        schema: ToolSchema = getattr(get_user, _TOOL_MARKER_ATTR)
        self.assertEqual(schema.return_annotation, "UserInfo")

    def test_decorator_excludes_auth_context(self):
        @tool
        def secured_func(x: str, auth_context: Optional[dict] = None) -> str:
            """Tool with auth."""
            return x

        schema: ToolSchema = getattr(secured_func, _TOOL_MARKER_ATTR)
        self.assertTrue(schema.has_auth_context)
        param_names = [p.name for p in schema.params]
        self.assertNotIn("auth_context", param_names)
        self.assertIn("x", param_names)


# ---------------------------------------------------------------------------
# _build_schema
# ---------------------------------------------------------------------------

class TestBuildSchema(unittest.TestCase):
    def test_optional_params(self):
        def func(x: str, limit: int = 10) -> List[str]:
            """Test func."""
            return []

        schema = _build_schema(func, "func", "")
        limit_param = next(p for p in schema.params if p.name == "limit")
        self.assertFalse(limit_param.required)
        self.assertEqual(limit_param.default, 10)

    def test_return_annotation_list(self):
        def func() -> List[dict]:
            return []

        schema = _build_schema(func, "f", "")
        self.assertEqual(schema.return_annotation, "List[dict]")

    def test_return_annotation_generic(self):
        def func() -> Optional[UserInfo]:
            return None

        schema = _build_schema(func, "f", "")
        self.assertEqual(schema.return_annotation, "Optional[UserInfo]")


# ---------------------------------------------------------------------------
# ToolSchema.code_signature / dsl_stub / full_code_block
# ---------------------------------------------------------------------------

class TestToolSchemaOutput(_ToolTestBase):
    def setUp(self):
        super().setUp()

        @tool(name="get-user")
        def get_user(user_id: str, include_deleted: bool = False) -> UserInfo:
            """Fetch user."""
            return UserInfo("Alice", 30, "vip")

        self.func = get_user
        self.schema: ToolSchema = getattr(get_user, _TOOL_MARKER_ATTR)

    def test_code_signature_basic(self):
        sig = self.schema.code_signature()
        self.assertIn("def get_user", sig)
        self.assertIn("user_id: str", sig)
        self.assertIn("include_deleted: bool = False", sig)
        self.assertIn("-> UserInfo", sig)
        self.assertIn('"""Fetch user."""', sig)

    def test_code_signature_handles_hyphen(self):
        sig = self.schema.code_signature()
        self.assertIn('# TOOL(action="get-user"', sig)
        self.assertIn("def get_user(", sig)

    def test_dsl_stub(self):
        stub = self.schema.dsl_stub()
        self.assertIn("GET_USER(", stub)
        self.assertIn("user_id: str", stub)
        self.assertIn("-> UserInfo", stub)

    def test_dsl_stub_custom_placeholder(self):
        stub = self.schema.dsl_stub("MY_GET_USER")
        self.assertIn("MY_GET_USER(", stub)

    def test_full_code_block_includes_type_def(self):
        register_tool_node(self.func)
        block = self.schema.full_code_block()
        self.assertIn("class UserInfo:", block)
        self.assertIn("name: str", block)
        self.assertIn("def get_user(", block)


# ---------------------------------------------------------------------------
# make_tool_node_class
# ---------------------------------------------------------------------------

class TestMakeToolNodeClass(_ToolTestBase):
    def test_creates_node_subclass(self):
        from plaita.node.basic import Node
        def my_func(x: str) -> str:
            return x.upper()

        schema = _build_schema(my_func, "my_func", "My func")
        cls = make_tool_node_class(my_func, "my_func", schema)
        self.assertTrue(issubclass(cls, Node))
        self.assertEqual(cls.node_type, "my_func")

    def test_node_execute_calls_func(self):
        def double(n: int) -> int:
            """Double n."""
            return n * 2

        schema = _build_schema(double, "double", "")
        cls = make_tool_node_class(double, "double", schema)

        registry = get_default_registry()
        registry.register(cls)
        try:
            from plaita import Flow
            flow = Flow.model_validate({
                "flow_id": "test", "version": "1", "runtime": "python",
                "inputType": {"dataType": "object"},
                "nodes": [
                    {"type": "start", "id": "start", "next": "d"},
                    {"type": "double", "id": "d", "n": "$INPUT.n", "next": "end"},
                    {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.d"},
                ],
            })
            result = flow.run(n=21)
            self.assertEqual(result, 42)
        finally:
            registry.unregister("double")

    def test_hyphen_name_converted(self):
        def func() -> None:
            pass
        schema = _build_schema(func, "my-tool", "")
        cls = make_tool_node_class(func, "my-tool", schema)
        self.assertEqual(cls.node_type, "my_tool")


# ---------------------------------------------------------------------------
# register_tool_node
# ---------------------------------------------------------------------------

class TestRegisterToolNode(_ToolTestBase):
    def test_registers_per_tool_node_in_registry(self):
        def get_item(item_id: str) -> dict:
            """Get item."""
            return {"id": item_id}

        specs = register_tool_node(get_item)
        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0].node_type, "get_item")
        self.assertEqual(specs[0].placeholder, "GET_ITEM")

        registry = get_default_registry()
        cls = registry.get("get_item")
        self.assertIsNotNone(cls)

    def test_per_tool_node_executes_in_flow(self):
        def compute(x: int, y: int) -> int:
            """Add x and y."""
            return x + y

        register_tool_node(compute)
        from plaita import Flow
        flow = Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "c"},
                {"type": "compute", "id": "c", "x": "$INPUT.a", "y": "$INPUT.b", "next": "end"},
                {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.c"},
            ],
        })
        result = flow.run(a=3, b=7)
        self.assertEqual(result, 10)

    def test_multiple_tools_registered(self):
        def tool_a(x: str) -> str:
            """A."""
            return x

        def tool_b(n: int) -> int:
            """B."""
            return n

        specs = register_tool_node(tool_a, tool_b)
        self.assertEqual(len(specs), 2)
        names = {s.node_type for s in specs}
        self.assertIn("tool_a", names)
        self.assertIn("tool_b", names)

    def test_tool_node_clear_removes_dynamic_nodes(self):
        def my_tool(x: str) -> str:
            """My tool."""
            return x

        register_tool_node(my_tool)
        registry = get_default_registry()
        self.assertIsNotNone(registry.get("my_tool"))

        ToolNode.clear()
        # After clear, dynamic node should be unregistered
        self.assertIsNone(registry.get("my_tool"))

    def test_tool_with_at_tool_decorator_uses_custom_name(self):
        @tool(name="custom-name")
        def my_func(val: str) -> str:
            """Custom named tool."""
            return val

        specs = register_tool_node(my_func)
        self.assertEqual(specs[0].name, "custom-name")
        self.assertEqual(specs[0].node_type, "custom_name")
        self.assertEqual(specs[0].placeholder, "CUSTOM_NAME")


# ---------------------------------------------------------------------------
# auth_context injection
# ---------------------------------------------------------------------------

class TestAuthContextInjection(_ToolTestBase):
    def test_auth_context_injected_when_declared(self):
        received_auth = {}

        def secured(user_id: str, auth_context: Optional[dict] = None) -> str:
            """Secured tool."""
            received_auth.update(auth_context or {})
            return user_id

        register_tool_node(secured)
        from plaita import Flow
        flow = Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "s"},
                {"type": "secured", "id": "s", "user_id": "$INPUT.uid", "next": "end"},
                {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.s"},
            ],
        })
        flow.global_context = {"auth_context": {"token": "abc123"}}
        result = flow.run(uid="u001")
        self.assertEqual(result, "u001")
        self.assertEqual(received_auth, {"token": "abc123"})

    def test_auth_context_not_injected_when_not_declared(self):
        call_kwargs = {}

        def plain_tool(x: str) -> str:
            """Plain."""
            call_kwargs["x"] = x
            return x

        register_tool_node(plain_tool)
        from plaita import Flow
        flow = Flow.model_validate({
            "flow_id": "test", "version": "1", "runtime": "python",
            "inputType": {"dataType": "object"},
            "nodes": [
                {"type": "start", "id": "start", "next": "p"},
                {"type": "plain_tool", "id": "p", "x": "$INPUT.x", "next": "end"},
                {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.p"},
            ],
        })
        flow.global_context = {"auth_context": {"token": "secret"}}
        result = flow.run(x="hello")
        self.assertEqual(result, "hello")
        self.assertNotIn("auth_context", call_kwargs)


# ---------------------------------------------------------------------------
# list_tools
# ---------------------------------------------------------------------------

class TestListTools(_ToolTestBase):
    def test_list_tools_code_format(self):
        def get_data(key: str) -> dict:
            """Get data by key."""
            return {}

        register_tool_node(get_data)
        tools = list_tools(as_code=True)
        # Should have at least one stub
        stubs = [t for t in tools if "GET_DATA" in t]
        self.assertTrue(stubs, "Expected GET_DATA stub in list_tools output")

    def test_list_tools_json_format(self):
        def get_user(user_id: str) -> str:
            """Get user name."""
            return "Alice"

        register_tool_node(get_user)
        tools = list_tools(as_code=False)
        self.assertIsInstance(tools, list)
        names = [t["name"] for t in tools]
        self.assertIn("get_user", names)

    def test_list_tools_complex_types_deduplicated(self):
        def tool_a(x: str) -> UserInfo:
            """A."""
            return UserInfo("Alice", 30, "vip")

        def tool_b(y: str) -> UserInfo:
            """B."""
            return UserInfo("Bob", 25, "normal")

        register_tool_node(tool_a, tool_b)
        tools = list_tools(as_code=True)
        # Find the type definitions block
        type_blocks = [t for t in tools if "class UserInfo:" in t]
        self.assertEqual(len(type_blocks), 1, "UserInfo should appear exactly once")
        # Count occurrences in full output
        full_output = "\n".join(tools)
        self.assertEqual(full_output.count("class UserInfo:"), 1)

    def test_list_tools_includes_nested_types(self):
        def get_order(order_id: str) -> Order:
            """Get order."""
            return Order("o1", UserInfo("Alice", 30, "vip"), [])

        register_tool_node(get_order)
        tools = list_tools(as_code=True)
        full = "\n".join(tools)
        self.assertIn("class Order:", full)
        self.assertIn("class UserInfo:", full)
        self.assertIn("class OrderLine:", full)

    def test_list_tools_empty(self):
        tools = list_tools(as_code=True)
        self.assertEqual(tools, [])


# ---------------------------------------------------------------------------
# register_tools_from_module
# ---------------------------------------------------------------------------

class TestRegisterToolsFromModule(_ToolTestBase):
    def test_discovers_tool_decorated_functions(self):
        import types
        mod = types.ModuleType("test_mod")
        mod.__name__ = "test_mod"

        @tool
        def tool_fn(x: str) -> str:
            """Decorated tool."""
            return x

        setattr(mod, "tool_fn", tool_fn)
        tool_fn.__module__ = "test_mod"

        specs = register_tools_from_module(mod)
        names = [s.name for s in specs]
        self.assertIn("tool_fn", names)

    def test_auto_discover_skips_private(self):
        import types
        mod = types.ModuleType("test_mod2")
        mod.__name__ = "test_mod2"

        def _private(x: str) -> str:
            """Private."""
            return x

        def public_fn(x: str) -> str:
            """Public function."""
            return x

        setattr(mod, "_private", _private)
        setattr(mod, "public_fn", public_fn)
        _private.__module__ = "test_mod2"
        public_fn.__module__ = "test_mod2"

        specs = register_tools_from_module(mod)
        names = [s.name for s in specs]
        self.assertNotIn("_private", names)
        self.assertIn("public_fn", names)

    def test_auto_discover_skips_no_docstring(self):
        import types
        mod = types.ModuleType("test_mod3")
        mod.__name__ = "test_mod3"

        def no_doc(x: str) -> str:
            return x

        setattr(mod, "no_doc", no_doc)
        no_doc.__module__ = "test_mod3"

        specs = register_tools_from_module(mod, auto_discover=True)
        names = [s.name for s in specs]
        self.assertNotIn("no_doc", names)


# ---------------------------------------------------------------------------
# _collect_complex_types / _complex_type_stub
# ---------------------------------------------------------------------------

class TestComplexTypeHelpers(unittest.TestCase):
    def test_collect_simple_type(self):
        result = _collect_complex_types(str)
        self.assertEqual(result, {})

    def test_collect_dataclass(self):
        result = _collect_complex_types(UserInfo)
        self.assertIn("UserInfo", result)

    def test_collect_nested_dataclass(self):
        result = _collect_complex_types(Order)
        self.assertIn("Order", result)
        self.assertIn("UserInfo", result)
        self.assertIn("OrderLine", result)

    def test_collect_generic(self):
        result = _collect_complex_types(List[UserInfo])
        self.assertIn("UserInfo", result)

    def test_collect_optional(self):
        result = _collect_complex_types(Optional[Order])
        self.assertIn("Order", result)

    def test_complex_type_stub_dataclass(self):
        stub = _complex_type_stub(UserInfo)
        self.assertIn("class UserInfo:", stub)
        self.assertIn("name: str", stub)
        self.assertIn("age: int", stub)

    def test_complex_type_stub_typeddict(self):
        stub = _complex_type_stub(SearchResult)
        self.assertIn("class SearchResult:", stub)
        self.assertIn("items:", stub)
        self.assertIn("total:", stub)


if __name__ == "__main__":
    unittest.main()
