"""Integration tests for MCP server tool endpoints and flow_runner module.

Tests cover:
- plaita_ai/flow_runner.py: compile_flow, run_flow, list_node_types,
  list_tools_registered, result_json
- plaita_ai/mcp/server.py: flow_compile, flow_run, flow_list_nodes,
  flow_list_tools, _load_plugins, _build_tool_instructions
"""
from __future__ import annotations

import json
import os
import sys
import types
import unittest
from dataclasses import dataclass
from typing import List, Optional
from unittest.mock import patch

# Ensure plaita-ai is importable
_HERE = os.path.dirname(__file__)
_PLAITA_AI = os.path.dirname(_HERE)
if _PLAITA_AI not in sys.path:
    sys.path.insert(0, _PLAITA_AI)

from plaita_ai.flow_runner import (
    CompileError,
    CompileResult,
    RunResult,
    _parse_codeflow_error,
    compile_flow,
    dumps_json,
    list_node_types,
    list_tools_registered,
    result_json,
    run_flow,
)
from plaita_ai.agent.fot.tools import (
    ToolNode,
    register_tool_node,
    tool,
)

# Check if mcp package is available
try:
    from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: F401
    _MCP_AVAILABLE = True
except ImportError:
    _MCP_AVAILABLE = False

_requires_mcp = unittest.skipUnless(_MCP_AVAILABLE, "mcp package not installed")


# ---------------------------------------------------------------------------
# Test fixtures — simple @flow source strings
# ---------------------------------------------------------------------------

SIMPLE_FLOW = '''
@flow("add_numbers", input_type="object")
def add_numbers(INPUT):
    a = INPUT.a
    b = INPUT.b
    result = F.add(a, b)
    return result
'''

INVALID_FLOW = """
this is not valid python @flow syntax !!!
"""

RUNTIME_ERROR_FLOW = '''
@flow("crash_flow", input_type="object")
def crash_flow(INPUT):
    result = NONEXISTENT_NODE(x=INPUT.x)
    return result
'''

GREETING_FLOW = '''
@flow("greet", input_type="object")
def greet(INPUT):
    name = INPUT.name
    result = F.concat("Hello, ", name, "!")
    return result
'''


# ---------------------------------------------------------------------------
# _parse_codeflow_error
# ---------------------------------------------------------------------------

class TestParseCodeflowError(unittest.TestCase):
    def test_parses_codeflow_format(self):
        exc = Exception("[codeflow] 第 5 行: undefined variable")
        err = _parse_codeflow_error(exc)
        self.assertEqual(err.line, 5)
        self.assertEqual(err.message, "undefined variable")

    def test_parses_question_mark_line(self):
        exc = Exception("[codeflow] 第 ? 行: unknown error")
        err = _parse_codeflow_error(exc)
        self.assertIsNone(err.line)
        self.assertEqual(err.message, "unknown error")

    def test_unmatched_returns_full_message(self):
        exc = Exception("some other error")
        err = _parse_codeflow_error(exc)
        self.assertIsNone(err.line)
        self.assertEqual(err.message, "some other error")


# ---------------------------------------------------------------------------
# compile_flow
# ---------------------------------------------------------------------------

class TestCompileFlow(unittest.TestCase):
    def test_valid_source_returns_ok(self):
        result = compile_flow(SIMPLE_FLOW)
        self.assertTrue(result.ok)
        self.assertIsNotNone(result.ir)
        self.assertEqual(result.errors, [])

    def test_valid_source_with_flow_id(self):
        result = compile_flow(SIMPLE_FLOW, flow_id="add_numbers")
        self.assertTrue(result.ok)
        self.assertEqual(result.flow_id, "add_numbers")

    def test_invalid_source_returns_errors(self):
        result = compile_flow(INVALID_FLOW)
        self.assertFalse(result.ok)
        self.assertTrue(len(result.errors) > 0)
        self.assertIsInstance(result.errors[0], CompileError)

    def test_result_has_ir_on_success(self):
        result = compile_flow(GREETING_FLOW)
        self.assertTrue(result.ok)
        self.assertIn("nodes", result.ir)

    def test_to_dict_ok(self):
        result = compile_flow(SIMPLE_FLOW)
        d = result.to_dict()
        self.assertIn("ok", d)
        self.assertIn("ir", d)
        self.assertIn("errors", d)
        self.assertTrue(d["ok"])

    def test_to_dict_error(self):
        result = compile_flow(INVALID_FLOW)
        d = result.to_dict()
        self.assertFalse(d["ok"])
        self.assertIsInstance(d["errors"], list)


# ---------------------------------------------------------------------------
# run_flow
# ---------------------------------------------------------------------------

class TestRunFlow(unittest.TestCase):
    def test_simple_flow_runs(self):
        result = run_flow(SIMPLE_FLOW, {"a": 3, "b": 7})
        self.assertTrue(result.ok)
        self.assertEqual(result.result, 10)
        self.assertIsNone(result.error)

    def test_greeting_flow_runs(self):
        result = run_flow(GREETING_FLOW, {"name": "World"})
        self.assertTrue(result.ok)
        self.assertIn("Hello", str(result.result))
        self.assertIn("World", str(result.result))

    def test_compile_error_returns_not_ok(self):
        result = run_flow(INVALID_FLOW, {})
        self.assertFalse(result.ok)
        self.assertEqual(result.error_type, "compile")
        self.assertIsNotNone(result.error)

    def test_runtime_error_returns_not_ok(self):
        """Flow with unknown node type fails with compile/runtime error."""
        result = run_flow(RUNTIME_ERROR_FLOW, {"x": 0})
        self.assertFalse(result.ok)
        self.assertIsNotNone(result.error_type)
        self.assertIsNotNone(result.error)

    def test_flow_with_globals(self):
        source = '''
@flow("global_flow", input_type="object")
def global_flow(INPUT):
    return "ok"
'''
        result = run_flow(source, {}, globals_ctx={"key": "val"})
        self.assertTrue(result.ok)

    def test_to_dict(self):
        result = run_flow(SIMPLE_FLOW, {"a": 1, "b": 2})
        d = result.to_dict()
        self.assertIn("ok", d)
        self.assertIn("result", d)
        self.assertIn("error", d)
        self.assertIn("error_type", d)

    def test_with_flow_id(self):
        result = run_flow(SIMPLE_FLOW, {"a": 5, "b": 5}, flow_id="add_numbers")
        self.assertTrue(result.ok)
        self.assertEqual(result.result, 10)


# ---------------------------------------------------------------------------
# list_node_types
# ---------------------------------------------------------------------------

class TestListNodeTypes(unittest.TestCase):
    def test_returns_list(self):
        nodes = list_node_types()
        self.assertIsInstance(nodes, list)
        self.assertTrue(len(nodes) > 0)

    def test_each_entry_has_required_keys(self):
        nodes = list_node_types()
        for node in nodes:
            self.assertIn("node_type", node)
            self.assertIn("placeholder", node)
            self.assertIn("node_name", node)

    def test_placeholder_is_uppercase(self):
        nodes = list_node_types()
        for node in nodes[:5]:
            self.assertEqual(node["placeholder"], node["node_type"].upper())

    def test_includes_builtin_nodes(self):
        nodes = list_node_types()
        types_ = {n["node_type"] for n in nodes}
        self.assertIn("start", types_)
        self.assertIn("end", types_)


# ---------------------------------------------------------------------------
# list_tools_registered
# ---------------------------------------------------------------------------

class TestListToolsRegistered(unittest.TestCase):
    def setUp(self):
        ToolNode.clear()

    def tearDown(self):
        ToolNode.clear()

    def test_empty_when_no_tools(self):
        result = list_tools_registered(as_code=True)
        self.assertEqual(result, [])

    def test_returns_code_stubs(self):
        def my_tool(x: str) -> str:
            """My tool."""
            return x

        register_tool_node(my_tool)
        result = list_tools_registered(as_code=True)
        self.assertIsInstance(result, list)
        full = "\n".join(result)
        # DSL stub uses UPPERCASE placeholder; also check description
        self.assertTrue("MY_TOOL" in full or "my_tool" in full.lower())

    def test_returns_json_format(self):
        def my_tool2(n: int) -> int:
            """My tool 2."""
            return n

        register_tool_node(my_tool2)
        result = list_tools_registered(as_code=False)
        self.assertIsInstance(result, list)
        names = [t["name"] for t in result]
        self.assertIn("my_tool2", names)


# ---------------------------------------------------------------------------
# result_json / dumps_json
# ---------------------------------------------------------------------------

class TestResultJson(unittest.TestCase):
    def test_with_to_dict_object(self):
        r = CompileResult(ok=True, flow_id="test", ir={"nodes": []})
        output = result_json(r)
        data = json.loads(output)
        self.assertTrue(data["ok"])
        self.assertEqual(data["flow_id"], "test")

    def test_with_plain_dict(self):
        output = result_json({"key": "value"})
        data = json.loads(output)
        self.assertEqual(data["key"], "value")

    def test_with_list(self):
        output = result_json([1, 2, 3])
        data = json.loads(output)
        self.assertEqual(data, [1, 2, 3])

    def test_dumps_json_ensure_chinese(self):
        output = dumps_json({"msg": "你好"})
        self.assertIn("你好", output)


# ---------------------------------------------------------------------------
# MCP server tools (server.py)
# ---------------------------------------------------------------------------

@_requires_mcp
class TestMcpServerTools(unittest.TestCase):
    def setUp(self):
        ToolNode.clear()

    def tearDown(self):
        ToolNode.clear()

    def test_flow_compile_valid(self):
        from plaita_ai.mcp.server import flow_compile
        output = flow_compile(SIMPLE_FLOW)
        data = json.loads(output)
        self.assertTrue(data["ok"])
        self.assertIn("ir", data)

    def test_flow_compile_invalid(self):
        from plaita_ai.mcp.server import flow_compile
        output = flow_compile(INVALID_FLOW)
        data = json.loads(output)
        self.assertFalse(data["ok"])
        self.assertTrue(len(data["errors"]) > 0)

    def test_flow_compile_with_flow_id(self):
        from plaita_ai.mcp.server import flow_compile
        output = flow_compile(SIMPLE_FLOW, flow_id="add_numbers")
        data = json.loads(output)
        self.assertTrue(data["ok"])
        self.assertEqual(data["flow_id"], "add_numbers")

    def test_flow_run_valid(self):
        from plaita_ai.mcp.server import flow_run
        output = flow_run(SIMPLE_FLOW, inputs_json='{"a": 4, "b": 6}')
        data = json.loads(output)
        self.assertTrue(data["ok"])
        self.assertEqual(data["result"], 10)

    def test_flow_run_compile_error(self):
        from plaita_ai.mcp.server import flow_run
        output = flow_run(INVALID_FLOW)
        data = json.loads(output)
        self.assertFalse(data["ok"])
        self.assertEqual(data["error_type"], "compile")

    def test_flow_run_with_globals(self):
        from plaita_ai.mcp.server import flow_run
        output = flow_run(SIMPLE_FLOW, inputs_json='{"a": 1, "b": 2}', globals_json='{"key": "val"}')
        data = json.loads(output)
        self.assertTrue(data["ok"])

    def test_flow_list_nodes_returns_json(self):
        from plaita_ai.mcp.server import flow_list_nodes
        output = flow_list_nodes()
        data = json.loads(output)
        self.assertIsInstance(data, list)
        self.assertTrue(len(data) > 0)

    def test_flow_list_nodes_includes_builtins(self):
        from plaita_ai.mcp.server import flow_list_nodes
        output = flow_list_nodes()
        data = json.loads(output)
        types_ = {n["node_type"] for n in data}
        self.assertIn("start", types_)
        self.assertIn("end", types_)

    def test_flow_list_tools_empty(self):
        from plaita_ai.mcp.server import flow_list_tools
        output = flow_list_tools()
        self.assertIn("No tools registered", output)

    def test_flow_list_tools_with_registered_tool(self):
        from plaita_ai.mcp.server import flow_list_tools

        def search(query: str) -> list:
            """Search for items."""
            return []

        register_tool_node(search)
        output = flow_list_tools(as_json=False)
        self.assertIn("search", output.lower())

    def test_flow_list_tools_as_json(self):
        from plaita_ai.mcp.server import flow_list_tools

        def my_action(key: str) -> str:
            """Do action."""
            return key

        register_tool_node(my_action)
        output = flow_list_tools(as_json=True)
        data = json.loads(output)
        self.assertIsInstance(data, list)
        names = [t["name"] for t in data]
        self.assertIn("my_action", names)

    def test_flow_list_tools_type_section(self):
        """Test that complex types produce a type definitions section."""
        from plaita_ai.mcp.server import flow_list_tools

        @dataclass
        class ComplexResult:
            items: list
            count: int

        def get_results() -> ComplexResult:
            """Get results."""
            return ComplexResult([], 0)

        register_tool_node(get_results)
        output = flow_list_tools(as_json=False)
        # Should include both type defs and tool signature
        self.assertIn("ComplexResult", output)

    def test_flow_get_skill_returns_text(self):
        from plaita_ai.mcp.server import flow_get_skill
        try:
            output = flow_get_skill()
            self.assertIsInstance(output, str)
            self.assertTrue(len(output) > 0)
        except FileNotFoundError:
            self.skipTest("flow-coder skill not installed")

    def test_flow_get_skill_reference(self):
        from plaita_ai.mcp.server import flow_get_skill_reference
        try:
            output = flow_get_skill_reference()
            self.assertIsInstance(output, str)
        except FileNotFoundError:
            self.skipTest("skill reference not installed")


# ---------------------------------------------------------------------------
# _load_plugins
# ---------------------------------------------------------------------------

@_requires_mcp
class TestLoadPlugins(unittest.TestCase):
    def test_load_valid_module(self):
        from plaita_ai.mcp.server import _load_plugins
        # Use a known importable module
        _load_plugins(["json"], [])

    def test_load_invalid_module_raises_systemexit(self):
        from plaita_ai.mcp.server import _load_plugins
        with self.assertRaises(SystemExit):
            _load_plugins(["nonexistent.module.xyz"], [])

    def test_extra_paths_added_to_sys_path(self):
        from plaita_ai.mcp.server import _load_plugins
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            _load_plugins([], [tmpdir])
            self.assertIn(tmpdir, sys.path)
            sys.path.remove(tmpdir)

    def test_empty_plugins_no_op(self):
        from plaita_ai.mcp.server import _load_plugins
        # Should not raise
        _load_plugins([], [])


# ---------------------------------------------------------------------------
# _build_tool_instructions
# ---------------------------------------------------------------------------

@_requires_mcp
class TestBuildToolInstructions(unittest.TestCase):
    def setUp(self):
        ToolNode.clear()

    def tearDown(self):
        ToolNode.clear()

    def test_empty_when_no_tools(self):
        from plaita_ai.mcp.server import _build_tool_instructions
        result = _build_tool_instructions()
        self.assertEqual(result, "")

    def test_returns_instructions_with_tools(self):
        from plaita_ai.mcp.server import _build_tool_instructions

        def fetch_data(url: str) -> dict:
            """Fetch data from URL."""
            return {}

        register_tool_node(fetch_data)
        result = _build_tool_instructions()
        self.assertIsInstance(result, str)
        self.assertIn("Custom Nodes", result)


if __name__ == "__main__":
    unittest.main()
