"""MCP server exposing plaita @flow compile/run tools."""

from __future__ import annotations

import importlib
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

from plaita_ai.flow_runner import (
    compile_flow,
    get_skill_reference,
    get_skill_text,
    list_node_types,
    list_tools_registered,
    result_json,
    run_flow,
)

logger = logging.getLogger(__name__)

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "mcp package is required for plaita-ai MCP server. Install with: pip install plaita-ai"
    ) from exc

mcp = FastMCP(
    "plaita-flow",
    instructions=(
        "Compile and run Plaita @flow DSL workflows. "
        "Use flow_compile to validate generated source before flow_run. "
        "On compile errors, fix the @flow source using line numbers and retry."
    ),
)


def _load_plugins(plugins: List[str], extra_paths: List[str]) -> None:
    """Import plugin modules so their node registrations take effect.

    Args:
        plugins: List of Python module paths to import (e.g. ``myapp.nodes``).
        extra_paths: Directories prepended to ``sys.path`` before importing.
            Useful when plugin modules live outside an installed package.
    """
    for p in extra_paths:
        if p not in sys.path:
            sys.path.insert(0, p)
    for mod_path in plugins:
        try:
            importlib.import_module(mod_path)
            logger.info("plaita-ai mcp: loaded plugin %r", mod_path)
        except Exception as exc:
            logger.error("plaita-ai mcp: failed to load plugin %r: %s", mod_path, exc)
            raise SystemExit(
                f"插件模块 {mod_path!r} 加载失败: {exc}\n"
                "请确认模块路径正确，且相关依赖已安装。"
            ) from exc


@mcp.tool()
def flow_compile(source: str, flow_id: Optional[str] = None) -> str:
    """Compile @flow Python source to Flow IR without executing.

    Args:
        source: Complete @flow source string (may include @childflow functions).
        flow_id: Optional main flow function name when multiple @flow defs exist.

    Returns:
        JSON: {ok, flow_id, ir, errors:[{line, message}]}
    """
    result = compile_flow(source, flow_id=flow_id)
    return result_json(result)


@mcp.tool()
def flow_run(
    source: str,
    inputs_json: str = "{}",
    flow_id: Optional[str] = None,
    globals_json: Optional[str] = None,
) -> str:
    """Compile and execute @flow source.

    Args:
        source: Complete @flow source string.
        inputs_json: JSON object passed as flow input fields (INPUT.x).
        flow_id: Optional main flow function name.
        globals_json: Optional JSON object for flow.global_context ($GLOBAL.*).

    Returns:
        JSON: {ok, result, error, error_type}
    """
    inputs: Dict[str, Any] = json.loads(inputs_json or "{}")
    globals_ctx: Optional[Dict[str, Any]] = None
    if globals_json:
        globals_ctx = json.loads(globals_json)
    result = run_flow(source, inputs, flow_id=flow_id, globals_ctx=globals_ctx)
    return result_json(result)


@mcp.tool()
def flow_list_nodes() -> str:
    """List node types registered in plaita (builtin + plugins).

    Custom nodes appear as UPPERCASE placeholders in @flow (e.g. llm -> LLM).

    Returns:
        JSON array: [{node_type, placeholder, node_name}, ...]
    """
    return result_json(list_node_types())


@mcp.tool()
def flow_list_tools(as_json: bool = False) -> str:
    """List all tools registered via @tool / register_tool_node() / register_tools_from_module().

    Call this before writing @flow to discover available TOOL(action=...) actions
    and understand each tool's input parameters and return type.

    Args:
        as_json: When False (default), returns Python-style function signature
            strings — directly usable as reference when writing @flow code.
            When True, returns JSON dicts (backward compat).

    Returns (as_json=False, default):
        Python code block with one ``def tool_name(params) -> return_type:`` stub
        per registered tool.  Example::

            def get_user(user_id: str) -> dict:
                \"\"\"查询用户信息\"\"\"

            def get_order(order_id: str, include_items: bool = False) -> dict:
                \"\"\"查询订单详情\"\"\"

    Returns (as_json=True):
        JSON array: [{name, description, params: {name: {type, required}}, return_type}, ...]
        Empty array if no tools have been registered yet.
    """
    tools = list_tools_registered(as_code=not as_json)
    if as_json:
        return result_json(tools)
    if not tools:
        return "# No tools registered yet.\n# Use register_tool_node() or @tool to register tools."
    # tools list: [type_definitions_block?, sig1, sig2, …]
    # Separate type definitions block from function signatures for clarity
    if len(tools) > 1 and not tools[0].startswith("def ") and not tools[0].startswith("# TOOL"):
        type_section = "# --- Referenced Types ---\n" + tools[0]
        func_section = "\n\n".join(tools[1:])
        return type_section + "\n\n# --- Available Tools ---\n" + func_section
    return "\n\n".join(tools)


@mcp.tool()
def flow_get_skill(skill_name: str = "flow-coder") -> str:
    """Return bundled flow-coder skill instructions for writing @flow DSL."""
    return get_skill_text(skill_name)


@mcp.tool()
def flow_get_skill_reference(
    skill_name: str = "flow-coder",
    reference: str = "codeflow-reference.md",
) -> str:
    """Return a bundled skill reference document (full @flow syntax)."""
    return get_skill_reference(skill_name, reference)


def _build_tool_instructions() -> str:
    """Return a tool-summary block for the MCP server instructions.

    Called after all plugins are loaded so registered tools are known.
    Returns an empty string when no tools are registered.
    """
    tools = list_tools_registered(as_code=True)
    if not tools:
        return ""
    lines = [
        "",
        "## Available Custom Nodes (registered tools)",
        "Call flow_list_tools for full signatures with parameter types.",
        "Use these UPPERCASE node names directly in @flow DSL:",
        "",
    ]
    # tools[0] may be type definitions, rest are DSL stubs
    start = 0
    if tools and not tools[0].startswith(("GET_", "SEND_", "SEARCH_", "def ")) and "\ndef " not in tools[0]:
        start = 1  # skip type definitions block for instructions summary
    for stub in tools[start:]:
        # First line of each stub (the node signature)
        first_line = stub.split("\n")[0]
        lines.append(f"  - {first_line}")
    return "\n".join(lines)


def main(plugins: Optional[List[str]] = None, extra_paths: Optional[List[str]] = None) -> None:
    """Entry point: ``python -m plaita_ai.mcp.server`` or ``plaita-ai mcp``.

    Custom node modules are loaded (in order) before the MCP server starts,
    so generated ``@flow`` can use those node placeholders.

    After plugins are loaded, registered tool signatures are automatically
    appended to the MCP server instructions so AI agents can see the available
    UPPERCASE node names without calling ``flow_list_tools`` first (Phase 1:
    one-time load strategy).

    Plugin resolution order:
    1. ``plugins`` parameter (programmatic / CLI ``--plugin`` flags).
    2. ``PLAITA_PLUGINS`` environment variable (comma-separated module paths).
    3. ``PLAITA_PLUGIN_PATH`` environment variable (extra ``sys.path`` entries).
    """
    all_plugins: List[str] = list(plugins or [])
    env_plugins = os.environ.get("PLAITA_PLUGINS", "").strip()
    if env_plugins:
        all_plugins.extend(p.strip() for p in env_plugins.split(",") if p.strip())

    all_paths: List[str] = list(extra_paths or [])
    env_path = os.environ.get("PLAITA_PLUGIN_PATH", "").strip()
    if env_path:
        all_paths.extend(p.strip() for p in env_path.split(os.pathsep) if p.strip())

    if all_plugins:
        _load_plugins(all_plugins, all_paths)

    # Phase 1: embed registered tool summary into server instructions
    tool_instructions = _build_tool_instructions()
    if tool_instructions:
        base_instructions = mcp.instructions or ""
        mcp.instructions = base_instructions + tool_instructions
        logger.info(
            "plaita-ai mcp: injected %d tool(s) into server instructions",
            len(list_tools_registered(as_code=True)) - 1,  # minus type block if any
        )

    mcp.run()


if __name__ == "__main__":
    main()
