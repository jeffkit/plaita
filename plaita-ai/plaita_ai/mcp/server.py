"""MCP server exposing plaita @flow compile/run tools."""

from __future__ import annotations

import importlib
import logging
import os
import sys
from typing import Any, Dict, List, Optional

from plaita_ai.flow_runner import (
    PathTraversalError,
    compile_flow,
    get_skill_reference,
    get_skill_text,
    list_node_types,
    list_tools_registered,
    result_json,
    run_flow,
)
from plaita_ai.json_guard import GuardedJsonError, guarded_json_loads

try:
    from plaita.dsl.codeflow import EmitError, emit_source
except ImportError:  # pragma: no cover - plaita is a hard dependency of plaita-ai
    EmitError = emit_source = None

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
        "On compile errors, fix the @flow source using line numbers and retry. "
        "flow_from_json reverse-generates @flow source from a JSON flow "
        "definition (migrate legacy flows to the DSL). "
        "For a deployed plaita (console configured via PLAITA_CONSOLE_URL), the "
        "console_* tools observe versions/runs/metrics, eval_* runs benchmark "
        "datasets against a version, and supervisor_iterate proposes + evaluates "
        "an improved version — it returns a promotion ticket and never publishes."
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
def flow_compile(source: str, flow_id: Optional[str] = None, profile: Optional[str] = None) -> str:
    """Compile @flow Python source to Flow IR without executing.

    Args:
        source: Complete @flow source string (may include @childflow functions).
        flow_id: Optional main flow function name when multiple @flow defs exist.
        profile: Optional target environment: tier name (inline/managed/
            managed:queue) or legacy profile name (normal/generator/distributed/
            local, normalized). When given, the result carries a NON-FATAL
            ``execution_requirements`` report — ``{profile, required, missing}``
            execution tiers the flow needs (recursively, incl. child/parallel/
            reference flows) that the target environment lacks. ``ok`` is
            unchanged: a flow needing suspend/approval capabilities still
            compiles, but you should route it to the managed runtime (plaita#331).

    Returns:
        JSON: {ok, flow_id, ir, errors:[{line, message}], execution_requirements?}
        (``execution_requirements`` present only when ``profile`` is given)
    """
    result = compile_flow(source, flow_id=flow_id, profile=profile)
    return result_json(result)


@mcp.tool()
def flow_from_json(definition: str) -> str:
    """Reverse-generate @flow source from a JSON flow definition (emit_source).

    Use this to migrate existing JSON/YAML flows to the @flow DSL, or to get
    an editable handle on a deployed flow. The generated source is immediately
    re-compiled as a round-trip check — an ``ok: true`` result is guaranteed
    compilable.

    Args:
        definition: Flow definition as a JSON string (flow_id + inputType + nodes[]).

    Returns:
        JSON: {ok, source, flow_id, errors:[{line, message}]}
        ``ok: false`` with ``error`` when the definition contains constructs
        that have no @flow representation (e.g. switch nodes).
    """
    if emit_source is None:  # pragma: no cover
        return result_json({"ok": False, "error": "plaita runtime with emit_source unavailable"})
    try:
        ir = guarded_json_loads(definition, what="definition")
    except GuardedJsonError as exc:
        return result_json({"ok": False, "error": str(exc)})
    try:
        source = emit_source(ir)
    except EmitError as exc:
        return result_json({"ok": False, "error": f"definition cannot be expressed as @flow: {exc}"})
    except Exception as exc:
        return result_json({"ok": False, "error": f"emit failed: {exc}"})
    compiled = compile_flow(source)
    payload: Dict[str, Any] = {
        "ok": bool(compiled.ok),
        "source": source,
        "flow_id": (ir or {}).get("flow_id") if isinstance(ir, dict) else None,
    }
    if not compiled.ok:
        payload["error"] = "round-trip compile failed"
        payload["errors"] = [e.to_dict() for e in compiled.errors]
    return result_json(payload)


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
        JSON: {ok, result, error, error_type}；error_type 是稳定字符串（如
        suspend_not_executable / error_result / compile）。挂起节点失败时额外
        多出 error_detail 键（结构化错误体含 fix，指向分布式运行时，见
        plaita#330）——其余载荷不含该键，形状不变。
    """
    # inputs_json/globals_json 是调用方自控文本：~12k 层嵌套（~24KB）就能让
    # json.loads 抛 RecursionError（A2R51-02），与坏语法同口径降级为具名
    # ok:false 信封，不得穿出工具体。
    try:
        inputs: Dict[str, Any] = guarded_json_loads(inputs_json or "{}", what="inputs_json")
        globals_ctx: Optional[Dict[str, Any]] = None
        if globals_json:
            globals_ctx = guarded_json_loads(globals_json, what="globals_json")
    except GuardedJsonError as exc:
        return result_json({"ok": False, "error": str(exc), "error_type": "GuardedJsonError"})
    result = run_flow(source, inputs, flow_id=flow_id, globals_ctx=globals_ctx)
    return result_json(result)


@mcp.tool()
def flow_list_nodes() -> str:
    """List node types registered in plaita (builtin + plugins).

    Custom nodes appear as UPPERCASE placeholders in @flow (e.g. llm -> LLM).

    Each row also carries authoring-time execution requirements (plaita#331,
    tier model 2026-10-25):
    ``tier`` = execution tier (inline / managed / managed:queue; managed+ parks
    the flow and needs the managed runtime, managed:queue additionally needs
    the queue wakeup services), ``suspends`` = whether the node parks the flow.

    Returns:
        JSON array: [{node_type, placeholder, node_name, tier, suspends}, ...]
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
    """Return bundled flow-coder skill instructions for writing @flow DSL.

    ``skill_name`` is contained to the bundled skills root: traversal,
    absolute paths and other non-bare names are refused with a named
    ``PathTraversalError`` refusal payload instead of reading host files
    (plaita#248).
    """
    try:
        return get_skill_text(skill_name)
    except PathTraversalError as exc:
        return result_json(
            {"ok": False, "error": str(exc), "error_type": "PathTraversalError"}
        )


@mcp.tool()
def flow_get_skill_reference(
    skill_name: str = "flow-coder",
    reference: str = "codeflow-reference.md",
) -> str:
    """Return a bundled skill reference document (full @flow syntax).

    ``reference`` is contained to the skill's references root: absolute paths,
    ``..`` traversal and symlink escapes are refused with a named
    ``PathTraversalError`` refusal payload instead of being read (#54).
    """
    try:
        return get_skill_reference(skill_name, reference)
    except PathTraversalError as exc:
        return result_json(
            {"ok": False, "error": str(exc), "error_type": "PathTraversalError"}
        )


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

    Tool bundle (optional):
    - ``PLAITA_TOOLS`` — path to tools.yaml / tools.json
    - ``PLAITA_RESOURCES`` — path to resources.yaml (datasources / vectorstores)
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

    # Flat tool bundle from env (HTTP/SQL/vector/native) — after plugins so
    # addressing / vectorstore registrations in plugins are available.
    try:
        from plaita_ai.tools.bootstrap import load_tools_from_env

        load_tools_from_env()
    except Exception as exc:
        logger.error("plaita-ai mcp: failed to load PLAITA_TOOLS: %s", exc)
        raise SystemExit(
            f"PLAITA_TOOLS 加载失败: {exc}\n"
            "请检查 YAML/JSON 路径与 schema，或先运行: plaita-ai tools validate <file>"
        ) from exc

    # Phase 1: embed registered tool summary into server instructions
    tool_instructions = _build_tool_instructions()
    if tool_instructions:
        base_instructions = mcp.instructions or ""
        mcp.instructions = base_instructions + tool_instructions
        logger.info(
            "plaita-ai mcp: injected %d tool(s) into server instructions",
            len(list_tools_registered(as_code=True)) - 1,  # minus type block if any
        )

    # Console ops plane (the supervisor data plane) — registered
    # unconditionally; tools read PLAITA_CONSOLE_* config lazily on first
    # call and report a clear error when the console is not configured.
    try:
        from plaita_ai import ops_mcp

        ops_mcp.register(mcp)
    except Exception as exc:  # pragma: no cover — registration is definition-only
        logger.error("plaita-ai mcp: failed to register console ops tools: %s", exc)

    mcp.run()


if __name__ == "__main__":
    main()
