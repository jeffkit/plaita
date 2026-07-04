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


def main(plugins: Optional[List[str]] = None, extra_paths: Optional[List[str]] = None) -> None:
    """Entry point: ``python -m plaita_ai.mcp.server`` or ``plaita-ai mcp``.

    Custom node modules are loaded (in order) before the MCP server starts,
    so generated ``@flow`` can use those node placeholders.

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

    mcp.run()


if __name__ == "__main__":
    main()
