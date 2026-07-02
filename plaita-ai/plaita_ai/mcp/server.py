"""MCP server exposing plaita @flow compile/run tools."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from plaita_ai.flow_runner import (
    compile_flow,
    get_skill_reference,
    get_skill_text,
    list_node_types,
    result_json,
    run_flow,
)

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


def main() -> None:
    """Entry point: ``python -m plaita_ai.mcp.server`` or ``plaita-ai mcp``."""
    mcp.run()


if __name__ == "__main__":
    main()
