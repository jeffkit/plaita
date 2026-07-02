"""Built-in LangChain tools wrapping plaita flow_runner (same kernel as MCP).

These are plain function-call tools — they integrate into any ReAct agent via
LangChain's tool-calling loop, exactly like user-provided tools.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional, Sequence

from langchain.tools import tool

from plaita_ai.agent.fot.tools import ToolLike, register_tool_node
from plaita_ai.flow_runner import (
    compile_flow,
    get_skill_reference,
    get_skill_text,
    list_node_types,
    result_json,
    run_flow,
)


def build_plaita_tools(
    *,
    globals_ctx: Optional[Dict[str, Any]] = None,
    flow_tools: Optional[Sequence[ToolLike]] = None,
) -> List[Callable[..., str]]:
    """Create the four plaita builtin tools as LangChain function-call tools.

    The returned callables are LangChain ``@tool``-decorated functions, so they
    participate in the normal tool-calling loop on equal footing with any
    user-provided tools.

    Args:
        globals_ctx: Merged into ``flow.global_context`` for ``plaita_run_flow``.
        flow_tools: Python callables registered as plaita ``ToolNode`` so that
            generated ``@flow`` can call them via ``TOOL(action=...)``. These
            are *not* exposed as direct agent tools here — pass them to the
            agent as regular ``tools`` if you want both.
    """
    ctx = dict(globals_ctx or {})
    if flow_tools:
        register_tool_node(*flow_tools)

    @tool
    def plaita_compile_flow(source: str, flow_id: Optional[str] = None) -> str:
        """Validate @flow Python DSL source without executing.

        Call this after drafting @flow code. Returns JSON:
        {"ok": bool, "flow_id": str|null, "errors": [{"line": int|null, "message": str}]}.
        On errors, fix the source using the line numbers and compile again.

        Args:
            source: Full @flow source (may include @childflow functions).
            flow_id: Main flow function name when multiple @flow defs exist.
        """
        return result_json(compile_flow(source, flow_id=flow_id))

    @tool
    def plaita_run_flow(
        source: str,
        inputs_json: str = "{}",
        flow_id: Optional[str] = None,
        globals_json: Optional[str] = None,
    ) -> str:
        """Compile and execute @flow source in one step.

        Returns JSON: {"ok": bool, "result": any, "error": str|null, "error_type": str|null}.
        Prefer calling plaita_compile_flow first to catch errors early, but this
        tool also compiles internally and will report compile errors.

        Args:
            source: Full @flow source.
            inputs_json: JSON object for INPUT fields, e.g. '{"name":"alice"}'.
            flow_id: Optional main flow function name.
            globals_json: Optional JSON object for flow.global_context ($GLOBAL.*).
        """
        inputs: Dict[str, Any] = json.loads(inputs_json or "{}")
        run_globals = ctx.copy()
        if globals_json:
            run_globals.update(json.loads(globals_json))
        return result_json(
            run_flow(source, inputs, flow_id=flow_id, globals_ctx=run_globals or None)
        )

    @tool
    def plaita_list_nodes() -> str:
        """List plaita node types available in @flow (builtin + registered + plugins).

        Custom nodes appear as UPPERCASE placeholders (e.g. tool -> TOOL).
        Returns JSON array: [{"node_type": str, "placeholder": str, "node_name": str}].
        """
        return result_json(list_node_types())

    @tool
    def plaita_get_dsl_reference(scope: str = "summary") -> str:
        """Fetch @flow DSL documentation from the flow-coder skill.

        Args:
            scope: "summary"/"skill" for the workflow-oriented SKILL.md,
                "full" for the complete codeflow-reference.md (authoritative syntax).
        """
        if scope == "full":
            return get_skill_reference("flow-coder", "codeflow-reference.md")
        return get_skill_text("flow-coder")

    return [
        plaita_compile_flow,
        plaita_run_flow,
        plaita_list_nodes,
        plaita_get_dsl_reference,
    ]
