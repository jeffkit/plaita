"""FoT planner — LangChain 1.x messages API (no legacy Chain / JSON actions)."""

from __future__ import annotations

from typing import Any, List, Optional, Sequence

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from plaita_ai.agent.fot.extract import extract_flow_source
from plaita_ai.agent.fot.prompts import (
    COMPOSE_SYSTEM,
    COMPOSE_USER,
    REVIEW_SYSTEM,
    REVIEW_USER,
    format_compile_errors,
    format_dsl_section,
    format_instruction_section,
    format_tools_section,
)
from plaita_ai.agent.fot.tools import ToolLike, ToolSpec, register_tool_node, tools_prompt_section
from plaita_ai.flow_runner import CompileError, CompileResult, compile_flow, get_skill_reference


def _load_dsl_reference() -> str:
    """Load the canonical flow-coder DSL reference once per process."""
    try:
        return get_skill_reference("flow-coder", "codeflow-reference.md")
    except FileNotFoundError:
        return ""


_DSL_REFERENCE = _load_dsl_reference()


def _invoke_model(model: BaseChatModel, system: str, user: str) -> str:
    response = model.invoke(
        [SystemMessage(content=system), HumanMessage(content=user)],
    )
    content = response.content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text", "")))
        content = "".join(parts)
    return str(content)


def plan_flow_source(
    model: BaseChatModel,
    task: str,
    *,
    tools: Optional[Sequence[ToolLike]] = None,
    instruction: str = "",
    tool_specs: Optional[List[ToolSpec]] = None,
) -> str:
    specs = tool_specs or (register_tool_node(*tools) if tools else [])
    system = COMPOSE_SYSTEM.format(
        tools_section=format_tools_section(tools_prompt_section(specs)),
        dsl_section=format_dsl_section(_DSL_REFERENCE),
        instruction_section=format_instruction_section(instruction),
    )
    user = COMPOSE_USER.format(task=task)
    raw = _invoke_model(model, system, user)
    return extract_flow_source(raw)


def review_flow_source(
    model: BaseChatModel,
    task: str,
    source: str,
    errors: List[CompileError],
    *,
    instruction: str = "",
    tool_specs: Optional[List[ToolSpec]] = None,
    tools: Optional[Sequence[ToolLike]] = None,
) -> str:
    specs = tool_specs or (register_tool_node(*tools) if tools else [])
    system = REVIEW_SYSTEM.format(
        tools_section=format_tools_section(tools_prompt_section(specs)),
        dsl_section=format_dsl_section(_DSL_REFERENCE),
        instruction_section=format_instruction_section(instruction),
    )
    user = REVIEW_USER.format(
        task=task,
        source=source,
        errors=format_compile_errors(errors),
    )
    raw = _invoke_model(model, system, user)
    return extract_flow_source(raw)


def plan_with_compile_loop(
    model: BaseChatModel,
    task: str,
    *,
    tools: Optional[Sequence[ToolLike]] = None,
    instruction: str = "",
    max_retries: int = 3,
    flow_id: Optional[str] = None,
) -> tuple[str, CompileResult, int]:
    """Compose/review until compile succeeds or retries exhausted."""
    specs = register_tool_node(*tools) if tools else []
    source = ""
    compiled = CompileResult(ok=False, errors=[CompileError(line=None, message="未开始")])
    attempts = 0

    for attempt in range(max_retries):
        attempts = attempt + 1
        if attempt == 0 or not source:
            source = plan_flow_source(
                model,
                task,
                instruction=instruction,
                tool_specs=specs,
            )
        else:
            source = review_flow_source(
                model,
                task,
                source,
                compiled.errors,
                instruction=instruction,
                tool_specs=specs,
            )

        compiled = compile_flow(source, flow_id=flow_id)
        if compiled.ok:
            return source, compiled, attempts

    return source, compiled, attempts
