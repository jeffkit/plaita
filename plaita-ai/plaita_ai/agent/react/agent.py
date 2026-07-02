"""Plaita built-in ReAct agent — standard ReAct + optional @flow escalation.

This is a *vanilla LangChain 1.x ``create_agent``* with two additions:
  1. plaita compile/run/list/reference tools injected as normal function-call
     tools (same loop as user tools).
  2. An adaptive system prompt that defaults to plain tool-calling and only
     suggests @flow orchestration when the task genuinely needs it.

When no plaita escalation is desired (``enable_flow=False`` or no plaita tools
injected), it behaves identically to a standard ReAct agent over the provided
tools.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Union

from langchain.agents import create_agent
from langchain.chat_models import init_chat_model
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage

from plaita_ai.agent.fot.tools import ToolLike, register_tool_node
from plaita_ai.agent.react.prompts import build_system_prompt
from plaita_ai.agent.react.tools import build_plaita_tools

ModelInput = Union[str, BaseChatModel]
Tool = Any  # BaseTool | Callable accepted by create_agent


@dataclass
class PlaitaAgentResult:
    text: str
    messages: List[BaseMessage] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "text": self.text,
            "message_count": len(self.messages),
        }


class PlaitaAgent:
    """Built-in ReAct agent with optional plaita @flow escalation.

    Tools composition:
      - ``tools``: regular function-call tools → injected into the ReAct loop
        **and** registered as plaita ``ToolNode`` so generated @flow can call
        them via ``TOOL(action="<name>", ...)`` (one list, both modes).
      - plaita builtin tools (compile/run/list_nodes/dsl_reference): injected
        as function-call tools when ``enable_flow`` is True (default).
      - ``flow_only_tools``: callables you want available *only* inside @flow
        (not as direct agent tools); registered as ToolNode but not added to
        the agent tool list.

    Example (pure ReAct, no @flow)::

        agent = PlaitaAgent(model="openai:gpt-4o-mini", tools=[weather],
                            enable_flow=False)
        agent.invoke("北京天气？")

    Example (ReAct + @flow escalation, same tools usable both ways)::

        agent = PlaitaAgent(model="openai:gpt-4o-mini", tools=[weather, calc])
        agent.invoke("查北京天气并把温度乘以 2")  # agent decides whether to @flow
    """

    def __init__(
        self,
        model: ModelInput,
        *,
        tools: Optional[Sequence[Tool]] = None,
        flow_only_tools: Optional[Sequence[ToolLike]] = None,
        instruction: str = "",
        globals_ctx: Optional[Dict[str, Any]] = None,
        enable_flow: bool = True,
        debug: bool = False,
    ) -> None:
        if isinstance(model, str):
            chat_model: BaseChatModel = init_chat_model(model)
        else:
            chat_model = model

        user_tools: List[Any] = list(tools or [])
        flow_only: List[ToolLike] = list(flow_only_tools or [])

        # Register user tools + flow-only tools as plaita ToolNode so @flow can
        # TOOL(action=...) them. User tools remain also direct agent tools.
        register_targets: List[ToolLike] = []
        for t in user_tools:
            register_targets.append(t)
        register_targets.extend(flow_only)
        if register_targets:
            register_tool_node(*register_targets)

        all_tools: List[Any] = list(user_tools)
        if enable_flow:
            plaita_tools = build_plaita_tools(
                globals_ctx=globals_ctx,
                flow_tools=None,  # already registered above
            )
            all_tools.extend(plaita_tools)

        self._graph = create_agent(
            model=chat_model,
            tools=all_tools,
            system_prompt=build_system_prompt(
                extra_instruction=instruction,
                enable_flow=enable_flow,
            ),
            debug=debug,
        )
        self.enable_flow = enable_flow

    def invoke(
        self,
        message: str,
        *,
        history: Optional[Sequence[BaseMessage]] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> PlaitaAgentResult:
        """Run one user turn (optional prior messages for multi-turn)."""
        messages: List[BaseMessage] = list(history or [])
        messages.append(HumanMessage(content=message))
        state = self._graph.invoke({"messages": messages}, config=config or {})
        out_messages = list(state.get("messages", messages))
        text = _last_ai_text(out_messages)
        return PlaitaAgentResult(text=text, messages=out_messages)


def _last_ai_text(messages: Sequence[BaseMessage]) -> str:
    for msg in reversed(messages):
        if isinstance(msg, AIMessage):
            content = msg.content
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                parts = []
                for block in content:
                    if isinstance(block, str):
                        parts.append(block)
                    elif isinstance(block, dict) and block.get("type") == "text":
                        parts.append(str(block.get("text", "")))
                return "".join(parts)
    return ""
