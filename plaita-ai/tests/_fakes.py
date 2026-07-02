"""Minimal fake chat model that supports ``bind_tools`` and scripted tool calls.

Lets us drive LangChain 1.x ``create_agent`` end-to-end **without an LLM API
key**: the model pops scripted ``AIMessage`` responses (some with ``tool_calls``)
from a queue, so the real ReAct tool-calling loop executes the plaita tools.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Union

from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult

Scripted = Union[str, AIMessage]


class FakeToolCallingModel(BaseChatModel):
    """Pop scripted responses; supports ``bind_tools`` (schema ignored)."""

    responses: List[AIMessage] = []
    _idx: int = 0

    def __init__(self, responses: Sequence[Scripted]) -> None:
        normalized = [
            r if isinstance(r, AIMessage) else AIMessage(content=r) for r in responses
        ]
        super().__init__(responses=normalized)
        self._idx = 0

    # BaseChatModel requires a model name
    @property
    def _llm_type(self) -> str:
        return "fake-tool-calling"

    def _generate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> ChatResult:
        if self._idx >= len(self.responses):
            # Exhausted: end the loop with a plain text message.
            msg = AIMessage(content="(no more scripted responses)")
        else:
            msg = self.responses[self._idx]
            self._idx += 1
        gen = ChatGeneration(message=msg)
        return ChatResult(generations=[gen])

    def bind_tools(self, tools: Sequence[Any], **kwargs: Any) -> "FakeToolCallingModel":
        # We ignore the schema — scripted tool_calls drive the loop. Returning
        # self is enough for create_agent's model node.
        return self


def ai_tool_call(name: str, args: dict, *, id: str = "1") -> AIMessage:
    """Helper to build a scripted AIMessage that calls a tool."""
    return AIMessage(content="", tool_calls=[{"name": name, "args": args, "id": id}])


def ai_text(text: str) -> AIMessage:
    return AIMessage(content=text)
