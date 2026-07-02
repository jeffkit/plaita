"""FoT Agent — LangChain 1.x planner + plaita @flow executor."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Union

from langchain.chat_models import init_chat_model
from langchain_core.language_models.chat_models import BaseChatModel

from plaita_ai.agent.fot.planner import plan_with_compile_loop
from plaita_ai.agent.fot.tools import ToolLike, register_tool_node
from plaita_ai.flow_runner import CompileError, RunResult, run_flow

ModelInput = Union[str, BaseChatModel]


@dataclass
class FoTResult:
    ok: bool
    result: Any = None
    source: str = ""
    attempts: int = 0
    compile_errors: List[CompileError] = field(default_factory=list)
    run: Optional[RunResult] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "result": self.result,
            "source": self.source,
            "attempts": self.attempts,
            "compile_errors": [e.to_dict() for e in self.compile_errors],
            "run": self.run.to_dict() if self.run else None,
        }


class FoTAgent:
    """Flow-of-Thought agent: LLM writes @flow → compile → run.

    Uses LangChain 1.x ``init_chat_model`` + message ``invoke`` — not legacy
    ``Chain`` / ``AgentExecutor`` / JSON actions (edan-style).
    """

    def __init__(
        self,
        model: ModelInput,
        tools: Optional[Sequence[ToolLike]] = None,
        *,
        instruction: str = "",
        max_compile_retries: int = 3,
        globals_ctx: Optional[Dict[str, Any]] = None,
        flow_id: Optional[str] = None,
    ) -> None:
        if isinstance(model, str):
            self.model: BaseChatModel = init_chat_model(model)
        else:
            self.model = model
        self.tools = list(tools or [])
        self.instruction = instruction
        self.max_compile_retries = max_compile_retries
        self.globals_ctx = dict(globals_ctx or {})
        self.flow_id = flow_id
        if self.tools:
            register_tool_node(*self.tools)

    def invoke(self, inputs: Dict[str, Any]) -> FoTResult:
        task = str(inputs.get("task") or inputs.get("input") or "").strip()
        if not task:
            raise ValueError("FoTAgent.invoke 需要 task= 或 input= 字段")

        run_inputs = {
            k: v
            for k, v in inputs.items()
            if k not in {"task", "input", "instruction"}
        }

        source, compiled, attempts = plan_with_compile_loop(
            self.model,
            task,
            tools=self.tools,
            instruction=self.instruction,
            max_retries=self.max_compile_retries,
            flow_id=self.flow_id,
        )

        if not compiled.ok:
            return FoTResult(
                ok=False,
                source=source,
                attempts=attempts,
                compile_errors=list(compiled.errors),
            )

        run_result = run_flow(
            source,
            run_inputs,
            flow_id=self.flow_id,
            globals_ctx=self.globals_ctx,
        )
        if not run_result.ok:
            return FoTResult(
                ok=False,
                source=source,
                attempts=attempts,
                run=run_result,
            )

        return FoTResult(
            ok=True,
            result=run_result.result,
            source=source,
            attempts=attempts,
            run=run_result,
        )
