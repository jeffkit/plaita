"""FoT Agent — LangChain 1.x planner + plaita @flow executor."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Union

from langchain.chat_models import init_chat_model
from langchain_core.language_models.chat_models import BaseChatModel

from plaita_ai.agent.fot.planner import (
    plan_with_compile_loop,
    rewrite_flow_source_for_run_error,
)
from plaita_ai.agent.fot.prompts import format_compile_errors
from plaita_ai.agent.fot.tools import ToolLike, register_tool_node
from plaita_ai.flow_runner import CompileError, RunResult, compile_flow, run_flow

ModelInput = Union[str, BaseChatModel]


@dataclass
class FoTResult:
    ok: bool
    result: Any = None
    source: str = ""
    attempts: int = 0
    #: run 失败后执行的修复轮数（独立于编译回路的 attempts）
    run_retries: int = 0
    compile_errors: List[CompileError] = field(default_factory=list)
    run: Optional[RunResult] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "result": self.result,
            "source": self.source,
            "attempts": self.attempts,
            "run_retries": self.run_retries,
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
        max_run_retries: int = 3,
        globals_ctx: Optional[Dict[str, Any]] = None,
        flow_id: Optional[str] = None,
    ) -> None:
        if isinstance(model, str):
            self.model: BaseChatModel = init_chat_model(model)
        else:
            self.model = model
        self.tools = list(tools or [])
        self.instruction = instruction
        #: 编译回路预算：总尝试次数（含首次生成；即 range(max_compile_retries)），
        #: 不是「额外重试次数」——3 意味着最多 1 次 compose + 2 次 review。
        self.max_compile_retries = max_compile_retries
        #: run 失败后的修复预算：额外重试轮数（首次运行不计入；默认 3，
        #: 对齐 flow-coder SKILL.md 第 5 步「最多重试 3 轮」）。
        self.max_run_retries = max_run_retries
        self.globals_ctx = dict(globals_ctx or {})
        self.flow_id = flow_id
        if self.tools:
            register_tool_node(*self.tools)

    async def ainvoke(self, inputs: Dict[str, Any]) -> FoTResult:
        """Async equivalent of ``invoke``.

        The FoT planning loop uses synchronous LLM calls internally.
        ``ainvoke`` offloads the entire plan→compile→run pipeline to a thread
        pool via ``asyncio.to_thread``, so the event loop is never blocked.

        Usage::

            result = await agent.ainvoke({"task": "查北京天气", "city": "北京"})
        """
        return await asyncio.to_thread(self.invoke, inputs)

    def invoke(self, inputs: Dict[str, Any]) -> FoTResult:
        task = str(inputs.get("task") or inputs.get("input") or "").strip()
        if not task:
            raise ValueError("FoTAgent.invoke 需要 task= 或 input= 字段")

        # Strip agent-control keys; keep all user-defined flow input fields.
        # "instruction" is intentionally NOT excluded — it may be a legitimate
        # flow INPUT field.  Override the agent instruction via the constructor
        # parameter, not via invoke().
        run_inputs = {
            k: v
            for k, v in inputs.items()
            if k not in {"task", "input"}
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

        run_inputs = {
            k: v
            for k, v in inputs.items()
            if k not in {"task", "input"}
        }

        # -- run-retry 环：编译通过但执行失败时，把运行错误回喂模型定点修复。
        # 独立于编译回路预算（max_run_retries 轮修复，首次运行不计入）；
        # 编译失败不进入本环（compile 预算耗尽已在上方返回）。
        run_result = run_flow(
            source,
            run_inputs,
            flow_id=self.flow_id,
            globals_ctx=self.globals_ctx,
        )
        run_retries = 0
        feedback = ""
        while not run_result.ok and run_retries < self.max_run_retries:
            run_retries += 1
            instruction = f"{self.instruction}\n{feedback}" if feedback else self.instruction
            try:
                fixed = rewrite_flow_source_for_run_error(
                    self.model,
                    task,
                    source,
                    error=str(run_result.error or "unknown run error"),
                    error_type=str(run_result.error_type or ""),
                    instruction=instruction,
                    tools=self.tools,
                )
            except ValueError as exc:
                # 修复轮输出没有 @flow 源码：这一轮算烧掉，回喂解析提示。
                feedback = f"上一轮修正输出未包含 @flow 源码：{exc}"
                continue
            recompiled = compile_flow(fixed, flow_id=self.flow_id)
            if not recompiled.ok:
                # 修复版没编译过：保留上一版可编译源码，回喂编译错误。
                feedback = "上一轮修正版未通过编译：" + format_compile_errors(recompiled.errors)
                continue
            source = fixed
            feedback = ""
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
                run_retries=run_retries,
                run=run_result,
            )

        return FoTResult(
            ok=True,
            result=run_result.result,
            source=source,
            attempts=attempts,
            run_retries=run_retries,
            run=run_result,
        )
