"""LLM integration test harness — reuse agent-benchmark task set.

Runs FoT / ReAct agents against the 24 benchmark tasks with deepseek-v4-flash
(OpenAI-compatible endpoint) and scores them with the shared validators.

Design: we capture the **@flow source** each agent produces, then run it
deterministically against every test case via ``flow_runner.run_flow``. This
isolates "did the agent write a correct flow" from "did it narrate results".
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Iterable, List, Optional

from tests.llm._bench_tasks import get_tasks, get_validators
from tests.llm.prompts import FOT_INSTRUCTION, PROMPT_VERSION, REACT_INSTRUCTION

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage

from plaita_ai.agent.fot import FoTAgent
from plaita_ai.agent.fot.tools import ToolLike
from plaita_ai.agent.react import PlaitaAgent
from plaita_ai.flow_runner import run_flow

logger = logging.getLogger("plaita_ai.tests.llm.harness")

# Loaded lazily via importlib (see _bench_tasks.py); kept at module scope so
# existing call sites can keep using TASKS / VALIDATORS directly.
TASKS = get_tasks()
VALIDATORS = get_validators()


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

def _resolve_api_key() -> str:
    """Resolve the LLM API key from the environment only.

    Previously this also grepped ``~/.zshrc`` for an ``export DEEPSEEK_API_KEY``
    line — that was invasive (reading the user's shell config), fragile (only
    worked for zsh users with that exact syntax), and gave misleading errors
    on CI. Now: env only. ``PLAITA_LLM_API_KEY`` wins, ``DEEPSEEK_API_KEY`` is
    a backward-compat fallback.
    """
    for name in ("PLAITA_LLM_API_KEY", "DEEPSEEK_API_KEY"):
        key = os.environ.get(name)
        if key:
            return key.strip()
    raise RuntimeError("PLAITA_LLM_API_KEY / DEEPSEEK_API_KEY 未设置")


def model_config() -> Dict[str, Any]:
    """Snapshot the model config derived from env (for result metadata)."""
    return {
        "provider": os.environ.get("PLAITA_LLM_PROVIDER", "openai").lower(),
        "model": os.environ.get("PLAITA_LLM_MODEL", "deepseek-v4-flash"),
        "base_url": os.environ.get("PLAITA_LLM_BASE_URL", "https://api.deepseek.com"),
    }


def build_model(*, seed: Optional[int] = None) -> BaseChatModel:
    """Build a chat model from env (DeepSeek OpenAI-compatible by default).

    ``seed`` is forwarded to providers that support it (OpenAI). Anthropic has
    no seed param; the caller should record the requested seed in result
    metadata regardless, so reproducibility intent is auditable even when the
    provider can't honour it.
    """
    cfg = model_config()
    provider = cfg["provider"]
    model_name = cfg["model"]
    base_url = cfg["base_url"]
    key = _resolve_api_key()

    if provider == "openai":
        from langchain_openai import ChatOpenAI

        kwargs: Dict[str, Any] = dict(
            model=model_name,
            base_url=base_url,
            api_key=key,
            temperature=0,
            timeout=120,
        )
        if seed is not None:
            kwargs["seed"] = seed
        return ChatOpenAI(**kwargs)
    if provider == "anthropic":
        # Anthropic-compatible endpoint (e.g. DeepSeek's /anthropic).
        from langchain_anthropic import ChatAnthropic

        if seed is not None:
            logger.warning(
                "provider=anthropic 不支持 seed；请求的 seed=%s 仅记录到 metadata，"
                "不保证可复现", seed,
            )
        return ChatAnthropic(
            model=model_name,
            base_url=base_url,
            api_key=key,
            temperature=0,
            timeout=120,
        )
    raise ValueError(f"未知 provider: {provider}")


# ---------------------------------------------------------------------------
# Task selection
# ---------------------------------------------------------------------------

def select_tasks(
    *,
    difficulty: Optional[str] = None,
    include_broken: bool = False,
    include_http: bool = False,
    task_ids: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    out = []
    wanted = set(task_ids) if task_ids is not None else None
    for t in TASKS:
        if wanted is not None and t["id"] not in wanted:
            continue
        if difficulty and t["difficulty"] != difficulty:
            continue
        if not include_broken and t.get("known_broken"):
            continue
        if not include_http and t.get("requires_http"):
            continue
        out.append(t)
    return out


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def score_source(task: Dict[str, Any], source: str) -> Dict[str, Any]:
    """Run ``source`` against every test case; score with the task validator."""
    validator = VALIDATORS[task["validator"]]
    cases = []
    passed = 0
    first_error: Optional[str] = None
    for tc in task["test_cases"]:
        run_res = run_flow(source, tc["input"])
        ok = False
        actual = None
        if run_res.ok:
            actual = run_res.result
            ok = bool(validator(actual, tc["expected"]))
            if ok:
                passed += 1
        else:
            if first_error is None:
                first_error = run_res.error
        cases.append({
            "input": tc["input"],
            "expected": tc["expected"],
            "actual": actual,
            "pass": ok,
            "run_error": run_res.error if not run_res.ok else None,
        })
    return {
        "task_id": task["id"],
        "passed": passed,
        "total": len(task["test_cases"]),
        "pass_rate": round(passed / len(task["test_cases"]), 3) if task["test_cases"] else 0.0,
        "cases": cases,
        "first_error": first_error,
    }


# ---------------------------------------------------------------------------
# Agent runners
# ---------------------------------------------------------------------------

def input_fields_hint(task: Dict[str, Any]) -> str:
    """Describe the flow's INPUT fields for the planner prompt.

    Use the **union** of all test cases' input keys (not just the first), so
    heterogeneous cases don't mislead the planner about the input schema.
    """
    keys = sorted(set().union(*(tc["input"].keys() for tc in task["test_cases"] if tc["input"])))
    if not keys:
        return "（无输入字段）"
    fields = ", ".join(f"{k}: <来自测试用例>" for k in keys)
    return f"输入字段：{fields}（从 INPUT.<name> 读取）"


def sample_input(task: Dict[str, Any]) -> Dict[str, Any]:
    return task["test_cases"][0]["input"]


def run_fot_task(
    task: Dict[str, Any],
    model: BaseChatModel,
    *,
    tools: Optional[List[ToolLike]] = None,
) -> Dict[str, Any]:
    """FoT: plan @flow once (no test input), score against all cases."""
    agent = FoTAgent(
        model=model,
        tools=tools or [],
        instruction=FOT_INSTRUCTION,
        max_compile_retries=3,
    )
    task_desc = f"{task['requirement']}\n{input_fields_hint(task)}"
    result = agent.invoke({"task": task_desc})
    source = result.source or ""
    score = score_source(task, source)
    score["agent"] = "fot"
    score["agent_ok"] = result.ok
    score["attempts"] = result.attempts
    score["compile_errors"] = [e.to_dict() for e in result.compile_errors]
    score["source"] = source
    score["prompt_version"] = PROMPT_VERSION
    return score


_FLOW_FENCE_RE = None


def _flow_from_text(text: str) -> str:
    """Best-effort: extract an ``@flow`` fenced block from free-form text.

    Used as a fallback when ReAct never called ``plaita_compile_flow`` /
    ``plaita_run_flow`` but pasted the source inline.
    """
    global _FLOW_FENCE_RE
    if _FLOW_FENCE_RE is None:
        import re
        _FLOW_FENCE_RE = re.compile(r"```(?:@flow|flow)?\s*(.*?@flow.*?)```", re.DOTALL | re.IGNORECASE)
    m = _FLOW_FENCE_RE.search(text or "")
    return m.group(1).strip() if m else ""


def _extract_flow_source_from_react(messages) -> str:
    """Pull @flow source from the ReAct message trace.

    Prefer the last ``plaita_run_flow`` call (the agent's final, run-verified
    source), then fall back to the last ``plaita_compile_flow`` call. If no
    tool call carried source, try to parse a fenced ``@flow`` block from the
    final AIMessage text. Warn (don't silently return "") when extraction
    fails so a benchmark run surfaces "agent didn't produce source" vs "flow
    logic wrong" distinctly.
    """
    run_src = ""
    compile_src = ""
    last_ai_text = ""
    for msg in messages:
        if not isinstance(msg, AIMessage):
            continue
        if isinstance(msg.content, str) and msg.content:
            last_ai_text = msg.content
        for tc in msg.tool_calls or []:
            name = tc.get("name")
            if name not in ("plaita_compile_flow", "plaita_run_flow"):
                continue
            src = tc.get("args", {}).get("source")
            if isinstance(src, str) and "@flow" in src:
                if name == "plaita_run_flow":
                    run_src = src
                else:
                    compile_src = src
    found = run_src or compile_src or _flow_from_text(last_ai_text)
    if not found:
        logger.warning("react: 未能从 messages 提取 @flow 源码（无 tool_call 也无 fenced block）")
    return found


def run_react_task(
    task: Dict[str, Any],
    model: BaseChatModel,
    *,
    tools: Optional[List[ToolLike]] = None,
) -> Dict[str, Any]:
    """ReAct: ask agent to produce+compile+run a flow with a sample input."""
    agent = PlaitaAgent(
        model=model,
        tools=tools or [],
        instruction=REACT_INSTRUCTION,
    )
    sample = sample_input(task)
    message = (
        f"需求：{task['requirement']}\n{input_fields_hint(task)}\n\n"
        f"先用样例输入 {sample} 跑通，然后把最终 @flow 源码贴给我。"
    )
    out = agent.invoke(message)
    source = _extract_flow_source_from_react(out.messages)
    score = score_source(task, source)
    score["agent"] = "react"
    score["agent_text"] = out.text
    score["source"] = source
    score["source_extracted"] = bool(source)
    score["prompt_version"] = PROMPT_VERSION
    return score
