"""LLM integration test harness — reuse agent-benchmark task set.

Runs FoT / ReAct agents against the 24 benchmark tasks with deepseek-v4-flash
(OpenAI-compatible endpoint) and scores them with the shared validators.

Design: we capture the **@flow source** each agent produces, then run it
deterministically against every test case via ``flow_runner.run_flow``. This
isolates "did the agent write a correct flow" from "did it narrate results".
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

# Reuse the canonical task set + validators from agent-benchmark.
_BENCH = Path(__file__).resolve().parents[3] / "agent-benchmark"
sys.path.insert(0, str(_BENCH))
from tasks import TASKS, VALIDATORS  # noqa: E402

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage

from plaita_ai.agent.fot import FoTAgent
from plaita_ai.agent.fot.tools import ToolLike
from plaita_ai.agent.react import PlaitaAgent
from plaita_ai.flow_runner import RunResult, run_flow


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

def _resolve_deepseek_key() -> str:
    key = os.environ.get("DEEPSEEK_API_KEY")
    if key:
        return key.strip()
    zshrc = Path.home() / ".zshrc"
    if zshrc.exists():
        m = re.search(
            r'export\s+DEEPSEEK_API_KEY\s*=\s*["\']([^"\']+)["\']',
            zshrc.read_text(encoding="utf-8"),
        )
        if m:
            return m.group(1)
    raise RuntimeError("DEEPSEEK_API_KEY 未提供")


def build_model() -> BaseChatModel:
    """Build a chat model from env (DeepSeek OpenAI-compatible by default)."""
    from langchain_openai import ChatOpenAI

    provider = os.environ.get("PLAITA_LLM_PROVIDER", "openai").lower()
    model_name = os.environ.get("PLAITA_LLM_MODEL", "deepseek-v4-flash")
    base_url = os.environ.get("PLAITA_LLM_BASE_URL", "https://api.deepseek.com")
    key = _resolve_deepseek_key()

    if provider == "openai":
        return ChatOpenAI(
            model=model_name,
            base_url=base_url,
            api_key=key,
            temperature=0,
            timeout=120,
        )
    if provider == "anthropic":
        # Anthropic-compatible endpoint (e.g. DeepSeek's /anthropic).
        from langchain_anthropic import ChatAnthropic

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
) -> List[Dict[str, Any]]:
    out = []
    for t in TASKS:
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
    """Describe the flow's INPUT fields for the planner prompt."""
    first = task["test_cases"][0]["input"]
    if not first:
        return "（无输入字段）"
    fields = ", ".join(f"{k}: <来自测试用例>" for k in first.keys())
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
        instruction=(
            "流程要对任意合法输入都成立，不要硬编码具体测试值。"
        ),
        max_compile_retries=3,
    )
    task_desc = f"{task['requirement']}\n{input_fields_hint(task)}"
    result = agent.invoke({"task": task_desc})
    source = result.source or ""
    score = score_source(task, source)
    score["agent_ok"] = result.ok
    score["attempts"] = result.attempts
    score["compile_errors"] = [e.to_dict() for e in result.compile_errors]
    score["source"] = source
    return score


def _extract_flow_source_from_react(messages) -> str:
    """Pull @flow source from the last plaita_compile_flow / plaita_run_flow call."""
    found = ""
    for msg in messages:
        if not isinstance(msg, AIMessage):
            continue
        for tc in msg.tool_calls or []:
            if tc.get("name") in ("plaita_compile_flow", "plaita_run_flow"):
                src = tc.get("args", {}).get("source")
                if isinstance(src, str) and "@flow" in src:
                    found = src
    return found


def run_react_task(
    task: Dict[str, Any],
    model: BaseChatModel,
    *,
    tools: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    """ReAct: ask agent to produce+compile+run a flow with a sample input."""
    agent = PlaitaAgent(
        model=model,
        tools=tools or [],
        instruction=(
            "本任务必须用 @flow 实现（不要纯文本回答）：先写源码、校验、跑样例。"
            "流程要对任意合法输入成立，不要硬编码具体测试值。"
        ),
    )
    sample = sample_input(task)
    message = (
        f"需求：{task['requirement']}\n{input_fields_hint(task)}\n\n"
        f"先用样例输入 {sample} 跑通，然后把最终 @flow 源码贴给我。"
    )
    out = agent.invoke(message)
    source = _extract_flow_source_from_react(out.messages)
    score = score_source(task, source)
    score["agent_text"] = out.text
    score["source"] = source
    score["source_extracted"] = bool(source)
    return score
