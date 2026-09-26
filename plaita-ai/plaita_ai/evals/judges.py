"""Optional LLM judge for eval cases with a subjective expectation.

Off by default and fully pluggable: when a case's ``expect`` carries a
``"judge"`` rubric, ``judge_output`` sends the rubric + input + output to an
OpenAI-compatible chat endpoint configured via environment:

    PLAITA_AI_JUDGE_BASE_URL   e.g. https://api.openai.com/v1 (or any compatible)
    PLAITA_AI_JUDGE_MODEL      e.g. gpt-4o-mini / glm-4-flash / deepseek-chat
    PLAITA_AI_JUDGE_API_KEY

Unconfigured → ``judge_output`` returns ``None`` and the case is excluded
from pass-rate averages instead of silently guessing.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import httpx

_TIMEOUT_S = 60.0


def judge_configured(env: Optional[Dict[str, str]] = None) -> bool:
    env = dict(os.environ if env is None else env)
    return bool(env.get("PLAITA_AI_JUDGE_BASE_URL") and env.get("PLAITA_AI_JUDGE_MODEL"))


def judge_output(
    rubric: str,
    input_data: Dict[str, Any],
    output: Any,
    env: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    """Judge one output against a rubric; None when the judge is unconfigured.

    Returns {"ok": bool, "score": 0..1, "reason": str}.
    """
    env = dict(os.environ if env is None else env)
    base_url = (env.get("PLAITA_AI_JUDGE_BASE_URL") or "").rstrip("/")
    model = env.get("PLAITA_AI_JUDGE_MODEL")
    if not (base_url and model):
        return None

    system = (
        "You are an evaluation judge for a workflow output. "
        "Answer with a single JSON object only: "
        '{"ok": true|false, "score": 0.0-1.0, "reason": "one sentence"}.'
    )
    user = (
        f"Rubric:\n{rubric}\n\n"
        f"Input:\n{json.dumps(input_data, ensure_ascii=False, default=str)}\n\n"
        f"Output:\n{json.dumps(output, ensure_ascii=False, default=str)}"
    )
    try:
        resp = httpx.post(
            f"{base_url}/chat/completions",
            headers={"Authorization": f"Bearer {env.get('PLAITA_AI_JUDGE_API_KEY', '')}"},
            json={
                "model": model,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                "temperature": 0,
            },
            timeout=_TIMEOUT_S,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
    except Exception:  # judge unavailability must never fail the eval run
        return {"ok": False, "score": 0.0, "reason": "judge endpoint unavailable"}

    try:
        verdict = json.loads(content)
    except ValueError:
        # Tolerate fenced or trailing-prose replies: grab the first {...}.
        start, end = content.find("{"), content.rfind("}")
        if start < 0 or end <= start:
            return {"ok": False, "score": 0.0, "reason": "judge reply was not JSON"}
        try:
            verdict = json.loads(content[start : end + 1])
        except ValueError:
            return {"ok": False, "score": 0.0, "reason": "judge reply was not JSON"}

    try:
        score = max(0.0, min(1.0, float(verdict.get("score", 0.0))))
    except (TypeError, ValueError):
        score = 0.0
    return {
        "ok": bool(verdict.get("ok")) or score >= 0.999,
        "score": round(score, 4),
        "reason": str(verdict.get("reason", ""))[:300],
    }
