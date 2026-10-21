"""Optional LLM judge for eval cases with a subjective expectation.

Off by default and fully pluggable: when a case's ``expect`` carries a
``"judge"`` rubric, ``judge_output`` sends the rubric + input + output to an
OpenAI-compatible chat endpoint configured via environment:

    PLAITA_AI_JUDGE_BASE_URL   e.g. https://api.openai.com/v1 (or any compatible)
    PLAITA_AI_JUDGE_MODEL      e.g. gpt-4o-mini / glm-4-flash / deepseek-chat
    PLAITA_AI_JUDGE_API_KEY

Unconfigured → ``judge_output`` returns ``None`` and the case is excluded
from pass-rate averages instead of silently guessing. A configured-but-broken
judge (transport / HTTP / bad reply envelope) returns a verdict stamped
``unavailable=True`` so callers can drop the case instead of scoring it 0.
"""

from __future__ import annotations

import json
import logging
import math
import os
from typing import Any, Dict, Optional

import httpx

_TIMEOUT_S = 60.0

_log = logging.getLogger(__name__)


def _unavailable_reply(reason: str) -> Dict[str, Any]:
    """Broken judge reply → ``unavailable`` stamp, never a 0/1-score verdict.

    ``assert_score``/``evaluate`` drop unavailable cases out of the averages,
    so a degraded reply must carry this stamp to stay out of the denominator.
    """
    return {"ok": False, "score": 0.0, "unavailable": True, "reason": reason}


def _normalize_verdict(verdict: Any) -> Dict[str, Any]:
    """Named-type gate on the judge's verdict fields (A2R32-04).

    Only ``{"ok": <real bool>, "score": <finite number>, "reason": str}`` is a
    judgment about the output: NaN/±Infinity are rejected explicitly (they
    used to clamp to a full 1.0 pass), numeric-looking strings and booleans
    are not scores, ``"false"`` is not an ``ok``, non-object JSON and missing
    fields are broken replies. Anything else degrades to ``unavailable``.
    """
    if not isinstance(verdict, dict):
        return _unavailable_reply("judge reply was not an object")
    raw_score = verdict.get("score")
    if isinstance(raw_score, bool) or not isinstance(raw_score, (int, float)):
        return _unavailable_reply("judge verdict score was not a finite number")
    try:
        finite = math.isfinite(raw_score)
    except OverflowError:  # int too large for float: magnitude, not a verdict
        finite = False
    if not finite:
        return _unavailable_reply("judge verdict score was not a finite number")
    ok = verdict.get("ok")
    if not isinstance(ok, bool):
        return _unavailable_reply("judge verdict ok was not a boolean")
    return {
        "ok": ok,
        "score": round(float(max(0.0, min(1.0, raw_score))), 4),
        "reason": str(verdict.get("reason", ""))[:300],
    }


def judge_configured(env: Optional[Dict[str, str]] = None) -> bool:
    """True only when URL, model and API key are all set.

    Half-configured (no key) means every request would be rejected — treat it
    as unconfigured so the case is skipped instead of scored 0.
    """
    env = dict(os.environ if env is None else env)
    return all(
        (env.get(name) or "").strip()
        for name in ("PLAITA_AI_JUDGE_BASE_URL", "PLAITA_AI_JUDGE_MODEL", "PLAITA_AI_JUDGE_API_KEY")
    )


def judge_output(
    rubric: str,
    input_data: Dict[str, Any],
    output: Any,
    env: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    """Judge one output against a rubric; None when the judge is unconfigured.

    Returns {"ok": bool, "score": 0..1, "reason": str} on a real verdict, or
    {"ok": False, "score": 0.0, "unavailable": True, ...} when the judge could
    not be reached or its reply was not a well-formed verdict object — the two
    must not be confused, since only the former is a judgment about the output
    (A2R32-04: broken shapes degrade to unavailable instead of scoring 0/1).
    """
    env = dict(os.environ if env is None else env)
    if not judge_configured(env):
        return None
    base_url = (env.get("PLAITA_AI_JUDGE_BASE_URL") or "").rstrip("/")
    model = env.get("PLAITA_AI_JUDGE_MODEL")

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
    except Exception as e:  # judge unavailability must never fail the eval run
        _log.warning("judge endpoint unreachable; case degrades to unavailable: %s", e)
        return _unavailable_reply("judge endpoint unavailable")
    if not isinstance(content, str):
        # 200 with a non-text envelope ("content": null, or a parts list):
        # a broken judge reply, not a verdict about the output.
        _log.warning("judge reply had no text content; case degrades to unavailable")
        return _unavailable_reply("judge reply had no text content")

    try:
        verdict = json.loads(content)
    except ValueError:
        # Tolerate fenced or trailing-prose replies: grab the first {...}.
        start, end = content.find("{"), content.rfind("}")
        if start < 0 or end <= start:
            _log.warning("judge reply was not JSON; case degrades to unavailable: %.200s", content)
            return _unavailable_reply("judge reply was not JSON")
        try:
            verdict = json.loads(content[start : end + 1])
        except ValueError:
            _log.warning("judge reply was not JSON; case degrades to unavailable: %.200s", content)
            return _unavailable_reply("judge reply was not JSON")

    try:
        return _normalize_verdict(verdict)
    except Exception as e:  # a weird reply shape must never fail the eval run
        _log.warning("judge verdict normalization failed; case degrades to unavailable: %s", e)
        return _unavailable_reply("judge reply could not be normalized")
