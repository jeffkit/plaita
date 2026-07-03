"""Unit tests for the benchmark runner's pure logic (no LLM calls)."""

from __future__ import annotations

import sys
from pathlib import Path

# tests/llm is importable when running from the plaita-ai checkout (pythonpath
# includes "."), but make the import robust for direct invocation too.
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.llm.harness import select_tasks  # noqa: E402
from tests.llm.runner import _envelope, _summary  # noqa: E402


def test_select_tasks_by_id():
    out = select_tasks(task_ids=["cond-grade", "does-not-exist"])
    assert [t["id"] for t in out] == ["cond-grade"]


def test_envelope_carries_repro_metadata():
    cfg = {"provider": "openai", "model": "deepseek-v4-flash", "base_url": "https://api.deepseek.com"}
    env = _envelope(
        "fot",
        [{"task_id": "t1", "pass_rate": 1.0, "passed": 2, "total": 2}],
        seed=0,
        cfg=cfg,
        timestamp="20260703-000000",
    )
    assert env["seed"] == 0
    assert env["model"] == "deepseek-v4-flash"
    assert env["prompt_version"]
    assert env["task_count"] == 1
    assert env["results"][0]["pass_rate"] == 1.0


def test_summary_aggregates_mean_pass_rate():
    cfg = {"provider": "openai", "model": "m", "base_url": "u"}
    envs = [
        _envelope("fot", [
            {"task_id": "a", "pass_rate": 1.0, "passed": 2, "total": 2},
            {"task_id": "b", "pass_rate": 0.5, "passed": 1, "total": 2},
        ], seed=0, cfg=cfg, timestamp="t"),
        _envelope("react", [
            {"task_id": "a", "pass_rate": 0.0, "passed": 0, "total": 2},
        ], seed=0, cfg=cfg, timestamp="t"),
    ]
    s = _summary(envs)
    assert s["per_agent"]["fot"]["mean_pass_rate"] == 0.75
    assert s["per_agent"]["fot"]["tasks"] == 2
    assert s["per_agent"]["react"]["mean_pass_rate"] == 0.0
