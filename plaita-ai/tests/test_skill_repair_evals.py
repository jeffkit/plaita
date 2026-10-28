"""Negative/repair eval suite for the flow-coder skill (plaita#351 P1-1/P1-2).

The suite is the acceptance gate itself: every negative case must fail first
pass with the documented error (line + substring + stable code), repair in one
round, and run to expectation — including the P1-2 isolation case whose IR pin
asserts a minimal-patch repair leaves the good branch byte-identical.
"""
from __future__ import annotations

import json
import os
import sys

_HERE = os.path.dirname(__file__)
_PLAITA_AI = os.path.dirname(_HERE)
if _PLAITA_AI not in sys.path:
    sys.path.insert(0, _PLAITA_AI)

from plaita_ai.evals.skill_repair import load_negative_cases, run_skill_repair_evals


def test_negative_cases_present_and_happy_path_intact():
    evals_path = os.path.join(_PLAITA_AI, "plaita_ai", "skills", "flow-coder", "evals", "evals.json")
    with open(evals_path, encoding="utf-8") as fh:
        raw = json.load(fh)
    # 既有 happy-path 用例零回归（#351 基线 21 条）
    assert len(raw["evals"]) == 21
    # 负向用例：#351 点名的 10 个反模式 + 1 条最小 patch 回归
    cases = load_negative_cases()
    assert len(cases) == 11
    ids = {c["id"] for c in cases}
    assert "neg-11-repair-isolation" in ids
    assert all({"broken_source", "repaired_source", "expected_error"} <= set(c) for c in cases)


def test_all_negative_cases_green_and_metrics_at_baseline():
    report = run_skill_repair_evals()
    assert report["cases_total"] == 11
    failures = [r for r in report["cases"] if not r["ok"]]
    assert not failures, f"failing cases: {json.dumps(failures, ensure_ascii=False)[:2000]}"
    # #351 验收基线：反模式首编必拒（0.0）、金标修复一轮闭环（1.0）、终态全绿（1.0）
    assert report["first_pass_compile_rate"] == 0.0
    assert report["avg_repair_rounds"] == 1.0
    assert report["final_pass_rate"] == 1.0


def test_isolation_case_pins_minimal_patch_contract():
    """P1-2 回归：修复只动坏节点区域，正确分支的节点与边不变。"""
    report = run_skill_repair_evals()
    row = next(r for r in report["cases"] if r["id"] == "neg-11-repair-isolation")
    assert row["first_pass_ok"]
    assert row["ir_pin_ok"], row.get("ir_pin_reason")
    assert row["runs_ok"]
