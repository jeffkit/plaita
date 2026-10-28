"""Negative/repair eval for the flow-coder skill (plaita#351 P1-1/P1-2).

The skill's ``evals.json`` historically carried only happy-path cases — there
was no way to answer "does the compiler actually reject the documented
anti-patterns, and does the documented repair actually work". This runner
executes the ``negative_evals`` block deterministically (no LLM involved):

stage 1 (first pass)  — the case's ``broken_source`` must FAIL to compile with
                        the expected line / message substring / stable ``code``;
stage 2 (repair)      — the case's ``repaired_source`` must compile (and, for
                        isolation cases, keep the good branch's nodes and
                        edges byte-identical — the "minimal patch" contract);
stage 3 (execute)     — each ``runs`` entry must produce the expected output,
                        or an ``error_result`` termination when
                        ``expect_error_result`` is set.

Report metrics (issue acceptance):
- ``first_pass_compile_rate`` — broken sources that compiled. Baseline 0.0:
  these anti-patterns MUST be rejected; >0 means a compiler regression or a
  stale case.
- ``avg_repair_rounds`` — mean repairs to reach a passing source (gold repairs
  round-trip in 1; a failing case is excluded from the mean and marked not ok).
- ``final_pass_rate`` — cases whose repair compiles, honors the IR pin and
  runs to expectation. Target 1.0.

CLI: ``python -m plaita_ai.evals.skill_repair`` prints the report as JSON.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from plaita_ai.flow_runner import compile_flow, run_flow

__all__ = ["load_negative_cases", "run_skill_repair_evals"]

_EVALS_JSON = Path(__file__).resolve().parents[1] / "skills" / "flow-coder" / "evals" / "evals.json"


def load_negative_cases(path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Load the skill's ``negative_evals`` block (empty when absent)."""
    source = Path(path) if path else _EVALS_JSON
    raw = json.loads(source.read_text(encoding="utf-8"))
    cases = raw.get("negative_evals")
    return [c for c in cases if isinstance(c, dict)] if isinstance(cases, list) else []


def _check_broken_compile(case: Dict[str, Any]) -> Dict[str, Any]:
    """Stage 1: broken_source must fail with the expected line/message/code."""
    expected = case.get("expected_error") or {}
    compiled = compile_flow(case["broken_source"])
    row: Dict[str, Any] = {"first_pass_compiled": compiled.ok}
    if compiled.ok:
        return row
    error = compiled.errors[0]
    row["error"] = error.to_dict()
    want_line = expected.get("line")
    row["error_line_ok"] = want_line is None or error.line == want_line
    want_msg = expected.get("message_contains")
    row["error_message_ok"] = not want_msg or want_msg in (error.message or "")
    want_code = expected.get("code")
    # code 断言同时钉住 P2-2 的稳定错误码面：编译报错文本变了但 code 没跟上，
    # 这里也会红（文本→code 映射随编译器同步维护）。
    row["error_code_ok"] = not want_code or error.code == want_code
    return row


def _check_ir_pin(case: Dict[str, Any], ir: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """P1-2 回归：修复后 IR 的节点/边只落在预期集合，正确分支逐节点不变。"""
    row: Dict[str, Any] = {"ir_pin_ok": True}
    want_ids = case.get("expected_node_ids")
    want_good = case.get("expected_good_nodes")
    if want_ids is None and want_good is None:
        return row
    nodes = {n.get("id"): n for n in (ir or {}).get("nodes", [])}
    if want_ids is not None and sorted(nodes) != sorted(want_ids):
        row["ir_pin_ok"] = False
        row["ir_pin_reason"] = f"node ids {sorted(nodes)} != expected {sorted(want_ids)}"
        return row
    for node_id, want in (want_good or {}).items():
        got = nodes.get(node_id) or {}
        for key in ("type", "next"):
            if got.get(key) != want.get(key):
                row["ir_pin_ok"] = False
                row["ir_pin_reason"] = (
                    f"good node {node_id!r}.{key} = {got.get(key)!r}, expected {want.get(key)!r}"
                )
                return row
    return row


def _run_case(case: Dict[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = {"id": case.get("id"), "name": case.get("name")}

    first = _check_broken_compile(case)
    row.update(first)
    row["first_pass_ok"] = not first.get("first_pass_compiled", True) and all(
        first.get(k, True) for k in ("error_line_ok", "error_message_ok", "error_code_ok")
    )

    repaired = compile_flow(case["repaired_source"])
    row["repair_compiles"] = repaired.ok
    if not repaired.ok:
        row["repair_error"] = repaired.errors[0].to_dict()

    ir_pin = _check_ir_pin(case, repaired.ir if repaired.ok else None)
    row.update(ir_pin)

    runs = case.get("runs") or []
    run_rows: List[Dict[str, Any]] = []
    for spec in runs:
        result = run_flow(case["repaired_source"], spec.get("inputs") or {})
        if spec.get("expect_error_result"):
            ok = (not result.ok) and result.error_type == "error_result"
            detail = {"error_type": result.error_type}
        else:
            ok = result.ok and result.result == spec.get("expect")
            detail = {"result": result.result, "error": result.error}
        run_rows.append({"ok": ok, **detail})
    row["runs_ok"] = all(r["ok"] for r in run_rows) if run_rows else True
    row["run_details"] = run_rows

    row["repair_rounds"] = 1 if (row["first_pass_ok"] and repaired.ok and ir_pin["ir_pin_ok"] and row["runs_ok"]) else None
    row["ok"] = row["first_pass_ok"] and repaired.ok and ir_pin["ir_pin_ok"] and row["runs_ok"]
    return row


def run_skill_repair_evals(path: Optional[Path] = None) -> Dict[str, Any]:
    """Run every negative case and produce the metrics report."""
    started = time.monotonic()
    cases = load_negative_cases(path)
    rows = [_run_case(c) for c in cases]
    total = len(rows)
    first_compiled = sum(1 for r in rows if r.get("first_pass_compiled"))
    rounds = [r["repair_rounds"] for r in rows if r.get("repair_rounds") is not None]
    passed = sum(1 for r in rows if r.get("ok"))
    return {
        "suite": "flow-coder negative/repair evals (plaita#351)",
        "cases_total": total,
        "cases": rows,
        # 基线：first_pass_compile_rate=0.0（负向模式必须被编译期拒绝）；
        # avg_repair_rounds=1.0（金标修复一轮闭环）；final_pass_rate=1.0。
        "first_pass_compile_rate": round(first_compiled / total, 4) if total else None,
        "avg_repair_rounds": round(sum(rounds) / len(rounds), 4) if rounds else None,
        "final_pass_rate": round(passed / total, 4) if total else None,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "wall_s": round(time.monotonic() - started, 3),
    }


if __name__ == "__main__":  # pragma: no cover
    print(json.dumps(run_skill_repair_evals(), ensure_ascii=False, indent=2))
