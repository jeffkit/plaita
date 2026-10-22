"""Runtime evaluation for deployed flow versions — the supervisor's referee.

A Dataset is a list of cases (input + expectations, stored as plain JSON so
it can live in git). ``evaluate`` runs every case against one flow version —
in-process via the console's dry-run endpoint by default (no deployment
needed), or as real executions — and scores the outputs. ``compare`` turns
two reports into a promote/rollback verdict.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from plaita_ai.console_client import ConsoleClient, ConsoleClientError
from plaita_ai.evals.judges import judge_output

__all__ = ["Case", "Dataset", "load_dataset", "evaluate", "compare"]

_log = logging.getLogger(__name__)


class Case:
    """One evaluation case: {"id", "input", "expect"} — plain JSON-able."""

    def __init__(self, id: str, input: Dict[str, Any], expect: Optional[Dict[str, Any]] = None):
        self.id = id
        self.input = input or {}
        self.expect = expect or {}

    @classmethod
    def from_dict(cls, raw: Dict[str, Any], index: int) -> "Case":
        case_id = str(raw.get("id") or f"case-{index + 1}")
        input_value = raw.get("input")
        input_value = input_value if isinstance(input_value, dict) else {}
        expect = raw.get("expect")
        expect = expect if isinstance(expect, dict) else {}
        return cls(case_id, input_value, expect)

    def to_dict(self) -> Dict[str, Any]:
        return {"id": self.id, "input": self.input, "expect": self.expect}


class Dataset:
    """An ordered list of cases; loadable from one JSON file or a directory."""

    def __init__(self, cases: List[Any], source: str = ""):
        # Accept dicts for convenience (tests, JSON round-trips); coerce to Case.
        self.cases: List[Case] = [
            c if isinstance(c, Case) else Case.from_dict(c, i) for i, c in enumerate(cases)
        ]
        self.source = source

    @classmethod
    def load(cls, path: str) -> "Dataset":
        p = Path(path)
        if p.is_dir():
            cases: List[Case] = []
            for file in sorted(p.glob("*.json")):
                if file.name.startswith("_"):
                    continue  # convention: _*.json holds metadata, not cases
                cases.extend(cls._load_file(file))
            if not cases:
                raise ConsoleClientError(f"no case files (*.json) found in {path}")
            return cls(cases, source=str(p))
        if p.is_file():
            return cls(cls._load_file(p), source=str(p))
        raise ConsoleClientError(f"dataset path not found: {path}")

    @staticmethod
    def _load_file(file: Path) -> List[Case]:
        try:
            raw = json.loads(file.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ConsoleClientError(f"cannot read dataset file {file}: {exc}") from exc
        items = raw.get("cases") if isinstance(raw, dict) else raw
        if not isinstance(items, list):
            raise ConsoleClientError(f"dataset file {file} must be a list or {{cases: [...]}}")
        return [Case.from_dict(item, i) for i, item in enumerate(items) if isinstance(item, dict)]

    def to_dict(self) -> Dict[str, Any]:
        return {"source": self.source, "cases": [c.to_dict() for c in self.cases]}

    def __len__(self) -> int:
        return len(self.cases)


def load_dataset(path: str) -> Dataset:
    return Dataset.load(path)


# -- scoring -----------------------------------------------------------------


def _get_path(payload: Any, dotted: str) -> Any:
    current = payload
    for part in str(dotted).split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def _judge_unavailable(result: Dict[str, Any]) -> bool:
    judge = result.get("judge")
    return isinstance(judge, dict) and bool(judge.get("unavailable"))


def _case_blind(result: Dict[str, Any]) -> bool:
    """This case carries no usable judgment: the judge was unreachable, or the
    case itself degraded to unavailable (A2R46-01 — a candidate-controlled
    payload blew up the input-side serialization chain). Either way its
    ok/score are not evidence and stay out of averages and compare decisions.
    """
    return bool(result.get("unavailable")) or _judge_unavailable(result)


def _json_safe_output(output: Any) -> Any:
    """报告要经得起下游 json.dumps（MCP 回复 / proposer prompt / 会话留痕）：
    候选自控的深嵌套输出不可序列化时以占位符入报告，本体丢弃（A2R46-01）。
    """
    try:
        json.dumps(output, ensure_ascii=False, default=str)
    except Exception as exc:
        return f"<unserializable output omitted: {type(exc).__name__}>"
    return output


def assert_score(case: Case, output: Any) -> Dict[str, Any]:
    """Score one output against a case's expect block.

    Supported expectations (combinable):
      {"contains": "substring"}            — substring of the stringified output
      {"equals_path": {"path": "a.b", "value": x}}
      {"not_empty": true}                  — output (or path) is present/truthy
      {"judge": "rubric"}                  — delegated to the LLM judge (optional)
    """
    reasons: List[str] = []
    score = 1.0
    expect = case.expect
    try:
        text = json.dumps(output, ensure_ascii=False, default=str) if output is not None else ""
    except Exception as exc:
        # A2R46-01 输入侧：output 是候选 flow 自控的——深嵌套结构（~10k 层约
        # 20KB）让 json.dumps 抛 RecursionError（RuntimeError 族；r73 只守了
        # judge 回复侧）。这不是对输出的判定，不能计 0 分：降级 unavailable
        # 剔出均值（与 judge 不可达同口径），兄弟 case 与整报告保留。
        return {
            "ok": False,
            "score": 0.0,
            "unavailable": True,
            "reasons": [f"output not serializable: {type(exc).__name__}: {exc}"[:500]],
        }

    if "contains" in expect:
        needle = str(expect["contains"])
        if needle not in text:
            reasons.append(f"output does not contain {needle!r}")
            score = 0.0

    if "equals_path" in expect:
        spec = expect["equals_path"] or {}
        actual = _get_path(output, str(spec.get("path", "")))
        if actual != spec.get("value"):
            reasons.append(f"path {spec.get('path')!r} = {actual!r}, expected {spec.get('value')!r}")
            score = 0.0

    if expect.get("not_empty"):
        if output is None or output == "" or output == {} or output == []:
            reasons.append("output is empty")
            score = 0.0

    result: Dict[str, Any] = {"ok": score == 1.0, "score": score, "reasons": reasons}

    if "judge" in expect:
        verdict = judge_output(str(expect["judge"]), case.input, output)
        if verdict is None:
            result["judge"] = "skipped"  # judge not configured — excluded from averages
        elif verdict.get("unavailable"):
            # judge unreachable: not a verdict about the output, so keep the
            # content score out of the average rather than folding in a 0.0.
            result["judge"] = verdict
            result["reasons"].append(str(verdict.get("reason", "judge endpoint unavailable")))
        else:
            result["judge"] = verdict
            result["score"] = round((score + float(verdict["score"])) / 2.0, 4)
            result["ok"] = result["score"] >= 0.999
            if not verdict.get("ok"):
                result["reasons"].append(str(verdict.get("reason", "judge rejected")))
    return result


# -- evaluation ----------------------------------------------------------------


def evaluate(
    client: ConsoleClient,
    flow_id: str,
    version: str,
    dataset: Dataset,
    mode: str = "dry-run",
    timeout_s: float = 120.0,
) -> Dict[str, Any]:
    """Run every case against one flow version and score the outputs.

    mode="dry-run" evaluates the saved definition in-process (fast, no
    deployment); mode="execution" starts a real execution per case and waits.
    """
    mode = "execution" if str(mode).lower() in ("execution", "run", "real") else "dry-run"
    definition: Optional[str] = None
    if mode == "dry-run":
        definition = client.get_version(flow_id, version).get("definition") or ""

    results: List[Dict[str, Any]] = []
    started = time.monotonic()
    for case in dataset.cases:
        entry: Dict[str, Any] = {"id": case.id, "ok": False, "score": 0.0}
        try:
            if mode == "dry-run":
                out = client.dry_run(definition or "", input=case.input)
                if out.get("error"):
                    entry["error"] = str(out["error"])[:500]
                    entry["reasons"] = ["dry-run error"]
                    results.append(entry)
                    continue
                output = out.get("result")
            else:
                started_at = client.start_execution(flow_id, version=version, params=case.input)
                execution_id = str(started_at.get("execution_id") or "")
                if not execution_id:
                    raise ConsoleClientError("start_execution returned no execution_id")
                done = client.wait_execution(execution_id, timeout_s=timeout_s)
                if str(done.get("status")) not in ("completed", "success", "succeeded"):
                    entry["error"] = f"execution {done.get('status')}"
                    try:
                        entry["reasons"] = [json.dumps(done.get("error"), default=str)[:500]]
                    except Exception as exc:
                        # A2R46-01 输入侧：错误载荷可嵌候选自控的深结构，
                        # json.dumps 的 RecursionError 不是判定——单 case 降级
                        # unavailable，兄弟 case 与整报告保留（降级可观测）。
                        entry["unavailable"] = True
                        entry["error"] = f"{type(exc).__name__}: {exc}"[:500]
                        entry["reasons"] = [
                            "case unavailable: execution error payload not serializable"
                        ]
                        _log.warning(
                            "eval case %s degraded to unavailable: "
                            "execution error payload not serializable: %s",
                            case.id,
                            exc,
                        )
                    results.append(entry)
                    continue
                output = done.get("output")
                # Langfuse 对账:观测开启时执行详情带 trace 深链,逐 case 挂上,
                # 让每条评测结论都能回放到完整轨迹。
                if not done.get("langfuse_trace_url"):
                    try:
                        done = client.get_execution(execution_id)
                    except ConsoleClientError:
                        pass
                if done.get("langfuse_trace_url"):
                    entry["langfuse_trace_url"] = done["langfuse_trace_url"]
            entry["output"] = _json_safe_output(output)
            entry.update(assert_score(case, output))
        except ConsoleClientError as exc:
            entry["error"] = str(exc)[:500]
            entry["reasons"] = ["console error"]
        results.append(entry)

    judge_unavailable = [r["id"] for r in results if _judge_unavailable(r)]
    unavailable = [r["id"] for r in results if r.get("unavailable")]
    scored = [r for r in results if r.get("judge") != "skipped" and not _case_blind(r)]
    pass_rate = (sum(1 for r in scored if r.get("ok")) / len(scored)) if scored else None
    avg_score = (
        round(sum(float(r.get("score") or 0.0) for r in scored) / len(scored), 4) if scored else None
    )
    traces = [r["langfuse_trace_url"] for r in results if r.get("langfuse_trace_url")]
    return {
        "flow_id": flow_id,
        "version": version,
        "mode": mode,
        "dataset": dataset.source,
        "cases": results,
        "pass_rate": pass_rate,
        "avg_score": avg_score,
        # 对账节:judge 不可达的用例既非通过也非否决,已排除出均值,单列计数
        "judge_unavailable_cases": len(judge_unavailable),
        # 对账节:输入侧序列化降级的用例(A2R46-01)同样非判定、剔出均值,单列计数
        "unavailable_cases": len(unavailable),
        # 对账节:观测关闭/ dry-run 模式为空列表(execution 模式逐 case 深链)
        "langfuse_traces": traces,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "wall_s": round(time.monotonic() - started, 3),
    }


def compare(base: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    """Diff two EvalReports of the same flow into a promote/rollback view."""
    by_id_base = {c["id"]: c for c in base.get("cases", [])}
    by_id_cand = {c["id"]: c for c in candidate.get("cases", [])}
    case_rows: List[Dict[str, Any]] = []
    regressions: List[str] = []
    improvements: List[str] = []
    for case_id in sorted(set(by_id_base) | set(by_id_cand)):
        b = by_id_base.get(case_id, {})
        c = by_id_cand.get(case_id, {})
        delta = round(float(c.get("score") or 0.0) - float(b.get("score") or 0.0), 4)
        row = {
            "id": case_id,
            "base_ok": b.get("ok"),
            "candidate_ok": c.get("ok"),
            "delta": delta,
        }
        blind = _case_blind(b) or _case_blind(c)
        if blind:
            # One side had no usable judge → ok/score deltas are not evidence.
            row["judge_unavailable"] = True
        case_rows.append(row)
        if blind:
            continue
        if b.get("ok") and not c.get("ok"):
            regressions.append(case_id)
        elif c.get("ok") and not b.get("ok"):
            improvements.append(case_id)

    def _num(report: Dict[str, Any], key: str) -> Optional[float]:
        value = report.get(key)
        return None if value is None else round(float(value), 4)

    base_pass, cand_pass = _num(base, "pass_rate"), _num(candidate, "pass_rate")
    base_avg, cand_avg = _num(base, "avg_score"), _num(candidate, "avg_score")
    return {
        "flow_id": candidate.get("flow_id") or base.get("flow_id"),
        "base_version": base.get("version"),
        "candidate_version": candidate.get("version"),
        "pass_rate": {"base": base_pass, "candidate": cand_pass, "delta": None
                      if base_pass is None or cand_pass is None else round(cand_pass - base_pass, 4)},
        "avg_score": {"base": base_avg, "candidate": cand_avg, "delta": None
                      if base_avg is None or cand_avg is None else round(cand_avg - base_avg, 4)},
        "improvements": improvements,
        "regressions": regressions,
        "cases": case_rows,
    }
