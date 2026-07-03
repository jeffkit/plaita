"""Benchmark runner — drives the LLM harness over the agent-benchmark task set.

Usage as a library:

    from tests.llm.runner import run_benchmark
    run_benchmark(agent="fot", task_ids=["cond-grade"], seed=0)

Usage as a script (from ``plaita-ai/``):

    python -m tests.llm.runner --agent fot --task-ids cond-grade --seed 0

Or via the ``plaita-ai`` CLI:

    plaita-ai llm-benchmark --agent both --seed 0 --out-dir runs

Each run writes:
- ``<out_dir>/<timestamp>_<agent>_<model>.json``  — per-task full results
- ``<out_dir>/summary.json``                       — aggregated pass_rate

Reproducibility: ``seed`` defaults to 0 (override with --seed). The runner
records ``seed`` / ``model`` / ``provider`` / ``base_url`` / ``prompt_version``
in the result envelope so historical runs are comparable. ``temperature=0`` is
kept; providers that honour ``seed`` give the best effort at bit-level
reproducibility.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from tests.llm.harness import (
    build_model,
    model_config,
    select_tasks,
)
from tests.llm.prompts import PROMPT_VERSION

logger = logging.getLogger("plaita_ai.tests.llm.runner")

AgentKind = Literal["fot", "react", "both"]


def _run_one(agent: str, task: Dict[str, Any], model) -> Dict[str, Any]:
    # Imported lazily so ``import runner`` doesn't drag the agent stack in for
    # callers that only want ``select_tasks`` / metadata helpers.
    if agent == "fot":
        from tests.llm.harness import run_fot_task
        return run_fot_task(task, model)
    from tests.llm.harness import run_react_task
    return run_react_task(task, model)


def _envelope(
    agent: str,
    task_results: List[Dict[str, Any]],
    *,
    seed: Optional[int],
    cfg: Dict[str, Any],
    timestamp: str,
) -> Dict[str, Any]:
    return {
        "timestamp": timestamp,
        "agent": agent,
        "model": cfg["model"],
        "provider": cfg["provider"],
        "base_url": cfg["base_url"],
        "seed": seed,
        "prompt_version": PROMPT_VERSION,
        "task_count": len(task_results),
        "results": task_results,
    }


def _summary(all_envelopes: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_agent: Dict[str, Dict[str, float]] = {}
    for env in all_envelopes:
        a = env["agent"]
        rates = [r["pass_rate"] for r in env["results"]]
        agg = by_agent.setdefault(a, {"tasks": 0, "sum_pass_rate": 0.0})
        agg["tasks"] += len(rates)
        agg["sum_pass_rate"] += sum(rates)
    return {
        "timestamp": all_envelopes[0]["timestamp"] if all_envelopes else "",
        "seed": all_envelopes[0]["seed"] if all_envelopes else None,
        "prompt_version": PROMPT_VERSION,
        "per_agent": {
            a: {
                "tasks": v["tasks"],
                "mean_pass_rate": round(v["sum_pass_rate"] / v["tasks"], 3) if v["tasks"] else 0.0,
            }
            for a, v in by_agent.items()
        },
        "runs": [{"agent": e["agent"], "file": e.get("_file"), "model": e["model"]} for e in all_envelopes],
    }


def _print_summary_table(all_envelopes: List[Dict[str, Any]], out) -> None:
    if not all_envelopes:
        print("(no tasks ran)", file=out)
        return
    header = f"{'task_id':<28} {'agent':<7} {'pass_rate':<10} {'passed/total':<14}"
    print(header, file=out)
    print("-" * len(header), file=out)
    for env in all_envelopes:
        for r in env["results"]:
            print(
                f"{r['task_id']:<28} {env['agent']:<7} {r['pass_rate']:<10} "
                f"{r['passed']}/{r['total']}",
                file=out,
            )


def _print_failures(envelope: Dict[str, Any], err) -> None:
    for r in envelope["results"]:
        if r.get("agent_ok") is False or r["pass_rate"] == 0 or not r.get("source"):
            print(f"[{envelope['agent']}] {r['task_id']}: agent_ok={r.get('agent_ok')} "
                  f"pass_rate={r['pass_rate']}", file=err)
            if r.get("first_error"):
                print(f"  first_error: {r['first_error']}", file=err)
            if r.get("compile_errors"):
                print(f"  compile_errors: {r['compile_errors'][:3]}", file=err)
            if not r.get("source"):
                print("  (no @flow source extracted)", file=err)


def run_benchmark(
    agent: AgentKind = "both",
    *,
    task_ids: Optional[List[str]] = None,
    difficulty: Optional[str] = None,
    include_broken: bool = False,
    include_http: bool = False,
    out_dir: str = "runs",
    seed: Optional[int] = 0,
    timestamp: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Run the benchmark and persist results. Returns the per-agent envelopes."""
    cfg = model_config()
    ts = timestamp or time.strftime("%Y%m%d-%H%M%S")
    tasks = select_tasks(
        difficulty=difficulty,
        include_broken=include_broken,
        include_http=include_http,
        task_ids=task_ids,
    )
    if not tasks:
        print(f"no tasks matched (task_ids={task_ids}, difficulty={difficulty})", file=sys.stderr)
        return []

    agents: List[str]
    if agent == "both":
        agents = ["fot", "react"]
    else:
        agents = [agent]

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    envelopes: List[Dict[str, Any]] = []
    for a in agents:
        model = build_model(seed=seed)
        task_results: List[Dict[str, Any]] = []
        for t in tasks:
            print(f"[{a}] {t['id']} ...", file=sys.stderr)
            try:
                task_results.append(_run_one(a, t, model))
            except Exception as exc:  # noqa: BLE001 — benchmark must not abort on one task
                logger.exception("task %s via %s crashed", t["id"], a)
                task_results.append({
                    "task_id": t["id"],
                    "agent": a,
                    "pass_rate": 0.0,
                    "passed": 0,
                    "total": len(t.get("test_cases", [])),
                    "cases": [],
                    "first_error": f"{type(exc).__name__}: {exc}",
                    "source": "",
                    "agent_ok": False,
                    "prompt_version": PROMPT_VERSION,
                })
        env = _envelope(a, task_results, seed=seed, cfg=cfg, timestamp=ts)
        safe_model = cfg["model"].replace("/", "_")
        fname = out_path / f"{ts}_{a}_{safe_model}.json"
        fname.write_text(json.dumps(env, ensure_ascii=False, indent=2), encoding="utf-8")
        env["_file"] = str(fname)
        envelopes.append(env)
        _print_failures(env, sys.stderr)

    summary = _summary(envelopes)
    (out_path / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    _print_summary_table(envelopes, sys.stdout)
    return envelopes


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="plaita-ai-llm-benchmark", description=__doc__.splitlines()[0])
    p.add_argument("--agent", choices=["fot", "react", "both"], default="both")
    p.add_argument("--task-ids", nargs="*", default=None, help="只跑指定 task id（默认全部非 broken/http）")
    p.add_argument("--difficulty", default=None, help="按难度过滤")
    p.add_argument("--include-broken", action="store_true")
    p.add_argument("--include-http", action="store_true")
    p.add_argument("--out-dir", default="runs")
    p.add_argument("--seed", type=int, default=0, help="可复现 seed（默认 0；传 none 关闭）")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    seed: Optional[int] = None if args.seed is not None and str(args.seed).lower() == "none" else args.seed
    run_benchmark(
        agent=args.agent,
        task_ids=args.task_ids,
        difficulty=args.difficulty,
        include_broken=args.include_broken,
        include_http=args.include_http,
        out_dir=args.out_dir,
        seed=seed,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
