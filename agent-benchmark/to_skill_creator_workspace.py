"""把 agent-benchmark 的 runs/ 产物转成 skill-creator iteration 布局。

输入：
  agent-benchmark/runs/<arm>/<task_id>/{solution.py, results.json, agent_stdout.txt, agent_stderr.txt}
  agent-benchmark/results/detail-*.json（harness 的逐任务记录，含 elapsed_s / pass_rate / cases）

输出（skill-creator viewer / aggregate 期望的布局）：
  <workspace>/iteration-1/eval-<name>/
      eval_metadata.json          {eval_id, eval_name, prompt, assertions}
      with_skill/run-1/
          outputs/{solution.py, results.json, agent_stdout.txt, agent_stderr.txt}
          grading.json            {expectations:[{text,passed,evidence}], summary, timing, execution_metrics}
          timing.json             {total_duration_seconds, ...}
      without_skill/run-1/同上

grading.json 由 results.json 与 tasks.py 的 expected 程序化比对生成，可靠可复用。
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from tasks import TASKS, VALIDATORS  # noqa: E402

PYLOKI_ROOT = ROOT.parent
WORKSPACE_DIR = Path.home() / ".claude" / "skills" / "flow-coder-workspace"
RUNS_DIR = WORKSPACE_DIR / "runs"   # 与 run_benchmark.py 的隔离 RUNS_DIR 一致
RESULTS_DIR = ROOT / "results"
SKILL_DIR = Path.home() / ".claude" / "skills" / "flow-coder"
EVALS_JSON = SKILL_DIR / "evals" / "evals.json"
WORKSPACE = WORKSPACE_DIR
ITER = WORKSPACE / "iteration-1"

ARM_MAP = {"skill": "with_skill", "noskill": "without_skill"}


def latest_detail() -> dict | None:
    files = sorted(RESULTS_DIR.glob("detail-*.json"))
    if not files:
        return None
    return json.loads(files[-1].read_text(encoding="utf-8"))


def find_record(detail: list[dict], task_id: str, arm: str) -> dict | None:
    for r in detail:
        if r["task_id"] == task_id and r["arm"] == arm:
            return r
    return None


def expectations_for(task: dict, cases: list[dict], dsl_ok: bool, dsl_evidence: str) -> list[dict]:
    """每个测试用例一条 expectation + 两条 DSL 用法 expectation。"""
    exps = []
    for c in cases:
        exps.append({
            "text": f"{c['input']} → {c['expected']!r}",
            "passed": c["pass"],
            "evidence": f"actual={c['actual']!r}",
        })
    # DSL 用法断言：从 solution.py 静态判断
    exps.append({
        "text": "流程用 @flow DSL 表达，未用普通 Python 函数绕过流程逻辑",
        "passed": dsl_ok,
        "evidence": dsl_evidence,
    })
    return exps


def check_dsl_usage(solution_src: str) -> tuple[bool, str]:
    """粗略判断 solution 是否用 @flow DSL 表达逻辑（而非普通 Python 绕过）。"""
    if not solution_src:
        return False, "未生成 solution.py"
    has_flow = ("@flow" in solution_src) and ("flow_from_source" in solution_src)
    # 反模式：把逻辑写在普通 def 里直接 return，FLOW_SRC 是个空壳
    stub = ('FLOW_SRC = r"""\n@flow("solution"' in solution_src
            or 'return INPUT' in solution_src)
    if has_flow and not stub:
        return True, "含 @flow 源码且通过 flow_from_source 执行"
    if has_flow and stub:
        return False, "FLOW_SRC 仍是骨架 stub，逻辑可能未填入 @flow"
    return False, "未检测到 @flow + flow_from_source"


def build_grading(task: dict, record: dict, solution_src: str) -> dict:
    cases = record.get("cases", [])
    dsl_ok, dsl_evidence = check_dsl_usage(solution_src)
    exps = expectations_for(task, cases, dsl_ok, dsl_evidence)
    passed = sum(1 for e in exps if e["passed"])
    total = len(exps)
    return {
        "expectations": exps,
        "summary": {
            "passed": passed,
            "failed": total - passed,
            "total": total,
            "pass_rate": round(passed / total, 3) if total else 0.0,
        },
        "execution_metrics": {
            "tool_calls": 0,
            "total_tool_calls": 0,
            "total_steps": 0,
            "errors_encountered": 0 if record.get("error") is None else 1,
            "output_chars": len(solution_src),
            "transcript_chars": 0,
        },
        "timing": {
            "total_duration_seconds": record.get("elapsed_s", 0.0),
        },
    }


def main() -> int:
    detail = latest_detail()
    if not detail:
        print("找不到 agent-benchmark/results/detail-*.json，请先跑 run_benchmark.py。", file=sys.stderr)
        return 2

    evals = json.loads(EVALS_JSON.read_text(encoding="utf-8"))["evals"]
    eval_by_name = {e["name"]: e for e in evals}

    if ITER.exists():
        import shutil
        shutil.rmtree(ITER)
    ITER.mkdir(parents=True, exist_ok=True)

    task_by_id = {t["id"]: t for t in TASKS}

    for e in evals:
        name = e["name"]
        task = task_by_id[name]
        eval_dir = ITER / f"eval-{name}"
        eval_dir.mkdir(parents=True, exist_ok=True)
        (eval_dir / "eval_metadata.json").write_text(json.dumps({
            "eval_id": e["id"],
            "eval_name": name,
            "prompt": e["prompt"],
            "assertions": e["expectations"],
        }, ensure_ascii=False, indent=2), encoding="utf-8")

        for arm in ("skill", "noskill"):
            cfg = ARM_MAP[arm]
            run_dir = eval_dir / cfg / "run-1"
            outs = run_dir / "outputs"
            outs.mkdir(parents=True, exist_ok=True)

            src_run = RUNS_DIR / arm / name
            record = find_record(detail, name, arm) or {}
            sol_src = ""
            if (src_run / "solution.py").exists():
                sol_src = (src_run / "solution.py").read_text(encoding="utf-8")
                (outs / "solution.py").write_text(sol_src, encoding="utf-8")
            for fn in ("results.json", "agent_stdout.txt", "agent_stderr.txt", "inputs.json"):
                f = src_run / fn
                if f.exists():
                    (outs / f.name).write_text(f.read_text(encoding="utf-8"), encoding="utf-8")

            grading = build_grading(task, record, sol_src)
            (run_dir / "grading.json").write_text(
                json.dumps(grading, ensure_ascii=False, indent=2), encoding="utf-8")
            (run_dir / "timing.json").write_text(json.dumps({
                "total_duration_seconds": record.get("elapsed_s", 0.0),
                "duration_ms": int(record.get("elapsed_s", 0.0) * 1000),
            }, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"已生成 skill-creator 工作区：{ITER}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
