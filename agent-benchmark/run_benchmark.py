"""agent-benchmark 执行器

把 tasks.py 里的每个需求交给一个 AI Agent（claude code CLI + deepseek-v4-flash），
让它用 plaita @flow 生成并执行流程，再对照预期输出自动评分，用于评估
flow-coder skill 的有效性。

工作方式（单任务）：
  1. 在 runs/<arm>/<task_id>/ 下写入 inputs.json（只含测试输入，不含 expected，
     防止 agent "背答案"）。
  2. 用 skill（可选）+ 任务需求 + 输出规范拼成 prompt，调用：
        claude -p --model <model> --dangerously-skip-permissions -d <plaita_root> <prompt>
     环境变量把请求路由到 DeepSeek 的 anthropic 兼容端点：
        ANTHROPIC_BASE_URL=https://api.deepseek.com/anthropic
        ANTHROPIC_AUTH_TOKEN=$DEEPSEEK_API_KEY
  3. agent 在 runs/<arm>/<task_id>/ 下产出 solution.py 与 results.json：
        [{"input": {...}, "actual": <value>}, ...]
  4. harness 读 results.json，按 task 的 validator 对照 expected 打分。

用法：
    python agent-benchmark/run_benchmark.py                  # 跑全部，skill 臂
    python agent-benchmark/run_benchmark.py --arm both       # skill vs 无 skill 对比
    python agent-benchmark/run_benchmark.py --tasks cond-grade,map-double
    python agent-benchmark/run_benchmark.py --difficulty easy
    python agent-benchmark/run_benchmark.py --list           # 列出任务
    python agent-benchmark/run_benchmark.py --skip-http      # 跳过需外网的 http 任务

环境变量（可在 shell 覆盖）：
    BENCHMARK_MODEL        默认 deepseek-v4-flash
    DEEPSEEK_BASE_URL      默认 https://api.deepseek.com/anthropic
    DEEPSEEK_API_KEY       从环境或 ~/.zshrc 读取
    BENCHMARK_TIMEOUT      单任务超时秒数，默认 360
    BENCHMARK_CLAUDE       claude 可执行文件路径，默认 which claude
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

# 让本脚本无论从哪里调用都能 import tasks / validators
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from tasks import TASKS, VALIDATORS  # noqa: E402

PLAITA_ROOT = ROOT.parent
# skill 读取优先级：包内权威副本（plaita_ai/skills/flow-coder，随评审同步）>
# 家目录副本（用户安装的旧版）。此前只读家目录——本地副本陈旧时 --arm skill
# 测的是旧 skill，评分结论失真（2026-09 盲区评审 P1-3）。
_SKILL_PKG_DIR = PLAITA_ROOT / "plaita-ai" / "plaita_ai" / "skills" / "flow-coder"
_SKILL_HOME_DIR = Path.home() / ".claude" / "skills" / "flow-coder"
SKILL_DIR = _SKILL_PKG_DIR if (_SKILL_PKG_DIR / "SKILL.md").exists() else _SKILL_HOME_DIR
SKILL_MD = SKILL_DIR / "SKILL.md"
SKILL_REF = SKILL_DIR / "references" / "codeflow-reference.md"
# 隔离工作区：放在仓库外，agent 无法浏览 plaita 源码/文档，只能靠 prompt 里的知识。
WORKSPACE_DIR = Path.home() / ".claude" / "skills" / "flow-coder-workspace"
RUNS_DIR = WORKSPACE_DIR / "runs"          # agent 工作目录（隔离）
RESULTS_DIR = ROOT / "results"             # harness 报告输出（留在仓库里便于查看）

# 运行时配置（main 里根据 CLI 参数设置）
CONFIG = {
    "isolated": True,      # True=隔离目录运行（推荐，测 skill 真实边际价值）
    "include_broken": False,
}


# ---------------------------------------------------------------------------
# 环境与配置
# ---------------------------------------------------------------------------

def load_deepseek_key() -> str:
    key = os.environ.get("DEEPSEEK_API_KEY")
    if key:
        return key.strip()
    zshrc = Path.home() / ".zshrc"
    if zshrc.exists():
        text = zshrc.read_text(encoding="utf-8")
        m = re.search(r'export\s+DEEPSEEK_API_KEY\s*=\s*["\']([^"\']+)["\']', text)
        if m:
            return m.group(1)
    raise RuntimeError(
        "未找到 DEEPSEEK_API_KEY：既不在环境变量里，也不在 ~/.zshrc 里。"
    )


def build_env() -> dict[str, str]:
    env = os.environ.copy()
    env["ANTHROPIC_BASE_URL"] = os.environ.get(
        "DEEPSEEK_BASE_URL", "https://api.deepseek.com/anthropic"
    )
    env["ANTHROPIC_AUTH_TOKEN"] = load_deepseek_key()
    # 隔离模式下 agent 在仓库外运行，靠 PYTHONPATH 让 solution.py 能 import plaita，
    # 而 agent 本身不会被指向 plaita 源码目录去浏览。
    env["PYTHONPATH"] = str(PYLOKI_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    return env


def model_name() -> str:
    return os.environ.get("BENCHMARK_MODEL", "deepseek-v4-flash")


def claude_bin() -> str:
    b = os.environ.get("BENCHMARK_CLAUDE")
    if b:
        return b
    out = shutil.which("claude")
    if not out:
        raise RuntimeError("找不到 claude 可执行文件，请装 claude code CLI 或设 BENCHMARK_CLAUDE")
    return out


def timeout_s() -> int:
    return int(os.environ.get("BENCHMARK_TIMEOUT", "360"))


# ---------------------------------------------------------------------------
# prompt 构造
# ---------------------------------------------------------------------------

def skill_block() -> str:
    parts = []
    if SKILL_MD.exists():
        parts.append(SKILL_MD.read_text(encoding="utf-8"))
    if SKILL_REF.exists():
        parts.append("\n\n# 附录：@flow DSL 完整参考\n\n" + SKILL_REF.read_text(encoding="utf-8"))
    return "\n".join(parts)


SOLUTION_TEMPLATE = '''"""由 benchmark harness 生成的运行器骨架。

请把你的 @flow 源码填进 FLOW_SRC，然后运行本文件：
    python {run_path}

它会编译、执行，并把每个测试输入的 actual 写入同目录 results.json。
"""
from __future__ import annotations

import json
from pathlib import Path

from plaita.dsl.codeflow import compile_source, flow_from_source

# 在下面的三引号之间填入你的 @flow 源码字符串
FLOW_SRC = r"""
@flow("solution", input_type="object", desc="TODO")
def solution(INPUT):
    return INPUT
"""

def main() -> None:
    here = Path(__file__).parent
    inputs = json.loads((here / "inputs.json").read_text(encoding="utf-8"))

    # 1. 编译校验（失败会抛带行号的异常）
    compile_source(FLOW_SRC)
    # 2. 构建并执行
    flow = flow_from_source(FLOW_SRC)

    results = []
    for item in inputs:
        actual = flow.run(**item["input"])
        results.append({{"input": item["input"], "actual": actual}})
    (here / "results.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("WROTE results.json")


if __name__ == "__main__":
    main()
'''


def build_prompt(task: dict[str, Any], run_dir: Path, with_skill: bool) -> str:
    run_path = run_dir / "solution.py"
    inputs_path = run_dir / "inputs.json"
    req = task["requirement"]
    isolated = CONFIG["isolated"]

    if with_skill:
        skill = skill_block()
    elif isolated:
        # 隔离模式下的 baseline：只给最少的 API 提示，不给 DSL 参考、不指向仓库文档。
        skill = (
            "你可以使用 plaita 库（已可通过 import 使用）。\n"
            "相关入口：`from plaita.dsl.codeflow import flow_from_source, compile_source`。\n"
            "`@flow` 是 plaita 的 DSL：用 `@flow` 装饰一个 Python 函数，函数体被静态编译成流程，\n"
            "而非真的当 Python 执行。运行期生成请用 `flow_from_source(源码字符串)`，\n"
            "校验用 `compile_source(源码字符串)`。请凭你已有的知识完成任务。\n\n"
        )
    else:
        skill = (
            "你可以参考项目内的 @flow DSL 文档：docs-site/docs/guide/code-dsl.md，"
            "以及示例 examples/agent/。请用 plaita 的 @flow DSL 完成任务。\n\n"
        )

    isolation_note = (
        "# 工作环境（重要）\n\n"
        "你在一个**隔离目录**里工作，plaita 只能通过 `import` 使用。\n"
        "**不要去文件系统搜索或读取 plaita 的源码、文档或示例**（例如不要找 docs-site、\n"
        "examples、plaita/ 等目录）。只依据本 prompt 提供的知识完成任务。\n"
        "只在当前工作目录下读写文件。\n\n"
        if isolated else ""
    )

    return f"""你是一个 plaita @flow 流程工程师。请按下面的需求，用 @flow DSL 生成一个可执行流程，编译校验后执行，把每个测试输入的运行结果写入指定 JSON 文件。

{skill}
{isolation_note}# 任务

需求：{req}

任务的 @flow 主流程 input_type 用 "object"，字段从 INPUT.x 读取。

# 测试输入

测试输入已写入文件：{inputs_path}
（一个 JSON 数组，每个元素形如 {{"input": {{...}}}}）。
请读取该文件，按顺序对每个 input 运行你的 flow，把 actual 结果收集起来。

# 你要做的事

1. 在文件 {run_path} 中写一个运行器：把你的 @flow 源码填入 FLOW_SRC 字符串变量，
   用 `compile_source(FLOW_SRC)` 先编译校验，再用 `flow_from_source(FLOW_SRC)` 构建，
   然后对 inputs.json 里每个 input 调 `flow.run(**input)`，把结果写入同目录的
   `results.json`，格式为：
   [{{"input": {{...}}, "actual": <运行结果>}}, ...]
   顺序与 inputs.json 一一对应。

   运行器骨架（你可以直接基于它改）：
```python
{SOLUTION_TEMPLATE.format(run_path=str(run_path))}
```

2. 执行 `python {run_path}`，确保 results.json 被写出。如果编译或执行报错，
   把错误信息回灌给自己，修正 @flow 源码后重试，最多 3 轮。

# 约束

- 只用 plaita 的 @flow DSL（flow_from_source / compile_source），不要把业务逻辑
  写成普通 Python 函数让 flow.run 绕过 DSL——流程逻辑必须由 @flow 源码表达。
- 节点调用（HTTP/CHILD/PARALLEL/MAP/FILTER/FIND/LOOP/REDUCE 等）只能作语句或
  赋值右侧，不能嵌在 return 表达式里。
- 不要用 f-string、三元表达式、推导式、lambda；字符串拼接用 F.concat。
- actual 若是 None，写 JSON null。
- 只在当前工作目录下写文件，不要修改仓库或其它任何文件。
- 完成后简短说明：你生成的 flow 做了什么、results.json 在哪、是否经历过自纠。

现在开始。
"""


# ---------------------------------------------------------------------------
# 运行单个任务
# ---------------------------------------------------------------------------

def prepare_run_dir(task: dict[str, Any], arm: str) -> Path:
    d = RUNS_DIR / arm / task["id"]
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True, exist_ok=True)
    inputs = [{"input": tc["input"]} for tc in task["test_cases"]]
    (d / "inputs.json").write_text(
        json.dumps(inputs, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return d


def run_agent(task: dict[str, Any], arm: str) -> dict[str, Any]:
    with_skill = arm == "skill"
    run_dir = prepare_run_dir(task, arm)
    prompt = build_prompt(task, run_dir, with_skill)

    isolated = CONFIG["isolated"]
    work_cwd = str(run_dir) if isolated else str(PYLOKI_ROOT)
    add_dir = str(run_dir) if isolated else str(PYLOKI_ROOT)

    cmd = [
        claude_bin(),
        "-p",
        "--model", model_name(),
        "--dangerously-skip-permissions",
        "--output-format", "text",
        "-d", add_dir,
        prompt,
    ]
    env = build_env()
    t0 = time.time()
    timed_out = False
    try:
        proc = subprocess.run(
            cmd,
            cwd=work_cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_s(),
        )
        stdout = proc.stdout
        stderr = proc.stderr
        rc = proc.returncode
    except subprocess.TimeoutExpired as e:
        timed_out = True
        stdout = (e.stdout or b"").decode("utf-8", "replace") if isinstance(e.stdout, bytes) else (e.stdout or "")
        stderr = (e.stderr or b"").decode("utf-8", "replace") if isinstance(e.stderr, bytes) else (e.stderr or "")
        rc = -1
    elapsed = time.time() - t0

    # 落盘 agent 原始输出，便于事后排查
    (run_dir / "agent_stdout.txt").write_text(stdout or "", encoding="utf-8")
    (run_dir / "agent_stderr.txt").write_text(stderr or "", encoding="utf-8")

    # 评分
    score = score_task(task, run_dir)

    return {
        "task_id": task["id"],
        "arm": arm,
        "category": task["category"],
        "difficulty": task["difficulty"],
        "passed": score["passed"],
        "pass_rate": score["pass_rate"],
        "cases": score["cases"],
        "elapsed_s": round(elapsed, 1),
        "timed_out": timed_out,
        "rc": rc,
        "error": score["error"],
        "run_dir": str(run_dir),
    }


def score_task(task: dict[str, Any], run_dir: Path) -> dict[str, Any]:
    results_file = run_dir / "results.json"
    cases = []
    if not results_file.exists():
        return {
            "passed": 0,
            "pass_rate": 0.0,
            "cases": [],
            "error": "results.json 未生成",
        }

    try:
        results = json.loads(results_file.read_text(encoding="utf-8"))
    except Exception as e:
        return {
            "passed": 0,
            "pass_rate": 0.0,
            "cases": [],
            "error": f"results.json 解析失败: {e}",
        }

    if not isinstance(results, list) or len(results) != len(task["test_cases"]):
        return {
            "passed": 0,
            "pass_rate": 0.0,
            "cases": [],
            "error": (
                f"results.json 条数({len(results) if isinstance(results, list) else 'N/A'})"
                f"与测试用例数({len(task['test_cases'])})不一致"
            ),
        }

    validator = VALIDATORS[task["validator"]]
    passed = 0
    for i, (got, tc) in enumerate(zip(results, task["test_cases"])):
        actual = got.get("actual") if isinstance(got, dict) else None
        expected = tc["expected"]
        ok = bool(validator(actual, expected))
        if ok:
            passed += 1
        cases.append({
            "index": i,
            "input": tc["input"],
            "expected": expected,
            "actual": actual,
            "pass": ok,
        })

    return {
        "passed": passed,
        "pass_rate": round(passed / len(task["test_cases"]), 3),
        "cases": cases,
        "error": None,
    }


# ---------------------------------------------------------------------------
# 汇总
# ---------------------------------------------------------------------------

def summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    by_arm: dict[str, list[dict[str, Any]]] = {}
    for r in records:
        by_arm.setdefault(r["arm"], []).append(r)
    arms = {}
    for arm, rs in by_arm.items():
        arms[arm] = {
            "tasks": len(rs),
            "fully_passed": sum(1 for r in rs if r["pass_rate"] == 1.0),
            "avg_pass_rate": round(sum(r["pass_rate"] for r in rs) / len(rs), 3) if rs else 0.0,
            "by_difficulty": _by_key(rs, "difficulty"),
            "by_category": _by_key(rs, "category"),
        }
    return {"arms": arms}


def _by_key(rs: list[dict[str, Any]], key: str) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for r in rs:
        groups.setdefault(r[key], []).append(r)
    out = {}
    for k, gs in groups.items():
        out[k] = {
            "tasks": len(gs),
            "avg_pass_rate": round(sum(g["pass_rate"] for g in gs) / len(gs), 3),
            "fully_passed": sum(1 for g in gs if g["pass_rate"] == 1.0),
        }
    return out


def write_report(records: list[dict[str, Any]], summary: dict[str, Any]) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    detail = RESULTS_DIR / f"detail-{ts}.json"
    report = RESULTS_DIR / f"report-{ts}.json"
    detail.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    report.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # 人读 markdown
    md = RESULTS_DIR / f"report-{ts}.md"
    lines = ["# Agent Benchmark 报告", "", f"生成时间：{ts}", ""]
    for arm, s in summary["arms"].items():
        lines.append(f"## 臂：{arm}")
        lines.append(f"- 任务数：{s['tasks']}")
        lines.append(f"- 全通过：{s['fully_passed']}/{s['tasks']}")
        lines.append(f"- 平均通过率：{s['avg_pass_rate']}")
        lines.append("")
        lines.append("| 难度 | 任务数 | 全通过 | 平均通过率 |")
        lines.append("|------|--------|--------|-----------|")
        for k, v in s["by_difficulty"].items():
            lines.append(f"| {k} | {v['tasks']} | {v['fully_passed']} | {v['avg_pass_rate']} |")
        lines.append("")
        lines.append("| 类别 | 任务数 | 全通过 | 平均通过率 |")
        lines.append("|------|--------|--------|-----------|")
        for k, v in s["by_category"].items():
            lines.append(f"| {k} | {v['tasks']} | {v['fully_passed']} | {v['avg_pass_rate']} |")
        lines.append("")
    # 逐任务明细
    lines.append("## 逐任务明细")
    lines.append("| arm | task | 难度 | 类别 | 通过率 | 耗时(s) | 备注 |")
    lines.append("|-----|------|------|------|--------|---------|------|")
    for r in records:
        note = r["error"] or ("timeout" if r["timed_out"] else "")
        lines.append(
            f"| {r['arm']} | {r['task_id']} | {r['difficulty']} | {r['category']} | "
            f"{r['pass_rate']} | {r['elapsed_s']} | {note} |"
        )
    md.write_text("\n".join(lines), encoding="utf-8")
    return md


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def select_tasks(args: argparse.Namespace) -> list[dict[str, Any]]:
    out = TASKS
    if args.tasks:
        ids = [x.strip() for x in args.tasks.split(",") if x.strip()]
        out = [t for t in out if t["id"] in ids]
    if args.difficulty:
        out = [t for t in out if t["difficulty"] == args.difficulty]
    if args.category:
        out = [t for t in out if t["category"] == args.category]
    if args.skip_http:
        out = [t for t in out if not t.get("requires_http")]
    if not args.include_broken:
        out = [t for t in out if not t.get("known_broken")]
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="agent-benchmark 执行器")
    ap.add_argument("--arm", choices=["skill", "noskill", "both"], default="skill",
                    help="skill=带 flow-coder skill；noskill=不带；both=两臂对比（评估 skill 有效性）")
    ap.add_argument("--tasks", default=None, help="逗号分隔的 task id，默认全部")
    ap.add_argument("--difficulty", default=None, choices=["easy", "medium", "hard"])
    ap.add_argument("--category", default=None, help="按 category 过滤")
    ap.add_argument("--skip-http", action="store_true", help="跳过 requires_http 的任务")
    ap.add_argument("--include-broken", action="store_true",
                    help="包含 known_broken 任务（默认跳过，避免在 plaita 运行时 bug 上浪费超时）")
    ap.add_argument("--no-isolated", action="store_true",
                    help="关闭隔离模式：在 plaita 仓库内运行（agent 可读源码/文档，不推荐用于评估 skill）")
    ap.add_argument("--list", action="store_true", help="列出任务后退出")
    args = ap.parse_args()

    CONFIG["isolated"] = not args.no_isolated
    CONFIG["include_broken"] = args.include_broken

    if args.list:
        for t in TASKS:
            flags = []
            if t.get("requires_http"):
                flags.append("[http]")
            if t.get("known_broken"):
                flags.append("[broken]")
            print(f"{t['id']:24s} {t['difficulty']:6s} {t['category']:22s} {' '.join(flags)}")
        return 0

    tasks = select_tasks(args)
    if not tasks:
        print("没有选中任何任务。", file=sys.stderr)
        return 2

    arms = ["skill", "noskill"] if args.arm == "both" else [args.arm]

    mode = "隔离（仓库外，agent 仅靠 prompt 知识）" if CONFIG["isolated"] else "仓库内（agent 可读源码/文档）"
    print(f"模型：{model_name()}  端点：{os.environ.get('DEEPSEEK_BASE_URL', 'https://api.deepseek.com/anthropic')}")
    print(f"模式：{mode}")
    print(f"任务数：{len(tasks)}  臂：{arms}  单任务超时：{timeout_s()}s")
    print(f"工作目录：{RUNS_DIR}")
    print("-" * 60)

    records: list[dict[str, Any]] = []
    for arm in arms:
        for t in tasks:
            print(f"[{arm}] {t['id']} ... ", end="", flush=True)
            rec = run_agent(t, arm)
            records.append(rec)
            tag = "PASS" if rec["pass_rate"] == 1.0 else f"{rec['pass_rate']}"
            note = rec["error"] or ("timeout" if rec["timed_out"] else "")
            print(f"{tag}  ({rec['elapsed_s']}s) {note}")

    print("-" * 60)
    summary = summarize(records)
    md = write_report(records, summary)
    print(f"报告：{md}")
    print(f"明细：{RESULTS_DIR}")
    # 简要打印臂对比
    for arm, s in summary["arms"].items():
        print(f"  [{arm}] 全通过 {s['fully_passed']}/{s['tasks']}  平均通过率 {s['avg_pass_rate']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
