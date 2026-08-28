"""pge_lib —— PGE-plaita 的业务函数库（CODE 节点经 sys.path 引用）。

全部函数为纯逻辑或 git/命令封装的薄壳：git 序列、gate 判定、plan 解析校验、
preserve 描述。真正的外部交互（agent 调用、gate 命令执行）由
AGENTRUN / CAPTURE 原子完成，本库只做原子产出后的判定与组装。
"""
from __future__ import annotations

import json
import re
import subprocess


def run_git(args: list[str], cwd: str) -> dict:
    """跑一条 git 命令。返回 {ok, output}（CAPTURE 原子等价的进程内形态）。"""
    proc = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)
    return {"ok": proc.returncode == 0,
            "output": (proc.stdout or "").strip(),
            "stderr": (proc.stderr or "").strip()}


def baseline_sha(repo: str, dry: bool = False) -> dict:
    if dry:
        return {"ok": True, "sha": "dry-run", "dry": True}
    r = run_git(["rev-parse", "HEAD"], repo)
    if not r["ok"]:
        return {"ok": False, "error": r["stderr"]}
    return {"ok": True, "sha": r["output"]}


def worktree_ensure(repo: str, worktree_dir: str, dry: bool = False) -> dict:
    """创建 worktree（已存在视为复用）。残留注册先 prune 再重试一次。"""
    if dry:
        return {"ok": True, "reused": True, "dry": True}
    from pathlib import Path
    if Path(worktree_dir).exists():
        return {"ok": True, "reused": True}
    r = run_git(["worktree", "add", worktree_dir], repo)
    if not r["ok"]:
        run_git(["worktree", "prune"], repo)
        r = run_git(["worktree", "add", worktree_dir], repo)
    if not r["ok"]:
        return {"ok": False, "error": r["stderr"]}
    run_git(["config", "extensions.worktreeConfig", "true"], repo)
    return {"ok": True, "reused": False}


def judge_gate(exit_code: int, gate_name: str) -> dict:
    ok = exit_code == 0
    return {"gate": gate_name, "passed": ok,
            "reason": "" if ok else f"gate {gate_name} exit {exit_code}"}


def dirty_gates_check(results: list[dict]) -> dict:
    """baseline gate 健康检查：全部通过才放行。"""
    failed = [r for r in results if not r.get("passed")]
    if not failed:
        return {"dirty": False, "checked": len(results), "failed_names": []}
    return {"dirty": True, "checked": len(results),
            "failed_names": [r["gate"] for r in failed]}


def parse_plan(result_text: str, max_sprints: int, dry: bool = False) -> dict:
    if dry:
        return {"ok": True, "plan": {"title": "dry-run spec",
                "sprints": [{"name": "dry-sprint", "user_stories": [], "notes": "dry"}]}}
    """从 Planner 输出提取并校验 spec（sprints 上限 clamp）。"""
    text = str(result_text or "")
    start = text.find("{")
    plan = None
    if start >= 0:
        depth = 0
        in_str = False
        esc = False
        for i in range(start, len(text)):
            ch = text[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        plan = json.loads(text[start:i + 1])
                    except json.JSONDecodeError:
                        pass
                    break
    if not plan or not isinstance(plan.get("sprints"), list) or not plan["sprints"]:
        return {"ok": False, "error": "Planner 输出缺少 sprints 数组"}
    plan["sprints"] = plan["sprints"][:max_sprints]
    for i, s in enumerate(plan["sprints"]):
        s["index"] = i + 1
    return {"ok": True, "plan": plan}


def make_diff(worktree_dir: str, baseline_sha: str) -> dict:
    run_git(["add", "--intent-to-add", "-A"], worktree_dir)
    r = run_git(["diff"], worktree_dir)
    if not r["ok"]:
        return {"diff": "", "error": r["stderr"]}
    return {"diff": r["output"]}


def review_verdict(text: str) -> str:
    if re.search(r"VERDICT:\s*PASS", text):
        return "pass"
    if re.search(r"VERDICT:\s*NEEDS_FIX", text):
        return "needs-fix"
    return "no-verdict"


def land_commit(worktree_dir: str, message: str) -> dict:
    run_git(["add", "-A"], worktree_dir)
    r = run_git(["commit", "-m", message], worktree_dir)
    sha = run_git(["rev-parse", "HEAD"], worktree_dir)
    return {"ok": r["ok"], "sha": sha.get("sha", ""), "output": r["output"] or r["stderr"]}
