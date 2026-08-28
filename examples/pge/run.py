"""PGE-plaita 运行入口。

用法：
    .runtime/venv/bin/python examples/pge/run.py \
        --repo /tmp/pge-target --goal "给 calc 新增 percentage 函数并配测试" \
        [--sprints 1] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))  # pge_lib 可导入

PLANNER_HEAD = """你是 Planner。把下面的需求扩展成完整产品 spec：
- 忠于用户需求，不要扩大 scope；需求简单 spec 就简单（1 个 sprint 常常足够）
- 不要强行加 AI 功能
- 每个 sprint 一组用户故事；所有代码改动都发生在当前 worktree

输出严格符合 JSON：{"title": "...", "summary": "...", "sprints": [{"name": "短名", "user_stories": ["..."], "notes": "..."}]}"""

GEN_HEAD = """你是 Generator。在当前工作区实现 sprint 的全部用户故事：
- 可以创建/修改文件、运行命令；改动都留在 worktree
- 完成后简要总结改动清单"""

REVIEW_HEAD = """你是独立 Reviewer（与 Generator 不同 provider，防自评放水）。
审查 diff，重点：correctness、regressions、安全问题。
最后一行必须恰好是 VERDICT:PASS 或 VERDICT:NEEDS_FIX。"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pge-plaita")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--goal", required=True)
    parser.add_argument("--gate", default="python -m pytest -q")
    parser.add_argument("--planner", default="glm-52")
    parser.add_argument("--generator", default="glm-52")
    parser.add_argument("--reviewer", default="glm-52")
    parser.add_argument("--sprints", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    repo = str(Path(args.repo).resolve())
    worktree_dir = str(Path(repo) / ".worktrees" / "pge-plaita")

    import plaita_nodes
    from plaita.node import get_default_registry, register_code_node
    register_code_node(default_backend="subprocess")
    get_default_registry()

    from plaita import Flow
    flow = Flow.from_file(str(HERE / "pge.flow"))
    if args.dry_run:
        flow.global_context["dry_run"] = True

    print(f"🏗 PGE-plaita | repo={repo} | goal={args.goal}")
    out = flow.run(
        repo=repo,
        goal=args.goal,
        gate=args.gate,
        worktree_dir=worktree_dir,
        lib_dir=str(HERE),
        plan_head=PLANNER_HEAD,
        gen_head=GEN_HEAD,
        review_head=REVIEW_HEAD,
        planner_agent=args.planner,
        generator=args.generator,
        reviewer_agent=args.reviewer,
        max_sprints=args.sprints,
        dry_run=args.dry_run,
    )
    print("✅ 完成：", json.dumps(out, ensure_ascii=False, default=str)[:300])
    return 0


if __name__ == "__main__":
    sys.exit(main())
