#!/usr/bin/env python3
"""plaita#68 变异复核驱动：**独立进程**重跑 mutmut 的非 killed 变异体。

用法::

    printf '%s\n' <id>... | python scripts/mutation68/recheck.py <repo-with-mutants> [workers]

输入：stdin 每行一个 **完整** ``module.mutant_name``（例如
``plaita.core.expression.x__fn_mul__mutmut_5``）——``MUTANT_UNDER_TEST`` 必须是这个
完整形式，mutmut 的 ``mutants/`` 树按模块挂载 trampoline。

判定口径（``docs/mutation-testing.md`` §3 / #124）：

* ``rc == 0`` → ``survived``（真实幸存者）
* 超时（本驱动 90s）/ 126 / 127 → ``error``（**不算 killed**，也不算等价）
* 其它非零 → ``killed``（mutmut 进程内统计漏报的假阳）

为什么需要独立进程：mutmut 默认在**同一进程**里跑测试，"幸存"可能是进程内状态
污染造成的假象；这里每个变异体一个全新解释器，并且用 ``-x`` 在首个失败处停下。
"""
import argparse
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# #68 判定用到的测试集（与 pyproject.toml 的 pytest_add_cli_args_test_selection 一致）
DEFAULT_TESTS = [
    "tests/unit/test_expression_size_guard.py",
    "tests/unit/test_expression.py",
    "tests/unit/test_expression_parser_mutations.py",
    "tests/unit/test_expression_cache.py",
    "tests/unit/test_expression_golden.py",
    "tests/unit/test_user_review_fixes.py",
    "tests/unit/test_issue55_attr_traversal.py",
]
TIMEOUT_SECONDS = 90.0


def classify(returncode: int) -> str:
    if returncode == 0:
        return "survived"
    if returncode in (124, 126, 127):
        return "error"
    return "killed"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo", help="仓库根（其下需有 mutmut 生成的 mutants/）")
    parser.add_argument("workers", nargs="?", type=int, default=1)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--timeout", type=float, default=TIMEOUT_SECONDS)
    parser.add_argument("--tests", nargs="*", default=DEFAULT_TESTS)
    args = parser.parse_args()

    mutants = Path(args.repo).resolve() / "mutants"
    if not mutants.is_dir():
        print(f"no mutants/ under {args.repo}（先 `mutmut run`）", file=sys.stderr)
        return 2
    ids = [line.strip() for line in sys.stdin if line.strip()]

    def run(mutant_id: str):
        env = {
            "MUTANT_UNDER_TEST": mutant_id,          # 完整 module.mutant_name
            "PYTHONPATH": str(mutants),
            "PATH": "/usr/bin:/bin:/usr/local/bin",
            "HOME": str(Path.home()),
        }
        try:
            proc = subprocess.run(
                [args.python, "-m", "pytest", "-x", "-q", "-p", "no:randomly", *args.tests],
                cwd=mutants, stdin=subprocess.DEVNULL, env=env,
                capture_output=True, text=True, timeout=args.timeout,
            )
            rc = proc.returncode
        except subprocess.TimeoutExpired:
            rc = 124
        return mutant_id, classify(rc), rc

    counts = {"killed": 0, "survived": 0, "error": 0}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        for mutant_id, status, rc in pool.map(run, ids):
            counts[status] += 1
            print(f"{mutant_id}\t{status}\trc={rc}", flush=True)
    print(f"TOTAL {len(ids)} killed={counts['killed']} survived={counts['survived']} "
          f"error={counts['error']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
