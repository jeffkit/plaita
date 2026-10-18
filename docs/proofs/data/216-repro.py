#!/usr/bin/env python3
"""plaita#216 repro / mutation-replay probe (9 diagnostic/cache survivors).

Run from the repository root (or with ``PYTHONPATH`` pointing at the tree under
test)::

    # "before": on the pre-fix tree the nine mutations SURVIVE the existing
    # diagnostic/cache suites (every run stays green).
    PYTHONPATH=. .venv/bin/python docs/proofs/data/216-repro.py \
        --tree . \
        --tests tests/unit/test_expression_cache.py \
                tests/unit/test_reviewfix_a_template_warn.py \
                tests/unit/test_expression_parser_root_scan.py

    # "after": the new per-item tests must KILL each mutation (every run red).
    PYTHONPATH=. .venv/bin/python docs/proofs/data/216-repro.py \
        --tree . --tests tests/unit/test_issue216_expression_parser_debt.py

Each mutation is applied to ``<tree>/plaita/core/expression_parser.py`` with an
exact literal replacement, one pytest process is run against ``--tests``, and
the original file is restored (``try/finally``).  Bytecode is disabled and
``__pycache__`` cleared before each run: two mutations can produce a source file
with the same size, and a stale ``.pyc`` from the previous mutant would silently
run the wrong code (observed while building this proof).

The issue's acceptance is "new test red under the mutation, current code
green".  The script therefore reports ``verdict: survived`` for a green run and
``killed`` for a red one; ``run_base`` is the un-mutated control.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import subprocess
import sys

MUTATIONS = [
    {
        "id": "216-1",
        "target": "_warn_suspicious_literal 尾句",
        "mutation": "rewrite the string. -> rewrite it.",
        "old": 'and {%% ... %%} for interpolation — rewrite the string."',
        "new": 'and {%% ... %%} for interpolation — rewrite it."',
        "test": "test_suspicious_literal_warning_text_is_pinned",
    },
    {
        "id": "216-2",
        "target": "_warn_unmatched_template 尾句",
        "mutation": "fix the inner expression -> repair the inner expression",
        "old": "evaluation — fix the inner expression (e.g. {%% $INPUT.x %%}).",
        "new": "evaluation — repair the inner expression (e.g. {%% $INPUT.x %%}).",
        "test": "test_unmatched_template_warning_text_is_pinned",
    },
    {
        "id": "216-3",
        "target": "_annotate_parse_error 提示短语",
        "mutation": "常见原因： -> 可能原因：",
        "old": '"常见原因：字符串常量里的引号/反斜杠未转义',
        "new": '"可能原因：字符串常量里的引号/反斜杠未转义',
        "test": "test_parse_error_hint_uses_common_cause_phrase",
    },
    {
        "id": "216-4",
        "target": "_warned_literals 去重重置阈值",
        "mutation": "> 512 -> > 1024",
        "old": "if len(_warned_literals) > 512:",
        "new": "if len(_warned_literals) > 1024:",
        "test": "test_suspicious_literal_dedupe_resets_above_512",
    },
    {
        "id": "216-5",
        "target": "_warned_unmatched_templates 去重重置阈值",
        "mutation": "> 512 -> > 1024",
        "old": "if len(_warned_unmatched_templates) > 512:",
        "new": "if len(_warned_unmatched_templates) > 1024:",
        "test": "test_unmatched_template_dedupe_resets_above_512",
    },
    {
        "id": "216-6",
        "target": "LRU 命中顺序",
        "mutation": "命中时不 cache.move_to_end(value)",
        "old": '''        compiled = cache.get(value)
        if compiled is not None:
            try:
                cache.move_to_end(value)
            except KeyError:  # pragma: no cover - evicted between get and move
                pass
            return compiled''',
        "new": '''        compiled = cache.get(value)
        if compiled is not None:
            return compiled''',
        "test": "test_lru_hit_moves_entry_to_newest",
    },
    {
        "id": "216-7",
        "target": "LRU 容量边界",
        "mutation": "while len(cache) > maxsize -> >= maxsize",
        "old": "while len(cache) > maxsize:",
        "new": "while len(cache) >= maxsize:",
        "test": "test_lru_capacity_boundary_is_maxsize_not_less",
    },
    {
        "id": "216-8",
        "target": "_render_path 字段段",
        "mutation": "raw -> target",
        "old": 'parts.append(f"[{target}]" if kind == "index" else f".{raw}")',
        "new": 'parts.append(f"[{target}]" if kind == "index" else f".{target}")',
        "test": "test_render_path_uses_raw_field_name_for_special_roots",
    },
    {
        "id": "216-9",
        "target": "LRU 逐出端",
        "mutation": "popitem(last=False) -> popitem()",
        "old": "cache.popitem(last=False)",
        "new": "cache.popitem()",
        "test": "test_lru_evicts_oldest_not_newest",
    },
]


def _clear_bytecode(tree: pathlib.Path) -> None:
    for cache in tree.glob("plaita/**/__pycache__"):
        shutil.rmtree(cache, ignore_errors=True)


def _pytest(tree: pathlib.Path, tests, timeout: int = 300):
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    env["PYTHONPATH"] = str(tree)
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", *tests, "-q", "--no-header", "-p", "no:cacheprovider"],
        cwd=tree, text=True, capture_output=True, timeout=timeout, env=env,
    )
    text = (proc.stdout + proc.stderr).strip()
    lines = text.splitlines()
    # 红文案要精确：长告警（#216-4/5 会打 600 行 WARNING）会把 tail 淹没，
    # 这里只保留断言/异常行 + pytest 摘要行。
    red = [line for line in lines
           if line.lstrip().startswith(("E ", ">", "assert"))
           or "FAILED " in line or "AssertionError" in line
           or "DID NOT RAISE" in line or "failed in" in line]
    return {
        "exit": proc.returncode,
        "summary": [line for line in lines if "passed" in line or "failed" in line or "error" in line][-3:],
        "tail": "\n".join(red[-14:] or lines[-12:]),
    }


def _apply(tree: pathlib.Path, mutation) -> str:
    source = tree / "plaita/core/expression_parser.py"
    original = source.read_text()
    count = original.count(mutation["old"])
    if count != 1:
        raise SystemExit(
            f"{mutation['id']}: mutation anchor found {count} times in {source}")
    source.write_text(original.replace(mutation["old"], mutation["new"]))
    return original


def _restore(tree: pathlib.Path, original: str) -> None:
    (tree / "plaita/core/expression_parser.py").write_text(original)
    _clear_bytecode(tree)


def replay(tree: pathlib.Path, tests, only=None) -> dict:
    source = tree / "plaita/core/expression_parser.py"
    if not source.exists():
        raise SystemExit(f"not a plaita tree: {source}")
    results = []
    selected = [m for m in MUTATIONS if only is None or m["id"] in only]
    if only:
        missing = set(only) - {m["id"] for m in selected}
        if missing:
            raise SystemExit(f"unknown mutation ids: {sorted(missing)}")
    # 只要 --tests 指向一个**已存在**且确实包含该用例的文件，就精确只跑那一
    # 条用例（"这一条测试杀掉这一条变异"的强证据）；否则跑整个 tests 列表
    #（before 探针：新测试文件在基线树上不存在，用既有套件证明"幸存"）。
    single_file = (
        len(tests) == 1 and tests[0].endswith(".py")
        and (tree / tests[0]).is_file()
    )
    for mutation in selected:
        original = _apply(tree, mutation)
        try:
            targets = tests
            if single_file and mutation["test"] in (tree / tests[0]).read_text():
                targets = [f"{tests[0]}::{mutation['test']}"]
            run = _pytest(tree, targets)
        finally:
            _restore(tree, original)
        results.append({
            "id": mutation["id"],
            "target": mutation["target"],
            "mutation": mutation["mutation"],
            "test": mutation["test"],
            "pytest_target": targets,
            "pytest_exit": run["exit"],
            "summary": run["summary"],
            "verdict": "killed" if run["exit"] != 0 else "survived",
            "red_tail": run["tail"] if run["exit"] != 0 else None,
        })
    return {"tree": str(tree), "tests": list(tests), "mutations": results}


def run(tree: pathlib.Path, tests, only=None) -> dict:
    _clear_bytecode(tree)
    base = _pytest(tree, tests)
    return {
        "issue": 216,
        "plaita_file": str(tree / "plaita/core/expression_parser.py"),
        "python": sys.version.split()[0],
        "base_run": {"exit": base["exit"], "summary": base["summary"]},
        "replay": replay(tree, tests, only),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tree", default=".", help="plaita checkout to mutate")
    parser.add_argument("--tests", nargs="+", required=True,
                        help="pytest targets (may be a test file or node ids)")
    parser.add_argument("--only", nargs="+", default=None,
                        help="only replay these mutation ids (e.g. 216-4 216-9)")
    parser.add_argument("--json", default=None, help="write the JSON result here")
    args = parser.parse_args(argv)

    result = run(pathlib.Path(args.tree).resolve(), args.tests, args.only)
    text = json.dumps(result, ensure_ascii=False, indent=2)
    print(text)
    if args.json:
        pathlib.Path(args.json).write_text(text + "\n")
    killed = sum(1 for m in result["replay"]["mutations"] if m["verdict"] == "killed")
    print(f"MUTATIONS_KILLED {killed}/{len(result['replay']['mutations'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
