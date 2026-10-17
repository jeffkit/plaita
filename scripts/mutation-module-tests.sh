#!/usr/bin/env bash
# 打印 `[tool.mutmut] only_mutate` 里每个模块对应的测试文件，供按 §7 收窄时使用。
#
# 存在理由（jeffkit/plaita#125）：`pytest_add_cli_args_test_selection` **优先于**
# `pytest_add_cli_args`，mutmut 用它逐变异点跑测试。**只改 `only_mutate` 而不改
# `test_selection`** 时，跑某模块实际执行的仍是 event/codeflow 的测试 ⇒ 分数被配置
# 压低。实测 `decide.py`：现状 73.7% vs 正确选择 93.9%（差 19.5 个百分点）。
#
# 用法:
#   bash scripts/mutation-module-tests.sh              # 列出全部映射
#   bash scripts/mutation-module-tests.sh decide       # 只看匹配的模块
#   bash scripts/mutation-module-tests.sh --snippet decide
#                                                      # 直接输出可粘贴的 toml 片段
set -uo pipefail
cd "$(dirname "$0")/.."

# 解释器与各 recheck_*.sh **同一套解析顺序**（plaita#167）：显式 PYTHON_BIN →
# 已激活的 ${VIRTUAL_ENV} → 仓根 .venv → PATH 上第一个 ≥3.11。
# 此前这里裸调 `python3`：macOS 系统 3.9.6 没有 `tomllib`，脚本于是丢一个
# `ModuleNotFoundError` 并**静默输出空模块表**（"没有匹配 '' 的模块"），让
# 「模块 → 测试集合」这条本该防假低分的路自己变成假红源。
# shellcheck source=scripts/lib/mutation_recheck_common.sh
source scripts/lib/mutation_recheck_common.sh

FILTER="${1:-}"
MODE="list"
if [[ "$FILTER" == "--snippet" ]]; then
    MODE="snippet"; FILTER="${2:-}"
fi

# 模块 → 测试文件候选（按 `tests/unit/test_<stem>*.py` 约定匹配；多候选取全部，
# 因为有些模块的测试分散在若干文件里，漏掉会重演 #125 的"假低分"）。
if ! modules=$("$MUT_PYTHON_BIN" - <<'PY'
import sys

# 具名跳过（#167）：解释器太老时说清缺什么、怎么修，而不是丢 traceback 后
# 输出空表——空表会被下游当成「没有匹配的模块」，正是假红/假绿的来源。
if sys.version_info < (3, 11):
    sys.stderr.write(
        "SKIP(#167): 解析到的解释器 %s 是 %d.%d，读 pyproject.toml 需要 Python "
        ">= 3.11（tomllib）。请设 PYTHON_BIN=<3.11+ 的解释器>、激活 venv，"
        "或把 3.11+ 放到 PATH 上后重跑；这不是模块表为空。\n"
        % (sys.executable, sys.version_info[0], sys.version_info[1])
    )
    raise SystemExit(4)

import tomllib

with open("pyproject.toml", "rb") as f:
    d = tomllib.load(f)
for m in d["tool"]["mutmut"].get("only_mutate", []):
    print(m)
PY
); then
    echo "模块表读取失败（解释器：${MUT_PYTHON_BIN}）——见上方 SKIP 理由。" >&2
    exit 4
fi

found_any=0
while IFS= read -r mod; do
    [[ -z "$mod" ]] && continue
    stem=$(basename "$mod" .py)
    [[ -n "$FILTER" && "$mod" != *"$FILTER"* ]] && continue

    files=$(ls tests/unit/ 2>/dev/null | grep -E "^test_${stem}(_|\.py)" | sed 's|^|tests/unit/|' | tr '\n' ' ')
    if [[ -z "${files// /}" ]]; then
        # 退化 1：按"父目录_文件名"匹配（event/core.py -> test_event_core*.py）
        parent=$(basename "$(dirname "$mod")")
        alt="${parent}_${stem}"
        files=$(ls tests/unit/ 2>/dev/null | grep -E "^test_${alt}" | sed 's|^|tests/unit/|' | tr '\n' ' ')
    fi
    if [[ -z "${files// /}" ]]; then
        # 退化 2：去掉前导下划线后按"父目录+名"宽松匹配
        #（dsl/codeflow/_common.py -> test_codeflow_*common*；_stmt -> test_codeflow_stmt*）
        bare="${stem#_}"
        files=$(ls tests/unit/ 2>/dev/null \
            | grep -E "^test_.*${parent}.*${bare}|^test_.*${bare}" \
            | sed 's|^|tests/unit/|' | tr '\n' ' ')
    fi
    found_any=1

    if [[ "$MODE" == "snippet" ]]; then
        echo "pytest_add_cli_args_test_selection = ["
        for f in $files; do echo "  \"$f\","; done
        echo "]"
    else
        if [[ -z "${files// /}" ]]; then
            printf '  %-40s %s\n' "$mod" "⚠ 未找到同名测试，请人工确认"
        else
            printf '  %-40s %s\n' "$mod" "$files"
        fi
    fi
done <<< "$modules"

if [[ $found_any -eq 0 ]]; then
    echo "没有匹配 '$FILTER' 的模块。可用模块：" >&2
    echo "$modules" | sed 's/^/  /' >&2
    exit 1
fi

if [[ "$MODE" == "list" ]]; then
    cat >&2 <<'EOF'

提示：按 docs/mutation-testing.md §7 收窄单模块时，**必须同时**把
`pytest_add_cli_args_test_selection` 换成上面对应的测试文件（见 #125）。
用 `--snippet <module>` 可直接生成 toml 片段。
EOF
fi
