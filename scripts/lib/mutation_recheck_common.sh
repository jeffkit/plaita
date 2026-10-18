#!/usr/bin/env bash
# 变异复核脚本的公共部分 —— 由各 recheck_*.sh `source` 引入。
#
# 存在的理由（jeffkit/plaita#124）：
#   * 缺陷 2：`$VAR` 紧跟全角标点时，bash 3.2(macOS) 会把多字节字符吞进变量名，
#     触发 `unbound variable`。凡在中文标点前引用变量，必须写 `${VAR}`。
#   * 缺陷 3：macOS 没有 GNU `timeout`，裸 `timeout` 以 rc=127 退出；而复核脚本的
#     判定是 `rc==0 → survived，否则 → killed`，于是**每个变异点都被记成 killed，
#     分数被伪造成 100%**，且没有任何报错。这里统一提供超时原语与退出码分类。
#
# 用法：
#   source "$(dirname "$0")/lib/mutation_recheck_common.sh"
#   mut_init_timeout                       # 解析 timeout/gtimeout，或启用 Python 回退
#   mut_no_match_exit "$WHAT"              # 无匹配变异点时打印消息并 exit 1（修缺陷 2）
#   mut_require_tests "$TESTS"             # 测试路径不存在则在开跑前中止（修缺陷 1）
#   mut_timeout 600 mutmut run            # 任意命令的超时原语（#167 的共用面）
#   mut_run "$m" 30 $TESTS ; rc=$?         # 跑一个变异点
#   mut_classify $rc                       # 回显 survived | killed | error
#
# shellcheck shell=bash

MUT_TIMEOUT_BIN=""
# 解释器（#167）：由文件末尾的 mut_resolve_python 解析填充——**不**在这里写死
# 裸 ``python3``（那正是 4 条测试假红的根因，见该函数上方的说明）。
MUT_PYTHON_BIN=""

# ---- Python 解释器解析（plaita#167）----------------------------------------- #
#
# 存在的理由：复核/配置脚本此前**裸调用 PATH 上的 `python3`**。macOS 的系统
# `python3` 是 3.9.x——没有 `tomllib`（3.11+ 才有）、也常常没有 pytest，于是
# 「健康机器」上 4 条测试稳定假红：与变异无关的长期红会让「基线不绿」的判断
# 失真，而按「已知环境红灯」人工豁免正是**可伪造通道**（#167 的口径）。
#
# 解析顺序（两条脚本共用，且只写这一处）：
#
#   1. ``PYTHON_BIN``——显式覆盖。工作流/runner 把**当前解释器**递进来
#      （pytest 侧的 ``sys.executable``，见 ``tests/conftest.py``）；
#   2. ``${VIRTUAL_ENV}/bin/python``——已激活的 venv（本地正常工作流）；
#   3. ``<repo>/.venv/bin/python``——仓根 venv，``scripts/ci-gate.sh`` 的既有约定；
#   4. PATH 兜底：``python3`` / ``python`` / ``python3.13`` … 取**第一个 ≥3.11**
#      的（版本化名字是正常安装产物：系统 3.9 与 3.11+ 常常并存）；
#   5. 连一个 ≥3.11 都没有：退回裸 ``python3``——**不是**静默失败，脚本侧会
#      在 Python 里做**具名跳过**（打印缺什么、怎么修），而不是丢一个 traceback。
mut_python_ok() {
    # $1 = 解释器路径/名字：存在且 >= 3.11 ⇒ 0
    command -v "$1" >/dev/null 2>&1 || return 1
    "$1" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' \
        >/dev/null 2>&1
}

mut_repo_root() {
    # 本文件住在 <repo>/scripts/lib/ ⇒ 仓根是上两级
    (cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
}

mut_resolve_python() {
    local cand
    for cand in "${PYTHON_BIN:-}" \
                "${VIRTUAL_ENV:+$VIRTUAL_ENV/bin/python}" \
                "$(mut_repo_root)/.venv/bin/python"; do
        [[ -n "$cand" ]] || continue
        if command -v "$cand" >/dev/null 2>&1; then
            printf '%s\n' "$cand"; return 0
        fi
    done
    for cand in python3 python python3.13 python3.12 python3.11; do
        if mut_python_ok "$cand"; then
            printf '%s\n' "$cand"; return 0
        fi
    done
    for cand in python3 python; do
        if command -v "$cand" >/dev/null 2>&1; then
            printf '%s\n' "$cand"; return 0
        fi
    done
    printf '%s\n' python3
}

mut_init_timeout() {
    if command -v timeout >/dev/null 2>&1; then
        MUT_TIMEOUT_BIN="timeout"
    elif command -v gtimeout >/dev/null 2>&1; then
        MUT_TIMEOUT_BIN="gtimeout"
    else
        MUT_TIMEOUT_BIN=""
        echo "[info] 本机无 timeout/gtimeout（macOS 默认），使用纯 Python 超时回退。" >&2
    fi
    command -v "$MUT_PYTHON_BIN" >/dev/null 2>&1 || {
        echo "找不到 ${MUT_PYTHON_BIN}（可用 PYTHON_BIN=... 覆盖）。" >&2; exit 1; }
    # 解释器还必须能 `import pytest`（#167）：解析到一个没有 pytest 的解释器时，
    # mut_run 的 rc 只是「没能真正执行」，而复核判定把 rc != 0 记成 killed ⇒
    # **伪造击杀**（与 #124 缺陷 3 同一形态，也是本单 4 条假红的另一半）。具名
    # 中止：说清缺什么、怎么修，绝不让它退化成静默的 killed。
    "$MUT_PYTHON_BIN" -c 'import pytest' >/dev/null 2>&1 || {
        echo "解释器 ${MUT_PYTHON_BIN} 里没有 pytest（'python -m pytest' 跑不起来）——" >&2
        echo "继续跑会把「没能执行」记成 killed（伪造击杀）。请设" >&2
        echo "PYTHON_BIN=<装了 pytest 的解释器> 或激活 venv 后重跑。" >&2
        exit 1; }
}

# 无匹配变异点：注意 ${VAR} 的花括号（缺陷 2）
mut_no_match_exit() {
    echo "没有匹配的变异点（filter=${1}）。" >&2
    exit 1
}

# 开跑前确认测试集合里的路径都存在，避免 pytest collection error 被记成 killed（缺陷 1）
mut_require_tests() {
    local missing="" t
    for t in $1; do
        [[ -e "$t" ]] || missing="${missing} ${t}"
    done
    if [[ -n "$missing" ]]; then
        echo "复核测试集合里有不存在的路径：${missing}" >&2
        echo "（这会以 collection error 被误判为 killed，故直接中止。）" >&2
        exit 1
    fi
}

# 通用超时执行：$1=超时秒数，其余=命令。退出码保持命令原值；124=超时。
# 所有 runner 辅助脚本（mut_run / recheck_mutants 自检 / baseline 的 mutmut run）
# 共用这一处：没有 timeout/gtimeout 时用**解析出来的**解释器做纯 Python 回退，
# 不再各自复制一份（#124 的裸 timeout 会以 127 伪造 killed；#167 要求同一解释器）。
mut_timeout() {
    local tmo="$1"; shift
    if [[ -n "$MUT_TIMEOUT_BIN" ]]; then
        "$MUT_TIMEOUT_BIN" "$tmo" "$@"
        return $?
    fi
    "$MUT_PYTHON_BIN" - "$tmo" "$@" <<'PYEOF'
import subprocess, sys
tmo, args = float(sys.argv[1]), sys.argv[2:]
try:
    raise SystemExit(subprocess.run(
        args, stdin=subprocess.DEVNULL, timeout=tmo).returncode)
except subprocess.TimeoutExpired:
    raise SystemExit(124)
PYEOF
}

# 跑一个变异点：$1=变异名 $2=超时秒数 $3...=pytest 参数
# 退出码：0=测试通过（可能 survived）；124=超时；其余=测试失败；127 不会出现。
mut_run() {
    local m="$1" tmo="$2"; shift 2
    # 赋值前缀会随函数调用导出给其子进程（pytest 需要看到 MUTANT_UNDER_TEST）。
    MUTANT_UNDER_TEST="$m" mut_timeout "$tmo" \
        "$MUT_PYTHON_BIN" -m pytest -x -q -p no:randomly "$@" \
        < /dev/null 2>&1
}

# 把退出码分类：0 → survived；124/126/127 → error（**绝不**算 killed）；其余 → killed
mut_classify() {
    case "$1" in
        0)            echo "survived" ;;
        124|126|127)  echo "error" ;;
        *)            echo "killed" ;;
    esac
}

# 解释器解析**在函数定义之后**求值（source 期一次）：所有消费者（mut_run /
# 各 recheck_*.sh / mutation-module-tests.sh）拿到的是同一个解释器——#167 的
# 「两条脚本用同一个解释器」由这一处保证，而不是各写一遍顺序。
MUT_PYTHON_BIN="$(mut_resolve_python)"
