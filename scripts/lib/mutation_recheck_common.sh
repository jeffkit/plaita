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
#   mut_run "$m" 30 $TESTS ; rc=$?         # 跑一个变异点
#   mut_classify $rc                       # 回显 survived | killed | error
#
# shellcheck shell=bash

MUT_TIMEOUT_BIN=""
MUT_PYTHON_BIN="${PYTHON_BIN:-python3}"

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

# 跑一个变异点：$1=变异名 $2=超时秒数 $3...=pytest 参数
# 退出码：0=测试通过（可能 survived）；124=超时；其余=测试失败；127 不会出现。
mut_run() {
    local m="$1" tmo="$2"; shift 2
    if [[ -n "$MUT_TIMEOUT_BIN" ]]; then
        MUTANT_UNDER_TEST="$m" "$MUT_TIMEOUT_BIN" "$tmo" \
            "$MUT_PYTHON_BIN" -m pytest -x -q -p no:randomly "$@" \
            < /dev/null 2>&1
        return $?
    fi
    MUTANT_UNDER_TEST="$m" "$MUT_PYTHON_BIN" - "$MUT_PYTHON_BIN" "$tmo" "$@" <<'PYEOF'
import subprocess, sys
py, tmo, args = sys.argv[1], float(sys.argv[2]), sys.argv[3:]
try:
    raise SystemExit(subprocess.run(
        [py, "-m", "pytest", "-x", "-q", "-p", "no:randomly", *args],
        stdin=subprocess.DEVNULL, timeout=tmo).returncode)
except subprocess.TimeoutExpired:
    raise SystemExit(124)
PYEOF
}

# 把退出码分类：0 → survived；124/126/127 → error（**绝不**算 killed）；其余 → killed
mut_classify() {
    case "$1" in
        0)            echo "survived" ;;
        124|126|127)  echo "error" ;;
        *)            echo "killed" ;;
    esac
}
