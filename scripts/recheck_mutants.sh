#!/usr/bin/env bash
# 快速复核 mutmut 标记为 timeout / suspicious 的变异点真实状态。
#
# mutmut 在本仓库存在两类假阳性 timeout（详见 docs/mutation-testing.md）：
#   1) Pool worker 复用 + 过期 deadline 误杀；
#   2) 单变异点 mutmut 进程内 pytest.main() 执行开销（~25s）超过 per-mutant 超时。
# 直接在 mutants/ 副本里用 MUTANT_UNDER_TEST 环境变量激活变异点、命令行跑 pytest，
# 单个 ~1s，可快速判定真实 killed/survived。
#
# 用法：scripts/recheck_mutants.sh [timeout|survived|all]
#
# 本脚本的判定规则是 `rc==0 → survived，否则 → killed`，因此**任何"没能真正跑起来"
# 的退出码都绝不能落进 killed 分支**——那会把复核安全网整个静默关掉，还给出满分。
# 见 jeffkit/plaita#124。
set -u
cd "$(dirname "$0")/.."

WHAT="${1:-timeout}"
case "$WHAT" in
    timeout)   filter="timeout" ;;
    survived)  filter="survived" ;;
    notchecked) filter="not checked" ;;
    all)       filter="" ;;
    *)         filter="$WHAT" ;;
esac

# ---- #124 / 缺陷 2：全角标点紧跟变量名时，bash 3.2(macOS) 会把多字节字符吞进变量名，
# ---- 于是 `${WHAT}` 必须带花括号。同一类写法全仓已扫过（仅此一处，见 #124）。
if [[ -z "$filter" ]]; then
    names=$(mutmut results 2>/dev/null | tr '\r' '\n' \
        | grep -E "^ *plaita\." | sed 's/:[[:space:]]*[a-z ]*$//' | sed 's/^[[:space:]]*//')
else
    names=$(mutmut results 2>/dev/null | tr '\r' '\n' \
        | grep -E ": *${filter}$" | sed "s/:[[:space:]]*${filter}//" | sed 's/^[[:space:]]*//')
fi

if [[ -z "$names" ]]; then
    echo "没有匹配的变异点（filter=${WHAT}）。" >&2; exit 1
fi

# ---- #124 / 缺陷 3：超时原语必须集中解析。macOS 无 GNU `timeout`，也没有 `gtimeout`；
# ---- 裸 `timeout` 会以 rc=127 退出，被下面的判定当成 killed ⇒ **伪造满分**。
# ---- 优先用 `timeout`/`gtimeout`，都没有就退回纯 Python 超时（不静默继续）。
TIMEOUT_BIN=""
if command -v timeout >/dev/null 2>&1; then
    TIMEOUT_BIN="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
    TIMEOUT_BIN="gtimeout"
fi
PYTHON_BIN="${PYTHON_BIN:-python3}"
command -v "$PYTHON_BIN" >/dev/null 2>&1 || {
    echo "找不到 ${PYTHON_BIN}（可用 PYTHON_BIN=... 覆盖）。" >&2; exit 1; }

# 复核时跑的测试集合：必须覆盖 only_mutate 当前目标模块对应的纯同步测试，
# 否则变异点会被误判为 survived。和 pyproject.toml 的
# pytest_add_cli_args_test_selection 保持一致（见 docs/mutation-testing.md）。
# ---- #124 / 缺陷 1：`tests/test_*.py` 两个路径不存在（实际在 tests/unit/ 下）。
# ---- pytest 遇到不存在路径会非零退出 ⇒ 原先会被记成 killed（**方向性错误**）。
TESTS="tests/unit/test_callback.py tests/unit/test_run_forwards_callbacks.py tests/unit/test_child_callback_dedup.py tests/unit/test_expression.py tests/unit/test_expression_golden.py tests/unit/test_parallel_executor.py tests/unit/test_calculate.py tests/unit/test_decide.py"

# ---- #124 / 缺陷 1（加固）：测试集合里任何一个路径不存在都在**开跑前**中止，
# ---- 而不是让 pytest 的 collection error 混进 killed。
missing=""
for t in $TESTS; do
    [[ -e "$t" ]] || missing="${missing} ${t}"
done
if [[ -n "$missing" ]]; then
    echo "复核测试集合里有不存在的路径：${missing}" >&2
    echo "（这会以 collection error 被误判为 killed，故直接中止。）" >&2
    exit 1
fi

cd mutants 2>/dev/null || { echo "mutants/ 不存在，先执行 mutmut run 生成。" >&2; exit 1; }

# ---- #124 缺陷 3（加固）：先对**未变异**的树跑一遍同一测试集合做自检。
# ---- 自检都不过，说明测试环境本身有问题，此时任何 killed/survived 结论都不可信。
echo "自检：未变异树上先跑一遍复核测试集合…" >&2
self_rc=0
if [[ -n "$TIMEOUT_BIN" ]]; then
    "$TIMEOUT_BIN" 300 "$PYTHON_BIN" -m pytest -x -q -p no:randomly $TESTS \
        < /dev/null > /dev/null 2>&1 || self_rc=$?
else
    "$PYTHON_BIN" - "$PYTHON_BIN" $TESTS <<'PYEOF' || self_rc=$?
import subprocess, sys
raise SystemExit(subprocess.run([sys.argv[1], "-m", "pytest", "-x", "-q",
                                 "-p", "no:randomly", *sys.argv[2:]],
                                stdin=subprocess.DEVNULL,
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                timeout=300).returncode)
PYEOF
fi
if [[ $self_rc -ne 0 ]]; then
    echo "自检失败（rc=${self_rc}）：复核测试集合在未变异树上就不通过。" >&2
    echo "这通常意味着测试集合过时或有环境问题，**本轮结论不可信**，已中止。" >&2
    exit 1
fi

killed=0; survived=0; errors=0; total=0
: > ../mutation-recheck.txt
while IFS= read -r m; do
    total=$((total+1))
    rc=0
    if [[ -n "$TIMEOUT_BIN" ]]; then
        out=$(MUTANT_UNDER_TEST="$m" "$TIMEOUT_BIN" 30 "$PYTHON_BIN" -m pytest -x -q \
              -p no:randomly $TESTS < /dev/null 2>&1) || rc=$?
    else
        out=$(MUTANT_UNDER_TEST="$m" "$PYTHON_BIN" - "$PYTHON_BIN" $TESTS <<'PYEOF' 2>&1 || rc=$?
import subprocess, sys
try:
    raise SystemExit(subprocess.run([sys.argv[1], "-m", "pytest", "-x", "-q",
                                     "-p", "no:randomly", *sys.argv[2:]],
                                    stdin=subprocess.DEVNULL,
                                    timeout=30,
                                    capture_output=False).returncode)
except subprocess.TimeoutExpired:
    raise SystemExit(124)
PYEOF
        )
    fi
    # ---- #124 / 缺陷 3：把"没能真正执行"与"测试跑失败"区分开。
    # 124 = 超时；126 = 不可执行；127 = 命令不存在；这些都不是 killed。
    case "$rc" in
        0)      status="survived"; survived=$((survived+1)) ;;
        124|126|127)
                status="error"
                errors=$((errors+1))
                echo "  ! 变异点 ${m} 未能真正执行（rc=${rc}），不计入 killed" >&2 ;;
        *)      status="killed"; killed=$((killed+1)) ;;
    esac
    printf '%-78s %s\n' "$m" "$status" | tee -a ../mutation-recheck.txt
done <<< "$names"

echo "------------------------------------------" | tee -a ../mutation-recheck.txt
echo "复核: $total  killed=$killed  survived=$survived  error=$errors" \
    | tee -a ../mutation-recheck.txt
if [[ $((killed+survived)) -gt 0 ]]; then
    "$PYTHON_BIN" -c "print(f'Mutation score (killed/(killed+survived)) = {$killed/($killed+$survived)*100:.1f}%')" \
        | tee -a ../mutation-recheck.txt
fi
# ---- #124：有 error 就非零退出，绝不把"跑不起来"当成满分收尾。
if [[ $errors -gt 0 ]]; then
    echo "有 ${errors} 个变异点未能真正执行，本轮结论不完整（score 仅供参考）。" >&2
    exit 1
fi
