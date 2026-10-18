#!/usr/bin/env bash
# sexpr 专用复核脚本：对 survived + timeout 变异点用独立进程复核真实状态。
# 用法: bash scripts/recheck_sexpr.sh [survived|timeout|all]
set -u
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib/mutation_recheck_common.sh
source scripts/lib/mutation_recheck_common.sh
mut_init_timeout

WHAT="${1:-all}"
case "$WHAT" in
    timeout)   filter="timeout" ;;
    survived)  filter="survived" ;;
    all)       filter="" ;;
    *)         filter="$WHAT" ;;
esac

if [[ -z "$filter" ]]; then
    names=$(mutmut results 2>/dev/null | tr '\r' '\n' \
        | grep -E "^ *plaita\." | sed 's/:[[:space:]]*[a-z ]*$//' | sed 's/^[[:space:]]*//')
else
    names=$(mutmut results 2>/dev/null | tr '\r' '\n' \
        | grep -E ": *${filter}$" | sed "s/:[[:space:]]*${filter}//" | sed 's/^[[:space:]]*//')
fi

if [[ -z "$names" ]]; then
    mut_no_match_exit "${WHAT}"
fi

TESTS="tests/unit/test_sexpr.py tests/unit/test_sexpr_extended.py tests/unit/test_sexpr_coverage3.py tests/unit/test_sexpr_coverage4.py tests/unit/test_sexpr_mutations.py"
mut_require_tests "$TESTS"
cd mutants 2>/dev/null || { echo "mutants/ 不存在，先执行 mutmut run 生成。" >&2; exit 1; }

killed=0; survived=0; errors=0; total=0
: > ../sexpr-recheck.txt
while IFS= read -r m; do
    [[ -z "$m" ]] && continue
    total=$((total+1))
    rc=0
    mut_run "$m" 30 $TESTS || rc=$?
    status=$(mut_classify "$rc")
    case "$status" in
        survived) survived=$((survived+1)) ;;
        error)    errors=$((errors+1));
                  echo "  ! 变异点 ${m} 未真正执行（rc=${rc}），不计入 killed" >&2 ;;
        *)        killed=$((killed+1)) ;;
    esac
    printf '%-90s %s\n' "$m" "$status" | tee -a ../sexpr-recheck.txt
    # 每50个打印进度
    if (( total % 50 == 0 )); then
        echo ">>> Progress: $total processed, killed=$killed, survived=$survived" | tee -a ../sexpr-recheck.txt
    fi
done <<< "$names"

echo "------------------------------------------" | tee -a ../sexpr-recheck.txt
echo "复核: $total  killed=$killed  survived=$survived  error=$errors" | tee -a ../sexpr-recheck.txt
if [[ $((killed+survived)) -gt 0 ]]; then
    "$MUT_PYTHON_BIN" -c "print(f'Recheck score (killed/(killed+survived)) = {$killed/($killed+$survived)*100:.1f}%')" | tee -a ../sexpr-recheck.txt
fi
# #124：有 error 就非零退出，绝不把"跑不起来"当成满分收尾。
if [[ $errors -gt 0 ]]; then
    echo "有 ${errors} 个变异点未能真正执行，本轮结论不完整（score 仅供参考）。" >&2
    exit 1
fi
