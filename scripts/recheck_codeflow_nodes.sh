#!/usr/bin/env bash
# codeflow/_nodes.py 专用复核脚本：对 survived + timeout 变异点用独立进程复核。
set -u
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib/mutation_recheck_common.sh
source scripts/lib/mutation_recheck_common.sh
mut_init_timeout

WHAT="${1:-survived}"
case "$WHAT" in
    timeout)   filter="timeout" ;;
    survived)  filter="survived" ;;
    all)       filter="" ;;
    *)         filter="$WHAT" ;;
esac

if [[ -z "$filter" ]]; then
    names=$(mutmut results 2>/dev/null | tr '\r' '\n' \
        | grep -E "^ *plaita\.dsl\.codeflow\._nodes\." | sed 's/:[[:space:]]*[a-z ]*$//' | sed 's/^[[:space:]]*//')
else
    names=$(mutmut results 2>/dev/null | tr '\r' '\n' \
        | grep -E "plaita\.dsl\.codeflow\._nodes\..*: *${filter}$" | sed "s/:[[:space:]]*${filter}//" | sed 's/^[[:space:]]*//')
fi

if [[ -z "$names" ]]; then
    mut_no_match_exit "${WHAT}"
fi

TESTS="tests/unit/test_codeflow.py tests/unit/test_codeflow_coverage3.py tests/unit/test_codeflow_coverage4.py tests/unit/test_codeflow_custom_node.py tests/unit/test_codeflow_extended.py tests/unit/test_codeflow_source.py tests/unit/test_codeflow_source_line.py tests/unit/test_codeflow_mutations.py tests/unit/test_codeflow_nodes_mutations.py"
mut_require_tests "$TESTS"
cd mutants 2>/dev/null || { echo "mutants/ 不存在，先执行 mutmut run 生成。" >&2; exit 1; }

killed=0; survived=0; errors=0; total=0
: > ../codeflow-nodes-recheck.txt
while IFS= read -r m; do
    [[ -z "$m" ]] && continue
    total=$((total+1))
    rc=0
    mut_run "$m" 45 $TESTS || rc=$?
    status=$(mut_classify "$rc")
    case "$status" in
        survived) survived=$((survived+1)) ;;
        error)    errors=$((errors+1));
                  echo "  ! 变异点 ${m} 未真正执行（rc=${rc}），不计入 killed" >&2 ;;
        *)        killed=$((killed+1)) ;;
    esac
    printf '%-90s %s\n' "$m" "$status" | tee -a ../codeflow-nodes-recheck.txt
    if (( total % 20 == 0 )); then
        echo ">>> Progress: $total processed, killed=$killed, survived=$survived" | tee -a ../codeflow-nodes-recheck.txt
    fi
done <<< "$names"

echo "------------------------------------------" | tee -a ../codeflow-nodes-recheck.txt
echo "复核: $total  killed=$killed  survived=$survived  error=$errors" | tee -a ../codeflow-nodes-recheck.txt
if [[ $((killed+survived)) -gt 0 ]]; then
    "$MUT_PYTHON_BIN" -c "print(f'Recheck score (killed/(killed+survived)) = {$killed/($killed+$survived)*100:.1f}%')" | tee -a ../codeflow-nodes-recheck.txt
fi
# #124：有 error 就非零退出，绝不把"跑不起来"当成满分收尾。
if [[ $errors -gt 0 ]]; then
    echo "有 ${errors} 个变异点未能真正执行，本轮结论不完整（score 仅供参考）。" >&2
    exit 1
fi
