#!/usr/bin/env bash
# codeflow/_stmt.py 专用复核脚本：对 survived + timeout 变异点用独立进程复核。
set -u
cd "$(dirname "$0")/.."

WHAT="${1:-survived}"
case "$WHAT" in
    timeout)   filter="timeout" ;;
    survived)  filter="survived" ;;
    all)       filter="" ;;
    *)         filter="$WHAT" ;;
esac

if [[ -z "$filter" ]]; then
    names=$(mutmut results 2>/dev/null | tr '\r' '\n' \
        | grep -E "^ *plaita\.dsl\.codeflow\._stmt\." | sed 's/:[[:space:]]*[a-z ]*$//' | sed 's/^[[:space:]]*//')
else
    names=$(mutmut results 2>/dev/null | tr '\r' '\n' \
        | grep -E "plaita\.dsl\.codeflow\._stmt\..*: *${filter}$" | sed "s/:[[:space:]]*${filter}//" | sed 's/^[[:space:]]*//')
fi

if [[ -z "$names" ]]; then
    echo "没有匹配的变异点（filter=$WHAT）。" >&2; exit 1
fi

TESTS="tests/unit/test_codeflow.py tests/unit/test_codeflow_coverage3.py tests/unit/test_codeflow_coverage4.py tests/unit/test_codeflow_custom_node.py tests/unit/test_codeflow_extended.py tests/unit/test_codeflow_source.py tests/unit/test_codeflow_source_line.py tests/unit/test_codeflow_mutations.py tests/unit/test_codeflow_nodes_mutations.py tests/unit/test_codeflow_stmt_mutations.py"
cd mutants 2>/dev/null || { echo "mutants/ 不存在，先执行 mutmut run 生成。" >&2; exit 1; }

killed=0; survived=0; total=0
: > ../codeflow-stmt-recheck.txt
while IFS= read -r m; do
    [[ -z "$m" ]] && continue
    total=$((total+1))
    out=$(MUTANT_UNDER_TEST="$m" timeout 45 python -m pytest -x -q -p no:randomly \
          $TESTS 2>&1)
    rc=$?
    if [[ $rc -eq 0 ]]; then
        status="survived"; survived=$((survived+1))
    else
        status="killed"; killed=$((killed+1))
    fi
    printf '%-90s %s\n' "$m" "$status" | tee -a ../codeflow-stmt-recheck.txt
done <<< "$names"

echo "------------------------------------------" | tee -a ../codeflow-stmt-recheck.txt
echo "复核: $total  killed=$killed  survived=$survived" | tee -a ../codeflow-stmt-recheck.txt
if [[ $((killed+survived)) -gt 0 ]]; then
    python3 -c "print(f'Recheck score (killed/(killed+survived)) = {$killed/($killed+$survived)*100:.1f}%')" | tee -a ../codeflow-stmt-recheck.txt
fi
