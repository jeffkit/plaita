#!/usr/bin/env bash
# sexpr 专用复核脚本：对 survived + timeout 变异点用独立进程复核真实状态。
# 用法: bash scripts/recheck_sexpr.sh [survived|timeout|all]
set -u
cd "$(dirname "$0")/.."

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
    echo "没有匹配的变异点（filter=$WHAT）。" >&2; exit 1
fi

TESTS="tests/unit/test_sexpr.py tests/unit/test_sexpr_extended.py tests/unit/test_sexpr_coverage3.py tests/unit/test_sexpr_coverage4.py tests/unit/test_sexpr_mutations.py"
cd mutants 2>/dev/null || { echo "mutants/ 不存在，先执行 mutmut run 生成。" >&2; exit 1; }

killed=0; survived=0; other=0; total=0
: > ../sexpr-recheck.txt
while IFS= read -r m; do
    [[ -z "$m" ]] && continue
    total=$((total+1))
    out=$(MUTANT_UNDER_TEST="$m" timeout 30 python -m pytest -x -q -p no:randomly \
          $TESTS 2>&1)
    rc=$?
    if [[ $rc -eq 0 ]]; then
        status="survived"; survived=$((survived+1))
    else
        status="killed"; killed=$((killed+1))
    fi
    printf '%-90s %s\n' "$m" "$status" | tee -a ../sexpr-recheck.txt
    # 每50个打印进度
    if (( total % 50 == 0 )); then
        echo ">>> Progress: $total processed, killed=$killed, survived=$survived" | tee -a ../sexpr-recheck.txt
    fi
done <<< "$names"

echo "------------------------------------------" | tee -a ../sexpr-recheck.txt
echo "复核: $total  killed=$killed  survived=$survived" | tee -a ../sexpr-recheck.txt
if [[ $((killed+survived)) -gt 0 ]]; then
    python3 -c "print(f'Recheck score (killed/(killed+survived)) = {$killed/($killed+$survived)*100:.1f}%')" | tee -a ../sexpr-recheck.txt
fi
