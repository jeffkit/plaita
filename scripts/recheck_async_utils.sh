#!/usr/bin/env bash
# Recheck mutmut results for plaita/core/async_utils.py
# Must run from WORKTREE_PATH; uses mutants/ dir with tests/... paths (no ../tests)
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

TESTS="tests/unit/test_async_utils.py"
cd mutants 2>/dev/null || { echo "mutants/ 不存在，先执行 mutmut run 生成。" >&2; exit 1; }

killed=0; survived=0; total=0
: > ../mutation-recheck-async-utils.txt
while IFS= read -r m; do
    total=$((total+1))
    out=$(MUTANT_UNDER_TEST="$m" timeout 30 python -m pytest -x -q -p no:randomly \
          $TESTS 2>&1)
    rc=$?
    if [[ $rc -eq 0 ]]; then
        status="survived"; survived=$((survived+1))
    else
        status="killed"; killed=$((killed+1))
    fi
    printf '%-90s %s\n' "$m" "$status" | tee -a ../mutation-recheck-async-utils.txt
done <<< "$names"

echo "------------------------------------------" | tee -a ../mutation-recheck-async-utils.txt
echo "复核: $total  killed=$killed  survived=$survived" | tee -a ../mutation-recheck-async-utils.txt
if [[ $((killed+survived)) -gt 0 ]]; then
    python3 -c "print(f'Mutation score (recheck) = {$killed/($killed+$survived)*100:.1f}%')" | tee -a ../mutation-recheck-async-utils.txt
fi
