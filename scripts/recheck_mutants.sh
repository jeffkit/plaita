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

# 复核时跑的测试集合：必须覆盖 only_mutate 当前目标模块对应的纯同步测试，
# 否则变异点会被误判为 survived。和 pyproject.toml 的
# pytest_add_cli_args_test_selection 保持一致（见 docs/mutation-testing.md）。
TESTS="tests/unit/test_callback.py tests/unit/test_run_forwards_callbacks.py tests/unit/test_child_callback_dedup.py tests/unit/test_expression.py tests/unit/test_expression_golden.py tests/unit/test_parallel_executor.py tests/test_calculate.py tests/test_decide.py"
cd mutants 2>/dev/null || { echo "mutants/ 不存在，先执行 mutmut run 生成。" >&2; exit 1; }

killed=0; survived=0; other=0; total=0
: > ../mutation-recheck.txt
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
    printf '%-78s %s\n' "$m" "$status" | tee -a ../mutation-recheck.txt
done <<< "$names"

echo "------------------------------------------" | tee -a ../mutation-recheck.txt
echo "复核: $total  killed=$killed  survived=$survived" | tee -a ../mutation-recheck.txt
if [[ $((killed+survived)) -gt 0 ]]; then
    python3 -c "print(f'Mutation score (killed/(killed+survived)) = {$killed/($killed+$survived)*100:.1f}%')" | tee -a ../mutation-recheck.txt
fi
