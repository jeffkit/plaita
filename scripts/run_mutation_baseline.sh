#!/usr/bin/env bash
# 运行变异测试基线：逐个变异点在独立进程中执行，规避 mutmut Pool worker
# 长时间复用导致的挂起问题（见 docs/mutation-testing.md）。
#
# 用法：
#   scripts/run_mutation_baseline.sh            # 跑 only_mutate 范围内全部变异点
#   scripts/run_mutation_baseline.sh --recheck  # 仅重跑当前 not checked 的变异点
#
# 产出：终端打印每个变异点状态，结尾汇总 mutation score，并把结果写入
#       mutation-baseline.txt（根目录）。
set -u
cd "$(dirname "$0")/.."

PER_MUTANT_TIMEOUT=60   # 单变异点墙钟上限（秒），防止个别变异点卡死

if [[ "${1:-}" == "--recheck" ]]; then
    MUTANTS=()
    while IFS= read -r line; do
        MUTANTS+=("$line")
    done < <(mutmut results 2>/dev/null | tr '\r' '\n' \
        | grep "not checked" | sed 's/:[[:space:]]*not checked//' | sed 's/^[[:space:]]*//')
else
    MUTANTS=()
    while IFS= read -r line; do
        MUTANTS+=("$line")
    done < <(mutmut results 2>/dev/null | tr '\r' '\n' \
        | grep -E "^ *plaita\." | sed 's/:[[:space:]]*[a-z ]*$//' | sed 's/^[[:space:]]*//')
fi

if [[ ${#MUTANTS[@]} -eq 0 ]]; then
    echo "没有需要运行的变异点。先执行 'mutmut run' 生成变异点。" >&2
    exit 1
fi

echo "共 ${#MUTANTS[@]} 个变异点，逐个执行（每个上限 ${PER_MUTANT_TIMEOUT}s）..."
killed=0; survived=0; timeout=0; other=0
: > mutation-baseline.txt

for m in "${MUTANTS[@]}"; do
    out=$(timeout "${PER_MUTANT_TIMEOUT}" mutmut run "$m" 2>&1 | tr '\r' '\n' \
          | grep -E "^🎉|^🙁|^⏰|^🤔|^🔇|^🧙|^🫥" | head -1)
    case "$out" in
        🎉*) status="killed";    killed=$((killed+1)) ;;
        🙁*) status="survived";  survived=$((survived+1)) ;;
        ⏰*) status="timeout";   timeout=$((timeout+1)) ;;
        *)   status="other";    other=$((other+1)) ;;
    esac
    printf '%-80s %s\n' "$m" "$status" | tee -a mutation-baseline.txt
done

total=${#MUTANTS[@]}
decidable=$((killed + survived))
if [[ $decidable -gt 0 ]]; then
    score=$(python3 -c "print(f'{$killed/$decidable*100:.1f}')")
else
    score="n/a"
fi
echo "------------------------------------------" | tee -a mutation-baseline.txt
echo "总计: $total  killed=$killed  survived=$survived  timeout=$timeout  other=$other" | tee -a mutation-baseline.txt
echo "Mutation score (killed/(killed+survived)) = $score%" | tee -a mutation-baseline.txt
