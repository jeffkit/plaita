#!/usr/bin/env bash
# run_unboosted_mutation_sweep.sh — 只跑阶段二仍未补强的 11 个模块
# 复用 run_full_mutation_sweep.sh 的 --from-idx/--to-idx 单模块入口。
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MAIN_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$MAIN_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="$MAIN_DIR/mutation-unboosted-${TIMESTAMP}.txt"
: > "$RESULTS_FILE"
echo "=== Plaita Unboosted Mutation Sweep $(date) ===" | tee -a "$RESULTS_FILE"
echo "PYENV_VERSION=${PYENV_VERSION:-unset}" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

# (idx, module) 平行数组 —— 取自 run_full_mutation_sweep.sh 的 MODULES
INDICES=(1 2 3 4 5 9 10 13 14 15 16)
MODULES=(
  "plaita/core/strategies.py"
  "plaita/core/state.py"
  "plaita/core/errors.py"
  "plaita/core/flow.py"
  "plaita/event/core.py"
  "plaita/event/memory.py"
  "plaita/io.py"
  "plaita/node/__init__.py"
  "plaita/node/event_node.py"
  "plaita/storage/memory.py"
  "plaita/storage/base.py"
)

GLOBAL_SURVIVED=0
SWEEP_START=$(date +%s)

for n in "${!INDICES[@]}"; do
  idx="${INDICES[$n]}"
  module="${MODULES[$n]}"
  echo "" | tee -a "$RESULTS_FILE"
  echo "━━━ [$((n+1))/${#INDICES[@]}] idx=$idx $module ━━━" | tee -a "$RESULTS_FILE"

  # 单模块跑：复用主脚本的全部逻辑（mutmut 初筛 + recheck + 结果行）
  bash "$SCRIPT_DIR/run_full_mutation_sweep.sh" --from-idx "$idx" --to-idx "$idx" \
      2>&1 | tee -a "$RESULTS_FILE"

  # 从该模块输出里抓最后一条结果行（status | module | score ...）
  line=$(grep -E "^(100%|NEEDS_FIX|SKIP|ZERO_MUTANTS) \| $module \|" "$RESULTS_FILE" | tail -1 || true)
  echo "  → $line" | tee -a "$RESULTS_FILE"
done

SWEEP_END=$(date +%s)
echo "" | tee -a "$RESULTS_FILE"
echo "=== SWEEP DONE ===" | tee -a "$RESULTS_FILE"
echo "Total elapsed: $((SWEEP_END - SWEEP_START))s" | tee -a "$RESULTS_FILE"
echo ""
echo "Result lines:"
grep -E "^(100%|NEEDS_FIX|SKIP|ZERO_MUTANTS) \|" "$RESULTS_FILE" || true
echo ""
echo "Full log: $RESULTS_FILE"
