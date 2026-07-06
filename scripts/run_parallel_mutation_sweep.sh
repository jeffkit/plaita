#!/usr/bin/env bash
# run_parallel_mutation_sweep.sh — 使用 git worktree 并发跑变异测试
#
# 原理：
#   把 17 个模块平均分成 N 批（默认 4），每批在独立 git worktree 内跑
#   run_full_mutation_sweep.sh --from-idx X --to-idx Y，互不干扰。
#   4 个 worker 并行 → 理论 ~4x 加速（3h → ~45min）。
#
# 用法:
#   bash scripts/run_parallel_mutation_sweep.sh           # 默认 4 worker
#   bash scripts/run_parallel_mutation_sweep.sh --workers 2
#   bash scripts/run_parallel_mutation_sweep.sh --workers 4 --keep-worktrees
#
# 前提: pyenv 环境 loki 已安装，mutmut 可用，磁盘 ≥ 2GB 空闲（每个 worktree ~100MB）
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MAIN_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

N_WORKERS=4
KEEP_WORKTREES=0
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="${MAIN_DIR}/parallel-sweep-${TIMESTAMP}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workers)       N_WORKERS="$2"; shift 2 ;;
    --workers=*)     N_WORKERS="${1#*=}"; shift ;;
    --keep-worktrees) KEEP_WORKTREES=1; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

TOTAL_MODULES=17

echo "=== Parallel Mutation Sweep ==="
echo "Workers:    $N_WORKERS"
echo "Modules:    $TOTAL_MODULES"
echo "Results:    $RESULTS_DIR"
echo "Started:    $(date)"
echo ""

mkdir -p "$RESULTS_DIR"

# ── 确保当前有 .coverage 文件（供 mutate_only_covered_lines 使用）────────
if [[ ! -f "$MAIN_DIR/.coverage" ]]; then
  echo "[setup] No .coverage found — running quick coverage pass..."
  cd "$MAIN_DIR"
  PYENV_VERSION=loki python -m pytest tests/ -m "not integration and not e2e" \
      --ignore=tests/e2e -q --no-header --cov=plaita --cov-report= 2>&1 | tail -3
  echo "[setup] .coverage generated."
fi

# ── 计算每个 worker 的模块索引范围 ─────────────────────────────────────────
# 分配策略: 尽量均匀，余数分配给最后几个 worker
BATCH_START=()
BATCH_END=()
base=$(( TOTAL_MODULES / N_WORKERS ))
rem=$(( TOTAL_MODULES % N_WORKERS ))

cur=0
for (( w=0; w<N_WORKERS; w++ )); do
  extra=$(( w < rem ? 1 : 0 ))
  size=$(( base + extra ))
  BATCH_START[$w]=$cur
  BATCH_END[$w]=$(( cur + size - 1 ))
  cur=$(( cur + size ))
done

echo "Module batches:"
for (( w=0; w<N_WORKERS; w++ )); do
  echo "  Worker $w: indices ${BATCH_START[$w]}-${BATCH_END[$w]}"
done
echo ""

# ── 创建 git worktrees ─────────────────────────────────────────────────────
WORKTREE_DIRS=()
for (( w=0; w<N_WORKERS; w++ )); do
  wt_dir="${MAIN_DIR}/../pyloki-mutation-worker-${w}"
  WORKTREE_DIRS[$w]="$wt_dir"

  # 如果 worktree 已存在先清理
  if [[ -d "$wt_dir" ]]; then
    echo "[worker $w] Removing existing worktree at $wt_dir"
    git -C "$MAIN_DIR" worktree remove --force "$wt_dir" 2>/dev/null || rm -rf "$wt_dir"
  fi

  echo "[worker $w] Creating worktree: $wt_dir"
  git -C "$MAIN_DIR" worktree add "$wt_dir" HEAD 2>&1
done

# worktree 不共享未跟踪文件；把 .coverage 复制到每个 worktree
for (( w=0; w<N_WORKERS; w++ )); do
  cp -f "$MAIN_DIR/.coverage" "${WORKTREE_DIRS[$w]}/.coverage" 2>/dev/null || true
done

echo ""

# ── 启动 worker 进程 ───────────────────────────────────────────────────────
WORKER_PIDS=()
WORKER_LOGS=()

for (( w=0; w<N_WORKERS; w++ )); do
  wt_dir="${WORKTREE_DIRS[$w]}"
  log_file="${RESULTS_DIR}/worker-${w}.log"
  result_file="${RESULTS_DIR}/worker-${w}-results.txt"
  WORKER_LOGS[$w]="$log_file"

  echo "[worker $w] Starting (indices ${BATCH_START[$w]}-${BATCH_END[$w]}) → $log_file"

  (
    cd "$wt_dir"
    RESULTS_FILE="${wt_dir}/mutation-sweep-results.txt" \
    PYENV_VERSION=loki \
      bash scripts/run_full_mutation_sweep.sh \
        --from-idx "${BATCH_START[$w]}" \
        --to-idx   "${BATCH_END[$w]}"
  ) > "$log_file" 2>&1 &

  WORKER_PIDS[$w]=$!
done

echo ""
echo "All $N_WORKERS workers launched. Waiting..."
echo "Tail logs in: $RESULTS_DIR/worker-N.log"
echo ""

# ── 监控进度 ─────────────────────────────────────────────────────────────────
while true; do
  running=0
  for (( w=0; w<N_WORKERS; w++ )); do
    pid=${WORKER_PIDS[$w]}
    if kill -0 "$pid" 2>/dev/null; then
      running=$(( running + 1 ))
    fi
  done

  [[ $running -eq 0 ]] && break

  echo "[$(date +%H:%M:%S)] $running/$N_WORKERS workers still running..."
  sleep 60
done

echo ""
echo "=== All workers completed ==="
echo ""

# ── 收集并合并结果 ─────────────────────────────────────────────────────────
MERGED_FILE="${RESULTS_DIR}/merged-results.txt"
echo "=== Plaita Parallel Mutation Sweep ${TIMESTAMP} ===" > "$MERGED_FILE"
echo "Workers: $N_WORKERS | Modules: $TOTAL_MODULES" >> "$MERGED_FILE"
echo "" >> "$MERGED_FILE"

GLOBAL_SURVIVED=0
TOTAL_SCORE_MODULES=0
SUCCESS_MODULES=()
FAIL_MODULES=()

for (( w=0; w<N_WORKERS; w++ )); do
  wt_dir="${WORKTREE_DIRS[$w]}"
  # 优先从 worktree 内 mutation-sweep-results.txt 读取（RESULTS_FILE 变量传入方式）
  result_file="${wt_dir}/mutation-sweep-results.txt"
  if [[ -f "$result_file" ]]; then
    cp "$result_file" "${RESULTS_DIR}/worker-${w}-results.txt"
  fi
  result_file="${RESULTS_DIR}/worker-${w}-results.txt"

  if [[ -f "$result_file" ]]; then
    echo "--- Worker $w (indices ${BATCH_START[$w]}-${BATCH_END[$w]}) ---" >> "$MERGED_FILE"
    cat "$result_file" >> "$MERGED_FILE"
    echo "" >> "$MERGED_FILE"

    # 统计全局数字
    while IFS='|' read -r status module score rest; do
      status="${status// /}"
      case "$status" in
        100%) SUCCESS_MODULES+=("$module"); TOTAL_SCORE_MODULES=$(( TOTAL_SCORE_MODULES + 1 )) ;;
        NEEDS_FIX*) FAIL_MODULES+=("$module | $status"); TOTAL_SCORE_MODULES=$(( TOTAL_SCORE_MODULES + 1 ))
          n=$(echo "$status" | grep -o '[0-9]\+' | head -1)
          GLOBAL_SURVIVED=$(( GLOBAL_SURVIVED + ${n:-0} )) ;;
      esac
    done < "$result_file"
  else
    echo "--- Worker $w: NO RESULT FILE ---" >> "$MERGED_FILE"
  fi
done

echo "" >> "$MERGED_FILE"
echo "=== SWEEP SUMMARY ===" >> "$MERGED_FILE"
echo "Total survived: $GLOBAL_SURVIVED" >> "$MERGED_FILE"
echo "Modules at 100%: ${#SUCCESS_MODULES[@]} / $TOTAL_SCORE_MODULES" >> "$MERGED_FILE"
echo "" >> "$MERGED_FILE"
for m in "${SUCCESS_MODULES[@]}"; do echo "  ✓ $m" >> "$MERGED_FILE"; done
for m in "${FAIL_MODULES[@]}";    do echo "  ✗ $m" >> "$MERGED_FILE"; done

cat "$MERGED_FILE"

echo ""
echo "Full results: $MERGED_FILE"
echo "Worker logs:  $RESULTS_DIR/worker-N.log"
echo "Finished:     $(date)"

# ── 清理 worktrees ──────────────────────────────────────────────────────────
if [[ $KEEP_WORKTREES -eq 0 ]]; then
  echo ""
  echo "Cleaning up worktrees..."
  for (( w=0; w<N_WORKERS; w++ )); do
    wt_dir="${WORKTREE_DIRS[$w]}"
    echo "  Removing: $wt_dir"
    git -C "$MAIN_DIR" worktree remove --force "$wt_dir" 2>/dev/null || rm -rf "$wt_dir"
  done
  git -C "$MAIN_DIR" worktree prune
  echo "Cleanup done."
fi
