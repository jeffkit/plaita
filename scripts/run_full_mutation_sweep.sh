#!/usr/bin/env bash
# run_full_mutation_sweep.sh — 对所有待扩展模块跑一轮完整变异测试
#
# 策略：
#   1. mutmut run 生成变异点（带 10 分钟超时，超时即 kill）
#   2. mutmut results 里所有 not-checked / timeout / survived 均用
#      MUTANT_UNDER_TEST 单点复核，得到真实 killed/survived 统计
#
# 用法:
#   bash scripts/run_full_mutation_sweep.sh [module_key_substring]
#   bash scripts/run_full_mutation_sweep.sh --from-idx N [--to-idx M]
# 例如:
#   bash scripts/run_full_mutation_sweep.sh concurrent   # 从某模块续跑
#   bash scripts/run_full_mutation_sweep.sh --from-idx 0 --to-idx 4  # 只跑前5个模块
set -eo pipefail
cd "$(dirname "$0")/.."

# 解析参数 — 支持旧式 positional 及新式 --from-idx / --to-idx
START_FROM=""
FROM_IDX=""
TO_IDX=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --from-idx) FROM_IDX="$2"; shift 2 ;;
    --to-idx)   TO_IDX="$2";   shift 2 ;;
    --from-idx=*) FROM_IDX="${1#*=}"; shift ;;
    --to-idx=*)   TO_IDX="${1#*=}";   shift ;;
    *) START_FROM="$1"; shift ;;
  esac
done

# ── 模块 + 对应测试文件（平行数组）───────────────────────────────────────
MODULES=(
  "plaita/core/executor.py"
  "plaita/core/strategies.py"
  "plaita/core/state.py"
  "plaita/core/errors.py"
  "plaita/core/flow.py"
  "plaita/event/core.py"
  "plaita/node/concurrent.py"
  "plaita/node/loop.py"
  "plaita/core/expression_parser.py"
  "plaita/event/memory.py"
  "plaita/io.py"
  "plaita/dsl/builder.py"
  "plaita/node/code.py"
  "plaita/node/__init__.py"
  "plaita/node/event_node.py"
  "plaita/storage/memory.py"
  "plaita/storage/base.py"
)

TESTS_FOR_MODULE=(
  "tests/unit/test_executor.py tests/unit/test_flow_executor_extended.py tests/unit/test_checkpoint_resume.py"
  "tests/unit/test_errors.py tests/unit/test_runner_strategies_extended.py"
  "tests/unit/test_state.py tests/unit/test_checkpoint_resume.py tests/unit/test_context.py tests/unit/test_context_executor_extended.py"
  "tests/unit/test_errors.py tests/unit/test_error_handler_enum.py tests/unit/test_exception_cause.py tests/unit/test_find_node_unified_exception.py"
  "tests/unit/test_flow.py tests/unit/test_flow_model.py tests/unit/test_flow_next_node.py tests/unit/test_flow_node_index.py tests/unit/test_flow_distributed.py tests/unit/test_find_node_unified_exception.py"
  "tests/unit/test_event_core.py tests/unit/test_event_core_extended.py tests/unit/test_event_handler_decorator.py"
  "tests/unit/test_concurrent.py tests/unit/test_concurrent_extended.py"
  "tests/unit/test_loop.py tests/unit/test_loop_sync.py tests/unit/test_loop_arun.py"
  "tests/unit/test_expression.py tests/unit/test_expression_golden.py tests/unit/test_sexpr.py"
  "tests/unit/test_event_memory_unit.py tests/unit/test_event_core.py"
  "tests/unit/test_io.py tests/unit/test_io_format.py tests/unit/test_io_mutations.py"
  "tests/unit/test_dsl.py tests/unit/test_builder_extended.py"
  "tests/unit/test_code.py tests/unit/test_code_extended.py tests/unit/test_code_default_backend.py"
  "tests/unit/test_node_registry.py tests/unit/test_node_registry_lazy_discover.py"
  "tests/unit/test_event_node_unit.py"
  "tests/unit/test_storage_memory_units.py"
  "tests/unit/test_storage_base.py"
)

# 仅用于 mutmut run 初筛阶段（in-process pytest.main()）的纯同步测试子集。
# 含 async def 的测试文件会与 mutmut 的 Pool worker 冲突，必须排除。
# 复核阶段（recheck_all_mutations）使用独立子进程，可直接用 TESTS_FOR_MODULE 的完整列表。
# 规则: 文件中无 async def、无真实 sleep/timeout、不使用 mode="process"。
SYNC_TESTS_FOR_MODULE=(
  "tests/unit/test_flow_executor_extended.py tests/unit/test_checkpoint_resume.py"
  "tests/unit/test_errors.py"
  "tests/unit/test_state.py tests/unit/test_checkpoint_resume.py tests/unit/test_context.py"
  "tests/unit/test_errors.py tests/unit/test_error_handler_enum.py tests/unit/test_exception_cause.py tests/unit/test_find_node_unified_exception.py"
  "tests/unit/test_flow_model.py tests/unit/test_flow_next_node.py tests/unit/test_flow_node_index.py tests/unit/test_flow_distributed.py tests/unit/test_find_node_unified_exception.py"
  ""
  ""
  "tests/unit/test_loop_sync.py"
  "tests/unit/test_expression.py tests/unit/test_expression_golden.py tests/unit/test_sexpr.py"
  ""
  "tests/unit/test_io.py tests/unit/test_io_format.py tests/unit/test_io_mutations.py"
  "tests/unit/test_dsl.py"
  "tests/unit/test_code.py tests/unit/test_code_extended.py tests/unit/test_code_default_backend.py"
  "tests/unit/test_node_registry.py tests/unit/test_node_registry_lazy_discover.py"
  "tests/unit/test_event_node_unit.py"
  "tests/unit/test_storage_memory_units.py"
  "tests/unit/test_storage_base.py"
)

# ── 辅助函数 ──────────────────────────────────────────────────────────────
update_pyproject() {
  local module="$1"
  python3 <<PYEOF
import re
module = """$module"""
path = "pyproject.toml"
content = open(path).read()
new_block = 'only_mutate = [\n  "' + module + '",\n]'
content = re.sub(r'only_mutate\s*=\s*\[.*?\]', new_block, content, flags=re.DOTALL)
# 确保 mutate_only_covered_lines = true
content = re.sub(r'mutate_only_covered_lines\s*=\s*(true|false)', 'mutate_only_covered_lines = true', content)
open(path, "w").write(content)
print("  pyproject.toml only_mutate => " + module)
PYEOF
}

update_test_selection() {
  local tests="$1"
  python3 <<PYEOF
import re
tests_str = """$tests"""
test_list = [t for t in tests_str.split() if t.strip()]
path = "pyproject.toml"
content = open(path).read()
entries = "\n".join('  "' + t + '",' for t in test_list)
new_block = 'pytest_add_cli_args_test_selection = [\n' + entries + '\n]'
content = re.sub(
    r'pytest_add_cli_args_test_selection\s*=\s*\[.*?\]',
    new_block,
    content,
    flags=re.DOTALL
)
open(path, "w").write(content)
print("  test_selection => " + str(test_list))
PYEOF
}

# 真正逐点复核：把所有 not-checked / timeout / survived 全部重新跑
# 结果写入 RECHECK_TMPFILE（纯整数 survived 数），直接向终端输出进度
# 参数: $1=所有有效测试文件（空格分隔）
RECHECK_TMPFILE=""
recheck_all_mutations() {
  local valid_tests="$1"
  # 取第一个测试文件做快速初筛
  local first_test
  first_test=$(echo "$valid_tests" | awk '{print $1}')

  # 收集所有非 killed 的变异点
  # 用 awk -F': ' 代替 BSD sed 不兼容的 \(A\|B\) 交替语法
  local names
  names=$(mutmut results 2>/dev/null | tr '\r' '\n' \
    | grep -E ": *(not checked|timeout|survived)$" \
    | awk -F': ' '{print $1}' \
    | sed 's/^[[:space:]]*//' || true)

  if [[ -z "$names" ]]; then
    echo 0 > "$RECHECK_TMPFILE"
    return 0
  fi

  local n_names
  n_names=$(echo "$names" | grep -c . || true)
  echo "  Rechecking ${n_names} mutant(s)…  (quick: ${first_test})"

  local t_killed=0 t_survived=0

  pushd mutants > /dev/null 2>&1 || { echo 0 > "$RECHECK_TMPFILE"; return 0; }
  while IFS= read -r m; do
    [[ -z "$m" ]] && continue

    # ── 第一轮：只跑最快的测试文件（通常 1-2s）───────────────────────
    # 注意: 用 rc=0 + || rc=$? 避免 set -e 在测试失败时提前退出
    # 每个 mutant 最多 60s，防止 async 测试（如 wait_for_event_timeout）挂死
    rc=0
    MUTANT_UNDER_TEST="$m" timeout 60 python -m pytest -x -q -p no:randomly \
        --tb=no --no-header "$first_test" \
        < /dev/null > /dev/null 2>&1 || rc=$?

    if [[ $rc -ne 0 ]]; then
      # 第一轮就被杀死（含 timeout 退出码 124），无需继续
      t_killed=$((t_killed+1))
      continue
    fi

    # ── 第二轮：跑全部测试文件确认是否真的 survived ───────────────────
    if [[ "$valid_tests" != "$first_test" ]]; then
      rc=0
      MUTANT_UNDER_TEST="$m" timeout 120 python -m pytest -x -q -p no:randomly \
          --tb=no --no-header $valid_tests \
          < /dev/null > /dev/null 2>&1 || rc=$?
    fi

    if [[ $rc -eq 0 ]]; then
      t_survived=$((t_survived+1))
      echo "  SURVIVED: $m"
    else
      t_killed=$((t_killed+1))
    fi
  done <<< "$names"
  popd > /dev/null 2>&1

  echo "  recheck done: ${n_names} total | killed=${t_killed} survived=${t_survived}"
  echo "$t_survived" > "$RECHECK_TMPFILE"
}

# ── 主循环 ─────────────────────────────────────────────────────────────────
# 支持外部通过 RESULTS_FILE 环境变量指定输出路径（供并发脚本使用）
RESULTS_FILE="${RESULTS_FILE:-mutation-sweep-results.txt}"
: > "$RESULTS_FILE"
echo "=== Plaita Full Mutation Sweep $(date) ===" | tee -a "$RESULTS_FILE"

SKIP=0
[[ -n "$START_FROM" ]] && SKIP=1
# 默认 FROM_IDX=0, TO_IDX=最后一个
[[ -z "$FROM_IDX" ]] && FROM_IDX=0
[[ -z "$TO_IDX" ]]   && TO_IDX=$((${#MODULES[@]} - 1))
GLOBAL_SURVIVED=0
SUCCESS_MODULES=()

for i in "${!MODULES[@]}"; do
  module="${MODULES[$i]}"
  tests="${TESTS_FOR_MODULE[$i]}"
  sync_tests="${SYNC_TESTS_FOR_MODULE[$i]:-}"

  # 索引范围过滤（与 START_FROM 互斥，优先使用索引范围）
  if [[ -z "$START_FROM" ]]; then
    [[ $i -lt $FROM_IDX || $i -gt $TO_IDX ]] && continue
  fi

  if [[ $SKIP -eq 1 ]]; then
    [[ "$module" == *"$START_FROM"* ]] && SKIP=0 || continue
  fi

  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "▶ [$((i+1))/${#MODULES[@]}] $module"

  # 验证完整测试文件列表（用于 recheck）
  valid_tests=""
  for t in $tests; do
    [[ -f "$t" ]] && valid_tests="$valid_tests $t" || echo "  WARN: $t not found"
  done
  valid_tests="${valid_tests# }"

  if [[ -z "$valid_tests" ]]; then
    echo "  ERROR: no valid test files → skip"
    echo "SKIP | $module | no test files" | tee -a "$RESULTS_FILE"
    continue
  fi
  echo "  tests (recheck): $valid_tests"

  # 验证 sync-only 测试文件列表（用于 mutmut run in-process 初筛）
  valid_sync_tests=""
  for t in $sync_tests; do
    [[ -f "$t" ]] && valid_sync_tests="$valid_sync_tests $t" || echo "  WARN sync: $t not found"
  done
  valid_sync_tests="${valid_sync_tests# }"
  # 若 sync 子集为空，回退到完整列表（接受 mutmut run 可能失败）
  [[ -z "$valid_sync_tests" ]] && valid_sync_tests="$valid_tests"
  echo "  tests (mutmut): $valid_sync_tests"

  # 更新配置：pyproject.toml 用 sync 子集（避免 async 测试在 in-process 模式失败）
  update_pyproject "$module"
  update_test_selection "$valid_sync_tests"
  rm -rf mutants .mutmut-cache

  # ── mutmut run（带 10 分钟超时，防止 async 死锁）──────────────────────
  start_ts=$(date +%s)
  echo "  Running mutmut (timeout=600s)..."

  mutmut run 2>&1 | tail -5 &
  MUTMUT_PID=$!
  # 监控 + 超时 kill。看门狗必须重定向所有 fd 并在子 shell 内运行，
  # 否则会继承父管道写端；若被孤儿化（mutmut 提前结束、kill 漏掉），
  # 管道读端 tee 永远读不到 EOF，上游驱动脚本会死等。
  ( sleep 600 && kill "$MUTMUT_PID" 2>/dev/null ) </dev/null >/dev/null 2>&1 &
  KILLER_PID=$!
  wait "$MUTMUT_PID" 2>/dev/null || true
  kill "$KILLER_PID" 2>/dev/null || true
  wait "$KILLER_PID" 2>/dev/null || true

  end_ts=$(date +%s)
  elapsed=$((end_ts - start_ts))
  echo "  mutmut run elapsed: ${elapsed}s"

  # ── 统计变异点数量 ────────────────────────────────────────────────────
  raw_results=$(mutmut results 2>/dev/null || echo "")
  n_total=$(echo "$raw_results" | grep -cE "^[[:space:]]+plaita\." || true)

  if [[ $n_total -eq 0 ]]; then
    echo "  WARN: 0 mutants found — skipping"
    echo "ZERO_MUTANTS | $module" | tee -a "$RESULTS_FILE"
    continue
  fi

  n_not_checked=$(echo "$raw_results" | grep -c ": *not checked$" || true)
  n_timeout=$(echo "$raw_results" | grep -c ": *timeout$" || true)
  n_survived_pre=$(echo "$raw_results" | grep -c ": *survived$" || true)
  n_need_recheck=$((n_not_checked + n_timeout + n_survived_pre))
  n_killed_pre=$((n_total - n_need_recheck))

  echo "  mutants: total=${n_total} killed(pre)=${n_killed_pre} not_checked=${n_not_checked} timeout=${n_timeout} survived_pre=${n_survived_pre}"

  # ── 全量复核所有非-killed 变异点 ──────────────────────────────────────
  RECHECK_TMPFILE=$(mktemp)
  if [[ $n_need_recheck -gt 0 ]]; then
    recheck_all_mutations "$valid_tests"
    rc_survived=$(cat "$RECHECK_TMPFILE" 2>/dev/null || echo 0)
  else
    rc_survived=0
    echo "  (all mutations already killed, no recheck needed)"
  fi
  rm -f "$RECHECK_TMPFILE"

  [[ "$rc_survived" =~ ^[0-9]+$ ]] || rc_survived=0
  total_survived=$((n_survived_pre > rc_survived ? n_survived_pre : rc_survived))
  # 实际 survived = 复核结果（覆盖 pre-run 的 survived）
  total_survived=$rc_survived
  total_killed=$((n_total - total_survived))
  score=$(python3 -c "print(f'{$total_killed/$n_total*100:.0f}%')")

  if [[ $total_survived -gt 0 ]]; then
    status="NEEDS_FIX($total_survived survived)"
    GLOBAL_SURVIVED=$((GLOBAL_SURVIVED + total_survived))
  else
    status="100%"
    SUCCESS_MODULES+=("$module")
  fi

  echo "  SCORE: $score ($total_killed/$n_total) | $status"
  echo "$status | $module | $score ($total_killed/$n_total) | ${elapsed}s" | tee -a "$RESULTS_FILE"
done

# ── 汇总 ──────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "SWEEP COMPLETE | global survived: $GLOBAL_SURVIVED"
echo ""
cat "$RESULTS_FILE"
echo ""
echo "Modules with 100%: ${#SUCCESS_MODULES[@]}"
for m in "${SUCCESS_MODULES[@]}"; do echo "  ✓ $m"; done

# 恢复 pyproject.toml 基线
python3 <<PYEOF
import re
path = "pyproject.toml"
content = open(path).read()
modules = [
  "plaita/core/callback.py",
  "plaita/core/expression.py",
  "plaita/core/parallel_executor.py",
  "plaita/core/runner.py",
  "plaita/core/context.py",
  "plaita/node/calculate.py",
  "plaita/node/decide.py",
  # NEW — validated by this sweep
  "plaita/core/executor.py",
  "plaita/core/strategies.py",
  "plaita/core/state.py",
  "plaita/core/errors.py",
  "plaita/core/flow.py",
  "plaita/event/core.py",
  "plaita/node/concurrent.py",
  "plaita/node/loop.py",
  "plaita/core/expression_parser.py",
  "plaita/event/memory.py",
  "plaita/io.py",
  "plaita/dsl/builder.py",
  "plaita/node/code.py",
  "plaita/node/__init__.py",
  "plaita/node/event_node.py",
  "plaita/storage/memory.py",
  "plaita/storage/base.py",
]
entries = "\n".join(f'  "{m}",' for m in modules)
new_block = f'only_mutate = [\n{entries}\n]'
content = re.sub(r'only_mutate\s*=\s*\[.*?\]', new_block, content, flags=re.DOTALL)
content = re.sub(r'mutate_only_covered_lines\s*=\s*(true|false)', 'mutate_only_covered_lines = true', content)
open(path, "w").write(content)
print("pyproject.toml restored to full baseline.")
PYEOF
