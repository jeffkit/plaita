#!/usr/bin/env bash
# smoke-bisect.sh — 当日 merge 自动二分（#182 落地熔断自动化的二分腿）
#
# 用法：smoke-bisect.sh [SINCE] [SMOKE_CMD...]
#   SINCE      二分窗口起点（git log --since 语法，默认 "24 hours ago"）
#   SMOKE_CMD  冒烟命令（默认：layering + SC-003 + 表达式族快速切片）
#
# 行为：对窗口内每个 merge commit（旧→新）+ 当前 HEAD 逐一跑冒烟；
#       首个红 = 当日肇事 merge（「首红优先处置」纪律的自动化）。
# 退出码：0=全绿；1=定位到肇事（stdout 有 CULPRIT=<sha>）；2=窗口内无 merge 但 HEAD 红。
#
# 可移植性：bash 3.2 兼容（macOS 自带版），不用 mapfile/关联数组/进程替换/tac。
set -u

SINCE="${1:-24 hours ago}"
shift || true
if [ "$#" -gt 0 ]; then SMOKE=("$@"); else
  SMOKE=(python -m pytest tests/integration/test_layering.py tests/e2e/test_success_criteria.py tests/unit/test_expression_provenance_perf.py tests/unit/test_no_silent_swallow.py -q -p no:randomly --no-header --timeout=300 --timeout-method=thread)
fi

git fetch origin main -q 2>/dev/null || true

# 候选 = 窗口内 merge 提交，--reverse 直接旧→新
MERGES=$(git log origin/main --reverse --merges --since="$SINCE" --format=%H 2>/dev/null)

echo "[bisect] 窗口内 merge 数: $(echo $MERGES | wc -w | tr -d ' ')"
for sha in $MERGES; do
  subject=$(git log -1 --format=%s "$sha")
  echo "[bisect] checkout $(git rev-parse --short "$sha") $subject"
  git checkout -q --detach "$sha" || { echo "[bisect] checkout 失败，跳过"; continue; }
  if "${SMOKE[@]}" >/dev/null 2>&1; then
    echo "[bisect]   绿"
  else
    echo "[bisect]   红 <- 首红"
    echo "CULPRIT=$(git rev-parse --short "$sha")"
    echo "CULPRIT_SUBJECT=$subject"
    git checkout -q --detach origin/main 2>/dev/null || true
    exit 1
  fi
done

echo "[bisect] 窗口内 merge 全绿；测 HEAD"
git checkout -q --detach origin/main || exit 2
if "${SMOKE[@]}" >/dev/null 2>&1; then
  echo "[bisect] HEAD 绿——窗口内无肇事（红因在窗口前或环境面）"
  exit 0
else
  echo "[bisect] HEAD 红但窗口内 merge 全绿 => 非当日 merge 引入（查环境/依赖面）"
  echo "CULPRIT=none-in-window"
  exit 2
fi
