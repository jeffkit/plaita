#!/usr/bin/env bash
# e2e-gate.sh — plaita-console argusai E2E 门（纯判定：退出码表达红/绿）。
#
# 职责：
#   1. 前置硬检查（Docker / mcp2cli / argusai-mcp / e2e.yaml）——作为门使用时
#      前置缺失一律红灯（exit 3），不静默跳过
#   2. 自清理：上一轮被中断的运行会留下 argusai 托管容器/网络（argus-clean
#      已知会泄漏空网络耗尽 Docker 地址池），跑之前先扫掉
#   3. 委托 e2e-run.sh 跑全量 suite，透传退出码
#
# 用法：
#   scripts/e2e-gate.sh              # 全量 10 suite
#   scripts/e2e-gate.sh --quick      # 冒烟子集（health/executions/errors，约 1 分钟）
#   scripts/e2e-gate.sh --check-prereqs   # 只查前置，不跑
#
# 退出码：0=绿；1=套件红；3=前置缺失。

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_PROJECT="$SCRIPT_DIR/.."

MODE="full"
[[ "${1:-}" == "--quick" ]] && MODE="quick"
CHECK_PREREQS=0
[[ "${1:-}" == "--check-prereqs" ]] && CHECK_PREREQS=1

# ---- 前置检查 ---------------------------------------------------------------
_missing=()
docker info >/dev/null 2>&1 || _missing+=("docker daemon 未运行")
for _c in "$HOME/.local/bin/mcp2cli" "/usr/local/bin/mcp2cli" "/opt/homebrew/bin/mcp2cli"; do
  [[ -x "$_c" ]] && break
done || true
[[ -x "${_c:-}" ]] || _missing+=("mcp2cli 未安装 (uv tool install mcp2cli)")
_argusai_found=0
for _root in "$(npm root -g 2>/dev/null)" "$HOME/.local/share/fnm/node-versions"/*/installation/lib/node_modules; do
  [[ -f "$_root/argusai-mcp/dist/index.js" ]] && { _argusai_found=1; break; }
done
[[ "$_argusai_found" -eq 1 ]] || command -v npx >/dev/null 2>&1 || _missing+=("argusai-mcp 未安装 (npm i -g argusai-mcp)")
[[ -f "$E2E_PROJECT/e2e.yaml" ]] || _missing+=("e2e.yaml 不存在")

if [[ "${#_missing[@]}" -gt 0 ]]; then
  echo "[e2e-gate] 前置缺失：" >&2
  for m in "${_missing[@]}"; do echo "  - $m" >&2; done
  exit 3
fi
[[ "$CHECK_PREREQS" -eq 1 ]] && { echo "[e2e-gate] 前置齐备 ✓"; exit 0; }

# ---- 自清理：上一轮中断残留 -------------------------------------------------
# 孤儿容器：带 argusai.project=plaita-console 标签但不属于任何活跃运行
for _c in $(docker ps -a --filter "label=argusai.project=plaita-console" --format '{{.Names}}' 2>/dev/null); do
  echo "[e2e-gate] 清理孤儿容器: $_c"
  docker rm -f "$_c" >/dev/null 2>&1 || true
done
# 泄漏网络：argusai-<project>-network 且没有容器挂着（argus-clean 已知泄漏）
for _n in $(docker network ls --format '{{.Name}}' 2>/dev/null | grep '^argusai-'); do
  _attached=$(docker network inspect "$_n" --format '{{len .Containers}}' 2>/dev/null || echo 0)
  if [[ "$_attached" == "0" ]]; then
    echo "[e2e-gate] 清理泄漏空网络: $_n"
    docker network rm "$_n" >/dev/null 2>&1 || true
  fi
done
# 残留会话状态（被中断的 run 会留下坏状态目录）
if [[ -d "$E2E_PROJECT/.argusai" && "${E2E_KEEP_HISTORY:-0}" != "1" ]]; then
  rm -rf "$E2E_PROJECT/.argusai"
fi

# ---- 跑套件 -----------------------------------------------------------------
ARGS=()
if [[ "$MODE" == "quick" ]]; then
  echo "[e2e-gate] quick 冒烟：health + executions + errors"
  for s in health executions errors; do
    "$SCRIPT_DIR/e2e-run.sh" "$s" "$@" || exit 1
  done
  echo "[e2e-gate] QUICK PASSED ✓"
  exit 0
fi

echo "[e2e-gate] 全量套件..."
exec "$SCRIPT_DIR/e2e-run.sh" "$@"
