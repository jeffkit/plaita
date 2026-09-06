#!/usr/bin/env bash
# e2e-run.sh — 跑 plaita-console 的 argusai E2E（MCP 路径：mcp2cli → argusai-mcp）
#
# 为什么走 MCP 路径而不用 `argusai` CLI：
#   argusai-cli 冻结在 0.12.3，`argusai run` 有 setup exec 不执行的回归；
#   0.14.x 起官方入口只有 argusai-mcp。与 recursive/.dev/scripts/e2e-run.sh
#   同款生命周期：init → build → setup → run → clean。
#
# 成功判定：status=passed 且 totals.total>0 且 totals.failed==0。
# total>0 防假绿灯（0.14.1 曾按 suite name 归并事件，名字不匹配时全部静默丢弃
# 仍报 passed；0.14.2 已修，guard 保留作纵深防御）。
#
# 用法：
#   scripts/e2e-run.sh              # 跑全部 suite
#   scripts/e2e-run.sh health       # 只跑指定 suite-id
#   scripts/e2e-run.sh health --no-build
#
# 前提：Docker、Node>=20、mcp2cli、npm i -g argusai-mcp

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_PROJECT="$SCRIPT_DIR/.."          # e2e.yaml 所在目录（plaita-console/）

SUITE=""
NO_BUILD=0
for _a in "$@"; do
  case "$_a" in
    --no-build) NO_BUILD=1 ;;
    --*) ;;  # 未知 flag 忽略（e2e-gate 会透传自己的选项）
    *) SUITE="$_a" ;;
  esac
done

# ---- resolve mcp2cli -------------------------------------------------------
MCP2CLI=""
for _c in "$HOME/.local/bin/mcp2cli" "/usr/local/bin/mcp2cli" "/opt/homebrew/bin/mcp2cli"; do
  [[ -x "$_c" ]] && { MCP2CLI="$_c"; break; }
done
[[ -n "$MCP2CLI" ]] || { echo "[e2e-run] mcp2cli not found (uv tool install mcp2cli)" >&2; exit 3; }

# ---- resolve argusai-mcp entry ---------------------------------------------
ARGUSAI_MCP_BIN=""
for _root in "$(npm root -g 2>/dev/null)" \
    "$HOME/.local/share/fnm/node-versions"/*/installation/lib/node_modules; do
  if [[ -f "$_root/argusai-mcp/dist/index.js" ]]; then
    ARGUSAI_MCP_BIN="$_root/argusai-mcp/dist/index.js"; break
  fi
done
if [[ -n "$ARGUSAI_MCP_BIN" ]]; then
  _MCP_STDIO_CMD="node $ARGUSAI_MCP_BIN"
elif command -v npx >/dev/null 2>&1; then
  _MCP_STDIO_CMD="npx argusai-mcp"
else
  echo "[e2e-run] argusai-mcp not found (npm i -g argusai-mcp)" >&2; exit 3
fi

SESSION="plaita-e2e-$$"

_argus() {
  local out
  out="$("$MCP2CLI" --session "$SESSION" "$@" 2>&1)"
  local rc=$?
  echo "$out"
  if echo "$out" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('success',True) else 1)" 2>/dev/null; then
    return $rc
  fi
  return 1
}

cleanup() {
  _argus argus-clean --project-path "$E2E_PROJECT" >/dev/null 2>&1 || true
  "$MCP2CLI" --session-stop "$SESSION" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---- lifecycle: init → build → setup → run → clean -------------------------
"$MCP2CLI" --mcp-stdio "$_MCP_STDIO_CMD" --session-start "$SESSION" >/dev/null 2>&1

if ! _argus argus-init --project-path "$E2E_PROJECT" >/dev/null 2>&1; then
  echo "[e2e-run] argus-init failed" >&2; exit 5
fi

if [[ "$NO_BUILD" -eq 0 ]]; then
  echo "[e2e-run] build..."
  _argus argus-build --project-path "$E2E_PROJECT" >/dev/null 2>&1 || echo "[e2e-run] build warned (continuing with existing image)" >&2
fi

echo "[e2e-run] setup..."
if ! _argus argus-setup --project-path "$E2E_PROJECT" >/dev/null 2>&1; then
  echo "[e2e-run] setup failed" >&2; exit 5
fi

# argus-run 的自动启动不等 healthcheck 通过就开始跑用例（实测 fetch failed
# 竞态），这里显式等两个 backend 健康再开跑。端口与 e2e.yaml 的 ports 映射一致
# （18080=API 面，18081=UI 面）。
echo "[e2e-run] wait for backend health..."
for _port in 18080 18081; do
  for _i in $(seq 1 60); do
    if curl -sf -m 2 "http://localhost:$_port/health" >/dev/null 2>&1; then
      break
    fi
    [[ "$_i" -eq 60 ]] && { echo "[e2e-run] backend :$_port not healthy after 60s" >&2; exit 5; }
    sleep 1
  done
done

FILTER_ARGS=()
[[ -n "$SUITE" ]] && FILTER_ARGS=(--filter "$SUITE")

echo "[e2e-run] run ${SUITE:-all suites}..."
RUN_OUT="$(_argus argus-run --project-path "$E2E_PROJECT" ${FILTER_ARGS[@]+"${FILTER_ARGS[@]}"} 2>&1)"
echo "$RUN_OUT" | python3 -c '
import sys, json
raw = sys.stdin.read()
i = raw.find("{")
try:
    d = json.loads(raw[i:])
except Exception:
    print("  (no JSON parsed)"); print(raw[:2000]); sys.exit(0)
data = d.get("data", {}) or {}
t = data.get("totals", {}) or {}
print("  status=%s totals=%s" % (data.get("status"), t))
for s in data.get("suites", []):
    print("  suite %s (%s): passed=%s failed=%s skipped=%s" % (
        s.get("id"), s.get("name", ""), s.get("passed"), s.get("failed"), s.get("skipped")))
' 2>&1 || true

if echo "$RUN_OUT" | python3 -c '
import sys, json
raw = sys.stdin.read()
i = raw.find("{")
d = json.loads(raw[i:])
data = d.get("data", {}) or {}
t = data.get("totals", {}) or {}
sys.exit(0 if (data.get("status") == "passed" and t.get("total", 0) > 0 and t.get("failed", 0) == 0) else 1)
' 2>/dev/null; then
  echo "[e2e-run] ${SUITE:-ALL} PASSED ✓"
  exit 0
else
  echo "[e2e-run] ${SUITE:-ALL} FAILED ✗" >&2
  exit 1
fi
