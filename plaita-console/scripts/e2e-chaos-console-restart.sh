#!/usr/bin/env bash
# e2e-chaos-console-restart.sh — 混沌回归：console 容器重启后的状态一致性。
#
# 考点（与 e2e-chaos-redis.sh 互补）：
#   1. lifespan 重启后重新 ping Redis 成功，**不得静默降级本地单机模式**
#      （降级的症状：/api/queues 等返回 503）；
#   2. SQLite（流程定义，容器文件系统内）与 Redis（执行态/引擎存储，容器外）
#      双库在重启后一致——无需重新发布即可直接启动执行并跑通；
#   3. /health 是静态返回不 ping Redis，健康的 backend 未必在集群模式，
#      必须用集群档专属端点验证。
#
# 容器名与 plaita-console/e2e.yaml 耦合。用法：--no-build 可选。退出码 0 绿/1 红。

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_PROJECT="$SCRIPT_DIR/.."

NO_BUILD=0
for _a in "$@"; do
  case "$_a" in
    --no-build) NO_BUILD=1 ;;
  esac
done

CONSOLE_C="plaita-console-plaita-console-backend-e2e"
API="http://localhost:18080"

MCP2CLI=""
for _c in "$HOME/.local/bin/mcp2cli" "/usr/local/bin/mcp2cli" "/opt/homebrew/bin/mcp2cli"; do
  [[ -x "$_c" ]] && { MCP2CLI="$_c"; break; }
done
[[ -n "$MCP2CLI" ]] || { echo "[chaos-cs] mcp2cli 未安装" >&2; exit 3; }
ARGUSAI_MCP_BIN=""
for _root in "$(npm root -g 2>/dev/null)" "$HOME/.local/share/fnm/node-versions"/*/installation/lib/node_modules; do
  [[ -f "$_root/argusai-mcp/dist/index.js" ]] && { ARGUSAI_MCP_BIN="$_root/argusai-mcp/dist/index.js"; break; }
done
[[ -n "$ARGUSAI_MCP_BIN" ]] || { echo "[chaos-cs] argusai-mcp 未安装" >&2; exit 3; }

SESSION="plaita-chaos-cs-$$"
_argus() { "$MCP2CLI" --session "$SESSION" "$@" 2>&1; }
cleanup() {
  _argus argus-clean --project-path "$E2E_PROJECT" >/dev/null 2>&1 || true
  "$MCP2CLI" --session-stop "$SESSION" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---- 起环境 -----------------------------------------------------------------
"$MCP2CLI" --mcp-stdio "node $ARGUSAI_MCP_BIN" --session-start "$SESSION" >/dev/null 2>&1
_argus argus-init --project-path "$E2E_PROJECT" >/dev/null 2>&1 || { echo "[chaos-cs] init 失败" >&2; exit 5; }
[[ "$NO_BUILD" -eq 1 ]] || _argus argus-build --project-path "$E2E_PROJECT" >/dev/null 2>&1 || true
_argus argus-setup --project-path "$E2E_PROJECT" >/dev/null 2>&1 || { echo "[chaos-cs] setup 失败" >&2; exit 5; }
for _i in $(seq 1 60); do
  curl -sf -m 2 "$API/health" >/dev/null 2>&1 && break
  [[ "$_i" -eq 60 ]] && { echo "[chaos-cs] backend 未就绪" >&2; exit 5; }
  sleep 1
done

# ---- 重启前：造一份已发布流程 ----------------------------------------------
curl -s -X POST "$API/api/flows" -H 'Content-Type: application/json' \
  -d '{"flow_id":"e2e-chaos-cs-flow","author":"chaos"}' >/dev/null
curl -s -X PUT "$API/api/flows/e2e-chaos-cs-flow/versions/1.0.0" -H 'Content-Type: application/json' \
  -d '{"definition":"{\"id\":\"e2e-chaos-cs-flow\",\"name\":\"chaos-cs\",\"nodes\":[{\"id\":\"start\",\"type\":\"start\",\"next\":\"end\"},{\"id\":\"end\",\"type\":\"end\",\"output\":\"$INPUT.value\",\"result_type\":\"success\"}]}","created_by":"chaos"}' >/dev/null
curl -s -X POST "$API/api/flows/e2e-chaos-cs-flow/publish" -H 'Content-Type: application/json' \
  -d '{"version":"1.0.0"}' >/dev/null
echo "[chaos-cs] 环境就绪，重启 console 容器..."

# ---- 故障注入：docker restart ------------------------------------------------
docker restart "$CONSOLE_C" >/dev/null || { echo "[chaos-cs] restart 失败" >&2; exit 5; }
for _i in $(seq 1 60); do
  curl -sf -m 2 "$API/health" >/dev/null 2>&1 && break
  [[ "$_i" -eq 60 ]] && { echo "[chaos-cs] 重启后 backend 未恢复" >&2; exit 5; }
  sleep 1
done

FAIL=0

# ---- 断言 1：未降级本地单机模式（降级时 /api/queues 返回 503）----------------
QS=$(curl -s -o /dev/null -w "%{http_code}" "$API/api/queues")
if [[ "$QS" == "200" ]]; then
  echo "[chaos-cs] ✓ 重启后仍为集群模式（/api/queues 200，未静默降级）"
else
  echo "[chaos-cs] ✗ 重启后 /api/queues 返回 $QS——lifespan 疑似降级本地模式" >&2
  FAIL=1
fi

# ---- 断言 2：重启前发布的流程仍在（SQLite + 引擎 Redis 存储双库一致）----------
FLOW_OK=$(curl -s "$API/api/flows/e2e-chaos-cs-flow" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('yes' if d.get('flow_id') == 'e2e-chaos-cs-flow' else 'no')
" 2>/dev/null)
if [[ "$FLOW_OK" == "yes" ]]; then
  echo "[chaos-cs] ✓ 重启前发布的流程仍在（无需重新发布）"
else
  echo "[chaos-cs] ✗ 重启后流程定义丢失" >&2
  FAIL=1
fi

# ---- 断言 3：不重新发布，直接启动执行并跑通至 completed ----------------------
curl -s -X POST "$API/api/executions" -H 'Content-Type: application/json' \
  -d '{"flow_id":"e2e-chaos-cs-flow","version":"1.0.0","params":{"value":1}}' >/dev/null
DONE=0
for _i in $(seq 1 60); do
  ST=$(curl -s "$API/api/executions?flow_id=e2e-chaos-cs-flow" | python3 -c "
import sys, json
d = json.load(sys.stdin)
ex = d.get('executions') or []
print(ex[0].get('status', '')) if ex else print('')
" 2>/dev/null)
  [[ "$ST" == "completed" ]] && { DONE=1; break; }
  sleep 1
done
if [[ "$DONE" -eq 1 ]]; then
  echo "[chaos-cs] ✓ 重启后执行跑通至 completed（发布同步未丢失）"
else
  echo "[chaos-cs] ✗ 重启后执行未完成（最后状态: ${ST:-未知}）" >&2
  FAIL=1
fi

if [[ "$FAIL" -eq 0 ]]; then
  echo "[chaos-cs] CHAOS PASSED ✓（集群模式保持 + 双库一致 + 执行跑通）"
  exit 0
fi
echo "[chaos-cs] CHAOS FAILED ✗" >&2
exit 1
