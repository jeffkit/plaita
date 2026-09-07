#!/usr/bin/env bash
# e2e-chaos-worker-restart.sh — 混沌回归：双 worker 全停期间任务不丢不跑，
# 拉起后恢复消费、注册表回到恰 2 实例。
#
# 考点：
#   1. worker 全停期间启动的执行只入队、无执行状态写入（不丢不跑）；
#   2. worker 拉起后新 consumer 从 Stream 读到积压消息并跑通（XREADGROUP
#      消费组的持久性）；
#   3. 服务注册表自愈：旧实例键 30s TTL 过期 + 新实例注册，最终恰 2 个
#      flow_worker 实例。
# 停机用 SIGTERM（docker stop）而非 kill -9：优雅停机路径有明确契约
# （XREADGROUP ≤1s 分片窗口）；kill -9 中途接管需要慢节点支撑，当前节点集
# 无确定性慢节点，豁免。
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

# 容器名带 argusai 项目命名空间前缀（isolation.namespace 未启用时确定性地加项目名）
WORKERS=(plaita-console-plaita-e2e-worker plaita-console-plaita-e2e-worker-2)
REDIS_C="plaita-console-plaita-e2e-redis"
API="http://localhost:18080"

MCP2CLI=""
for _c in "$HOME/.local/bin/mcp2cli" "/usr/local/bin/mcp2cli" "/opt/homebrew/bin/mcp2cli"; do
  [[ -x "$_c" ]] && { MCP2CLI="$_c"; break; }
done
[[ -n "$MCP2CLI" ]] || { echo "[chaos-wr] mcp2cli 未安装" >&2; exit 3; }
ARGUSAI_MCP_BIN=""
for _root in "$(npm root -g 2>/dev/null)" "$HOME/.local/share/fnm/node-versions"/*/installation/lib/node_modules; do
  [[ -f "$_root/argusai-mcp/dist/index.js" ]] && { ARGUSAI_MCP_BIN="$_root/argusai-mcp/dist/index.js"; break; }
done
[[ -n "$ARGUSAI_MCP_BIN" ]] || { echo "[chaos-wr] argusai-mcp 未安装" >&2; exit 3; }

SESSION="plaita-chaos-wr-$$"
_argus() { "$MCP2CLI" --session "$SESSION" "$@" 2>&1; }
cleanup() {
  _argus argus-clean --project-path "$E2E_PROJECT" >/dev/null 2>&1 || true
  "$MCP2CLI" --session-stop "$SESSION" >/dev/null 2>&1 || true
  docker start plaita-console-plaita-e2e-worker plaita-console-plaita-e2e-worker-2 >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---- 起环境 -----------------------------------------------------------------
"$MCP2CLI" --mcp-stdio "node $ARGUSAI_MCP_BIN" --session-start "$SESSION" >/dev/null 2>&1
_argus argus-init --project-path "$E2E_PROJECT" >/dev/null 2>&1 || { echo "[chaos-wr] init 失败" >&2; exit 5; }
[[ "$NO_BUILD" -eq 1 ]] || _argus argus-build --project-path "$E2E_PROJECT" >/dev/null 2>&1 || true
_argus argus-setup --project-path "$E2E_PROJECT" >/dev/null 2>&1 || { echo "[chaos-wr] setup 失败" >&2; exit 5; }
for _i in $(seq 1 60); do
  curl -sf -m 2 "$API/health" >/dev/null 2>&1 && break
  [[ "$_i" -eq 60 ]] && { echo "[chaos-wr] backend 未就绪" >&2; exit 5; }
  sleep 1
done

# ---- 造已发布流程 ------------------------------------------------------------
curl -s -X POST "$API/api/flows" -H 'Content-Type: application/json' \
  -d '{"flow_id":"e2e-chaos-wr-flow","author":"chaos"}' >/dev/null
curl -s -X PUT "$API/api/flows/e2e-chaos-wr-flow/versions/1.0.0" -H 'Content-Type: application/json' \
  -d '{"definition":"{\"id\":\"e2e-chaos-wr-flow\",\"name\":\"chaos-wr\",\"nodes\":[{\"id\":\"start\",\"type\":\"start\",\"next\":\"end\"},{\"id\":\"end\",\"type\":\"end\",\"output\":\"$INPUT.value\",\"result_type\":\"success\"}]}","created_by":"chaos"}' >/dev/null
curl -s -X POST "$API/api/flows/e2e-chaos-wr-flow/publish" -H 'Content-Type: application/json' \
  -d '{"version":"1.0.0"}' >/dev/null

# ---- 故障注入：双 worker 全停 ------------------------------------------------
docker stop "${WORKERS[@]}" >/dev/null || { echo "[chaos-wr] stop 失败" >&2; exit 5; }
echo "[chaos-wr] 双 worker 已停止，停机窗口内入队执行..."
curl -s -X POST "$API/api/executions" -H 'Content-Type: application/json' \
  -d '{"flow_id":"e2e-chaos-wr-flow","version":"1.0.0","params":{"value":1}}' >/dev/null
sleep 5

FAIL=0
# ---- 断言 1：停机窗口内不丢不跑（无执行状态写入）-----------------------------
QUEUED=$(curl -s "$API/api/executions?flow_id=e2e-chaos-wr-flow" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('total', -1))
" 2>/dev/null)
if [[ "$QUEUED" == "0" ]]; then
  echo "[chaos-wr] ✓ 停机窗口内任务只入队不执行（无状态写入）"
else
  echo "[chaos-wr] ✗ 停机窗口内出现执行状态（total=$QUEUED，预期 0）" >&2
  FAIL=1
fi

# ---- 恢复：拉起双 worker ------------------------------------------------------
docker start "${WORKERS[@]}" >/dev/null || { echo "[chaos-wr] start 失败" >&2; exit 5; }

# ---- 断言 2：拉起后积压任务被消费跑通 ----------------------------------------
DONE=0
for _i in $(seq 1 60); do
  ST=$(curl -s "$API/api/executions?flow_id=e2e-chaos-wr-flow" | python3 -c "
import sys, json
d = json.load(sys.stdin)
ex = d.get('executions') or []
print(ex[0].get('status', '')) if ex else print('')
" 2>/dev/null)
  [[ "$ST" == "completed" ]] && { DONE=1; break; }
  sleep 1
done
if [[ "$DONE" -eq 1 ]]; then
  echo "[chaos-wr] ✓ 拉起后积压任务跑通至 completed（消费组持久性）"
else
  echo "[chaos-wr] ✗ 拉起后任务未完成（最后状态: ${ST:-未知}）" >&2
  FAIL=1
fi

# ---- 断言 3：注册表自愈至恰 2 个 flow_worker 实例（旧键 30s TTL 过期）--------
REG_OK=0
for _i in $(seq 1 60); do
  CNT=$(docker exec "$REDIS_C" redis-cli --scan --pattern 'plaita:registry:flow_worker:*' 2>/dev/null | wc -l | tr -d ' ')
  [[ "$CNT" == "2" ]] && { REG_OK=1; break; }
  sleep 1
done
if [[ "$REG_OK" -eq 1 ]]; then
  echo "[chaos-wr] ✓ 注册表自愈至恰 2 个 flow_worker 实例"
else
  echo "[chaos-wr] ✗ 注册表实例数异常（当前 ${CNT:-未知}，预期 2）" >&2
  FAIL=1
fi

if [[ "$FAIL" -eq 0 ]]; then
  echo "[chaos-wr] CHAOS PASSED ✓（停机不丢不跑 + 拉起恢复 + 注册表自愈）"
  exit 0
fi
echo "[chaos-wr] CHAOS FAILED ✗" >&2
exit 1
