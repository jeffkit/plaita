#!/usr/bin/env bash
# e2e-chaos-dlq.sh — 混沌回归：必失败任务经重投收容进 DLQ，可观察。
#
# 场景（2026-10-10 重写）。旧场景是「resume_type=continue 打在 approval 挂起执行
# 上 → 内核拒绝（ResumeError）→ RuntimeError → 任务不 ack 留 pending → 回收进
# DLQ」；plaita#33 起 continue/retry 打在挂起执行上是**幂等 no-op**（消息被 ack、
# 状态保持 suspended），那条链路不再产生任何 pending/DLQ 记录，故换一个**确定
# 性失败**的任务覆盖同一四处机制：at-least-once 重投 → reclaim → max_deliveries
# → 死信守卫 → DLQ 信封。
#
# 新场景（机器亲和闸，plaita#41）：
#   1. 派发一个**本机不亲和**的 start 任务：params.repo = worker 容器里不存在的
#      绝对路径 → 亲和闸在 start 之前拦下（任务本身合法，只是该 repo 只存在于
#      编排方那台机器）→ 首次投递走「交接」（ack 原消息 + 重入队同体新副本），
#      第二次投递命中「本机刚让过这条」判据 → **不 ack 留 pending**；
#   2. 「清道夫」worker（PLAITA_CLAIM_MIN_IDLE_MS=3000 + PLAITA_MAX_DELIVERIES=1，
#      独立容器——不污染共享组主 worker 的旋钮）3s 后回收该 pending → delivery
#      超限 → 死信守卫（该执行状态从未落盘 → 放行）→ 写 DLQ。
# 断言：任务确实从未跑起来（执行列表为空）+ DLQ 收到**信封完整**的那条任务
#（reason=max_deliveries=1、payload 带 execution_id、source_stream 正确）。
#
# 前置：worker 侧未设 PLAITA_DISABLE_AFFINITY=1（e2e.yaml 未设，亲和闸生效）。
# 教训背景：激进 reclaim 旋钮放在共享消费组的主 worker 上会抢走兄弟 worker
# 处理中的任务（实测冷启动任务超 3s 即被回收），故清道夫独立成容器、用完即焚。
#
# 容器/网络名与 plaita-console/e2e.yaml 耦合（带 argusai 项目命名空间前缀）。
# 用法：--no-build。退出码 0 绿/1 红。

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_PROJECT="$SCRIPT_DIR/.."

NO_BUILD=0
for _a in "$@"; do
  case "$_a" in
    --no-build) NO_BUILD=1 ;;
  esac
done

NET="argusai-plaita-console-network"
API="http://localhost:18080"
REDIS_C="plaita-console-plaita-e2e-redis"
QUEUE="plaita:flow:queue"
DLQ="$QUEUE:dlq"
GROUP="plaita-workers"
FLOW_ID="e2e-dlq-flow"
# worker 容器里必然不存在的绝对路径（亲和闸只对绝对路径生效）
ABSENT_REPO="/tmp/plaita-e2e-dlq-absent-repo"

MCP2CLI=""
for _c in "$HOME/.local/bin/mcp2cli" "/usr/local/bin/mcp2cli" "/opt/homebrew/bin/mcp2cli"; do
  [[ -x "$_c" ]] && { MCP2CLI="$_c"; break; }
done
[[ -n "$MCP2CLI" ]] || { echo "[chaos-dlq] mcp2cli not found" >&2; exit 3; }
ARGUSAI_MCP_BIN=""
for _root in "$(npm root -g 2>/dev/null)" "$HOME/.local/share/fnm/node-versions"/*/installation/lib/node_modules; do
  [[ -f "$_root/argusai-mcp/dist/index.js" ]] && { ARGUSAI_MCP_BIN="$_root/argusai-mcp/dist/index.js"; break; }
done
[[ -n "$ARGUSAI_MCP_BIN" ]] || { echo "[chaos-dlq] argusai-mcp not found" >&2; exit 3; }

SESSION="plaita-chaos-dlq-$$"
_argus() { "$MCP2CLI" --session "$SESSION" "$@" 2>&1; }
cleanup() {
  docker rm -f e2e-dlq-scavenger >/dev/null 2>&1 || true
  _argus argus-clean --project-path "$E2E_PROJECT" >/dev/null 2>&1 || true
  "$MCP2CLI" --session-stop "$SESSION" >/dev/null 2>&1 || true
}
trap cleanup EXIT

"$MCP2CLI" --mcp-stdio "node $ARGUSAI_MCP_BIN" --session-start "$SESSION" >/dev/null 2>&1
_argus argus-init --project-path "$E2E_PROJECT" >/dev/null 2>&1 || { echo "[chaos-dlq] init failed" >&2; exit 5; }
[[ "$NO_BUILD" -eq 1 ]] || _argus argus-build --project-path "$E2E_PROJECT" >/dev/null 2>&1 || true
_argus argus-setup --project-path "$E2E_PROJECT" >/dev/null 2>&1 || { echo "[chaos-dlq] setup failed" >&2; exit 5; }
for _i in $(seq 1 60); do
  curl -sf -m 2 "$API/health" >/dev/null 2>&1 && break
  [[ "$_i" -eq 60 ]] && { echo "[chaos-dlq] backend not ready" >&2; exit 5; }
  sleep 1
done

# ---- 造一条合法流程（任务的失败点只是 repo 不在本机）-------------------------
curl -s -X POST "$API/api/flows" -H 'Content-Type: application/json' \
  -d "{\"flow_id\":\"$FLOW_ID\",\"author\":\"chaos\"}" >/dev/null
curl -s -X PUT "$API/api/flows/$FLOW_ID/versions/1.0.0" -H 'Content-Type: application/json' \
  -d "{\"definition\":\"{\\\"id\\\":\\\"$FLOW_ID\\\",\\\"name\\\":\\\"dlq\\\",\\\"nodes\\\":[{\\\"id\\\":\\\"start\\\",\\\"type\\\":\\\"start\\\",\\\"next\\\":\\\"end\\\"},{\\\"id\\\":\\\"end\\\",\\\"type\\\":\\\"end\\\",\\\"output\\\":\\\"ok\\\",\\\"result_type\\\":\\\"success\\\"}]}\",\"created_by\":\"chaos\"}" >/dev/null
curl -s -X POST "$API/api/flows/$FLOW_ID/publish" -H 'Content-Type: application/json' \
  -d '{"version":"1.0.0"}' >/dev/null

# ---- 派发不亲和任务：worker 拿不到 repo 路径，永远跑不起来 --------------------
START_RESP=$(curl -s -X POST "$API/api/executions" -H 'Content-Type: application/json' \
  -d "{\"flow_id\":\"$FLOW_ID\",\"version\":\"1.0.0\",\"params\":{\"repo\":\"$ABSENT_REPO\"}}")
EXEC_ID=$(printf '%s' "$START_RESP" | python3 -c "
import sys, json
print(json.load(sys.stdin).get('execution_id', ''))
" 2>/dev/null)
[[ -n "$EXEC_ID" ]] || { echo "[chaos-dlq] start 未返回 execution_id: $START_RESP" >&2; exit 1; }
echo "[chaos-dlq] 不亲和 start 已入队: $EXEC_ID (repo=$ABSENT_REPO)"

FAIL=0

# ---- 断言 1：交接后任务留在 pending（无人 ack），且执行状态从未落盘 -----------
PEND=0
PEND_N=0
for _i in $(seq 1 30); do
  PEND_N=$(docker exec "$REDIS_C" redis-cli --raw XPENDING "$QUEUE" "$GROUP" 2>/dev/null | head -1 | tr -d '\r')
  [[ "${PEND_N:-0}" -ge 1 ]] && { PEND=1; break; }
  sleep 1
done
TOTAL=$(curl -s "$API/api/executions?flow_id=$FLOW_ID" | python3 -c "
import sys, json
print(json.load(sys.stdin).get('total', -1))
" 2>/dev/null)
if [[ "$PEND" -eq 1 && "$TOTAL" == "0" ]]; then
  echo "[chaos-dlq] OK: 不亲和任务留 pending=$PEND_N，执行未落盘（total=0）"
else
  echo "[chaos-dlq] FAIL: pending=${PEND_N:-?}（预期 ≥1），执行 total=${TOTAL:-?}（预期 0）" >&2
  FAIL=1
fi

# ---- 清道夫 worker：3s reclaim + 首见即收容 ----------------------------------
echo "[chaos-dlq] 启动清道夫 worker（claim=3000ms, max_deliveries=1）..."
docker run -d --name e2e-dlq-scavenger --network "$NET" \
  -e E2E_TEST_REDIS_HOST=plaita-e2e-redis \
  -e PLAITA_REDIS_URL=redis://plaita-e2e-redis:6379/0 \
  -e PLAITA_CLAIM_MIN_IDLE_MS=3000 \
  -e PLAITA_MAX_DELIVERIES=1 \
  plaita-e2e-worker:latest >/dev/null || { echo "[chaos-dlq] scavenger start failed" >&2; exit 5; }

# ---- 断言 2：DLQ 收到那条任务（信封完整）-------------------------------------
DLQED=0
N=0
ENTRY=""
for _i in $(seq 1 30); do
  N=$(docker exec "$REDIS_C" redis-cli XLEN "$DLQ" 2>/dev/null | tr -d '\r')
  if [[ "${N:-0}" -ge 1 ]]; then
    DLQED=1
    ENTRY=$(docker exec "$REDIS_C" redis-cli --raw XRANGE "$DLQ" - + COUNT 1 2>/dev/null)
    break
  fi
  sleep 1
done
if [[ "$DLQED" -eq 1 ]] \
  && grep -q "max_deliveries=1" <<<"$ENTRY" \
  && grep -q "$EXEC_ID" <<<"$ENTRY" \
  && grep -q "$QUEUE" <<<"$ENTRY"; then
  echo "[chaos-dlq] OK: 失败任务已死信（XLEN=$N，信封含 reason/payload/source_stream）"
else
  echo "[chaos-dlq] FAIL: DLQ 未收到信封完整的目标任务（XLEN=${N:-0}）" >&2
  printf '%s\n' "$ENTRY" | sed 's/^/[chaos-dlq]   /' >&2
  FAIL=1
fi

if [[ "$FAIL" -eq 0 ]]; then
  echo "[chaos-dlq] CHAOS PASSED (不亲和必失败任务 → reclaim → DLQ 收容)"
  exit 0
fi
echo "[chaos-dlq] CHAOS FAILED" >&2
exit 1
