#!/usr/bin/env bash
# e2e-chaos-dlq.sh — 混沌回归：注定失败的任务经重投收容进 DLQ，可观察。
#
# 场景（#33/#73 起换载体）：approval 挂起派发**持续失败**——把服务队列键
# ``plaita:approval:queue`` 塞成 string，worker 的 RPUSH 撞 WRONGTYPE →
# ``ServiceDispatchError`` → 走「不 ack」兜底路径（执行**已**落 suspended、
# **不**终态化）；「清道夫」worker（PLAITA_CLAIM_MIN_IDLE_MS=3000 +
# PLAITA_MAX_DELIVERIES=1，独立容器，不污染共享组主 worker 的旋钮）3s 后回收
# 这条 pending 消息 → 死信守卫按「非终态 + 租约空」判活（见
# FlowWorker._dead_letter_guard）→ 重入队一份新副本（恢复路径重生）+ 原消息
# dead_letter。
# 断言：① ``plaita:flow:queue:dlq`` 收到该消息（收容可观察）；② 执行仍**非**
# 终态（suspended/running）——派发失败不该把执行误终态化，它就是重投要救的
# 那个执行。
#
# 为什么换载体：旧载体是「挂起执行上投 resume_type=continue」，其语义已改为
# worker 入口**幂等短路**（消息 ack + 状态保持 suspended，见 #33）——既不终态化
# 也不留 pending，跑不出任何 DLQ；确定性节点失败（#73）同理，现在是 poison ack
# （NodeFailureTerminalizedError），不再进 DLQ。仍会被重投耗尽而收容的只剩
# 「不 ack 兜底」这一族（ServiceDispatchError / StatePersistError /
# ExecutionStateLoadError），本脚本用第一个做载体——它不需要外部依赖，且能把
# 「执行不丢（非终态 + 守卫重入队）」一并断言掉。
#
# 教训背景：激进 reclaim 旋钮放在共享消费组的主 worker 上会抢走兄弟 worker
# 处理中的任务（冷启动任务超 3s 即被回收），故清道夫独立成容器、用完即焚。
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
DLQ_KEY="plaita:flow:queue:dlq"
# 服务派发的目标键（flow_worker._dispatch_service_task：plaita:{subtype}:queue）。
# 本仓无 approval:queue 的消费者（审批服务不从这里收任务），故污染它只影响
# worker 的 RPUSH——正是本场景要的确定性派发失败。
APPROVAL_QUEUE="plaita:approval:queue"

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
  docker exec "$REDIS_C" redis-cli DEL "$APPROVAL_QUEUE" >/dev/null 2>&1 || true
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

# ---- create + publish an approval flow --------------------------------------
curl -s -X POST "$API/api/flows" -H 'Content-Type: application/json' \
  -d '{"flow_id":"e2e-dlq-flow","author":"chaos"}' >/dev/null
curl -s -X PUT "$API/api/flows/e2e-dlq-flow/versions/1.0.0" -H 'Content-Type: application/json' \
  -d '{"definition":"{\"id\":\"e2e-dlq-flow\",\"name\":\"dlq\",\"nodes\":[{\"id\":\"start\",\"type\":\"start\",\"next\":\"approval\"},{\"id\":\"approval\",\"type\":\"approval\",\"approval_title\":\"D\",\"approval_content\":\"dlq\",\"approvers\":[\"e2e\"],\"event_type\":\"approval_decision\",\"next\":\"end\"},{\"id\":\"end\",\"type\":\"end\",\"result_type\":\"success\"}]}","created_by":"chaos"}' >/dev/null
curl -s -X POST "$API/api/flows/e2e-dlq-flow/publish" -H 'Content-Type: application/json' \
  -d '{"version":"1.0.0"}' >/dev/null

# ---- 污染派发目标键：挂起时的 RPUSH 必失败（WRONGTYPE）-----------------------
WT=$(docker exec "$REDIS_C" redis-cli SET "$APPROVAL_QUEUE" "chaos-wrongtype" 2>/dev/null | tr -d '\r')
[[ "$WT" == "OK" ]] || { echo "[chaos-dlq] failed to poison $APPROVAL_QUEUE (got: ${WT:-none})" >&2; exit 5; }

curl -s -X POST "$API/api/executions" -H 'Content-Type: application/json' \
  -d '{"flow_id":"e2e-dlq-flow","version":"1.0.0","params":{}}' >/dev/null

EXEC_ID=""
for _i in $(seq 1 20); do
  EXEC_ID=$(curl -s "$API/api/executions?flow_id=e2e-dlq-flow" | python3 -c "
import sys, json
d = json.load(sys.stdin)
ex = d.get('executions') or []
print(ex[0].get('execution_id', '')) if ex else print('')
" 2>/dev/null)
  [[ -n "$EXEC_ID" ]] && break
  sleep 1
done
[[ -n "$EXEC_ID" ]] || { echo "[chaos-dlq] execution did not appear" >&2; exit 1; }

# 前置：worker 已把执行落成 suspended（派发失败发生在落盘**之后**，见
# _process_execution_result 的 is_suspend 分支）——没到 suspended 就说明
# 前置链路没跑起来，后面的断言不可信。
SUSP=0
ST=""
for _i in $(seq 1 30); do
  ST=$(curl -s "$API/api/executions/$EXEC_ID" | python3 -c "
import sys, json
print(json.load(sys.stdin).get('status', ''))
" 2>/dev/null)
  [[ "$ST" == "suspended" ]] && { SUSP=1; break; }
  sleep 1
done
if [[ "$SUSP" -ne 1 ]]; then
  echo "[chaos-dlq] FAIL: execution never suspended (current: ${ST:-unknown})" >&2
  exit 1
fi
echo "[chaos-dlq] execution suspended: $EXEC_ID — 派发目标被污染，起清道夫..."

docker run -d --name e2e-dlq-scavenger --network "$NET" \
  -e E2E_TEST_REDIS_HOST=plaita-e2e-redis \
  -e PLAITA_REDIS_URL=redis://plaita-e2e-redis:6379/0 \
  -e PLAITA_CLAIM_MIN_IDLE_MS=3000 \
  -e PLAITA_MAX_DELIVERIES=1 \
  plaita-e2e-worker:latest >/dev/null || { echo "[chaos-dlq] scavenger start failed" >&2; exit 5; }

FAIL=0

# ---- assert 1: 消息被回收并收容进 DLQ ----------------------------------------
DLQ=0
N=0
for _i in $(seq 1 30); do
  N=$(docker exec "$REDIS_C" redis-cli XLEN "$DLQ_KEY" 2>/dev/null | tr -d '\r')
  [[ "${N:-0}" -ge 1 ]] && { DLQ=1; break; }
  sleep 1
done
if [[ "$DLQ" -eq 1 ]]; then
  echo "[chaos-dlq] OK: 派发失败的消息重投耗尽后进了 DLQ (XLEN=$N)"
else
  echo "[chaos-dlq] FAIL: DLQ empty" >&2
  FAIL=1
fi

# 佐证（非断言）：worker 日志里有派发失败（确认 DLQ 来源是本场景，而非别的失败）
FOUND=0
for _c in $(docker ps --format '{{.Names}}' 2>/dev/null | grep 'worker' || true); do
  docker logs --tail 300 "$_c" 2>&1 | grep -q "挂起任务投递失败" && { FOUND=1; break; }
done
if [[ "$FOUND" -eq 1 ]]; then
  echo "[chaos-dlq] 佐证: worker 日志确认挂起任务投递失败（ServiceDispatchError）"
else
  echo "[chaos-dlq] 提示: worker 日志尾部未见派发失败记录（可能已滚出 tail 窗口）"
fi

# ---- assert 2: 执行仍非终态（派发失败不终态化）-------------------------------
NT=0
ST=""
for _i in $(seq 1 10); do
  ST=$(curl -s "$API/api/executions/$EXEC_ID" | python3 -c "
import sys, json
print(json.load(sys.stdin).get('status', ''))
" 2>/dev/null)
  case "$ST" in
    suspended|running) NT=1; break ;;
    error|completed|cancelled) break ;;
  esac
  sleep 1
done
if [[ "$NT" -eq 1 ]]; then
  echo "[chaos-dlq] OK: 执行未被误终态化（status=$ST，重投/人工仍可救）"
else
  echo "[chaos-dlq] FAIL: 执行被终态化（status=${ST:-unknown}）——派发失败不该封死执行" >&2
  FAIL=1
fi

if [[ "$FAIL" -eq 0 ]]; then
  echo "[chaos-dlq] CHAOS PASSED (dispatch failure contained in DLQ + execution not terminalized)"
  exit 0
fi
echo "[chaos-dlq] CHAOS FAILED" >&2
exit 1
