#!/usr/bin/env bash
# e2e-chaos-dlq.sh — 混沌回归：必失败任务经重投收容进 DLQ，可观察。
#
# 场景：approval 挂起执行上投 resume_type=continue——内核拒绝绕过挂起节点
# （ResumeError → RuntimeError），任务不 ack 留 pending；「清道夫」worker
# （PLAITA_CLAIM_MIN_IDLE_MS=3000 + PLAITA_MAX_DELIVERIES=1，独立容器，
# 不污染共享组主 worker 的旋钮）3s 后回收重投 → 首败即 dead_letter。
# 断言：执行终态 error + DLQ stream length ≥ 1。
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

# ---- create + publish + start an approval flow, wait until suspended --------
curl -s -X POST "$API/api/flows" -H 'Content-Type: application/json' \
  -d '{"flow_id":"e2e-dlq-flow","author":"chaos"}' >/dev/null
curl -s -X PUT "$API/api/flows/e2e-dlq-flow/versions/1.0.0" -H 'Content-Type: application/json' \
  -d '{"definition":"{\"id\":\"e2e-dlq-flow\",\"name\":\"dlq\",\"nodes\":[{\"id\":\"start\",\"type\":\"start\",\"next\":\"approval\"},{\"id\":\"approval\",\"type\":\"approval\",\"approval_title\":\"D\",\"approval_content\":\"dlq\",\"approvers\":[\"e2e\"],\"event_type\":\"approval_decision\",\"next\":\"end\"},{\"id\":\"end\",\"type\":\"end\",\"result_type\":\"success\"}]}","created_by":"chaos"}' >/dev/null
curl -s -X POST "$API/api/flows/e2e-dlq-flow/publish" -H 'Content-Type: application/json' \
  -d '{"version":"1.0.0"}' >/dev/null
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
echo "[chaos-dlq] execution suspended: $EXEC_ID — resume continue + scavenger worker..."

curl -s -X POST "$API/api/executions/$EXEC_ID/resume" -H 'Content-Type: application/json' \
  -d '{"resume_type":"continue"}' >/dev/null
docker run -d --name e2e-dlq-scavenger --network "$NET" \
  -e E2E_TEST_REDIS_HOST=plaita-e2e-redis \
  -e PLAITA_REDIS_URL=redis://plaita-e2e-redis:6379/0 \
  -e PLAITA_CLAIM_MIN_IDLE_MS=3000 \
  -e PLAITA_MAX_DELIVERIES=1 \
  plaita-e2e-worker:latest >/dev/null || { echo "[chaos-dlq] scavenger start failed" >&2; exit 5; }

FAIL=0

# ---- assert 1: execution ends in error ---------------------------------------
ERR=0
ST=""
for _i in $(seq 1 30); do
  ST=$(curl -s "$API/api/executions/$EXEC_ID" | python3 -c "
import sys, json
print(json.load(sys.stdin).get('status', ''))
" 2>/dev/null)
  [[ "$ST" == "error" ]] && { ERR=1; break; }
  sleep 1
done
if [[ "$ERR" -eq 1 ]]; then
  echo "[chaos-dlq] OK: kernel rejected continue, execution terminal=error"
else
  echo "[chaos-dlq] FAIL: execution not error (current: ${ST:-unknown})" >&2
  FAIL=1
fi

# ---- assert 2: DLQ received the task (poll XLEN) ------------------------------
DLQ=0
N=0
for _i in $(seq 1 30); do
  N=$(docker exec "$REDIS_C" redis-cli XLEN plaita:flow:queue:dlq 2>/dev/null | tr -d '\r')
  [[ "${N:-0}" -ge 1 ]] && { DLQ=1; break; }
  sleep 1
done
if [[ "$DLQ" -eq 1 ]]; then
  echo "[chaos-dlq] OK: failed task dead-lettered (XLEN=$N)"
else
  echo "[chaos-dlq] FAIL: DLQ empty" >&2
  FAIL=1
fi

if [[ "$FAIL" -eq 0 ]]; then
  echo "[chaos-dlq] CHAOS PASSED (kernel reject + DLQ)"
  exit 0
fi
echo "[chaos-dlq] CHAOS FAILED" >&2
exit 1
