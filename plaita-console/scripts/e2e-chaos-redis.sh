#!/usr/bin/env bash
# e2e-chaos-redis.sh — 混沌回归：Redis 短时断连，worker/console 不得暴毙。
#
# 回归 phase2.8 断点③：redis-py 5+ 把阻塞超时从「返回 None」改为「抛
# TimeoutError」，引擎三处循环（控制通道 pubsub / XREADGROUP 主循环 /
# 事件总线订阅）当初全部未接——worker 5 秒必退。修复契约：瞬断期间
# 进程存活，恢复后继续消费。
#
# 为什么不做成 suite 用例：断连需要对容器做宿主机侧 docker network 手术，
# argusai 的 exec 步骤只在容器内执行、YAML 表达不了，故独立成脚本。
#
# 流程：起环境 → 断连 Redis 12s → 断言 worker/console 容器仍存活 → 重连
# （恢复 network alias）→ 创建/发布/启动简单 flow → 轮询到 completed
# （证明 worker 恢复消费，不只是活着）。
#
# 容器/网络名与 plaita-console/e2e.yaml 耦合（isolation.namespace 未启用时的
# 确定性命名）。用法：scripts/e2e-chaos-redis.sh [--no-build]。退出码：0 绿 / 1 红。

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
REDIS_C="plaita-console-plaita-e2e-redis"
WORKER_C="plaita-console-plaita-e2e-worker"
CONSOLE_C="plaita-console-plaita-console-backend-e2e"
API="http://localhost:18080"
OUTAGE_SECONDS=12

# ---- 前置 -------------------------------------------------------------------
docker info >/dev/null 2>&1 || { echo "[chaos] docker daemon 未运行" >&2; exit 3; }
[[ -x "$HOME/.local/bin/mcp2cli" || -x "/usr/local/bin/mcp2cli" || -x "/opt/homebrew/bin/mcp2cli" ]] \
  || { echo "[chaos] mcp2cli 未安装" >&2; exit 3; }

MCP2CLI=""
for _c in "$HOME/.local/bin/mcp2cli" "/usr/local/bin/mcp2cli" "/opt/homebrew/bin/mcp2cli"; do
  [[ -x "$_c" ]] && { MCP2CLI="$_c"; break; }
done
ARGUSAI_MCP_BIN=""
for _root in "$(npm root -g 2>/dev/null)" "$HOME/.local/share/fnm/node-versions"/*/installation/lib/node_modules; do
  [[ -f "$_root/argusai-mcp/dist/index.js" ]] && { ARGUSAI_MCP_BIN="$_root/argusai-mcp/dist/index.js"; break; }
done
[[ -n "$ARGUSAI_MCP_BIN" ]] || { echo "[chaos] argusai-mcp 未安装" >&2; exit 3; }

SESSION="plaita-chaos-$$"
_argus() { "$MCP2CLI" --session "$SESSION" "$@" 2>&1; }
cleanup() {
  _argus argus-clean --project-path "$E2E_PROJECT" >/dev/null 2>&1 || true
  "$MCP2CLI" --session-stop "$SESSION" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---- 起环境（与 e2e-run.sh 同款生命周期）------------------------------------
"$MCP2CLI" --mcp-stdio "node $ARGUSAI_MCP_BIN" --session-start "$SESSION" >/dev/null 2>&1
_argus argus-init --project-path "$E2E_PROJECT" >/dev/null 2>&1 || { echo "[chaos] init 失败" >&2; exit 5; }
[[ "$NO_BUILD" -eq 1 ]] || _argus argus-build --project-path "$E2E_PROJECT" >/dev/null 2>&1 || true
_argus argus-setup --project-path "$E2E_PROJECT" >/dev/null 2>&1 || { echo "[chaos] setup 失败" >&2; exit 5; }
for _i in $(seq 1 60); do
  curl -sf -m 2 "$API/health" >/dev/null 2>&1 && break
  [[ "$_i" -eq 60 ]] && { echo "[chaos] backend 未就绪" >&2; exit 5; }
  sleep 1
done
echo "[chaos] 环境就绪，开始注入故障：断连 Redis ${OUTAGE_SECONDS}s"

# ---- 故障注入 ----------------------------------------------------------------
docker network disconnect "$NET" "$REDIS_C" || { echo "[chaos] 断连失败" >&2; exit 5; }
sleep "$OUTAGE_SECONDS"

# ---- 断言 1：瞬断期间进程存活（断点③核心契约）--------------------------------
_alive() { [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == "true" ]]; }
FAIL=0
if ! _alive "$WORKER_C"; then
  echo "[chaos] ✗ worker 在断连期间退出（断点③回归失败：5 秒暴毙复现）" >&2
  FAIL=1
else
  echo "[chaos] ✓ worker 瞬断期间存活"
fi
if ! _alive "$CONSOLE_C"; then
  echo "[chaos] ✗ console 在断连期间退出" >&2
  FAIL=1
else
  echo "[chaos] ✓ console 瞬断期间存活"
fi

# ---- 恢复：重连并找回 network alias（disconnect 会丢别名）--------------------
docker network connect --alias plaita-e2e-redis "$NET" "$REDIS_C" || { echo "[chaos] 重连失败" >&2; exit 5; }
for _i in $(seq 1 30); do
  curl -sf -m 2 "$API/health" >/dev/null 2>&1 && break
  sleep 1
done
echo "[chaos] Redis 已重连，验证恢复消费..."

# ---- 断言 2：恢复后全链路可跑（worker 恢复消费，不只是活着）-------------------
curl -s -X POST "$API/api/flows" -H 'Content-Type: application/json' \
  -d '{"flow_id":"e2e-chaos-flow","author":"chaos"}' >/dev/null
curl -s -X PUT "$API/api/flows/e2e-chaos-flow/versions/1.0.0" -H 'Content-Type: application/json' \
  -d '{"definition":"{\"id\":\"e2e-chaos-flow\",\"name\":\"chaos\",\"nodes\":[{\"id\":\"start\",\"type\":\"start\",\"next\":\"end\"},{\"id\":\"end\",\"type\":\"end\",\"output\":\"$INPUT.value\",\"result_type\":\"success\"}]}","created_by":"chaos"}' >/dev/null
curl -s -X POST "$API/api/flows/e2e-chaos-flow/publish" -H 'Content-Type: application/json' -d '{"version":"1.0.0"}' >/dev/null
curl -s -X POST "$API/api/executions" -H 'Content-Type: application/json' \
  -d '{"flow_id":"e2e-chaos-flow","version":"1.0.0","params":{"value":1}}' >/dev/null

DONE=0
for _i in $(seq 1 60); do
  ST=$(curl -s "$API/api/executions?flow_id=e2e-chaos-flow" | python3 -c "
import sys, json
d = json.load(sys.stdin)
ex = d.get('executions') or []
print(ex[0].get('status', '')) if ex else print('')
" 2>/dev/null)
  [[ "$ST" == "completed" ]] && { DONE=1; break; }
  sleep 1
done
if [[ "$DONE" -eq 1 ]]; then
  echo "[chaos] ✓ 恢复后执行跑通至 completed（worker 恢复消费）"
else
  echo "[chaos] ✗ 恢复后执行未完成（最后状态: ${ST:-未知}）——worker 存活但未恢复消费" >&2
  FAIL=1
fi

if [[ "$FAIL" -eq 0 ]]; then
  echo "[chaos] CHAOS PASSED ✓（瞬断存活 + 恢复消费）"
  exit 0
fi
echo "[chaos] CHAOS FAILED ✗" >&2
exit 1
