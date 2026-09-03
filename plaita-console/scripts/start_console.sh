#!/usr/bin/env bash
# plaita-console 本地一键启动脚本
#
# 用法: ./start_console.sh [start|stop|status|restart|fg]
#   start   后台启动（默认），日志 /tmp/plaita-console.log，PID /tmp/plaita-console.pid
#   fg      前台启动（Ctrl-C 退出）
#   stop    停止 console（不影响已拉起的 flow_worker 等引擎服务）
#   status  查看运行状态
#
# 环境变量均可覆盖，以下为本地开发默认值：
#   PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN=true   管理面免密（仅本机开发！）
#                                              局域网/生产请改设 PLAITA_CONSOLE_ADMIN_API_KEY
#   PLAITA_CONSOLE_NODE_MODULES / NODE_PATH    外部节点模块（plaita_nodes 需要 agentproc
#                                              与 plaita-nodes、mediaflow 路径配合）
#   PLAITA_CONSOLE_SECRET_ID / SECRET_KEY      对外契约接口 /api/flowVersion/semver/detail
#                                              的 HMAC 密钥，未设置时该接口 fail-closed
set -euo pipefail

CONSOLE_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BACKEND_DIR="${CONSOLE_ROOT}/backend"
LOG_FILE="${PLAITA_CONSOLE_LOG:-/tmp/plaita-console.log}"
PID_FILE="${PLAITA_CONSOLE_PIDFILE:-/tmp/plaita-console.pid}"

PYTHON="${PLAITA_CONSOLE_PYTHON:-/opt/homebrew/bin/python3}"
[ -x "$PYTHON" ] || PYTHON="$(command -v python3)"

export PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN="${PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN:-true}"
export PLAITA_CONSOLE_REDIS_URL="${PLAITA_CONSOLE_REDIS_URL:-redis://localhost:6379/0}"
export PLAITA_CONSOLE_NODE_PATH="${PLAITA_CONSOLE_NODE_PATH:-$HOME/projects/infra4agent/mediaflow:$HOME/projects/infra4agent/plaita-nodes/src:$HOME/projects/infra4agent/agentproc/sdk/python/src}"
export PLAITA_CONSOLE_NODE_MODULES="${PLAITA_CONSOLE_NODE_MODULES:-plaita_nodes,plaita_flows.nodes}"

console_pid() {
  if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    cat "$PID_FILE"
    return 0
  fi
  lsof -tiTCP:"${PLAITA_CONSOLE_PORT:-8080}" -sTCP:LISTEN 2>/dev/null | head -1
}

do_start() {
  if [ -n "$(console_pid)" ]; then
    echo "console 已在运行 (pid $(console_pid))，如需重启请用 restart"
    exit 0
  fi
  cd "$BACKEND_DIR"
  if [ "${1:-}" = "fg" ]; then
    exec "$PYTHON" run.py
  fi
  nohup "$PYTHON" run.py > "$LOG_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  echo "console 启动中 (pid $(cat "$PID_FILE"))，日志: $LOG_FILE"
  for _ in $(seq 1 20); do
    sleep 1
    if curl -sf --max-time 2 "http://127.0.0.1:${PLAITA_CONSOLE_PORT:-8080}/api/cluster/instances" >/dev/null 2>&1; then
      echo "console 就绪: http://127.0.0.1:${PLAITA_CONSOLE_PORT:-8080}"
      return 0
    fi
  done
  echo "警告: 20s 内未就绪，请查看 $LOG_FILE"
  exit 1
}

do_stop() {
  local pid
  pid="$(console_pid)"
  if [ -z "$pid" ]; then
    echo "console 未在运行"
    rm -f "$PID_FILE"
    return 0
  fi
  kill "$pid"
  echo "已停止 console (pid $pid)；引擎服务（flow_worker 等）不受影响"
  rm -f "$PID_FILE"
}

do_status() {
  local pid
  pid="$(console_pid)"
  if [ -n "$pid" ]; then
    echo "console 运行中 (pid $pid)"
    curl -s --max-time 3 "http://127.0.0.1:${PLAITA_CONSOLE_PORT:-8080}/api/cluster/instances" \
      | python3 -c "import json,sys; [print(' ', i['service_type'], i['status']) for i in json.load(sys.stdin).get('instances', [])]" \
      || echo "  (集群实例状态获取失败)"
  else
    echo "console 未在运行"
  fi
}

case "${1:-start}" in
  start)          do_start ;;
  fg)             do_start fg ;;
  stop)           do_stop ;;
  restart)        do_stop; sleep 1; do_start ;;
  status)         do_status ;;
  *) echo "用法: $0 [start|stop|status|restart|fg]"; exit 1 ;;
esac
