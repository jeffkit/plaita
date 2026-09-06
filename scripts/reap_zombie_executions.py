"""僵尸执行巡检/清理脚本（2026-09 分布式演练 P1-2 的运维配套）。

worker 崩溃后，pending 里的 start 任务重投会创建**全新执行**重跑副本；
旧执行永久停留在 `running` 状态且无人认领。本脚本把"超时未更新"的
running 执行标记为 error（orphaned），供监控/人工复核。

用法：
    python scripts/reap_zombie_executions.py \
        --redis-url redis://localhost:6379/0 \
        --idle-minutes 60          # running 且 last_update_time 早于 N 分钟
        [--dry-run]                # 只列出，不改动
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from plaita.storage.redis import RedisExecutionStorage  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="标记僵尸 running 执行为 error(orphaned)")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--namespace", default="plaita")
    parser.add_argument("--idle-minutes", type=int, default=60,
                        help="running 且 last_update_time 早于 N 分钟视为僵尸")
    parser.add_argument("--dry-run", action="store_true", help="只列出，不改动")
    args = parser.parse_args()

    storage = RedisExecutionStorage(redis_url=args.redis_url, namespace=args.namespace)
    executions = storage.list_executions()
    cutoff = datetime.now() - timedelta(minutes=args.idle_minutes)

    reaped = 0
    found = 0
    for state in executions:
        if (state.status or "").lower() != "running":
            continue
        updated = state.last_update_time or state.start_time
        if not updated:
            continue
        try:
            last = datetime.fromisoformat(updated)
        except ValueError:
            continue
        if last >= cutoff:
            continue
        found += 1
        print(f"[zombie] {state.execution_id} flow={state.flow_id} "
              f"last_update={state.last_update_time}")
        if not args.dry_run:
            state.status = "error"
            state.error = {
                "message": (f"orphaned: running with no update for {args.idle_minutes}m "
                            "(worker crash during start-task redelivery)")
            }
            state.end_time = datetime.now().isoformat()
            storage.save_execution_state(state.execution_id, state)
            reaped += 1

    action = "would reap" if args.dry_run else "reaped"
    print(f"{action}: {reaped} of {found} zombie execution(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
