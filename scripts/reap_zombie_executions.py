"""僵尸执行巡检/清理脚本（2026-09 分布式演练 P1-2 的运维配套）。

worker 崩溃后，pending 里的 start 任务重投会创建**全新执行**重跑副本；
旧执行永久停留在 `running` 状态且无人认领。本脚本把"超时未更新"的
running 执行标记为 error（orphaned），供监控/人工复核。

判据不能只看 last_update_time：执行状态在**单节点执行期间无心跳**——只有
`PERSIST_EVERY_N_STEPS` 步界才写回 last_update_time，start / retry_wakeup
也不更新；一个 3 小时的 AGENTRUN 长节点，其 last_update_time 可以陈旧 3
小时而执行完全健康（resume 租约正被看门狗每 40s 续）。故叠加两道闸
（2026-10 修复）：

1. 租约键 ``{ns}:execution:lease:{id}`` 存在 → 活 worker 正推进长步骤，跳过；
2. 落盘走条件写（状态键与巡检读到的原始串一致才写）→ 活 worker 的步界写若
   发生在巡检读之后，本次落盘放弃：不把它的 running/completed 覆写回 error
   （否则监控先见 error 又翻回 completed，按 error 驱动补偿的 keeper 还会
   触发真·双跑）。

修复前另有两处：构造签名（``redis_url`` 掉进 ``**kwargs`` 透传给
``redis.Redis``，新版 redis-py 直接 TypeError，脚本开箱即坏）、dry-run 汇总
把 "would reap" 恒报 0。

用法：
    python scripts/reap_zombie_executions.py \
        --redis-url redis://localhost:6379/0 \
        --idle-minutes 60          # running 且 last_update_time 早于 N 分钟
        [--dry-run]                # 只列出，不改动
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import redis  # noqa: E402

from plaita.storage.base import ExecutionState  # noqa: E402
from plaita.storage.redis import (  # noqa: E402
    RedisExecutionStorage,
    execution_index_key,
    execution_start_time_score,
    execution_state_ttl_seconds,
)

ORPHAN_REASON = "worker crash during start-task redelivery"

# 条件写：KEYS = 状态键 / 租约键 / 执行列表索引 ZSET，
# ARGV = 巡检读到的原始串 / error 原始串 / TTL 秒 / 索引 score / execution_id。
# 返回 1=已落盘、0=租约被持有（活 worker）、-1=状态已被并发改写。
# 单段 Lua 是必须的：读-判-写拆成多次往返，会被 worker 的步界写插进来。
# 索引成员也在此维护——本路径绕过 save_execution_state 直接 SET 状态键。
_CONDITIONAL_ORPHAN_LUA = """
if redis.call('exists', KEYS[2]) == 1 then
  return 0
end
if redis.call('get', KEYS[1]) ~= ARGV[1] then
  return -1
end
if tonumber(ARGV[3]) > 0 then
  redis.call('set', KEYS[1], ARGV[2], 'EX', ARGV[3])
else
  redis.call('set', KEYS[1], ARGV[2])
end
redis.call('zadd', KEYS[3], ARGV[4], ARGV[5])
return 1
"""


def build_storage(redis_url: str, namespace: str) -> RedisExecutionStorage:
    """按 Redis URL 建执行存储。

    ``RedisExecutionStorage`` 的连接参数签名是 host/port/db/password/client
    ——传 ``redis_url=`` 会掉进 ``**kwargs`` 透给 ``redis.Redis(...)``，而新版
    redis-py 的 ``Redis.__init__`` 没有 ``**kwargs``，直接 TypeError（脚本
    开箱即坏的根因）。URL 解析交给 ``Redis.from_url``（username/db/ssl 一并
    覆盖），并沿用存储自身的 ``decode_responses=True``。
    """
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    return RedisExecutionStorage(client=client, namespace=namespace)


def _lease_key(namespace: str, execution_id: str) -> str:
    return f"{namespace}:execution:lease:{execution_id}"


def _is_stale(state: ExecutionState, cutoff: datetime) -> bool:
    updated = state.last_update_time or state.start_time
    if not updated:
        return False
    try:
        return datetime.fromisoformat(updated) < cutoff
    except ValueError:
        return False


def list_all_executions(storage: RedisExecutionStorage) -> List[ExecutionState]:
    """翻页取全量执行。

    list_executions 默认按 start_time 升序取前 100 条——不翻页就永远只巡检
    最旧的 100 个执行，新僵尸漏掉。停条件用「空页」而非「短页」：短页不该被
    当成数据结束的判据（列表侧一旦返回短页，巡检会静默截断成 no-op）。
    """
    executions: List[ExecutionState] = []
    offset = 0
    while True:
        page = storage.list_executions(limit=100, offset=offset)
        if not page:
            return executions
        executions.extend(page)
        offset += len(page)


def orphan_execution(
    storage: RedisExecutionStorage,
    execution_id: str,
    cutoff: datetime,
    idle_minutes: int,
    log: Callable[[str], None] = print,
) -> bool:
    """条件写把执行终态化为 error(orphaned)；跳过或竞态时返回 False。

    落盘前**重新读一次**状态并对新副本复核判据：列表扫描到落盘之间的窗口里
    活 worker 可能已推进（last_update_time 变新）——按窗口外的旧副本落 error
    就是误杀。复核通过后再由 Lua 保证「读到的原始串 == 落盘瞬间的值」。
    """
    client = storage.client
    namespace = storage.namespace
    key = storage.get_namespace_key("execution", execution_id)
    raw = client.get(key)
    if raw is None:
        log("  ! 状态键已不存在，跳过")
        return False
    try:
        state = ExecutionState.model_validate(storage.deserialize_state(raw))
    except Exception as e:
        log(f"  ! 状态反序列化失败，跳过: {e}")
        return False
    if (state.status or "").lower() != "running" or not _is_stale(state, cutoff):
        log("  ! 状态已前进（不再超期 running），跳过")
        return False
    state.status = "error"
    state.error = {
        "message": f"orphaned: running with no update for {idle_minutes}m ({ORPHAN_REASON})"
    }
    state.end_time = datetime.now().isoformat()
    result = int(
        client.eval(
            _CONDITIONAL_ORPHAN_LUA,
            3,
            key,
            _lease_key(namespace, execution_id),
            execution_index_key(namespace),
            raw,
            storage.serialize_state(state.model_dump()),
            str(execution_state_ttl_seconds()),
            str(execution_start_time_score(state)),
            execution_id,
        )
    )
    if result == 1:
        return True
    reason = "租约被持有（活 worker）" if result == 0 else "状态已被并发改写"
    log(f"  ! 未落盘：{reason}")
    return False


def reap(
    storage: RedisExecutionStorage,
    *,
    idle_minutes: int = 60,
    dry_run: bool = False,
    now: Optional[datetime] = None,
    log: Callable[[str], None] = print,
    alert: Optional[Callable[[ExecutionState], None]] = None,
) -> Tuple[int, int]:
    """巡检并处置僵尸执行，返回 (处置数 / dry-run 拟处置数, 超期 running 数)。

    ``alert``（#26）在**真处置成功**后逐条回调（dry-run 不告警）；回调自身
    抛错不得影响巡检推进——告警只是旁路，处置结果已经落盘。
    """
    client = storage.client
    namespace = storage.namespace
    cutoff = (now or datetime.now()) - timedelta(minutes=idle_minutes)
    found = 0
    handled = 0
    for listed in list_all_executions(storage):
        if (listed.status or "").lower() != "running" or not _is_stale(listed, cutoff):
            continue
        lease = client.get(_lease_key(namespace, listed.execution_id))
        if lease:
            log(f"[skip] {listed.execution_id} flow={listed.flow_id} "
                f"last_update={listed.last_update_time} lease={lease}（活 worker）")
            continue
        found += 1
        log(f"[zombie] {listed.execution_id} flow={listed.flow_id} "
            f"last_update={listed.last_update_time}")
        if dry_run:
            handled += 1
        elif orphan_execution(storage, listed.execution_id, cutoff, idle_minutes, log):
            handled += 1
            if alert is not None:
                try:
                    alert(listed)
                except Exception as e:  # noqa: BLE001 - 告警旁路不得影响巡检
                    log(f"  ! 告警发送失败: {e}")
    return handled, found


def main() -> int:
    parser = argparse.ArgumentParser(description="标记僵尸 running 执行为 error(orphaned)")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--namespace", default="plaita")
    parser.add_argument("--idle-minutes", type=int, default=60,
                        help="running 且 last_update_time 早于 N 分钟视为僵尸")
    parser.add_argument("--dry-run", action="store_true", help="只列出，不改动")
    parser.add_argument("--alert-webhook", default=os.environ.get("PLAITA_ALERT_WEBHOOK", ""),
                        help="处置僵尸后 JSON POST 的告警 webhook（#26）；"
                             "默认跟随 PLAITA_ALERT_WEBHOOK")
    args = parser.parse_args()

    storage = build_storage(args.redis_url, args.namespace)

    alerter = None
    send_alert = None
    if args.alert_webhook:
        from plaita.server.alerts import WebhookAlerter

        alerter = WebhookAlerter(args.alert_webhook)

        def send_alert(state: ExecutionState) -> None:
            alerter.send({
                "event": "zombie_reaped",
                "execution_id": state.execution_id,
                "flow_id": state.flow_id,
                "last_update_time": state.last_update_time,
                "idle_minutes": args.idle_minutes,
                "namespace": args.namespace,
                "reason": ORPHAN_REASON,
                "ts": time.time(),
            })

    handled, found = reap(
        storage, idle_minutes=args.idle_minutes, dry_run=args.dry_run, alert=send_alert
    )
    if alerter is not None:
        alerter.close()

    action = "would reap" if args.dry_run else "reaped"
    print(f"{action}: {handled} of {found} zombie execution(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
