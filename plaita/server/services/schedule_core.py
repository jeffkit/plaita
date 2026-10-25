"""调度面共享核心（纯时间函数，2026-10-25 触发面收敛第一步）。

**单一事实源**：cron 校验/下次触发计算（croniter，本地时区）+ 到期时点规范
化 + 派发幂等键。两个消费方——集群档 ``schedule_service``（Redis hash + 独立
触发进程）与本地档 console ``local_scheduler``（SQLite + 进程内循环）——语义
必须一致的部分全部住这里；存储与派发（Stream 入队 vs engine backend 起跑）
是各自适配器的事，不进核心。

**共享语义（两侧合同，改动须同 PR 同步两侧文档）**：

- cron 5 段（minute hour day month weekday），**本地时区**解释（naive
  datetime + ``timestamp()``，非 UTC）；
- 错过的周期**不补偿**：到期后只触发一次，``next_run_at`` 直接跳到当前时间
  之后的下一个 cron 时点（停机一小时不会连环补跑）；
- due 幂等：集群档 = :func:`build_dedup_key`（``sched:{id}:due:{due_ms}``，
  worker 侧 SET NX 只放行一次）；本地档 = ``next_run_at`` 条件更新（A48-02，
  先推进再执行）。键形不同（存储适配），「同一 due 只触发一次」的合同相同。

**为什么抽本模块**：console 的 local_scheduler 此前从 ``schedule_service``
import 这两个函数——为两个纯函数把 redis/dedup/触发面整包拖进 import 图。
核心下沉后 console 只依赖本模块（零 redis 依赖）；``schedule_service`` 再导出
保持公开 import 路径守恒。

**刻意不做跨面合并**：唤醒面（delay/approval 等"在跑的执行"的唤醒）与触发面
（本模块服务的"没开始的执行"的启动）数据模型与故障域都不同——唤醒面合并见
:mod:`plaita.server.services.wake`；调度真相 DB 化（消掉 Redis hash/SQLite
双源）另单，方向对齐 E2「runtime 单真相=DB 列」。
"""
from __future__ import annotations

import itertools
import time
from datetime import datetime
from typing import Any, Dict, Optional

from croniter import croniter

from ...logger import logger
from ...core.url_redaction import _scrub_url_text

#: manual 键的进程内序号（原 schedule_service 实现原样搬迁）。
_MANUAL_SEQ = itertools.count()

__all__ = [
    "validate_cron",
    "next_run_after",
    "normalized_due_ms",
    "build_dedup_key",
]


def validate_cron(cron: str) -> bool:
    """校验 cron 表达式（5 段，minute hour day month weekday）。"""
    if not cron or not isinstance(cron, str):
        return False
    try:
        croniter(cron, datetime.now())
        return True
    except (ValueError, KeyError) as e:
        logger.debug("非法 cron 表达式 %r: %s", cron, _scrub_url_text(e))
        return False


def next_run_after(cron: str, dt: Optional[datetime] = None) -> int:
    """cron 在 dt（默认现在，本地时区）之后的下一次触发，返回 epoch 毫秒。

    注意必须走 get_next(datetime) + naive.timestamp()（按本地时区解释）；
    get_next(float) 会把 naive 基准当 UTC，导致触发时间偏移时区。
    """
    it = croniter(cron, dt or datetime.now())
    nxt = it.get_next(datetime)
    return int(nxt.timestamp() * 1000)


def normalized_due_ms(schedule: Dict[str, Any], due_at: Any = None) -> Optional[int]:
    """本次到期时点的规范表示：epoch 毫秒整数，取不到返回 None。

    优先取调用方显式传入的 ``due_at``（cron 侧即 ``_fire_if_due`` 判定用的
    ``next_run_at``），其次取调度定义里的 ``next_run_at``。规范化固定为
    ``int(float(...))``：同一 due 的等价写法（``"1791530071848"`` /
    ``1791530071848`` / ``1791530071848.0``）必须得到同一个键。
    """
    for candidate in (due_at, schedule.get("next_run_at")):
        if candidate is None or candidate == "":
            continue
        try:
            return int(float(candidate))
        except (TypeError, ValueError):
            continue
    return None


def build_dedup_key(
    schedule_id: str,
    trigger_kind: str,
    due_ms: Optional[int],
    wall_second: int,
) -> str:
    """构造派发幂等键（#23 引入，plaita#118 修正取值基准；键空间两段不相交）。

    - **cron**：``sched:{schedule_id}:due:{due_ms}``。``due_ms`` = **本次到期
      时点**（见 :func:`normalized_due_ms`）。同一个 due 无论被触发几次（状态
      回写失败 + 30s 锁过期重触发、消息 XCLAIM 重派）都得到**同一个键**，
      worker 侧 ``SET NX`` 只放行一次；不同 due 的键必然不同，不会被误吞。
    - **manual**（console「立即触发」）：``sched:{schedule_id}:manual:{ns}-{seq}``，
      每次调用唯一——manual 是用户显式要求「立刻再跑一次」，不得与 cron 同秒
      互相吞并；同一条 manual 消息被重派时键随消息走，仍收敛。
    - **降级**：cron 且 ``due_ms`` 取不到时退回
      ``sched:{schedule_id}:fired:{墙钟秒}`` 并由调用方 warning（调度循环会先
      自愈 ``next_run_at``，该路径只在定义缺失且调用方未传 due 时可达）。
    """
    if trigger_kind == "manual":
        return "sched:{schedule_id}:manual:{ns}-{seq}".format(
            schedule_id=schedule_id, ns=time.time_ns(), seq=next(_MANUAL_SEQ)
        )
    if due_ms is not None:
        return "sched:{schedule_id}:due:{due_ms}".format(
            schedule_id=schedule_id, due_ms=due_ms
        )
    return "sched:{schedule_id}:fired:{wall_second}".format(
        schedule_id=schedule_id, wall_second=wall_second
    )
