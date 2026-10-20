"""事件/订阅索引的有界清扫（台账 r70 A69-03）。

背景：事件类型索引（ZSET）与订阅四索引（SET）在写入时续期 TTL，但**成员**
不会随数据键过期而消失。活跃流量下桶永不整体过期，``zcard``/``scard`` 会
持续高于 live 数；``list_events``/``list_subscriptions`` 的 N+1 与「载荷
缺失」ERROR 随 churn 放大。

修复分两层：

1. **读时惰性清理**（``RedisEventStorage.list_events`` /
   ``RedisEventSubscriptionStorage.list_subscriptions``）：被选中的桶里确认
   数据缺失的成员当次 ``ZREM``/``SREM``——热桶立即收敛；
2. **有界 janitor**（本模块）：``EventFilter`` 生命周期挂载，周期性 ``SCAN``
   索引键 + ``ZSCAN``/``SSCAN`` 游标，每 tick 只检查固定成员数
   （``PLAITA_EVENT_INDEX_SWEEP_BUDGET``，默认 500），覆盖惰性清理够不到的
   冷桶（``flow:``/``node:``）。

原则：

* 只按 ``EXISTS data`` 判死活，**不按 score 裁剪**——事件 zset 的 score 是
  生产者可控的时间戳，按 score 会误删「时间戳老但载荷仍活」的事件；
* 游标只存在进程内（不新建永久 Redis 键），重启从头扫，幂等；
* 预算语义是「检查的成员数」而非「删除数」，高存活桶也不会单 tick 无界；
* 单桶失败只 WARNING 并计入 ``event_index_sweep_errors_total``，不终止循环；
* 关闭开关 ``PLAITA_DISABLE_EVENT_INDEX_SWEEP=1`` 必须在启动时 WARNING：
  这是「过期成员继续累积」的已知降级，不得静默。
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
from typing import Any, Dict, Optional, Tuple

from plaita.core.url_redaction import _redact_env_value, _scrub_exception_text

logger = logging.getLogger("plaita.event.index_janitor")

DEFAULT_SWEEP_INTERVAL_SECONDS = 60.0
DEFAULT_SWEEP_BUDGET = 500

SWEEP_INTERVAL_ENV = "PLAITA_EVENT_INDEX_SWEEP_INTERVAL"
SWEEP_BUDGET_ENV = "PLAITA_EVENT_INDEX_SWEEP_BUDGET"
SWEEP_DISABLE_ENV = "PLAITA_DISABLE_EVENT_INDEX_SWEEP"

#: 进程级指标（无 Prometheus 依赖的 event 层；由日志与 :func:`index_sweep_metrics`
#: 读取。不得为此 import ``plaita.server.metrics``——event 不得依赖 server）。
_METRICS: Dict[str, int] = {
    "event_index_sweep_examined_total": 0,
    "event_index_sweep_pruned_total": 0,
    "event_index_sweep_errors_total": 0,
}


def index_sweep_metrics() -> Dict[str, int]:
    """返回清扫指标的副本（测试/运维入口）。"""
    return dict(_METRICS)


def reset_index_sweep_metrics() -> None:
    """重置进程级指标（测试用）。"""
    for key in _METRICS:
        _METRICS[key] = 0


def index_sweep_disabled() -> bool:
    return os.environ.get(SWEEP_DISABLE_ENV, "").strip() == "1"


def _resolve_interval(value: Optional[float]) -> float:
    raw: Any = value if value is not None else os.environ.get(SWEEP_INTERVAL_ENV)
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return DEFAULT_SWEEP_INTERVAL_SECONDS
    try:
        seconds = float(raw)
    except (TypeError, ValueError):
        logger.warning(
            "%s=%s 非法（需正数秒），清扫周期回退默认 %.0fs",
            SWEEP_INTERVAL_ENV, _redact_env_value(raw), DEFAULT_SWEEP_INTERVAL_SECONDS,
        )
        return DEFAULT_SWEEP_INTERVAL_SECONDS
    if seconds <= 0 or not math.isfinite(seconds):
        # A78-04：``nan``/``inf`` 通过 ``float()`` 但会让 ``wait_for`` 的定时器
        # 永不触发——清扫变成「启动扫一轮后静默停摆」，且原日志只是 INFO。
        logger.warning(
            "%s=%s 非法（需有限正数秒），清扫周期回退默认 %.0fs",
            SWEEP_INTERVAL_ENV, _redact_env_value(raw), DEFAULT_SWEEP_INTERVAL_SECONDS,
        )
        return DEFAULT_SWEEP_INTERVAL_SECONDS
    return seconds


def _resolve_budget(value: Optional[int]) -> int:
    raw: Any = value if value is not None else os.environ.get(SWEEP_BUDGET_ENV)
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return DEFAULT_SWEEP_BUDGET
    try:
        budget = int(raw)
    except (TypeError, ValueError):
        logger.warning(
            "%s=%s 非法（需整数），清扫预算回退默认 %d 成员/tick",
            SWEEP_BUDGET_ENV, _redact_env_value(raw), DEFAULT_SWEEP_BUDGET,
        )
        return DEFAULT_SWEEP_BUDGET
    if budget <= 0:
        # 规格：``<=0`` 视为 1（保留「每 tick 至少检查一个」的收敛性，
        # 而不是悄悄变成无界或完全停摆）。
        logger.warning(
            "%s=%d 非正数，按 1 成员/tick 执行（不静默停摆）",
            SWEEP_BUDGET_ENV, budget,
        )
        return 1
    return budget


class IndexJanitor:
    """周期性、有界地清理事件/订阅索引中的过期成员。

    Args:
        event_storage: 需实现 ``prune_indexes(budget)``（RedisEventStorage）。
        subscription_storage: 需实现 ``prune_indexes(budget)``
            （RedisEventSubscriptionStorage）；内存后端没有该方法时跳过。
        interval_seconds: 清扫周期；None 取 ``PLAITA_EVENT_INDEX_SWEEP_INTERVAL``
            （默认 60s）。
        budget: 每 tick 每个存储最多检查的成员数；None 取
            ``PLAITA_EVENT_INDEX_SWEEP_BUDGET``（默认 500，``<=0`` 按 1）。
    """

    def __init__(
        self,
        event_storage=None,
        subscription_storage=None,
        interval_seconds: Optional[float] = None,
        budget: Optional[int] = None,
    ):
        self.event_storage = event_storage
        self.subscription_storage = subscription_storage
        self.interval_seconds = _resolve_interval(interval_seconds)
        self.budget = _resolve_budget(budget)
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._stopped: Optional[asyncio.Event] = None

    async def start(self) -> None:
        """启动后台清扫任务（禁用时只 WARNING，不挂载）。"""
        if index_sweep_disabled():
            logger.warning(
                "索引清扫已关闭（%s=1）：事件/订阅索引的过期成员将继续累积，"
                "zcard/scard 可持续高于 live 数；这是已知降级，恢复需移除该环境变量",
                SWEEP_DISABLE_ENV,
            )
            return
        if self._running:
            return
        self._running = True
        self._stopped = asyncio.Event()
        self._task = asyncio.create_task(self._run_loop())
        logger.info(
            "索引清扫已挂载（周期 %.0fs，预算 %d 成员/tick）",
            self.interval_seconds, self.budget,
        )

    async def stop(self) -> None:
        """停止后台任务（有界等待，超时取消）。"""
        if not self._running:
            return
        self._running = False
        if self._stopped is not None:
            self._stopped.set()
        task, self._task = self._task, None
        if task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=5.0)
            except asyncio.TimeoutError:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001 — 停止失败不阻断关停主链路
                logger.warning("停止索引清扫任务失败: %s", _scrub_exception_text(exc))

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self.sweep_once()
            except Exception as exc:  # noqa: BLE001 — 清扫失败不终止循环
                _METRICS["event_index_sweep_errors_total"] += 1
                logger.warning("索引清扫本轮失败（下一轮继续）: %s", _scrub_exception_text(exc))
            if self._stopped is None:
                return
            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=self.interval_seconds
                )
            except asyncio.TimeoutError:
                continue

    async def sweep_once(self) -> Tuple[int, int, int]:
        """执行一轮清扫，返回 ``(examined, pruned, errors)``。

        对事件与订阅两个存储分别调用 ``prune_indexes(budget=...)``；某个存储
        失败只记 WARNING + errors，不影响另一个存储。
        """
        if index_sweep_disabled():
            return 0, 0, 0

        examined = pruned = errors = 0
        for label, storage in (
            ("事件", self.event_storage),
            ("订阅", self.subscription_storage),
        ):
            prune = getattr(storage, "prune_indexes", None) if storage is not None else None
            if prune is None:
                continue
            try:
                ex, pr, err = await prune(budget=self.budget)
            except Exception as exc:  # noqa: BLE001 — 单存储失败不阻断另一个
                errors += 1
                logger.warning(
                    "索引清扫: %s索引失败（本 tick 跳过，下 tick 重试）: %s",
                    label, _scrub_exception_text(exc),
                )
                continue
            examined += int(ex)
            pruned += int(pr)
            errors += int(err)

        _METRICS["event_index_sweep_examined_total"] += examined
        _METRICS["event_index_sweep_pruned_total"] += pruned
        _METRICS["event_index_sweep_errors_total"] += errors
        if examined or pruned or errors:
            logger.info(
                "索引清扫: 检查 %d 个成员，移除 %d 个过期成员，错误 %d",
                examined, pruned, errors,
            )
        return examined, pruned, errors
