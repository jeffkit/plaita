"""事件→resume 链的兜底回扫（Track C 任务3，2026-10 分布式可靠性修复）。

现状（为什么需要 reconcile）：
- ``RedisEventBus.publish`` = 存事件（``plaita:event:types:{type}`` zset 索引，
  score=事件时间戳，TTL 7 天）+ Redis Pub/Sub 通知（plaita/event/redis.py:807-812）；
- EventFilter 只经 ``register_handler`` 的 Pub/Sub 推送消费
  （plaita/server/event_filter.py:289-292 → redis.py ``_listen_for_*``）。
  Pub/Sub 不持久：EventFilter 重启/重连窗口内的事件通知丢了即永失——
  事件在存储里明明躺 7 天，却没有任何回扫补偿（全仓 ``list_events`` 无
  消费方），错过通知的挂起执行就此僵尸。

本类周期性 + 启动回填地扫描事件存储时间窗，对每条事件调用
``EventFilter.handle_event``：其 SET NX 去重键
（``plaita:event_filter:dedup:{event_id}:{subscription_id}``）保证推送链路
已处理的事件不会被重复 resume（幂等，at-least-once 兜底安全）。游标
（上次扫到的时间戳）记在 Redis，跨重启避免重复扫。

回滚开关：``PLAITA_DISABLE_EVENT_RECONCILE=1``（EventFilter.start 不挂载）。
周期/回填窗口可用 ``PLAITA_EVENT_RECONCILE_INTERVAL`` /
``PLAITA_EVENT_RECONCILE_BACKFILL``（秒）覆盖默认值。

任何扫描/回放异常都吞掉打 warning，绝不影响 Pub/Sub 推送主链路。
"""
import asyncio
import logging
import os
import time
from typing import Optional

logger = logging.getLogger("plaita.server.event_reconcile")

# 默认周期 300s、启动回填最近 1h（可经环境变量覆盖）。
DEFAULT_INTERVAL_SECONDS = 300.0
DEFAULT_BACKFILL_SECONDS = 3600.0
# 每轮回扫的最大事件数（list_events 按 types zset 走 zrangebyscore，
# 限制单轮工作量；剩余部分下一轮从游标继续）。
DEFAULT_BATCH_SIZE = 200
# 游标键 TTL：7 天自清理（仓内键惯例）。游标丢失只导致从回填窗口重扫，
# handle_event 的去重键保证幂等，不会重复 resume。
CURSOR_TTL_SECONDS = 7 * 86400


class EventReconciler:
    """事件存储兜底回扫器：补偿 Pub/Sub 通知丢失窗口内错过的事件。"""

    CURSOR_KEY = "plaita:event_reconcile:cursor"

    def __init__(
        self,
        event_storage,
        event_filter,
        redis_client=None,
        interval_seconds: Optional[float] = None,
        backfill_seconds: Optional[float] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        """
        Args:
            event_storage: 事件存储（需实现 list_events(start_time, end_time, limit)，
                即 RedisEventBus.event_storage / MemoryEventStorage 均可）。
            event_filter: EventFilter 实例（复用其 handle_event——resume 任务
                组装、终态 GC、SET NX 去重全在里面）。
            redis_client: 同步 Redis 客户端（游标存取）；None 时游标退化为
                进程内 None（每轮回填窗口重扫，幂等性仍由去重键保证）。
            interval_seconds: 扫描周期（秒）；None 取 PLAITA_EVENT_RECONCILE_INTERVAL。
            backfill_seconds: 启动回填/游标丢失时的回扫窗口（秒）；
                None 取 PLAITA_EVENT_RECONCILE_BACKFILL。
            batch_size: 每轮最大扫描事件数。
        """
        self.event_storage = event_storage
        self.event_filter = event_filter
        self.redis_client = redis_client
        self.interval_seconds = float(
            interval_seconds
            if interval_seconds is not None
            else os.environ.get("PLAITA_EVENT_RECONCILE_INTERVAL")
            or DEFAULT_INTERVAL_SECONDS
        )
        self.backfill_seconds = float(
            backfill_seconds
            if backfill_seconds is not None
            else os.environ.get("PLAITA_EVENT_RECONCILE_BACKFILL")
            or DEFAULT_BACKFILL_SECONDS
        )
        self.batch_size = int(batch_size)
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def scan_once(self) -> int:
        """扫描 (游标或回填窗口起点, now] 窗口并回放，返回回放事件数。"""
        now = time.time()
        window_start = self._load_cursor()
        if window_start is None:
            window_start = now - self.backfill_seconds
        else:
            # 游标过老（停机超过回填窗口）时封顶到回填窗口——更早的事件
            # 已超出补偿窗口（事件 TTL 7 天，但挂起执行早被超时/GC 处置）。
            window_start = max(window_start, now - self.backfill_seconds)
        window_end = now

        try:
            events = await self.event_storage.list_events(
                start_time=window_start, end_time=window_end, limit=self.batch_size
            )
        except Exception as e:  # noqa: BLE001 — 扫描失败绝不影响推送主链路
            logger.warning(
                "事件回扫: 扫描事件存储失败（本轮放弃）: %s", e, exc_info=True
            )
            return 0

        replayed = 0
        for event in events:
            try:
                # handle_event 内部 SET NX 去重键保证：Pub/Sub 推送已处理的
                # 事件不会重复 resume；终态执行的残留订阅也会被就地回收。
                await self.event_filter.handle_event(event)
                replayed += 1
            except Exception as e:  # noqa: BLE001 — 单事件失败不拖累其余
                logger.warning(
                    "事件回扫: 回放事件 %s 失败（跳过）: %s", event.event_id, e,
                    exc_info=True,
                )
        # 游标推进到窗口上界（回放幂等可容忍边界事件下轮重扫；若因个别
        # 失败不推进，同一窗口会被无限重扫）。写失败只多扫不漏扫。
        self._save_cursor(window_end)
        if events:
            logger.info(
                "事件回扫: 窗口 (%.0f, %.0f] 扫到 %d 条事件，回放 %d 条",
                window_start, window_end, len(events), replayed,
            )
        return replayed

    def _load_cursor(self) -> Optional[float]:
        if self.redis_client is None:
            return None
        try:
            raw = self.redis_client.get(self.CURSOR_KEY)
        except Exception as e:  # noqa: BLE001 — 读不到就按回填窗口扫
            logger.warning("事件回扫: 读取游标失败（按回填窗口扫）: %s", e)
            return None
        if not raw:
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    def _save_cursor(self, ts: float) -> None:
        if self.redis_client is None:
            return
        try:
            self.redis_client.set(self.CURSOR_KEY, str(ts), ex=CURSOR_TTL_SECONDS)
        except Exception as e:  # noqa: BLE001 — 游标写失败只多扫不漏扫
            logger.warning("事件回扫: 写入游标失败: %s", e)

    async def start(self) -> None:
        """启动回扫后台任务（生命周期同 SubscriptionTimeoutChecker）。"""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """停止回扫后台任务。"""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self.scan_once()
            except Exception as e:  # noqa: BLE001 — 循环体任何异常不终止回扫
                logger.warning("事件回扫: 扫描循环异常（忽略）: %s", e, exc_info=True)
            await asyncio.sleep(self.interval_seconds)
