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

扫描失败（读存储）：吞掉打 warning 并放弃本轮（游标不动，多扫不漏扫）。
回放失败（#153）：本轮**整轮作废**——不推进游标、**失败事件不计入
``replayed``/``last_replayed``**（真实不变式是「失败事件永不被计成 replayed」；
失败前已成功回放的事件仍计入 ``last_replayed``——「``last_replayed == 0``」只在
**首个事件即失败**时成立，不是构造性保证），计入 ``last_rejected`` 后上抛；
``_run_loop`` 记日志并在下一轮重试，绝不影响 Pub/Sub 推送主链路。
"""
import asyncio
import logging
import os
import time
from typing import Optional, Tuple
from plaita.core.url_redaction import _scrub_exception_text, _scrub_url_text

logger = logging.getLogger("plaita.server.event_reconcile")

# 默认周期 300s、启动回填最近 1h（可经环境变量覆盖）。
DEFAULT_INTERVAL_SECONDS = 300.0
DEFAULT_BACKFILL_SECONDS = 3600.0
# 每轮回扫的最大事件数。``list_events`` 按 ``(timestamp, event_id)`` 全局升序
# 返回，本值是**限速**而不是窗口边界：一轮取满即说明窗口可能还没排空，游标只
# 推进到本轮已消费的最后一条，下一轮从这里续扫（见 ``scan_once``／#85）。
DEFAULT_BATCH_SIZE = 200
# 游标键 TTL：7 天自清理（仓内键惯例）。游标丢失只导致从回填窗口重扫，
# handle_event 的去重键保证幂等，不会重复 resume。
CURSOR_TTL_SECONDS = 7 * 86400
# 复合游标：``"<timestamp>|<event_id>"``。只存时间戳时，两个事件**同一时间戳**
# 会让游标反复取到同一个头（活锁）；event_id 是 zset member、天然唯一，让游标
# 严格前进。纯时间戳的旧游标仍可读（解析成 ``(ts, None)``）。
CURSOR_SEPARATOR = "|"
# 跳过「已消费前缀」时最多把单轮取数放大到 batch_size 的这么多倍（防御性上界：
# 同一时间戳上堆了超长前缀时不无界扫描，保留游标、下一轮再来）。
CURSOR_SKIP_MAX_FACTOR = 10


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
        #: #153 上一轮 ``scan_once`` 的计数（每轮重置）：成功回放数 / 失败数。
        #: 失败轮不推进游标且上抛；``last_replayed`` 是**失败前已成功回放**的条数
        #: （不是恒 0），``last_rejected`` 是失败事件数——供调用方与运维读出
        #: 「本轮作废」的规模。
        self.last_replayed = 0
        self.last_rejected = 0

    async def scan_once(self) -> int:
        """扫描 (游标或回填窗口起点, now] 窗口并回放，返回回放事件数。

        游标语义（#85——「窗口是否排空」是推进到窗口上界的**唯一**判据）：

        * 一轮取回 ``batch_size`` 条（同一时间戳的超长已消费前缀会让取数临时
          放大，见 :meth:`_next_page`）；**只有确认窗口已排空**（本页没有被
          存储截断，且没有因 ``batch_size`` 留下的剩余）才把游标推到
          ``window_end``；
        * 未排空 ⇒ 游标只推进到**本轮已消费的最后一条事件**
          ``(timestamp, event_id)``，下一轮从该位点续扫——旧实现每轮无条件
          ``_save_cursor(window_end)``，窗口内第 ``batch_size + 1`` 条起的事件
          再也没有窗口覆盖（永久漏扫）；
        * 本页全部是已消费事件（同一时间戳超长前缀）⇒ 至多把取数放大
          ``CURSOR_SKIP_MAX_FACTOR`` 倍重取；仍越不过就不推进、响亮告警
          （**不漏扫**，下一轮重试）。

        #153 的失败语义保持不变：任一事件回放失败即**整轮作废**——不推进游标，
        **失败事件不计入** ``replayed``/``last_replayed``，计入
        ``self.last_rejected`` 后响亮上抛（由 ``_run_loop`` 记日志并在下一轮
        重试）。游标停在原处 ⇒ 失败事件仍留在下一轮窗口内，Redis 恢复后会被
        真正重放，而不会被"失败也算成功"的游标永久跳过。

        真实不变式是「**失败事件永不被计成 replayed**」，不是「``last_replayed
        == 0``」：多事件轮里若前面已有事件成功回放，``last_replayed`` 停在成功
        条数上（例如 2 事件轮首成功、次失败 ⇒ ``(last_replayed, last_rejected)
        == (1, 1)``）；``last_replayed == 0`` 只在**首个事件即失败**时成立。
        """
        self.last_replayed = 0
        self.last_rejected = 0
        if self.batch_size <= 0:
            # #85：旧实现取 0 条却照常把游标推到 window_end（整窗被跳过）。
            logger.warning(
                "事件回扫: batch_size=%d 非法（<=0），本轮跳过、游标不动",
                self.batch_size,
            )
            return 0

        now = time.time()
        cursor = self._load_cursor()
        if cursor is None:
            window_start = now - self.backfill_seconds
        else:
            # 游标过老（停机超过回填窗口）时封顶到回填窗口——更早的事件
            # 已超出补偿窗口（事件 TTL 7 天，但挂起执行早被超时/GC 处置）。
            window_start = max(cursor[0], now - self.backfill_seconds)
        window_end = now

        page, truncated = await self._next_page(window_start, window_end, cursor)
        if page is None:
            # 读失败或同一时间戳前缀越不过：本轮放弃，游标不动（不漏扫）。
            return 0
        pending = [
            event for event in page
            if cursor is None
            or (event.timestamp, event.event_id) > cursor
        ]

        replayed = 0
        consumed = []
        for event in pending[: self.batch_size]:
            try:
                # handle_event 内部 SET NX 去重键保证：Pub/Sub 推送已处理的
                # 事件不会重复 resume；终态执行的残留订阅也会被就地回收。
                await self.event_filter.handle_event(event)
                replayed += 1
                self.last_replayed = replayed
                consumed.append(event)
            except Exception as e:
                # #153：失败**不再**被计成 replayed（`last_replayed` 停在失败前
                # 已成功的条数）、也不再推进游标。上抛即跳过下方的游标推进，
                # 本轮整轮作废；失败事件留在下一轮窗口内，Redis 恢复后被真正重放。
                self.last_rejected += 1
                logger.error(
                    "事件回扫: 回放事件 %s 失败——本轮游标不推进"
                    "（replayed=%d，rejected=%d）：%s",
                    event.event_id, replayed, self.last_rejected, _scrub_exception_text(e),
                )
                raise

        new_cursor = None
        if not truncated and len(pending) <= len(consumed):
            # 窗口已确认排空：这是**唯一**推进到窗口上界的条件。写失败只多扫
            # 不漏扫（且 _save_cursor_raw 会响亮记录）。
            self._save_cursor(window_end)
        elif consumed:
            new_cursor = (consumed[-1].timestamp, consumed[-1].event_id)
            if new_cursor is not None and new_cursor[0] > window_end:
                # 发布方时钟超前：游标不得越过窗口上界，否则后续窗口接不上。
                self._save_cursor(window_end)
            else:
                self._save_cursor_key(consumed[-1])
        # else：本页全是已消费前缀且被截断（_next_page 已告警）——不推进。

        if page:
            logger.info(
                "事件回扫: 窗口 (%.0f, %.0f] 扫到 %d 条事件，回放 %d 条，"
                "游标%s窗口上界",
                window_start, window_end, len(page), replayed,
                "推进到" if (not truncated and len(pending) <= len(consumed))
                else "停在已消费位点，未到",
            )
        return replayed

    async def _next_page(
        self, window_start: float, window_end: float, cursor: Optional[tuple],
    ) -> Tuple[Optional[list], bool]:
        """取回窗口内从 ``window_start`` 起的一页，返回 ``(page, truncated)``。

        ``page`` 按 ``(timestamp, event_id)`` 升序；``truncated`` 表示存储
        **可能还有更多**（本页取满）。若整页都是已消费的边界前缀（同一时间戳
        上堆了超长前缀），把取数翻倍重取（上限 ``CURSOR_SKIP_MAX_FACTOR`` 倍）
        以便越过前缀；越过不了则返回 ``(None, True)`` 并告警——**不推进游标、
        不漏扫**，下一轮重试。

        存储读失败返回 ``(None, False)``（既有语义：扫描失败只放弃本轮，绝不影响
        推送主链路）。
        """
        limit = self.batch_size
        while True:
            try:
                page = await self.event_storage.list_events(
                    start_time=window_start, end_time=window_end, limit=limit
                )
            except Exception as e:  # noqa: BLE001 — 扫描失败绝不影响推送主链路
                logger.warning(
                    "事件回扫: 扫描事件存储失败（本轮放弃）: %s",
                    _scrub_exception_text(e),
                )
                return None, False
            page = sorted(page, key=lambda event: (event.timestamp, event.event_id))
            # limit<=0 的一页不是「被截断」——没有可截断的条目（batch_size 守卫
            # 被绕过时不能让退化页静默宣告排空）。
            truncated = limit > 0 and len(page) >= limit
            pending = [
                event for event in page
                if cursor is None
                or (event.timestamp, event.event_id) > cursor
            ]
            if pending or not truncated:
                return page, truncated
            if limit >= self.batch_size * CURSOR_SKIP_MAX_FACTOR:
                logger.warning(
                    "事件回扫: 窗口 (%.0f, %.0f] 的已消费前缀占满 %d 条仍越不过"
                    "（同一时间戳并列），本轮不推进游标、下一轮重试",
                    window_start, window_end, limit,
                )
                return None, True
            limit *= 2

    @staticmethod
    def _parse_cursor(raw) -> Optional[tuple]:
        """解析游标：``"<ts>"``（旧格式）或 ``"<ts>|<event_id>"``（#85 复合游标）。"""
        if raw is None:
            return None
        if isinstance(raw, (bytes, bytearray)):
            try:
                raw = raw.decode()
            except Exception:  # noqa: BLE001 — 坏字节游标按「无游标」处理
                logger.warning("事件回扫: 游标不是合法 UTF-8（按回填窗口扫）")
                return None
        text = str(raw).strip()
        if not text:
            return None
        ts_part, sep, event_id = text.partition(CURSOR_SEPARATOR)
        try:
            ts = float(ts_part)
        except (TypeError, ValueError):
            return None
        # 旧格式（纯时间戳）的 event_id 视为**最早**的边界（空串），这样
        # ``(ts, event_id) > cursor`` 仍会取到该时间戳上的全部事件。
        return (ts, event_id if sep else "")

    def _load_cursor(self) -> Optional[tuple]:
        if self.redis_client is None:
            return None
        try:
            raw = self.redis_client.get(self.CURSOR_KEY)
        except Exception as e:  # noqa: BLE001 — 读不到就按回填窗口扫
            logger.warning("事件回扫: 读取游标失败（按回填窗口扫）: %s", _scrub_url_text(e))
            return None
        return self._parse_cursor(raw)

    def _save_cursor(self, ts: float) -> None:
        """把游标推到窗口上界（仅用于**确认排空**的轮次）。"""
        self._save_cursor_raw(repr(float(ts)))

    def _save_cursor_key(self, event) -> None:
        """把游标推进到「本轮已消费的最后一条事件」（未排空轮次）。"""
        self._save_cursor_raw(
            f"{event.timestamp!r}{CURSOR_SEPARATOR}{event.event_id}"
        )

    def _save_cursor_raw(self, raw: str) -> None:
        if self.redis_client is None:
            return
        cursor = self._parse_cursor(raw)
        if cursor is None:
            return
        current = self._load_cursor()
        # #85 单调守卫：陈旧写者/改写不得把游标拖回去（会整窗重扫）。
        if current is not None and cursor < (current[0], current[1]):
            logger.warning(
                "事件回扫: 拒绝回退游标 %r ← %r（游标单调）",
                cursor, current,
            )
            return
        try:
            self.redis_client.set(self.CURSOR_KEY, raw, ex=CURSOR_TTL_SECONDS)
        except Exception as e:  # noqa: BLE001 — 游标写失败只多扫不漏扫
            # #85：写失败 = 下一轮重扫（去重键保证不重复 resume），但必须
            # **响亮**——静默写失败会让运维以为续扫位点已持久化。
            logger.error(
                "事件回扫: 写入游标失败（下一轮会重扫同一窗口，去重键保证不重复 resume）: %s",
                _scrub_exception_text(e),
            )

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
                logger.warning("事件回扫: 扫描循环异常（忽略）: %s", _scrub_exception_text(e))
            await asyncio.sleep(self.interval_seconds)
