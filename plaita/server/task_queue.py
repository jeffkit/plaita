"""Redis Stream task queue for FlowWorker / EventFilter (at-least-once + DLQ)."""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

logger = logging.getLogger("plaita.server.task_queue")

DEFAULT_CONSUMER_GROUP = "plaita-workers"
PAYLOAD_FIELD = "payload"
DEFAULT_CLAIM_MIN_IDLE_MS = 60_000
DEFAULT_MAX_DELIVERIES = 5
DEFAULT_DLQ_SUFFIX = ":dlq"

# 消息信封版本：**语义**版本的唯一入口。字段只增不改语义时不 bump；
# 一旦改了同名字段的含义/必需性，必须 bump，让仍按旧语义解析的消费者
# 明确拒收（进 DLQ）而不是「猜着跑」。缺该字段 = v1（历史消息）。
TASK_SCHEMA_VERSION = 1
SCHEMA_FIELD = "schema_version"
# C4-1：DLQ 保留条数。DLQ 是纯记录流（无消费者），不裁剪会无限增长；
# 每次入队后 XTRIM 保留最近 N 条。env 可调（PLAITA_DLQ_MAX_LEN）。
DEFAULT_DLQ_MAX_LEN = 1000


def _dlq_max_len_from_env() -> int:
    raw = (os.environ.get("PLAITA_DLQ_MAX_LEN") or "").strip()
    if not raw:
        return DEFAULT_DLQ_MAX_LEN
    try:
        return max(1, int(raw))
    except ValueError:
        logger.warning("PLAITA_DLQ_MAX_LEN=%r 非法，回退默认 %d", raw, DEFAULT_DLQ_MAX_LEN)
        return DEFAULT_DLQ_MAX_LEN
# redis 瞬断（DNS 解析失败/连接拒绝）后的重试退避：退避完返回 None 让主循环
# 继续轮询，Redis 恢复后下一次 read 自动重连恢复消费。模块常量便于测试归零。
RECONNECT_BACKOFF_SECONDS = 1.0
# 队列残留回收（#43）：单轮 sweep 扫描/删除的条目数上限。批量有界 ⇒ 单次
# Redis 往返有界（sweep 跑在消费线程里）；剩余残留下轮继续回收。
DEFAULT_RESIDUE_SWEEP_BATCH = 256


@dataclass(frozen=True)
class StreamTask:
    """One task read from the stream (pending until acked)."""

    message_id: str
    body: Dict[str, Any]
    delivery_count: int = 1
    # 消息信封版本（缺失/非法 → v1）。消费者据此拒收「比自己新」的语义
    schema_version: int = TASK_SCHEMA_VERSION


def _decode(value: Union[str, bytes]) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _schema_version_from_fields(fields: Dict[Any, Any]) -> int:
    """信封版本解析：缺失（历史消息）→ v1；非法值 → v1 + 告警（不因信封坏掉吞消息）。"""
    raw = fields.get(SCHEMA_FIELD, fields.get(SCHEMA_FIELD.encode()))
    if raw is None:
        return TASK_SCHEMA_VERSION
    try:
        return int(_decode(raw))
    except (TypeError, ValueError):
        logger.warning("任务信封 schema_version=%r 无法解析，按 v%d 处理", raw, TASK_SCHEMA_VERSION)
        return TASK_SCHEMA_VERSION


def _payload_from_fields(fields: Dict[Any, Any]) -> str:
    for key in (PAYLOAD_FIELD, b"payload"):
        if key in fields:
            return _decode(fields[key])
    raise ValueError("missing payload field")


def enqueue_task(redis_client, stream_key: str, task: Dict[str, Any]) -> str:
    """Append a start/resume task. Creates the stream if needed.

    信封带 ``schema_version``（与 payload 分离：payload 是领域消息，信封是
    传输契约）。旧消费者忽略未知字段；新消费者对「比自己新」的版本拒收。
    """
    msg_id = redis_client.xadd(
        stream_key,
        {
            PAYLOAD_FIELD: json.dumps(task, ensure_ascii=False),
            SCHEMA_FIELD: str(TASK_SCHEMA_VERSION),
        },
    )
    return _decode(msg_id)


def dlq_stream_key(stream_key: str, suffix: str = DEFAULT_DLQ_SUFFIX) -> str:
    return f"{stream_key}{suffix}"


class RedisStreamTaskQueue:
    """Consumer-group queue with reclaim + dead-letter after max deliveries."""

    def __init__(
        self,
        redis_client,
        stream_key: str,
        *,
        group_name: str = DEFAULT_CONSUMER_GROUP,
        consumer_name: str = "worker-1",
        claim_min_idle_ms: int = DEFAULT_CLAIM_MIN_IDLE_MS,
        max_deliveries: int = DEFAULT_MAX_DELIVERIES,
        dlq_key: Optional[str] = None,
        dlq_max_len: Optional[int] = None,
        claim_guard: Optional[Callable[[StreamTask], bool]] = None,
        dead_letter_guard: Optional[Callable[[StreamTask], bool]] = None,
        max_schema_version: Optional[int] = TASK_SCHEMA_VERSION,
        on_dead_letter: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        # 拒收「比自己新」的消息（进 DLQ）是**默认**行为：语义不兼容时宁可显式
        # 死信+告警，也不要按旧语义猜着跑。None 关闭校验（仅供测试/特殊场景）。
        self.max_schema_version = max_schema_version
        self.redis = redis_client
        self.stream_key = stream_key
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.claim_min_idle_ms = claim_min_idle_ms
        self.max_deliveries = max(1, int(max_deliveries))
        self.dlq_key = dlq_key or dlq_stream_key(stream_key)
        # C4-1：DLQ 裁剪上限（构造参数优先，其次 env，最后默认值）
        if dlq_max_len is not None:
            self.dlq_max_len = max(1, int(dlq_max_len))
        else:
            self.dlq_max_len = _dlq_max_len_from_env()
        # #47 claim 闸门（XCLAIM 前的活跃性判定）：``_reclaim_one`` 原来只按
        # idle 判空闲，不看该 pending 对应的执行**是否还有活持有者**——长步骤
        # 的 pending 消息于是每 claim_min_idle_ms 被同伴抢一次，delivery_count
        # 虚增到 max_deliveries；触顶后即便死信守卫拦下，被拦的条目仍每轮被
        # 抢一次（idle 归零刷新、计数继续涨）。闸门返回 False = 该条现在不该
        # 被抢：**不 XCLAIM、不计数**，消息留 pending 等下一轮（XPENDING 读数
        # 稳定）。None（默认）恒放行——存量行为零变化。
        self.claim_guard = claim_guard
        # Track B 任务1（2026-10-02 评审）：死信守卫。长步处理中的消息会被
        # 同伴按 idle 阈值反复抢走，delivery_count 随之虚增到 max_deliveries；
        # 若此时无条件死信（内部 XACK 原消息），存活持有者一崩，执行永久
        # 失去恢复机会。守卫（如「执行租约仍被持有」）在任何 dead_letter
        # 决策前调用：True=允许死信；False 或抛异常=跳过（消息留 pending）。
        # None（默认）恒放行——存量行为零变化。
        self.dead_letter_guard = dead_letter_guard
        # #26 告警钩子：死信只 logger.error 时值守全靠人刷日志。钩子在**入队
        # 之后**调用（事件已持久化，钩子失败不回滚死信语义），且 best-effort
        # ——WebhookAlerter.send 是非阻塞入队，绝不反压消费主循环。
        self.on_dead_letter = on_dead_letter
        self._metrics = {
            "enqueued": 0,
            "acked": 0,
            "reclaimed": 0,
            "dead_lettered": 0,
            "lease_conflicts": 0,
            "poison_acked": 0,
            "failed": 0,
            "dlq_guard_skipped": 0,
            "claim_guard_skipped": 0,
            "residue_swept": 0,
        }

    def ensure_group(self) -> None:
        try:
            self.redis.xgroup_create(
                self.stream_key,
                self.group_name,
                id="0",
                mkstream=True,
            )
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def enqueue(self, task: Dict[str, Any]) -> str:
        msg_id = enqueue_task(self.redis, self.stream_key, task)
        self._metrics["enqueued"] += 1
        return msg_id

    def read(self, block_ms: int = 10_000) -> Optional[StreamTask]:
        """Read one task: reclaim stale pending first, then new messages.

        Messages that already exceeded ``max_deliveries`` are moved to the DLQ
        and acked (not returned).
        """
        try:
            return self._read_once(block_ms)
        except RedisConnectionError:
            # 瞬断（DNS 解析失败/连接拒绝）不得炸穿 run() 主循环——2026-09 的
            # 修复只容忍了 BLOCK 到期的 TimeoutError，漏了这一支：容器网络抖动
            # /Redis 重启窗口内 worker 直接退出（e2e-chaos-redis.sh 实测复现）。
            # 退避后返回 None 继续轮询，Redis 恢复后自动重连恢复消费。
            time.sleep(RECONNECT_BACKOFF_SECONDS)
            return None

    def _read_once(self, block_ms: int) -> Optional[StreamTask]:
        self.ensure_group()
        # Drain any over-delivered pending into DLQ before serving work.
        # 超限条目的处置（死信或守卫拒绝后的跳过）与 claim 闸门（#47，租约活
        # 着就不抢）内联在 _reclaim_one 的单轮扫描里——扫描天然收敛，无需外层
        # while 重扫（重扫会在 claim_min_idle_ms 极小时对守卫拒绝的条目空转，
        # 见 _reclaim_one）。
        reclaimed = self._reclaim_one()
        if reclaimed is not None:
            self._metrics["reclaimed"] += 1
            return reclaimed

        try:
            resp = self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumer_name,
                streams={self.stream_key: ">"},
                count=1,
                block=block_ms,
            )
        except RedisTimeoutError:
            # redis-py 5+ 在 BLOCK 到期时抛 TimeoutError（旧版返回空列表）。
            # 空轮询是常态，必须返回 None 让上层继续循环——
            # 否则异常炸穿 run() 主循环，worker 启动数秒后即退出。
            return None
        task = self._parse_read_response(resp, delivery_count=1)
        if task is not None and self.max_schema_version is not None and task.schema_version > self.max_schema_version:
            # 语义上无法理解的更新消息：拒收并留证据（DLQ 含 message_id/payload），
            # 绝不按旧语义硬跑——升级期「先升消费者再升生产者」的兜底防线。
            self._metrics["schema_rejected"] = self._metrics.get("schema_rejected", 0) + 1
            self.dead_letter(
                task,
                reason=(
                    f"unsupported schema_version={task.schema_version} "
                    f"(this build understands <= {self.max_schema_version})"
                ),
            )
            return None
        if task is not None and task.delivery_count >= self.max_deliveries:
            if self._guard_allows_dead_letter(task):
                self.dead_letter(task, reason=f"max_deliveries={self.max_deliveries}")
            return None
        return task

    def ack(self, message_id: str) -> None:
        acked = self.redis.xack(self.stream_key, self.group_name, message_id)
        self._metrics["acked"] += 1
        if acked:
            # C4-1：ack 成功后从 Stream 精确删除该条目，否则已确认消息永驻
            # 内存（stream 只增不减，长跑 worker 的 Redis 占用线性膨胀）。
            # 安全性已核实：XACK 把条目移出本消费组 PEL 后，XDEL 仅清理条目
            # 本体；对已 ack 的条目 XDEL 不会影响任何 pending 语义。本仓
            # 每 Stream 只挂一个消费组（plaita-workers），不存在其他组 PEL
            # 引用被删条目的路径。best-effort：XDEL 失败只影响空间回收，
            # 不回滚 ack（消息语义已终结）。
            try:
                self.redis.xdel(self.stream_key, message_id)
            except Exception as exc:
                logger.debug(
                    "xdel(%s, %s) failed (ack already applied): %s",
                    self.stream_key,
                    message_id,
                    exc,
                )

    def _group_last_delivered_id(self) -> Optional[str]:
        """本消费组的 ``last-delivered-id``（组不存在/读不到则 None）。"""
        for group in self.redis.xinfo_groups(self.stream_key):
            if not isinstance(group, dict):
                continue
            if _decode(group.get("name")) == self.group_name:
                return _decode(group.get("last-delivered-id"))
        return None

    def sweep_acked_residue(self, batch_size: int = DEFAULT_RESIDUE_SWEEP_BATCH) -> int:
        """兜底回收「已 ack 未 XDEL」的残留条目（#43），返回删除条数。

        ack() 的 XDEL 是 best-effort：XACK 与 XDEL 之间进程被杀就留下已确认
        却未删除的条目（XDEL 全仓库只出现在 ack() 与本方法，故残留只可能来自
        这个窗口——跳过一次 ack 的条目留在 PEL 里是 pending，本方法不碰）
        ——只增不减地计入 XLEN，把运维的「积压」读数读成假阳性（2026-10-07
        实测误判）。本方法把这些条目按 ack() 同样的语义幂等回收，判据两条
        同时成立才删：

        - id ≤ 消费组 ``last-delivered-id``：已交付；大于它的条目尚未投递，
          是**合法积压**，必须保留；
        - 不在本组 PEL 中：已确认；pending（已交付未 ack）条目语义不变，
          绝不删（读不到 PEL 时本轮整体跳过——宁留残留不误删）。

        与 ack() 同一假设：每 Stream 只挂一个消费组。best-effort：任何
        Redis 失败只记 debug 并返回 0，不得影响消费主循环。单轮只扫 id 最小的
        ``batch_size`` 条：窗口里全是 pending 时本轮删 0 条，等它们被 ack 后
        的下轮继续收敛（收敛速率 = batch_size / 扫描间隔）。
        """
        batch_size = max(1, int(batch_size))
        try:
            last_delivered = self._group_last_delivered_id()
            if not last_delivered:
                return 0
            entries = self.redis.xrange(
                self.stream_key, min="-", max=last_delivered, count=batch_size
            )
        except Exception as exc:
            logger.debug("残留回收扫描失败（best-effort，忽略）: %s", exc)
            return 0
        if not entries:
            return 0
        ids = [_decode(entry[0]) for entry in entries]
        try:
            pending = self.redis.xpending_range(
                self.stream_key,
                self.group_name,
                min=ids[0],
                max=ids[-1],
                count=batch_size,
            )
        except Exception as exc:
            logger.debug("残留回收读 PEL 失败（本轮跳过）: %s", exc)
            return 0
        pending_ids = {
            _decode(entry.get("message_id") if isinstance(entry, dict) else entry[0])
            for entry in pending or []
        }
        victims = [mid for mid in ids if mid not in pending_ids]
        if not victims:
            return 0
        try:
            deleted = int(self.redis.xdel(self.stream_key, *victims) or 0)
        except Exception as exc:
            logger.debug("残留回收 XDEL 失败（下轮重试）: %s", exc)
            return 0
        self._metrics["residue_swept"] += deleted
        if deleted:
            logger.info(
                "队列残留回收：XDEL %d 条已 ack 未删条目（%s）",
                deleted,
                self.stream_key,
            )
        return deleted

    def _claim_guard_allows_claim(self, task: StreamTask) -> bool:
        """XCLAIM 前的活跃性闸门（#47）。

        无闸门（默认 None）恒 True——存量行为零变化。闸门返回 False 表示
        「这条 pending 现在不该被抢」（典型判据：它对应的执行仍有活持有者），
        于是**跳过**该条：不 XCLAIM、不递增 delivery_count、idle 也不刷新，
        消息留 pending 等闸门放行的下一轮。闸门抛异常同样按「不放行」处理：
        抢消息是不可逆动作（XCLAIM 即改写归属与投递计数），拿不准宁可
        下一轮再试。
        """
        guard = self.claim_guard
        if guard is None:
            return True
        try:
            allowed = bool(guard(task))
        except Exception as exc:
            self._metrics["claim_guard_skipped"] += 1
            logger.warning(
                "claim_guard raised for %s (delivery_count=%s); "
                "skip claim, message stays pending: %s",
                task.message_id,
                task.delivery_count,
                exc,
            )
            return False
        if not allowed:
            self._metrics["claim_guard_skipped"] += 1
        return allowed

    def _guard_allows_dead_letter(self, task: StreamTask) -> bool:
        """dead_letter 决策前的守卫闸门（Track B 任务1）。

        无守卫（默认 None）恒 True——存量行为零变化。守卫返回 False 或
        抛异常一律按「拒绝死信」处理：warning 日志、消息留在 pending 等
        待下一轮处置、``dlq_guard_skipped`` 计数。典型守卫是「执行租约
        是否仍被存活 worker 持有」：持有中说明持有者正常处理长步，此时
        死信会让该执行永久失去 resume 机会。
        """
        guard = self.dead_letter_guard
        if guard is None:
            return True
        try:
            allowed = bool(guard(task))
        except Exception as exc:
            self._metrics["dlq_guard_skipped"] += 1
            logger.warning(
                "dead_letter_guard raised for %s (delivery_count=%s); "
                "skip dead-letter, message stays pending: %s",
                task.message_id,
                task.delivery_count,
                exc,
            )
            return False
        if not allowed:
            self._metrics["dlq_guard_skipped"] += 1
            logger.warning(
                "dead_letter_guard refused %s (delivery_count=%s); "
                "skip dead-letter, message stays pending",
                task.message_id,
                task.delivery_count,
            )
        return allowed

    def dead_letter(self, task: StreamTask, *, reason: str) -> str:
        """Move task payload to DLQ stream and ack the original message."""
        envelope = {
            "reason": reason,
            "source_stream": self.stream_key,
            "source_id": task.message_id,
            "delivery_count": task.delivery_count,
            "dead_lettered_at": time.time(),
            "payload": task.body,
        }
        dlq_id = self.redis.xadd(
            self.dlq_key,
            {PAYLOAD_FIELD: json.dumps(envelope, ensure_ascii=False)},
        )
        # C4-1：DLQ 无消费者，入队后裁剪只保留最近 N 条，防无限增长。
        try:
            self.redis.xtrim(self.dlq_key, maxlen=self.dlq_max_len, approximate=False)
        except Exception as exc:
            logger.debug("xtrim(%s, maxlen=%s) failed: %s", self.dlq_key, self.dlq_max_len, exc)
        self.ack(task.message_id)
        self._metrics["dead_lettered"] += 1
        logger.error(
            "Dead-lettered task %s -> %s id=%s (%s)",
            task.message_id,
            self.dlq_key,
            _decode(dlq_id),
            reason,
        )
        # #26：死信事件外发（webhook 等）。best-effort——钩子抛错只记 warning，
        # 死信本身已落 DLQ，不得因告警通道故障影响 at-least-once 语义。
        if self.on_dead_letter is not None:
            try:
                self.on_dead_letter({
                    "event": "dead_letter",
                    "queue": self.stream_key,
                    "dlq_key": self.dlq_key,
                    "dlq_id": _decode(dlq_id),
                    "message_id": task.message_id,
                    "delivery_count": task.delivery_count,
                    "reason": reason,
                    "ts": time.time(),
                })
            except Exception as exc:  # noqa: BLE001 - 告警旁路
                logger.warning("dead_letter 告警钩子失败: %s", exc)
        return _decode(dlq_id)

    def note_lease_conflict(self) -> None:
        self._metrics["lease_conflicts"] += 1

    def note_poison(self) -> None:
        self._metrics["poison_acked"] += 1

    def note_failed(self) -> None:
        self._metrics["failed"] += 1

    def stats(self) -> Dict[str, Any]:
        """Operational snapshot (best-effort; fake/partial Redis ok)."""
        pending_count = 0
        stream_len = 0
        dlq_len = 0
        try:
            stream_len = int(self.redis.xlen(self.stream_key) or 0)
        except Exception as exc:
            logger.debug("xlen(%s) failed: %s", self.stream_key, exc)
        try:
            dlq_len = int(self.redis.xlen(self.dlq_key) or 0)
        except Exception as exc:
            logger.debug("xlen(%s) failed: %s", self.dlq_key, exc)
        try:
            summary = self.redis.xpending(self.stream_key, self.group_name)
            if isinstance(summary, dict):
                pending_count = int(summary.get("pending", 0) or 0)
            elif summary:
                pending_count = int(summary[0] or 0)
        except Exception as exc:
            logger.debug("xpending(%s, %s) failed: %s", self.stream_key, self.group_name, exc)
        return {
            "stream_key": self.stream_key,
            "dlq_key": self.dlq_key,
            "group": self.group_name,
            "consumer": self.consumer_name,
            "stream_length": stream_len,
            "pending": pending_count,
            "dlq_length": dlq_len,
            "max_deliveries": self.max_deliveries,
            "claim_min_idle_ms": self.claim_min_idle_ms,
            **self._metrics,
        }

    def _parse_read_response(self, resp, delivery_count: int = 1) -> Optional[StreamTask]:
        if not resp:
            return None
        _stream, messages = resp[0]
        if not messages:
            return None
        msg_id, fields = messages[0]
        return self._task_from_fields(_decode(msg_id), fields, delivery_count)

    def _task_from_fields(
        self, message_id: str, fields: Dict[Any, Any], delivery_count: int
    ) -> StreamTask:
        try:
            body = json.loads(_payload_from_fields(fields))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            logger.error("Invalid task payload id=%s: %s", message_id, exc)
            raise ValueError(f"Invalid task payload: {exc}") from exc
        if not isinstance(body, dict):
            raise ValueError("task payload must be a JSON object")
        return StreamTask(
            message_id=message_id,
            body=body,
            delivery_count=delivery_count,
            schema_version=_schema_version_from_fields(fields),
        )

    def _pending_delivery_count(self, message_id: str) -> int:
        try:
            pending = self.redis.xpending_range(
                self.stream_key,
                self.group_name,
                min=message_id,
                max=message_id,
                count=1,
            )
        except Exception as exc:
            logger.debug("xpending_range delivery count failed for %s: %s", message_id, exc)
            return 1
        if not pending:
            return 1
        entry = pending[0]
        if isinstance(entry, dict):
            return int(entry.get("times_delivered") or 1)
        # tuple: (id, consumer, idle, deliveries)
        if len(entry) >= 4:
            return int(entry[3] or 1)
        return 1

    def _peek_pending_task(self, message_id: str, delivery_count: int) -> Optional[StreamTask]:
        """只读窥视一条 pending 条目的本体（不 XCLAIM、不动归属/计数）。

        ``xpending_range`` 只返回 id/consumer/idle/times_delivered，而回收前的
        判定（如「执行租约是否仍被持有」#47）需要消息体。XRANGE 按 id 精确读
        一条，拿到即按与回收路径**同一规则**解析成 StreamTask。任何失败
        （条目已 XDEL / Redis 瞬断 / 载荷损坏）返回 None，调用方按存量路径
        继续 XCLAIM——窥视只用于「提前放弃回收」，绝不改变既有处置。
        """
        try:
            entries = self.redis.xrange(
                self.stream_key, min=message_id, max=message_id, count=1
            )
        except Exception as exc:
            logger.debug("回收前窥视 %s 失败（按存量路径继续）: %s", message_id, exc)
            return None
        if not entries:
            return None
        try:
            return self._task_from_fields(
                _decode(entries[0][0]), entries[0][1], delivery_count=delivery_count
            )
        except (TypeError, ValueError):
            # 载荷损坏：留给存量 XCLAIM 路径按同样语义处置
            return None

    def _reclaim_one(self) -> Optional[StreamTask]:
        """抢回一条 idle 超阈的 pending 消息（一条；无则 None）。

        扫描窗口（8 条）内逐条处置：

        - claim 闸门（``claim_guard`` #47）放行才 XCLAIM——不放行的条目**碰都不
          碰**（归属/计数/idle 全不变），继续扫下一条；
        - 超 ``max_deliveries`` 的条目**就地**处置，且死信去留（``dead_letter_guard``）
          在 XCLAIM **之前**判定：守卫拒绝（False/异常）说明这条按守卫语义就该
          留 pending 等终态，抢它没有任何处置收益——只会把 idle 归零、把投递数
          继续推高（#47 的 churn 噪音），故连 XCLAIM 都不做；守卫允许才 XCLAIM
          并随后死信。
        - 两种处置都继续扫下一条 pending。处置必须在同一轮扫描内完成而不能
          交给调用方重扫：XCLAIM 会把条目 idle 归零，``claim_min_idle_ms`` 极
          小时（如单测的 1ms）下一轮扫描会立刻再抢到同一条目，外层循环空转。

        两个闸门都缺省（None）时不做窥视，路径与存量逐字一致。
        """
        try:
            pending = self.redis.xpending_range(
                self.stream_key,
                self.group_name,
                min="-",
                max="+",
                count=8,
            )
        except Exception as exc:
            logger.debug("xpending_range reclaim scan failed: %s", exc)
            return None
        if not pending:
            return None

        peek_enabled = self.claim_guard is not None or self.dead_letter_guard is not None
        for entry in pending:
            if isinstance(entry, dict):
                msg_id = entry.get("message_id")
                idle = entry.get("time_since_delivered", 0)
                deliveries = int(entry.get("times_delivered") or 1)
            else:
                msg_id, _consumer, idle, deliveries = entry[0], entry[1], entry[2], entry[3]
                deliveries = int(deliveries or 1)
            if idle < self.claim_min_idle_ms:
                continue
            msg_id_str = _decode(msg_id)
            # 窥视一次拿到消息体：两个闸门都只需要消息体（归属/计数不变）。
            pending_task = (
                self._peek_pending_task(msg_id_str, max(deliveries, 1))
                if peek_enabled
                else None
            )
            # 超限条目的死信去留：已判定（None=未判定，如窥视失败/未超限）。
            # 守卫带副作用（重入队恢复副本 + 冷却闸），一轮内**只调一次**。
            pre_dead_letter_allowed: Optional[bool] = None
            if pending_task is not None:
                if not self._claim_guard_allows_claim(pending_task):
                    continue
                if pending_task.delivery_count >= self.max_deliveries:
                    pre_dead_letter_allowed = self._guard_allows_dead_letter(pending_task)
                    if not pre_dead_letter_allowed:
                        continue
            claimed = self.redis.xclaim(
                self.stream_key,
                self.group_name,
                self.consumer_name,
                min_idle_time=self.claim_min_idle_ms,
                message_ids=[msg_id_str],
            )
            if not claimed:
                continue
            mid, fields = claimed[0]
            # delivery_count 语义（2026-10-03 澄清，勿「顺手修」成 +1）：
            # `deliveries` = XCLAIM 前的 times_delivered（已完成派发次数），
            # 本次 XCLAIM 是第 deliveries+1 次投递，故这里上报的是
            # 「即将进行的这次处理」的**前一次**序号——看似差一，但两侧
            # `>= max_deliveries` 死信检查（本函数与 FlowWorker.run 兜底）
            # 净值恰为「max_deliveries 次处理后死信」，与「最大投递次数」
            # 契约一致。改成 +1 会让死信提前一轮（5 次变 4 次）。该值仅作
            # 阈值判据与可观测透传（FlowWorker 节点重试预算另用 Redis 计数
            # 键，不依赖它），报告展示时理解为 attempt-1 即可。
            task = self._task_from_fields(
                _decode(mid), fields, delivery_count=max(deliveries, 1)
            )
            if task.delivery_count >= self.max_deliveries:
                allowed = (
                    pre_dead_letter_allowed
                    if pre_dead_letter_allowed is not None
                    else self._guard_allows_dead_letter(task)
                )
                if allowed:
                    self.dead_letter(
                        task,
                        reason=f"max_deliveries={self.max_deliveries}",
                    )
                continue
            return task
        return None
