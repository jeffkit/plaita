"""Redis Stream task queue for FlowWorker / EventFilter (at-least-once + DLQ)."""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

logger = logging.getLogger("plaita.server.task_queue")

DEFAULT_CONSUMER_GROUP = "plaita-workers"
PAYLOAD_FIELD = "payload"
DEFAULT_CLAIM_MIN_IDLE_MS = 60_000
DEFAULT_MAX_DELIVERIES = 5
DEFAULT_DLQ_SUFFIX = ":dlq"
# redis 瞬断（DNS 解析失败/连接拒绝）后的重试退避：退避完返回 None 让主循环
# 继续轮询，Redis 恢复后下一次 read 自动重连恢复消费。模块常量便于测试归零。
RECONNECT_BACKOFF_SECONDS = 1.0


@dataclass(frozen=True)
class StreamTask:
    """One task read from the stream (pending until acked)."""

    message_id: str
    body: Dict[str, Any]
    delivery_count: int = 1


def _decode(value: Union[str, bytes]) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _payload_from_fields(fields: Dict[Any, Any]) -> str:
    for key in (PAYLOAD_FIELD, b"payload"):
        if key in fields:
            return _decode(fields[key])
    raise ValueError("missing payload field")


def enqueue_task(redis_client, stream_key: str, task: Dict[str, Any]) -> str:
    """Append a start/resume task. Creates the stream if needed."""
    msg_id = redis_client.xadd(
        stream_key,
        {PAYLOAD_FIELD: json.dumps(task, ensure_ascii=False)},
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
    ):
        self.redis = redis_client
        self.stream_key = stream_key
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.claim_min_idle_ms = claim_min_idle_ms
        self.max_deliveries = max(1, int(max_deliveries))
        self.dlq_key = dlq_key or dlq_stream_key(stream_key)
        self._metrics = {
            "enqueued": 0,
            "acked": 0,
            "reclaimed": 0,
            "dead_lettered": 0,
            "lease_conflicts": 0,
            "poison_acked": 0,
            "failed": 0,
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
        while True:
            reclaimed = self._reclaim_one()
            if reclaimed is None:
                break
            if reclaimed.delivery_count >= self.max_deliveries:
                self.dead_letter(
                    reclaimed,
                    reason=f"max_deliveries={self.max_deliveries}",
                )
                continue
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
        if task is not None and task.delivery_count >= self.max_deliveries:
            self.dead_letter(task, reason=f"max_deliveries={self.max_deliveries}")
            return None
        return task

    def ack(self, message_id: str) -> None:
        self.redis.xack(self.stream_key, self.group_name, message_id)
        self._metrics["acked"] += 1

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
        self.ack(task.message_id)
        self._metrics["dead_lettered"] += 1
        logger.error(
            "Dead-lettered task %s -> %s id=%s (%s)",
            task.message_id,
            self.dlq_key,
            _decode(dlq_id),
            reason,
        )
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
            message_id=message_id, body=body, delivery_count=delivery_count
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

    def _reclaim_one(self) -> Optional[StreamTask]:
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
            # XCLAIM increments delivery count; use pending's count + 1 as best effort
            return self._task_from_fields(
                _decode(mid), fields, delivery_count=max(deliveries, 1) + 0
            )
        return None
