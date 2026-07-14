"""Redis Stream task queue for FlowWorker / EventFilter (at-least-once)."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

logger = logging.getLogger("plaita.server.task_queue")

DEFAULT_CONSUMER_GROUP = "plaita-workers"
PAYLOAD_FIELD = "payload"
DEFAULT_CLAIM_MIN_IDLE_MS = 60_000


@dataclass(frozen=True)
class StreamTask:
    """One task read from the stream (pending until acked)."""

    message_id: str
    body: Dict[str, Any]


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


class RedisStreamTaskQueue:
    """Consumer-group based queue: ack after success; unacked tasks can be reclaimed."""

    def __init__(
        self,
        redis_client,
        stream_key: str,
        *,
        group_name: str = DEFAULT_CONSUMER_GROUP,
        consumer_name: str = "worker-1",
        claim_min_idle_ms: int = DEFAULT_CLAIM_MIN_IDLE_MS,
    ):
        self.redis = redis_client
        self.stream_key = stream_key
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.claim_min_idle_ms = claim_min_idle_ms

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
        return enqueue_task(self.redis, self.stream_key, task)

    def read(self, block_ms: int = 10_000) -> Optional[StreamTask]:
        """Read one task: reclaim stale pending first, then new messages."""
        self.ensure_group()
        reclaimed = self._reclaim_one()
        if reclaimed is not None:
            return reclaimed
        resp = self.redis.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_name,
            streams={self.stream_key: ">"},
            count=1,
            block=block_ms,
        )
        return self._parse_read_response(resp)

    def ack(self, message_id: str) -> None:
        self.redis.xack(self.stream_key, self.group_name, message_id)

    def _parse_read_response(self, resp) -> Optional[StreamTask]:
        if not resp:
            return None
        _stream, messages = resp[0]
        if not messages:
            return None
        msg_id, fields = messages[0]
        return self._task_from_fields(_decode(msg_id), fields)

    def _task_from_fields(self, message_id: str, fields: Dict[Any, Any]) -> StreamTask:
        try:
            body = json.loads(_payload_from_fields(fields))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            logger.error("Invalid task payload id=%s: %s", message_id, exc)
            raise ValueError(f"Invalid task payload: {exc}") from exc
        if not isinstance(body, dict):
            raise ValueError("task payload must be a JSON object")
        return StreamTask(message_id=message_id, body=body)

    def _reclaim_one(self) -> Optional[StreamTask]:
        try:
            pending = self.redis.xpending_range(
                self.stream_key,
                self.group_name,
                min="-",
                max="+",
                count=8,
            )
        except Exception:
            return None
        if not pending:
            return None

        for entry in pending:
            if isinstance(entry, dict):
                msg_id = entry.get("message_id")
                idle = entry.get("time_since_delivered", 0)
            else:
                msg_id, _consumer, idle, _ = entry
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
            return self._task_from_fields(_decode(mid), fields)
        return None
