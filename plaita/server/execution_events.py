"""执行级事件时间线：带时间戳的节点事件 → per-execution Redis Stream。

执行状态（ExecutionState）只有整条流程的起止时间与终态快照，节点维度的时间线
只存在于 Langfuse——Langfuse 不可用/未启用时，出事后除了 state.json 的
context 没有第二个时间线来源，且「这个节点排队多久、跑了多久」在系统里没有
答案。本模块把它补上：worker 每收到一个节点生命周期回调，就向
``plaita:execution:events:{execution_id}`` 追加一条带时间戳的 Stream 记录，
并同频道 pub 一份供 SSE 实时推送。

设计要点：
- **Stream 是持久层**：断线/刷新后控制台可 XRANGE 重放（console 侧
  「重放 + 实时」双段），Pub/Sub 只承担实时。同频道同 key 名是有意为之——
  控制台删除执行时 ``DEL`` 该键即连同时间线一起清理。
- **时间线自成一路**：控制台把本频道载荷推成 SSE 事件 ``timeline``，
  ``update`` 保留给执行状态快照（详情页按 ``update`` 整体刷新 execution，
  两种载荷混在同一事件名下会互相覆盖）。判别依据是载荷的 ``event`` 落在
  ``TIMELINE_EVENT_TYPES``——两端共用本常量，不各写一份字符串。
- **被动旁路**：不写 context、不干预调度；写失败（redis 抖动）只告警。
  宿主不挂它 = 零行为变化（基类 ``_make_event_recorder`` 返回 None）。
- **自清理**：每次追加刷新 TTL（默认 7 天）+ ``MAXLEN`` 上限——终态执行没有
  人来删这条 Stream，不能靠调用方记得清理。
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

from plaita.core.callback import FlowCallback

logger = logging.getLogger(__name__)

EXECUTION_EVENTS_KEY_PREFIX = "plaita:execution:events:"
# 单条记录的事件载荷字段名（与任务队列 PAYLOAD_FIELD 同风格）
PAYLOAD_FIELD = "data"
# 事件类型（载荷 ``event`` 字段的取值）。控制台按 ``TIMELINE_EVENT_TYPES``
# 把时间线事件与状态快照分流成两个 SSE 事件名（见模块 docstring）。
EVENT_FLOW_START = "flow_start"
EVENT_NODE_START = "node_start"
EVENT_NODE_END = "node_end"
TIMELINE_EVENT_TYPES = frozenset({EVENT_FLOW_START, EVENT_NODE_START, EVENT_NODE_END})
DEFAULT_EVENTS_MAXLEN = 1000
DEFAULT_EVENTS_TTL_SECONDS = 7 * 86400
_MAX_ERROR_MESSAGE_CHARS = 2000


def execution_events_key(execution_id: str) -> str:
    """执行时间线键名（= SSE 实时频道名，见模块 docstring）。"""
    return f"{EXECUTION_EVENTS_KEY_PREFIX}{execution_id}"


def _parse_ts(value: Any) -> Optional[datetime]:
    """ISO 时间戳 → datetime；naive 按**读取方**本地时区解释。

    带偏移的时间戳（入队侧现在写 ``datetime.now().astimezone().isoformat()``）
    按同一时轴差分，入队机与 worker 机时区不同不再算成整小时级假等待；老消息
    只写本地时间（naive 字符串本身不含 TZ 信息），只能按读取方本地时区解释。
    """
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.astimezone()


def queue_wait_ms(queued_at: Optional[str], started_at: Optional[str]) -> Optional[int]:
    """排队时长（毫秒）：入队时间到 worker 认账开始执行的间隔。

    两个时间戳由不同进程写（入队侧 / worker 侧）。真实时钟偏斜不做补偿：偏斜
    为负（回拨）没有意义，取 0；正偏斜无从与「真的排了很久队」区分。缺失或
    不可解析 → None（老消息无 ``timestamp``）。
    """
    if not queued_at or not started_at:
        return None
    started = _parse_ts(started_at)
    queued = _parse_ts(queued_at)
    if started is None or queued is None:
        return None
    return max(0, int((started - queued).total_seconds() * 1000))


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts).isoformat()


def _error_payload(error: Any, exception: Optional[BaseException]) -> Optional[Dict[str, Any]]:
    """把回调的 error/exception 归一成可序列化的短载荷（时间线上要能读出错因）。"""
    if not error and exception is None:
        return None
    if isinstance(error, dict):
        payload: Dict[str, Any] = {
            "code": error.get("code"),
            "message": error.get("message"),
        }
    elif error is not None:
        payload = {"code": None, "message": str(error)}
    else:
        payload = {
            "code": getattr(exception, "code", None),
            "message": getattr(exception, "message", None) or str(exception),
        }
    payload["message"] = str(payload.get("message") or "")[:_MAX_ERROR_MESSAGE_CHARS]
    if exception is not None:
        payload["type"] = type(exception).__name__
    return payload


class ExecutionEventRecorder(FlowCallback):
    """把节点生命周期事件写进 per-execution Stream + 实时频道。"""

    def __init__(
        self,
        redis_client,
        execution_id: str,
        *,
        maxlen: int = DEFAULT_EVENTS_MAXLEN,
        ttl_seconds: int = DEFAULT_EVENTS_TTL_SECONDS,
        clock=time.time,
    ) -> None:
        self._redis = redis_client
        self.execution_id = str(execution_id)
        self.key = execution_events_key(execution_id)
        self._maxlen = max(1, int(maxlen))
        self._ttl_seconds = int(ttl_seconds)
        self._clock = clock
        self._open: Dict[str, float] = {}

    def record_flow_start(
        self,
        *,
        queued_at: Optional[str] = None,
        queue_wait_ms: Optional[int] = None,
        started_at: Optional[str] = None,
    ) -> None:
        """时间线首条：worker 认账开始执行（含入队到认账的排队时长）。"""
        self._append(
            EVENT_FLOW_START,
            queued_at=queued_at,
            queue_wait_ms=queue_wait_ms,
            started_at=started_at or _iso(self._clock()),
        )

    def on_node_start(self, flow, node, **kwargs) -> None:  # noqa: ARG002 — 回调契约
        node_id = str(getattr(node, "id", ""))
        self._open[node_id] = self._clock()
        self._append(EVENT_NODE_START, node_id=node_id, node_name=self._node_name(node))

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs) -> None:  # noqa: ARG002
        node_id = str(getattr(node, "id", ""))
        now = self._clock()
        started = self._open.pop(node_id, None)
        self._append(
            EVENT_NODE_END,
            node_id=node_id,
            node_name=self._node_name(node),
            duration_ms=None if started is None else max(0, int((now - started) * 1000)),
            status="error" if (error or exception is not None) else "success",
            error=_error_payload(error, exception),
        )

    @staticmethod
    def _node_name(node) -> str:
        return str(getattr(node, "name", "") or getattr(node, "id", ""))

    def _append(self, event: str, **fields) -> None:
        """追加一条事件并 pub 一份实时副本；任何失败只告警（观测绝不阻断执行）。"""
        try:
            now = self._clock()
            payload: Dict[str, Any] = {
                "event": event,
                "execution_id": self.execution_id,
                "ts": _iso(now),
                "ts_ms": int(now * 1000),
                **fields,
            }
            stream_id = self._redis.xadd(
                self.key,
                {PAYLOAD_FIELD: json.dumps(payload, ensure_ascii=False, default=str)},
                maxlen=self._maxlen,
                approximate=True,
            )
            self._redis.expire(self.key, self._ttl_seconds)
            payload["stream_id"] = (
                stream_id.decode() if isinstance(stream_id, bytes) else str(stream_id)
            )
            self._redis.publish(self.key, json.dumps(payload, ensure_ascii=False, default=str))
        except Exception:  # noqa: BLE001 — 观测失败绝不影响执行
            logger.warning(
                "执行时间线写入失败 event=%s execution_id=%s",
                event, self.execution_id, exc_info=True,
            )
