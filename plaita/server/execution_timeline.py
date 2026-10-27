"""run 级事件时间线（plaita#25）。

背景：执行状态里虽然已有 ``node_timings``（节点耗时聚合，见
``plaita/server/node_timings.py``），但**没有任何有序事件流**，于是三个问题在
系统里都答不出来：

* 「这个 run 排队多久？」——console 入队写了 ``timestamp``，worker 从不读它；
* 「出事时到底走到哪一步、哪一步开始慢下来的？」——只有每节点的聚合快照，
  没有带时间戳的 start/end/error 事件序；
* 集群档 SSE 频道 ``plaita:execution:events:{id}`` **全仓零发布者**（只有
  console 的订阅方与删除方），订阅者除一次 initial_state 外永远静默。

本模块是该缺口的**唯一发布者**与**唯一事件源**：

- :class:`ExecutionTimelineCallback` 是**被动** ``FlowCallback``：只监听
  ``on_node_start`` / ``on_node_end``，把事件按序追加进有界列表（超限丢最旧
  并计数，**绝不静默**）；宿主（worker）在落盘收口处 ``snapshot()`` 写进
  ``ExecutionState.timeline``；
- 同一份事件经 ``publisher``（worker 注入）实时发布到 SSE 频道——失败节点
  的实时可见性是「除 Langfuse 外没有第二时间线」的第一层补位；**落盘**面由
  ``finish_attempt``（plaita#25 P2）补齐：错误路径虽然不改状态行（非终态、
  可重投/可 cancel），也要把这次尝试的失败节点与收口事件写进时间线；
- 排队时长由入队时间（console 消息 ``timestamp``）与 ``run_start`` 的差值
  算出；**没有入队时间时是 ``None`` 而不是 0**（「未采集」≠「排队 0ms」）。

设计要点与 ``node_timings`` 同款：不改流程语义、不写 context、不干预调度、
回调内任何异常都吞掉并告警（观测代码绝不得把执行带崩）；宿主不挂它即与今天
完全一致；老状态读出 ``None``，零回归。

与 ``node_timings`` 的分工（避免第二真相源）：``node_timings`` 是**每节点
聚合视图**（attempts/total_duration_ms/在跑标记，供 keeper 活性判据），
``timeline`` 是**有序事件日志**（谁在什么时候开始/结束/失败）。两者都由同一
对回调钩子采集、在同一落盘收口写入，不互相派生。
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from plaita.core.callback import FlowCallback
from plaita.core.url_redaction import _scrub_exception_text
from plaita.core.url_redaction import _redact_env_value
from plaita.core.url_redaction import _scrub_url_text

logger = logging.getLogger(__name__)

__all__ = [
    "CHANNEL_PREFIX",
    "DEFAULT_MAX_EVENTS",
    "ENV_MAX_EVENTS",
    "ENV_PUBLISH",
    "ExecutionTimelineCallback",
    "merge_timeline",
    "publish_enabled",
]

#: SSE 频道前缀——与 console 订阅侧（``plaita-console/backend/api/executions.py``）
#: 逐字一致。改这里必须同时改那边，否则订阅者收不到（本条就是 #25 的缺陷本体）。
CHANNEL_PREFIX = "plaita:execution:events:"

#: 事件上限（条）。上限存在的理由：execute state 每步整行写回，事件流无界会让
#: 长跑 run 的状态行无界膨胀；超限丢**最旧**并计 ``dropped``。
DEFAULT_MAX_EVENTS = 500
ENV_MAX_EVENTS = "PLAITA_EXECUTION_TIMELINE_MAX"
#: ``=0`` 关闭实时发布（只落盘/只读时间线），用于排障与无 pubsub 的部署。
ENV_PUBLISH = "PLAITA_EXECUTION_EVENTS_PUBLISH"


def publish_enabled() -> bool:
    """执行事件实时发布开关（默认开）。"""
    return os.environ.get(ENV_PUBLISH, "1") != "0"


def _resolve_max(max_events: Optional[int]) -> int:
    if max_events is not None:
        try:
            return max(1, int(max_events))
        except (TypeError, ValueError):
            pass
    raw = os.environ.get(ENV_MAX_EVENTS)
    if raw:
        try:
            return max(1, int(raw))
        except (TypeError, ValueError):
            logger.warning("非法 %s=%s，回退默认 %d", ENV_MAX_EVENTS,
                           _redact_env_value(raw), DEFAULT_MAX_EVENTS)
    return DEFAULT_MAX_EVENTS


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _parse_iso_ms(value: Any) -> Optional[int]:
    """ISO 时间串 → epoch 毫秒；不可解析返回 ``None``（不猜）。"""
    if not isinstance(value, str) or not value:
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        # 入队时间由 console 用 ``datetime.now().isoformat()`` 写（本机墙钟、
        # 无时区）——按本地时区解释，与写入方同一基准。
        dt = dt.astimezone()
    return int(dt.timestamp() * 1000)


def _node_label(node: object) -> str:
    """观测告警标签：只有类型名与 id，绝不 ``repr(node)``。

    与 ``node_timings._node_label`` 同款（plaita#158）：节点是 pydantic 模型，
    ``repr`` 会带上 ``RedisNode.target`` 这类含口令字段。``node.id`` 本身可能就是
    失败原因，故再兜一层。
    """
    node_type = type(node).__name__
    try:
        return f"{node_type}(id={node.id!s})"
    except Exception as exc:  # noqa: BLE001 — 标签构造失败也必须能告警
        logger.debug("节点标签构造失败，退回类型名: %s", _scrub_exception_text(exc))
        return f"<{node_type}>"


def _error_text(error: Any, exception: Any) -> Optional[str]:
    """事件 ``error`` 文本：``str(src)`` 过 ``_scrub_url_text``（A2R49-02）。

    这份文本有**两个读面**：落盘进 ``ExecutionState.timeline``（console
    ``/timeline`` 端点透出）+ 经 publisher 实时发布到 SSE 频道（console
    update 帧零滤透传——worker 是频道唯一发布者，scrub 必须在构造处）。
    与本模块 logger 侧 ``_scrub_exception_text``（#151）同判据；只抹凭据
    分量，异常类型与消息主体原样。
    """
    src = exception if exception is not None else error
    if src is None:
        return None
    try:
        return _scrub_url_text(str(src))
    except Exception:  # noqa: BLE001
        return f"<{type(src).__name__}>"


def _resequence(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{**e, "seq": i + 1} for i, e in enumerate(events)]


class ExecutionTimelineCallback(FlowCallback):
    """按序采集 run/节点事件；``snapshot()`` 返回可直接落盘的字典。

    必须继承 ``FlowCallback``：分发器对每个 handler 逐个 getattr 所有钩子，
    缺钩子会打「has no attribute」告警（与 ``NodeTimingCallback`` 同因）。
    """

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.time,
        publisher: Optional[Callable[[Dict[str, Any]], None]] = None,
        max_events: Optional[int] = None,
        enqueued_at: Optional[str] = None,
    ) -> None:
        self._clock = clock
        self._publisher = publisher
        self._max = _resolve_max(max_events)
        self._events: List[Dict[str, Any]] = []
        self._seq = 0
        self._dropped = 0
        self._started = False
        self._finished = False
        self._open: Dict[str, float] = {}
        self._attempts: Dict[str, int] = {}
        self._enqueued_at = enqueued_at
        self._enqueued_ms = _parse_iso_ms(enqueued_at)
        self._queue_wait_ms: Optional[int] = None
        # 落盘水位：``drain()`` 只交付上次 drain 之后的新事件，避免宿主把
        # 「全量快照」并进已有状态时把老事件重复一遍（每步落盘 ⇒ N 倍膨胀）。
        self._drained = 0
        self._drained_dropped = 0

    # ------------------------------------------------------------------
    # 发布/追加
    # ------------------------------------------------------------------

    def _append(self, event: Dict[str, Any]) -> None:
        """追加一条事件并 best-effort 发布；任何失败都只告警。"""
        try:
            now = self._clock()
            self._seq += 1
            record: Dict[str, Any] = {
                "seq": self._seq,
                "at": _iso(now),
                "at_ms": int(now * 1000),
                **event,
            }
            self._events.append(record)
            overflow = len(self._events) - self._max
            if overflow > 0:
                del self._events[:overflow]
                self._dropped += overflow
                # 水位随丢旧事件前移，否则 ``drain()`` 会把还没交付的事件
                # 当成已交付（丢事件必须计进 dropped，不能静默少事件）。
                self._drained = max(0, self._drained - overflow)
            pub = self._publisher
            if pub is not None:
                try:
                    pub(dict(record))
                except Exception as exc:  # noqa: BLE001 — 订阅方不可用不得影响执行
                    logger.warning("执行事件发布失败（忽略，不影响执行）: %s", _scrub_exception_text(exc))
        except Exception as exc:  # noqa: BLE001 — 观测失败绝不影响执行
            logger.warning("执行事件采集失败（忽略）: %s", _scrub_exception_text(exc))

    def _mark(self, event_type: str, **fields: Any) -> None:
        self._append({"type": event_type, **fields})

    # ------------------------------------------------------------------
    # run 级事件
    # ------------------------------------------------------------------

    def start_run(self) -> None:
        """run 开始（worker 在 start 窗口调用）；幂等。"""
        if self._started:
            return
        self._started = True
        if self._enqueued_ms is not None:
            self._mark(
                "enqueued",
                at=self._enqueued_at,
                at_ms=self._enqueued_ms,
            )
            self._queue_wait_ms = max(0, int(self._clock() * 1000) - self._enqueued_ms)
        self._mark(
            "run_start",
            enqueued_at=self._enqueued_at,
            queue_wait_ms=self._queue_wait_ms,
        )

    def finish_run(self, status: Any = None) -> None:
        """run 收口（终态落盘时调用）；幂等——重复调用只留第一次。"""
        if self._finished:
            return
        self._finished = True
        self._mark("run_end", status=str(status) if status is not None else None,
                   final=True)

    def finish_attempt(self, status: Any = None) -> None:
        """**投递尝试**的收口事件（plaita#25 P2，错误路径专用）。

        与 :meth:`finish_run` 的区别只有一条：**不置终态标记**——状态行仍非
        终态（可重投/可 cancel，既有契约见
        ``tests/unit/test_g1_resume_retry.py::TestWorkerHonorsPremintedId``），
        下一次尝试还要能再收口一次。事件用 ``final=False`` 标明「这只是本次
        尝试的收口」，读取方取**最后一条** ``run_end`` 即当前口径，取
        ``final=True`` 的那条才是执行的终态。

        失败节点事件由 runner 的 abort 路径补发（见 ``core/runner.py``），本
        方法只负责给这次尝试补上收口锚点——否则失败 run 的落盘时间线停在
        半途，事后无法判断「这次到底跑完没有、以什么状态收的」。
        """
        self._mark("run_end", status=str(status) if status is not None else None,
                   final=False)

    # ------------------------------------------------------------------
    # 节点事件
    # ------------------------------------------------------------------

    def on_node_start(self, flow, node, span=None, **kwargs) -> None:  # noqa: ARG002 — 回调契约
        try:
            node_id = str(node.id)
            # plaita#344 span 模型：配对键 = span_id（实例唯一，并发帧不互踩）；
            # attempt 键 = (frame_path, iteration, node_id)——同帧重入（resume
            # 形状的重试）递增，循环轮次由 iteration 表达、不使 attempt 膨胀。
            # 无 span（旧宿主直调/resume 收尾腿）回退 node_id 键（历史形状）。
            if span is not None:
                span_key = span.span_id
                frame_path = list(span.frame_path)
                iteration = span.iteration
                parent_span_id = span.parent_span_id
                attempt_key = (tuple(span.frame_path), span.iteration, node_id)
            else:
                span_key = node_id
                frame_path, iteration, parent_span_id = [], None, None
                attempt_key = ((), None, node_id)
            self._open[span_key] = self._clock()
            self._attempts[attempt_key] = self._attempts.get(attempt_key, 0) + 1
            self._mark(
                "node_start",
                node_id=node_id,
                attempt=self._attempts[attempt_key],
                span_id=span_key,
                parent_span_id=parent_span_id,
                frame_path=frame_path,
                iteration=iteration,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "时间线 on_node_start 失败: %s: %s",
                _node_label(node), _scrub_exception_text(exc),
            )

    def on_node_end(self, flow, node, result=None, error=None, exception=None, span=None, **kwargs) -> None:  # noqa: ARG002
        try:
            node_id = str(node.id)
            if span is not None:
                span_key = span.span_id
                frame_path = list(span.frame_path)
                iteration = span.iteration
                parent_span_id = span.parent_span_id
                attempt_key = (tuple(span.frame_path), span.iteration, node_id)
            else:
                span_key = node_id
                frame_path, iteration, parent_span_id = [], None, None
                attempt_key = ((), None, node_id)
            started = self._open.pop(span_key, None)
            ended = self._clock()
            duration_ms = max(0, int((ended - started) * 1000)) if started is not None else None
            fields: Dict[str, Any] = {
                "node_id": node_id,
                "attempt": self._attempts.get(attempt_key, 1),
                "ok": not (error or exception),
                "error": _error_text(error, exception),
                "span_id": span_key,
                "parent_span_id": parent_span_id,
                "frame_path": frame_path,
                "iteration": iteration,
            }
            if duration_ms is not None:
                fields["duration_ms"] = duration_ms
            self._mark("node_end", **fields)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "时间线 on_node_end 失败: %s: %s",
                _node_label(node), _scrub_exception_text(exc),
            )

    # ------------------------------------------------------------------
    # 快照
    # ------------------------------------------------------------------

    def snapshot(self) -> Dict[str, Any]:
        """当前事件流（深拷一层，可直接落盘）。

        ``queue_wait_ms``/``enqueued_at`` 为 ``None`` = **不知道**（无入队时间），
        与「排队 0ms」严格区分——读取方不得把 None 折算成 0。
        """
        return {
            "events": [dict(e) for e in self._events],
            "dropped": self._dropped,
            "max_events": self._max,
            "enqueued_at": self._enqueued_at,
            "queue_wait_ms": self._queue_wait_ms,
        }

    def drain(self) -> Dict[str, Any]:
        """**自上次 drain 之后**的新事件（宿主落盘用；``snapshot()`` 仍是全量）。

        宿主每个节点边界都会把快照并进 ``ExecutionState.timeline``——若交付全量，
        已落盘事件会在每一步被重复追加（N 步 N 倍膨胀）。水位随被上限丢弃的
        旧事件同步前移，丢事件只经 ``dropped`` 计数，不静默丢。
        """
        events = [dict(e) for e in self._events[self._drained:]]
        self._drained = len(self._events)
        newly_dropped = self._dropped - self._drained_dropped
        self._drained_dropped = self._dropped
        return {
            "events": events,
            "dropped": newly_dropped,
            "max_events": self._max,
            "enqueued_at": self._enqueued_at,
            "queue_wait_ms": self._queue_wait_ms,
        }

    def reset(self) -> None:
        self._events.clear()
        self._open.clear()
        self._attempts.clear()
        self._seq = 0
        self._dropped = 0
        self._drained = 0
        self._drained_dropped = 0
        self._started = False
        self._finished = False
        self._queue_wait_ms = None


def merge_timeline(
    old: Optional[Dict[str, Any]],
    new: Optional[Dict[str, Any]],
    *,
    max_events: Optional[int] = None,
) -> Dict[str, Any]:
    """把本次进程采到的事件并进状态里已有的事件（resume 跨进程）。

    与 ``node_timings`` 的合并不覆盖语义一致：旧事件不能被新进程抹掉；
    排队时长以**首次**记录为准（resume 进程不知道原始入队时间，值为 None，
    不能反过来把已有的答案擦成 None）。
    """
    new = new if isinstance(new, dict) else {}
    if not isinstance(old, dict):
        events = _resequence(list(new.get("events") or []))
        out = {
            "events": events,
            "dropped": int(new.get("dropped") or 0),
            "max_events": int(new.get("max_events") or _resolve_max(None)),
            "enqueued_at": new.get("enqueued_at"),
            "queue_wait_ms": new.get("queue_wait_ms"),
        }
        return out

    cap = max_events
    if cap is None:
        cap = int(new.get("max_events") or old.get("max_events") or _resolve_max(None))
    events = list(old.get("events") or []) + list(new.get("events") or [])
    dropped = int(old.get("dropped") or 0) + int(new.get("dropped") or 0)
    if len(events) > cap:
        dropped += len(events) - cap
        events = events[-cap:]
    queue_wait = old.get("queue_wait_ms")
    if queue_wait is None:
        queue_wait = new.get("queue_wait_ms")
    return {
        "events": _resequence(events),
        "dropped": dropped,
        "max_events": cap,
        "enqueued_at": old.get("enqueued_at") or new.get("enqueued_at"),
        "queue_wait_ms": queue_wait,
    }


def event_payload(execution_id: str, event: Dict[str, Any]) -> str:
    """SSE 频道载荷：事件 + 执行 id（订阅方无需再解析频道名）。"""
    return json.dumps(
        {"execution_id": execution_id, **event}, ensure_ascii=False, default=str
    )
