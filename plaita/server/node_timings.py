"""节点级耗时采集。

执行状态里原本只有整条流程的 ``start_time``/``end_time``，节点维度没有任何时间
信息——执行详情页因此只能显示「跑了哪些节点」，说不出「哪个节点慢」。

本模块是一个**被动**的 ``FlowCallback`` 实现：只监听 ``on_node_start`` /
``on_node_end``，把每次节点执行的开始/结束时间记下来，由宿主（worker）在落盘
执行状态时取 ``snapshot()`` 写进 ``ExecutionState.node_timings``。

设计要点：
- **不改流程语义**：不写 context、不干预调度、不抛异常（回调里任何异常都吞掉
  并告警），宿主不挂它就与今天完全一致。
- **可循环**：同一个节点被访问多次时记录 ``attempts`` 与 ``total_duration_ms``，
  ``duration_ms`` 为最后一次；瀑布图按最后一次落位，旁注次数与合计。
- **时钟自洽**：除 ISO 时间戳外同时记录 epoch 毫秒，前端据此算相对偏移，避免
  与 ``start_time`` 的时区基准不一致导致的错位。
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

from plaita.core.callback import FlowCallback

logger = logging.getLogger(__name__)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts).isoformat()


class NodeTimingCallback(FlowCallback):
    """采集节点开始/结束时间；``snapshot()`` 返回可直接序列化的字典。

    必须继承 ``FlowCallback``：回调分发器对每个 handler 逐个 getattr 所有钩子，
    缺钩子会打「Error in on_flow_xxx callback: ... has no attribute ...」告警
    （2026-10-08 实测：每个 flow 两条，纯噪声）——基类提供全套 no-op，
    只覆写关心的钩子即不会再触发。
    """
    """采集节点开始/结束时间；``snapshot()`` 返回可直接序列化的字典。"""

    def __init__(self, clock=time.time) -> None:
        # clock 可注入（测试用假时钟），默认墙钟
        self._clock = clock
        self._open: Dict[str, float] = {}
        self._timings: Dict[str, Dict[str, Any]] = {}

    def on_node_start(self, flow, node, **kwargs) -> None:  # noqa: ARG002 — 回调契约
        try:
            self._open[str(node.id)] = self._clock()
        except Exception:  # noqa: BLE001 — 观测失败绝不影响执行
            logger.warning("节点耗时采集 on_node_start 失败: %r", node, exc_info=True)

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs) -> None:  # noqa: ARG002
        try:
            node_id = str(node.id)
            started = self._open.pop(node_id, None)
            if started is None:
                # 没见过 start（回调中途接管/老 worker）：只记结束时间，不编造耗时
                ended = self._clock()
                self._record(node_id, ended, ended, None, error or exception)
                return
            ended = self._clock()
            self._record(node_id, started, ended, max(0, int((ended - started) * 1000)), error or exception)
        except Exception:  # noqa: BLE001
            logger.warning("节点耗时采集 on_node_end 失败: %r", node, exc_info=True)

    def _record(
        self,
        node_id: str,
        started: float,
        ended: float,
        duration_ms: Optional[int],
        error: Any,
    ) -> None:
        prev = self._timings.get(node_id)
        attempts = int(prev.get("attempts", 0)) + 1 if prev else 1
        total = int(prev.get("total_duration_ms", 0)) if prev else 0
        if duration_ms is not None:
            total += duration_ms
        entry: Dict[str, Any] = {
            "started_at": _iso(started),
            "ended_at": _iso(ended),
            "started_ms": int(started * 1000),
            "ended_ms": int(ended * 1000),
            "attempts": attempts,
            "failed": bool(error),
        }
        if duration_ms is not None:
            entry["duration_ms"] = duration_ms
            entry["total_duration_ms"] = total
        self._timings[node_id] = entry

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        """当前已采集的节点耗时（浅拷贝，可直接落盘）。"""
        return {node_id: dict(entry) for node_id, entry in self._timings.items()}

    def reset(self) -> None:
        self._open.clear()
        self._timings.clear()
