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
from plaita.core.url_redaction import _scrub_exception_text

logger = logging.getLogger(__name__)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts).isoformat()


def _node_label(node: object) -> str:
    """观测告警里的节点标签：**只有**类型名与 id，绝不 ``repr(node)``。

    plaita#158（同类面）：节点对象是 pydantic 模型，``repr`` 会带上全部字段——
    ``RedisNode.target`` 正是带口令的 Redis URL，``%r`` 会把它写进 WARNING 日志。
    本函数只回答「是哪个节点」，且 ``node.id`` 本身可能就是失败原因（``on_node_start``
    里失败的那句正是 ``str(node.id)``），故再兜一层，不让标签构造把回调炸第二次。
    """
    node_type = type(node).__name__
    try:
        return f"{node_type}(id={node.id!s})"
    except Exception as exc:  # noqa: BLE001 — 标签构造失败也必须能告警
        # 不外抛：调用方正在告警路径上（观测代码不得把执行带崩）。
        logger.debug("节点标签构造失败，退回类型名: %s", _scrub_exception_text(exc))
        return f"<{node_type}>"


class NodeTimingCallback(FlowCallback):
    """采集节点开始/结束时间；``snapshot()`` 返回可直接序列化的字典。

    必须继承 ``FlowCallback``：回调分发器对每个 handler 逐个 getattr 所有钩子，
    缺钩子会打「Error in on_flow_xxx callback: ... has no attribute ...」告警
    （2026-10-08 实测：每个 flow 两条，纯噪声）——基类提供全套 no-op，
    只覆写关心的钩子即不会再触发。
    """
    """采集节点开始/结束时间；``snapshot()`` 返回可直接序列化的字典。"""

    # 每节点保留的逐轮明细上限（plaita#344）：万轮 loop 不撑爆快照；截断计数
    # 可观测（occurrences_truncated）。
    MAX_OCCURRENCES_PER_NODE = 200

    def __init__(self, clock=time.time) -> None:
        # clock 可注入（测试用假时钟），默认墙钟
        self._clock = clock
        # 配对键 = span_id（plaita#344 span 模型）；无 span（旧宿主直调）回退
        # node_id——两种键同池不撞（span_id 含随机后缀）。
        self._open: Dict[str, float] = {}
        self._timings: Dict[str, Dict[str, Any]] = {}

    def on_node_start(self, flow, node, span=None, **kwargs) -> None:  # noqa: ARG002 — 回调契约
        try:
            key = span.span_id if span is not None else str(node.id)
            self._open[key] = self._clock()
        except Exception as exc:  # noqa: BLE001 — 观测失败绝不影响执行
            logger.warning("节点耗时采集 on_node_start 失败: %s: %s", _node_label(node), _scrub_exception_text(exc))

    def on_node_end(self, flow, node, result=None, error=None, exception=None, span=None, **kwargs) -> None:  # noqa: ARG002
        try:
            node_id = str(node.id)
            key = span.span_id if span is not None else node_id
            started = self._open.pop(key, None)
            if started is None:
                # 没见过 start（回调中途接管/老 worker/resume 收尾腿）：只记结束时间，不编造耗时
                ended = self._clock()
                self._record(node_id, ended, ended, None, error or exception, span=span)
                return
            ended = self._clock()
            self._record(node_id, started, ended, max(0, int((ended - started) * 1000)), error or exception, span=span)
        except Exception as exc:  # noqa: BLE001
            logger.warning("节点耗时采集 on_node_end 失败: %s: %s", _node_label(node), _scrub_exception_text(exc))

    def _record(
        self,
        node_id: str,
        started: float,
        ended: float,
        duration_ms: Optional[int],
        error: Any,
        span: Any = None,
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
        # 逐轮明细（plaita#344，additive）：每执行实例一条；聚合字段语义不变。
        # 注意 entry 每次重建——occurrences 必须自 prev 带过来再追加。
        occ = list(prev.get("occurrences") or []) if prev else []
        occ.append({
            "span_id": span.span_id if span is not None else None,
            "iteration": span.iteration if span is not None else None,
            "frame_path": list(span.frame_path) if span is not None else [],
            "started_at": _iso(started),
            "ended_at": _iso(ended),
            "duration_ms": duration_ms,
            "failed": bool(error),
        })
        truncated = int(prev.get("occurrences_truncated", 0)) if prev else 0
        if len(occ) > self.MAX_OCCURRENCES_PER_NODE:
            occ.pop(0)
            truncated += 1
        if truncated:
            entry["occurrences_truncated"] = truncated
        entry["occurrences"] = occ
        self._timings[node_id] = entry

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        """当前已采集的节点耗时（浅拷贝，可直接落盘）。

        **在跑节点也要落盘**（2026-10-10，plaita#27/#31/#55/#32/#62 误杀事故）：
        原先只返回 ``_timings``（仅在 ``on_node_end`` 写入），于是**正在执行的
        节点在 ``node_timings`` 里完全不可见**。keeper 侧的活性判据
        （``console_exec.worker_alive`` 判据① 「有 started 无 ended 的节点 =
        活证据」）因此永远看不到它——沙箱 ``sandbox_agent`` 节点（impl）跑
        35 分钟期间，宿主侧 ``node_timings`` 停在最后一个已完成节点上，
        keeper 判「末节点 ended_at 停滞超 1800s」→ **把健康 run 判死并 cancel**。

        实测事故：execution 7c1f513f/31e74058/26e861d5 的产出分别在
        10:19:18/10:19:15/09:28 落盘，而 keeper 在 10:19:15/10:19:23/10:03
        判死——**落盘与误杀同一秒**；worktree 里躺着完整的 +250 行实现。

        这里为 ``_open`` 里的在跑节点补一条 ``ended_at=""`` 的条目：语义与
        keeper 判据约定一致（有 ``started_at``、无 ``ended_at`` = 在跑 =
        活证据），且**不编造** ``ended_at``/``duration_ms``。
        已完成节点仍以 ``_timings`` 为准（同一节点重跑时结束记录覆盖在跑记录）。
        """
        out = {node_id: dict(entry) for node_id, entry in self._timings.items()}
        for node_id, started in self._open.items():
            if node_id in out:
                continue        # 已有终态记录（重跑场景）：以完成记录为准
            out[node_id] = {
                "started_at": _iso(started),
                "ended_at": "",   # 空 = 仍在跑（keeper 活性判据依赖此形态）
                "started_ms": int(started * 1000),
                "ended_ms": None,
                "attempts": 1,
                "failed": False,
            }
        return out

    def has_open_nodes(self) -> bool:
        """是否仍有「已开始、未结束」的节点 = 该执行此刻还在干活。

        plaita#197：失租 orphan 观测的存活判据。`snapshot()` 会把在跑节点
        补成 ``ended_at=""``，但这里只需要一个是/否，且必须读**当前**状态
        （不是某次落盘快照）。执行结束后 ``on_node_end`` 会把它从 ``_open``
        摘除 ⇒ 观测集可摘除、心跳停止，keeper 的 stale 判据重新可见。
        """
        return bool(self._open)

    def reset(self) -> None:
        self._open.clear()
        self._timings.clear()
