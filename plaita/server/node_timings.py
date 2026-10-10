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

    def close_open(self, *, failed: bool = True) -> None:
        """把仍在跑的节点按「已异常结束」收口（宿主在**异常终态落盘前**调用）。

        内核只在**成功**路径调 ``on_node_end``（``runner.run_node`` 里它排在
        ``_execute_with_retry`` 之后；节点抛错/超时/取消时那一行根本走不到）。
        于是异常节点的 id 永远留在 ``_open`` 里，``snapshot()`` 会把一条
        ``ended_at=""`` 的「在跑」记录带进**终态**执行状态：keeper 的活性
        判据①（有 started、无 ended = 活证据）从此**永远命中**，真正挂死的
        执行再也无法被回收——正是误杀修复的镜像问题。

        复现（2026-10-10）：suspended 执行经 event 唤醒 → 下一节点超时 →
        terminal error，而状态里躺着 ``boom: {started_at: …, ended_at: ""}``，
        终态落盘与 ``_collect_node_timings`` 的合并都改不掉它。

        异常终态（error/cancelled）落盘前调用：把 ``_open`` 逐条按「已结束 +
        失败」写进 ``_timings`` 并清空，终态文档里不再有「在跑」条目。
        """
        ended = self._clock()
        for node_id, started in list(self._open.items()):
            self._record(
                node_id,
                started,
                ended,
                max(0, int((ended - started) * 1000)),
                error=None,
                failed=failed,
            )
        self._open.clear()

    def _record(
        self,
        node_id: str,
        started: float,
        ended: float,
        duration_ms: Optional[int],
        error: Any,
        failed: Optional[bool] = None,
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
            "failed": bool(error) if failed is None else bool(failed),
        }
        if duration_ms is not None:
            entry["duration_ms"] = duration_ms
            entry["total_duration_ms"] = total
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

        「在跑」形态因此**只在节点确实在跑时**成立：异常结束（抛错/超时/
        取消）的节点由宿主在终态落盘前经 :meth:`close_open` 收口，绝不把
        ``ended_at=""`` 留在终态文档里（否则判据①永久命中 → 挂死执行无法回收）。
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

    def reset(self) -> None:
        self._open.clear()
        self._timings.clear()
