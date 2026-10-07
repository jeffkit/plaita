"""plaita.usage — 执行用量归集（不依赖任何观测后端）。

Langfuse 链路（:mod:`plaita.obs`）把 token 用量交给 Langfuse 服务端聚合，
未启用 Langfuse 的部署则完全看不到用量。本模块提供 :class:`UsageCollector`：
一个纯 :class:`~plaita.core.callback.FlowCallback`，按节点归集 llm /
agentrun 节点输出的 token 用量，run 终态由宿主（FlowWorker / console
本地执行器）写入 ``ExecutionState.usage`` / ``LocalExecution.usage_json``。

形状契约（与 ``LangfuseCallback`` 读同一份节点输出）：

- 节点结果为 dict 且含 ``usage``（llm / agentrun 节点输出契约）→ 经
  :func:`plaita.obs.map_openai_usage` 规范化后归入该节点；
- 节点结果无自身 ``usage`` 但含 ``observations``（agentrun ``details=true``）
  → 累加各 observation 的 ``usage``。两者**互斥**：节点自身 usage 已是
  聚合值，再叠加 observation 会重复计数。

``summary()`` 产出::

    {
      "total": {"input": 5, "output": 1, "total": 6},
      "nodes": {"agent": {"input": 5, "output": 1, "total": 6}}
    }

无任何用量时返回 None（存储侧保持零字段，与历史行为一致）。
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from plaita.core.callback import FlowCallback
from plaita.obs import map_openai_usage

__all__ = ["UsageCollector"]


def _merge(target: Dict[str, int], addition: Dict[str, int]) -> None:
    for key, value in addition.items():
        target[key] = target.get(key, 0) + value


def _usage_from_result(result: Any) -> Optional[Dict[str, int]]:
    """节点结果 → 规范用量；节点自身 usage 优先，回退累加 observations。"""
    if not isinstance(result, dict):
        return None
    own = map_openai_usage(result.get("usage"))
    if own:
        return own
    total: Dict[str, int] = {}
    for item in result.get("observations") or []:
        if not isinstance(item, dict):
            continue
        child = map_openai_usage(item.get("usage"))
        if child:
            _merge(total, child)
    return total or None


class UsageCollector(FlowCallback):
    """按节点归集 token 用量；终态由宿主读 :meth:`summary` 落盘。"""

    def __init__(self) -> None:
        self._nodes: Dict[str, Dict[str, int]] = {}

    def reset(self) -> None:
        self._nodes = {}

    def seed(self, usage: Optional[Dict[str, Any]]) -> None:
        """以已持久化的用量打底（Distributed resume：跨进程继续累加）。

        非 dict / 空用量等价于 :meth:`reset`；条目经
        :func:`map_openai_usage` 清洗（未知键、非 int 值丢弃）。
        """
        self.reset()
        if not isinstance(usage, dict):
            return
        nodes = usage.get("nodes")
        if not isinstance(nodes, dict):
            return
        for node_id, entry in nodes.items():
            mapped = map_openai_usage(entry)
            if mapped:
                _merge(self._nodes.setdefault(str(node_id), {}), mapped)

    def on_flow_start(self, flow, **kwargs) -> None:
        # fresh start 才触发（Distributed 续跑走 on_flow_resume）——此处清空
        # 防上一执行的用量串入；resume 的累加基线由宿主 seed 提供。
        self.reset()

    def on_node_end(self, flow, node, result=None, error=None, exception=None, **kwargs) -> None:
        if error or exception:
            return
        usage = _usage_from_result(result)
        if usage:
            _merge(self._nodes.setdefault(str(node.id), {}), usage)

    def summary(self) -> Optional[Dict[str, Any]]:
        """run 级汇总；无用量时 None。"""
        if not self._nodes:
            return None
        total: Dict[str, int] = {}
        for entry in self._nodes.values():
            _merge(total, entry)
        return {
            "total": total,
            "nodes": {node_id: dict(entry) for node_id, entry in self._nodes.items()},
        }
