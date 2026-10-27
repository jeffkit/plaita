"""plaita.core.spans — 节点执行实例身份（execution trace span 模型，plaita#344）。

**要解决的问题**：节点耗时/事件记录此前以裸 ``node_id`` 为配对键——loop/map
多轮执行同一子节点时每轮耗时互相覆盖（只剩聚合）、map/parallel 并发帧下
start/end 互踩（时长算错）、attempt 计数把重试与迭代混为一谈。

**模型**：每次节点执行实例 = 一个 span——

- ``span_id``：实例唯一 id（node id + 随机后缀）；
- ``parent_span_id``：所属帧节点（loop/parallel/child 等）本次执行的 span；
- ``frame_path``：帧节点 id 根→叶（如 ``("loop1", "loop2")``）；
- ``iteration``：帧内限定符——loop 族 = ``$LOOP-INDEX``（0-based int），
  parallel = 分支名（str），child 内联子流程 = ``None``。

**接线**（引擎单点盖章，复合节点零改动）：

- ``NodeRunner.run_node`` 执行前生成 span 并盖章到执行上下文
  （``current_span_id`` / ``current_frame_node_id``），随 ``on_node_start/
  on_node_end`` 的 ``span=`` kwarg 下发；
- ``FlowExecution.get_child_execution()`` 传播帧上下文（frame_path 延长 +
  parent span 继承——盖章的正是当前复合节点）；
- ``ownership.bind_child_step`` 绑定 ``frame_key``（= sub_key：迭代索引/分支
  名——该函数的调用点恰为全部复合容器，零新增调用点）。

记录器（node_timings / execution_timeline）按 ``span_id`` 配对，聚合面保持
向后兼容。**边界**：resume 收尾腿（strategies 的 node.resume 后 on_node_end）
无同进程 start 可配对，维持无 span 回退（与现状一致的零时长记录）；Langfuse
观测端（obs.py 的 LIFO 栈）不受影响。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional, Tuple
from uuid import uuid4

logger = logging.getLogger("plaita.core.spans")

__all__ = ["NodeSpan", "build_node_span"]


@dataclass(frozen=True)
class NodeSpan:
    """一次节点执行实例的身份。"""

    span_id: str
    parent_span_id: Optional[str]
    frame_path: Tuple[str, ...]
    iteration: Any  # int（loop 族 index）/ str（parallel 分支名）/ None（根帧或 child）
    node_id: str

    def to_dict(self) -> dict:
        return {
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "frame_path": list(self.frame_path),
            "iteration": self.iteration,
            "node_id": self.node_id,
        }


def build_node_span(execution: Any, node: Any) -> NodeSpan:
    """从执行上下文读帧状态 + 生成实例唯一 span。

    鸭子读取（测试替身/旧宿主缺帧字段时按根帧处理）；任何异常降级为
    ``span=None`` 由调用方兜底（观测面绝不阻断执行）。
    """
    node_id = str(getattr(node, "id", "?"))
    frame_path = tuple(getattr(execution, "frame_path", ()) or ())
    iteration = getattr(execution, "frame_key", None)
    parent = getattr(execution, "frame_parent_span", None)
    span_id = f"{node_id}-{uuid4().hex[:12]}"
    return NodeSpan(
        span_id=span_id,
        parent_span_id=parent if isinstance(parent, str) else None,
        frame_path=frame_path,
        iteration=iteration,
        node_id=node_id,
    )
