"""B1 所有权 v1（plaita#322）的引擎面混入。

职责拆分（SC-003，先例=``executor_express.ExpressNamespaceMixin``）：
step token 绑定/幂等键/子调用派生/进度上报是 FlowExecution 的一个独立职责面，
整体安置在本混入，避免宿主类超 SC-003 硬顶。机制与义务声明的权威文档在
``plaita/core/ownership.py``（模块 docstring）与 ``docs/WORKER_PROTOCOL.md``
§2.1；本文件只承载引擎面 API 的薄实现。
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from plaita.core.url_redaction import _scrub_exception_text

logger = logging.getLogger(__name__)


class OwnershipMixin:
    """step token / 幂等键 / 进度上报的执行对象面（plaita#322）。

    依赖宿主（``FlowExecution``）提供的成员：``parent``（父执行链）、
    ``execution_id``（run 身份）、``callback_manager``（子执行构造）。
    """

    # 实例状态走类级缺省（bind_step/bind_progress_reporter 就地赋值即实例化），
    # 不要求宿主 __init__ 配合。
    _step_scope: Optional[str] = None
    _progress_reporter = None

    def _root_execution(self) -> Any:
        node = self
        seen = 0
        while node.parent is not None and seen < 256:
            node = node.parent
            seen += 1
        return node

    def bind_step(self, step_id: Optional[str]) -> None:
        """worker 在 run_distributed 前绑定状态行声明的 step token。

        ``None`` = 本步无声明（老行/挂起恢复首步）——tokenless，连接器不出
        收据（诚实优于编造）。子执行不经本方法（它们经
        ``core/ownership.bind_child_step`` 派生作用域，或直接沿父链继承）。
        """
        self._step_scope = step_id

    @property
    def step_id(self) -> Optional[str]:
        """本执行视角的 step token：自身绑定优先，未绑定沿父链继承。"""
        if self._step_scope is not None:
            return self._step_scope
        if self.parent is not None:
            return self.parent.step_id
        return None

    @property
    def idempotency_key(self) -> Optional[str]:
        """幂等键 ``f"{execution_id}:{step_id}"``（#322）。

        执行段**恒取根执行 id**：子执行的 ``$EXECUTION_ID`` 是构造期随机
        uuid（``ExecutionContext.__init__``），跨重放不稳定；run 身份只根上
        有。任一段缺失 = None（无声明即无所有权主张）。
        """
        from plaita.core.ownership import compose_idempotency_key

        return compose_idempotency_key(self._root_execution().execution_id, self.step_id)

    def derive_key(self, sub_key: Any) -> Optional[str]:
        """复合节点为子调用派生的完整幂等键 ``f"{idempotency_key}:{sub_key}"``。

        与 ``core/ownership.bind_child_step`` 同一组合律——循环 i 次迭代 /
        parallel 第 j 分支各得稳定且互异的键；同 sub_key 重放复现同键。
        """
        base = self.idempotency_key
        if not base:
            return None
        return f"{base}:{sub_key}"

    def bind_progress_reporter(self, reporter) -> None:
        """worker 绑定进度上报回调（``_report_step_progress`` 的部分应用）。

        仅根执行需要绑定；子执行经 :meth:`report_progress` 沿父链找到它。
        """
        self._progress_reporter = reporter

    def report_progress(self, note: Optional[str] = None) -> None:
        """节点执行内进度上报（#322）：刷新状态行 ``last_report_at``（经
        worker 回调，节流落行）。

        引擎面默认无回调 = no-op（本地/非 worker run 零行为变化）；回调
        抛错只留 DEBUG 日志——进度上报**绝不打断节点执行**（观测路径纪律，
        同看门狗心跳）。``note`` 仅进日志（诊断面），不落状态行。
        """
        reporter = self._progress_reporter
        if reporter is None:
            if self.parent is not None:
                self.parent.report_progress(note)
            return
        try:
            reporter(note)
        except Exception as exc:  # noqa: BLE001 — 观测路径不得影响执行
            logger.debug(
                "report_progress 回调失败（忽略）: execution=%s: %s",
                self.execution_id, _scrub_exception_text(exc),
            )
