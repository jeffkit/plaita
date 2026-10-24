"""节点执行上下文窄接口。

节点 ``execute(execution)`` 历史上拿到完整 ``FlowExecution`` facade。
本 Protocol 声明节点**应当**依赖的最小能力面；``FlowExecution`` 已实现这些方法，
自定义节点与类型检查可逐步切到本接口，避免依赖 run/arun/debug 等驱动入口。
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Protocol, runtime_checkable


@runtime_checkable
class NodeExecutionContext(Protocol):
    """节点执行时可见的窄上下文（evaluate / state / child）。

    B1 所有权 v1（plaita#322）新增四成员：``step_id`` / ``idempotency_key``
    / ``derive_key`` / ``report_progress``。副作用收据面只给内置连接器
    （http 等自动带 ``Idempotency-Key``），不给 code 节点；``report_progress``
    供长步节点自报进度（刷新状态行 ``last_report_at``，进度租约锚）。
    ``FlowExecution`` 已实现全部成员；测试替身缺成员时按「无声明」处理。
    """

    @property
    def express_prefix(self) -> str: ...

    @property
    def execution_id(self) -> str: ...

    @property
    def step_id(self) -> Optional[str]: ...

    @property
    def idempotency_key(self) -> Optional[str]: ...

    @property
    def context(self) -> Dict[str, Any]: ...

    def evaluate(self, value: Any) -> Any: ...

    def get_state(self, key: str, default: Any = None) -> Any: ...

    def set_state(self, key: str, value: Any) -> None: ...

    def set_scratch(self, key: str, value: Any) -> None: ...

    def get_child_execution(self) -> "NodeExecutionContext": ...

    def derive_key(self, sub_key: Any) -> Optional[str]: ...

    def report_progress(self, note: Optional[str] = None) -> None: ...

    def run_compatible(self, flow, lazy, *args, **params) -> Any: ...

    async def arun_compatible(self, flow, lazy, *args, **params) -> Any: ...
