"""表达式命名空间 mixin——按职责从 ``FlowExecution`` 拆出(SC-003,2026-10-17)。

6 组 ``express_*`` property 是对 ``ExecutionContext`` 同名属性的**纯委托**:
它们只回答「表达式各根的名字怎么叫」(租户/嵌入场景可改写),与执行编排
无关。独立成 mixin 后 ``FlowExecution`` 回到 SC-003 硬天花板内,而调用方
零感知——属性仍经 ``FlowExecution`` 实例可达(与 :mod:`executor_compat` 的
``CompatRunMixin`` 同一拆分先例)。
"""
from __future__ import annotations


class ExpressNamespaceMixin:
    """表达式根命名:``$<prefix><name>`` 的各段名字皆可按场景改写。"""

    @property
    def express_prefix(self) -> str:
        return self._ctx.express_prefix

    @express_prefix.setter
    def express_prefix(self, value: str) -> None:
        self._ctx.express_prefix = value

    @property
    def express_input_name(self) -> str:
        return self._ctx.express_input_name

    @express_input_name.setter
    def express_input_name(self, value: str) -> None:
        self._ctx.express_input_name = value

    @property
    def express_parent_name(self) -> str:
        return self._ctx.express_parent_name

    @express_parent_name.setter
    def express_parent_name(self, value: str) -> None:
        self._ctx.express_parent_name = value

    @property
    def express_node_name(self) -> str:
        return self._ctx.express_node_name

    @express_node_name.setter
    def express_node_name(self, value: str) -> None:
        self._ctx.express_node_name = value

    @property
    def express_global_name(self) -> str:
        return self._ctx.express_global_name

    @express_global_name.setter
    def express_global_name(self, value: str) -> None:
        self._ctx.express_global_name = value

    @property
    def express_environment_variable(self) -> str:
        return self._ctx.express_environment_variable

    @express_environment_variable.setter
    def express_environment_variable(self, value: str) -> None:
        self._ctx.express_environment_variable = value
