from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional

from pydantic import BaseModel, Field, model_validator

from plaita.core.errors import ErrorHandler, RecoverableErrorHandler

if TYPE_CHECKING:
    # FlowExecution is the concrete type passed as `execution` at runtime.
    # It is declared here only for static analysis; importing it at module level
    # would create a circular dependency (node → executor → node).
    #
    # Design note: ideally nodes would depend on a narrow NodeExecutionContext
    # interface (evaluate, get_state, get_child_execution) rather than the full
    # FlowExecution facade.  That refactor is deferred; for now this annotation
    # documents the intent without changing runtime behaviour.
    from plaita.core.executor import FlowExecution


class NodeConfigException(RuntimeError):
    pass


class Node(BaseModel):
    # Class attributes (not included in the model's schema)
    node_type: ClassVar[str] = "ignore"
    node_name: ClassVar[str] = "ignore"
    branching: ClassVar[bool] = False
    async_node: ClassVar[bool] = False
    # 是否为"挂起型"节点: 执行后暂停流程, 等待外部 resume (如 EventNode)。
    # 内核 DistributedStrategy 据此走 suspend 分支并调 resume(), 不再 isinstance
    # 具体节点类型, 从而切断 core -> plaita.node.event_node 的反向依赖。
    is_suspending: ClassVar[bool] = False

    # Instance fields
    id: str = Field(..., description="Node identifier")
    name: Optional[str] = Field(None, description="Node name")
    desc: Optional[str] = Field(None, description="Node description")
    output: Optional[Any] = None
    next: Optional[Any] = None
    timeout: str = Field("", description="Timeout configuration")
    # input_type: Optional[Union[Property, List[Property]]] = None
    # output_type: Optional[Union[Property, List[Property]]] = None
    timeout_handler: ErrorHandler = Field(default_factory=lambda: ErrorHandler())
    error_handler: RecoverableErrorHandler = Field(default_factory=lambda: RecoverableErrorHandler())

    @model_validator(mode="before")
    @classmethod
    def setup_error_handler(cls, values: Dict) -> Dict:
        timeout_handler_config = values.get("timeoutHandler") or values.get("timeout_handler")
        error_handler_config = values.get("errorHandler") or values.get("error_handler")

        if timeout_handler_config:
            values["timeout_handler"] = ErrorHandler(**timeout_handler_config)
        if error_handler_config:
            values["error_handler"] = RecoverableErrorHandler(**error_handler_config)
        return values

    def validate(self) -> None:
        """构造期校验节点配置。由 ``FlowBuilder.validate`` 调用，子类可覆写
        （如 ``Switch`` / ``HTTP``）以在流程构建阶段就拦下非法配置。"""
        return None

    def _validate_output(self, result: Any) -> None:
        """运行期校验节点输出。``run`` 会在 ``execute`` 后调用本方法，子类可
        覆写以校验产出格式。默认无操作。"""
        return None

    def run(self, execution) -> Any:
        result = self.execute(execution)
        self._validate_output(result)
        return result

    def execute(self, execution) -> Any:
        """执行节点并返回输出。``execution`` 为必传的执行上下文（facade）。"""
        raise NotImplementedError()

    def resume(self, execution, resume_type, resume_data=None) -> Any:
        """恢复一个此前挂起的节点 (仅 ``is_suspending`` 节点需要实现)。

        内核 ``DistributedStrategy._handle_resume`` 通过本方法多态分发 resume,
        避免直接 ``isinstance`` 具体挂起节点类型 (如 EventNode), 让 core 层
        不反向依赖 node 插件层。基类默认抛错, 挂起型节点覆写之。
        """
        raise NotImplementedError(
            f"Node {self.id} ({type(self).__name__}) is not a suspending node and "
            f"cannot be resumed; is_suspending={self.is_suspending}"
        )
