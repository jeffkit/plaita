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

    # @property
    # def input_property(self):
    #     return self.input_type

    # @property
    # def output_property(self):
    #     return self.output_type

    def validate(self):
        # 校验节点配置是否正确,在构造Flow时本方法会被调用。
        pass

    def _validate_input(self, *args, **kwargs):
        # 校验输入值是否满足要求
        pass

    def _validate_output(self, result):
        # 校验输出值是否符合预期格式
        pass

    def run(self, execution: FlowExecution = None) -> Any:
        result = self.execute(execution)
        self._validate_output(result)
        return result

    def execute(self, execution: FlowExecution = None) -> Any:
        raise NotImplementedError()
