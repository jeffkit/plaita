"""
plaita.core.errors — Canonical location for Plaita error classes.

Migrated from plaita/errors.py (which is now a compatibility shim).
"""

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class FlowResultError(RuntimeError):
    """流程执行结果异常，用于在End节点中抛出"""

    def __init__(self, code=None, message=None):
        super().__init__(f"FlowResultError: code={code}, message={message}")
        self.code = code
        self.message = message


class NodeException(RuntimeError):
    """节点执行异常,在节点的Execution方法中抛出"""

    def __init__(self, code, message):
        super().__init__(f"NodeException: code={code}, message={message}")
        self.code = code
        self.message = message


class FlowErrorType(Enum):
    """流程异常类型"""

    NODE_ERROR = "node_error"
    FLOW_ERROR = "flow_error"
    ERROR_RESULT = "error_result"
    INVALID_CALLBACK = "invalid_callback"
    EXECUTION_NOT_FOUND = "execution_not_found"
    NODE_NOT_FOUND = "node_not_found"
    INVALID_NODE_TYPE = "invalid_node_type"
    EXECUTION_TIMEOUT = "execution_timeout"
    CALLBACK_TIMEOUT = "callback_timeout"


class FlowExecutionException(RuntimeError):
    """流程执行异常"""

    def __init__(self, code, message, error_type, node=None):
        self.code = code
        self.message = message
        self.error_type = error_type
        self.node = node


class ErrorStrategy(Enum):
    """
    错误处理策略
    - abort: 结束流程，返回指定code及message
    - continue: 继续执行,但本节点无返回值
    - continue-with: 继续执行,并返回指定的值
    """

    ABORT = "abort"
    CONTINUE = "continue"
    CONTINUE_WITH = "continue-with"


class ErrorHandler(BaseModel):
    """错误处理器"""

    strategy: Optional[str] = Field(ErrorStrategy.ABORT.value)
    default_value: Optional[dict] = Field(None, alias="defaultValue")
    error_code: Optional[int] = Field(-9527, alias="code")
    error_message: Optional[str] = Field(None, alias="message")

    @field_validator("strategy", mode="before")
    @classmethod
    def _validate_strategy(cls, v):
        if v is None:
            return ErrorStrategy.ABORT.value
        if isinstance(v, ErrorStrategy):
            return v.value
        # 接受下划线别名 continue_with，归一化为规范的连字符 continue-with。
        if v == "continue_with":
            v = ErrorStrategy.CONTINUE_WITH.value
        if v not in {s.value for s in ErrorStrategy}:
            raise ValueError(f"Invalid error handler strategy: {v}")
        return v

    def handle(self):
        """
        处理超时异常
        - abort: 抛出TimeoutError异常
        - continue: 返回None
        - continue-with: 返回默认值
        """
        s = self.strategy
        if _strategy_eq(s, ErrorStrategy.ABORT):
            message = self.error_message or "Node execution timed out"
            raise TimeoutError(message)
        elif _strategy_eq(s, ErrorStrategy.CONTINUE):
            return None
        elif _strategy_eq(s, ErrorStrategy.CONTINUE_WITH):
            return self.default_value


def _strategy_eq(value, member: ErrorStrategy) -> bool:
    """Compare a strategy value that may be an ErrorStrategy member or its string value."""
    return value == member or value == member.value


class RecoverableErrorHandler(ErrorHandler):
    retry_times: int = Field(0, alias="retryTimes")


# 节点 abort 时, 未配置 error_handler 用的默认错误码。
# 与 ErrorHandler.error_code 的默认 (-9527) 区分: 后者是"用户配了 handler 但
# 没显式给 code"的默认; 这里是"根本没有 handler"的兜底码。
DEFAULT_NODE_ABORT_CODE = -520
