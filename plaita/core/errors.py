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


class ResumeType(Enum):
    """分布式恢复语义类型, 取代裸字符串 magic string。

    ``CONTINUE`` 为默认 (带 saved_context 直接续跑), 其余三种仅对挂起的
    ``EventNode`` 生效 (由 ``DistributedStrategy._handle_resume`` 处理)。
    """

    CONTINUE = "continue"
    CANCEL = "cancel"
    TIMEOUT = "timeout"
    EVENT = "event"

    @classmethod
    def coerce(cls, value) -> "ResumeType":
        """接受 enum 或字符串 (兼容外部旧调用方), 无法识别时抛 ``ResumeError``。"""
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls(value)
            except ValueError:
                raise ResumeError(f"Unsupported resume type: {value!r}")
        raise ResumeError(f"Unsupported resume type: {value!r}")


# 节点 abort 时, 未配置 error_handler 用的默认错误码。
# 与 ErrorHandler.error_code 的默认 (-9527) 区分: 后者是"用户配了 handler 但
# 没显式给 code"的默认; 这里是"根本没有 handler"的兜底码。
# 定义在异常子类之前, 供 NodeExecutionError 作为默认 code 引用。
DEFAULT_NODE_ABORT_CODE = -520


class FlowExecutionException(RuntimeError):
    """流程执行异常基类。

    保留 ``(code, message, error_type, node)`` 位置签名以兼容历史调用方。
    新代码应优先抛出具体子类（``NodeNotFoundError`` / ``NodeExecutionError``
    / ``FlowErrorException`` 等），它们自带默认 ``code`` / ``error_type``，
    调用方只需提供 ``message`` 与 ``node``，不必再往调用点塞魔法数字。
    """

    # 子类覆盖这两个类属性来声明默认语义；直接实例化基类时也安全。
    code: int = -500
    error_type: "FlowErrorType" = FlowErrorType.FLOW_ERROR

    def __init__(self, code: int = -500, message: str = "", error_type=None, node=None,
                 source_line: Optional[int] = None):
        self.code = code
        self.message = message
        self.error_type = error_type if error_type is not None else type(self).error_type
        self.node = node
        # 运行期错误回标的源码行号（仅 @flow 前端编译期写入 IR 时可用）。
        # 调用方可显式传入；未传时由 runner 从 node.source_line 兜底回填。
        self.source_line = source_line
        super().__init__(message)


class NodeNotFoundError(FlowExecutionException):
    """节点找不到（含流程缺少 start 节点）。"""

    code = -500
    error_type = FlowErrorType.NODE_NOT_FOUND

    def __init__(self, node_id=None, message=None, node=None):
        if message is None:
            message = f"Node with id '{node_id}' not found" if node_id else "Node not found"
        super().__init__(self.code, message, self.error_type, node)


class FlowStartMissingError(NodeNotFoundError):
    """流程缺少起始节点。共享 NODE_NOT_FOUND 语义。"""

    def __init__(self, message="Flow has no start node", node=None):
        super().__init__(message=message, node=node)


class NodeExecutionError(FlowExecutionException):
    """节点执行出错（abort 策略）。

    ``code`` 默认取 ``DEFAULT_NODE_ABORT_CODE``；当调用方配了 error_handler
    时由 runner 传入该 handler 的 ``error_code``。
    """

    code = DEFAULT_NODE_ABORT_CODE
    error_type = FlowErrorType.NODE_ERROR

    def __init__(self, message: str, node=None, code: int = DEFAULT_NODE_ABORT_CODE):
        super().__init__(code, message, self.error_type, node)


class NodeTimeoutError(FlowExecutionException):
    """节点超时。``error_type`` 随超时来源切换：节点自身超时为 NODE_ERROR，
    流程级超时强加给节点时为 FLOW_ERROR。"""

    code = -1
    error_type = FlowErrorType.NODE_ERROR

    def __init__(self, message: str, node=None, error_type=FlowErrorType.NODE_ERROR):
        super().__init__(self.code, message, error_type, node)


class FlowTimeoutError(FlowExecutionException):
    """流程整体超时。"""

    code = -1
    error_type = FlowErrorType.FLOW_ERROR

    def __init__(self, message: str = "Flow execution timeout", node=None):
        super().__init__(self.code, message, self.error_type, node)


class FlowErrorException(FlowExecutionException):
    """流程执行通用错误（兜底包装）。"""

    code = -500
    error_type = FlowErrorType.FLOW_ERROR

    def __init__(self, message: str, node=None):
        super().__init__(self.code, message, self.error_type, node)


class ErrorResultException(FlowExecutionException):
    """End 节点返回错误结果。code/message 来自 ``FlowResultError``。"""

    error_type = FlowErrorType.ERROR_RESULT

    def __init__(self, code: int, message: str, node=None):
        super().__init__(code, message, self.error_type, node)


class ResumeError(FlowExecutionException):
    """分布式恢复阶段的不合法状态（无挂起节点 / 节点非 EventNode / 状态不对 /
    resume_type 不支持 / 恢复执行抛错）。"""

    code = -500
    error_type = FlowErrorType.NODE_ERROR

    def __init__(self, message: str, node=None):
        super().__init__(self.code, message, self.error_type, node)


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
    """错误处理器

    ``strategy`` 字段是 ``ErrorStrategy`` enum (2026-07 重构前是 ``Optional[str]``，
    比较时全程依赖 ``_strategy_eq`` 这个 wart 同时识别 enum 与字符串)。Pydantic
    会自动处理 enum ↔ "abort"/"continue"/"continue-with" 字符串的双向序列化，
    JSON 输入仍写字符串即可。历史 ``continue_with`` (下划线) 别名在 validator
    里归一化为规范连字符。
    """

    strategy: ErrorStrategy = Field(ErrorStrategy.ABORT)
    default_value: Optional[dict] = Field(None, alias="defaultValue")
    error_code: Optional[int] = Field(-9527, alias="code")
    error_message: Optional[str] = Field(None, alias="message")

    @field_validator("strategy", mode="before")
    @classmethod
    def _validate_strategy(cls, v):
        if v is None:
            return ErrorStrategy.ABORT
        if isinstance(v, ErrorStrategy):
            return v
        if isinstance(v, str):
            # 接受下划线别名 continue_with, 归一化为规范的连字符 continue-with
            if v == "continue_with":
                v = ErrorStrategy.CONTINUE_WITH.value
            try:
                return ErrorStrategy(v)
            except ValueError:
                raise ValueError(f"Invalid error handler strategy: {v!r}")
        raise ValueError(f"Invalid error handler strategy: {v!r}")

    def handle(self):
        """
        处理超时异常
        - abort: 抛出TimeoutError异常
        - continue: 返回None
        - continue-with: 返回默认值
        """
        if self.strategy == ErrorStrategy.ABORT:
            message = self.error_message or "Node execution timed out"
            raise TimeoutError(message)
        elif self.strategy == ErrorStrategy.CONTINUE:
            return None
        elif self.strategy == ErrorStrategy.CONTINUE_WITH:
            return self.default_value


def _strategy_eq(value, member: ErrorStrategy) -> bool:
    """.. deprecated:: 2026-07
    ``ErrorHandler.strategy`` 现已存为 ``ErrorStrategy`` enum, 直接用 ``==`` 比较即可。
    本函数仅为向后兼容外部 import 保留, 内部所有调用点已迁移。
    """
    return value == member or value == member.value


class RecoverableErrorHandler(ErrorHandler):
    retry_times: int = Field(0, alias="retryTimes")
