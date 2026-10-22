"""
plaita.core.errors — Canonical location for Plaita error classes.

Migrated from plaita/errors.py (which is now a compatibility shim).
"""

from enum import Enum
from typing import Any, Optional

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


#: 输出校验异常的可重试标记（plaita#219 × node-F2）。
#:
#: node-F2 把非 ``NodeException`` 的 ``ValueError`` 归为确定性配置错误、不再
#: 重试；但 ``Node._validate_output`` 的既有契约（#219）是「校验异常按普通
#: 节点异常走 retryTimes / errorHandler」。两条契约的交集用实例标记区分：
#: 节点自主抛出的 ValueError 不重试，校验钩子抛出的异常保持重试。
_OUTPUT_VALIDATION_RETRY_ATTR = "_plaita_output_validation_retryable"


def mark_output_validation_retryable(exc: BaseException) -> None:
    """给输出校验钩子抛出的异常打上可重试标记。

    打标而不是换异常类型：用户日志与既有测试都按原异常类型/文案断言
    （``ValueError: …``）。``BaseException`` 实例始终有 ``__dict__``（即使
    子类声明 ``__slots__``），``setattr`` 不会失败。
    """
    setattr(exc, _OUTPUT_VALIDATION_RETRY_ATTR, True)


def is_output_validation_retryable(exc: BaseException) -> bool:
    """该异常是否来自节点输出校验钩子（见 :func:`mark_output_validation_retryable`）。"""
    return bool(getattr(exc, _OUTPUT_VALIDATION_RETRY_ATTR, False))


#: 确定性错误类型名集（node-F2 × plaita-go#43，owner 裁决 2026-10-22：两侧同集）。
#:
#: 这些类型（及其子类）表示「同一份代码在同一份输入上重跑不可能变对」——
#: 参数/配置错（ValueError）、类型误用（TypeError）、未定义名字（NameError）、
#: 语法/编码错（SyntaxError）。重试只会烧预算、放大副作用；与 plaita-go
#: ``pyDeterministicEnvelopeTypes``（r69 终态，Go 侧不放松）逐字对齐。
#:
#: 两个消费面：
#: - 类型面：in-process 后端用户异常原样穿出，按 ``isinstance`` 命中；
#: - 信封面：subprocess/docker 后端把 ``ok=false`` 信封包成 RuntimeError，
#:   内层类型名以 ``origin_type`` 属性穿透分类（见
#:   ``plaita.node.code.SandboxEnvelopeError``；按属性而非文本前缀，不做
#:   文本嗅探）。
DETERMINISTIC_ERROR_TYPES = frozenset(
    {"ValueError", "TypeError", "NameError", "SyntaxError"}
)


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

    ``CONTINUE`` 为默认 (带 saved_context 直接续跑); ``RETRY`` 专对 error 态
    execution 放行 (worker 终态短路例外), 策略层与 continue 同一条步进路径——
    失败节点在 checkpoint 里无条目, 从 last_node_id 后继步进即恰好重跑失败
    节点 (G1); 其余三种仅对挂起的 ``EventNode`` 生效 (由
    ``DistributedStrategy._handle_resume`` 处理)。
    """

    CONTINUE = "continue"
    RETRY = "retry"
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


class ExecutionDeadlineError(FlowTimeoutError):
    """执行级总预算（deadline）耗尽（plaita#267）。

    与 :class:`FlowTimeoutError` 同 ``code=-1`` / ``FLOW_ERROR`` 归属，既有
    消费方（normal 模式错误契约、worker 的「超时不重试」判据）零回归；单独
    成类是为了让**执行总预算到期**与节点自身超时可判别：

    - worker ``_is_retryable_node_failure`` 按 ``FlowTimeoutError`` 家族判定
      「不重试」，执行终态化 error 可被收尸（不会再重投一次）；
    - 观测端可按类型名区分「执行 deadline 到期」与「节点 timeoutHandler 超时」。
    """


class FlowErrorException(FlowExecutionException):
    """流程执行通用错误（兜底包装）。"""

    code = -500
    error_type = FlowErrorType.FLOW_ERROR

    def __init__(self, message: str, node=None):
        super().__init__(self.code, message, self.error_type, node)


class FlowCancelledException(FlowExecutionException):
    """执行已被取消（2026-10 波次③）。

    由 ``FlowExecution.cancel()`` 置位 ``cancel_requested`` 后，引擎在**节点
    入口**抛出本异常拒绝继续执行。职责边界：

    - 不经节点 ``errorHandler`` 分发（不重试、不被 continue 策略吞掉）——
      取消是执行级意图，不是节点错误；在 runner 重试循环**之前**抛出，
      沿 ``FlowExecutionException`` 同族既有语义冒泡（normal 模式原样透传，
      distributed 模式按既有契约归一化为 ``FlowErrorException``/-500）。
    - ``code=-4`` 为取消专属错误码（与 -500 通用/-520 节点 abort/-1 超时
      区分），供上层 worker 识别取消终态。
    """

    code = -4
    error_type = FlowErrorType.FLOW_ERROR

    def __init__(self, message: str = "Flow execution cancelled", node=None):
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


class ResumeGuardError(ResumeError):
    """恢复**守卫**：恢复请求与执行状态不匹配（策略层在分发到节点
    ``resume()`` **之前**就拒绝），执行应保持原状而不是被终态化。

    与基类的分工（worker 侧 ``_chain_has_resume_guard_error`` 靠本类型区分）：

    - **本类（守卫）**：continue/retry 试图绕过 pending 挂起节点、resume 打在
      无挂起节点 / 非挂起节点的 checkpoint 上。这类请求不携带推进语义，且几乎
      都来自 at-least-once 重投与竞速（重复投递的 event resume 打在已推进的
      checkpoint 上同族）——执行保持 suspended 等真正的决议路径
      （event/cancel/timeout），终态化成 error 会把可恢复的挂起执行永久打封
      （连 event 都被终态短路拒绝，plaita#33）。
    - **其余 ``ResumeError``**（含 ``strategies._handle_resume`` 把
      ``current_node.resume()`` 抛出的异常包成的那个）：是**执行自身失败**，
      由 worker 现状路径终态化 error（可观测、可人工 retry），不得静默保持原状。
    """

    def __init__(self, message: str, node=None):
        super().__init__(message, node)


class StateSnapshotError(FlowExecutionException):
    """状态快照出口无法安全物化（plaita#107）。

    ``CheckpointState.to_checkpoint_dict`` / ``update_node_result`` 的出口走
    ``plaita.core.state._snapshot_value``：它现在是显式栈的迭代物化，Python
    递归深度与数据深度解耦，容器环按占位符安全截断。唯一仍会抛出的情形是
    数据深度超过防御性上限——此时抛本异常而不是让 ``RecursionError`` 穿透到
    分布式归一化层（后者会把真实原因抹成误导性的
    ``FlowErrorException("maximum recursion depth exceeded")``）。消息携带
    超限深度与容器路径，normal 模式原样透传、distributed 归一化后仍保留该文本。
    """


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
    # Any 而非 dict: 文档承诺 "continue_with 用 defaultValue 作为本节点输出",
    # 标量/字符串是最自然的写法; 历史上限定 dict 让文档示例
    # ({"defaultValue": "fallback"}) 直接 ValidationError。
    default_value: Optional[Any] = Field(None, alias="defaultValue")
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


#: 单节点失败重试次数上限（A2 r22 升级 P1）。
#:
#: 发布面允许 editor 发布 flow，而 worker 为多租户共享：``retryTimes=1e9``
#: 足以把 worker 长期钉死在单个节点的重试循环里（跨租户独占）。上限取 10
#: ——远超正常瞬态重试需求，同时把单节点最坏耗时锁在有限量级。构造期
#: （Pydantic 校验）拒绝，console 保存/发布据此返回 422，而不是等运行期。
MAX_RETRY_TIMES = 10


class RecoverableErrorHandler(ErrorHandler):
    retry_times: int = Field(0, alias="retryTimes")

    @field_validator("retry_times", mode="before")
    @classmethod
    def _validate_retry_times_shape(cls, value):
        # A2R35-01：布尔/数字字符串此前被 Pydantic 宽松强转（true→1、"5"→5），
        # 五入口（JSON/Builder/@flow/console/sexpr）全收，与 maxRedirects/
        # requestTimeout 的 sexpr 严格式构成契约分叉——统一为构造期具名拒绝。
        if isinstance(value, bool):
            raise ValueError(
                f"retryTimes 必须是整数，不能是布尔值: {value!r}"
                "（true/false 会被宽松强转成 1/0，请直接写整数）"
            )
        if isinstance(value, str):
            raise ValueError(
                f"retryTimes 必须是整数，不能是字符串: {value!r}"
                "（数字字符串不会被强转，请直接写整数）"
            )
        return value

    @field_validator("retry_times")
    @classmethod
    def _validate_retry_times_upper_bound(cls, value: int) -> int:
        # B2R27-01 口径变更（2026-10-21）：负数此前构造期接受、运行期
        # ``max(0, ...)`` 静默钳 0（无任何 WARNING，与「任何降级必须可观测」
        # 相悖）；现与其他入口的 -1 具名拒绝对齐，构造期直接拒绝。
        if value < 0:
            raise ValueError(
                f"retryTimes 不能为负值: {value}"
                "（0 表示不重试；负值此前被静默钳 0，现已构造期拒绝）"
            )
        if value > MAX_RETRY_TIMES:
            raise ValueError(
                f"retryTimes 超过上限: {value} > {MAX_RETRY_TIMES}"
                "（单节点重试次数上限，防止共享 worker 被单个流程长期独占）"
            )
        return value


class ExecutionLeaseError(RuntimeError):
    """执行租约失守（resume 抢占失败 / fence 世代失配）。

    定义下沉到 core（plaita#96）：storage（fenced CAS 写拒绝旧世代）与
    server（execution_lease / flow_worker）两侧必须共用**同一类对象**——
    定义留在 server 曾迫使 storage 相对导入 ``..server``，构成被分层
    禁止的反向依赖。server 侧 ``execution_lease.py`` 复用本定义。
    """
