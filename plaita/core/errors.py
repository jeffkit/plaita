"""
plaita.core.errors — Canonical location for Plaita error classes.

Migrated from plaita/errors.py (which is now a compatibility shim).
"""

from enum import Enum
import logging
import os
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

from plaita.core.url_redaction import _redact_env_value


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
    # plaita#330：挂起节点在 normal/generator 根出口的具名终态（对外稳定字符串
    # ``suspend_not_executable``，flow_run / SDK 按 `ok=false` + 该类型判终态不重试）。
    # plaita#331 起同一字符串也覆盖执行入口的静态拒绝（park/resume 族 token 不被
    # 目标 profile 满足）——#317 对拍锚「normal/debug + 挂起 → 具名拒绝」的机器判据不变。
    SUSPEND_NOT_EXECUTABLE = "suspend_not_executable"
    # plaita#331：flow 执行要求闭包里**非挂起族** token（network/sandbox）不被目标
    # profile 满足。内置 normal/generator/distributed 三 profile 均含这两 token，
    # 本类型只在自定义 profile（console 池 profile 等）上出现。
    EXECUTION_PROFILE_UNSATISFIED = "execution_profile_unsatisfied"


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


class SuspendCallFrame:
    """嵌套挂起调用栈的一帧（plaita#311 NS-M3，rs ``CallFrame`` 对应面）。

    描述「一个待返回的父执行帧」：子流程完成后，其返回值回填到本帧
    ``flow`` 的 ``node`` 节点（``$NODE[node]`` + ``$LAST_NODE``），父流程从
    ``node`` 的后继继续步进。``context`` 是该帧的活 ``ExecutionContext``。

    传播时压入序 = 调用链传播序（最内父帧先入，``FlowSuspendedCarrier.frames``
    index 0 = 最内父帧，递增到根），恢复时按同序消费（最深帧先返回）。
    """

    __slots__ = ("flow", "context", "node")

    def __init__(self, flow, context, node) -> None:
        self.flow = flow
        self.context = context
        self.node = node


class FlowSuspendedCarrier(FlowExecutionException):
    """嵌套挂起传播载体（plaita#311 NS-M3，rs ``FlowError::Suspended`` 对应面）。

    挂起型节点（``is_suspending=True``）在**非分布式边界**返回 pending 载荷时
    由 ``_advance_one`` 抛出，沿调用链逐层上抛、每层经 ``push_frame`` 压入
    「待返回帧」——直到分布式步边界把它转换成本执行的合法挂起（传播半边），
    或穿出 normal/generator 模式成为具名失败（缺陷修复半边：历史上 pending
    载荷被当普通数据、流程假完成，事件永不消费且零报错）。

    与取消（``FlowCancelledException``）同待遇：

    - 不进节点 retry / errorHandler / timeoutHandler 策略链——``NodeRunner.
      _execute_with_retry`` 在通用 except 之前原样上抛（runner.py 逃逸分支）；
    - ``run_node`` 的 ``except BaseException`` 补发 error 形态 ``on_node_end``
      ——穿越型子节点挂起的 NodeEnd 携带 error 文本，与 rs 两种 NodeEnd
      形状口径一致（验证报告 Q2）。

    ``code=-500``：载体本身不是终态错误（分布式边界会转换成挂起）；仅在
    无法表达的挂起位置（normal/generator 模式）才作为具名失败呈现。
    """

    code = -500
    error_type = FlowErrorType.FLOW_ERROR

    def __init__(self, message: str, node=None, flow=None, context=None,
                 node_result=None) -> None:
        super().__init__(self.code, message, self.error_type, node)
        # 挂起现场（最内层帧）：挂起节点 + 其所在 flow + 该帧的活上下文 +
        # 节点返回的 pending 载荷（订阅注册与逐帧 checkpoint 的素材）。
        self.flow = flow
        self.context = context
        self.node_result = node_result
        # 待返回父帧栈（传播层压入）：index 0 = 最内父帧，递增到根。
        self.frames: "list[SuspendCallFrame]" = []

    def push_frame(self, flow, context, node) -> None:
        """传播层压帧：本层 flow/上下文/子节点作为「待返回的调用帧」入栈。"""
        self.frames.append(SuspendCallFrame(flow=flow, context=context, node=node))


class FlowSuspendRejectedError(FlowExecutionException):
    """挂起作用域闸的具名拒绝（plaita#311 NS-M3，rs ``FlowError::SuspendRejected``
    对应面）。

    v1 语义契约（plaita-rs docs/PROGRAM.md 程式一）：只有 child/reference
    调用链内的挂起可以穿透到分布式恢复边界；迭代帧（loop/map/filter/find/
    reduce/while）与 parallel 分支帧上的挂起**具名拒绝**——恢复位置需要迭代
    状态快照，v1 无法表达，绝不静默（消息点名阻断帧类型）。

    拒绝是终态错误：不挂起、不写 pending、不注册订阅；经 normal 模式原样
    透传 / distributed 模式归一化为 ``FlowErrorException``/-500（消息保留）。
    """

    code = -500
    error_type = FlowErrorType.FLOW_ERROR

    def __init__(self, message: str, node=None):
        super().__init__(self.code, message, self.error_type, node)


#: 回滚旋钮（plaita#330 §7，A2 评审 §5 收窄形态）：
#: ``PLAITA_SUSPEND_GUARD=warn`` 只作用于 **#330 新增两半边**——①normal/generator
#: 根出口的呈现转换（保 NS-M3 原样裸 carrier + WARNING）；②注册期能力契约不变量
#: （拒绝降 WARNING 放行）——以及 **#331 的两道静态闸**（③执行入口
#: ``validate_for_profile`` 拒绝、④NodeRunner execute 前兜底断言，均降 WARNING 放行，
#: 交回运行期闸/呈现；见 plaita.core.capabilities）。
#: **关不掉 NS-M3 的运行期 carrier 闸**。到期删除：
#: 2026-11-30（docs/governance/execution-capabilities.md 记档防孤儿旋钮）。
SUSPEND_GUARD_ENV = "PLAITA_SUSPEND_GUARD"


def suspend_guard_mode() -> str:
    """读回滚旋钮，返回 ``"error"``（默认/严格）或 ``"warn"``；非法值 WARNING 回落严格。

    非法值经 :func:`_redact_env_value` 掩形（#151 C 类「env 投影」：告警只给
    长度/形态，不回显值本体——一份被误置成凭据的 env 不得原文进日志）。
    """
    raw = (os.environ.get(SUSPEND_GUARD_ENV) or "").strip().lower()
    if not raw:
        return "error"
    if raw == "warn":
        return "warn"
    logger = logging.getLogger("plaita.core.errors")
    logger.warning(
        "%s 非法值 %s（合法：error/warn）——按严格档 error 处理",
        SUSPEND_GUARD_ENV, _redact_env_value(raw),
    )
    return "error"


class SuspendNotExecutableError(FlowExecutionException):
    """挂起节点在当前宿主不可执行的具名终态失败（plaita#330 / #331）。

    两个抛出面共用同一类与同一 ``error_type`` 字符串（#317 对拍锚不变）：

    - **入口静态拒绝**（plaita#331，``stage="entry"``）：执行入口按
      ``required_capabilities(flow, recursive=True)`` × 目标 profile 能力集判定，
      park/resume 族 token 缺失即在**任何节点执行前**拒绝；错误体多出稀疏键
      ``profile / required / missing``。NodeRunner 的 execute 前兜底断言
      （``stage="runner"``）同形——它本不应触达，触达即入口漏了（WARNING）。
    - **运行期根出口呈现**（plaita#330，``stage="runtime"``）：闸点
      （``_advance_one``）维持抛 :class:`FlowSuspendedCarrier`（NS-M3 传播载体
      不动）；仅当载体穿出 normal/generator **模式根**（无父执行）时，由根出口把
      它包装成本类——子执行里 carrier 原样上抛（分布式父执行靠它把挂起送到步
      边界）。静态闭包看不见的复合节点（自定义属性持有子流程）走这条兜底。

    独立异常类（非 carrier 子类）：worker / 上层按类型判**终态不重试**
    （同 ``FlowTimeoutError`` 家族先例）；若继承 carrier，任何
    ``except FlowSuspendedCarrier`` 都会把它误当可挂起载体。

    错误体（:meth:`to_dict`）：``{error_type, node_id, node_type, source_line,
    flow_id, cause, fix, frame_path, stage, message}``（入口/兜底面另有
    ``profile, required, missing``）；``message`` 首行写死
    「这不是源码缺陷，不要删除/替换该节点」；``fix.call`` 给出可原样执行的
    分布式驱动调用（restructure 时为 ``None`` = 明确告知不可本地执行）。
    ``cause`` 阶段口径（A2 评审 §1）：``environment``（profile 不满足）与过渡期
    ``undeclared``（注册不变量被旋钮降级放行的节点）；``unregistered`` 是加载期
    事实，不经本类呈现。
    """

    code = -500
    error_type = FlowErrorType.SUSPEND_NOT_EXECUTABLE

    def __init__(
        self,
        message: str,
        *,
        node=None,
        node_id: Optional[str] = None,
        node_type: Optional[str] = None,
        source_line: Optional[int] = None,
        flow_id: Optional[str] = None,
        cause: str = "environment",
        fix: Optional[dict] = None,
        frame_path: Optional[list] = None,
        stage: str = "runtime",
        profile: Optional[str] = None,
        required: Optional[list] = None,
        missing: Optional[list] = None,
    ) -> None:
        super().__init__(self.code, message, self.error_type, node,
                         source_line=source_line)
        self.node_id = node_id or getattr(node, "id", None)
        self.node_type = node_type or getattr(node, "node_type", None)
        self.flow_id = flow_id
        self.cause = cause
        self.fix = dict(fix or {})
        self.frame_path = list(frame_path or [])
        self.stage = stage
        self.profile = profile
        self.required = list(required) if required is not None else None
        self.missing = list(missing) if missing is not None else None

    def to_dict(self) -> dict:
        """结构化错误体（flow_run ``error_detail`` / 日志 WARNING 共用形状）。

        ``profile/required/missing`` 为稀疏键：仅入口/兜底面（静态判定）携带，
        运行期根出口呈现不含——消费方据此可辨两面。
        """
        body = {
            "error_type": self.error_type.value,
            "message": str(self),
            "node_id": self.node_id,
            "node_type": self.node_type,
            "source_line": self.source_line,
            "flow_id": self.flow_id,
            "cause": self.cause,
            "fix": dict(self.fix),
            "frame_path": list(self.frame_path),
            "stage": self.stage,
        }
        if self.profile is not None:
            body["profile"] = self.profile
            body["required"] = list(self.required or [])
            body["missing"] = list(self.missing or [])
        return body


class ExecutionProfileError(FlowExecutionException):
    """flow 的执行要求闭包里**非挂起族** token 不被目标 profile 满足（plaita#331）。

    与 :class:`SuspendNotExecutableError` 同为执行入口的拒绝面、同一判定函数
    （``validate_for_profile``）产出，按缺失 token 的族分流：缺 park/resume 族
    （suspend:*/event_bus/queue）且存在挂起节点 → ``SuspendNotExecutableError``
    （#317 机器判据）；只缺 network/sandbox → 本类。内置三 profile 均含这两
    token，本类只在自定义 profile（console 池 profile 等）上出现。

    错误体：``{error_type, message, profile, required, missing, flow_id, sites, fix}``。
    """

    code = -500
    error_type = FlowErrorType.EXECUTION_PROFILE_UNSATISFIED

    def __init__(
        self,
        message: str,
        *,
        profile: str,
        required: list,
        missing: list,
        flow_id: Optional[str] = None,
        sites: Optional[list] = None,
        fix: Optional[dict] = None,
    ) -> None:
        super().__init__(self.code, message, self.error_type, None)
        self.profile = profile
        self.required = list(required)
        self.missing = list(missing)
        self.flow_id = flow_id
        self.sites = list(sites or [])
        self.fix = dict(fix or {})

    def to_dict(self) -> dict:
        return {
            "error_type": self.error_type.value,
            "message": str(self),
            "profile": self.profile,
            "required": list(self.required),
            "missing": list(self.missing),
            "flow_id": self.flow_id,
            "sites": [dict(s) for s in self.sites],
            "fix": dict(self.fix),
        }


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
