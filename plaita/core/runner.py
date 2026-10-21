"""
plaita.core.runner — NodeRunner for single-node execution.

Handles timeout enforcement (thread-based for sync, asyncio.wait_for for async),
retry logic, error strategy dispatch (abort/continue/continue-with), and
cooperative cancellation via threading.Event.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import datetime
import functools
import logging
import math
import os
import sys
import threading
import time
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

import isodate
from concurrent.futures import ThreadPoolExecutor

from plaita.core.errors import (
    DEFAULT_NODE_ABORT_CODE,
    ErrorResultException,
    ErrorStrategy,
    ExecutionDeadlineError,
    FlowCancelledException,
    FlowErrorType,
    FlowExecutionException,
    FlowResultError,
    NodeException,
    NodeExecutionError,
    NodeTimeoutError,
    is_output_validation_retryable,
    mark_output_validation_retryable,
)
from plaita.core.url_redaction import _scrub_exception_text, _scrub_url_text
from plaita.tenant_context import mark_tenant_derived

if TYPE_CHECKING:
    from plaita.core.context import ExecutionContext
    from plaita.core.callback import CallbackManager
    from plaita.node.basic import Node

logger = logging.getLogger("plaita.core.runner")


class _NodeBudgetExceeded(Exception):
    """引擎自身强加的节点**预算**超时（#194）。

    Python 3.10+ 里 ``asyncio.TimeoutError`` 与内建 ``TimeoutError`` 合流，
    节点自己抛出的超时（aiohttp 上游读超时、socket.timeout 等）与引擎
    ``wait_for`` 产生的超时在 catch 点完全同型。用本私有类型标记「预算
    超时」，让节点异常（含 ``TimeoutError``）落进正常重试 / errorHandler
    分支，而不是被当成预算超时直接 abort（retryTimes 静默失效）。
    """


def _node_source_loc(node) -> Optional[int]:
    """取节点回标的源码行号（仅 @flow 前端编译期写入；其余前端为 None）。"""
    return getattr(node, "source_line", None)


def _node_loc_suffix(node) -> str:
    """拼运行期错误消息的源码行号后缀，无回标时返回空串。"""
    sl = _node_source_loc(node)
    return f" (源码第 {sl} 行)" if sl is not None else ""


def _validate_node_output(node: Any, result: Any) -> None:
    """调用节点的 ``_validate_output``（plaita#219）。

    ``Node.run``（sync 路径）一直在 ``execute`` 之后调它；async 路径
    （``NodeRunner._run_async_node``）此前**漏调**，同一个节点逻辑写成
    ``execute`` 或 ``arun`` 输出校验行为不同（公开扩展点缺口）。

    ``getattr`` 而非直接属性访问：``arun`` 是鸭子类型扩展点，历史/第三方
    执行器可能传入不继承 ``Node`` 的对象（``_run_with_timeout`` 只检查
    ``arun`` 是协程函数）。没有该校验钩子的对象维持原行为（无操作）。
    """
    validate = getattr(node, "_validate_output", None)
    if validate is not None:
        try:
            validate(result)
        except Exception as exc:  # noqa: BLE001 — 打标后原样抛出
            # plaita#219：输出校验异常仍按普通节点异常走 retryTimes——即使它是
            # ValueError，也不得被 node-F2 的配置错误分类误伤。
            mark_output_validation_retryable(exc)
            raise


def _coerce_strategy(value) -> ErrorStrategy:
    """容忍 ErrorStrategy enum / 字符串 / Mock / None, 一律归一为 enum。

    ErrorHandler.strategy 字段已是 enum, 但仍有外部代码 (含单元测试里的
    Mock 对象) 把 strategy 设为裸字符串。本 helper 让 runner 在所有输入
    形态下都能正确比较, 不必每个调用点都重复 ``==`` + ``.value`` 两套判断。
    """
    if isinstance(value, ErrorStrategy):
        return value
    if value is None:
        return ErrorStrategy.ABORT
    if isinstance(value, str):
        if value == "continue_with":
            return ErrorStrategy.CONTINUE_WITH
        try:
            return ErrorStrategy(value)
        except ValueError:
            return ErrorStrategy.ABORT
    # 兜底: 未知类型 (如 Mock) 默认 abort, 行为最安全
    return ErrorStrategy.ABORT


#: NodeException 错误码 → 重试分类（node-F2）。
#:
#: 数值与 ``plaita.node.http`` 的 ``HTTP_GEN_REQUEST_ERROR`` /
#: ``HTTP_DO_REQUEST_ERROR`` / ``HTTP_NODE_EXEC_ERROR`` 同源；core 不能反向
#: import node（分层禁止），故在此以字面量声明并由测试钉住。
#:
#: - 1001 请求生成错误：URL/表达式/请求体拼装失败，确定性配置问题——重试只会
#:   把同一个错误重复 retryTimes 次，白白放大失败与上游压力；
#: - 1002 发送请求错误：连接失败/超时/上游 5xx 抖动，可重试；
#: - 1003 节点处理错误：上游返回非预期响应（含 429 限流），可重试——但必须
#:   有退避，否则连续重试会把限流窗口打爆。
_NON_RETRYABLE_NODE_CODES = frozenset({1001})
_RETRYABLE_NODE_CODES = frozenset({1002, 1003})

#: 指数退避：第 n 次重试前等待 ``1s × 2^n``，封顶 30s（node-F2）。
_RETRY_BASE_DELAY_SECONDS = 1.0
_RETRY_MAX_DELAY_SECONDS = 30.0
#: 指数上限：2^30 秒已远大于封顶值，仅用于避免 ``2 ** 巨数`` 的构造开销。
_RETRY_MAX_BACKOFF_EXPONENT = 30


def is_retryable_error(exc: BaseException) -> bool:
    """按异常类型/错误码判定是否值得重试（node-F2）。

    - ``NodeException``：1001（请求生成 = 配置/参数确定性错误）不重试；
      1002/1003（发送/处理 = 瞬态）重试；其余未知码维持历史行为（重试），
      避免把未来新增的错误码静默降级成「一次都不重试」。
    - 非 ``NodeException`` 的 ``ValueError``：参数/配置类确定性错误，不重试；
      但节点输出校验钩子抛出的异常除外（#219 契约：校验异常仍走 retryTimes）。
    - 其余异常（``RuntimeError``、节点自身 ``TimeoutError`` 等）：保持历史行为。

    A50-03：公开导出。worker 层的重投判据必须复用同一份分类，否则
    ``NodeExecutionError`` 的 abort 包装会把 1001/ValueError 重新判成可重试，
    core 的「确定性不重试」在跨层边界丢失（同一错误被 worker 重投 5 次）。
    """
    if isinstance(exc, NodeException):
        code = getattr(exc, "code", None)
        if code in _RETRYABLE_NODE_CODES:
            return True
        if code in _NON_RETRYABLE_NODE_CODES:
            return False
        return True
    if isinstance(exc, ValueError):
        return is_output_validation_retryable(exc)
    return True


# 兼容旧私有名（本地/下游对 ``_is_retryable_error`` 的引用与测试注释）。
_is_retryable_error = is_retryable_error


def _retry_backoff_seconds(attempt: int) -> float:
    """第 ``attempt`` 次重试（0 起）前的等待秒数：1s × 2^n，封顶 30s。"""
    exponent = min(max(attempt, 0), _RETRY_MAX_BACKOFF_EXPONENT)
    return min(
        _RETRY_MAX_DELAY_SECONDS,
        _RETRY_BASE_DELAY_SECONDS * (2 ** exponent),
    )


async def _backoff_sleep(delay: float) -> None:
    """重试间隔。独立函数便于测试替换，避免单测真实等待数秒。"""
    await asyncio.sleep(delay)


#: 取消轮询粒度（秒）：重试退避期间每片最多等这么久就复查一次
#: ``cancel_requested``——plaita#267 验收要求 cancel 在 ≤1s 内打断重试/退避。
_CANCEL_POLL_INTERVAL_SECONDS = 0.5

#: 节点级 timeout 的**构造期**上限（毫秒，24h）。#267：``sys.float_info.max``
#: 只挡得住 ``/1000.0`` 的 OverflowError（≈1.8e308），``"999999999999999999"``
#: （≈3,170 万年）此前会被原样接受。24h 远大于任何合法节点超时，同时让
#: 「一个节点钉住共享 worker 数天」在构造期就不可能表达；需要更长预算的场景
#: 用 flow 级 deadline（显式 flow.timeout）。
MAX_NODE_TIMEOUT_MS = 24 * 60 * 60 * 1000


def _cancelled_error(node) -> FlowCancelledException:
    """执行级取消的具名异常（与节点入口的取消拒绝同形）。"""
    err = FlowCancelledException(
        f"Execution cancelled while running node {node.name or node.id}",
        node=node,
    )
    err.source_line = _node_source_loc(node)
    return err


def _raise_if_cancel_requested(cancel_requested, node) -> None:
    """``cancel_requested`` 已置位时立即抛取消（重试循环/退避的共用判据）。

    取消是**执行级意图**：必须拦在重试与 errorHandler 之前，否则
    ``retryTimes=10`` 会把取消拖到退避结束（plaita#267）。
    """
    if cancel_requested is not None and cancel_requested.is_set():
        logger.info("node %s aborted: execution cancellation requested", node.id)
        raise _cancelled_error(node)


async def _backoff_sleep_cancellable(delay: float, cancel_requested, node) -> None:
    """可被 cancel 打断的退避等待（plaita#267）。

    真实等待按 :data:`_CANCEL_POLL_INTERVAL_SECONDS` 分片轮询 ``cancel_requested``，
    最坏延迟 ≈ 一个分片；测试替换的 ``_backoff_sleep``（立即返回）仍只被调用
    一次、收到完整 ``delay``，既有延迟断言零回归。
    """
    if cancel_requested is None:
        await _backoff_sleep(delay)
        return
    _raise_if_cancel_requested(cancel_requested, node)
    if delay <= 0:
        await _backoff_sleep(delay)
        return
    sleeper = asyncio.ensure_future(_backoff_sleep(delay))
    try:
        while True:
            done, _ = await asyncio.wait(
                {sleeper}, timeout=_CANCEL_POLL_INTERVAL_SECONDS
            )
            if sleeper in done:
                await sleeper  # 传播替换实现可能抛出的异常
                return
            if cancel_requested.is_set():
                sleeper.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await sleeper
                logger.info(
                    "node %s retry backoff interrupted by execution cancellation",
                    node.id,
                )
                raise _cancelled_error(node)
    finally:
        if not sleeper.done():
            sleeper.cancel()


#: 非法 timeout 的统一拒收文案（plaita#139）：错误消息逐字复用，避免各处措辞漂移。
_TIMEOUT_FORMS = (
    "use milliseconds as a plain number (e.g. 300 or '300') or an ISO 8601 "
    "duration (e.g. 'PT0.3S', 'PT2S', 'PT1M')"
)

#: 毫秒预算的上界：引擎把毫秒换算成秒时走 float（``timeout_ms / 1000.0``，
#: runner / strategies 多处），超过 ``sys.float_info.max`` 的整数在这一步抛
#: ``OverflowError: int too large to convert to float``——那是内部崩溃，不是
#: 具名拒绝，也不是可用预算。校验层据此具名拒绝（#139 家族：调用方给的预算
#: 要么被采用，要么被点名拒绝，不能变成引擎内部异常）。
_MAX_TIMEOUT_MS = sys.float_info.max


class InvalidTimeoutError(ValueError):
    """非法 timeout 的具名拒绝（plaita#139）。

    ``reason`` 不含值/形状前缀，供上层（distributed 校验层）补上"来源"上下文
    后重新抛出——避免出现 ``Invalid timeout X from S: Invalid timeout X: ...``
    这类重复前缀。
    """

    def __init__(self, value, reason: str, source: Optional[str] = None):
        self.value = value
        self.reason = reason
        self.source = source
        where = f" from {source}" if source else ""
        super().__init__(f"Invalid timeout {value!r}{where}: {reason}; {_TIMEOUT_FORMS}")


def _parse_timeout_uncached(timeout):
    """解析超时为毫秒预算；``None``/``""`` = 不限额，``0`` = 零预算（最严）。

    规则（plaita#138 / #139；**逐句与实现一致**）：

    - ``None`` 或空串 ``""`` ⇒ ``None``：**未提供**超时，不限额。
    - ``0`` / ``0.0`` / ``False`` / ``"0"`` / ``"PT0S"`` / ``"PT"`` ⇒ ``0``：
      **显式零预算**，是最严的限额（立即超时），**不再**被当作"不限额"
      —— 这正是 #138：旧实现让"取最严"在取到最严值（0）时反而完全失效。
    - 正数 ``int``/``float`` ⇒ 毫秒；数字串（``"300"``）⇒ 毫秒；ISO 8601
      **固定长度**时长串（``"PT0.3S"``）⇒ 毫秒。
    - 其余一律 :class:`InvalidTimeoutError`（``ValueError`` 子类，点名违规值 +
      受支持形状）：负数（``-5`` 曾被原样返回，``-0.5`` 更被 ``int()`` 截成
      ``0``）、非有限 ``float``（``inf`` 曾抛裸 ``OverflowError``）、超过
      :data:`_MAX_TIMEOUT_MS` 的整数（引擎内部 ``/1000.0`` 处才炸）、含年/月
      分量的 ISO 时长（无固定秒数，``total_seconds()`` 会静默丢掉年/月）、
      非法类型与非法串。**永不静默返回 ``None``**（= 静默丢弃调用方预算，
      plaita#139）。

    历史上这里以 ``if not timeout: return None`` 开头（#138），并以
    ``return None`` 收尾处理所有非法类型（#139）。
    """
    if timeout is None:
        return None
    if isinstance(timeout, bool):
        # bool 是 int 子类；显式归一，避免 True/False 走数值分支的歧义。
        timeout = int(timeout)
    if isinstance(timeout, (int, float)):
        if isinstance(timeout, float) and not math.isfinite(timeout):
            raise InvalidTimeoutError(timeout, "must be a finite number")
        if timeout < 0:
            raise InvalidTimeoutError(timeout, "must be a non-negative number")
        if timeout > _MAX_TIMEOUT_MS:
            raise InvalidTimeoutError(
                timeout,
                f"exceeds the maximum supported budget ({_MAX_TIMEOUT_MS!r} ms)",
            )
        return int(timeout)
    if isinstance(timeout, str):
        if not timeout:
            return None
        if timeout.isdigit():
            value = int(timeout)
            if value > _MAX_TIMEOUT_MS:
                raise InvalidTimeoutError(
                    timeout,
                    f"exceeds the maximum supported budget ({_MAX_TIMEOUT_MS!r} ms)",
                )
            return value
        try:
            duration = isodate.parse_duration(timeout)
        except Exception as e:
            # 历史上第三方 isodate.ISO8601Error 裸穿透，用户不知道合法格式长什么样
            raise InvalidTimeoutError(
                timeout, "is not a valid ISO 8601 duration"
            ) from e
        if not isinstance(duration, datetime.timedelta):
            # isodate 用 ``Duration`` 表示含年/月分量的时长——它们没有固定秒数，
            # ``total_seconds()`` 会把年/月静默丢掉（``"P1Y"`` → 0、
            # ``"P1YT1H"`` → 3600）。当超时用只会静默给出错误的预算。
            raise InvalidTimeoutError(
                timeout,
                "contains year/month components, which have no fixed length",
            )
        total_seconds = duration.total_seconds()
        if total_seconds < 0:
            raise InvalidTimeoutError(timeout, "must be a non-negative duration")
        return int(total_seconds * 1000)
    raise InvalidTimeoutError(
        timeout, f"has unsupported type {type(timeout).__name__}"
    )


@functools.lru_cache(maxsize=256)
def _parse_timeout_cached(timeout):
    return _parse_timeout_uncached(timeout)


def _parse_timeout(timeout) -> Optional[int]:
    """解析节点/flow 超时为毫秒数（2026-10 加 memo：isodate 解析 ~4µs/次）。

    ``None``/``""`` ⇒ ``None``（未提供 = 不限额）；``0``/``"PT0S"`` ⇒ ``0``
    （**显式零预算**，最严；plaita#138 起不再被静默归成不限额）。

    lru_cache 不缓存异常，非法串保持每次抛 ValueError；不可哈希的输入在进
    缓存前按类型门直接走原逻辑（行为与旧版逐分支一致）。
    """
    if timeout is None or isinstance(timeout, (bool, int, float, str)):
        return _parse_timeout_cached(timeout)
    return _parse_timeout_uncached(timeout)


def _handle_timeout(flow, handler, node, time_limit_by_flow: bool):
    try:
        if handler:
            return handler.handle()
    except TimeoutError:
        message = f"Timeout handler strategy for executing node {node.name or node.id} is abort"
    else:
        message = f"Node {node.name or node.id} execution timeout"
    message += _node_loc_suffix(node)
    error_type = FlowErrorType.FLOW_ERROR if time_limit_by_flow else FlowErrorType.NODE_ERROR
    err = NodeTimeoutError(message, node=node, error_type=error_type)
    err.source_line = _node_source_loc(node)
    raise err


def _handle_flow_result_error(flow, node, e: FlowResultError):
    err = ErrorResultException(e.code, e.message, node=node)
    err.source_line = _node_source_loc(node)
    raise err from e


def _handle_node_error(flow, node, error_handler, e: Exception):
    strategy = _coerce_strategy(error_handler.strategy if error_handler else None)
    if not error_handler or strategy == ErrorStrategy.ABORT:
        # abort：紧接着 raise，完整 traceback 由异常本身携带——这里再打一份
        # exc_info 是双份噪音，故只留 raise。
        code = DEFAULT_NODE_ABORT_CODE if not error_handler else error_handler.error_code
        message = f"执行节点{node.name or node.id}出错了: {type(e).__name__}: {e}{_node_loc_suffix(node)}"
        if error_handler and error_handler.error_message:
            message = error_handler.error_message
        err = NodeExecutionError(message, node=node, code=code)
        err.source_line = _node_source_loc(node)
        raise err from e
    # 被 errorHandler 兜住的错误：简明一行（含策略与异常摘要），完整 traceback
    # 只在 DEBUG 级别提供——历史上无条件 exc_info 打整段栈，是主要日志噪音源。
    logger.warning(
        "node %s error handled (errorHandler.strategy=%s): %s: %s",
        node.name or node.id, strategy.value, type(e).__name__, _scrub_url_text(e),
    )
    logger.debug("handled node error traceback: %s", _scrub_exception_text(e))
    if strategy == ErrorStrategy.CONTINUE:
        return None
    if strategy == ErrorStrategy.CONTINUE_WITH:
        return error_handler.default_value
    return None


def _get_error_result(error_handler):
    if not error_handler:
        return None
    strategy = _coerce_strategy(error_handler.strategy)
    if strategy == ErrorStrategy.CONTINUE:
        return None
    if strategy == ErrorStrategy.CONTINUE_WITH:
        return error_handler.default_value
    return None


def _node_error_payload(exc: BaseException) -> Dict[str, Any]:
    """失败节点的 ``on_node_end(error=...)`` 载荷（plaita#25）。

    形状与 resume 失败路径（``plaita/core/strategies.py`` 的
    ``{"code": -500, "message": str(e)}``）逐字同形，读取方无需分支；
    ``code`` 优先取异常自带的 ``code``（``NodeExecutionError`` 等带码异常），
    否则 -500（与 distributed 错误归一化同口径）。
    """
    try:
        code = getattr(exc, "code", None)
        code = int(code) if code is not None else -500
    except Exception as exc:  # noqa: BLE001 — 取码失败不得影响补发
        logger.debug("失败节点补发的 code 提取失败，回退 -500: %s",
                     _scrub_exception_text(exc))
        code = -500
    try:
        message = str(exc)
    except Exception:  # noqa: BLE001
        message = f"<{type(exc).__name__}>"
    return {"code": code, "message": message}

# 无超时同步节点的共享执行池（2026-09 性能评审 P1）：历史上每个 sync 节点
# 无条件裸起一条 daemon 线程，thread start+join 占线性流程运行时间的 64%。
# 复用池消除线程创建开销；带超时的节点仍走裸 daemon 线程——超时遗弃语义
# 需要"线程不进 atexit 等待"，池做不到。
_SYNC_NODE_POOL: Optional[ThreadPoolExecutor] = None
_SYNC_NODE_POOL_PID: Optional[int] = None


def _get_sync_node_pool() -> ThreadPoolExecutor:
    """按进程惰性创建共享节点池。

    ``Parallel(mode=process)`` 在 Linux 上 fork 子进程执行分支——fork 不继承
    线程，模块级池在子进程里是"没有 worker 的空壳"，提交任务即永久挂死
    （CI Linux 实测 30 分钟 hang；macOS spawn 重新 import 不受影响，故本地
    不复现）。按 PID 检测并在 fork 后重建。
    """
    global _SYNC_NODE_POOL, _SYNC_NODE_POOL_PID
    pid = os.getpid()
    if _SYNC_NODE_POOL is None or _SYNC_NODE_POOL_PID != pid:
        _SYNC_NODE_POOL = ThreadPoolExecutor(thread_name_prefix="plaita-node")
        _SYNC_NODE_POOL_PID = pid
    return _SYNC_NODE_POOL
# 嵌套防护（与 parallel_executor 的池饿死修复同思路）：worker 线程内再提交
# 同一池并阻塞等待（子流程/分支内的 sync 节点），任务数 >= max_workers 时
# 全部 worker 互相等待——CI 2 核（pool=6）上宽分支流程 30 分钟挂死实测。
# 已在池 worker 上的嵌套节点直接内联执行。
_NODE_POOL_TLS = threading.local()


def _node_pool_bound(fn):
    def _wrapped(ctx):
        _NODE_POOL_TLS.in_node_pool = True
        try:
            return fn(ctx)
        finally:
            _NODE_POOL_TLS.in_node_pool = False
    return _wrapped


class NodeRunner:
    """Handles single-node execution with timeout, retry, and error handling."""

    def __init__(self, context: ExecutionContext, node_execution=None) -> None:
        self.context = context
        self.node_execution = node_execution

    async def run_node(
        self,
        flow,
        node,
        *,
        max_timeout_ms: Optional[int] = None,
        callback_manager: Optional[CallbackManager] = None,
    ) -> Tuple[Any, Optional[str]]:
        """Execute a node and return (result, branch).

        Triggers callback_manager.on_node_start/on_node_end.
        Updates context with last_node_id, last_branch, and node result.
        """
        if callback_manager:
            callback_manager.on_node_start(flow, node)

        try:
            result = await self._execute_with_retry(flow, node, max_timeout_ms)
        except BaseException as exc:
            # plaita#25：abort 策略（以及取消/超时）是**直接 raise**，
            # 历史上 on_node_end 永不触发 ⇒ 失败节点在 node_timings、事件时间线
            # 与 Langfuse span 三处同时失踪（obs.py 模块 docstring 自认），
            # 「失败节点恰缺失」的观测盲区由此而来。补发一次 error 形态的
            # on_node_end：错误形状与 resume 失败路径（strategies.py）逐字同形。
            # 回调分发器逐 handler 吞异常，故这里不会遮蔽原始错误。
            # `except BaseException` 是为了把取消（CancelledError/
            # FlowCancelledException）也收进时间线——它们同样是终态事件。
            if callback_manager:
                callback_manager.on_node_end(
                    flow, node, None, _node_error_payload(exc), exception=exc,
                    # 收口批①（裁决 B）：这条是 **abort/取消/超时** 的补发路径 ⇒ 用
                    # **路径标记**告诉带 span 的观测端「标注错误但保持 span 打开」。
                    # 直接错误钩子不带此标记 ⇒ 仍按既有契约终结 span。
                    keep_span_open=True,
                )
            raise

        self.context.last_node_id = node.id
        logger.debug("result: %s", result)
        logger.debug("set last node id: %s", node.id)

        branch = result if node.branching else None
        self.context.last_branch = branch
        self.context.update_node_result(node, result)

        if callback_manager:
            callback_manager.on_node_end(flow, node, result)

        logger.debug("branch: %s", branch)
        return result, branch

    async def _execute_with_retry(
        self,
        flow,
        node,
        max_timeout_ms: Optional[int],
    ) -> Any:
        error_handler = node.error_handler
        # retryTimes 负值已在构造期具名拒绝（B2R27-01 口径变更，2026-10-21）；
        # 这里仍钳到 >=0 作纵深防御——绕过 Pydantic 的构造路径（如
        # model_construct）不致让 range(max_retries+1) 为空、把错误静默吞成
        # 流程"成功"。
        max_retries = max(0, error_handler.retry_times if error_handler else 0)
        config_timeout = self._parse_timeout(node.timeout)

        # cancel_event 的作用域是**当前节点**: 上一个节点超时置位的 cancel 信号
        # 在进入本节点前必须复位——否则本次运行后续所有消费该标志的 process
        # 模式 Parallel 会静默跳过全部分支, 流程带着空结果"成功"结束。
        # (被超时遗弃的上一个节点的线程不受影响: 它丢弃的是结果, 不再消费该标志。)
        exec_ctx = self.node_execution or self.context
        cancel_event = getattr(exec_ctx, "cancel_event", None)
        if cancel_event is not None and cancel_event.is_set():
            logger.debug("resetting cancel_event carried over from a previous node before running %s", node.id)
            cancel_event.clear()

        # cancel_requested 是**执行级**取消意图 (2026-10 波次③): 由
        # ``FlowExecution.cancel()`` 置位, 粘滞、不随节点复位, 与上面每节点
        # clear 的 cancel_event 职责拆分。置位时在本节点入口直接拒绝执行——
        # 在重试循环与 errorHandler 分发之前抛出, 保证取消不被重试/continue
        # 策略吞掉, 沿 FlowExecutionException 同族语义冒泡。只读, 永不 clear。
        cancel_requested = getattr(exec_ctx, "cancel_requested", None)
        if cancel_requested is not None and cancel_requested.is_set():
            err = FlowCancelledException(
                f"Execution cancelled before running node {node.name or node.id}",
                node=node,
            )
            err.source_line = _node_source_loc(node)
            logger.info("node %s rejected: execution cancellation requested", node.id)
            raise err

        total_timeout_ms: Optional[int] = None
        # plaita#138：0 是**合法的最严预算**，必须用 ``is not None`` 判定；
        # 旧的 ``if config_timeout:`` / ``elif max_timeout_ms:`` 把 0 当 falsy
        # 跳过 ⇒ 显式零预算退化成"不限额"（策略传下来的 remaining 归零同理）。
        if config_timeout is not None:
            total_timeout_ms = config_timeout
            if max_timeout_ms is not None:
                total_timeout_ms = min(config_timeout, max_timeout_ms)
        elif max_timeout_ms is not None:
            total_timeout_ms = max_timeout_ms

        # 超时归属：flow 级预算存在且比节点自带预算更严（或节点未给）时，
        # 该节点的超时是"流程级强加"。旧式真值判定在 config_timeout=0 时把
        # 节点自带的零预算误判成"没有节点预算"，从而把归属错记成 FLOW_ERROR。
        time_limit_by_flow = (
            max_timeout_ms is not None
            and (config_timeout is None or max_timeout_ms < config_timeout)
        )

        start_time = time.time()

        for attempt in range(max_retries + 1):
            # plaita#267：取消检查下沉到**每次尝试**的入口——退避期间被取消
            # 由 _backoff_sleep_cancellable 当场打断，这里挡住「取消恰好发生在
            # 退避结束与下一次尝试之间」的窗口。
            _raise_if_cancel_requested(cancel_requested, node)
            try:
                if total_timeout_ms is not None:
                    elapsed_ms = int((time.time() - start_time) * 1000)
                    remaining_timeout = max(0, total_timeout_ms - elapsed_ms)
                else:
                    remaining_timeout = None

                result = await self._run_with_timeout(node, remaining_timeout)
                return result

            except FlowResultError as e:
                _handle_flow_result_error(flow, node, e)
            except _NodeBudgetExceeded:
                # #194：只把**引擎预算**超时交给 timeout_handler；节点自己抛的
                # TimeoutError（含 asyncio.TimeoutError / socket.timeout）走下面
                # 的普通异常分支——重试与 errorHandler 不再静默失效。
                _raise_if_cancel_requested(cancel_requested, node)
                if time_limit_by_flow:
                    # plaita#267：执行级总预算到期必须**具名**且不可被节点
                    # timeoutHandler/errorHandler 吞掉（否则一个 continue 策略
                    # 就能把 deadline 变成摆设）。worker 按 FlowTimeoutError
                    # 家族判定不重试并终态化 error（可收尸）。
                    if node.timeout_handler is not None:
                        logger.warning(
                            "node %s execution deadline exceeded; node timeoutHandler "
                            "bypassed (execution-level budget is terminal)",
                            node.id,
                        )
                    err = ExecutionDeadlineError(
                        f"Flow execution Timeout: execution deadline of "
                        f"{total_timeout_ms}ms exceeded while running node "
                        f"{node.name or node.id}"
                        f"{_node_loc_suffix(node)}",
                        node=node,
                    )
                    err.source_line = _node_source_loc(node)
                    raise err
                return _handle_timeout(flow, node.timeout_handler, node, time_limit_by_flow)
            except (Exception, NodeException) as e:
                # 取消优先于任何节点错误/重试分类：节点在取消后抛出的普通异常
                # 不得被包装成 NodeExecutionError 或再重试一轮（#267）。
                _raise_if_cancel_requested(cancel_requested, node)
                if attempt == max_retries or not _is_retryable_error(e):
                    return _handle_node_error(flow, node, error_handler, e)
                # node-F2：只对瞬态错误重试，重试间隔按指数退避（1s×2^n，封顶
                # 30s）——确定性配置错误（1001 / ValueError）在上面的分类处直接
                # 落到 errorHandler，不再被盲目重试 retryTimes 次；429/限流类
                # 瞬态错误也不再零间隔连打。
                delay = _retry_backoff_seconds(attempt)
                if total_timeout_ms is not None:
                    # 退避也计入执行预算：不得睡过 deadline 再让下一次尝试
                    # 发现预算已耗尽（否则总墙钟可超 deadline 最多 30s）。
                    elapsed_ms = int((time.time() - start_time) * 1000)
                    delay = min(delay, max(0.0, (total_timeout_ms - elapsed_ms) / 1000.0))
                # #151 日志卫生：异常对象/类型投影先落到局部变量，logger 参数里
                # 只允许出现已清洗文本（`_scrub_url_text(e)`）——结构化守卫按
                # AST 判定，裸 `e` 出现在 logger 实参（含 f-string）即判红。
                exc_name = type(e).__name__
                code_suffix = f" code={e.code}" if isinstance(e, NodeException) else ""
                logger.warning(
                    "node %s attempt %d/%d failed (%s%s); retrying in %.1fs: %s",
                    node.name or node.id,
                    attempt + 1,
                    max_retries + 1,
                    exc_name,
                    code_suffix,
                    delay,
                    _scrub_url_text(e),
                )
                await _backoff_sleep_cancellable(delay, cancel_requested, node)

        return _get_error_result(error_handler)

    async def _run_with_timeout(self, node, timeout_ms: Optional[int]) -> Any:
        """Execute node with timeout. Uses asyncio for async nodes, threads for sync."""
        if hasattr(node, "arun") and asyncio.iscoroutinefunction(node.arun):
            return await self._run_async_node(node, timeout_ms)
        return await self._run_sync_node(node, timeout_ms)

    async def _run_async_node(self, node, timeout_ms: Optional[int]) -> Any:
        exec_ctx = self.node_execution or self.context
        if timeout_ms is None:
            result = await node.arun(exec_ctx)
            _validate_node_output(node, result)
            return result
        # #194：不能用 ``wait_for`` + ``except asyncio.TimeoutError`` 判预算——
        # py3.10+ 节点自身抛出的 TimeoutError 与 wait_for 的预算超时在 catch
        # 点完全同型，会把业务超时误当引擎超时。先看 task 是否完成：完成 ⇒
        # 原样透传节点异常（含 TimeoutError，交给重试/errorHandler），未完成
        # ⇒ 才是引擎预算超时。
        task = asyncio.ensure_future(node.arun(exec_ctx))
        try:
            result = await asyncio.wait_for(task, timeout=timeout_ms / 1000.0)
        except asyncio.TimeoutError:
            # 区分来源：wait_for 因预算取消 task 时 task 未完成；task 已完成且
            # 带异常 ⇒ 那是节点自己抛的 TimeoutError，原样透传。
            if task.done() and not task.cancelled():
                exc = task.exception()
                if exc is not None:
                    raise exc
            raise _NodeBudgetExceeded(
                f"Node execution timed out after {timeout_ms}ms"
            )
        # plaita#219：``arun`` 路径此前**不调用** ``_validate_output``——同一节点
        # 逻辑写成 ``execute`` 或 ``arun``，输出校验行为不同。校验放在「正常返回」
        # 之后（两条分支各一次），超时/异常照旧透传，不改变 #194 的归属语义。
        _validate_node_output(node, result)
        return result

    async def _run_sync_node(self, node, timeout_ms: Optional[int]) -> Any:
        """Run a sync node in a raw daemon thread to avoid blocking process exit.

        Uses a daemon threading.Thread instead of ThreadPoolExecutor so Python's
        atexit mechanism does not wait for lingering threads after a timeout
        (ThreadPoolExecutor workers are joined by atexit even when daemon=True).
        On timeout we set cancel_event (cooperative cancel) and let wait_for
        abandon the shielded future — the background thread keeps running but
        will not delay pytest/process teardown.
        """
        exec_ctx = self.node_execution or self.context
        loop = asyncio.get_running_loop()
        # 调度上下文（租户 ContextVar 等）只活在当前线程：ContextVar 不随
        # 新线程继承，节点线程里读到的会是默认值——非 default 租户的节点
        # 因此串到 default 凭据文件（plaita#58）。快照当前 context，让下面
        # 两条跨线程路径都在它的副本里跑。
        node_ctx = contextvars.copy_context()

        if timeout_ms is None:
            # 无超时：直接在共享池中执行并 await——不参与超时遗弃语义，
            # 也就无需裸线程。已在池 worker 上的嵌套调用内联执行（防池饿死，
            # 见 _NODE_POOL_TLS 说明）——同线程，context 本就在。
            if getattr(_NODE_POOL_TLS, "in_node_pool", False):
                return node.run(exec_ctx)

            def _run_node_in_pool() -> Any:
                # plaita#239：跨线程执行 → 租户来源标记降级为 derived。
                mark_tenant_derived()
                return _node_pool_bound(node.run)(exec_ctx)

            return await loop.run_in_executor(
                _get_sync_node_pool(),
                functools.partial(node_ctx.run, _run_node_in_pool),
            )

        cancel_event = getattr(exec_ctx, "cancel_event", None)
        fut: asyncio.Future = loop.create_future()

        def _target() -> None:
            # plaita#239：超时遗弃路径同样跨线程，来源标记 derived。
            mark_tenant_derived()
            try:
                result = node.run(exec_ctx)
                if not loop.is_closed() and not fut.done():
                    loop.call_soon_threadsafe(fut.set_result, result)
            except Exception as exc:  # noqa: BLE001
                if not loop.is_closed() and not fut.done():
                    loop.call_soon_threadsafe(fut.set_exception, exc)

        threading.Thread(
            target=functools.partial(node_ctx.run, _target),
            daemon=True,
            name=f"plaita-sync-{getattr(node, 'id', 'node')}",
        ).start()

        if timeout_ms is None:
            return await fut

        # #194：同 ``_run_async_node``——节点线程自己抛的 TimeoutError 会经
        # fut 透传，必须与「fut 未完成 = 引擎预算超时」分家；否则节点异常被
        # 误当预算超时 abort（retryTimes 失效）。
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout_ms / 1000.0)
        except asyncio.TimeoutError:
            if fut.done() and not fut.cancelled():
                exc = fut.exception()
                if exc is not None:
                    raise exc
            if cancel_event is not None:
                cancel_event.set()
            logger.warning(
                "Sync node %s timed out after %sms (cancel_event set)",
                getattr(node, "id", "unknown"),
                timeout_ms,
            )
            raise _NodeBudgetExceeded(
                f"Node execution timed out after {timeout_ms}ms"
            )

    # Thin wrappers keep historical ``runner._get_error_result`` call sites.
    def _handle_timeout(self, flow, handler, node, time_limit_by_flow: bool):
        return _handle_timeout(flow, handler, node, time_limit_by_flow)

    def _handle_flow_result_error(self, flow, node, e: FlowResultError):
        return _handle_flow_result_error(flow, node, e)

    def _handle_node_error(self, flow, node, error_handler, e: Exception):
        return _handle_node_error(flow, node, error_handler, e)

    def _get_error_result(self, error_handler):
        return _get_error_result(error_handler)

    _parse_timeout = staticmethod(_parse_timeout)
