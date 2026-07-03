"""
plaita.core.runner — NodeRunner for single-node execution.

Handles timeout enforcement (thread-based for sync, asyncio.wait_for for async),
retry logic, error strategy dispatch (abort/continue/continue-with), and
cooperative cancellation via threading.Event.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional, Tuple, TYPE_CHECKING

import isodate

# Bounded shared pool for sync-node execution. Replaces the old per-node
# daemon-thread spawn so a parallel fan-out of sync nodes is bounded instead of
# unbounded. A stuck node occupies a worker until it returns (same abandon
# semantics as before on timeout — cancel_event is set cooperatively); sizing is
# generous to avoid starving legitimate concurrent sync work.
_SYNC_NODE_POOL = ThreadPoolExecutor(
    max_workers=int(os.environ.get("PLAITA_SYNC_NODE_POOL_SIZE", "32")),
    thread_name_prefix="plaita-sync-node",
)

from plaita.core.errors import (
    DEFAULT_NODE_ABORT_CODE,
    ErrorResultException,
    ErrorStrategy,
    FlowErrorType,
    FlowExecutionException,
    FlowResultError,
    NodeException,
    NodeExecutionError,
    NodeTimeoutError,
)

if TYPE_CHECKING:
    from plaita.core.context import ExecutionContext
    from plaita.core.callback import CallbackManager
    from plaita.node.basic import Node

logger = logging.getLogger("plaita.core.runner")


def _node_source_loc(node) -> Optional[int]:
    """取节点回标的源码行号（仅 @flow 前端编译期写入；其余前端为 None）。"""
    return getattr(node, "source_line", None)


def _node_loc_suffix(node) -> str:
    """拼运行期错误消息的源码行号后缀，无回标时返回空串。"""
    sl = _node_source_loc(node)
    return f" (源码第 {sl} 行)" if sl is not None else ""


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

        result = await self._execute_with_retry(flow, node, max_timeout_ms)

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
        max_retries = error_handler.retry_times if error_handler else 0
        config_timeout = self._parse_timeout(node.timeout)

        total_timeout_ms: Optional[int] = None
        if config_timeout:
            total_timeout_ms = config_timeout
            if max_timeout_ms:
                total_timeout_ms = min(config_timeout, max_timeout_ms)
        elif max_timeout_ms:
            total_timeout_ms = max_timeout_ms

        time_limit_by_flow = (not config_timeout and max_timeout_ms is not None) or (
            max_timeout_ms is not None and config_timeout and max_timeout_ms < config_timeout
        )

        start_time = time.time()

        for attempt in range(max_retries + 1):
            try:
                if total_timeout_ms is not None:
                    elapsed_ms = int((time.time() - start_time) * 1000)
                    remaining_timeout = max(0, total_timeout_ms - elapsed_ms)
                else:
                    remaining_timeout = None

                result = await self._run_with_timeout(node, remaining_timeout)
                return result

            except FlowResultError as e:
                self._handle_flow_result_error(flow, node, e)
            except (TimeoutError, asyncio.TimeoutError):
                return self._handle_timeout(flow, node.timeout_handler, node, time_limit_by_flow)
            except (Exception, NodeException) as e:
                if attempt == max_retries:
                    return self._handle_node_error(flow, node, error_handler, e)

        return self._get_error_result(error_handler)

    async def _run_with_timeout(self, node, timeout_ms: Optional[int]) -> Any:
        """Execute node with timeout. Uses asyncio for async nodes, threads for sync."""
        if hasattr(node, "arun") and asyncio.iscoroutinefunction(node.arun):
            return await self._run_async_node(node, timeout_ms)
        return await self._run_sync_node(node, timeout_ms)

    async def _run_async_node(self, node, timeout_ms: Optional[int]) -> Any:
        exec_ctx = self.node_execution or self.context
        if timeout_ms is not None:
            try:
                return await asyncio.wait_for(
                    node.arun(exec_ctx),
                    timeout=timeout_ms / 1000.0,
                )
            except asyncio.TimeoutError:
                raise TimeoutError(f"Node execution timed out after {timeout_ms}ms")
        return await node.arun(exec_ctx)

    async def _run_sync_node(self, node, timeout_ms: Optional[int]) -> Any:
        """Run a sync node on the bounded shared thread pool; bridge via the loop.

        Uses ``loop.run_in_executor`` against ``_SYNC_NODE_POOL`` instead of
        spawning a fresh daemon thread per node, so concurrent sync work is
        bounded. On timeout we set ``cancel_event`` (cooperative cancel) and let
        ``wait_for`` abandon the future — the underlying worker keeps running
        the node to completion but won't block loop teardown (daemon threads in
        the pool).
        """
        exec_ctx = self.node_execution or self.context
        loop = asyncio.get_running_loop()
        cancel_event = getattr(exec_ctx, "cancel_event", None)

        if timeout_ms is None:
            return await loop.run_in_executor(_SYNC_NODE_POOL, node.run, exec_ctx)
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(_SYNC_NODE_POOL, node.run, exec_ctx),
                timeout=timeout_ms / 1000.0,
            )
        except asyncio.TimeoutError:
            if cancel_event is not None:
                cancel_event.set()
            logger.warning("Sync node %s timed out after %sms (cancel_event set)",
                           getattr(node, "id", "unknown"), timeout_ms)
            raise TimeoutError(f"Node execution timed out after {timeout_ms}ms")

    # -- error handling (extracted from FlowExecution) --

    def _handle_timeout(self, flow, handler, node, time_limit_by_flow: bool):
        try:
            if handler:
                result = handler.handle()
                return result
        except TimeoutError:
            message = f"Timeout handler strategy for executing node {node.name or node.id} is abort"
        else:
            message = f"Node {node.name or node.id} execution timeout"
        message += _node_loc_suffix(node)

        error_type = FlowErrorType.FLOW_ERROR if time_limit_by_flow else FlowErrorType.NODE_ERROR
        err = NodeTimeoutError(message, node=node, error_type=error_type)
        err.source_line = _node_source_loc(node)
        raise err

    def _handle_flow_result_error(self, flow, node, e: FlowResultError):
        err = ErrorResultException(e.code, e.message, node=node)
        err.source_line = _node_source_loc(node)
        raise err from e

    def _handle_node_error(self, flow, node, error_handler, e: Exception):
        logger.warning("handle node error: %s", node.name or node.id, exc_info=True)
        strategy = _coerce_strategy(error_handler.strategy if error_handler else None)
        if not error_handler or strategy == ErrorStrategy.ABORT:
            code = DEFAULT_NODE_ABORT_CODE if not error_handler else error_handler.error_code
            message = f"执行节点{node.name or node.id}出错了: {type(e).__name__}: {e}{_node_loc_suffix(node)}"
            if error_handler and error_handler.error_message:
                message = error_handler.error_message
            err = NodeExecutionError(message, node=node, code=code)
            err.source_line = _node_source_loc(node)
            raise err from e
        elif strategy == ErrorStrategy.CONTINUE:
            return None
        elif strategy == ErrorStrategy.CONTINUE_WITH:
            return error_handler.default_value

    def _get_error_result(self, error_handler):
        if not error_handler:
            return None
        strategy = _coerce_strategy(error_handler.strategy)
        if strategy == ErrorStrategy.CONTINUE:
            return None
        elif strategy == ErrorStrategy.CONTINUE_WITH:
            return error_handler.default_value
        return None

    @staticmethod
    def _parse_timeout(timeout) -> Optional[int]:
        if not timeout:
            return None
        if isinstance(timeout, (int, float)):
            return int(timeout)
        if not isinstance(timeout, str):
            return None
        if timeout.isdigit():
            return int(timeout)
        duration = isodate.parse_duration(timeout)
        return None if duration.total_seconds() == 0 else int(duration.total_seconds() * 1000)
