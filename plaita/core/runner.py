"""
plaita.core.runner — NodeRunner for single-node execution.

Handles timeout enforcement (thread-based for sync, asyncio.wait_for for async),
retry logic, error strategy dispatch (abort/continue/continue-with), and
cooperative cancellation via threading.Event.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any, Optional, Tuple, TYPE_CHECKING

import isodate

from plaita.core.errors import (
    DEFAULT_NODE_ABORT_CODE,
    ErrorStrategy,
    FlowErrorType,
    FlowExecutionException,
    FlowResultError,
    NodeException,
    _strategy_eq,
)

if TYPE_CHECKING:
    from plaita.core.context import ExecutionContext
    from plaita.core.callback import CallbackManager
    from plaita.node.basic import Node

logger = logging.getLogger("plaita.core.runner")


def _set_result(fut, value):
    if not fut.done():
        fut.set_result(value)


def _set_exc(fut, exc):
    if not fut.done():
        fut.set_exception(exc)


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
        Updates context with LAST_NODE, BRANCH, and node result.
        """
        if callback_manager:
            callback_manager.on_node_start(flow, node)

        result = await self._execute_with_retry(flow, node, max_timeout_ms)

        pfx = self.context.express_prefix
        self.context.set_state(f"{pfx}LAST_NODE", node.id)
        logger.debug("result: %s", result)
        logger.debug("set last node id: %s", node.id)

        branch = result if node.branching else None
        self.context.set_state(f"{pfx}BRANCH", branch)
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
        """Run sync node on a daemon thread; bridge result via a Future.

        Keeps the event loop free (we ``await`` a future, never ``join``).
        On timeout, set ``cancel_event`` (cooperative cancel) and abandon the
        daemon thread -- it keeps running but won't block loop teardown/exit.
        """
        exec_ctx = self.node_execution or self.context
        loop = asyncio.get_running_loop()
        cancel_event = getattr(exec_ctx, "cancel_event", None)
        fut = loop.create_future()

        def _run():
            try:
                result = node.run(exec_ctx)
            except BaseException as e:  # noqa: BLE001 - 透传节点原始异常
                if not fut.done():
                    loop.call_soon_threadsafe(_set_exc, fut, e)
            else:
                if not fut.done():
                    loop.call_soon_threadsafe(_set_result, fut, result)

        threading.Thread(target=_run, daemon=True).start()

        if timeout_ms is None:
            return await fut
        try:
            return await asyncio.wait_for(fut, timeout=timeout_ms / 1000.0)
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

        error = {"code": -1, "message": message}
        error_type = FlowErrorType.FLOW_ERROR if time_limit_by_flow else FlowErrorType.NODE_ERROR
        raise FlowExecutionException(-1, message, error_type, node)

    def _handle_flow_result_error(self, flow, node, e: FlowResultError):
        error = {"code": e.code, "message": e.message}
        raise FlowExecutionException(e.code, e.message, FlowErrorType.ERROR_RESULT) from e

    def _handle_node_error(self, flow, node, error_handler, e: Exception):
        logger.warning(f"handle node error: {node.name or node.id}", exc_info=True)
        strategy = error_handler.strategy if error_handler else ErrorStrategy.ABORT
        if not error_handler or _strategy_eq(strategy, ErrorStrategy.ABORT):
            code = DEFAULT_NODE_ABORT_CODE if not error_handler else error_handler.error_code
            message = f"执行节点{node.name or node.id}出错了: {type(e).__name__}: {e}"
            if error_handler and error_handler.error_message:
                message = error_handler.error_message
            raise FlowExecutionException(code, message, FlowErrorType.NODE_ERROR, node) from e
        elif _strategy_eq(strategy, ErrorStrategy.CONTINUE):
            return None
        elif _strategy_eq(strategy, ErrorStrategy.CONTINUE_WITH):
            return error_handler.default_value

    def _get_error_result(self, error_handler):
        if not error_handler:
            return None
        strategy = error_handler.strategy
        if _strategy_eq(strategy, ErrorStrategy.CONTINUE):
            return None
        elif _strategy_eq(strategy, ErrorStrategy.CONTINUE_WITH):
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
