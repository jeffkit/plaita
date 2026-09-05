"""plaita.core.strategies — Execution strategies and mode/option definitions.

0.5.0 从 ``plaita.core.executor`` 拆出。本模块只装"执行策略"层:
- ``ExecutionMode`` / ``ExecutionStrategy`` Protocol / ``RunOptions`` / ``_StateView``;
- 三个具体策略 ``NormalStrategy`` / ``GeneratorStrategy`` / ``DistributedStrategy``;
- 策略共用的模块级 helper: ``_advance_one`` / ``_create_lazy_output`` /
  ``_create_end_output`` / ``_subscribe_event``。

Driver (``FlowExecution``) 留在 ``plaita.core.executor``, 通过本模块组合策略。
``executor`` 重新导出本模块的公开符号, 保持 ``from plaita.core.executor import
ExecutionMode / NormalStrategy ...`` 的历史导入路径不断。
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, TYPE_CHECKING

from plaita.core.callback import BaseCallbackManager, CallbackManager
from plaita.core.context import ExecutionContext
from plaita.core.errors import (
    FlowExecutionException,
    FlowStartMissingError,
    FlowTimeoutError,
    ResumeError,
    ResumeType,
)
from plaita.core.runner import NodeRunner

if TYPE_CHECKING:
    from plaita.event.core import EventBus

logger = logging.getLogger("plaita.core.strategies")


@dataclass
class RunOptions:
    """Per-run execution knobs, owned by the Driver (``FlowExecution``).

    Bundling ``mode`` / ``timeout`` into a dataclass makes "these are call-time
    options, not component lifecycle state" explicit. Strategies still receive
    ``timeout_ms`` as a loose param (their protocol is fixed by SC-010 tests);
    this object is Driver-internal plus the ``execution.options`` facade for
    node plugins that read ``execution.mode``.

    0.5.0 起 ``mode`` 类型为 ``Optional[ExecutionMode]``——历史上是裸字符串
    (``"normal"``/``"generator"``/``"distributed"``), 全库散落 ``mode == "generator"``
    字符串比较, 拼写错误静默成 False。现在内部一律走 enum, 公共入口 (``Flow.run``
    / ``FlowExecution.__init__``) 仍接受字符串, 经 ``_coerce_mode`` 统一一次。
    """

    mode: Optional[ExecutionMode] = None
    timeout: Optional[int] = None


def _coerce_mode(m) -> Optional[ExecutionMode]:
    """把字符串 / enum / None 统一成 ``Optional[ExecutionMode]``。

    公共入口接受 ``mode="generator"`` 这种历史字符串写法, 在边界处 coerce 一次,
    内部比较全用 enum, 杜绝 ``mode == "generater"`` 这类拼写错误静默成 False。
    """
    if m is None or isinstance(m, ExecutionMode):
        return m
    if isinstance(m, str):
        return ExecutionMode.from_string(m)
    return m


class _StateView:
    """Typed, ``None``-normalized read view over ``ExecutionContext`` state.

    ``execution.state.flow_id`` returns ``None`` for an unset key, vs the raw
    ``CheckpointState`` field which holds the ``_UNSET`` sentinel. Replaces the
    historical ``execution.flow_id`` / ``execution.last_node_id`` /
    ``execution.last_branch`` facade pass-throughs so the Driver no longer
    re-exports typed state under bare attribute names.
    """

    __slots__ = ("_ctx",)

    def __init__(self, ctx: ExecutionContext) -> None:
        self._ctx = ctx

    @property
    def flow_id(self) -> Optional[str]:
        return self._ctx.get_state(f"{self._ctx.express_prefix}FLOW_ID")

    @property
    def last_node_id(self) -> Optional[str]:
        return self._ctx.get_state(f"{self._ctx.express_prefix}LAST_NODE")

    @property
    def last_branch(self) -> Optional[str]:
        return self._ctx.get_state(f"{self._ctx.express_prefix}BRANCH")


class ExecutionMode(Enum):
    NORMAL = "normal"
    GENERATOR = "generator"
    DISTRIBUTED = "distributed"

    @classmethod
    def from_string(cls, mode: str) -> "ExecutionMode":
        try:
            return cls[mode.upper()]
        except KeyError:
            valid = ", ".join(m.value for m in cls)
            raise ValueError(
                f"Unknown execution mode: {mode!r}. Valid modes: {valid}"
            ) from None


class ExecutionStrategy(Protocol):
    """Protocol for mode-specific flow execution."""

    async def execute(
        self,
        flow,
        context: ExecutionContext,
        runner: NodeRunner,
        callback_manager: CallbackManager,
        params: Optional[Dict] = None,
        timeout_ms: Optional[int] = None,
        **options: Any,
    ) -> Any: ...


class NormalStrategy:
    """Execute all nodes to completion, return final result."""

    async def execute(self, flow, context, runner, callback_manager, params=None, timeout_ms=None, **options):
        next_node = flow.start_node
        if next_node is None:
            raise FlowStartMissingError()
        result = None
        reached_end = False
        start_time = time.time()

        while next_node:
            remaining = None
            if timeout_ms is not None:
                elapsed_ms = int((time.time() - start_time) * 1000)
                remaining = max(0, timeout_ms - elapsed_ms)

            result, branch, next_node, reached_end = await _advance_one(
                flow, runner, callback_manager, next_node, max_timeout_ms=remaining,
            )
            if reached_end:
                break

            logger.debug("next_node: %s with branch: %s", next_node, branch)

            if timeout_ms is not None and (time.time() - start_time) > timeout_ms / 1000:
                raise FlowTimeoutError()

        if not reached_end:
            logger.debug("not reached_end: %s", next_node)
            pfx = context.express_prefix
            result = context.get_state(f"{pfx}{context.express_node_name}", {})

        return result


class GeneratorStrategy:
    """Async generator yielding per-node output for debug/stepping."""

    async def execute(self, flow, context, runner, callback_manager, params=None, timeout_ms=None, **options):
        next_node = flow.start_node
        if next_node is None:
            raise FlowStartMissingError()
        reached_end = False
        start_time = time.time()

        while next_node:
            current = next_node
            remaining = None
            if timeout_ms is not None:
                remaining = max(0, timeout_ms - int((time.time() - start_time) * 1000))
            result, branch, next_node, reached_end = await _advance_one(
                flow, runner, callback_manager, current, max_timeout_ms=remaining,
            )
            # End 节点这一步就是流程终点, is_end 必须为 True——历史上这里
            # 恒为 False, 消费方按 ``step["is_end"]`` 判完成永远不触发
            # (0.5.0 回归: docs/guide/execution-modes.md 的完成判断失效)。
            yield _create_lazy_output(
                current, result, branch, context.to_dict(),
                is_end=reached_end, execution_id=context.execution_id,
            )
            if reached_end:
                break

            logger.debug("next_node: %s with branch: %s", next_node, branch)

            if timeout_ms is not None and (time.time() - start_time) > timeout_ms / 1000:
                raise FlowTimeoutError()

        if not reached_end:
            logger.debug("not reached_end: %s", next_node)
            pfx = context.express_prefix
            result = context.get_state(f"{pfx}{context.express_node_name}", {})
            yield _create_end_output(None, result, context.to_dict(), execution_id=context.execution_id)


class DistributedStrategy:
    """Execute one node per call with context persistence for suspend/resume."""

    async def execute(self, flow, context, runner, callback_manager, params=None, timeout_ms=None, **options):
        saved_context = options.get("saved_context")
        resume_type = ResumeType.coerce(options.get("resume_type", "continue"))
        resume_data = options.get("resume_data")
        pfx = context.express_prefix

        if saved_context:
            context.context = saved_context
        else:
            context.clean()
            context.setup_flow(flow, (), params or {})
            callback_manager.on_flow_start(flow)

        if saved_context and resume_type is not ResumeType.CONTINUE:
            return await self._handle_resume(flow, context, runner, callback_manager, resume_type, resume_data)

        if saved_context and resume_type is ResumeType.CONTINUE:
            # 防御: 挂起中的 EventNode 不允许用默认 ``continue`` 绕过——历史上
            # 这条路会跳过 pending 校验直接推进到 End, 流程"正常完成", 事件
            # 永不消费且订阅泄漏, 无任何报错。只有事件已被 resume 消费
            # (状态不再是 pending) 后才允许 continue 推进后续节点。
            last_node_id = context.last_node_id
            if last_node_id:
                suspended_node = flow.find_node_by_id(last_node_id)
                # ``is True`` 而非真值判断: is_suspending 是 bool 属性, 需要滤掉
                # mock/异构节点对象上的 truthy 属性代理。
                if suspended_node is not None and getattr(suspended_node, "is_suspending", False) is True:
                    pfx_resume = context.express_prefix
                    node_results = context.get_state(f"{pfx_resume}{context.express_node_name}", {})
                    prev_state = node_results.get(last_node_id) if isinstance(node_results, dict) else None
                    status = prev_state.get("status", "") if isinstance(prev_state, dict) else ""
                    if status == "pending":
                        raise ResumeError(
                            f"Execution is suspended at EventNode {last_node_id!r} (status=pending); "
                            "resume_type='continue' would silently skip it. "
                            "Use resume_type='event'/'cancel'/'timeout' to resolve the event first.",
                            node=suspended_node,
                        )

        current_node, result, branch = await self._determine_current_node(flow, context, runner, callback_manager)

        if not current_node:
            result = context.get_state(f"{pfx}{context.express_node_name}", {})
            return _create_end_output(None, result, context.to_dict(), execution_id=context.execution_id)

        return await self._execute_current_node(flow, context, runner, callback_manager, current_node)

    async def _determine_current_node(self, flow, context, runner, callback_manager):
        last_node_id = context.last_node_id
        if last_node_id:
            return self._get_next_from_last(flow, context, last_node_id)
        return await self._start_new_flow(flow, context, runner, callback_manager)

    def _get_next_from_last(self, flow, context, last_node_id):
        pfx = context.express_prefix
        current_node = flow.find_node_by_id(last_node_id)
        node_results = context.get_state(f"{pfx}{context.express_node_name}", {})
        result = node_results.get(last_node_id)
        branch = context.last_branch

        # 统一走 flow.next_node: 分支节点按 branch 选 branch.next, 普通节点走 next,
        # 避免在此重复实现一套与 flow._get_branch_target 易漂移的图遍历逻辑。
        current_node = flow.next_node(current_node, branch)
        return current_node, result, branch

    async def _start_new_flow(self, flow, context, runner, callback_manager):
        start_node = flow.start_node
        if not start_node:
            return None, None, None

        result, branch = await runner.run_node(flow, start_node, callback_manager=callback_manager)

        # flow.next_node already handles both branching and non-branching nodes;
        # no need to replicate the "if start_node.next" guard here.
        current_node = flow.next_node(start_node, branch)
        return current_node, result, branch

    async def _execute_current_node(self, flow, context, runner, callback_manager, current_node):
        result, branch = await runner.run_node(flow, current_node, callback_manager=callback_manager)

        if flow.is_end_node(current_node):
            return _create_end_output(current_node, result, context.to_dict(), execution_id=context.execution_id)

        if current_node.is_suspending:
            # 订阅失败时禁止挂起：否则 execution 停在 suspended，而 pending
            # 校验又会挡住 resume，流程永久僵尸化。
            subscribed = await _subscribe_event(current_node, flow, result, context)
            if not subscribed:
                raise FlowExecutionException(
                    message=(
                        f"Event subscription failed for node {current_node.id}; "
                        "refusing to suspend without an active subscription"
                    ),
                    node=current_node,
                )
            callback_manager.on_node_suspend(flow, current_node)
            callback_manager.on_flow_suspend(flow)
            return _create_lazy_output(
                current_node, result, branch, context.to_dict(), is_suspend=True, execution_id=context.execution_id,
            )

        return _create_lazy_output(current_node, result, branch, context.to_dict(), execution_id=context.execution_id)

    async def _handle_resume(self, flow, context, runner, callback_manager, resume_type, resume_data):
        # 统一在此 coerce, 覆盖 execute (已 coerce, 幂等) 与 _handle_resume_operation
        # (历史直传字符串) 两条入口, 避免裸字符串漏到下面的 enum 比较。
        resume_type = ResumeType.coerce(resume_type)
        pfx = context.express_prefix

        last_node_id = context.last_node_id
        if not last_node_id:
            raise ResumeError("No suspended node found for resume")

        current_node = flow.find_node_by_id(last_node_id)
        # 用 is_suspending 标志判定, 而非 isinstance(EventNode): core 层不反向依赖
        # node 插件层; 保留 "is not an EventNode" 文案以兼容历史测试断言。
        if not current_node.is_suspending:
            raise ResumeError(f"Node {current_node.id} is not an EventNode", node=current_node)
        node_results = context.get_state(f"{pfx}{context.express_node_name}", {})
        prev_state = node_results.get(last_node_id, {})
        if prev_state.get("status", "") != "pending":
            raise ResumeError(
                f"EventNode {current_node.id} is not in pending status: {prev_state.get('status', '')}",
                node=current_node,
            )
        if resume_type not in (ResumeType.CANCEL, ResumeType.TIMEOUT, ResumeType.EVENT):
            raise ResumeError(
                f"Unsupported resume type for EventNode: {resume_type.value}, only support cancel, timeout, event",
                node=current_node,
            )

        exec_ctx = runner.node_execution or context
        callback_manager.on_flow_resume(flow)
        callback_manager.on_node_resume(flow, current_node)

        try:
            # 多态分发下沉到节点自身 (Node.resume), core 不再 import EventNode
            result = current_node.resume(exec_ctx, resume_type, resume_data)
            callback_manager.on_node_end(flow, current_node, result)
        except Exception as e:
            error = {"code": -500, "message": str(e)}
            callback_manager.on_node_end(flow, current_node, None, error, exception=e)
            logger.error("Error during resume: %s", e, exc_info=True)
            resume_err = ResumeError(str(e), node=current_node)
            resume_err.source_line = getattr(current_node, "source_line", None)
            raise resume_err from e

        context.update_node_result(current_node, result)
        return _create_lazy_output(current_node, result, None, context.to_dict(), is_suspend=False, execution_id=context.execution_id)


# ---------------------------------------------------------------------------
# Strategy-shared helpers (module-level)
# ---------------------------------------------------------------------------


def _create_lazy_output(node, result, branch, context, is_suspend=False, execution_id=None, is_end=False):
    return {
        "id": node.id,
        "type": node.node_type,
        "name": node.name,
        "result": result,
        "branch": branch or "",
        "context": context,
        "is_end": is_end,
        "is_suspend": is_suspend,
        "execution_id": execution_id,
    }


async def _advance_one(flow, runner, callback_manager, node, max_timeout_ms=None):
    """Run one node via *runner*, update LAST_NODE/BRANCH/$NODE state, then resolve
    the next node. Returns ``(result, branch, next_node, is_end)``.

    Shared by ``NormalStrategy`` and ``GeneratorStrategy`` so the
    run → detect-end → resolve-next sequence lives in exactly one place.
    Distributed mode cannot share it wholesale because it suspends on
    ``EventNode`` instead of advancing, so it keeps its own single-step path.
    """
    result, branch = await runner.run_node(
        flow, node, max_timeout_ms=max_timeout_ms, callback_manager=callback_manager,
    )
    is_end = flow.is_end_node(node)
    next_node = None if is_end else flow.next_node(node, branch)
    return result, branch, next_node, is_end


def _create_end_output(node, result, context, execution_id=None):
    return {
        "id": node.id if node else None,
        "type": "end",
        "name": node.name if node else "结束",
        "result": result,
        "branch": "",
        "context": context,
        "is_end": True,
        "is_suspend": False,
        "execution_id": execution_id,
    }


async def _subscribe_event(node, flow, node_state, context):
    """Subscribe an EventNode to its event bus.

    This is an async function: when ``event_bus.register_subscription`` is a
    coroutine function it is awaited directly inside the caller's event loop,
    rather than being bridged through a worker thread + fresh loop.
    """
    event_bus = context.get_or_create_event_bus()
    if not event_bus:
        error_state = node.on_error(context, "Unable to get event bus")
        context.update_node_result(node, error_state)
        return False

    try:
        resolved_event_type = node_state.get("event_type", node.event_type)
        subscription_params = {
            "event_type": resolved_event_type,
            "filter_condition": node.event_filter,
            "correlation_id": context.execution_id,
            "flow_id": flow.flow_id,
            "node_id": node.id,
        }

        if asyncio.iscoroutinefunction(event_bus.register_subscription):
            subscription_id = await event_bus.register_subscription(**subscription_params)
        else:
            subscription_id = event_bus.register_subscription(**subscription_params)

        node_state["subscription_id"] = subscription_id
        context.update_node_result(node, node_state)
        return True
    except Exception as e:
        error_state = node.on_error(context, f"Subscribe failed: {e}")
        context.update_node_result(node, error_state)
        return False
