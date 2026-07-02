"""
plaita.core.executor — Execution strategies and FlowExecution facade.

Defines ExecutionMode enum, ExecutionStrategy Protocol, and three concrete
strategies (Normal, Generator, Distributed). FlowExecution is a thin facade
that composes ExecutionContext, NodeRunner, CallbackManager, and a strategy.
"""

from __future__ import annotations

import asyncio
import logging
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, TYPE_CHECKING

from plaita.core.callback import (
    BaseCallbackManager,
    CallbackManager,
    FlowCallback,
    LoggerCallback,
)
from plaita.core.context import ExecutionContext
from plaita.core.errors import (
    FlowErrorException,
    FlowExecutionException,
    FlowStartMissingError,
    FlowTimeoutError,
    ResumeError,
    ResumeType,
)
from plaita.core.runner import NodeRunner

if TYPE_CHECKING:
    from plaita.event.core import EventBus

logger = logging.getLogger("plaita.core.executor")


class ExecutionMode(Enum):
    NORMAL = "normal"
    GENERATOR = "generator"
    DISTRIBUTED = "distributed"

    @classmethod
    def from_string(cls, mode: str) -> ExecutionMode:
        return cls[mode.upper()]


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

        while next_node:
            current = next_node
            result, branch, next_node, reached_end = await _advance_one(
                flow, runner, callback_manager, current,
            )
            yield _create_lazy_output(
                current, result, branch, context.context, execution_id=context.execution_id,
            )
            if reached_end:
                break

            logger.debug("next_node: %s with branch: %s", next_node, branch)

        if not reached_end:
            logger.debug("not reached_end: %s", next_node)
            pfx = context.express_prefix
            result = context.get_state(f"{pfx}{context.express_node_name}", {})
            yield _create_end_output(None, result, context.context, execution_id=context.execution_id)


class DistributedStrategy:
    """Execute one node per call with context persistence for suspend/resume."""

    async def execute(self, flow, context, runner, callback_manager, params=None, timeout_ms=None, **options):
        from plaita.node.event_node import EventNode

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

        current_node, result, branch = await self._determine_current_node(flow, context, runner, callback_manager)

        if not current_node:
            result = context.get_state(f"{pfx}{context.express_node_name}", {})
            return _create_end_output(None, result, context.context, execution_id=context.execution_id)

        return await self._execute_current_node(flow, context, runner, callback_manager, current_node)

    async def _determine_current_node(self, flow, context, runner, callback_manager):
        pfx = context.express_prefix
        last_node_id = context.get_state(f"{pfx}LAST_NODE")
        if last_node_id:
            return self._get_next_from_last(flow, context, last_node_id)
        return await self._start_new_flow(flow, context, runner, callback_manager)

    def _get_next_from_last(self, flow, context, last_node_id):
        pfx = context.express_prefix
        current_node = flow.find_node_by_id(last_node_id)
        node_results = context.get_state(f"{pfx}{context.express_node_name}", {})
        result = node_results.get(last_node_id)
        branch = context.get_state(f"{pfx}BRANCH")

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
        from plaita.node.event_node import EventNode

        result, branch = await runner.run_node(flow, current_node, callback_manager=callback_manager)

        if flow.is_end_node(current_node):
            return _create_end_output(current_node, result, context.context, execution_id=context.execution_id)

        if isinstance(current_node, EventNode):
            await _subscribe_event(current_node, flow, result, context)
            callback_manager.on_node_suspend(flow, current_node)
            callback_manager.on_flow_suspend(flow)
            return _create_lazy_output(
                current_node, result, branch, context.context, is_suspend=True, execution_id=context.execution_id,
            )

        return _create_lazy_output(current_node, result, branch, context.context, execution_id=context.execution_id)

    async def _handle_resume(self, flow, context, runner, callback_manager, resume_type, resume_data):
        from plaita.node.event_node import EventNode
        # 统一在此 coerce, 覆盖 execute (已 coerce, 幂等) 与 _handle_resume_operation
        # (历史直传字符串) 两条入口, 避免裸字符串漏到下面的 enum 比较。
        resume_type = ResumeType.coerce(resume_type)
        pfx = context.express_prefix

        last_node_id = context.get_state(f"{pfx}LAST_NODE")
        if not last_node_id:
            raise ResumeError("No suspended node found for resume")

        current_node = flow.find_node_by_id(last_node_id)
        if not isinstance(current_node, EventNode):
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
            if resume_type is ResumeType.CANCEL:
                result = current_node.on_cancel(exec_ctx)
            elif resume_type is ResumeType.TIMEOUT:
                result = current_node.on_timeout(exec_ctx)
            else:
                result = current_node.on_event(exec_ctx, resume_data)
            callback_manager.on_node_end(flow, current_node, result)
        except Exception as e:
            error = {"code": -500, "message": str(e)}
            callback_manager.on_node_end(flow, current_node, None, error, exception=e)
            logger.error("Error during resume: %s", e, exc_info=True)
            raise ResumeError(str(e), node=current_node) from e

        context.update_node_result(current_node, result)
        return _create_lazy_output(current_node, result, None, context.context, is_suspend=False, execution_id=context.execution_id)


# ---------------------------------------------------------------------------
# Sync/async bridge helpers (module-level; not counted toward facade LOC)
# ---------------------------------------------------------------------------
# 桥接逻辑下沉到 plaita.core.async_utils, 避免 core 反向依赖 event 层。

from plaita.core.async_utils import (  # noqa: E402
    async_gen_to_sync as _async_gen_to_sync,
    run_async_from_sync as _run_async_sync,
)


# ---------------------------------------------------------------------------
# FlowExecution facade
# ---------------------------------------------------------------------------

class FlowExecution:
    """Thin facade composing ExecutionContext, NodeRunner, and strategies.

    Nodes receive this instance as the ``execution`` parameter.  State and
    context access is delegated to the underlying ``ExecutionContext`` via
    **explicit** properties/methods — there is no ``__getattr__``/``__setattr__``
    catch-all, so ``execution.foo`` is either a real attribute, an explicit
    delegate, or a loud ``AttributeError``.  No more phantom attributes that
    silently land in context state.
    """

    def __init__(
        self,
        parent: Optional[FlowExecution] = None,
        verbose: bool = False,
        mode: Optional[str] = None,
        callback_handlers: Optional[List[FlowCallback]] = None,
        callback_manager: Optional[BaseCallbackManager] = None,
        event_bus: Optional[EventBus] = None,
    ):
        self.mode = mode
        self.timeout = None
        self.parent = parent
        self.verbose = verbose
        self._ctx = ExecutionContext(parent=parent._ctx if parent else None, event_bus=event_bus)

        if callback_manager:
            self.callback_manager = callback_manager
            for handler in callback_handlers or []:
                self.callback_manager.add_handler(handler)
        else:
            self.callback_manager = CallbackManager(callback_handlers or [])

        if self.verbose:
            self.callback_manager.add_handler(LoggerCallback())

        self._runner = NodeRunner(self._ctx, node_execution=self)
        self._strategies = {
            ExecutionMode.NORMAL.value: NormalStrategy(),
            ExecutionMode.GENERATOR.value: GeneratorStrategy(),
            ExecutionMode.DISTRIBUTED.value: DistributedStrategy(),
        }

    # -- explicit delegation to ExecutionContext --
    #
    # Every attribute a node is allowed to read/write on ``execution`` is
    # declared here.  Anything not listed simply does not exist on the facade,
    # which is the whole point: typos fail loudly instead of mutating context.

    @property
    def context(self) -> Dict[str, Any]:
        return self._ctx.context

    @context.setter
    def context(self, value: Dict[str, Any]) -> None:
        self._ctx.context = value

    @property
    def execution_id(self) -> str:
        return self._ctx.execution_id

    @property
    def event_bus(self):
        return self._ctx.event_bus

    @event_bus.setter
    def event_bus(self, value) -> None:
        self._ctx.event_bus = value

    @property
    def cancel_event(self):
        return self._ctx.cancel_event

    @property
    def express_prefix(self) -> str:
        return self._ctx.express_prefix

    @express_prefix.setter
    def express_prefix(self, value: str) -> None:
        self._ctx.express_prefix = value

    @property
    def express_input_name(self) -> str:
        return self._ctx.express_input_name

    @express_input_name.setter
    def express_input_name(self, value: str) -> None:
        self._ctx.express_input_name = value

    @property
    def express_parent_name(self) -> str:
        return self._ctx.express_parent_name

    @express_parent_name.setter
    def express_parent_name(self, value: str) -> None:
        self._ctx.express_parent_name = value

    @property
    def express_node_name(self) -> str:
        return self._ctx.express_node_name

    @express_node_name.setter
    def express_node_name(self, value: str) -> None:
        self._ctx.express_node_name = value

    @property
    def express_global_name(self) -> str:
        return self._ctx.express_global_name

    @express_global_name.setter
    def express_global_name(self, value: str) -> None:
        self._ctx.express_global_name = value

    @property
    def express_environment_variable(self) -> str:
        return self._ctx.express_environment_variable

    @express_environment_variable.setter
    def express_environment_variable(self, value: str) -> None:
        self._ctx.express_environment_variable = value

    # -- state helpers (delegate to ExecutionContext) --

    def set_state(self, key: str, value: Any) -> None:
        return self._ctx.set_state(key, value)

    def get_state(self, key: str, default: Any = None) -> Any:
        return self._ctx.get_state(key, default)

    def evaluate(self, value: Any) -> Any:
        return self._ctx.evaluate(value)

    def get_global_variable(self, key: str, default: Any = None) -> Any:
        return self._ctx.get_global_variable(key, default)

    def get_or_create_event_bus(self):
        return self._ctx.get_or_create_event_bus()

    def update_node_result(self, node, result: Any) -> None:
        return self._ctx.update_node_result(node, result)

    def clean(self) -> None:
        return self._ctx.clean()

    def setup_flow(self, flow, args: tuple, kwargs: dict) -> None:
        return self._ctx.setup_flow(flow, args, kwargs)

    # -- renamed context helpers (kept for backward compat) --

    def _set_state(self, key, value):
        self._ctx.set_state(key, value)

    def _get_state(self, key, default=None):
        return self._ctx.get_state(key, default)

    def _update_node_result(self, node, result):
        self._ctx.update_node_result(node, result)

    def _get_execution_id(self):
        return self._ctx.execution_id

    def _setup_flow_context(self, flow, args, kwargs):
        self._ctx.setup_flow(flow, args, kwargs)

    # -- backward-compat delegates to NodeRunner / DistributedStrategy --

    def _process_node(self, flow, node, lazy=False, max_timeout=None):
        return _run_async_sync(self._runner.run_node(
            flow, node, max_timeout_ms=max_timeout, callback_manager=self.callback_manager))

    def _handle_resume_operation(self, flow, resume_type, resume_data=None):
        return _run_async_sync(self._strategies[ExecutionMode.DISTRIBUTED.value]._handle_resume(
            flow, self._ctx, self._runner, self.callback_manager, resume_type, resume_data))

    # -- child execution & shared utilities --

    def get_child_execution(self):
        # 用 child() 不传额外 handler: 子级通过 inherit_handlers 继承父级各一份,
        # 避免把父级 handler 当"新增 handler"再传一次导致双发。
        child = FlowExecution(self, callback_manager=self.callback_manager.child())
        child.mode = self.mode
        return child

    @staticmethod
    def _parse_timeout(timeout) -> Optional[int]:
        return NodeRunner._parse_timeout(timeout)

    # -- public entry points (preserved signatures) --

    @classmethod
    def run(
        cls,
        flow,
        params: Optional[Dict] = None,
        mode: Optional[Any] = None,
        timeout: Optional[int] = None,
        context: Optional[Dict] = None,
        event_bus=None,
        callback_handlers: Optional[List[FlowCallback]] = None,
        **options,
    ):
        execution = cls(event_bus=event_bus, callback_handlers=callback_handlers)
        for opt in ("express_prefix", "express_input_name", "express_parent_name",
                    "express_node_name", "express_global_name", "express_environment_variable"):
            if opt in options:
                setattr(execution, opt, options[opt])
        execution.mode = mode.value if isinstance(mode, ExecutionMode) else mode
        execution.timeout = timeout or execution.timeout
        execution.clean()
        if execution.mode == "distributed":
            # 统一走 run_distributed, 避免与 run_distributed 维护两份几乎相同的
            # 桥接/错误处理逻辑; options 里可带 resume_type / resume_data。
            #
            # 注意：本方法每次调用都创建新实例。如果需要跨多个步骤保留用户回调，
            # 应直接实例化 FlowExecution 并复用同一实例调用 run_distributed()，
            # 而非反复调用此类方法。
            return execution.run_distributed(
                flow,
                params,
                saved_context=context,
                resume_type=options.get("resume_type", "continue"),
                resume_data=options.get("resume_data"),
                timeout=timeout,
            )
        return execution.execute(flow, params, lazy=(execution.mode == "generator"), timeout=timeout)

    def execute(self, flow, params=None, lazy=False, timeout=None, **options):
        if params is not None and not isinstance(params, dict):
            raise TypeError("params must be a dictionary or None")
        if params is None:
            params = {}
        # 调用方传入的 timeout 与已设置的 self.timeout 取更严者, 不再静默丢弃
        if timeout is not None:
            merged = self._merge_timeout(self.timeout, timeout)
            self.timeout = merged
        return self.run_compatible(flow, lazy, **params)

    def run_compatible(self, flow, lazy, *args, **kwargs):
        """Sync execution. Returns the result, or a sync generator when lazy.

        In lazy/generator mode ``on_flow_end`` is deferred until the returned
        generator is actually consumed or closed — historically it fired
        immediately with the unconsumed generator as ``result``, which meant
        the lifecycle end callback ran before any node executed.
        """
        if lazy:
            return self._lazy_sync_generator(flow, args, kwargs)
        return _run_async_sync(self._finish(self._prepare_strategy(flow, False, args, kwargs), flow))

    def _lazy_sync_generator(self, flow, args, kwargs):
        syncgen = _async_gen_to_sync(self._prepare_strategy(flow, True, args, kwargs))
        exception = None
        try:
            while True:
                try:
                    yield next(syncgen)
                except StopIteration:
                    break
        except Exception as e:
            exception = e
            raise
        finally:
            syncgen.close()
            if exception is not None and not isinstance(exception, FlowExecutionException):
                error = {"code": -500, "message": str(exception)}
                self.callback_manager.on_flow_end(flow, None, error, exception=exception)
            else:
                self.callback_manager.on_flow_end(flow, result=None)

    async def arun_compatible(self, flow, lazy, *args, **kwargs):
        """Async execution — canonical path."""
        if lazy:
            return self._lazy_async_generator(flow, args, kwargs)
        return await self._finish(self._prepare_strategy(flow, False, args, kwargs), flow)

    async def _lazy_async_generator(self, flow, args, kwargs):
        agen = self._prepare_strategy(flow, True, args, kwargs)
        exception = None
        try:
            async for item in agen:
                yield item
        except Exception as e:
            exception = e
            raise
        finally:
            if exception is not None and not isinstance(exception, FlowExecutionException):
                error = {"code": -500, "message": str(exception)}
                self.callback_manager.on_flow_end(flow, None, error, exception=exception)
            else:
                self.callback_manager.on_flow_end(flow, result=None)

    def _prepare_strategy(self, flow, lazy, args, kwargs):
        """Sync orchestration: clean, flow_start, setup_flow. Returns the
        strategy's awaitable/async-generator for later driving."""
        self.clean()
        self.callback_manager.on_flow_start(flow)
        self._ctx.setup_flow(flow, args, kwargs)
        # flow 级超时与调用方超时取更严者
        timeout_ms = self._merge_timeout_ms(
            self._parse_timeout(flow.timeout),
            self._parse_timeout(self.timeout),
        )
        strategy = self._strategies[
            ExecutionMode.GENERATOR.value if lazy else ExecutionMode.NORMAL.value
        ]
        return strategy.execute(flow, self._ctx, self._runner, self.callback_manager, None, timeout_ms)

    @staticmethod
    def _merge_timeout(a, b):
        """Merge two raw timeout values (ms int/float or ISO duration string).

        Returns the more restrictive (smaller) non-empty one. ``None``/empty
        means "no limit".
        """
        ta = NodeRunner._parse_timeout(a)
        tb = NodeRunner._parse_timeout(b)
        ms = FlowExecution._merge_timeout_ms(ta, tb)
        return ms

    @staticmethod
    def _merge_timeout_ms(a_ms, b_ms):
        if a_ms is None:
            return b_ms
        if b_ms is None:
            return a_ms
        return min(a_ms, b_ms)

    async def _finish(self, coro, flow):
        try:
            result = await coro
        except FlowExecutionException:
            raise
        except Exception as e:
            error = {"code": -500, "message": str(e)}
            self.callback_manager.on_flow_end(flow, None, error, exception=e)
            logger.error("flow error", exc_info=True)
            raise FlowErrorException(str(e)) from e
        self.callback_manager.on_flow_end(flow, result=result)
        return result

    # -- distributed mode (sync, preserves legacy -500 error semantics) --

    def run_distributed(
        self,
        flow,
        params: Optional[Dict] = None,
        *,
        saved_context: Optional[Dict] = None,
        resume_type: str = "continue",
        resume_data: Optional[Any] = None,
        timeout: Optional[int] = None,
    ) -> Dict:
        """Drive one distributed step, **reusing this execution's** context /
        runner / callback manager so user callbacks persist across steps.

        Prefer this over the ``FlowExecution.run`` classmethod when you need
        to advance a distributed flow node-by-node without losing callbacks.
        """
        coro = self._strategies[ExecutionMode.DISTRIBUTED.value].execute(
            flow, self._ctx, self._runner, self.callback_manager, params, timeout,
            saved_context=saved_context, resume_type=resume_type, resume_data=resume_data,
        )
        # 历史上 run_distributed 把任何异常（含具体的 FlowExecutionException 子类）
        # 归一化为 FLOW_ERROR / -500 作为分布式对外契约；此处保留该契约，
        # 具体子类仅用于内部抛点与 normal 模式（_finish 让其透传）。
        try:
            return _run_async_sync(coro)
        except Exception as e:
            error = {"code": -500, "message": str(e)}
            self.callback_manager.on_flow_end(flow, None, error, exception=e)
            logger.error("flow error", exc_info=True)
            raise FlowErrorException(str(e)) from e

    def _run_distributed(self, flow, params=None, timeout=None, context=None,
                         resume_type="continue", resume_data=None, **options):
        """Backward-compat wrapper around :meth:`run_distributed`.

        Kept because historical callers invoked ``FlowExecution.run(..., mode=
        DISTRIBUTED, context=...)`` which routed here. New code should call
        ``run_distributed`` directly.
        """
        return self.run_distributed(
            flow,
            params,
            saved_context=context,
            resume_type=resume_type,
            resume_data=resume_data,
            timeout=timeout,
        )


# ---------------------------------------------------------------------------
# Helpers (module-level)
# ---------------------------------------------------------------------------


def _create_lazy_output(node, result, branch, context, is_suspend=False, execution_id=None):
    return {
        "id": node.id,
        "type": node.node_type,
        "name": node.name,
        "result": result,
        "branch": branch or "",
        "context": context,
        "is_end": False,
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
