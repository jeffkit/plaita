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
from plaita.core.errors import FlowErrorType, FlowExecutionException, FlowResultError
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
        from plaita.node import End
        next_node = flow.start_node
        if next_node is None:
            raise FlowExecutionException(
                -500, "Flow has no start node", FlowErrorType.NODE_NOT_FOUND,
            )
        result = None
        reached_end = False
        start_time = time.time()

        while next_node:
            remaining = None
            if timeout_ms is not None:
                elapsed_ms = int((time.time() - start_time) * 1000)
                remaining = max(0, timeout_ms - elapsed_ms)

            result, branch = await runner.run_node(
                flow, next_node, max_timeout_ms=remaining, callback_manager=callback_manager,
            )

            if next_node.node_type == End.node_type:
                reached_end = True
                break

            next_node = flow.next_node(next_node, branch)
            logger.debug("next_node: %s with branch: %s", next_node, branch)

            if timeout_ms is not None and (time.time() - start_time) > timeout_ms / 1000:
                raise FlowExecutionException(-1, "Flow execution timeout", FlowErrorType.FLOW_ERROR)

        if not reached_end:
            logger.debug("not reached_end: %s", next_node)
            pfx = context.express_prefix
            result = context.get_state(f"{pfx}{context.express_node_name}", {})

        return result


class GeneratorStrategy:
    """Async generator yielding per-node output for debug/stepping."""

    async def execute(self, flow, context, runner, callback_manager, params=None, timeout_ms=None, **options):
        from plaita.node import End
        next_node = flow.start_node
        if next_node is None:
            raise FlowExecutionException(
                -500, "Flow has no start node", FlowErrorType.NODE_NOT_FOUND,
            )
        reached_end = False

        while next_node:
            result, branch = await runner.run_node(
                flow, next_node, callback_manager=callback_manager,
            )

            yield _create_lazy_output(next_node, result, branch, context.context, execution_id=context.execution_id)

            if next_node.node_type == End.node_type:
                reached_end = True
                break

            next_node = flow.next_node(next_node, branch)
            logger.debug("next_node: %s with branch: %s", next_node, branch)

        if not reached_end:
            logger.debug("not reached_end: %s", next_node)
            pfx = context.express_prefix
            result = context.get_state(f"{pfx}{context.express_node_name}", {})
            yield _create_end_output(next_node, result, context.context, execution_id=context.execution_id)


class DistributedStrategy:
    """Execute one node per call with context persistence for suspend/resume."""

    async def execute(self, flow, context, runner, callback_manager, params=None, timeout_ms=None, **options):
        from plaita.node import End
        from plaita.node.event_node import EventNode

        saved_context = options.get("saved_context")
        resume_type = options.get("resume_type", "continue")
        resume_data = options.get("resume_data")
        pfx = context.express_prefix

        if saved_context:
            context.context = saved_context
        else:
            context.clean()
            context.setup_flow(flow, (), params or {})
            callback_manager.on_flow_start(flow)

        if saved_context and resume_type != "continue":
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

        if start_node.next:
            current_node = flow.find_node_by_id(start_node.next)
        else:
            current_node = flow.next_node(start_node, branch)

        return current_node, result, branch

    async def _execute_current_node(self, flow, context, runner, callback_manager, current_node):
        from plaita.node import End
        from plaita.node.event_node import EventNode

        result, branch = await runner.run_node(flow, current_node, callback_manager=callback_manager)

        if current_node.node_type == End.node_type:
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
        pfx = context.express_prefix

        last_node_id = context.get_state(f"{pfx}LAST_NODE")
        if not last_node_id:
            raise FlowExecutionException(-500, "No suspended node found for resume", FlowErrorType.NODE_ERROR, None)

        current_node = flow.find_node_by_id(last_node_id)
        if not current_node:
            raise FlowExecutionException(-500, f"Cannot find node {last_node_id}", FlowErrorType.NODE_ERROR, None)
        if not isinstance(current_node, EventNode):
            raise FlowExecutionException(-500, f"Node {current_node.id} is not an EventNode", FlowErrorType.NODE_ERROR, current_node)

        node_results = context.get_state(f"{pfx}{context.express_node_name}", {})
        prev_state = node_results.get(last_node_id, {})
        if prev_state.get("status", "") != "pending":
            raise FlowExecutionException(-500, f"EventNode {current_node.id} is not in pending status: {prev_state.get('status', '')}", FlowErrorType.NODE_ERROR, current_node)
        if resume_type not in ("cancel", "timeout", "event"):
            raise FlowExecutionException(-500, f"Unsupported resume type for EventNode: {resume_type}, only support cancel, timeout, event", FlowErrorType.NODE_ERROR, current_node)

        exec_ctx = runner.node_execution or context
        callback_manager.on_flow_resume(flow)
        callback_manager.on_node_resume(flow, current_node)

        try:
            if resume_type == "cancel":
                result = current_node.on_cancel(exec_ctx)
            elif resume_type == "timeout":
                result = current_node.on_timeout(exec_ctx)
            else:
                result = current_node.on_event(exec_ctx, resume_data)
            callback_manager.on_node_end(flow, current_node, result)
        except Exception as e:
            error = {"code": -500, "message": str(e)}
            callback_manager.on_node_end(flow, current_node, None, error, exception=e)
            logger.error("Error during resume: %s", e, exc_info=True)
            raise FlowExecutionException(-500, str(e), FlowErrorType.NODE_ERROR, current_node) from e

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

    Nodes receive this instance as the ``execution`` parameter; context,
    state, and callback attributes are proxied to the underlying
    ExecutionContext and CallbackManager so the public API stays
    backward-compatible while the class itself stays small.
    """

    _REAL_ATTRS = frozenset({
        "mode", "timeout", "parent", "verbose", "_ctx",
        "callback_manager", "_runner", "_strategies", "_strict_attrs",
    })

    def __init__(
        self,
        parent: Optional[FlowExecution] = None,
        verbose: bool = False,
        mode: Optional[str] = None,
        callback_handlers: Optional[List[FlowCallback]] = None,
        callback_manager: Optional[BaseCallbackManager] = None,
        event_bus: Optional[EventBus] = None,
        strict_attrs: bool = False,
    ):
        # strict_attrs=True 时, 未知公共属性写入会抛 AttributeError 而不是
        # 静默落到 context state。给想要 fail-fast、避免拼写错误(如 tiemout)
        # 静默持久化的调用方一个安全开关; 默认 False 保持向后兼容。
        self._strict_attrs = strict_attrs
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

    # -- attribute proxy to ExecutionContext / CallbackManager --

    def __getattr__(self, name: str):
        # Only invoked when normal attribute lookup fails.
        # 不要代理双下方法(如 __getstate__/__setstate__/__reduce_ex__), 否则会把
        # context 的协议方法泄漏到 facade 上, 破坏 pickle 等机制。
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")
        ctx = self.__dict__.get("_ctx")
        if ctx is not None:
            if hasattr(ctx, name):
                return getattr(ctx, name)
            # 读写对称: 已写入 context state 的键可经由属性访问读回
            if name in ctx.context:
                return ctx.context[name]
        if name.startswith("trigger_"):
            cm = self.__dict__.get("callback_manager")
            if cm is not None:
                method = getattr(cm, "on_" + name[len("trigger_"):], None)
                if callable(method):
                    return method
        raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")

    def __setattr__(self, name: str, value: Any) -> None:
        if name in self._REAL_ATTRS or name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        ctx = self.__dict__.get("_ctx")
        if ctx is not None and hasattr(ctx, name):
            # context 上已有的属性(如 express_prefix)写到 context
            setattr(ctx, name, value)
            return
        if self.__dict__.get("_strict_attrs"):
            # fail-fast: 拼写错误(如 self.tiemout = ...)不再静默持久化
            raise AttributeError(
                f"{type(self).__name__!r} has no attribute {name!r}; "
                f"set strict_attrs=False to persist unknown attrs into context state, "
                f"or use set_state() explicitly."
            )
        if ctx is not None:
            # 未知公共属性写入 context state, 避免在 facade 上产生"幻影属性",
            # 并使其可被分布式持久化(to_dict)与读写对称访问。
            ctx.set_state(name, value)
            return
        object.__setattr__(self, name, value)

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
        except Exception as e:
            if isinstance(e, FlowExecutionException):
                raise
            error = {"code": -500, "message": str(e)}
            self.callback_manager.on_flow_end(flow, None, error, exception=e)
            logger.error("flow error", exc_info=True)
            raise FlowExecutionException(-500, str(e), FlowErrorType.FLOW_ERROR) from e
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
        try:
            return _run_async_sync(coro)
        except Exception as e:
            error = {"code": -500, "message": str(e)}
            self.callback_manager.on_flow_end(flow, None, error, exception=e)
            logger.error("flow error", exc_info=True)
            raise FlowExecutionException(-500, str(e), FlowErrorType.FLOW_ERROR) from e

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


def _create_end_output(node, result, context, execution_id=None):
    from plaita.node import End
    return {
        "id": node.id if node else None,
        "type": End.node_type,
        "name": node.name if node else End.node_name,
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
