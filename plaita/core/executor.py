"""plaita.core.executor — FlowExecution driver facade.

0.5.0 起, 执行策略层 (``ExecutionMode`` / ``ExecutionStrategy`` Protocol / 三个
策略 ``NormalStrategy`` / ``GeneratorStrategy`` / ``DistributedStrategy`` / 模块级
helper ``_advance_one`` / ``_create_lazy_output`` / ``_create_end_output`` /
``_subscribe_event``) 已拆到 ``plaita.core.strategies``。本模块只保留
``FlowExecution`` facade——组合 ``ExecutionContext`` / ``NodeRunner`` /
``CallbackManager`` / 策略, 提供公共运行入口 (``run`` / ``arun`` / ``debug`` /
``run_compatible`` / ``arun_compatible`` / ``run_distributed``)。

SC-003 对类体是软预算（见 ``tests/e2e/test_success_criteria.py``）：显式委托
属性允许膨胀，不必为过门再拆 mixin。

历史导入路径 ``from plaita.core.executor import ExecutionMode / NormalStrategy /
GeneratorStrategy / DistributedStrategy / ExecutionStrategy / RunOptions /
_subscribe_event`` 通过下方 re-export 保持不变, 不影响既有调用方与测试。
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from plaita.core._error_normalization import (
    emit_flow_end_on_close as _emit_flow_end_on_close,
    finish_normal as _finish_normal,
    raise_distributed_error as _raise_distributed_error,
)
from plaita.core.async_utils import (
    drive_strategy as _drive_strategy,
    run_async_from_sync as _run_async_sync,
)
from plaita.core.callback import (
    BaseCallbackManager,
    CallbackManager,
    FlowCallback,
    LoggerCallback,
)
from plaita.core.context import ExecutionContext

logger = logging.getLogger(__name__)
from plaita.core.runner import NodeRunner
# 策略层符号 re-export, 保持 ``from plaita.core.executor import ...`` 历史路径。
from plaita.core.strategies import (  # noqa: F401
    DistributedStrategy,
    ExecutionMode,
    ExecutionStrategy,
    GeneratorStrategy,
    NormalStrategy,
    RunOptions,
    _StateView,
    _advance_one,
    _coerce_mode,
    _create_end_output,
    _create_lazy_output,
    _subscribe_event,
)
from plaita.core.errors import FlowExecutionException

if TYPE_CHECKING:
    from plaita.event.core import EventBus


# ``FlowExecution.run`` 认识的调用期 options（express_* 命名空间与分布式 resume 参数）
_KNOWN_RUN_OPTIONS = frozenset({
    "resume_type", "resume_data",
    "express_prefix", "express_input_name", "express_parent_name",
    "express_node_name", "express_global_name", "express_environment_variable",
})


def _reentry_error() -> FlowExecutionException:
    """FlowExecution 非重入守卫的错误构造（消息含修复指引）。"""
    return FlowExecutionException(
        message=(
            "This FlowExecution instance is already running; concurrent runs on one "
            "instance corrupt shared execution state ($INPUT/$NODE/LAST_NODE). Create "
            "a new FlowExecution per concurrent run, or use child executions for sub-flows."
        )
    )


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
        registry=None,
    ):
        self.options = RunOptions(mode=_coerce_mode(mode), timeout=None)
        self.parent = parent
        self.verbose = verbose
        # 可选自定义节点注册表。``None`` 时沿用 ``flow`` 自身已解析的节点
        # (常规路径: ``Flow.model_validate`` 已用默认 registry 解析过)。传入
        # 后会在执行前对含 dict 节点的 flow 兜底解析一次，支持"用一个隔离
        # registry 跑某个 flow"的场景而不依赖进程级单例。
        self._registry = registry
        self._ctx = ExecutionContext(parent=parent._ctx if parent else None, event_bus=event_bus)
        # 非重入守卫: FlowExecution 的 $INPUT/$NODE/LAST_NODE 全是实例级状态,
        # 两个线程同时对同一实例 run 会互相踩状态（静默结果串扰）。子流程走
        # get_child_execution() 的新实例, 不受影响。
        self._running = False

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

    @property
    def state(self) -> _StateView:
        """Typed, ``None``-normalized view over context state.

        ``execution.state.flow_id`` / ``.last_node_id`` / ``.last_branch``.
        Replaces the former ``execution.flow_id`` etc. bare-attribute
        pass-throughs (break change — see MIGRATION.md).
        """
        return _StateView(self._ctx)

    # -- per-run options (RunOptions facade; keeps execution.mode/.timeout API) --

    @property
    def mode(self) -> Optional[ExecutionMode]:
        return self.options.mode

    @mode.setter
    def mode(self, value) -> None:
        # 公共入口仍接受字符串 ("normal"/"generator"/"distributed"), 边界处
        # coerce 成 enum; 内部比较一律走 ExecutionMode, 杜绝拼写错误静默成 False。
        self.options.mode = _coerce_mode(value)

    @property
    def timeout(self) -> Optional[int]:
        return self.options.timeout

    @timeout.setter
    def timeout(self, value: Optional[int]) -> None:
        self.options.timeout = value

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

    # -- sync helpers (used by tests and distributed bootstrap) --

    def _process_node(self, flow, node, lazy=False, max_timeout=None):
        return _run_async_sync(self._runner.run_node(
            flow, node, max_timeout_ms=max_timeout, callback_manager=self.callback_manager))

    def _handle_resume_operation(self, flow, resume_type, resume_data=None):
        return _run_async_sync(self._strategies[ExecutionMode.DISTRIBUTED.value]._handle_resume(
            flow, self._ctx, self._runner, self.callback_manager, resume_type, resume_data))

    def get_child_execution(self):
        # 用 child() 不传额外 handler: 子级通过 inherit_handlers 继承父级各一份,
        # 避免把父级 handler 当"新增 handler"再传一次导致双发。
        child = FlowExecution(self, callback_manager=self.callback_manager.child())
        child.mode = self.mode
        return child

    @staticmethod
    def _parse_timeout(timeout) -> Optional[int]:
        return NodeRunner._parse_timeout(timeout)

    # -- public entry points --

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
        execution.mode = mode  # setter 内部 _coerce_mode 统一成 enum
        # ``timeout=0`` 是合法值（立即超时），不再被 ``or`` 当 falsy 丢弃
        execution.timeout = timeout if timeout is not None else execution.timeout
        # 历史上未知 options 静默忽略——"express_prefx" 这类拼写错误无声失效
        unknown_options = set(options) - _KNOWN_RUN_OPTIONS
        if unknown_options:
            logger.warning("FlowExecution.run: ignoring unknown options %s (valid: %s)",
                           sorted(unknown_options), sorted(_KNOWN_RUN_OPTIONS))
        execution.clean()
        if execution.mode == ExecutionMode.DISTRIBUTED:
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
        return execution.execute(flow, params, lazy=(execution.mode == ExecutionMode.GENERATOR), timeout=timeout)

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

    def _begin_run(self):
        if self._running:
            raise _reentry_error()
        self._running = True

    def run_compatible(self, flow, lazy, *args, **kwargs):
        """Sync execution. Returns the result, or a sync generator when lazy.

        In lazy/generator mode ``on_flow_end`` is deferred until the returned
        generator is actually consumed or closed — historically it fired
        immediately with the unconsumed generator as ``result``, which meant
        the lifecycle end callback ran before any node executed.
        """
        self._begin_run()
        try:
            return _drive_strategy(
                self._prepare_strategy(flow, lazy, args, kwargs),
                lazy=lazy, sync=True,
                finish_coro=lambda coro: _finish_normal(coro, flow, self.callback_manager),
                on_lazy_finally=lambda exc: (
                    _emit_flow_end_on_close(flow, exc, self.callback_manager), setattr(self, "_running", False),
                ),
            )
        finally:
            if not lazy:
                self._running = False

    async def arun_compatible(self, flow, lazy, *args, **kwargs):
        """Async execution — canonical path."""
        self._begin_run()
        try:
            driven = _drive_strategy(
                self._prepare_strategy(flow, lazy, args, kwargs),
                lazy=lazy, sync=False,
                finish_coro=lambda coro: _finish_normal(coro, flow, self.callback_manager),
                on_lazy_finally=lambda exc: (
                    _emit_flow_end_on_close(flow, exc, self.callback_manager), setattr(self, "_running", False),
                ),
            )
            if lazy:
                return driven
            return await driven
        finally:
            if not lazy:
                self._running = False

    def _ensure_flow_resolved(self, flow) -> None:
        """执行前兜底：若 ``flow.nodes`` 仍含 dict 形态节点 (绕过
        ``model_validate`` 直接 ``Flow(...)`` 构造的情况), 用本实例的
        ``registry`` 或默认 registry 解析一次。常规路径下 ``model_validate``
        已解析过, 此处 no-op。"""
        if flow is None:
            return
        nodes = getattr(flow, "nodes", None) or []
        if any(isinstance(n, dict) for n in nodes):
            flow.resolve_nodes(self._registry)

    def _prepare_strategy(self, flow, lazy, args, kwargs):
        """Sync orchestration: clean, flow_start, setup_flow. Returns the
        strategy's awaitable/async-generator for later driving."""
        self._ensure_flow_resolved(flow)
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
        return FlowExecution._merge_timeout_ms(ta, tb)

    @staticmethod
    def _merge_timeout_ms(a_ms, b_ms):
        if a_ms is None:
            return b_ms
        if b_ms is None:
            return a_ms
        return min(a_ms, b_ms)

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
        self._ensure_flow_resolved(flow)
        coro = self._strategies[ExecutionMode.DISTRIBUTED.value].execute(
            flow, self._ctx, self._runner, self.callback_manager, params, timeout,
            saved_context=saved_context, resume_type=resume_type, resume_data=resume_data,
        )
        # 历史上 run_distributed 把任何异常（含具体的 FlowExecutionException 子类）
        # 归一化为 FLOW_ERROR / -500 作为分布式对外契约；此处保留该契约，
        # 具体子类仅用于内部抛点与 normal 模式（_finish_normal 让其透传）。
        try:
            return _run_async_sync(coro)
        except Exception as e:
            _raise_distributed_error(e, flow, self.callback_manager)

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
