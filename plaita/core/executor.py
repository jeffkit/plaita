"""plaita.core.executor — FlowExecution driver facade.

0.5.0 起, 执行策略层 (``ExecutionMode`` / ``ExecutionStrategy`` Protocol / 三个
策略 ``NormalStrategy`` / ``GeneratorStrategy`` / ``DistributedStrategy`` / 模块级
helper ``_advance_one`` / ``_create_lazy_output`` / ``_create_end_output`` /
``_subscribe_event``) 已拆到 ``plaita.core.strategies``。本模块只保留
``FlowExecution`` facade——组合 ``ExecutionContext`` / ``NodeRunner`` /
``CallbackManager`` / 策略, 提供公共运行入口 (``run`` / ``arun`` / ``debug`` /
``run_compatible`` / ``arun_compatible`` / ``run_distributed``)。

委托属性与入口方法分别在 ``_execution_delegates`` / ``_execution_entry``，
以满足 SC-003（单类 < 200 LOC）。

历史导入路径 ``from plaita.core.executor import ExecutionMode / NormalStrategy /
GeneratorStrategy / DistributedStrategy / ExecutionStrategy / RunOptions /
_subscribe_event`` 通过下方 re-export 保持不变, 不影响既有调用方与测试。
"""
from __future__ import annotations

from typing import List, Optional, TYPE_CHECKING

from plaita.core._execution_delegates import _ExecutionContextDelegates
from plaita.core._execution_entry import _ExecutionEntryPoints
from plaita.core.async_utils import run_async_from_sync as _run_async_sync
from plaita.core.callback import (
    BaseCallbackManager,
    CallbackManager,
    FlowCallback,
    LoggerCallback,
)
from plaita.core.context import ExecutionContext
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

if TYPE_CHECKING:
    from plaita.event.core import EventBus


class FlowExecution(_ExecutionContextDelegates, _ExecutionEntryPoints):
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
