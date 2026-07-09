"""Public run entry points for the FlowExecution facade.

Extracted so ``FlowExecution`` stays under the SC-003 LOC budget.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from plaita.core._error_normalization import (
    emit_flow_end_on_close as _emit_flow_end_on_close,
    finish_normal as _finish_normal,
    raise_distributed_error as _raise_distributed_error,
)
from plaita.core.async_utils import (
    drive_strategy as _drive_strategy,
    run_async_from_sync as _run_async_sync,
)
from plaita.core.callback import FlowCallback
from plaita.core.runner import NodeRunner
from plaita.core.strategies import ExecutionMode


class _ExecutionEntryPoints:
    """``run`` / ``execute`` / ``run_compatible`` / ``run_distributed`` surface."""

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
        execution.timeout = timeout or execution.timeout
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


    def run_compatible(self, flow, lazy, *args, **kwargs):
        """Sync execution. Returns the result, or a sync generator when lazy.

        In lazy/generator mode ``on_flow_end`` is deferred until the returned
        generator is actually consumed or closed — historically it fired
        immediately with the unconsumed generator as ``result``, which meant
        the lifecycle end callback ran before any node executed.
        """
        return _drive_strategy(
            self._prepare_strategy(flow, lazy, args, kwargs),
            lazy=lazy, sync=True,
            finish_coro=lambda coro: _finish_normal(coro, flow, self.callback_manager),
            on_lazy_finally=lambda exc: _emit_flow_end_on_close(flow, exc, self.callback_manager),
        )


    async def arun_compatible(self, flow, lazy, *args, **kwargs):
        """Async execution — canonical path."""
        driven = _drive_strategy(
            self._prepare_strategy(flow, lazy, args, kwargs),
            lazy=lazy, sync=False,
            finish_coro=lambda coro: _finish_normal(coro, flow, self.callback_manager),
            on_lazy_finally=lambda exc: _emit_flow_end_on_close(flow, exc, self.callback_manager),
        )
        if lazy:
            return driven
        return await driven


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
        ms = _ExecutionEntryPoints._merge_timeout_ms(ta, tb)
        return ms


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

