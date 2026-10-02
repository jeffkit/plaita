"""FlowExecution 的兼容执行入口（SC-003 尺寸预算外置）。

``run_compatible`` / ``arun_compatible`` 是 sync/async 兼容层：驱动策略层并
管理 ``_running`` 重入守卫与 lazy generator 的生命周期回调。依赖宿主的
``_begin_run/_prepare_strategy/_running/callback_manager``（mixin 契约）。
"""
from __future__ import annotations

from plaita.core._error_normalization import (
    emit_flow_end_on_close as _emit_flow_end_on_close,
    finish_normal as _finish_normal,
)
from plaita.core.async_utils import drive_strategy as _drive_strategy


class CompatRunMixin:
    """sync/async 兼容执行入口；由 ``FlowExecution`` 混入。"""

    def run_compatible(self, flow, lazy, *args, **kwargs):
        """Sync execution. Returns the result, or a sync generator when lazy.

        In lazy/generator mode ``on_flow_end`` is deferred until the returned
        generator is actually consumed or closed — historically it fired
        immediately with the unconsumed generator as ``result``, which meant
        the lifecycle end callback ran before any node executed.
        """
        self._begin_run()
        try:
            try:
                return _drive_strategy(
                    self._prepare_strategy(flow, lazy, args, kwargs),
                    lazy=lazy, sync=True,
                    finish_coro=lambda coro: _finish_normal(coro, flow, self.callback_manager),
                    on_lazy_finally=lambda exc: (
                        _emit_flow_end_on_close(flow, exc, self.callback_manager), setattr(self, "_running", False),
                    ),
                )
            except BaseException:
                # review-fix B3: lazy 模式下 generator 交出前抛错（典型是
                # _prepare_strategy 校验失败）时 on_lazy_finally 尚未注册，
                # 不复位 _running 会永久毒化实例——下次 run 报 "already
                # running" 伪装真实死因。交出之后由 on_lazy_finally 复位。
                if lazy:
                    self._running = False
                raise
        finally:
            if not lazy:
                self._running = False

    async def arun_compatible(self, flow, lazy, *args, **kwargs):
        """Async execution — canonical path."""
        self._begin_run()
        try:
            try:
                driven = _drive_strategy(
                    self._prepare_strategy(flow, lazy, args, kwargs),
                    lazy=lazy, sync=False,
                    finish_coro=lambda coro: _finish_normal(coro, flow, self.callback_manager),
                    on_lazy_finally=lambda exc: (
                        _emit_flow_end_on_close(flow, exc, self.callback_manager), setattr(self, "_running", False),
                    ),
                )
            except BaseException:
                # review-fix B3: 同 run_compatible——lazy 交出 generator 前
                # 抛错必须复位 _running，否则实例永久毒化。
                if lazy:
                    self._running = False
                raise
            if lazy:
                return driven
            return await driven
        finally:
            if not lazy:
                self._running = False

