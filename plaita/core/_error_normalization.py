"""Error normalization policies for flow execution.

Three intentionally-different policies live here so their contracts are visible
in one place rather than scattered across ``FlowExecution`` methods:

- :func:`finish_normal` — **normal (eager) mode**. ``FlowExecutionException``
  subclasses propagate unchanged but fire ``on_flow_end`` with their own
  ``code``/``message`` here (C1-4: no raise site ever fired it); any other
  exception is wrapped into ``FlowErrorException`` / ``-500`` and
  ``on_flow_end`` is fired here.
- :func:`raise_distributed_error` — **distributed (eager) mode**. *Every*
  exception, including ``FlowExecutionException`` subclasses, is normalized to
  ``FlowErrorException`` / ``-500``. Distributed callers expect a single flat
  error contract; the subclass detail is internal-only.
- :func:`emit_flow_end_on_close` — **lazy generator ``finally``**. Non-
  ``FlowExecutionException`` exceptions get a ``-500`` ``on_flow_end``;
  ``FlowExecutionException`` gets one carrying its own ``code``/``message``;
  a clean exit emits the historical ``result=None`` ``on_flow_end``.

Do **not** collapse these into one helper — the normal-vs-distributed
difference is a documented external contract (see ``FlowExecution.run_distributed``
docstring).
"""
from __future__ import annotations

import logging

from plaita.core.errors import FlowErrorException, FlowExecutionException

logger = logging.getLogger("plaita.core.executor")


async def finish_normal(coro, flow, callback_manager):
    """Await ``coro`` for normal mode; fire ``on_flow_end``; normalize errors.

    ``FlowExecutionException`` subclasses propagate untouched after firing
    ``on_flow_end`` with the exception's own ``code``/``message`` (C1-4,
    2026-10 评审修复包 C1：历史上直接透传不回调——注释声称 raise site 已发，
    但全仓 ``on_flow_end`` 只在本模块出现，raise site 从未发过，eager 路径
    FEE 时生命周期回调整个缺失). Anything else becomes a
    ``FlowErrorException`` / ``-500`` and triggers ``on_flow_end`` here.
    """
    try:
        result = await coro
    except FlowExecutionException as e:
        error = {"code": getattr(e, "code", -500) or -500, "message": str(e)}
        callback_manager.on_flow_end(flow, None, error, exception=e)
        raise
    except Exception as e:
        error = {"code": -500, "message": str(e)}
        callback_manager.on_flow_end(flow, None, error, exception=e)
        # raise 已携带完整 traceback，这里不再重复打栈
        logger.error("flow error: %s", e)
        raise FlowErrorException(str(e)) from e
    callback_manager.on_flow_end(flow, result=result)
    return result


def raise_distributed_error(e, flow, callback_manager):
    """Distributed-mode normalization: *all* exceptions → ``FlowErrorException`` / ``-500``.

    Unlike :func:`finish_normal`, ``FlowExecutionException`` subclasses are also
    flattened — the distributed external contract is a single error shape, the
    subclass detail is internal-only.
    """
    error = {"code": -500, "message": str(e)}
    callback_manager.on_flow_end(flow, None, error, exception=e)
    logger.error("flow error: %s", e)
    raise FlowErrorException(str(e)) from e


def emit_flow_end_on_close(flow, exception, callback_manager):
    """Lazy generator ``finally``: emit the deferred ``on_flow_end``.

    If the generator exited with a non-``FlowExecutionException`` exception,
    normalize it to ``-500`` and fire ``on_flow_end``. A
    ``FlowExecutionException`` fires ``on_flow_end`` with its own
    ``code``/``message`` in ``error`` (C1-4，2026-10 评审修复包 C1：历史上
    lazy 分支把 FEE 抹成 ``result=None, error=None``，宿主从回调无法区分
    失败与正常结束). A clean exit keeps the historical ``result=None``
    callback (the end step itself carries the flow result).
    """
    if exception is not None and not isinstance(exception, FlowExecutionException):
        error = {"code": -500, "message": str(exception)}
        callback_manager.on_flow_end(flow, None, error, exception=exception)
    elif exception is not None:
        error = {"code": getattr(exception, "code", -500) or -500, "message": str(exception)}
        callback_manager.on_flow_end(flow, None, error, exception=exception)
    else:
        callback_manager.on_flow_end(flow, result=None)
