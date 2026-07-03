"""Error normalization policies for flow execution.

Three intentionally-different policies live here so their contracts are visible
in one place rather than scattered across ``FlowExecution`` methods:

- :func:`finish_normal` — **normal (eager) mode**. ``FlowExecutionException``
  subclasses pass through unchanged (they fire their own ``on_flow_end`` at the
  raise site); any other exception is wrapped into ``FlowErrorException`` /
  ``-500`` and ``on_flow_end`` is fired here.
- :func:`raise_distributed_error` — **distributed (eager) mode**. *Every*
  exception, including ``FlowExecutionException`` subclasses, is normalized to
  ``FlowErrorException`` / ``-500``. Distributed callers expect a single flat
  error contract; the subclass detail is internal-only.
- :func:`emit_flow_end_on_close` — **lazy generator ``finally``**. Non-
  ``FlowExecutionException`` exceptions get a ``-500`` ``on_flow_end``;
  otherwise a ``result=None`` ``on_flow_end`` is emitted (the subclass already
  fired its own).

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

    ``FlowExecutionException`` subclasses propagate untouched (their raise site
    already owns the lifecycle callback). Anything else becomes a
    ``FlowErrorException`` with ``-500`` and triggers ``on_flow_end`` here.
    """
    try:
        result = await coro
    except FlowExecutionException:
        raise
    except Exception as e:
        error = {"code": -500, "message": str(e)}
        callback_manager.on_flow_end(flow, None, error, exception=e)
        logger.error("flow error", exc_info=True)
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
    logger.error("flow error", exc_info=True)
    raise FlowErrorException(str(e)) from e


def emit_flow_end_on_close(flow, exception, callback_manager):
    """Lazy generator ``finally``: emit the deferred ``on_flow_end``.

    If the generator exited with a non-``FlowExecutionException`` exception,
    normalize it to ``-500`` and fire ``on_flow_end``. Otherwise (clean exit or
    a ``FlowExecutionException`` that already fired its own callback) emit a
    ``result=None`` ``on_flow_end``.
    """
    if exception is not None and not isinstance(exception, FlowExecutionException):
        error = {"code": -500, "message": str(exception)}
        callback_manager.on_flow_end(flow, None, error, exception=exception)
    else:
        callback_manager.on_flow_end(flow, result=None)
