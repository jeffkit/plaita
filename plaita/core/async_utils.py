"""
plaita.core.async_utils — sync/async bridging utilities.

These helpers are deliberately placed in the **core** layer so that both
``plaita.core`` and ``plaita.event`` can depend on them without creating a
reverse dependency (core → event).  Previously the bridge lived in
``plaita.event.core``, which forced ``plaita.core.executor`` to import upward
into the event layer — violating the documented layering
(``core → event → storage → server``).

Public helpers:
- ``run_async_from_sync(coro)``: drive a coroutine to completion from
  synchronous code. Uses ``asyncio.run`` when no loop is running, otherwise
  offloads to a worker thread with a fresh loop.
- ``async_gen_to_sync(agen)``: yield items from an async generator using a
  single persistent loop (or a worker thread + queue when a loop is already
  running in the calling thread).
"""

from __future__ import annotations

import asyncio
import logging
import threading

logger = logging.getLogger("plaita.core.async_utils")


def run_async_from_sync(coro):
    """Run an async coroutine from a synchronous context.

    If no event loop is running, uses ``asyncio.run``.
    If a loop is already running (e.g. inside an async framework),
    offloads to a thread via a short-lived ``ThreadPoolExecutor`` so we
    do not nest ``asyncio.run`` inside a running loop.
    """
    import concurrent.futures

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # No timeout here: the coroutine itself carries node/flow-level timeouts.
        # A hard-coded wall-clock cap here would silently override all user-configured
        # timeouts and cause mysterious failures for any flow that runs longer than
        # the arbitrary constant.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()
    return asyncio.run(coro)


def async_gen_to_sync(agen):
    """Drive an async generator from sync code, yielding each item.

    A single persistent event loop is used so the async generator's state
    stays valid across yields. When a loop is already running in this
    thread, the generator is driven inside a dedicated worker thread and
    items are forwarded through a queue.
    """
    try:
        asyncio.get_running_loop()
        has_loop = True
    except RuntimeError:
        has_loop = False

    if not has_loop:
        loop = asyncio.new_event_loop()
        try:
            while True:
                try:
                    item = loop.run_until_complete(agen.__anext__())
                except StopAsyncIteration:
                    break
                yield item
        finally:
            try:
                loop.run_until_complete(agen.aclose())
            except Exception:
                pass
            loop.close()
    else:
        import queue as _queue
        _DONE = object()
        q: "_queue.Queue" = _queue.Queue()

        def _drive():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                while True:
                    try:
                        item = loop.run_until_complete(agen.__anext__())
                    except StopAsyncIteration:
                        q.put(_DONE)
                        break
                    except BaseException as e:  # noqa: BLE001
                        q.put(e)
                        break
                    q.put(item)
            finally:
                try:
                    loop.run_until_complete(agen.aclose())
                except Exception:
                    pass
                loop.close()

        worker = threading.Thread(target=_drive, daemon=True)
        worker.start()
        try:
            while True:
                item = q.get()
                if item is _DONE:
                    break
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            worker.join(timeout=5)
            if worker.is_alive():
                logger.warning(
                    "async_gen_to_sync worker still alive after join; "
                    "underlying async generator may not have closed cleanly"
                )
