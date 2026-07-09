"""Tests for plaita.core.async_utils — sync/async bridge utilities.

Coverage target: plaita/core/async_utils.py (53% → target 85%+)

Covers:
- run_async_from_sync: no-loop path (asyncio.run) and loop-running path (thread)
- async_gen_to_sync: no-loop path (single event loop) and loop-running path (thread+queue)
- drive_strategy: all 4 combinations of sync/async × lazy/eager
- _drive_lazy_async: async lazy generator path
"""

from __future__ import annotations

import asyncio
import unittest

from plaita.core.async_utils import (
    async_gen_to_sync,
    drive_strategy,
    run_async_from_sync,
)


# ---------------------------------------------------------------------------
# run_async_from_sync
# ---------------------------------------------------------------------------

class TestRunAsyncFromSync(unittest.TestCase):
    def test_no_loop_simple_coroutine(self):
        async def add(a, b):
            return a + b

        result = run_async_from_sync(add(3, 4))
        self.assertEqual(result, 7)

    def test_no_loop_with_async_sleep(self):
        async def coro():
            await asyncio.sleep(0)
            return "done"

        result = run_async_from_sync(coro())
        self.assertEqual(result, "done")

    def test_inside_running_loop_uses_thread(self):
        """When called from inside a running loop, offloads to a thread."""
        async def inner():
            async def coro():
                return 42

            return run_async_from_sync(coro())

        result = asyncio.run(inner())
        self.assertEqual(result, 42)

    def test_exception_propagates(self):
        async def failing():
            raise ValueError("oops")

        with self.assertRaises(ValueError, msg="oops"):
            run_async_from_sync(failing())


# ---------------------------------------------------------------------------
# async_gen_to_sync
# ---------------------------------------------------------------------------

class TestAsyncGenToSync(unittest.TestCase):
    def test_yields_all_items(self):
        async def gen():
            for i in range(5):
                yield i

        result = list(async_gen_to_sync(gen()))
        self.assertEqual(result, [0, 1, 2, 3, 4])

    def test_empty_generator(self):
        async def gen():
            return
            yield  # make it an async generator

        result = list(async_gen_to_sync(gen()))
        self.assertEqual(result, [])

    def test_exception_propagates(self):
        async def gen():
            yield 1
            raise RuntimeError("gen error")
            yield 2  # noqa: unreachable

        with self.assertRaises(RuntimeError, msg="gen error"):
            list(async_gen_to_sync(gen()))

    def test_inside_running_loop_uses_thread_and_queue(self):
        """When called from inside a running event loop, uses thread+queue path."""
        async def gen():
            for i in range(3):
                yield i * 10

        async def wrapper():
            # Calling async_gen_to_sync from inside asyncio.run() — loop IS running.
            return list(async_gen_to_sync(gen()))

        result = asyncio.run(wrapper())
        self.assertEqual(result, [0, 10, 20])

    def test_inside_running_loop_exception_propagates(self):
        """Exception in thread+queue path propagates to caller."""
        async def gen():
            yield 1
            raise RuntimeError("thread gen error")
            yield 2  # noqa

        async def wrapper():
            return list(async_gen_to_sync(gen()))

        with self.assertRaises(RuntimeError):
            asyncio.run(wrapper())

    def test_aclose_error_suppressed(self):
        """If agen.aclose() raises during early generator close, it's suppressed.

        This covers lines 137-138 (except Exception branch in the finally block).
        """
        async def gen_with_broken_close():
            try:
                yield 1
                yield 2
            finally:
                raise RuntimeError("cleanup fail")

        g = async_gen_to_sync(gen_with_broken_close())
        first = next(g)
        self.assertEqual(first, 1)
        # Explicitly close the generator — triggers finally → agen.aclose() → error
        # The error is caught by the except clause and NOT propagated.
        g.close()  # Should not raise even though aclose() internally raises

    def test_aclose_error_suppressed_in_worker_thread(self):
        """Worker-thread path suppresses agen.aclose() failures (lines 162-163)."""

        class _Agen:
            def __init__(self):
                self._done = False

            def __anext__(self):
                async def _one():
                    if self._done:
                        raise StopAsyncIteration
                    self._done = True
                    return 1

                return _one()

            def aclose(self):
                async def _boom():
                    raise RuntimeError("worker aclose fail")

                return _boom()

        async def wrapper():
            return list(async_gen_to_sync(_Agen()))

        result = asyncio.run(wrapper())
        self.assertEqual(result, [1])

    def test_worker_join_timeout_logs_warning(self):
        """If worker is still alive after join(timeout=5), log a warning (line 179)."""
        import logging
        import threading
        from unittest.mock import patch

        async def slow_gen():
            yield 1
            await asyncio.sleep(60)
            yield 2

        real_thread = threading.Thread

        class StickyThread(real_thread):
            def join(self, timeout=None):  # noqa: A003
                return None

            def is_alive(self):
                return True

        async def wrapper():
            with patch("plaita.core.async_utils.threading.Thread", StickyThread):
                with self.assertLogs("plaita.core.async_utils", level=logging.WARNING) as cm:
                    g = async_gen_to_sync(slow_gen())
                    next(g)
                    g.close()
                self.assertTrue(
                    any("worker still alive" in r.getMessage() for r in cm.records),
                    cm.output,
                )

        asyncio.run(wrapper())


# ---------------------------------------------------------------------------
# drive_strategy
# ---------------------------------------------------------------------------

class TestDriveStrategy(unittest.TestCase):
    def _make_eager_coro(self, value):
        async def coro():
            return value
        return coro()

    def _make_lazy_agen(self, items):
        async def gen():
            for item in items:
                yield item
        return gen()

    def _identity_finish(self, coro):
        return coro

    def _noop_on_finally(self, exc):
        pass

    def test_sync_eager(self):
        """sync=True, lazy=False → returns the driven result directly."""
        result = drive_strategy(
            self._make_eager_coro(99),
            lazy=False,
            sync=True,
            finish_coro=self._identity_finish,
            on_lazy_finally=self._noop_on_finally,
        )
        self.assertEqual(result, 99)

    def test_async_eager(self):
        """sync=False, lazy=False → returns a coroutine to be awaited."""
        coro = drive_strategy(
            self._make_eager_coro(77),
            lazy=False,
            sync=False,
            finish_coro=self._identity_finish,
            on_lazy_finally=self._noop_on_finally,
        )
        result = asyncio.run(coro)
        self.assertEqual(result, 77)

    def test_sync_lazy(self):
        """sync=True, lazy=True → returns a sync generator."""
        gen = drive_strategy(
            self._make_lazy_agen([1, 2, 3]),
            lazy=True,
            sync=True,
            finish_coro=self._identity_finish,
            on_lazy_finally=self._noop_on_finally,
        )
        result = list(gen)
        self.assertEqual(result, [1, 2, 3])

    def test_sync_lazy_exception_propagates(self):
        """Exception in lazy sync generator propagates and on_finally is called."""
        async def failing_gen():
            yield 1
            raise RuntimeError("fail in gen")
            yield 2  # noqa

        finally_called = []

        def on_finally(exc):
            finally_called.append(exc)

        gen = drive_strategy(
            failing_gen(),
            lazy=True,
            sync=True,
            finish_coro=self._identity_finish,
            on_lazy_finally=on_finally,
        )
        with self.assertRaises(RuntimeError):
            list(gen)
        self.assertEqual(len(finally_called), 1)
        self.assertIsInstance(finally_called[0], RuntimeError)

    def test_sync_lazy_on_finally_called_on_success(self):
        """on_lazy_finally is called with None when iteration completes normally."""
        finally_called = []

        def on_finally(exc):
            finally_called.append(exc)

        gen = drive_strategy(
            self._make_lazy_agen([1, 2]),
            lazy=True,
            sync=True,
            finish_coro=self._identity_finish,
            on_lazy_finally=on_finally,
        )
        list(gen)
        self.assertEqual(finally_called, [None])

    def test_async_lazy(self):
        """sync=False, lazy=True → returns an async generator."""
        async def run():
            agen = drive_strategy(
                self._make_lazy_agen([10, 20, 30]),
                lazy=True,
                sync=False,
                finish_coro=self._identity_finish,
                on_lazy_finally=self._noop_on_finally,
            )
            result = []
            async for item in agen:
                result.append(item)
            return result

        result = asyncio.run(run())
        self.assertEqual(result, [10, 20, 30])

    def test_async_lazy_on_finally_called_on_success(self):
        """on_lazy_finally called with None after successful async lazy iteration."""
        finally_called = []

        def on_finally(exc):
            finally_called.append(exc)

        async def run():
            agen = drive_strategy(
                self._make_lazy_agen([1]),
                lazy=True,
                sync=False,
                finish_coro=self._identity_finish,
                on_lazy_finally=on_finally,
            )
            async for _ in agen:
                pass

        asyncio.run(run())
        self.assertEqual(finally_called, [None])

    def test_async_lazy_on_finally_called_on_exception(self):
        """on_lazy_finally called with exception after async lazy iteration error."""
        finally_called = []

        def on_finally(exc):
            finally_called.append(exc)

        async def failing_gen():
            yield 1
            raise ValueError("agen fail")
            yield 2  # noqa

        async def run():
            agen = drive_strategy(
                failing_gen(),
                lazy=True,
                sync=False,
                finish_coro=self._identity_finish,
                on_lazy_finally=on_finally,
            )
            try:
                async for _ in agen:
                    pass
            except ValueError:
                pass

        asyncio.run(run())
        self.assertEqual(len(finally_called), 1)
        self.assertIsInstance(finally_called[0], ValueError)


# ---------------------------------------------------------------------------
# Mutation-killing tests — precise assertions to cover survived mutants
# ---------------------------------------------------------------------------

class TestRunAsyncFromSyncMutationKillers(unittest.TestCase):
    """Targeted tests for survived mutmut mutations in run_async_from_sync."""

    def test_uses_exactly_one_worker_when_loop_running(self):
        """ThreadPoolExecutor must use max_workers=1 (not None or 2)."""
        import concurrent.futures
        from unittest.mock import patch

        pool_kwargs: list = []
        orig = concurrent.futures.ThreadPoolExecutor

        class TrackTPE(orig):  # type: ignore[misc]
            def __init__(self, **kw):
                pool_kwargs.append(kw)
                super().__init__(**kw)

        async def inner():
            async def coro():
                return 99

            import concurrent.futures as _cf
            with patch.object(_cf, "ThreadPoolExecutor", TrackTPE):
                return run_async_from_sync(coro())

        result = asyncio.run(inner())
        self.assertEqual(result, 99)
        self.assertEqual(len(pool_kwargs), 1)
        self.assertEqual(pool_kwargs[0].get("max_workers"), 1)


class TestAsyncGenToSyncMutationKillers(unittest.TestCase):
    """Targeted tests for survived mutmut mutations in async_gen_to_sync."""

    # --- no-loop path: gen runs in the calling thread ---

    def test_no_loop_gen_runs_in_calling_thread(self):
        """In no-loop path, the async gen coroutine runs in the calling thread (not a worker)."""
        import threading

        calling_thread = threading.current_thread()
        threads_seen: list = []

        async def gen():
            threads_seen.append(threading.current_thread())
            yield 1

        result = list(async_gen_to_sync(gen()))
        self.assertEqual(result, [1])
        self.assertIs(threads_seen[0], calling_thread,
                      "no-loop path must run gen in calling thread, not a worker thread")

    # --- no-loop path: aclose() is explicitly called in finally ---

    def test_no_loop_aclose_called_explicitly(self):
        """async_gen_to_sync (no-loop path) must call agen.aclose() explicitly.

        Uses a wrapper to detect the call directly so GC-based auto-close
        cannot mask a missing explicit aclose() call.
        """
        aclose_called: list = []

        class _TrackClose:
            """Thin proxy that records whether aclose() was explicitly invoked."""

            def __init__(self, inner):
                self._inner = inner

            def __aiter__(self):
                return self

            async def __anext__(self):
                return await self._inner.__anext__()

            async def aclose(self):
                aclose_called.append(True)
                await self._inner.aclose()

        async def gen():
            yield 1
            yield 2

        wrapped = _TrackClose(gen())
        g = async_gen_to_sync(wrapped)
        _ = next(g)   # suspended after item 1
        g.close()     # finally → loop.run_until_complete(wrapped.aclose())

        self.assertEqual(aclose_called, [True],
                         "loop.run_until_complete(agen.aclose()) must be called explicitly")

    # --- no-loop path: logger.debug message and exc_info on aclose failure ---

    def test_no_loop_aclose_failure_logs_correct_message(self):
        """When aclose() fails in no-loop path, correct message and exc_info=True are logged."""
        from unittest.mock import patch

        async def gen_broken_close():
            try:
                yield 1
            finally:
                raise RuntimeError("aclose fail")

        with patch("plaita.core.async_utils.logger") as mock_logger:
            g = async_gen_to_sync(gen_broken_close())
            _ = next(g)
            g.close()

            mock_logger.debug.assert_called_once_with(
                "async gen aclose failed during sync drain",
                exc_info=True,
            )

    # --- thread path: yields None items correctly (_DONE must be object(), not None) ---

    def test_thread_path_yields_none_items(self):
        """_DONE sentinel must be object() so None items are NOT misidentified as done."""

        async def gen():
            yield None
            yield None

        async def wrapper():
            return list(async_gen_to_sync(gen()))

        result = asyncio.run(wrapper())
        self.assertEqual(result, [None, None],
                         "_DONE = None would eat None items; must be object()")

    # --- thread path: worker thread is daemon ---

    def test_thread_path_worker_is_daemon(self):
        """Worker thread spawned in has-loop path must be daemon=True."""
        import threading

        worker_daemon: list = []

        async def gen():
            worker_daemon.append(threading.current_thread().daemon)
            yield 42

        async def wrapper():
            return list(async_gen_to_sync(gen()))

        result = asyncio.run(wrapper())
        self.assertEqual(result, [42])
        self.assertEqual(len(worker_daemon), 1)
        self.assertTrue(worker_daemon[0], "worker thread must be daemon=True")

    # --- thread path: aclose() explicitly called in _drive's finally ---

    def test_thread_path_aclose_called_explicitly(self):
        """_drive's finally must call agen.aclose() explicitly (not just rely on GC)."""
        import threading

        aclose_called = threading.Event()

        class _TrackClose:
            """Thin proxy that records whether aclose() was explicitly invoked."""

            def __init__(self, inner):
                self._inner = inner

            def __aiter__(self):
                return self

            async def __anext__(self):
                return await self._inner.__anext__()

            async def aclose(self):
                aclose_called.set()
                await self._inner.aclose()

        async def gen():
            yield 1
            yield 2

        async def wrapper():
            wrapped = _TrackClose(gen())
            g = async_gen_to_sync(wrapped)
            first = next(g)     # suspended after item 1
            g.close()           # consumer close → worker.join → _drive finally → aclose
            return first, aclose_called.wait(timeout=2)

        first, called = asyncio.run(wrapper())
        self.assertEqual(first, 1)
        self.assertTrue(called, "agen.aclose() must be explicitly called in _drive finally")

    # --- thread path: worker.join uses timeout=5 ---

    def test_thread_path_worker_join_uses_timeout_5(self):
        """worker.join must be called with timeout=5 (not None or other value)."""
        import threading
        from unittest.mock import patch

        join_timeouts: list = []
        orig_join = threading.Thread.join

        def capture_join(self_t, timeout=None):
            join_timeouts.append(timeout)
            return orig_join(self_t, timeout=timeout)

        async def gen():
            yield 1

        async def wrapper():
            with patch.object(threading.Thread, "join", capture_join):
                return list(async_gen_to_sync(gen()))

        result = asyncio.run(wrapper())
        self.assertEqual(result, [1])
        self.assertIn(5, join_timeouts, "worker.join must use timeout=5")


if __name__ == "__main__":
    unittest.main()
