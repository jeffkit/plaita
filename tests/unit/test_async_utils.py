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


if __name__ == "__main__":
    unittest.main()
