"""``plaita.core.parallel_executor`` 行为钉死 (任务 #3).

抽 ``ParallelExecutor`` 协议后, 三种执行模式 (Map.concurrent / Parallel.pool_execute
/ 集合节点非并发) 走同一套执行器接口。本测试钉死协议本身的契约:

- ``map`` 结果顺序与输入一致
- ``max_workers`` 用 semaphore gate 并发 (而非新开池), 上限生效
- ``ThreadParallelExecutor.supports_cancel_propagation`` 为 True,
  ``ProcessParallelExecutor`` 为 False (cancel_event 跨进程丢失, 文档化在协议上)
- ``SequentialExecutor`` 零并发, 用于非并发集合节点
- ``make_executor`` 对未知 mode 报错 (coroutine 已下线)
"""

from __future__ import annotations

import threading
import time
import unittest
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from plaita.core.parallel_executor import (
    PROCESS,
    THREAD,
    ProcessParallelExecutor,
    SequentialExecutor,
    ThreadParallelExecutor,
    make_executor,
)


class TestThreadParallelExecutor(unittest.TestCase):
    def test_map_preserves_order(self):
        ex = ThreadParallelExecutor()
        items = list(range(20))
        result = ex.map(lambda x: x * x, items)
        self.assertEqual(result, [x * x for x in items])

    def test_map_empty_items(self):
        ex = ThreadParallelExecutor()
        self.assertEqual(ex.map(lambda x: x, []), [])

    def test_map_propagates_exceptions(self):
        ex = ThreadParallelExecutor()

        def fn(x):
            if x == 3:
                raise ValueError("boom")
            return x

        with self.assertRaises(ValueError):
            ex.map(fn, [1, 2, 3, 4])

    def test_pool_defaults_to_background_singleton(self):
        from plaita.core.parallel_executor import BackGroundThreadPool

        self.assertIs(ThreadParallelExecutor()._pool, BackGroundThreadPool)
        custom = ThreadPoolExecutor()
        try:
            self.assertIs(ThreadParallelExecutor(pool=custom)._pool, custom)
        finally:
            custom.shutdown(wait=False)

    def test_max_workers_forwarded(self):
        # max_workers 必须透传到 _max_workers (make_executor / __init__ 都不能丢)
        self.assertEqual(ThreadParallelExecutor(max_workers=4)._max_workers, 4)
        self.assertIsNone(ThreadParallelExecutor()._max_workers)

    def test_max_workers_gates_concurrency(self):
        # 用一个 barrier 探测同时 in-flight 的任务数: 超过 max_workers 时阻塞,
        # 让我们观察到并发上限。
        max_workers = 3
        ex = ThreadParallelExecutor(max_workers=max_workers)
        in_flight = 0
        peak = 0
        state_lock = threading.Lock()

        def observe(x):
            nonlocal in_flight, peak
            with state_lock:
                in_flight += 1
                peak = max(peak, in_flight)
            time.sleep(0.05)
            with state_lock:
                in_flight -= 1
            return x

        ex.map(observe, list(range(12)))
        # peak 不应超过 max_workers (semaphore gate 在共享池上限制并发)。
        self.assertLessEqual(peak, max_workers)
        # 同时 max_workers 应该真的生效 (大于 1 才算并发), 否则 gate 失效。
        self.assertGreater(peak, 1)

    def test_supports_cancel_propagation_true(self):
        self.assertTrue(ThreadParallelExecutor().supports_cancel_propagation)


class TestProcessParallelExecutor(unittest.TestCase):
    def test_supports_cancel_propagation_false(self):
        # cancel_event 不可 pickle, 子进程拿到全新未触发的 Event —— 协议上显式声明。
        self.assertFalse(ProcessParallelExecutor().supports_cancel_propagation)

    def test_lock_is_process_lock(self):
        ex = ProcessParallelExecutor()
        # ``multiprocessing.Lock`` 是工厂函数, 实例类型在 synchronize 模块。
        self.assertEqual(type(ex.lock).__module__, "multiprocessing.synchronize")
        self.assertIn("Lock", type(ex.lock).__name__)

    def test_pool_defaults_to_background_singleton(self):
        # pool 默认复用单例; 显式传入时用传入的 (杀掉 ``pool or BackGroundPool``
        # 被改成 None / and 的变异)。
        from plaita.core.parallel_executor import BackGroundProcessPool

        self.assertIs(ProcessParallelExecutor()._pool, BackGroundProcessPool)
        custom = ProcessPoolExecutor()
        try:
            self.assertIs(ProcessParallelExecutor(pool=custom)._pool, custom)
        finally:
            custom.shutdown(wait=False)

    def test_max_workers_forwarded(self):
        self.assertEqual(ProcessParallelExecutor(max_workers=4)._max_workers, 4)
        self.assertIsNone(ProcessParallelExecutor()._max_workers)


class TestSequentialExecutor(unittest.TestCase):
    def test_map_is_in_order_and_serial(self):
        ex = SequentialExecutor()
        result = ex.map(lambda x: x + 1, [10, 20, 30])
        self.assertEqual(result, [11, 21, 31])

    def test_supports_cancel_propagation_true(self):
        self.assertTrue(SequentialExecutor().supports_cancel_propagation)

    def test_submit_not_supported(self):
        # 不支持 submit: 抛 NotImplementedError, 且文案精确钉死 (杀掉把文案改成
        # None / 加 XX 包装的等价变异)。
        with self.assertRaises(NotImplementedError) as ctx:
            SequentialExecutor().submit(lambda: None)
        self.assertEqual(str(ctx.exception), "SequentialExecutor 不支持 submit/background 语义")

    def test_wait_not_supported(self):
        with self.assertRaises(NotImplementedError) as ctx:
            SequentialExecutor.wait([])
        self.assertEqual(str(ctx.exception), "SequentialExecutor 不支持 wait 语义")

    def test_lock_not_supported(self):
        with self.assertRaises(NotImplementedError) as ctx:
            _ = SequentialExecutor().lock
        self.assertEqual(str(ctx.exception), "SequentialExecutor 不支持 lock 语义")


class TestMakeExecutor(unittest.TestCase):
    def test_thread(self):
        self.assertIsInstance(make_executor(THREAD), ThreadParallelExecutor)

    def test_process(self):
        self.assertIsInstance(make_executor(PROCESS), ProcessParallelExecutor)

    def test_max_workers_forwarded(self):
        # max_workers 必须透传到具体执行器 (杀掉 max_workers=None 替换变异)。
        self.assertEqual(make_executor(THREAD, max_workers=3)._max_workers, 3)
        self.assertEqual(make_executor(PROCESS, max_workers=3)._max_workers, 3)

    def test_unknown_mode_raises(self):
        # coroutine 模式已下线, make_executor 不接。文案点名 coroutine / Unknown,
        # 杀掉把 ValueError 参数改成 None 的等价变异。
        with self.assertRaises(ValueError) as ctx:
            make_executor("coroutine")
        msg = str(ctx.exception)
        self.assertTrue("coroutine" in msg or "Unknown" in msg, msg)


if __name__ == "__main__":
    unittest.main()
