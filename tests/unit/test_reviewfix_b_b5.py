"""review-fix B5 回归：后台并行池按 PID 惰性单例（fork 防护）。

历史行为：``BackGroundThreadPool`` / ``BackGroundProcessPool`` 是 import 期
创建的模块级单例。「import 后 fork」部署（gunicorn/FastAPI preload 等）里，
子进程继承的池没有 worker 线程/manager 线程——首个 Map/Parallel 提交永久
挂起（与 ``runner._get_sync_node_pool`` 同根因，cf0830f 实测 CI Linux 30min
hang）。review-fix B5 把该 PID 检测模式抽成 ``get_background_pool`` 套到两池：
同 PID 内仍是单例，fork 后首次获取即重建。
"""
from __future__ import annotations

import os
import unittest

from plaita.core import parallel_executor as pe
from plaita.core.parallel_executor import (
    ProcessParallelExecutor,
    ThreadParallelExecutor,
    get_background_pool,
)


class _SentinelPool:
    """不可用的假池对象——若被提交即失败，用于证明缓存被重建。"""


class TestBackgroundPoolPidGuard(unittest.TestCase):
    def tearDown(self):
        # 清掉测试注入的缓存条目，恢复真实单例
        pe._POOL_CACHE.pop("thread", None)
        pe._POOL_CACHE.pop("process", None)

    def test_stale_pid_entry_triggers_rebuild(self):
        """模拟 fork：缓存条目 pid 是"父进程"的 → 首次获取必须重建。"""
        sentinel = _SentinelPool()
        pe._POOL_CACHE["thread"] = (os.getpid() + 999999, sentinel)

        pool = get_background_pool("thread")
        self.assertIsNot(pool, sentinel, "过期 PID 的缓存条目必须被重建")
        self.assertIs(pe._POOL_CACHE["thread"][1], pool)
        self.assertEqual(pe._POOL_CACHE["thread"][0], os.getpid())

    def test_rebuilt_pool_is_usable(self):
        """重建出的 thread 池在当前进程真实可用（fork 子进程同路径）。"""
        pe._POOL_CACHE["thread"] = (os.getpid() + 999999, _SentinelPool())
        pool = get_background_pool("thread")
        self.assertEqual(pool.submit(lambda: 41 + 1).result(timeout=10), 42)

    def test_same_pid_returns_singleton(self):
        """同 PID 内保持单例语义（守卫不得每次新建）。"""
        a = get_background_pool("thread")
        b = get_background_pool("thread")
        self.assertIs(a, b)

        pa = get_background_pool("process")
        pb = get_background_pool("process")
        self.assertIs(pa, pb)

    def test_legacy_names_resolve_to_current_pid_pool(self):
        """历史名字 BackGroundThreadPool/BackGroundProcessPool 仍可用，
        且解析为当前 PID 的池（模块 __getattr__ 路径）。"""
        self.assertIs(pe.BackGroundThreadPool, get_background_pool("thread"))
        self.assertIs(pe.BackGroundProcessPool, get_background_pool("process"))

    def test_executor_defaults_use_pid_guarded_pool(self):
        """两个执行器的默认池经 getter 获取（fork 后拿重建池）。"""
        self.assertIs(ThreadParallelExecutor()._pool, get_background_pool("thread"))
        self.assertIs(ProcessParallelExecutor()._pool, get_background_pool("process"))

    def test_unknown_kind_rejected(self):
        with self.assertRaises(ValueError):
            get_background_pool("coroutine")

    @unittest.skipIf(not hasattr(os, "fork"), "platform without os.fork")
    def test_forked_child_rebuilds_and_uses_fresh_pool(self):
        """真 fork：子进程首次获取触发重建，且新池在子进程内可用。"""
        parent_pool = get_background_pool("thread")

        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:  # 子进程：只写管道后 _exit，绝不碰 pytest
            try:
                first = get_background_pool("thread")
                rebuilt = (
                    pe._POOL_CACHE["thread"][0] == os.getpid()
                    and first is not parent_pool
                )
                res = first.submit(lambda: 6 * 7).result(timeout=10)
                os.write(w, f"REBUILT={rebuilt} RES={res}".encode())
            except BaseException as e:  # noqa: BLE001
                os.write(w, f"ERR:{type(e).__name__}:{e}".encode()[:512])
            finally:
                os._exit(0)

        os.close(w)
        try:
            data = os.read(r, 4096).decode(errors="replace")
        finally:
            os.close(r)
            os.waitpid(pid, 0)

        self.assertNotIn("ERR:", data, f"子进程内重建池不可用: {data}")
        self.assertIn("REBUILT=True", data, f"子进程未触发重建: {data}")
        self.assertIn("RES=42", data)


if __name__ == "__main__":
    unittest.main()
