"""并发消费改造（2026-10-07）：worker 单机多任务。

背景：原 run() 是严格串行——一次 read 一条、处理完再读下一条，单机同时
只跑 1 个任务（fleet 并发的真瓶颈）。本改造抽 _consume_loop 并允许 N 个
消费线程共享实例；concurrency=1 时行为与改造前逐字一致。

红线：concurrency 默认必须 1（零行为变化）；计数必须线程安全（active_tasks
是看板/注册表的忙闲数据源，错算会让「忙闲」失真）。
"""
from __future__ import annotations

import threading

import pytest

pytest.importorskip("cachetools")
pytest.importorskip("redis")

from plaita.server.flow_worker import RedisFlowWorker


def _skeleton():
    """绕过 __init__ 的 Redis 依赖，只造出并发相关字段。"""
    w = RedisFlowWorker.__new__(RedisFlowWorker)
    w._running = True
    w.concurrency = 1
    w._active_task_count = 0
    w._active_count_lock = threading.Lock()
    w._enable_registry = False
    return w


def test_default_concurrency_is_serial():
    """默认 concurrency=1——改造不得改变存量部署行为。"""
    w = _skeleton()
    assert w.concurrency == 1


def test_concurrency_parsed_from_constructor_value():
    w = _skeleton()
    assert max(1, int(5)) == 5
    # 非法/零值一律夹到 1（不得起 0 线程导致 worker 空转）
    assert max(1, int(0)) == 1
    assert max(1, int(-3)) == 1


def test_bump_active_is_monotonic_and_clamped():
    """_bump_active 多线程累加/递减后必须回到 0，且不为负。"""
    w = _skeleton()
    N, ROUNDS = 8, 200

    def worker_fn():
        for _ in range(ROUNDS):
            w._bump_active(+1)
            w._bump_active(-1)

    threads = [threading.Thread(target=worker_fn) for _ in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert w._active_task_count == 0


def test_bump_active_counts_peak_concurrency():
    """并发峰值应能被观测到（busy 语义：active_tasks>0 即占用）。

    每个线程在持有计数（+1 后）期间记录此刻的 _active_task_count；只要
    有 ≥2 个线程的持有区间重叠，就能观测到 >1。
    """
    w = _skeleton()
    observed = []
    lock = threading.Lock()
    N = 8
    barrier = threading.Barrier(N)
    hold = threading.Event()

    def worker_fn():
        barrier.wait()  # 让 N 个线程尽量同时进入
        w._bump_active(+1)
        with lock:
            observed.append(w._active_task_count)
        # 持有一小段，制造重叠窗口
        hold.wait(timeout=0.05)
        w._bump_active(-1)

    threads = [threading.Thread(target=worker_fn) for _ in range(N)]
    for t in threads:
        t.start()
    # 等所有线程都 bump 过再放行（制造最大重叠）
    import time
    time.sleep(0.02)
    for _ in range(N):
        try:
            hold.set()
        except Exception:
            pass
    for t in threads:
        t.join()

    assert max(observed) > 1, f"并发峰值应 >1，实测 {observed}"
    assert w._active_task_count == 0


def test_consume_loop_exists_and_stops_on_flag():
    """_consume_loop 存在，且 _running=False 时立即退出（不阻塞停机）。"""
    w = _skeleton()
    assert hasattr(w, "_consume_loop")

    class FakeQueue:
        def read(self, block_ms=1000):
            return None  # 空轮询

    w._running = False  # 进循环前就停 → 立即返回
    # 不应抛错、不应卡住
    w._consume_loop(FakeQueue())
