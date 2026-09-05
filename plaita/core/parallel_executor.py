"""plaita.core.parallel_executor — 统一的并行执行器协议与实现.

2026-07 review (任务 #3) 指出: ``flow.next_node`` 之外, ``Map.concurrent=True`` 与
``Parallel.pool_execute`` 各有一套"把 child_flow 投到并行执行器"的实现, API 完全
不通信——任何"分支/并发调度策略"的修改要改 3 处。本模块抽出 ``ParallelExecutor``
协议, 让 Map / Parallel (以及未来的集合节点) 走同一套执行器接口.

设计要点
--------
- ``ParallelExecutor.map(fn, items) -> list``: 集合节点 (Map/Filter/Find/Reduce)
  的统一入口, 结果顺序与 ``items`` 一致。
- ``ParallelExecutor.submit(fn, *args, **kwargs)`` / ``wait(futures)`` / ``lock``:
  ``Parallel`` 节点需要 fire-and-forget + join 两种语义, 直接用底层原语。
- **不再自起池**: 默认复用模块级单例 ``BackGroundThreadPool`` / ``BackGroundProcessPool``
  (有 ``max_workers`` 与 ``atexit`` 钩子)。``max_workers`` 限制用 ``Semaphore``
  在共享池上 gate 提交, 而不是为每次调用新开一个 ``ThreadPoolExecutor``——历史上
  ``Parallel`` 节点递归嵌套会指数级开 worker, 这条路堵死。
- **cancel/timeout 跨进程不传播**: ``ProcessParallelExecutor.supports_cancel_propagation
  == False``。``ExecutionContext.__getstate__`` 会弹掉不可 pickle 的
  ``threading.Event`` (cancel_event), 子进程拿到的是全新未触发的 Event, 父进程的
  超时/取消信号**不会跨进程传播**。需要响应取消的分支应改用 thread 模式。这条限
  制在 ``ParallelExecutor`` 协议上显式声明, 调用方不必各自 try。
"""

from __future__ import annotations

import atexit
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from concurrent.futures.process import ProcessPoolExecutor
from multiprocessing import Lock as ProcessLock
from threading import Lock, Semaphore
from typing import Any, Callable, List, Optional, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

THREAD = "thread"
PROCESS = "process"

# ---------------------------------------------------------------------------
# 嵌套提交死锁防护
# ---------------------------------------------------------------------------
# ``BackGroundThreadPool`` 是模块级单例。若一个任务**本身跑在该池的 worker 线程
# 上**, 又向同一个池提交分支并原地阻塞等待 (Parallel join / Map.concurrent),
# 则当并发分支数 >= max_workers 时: 所有 worker 都在阻塞等各自的子 future,
# 子 future 排在队列里永远轮不到 —— 无超时、无报错的永久死锁。
# 防护: ``ThreadParallelExecutor._wrap`` 在任务执行期间给线程打 thread-local
# 标记; 节点层 (Parallel.pool_execute / Map._build_executor) 检测到"自己正跑
# 在池 worker 上"就降级为串行执行, 不再向共享池提交。
_pool_tls = threading.local()


def in_plaita_pool_thread() -> bool:
    """当前线程是否正在执行 ``ThreadParallelExecutor`` 提交的任务。"""
    return getattr(_pool_tls, "in_pool", False)

# 后台分支是 fire-and-forget (submit 后不持 future 引用, 不取结果)。模块级单例池
# 的 max_workers 取自环境变量, 默认 thread 8 / process = cpu 数。pytest / Jupyter /
# Web 服务进程隐式持有这两个 pool, 解释器退出时若不显式 shutdown, 待办 future 可能
# 丢——见下方 ``_shutdown_background_pools`` 的 atexit 钩子。
_DEFAULT_BG_THREAD_WORKERS = int(os.environ.get("PLAITA_BG_THREAD_WORKERS", "8"))
_DEFAULT_BG_PROCESS_WORKERS = int(os.environ.get("PLAITA_BG_PROCESS_WORKERS", str(os.cpu_count() or 4)))

BackGroundThreadPool = ThreadPoolExecutor(
    max_workers=_DEFAULT_BG_THREAD_WORKERS,
    thread_name_prefix="plaita-bg-thread",
)
BackGroundProcessPool = ProcessPoolExecutor(
    max_workers=_DEFAULT_BG_PROCESS_WORKERS,
)


def _shutdown_background_pools() -> None:
    """进程退出时显式 shutdown 后台池, 避免待办任务悬挂。"""
    # cancel_futures=True: 解释器已经在退出, 没机会跑排队中的任务, 与其卡住等,
    # 不如直接丢。已在跑的会被 wait。
    for pool, name in (
        (BackGroundThreadPool, "BackGroundThreadPool"),
        (BackGroundProcessPool, "BackGroundProcessPool"),
    ):
        try:
            pool.shutdown(wait=False, cancel_futures=True)
        except Exception:  # pragma: no cover - 退出路径上的 best-effort
            logger.debug("%s shutdown raised during atexit", name, exc_info=True)


atexit.register(_shutdown_background_pools)


@runtime_checkable
class ParallelExecutor(Protocol):
    """统一并行执行器协议。

    集合节点 (Map/Filter/Find/Reduce) 用 ``map``; ``Parallel`` 节点用
    ``submit`` + ``wait`` + ``lock`` 组合 fire-and-forget / join 两种语义。

    ``supports_cancel_propagation`` 把进程模式 cancel_event 跨进程丢失的限制
    显式声明在协议上——调用方不必各自 try/except, 判断一次即可。
    """

    @property
    def supports_cancel_propagation(self) -> bool: ...

    def map(self, fn: Callable[..., Any], items: List[Any]) -> List[Any]:
        """对 ``items`` 并发执行 ``fn``, 返回与 ``items`` 同序的结果列表。

        任一 ``fn(item)`` 抛出的异常会在对应位置 ``.result()`` 时原样抛出,
        不静默吞成 None。
        """
        ...

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """提交一个后台任务, 返回 future-like (有 ``result()``)。"""
        ...

    @staticmethod
    def wait(futures) -> Any:
        """阻塞到所有 future 完成, 返回 ``(done, not_done)``。"""
        ...

    @property
    def lock(self) -> Any:
        """分支结果收集用的同步原语 (thread→``threading.Lock``,
        process→``multiprocessing.Lock``)。"""
        ...


class _BaseExecutor:
    """共享池执行器的通用骨架。

    子类提供 ``_pool`` (一个 ``concurrent.futures.Executor``) 与 ``_lock``。
    ``max_workers`` 不为 None 时用 ``Semaphore`` 在共享池上 gate 提交数,
    避免为每次 ``map`` 新开一个 ``ThreadPoolExecutor``。
    """

    def __init__(self, *, max_workers: Optional[int] = None) -> None:
        self._max_workers = max_workers
        self._sem = Semaphore(max_workers) if max_workers else None

    def _wrap(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        """用 semaphore 限制并发在 ``max_workers`` 以内。

        ``Semaphore`` 在共享池上 gate: acquire 后才 submit, fn 跑完 release。
        这样 ``max_workers=2`` 时, 即使共享池有 8 个 worker, 也最多 2 个在跑本
        执行器的任务——等价于独占一个 2-worker 池, 但不真的开池。
        """
        if self._sem is None:
            return fn
        sem = self._sem

        def _gated(*args: Any, **kwargs: Any) -> Any:
            try:
                return fn(*args, **kwargs)
            finally:
                sem.release()

        return _gated

    def map(self, fn: Callable[..., Any], items: List[Any]) -> List[Any]:
        if not items:
            return []
        wrapped = self._wrap(fn)
        futures = []
        for item in items:
            if self._sem is not None:
                self._sem.acquire()
            futures.append(self._pool.submit(wrapped, item))
        return [f.result() for f in futures]

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        wrapped = self._wrap(fn)
        if self._sem is not None:
            self._sem.acquire()
        return self._pool.submit(wrapped, *args, **kwargs)

    @staticmethod
    def wait(futures) -> Any:
        return wait(futures)


class ThreadParallelExecutor(_BaseExecutor):
    """线程池执行器, 默认复用 ``BackGroundThreadPool`` 单例。

    ``max_workers`` 给定时用 semaphore 限并发 (而非新开池), 用于 ``Map.max_concurrent``。
    ``supports_cancel_propagation=True``: ``cancel_event`` 是 ``threading.Event``,
    线程间共享, 父超时/取消能传到分支。

    提交的任务执行期间线程会打上 ``in_plaita_pool_thread()`` 标记, 供嵌套的
    Parallel/Map 检测并降级串行——见模块头"嵌套提交死锁防护"。
    """

    def __init__(
        self,
        *,
        pool: Optional[ThreadPoolExecutor] = None,
        max_workers: Optional[int] = None,
    ) -> None:
        super().__init__(max_workers=max_workers)
        self._pool = pool or BackGroundThreadPool
        self._lock = Lock()

    def _wrap(self, fn: Callable[..., Any]) -> Callable[..., Any]:
        wrapped = super()._wrap(fn)

        def _marked(*args: Any, **kwargs: Any) -> Any:
            _pool_tls.in_pool = True
            try:
                return wrapped(*args, **kwargs)
            finally:
                _pool_tls.in_pool = False

        return _marked

    @property
    def supports_cancel_propagation(self) -> bool:
        return True

    @property
    def lock(self) -> Any:
        return self._lock


class ProcessParallelExecutor(_BaseExecutor):
    """进程池执行器, 默认复用 ``BackGroundProcessPool`` 单例。

    ``supports_cancel_propagation=False``: ``ExecutionContext.__getstate__`` 弹掉
    不可 pickle 的 ``cancel_event``, 子进程拿到全新未触发的 Event, 父进程的
    超时/取消**不会跨进程传播**。需要响应取消的分支应改用 thread 模式。
    """

    def __init__(
        self,
        *,
        pool: Optional[ProcessPoolExecutor] = None,
        max_workers: Optional[int] = None,
    ) -> None:
        super().__init__(max_workers=max_workers)
        self._pool = pool or BackGroundProcessPool
        self._lock = ProcessLock()

    @property
    def supports_cancel_propagation(self) -> bool:
        return False

    @property
    def lock(self) -> Any:
        return self._lock


class SequentialExecutor:
    """零并发的 ``ParallelExecutor`` 实现, 用于集合节点的非并发路径。

    让 Map/Filter/Find/Reduce 的并发与非并发分支走同一套 ``executor.map`` 代码,
    避免在 ``execute`` 里维护两份控制流。``supports_cancel_propagation=True``
    因为根本不跨执行体。
    """

    @property
    def supports_cancel_propagation(self) -> bool:
        return True

    def map(self, fn: Callable[..., Any], items: List[Any]) -> List[Any]:
        return [fn(item) for item in items]

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("SequentialExecutor 不支持 submit/background 语义")

    @staticmethod
    def wait(futures) -> Any:
        raise NotImplementedError("SequentialExecutor 不支持 wait 语义")

    @property
    def lock(self) -> Any:
        raise NotImplementedError("SequentialExecutor 不支持 lock 语义")


def make_executor(mode: str = THREAD, *, max_workers: Optional[int] = None) -> ParallelExecutor:
    """按 ``Parallel`` 节点的 ``mode`` 字段选执行器。

    - ``thread``  → ``ThreadParallelExecutor``
    - ``process`` → ``ProcessParallelExecutor``
    - 其他       → ``ValueError`` (``coroutine`` 模式已下线, 见 ``Parallel.execute``)
    """
    if mode == THREAD:
        return ThreadParallelExecutor(max_workers=max_workers)
    if mode == PROCESS:
        return ProcessParallelExecutor(max_workers=max_workers)
    raise ValueError(f"Unknown parallel mode: {mode!r} (expected 'thread' or 'process')")
