"""#48 回归守卫：Redis **瞬时故障**不得炸穿 worker 消费循环。

缺陷（基线 44febf5）：`RedisStreamTaskQueue.read()` 只吞 `RedisConnectionError`，
而 redis-py 8.x 里 `TimeoutError` 与 `ConnectionError` 是 `RedisError` 下**互不继承**
的兄弟；`ensure_group()` 又只放过 `BUSYGROUP`、其余 `raise`，于是读超时绕开兜底、
经 `_read_once` → `_consume_loop` → `run()`（无 except）→ `main()` 的 `sys.exit(1)`，
**worker 进程整体退出**。

本文件把三层修复各钉一条（**每一层都能单独让它红**）：

* L1 `read()`  —— 瞬时家族（ConnectionError / redis TimeoutError / socket.timeout）吞掉退避重试；
* L2 `ensure_group()` —— 瞬时故障记 warning 返回；**非瞬时仍上抛**（防空吞配置错误）；
* L3 `_consume_loop()` —— 最外层兜底：任何 `RedisError` 都不得穿出，循环继续轮询。

**故意不写成「断言不抛异常」**：那样在修复前后都容易假绿。这里断言的是
**行为**（吞掉并返回 None / 循环继续 / 非瞬时仍抛），且用一个**活对照**证明
夹具走的是真代码路径（`ConnectionError` 在修复前就已能吞）。
"""
from __future__ import annotations

import socket
import threading

import pytest
import redis.exceptions as RE

from plaita.server import task_queue as TQ
from plaita.server.flow_worker import RedisFlowWorker


class _StubRedis:
    """实现 `read()` 空轮询路径会碰到的三个方法；异常由 `exc` 注入。"""

    def __init__(self, exc=None):
        self.exc = exc

    def xgroup_create(self, *a, **k):
        if self.exc is not None:
            raise self.exc
        return True

    def xpending_range(self, *a, **k):
        return []

    def xreadgroup(self, *a, **k):
        return []


def _read(exc):
    """返回 ('propagated', 异常名) 或 ('returned', 值)。退避置 0 以只测控制流。"""
    TQ.RECONNECT_BACKOFF_SECONDS = 0.0
    q = TQ.RedisStreamTaskQueue(_StubRedis(exc), "plaita:flow:queue:test48")
    try:
        return "returned", q.read(block_ms=0)
    except BaseException as e:  # noqa: BLE001
        return "propagated", type(e).__name__


# ── L1：read() 对瞬时家族一律吞掉 ────────────────────────────────────────

@pytest.mark.parametrize("exc,label", [
    (RE.ConnectionError("conn refused"), "ConnectionError（修复前的既有兜底，活对照）"),
    (RE.TimeoutError("Timeout reading from socket"), "redis TimeoutError（缺陷本体）"),
    (socket.timeout("timed out"), "socket.timeout（issue 点名要覆盖）"),
])
def test_read_swallows_transient_redis_failures(exc, label):
    kind, val = _read(exc)
    assert kind == "returned", f"{label} 穿出了 read() ⇒ worker 会退出"
    assert val is None, f"{label} 应退避后返回 None（继续轮询），实得 {val!r}"


def test_timeout_is_not_a_connection_error_subclass():
    """把根因钉在类型事实上：这条一旦变假，说明 redis-py 换了继承树，需重审。"""
    assert issubclass(RE.TimeoutError, RE.RedisError)
    assert not issubclass(RE.TimeoutError, RE.ConnectionError)


# ── L2：ensure_group() 分层（瞬时吞、非瞬时抛）────────────────────────────

@pytest.mark.parametrize("exc", [
    RE.ConnectionError("conn refused"),
    RE.TimeoutError("Timeout creating group"),
    socket.timeout("timed out"),
])
def test_ensure_group_does_not_raise_on_transient(exc):
    q = TQ.RedisStreamTaskQueue(_StubRedis(exc), "plaita:flow:queue:test48")
    q.ensure_group()          # 不得抛：留待上层下一轮重试


@pytest.mark.parametrize("exc", [
    RE.ResponseError("NOAUTH Authentication required"),
    RE.ResponseError("NOPERM this user has no permissions"),
])
def test_ensure_group_still_raises_on_config_errors(exc):
    """反过度吞并：配置/权限类必须**仍然上抛**，否则真问题被静默重试藏起来。"""
    q = TQ.RedisStreamTaskQueue(_StubRedis(exc), "plaita:flow:queue:test48")
    with pytest.raises(RE.ResponseError):
        q.ensure_group()


def test_ensure_group_swallows_busygroup():
    q = TQ.RedisStreamTaskQueue(_StubRedis(RE.ResponseError("BUSYGROUP already exists")),
                                "plaita:flow:queue:test48")
    q.ensure_group()          # 既有语义：组已存在属正常


# ── L3：_consume_loop() 最外层兜底（本单的主验收：进程不退出）────────────

class _FakeWorker:
    read_block_ms = 1_000

    def __init__(self, n_raise):
        self._running = True
        self._drain_event = threading.Event()
        self._n_raise = n_raise
        self.reads = 0

    def _sweep_residue_if_due(self, queue):
        pass

    def _bump_active(self, delta):
        pass

    def _disk_guard_blocks_claim(self):
        # main（#49 之后）在 read() 之前会做本机盘预检；老版本没有这个方法。
        # 两边都提供「不拦」实现，让本测试跨版本可用（rebase 后才发现缺它）。
        return False


class _FlakyQueue:
    """前 `n_raise` 次 read() 抛异常，之后让循环自然退出。"""

    def __init__(self, worker, exc):
        self.worker = worker
        self.exc = exc

    def read(self, block_ms=0):
        self.worker.reads += 1
        if self.worker.reads <= self.worker._n_raise:
            raise self.exc
        self.worker._running = False
        return None


@pytest.mark.parametrize("exc", [
    RE.TimeoutError("Timeout reading from socket"),
    RE.ConnectionError("conn reset by peer"),
    socket.timeout("timed out"),
    RE.ResponseError("LOADING Redis is loading the dataset in memory"),
])
def test_consume_loop_survives_transient_redis_failures(exc):
    """L3 主验收：队列连抛 3 次 ⇒ 循环**不退出**、继续轮询（读到第 4 次）。

    注意这里注入的是 `_FlakyQueue`，绕过了 L1/L2 —— 专门测**循环级**兜底：
    即使某个 Redis 异常真的冒到了 `_consume_loop`，进程也必须活着。
    """
    TQ.RECONNECT_BACKOFF_SECONDS = 0.0
    w = _FakeWorker(n_raise=3)
    RedisFlowWorker._consume_loop(w, _FlakyQueue(w, exc))   # 不得抛
    assert w.reads >= 4, f"循环没继续轮询（read 只被调用 {w.reads} 次）⇒ 异常穿出了循环"


def test_consume_loop_exits_when_draining():
    """停机语义不回归：draining 置位后循环应立刻退出，不再领新任务。"""
    TQ.RECONNECT_BACKOFF_SECONDS = 0.0
    w = _FakeWorker(n_raise=0)
    w._drain_event.set()

    class _NeverRead:
        def read(self, block_ms=0):
            raise AssertionError("draining 期间不该再读队列")

    RedisFlowWorker._consume_loop(w, _NeverRead())
    assert w.reads == 0
