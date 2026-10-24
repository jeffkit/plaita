"""台账 r55 / A2R18：S18-01 drain 语义 + S18-03 交接顺序 + S18-04 回归。

三组契约（规格见 duo-a-ledger.md 第五十五轮）：

- **S18-01**（A45-01/A45-02/A46-06）：claim gate + advisory timeout +
  未完成不 ack + 可中断退避。drain 后 read 返回的任务不 dispatch/ack；
  强停无论 active 是否为 0 都置 ``_running=False``；在途执行尽力 cancel；
  退避分片后可被 D2 打断（不 enqueue、不 ack，消息留 pending）。
- **S18-03**（A45-05）：交接必须 enqueue→ack。enqueue 失败时原条留 pending
  （pre-fix 先 ack 会同时移出 PEL 并从 Stream 删除 = 唯一载体消失）。
- **S18-04**（A46-04）：``resume_type=event/timeout/cancel`` 决议载体在租约
  冲突窗口内不 ack；参数化覆盖 concurrency=1/2 与
  ``PLAITA_DISABLE_HOLDER_LIVENESS=1`` 回滚开关。
"""
from __future__ import annotations

import threading
import time

import pytest

pytest.importorskip("cachetools")
pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis  # noqa: E402

from plaita.event.memory import InMemoryEventBus  # noqa: E402
from plaita.server.execution_lease import ExecutionLeaseError  # noqa: E402
from plaita.server.flow_worker import (  # noqa: E402
    NodeExecutionRetryableError,
    RedisFlowWorker,
)
from plaita.server.registry import ServiceRegistry  # noqa: E402
from plaita.server.task_queue import (  # noqa: E402
    RedisStreamTaskQueue,
    StreamTask,
)
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage  # noqa: E402

STREAM = "plaita:flow:queue:a2r18"
LEASE_KEY = "plaita:execution:lease:exec-1"
HOLDER = "vm-a2r18-1a2b3c4d"
HOLDER_TOKEN = f"resume:{HOLDER}:abcd0123456789ef"
DECISION_TYPES = ("event", "timeout", "cancel")


def _worker(redis=None, **kw) -> RedisFlowWorker:
    kw.setdefault("enable_registry", False)
    kw.setdefault("enable_redis_logging", False)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/0",
        queue_name=STREAM,
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        event_bus=InMemoryEventBus(),
        redis_client=redis,
        **kw,
    )


def _task(body, mid="1-0", delivery_count=1) -> StreamTask:
    return StreamTask(
        message_id=mid, body=body, delivery_count=delivery_count
    )


# ── S18-01：drain 语义 ────────────────────────────────────────────────────


def test_post_read_gate_does_not_dispatch_after_drain():
    """read 在 drain 前发出、drain 后返回：不 dispatch/不 ack/不 bump。

    对应 r45 探针 ``[new-task-after-drain] dispatched_after_drain=True``。
    """
    worker = _worker()
    calls = []

    class _GateQueue:
        def read(self, block_ms=0):
            # 模拟 XREADGROUP 已发出、返回前收到 SIGTERM/stop。
            worker._drain_event.set()
            return _task({"type": "start", "flow_id": "f"})

        def ack(self, mid):  # pragma: no cover - 不应被调用
            calls.append(("ack", mid))

    worker._running = True
    worker._dispatch_task = lambda *a, **k: calls.append(("dispatch",))
    worker._consume_loop(_GateQueue())

    assert calls == [], "drain 后返回的任务不得 dispatch 或 ack"
    assert worker._active_task_count == 0, "也不得 bump active"


def test_force_stop_without_active_still_stops_loop():
    """A45-02：active=0 时定时器也必须置 ``_running=False``（杀早退分支）。"""
    worker = _worker()
    worker._running = True
    worker._drain_event.set()
    worker._active_task_count = 0
    worker._force_stop_after_drain(timeout=0.01)
    assert worker._running is False
    assert worker._force_stop_event.is_set()


def test_force_stop_requests_best_effort_cancel():
    """A45-01：强停对在途执行调 cancel（spy），且不持登记锁调用。"""
    worker = _worker()
    worker._running = True
    worker._drain_event.set()
    worker._active_task_count = 1

    class _Execution:
        def __init__(self):
            self.cancelled = 0

        def cancel(self):
            self.cancelled += 1

    execution = _Execution()
    worker._register_cancel_watch("exec-1", execution)
    worker._force_stop_after_drain(timeout=0.01)

    assert worker._running is False
    assert execution.cancelled == 1, "强停必须尽力 cancel 在途执行"


def test_force_stop_cancel_failure_is_observable(caplog):
    """cancel 抛异常不得带崩强停路径，且留 WARNING（不静默降级）。"""
    worker = _worker()
    worker._running = True
    worker._drain_event.set()
    worker._active_task_count = 1

    class _Execution:
        def cancel(self):
            raise RuntimeError("boom")

    worker._register_cancel_watch("exec-boom", _Execution())
    worker._force_stop_after_drain(timeout=0.01)
    assert worker._running is False
    assert any("drain-force-stop-cancel" in r.message for r in caplog.records)


def test_retry_backoff_aborts_on_force_stop(monkeypatch):
    """A46-06/I5：D2 打断退避 → 不 enqueue、不 ack，返回 False 留 pending。"""
    import plaita.server.flow_worker as fw

    worker = _worker()
    worker._force_stop_event.set()
    sleeps = []
    monkeypatch.setattr(fw, "_sleep", lambda s: sleeps.append(s))

    calls = []

    class _Queue:
        def enqueue(self, *a, **k):  # pragma: no cover - 不应被调用
            calls.append("enqueue")

        def ack(self, mid):  # pragma: no cover - 不应被调用
            calls.append("ack")

    task = _task(
        {"type": "start", "flow_id": "f", "execution_id": "exec-b"},
        mid="5-0",
    )
    ok = worker._requeue_retryable_failure(
        task, _Queue(), NodeExecutionRetryableError("blip", attempt=5)
    )

    assert ok is False
    assert calls == [], "强停后不得再 enqueue/ack 新副本"
    assert sleeps == [], "已到 D2 时应在第一个分片前就放弃"


def test_retry_backoff_is_interruptible_mid_sleep(monkeypatch):
    """退避分片中途置 D2 → 下一片前放弃（不是睡满整个 delay）。"""
    import plaita.server.flow_worker as fw

    worker = _worker()
    sleeps = []

    def _fake_sleep(seconds):
        sleeps.append(seconds)
        # 第一片后模拟 drain 定时器到期。
        worker._force_stop_event.set()

    monkeypatch.setattr(fw, "_sleep", _fake_sleep)

    class _Queue:
        def enqueue(self, *a, **k):  # pragma: no cover
            raise AssertionError("强停后不得入队")

        def ack(self, mid):  # pragma: no cover
            raise AssertionError("强停后不得 ack")

    task = _task({"type": "start", "flow_id": "f", "execution_id": "exec-m"})
    assert worker._requeue_retryable_failure(
        task, _Queue(), NodeExecutionRetryableError("blip", attempt=1)
    ) is False
    assert len(sleeps) == 1, "D2 应在下一分片前打断，不得睡满 1s"


def test_stop_wait_true_waits_for_inflight():
    """B45-06：``stop(wait=True)`` 在停看门狗/注销前等到在途归零。"""
    worker = _worker()
    worker._running = True
    worker._active_task_count = 1
    worker._enable_registry = False
    worker._stop_lease_watchdog = lambda: None
    worker._stop_cancel_watcher = lambda: None

    def _finish():
        time.sleep(0.05)
        worker._active_task_count = 0

    threading.Thread(target=_finish, daemon=True).start()
    worker.stop(wait=True)
    assert worker._active_task_count == 0


# ── S18-03：交接顺序 enqueue→ack ──────────────────────────────────────────


def _real_queue(redis) -> RedisStreamTaskQueue:
    q = RedisStreamTaskQueue(redis, STREAM, consumer_name="c-a2r18")
    q.ensure_group()
    return q


def test_handover_enqueue_failure_leaves_pending():
    """enqueue 失败 → 原条仍在 PEL/Stream（pre-fix 形态 pending=0/stream=0）。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    worker = _worker(redis)
    q = _real_queue(redis)
    body = {
        "type": "start", "flow_id": "f", "execution_id": "exec-h1",
        "params": {"repo": "/nonexistent/xyz"},
    }
    q.enqueue(body)
    task = q.read(block_ms=10)
    assert task is not None
    assert redis.xpending(STREAM, q.group_name)["pending"] == 1

    def _boom(*a, **k):
        raise ConnectionError("redis down")

    q.enqueue = _boom  # type: ignore[assignment]
    ok = worker._handover_non_affine(task, q, RuntimeError("no affinity"))

    assert ok is False
    assert redis.xpending(STREAM, q.group_name)["pending"] == 1, (
        "enqueue 失败不得 ack：原条必须留在 PEL 等 reclaim"
    )
    assert redis.xlen(STREAM) == 1, "原条也不得从 Stream 删除"


def test_handover_success_acks_original_and_enqueues_copy():
    """正常交接：新副本入队（delivery 归 1），原条 acked。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    worker = _worker(redis)
    q = _real_queue(redis)
    body = {
        "type": "start", "flow_id": "f", "execution_id": "exec-h2",
        "params": {"repo": "/nonexistent/xyz"},
    }
    q.enqueue(body)
    task = q.read(block_ms=10)

    assert worker._handover_non_affine(task, q, RuntimeError("x")) is True
    assert redis.xpending(STREAM, q.group_name)["pending"] == 0, "原条应被 ack"
    assert redis.xlen(STREAM) == 1, "恰好一份新副本（原条已 XDEL）"
    copy = q.read(block_ms=10)
    assert copy is not None and copy.delivery_count == 1


def test_handover_ack_failure_keeps_duplicate_and_pending():
    """ack 失败：新副本已入队、原条仍 pending（重复由租约/终态吸收）。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    worker = _worker(redis)
    q = _real_queue(redis)
    body = {
        "type": "start", "flow_id": "f", "execution_id": "exec-h3",
        "params": {"repo": "/nonexistent/xyz"},
    }
    q.enqueue(body)
    task = q.read(block_ms=10)

    def _ack_boom(mid):
        raise ConnectionError("xack down")

    q.ack = _ack_boom  # type: ignore[assignment]
    ok = worker._handover_non_affine(task, q, RuntimeError("x"))

    assert ok is False
    assert redis.xpending(STREAM, q.group_name)["pending"] == 1, "原条仍 pending"
    assert redis.xlen(STREAM) == 2, "原条 + 新副本（重复无害，不能丢）"


# ── S18-04：决议载体 × 并发 × 回滚开关 ────────────────────────────────────


def _seed_live_holder(redis) -> None:
    redis.set(LEASE_KEY, f"{HOLDER_TOKEN}:7", ex=120)
    redis.set(
        f"{ServiceRegistry.REGISTRY_PREFIX}:flow_worker:{HOLDER}",
        '{"instance_id": "%s", "status": "running"}' % HOLDER,
        ex=30,
    )


def _run_consume_until_settled(
    worker, concurrency, want_pending, dispatched, timeout=5.0
):
    """起 concurrency 个消费循环，等 dispatch 已发生且 pending 达到预期后停。

    必须先等 ``dispatched``：rollback 的期望 pending=0 与「消息尚未被读」的
    初始值相同，只等 pending 会假绿（根本没跑到租约冲突分支）。
    """
    worker._running = True
    queue = worker._get_task_queue()
    queue.ensure_group()
    threads = [
        threading.Thread(target=worker._consume_loop, args=(queue,), daemon=True)
        for _ in range(concurrency)
    ]
    for t in threads:
        t.start()
    deadline = time.time() + timeout
    try:
        while time.time() < deadline and not dispatched.is_set():
            time.sleep(0.02)
        if not dispatched.is_set():
            raise AssertionError("等待租约冲突分支 dispatch 超时")
        while time.time() < deadline:
            pending = worker.redis_client.xpending(STREAM, queue.group_name)["pending"]
            if pending == want_pending:
                break
            time.sleep(0.02)
        else:
            pending = worker.redis_client.xpending(STREAM, queue.group_name)["pending"]
            raise AssertionError(f"等待 pending={want_pending} 超时（当前 {pending}）")
    finally:
        worker._running = False
        for t in threads:
            t.join(timeout=2.0)
    return queue


@pytest.mark.parametrize("concurrency", [1, 2], ids=["c1", "c2"])
@pytest.mark.parametrize("resume_type", DECISION_TYPES)
def test_decision_carrier_lease_conflict(
    resume_type, concurrency, monkeypatch
):
    """决议载体在租约冲突窗口内不 ack（A46-04/A47-F2；#322 收敛后保留）。

    S18-04 要求：event/timeout/cancel × concurrency>1 参数化。
    plaita#322 改种：PLAITA_DISABLE_HOLDER_LIVENESS 回滚开关随存活核算一并
    退役（其旁路语义 = 非决议载体的现默认），决议载体的不-ack 判据与信号
    无关，原「rollback 复现旧行为」臂随之退役。
    """
    redis = fakeredis.FakeRedis(decode_responses=True)
    worker = _worker(redis, concurrency=concurrency, claim_min_idle_ms=60_000)
    # 只打开判定闸，避免 init_registry 的注册/心跳写入；本用例只读注册键。
    worker._enable_registry = True
    _seed_live_holder(redis)

    body = {
        "type": "resume",
        "execution_id": "exec-1",
        "resume_type": resume_type,
        "tenant_id": "default",
    }
    queue = worker._get_task_queue()
    queue.ensure_group()
    msg_id = queue.enqueue(body)

    dispatched = threading.Event()

    def _lease_boom(*a, **k):
        dispatched.set()
        raise ExecutionLeaseError("held by live holder")

    worker._dispatch_task = _lease_boom  # type: ignore[assignment]

    queue = _run_consume_until_settled(
        worker, concurrency, 1, dispatched
    )

    pending = redis.xpending(STREAM, queue.group_name)["pending"]
    assert pending == 1, "决议载体不得 ack"
    # 可恢复：持有者释放租约后，peer 以 claim_min_idle=0 回收并推进。
    redis.delete(LEASE_KEY)
    _next, claimed, _deleted = redis.xautoclaim(
        STREAM, queue.group_name, "peer-a2r18", min_idle_time=0, start_id="0-0"
    )
    assert [m[0] for m in claimed] == [msg_id]
    assert claimed[0][1]["payload"].find(resume_type) != -1
