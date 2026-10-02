"""ReviewFix D1 回归：存储层瞬态错误不得吞成 None。

基线缺陷：RedisExecutionStorage.load_execution_state 把「键不存在」与
「读取/反序列化失败」都返回 None → worker 把 resume 消息当毒丸 ack，
Redis 抖动一次 = 挂起执行永久失去恢复机会。

修复后契约：
- 键不存在 → None（保持现签名兼容，worker 走 poison ack）；
- 读取/反序列化失败 → 抛 ExecutionStateLoadError（worker 不 ack、不 poison，
  走既有重投递/DLQ 路径）。
"""
import threading
from unittest.mock import Mock

import fakeredis
import pytest
import redis.exceptions

from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import StreamTask
from plaita.storage.base import ExecutionState
from plaita.storage.redis import ExecutionStateLoadError, RedisExecutionStorage


# ---------- 存储侧：区分「键不存在」与「读取失败」 ----------


def _make_storage():
    client = fakeredis.FakeRedis(decode_responses=True)
    return RedisExecutionStorage(client=client), client


def test_missing_key_still_returns_none():
    """键不存在 → None（现签名兼容，worker 侧 poison ack 语义不变）。"""
    storage, _ = _make_storage()
    assert storage.load_execution_state("exec-missing") is None


def test_roundtrip_state_still_works():
    storage, _ = _make_storage()
    state = ExecutionState(
        execution_id="exec-ok", flow_id="flow-ok", status="suspended", context={}
    )
    storage.save_execution_state("exec-ok", state)
    loaded = storage.load_execution_state("exec-ok")
    assert loaded is not None
    assert loaded.status == "suspended"


def test_redis_blip_raises_instead_of_none():
    """读取瞬断（连接错误）→ 抛 ExecutionStateLoadError，不再吞成 None。"""
    storage, client = _make_storage()

    def blip(key):
        raise redis.exceptions.ConnectionError("connection reset by peer")

    client.get = blip
    with pytest.raises(ExecutionStateLoadError):
        storage.load_execution_state("exec-1")


def test_redis_timeout_raises_instead_of_none():
    storage, client = _make_storage()

    def slow(key):
        raise redis.exceptions.TimeoutError("read timed out")

    client.get = slow
    with pytest.raises(ExecutionStateLoadError):
        storage.load_execution_state("exec-1")


def test_corrupt_payload_raises_instead_of_none():
    """反序列化失败（数据损坏）→ 抛 ExecutionStateLoadError。"""
    storage, client = _make_storage()
    key = storage.get_namespace_key("execution", "exec-corrupt")
    client.set(key, "{not-json")
    with pytest.raises(ExecutionStateLoadError):
        storage.load_execution_state("exec-corrupt")


def test_error_chains_from_original_exception():
    """抛出的新异常保留原异常链（from e），便于排障。"""
    storage, client = _make_storage()

    def blip(key):
        raise redis.exceptions.ConnectionError("boom")

    client.get = blip
    with pytest.raises(ExecutionStateLoadError) as exc_info:
        storage.load_execution_state("exec-1")
    assert isinstance(exc_info.value.__cause__, redis.exceptions.ConnectionError)


def test_load_error_is_not_value_error():
    """守卫：ExecutionStateLoadError 不得是 ValueError 子类——
    FlowWorker.run 把 ValueError 当畸形消息 poison ack，继承它会让
    瞬态错误仍被丢弃。"""
    assert not issubclass(ExecutionStateLoadError, ValueError)


# ---------- worker 侧：读异常不 ack / 键缺失 poison ack 不变 ----------


class RecordingQueue:
    """替代 RedisStreamTaskQueue 的假队列：记录 ack/poison/failed/DLQ 行为。"""

    max_deliveries = 5
    consumer_name = "test-consumer"

    def __init__(self, tasks):
        self._tasks = list(tasks)
        self.acked = []
        self.poison_count = 0
        self.failed_count = 0
        self.dead_lettered = []
        self.lease_conflicts = 0
        self.on_poison = None
        self.on_failed = None
        self.on_dead_letter = None

    def ensure_group(self):
        pass

    def read(self, block_ms=0):
        if self._tasks:
            return self._tasks.pop(0)
        return None

    def ack(self, message_id):
        self.acked.append(message_id)

    def note_poison(self):
        self.poison_count += 1
        if self.on_poison:
            self.on_poison()

    def note_failed(self):
        self.failed_count += 1
        if self.on_failed:
            self.on_failed()

    def note_lease_conflict(self):
        self.lease_conflicts += 1

    def dead_letter(self, task, *, reason):
        self.dead_lettered.append((task.message_id, reason))
        self.ack(task.message_id)
        if self.on_dead_letter:
            self.on_dead_letter()


def _make_worker(execution_storage):
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:reviewfix-d1",
        execution_storage=execution_storage,
        flow_storage=Mock(),
        redis_client=fakeredis.FakeRedis(decode_responses=True),
        enable_registry=False,
        enable_redis_logging=False,
        read_block_ms=100,
    )
    queue = RecordingQueue([])
    worker._get_task_queue = lambda: queue
    return worker, queue


def _run_and_stop_on(worker, queue, trigger_attr, timeout=10.0):
    """后台线程跑 run()，钩子触发后走真实 stop() 语义，断言主循环干净退出。"""
    done = threading.Event()

    def hook():
        worker.stop()
        done.set()

    setattr(queue, trigger_attr, hook)
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    assert done.wait(timeout), f"等待 {trigger_attr} 钩子超时"
    thread.join(timeout=5)
    assert not thread.is_alive(), "run() 主循环未在停机置位后退出"


def test_storage_load_error_message_stays_pending_not_acked():
    """存储读异常 → 消息不 ack、不 poison，留在 pending 等重投递。"""
    storage = Mock()
    storage.load_execution_state.side_effect = ExecutionStateLoadError(
        "读取执行状态失败（可能是 Redis 瞬断）: exec-1: connection reset"
    )
    worker, queue = _make_worker(storage)
    queue._tasks.append(
        StreamTask(
            message_id="msg-1",
            body={
                "type": "resume",
                "flow_id": "flow-1",
                "execution_id": "exec-1",
                "resume_type": "event",
            },
            delivery_count=1,
        )
    )

    _run_and_stop_on(worker, queue, "on_failed")

    assert queue.acked == [], "瞬态读异常的消息被 ack 了（应留在 pending 重投）"
    assert queue.poison_count == 0, "瞬态读异常被当毒丸处理了"
    assert queue.failed_count == 1
    assert queue.dead_lettered == [], "首次失败不应进 DLQ"


def test_storage_load_error_goes_to_dlq_after_max_deliveries():
    """超过 max_deliveries 后 DLQ 兜底（与既有 task_queue 容忍设计一致）。"""
    storage = Mock()
    storage.load_execution_state.side_effect = ExecutionStateLoadError("still broken")
    worker, queue = _make_worker(storage)
    queue._tasks.append(
        StreamTask(
            message_id="msg-2",
            body={"type": "resume", "flow_id": "f", "execution_id": "e",
                  "resume_type": "event"},
            delivery_count=5,  # >= max_deliveries
        )
    )

    _run_and_stop_on(worker, queue, "on_dead_letter")

    assert len(queue.dead_lettered) == 1
    assert queue.dead_lettered[0][0] == "msg-2"
    assert "ExecutionStateLoadError" in queue.dead_lettered[0][1]
    assert queue.poison_count == 0


def test_missing_state_still_poison_acked():
    """键不存在（None）→ 维持现状：ValueError → poison ack，消息不重投。"""
    storage = Mock()
    storage.load_execution_state.return_value = None
    worker, queue = _make_worker(storage)
    queue._tasks.append(
        StreamTask(
            message_id="msg-3",
            body={"type": "resume", "flow_id": "f", "execution_id": "e",
                  "resume_type": "event"},
            delivery_count=1,
        )
    )

    _run_and_stop_on(worker, queue, "on_poison")

    assert queue.acked == ["msg-3"], "键缺失的畸形 resume 应被 poison ack"
    assert queue.poison_count == 1
    assert queue.dead_lettered == []


def test_load_error_does_not_mark_execution_error():
    """读异常不得把执行状态改写成 error（异常发生在终态改写之前）。"""
    saved = {}

    storage = Mock()
    storage.load_execution_state.side_effect = ExecutionStateLoadError("blip")
    storage.save_execution_state.side_effect = (
        lambda eid, state: saved.setdefault(eid, state)
    )
    worker, queue = _make_worker(storage)
    queue._tasks.append(
        StreamTask(
            message_id="msg-4",
            body={"type": "resume", "flow_id": "f", "execution_id": "e",
                  "resume_type": "event"},
            delivery_count=1,
        )
    )

    _run_and_stop_on(worker, queue, "on_failed")

    assert saved == {}, "读异常不应触发状态改写"
