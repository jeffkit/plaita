"""plaita#189：流程定义读取失败不得被吞成 None（Redis 瞬断 ≠ 定义不存在）。

基线缺陷：``RedisFlowStorage.get_flow`` 用 ``except Exception -> return None``
把瞬断/超时/反序列化失败与「键不存在」混成一类；worker 的
``get_flow_definition`` 又把 None 一律翻成 ``ValueError("找不到流程定义")``，
而消费循环把 ValueError 当畸形消息 **poison ack**。Redis 抖动一次 =
start/resume 载体永久消失（#48 / ReviewFix D1 的执行状态腿已修，本单补定义腿）。

修复契约：
- 真不存在（flow/version 缺失）→ 仍是 None（存量语义不变）；
- 读取失败 / 定义 JSON 损坏 → 抛 ``FlowDefinitionLoadError``（非 ValueError）；
- worker 原样上抛 ⇒ 消费循环不 ack、不 poison，走重投递/DLQ。
"""
from __future__ import annotations

import threading
from unittest.mock import Mock

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")
pytest.importorskip("cachetools")

import fakeredis
import redis.exceptions

from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import StreamTask
from plaita.storage.redis import FlowDefinitionLoadError, RedisFlowStorage

FLOW = {"flow_id": "f1", "version": "1.0", "name": "demo", "nodes": []}


def _storage():
    client = fakeredis.FakeRedis(decode_responses=True)
    return RedisFlowStorage(client=client), client


# ---------- 存储侧：真不存在 vs 读取失败 ----------


def test_missing_flow_still_returns_none():
    storage, _ = _storage()
    assert storage.get_flow("ghost") is None


def test_missing_version_still_returns_none():
    storage, _ = _storage()
    storage.save_flow(FLOW)
    assert storage.get_flow("f1", "9.9") is None
    assert storage.get_flow("f1") is not None


def test_sismember_transient_error_raises_named_error():
    storage, client = _storage()

    def blip(*a, **k):
        raise redis.exceptions.ConnectionError("connection reset by peer")

    client.sismember = blip
    with pytest.raises(FlowDefinitionLoadError) as exc_info:
        storage.get_flow("f1", "1.0")
    assert isinstance(exc_info.value.__cause__, redis.exceptions.ConnectionError)


def test_get_transient_error_raises_named_error():
    storage, client = _storage()
    storage.save_flow(FLOW)

    def slow(*a, **k):
        raise redis.exceptions.TimeoutError("read timed out")

    client.get = slow
    with pytest.raises(FlowDefinitionLoadError):
        storage.get_flow("f1", "1.0")


def test_corrupt_definition_raises_named_error():
    storage, client = _storage()
    storage.save_flow(FLOW)
    client.set(storage.get_namespace_key("flow", "f1", "1.0"), "{not-json")
    with pytest.raises(FlowDefinitionLoadError):
        storage.get_flow("f1", "1.0")


def test_load_error_is_not_value_error():
    """守卫：FlowDefinitionLoadError 不得是 ValueError——
    消费循环把 ValueError 当畸形消息 poison ack。"""
    assert not issubclass(FlowDefinitionLoadError, ValueError)


# ---------- worker 侧：读取失败不 ack / 不存在仍是 poison ack ----------


class RecordingQueue:
    max_deliveries = 5
    consumer_name = "test-consumer"

    def __init__(self, tasks):
        self._tasks = list(tasks)
        self.acked = []
        self.poison_count = 0
        self.failed_count = 0
        self.dead_lettered = []
        self.on_poison = None
        self.on_failed = None

    def ensure_group(self):
        pass

    def read(self, block_ms=0):
        return self._tasks.pop(0) if self._tasks else None

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
        pass

    def dead_letter(self, task, *, reason):
        self.dead_lettered.append((task.message_id, reason))
        self.ack(task.message_id)


def _make_worker(flow_storage):
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue189",
        execution_storage=Mock(),
        flow_storage=flow_storage,
        redis_client=fakeredis.FakeRedis(decode_responses=True),
        enable_registry=False,
        enable_redis_logging=False,
        read_block_ms=20,
    )
    queue = RecordingQueue([])
    worker._get_task_queue = lambda: queue
    return worker, queue


def _start_task(**overrides):
    body = {"type": "start", "flow_id": "f1", "params": {}}
    body.update(overrides)
    return StreamTask(message_id="msg-1", body=body, delivery_count=1)


def _run_and_stop_on(worker, queue, trigger_attr, timeout=10.0):
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


def test_worker_propagates_named_error_instead_of_value_error():
    storage = Mock()
    storage.get_flow.side_effect = FlowDefinitionLoadError("读取流程定义失败: f1")
    worker, _ = _make_worker(storage)
    with pytest.raises(FlowDefinitionLoadError):
        worker.get_flow_definition("f1", "1.0")
    with pytest.raises(FlowDefinitionLoadError):
        worker.get_flow_definition_hash("f1", "1.0")


def test_start_carrier_stays_pending_on_load_error():
    """瞬断：start 载体不 ack、不 poison、不首次死信（等重投）。"""
    storage = Mock()
    storage.get_flow.side_effect = FlowDefinitionLoadError("读取流程定义失败: f1")
    worker, queue = _make_worker(storage)
    queue._tasks.append(_start_task())

    _run_and_stop_on(worker, queue, "on_failed")

    assert queue.acked == [], "瞬态读故障的消息被 ack 了（应留在 PEL 重投）"
    assert queue.poison_count == 0, "瞬态读故障被当毒丸处理了"
    assert queue.failed_count == 1
    assert queue.dead_lettered == [], "首次失败不应进 DLQ"


def test_missing_definition_still_poison_ack():
    """真不存在：存量语义不变——ValueError + poison ack（不无限重投）。"""
    storage = Mock()
    storage.get_flow.return_value = None
    worker, queue = _make_worker(storage)
    queue._tasks.append(_start_task())

    _run_and_stop_on(worker, queue, "on_poison")

    assert queue.acked == ["msg-1"]
    assert queue.poison_count == 1
    assert queue.failed_count == 0
