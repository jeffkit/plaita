"""A51-01 / A52-01：执行删除墓碑（round 52 可派工规格）。

删除执行只清 state/index/fence/lease（r36），任何在删除后仍被消费的
``start`` 载体都会走 ``start_flow`` 的「盘上无状态 = 全新执行」分支，把已删
执行以同一 ``execution_id`` 重建并跑完（旧 ``$INPUT`` 重新执行）。本文件钉住：

1. 墓碑与四删同原子落盘（pipeline 失败 → delete 返回 False）；
2. 四条可达载体路径（队列 start / XCLAIM 回收 / 节点失败重投 / 死信守卫
   克隆）在删除后都不得重建；
3. 平台根墓碑不被租户清理擦除（tenant namespace purge 后仍生效）；
4. 新 execution_id 正常放行、同 id 重放拒绝、dedup 映射不把旧 id 套到新启动；
5. 墓碑 TTL 到期后的历史载体由消息最大年龄闸兜底。

pre-fix（基线）预期：除「新 id 放行」对照外全部失败（重建 completed）。
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import RedisStreamTaskQueue, StreamTask, enqueue_task
from plaita.server.tenant_context import (
    reset_current_tenant,
    set_current_tenant,
    tenant_namespace,
)
from plaita.storage.base import ExecutionState
from plaita.storage.fenced import FencedExecutionStorage
from plaita.storage.memory import MemoryFlowStorage
from plaita.storage.redis import RedisExecutionStorage

QUEUE = "r52:tombstone"
DLQ = QUEUE + ":dlq"
TENANT = "acme"
EXEC = "exec-deleted"
FLOW = {
    "flow_id": "f1",
    "version": "v1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end"},
    ],
}


class Env:
    def __init__(self, fake, storage, worker):
        self.fake = fake
        self.storage = storage
        self.worker = worker


@pytest.fixture()
def env():
    fake = fakeredis.FakeRedis(decode_responses=True)
    fake.flushdb()
    storage = FencedExecutionStorage(
        RedisExecutionStorage(client=fake, namespace=tenant_namespace(TENANT))
    )
    flows = MemoryFlowStorage()
    flows.save_flow(FLOW)
    worker = RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=storage,
        flow_storage=flows,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )
    return Env(fake, storage, worker)


def _tenant():
    return set_current_tenant(TENANT, source="explicit")


def _seed_running(storage, execution_id=EXEC, context=None):
    token = _tenant()
    try:
        storage.save_execution_state(
            execution_id,
            ExecutionState(
                execution_id=execution_id,
                flow_id="f1",
                flow_version="v1",
                status="running",
                context=context or {"$LAST_NODE": "start"},
                tenant_id=TENANT,
            ),
        )
    finally:
        reset_current_tenant(token)


def _delete(storage, execution_id=EXEC):
    token = _tenant()
    try:
        return storage.delete_execution_state(execution_id)
    finally:
        reset_current_tenant(token)


def _start_body(execution_id=EXEC, **extra):
    body = {
        "type": "start",
        "flow_id": "f1",
        "execution_id": execution_id,
        "tenant_id": TENANT,
        "params": {"secret": "old-tenant-input"},
        "timestamp": datetime.now().isoformat(),
    }
    body.update(extra)
    return body


def _tombstone_key(execution_id=EXEC):
    # 平台根键（不匹配 default/tenant 的 {ns}:* 清理模式）。
    return f"plaita:exec_tombstone:{execution_id}"


def _state(storage, execution_id=EXEC):
    token = _tenant()
    try:
        return storage.load_execution_state(execution_id)
    finally:
        reset_current_tenant(token)


def test_delete_writes_tombstone_atomically(env, monkeypatch):
    """墓碑与状态/索引/fence/lease 同一 pipeline；失败时 delete 返回 False。"""
    monkeypatch.setenv("PLAITA_EXECUTION_TOMBSTONE_TTL_SECONDS", "3600")
    _seed_running(env.storage)

    assert _delete(env.storage) is True
    raw = env.fake.get(_tombstone_key())
    assert raw, "删除必须写执行墓碑"
    payload = json.loads(raw)
    assert payload["tenant"] == TENANT
    assert payload.get("reason") == "execution_deleted"
    assert env.fake.ttl(_tombstone_key()) > 0
    assert _state(env.storage) is None

    # 原子性：pipeline 异常时既不能删状态，也不能留下「状态已删、墓碑未写」
    # 的中间态；console 按 False → 503。
    _seed_running(env.storage)
    env.fake.delete(_tombstone_key())

    def boom(*_a, **_kw):
        raise RuntimeError("redis pipeline down")

    monkeypatch.setattr(env.fake, "pipeline", boom)
    assert _delete(env.storage) is False
    assert env.fake.exists(_tombstone_key()) == 0
    assert _state(env.storage) is not None


def test_queued_start_after_delete_is_discarded(env):
    """路径①：删除后消费删除前入队的 start → 不得重建（state 保持 None）。"""
    _seed_running(env.storage)
    assert _delete(env.storage) is True

    env.worker._dispatch_task(_start_body())

    assert _state(env.storage) is None
    assert env.fake.exists(_tombstone_key()) == 1


def test_reclaim_start_after_delete_is_discarded(env):
    """路径②：崩溃未 ack → XCLAIM 回收 → 不得重建。"""
    _seed_running(env.storage)
    queue = RedisStreamTaskQueue(
        env.fake, QUEUE, consumer_name="w1", claim_min_idle_ms=0, max_deliveries=10
    )
    queue.ensure_group()
    queue.enqueue(_start_body())
    first = queue.read(block_ms=1)
    assert first is not None  # worker 崩溃前未 ack

    assert _delete(env.storage) is True
    reclaimed = queue.read(block_ms=1)
    assert reclaimed is not None
    assert queue._metrics["reclaimed"] >= 1

    env.worker._dispatch_task(
        reclaimed.body, delivery_count=reclaimed.delivery_count
    )
    assert _state(env.storage) is None


def test_requeue_after_delete_does_not_enqueue(env):
    """路径③：节点失败重投时 state 已删 → ack 原条、不产生 start 副本。"""

    class RecordingQueue:
        def __init__(self):
            self.enqueued = []
            self.acked = []

        def enqueue(self, body, **kwargs):
            self.enqueued.append((dict(body), kwargs))

        def ack(self, message_id):
            self.acked.append(message_id)

    class RetryExc(Exception):
        attempt = 0

    _seed_running(env.storage)
    assert _delete(env.storage) is True

    task = StreamTask("1-0", _start_body(), 1)
    queue = RecordingQueue()
    consumed = env.worker._requeue_retryable_failure(task, queue, RetryExc("node failed"))

    assert consumed is True
    assert queue.enqueued == []
    assert queue.acked == ["1-0"]


def test_guard_window_after_delete_does_not_clone(env):
    """路径④：守卫读 state 后删除 → 租约空 → 不得克隆 start 副本。"""
    _seed_running(env.storage)
    queue = env.worker._get_task_queue()
    queue.ensure_group()
    queue.enqueue(_start_body())
    task = queue.read(block_ms=1)
    assert task is not None

    original = env.worker._max_node_retry_counter

    def delete_between_state_and_lease(execution_id):
        # 守卫已读完 state（running），这里删除，随后守卫探测租约必为空。
        assert _delete(env.storage, execution_id) is True
        return original(execution_id)

    env.worker._max_node_retry_counter = delete_between_state_and_lease
    try:
        allowed = queue.dead_letter_if_allowed(task, reason="delivery exhausted")
    finally:
        env.worker._max_node_retry_counter = original

    assert allowed is True
    # dead_letter ack 会 XDEL 原条；任何残留条目都是「克隆恢复副本」
    # （pre-fix 恰好 1 条），post-fix 必须为 0。
    assert env.fake.xlen(QUEUE) == 0
    assert env.fake.xlen(DLQ) == 1
    assert _state(env.storage) is None


def test_dedup_after_delete_does_not_restart_same_id(env):
    """同 id 重放：拒绝、释放 dedup 映射；新 id + 旧 dedup_key 可正常启动。"""
    _seed_running(env.storage)
    assert _delete(env.storage) is True
    dedup_key = "plaita:acme:start-dedup:nightly"
    env.fake.set(dedup_key, EXEC)

    env.worker._dispatch_task(_start_body(dedup_key="nightly"))
    assert _state(env.storage) is None
    assert env.fake.exists(dedup_key) == 0, "同 id 重放必须释放孤儿/重放映射"

    env.worker._dispatch_task(
        _start_body(execution_id="exec-fresh-dedup", dedup_key="nightly")
    )
    fresh = _state(env.storage, "exec-fresh-dedup")
    assert fresh is not None and fresh.status == "completed"


def test_start_flow_direct_call_is_rejected(env):
    """直达调用同样拒绝（不依赖队列分派闸）。"""
    _seed_running(env.storage)
    assert _delete(env.storage) is True

    token = _tenant()
    try:
        result = env.worker.start_flow(
            "f1", {"secret": "old-tenant-input"}, execution_id=EXEC
        )
    finally:
        reset_current_tenant(token)

    assert result.get("discarded") is True
    assert result.get("status") == "deleted"
    assert _state(env.storage) is None


def test_tenant_purge_does_not_erase_tombstone(env):
    """墓碑在平台根：tenant namespace purge 后仍拦截同名重建前的旧载体。"""
    _seed_running(env.storage)
    assert _delete(env.storage) is True
    for key in env.fake.scan_iter("plaita:acme:*"):
        env.fake.delete(key)

    assert env.fake.exists(_tombstone_key()) == 1
    env.worker._dispatch_task(_start_body())
    assert _state(env.storage) is None


def test_fresh_execution_id_after_delete_is_allowed(env):
    """对照：新 execution_id 正常完成（删除不误伤新启动）。"""
    _seed_running(env.storage)
    assert _delete(env.storage) is True

    env.worker._dispatch_task(_start_body(execution_id="exec-new"))

    fresh = _state(env.storage, "exec-new")
    assert fresh is not None and fresh.status == "completed"


def test_stale_carrier_beyond_ttl_is_dropped(env, monkeypatch):
    """墓碑 TTL 到期后的历史载体由消息最大年龄闸兜底（不复活）。"""
    monkeypatch.setenv("PLAITA_TASK_MAX_AGE_SECONDS", "60")
    _seed_running(env.storage)
    assert _delete(env.storage) is True
    # 模拟墓碑 TTL 到期（键消失），只剩一条远超最大年龄的 start 载体。
    env.fake.delete(_tombstone_key())

    old = (datetime.now() - timedelta(seconds=120)).isoformat()
    env.worker._dispatch_task(_start_body(timestamp=old))

    assert _state(env.storage) is None
