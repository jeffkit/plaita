"""A69-04（台账第七十轮 PR1）：墓碑 TTL 独立化 + 载体时间戳闭合。

## 缺陷（基线 `3cbf8f4`）

- 墓碑 TTL 跟随 ``PLAITA_EXECUTION_STATE_TTL_DAYS``：state TTL=0 时
  ``execution_tombstone_ttl_seconds()==0``，每次删除留下**永久**墓碑键；
- 显式 ``PLAITA_EXECUTION_TOMBSTONE_TTL_SECONDS=0`` 同样表示永久；
- 墓碑不短于任务最大年龄的语义没有保证（显式 7d + 30d 年龄闸 = 墓碑先过期）；
- 年龄闸只看 payload ``timestamp``，缺失/畸形时 **fail-open**；而
  ``enqueue_task``/EventFilter resume 根本不写 timestamp ⇒ 同 id start 可重建
  已删执行；重投/克隆路径经 ``enqueue_task`` 补 now 还会把老载体洗新；
- DLQ replay 原样入队，缺 timestamp 时也没有源时间证据。

## 本文件钉住的契约

1. 墓碑独立默认 30d；``max(30d, STATE_TTL)`` 只作下限（state=0 不再永久）；
2. ``TOMBSTONE_TTL<=0`` 非法回退默认 + WARNING（policy 变更）；clamp 到
   ``TASK_MAX_AGE_SECONDS``；
3. 墓碑 ``SET NX`` + 只延长 TTL：二次 delete 保留首次 deleted_at；
4. 载体年龄 = max(payload ISO, Stream ID 时间)；两源都不可判 → fail-closed；
5. ``enqueue_task`` 只对缺失值注入 now；重投/交接/克隆保留源年龄；DLQ replay
   用 ``dead_lettered_at``/DLQ Stream ID，不用 now。

## 负控（还原修复后必须精确红）

- 恢复「跟随 state TTL」：``test_state_ttl_zero_does_not_make_tombstone_permanent``
  看到 ttl==-1；恢复「<=0 永久」：``test_explicit_tombstone_zero_falls_back`` 0；
- 去掉 clamp：``test_tombstone_clamped_to_task_max_age`` 得到 7d；
- 恢复覆盖式 SET：``test_repeated_delete_preserves_first_deleted_at`` 看到
  deleted_at 变化（或 TTL 不延长）；
- 只注入 now / 不保留源时间：``test_start_retry_preserves_source_age`` 与
  ``test_dlq_replay_does_not_refresh_timestamp`` 红；
- 年龄闸 fail-open：``test_missing_timestamp_with_old_stream_id_is_dropped`` /
  ``test_unknown_age_fails_closed`` / ``test_future_payload_timestamp_cannot_bypass``
  执行被重建（state 非 None）。
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.dlq_replay import replay_dead_letter
from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import StreamTask, enqueue_task
from plaita.server.tenant_context import (
    reset_current_tenant,
    set_current_tenant,
    tenant_namespace,
)
from plaita.storage.base import ExecutionState
from plaita.storage.fenced import (
    FencedExecutionStorage,
    reset_current_fence_token,
    set_current_fence_token,
)
from plaita.storage.memory import MemoryFlowStorage
from plaita.storage.redis import (
    RedisExecutionStorage,
    execution_tombstone_key,
    execution_tombstone_ttl_seconds,
)

DEFAULT_TOMBSTONE_TTL = 30 * 86400

QUEUE = "r70:a69_04"
TARGET = "r70:a69_04:target"
DLQ = "r70:a69_04:dlq"
TENANT = "acme"
EXEC = "exec-a69-04"
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
def env(monkeypatch):
    for name in (
        "PLAITA_EXECUTION_TOMBSTONE_TTL_SECONDS",
        "PLAITA_EXECUTION_STATE_TTL_DAYS",
        "PLAITA_TASK_MAX_AGE_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)
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


def _seed_running(storage, execution_id=EXEC):
    token = _tenant()
    try:
        storage.save_execution_state(
            execution_id,
            ExecutionState(
                execution_id=execution_id,
                flow_id="f1",
                flow_version="v1",
                status="running",
                context={"$LAST_NODE": "start"},
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


def _state(storage, execution_id=EXEC):
    token = _tenant()
    try:
        return storage.load_execution_state(execution_id)
    finally:
        reset_current_tenant(token)


def _start_body(execution_id=EXEC, timestamp=None, **extra):
    body = {
        "type": "start",
        "flow_id": "f1",
        "execution_id": execution_id,
        "tenant_id": TENANT,
        "params": {"secret": "old-tenant-input"},
    }
    if timestamp is not None:
        body["timestamp"] = timestamp
    body.update(extra)
    return body


def _stream_id(delta: timedelta) -> str:
    ms = int((datetime.now() + delta).timestamp() * 1000)
    return f"{ms}-0"


def _old_stream_id(days: int = 40) -> str:
    return _stream_id(timedelta(days=-days))


# ---------------------------------------------------------------------------
# 墓碑 TTL resolver / 落盘语义
# ---------------------------------------------------------------------------


def test_state_ttl_zero_does_not_make_tombstone_permanent(env, monkeypatch):
    """负控核心：关闭 state TTL 不再制造永久墓碑（独立 30d 下限）。"""
    monkeypatch.setenv("PLAITA_EXECUTION_STATE_TTL_DAYS", "0")

    assert (
        execution_tombstone_ttl_seconds()
        == DEFAULT_TOMBSTONE_TTL
        == 30 * 86400
    )

    _seed_running(env.storage)
    assert _delete(env.storage) is True
    ttl = env.fake.ttl(execution_tombstone_key(EXEC))
    assert ttl > 0, "state TTL=0 时墓碑仍必须有 TTL（不能是 -1 永久）"


def test_explicit_tombstone_zero_falls_back(env, monkeypatch, caplog):
    """policy：``TOMBSTONE_TTL<=0`` 非法回退默认 + WARNING（不再表示永久）。"""
    monkeypatch.setenv("PLAITA_EXECUTION_TOMBSTONE_TTL_SECONDS", "0")

    with caplog.at_level(logging.WARNING):
        ttl = execution_tombstone_ttl_seconds()

    assert ttl == DEFAULT_TOMBSTONE_TTL
    assert any(
        "<=0" in record.getMessage() and "回退" in record.getMessage()
        for record in caplog.records
    ), "<=0 回退必须留下 WARNING"


def test_tombstone_clamped_to_task_max_age(env, monkeypatch, caplog):
    """墓碑 horizon 不得短于年龄闸 horizon（显式 7d → clamp 30d）。"""
    monkeypatch.setenv("PLAITA_EXECUTION_TOMBSTONE_TTL_SECONDS", str(7 * 86400))

    with caplog.at_level(logging.WARNING):
        ttl = execution_tombstone_ttl_seconds()

    assert ttl == 30 * 86400
    assert any("clamp" in record.getMessage() for record in caplog.records)


def test_tombstone_not_shorter_than_state_ttl(env, monkeypatch):
    """不回归：墓碑仍不短于 state TTL（state 90d → 90d）。"""
    monkeypatch.setenv("PLAITA_EXECUTION_STATE_TTL_DAYS", "90")

    assert execution_tombstone_ttl_seconds() == 90 * 86400


def test_repeated_delete_preserves_first_deleted_at(env, monkeypatch):
    """二次 delete 不覆盖首次证据，只延长 TTL（SET NX + 只延长）。"""
    # 关闭年龄闸以免 clamp 把两个显式 TTL 都拉到 30d（本用例只测 TTL 延长）。
    monkeypatch.setenv("PLAITA_TASK_MAX_AGE_SECONDS", "0")
    key = execution_tombstone_key(EXEC)

    monkeypatch.setenv("PLAITA_EXECUTION_TOMBSTONE_TTL_SECONDS", "100")
    _seed_running(env.storage)
    assert _delete(env.storage) is True
    first_payload = json.loads(env.fake.get(key))
    first_ttl = env.fake.ttl(key)
    assert 0 < first_ttl <= 100

    monkeypatch.setenv("PLAITA_EXECUTION_TOMBSTONE_TTL_SECONDS", "200")
    _seed_running(env.storage)
    assert _delete(env.storage) is True
    second_payload = json.loads(env.fake.get(key))
    second_ttl = env.fake.ttl(key)

    assert second_payload == first_payload, "二次删除不得覆盖首次 deleted_at/tenant/reason"
    assert second_ttl > first_ttl, "TTL 只允许延长（100s → 200s）"


def test_fenced_delete_preserves_first_deleted_at(env, monkeypatch):
    """fenced 档（同段 Lua）同一语义：只创建 + 只延长，不覆盖首次证据。"""
    monkeypatch.setenv("PLAITA_TASK_MAX_AGE_SECONDS", "0")
    monkeypatch.setenv("PLAITA_EXECUTION_TOMBSTONE_TTL_SECONDS", "100")
    key = execution_tombstone_key(EXEC)
    fence_key = f"plaita:acme:execution:fence:{EXEC}"
    fencegen_key = f"plaita:acme:execution:fencegen:{EXEC}"
    env.fake.set(fencegen_key, "1")

    def fenced_delete() -> bool:
        env.fake.set(fence_key, "1")
        token = set_current_fence_token(1)
        try:
            return _delete(env.storage)
        finally:
            reset_current_fence_token(token)

    assert fenced_delete() is True
    first_payload = json.loads(env.fake.get(key))
    first_ttl = env.fake.ttl(key)

    monkeypatch.setenv("PLAITA_EXECUTION_TOMBSTONE_TTL_SECONDS", "200")
    assert fenced_delete() is True
    second_payload = json.loads(env.fake.get(key))

    assert second_payload == first_payload
    assert env.fake.ttl(key) > first_ttl


# ---------------------------------------------------------------------------
# 载体 timestamp 注入 / 保留
# ---------------------------------------------------------------------------


def test_enqueue_injects_timestamp_only_when_missing(env):
    """新载体缺 timestamp → 注入 now；已有旧值绝不覆盖。"""
    enqueue_task(env.fake, QUEUE, {"type": "start", "flow_id": "f1", "execution_id": "e1"})
    first = json.loads(env.fake.xrange(QUEUE, count=1)[0][1]["payload"])
    assert "timestamp" in first
    injected_age = (datetime.now() - datetime.fromisoformat(first["timestamp"])).total_seconds()
    assert injected_age < 5

    old = (datetime.now() - timedelta(days=10)).isoformat()
    enqueue_task(
        env.fake,
        QUEUE,
        {"type": "start", "flow_id": "f1", "execution_id": "e2", "timestamp": old},
    )
    second = json.loads(env.fake.xrange(QUEUE, count=2)[-1][1]["payload"])
    assert second["timestamp"] == old


def test_start_retry_preserves_source_age(env):
    """无状态 start 重投保留源年龄（源无 timestamp 时由源 Stream ID 派生）。"""
    class RecordingQueue:
        def __init__(self):
            self.enqueued = []

        def enqueue(self, body, **kwargs):
            self.enqueued.append(dict(body))

        def ack(self, message_id):
            pass

    source_id = _old_stream_id(40)
    task = StreamTask(source_id, _start_body(), 1)
    queue = RecordingQueue()
    consumed = env.worker._requeue_retryable_failure(task, queue)

    assert consumed is True
    assert len(queue.enqueued) == 1
    carried = datetime.fromisoformat(queue.enqueued[0]["timestamp"])
    assert carried < datetime.now() - timedelta(days=39), "重投不得补 now 洗白源年龄"


@pytest.mark.parametrize(
    "payload_timestamp",
    [
        "not-a-date",
        (datetime.now() + timedelta(days=1)).isoformat(),
    ],
    ids=["malformed", "future"],
)
def test_start_retry_keeps_old_stream_evidence(env, payload_timestamp):
    """payload 时间戳畸形/超前时，重投仍以源 Stream ID 的老年龄为准。

    回归（A69 核验残面）：旧实现只要 payload ``timestamp`` 是非空字符串就
    原样复制；畸形值不是年龄证据、超前值会把源 Stream ID 已有的老年龄洗掉，
    两者都会让重投副本的年龄归零。
    """
    class RecordingQueue:
        def __init__(self):
            self.enqueued = []

        def enqueue(self, body, **kwargs):
            self.enqueued.append(dict(body))

        def ack(self, message_id):
            pass

    source_id = _old_stream_id(29)
    task = StreamTask(
        source_id, _start_body(timestamp=payload_timestamp), 1
    )
    queue = RecordingQueue()
    consumed = env.worker._requeue_retryable_failure(task, queue)

    assert consumed is True
    assert len(queue.enqueued) == 1
    carried = datetime.fromisoformat(queue.enqueued[0]["timestamp"])
    assert carried < datetime.now() - timedelta(days=28), (
        f"重投把源年龄洗成了 payload 值: {carried!r}"
    )


def test_event_filter_resume_gets_timestamp(env):
    """EventFilter 的 event/timeout resume 经 enqueue_task 自动获得年龄起点。"""
    for resume_type in ("event", "timeout"):
        enqueue_task(
            env.fake,
            QUEUE,
            {
                "type": "resume",
                "flow_id": "f1",
                "execution_id": f"e-{resume_type}",
                "resume_type": resume_type,
                "tenant_id": TENANT,
                "data": {"subscription_id": "s1"},
            },
        )
    rows = env.fake.xrange(QUEUE)
    assert len(rows) == 2
    for _mid, fields in rows:
        body = json.loads(fields["payload"])
        assert "timestamp" in body and body["timestamp"].strip()


def test_dlq_replay_does_not_refresh_timestamp(env):
    """DLQ replay 用 dead_lettered_at，不用 now；老载体回灌后仍被年龄闸拦。"""
    dead_at = (datetime.now() - timedelta(days=40)).timestamp()
    envelope = {
        "reason": "max_deliveries=5",
        "source_stream": QUEUE,
        "source_id": _old_stream_id(40),
        "delivery_count": 5,
        "dead_lettered_at": dead_at,
        "schema_version": 1,
        "payload": _start_body(),
    }
    dlq_id = env.fake.xadd(DLQ, {"payload": json.dumps(envelope)})

    result = replay_dead_letter(
        env.fake,
        dlq_key=DLQ,
        dlq_id=dlq_id,
        target_stream=TARGET,
        actor="operator",
    )
    assert result.replayed is True

    target_mid, fields = env.fake.xrange(TARGET, count=1)[0]
    replayed = json.loads(fields["payload"])
    carried = datetime.fromisoformat(replayed["timestamp"])
    assert abs((carried.timestamp() - dead_at)) < 1.0, "replay 必须沿用 dead_lettered_at"

    # 老载体回灌后派发：payload 老、目标 Stream ID 新 → max 取老 → 丢弃。
    env.worker._dispatch_task(replayed, stream_message_id=target_mid)
    assert _state(env.storage) is None


# ---------------------------------------------------------------------------
# 年龄闸：双源 max / fail-closed
# ---------------------------------------------------------------------------


def test_missing_timestamp_with_old_stream_id_is_dropped(env, caplog):
    """payload 缺 timestamp 时由 Stream ID 判定：老条目必须丢弃并 WARNING。"""
    body = _start_body()

    with caplog.at_level(logging.WARNING):
        env.worker._dispatch_task(body, stream_message_id=_old_stream_id(40))

    assert _state(env.storage) is None, "墓碑到期后的老载体不得重建执行"
    assert any(
        EXEC in record.getMessage() and "最大年龄" in record.getMessage()
        for record in caplog.records
    )


def test_future_payload_timestamp_cannot_bypass(env):
    """未来 payload 时间戳不能洗白老 Stream ID（取 max 年龄）。"""
    future = (datetime.now() + timedelta(days=1)).isoformat()

    env.worker._dispatch_task(
        _start_body(timestamp=future),
        stream_message_id=_old_stream_id(40),
    )

    assert _state(env.storage) is None


def test_unknown_age_fails_closed(env, caplog):
    """两源都不可判 → fail-closed 丢弃 + carrier_age_unknown_total 计数。"""
    body = _start_body()

    with caplog.at_level(logging.WARNING):
        env.worker._dispatch_task(body, stream_message_id="not-a-stream-id")

    assert _state(env.storage) is None
    assert env.worker._carrier_age_unknown_total == 1
    assert any(
        "年龄无法判定" in record.getMessage() for record in caplog.records
    )


def test_fresh_carrier_with_missing_payload_timestamp_is_allowed(env):
    """对照：Stream ID 年轻且 payload 缺 timestamp → 放行（不过度拦截）。"""
    env.worker._dispatch_task(_start_body(), stream_message_id=_stream_id(timedelta()))

    state = _state(env.storage)
    assert state is not None and state.status == "completed"
