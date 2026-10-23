"""#312 (r86)：A125 收容语义不辨认识论性拒绝——读失败型拒绝被终局化。

A125（``41f7eda``）把 ``TenantWriteRejected`` 一律收容终局（隔离流 + ack），
但该异常混杂两类：

- **确认态**（tenant_disabled / generation_changed / 确认的世代缺失）——租户
  真停用/世代真前移，收容合理（A125 对这半边是对的，本单守恒）；
- **认识论性拒绝**（读失败/瞬断）——``tenant_write_allowed`` 读状态失败
  fail-closed 返回 ``status_read_failed``；``read_tenant_generation`` 吞异常
  返回 None ⇒ meta 读失败伪装成 ``generation_missing``。租户健康、载体可救，
  收容+ack 却把唯一恢复载体终局化进 console 不可见的隔离流 = 可救塌缩不可救。

修法（与 #310 StatePersistError 同哲学：读不到真实状态时不做不可逆处置）：

- ``tenant_status``：世代读取分裂 strict 内核（读失败可辨识），
  ``tenant_write_allowed`` 对 meta 读失败具名 ``generation_read_failed``；
- ``task_queue._quarantine_rejected_dead_letter``：认识论性拒绝（
  ``status_read_failed`` / ``generation_read_failed``）不收容不 ack，留
  pending 走 at-least-once 重投（修前语义）；确认态照收容；
- ``flow_worker._dead_letter_guard``：读失败型关闭改「保守跳过死信」
  （return False，与执行状态读失败 ：5276 对齐）；确认态维持不克隆放行收容。

负控（复刻 A1 r136 探针，修复前全红——瞬断窗健康租户载体：修前收容塌缩，
修后重投可救）：

- 格①②（E2E 两轮交付）：交付一注入瞬态 hget 故障 ⇒ 不 ack 不收容、PEL 非
  零；交付二（重投、读恢复）⇒ 健康租户走主 DLQ 正常终局——#310 家族同款
  「读失败交还重投」交互格。
- 格③④（队列层直格）：belt-2 分流直证（守卫缺席时 ``dead_letter`` 抛出的
  读失败型拒绝也必须留 pending）。
- 格⑤⑥⑦（守卫直格）：读失败型关闭保守跳过（False、零克隆）；确认态
  generation_changed 照旧放行收容（True、零克隆）。
- 格⑧（确认态守恒对照）：generation_changed 收容 + ack 面与修前逐字守恒
  （disabled/generation_missing 两面由 A125 既有 15 格锁定，定向同跑）。
"""
from __future__ import annotations

import json
from datetime import datetime

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import (
    PAYLOAD_FIELD,
    RedisStreamTaskQueue,
    StreamTask,
    enqueue_task,
)
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.tenant_context import TENANT_STATUS_KEY, TENANT_STATUS_META_KEY

QUEUE = "test:a312:tasks"
GROUP = "plaita-workers"
DLQ = f"{QUEUE}:dlq"


def _worker(fake, storage):
    flow_def = {
        "flow_id": "f1",
        "version": "1",
        "nodes": [
            {"id": "start", "type": "start", "next": "end"},
            {"id": "end", "type": "end", "output": "ok"},
        ],
    }
    fs = MemoryFlowStorage()
    fs.save_flow(flow_def)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=storage,
        flow_storage=fs,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


def _queue(fake, worker, *, max_deliveries=5, with_guard=True):
    return RedisStreamTaskQueue(
        fake,
        QUEUE,
        group_name=GROUP,
        consumer_name="c1",
        max_deliveries=max_deliveries,
        dlq_key=DLQ,
        claim_min_idle_ms=0,
        dead_letter_guard=worker._dead_letter_guard if with_guard else None,
    )


def _terminal_state(exec_id):
    return ExecutionState(
        execution_id=exec_id,
        flow_id="f1",
        flow_version="1",
        status="completed",
        context={},
        end_time=datetime.now().isoformat(),
    )


def _running_state(exec_id):
    return ExecutionState(
        execution_id=exec_id,
        flow_id="f1",
        flow_version="1",
        status="running",
        context={"$LAST_NODE": "start"},
    )


def _body(exec_id, tenant):
    return {
        "type": "resume",
        "flow_id": "f1",
        "execution_id": exec_id,
        "resume_type": "continue",
        "tenant_id": tenant,
        "timestamp": datetime.now().isoformat(),
    }


def _to_pel(fake, body):
    """入队并把唯一消息拉进 PEL（模拟已消费/超投递后的 pending 状态）。"""
    enqueue_task(fake, QUEUE, dict(body))
    fake.xgroup_create(QUEUE, GROUP, id="0")
    resp = fake.xreadgroup(GROUP, "c1", {QUEUE: ">"}, count=10)
    ids = [
        mid if isinstance(mid, str) else mid.decode()
        for _stream, entries in resp
        for mid, _fields in entries
    ]
    assert len(ids) == 1
    return ids[0]


def _quarantine_len(fake, tenant):
    return int(fake.xlen(f"{DLQ}:quarantine:{tenant}") or 0)


def _quarantine_entries(fake, tenant):
    return fake.xrange(f"{DLQ}:quarantine:{tenant}") or []


def _pel_count(fake):
    return int(fake.xpending(QUEUE, GROUP)["pending"])


def _poison_hget_once(fake, hash_key):
    """一次性注入：对指定 HASH 的下一次 hget 抛 ConnectionError，其余放行。

    模拟 Redis 瞬断窗（A1 r136 探针同款）：守卫/写闸前的其它 Redis 读
    （计数器 get、租约 exists、流操作）不受影响，故障精确落在租户闸的
    一次读上，下一轮自愈。
    """
    original_hget = fake.hget
    state = {"armed": True}

    def hget(name, key):
        if state["armed"] and name == hash_key:
            state["armed"] = False
            raise ConnectionError("transient redis flap")
        return original_hget(name, key)

    fake.hget = hget  # type: ignore[method-assign]


def _healthy_tenant(fake, tenant, generation="g2"):
    fake.hset(TENANT_STATUS_KEY, tenant, "active")
    fake.hset(
        TENANT_STATUS_META_KEY,
        tenant,
        json.dumps({"tenant_generation": generation}),
    )


class TestEpistemicRedelivery:
    """E2E 两轮交付：瞬断窗不终局化，自愈后健康租户走主 DLQ（修前主格）。"""

    def test_status_read_failure_pending_then_salvageable(self):
        """格①：状态读抖动一轮 ⇒ 不收容不 ack；自愈后重投走主 DLQ 可救。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-s312a", _terminal_state("exec-s312a"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker, max_deliveries=1)
        _healthy_tenant(fake, "beta")

        body = _body("exec-s312a", "beta")
        # 载体带源世代（G2 同构）：自愈轮过闸走主 DLQ，而非 R2 的
        # legacy×已发布世代收容面（那面由 A125 r2 格锁定）。
        enqueue_task(fake, QUEUE, dict(body), tenant_generation="g2")
        queue.ensure_group()
        assert queue.read(block_ms=1) is not None  # 首投 → PEL

        _poison_hget_once(fake, TENANT_STATUS_KEY)
        assert queue.read(block_ms=1) is None  # reclaim → 死信尝试被认识论拒绝
        assert _pel_count(fake) == 1, "读失败型拒绝必须留 pending（at-least-once）"
        assert _quarantine_len(fake, "beta") == 0, "健康租户不得被收容终局化"
        assert fake.xlen(DLQ) == 0
        assert queue.stats()["tenant_reject_epistemic_pending"] == 1

        # 交付二（读恢复）：重投后走真实状态 → 主 DLQ 正常终局。
        # （reclaim 死信轮就地处置后 continue，read() 返回 None、副作用在流上。）
        assert queue.read(block_ms=1) is None
        assert fake.xlen(DLQ) == 1, "瞬断自愈后载体可救（主 DLQ 终局）"
        assert _pel_count(fake) == 0
        assert _quarantine_len(fake, "beta") == 0

    def test_meta_read_failure_not_quarantined_as_generation_missing(self):
        """格②：meta 读抖动伪装 generation_missing 的那格被收回——留 pending。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-s312b", _terminal_state("exec-s312b"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker, max_deliveries=1)
        _healthy_tenant(fake, "beta")

        body = _body("exec-s312b", "beta")
        enqueue_task(fake, QUEUE, dict(body), tenant_generation="g2")
        queue.ensure_group()
        assert queue.read(block_ms=1) is not None

        _poison_hget_once(fake, TENANT_STATUS_META_KEY)
        assert queue.read(block_ms=1) is None
        assert _pel_count(fake) == 1
        assert _quarantine_len(fake, "beta") == 0, (
            "meta 读失败不得产生 tenant_write_rejected='generation_missing' 收容"
        )
        assert queue.stats()["tenant_reject_epistemic_pending"] == 1

        # 自愈后：同载体按真实状态（active+g2 匹配）走主 DLQ。
        assert queue.read(block_ms=1) is None
        assert fake.xlen(DLQ) == 1
        assert _pel_count(fake) == 0
        assert _quarantine_len(fake, "beta") == 0


class TestQueueLayerRouting:
    """belt-2 直格：守卫缺席时队列层分流必须独立成立（A125 既有面的收口）。"""

    def test_status_read_failed_direct_returns_false(self):
        """格③：status_read_failed 直格——dead_letter_if_allowed False + 留 pending。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-s312c", _terminal_state("exec-s312c"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker, with_guard=False)
        _healthy_tenant(fake, "acme")

        body = _body("exec-s312c", "acme")
        mid = _to_pel(fake, body)
        task = StreamTask(
            message_id=mid, body=dict(body), delivery_count=5,
            tenant_generation="g2",
        )

        _poison_hget_once(fake, TENANT_STATUS_KEY)
        assert queue.dead_letter_if_allowed(task, reason="max_deliveries=5") is False
        assert _pel_count(fake) == 1
        assert _quarantine_len(fake, "acme") == 0
        assert fake.xlen(DLQ) == 0
        assert queue.stats()["tenant_reject_epistemic_pending"] == 1
        assert queue.stats()["tenant_quarantined"] == 0

    def test_generation_read_failed_named_reason_no_quarantine(self):
        """格④：meta 读失败具名 generation_read_failed——不落任何收容条目。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-s312d", _terminal_state("exec-s312d"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker, with_guard=False)
        _healthy_tenant(fake, "beta")

        body = _body("exec-s312d", "beta")
        mid = _to_pel(fake, body)
        task = StreamTask(
            message_id=mid, body=dict(body), delivery_count=5,
            tenant_generation="g2",
        )

        _poison_hget_once(fake, TENANT_STATUS_META_KEY)
        assert queue.dead_letter_if_allowed(task, reason="max_deliveries=5") is False
        assert _quarantine_entries(fake, "beta") == []
        assert queue.stats()["tenant_reject_epistemic_pending"] == 1


class TestGuardEpistemicSkip:
    """belt-1 直格：守卫对读失败型关闭改保守跳过（与执行状态读失败同纪律）。"""

    def test_guard_conservative_skip_on_status_read_failure(self):
        """格⑤：状态读失败 ⇒ False（不克隆、不放行收容、消息留 pending）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-s312e", _running_state("exec-s312e"))
        worker = _worker(fake, storage)
        _healthy_tenant(fake, "acme")

        task = StreamTask(
            message_id="1-1", body=_body("exec-s312e", "acme"),
            delivery_count=5, tenant_generation="g2",
        )
        _poison_hget_once(fake, TENANT_STATUS_KEY)
        assert worker._dead_letter_guard(task) is False
        assert fake.xlen(QUEUE) == 0, "读失败窗不得克隆恢复副本"

    def test_guard_conservative_skip_on_meta_read_failure(self):
        """格⑥：meta 读失败（原伪装 generation_missing）⇒ 同样保守跳过。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-s312f", _running_state("exec-s312f"))
        worker = _worker(fake, storage)
        _healthy_tenant(fake, "beta")

        task = StreamTask(
            message_id="1-2", body=_body("exec-s312f", "beta"),
            delivery_count=5, tenant_generation="g2",
        )
        _poison_hget_once(fake, TENANT_STATUS_META_KEY)
        assert worker._dead_letter_guard(task) is False
        assert fake.xlen(QUEUE) == 0

    def test_guard_still_releases_on_confirmed_generation_changed(self):
        """格⑦：确认态 generation_changed 照旧放行收容（True、不克隆）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-s312g", _running_state("exec-s312g"))
        worker = _worker(fake, storage)
        fake.hset(TENANT_STATUS_KEY, "beta", "active")
        fake.hset(
            TENANT_STATUS_META_KEY,
            "beta",
            json.dumps({"tenant_generation": "g3"}),
        )

        task = StreamTask(
            message_id="1-3", body=_body("exec-s312g", "beta"),
            delivery_count=5, tenant_generation="g2",
        )
        assert worker._dead_letter_guard(task) is True
        assert fake.xlen(QUEUE) == 0, "确认态不克隆（A125 语义守恒）"


class TestConfirmedConservation:
    """确认态收容对照面守恒：本单只收回「读失败≠确认」一格，其余逐字不动。"""

    def test_generation_changed_still_quarantined_and_acked(self):
        """格⑧：generation_changed 收容 + ack 与修前逐字守恒（A125 未锁此格）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-s312h", _terminal_state("exec-s312h"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker, with_guard=False)
        fake.hset(TENANT_STATUS_KEY, "beta", "active")
        fake.hset(
            TENANT_STATUS_META_KEY,
            "beta",
            json.dumps({"tenant_generation": "g3"}),
        )

        body = _body("exec-s312h", "beta")
        mid = _to_pel(fake, body)
        task = StreamTask(
            message_id=mid, body=dict(body), delivery_count=5,
            tenant_generation="g2",
        )

        assert queue.dead_letter_if_allowed(task, reason="max_deliveries=5") is True
        assert _pel_count(fake) == 0
        assert fake.xlen(DLQ) == 0
        assert _quarantine_len(fake, "beta") == 1
        (_id, fields), = _quarantine_entries(fake, "beta")
        envelope = json.loads(fields[PAYLOAD_FIELD])
        assert envelope["tenant_write_rejected"] == "generation_changed"
        assert envelope["quarantined"] is True
        assert envelope["tenant_generation"] == "g2"
