"""A125-01/02 (r83): tenant-gated dead letters get a terminal exit (containment).

不对称面（A1 r125 台账）：family/budget 死信都有年龄出口（B2R31-01），唯独
租户写闸（#277）拒绝后没有任何终局出口——

- A125-01：停用/世代变更租户的超投递载体，守卫恢复分支每轮裸 XADD 泵 1 条
  注定被派发闸丢弃的垃圾副本（``enqueue_task`` 不过租户写闸），``dead_letter``
  写闸拒绝 → 原消息永滞 PEL，形成泵循环；
- A125-02：legacy 载体（无世代）× 已发布世代租户，派发侧 #283 legacy 宽限
  照常执行，死信写闸却 fail-closed 永拒（``generation_missing``）——「执行
  得到、死不掉」，即使执行已终态。

修法（收容语义）：

- 队列层 ``dead_letter_if_allowed`` 捕 ``TenantWriteRejected`` → 信封写**租
  户隔离流** ``{dlq}:quarantine:{tenant}`` + ack 原消息（终局、可观察可救、
  平台 DLQ 纯净性不变）；收容写失败才留 pending。
- 守卫恢复分支先过 ``tenant_write_allowed``：写闸关闭不克隆副本，放行死信
  交队列层收容终局。

四象限负控（复刻 A1 探针 /private/tmp/a125_probe_dlq_terminal_exit.py 的
R1/G1/R2/G2，且按 B125-01 把守卫接通——「守卫先泵副本 × 写闸拒绝」组合
此前零覆盖）：

- R1 停用租户×running×超投递（守卫接通）：不泵副本、原消息收容 ack、PEL 清零。
- G1 active 租户同构：主 DLQ 终局处置（存量语义不变，对照锁定）。
- R2 legacy 载体×已发布世代×终态：死信经收容可达（「永滞 PEL」消除）。
- G2 同构带源世代：主 DLQ 一次死信（存量语义不变，对照锁定）。
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

QUEUE = "test:a125:tasks"
GROUP = "plaita-workers"
DLQ = f"{QUEUE}:dlq"
FLOW_DEF = {
    "flow_id": "f1",
    "version": "1",
    "nodes": [
        {"id": "start", "type": "start", "next": "end"},
        {"id": "end", "type": "end", "output": "ok"},
    ],
}


def _worker(fake, storage):
    fs = MemoryFlowStorage()
    fs.save_flow(FLOW_DEF)
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name=QUEUE,
        execution_storage=storage,
        flow_storage=fs,
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


def _queue(fake, worker, *, max_deliveries=5):
    return RedisStreamTaskQueue(
        fake,
        QUEUE,
        group_name=GROUP,
        consumer_name="c1",
        max_deliveries=max_deliveries,
        dlq_key=DLQ,
        claim_min_idle_ms=0,
        dead_letter_guard=worker._dead_letter_guard,
    )


def _running_state(exec_id):
    return ExecutionState(
        execution_id=exec_id,
        flow_id="f1",
        flow_version="1",
        status="running",
        context={"$LAST_NODE": "start"},
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


class TestFourQuadrants:
    def test_r1_disabled_tenant_contained_not_pumped(self):
        """R1：停用租户×running×超投递——收容 ack，不泵副本（先红后绿主格）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-r1", _running_state("exec-r1"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker)
        fake.hset(TENANT_STATUS_KEY, "acme", "disabled")

        mid = _to_pel(fake, _body("exec-r1", "acme"))
        task = StreamTask(message_id=mid, body=_body("exec-r1", "acme"),
                          delivery_count=5)

        assert queue.dead_letter_if_allowed(task, reason="max_deliveries=5") is True
        # 原消息终局：PEL 清零、流内无垃圾副本（流内唯一条目已随 ack XDEL）。
        assert int(fake.xpending(QUEUE, GROUP)["pending"]) == 0
        assert fake.xlen(QUEUE) == 0
        # 平台 DLQ 纯净（#277），证据在租户隔离流（可观察可救）。
        assert fake.xlen(DLQ) == 0
        assert _quarantine_len(fake, "acme") == 1
        assert queue.stats()["tenant_quarantined"] == 1
        assert queue.stats()["tenant_write_rejected"] == 1

    def test_r1_pump_loop_extinct_via_reclaim(self):
        """R1 端到端：真实 reclaim 机器上泵循环熄灭——一轮收容后 PEL 恒空。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-loop", _running_state("exec-loop"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker, max_deliveries=1)
        fake.hset(TENANT_STATUS_KEY, "acme", "disabled")
        queue.ensure_group()

        enqueue_task(fake, QUEUE, _body("exec-loop", "acme"))
        assert queue.read(block_ms=1) is not None  # 首投 → PEL

        # 修前：每轮 reclaim 泵 1 条副本 + 拒绝留 pending（A1 探针 R1 泵循环）。
        # 修后：第 2 轮 reclaim 命中守卫写闸关闭 → 不泵副本 → 收容 ack。
        assert queue.read(block_ms=1) is None
        assert int(fake.xpending(QUEUE, GROUP)["pending"]) == 0
        assert fake.xlen(QUEUE) == 0, "恢复副本不得入流（派发闸必丢）"
        assert fake.xlen(DLQ) == 0
        assert _quarantine_len(fake, "acme") == 1
        # 第 3 轮：无可回收条目，泵循环不可复活。
        assert queue.read(block_ms=1) is None
        assert _quarantine_len(fake, "acme") == 1

    def test_g1_active_tenant_main_dlq_unchanged(self):
        """G1 对照锁定：active 租户同构场景走主 DLQ 终局，不进收容。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-g1", _running_state("exec-g1"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker)
        fake.hset(TENANT_STATUS_KEY, "acme", "active")

        mid = _to_pel(fake, _body("exec-g1", "acme"))
        task = StreamTask(message_id=mid, body=_body("exec-g1", "acme"),
                          delivery_count=5)

        assert queue.dead_letter_if_allowed(task, reason="max_deliveries=5") is True
        assert fake.xlen(DLQ) == 1
        assert int(fake.xpending(QUEUE, GROUP)["pending"]) == 0
        assert _quarantine_len(fake, "acme") == 0

    def test_r2_legacy_carrier_dead_letter_reachable_even_terminal(self):
        """R2：legacy 载体×已发布世代×终态执行——死信经收容可达（原永滞 PEL）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-r2", _terminal_state("exec-r2"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker)
        fake.hset(TENANT_STATUS_KEY, "beta", "active")
        fake.hset(TENANT_STATUS_META_KEY, "beta",
                  json.dumps({"tenant_generation": "g2"}))

        body = _body("exec-r2", "beta")  # 无 tenant_generation = legacy
        mid = _to_pel(fake, body)
        task = StreamTask(message_id=mid, body=dict(body), delivery_count=5)

        # 修前：每轮 allowed=False、DLQ=0、PEL 恒 1（A1 探针 R2 永不可达）。
        # 修后：一轮即收容终局；ack 后 reclaim 不再见到该消息（多轮语义由
        # test_r1_pump_loop_extinct_via_reclaim 端到端覆盖）。
        assert queue.dead_letter_if_allowed(task, reason="max_deliveries=5") is True
        assert int(fake.xpending(QUEUE, GROUP)["pending"]) == 0
        assert fake.xlen(DLQ) == 0
        assert _quarantine_len(fake, "beta") == 1

        entries = _quarantine_entries(fake, "beta")
        envelope = json.loads(entries[0][1][PAYLOAD_FIELD])
        assert envelope["tenant_write_rejected"] == "generation_missing"
        assert envelope["tenant_generation"] is None
        assert envelope["payload"]["execution_id"] == "exec-r2"

    def test_g2_current_generation_carrier_main_dlq(self):
        """G2 对照锁定：带源世代的同构载体主 DLQ 一次死信，不进收容。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-g2", _terminal_state("exec-g2"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker)
        fake.hset(TENANT_STATUS_KEY, "beta", "active")
        fake.hset(TENANT_STATUS_META_KEY, "beta",
                  json.dumps({"tenant_generation": "g2"}))

        body = _body("exec-g2", "beta")
        mid = _to_pel(fake, body)
        task = StreamTask(message_id=mid, body=dict(body), delivery_count=5,
                          tenant_generation="g2")

        assert queue.dead_letter_if_allowed(task, reason="max_deliveries=5") is True
        assert fake.xlen(DLQ) == 1
        assert int(fake.xpending(QUEUE, GROUP)["pending"]) == 0
        assert _quarantine_len(fake, "beta") == 0


class TestGuardNoPump:
    def test_guard_skips_pump_when_gate_closed(self):
        """守卫直格：写闸关闭（停用）⇒ True 且不入流任何副本（B125-01 组合）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-guard", _running_state("exec-guard"))
        worker = _worker(fake, storage)
        fake.hset(TENANT_STATUS_KEY, "acme", "disabled")

        task = StreamTask(message_id="1-1", body=_body("exec-guard", "acme"),
                          delivery_count=5)
        assert worker._dead_letter_guard(task) is True
        assert fake.xlen(QUEUE) == 0, "写闸关闭不得克隆恢复副本"

    def test_guard_skips_pump_for_legacy_when_generation_published(self):
        """守卫直格：legacy×已发布世代（generation_missing）同样不泵副本。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-legacy", _running_state("exec-legacy"))
        worker = _worker(fake, storage)
        fake.hset(TENANT_STATUS_KEY, "beta", "active")
        fake.hset(TENANT_STATUS_META_KEY, "beta",
                  json.dumps({"tenant_generation": "g2"}))

        task = StreamTask(message_id="1-2", body=_body("exec-legacy", "beta"),
                          delivery_count=5)  # 无世代
        assert worker._dead_letter_guard(task) is True
        assert fake.xlen(QUEUE) == 0

    def test_guard_still_pumps_for_active_tenant(self):
        """对照锁定：active 租户恢复路径不受影响——守卫照常克隆副本。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-active", _running_state("exec-active"))
        worker = _worker(fake, storage)
        fake.hset(TENANT_STATUS_KEY, "acme", "active")

        task = StreamTask(message_id="1-3", body=_body("exec-active", "acme"),
                          delivery_count=5)
        assert worker._dead_letter_guard(task) is True
        assert fake.xlen(QUEUE) == 1, "active 租户恢复副本照常入流"
        copy = json.loads(fake.xrange(QUEUE)[0][1][PAYLOAD_FIELD])
        assert copy["execution_id"] == "exec-active"


class TestContainmentContract:
    def test_quarantine_envelope_shape(self):
        """收容信封与 dead_letter 同形 + 拒绝原因/收容标记，源代际全保留。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-env", _terminal_state("exec-env"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker)
        fake.hset(TENANT_STATUS_KEY, "beta", "active")
        fake.hset(TENANT_STATUS_META_KEY, "beta",
                  json.dumps({"tenant_generation": "g2"}))

        body = _body("exec-env", "beta")
        mid = _to_pel(fake, body)
        task = StreamTask(message_id=mid, body=dict(body), delivery_count=5)

        queue.dead_letter_if_allowed(task, reason="probe-reason")
        (_id, fields), = _quarantine_entries(fake, "beta")
        envelope = json.loads(fields[PAYLOAD_FIELD])
        assert envelope["reason"] == "probe-reason"
        assert envelope["source_stream"] == QUEUE
        assert envelope["source_id"] == mid
        assert envelope["delivery_count"] == 5
        assert envelope["schema_version"] == task.schema_version
        assert envelope["tenant_write_rejected"] == "generation_missing"
        assert envelope["quarantined"] is True
        assert envelope["payload"] == body
        assert "dead_lettered_at" in envelope

    def test_platform_dlq_purity_preserved(self):
        """#277 核心不变量锁定：收容语义下平台 DLQ 仍不收旧 payload。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-pure", _running_state("exec-pure"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker)
        fake.hset(TENANT_STATUS_KEY, "acme", "disabled")

        body = _body("exec-pure", "acme")
        body["params"] = {"secret": "OLD-SECRET"}
        mid = _to_pel(fake, body)
        task = StreamTask(message_id=mid, body=dict(body), delivery_count=5)
        queue.dead_letter_if_allowed(task, reason="max_deliveries=5")

        for _eid, fields in (fake.xrange(DLQ) or []):
            assert "OLD-SECRET" not in str(fields)
        quarantined = json.loads(_quarantine_entries(fake, "acme")[0][1][PAYLOAD_FIELD])
        assert quarantined["payload"]["params"]["secret"] == "OLD-SECRET"

    def test_direct_dead_letter_still_raises(self):
        """直调 dead_letter 契约不变：仍抛具名 TenantWriteRejected。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _worker(fake, MemoryExecutionStorage())
        queue = _queue(fake, worker)
        fake.hset(TENANT_STATUS_KEY, "acme", "disabled")

        from plaita.tenant_context import TenantWriteRejected

        task = StreamTask(message_id="1-9", body=_body("exec-x", "acme"),
                          delivery_count=1)
        with pytest.raises(TenantWriteRejected) as excinfo:
            queue.dead_letter(task, reason="probe")
        assert excinfo.value.tenant_id == "acme"
        assert excinfo.value.reason == "tenant_disabled"
        assert fake.xlen(DLQ) == 0
        assert _quarantine_len(fake, "acme") == 0

    def test_quarantine_write_failure_leaves_pending(self):
        """收容写失败（Redis 故障族）⇒ False + 留 pending，绝不静默丢消息。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-fail", _running_state("exec-fail"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker)
        fake.hset(TENANT_STATUS_KEY, "acme", "disabled")

        body = _body("exec-fail", "acme")
        mid = _to_pel(fake, body)
        task = StreamTask(message_id=mid, body=dict(body), delivery_count=5)

        original_xadd = fake.xadd

        def boom(key, *a, **k):
            if "quarantine" in str(key):
                raise ConnectionError("redis down")
            return original_xadd(key, *a, **k)

        fake.xadd = boom  # type: ignore[method-assign]
        assert queue.dead_letter_if_allowed(task, reason="max_deliveries=5") is False
        assert int(fake.xpending(QUEUE, GROUP)["pending"]) == 1
        assert queue.stats()["tenant_quarantine_failures"] == 1

    def test_ack_failure_returns_false_and_stays_pending(self):
        """收容落盘后 ack 失败 ⇒ False + 留 pending（at-least-once，可重复收容）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-ack", _running_state("exec-ack"))
        worker = _worker(fake, storage)
        queue = _queue(fake, worker)
        fake.hset(TENANT_STATUS_KEY, "acme", "disabled")

        body = _body("exec-ack", "acme")
        mid = _to_pel(fake, body)
        task = StreamTask(message_id=mid, body=dict(body), delivery_count=5)

        def boom(_mid):
            raise ConnectionError("ack failed")

        queue.ack = boom  # type: ignore[method-assign]
        assert queue.dead_letter_if_allowed(task, reason="max_deliveries=5") is False
        assert int(fake.xpending(QUEUE, GROUP)["pending"]) == 1
        assert _quarantine_len(fake, "acme") == 1, "收容条目已落盘（at-least-once）"
        assert queue.stats()["dlq_ack_failures"] == 1

    def test_on_dead_letter_hook_fires_with_quarantine_marker(self):
        """收容走 #26 告警钩子：quarantined/reject_reason 字段可与 DLQ 事件区分。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        storage = MemoryExecutionStorage()
        storage.save_execution_state("exec-hook", _terminal_state("exec-hook"))
        fs = MemoryFlowStorage()
        fs.save_flow(FLOW_DEF)
        worker = RedisFlowWorker(
            redis_url="redis://localhost:6379/15",
            queue_name=QUEUE,
            execution_storage=storage,
            flow_storage=fs,
            redis_client=fake,
            enable_registry=False,
            enable_redis_logging=False,
        )
        events = []
        queue = RedisStreamTaskQueue(
            fake,
            QUEUE,
            group_name=GROUP,
            consumer_name="c1",
            max_deliveries=5,
            dlq_key=DLQ,
            claim_min_idle_ms=0,
            dead_letter_guard=worker._dead_letter_guard,
            on_dead_letter=events.append,
        )
        fake.hset(TENANT_STATUS_KEY, "beta", "active")
        fake.hset(TENANT_STATUS_META_KEY, "beta",
                  json.dumps({"tenant_generation": "g2"}))

        body = _body("exec-hook", "beta")
        mid = _to_pel(fake, body)
        task = StreamTask(message_id=mid, body=dict(body), delivery_count=5)
        queue.dead_letter_if_allowed(task, reason="probe")

        (event,) = events
        assert event["quarantined"] is True
        assert event["reject_reason"] == "generation_missing"
        assert event["tenant_id"] == "beta"
        assert event["dlq_key"] == f"{DLQ}:quarantine:beta"

    def test_quarantine_trimmed_to_dlq_max_len(self):
        """收容流按键裁剪同 dlq_max_len——收容不是无界增长的新口子。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _worker(fake, MemoryExecutionStorage())
        queue = RedisStreamTaskQueue(
            fake,
            QUEUE,
            group_name=GROUP,
            consumer_name="c1",
            max_deliveries=5,
            dlq_key=DLQ,
            claim_min_idle_ms=0,
            dlq_max_len=3,
            dead_letter_guard=worker._dead_letter_guard,
        )
        fake.hset(TENANT_STATUS_KEY, "acme", "disabled")

        for i in range(6):
            task = StreamTask(
                message_id=f"{i + 1}-1",
                body=_body(f"exec-trim-{i}", "acme"),
                delivery_count=5,
            )
            assert queue.dead_letter_if_allowed(
                task, reason="max_deliveries=5"
            ) is True
        assert _quarantine_len(fake, "acme") == 3
