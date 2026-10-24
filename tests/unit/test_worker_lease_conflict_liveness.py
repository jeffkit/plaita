"""租约冲突 ack 判据——#322 所有权 v1 收敛后的信号盲契约。

历史（如实记，配方背书）：

- 2026-10-06 修：租约冲突无条件 ack → 对端每 60s XCLAIM 烧 delivery（假死信
  风暴）→ 引入「持有者在册 → ack」。
- 2026-10-08 #50 修：持有者死后租约键有 ≤TTL 尾巴，另一 worker 回收重投消息
  命中冲突分支把**唯一重投载体** ack 掉 → 执行停 running（zombie reap 兜底）
  → 引入注册表存活核算（在册且 running → ack；不在册/draining → 不 ack）。
- 2026-10-11（draining）/2026-10-19（A46-04/A47-F2 决议载体）/
  2026-10-20（B65-03 --no-registry）三轮收紧。
- **2026-10-24 #322 收敛**：存活核算整体移除。理由（#322 正文 + A1 r138
  窗②闭窗判定）：步骤级所有权落地后，重投/重建消费**同一状态行的同一
  step token 声明** ⇒ 连接器幂等键稳定 ⇒ 外部系统去重，「ack 掉唯一载体」
  从不可恢复降级为**可恢复**（#321 对账器「running 无租约 → continue」
  按 next_step 重建；落地前由 zombie reap 兜底）。存活判据的成本恰好是
  它读的信号本身——注册表键被删/副本落后/键过期都会把判据翻错面
  （#322 验收：注册表键被删/副本落后不影响正确性），信号盲是唯一不依赖
  信号质量的形态。

新契约（本文件全部格）：

- **决议载体**（`resume_type ∈ {event, timeout, cancel}`）**无条件不 ack**
  （A46-04/A47-F2 保留：它们不是重复载体，是挂起执行唯一唤醒来源，与
  存活无关——本判据与 #322 收敛正交）；
- 其余冲突（continue/retry/start、无 resume_type 等）**一律 ack**——不读
  租约键、不读注册表、不看 `--no-registry`、不看任何回滚开关。
"""
from __future__ import annotations

import pytest

pytest.importorskip("fakeredis")
import fakeredis  # noqa: E402

from plaita.server.execution_lease import (  # noqa: E402
    ExecutionLeaseError,
    RedisExecutionLease,
    holder_instance_id,
    new_holder_token,
)
from plaita.server.flow_worker import FlowWorker, RedisFlowWorker  # noqa: E402
from plaita.server.registry import ServiceRegistry  # noqa: E402
from plaita.server.task_queue import StreamTask  # noqa: E402
from plaita.server.tenant_context import TenantRoutingExecutionLease  # noqa: E402
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage  # noqa: E402

INSTANCE = "vm-a-1a2b3c4d"
UUID16 = "abcd0123456789ef"
LEASE_KEY = "plaita:execution:lease:exec-1"


def _lease_value(with_gen: bool = True) -> str:
    holder = f"resume:{INSTANCE}:{UUID16}"
    return f"{holder}:7" if with_gen else holder


def _worker(redis, *, enable_registry: bool = True) -> RedisFlowWorker:
    w = RedisFlowWorker.__new__(RedisFlowWorker)
    w.redis_client = redis
    w.execution_lease = TenantRoutingExecutionLease(redis)
    w._enable_registry = enable_registry
    w.lease_ttl_seconds = 120
    return w


def _body() -> dict:
    return {"type": "resume", "execution_id": "exec-1"}


# ---------------------------------------------------------------------------
# 信号盲契约：非决议载体的租约冲突一律 ack（判据不读任何存活信号）
# ---------------------------------------------------------------------------


def test_dead_holder_tail_conflict_is_ack_safe():
    """持有者已死、租约键是 ≤TTL 尾巴（#50 原事故形态）→ ack。

    #322 收敛：声明的 step token 让重跑外部去重；载体丢失由 #321 对账器
    （running 无租约 → continue）或 zombie reap 收敛——不再用注册表猜测
    持有者存亡。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is True


def test_live_holder_conflict_is_ack_safe():
    """活持有者（正常竞争重复载体）→ ack（2026-10-06 语义，信号盲版）。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    redis.set(
        f"{ServiceRegistry.REGISTRY_PREFIX}:flow_worker:{INSTANCE}",
        '{"instance_id": "%s", "status": "running"}' % INSTANCE,
        ex=30,
    )
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is True


def test_registry_key_deleted_is_ack_safe():
    """#322 验收直格：注册表键被删（failover/运维误删/副本落后读不到）→
    ack 判据不受影响——判据根本不读注册表。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    # 刻意写入又删除：历史实现读到「不在册」判 False（吞载体）；
    # 收敛后判据信号盲，同形 ack。
    redis.set(
        f"{ServiceRegistry.REGISTRY_PREFIX}:flow_worker:{INSTANCE}",
        '{"instance_id": "%s"}' % INSTANCE,
        ex=30,
    )
    redis.delete(f"{ServiceRegistry.REGISTRY_PREFIX}:flow_worker:{INSTANCE}")
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is True


def test_draining_holder_conflict_is_ack_safe():
    """draining 持有者 → ack（S18-02A 的保守核算随存活判据一并退役）。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    redis.set(
        f"{ServiceRegistry.REGISTRY_PREFIX}:flow_worker:{INSTANCE}",
        '{"instance_id": "%s", "status": "draining"}' % INSTANCE,
        ex=30,
    )
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is True


def test_registry_entry_corrupt_is_ack_safe():
    """注册表值非法 JSON（副本落后窗的常见形态）→ ack 不受影响。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    redis.set(
        f"{ServiceRegistry.REGISTRY_PREFIX}:flow_worker:{INSTANCE}",
        "not-json",
        ex=30,
    )
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is True


def test_legacy_lease_value_is_ack_safe():
    """旧格式租约（无 instance id，反解不出持有者）→ ack。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, f"resume:{UUID16}:7", ex=120)
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is True


def test_registry_disabled_conflict_is_ack_safe():
    """--no-registry（B65-03 原形态）→ ack。信号盲判据下无例外分支。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    assert _worker(redis, enable_registry=False)._lease_conflict_ack_safe(_body()) is True


def test_missing_lease_is_ack_safe():
    """租约键已消失（异常时序）→ ack。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is True


def test_no_execution_id_is_ack_safe():
    """start 消息无 execution_id（无执行可保护）→ ack。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    assert _worker(redis)._lease_conflict_ack_safe({"type": "start"}) is True


# ---------------------------------------------------------------------------
# 决议载体例外（A46-04/A47-F2 保留，与存活判据收敛正交）
# ---------------------------------------------------------------------------
#
# 挂起窗口 [订阅注册, 租约释放) 内到达的 event/timeout/cancel 不是「重复载体」，
# 而是 suspended 执行**唯一**的决议来源（#33：挂起只能被 event/cancel/timeout
# 唤醒）。持有者落盘 suspended 并释放租约后零载体 ⇒ 执行永久 suspended。
# 故决议载体一律不 ack，留 pending 等租约释放后 reclaim（#242 守卫保证
# 不死信）；continue/retry/start 无推进语义，冲突即 ack。

DECISION_RESUME_TYPES = ("event", "timeout", "cancel")


def _resume_body(resume_type: str) -> dict:
    return {
        "type": "resume",
        "execution_id": "exec-1",
        "resume_type": resume_type,
    }


@pytest.mark.parametrize("resume_type", DECISION_RESUME_TYPES)
def test_decision_carrier_is_not_ack_safe_with_live_holder(resume_type):
    """持有者活着也不能 ack event/timeout/cancel——它们是唯一决议载体。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    redis.set(
        f"{ServiceRegistry.REGISTRY_PREFIX}:flow_worker:{INSTANCE}",
        '{"instance_id": "%s", "status": "running"}' % INSTANCE,
        ex=30,
    )
    assert (
        _worker(redis)._lease_conflict_ack_safe(_resume_body(resume_type)) is False
    )


@pytest.mark.parametrize("resume_type", DECISION_RESUME_TYPES)
def test_decision_carrier_is_not_ack_safe_without_any_signal(resume_type):
    """信号缺失（--no-registry / 租约键消失）对决议载体不得退化为 ack
    （fail-closed 与信号无关——判据本身就不读信号）。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    assert (
        _worker(redis, enable_registry=False)
        ._lease_conflict_ack_safe(_resume_body(resume_type))
        is False
    )
    assert (
        _worker(redis)._lease_conflict_ack_safe(_resume_body(resume_type)) is False
    )


@pytest.mark.parametrize("resume_type", ("continue", "retry"))
def test_non_decision_resume_keeps_ack(resume_type):
    """continue/retry 不携带推进语义 → 冲突即 ack。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    assert (
        _worker(redis)._lease_conflict_ack_safe(_resume_body(resume_type)) is True
    )


def test_start_body_keeps_ack():
    """start 载体冲突即 ack。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    assert (
        _worker(redis)._lease_conflict_ack_safe(
            {"type": "start", "execution_id": "exec-1"}
        )
        is True
    )


def test_decision_carrier_stays_pending_and_reclaimable_after_lease_release():
    """端到端（Redis 层）：决议载体冲突不 ack → 留 PEL → 租约释放后同组
    peer 可回收（载体保活的物理前提，A46-04 验收形态保留）。"""
    stream, group = "plaita:flow:queue:decision", "plaita-workers"
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    redis.xgroup_create(stream, group, id="0", mkstream=True)
    redis.xadd(
        stream,
        {"data": '{"type": "resume", "execution_id": "exec-1", "resume_type": "event"}'},
    )
    _, msgs = redis.xreadgroup(group, INSTANCE, {stream: ">"}, count=1)[0]
    msg_id = msgs[0][0]

    assert (
        _worker(redis)._lease_conflict_ack_safe(_resume_body("event")) is False
    )
    assert redis.xpending(stream, group)["pending"] == 1

    redis.delete(LEASE_KEY)
    _next, claimed, _deleted = redis.xautoclaim(
        stream, group, "peer-consumer", min_idle_time=0, start_id="0-0"
    )
    assert [m[0] for m in claimed] == [msg_id]


# ---------------------------------------------------------------------------
# token 形状工具（租约值内嵌 instance id 的解析面——观测用途保留）
# ---------------------------------------------------------------------------


def test_new_holder_token_embeds_instance_id():
    token = new_holder_token(prefix="resume", instance_id=INSTANCE)
    assert holder_instance_id(token) == INSTANCE


def test_new_holder_token_without_instance_keeps_legacy_shape():
    token = new_holder_token(prefix="resume", instance_id=None)
    assert holder_instance_id(token) is None


def test_holder_instance_id_rejects_unrecognized_shapes():
    assert holder_instance_id(None) is None
    assert holder_instance_id("") is None
    assert holder_instance_id("weird") is None


def test_holder_instance_id_all_decimal_hex_suffix_is_new_format():
    token = new_holder_token(prefix="start", instance_id="i")
    assert holder_instance_id(f"{token}:7") == "i"


def test_holder_instance_id_genuine_legacy_numeric_generation_is_none():
    assert holder_instance_id("resume:abcd0123456789ef:7") is None


def test_new_holder_token_suffix_is_fixed_length_hex():
    """#155 不变量：新格式末段定长 16 位 hex（新旧形状唯一的区分依据）。"""
    for prefix in ("start", "resume", "worker"):
        for instance_id in (None, INSTANCE):
            tok = new_holder_token(prefix=prefix, instance_id=instance_id)
            suffix = tok.rsplit(":", 1)[1]
            assert len(suffix) == 16
            int(suffix, 16)  # 必须是 hex
            assert holder_instance_id(tok) == instance_id


def test_start_flow_holder_carries_instance_id():
    """start 租约值仍嵌注册表 instance id（观测面：冲突日志可反解持有者）。"""
    token = new_holder_token(prefix="start", instance_id=INSTANCE)
    assert holder_instance_id(token) == INSTANCE


def test_consume_loop_gates_ack_on_judgement():
    """消费循环的租约冲突分支必须经判据分流（源码形状锁定）。"""
    import inspect

    src = inspect.getsource(RedisFlowWorker)
    assert "_lease_conflict_ack_safe(task.body)" in src
    # ack 只能发生在判据放行的分支内
    idx = src.find("_lease_conflict_ack_safe(task.body)")
    assert "queue.ack(task.message_id)" in src[idx:idx + 400]


# ---------------------------------------------------------------------------
# 端到端（Redis 层）：非决议冲突 ack 后消息出 PEL（新契约的物理面）
# ---------------------------------------------------------------------------


def test_non_decision_conflict_acks_message_out_of_pel():
    """非决议载体冲突 → ack → 消息出 PEL（不占 pending、不参与回收）。

    载体丢失的收敛路径（#321 对账器 / zombie reap）与外部副作用去重
    （#322 幂等键）不在本格——本格只钉「判据放行 ⇒ ack 发生」。"""
    stream, group = "plaita:flow:queue:blind", "plaita-workers"
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    redis.xgroup_create(stream, group, id="0", mkstream=True)
    redis.xadd(
        stream,
        {"data": '{"type": "resume", "execution_id": "exec-1", "resume_type": "continue"}'},
    )
    _, msgs = redis.xreadgroup(group, INSTANCE, {stream: ">"}, count=1)[0]

    assert _worker(redis)._lease_conflict_ack_safe(
        {"type": "resume", "execution_id": "exec-1", "resume_type": "continue"}
    ) is True
    # 模拟判据放行后消费循环的 ack
    redis.xack(stream, group, msgs[0][0])
    assert redis.xpending(stream, group)["pending"] == 0


class _StubQueue:
    """最小队列桩：弹一条任务后停机；记录 ack/冲突计数。"""

    def __init__(self, worker: RedisFlowWorker, tasks):
        self._worker = worker
        self._tasks = list(tasks)
        self.acked = []
        self.lease_conflicts = 0
        self.consumer_name = "stub"
        self.max_deliveries = 5

    def read(self, block_ms=1000):
        if self._tasks:
            return self._tasks.pop(0)
        self._worker.stop()
        return None

    def ack(self, message_id):
        self.acked.append(message_id)

    def note_lease_conflict(self):
        self.lease_conflicts += 1

    def ensure_group(self):
        pass


def _full_worker(redis) -> RedisFlowWorker:
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="plaita:flow:queue:blind-run",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        redis_client=redis,
        enable_registry=False,
        enable_redis_logging=False,
    )


def test_run_loop_acks_non_decision_conflict():
    """run() 分支负控：非决议载体的租约冲突按判据 ack（判据信号盲）。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    worker = _full_worker(redis)
    task = StreamTask(
        message_id="m1",
        body={"type": "resume", "execution_id": "exec-1", "resume_type": "continue"},
        delivery_count=1,
    )
    stub = _StubQueue(worker, [task])
    mp = pytest.MonkeyPatch()
    try:
        mp.setattr(worker, "_get_task_queue", lambda: stub)
        mp.setattr(
            worker,
            "_dispatch_task",
            lambda *a, **k: (_ for _ in ()).throw(
                ExecutionLeaseError("execution is leased by another worker")
            ),
        )
        worker.run()
    finally:
        mp.undo()
    assert stub.acked == ["m1"], "判据放行 ⇒ ack 发生（信号盲契约）"
    assert stub.lease_conflicts == 1


def test_run_loop_does_not_ack_decision_conflict():
    """run() 分支负控：决议载体冲突不 ack（A46-04/A47-F2 保留）。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    worker = _full_worker(redis)
    task = StreamTask(
        message_id="m2",
        body={"type": "resume", "execution_id": "exec-1", "resume_type": "event"},
        delivery_count=1,
    )
    stub = _StubQueue(worker, [task])
    mp = pytest.MonkeyPatch()
    try:
        mp.setattr(worker, "_get_task_queue", lambda: stub)
        mp.setattr(
            worker,
            "_dispatch_task",
            lambda *a, **k: (_ for _ in ()).throw(
                ExecutionLeaseError("execution is leased by a frozen holder")
            ),
        )
        worker.run()
    finally:
        mp.undo()
    assert stub.acked == [], "决议载体是挂起执行唯一唤醒来源，不得 ack"
    assert stub.lease_conflicts == 1
