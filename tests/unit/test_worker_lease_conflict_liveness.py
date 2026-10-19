"""租约冲突 ack 的持有者存活闸（plaita#50）。

背景（2026-10-08 维护窗第二次实证）：SIGTERM 杀掉租约持有者后，租约键仍有
≤TTL（120s）尾巴；另一 worker 按 claim_min_idle（60s）回收重投消息时命中
ExecutionLeaseError 分支**直接 ack**——非终态执行唯一的重投载体被吞，执行
停在 running，恢复退化成 2h zombie reap。

契约：
- 持有者仍在服务注册表（活持有者）→ ack（保留 2026-10-06 假死信修复）；
- 持有者已不在注册表（死后 TTL 尾巴）→ 不 ack，留 pending 等租约过期后
  reclaim 续跑；
- 任何信号缺失（旧格式租约 / 未启用注册表 / 读租约失败）→ 退化 ack，
  不回归假死信风暴。
"""
from __future__ import annotations

import pytest

pytest.importorskip("fakeredis")
import fakeredis  # noqa: E402

from plaita.server.execution_lease import (  # noqa: E402
    RedisExecutionLease,
    holder_instance_id,
    new_holder_token,
)
from plaita.server.flow_worker import FlowWorker, RedisFlowWorker  # noqa: E402
from plaita.server.registry import ServiceRegistry  # noqa: E402
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


def test_dead_holder_lease_conflict_is_not_ack_safe():
    """持有者已不在注册表 → ack 不安全（消息须留 pending 等租约过期）。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is False


def test_live_holder_lease_conflict_is_ack_safe():
    """持有者在册（心跳内）→ ack 安全（2026-10-06 假死信修复保留）。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    redis.set(
        f"{ServiceRegistry.REGISTRY_PREFIX}:flow_worker:{INSTANCE}",
        '{"instance_id": "%s"}' % INSTANCE,
        ex=30,
    )
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is True


def test_draining_holder_lease_conflict_is_not_ack_safe():
    """持有者在册但 draining（停机中）→ ack 不安全（2026-10-11 Mac 重启丢任务）。

    draining 持有者可能在 drain 超时后被放弃；若此时 ack 释放了消息，执行
    失去唯一重投载体，停在 running。必须留 pending。
    """
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    redis.set(
        f"{ServiceRegistry.REGISTRY_PREFIX}:flow_worker:{INSTANCE}",
        '{"instance_id": "%s", "status": "draining"}' % INSTANCE,
        ex=30,
    )
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is False


def test_running_holder_with_status_field_is_ack_safe():
    """持有者在册且 status=running → 仍 ack 安全（只有 draining 才收紧）。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    redis.set(
        f"{ServiceRegistry.REGISTRY_PREFIX}:flow_worker:{INSTANCE}",
        '{"instance_id": "%s", "status": "running"}' % INSTANCE,
        ex=30,
    )
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is True


def test_draining_entry_unparseable_defaults_to_ack():
    """注册表值非法 JSON → 无法判 draining → 保持原语义（ack）。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    redis.set(
        f"{ServiceRegistry.REGISTRY_PREFIX}:flow_worker:{INSTANCE}",
        "not-json",
        ex=30,
    )
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is True


def test_registry_entry_expired_after_death_is_not_ack_safe():
    """注册表 TTL（30s）先于 60s 首次回收消失：键过期即判死。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    # 模拟心跳停止后注册表键到期：不写入注册表键即可（上一用例已覆盖在册）
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is False


def test_legacy_lease_value_defaults_to_ack():
    """旧格式租约（无 instance id）反解不出持有者 → 存活未知 → 退化 ack。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, f"resume:{UUID16}:7", ex=120)
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is True


def test_registry_disabled_defaults_to_ack():
    """未启用注册表（--no-registry）无存活信号 → 退化 ack。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    assert _worker(redis, enable_registry=False)._lease_conflict_ack_safe(_body()) is True


def test_missing_lease_defaults_to_ack():
    """租约键已消失（异常时序）→ 无从判定 → 退化 ack。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is True


def test_no_execution_id_defaults_to_ack():
    """start 消息无 execution_id（无执行可保护）→ 退化 ack。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    assert _worker(redis)._lease_conflict_ack_safe({"type": "start"}) is True


def test_rollback_switch_bypasses_liveness_gate(monkeypatch):
    """PLAITA_DISABLE_HOLDER_LIVENESS=1 → 整体旁路，无条件 ack（回滚开关）。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    monkeypatch.setenv("PLAITA_DISABLE_HOLDER_LIVENESS", "1")
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is True


# ---- A46-04 / A47-F2：决议载体在租约冲突窗口内不得 ack ----
#
# 挂起窗口 [订阅注册, 租约释放) 内到达的 event/timeout/cancel 不是「重复载体」，
# 而是 suspended 执行**唯一**的决议来源（#33：挂起只能被 event/cancel/timeout
# 唤醒）。旧判据只看持有者是否在册，把这三类当重复载体 ack 掉 → 持有者落盘
# suspended 并释放租约后零载体，执行永久 suspended（默认 event 节点自动恢复网
# 为 0）。故决议载体一律不 ack，留 pending 等租约释放后 reclaim（#242 守卫保证
# 不死信）；continue/retry/start 无推进语义，保持原 ack 语义。

DECISION_RESUME_TYPES = ("event", "timeout", "cancel")


def _resume_body(resume_type: str) -> dict:
    return {
        "type": "resume",
        "execution_id": "exec-1",
        "resume_type": resume_type,
    }


def _set_live_holder(redis) -> None:
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    redis.set(
        f"{ServiceRegistry.REGISTRY_PREFIX}:flow_worker:{INSTANCE}",
        '{"instance_id": "%s", "status": "running"}' % INSTANCE,
        ex=30,
    )


@pytest.mark.parametrize("resume_type", DECISION_RESUME_TYPES)
def test_decision_carrier_is_not_ack_safe_with_live_holder(resume_type):
    """持有者活着也不能 ack event/timeout/cancel——它们是唯一决议载体。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    _set_live_holder(redis)
    assert (
        _worker(redis)._lease_conflict_ack_safe(_resume_body(resume_type)) is False
    )


@pytest.mark.parametrize("resume_type", DECISION_RESUME_TYPES)
def test_decision_carrier_is_not_ack_safe_without_any_signal(resume_type):
    """信号缺失（--no-registry / 租约键消失）对决议载体不得退化为 ack。

    旧实现在这些分支 fail-open；对决议载体那是「唯一载体被吞」，必须 fail-closed。
    """
    redis = fakeredis.FakeRedis(decode_responses=True)
    assert (
        _worker(redis, enable_registry=False)
        ._lease_conflict_ack_safe(_resume_body(resume_type))
        is False
    )
    # 租约键已消失（异常时序）：仍不 ack
    assert (
        _worker(redis)._lease_conflict_ack_safe(_resume_body(resume_type)) is False
    )


@pytest.mark.parametrize("resume_type", ("continue", "retry"))
def test_non_decision_resume_keeps_ack_with_live_holder(resume_type):
    """continue/retry 不携带推进语义 → 持有者在册时保持原 ack（#33 不变）。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    _set_live_holder(redis)
    assert (
        _worker(redis)._lease_conflict_ack_safe(_resume_body(resume_type)) is True
    )


def test_start_body_keeps_ack_with_live_holder():
    """start 载体保持原语义：持有者在册 → ack（#23/#50 不变）。"""
    redis = fakeredis.FakeRedis(decode_responses=True)
    _set_live_holder(redis)
    assert (
        _worker(redis)._lease_conflict_ack_safe(
            {"type": "start", "execution_id": "exec-1"}
        )
        is True
    )


def test_decision_carrier_stays_pending_and_reclaimable_after_lease_release():
    """端到端（Redis 层）：决议载体不被 ack → 租约释放后 peer 可回收推进。

    对应 A47-F2 配方探针：pre-fix 窗口内 ack → 永久 suspended；post-fix
    pending=1 → 租约释放后由 XCLAIM 接管（delivery 归 1 的副本）。
    """
    stream, group = "plaita:flow:queue:f2", "plaita-workers"
    redis = fakeredis.FakeRedis(decode_responses=True)
    _set_live_holder(redis)
    redis.xgroup_create(stream, group, id="0", mkstream=True)
    redis.xadd(
        stream,
        {"data": '{"type": "resume", "execution_id": "exec-1", "resume_type": "event"}'},
    )
    _, msgs = redis.xreadgroup(group, INSTANCE, {stream: ">"}, count=1)[0]
    msg_id = msgs[0][0]

    assert _worker(redis)._lease_conflict_ack_safe(_resume_body("event")) is False
    assert redis.xpending(stream, group)["pending"] == 1

    # 持有者落盘 suspended 后释放租约：消息仍在，可被同组 peer 回收
    redis.delete(LEASE_KEY)
    _next, claimed, _deleted = redis.xautoclaim(
        stream, group, "peer-consumer", min_idle_time=0, start_id="0-0"
    )
    assert [m[0] for m in claimed] == [msg_id]
    assert '"resume_type": "event"' in claimed[0][1]["data"]


def test_tenant_routed_lease_is_read_under_message_tenant():
    """租约键按消息 tenant_id 路由读取（守卫同款键格式）。"""
    from plaita.server.tenant_context import tenant_namespace

    redis = fakeredis.FakeRedis(decode_responses=True)
    ns = tenant_namespace("acme")
    redis.set(f"{ns}:execution:lease:exec-t", _lease_value(), ex=120)
    w = _worker(redis)
    assert w._lease_conflict_ack_safe(
        {"type": "resume", "execution_id": "exec-t", "tenant_id": "acme"}
    ) is False
    # 消息不带 tenant（default namespace）时读不到 acme 的租约 → 退化 ack
    assert w._lease_conflict_ack_safe({"type": "resume", "execution_id": "exec-t"}) is True


# ---- holder token 形状与反解 ----


def test_new_holder_token_embeds_instance_id():
    tok = new_holder_token(prefix="resume", instance_id=INSTANCE)
    assert tok == f"resume:{INSTANCE}:{tok.split(':')[2]}"
    assert holder_instance_id(tok) == INSTANCE
    # fencing 档租约值 = holder + 世代
    assert holder_instance_id(f"{tok}:42") == INSTANCE


def test_new_holder_token_without_instance_keeps_legacy_shape():
    tok = new_holder_token(prefix="resume")
    assert tok.startswith("resume:") and tok.count(":") == 1
    assert holder_instance_id(tok) is None


def test_holder_instance_id_rejects_unrecognized_shapes():
    assert holder_instance_id(None) is None
    assert holder_instance_id("") is None
    # 旧格式 holder + fencing 世代（末段纯数字）
    assert holder_instance_id(f"resume:{UUID16}:7") is None
    # 前缀不在白名单（外部写入者）
    assert holder_instance_id("other:a:b") is None
    assert holder_instance_id("other:a:b:c") is None


# plaita#155：旧格式守卫与 new_holder_token 的末段形状相撞——
# ``uuid.uuid4().hex[:16]`` 恰好全为十进制数字时（``P=(10/16)^15≈1/1153``；
# uuid4 把版本半字节钉在 '4'），``parts[2].isdigit()`` 为真、被当成
# ``{prefix}:{uuid}:{gen}`` 旧格式吞掉 ⇒ 概率性假红（首跑红/重跑绿）。
# 下面两条把该边界**确定性钉死**：全数字末段必须按新格式反解，真实旧格式
# （短十进制世代号）仍必须返回 None。
ALL_DECIMAL_UUID16 = "1111111111111111"


def test_holder_instance_id_all_decimal_hex_suffix_is_new_format():
    """#155 回归输入：16 位全数字末段是新格式 uuid，不得被旧格式守卫吞掉。"""
    tok = f"resume:{INSTANCE}:{ALL_DECIMAL_UUID16}"
    assert holder_instance_id(tok) == INSTANCE
    # fencing 档（holder + 世代）同款边界
    assert holder_instance_id(f"{tok}:42") == INSTANCE
    # 报告原文里的字面量（等同上面 INSTANCE 的取值）
    assert holder_instance_id("resume:vm-a-1a2b3c4d:1111111111111111") == INSTANCE
    # 三个白名单前缀都要能反解
    for prefix in ("start", "resume", "worker"):
        assert holder_instance_id(f"{prefix}:{INSTANCE}:{ALL_DECIMAL_UUID16}") == INSTANCE


def test_holder_instance_id_genuine_legacy_numeric_generation_is_none():
    """真实旧格式（末段是来自 Redis INCR 的短十进制世代号）仍返回 None。"""
    for gen in ("0", "1", "7", "42", "999999", "123456789012345"):
        assert holder_instance_id(f"resume:{UUID16}:{gen}") is None
    # 边界：16 位是**新格式**末段的定长，故 16 位纯数字按新格式反解（现实里
    # 的世代号要 10^15 次 INCR 才够长，不可达）；16 位以上仍按旧格式。
    assert holder_instance_id(f"resume:{UUID16}:1234567890123456") == UUID16
    assert holder_instance_id(f"resume:{UUID16}:12345678901234567") is None


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
    """start 路径的 holder 嵌入注册表 instance id（租约值可反解持有者）。"""
    pytest.importorskip("cachetools")

    class _SpyLease(RedisExecutionLease):
        holders: list = []

        def try_acquire(self, execution_id, holder, ttl_seconds):
            type(self).holders.append(holder)
            return super().try_acquire(execution_id, holder, ttl_seconds)

        def try_acquire_fenced(self, execution_id, holder, ttl_seconds):
            type(self).holders.append(holder)
            return super().try_acquire_fenced(execution_id, holder, ttl_seconds)

    class _WithInstance(FlowWorker):
        instance_id = INSTANCE

    redis = fakeredis.FakeRedis(decode_responses=True)
    flow_storage = MemoryFlowStorage()
    flow_storage.save_flow({
        "flow_id": "f1", "version": "1.0.0",
        "nodes": [
            {"id": "start", "type": "start", "next": "end"},
            {"id": "end", "type": "end", "output": "ok"},
        ],
    })
    worker = _WithInstance(
        MemoryExecutionStorage(), flow_storage,
        execution_lease=_SpyLease(redis), lease_ttl_seconds=60,
    )
    result = worker.start_flow("f1", params={}, execution_id="exec-holder")
    assert result.get("execution_id") == "exec-holder"
    holder = _SpyLease.holders[-1]
    assert holder_instance_id(holder) == INSTANCE


def test_consume_loop_gates_ack_on_liveness():
    """消费循环的租约冲突分支必须经存活闸分流（源码形状锁定）。"""
    import inspect

    src = inspect.getsource(RedisFlowWorker)
    assert "_lease_conflict_ack_safe(task.body)" in src
    # ack 只能发生在闸放行的分支内
    idx = src.find("_lease_conflict_ack_safe(task.body)")
    assert "queue.ack(task.message_id)" in src[idx:idx + 400]


def test_draining_holder_conflict_keeps_message_reclaimable_by_peer():
    """端到端契约（Redis 层）：draining 持有者的租约冲突 → 不 ack → 消息仍在
    PEL → 同组 peer 经 XAUTOCLAIM 可回收并续跑（2026-10-11 drain 丢消息修复）。

    claim_min_idle 在此用 0 代替（真实为 60s，见 claim_min_idle_ms）；断言的是
    「未被 ack 即可被回收」这一前提。
    """
    stream, group = "plaita:flow:queue:test", "plaita-workers"
    redis = fakeredis.FakeRedis(decode_responses=True)
    redis.set(LEASE_KEY, _lease_value(), ex=120)
    redis.set(
        f"{ServiceRegistry.REGISTRY_PREFIX}:flow_worker:{INSTANCE}",
        '{"instance_id": "%s", "status": "draining"}' % INSTANCE,
        ex=30,
    )
    redis.xgroup_create(stream, group, id="0", mkstream=True)
    redis.xadd(stream, {"data": '{"type": "resume", "execution_id": "exec-1"}'})
    _, msgs = redis.xreadgroup(group, INSTANCE, {stream: ">"}, count=1)[0]
    msg_id = msgs[0][0]

    # 持有者 drain 中：不 ack（与 worker 分支一致）
    assert _worker(redis)._lease_conflict_ack_safe(_body()) is False
    assert redis.xpending(stream, group)["pending"] == 1

    # 同组 peer 回收（claim_min_idle=0 仅为单测）
    _next, claimed, _deleted = redis.xautoclaim(
        stream, group, "peer-consumer", min_idle_time=0, start_id="0-0")
    assert [m[0] for m in claimed] == [msg_id]
