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
