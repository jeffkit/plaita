"""plaita#57 红绿对照：死信守卫终态化写盘的租户命名空间（P0 数据丢失）。

缺陷：``_dead_letter_guard`` 只把**读段**（加载执行状态、读节点重试计数）
包在租户上下文里（set→reset），而「节点重试预算耗尽 → 终态化 error →
放行死信」的写盘（``_persist_state_or_raise``）落在 ``reset_current_tenant()``
之后——``TenantRoutingExecutionStorage`` 按 ContextVar 路由键空间，此刻已
回 default ⇒ 非 default 租户的执行 state 被写进 default 命名空间
（``plaita:execution:*`` 而非 ``plaita:{tenant_id}:execution:*``），
t1 自己的键仍停在 running（跨租户写 + 租户隔离破坏）。

同一分支的兜底总计数读取（``_node_retry_total_exhausted``）也在上下文外，
读的是 default 的 ``{ns}:execution:noderefetch:*``——t1 的兜底总上限判定
因此恒空（读侧同病）。

红 = 终态化写进 ``plaita:execution:exec-1`` 且进入 ``plaita:execution:index``
（default 键空间出现非 default 租户的 execution state）；
绿 = 写进 ``plaita:t1:execution:exec-1``、default 键空间零污染。

断言全部读**裸键**（fakeredis 原始 get/exists/zrange），不走 ContextVar
路由的 storage 读——避免断言自身被当前租户上下文污染。守卫的其余语义
（终态放行 / 租约在跳过 / 未达预算重入队 / 写盘失败保守跳过）由
``test_worker_node_retry.py`` 与本文件的 default 变体锚定，不在本单改动。
"""
import json

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.task_queue import StreamTask
from plaita.server.tenant_context import (
    TenantRoutingExecutionStorage,
    reset_current_tenant,
    set_current_tenant,
)
from plaita.storage.base import ExecutionState
from plaita.storage.memory import MemoryFlowStorage

DEFAULT_NS = "plaita"
T1_NS = "plaita:t1"
BUDGET = 5  # 与 DEFAULT_MAX_DELIVERIES 一致（_node_retry_budget 回退值）


def _worker(fake) -> RedisFlowWorker:
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue57-guard",
        execution_storage=TenantRoutingExecutionStorage(client=fake),
        flow_storage=MemoryFlowStorage(),
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


def _task(tenant_id) -> StreamTask:
    return StreamTask(
        message_id="m1",
        body={"type": "resume", "execution_id": "exec-1", "tenant_id": tenant_id,
              "resume_type": "event"},
        delivery_count=BUDGET,
    )


def _seed_running_in_tenant(fake, tenant_id: str) -> None:
    """在 tenant 的真实键空间预置 running 执行（经租户路由 storage 写入）。"""
    storage = TenantRoutingExecutionStorage(client=fake)
    state = ExecutionState(
        execution_id="exec-1",
        flow_id="f1",
        flow_version="1",
        status="running",
        context={"$LAST_NODE": "start", "$NODE": {"start": {"status": "success"}}},
    )
    token = set_current_tenant(tenant_id)
    try:
        assert storage.save_execution_state("exec-1", state) is True
    finally:
        reset_current_tenant(token)


class TestGuardFinalizeTenantNamespace:
    def test_t1_finalize_lands_in_t1_namespace_not_default(self):
        """非 default 租户死信终态化：state 必须落 ``plaita:t1`` 键空间，
        default 键空间（``plaita:execution:*`` + index）零污染。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _worker(fake)
        _seed_running_in_tenant(fake, "t1")
        fake.set(f"{T1_NS}:execution:noderetry:exec-1:a", str(BUDGET))

        assert worker._dead_letter_guard(_task("t1")) is True

        # 红/绿判据 1：default 键空间不出现 t1 的执行 state
        assert fake.exists(f"{DEFAULT_NS}:execution:exec-1") == 0
        assert "exec-1" not in (fake.zrange(f"{DEFAULT_NS}:execution:index", 0, -1) or [])
        # 红/绿判据 2：t1 键空间里是终态 error（裸键读，反序列化核对）
        raw = fake.get(f"{T1_NS}:execution:exec-1")
        assert raw is not None, "t1 键空间的执行 state 未被终态化"
        payload = json.loads(raw)
        assert payload["status"] == "error"
        assert payload["error"]["node_retries"] == BUDGET
        # 终态化分支不重入队（与既有 TestGuardBackstop 语义一致）
        assert fake.xlen("test:issue57-guard") == 0

    def test_t1_total_backstop_reads_own_namespace(self):
        """兜底总上限（noderefetch 键，NODE_RETRY_TOTAL_MAX=100）对该租户的
        键空间生效：t1 的 noderefetch 达限而单节点计数未达预算 ⇒ 也终态化，
        且落 t1 键空间（修复前这条判定读 default 键空间，对 t1 恒空）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _worker(fake)
        _seed_running_in_tenant(fake, "t1")
        fake.set(f"{T1_NS}:execution:noderetry:exec-1:a", "2")
        fake.set(f"{T1_NS}:execution:noderefetch:exec-1",
                 str(worker.NODE_RETRY_TOTAL_MAX))

        assert worker._dead_letter_guard(_task("t1")) is True

        assert fake.exists(f"{DEFAULT_NS}:execution:exec-1") == 0
        raw = fake.get(f"{T1_NS}:execution:exec-1")
        assert raw is not None
        assert json.loads(raw)["status"] == "error"
        assert fake.xlen("test:issue57-guard") == 0

    def test_t1_below_budget_reenqueues_unchanged(self):
        """未达预算：重入队语义不动——重入队 + t1 state 仍 running，
        default 键空间零污染（守卫其余分支不因本单漂移）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _worker(fake)
        _seed_running_in_tenant(fake, "t1")
        fake.set(f"{T1_NS}:execution:noderetry:exec-1:a", "2")

        assert worker._dead_letter_guard(_task("t1")) is True

        assert fake.xlen("test:issue57-guard") == 1
        raw = fake.get(f"{T1_NS}:execution:exec-1")
        assert raw is not None
        assert json.loads(raw)["status"] == "running"
        assert fake.exists(f"{DEFAULT_NS}:execution:exec-1") == 0


class TestGuardDefaultTenantUnchanged:
    def test_default_finalize_keeps_legacy_namespace(self):
        """default 租户行为不回归：经租户路由包装器仍落历史前缀
        ``plaita``（tenant_namespace 对 default/空映射回 LEGACY_NAMESPACE）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _worker(fake)
        _seed_running_in_tenant(fake, "default")
        fake.set(f"{DEFAULT_NS}:execution:noderetry:exec-1:a", str(BUDGET))

        assert worker._dead_letter_guard(_task("default")) is True

        raw = fake.get(f"{DEFAULT_NS}:execution:exec-1")
        assert raw is not None
        payload = json.loads(raw)
        assert payload["status"] == "error"
        assert payload["error"]["node_retries"] == BUDGET
        assert "exec-1" in (fake.zrange(f"{DEFAULT_NS}:execution:index", 0, -1) or [])
        # 不产生 t1 之外的租户键
        assert fake.exists(f"{T1_NS}:execution:exec-1") == 0
