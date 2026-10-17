"""plaita#28 守卫：每租户 start 准入配额（最小公平性原语）。

缺陷（issue #28）：任务队列单键单 stream FIFO + worker 串行消费，无任何按
租户的并发上限/准入——一个租户可无限入队并占满 worker，其他租户无限期排队；
没有拒绝、降级路径，也没有「每租户并发 run 数」这一最基本的配额概念。

最小修复面（issue 建议的「最小可行」）：
- 配额原语 ``plaita.server.tenant_quota``：每租户 running 名额（ZSET 成员=
  execution_id，score=过期时刻——崩溃自愈，无需显式清账的兜底）；
  上限解析 console/worker 共用：per-tenant HASH 覆盖 > env 全局默认 > 不限。
- console start 入口：超限 **429**（入口拒绝，积压根本不形成）。
- worker start 消息准入：非入口生产方（无名额）超限时**延迟**（重投尾部+
  ack，不烧 delivery、不死信），有界后放行（存活性优先于软配额）。
- 执行终态释放名额（单收口 ``_persist_state_or_raise``）；suspended 不释放。
- fail-open：未配额租户 / 无 redis / 无 execution_id / redis 故障 → 放行，
  零行为变化。回滚开关 PLAITA_DISABLE_TENANT_QUOTA=1。
"""
from __future__ import annotations

import threading
import time

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker, TenantQuotaDeferred
from plaita.server.tenant_quota import (
    DEFAULT_RUNNING_TTL_SECONDS,
    TENANT_QUOTA_KEY,
    publish_tenant_quota,
    release_tenant_start,
    tenant_max_running,
    tenant_running_count,
    try_admit_tenant_start,
)
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


def _make_worker(fake) -> RedisFlowWorker:
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue28-quota",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
        lease_ttl_seconds=60,
    )


def _start_msg(tenant="acme", execution_id="e1", **extra):
    msg = {"type": "start", "flow_id": "f1", "tenant_id": tenant}
    if execution_id is not None:
        msg["execution_id"] = execution_id
    msg.update(extra)
    return msg


# ============ 配额原语 ============


class TestQuotaPrimitive:
    def test_fail_open_without_cap(self):
        """未配额租户（无 HASH 记录、无 env）→ 放行，且不写任何键（零行为变化）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        assert tenant_max_running(fake, "acme") is None
        assert try_admit_tenant_start(fake, "acme", "e1") is True
        assert fake.keys("plaita:tenant_running:*") == []

    def test_publish_and_resolve_cap_hash_overrides_env(self, monkeypatch):
        fake = fakeredis.FakeRedis(decode_responses=True)
        monkeypatch.setenv("PLAITA_TENANT_MAX_RUNNING", "5")
        assert tenant_max_running(fake, "acme") == 5  # env 全局默认
        publish_tenant_quota(fake, "acme", 2)
        assert tenant_max_running(fake, "acme") == 2  # per-tenant 覆盖
        assert tenant_max_running(fake, "beta") == 5  # 其他租户仍用 env
        publish_tenant_quota(fake, "acme", 0)  # 0 = 取消配额（不限）
        assert tenant_max_running(fake, "acme") is None
        publish_tenant_quota(fake, "acme", None)  # 删记录 → 回落 env
        assert tenant_max_running(fake, "acme") == 5
        assert fake.hget(TENANT_QUOTA_KEY, "acme") is None

    def test_invalid_env_and_values_fail_open(self, monkeypatch):
        fake = fakeredis.FakeRedis(decode_responses=True)
        monkeypatch.setenv("PLAITA_TENANT_MAX_RUNNING", "not-a-number")
        assert tenant_max_running(fake, "acme") is None
        monkeypatch.setenv("PLAITA_TENANT_MAX_RUNNING", "-3")
        assert tenant_max_running(fake, "acme") is None

    def test_admit_under_cap_and_count(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        publish_tenant_quota(fake, "acme", 2)
        assert try_admit_tenant_start(fake, "acme", "e1") is True
        assert try_admit_tenant_start(fake, "acme", "e2") is True
        assert tenant_running_count(fake, "acme") == 2
        # 满员：第三家被拒
        assert try_admit_tenant_start(fake, "acme", "e3") is False
        assert tenant_running_count(fake, "acme") == 2  # 拒绝者不占名额

    def test_admit_same_execution_is_idempotent(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "e1") is True
        # 同一 execution 再次 admit（消息重投）不重复占名额、不被拒
        assert try_admit_tenant_start(fake, "acme", "e1") is True
        assert tenant_running_count(fake, "acme") == 1

    def test_release_frees_slot(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "e1") is True
        assert try_admit_tenant_start(fake, "acme", "e2") is False
        release_tenant_start(fake, "acme", "e1")
        assert try_admit_tenant_start(fake, "acme", "e2") is True

    def test_per_tenant_isolation(self):
        """acme 满员绝不拦 beta——这是配额原语的公平性本体。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "a1") is True
        assert try_admit_tenant_start(fake, "acme", "a2") is False
        for i in range(5):
            assert try_admit_tenant_start(fake, "beta", f"b{i}") is True

    def test_expired_entries_self_heal(self):
        """崩溃自愈：名额条目带 TTL，过期后自动让位（无需显式清账）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "e1") is True
        # 模拟条目过期：把 score 拨回过去
        key = "plaita:tenant_running:acme"
        fake.zadd(key, {"e1": time.time() - 1})
        assert tenant_running_count(fake, "acme") == 0
        assert try_admit_tenant_start(fake, "acme", "e2") is True

    def test_entry_ttl_from_env(self, monkeypatch):
        # 条目 TTL 有 1s 下限（防误配成 0 让配额永不生效）——测试用最小合法值
        monkeypatch.setenv("PLAITA_TENANT_RUNNING_TTL_SECONDS", "1")
        fake = fakeredis.FakeRedis(decode_responses=True)
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "e1") is True
        assert try_admit_tenant_start(fake, "acme", "e2") is False
        time.sleep(1.3)
        assert try_admit_tenant_start(fake, "acme", "e2") is True
        monkeypatch.delenv("PLAITA_TENANT_RUNNING_TTL_SECONDS")
        assert DEFAULT_RUNNING_TTL_SECONDS > 0

    def test_fail_open_on_redis_errors(self, monkeypatch):
        class Boom:
            def __getattr__(self, name):
                raise RuntimeError("redis down")

        fake = Boom()
        # 读失败/执行失败一律放行（fail-open），绝不把配额闸变成全局拒绝闸
        assert try_admit_tenant_start(fake, "acme", "e1") is True
        assert tenant_max_running(fake, "acme") is None

    def test_no_redis_and_no_execution_id_fail_open(self):
        assert try_admit_tenant_start(None, "acme", "e1") is True
        fake = fakeredis.FakeRedis(decode_responses=True)
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", None) is True
        assert try_admit_tenant_start(fake, "acme", "") is True


# ============ worker 侧准入 ============


class TestWorkerStartGate:
    def test_over_quota_foreign_start_raises_defer(self):
        """非入口生产方（消息无名额）超限 start：派发前被闸下（TenantQuotaDeferred）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "slot-holder") is True
        with pytest.raises(TenantQuotaDeferred):
            worker._dispatch_task(_start_msg(execution_id="e1"))

    def test_under_quota_start_executes(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        publish_tenant_quota(fake, "acme", 2)
        calls = []
        worker.start_flow = lambda *a, **k: calls.append("start")
        worker._dispatch_task(_start_msg(execution_id="e1"))
        assert calls == ["start"]
        assert tenant_running_count(fake, "acme") == 1  # worker 侧准入占名额

    def test_console_admitted_start_passes_worker_gate(self):
        """console 入口已占名额的 start 抵达 worker：照常执行（不二次 admit）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "e1") is True  # console 429 前已占
        assert tenant_running_count(fake, "acme") == 1
        calls = []
        worker.start_flow = lambda *a, **k: calls.append("start")
        worker._dispatch_task(_start_msg(execution_id="e1"))
        assert calls == ["start"]
        assert tenant_running_count(fake, "acme") == 1  # 未重复占名额

    def test_tenant_isolation_at_worker_gate(self):
        """acme 满员不拦 beta 的 start（worker 闸同样按租户隔离）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "slot-holder") is True
        calls = []
        worker.start_flow = lambda *a, **k: calls.append(a[1] if len(a) > 1 else "start")
        worker._dispatch_task(_start_msg(tenant="beta", execution_id="b1"))
        assert len(calls) == 1

    def test_resume_bypasses_gate(self):
        """resume 是救援流量：超限租户的 resume 照常推进（不受 start 配额拦）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "slot-holder") is True
        calls = []
        worker.resume_flow = lambda *a, **k: calls.append("resume")
        worker._dispatch_task({"type": "resume", "tenant_id": "acme", "execution_id": "e1"})
        assert calls == ["resume"]

    def test_message_without_execution_id_fail_open(self):
        """无 execution_id 的旧生产方消息：不闸（fail-open，存量行为零变化）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "slot-holder") is True
        calls = []
        worker.start_flow = lambda *a, **k: calls.append("start")
        worker._dispatch_task(_start_msg(execution_id=None))
        assert calls == ["start"]

    def test_disable_switch_restores_baseline(self, monkeypatch):
        """回滚开关 PLAITA_DISABLE_TENANT_QUOTA=1：闸完全旁路。"""
        monkeypatch.setenv("PLAITA_DISABLE_TENANT_QUOTA", "1")
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "slot-holder") is True
        calls = []
        worker.start_flow = lambda *a, **k: calls.append("start")
        worker._dispatch_task(_start_msg(execution_id="e1"))
        assert calls == ["start"]


# ============ worker 侧延迟（排队）与放行 ============


class TestWorkerDefer:
    def _task(self, body, delivery_count=1):
        from plaita.server.task_queue import StreamTask

        return StreamTask(message_id="m1", body=body, delivery_count=delivery_count)

    def test_defer_requeues_to_tail_and_acks(self):
        """超限 start：不执行、同体新副本重投尾部（带 quota_defers 计数）、原条 ack。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        queue = worker._get_task_queue()
        queue.ensure_group()
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "slot-holder") is True

        msg_id = queue.enqueue(_start_msg(execution_id="e1"))
        task = queue.read(block_ms=100)
        assert task is not None and task.message_id == msg_id

        with pytest.raises(TenantQuotaDeferred):
            worker._dispatch_task(task.body)
        assert worker._defer_over_quota_start(task, queue, TenantQuotaDeferred("quota full")) is True

        # 原条已 ack（PEL 无残留），新副本在 stream 尾部且带递增计数
        copy = queue.read(block_ms=100)
        assert copy is not None, "重投副本应在 stream 尾部"
        assert copy.body.get("quota_defers") == 1
        assert copy.body.get("execution_id") == "e1"

    def test_defer_budget_then_bypass_for_liveness(self):
        """延迟有界：达上限后带 quota_bypass 放行（存活性 > 软配额）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        queue = worker._get_task_queue()
        queue.ensure_group()

        body = _start_msg(execution_id="e1", quota_defers=10_000)
        body["quota_defers"] = worker._quota_defer_max()
        task = self._task(body)
        assert worker._defer_over_quota_start(task, queue, TenantQuotaDeferred("quota full")) is True
        copy = queue.read(block_ms=100)
        assert copy is not None
        assert copy.body.get("quota_bypass") is True

        # bypass 消息抵达 worker：超限也放行（不再 defer）
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "slot-holder") is True
        calls = []
        worker.start_flow = lambda *a, **k: calls.append("start")
        worker._dispatch_task(copy.body)
        assert calls == ["start"]

    def test_consume_loop_defers_then_admits_after_release(self, monkeypatch):
        """端到端：超限 → defer（start 未执行）→ 释放名额 → 副本获准执行。

        把延迟预算调大（fakeredis 往返是微秒级，默认 64 会在断言窗内烧完触发
        bypass 放行——那是另一条守卫的职责），本测试只验证「排队→获准」主线。
        """
        monkeypatch.setenv("PLAITA_TENANT_QUOTA_DEFER_MAX", "1000000")
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        queue = worker._get_task_queue()
        queue.ensure_group()
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "slot-holder") is True

        started = threading.Event()
        worker.start_flow = lambda *a, **k: started.set()

        queue.enqueue(_start_msg(execution_id="e1"))
        worker._running = True
        loop = threading.Thread(target=worker._consume_loop, args=(queue,), daemon=True)
        loop.start()
        try:
            # 名额未释放：start 不得执行（defer 重投尾部排队）
            assert not started.wait(timeout=1.0), "超限 start 被立即执行了（闸未生效）"

            # 名额释放（模拟持有者终态）→ 副本获准执行
            release_tenant_start(fake, "acme", "slot-holder")
            assert started.wait(timeout=5.0), "释放名额后重投副本应获准执行"
        finally:
            worker._running = False
            loop.join(timeout=5)
        assert not loop.is_alive()


# ============ 终态释放收口 ============


class TestReleaseOnTerminal:
    def _state(self, status, tenant="acme", execution_id="e1"):
        from plaita.storage.base import ExecutionState

        return ExecutionState(
            execution_id=execution_id,
            flow_id="f1",
            tenant_id=tenant,
            context={},
            status=status,
            start_time="2026-10-16T00:00:00",
            invoker="worker",
        )

    def test_terminal_status_releases_slot(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        publish_tenant_quota(fake, "acme", 1)
        for status in ("completed", "error", "cancelled"):
            assert try_admit_tenant_start(fake, "acme", "e1") is True
            worker._persist_state_or_raise("e1", self._state(status), "test")
            assert tenant_running_count(fake, "acme") == 0, status

    def test_suspended_keeps_slot(self):
        """挂起执行仍占名额（它还在被 admitted 的生命周期内，resume 复用）。"""
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "e1") is True
        worker._persist_state_or_raise("e1", self._state("suspended"), "test")
        assert tenant_running_count(fake, "acme") == 1

    def test_release_uses_state_tenant_not_contextvar(self):
        """释放按 state.tenant_id 路由——看门狗等无租户上下文线程也放对账。"""
        from plaita.server.tenant_context import set_current_tenant, reset_current_tenant

        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        publish_tenant_quota(fake, "acme", 1)
        publish_tenant_quota(fake, "beta", 1)
        assert try_admit_tenant_start(fake, "acme", "e1") is True
        token = set_current_tenant("beta")  # 故意错的上下文
        try:
            worker._persist_state_or_raise("e1", self._state("completed", tenant="acme"), "test")
        finally:
            reset_current_tenant(token)
        assert tenant_running_count(fake, "acme") == 0
        assert tenant_running_count(fake, "beta") == 0


# ============ schedule 入口准入 ============


class TestScheduleAdmission:
    """schedule 风暴是 issue #28 点名的积压来源：入口预铸 id + 占名额。"""

    def _schedule(self, tenant="acme"):
        return {
            "schedule_id": "s1",
            "name": "n",
            "flow_id": "f1",
            "version": "1",
            "cron": "* * * * *",
            "params": {},
            "tenant_id": tenant,
            "enabled": True,
        }

    def test_trigger_under_cap_precasts_execution_id_and_admits(self):
        import json as _json

        from plaita.server.services.schedule_service import fire_schedule

        fake = fakeredis.FakeRedis(decode_responses=True)
        publish_tenant_quota(fake, "acme", 5)  # 配了上限才占名额（fail-open 不跟踪）
        msg_id = fire_schedule(
            fake, self._schedule(), "test:issue28-sched", trigger_kind="manual"
        )
        assert msg_id
        entries = fake.xrange("test:issue28-sched")
        body = _json.loads(entries[-1][1]["payload"])
        assert body.get("execution_id"), "调度消息应预铸 execution_id（与 console 形态对齐）"
        assert tenant_running_count(fake, "acme") == 1
        # 名额成员就是消息携带的 execution_id（worker 终态释放按同一路由）
        assert fake.zrange("plaita:tenant_running:acme", 0, -1) == [body["execution_id"]]

    def test_trigger_over_cap_skips_enqueue(self):
        from plaita.server.services.schedule_service import fire_schedule

        fake = fakeredis.FakeRedis(decode_responses=True)
        stream = "test:issue28-sched-over"
        publish_tenant_quota(fake, "acme", 1)
        assert try_admit_tenant_start(fake, "acme", "slot-holder") is True
        msg_id = fire_schedule(fake, self._schedule(), stream, trigger_kind="manual")
        assert msg_id is None, "超限触发不得入队（等下一次 cron 到期自然重试）"
        assert fake.xlen(stream) == 0

    def test_trigger_fail_open_without_cap(self):
        from plaita.server.services.schedule_service import fire_schedule

        fake = fakeredis.FakeRedis(decode_responses=True)
        msg_id = fire_schedule(
            fake, self._schedule(), "test:issue28-sched-open", trigger_kind="manual"
        )
        assert msg_id, "未配额租户零行为变化（照常入队）"
