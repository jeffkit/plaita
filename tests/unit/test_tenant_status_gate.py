"""停用租户闸（#27）：停用对运行面不再是 no-op。

背景（gap 单）：``set_tenant_status`` 只改 SQLite 一行，运行面三处都不看
租户状态——已签发会话继续全权访问、调度继续入队、挂起 run 继续 resume。
本文件钉住运行面两处闸：调度入队（``fire_schedule``）与 worker 派发
（``_dispatch_task``）；会话闸在 console 侧（tests/console/test_tenants.py）。

口径：Redis 停用集合缺席 = 无人停用（存量部署行为不变）；无 Redis client
一律放行（宽松优先，闸失效不得改变存量行为）；空/缺省租户 ID 视为 default。
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.services.schedule_service import SCHEDULES_KEY, fire_schedule
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.tenant_context import (
    DISABLED_TENANTS_KEY,
    is_tenant_disabled,
    set_tenant_disabled,
)

QUEUE = "plaita:flow:queue"


def _redis():
    return fakeredis.FakeRedis(decode_responses=True)


class TestDisabledTenantKey:
    def test_absent_key_or_missing_client_means_allowed(self):
        r = _redis()
        assert is_tenant_disabled(r, "acme") is False
        assert is_tenant_disabled(None, "acme") is False
        assert is_tenant_disabled(r, None) is False
        assert is_tenant_disabled(r, "") is False

    def test_toggle_round_trip_is_per_tenant(self):
        r = _redis()
        set_tenant_disabled(r, "acme", True)
        assert is_tenant_disabled(r, "acme") is True
        assert is_tenant_disabled(r, "default") is False
        set_tenant_disabled(r, "acme", False)
        assert is_tenant_disabled(r, "acme") is False

    def test_empty_tenant_id_is_default(self):
        r = _redis()
        set_tenant_disabled(r, "default", True)
        assert is_tenant_disabled(r, None) is True
        assert is_tenant_disabled(r, "") is True

    def test_set_without_client_or_tenant_id_is_noop(self):
        set_tenant_disabled(None, "acme", True)  # 本地档（无 Redis）不得抛
        r = _redis()
        set_tenant_disabled(r, "", True)  # 空 ID 不写集合
        assert r.smembers(DISABLED_TENANTS_KEY) == set()


def _schedule(tenant_id=None, schedule_id="s1"):
    schedule = {
        "schedule_id": schedule_id, "name": "n", "flow_id": "f",
        "cron": "* * * * *", "params": {}, "enabled": True,
    }
    if tenant_id is not None:
        schedule["tenant_id"] = tenant_id
    return schedule


class TestFireScheduleTenantGate:
    def test_disabled_tenant_not_enqueued(self):
        r = _redis()
        set_tenant_disabled(r, "acme", True)
        assert fire_schedule(r, _schedule("acme"), QUEUE) is None
        assert r.xlen(QUEUE) == 0
        # 未入队就不该留触发痕迹（历史/状态回写都不该发生）
        assert r.exists("plaita:schedule:fires:s1") == 0
        assert r.hget(SCHEDULES_KEY, "s1") is None

    def test_active_tenant_still_enqueued(self):
        r = _redis()
        set_tenant_disabled(r, "other", True)
        assert fire_schedule(r, _schedule("acme"), QUEUE) is not None
        assert r.xlen(QUEUE) == 1

    def test_missing_tenant_id_counts_as_default(self):
        """旧值（无 tenant_id）= default 租户——default 停用时同样拦下。"""
        r = _redis()
        set_tenant_disabled(r, "default", True)
        assert fire_schedule(r, _schedule(), QUEUE) is None
        assert r.xlen(QUEUE) == 0


def _worker(redis):
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/0",
        queue_name=QUEUE,
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        redis_client=redis,
        enable_registry=False,
    )


class TestWorkerTenantGate:
    def test_disabled_tenant_drops_start_and_resume(self):
        r = _redis()
        set_tenant_disabled(r, "acme", True)
        w = _worker(r)
        with patch.object(w, "start_flow") as start, patch.object(w, "resume_flow") as resume:
            w._dispatch_task(
                {"type": "start", "flow_id": "f", "params": {}, "tenant_id": "acme"}
            )
            w._dispatch_task({
                "type": "resume", "flow_id": "f", "execution_id": "e",
                "resume_type": "event", "data": {}, "tenant_id": "acme",
            })
        start.assert_not_called()
        resume.assert_not_called()

    def test_active_tenant_dispatched(self):
        r = _redis()
        set_tenant_disabled(r, "other", True)
        w = _worker(r)
        with patch.object(w, "start_flow") as start:
            w._dispatch_task(
                {"type": "start", "flow_id": "f", "params": {}, "tenant_id": "acme"}
            )
        start.assert_called_once()

    def test_missing_tenant_id_counts_as_default(self):
        r = _redis()
        set_tenant_disabled(r, "default", True)
        w = _worker(r)
        with patch.object(w, "start_flow") as start:
            w._dispatch_task({"type": "start", "flow_id": "f", "params": {}})
        start.assert_not_called()

    def test_skeleton_worker_without_redis_client_allows(self):
        """以 __new__ 构造的骨架实例（无 redis_client）放行——不得 AttributeError。"""
        w = RedisFlowWorker.__new__(RedisFlowWorker)
        with patch.object(RedisFlowWorker, "start_flow") as start:
            w._dispatch_task({"type": "start", "flow_id": "f", "params": {}})
        start.assert_called_once()
