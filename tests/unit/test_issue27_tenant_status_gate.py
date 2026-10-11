"""plaita#27：停用租户必须对**运行面**生效，而不只是下回切换租户时拦一下。

- ``FlowWorker._dispatch_task``：停用租户的 start/resume 一律丢弃（ack），
  否则已入队的调度消息、挂起执行的 resume 会照跑。
- ``tenant_status`` 共享视图：缺失/无客户端 = active（fail-open），
  disabled 才拦。
"""
import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.server.tenant_status import (
    TENANT_STATUS_KEY,
    clear_tenant_status,
    publish_tenant_status,
    tenant_is_active,
)
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage


def _make_worker(fake) -> RedisFlowWorker:
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:issue27-tenant-gate",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
        lease_ttl_seconds=60,
    )


class TestTenantStatusView:
    def test_missing_record_is_active(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        assert tenant_is_active(fake, "acme") is True
        assert tenant_is_active(None, "acme") is True  # 无 Redis = fail-open
        assert tenant_is_active(fake, None) is True

    def test_disabled_and_cleared(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        publish_tenant_status(fake, "acme", "disabled")
        assert fake.hget(TENANT_STATUS_KEY, "acme") == "disabled"
        assert tenant_is_active(fake, "acme") is False
        publish_tenant_status(fake, "acme", "active")
        assert tenant_is_active(fake, "acme") is True
        publish_tenant_status(fake, "acme", "disabled")
        clear_tenant_status(fake, "acme")
        assert tenant_is_active(fake, "acme") is True


class TestDispatchDropsDisabledTenant:
    def test_start_and_resume_dropped(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        calls = []
        worker.start_flow = lambda *a, **k: calls.append(("start", a, k))
        worker.resume_flow = lambda *a, **k: calls.append(("resume", a, k))

        publish_tenant_status(fake, "acme", "disabled")
        worker._dispatch_task({"type": "start", "tenant_id": "acme", "flow_id": "f1"})
        worker._dispatch_task(
            {"type": "resume", "tenant_id": "acme", "execution_id": "e1"}
        )
        assert calls == [], "停用租户的 start/resume 必须丢弃"

        # 未发布状态的租户 / default 不受影响（fail-open）
        worker._dispatch_task({"type": "start", "tenant_id": "default", "flow_id": "f1"})
        assert [c[0] for c in calls] == ["start"]

    def test_reenabled_tenant_runs_again(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _make_worker(fake)
        calls = []
        worker.start_flow = lambda *a, **k: calls.append("start")

        publish_tenant_status(fake, "acme", "disabled")
        worker._dispatch_task({"type": "start", "tenant_id": "acme", "flow_id": "f1"})
        assert calls == []

        publish_tenant_status(fake, "acme", "active")
        worker._dispatch_task({"type": "start", "tenant_id": "acme", "flow_id": "f1"})
        assert calls == ["start"]
