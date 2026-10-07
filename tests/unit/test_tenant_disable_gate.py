"""租户停用闸（plaita#27）：停用租户的 worker 任务不再派发。

- ``plaita.tenant_status`` 的发布/读取语义（缺失/异常 = 未停用，fail-open）
- ``RedisFlowWorker._dispatch_task`` 对 start/resume 一律跳过（消息被 ack 丢弃）
"""
from unittest.mock import MagicMock

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("lupa")
pytest.importorskip("cachetools")
pytest.importorskip("redis")

import fakeredis

from plaita.server.flow_worker import RedisFlowWorker
from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage
from plaita.tenant_status import is_tenant_disabled, publish_tenant_status


def _worker(fake) -> RedisFlowWorker:
    return RedisFlowWorker(
        redis_url="redis://localhost:6379/15",
        queue_name="test:tenant-gate",
        execution_storage=MemoryExecutionStorage(),
        flow_storage=MemoryFlowStorage(),
        redis_client=fake,
        enable_registry=False,
        enable_redis_logging=False,
    )


class TestTenantStatusStore:
    def test_disabled_and_active_roundtrip(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        assert is_tenant_disabled(fake, "acme") is False
        publish_tenant_status(fake, "acme", "disabled")
        assert is_tenant_disabled(fake, "acme") is True
        publish_tenant_status(fake, "acme", "active")
        assert is_tenant_disabled(fake, "acme") is False

    def test_missing_redis_and_read_failure_fail_open(self):
        assert is_tenant_disabled(None, "acme") is False
        assert is_tenant_disabled(fakeredis.FakeRedis(), "") is False

        class Broken:
            def hget(self, *args, **kwargs):
                raise RuntimeError("redis down")

        assert is_tenant_disabled(Broken(), "acme") is False


class TestDispatchTenantGate:
    def _prepared(self):
        fake = fakeredis.FakeRedis(decode_responses=True)
        worker = _worker(fake)
        worker.start_flow = MagicMock(return_value={"execution_id": "e1"})
        worker.resume_flow = MagicMock(return_value={"execution_id": "e1"})
        return fake, worker

    def test_start_and_resume_skipped_for_disabled_tenant(self):
        fake, worker = self._prepared()
        publish_tenant_status(fake, "acme", "disabled")

        worker._dispatch_task({"type": "start", "tenant_id": "acme", "flow_id": "f1"})
        worker._dispatch_task({
            "type": "resume", "tenant_id": "acme", "flow_id": "f1", "execution_id": "e1",
        })

        worker.start_flow.assert_not_called()
        worker.resume_flow.assert_not_called()

    def test_other_tenant_still_dispatched(self):
        fake, worker = self._prepared()
        publish_tenant_status(fake, "acme", "disabled")

        worker._dispatch_task({"type": "start", "tenant_id": "other", "flow_id": "f1"})

        worker.start_flow.assert_called_once()
