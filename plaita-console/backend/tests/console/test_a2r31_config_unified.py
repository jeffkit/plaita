"""A2R31 console 侧回归：统一 queue resolver / 启动漂移预检 / 纪元写入。"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("fakeredis")

import fakeredis

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from plaita.runtime_config import ConfigError  # noqa: E402


class TestConsoleQueueUnified:
    def test_empty_env_rejected_by_all_read_points(self, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", "")
        monkeypatch.delenv("PLAITA_QUEUE_NAME", raising=False)
        monkeypatch.delenv("QUEUE_NAME", raising=False)

        from api import executions
        from plaita.server.capacity import dispatch_queue_from_env
        from plaita.server.services.schedule_service import resolve_dispatch_queue_name

        with pytest.raises(ConfigError):
            executions._task_queue_name()
        with pytest.raises(ConfigError):
            dispatch_queue_from_env()
        with pytest.raises(ConfigError):
            resolve_dispatch_queue_name()

    def test_single_nonempty_value_shared(self, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_TASK_QUEUE", "plaita:flow:queue:v2")
        from api import executions
        from plaita.server.capacity import dispatch_queue_from_env
        from plaita.server.services.schedule_service import resolve_dispatch_queue_name

        assert executions._task_queue_name() == "plaita:flow:queue:v2"
        assert dispatch_queue_from_env() == "plaita:flow:queue:v2"
        assert resolve_dispatch_queue_name() == "plaita:flow:queue:v2"


class TestConfigDriftPreflight:
    def _seed(self, redis, queue_name="plaita:flow:queue:v9"):
        info = {
            "instance_id": "w1",
            "service_type": "flow_worker",
            "status": "running",
            "metadata": {
                "queue_name": queue_name,
                "redis_host": "other-host",
                "redis_port": 6379,
                "redis_db": 9,
                "dlq_key": "custom:dead",
            },
        }
        redis.set("plaita:registry:flow_worker:w1", json.dumps(info))

    def test_drift_fields_reported(self, monkeypatch):
        monkeypatch.delenv("PLAITA_CONSOLE_TASK_QUEUE", raising=False)
        monkeypatch.delenv("PLAITA_REDIS_URL", raising=False)
        monkeypatch.delenv("REDIS_URL", raising=False)
        monkeypatch.delenv("PLAITA_CONSOLE_REDIS_URL", raising=False)
        from services import config_drift

        redis = fakeredis.FakeRedis(decode_responses=True)
        self._seed(redis)
        result = config_drift.preflight_config_drift(redis)
        fields = {item["field"] for item in result["drift"]}
        assert "queue_name" in fields
        assert "redis_target" in fields
        assert "dlq_key" in fields
        assert result["workers"] == 1

    def test_no_workers_is_info_not_warning(self):
        from services import config_drift

        redis = fakeredis.FakeRedis(decode_responses=True)
        result = config_drift.preflight_config_drift(redis)
        assert result["workers"] == 0
        assert result["drift"] == []
        assert result["status"] == "ok"


class TestTenantStatusEpochWrite:
    def test_epoch_written_with_status(self, monkeypatch):
        monkeypatch.setenv("PLAITA_DEPLOYMENT_EPOCH", "200")
        from services import tenants_svc
        from plaita.server.tenant_status import TENANT_STATUS_EPOCH_KEY

        redis = fakeredis.FakeRedis(decode_responses=True)
        tenants_svc._publish_status(redis, "acme", "disabled")
        assert redis.get(TENANT_STATUS_EPOCH_KEY) == "200"
        assert redis.hget("plaita:tenant_status", "acme") == "disabled"

    def test_empty_epoch_is_not_written(self, monkeypatch):
        monkeypatch.setenv("PLAITA_DEPLOYMENT_EPOCH", "")
        from services import tenants_svc
        from plaita.server.tenant_status import TENANT_STATUS_EPOCH_KEY

        redis = fakeredis.FakeRedis(decode_responses=True)
        tenants_svc._publish_status(redis, "acme", "active")
        assert redis.get(TENANT_STATUS_EPOCH_KEY) is None


class TestRegistryDlqCollection:
    def test_custom_dlq_from_registry_enters_cleanup_set(self):
        from services import tenants_svc

        redis = fakeredis.FakeRedis(decode_responses=True)
        redis.set(
            "plaita:registry:flow_worker:w1",
            json.dumps({
                "instance_id": "w1",
                "metadata": {"dlq_key": "custom:dead"},
            }),
        )
        names = tenants_svc._tenant_dlq_names(redis)
        assert "custom:dead" in names


class TestStrictConsoleBooleans:
    def test_debug_accepts_one(self, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_DEBUG", "1")
        from config import get_settings

        assert get_settings().debug is True

    def test_debug_unknown_is_false_with_warning(self, monkeypatch, caplog):
        monkeypatch.setenv("PLAITA_CONSOLE_DEBUG", "bogus")
        from config import get_settings

        with caplog.at_level("WARNING"):
            assert get_settings().debug is False
        assert "PLAITA_CONSOLE_DEBUG" in caplog.text

    def test_allow_insecure_admin_unknown_fail_closed(self, monkeypatch):
        monkeypatch.setenv("PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN", "bogus")
        from config import get_settings

        assert get_settings().allow_insecure_admin is False
