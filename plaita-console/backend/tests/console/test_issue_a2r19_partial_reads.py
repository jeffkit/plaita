"""A2R19-04：只读可观测面不得把部分 Redis 故障折叠成 200 零值/缺行。

缺陷基线（台账 r19 §2）：``api/metrics.py`` 的 ``_xlen``/``_pending`` 吞异常
返回 0 且只 DEBUG；``_registry_metrics`` 扫描失败返回空表；``events``/``logs``
逐键 ``continue`` 无日志。部分命令失败（ACL、集群单槽）时端点 200 + 零值/缺行，
默认 INFO 级别完全不可见——值班会把「读不到」当成「真的没有」。

修复口径：
- metrics：``plaita_metrics_scrape_degraded`` + ``plaita_metrics_failed_commands_total``
  具名失败计数，WARNING（60s 限流）；全断仍 500（fail-closed 不变）。
- events/logs/订阅：响应带 ``degraded``/``skipped``，并打 WARNING。

负控为修复前实现（无 degraded 字段、无失败计数）。
"""
from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path

import fakeredis
import pytest
from fastapi import Depends, FastAPI
from starlette.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from api import events as events_api  # noqa: E402
from api import logs as logs_api  # noqa: E402
from api import metrics as metrics_api  # noqa: E402
from auth import require_auth  # noqa: E402

ADMIN_KEY = "a2r19-admin-key"


class _FaultyRedis:
    """fakeredis 代理：按命令/键注入「单命令失败」（模拟 ACL/单槽错误）。"""

    def __init__(self, inner):
        self._inner = inner
        self.fail_key_commands: set[str] = set()
        self.fail_keys: set[str] = set()
        self.fail_scan_patterns: set[str] = set()

    @staticmethod
    def _key(args, kwargs):
        if args:
            return args[0]
        return kwargs.get("name") or kwargs.get("key")

    def _maybe_fail(self, command, key):
        if command in self.fail_key_commands or (key and key in self.fail_keys):
            raise RuntimeError(f"simulated {command} failure for {key!r}")

    def __getattr__(self, name):
        attr = getattr(self._inner, name)

        if name == "keys":
            def keys(pattern="*", **kwargs):
                if any(p in str(pattern) for p in self.fail_scan_patterns):
                    raise RuntimeError(f"simulated KEYS failure for {pattern!r}")
                return attr(pattern, **kwargs)
            return keys

        if name == "scan":
            def scan(*args, **kwargs):
                # B2 r44 起读面走有界 SCAN（不再 KEYS），故障注入同样覆盖 SCAN。
                pattern = kwargs.get("match", args[1] if len(args) > 1 else "*")
                if any(p in str(pattern) for p in self.fail_scan_patterns):
                    raise RuntimeError(f"simulated SCAN failure for {pattern!r}")
                return attr(*args, **kwargs)
            return scan

        if name in {"xlen", "xpending", "type", "get", "xrevrange", "xrange"}:
            def with_fault(*args, **kwargs):
                self._maybe_fail(name, self._key(args, kwargs))
                return attr(*args, **kwargs)
            return with_fault

        return attr


def _settings():
    from config import Settings

    s = Settings()
    s.console_env = "dev"
    s.admin_api_key = ADMIN_KEY
    s.allow_insecure_admin = False
    return s


@pytest.fixture()
def redis():
    return _FaultyRedis(fakeredis.FakeRedis(decode_responses=True))


@pytest.fixture()
def client(redis, monkeypatch):
    monkeypatch.setattr("config.get_settings", lambda: _settings())
    monkeypatch.setattr("auth.get_settings", lambda: _settings())
    app = FastAPI()
    app.state.redis = redis
    app.state.local_mode = False
    for module in (events_api, logs_api, metrics_api):
        app.include_router(
            module.router, prefix="/api", dependencies=[Depends(require_auth)]
        )
    return TestClient(app)


def _headers():
    return {"X-Admin-API-Key": ADMIN_KEY}


def _event_record(event_id: str) -> dict:
    return {
        "event_id": event_id,
        "event_type": "approval",
        "data": {"ok": True},
        "timestamp": 1.0,
    }


def _log_record(instance: str, message: str, context: str = "{}") -> dict:
    return {
        "timestamp": "2026-10-19T10:00:00",
        "level": "INFO",
        "service_type": "flow_worker",
        "instance_id": instance,
        "tenant_id": "default",
        "message": message,
        "context": context,
    }


class TestMetricsDegradation:
    def test_command_failure_marks_scrape_degraded(self, client, redis, monkeypatch, caplog):
        monkeypatch.setattr(metrics_api, "_last_degraded_warn_at", 0.0)
        redis._inner.set("plaita:registry:flow_worker:w1", "{}")
        redis.fail_key_commands = {"xlen"}

        with caplog.at_level(logging.WARNING, logger=metrics_api.__name__):
            resp = client.get("/api/metrics", headers=_headers())

        assert resp.status_code == 200
        body = resp.text
        assert "plaita_metrics_scrape_degraded 1" in body
        assert 'plaita_metrics_failed_commands_total{command="xlen"}' in body
        # 零值仍然导出（不打崩页面），但已被标为不可信
        assert "plaita_queue_stream_length{" in body
        assert any("集群指标采集降级" in rec.message for rec in caplog.records)

    def test_registry_scan_failure_is_distinguishable_from_zero_workers(
        self, client, redis, monkeypatch
    ):
        monkeypatch.setattr(metrics_api, "_last_degraded_warn_at", 0.0)
        redis._inner.set("plaita:registry:flow_worker:w1", "{}")
        redis.fail_scan_patterns = {"registry"}

        body = client.get("/api/metrics", headers=_headers()).text

        assert "plaita_workers_registered_total 0" in body
        assert "plaita_metrics_scrape_degraded 1" in body
        assert 'plaita_metrics_failed_commands_total{command="registry_scan"} 1' in body

    def test_healthy_scrape_has_no_degraded_marker(self, client, redis):
        redis._inner.set("plaita:registry:flow_worker:w1", "{}")
        body = client.get("/api/metrics", headers=_headers()).text
        assert "plaita_metrics_scrape_degraded 0" in body
        assert "plaita_metrics_failed_commands_total" not in body

    def test_healthy_scrape_keeps_marker_clear_and_warn_debounced(
        self, client, redis, monkeypatch, caplog
    ):
        """锁定 r44(S5)/r49(A2R19-04) 两版合并接缝：

        - 健康抓取（无命令失败）只导出 marker=0，不粘滞、不告警、不消耗去抖窗口；
        - 命令失败才置 1，且 WARNING 在 60s 窗口内只发一次（`_last_degraded_warn_at`）；
        - 失败恢复后的下一次健康抓取立即回到 0 且清空失败计数。
        """
        def degraded_warnings():
            return [r for r in caplog.records if "集群指标采集降级" in r.message]

        monkeypatch.setattr(metrics_api, "_last_degraded_warn_at", 0.0)
        redis._inner.set("plaita:registry:flow_worker:w1", "{}")

        with caplog.at_level(logging.WARNING, logger=metrics_api.__name__):
            healthy = client.get("/api/metrics", headers=_headers()).text
            assert "plaita_metrics_scrape_degraded 0" in healthy
            assert "plaita_metrics_failed_commands_total" not in healthy
            assert metrics_api._last_degraded_warn_at == 0.0
            assert degraded_warnings() == []

            redis.fail_key_commands = {"xlen"}
            first = client.get("/api/metrics", headers=_headers()).text
            assert "plaita_metrics_scrape_degraded 1" in first
            warned_at = metrics_api._last_degraded_warn_at
            assert warned_at > 0.0
            assert len(degraded_warnings()) == 1

            second = client.get("/api/metrics", headers=_headers()).text
            assert "plaita_metrics_scrape_degraded 1" in second
            assert metrics_api._last_degraded_warn_at == warned_at
            assert len(degraded_warnings()) == 1  # 窗口内不重复刷屏

            monkeypatch.setattr(
                metrics_api, "_last_degraded_warn_at", time.monotonic() - 61.0
            )
            client.get("/api/metrics", headers=_headers())
            assert len(degraded_warnings()) == 2  # 窗口过后重新告警

            redis.fail_key_commands = set()
            recovered = client.get("/api/metrics", headers=_headers()).text
            assert "plaita_metrics_scrape_degraded 0" in recovered
            assert "plaita_metrics_failed_commands_total" not in recovered


class TestEventsDegradation:
    def test_one_key_failure_returns_partial_with_degraded_flag(
        self, client, redis, caplog
    ):
        inner = redis._inner
        inner.set("plaita:event:ev1", json.dumps(_event_record("ev1")))
        inner.set("plaita:event:ev2", json.dumps(_event_record("ev2")))
        redis.fail_keys = {"plaita:event:ev2"}

        with caplog.at_level(logging.WARNING, logger=events_api.__name__):
            resp = client.get("/api/events", headers=_headers())

        assert resp.status_code == 200
        body = resp.json()
        assert [e["event_id"] for e in body["events"]] == ["ev1"]
        assert body["degraded"] is True
        assert body["skipped"] == 1
        assert any("事件列表读取降级" in rec.message for rec in caplog.records)

    def test_all_keys_failure_is_not_reported_as_no_events(self, client, redis):
        inner = redis._inner
        inner.set("plaita:event:ev1", json.dumps(_event_record("ev1")))
        inner.set("plaita:event:ev2", json.dumps(_event_record("ev2")))
        redis.fail_keys = {"plaita:event:ev1", "plaita:event:ev2"}

        body = client.get("/api/events", headers=_headers()).json()

        assert body["events"] == []
        assert body["degraded"] is True
        assert body["skipped"] == 2

    def test_subscriptions_failure_marks_degraded(self, client, redis):
        inner = redis._inner
        inner.set(
            "plaita:subscription:sub1",
            json.dumps({"subscription_id": "sub1", "event_type": "approval"}),
        )
        redis.fail_keys = {"plaita:subscription:sub1"}

        body = client.get("/api/events/subscriptions", headers=_headers()).json()

        assert body["subscriptions"] == []
        assert body["degraded"] is True
        assert body["skipped"] == 1


class TestLogsDegradation:
    def test_stream_read_failure_returns_partial_with_degraded_flag(
        self, client, redis, caplog
    ):
        inner = redis._inner
        inner.xadd("plaita:logs:flow_worker:a", _log_record("a", "kept"))
        inner.xadd("plaita:logs:flow_worker:b", _log_record("b", "lost"))
        redis.fail_keys = {"plaita:logs:flow_worker:b"}

        with caplog.at_level(logging.WARNING, logger=logs_api.__name__):
            resp = client.get("/api/logs", headers=_headers())

        assert resp.status_code == 200
        body = resp.json()
        assert [entry["message"] for entry in body["logs"]] == ["kept"]
        assert body["degraded"] is True
        assert body["skipped"] == 1
        assert any("日志列表读取降级" in rec.message for rec in caplog.records)

    def test_entry_parse_failure_counts_as_skipped(self, client, redis):
        inner = redis._inner
        inner.xadd("plaita:logs:flow_worker:a", _log_record("a", "good"))
        inner.xadd(
            "plaita:logs:flow_worker:a",
            _log_record("a", "bad-context", context="{not-json"),
        )

        body = client.get("/api/logs", headers=_headers()).json()

        assert [entry["message"] for entry in body["logs"]] == ["good"]
        assert body["degraded"] is True
        assert body["skipped"] == 1

    def test_instance_logs_failure_marks_degraded(self, client, redis):
        inner = redis._inner
        inner.xadd("plaita:logs:flow_worker:a", _log_record("a", "kept"))
        redis.fail_keys = {"plaita:logs:flow_worker:a"}

        body = client.get("/api/logs/instance/a", headers=_headers()).json()

        assert body["logs"] == []
        assert body["degraded"] is True
        assert body["skipped"] == 1
