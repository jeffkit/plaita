"""#26 告警钩子：死信 webhook + worker 接线。

死信此前只 ``logger.error``、全仓 grep alert/webhook/notify 零命中——值守刷不到
「死信产生」。本组覆盖：有界队列 webhook 发送器、队列死信钩子、worker 接线。
"""
from __future__ import annotations

import json
import threading

import pytest

pytest.importorskip("fakeredis")
pytest.importorskip("redis")
pytest.importorskip("cachetools")

import fakeredis

from plaita.server.alerts import WebhookAlerter
from plaita.server.task_queue import RedisStreamTaskQueue


class _Recorder:
    """线程安全记录 webhook POST。"""

    def __init__(self, fail: bool = False):
        self.calls = []
        self.fail = fail
        self._lock = threading.Lock()

    def __call__(self, url, body, headers, timeout):
        with self._lock:
            self.calls.append((url, json.loads(body.decode()), dict(headers), timeout))
        if self.fail:
            raise RuntimeError("mock webhook 挂了")


class TestWebhookAlerter:
    def test_send_posts_json_with_headers(self):
        rec = _Recorder()
        alerter = WebhookAlerter("http://alerts/hook", post=rec)
        assert alerter.send({"event": "dead_letter", "message_id": "1-1"}) is True
        assert alerter.flush(timeout=5) is True
        alerter.close()
        assert len(rec.calls) == 1
        url, payload, headers, timeout = rec.calls[0]
        assert url == "http://alerts/hook"
        assert payload["event"] == "dead_letter"
        assert headers["Content-Type"] == "application/json"
        assert alerter.stats()["sent"] == 1

    def test_failure_is_counted_not_raised(self):
        rec = _Recorder(fail=True)
        alerter = WebhookAlerter("http://alerts/hook", post=rec)
        alerter.send({"event": "x"})
        assert alerter.flush(timeout=5) is True
        alerter.close()
        stats = alerter.stats()
        assert stats["failed"] == 1
        assert stats["sent"] == 0

    def test_full_queue_drops_and_counts(self):
        release = threading.Event()
        started = threading.Event()
        rec = _Recorder()

        def slow(url, body, headers, timeout):
            started.set()
            release.wait(5)
            rec(url, body, headers, timeout)

        alerter = WebhookAlerter("http://alerts/hook", queue_size=1, post=slow)
        assert alerter.send({"n": 1}) is True
        # 等后台线程真的取走第一条并阻塞在 slow 里，队列才有空位
        assert started.wait(5) is True
        assert alerter.send({"n": 2}) is True
        assert alerter.send({"n": 3}) is False
        assert alerter.stats()["dropped"] == 1
        release.set()
        alerter.close()


class TestDeadLetterHook:
    def _queue(self, hook=None):
        redis = fakeredis.FakeRedis(decode_responses=True)
        return redis, RedisStreamTaskQueue(
            redis, "plaita:flow:queue:alert", group_name="g", consumer_name="c1",
            on_dead_letter=hook,
        )

    def test_dead_letter_invokes_hook_with_envelope(self):
        events = []
        redis, q = self._queue(hook=events.append)
        q.enqueue({"type": "start", "flow_id": "f1"})
        task = q.read(block_ms=100)
        q.dead_letter(task, reason="max_deliveries=5")
        assert len(events) == 1
        event = events[0]
        assert event["event"] == "dead_letter"
        assert event["queue"] == "plaita:flow:queue:alert"
        assert event["dlq_key"] == "plaita:flow:queue:alert:dlq"
        assert event["message_id"] == task.message_id
        assert event["reason"] == "max_deliveries=5"

    def test_hook_exception_does_not_break_dead_letter(self):
        def boom(_event):
            raise RuntimeError("告警通道故障")

        redis, q = self._queue(hook=boom)
        q.enqueue({"type": "start", "flow_id": "f1"})
        task = q.read(block_ms=100)
        dlq_id = q.dead_letter(task, reason="boom")
        assert dlq_id
        assert redis.xlen("plaita:flow:queue:alert:dlq") == 1
        assert q.stats()["dead_lettered"] == 1

    def test_no_hook_is_zero_change(self):
        redis, q = self._queue()
        q.enqueue({"type": "start", "flow_id": "f1"})
        task = q.read(block_ms=100)
        assert q.dead_letter(task, reason="x")
        assert redis.xlen("plaita:flow:queue:alert:dlq") == 1


class TestWorkerWiring:
    def _worker(self, **kw):
        from plaita.event.memory import InMemoryEventBus
        from plaita.server.flow_worker import RedisFlowWorker
        from plaita.storage.memory import MemoryExecutionStorage, MemoryFlowStorage

        kw.setdefault("enable_registry", False)
        kw.setdefault("enable_redis_logging", False)
        return RedisFlowWorker(
            redis_url="redis://localhost:6379/0",
            queue_name="test:queue",
            execution_storage=MemoryExecutionStorage(),
            flow_storage=MemoryFlowStorage(),
            event_bus=InMemoryEventBus(),
            **kw,
        )

    def test_dead_letter_forwarded_to_alerter(self):
        rec = _Recorder()
        worker = self._worker()
        worker._alerter = WebhookAlerter("http://alerts/hook", post=rec)
        worker._on_dead_letter({"event": "dead_letter", "message_id": "1-1"})
        assert worker._alerter.flush(timeout=5) is True
        worker._stop_alerting()
        assert rec.calls[0][1]["message_id"] == "1-1"

    def test_no_alerter_is_noop(self):
        worker = self._worker()
        worker._on_dead_letter({"event": "dead_letter"})  # 不得抛

    def test_start_alerting_only_with_webhook(self):
        worker = self._worker()
        assert worker._alert_webhook is None
        worker._start_alerting()
        assert worker._alerter is None

        worker2 = self._worker(alert_webhook="http://alerts/hook")
        worker2._start_alerting()
        assert worker2._alerter is not None
        worker2._stop_alerting()
        assert worker2._alerter is None

    def test_task_queue_gets_hook_when_configured(self):
        worker = self._worker(alert_webhook="http://alerts/hook")
        queue = worker._get_task_queue()
        assert queue.on_dead_letter == worker._on_dead_letter

    def test_metrics_text_includes_worker_and_alert_counters(self):
        worker = self._worker(alert_webhook="http://alerts/hook")
        worker._start_alerting()
        try:
            text = worker.metrics_text()
        finally:
            worker._stop_alerting()
        assert "plaita_worker_up" in text
        assert "plaita_queue_stream_length" in text
        assert "plaita_alert_webhook_sent_total" in text

    def test_metrics_server_disabled_by_default(self):
        worker = self._worker()
        assert worker._metrics_port == 0
        worker._start_metrics_server()
        assert worker._metrics_server is None

    def test_metrics_server_wired_with_port(self, monkeypatch):
        started = {}

        class _FakeServer:
            def __init__(self, render, *, host, port):
                started.update(render=render, host=host, port=port)

            def start(self):
                started["started"] = True

            def stop(self):
                started["stopped"] = True

        monkeypatch.setattr(
            "plaita.server.flow_worker.MetricsHttpServer", _FakeServer
        )
        worker = self._worker(metrics_port=9100, metrics_host="127.0.0.1")
        worker._start_metrics_server()
        assert started["started"] is True
        assert started["port"] == 9100
        assert started["host"] == "127.0.0.1"
        assert started["render"] == worker.metrics_text
        worker._stop_metrics_server()
        assert started["stopped"] is True
