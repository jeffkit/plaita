"""#26 观测零导出：Prometheus 文本渲染 + worker /metrics 抓取端。

此前全仓没有任何指标端点——死信、积压、worker 全灭、观测丢弃都只能靠人刷
日志/看板发现。本组覆盖 ``plaita.server.metrics`` 的渲染契约与 HTTP 抓取端。
"""
from __future__ import annotations

import urllib.error
import urllib.request

import pytest

pytest.importorskip("cachetools")

from plaita.server.metrics import (
    Metric,
    MetricsHttpServer,
    collect_queue_metrics,
    collect_worker_metrics,
    render_prometheus,
    render_queue_metrics,
)


class TestRenderPrometheus:
    def test_type_and_help_precede_samples_once(self):
        out = render_prometheus([
            Metric("a_total", 1, {"x": "1"}, "counter", "计数"),
            Metric("a_total", 2, {"x": "2"}, "counter", "计数"),
        ])
        lines = out.strip().splitlines()
        assert lines[0] == "# HELP plaita_a_total 计数"
        assert lines[1] == "# TYPE plaita_a_total counter"
        assert lines[2] == 'plaita_a_total{x="1"} 1'
        assert lines[3] == 'plaita_a_total{x="2"} 2'

    def test_labels_sorted_and_escaped(self):
        out = render_prometheus([Metric("m", 1, {"b": 'va"l', "a": "x\ny"})])
        assert 'plaita_m{a="x\\ny",b="va\\"l"} 1' in out

    def test_bool_renders_as_gauge_int(self):
        out = render_prometheus([Metric("up", True, {}, "gauge")])
        assert "plaita_up 1" in out
        assert "plaita_up 0" in render_prometheus([Metric("up", False)])

    def test_non_numeric_value_skipped(self):
        assert render_prometheus([Metric("bad", object())]) == ""

    def test_prefix_can_be_disabled(self):
        assert render_prometheus([Metric("m", 3)], prefix="") == "# TYPE m gauge\nm 3\n"

    def test_empty_is_empty_string(self):
        assert render_prometheus([]) == ""


class TestCollectQueueMetrics:
    STATS = {
        "stream_key": "plaita:flow:queue:v2",
        "group": "plaita-workers",
        "stream_length": 3,
        "pending": 1,
        "dlq_length": 2,
        "max_deliveries": 5,
        "enqueued": 10,
        "dead_lettered": 2,
        "schema_rejected": 0,
    }

    def test_gauges_and_counters_split(self):
        metrics = {(m.name): m for m in collect_queue_metrics(self.STATS)}
        assert metrics["queue_stream_length"].value == 3
        assert metrics["queue_stream_length"].type == "gauge"
        assert metrics["queue_dead_lettered_total"].type == "counter"
        assert metrics["queue_dead_lettered_total"].labels["stream"] == "plaita:flow:queue:v2"
        assert metrics["queue_dead_lettered_total"].labels["group"] == "plaita-workers"

    def test_absent_fields_are_skipped(self):
        names = {m.name for m in collect_queue_metrics({"stream_key": "q"})}
        assert names == set()

    def test_render_queue_metrics_contains_dlq_length(self):
        text = render_queue_metrics(self.STATS)
        assert "plaita_queue_dlq_length{" in text
        assert "plaita_queue_schema_rejected_total" in text

    def test_extra_labels_merged(self):
        metrics = collect_queue_metrics(self.STATS, extra_labels={"tenant": "t1"})
        assert all(m.labels["tenant"] == "t1" for m in metrics)


class TestCollectWorkerMetrics:
    def test_heartbeat_only_when_present(self):
        without = {m.name for m in collect_worker_metrics()}
        assert "worker_heartbeat_timestamp_seconds" not in without
        with_hb = {
            m.name: m for m in collect_worker_metrics(heartbeat_timestamp=123.0)
        }
        assert with_hb["worker_heartbeat_timestamp_seconds"].value == 123.0

    def test_flags(self):
        metrics = {
            m.name: m for m in collect_worker_metrics(
                active_tasks=2, draining=True, running=True, instance="w1", registered=True
            )
        }
        assert metrics["worker_active_tasks"].value == 2
        assert metrics["worker_draining"].value is True
        assert metrics["worker_up"].labels["instance"] == "w1"
        assert metrics["worker_registered"].value is True


class TestMetricsHttpServer:
    def test_serves_metrics_and_404s_other_paths(self):
        server = MetricsHttpServer(lambda: "plaita_up 1\n", host="127.0.0.1", port=0)
        port = server.start()
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
                assert resp.status == 200
                assert resp.read().decode() == "plaita_up 1\n"
                assert resp.headers["Content-Type"].startswith("text/plain")
            with pytest.raises(urllib.error.HTTPError) as exc:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/nope", timeout=5)
            assert exc.value.code == 404
        finally:
            server.stop()

    def test_render_failure_returns_500_not_crash(self):
        def boom() -> str:
            raise RuntimeError("stats 读失败")

        server = MetricsHttpServer(boom, host="127.0.0.1", port=0)
        port = server.start()
        try:
            with pytest.raises(urllib.error.HTTPError) as exc:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5)
            assert exc.value.code == 500
        finally:
            server.stop()

    def test_stop_is_idempotent(self):
        server = MetricsHttpServer(lambda: "", host="127.0.0.1", port=0)
        server.start()
        server.stop()
        server.stop()
