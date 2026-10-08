"""Prometheus 文本导出（#26 观测零导出修复）。

把队列/worker 运行态渲染为 Prometheus text exposition format (0.0.4)——
worker 的 ``/metrics`` 抓取端与 console 的集群汇总端共用本模块。只依赖标准
库：核心包不因「加一个指标」被迫吃下 prometheus-client。

导出内容（值域来自 :meth:`RedisStreamTaskQueue.stats` 与 worker 注册信息）：

- 队列 gauge：``stream_length`` / ``pending`` / ``dlq_length`` / 阈值；
- 队列计数器：``enqueued`` / ``acked`` / ``dead_lettered`` 等进程内累计；
- worker：存活 / 活跃任务数 / draining / 心跳时间戳。

计数器与 gauge 必须分开（``_total`` 后缀 + TYPE：counter）——混用会让
Prometheus 侧的 ``rate()`` 失真。
"""
from __future__ import annotations

import logging
import math
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

logger = logging.getLogger("plaita.server.metrics")

# 指标名前缀：Prometheus 侧的命名空间，也是「全仓零指标」的可 grep 锚点。
METRIC_PREFIX = "plaita"

# RedisStreamTaskQueue.stats() 字段 → (指标名, help)。计数器与 gauge 分表。
QUEUE_GAUGES: Tuple[Tuple[str, str, str], ...] = (
    ("stream_length", "queue_stream_length", "Stream 条目数（含未删除残留）"),
    ("pending", "queue_pending", "本消费组 PEL 中未 ack 的条目数"),
    ("dlq_length", "queue_dlq_length", "死信 Stream 条目数"),
    ("max_deliveries", "queue_max_deliveries", "超限进 DLQ 的投递阈值"),
    ("claim_min_idle_ms", "queue_claim_min_idle_ms", "pending 可被回收的最短空闲（毫秒）"),
)
QUEUE_COUNTERS: Tuple[Tuple[str, str, str], ...] = (
    ("enqueued", "queue_enqueued_total", "本进程入队总数"),
    ("acked", "queue_acked_total", "本进程 ack 总数"),
    ("reclaimed", "queue_reclaimed_total", "本进程 XCLAIM 回收总数"),
    ("dead_lettered", "queue_dead_lettered_total", "本进程死信总数"),
    ("lease_conflicts", "queue_lease_conflicts_total", "本进程租约冲突数"),
    ("poison_acked", "queue_poison_acked_total", "本进程畸形消息丢弃数"),
    ("failed", "queue_failed_total", "本进程处理失败数"),
    ("dlq_guard_skipped", "queue_dlq_guard_skipped_total", "守卫拒绝死信、消息留 pending 数"),
    ("residue_swept", "queue_residue_swept_total", "本进程回收的已 ack 未删残留数"),
    ("schema_rejected", "queue_schema_rejected_total", "信封版本过新被拒收数"),
)


@dataclass(frozen=True)
class Metric:
    """一条待导出的样本。``labels`` 键序在渲染时统一排序，保证输出稳定。"""

    name: str
    value: Any
    labels: Mapping[str, str] = field(default_factory=dict)
    type: str = "gauge"
    help: str = ""


def _escape_label_value(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _render_labels(labels: Mapping[str, str]) -> str:
    if not labels:
        return ""
    inner = ",".join(f'{k}="{_escape_label_value(labels[k])}"' for k in sorted(labels))
    return "{" + inner + "}"


def _format_value(value: Any) -> Optional[str]:
    """数值 → Prometheus 字面量；无可表示的值返回 None（该样本跳过）。"""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "+Inf" if value > 0 else "-Inf"
        return repr(value)
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return None


def render_prometheus(metrics: Iterable[Metric], *, prefix: str = METRIC_PREFIX) -> str:
    """渲染为 Prometheus text exposition；样本按 (指标名, 标签) 稳定排序。

    同一指标名的 HELP/TYPE 只输出一次且先于其全部样本——Prometheus 解析器
    要求 TYPE 位于该族任何样本之前。
    """
    lines: List[str] = []
    declared: set = set()
    for metric in sorted(metrics, key=lambda m: (m.name, tuple(sorted(m.labels.items())))):
        full = f"{prefix}_{metric.name}" if prefix else metric.name
        rendered = _format_value(metric.value)
        if rendered is None:
            logger.debug("指标 %s 的值 %r 非数值，跳过", full, metric.value)
            continue
        if full not in declared:
            if metric.help:
                lines.append(f"# HELP {full} {metric.help}")
            lines.append(f"# TYPE {full} {metric.type}")
            declared.add(full)
        lines.append(f"{full}{_render_labels(metric.labels)} {rendered}")
    return "\n".join(lines) + ("\n" if lines else "")


def collect_queue_metrics(
    stats: Mapping[str, Any], *, extra_labels: Optional[Mapping[str, str]] = None
) -> List[Metric]:
    """``RedisStreamTaskQueue.stats()`` 快照 → 指标列表（缺字段即跳过）。"""
    labels: Dict[str, str] = {}
    stream = stats.get("stream_key")
    if stream:
        labels["stream"] = str(stream)
    group = stats.get("group")
    if group:
        labels["group"] = str(group)
    if extra_labels:
        labels.update({k: str(v) for k, v in extra_labels.items()})
    metrics: List[Metric] = []
    for key, name, help_text in QUEUE_GAUGES:
        if key in stats and stats[key] is not None:
            metrics.append(Metric(name, stats[key], dict(labels), "gauge", help_text))
    for key, name, help_text in QUEUE_COUNTERS:
        if key in stats and stats[key] is not None:
            metrics.append(Metric(name, stats[key], dict(labels), "counter", help_text))
    return metrics


def collect_worker_metrics(
    *,
    active_tasks: int = 0,
    draining: bool = False,
    running: bool = True,
    heartbeat_timestamp: Optional[float] = None,
    instance: Optional[str] = None,
    registered: bool = False,
) -> List[Metric]:
    """worker 自身运行态 → 指标列表。``instance`` 作为标签区分多副本。"""
    labels: Dict[str, str] = {}
    if instance:
        labels["instance"] = str(instance)
    metrics = [
        Metric("worker_up", 1 if running else 0, dict(labels), "gauge", "worker 运行中（消费循环在跑，1/0）"),
        Metric("worker_active_tasks", active_tasks, dict(labels), "gauge", "在途任务数"),
        Metric("worker_draining", bool(draining), dict(labels), "gauge", "是否正在 draining（1/0）"),
        Metric(
            "worker_registered",
            bool(registered),
            dict(labels),
            "gauge",
            "是否已注册到服务注册表（1/0）",
        ),
    ]
    if heartbeat_timestamp is not None:
        metrics.append(
            Metric(
                "worker_heartbeat_timestamp_seconds",
                heartbeat_timestamp,
                dict(labels),
                "gauge",
                "最后一次心跳的 Unix 时间戳（秒）——长时间不前进 = 心跳线程卡死",
            )
        )
    return metrics


def render_queue_metrics(stats: Mapping[str, Any], *, prefix: str = METRIC_PREFIX) -> str:
    """单队列快照 → Prometheus 文本（console 汇总端的便捷入口）。"""
    return render_prometheus(collect_queue_metrics(stats), prefix=prefix)


class MetricsHttpServer:
    """最小 Prometheus 抓取端（stdlib ``http.server``，只服务 ``GET /metrics``）。

    ``render`` 每次抓取时调用，拿的是实时快照。``port=0`` 由内核分配，绑定
    后的实际端口见 :attr:`bound_port`（测试用）。
    """

    def __init__(
        self,
        render: Callable[[], str],
        *,
        host: str = "0.0.0.0",
        port: int = 9100,
    ) -> None:
        self._render = render
        self._host = host
        self._port = int(port)
        self._server = None
        self._thread: Optional[threading.Thread] = None
        self.bound_port = 0

    def start(self) -> int:
        """绑定并起后台服务线程，返回实际监听端口。"""
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from urllib.parse import urlparse

        render = self._render

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler 契约
                if urlparse(self.path).path != "/metrics":
                    self.send_response(404)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                try:
                    body = render().encode("utf-8")
                    status = 200
                except Exception:  # noqa: BLE001 - 抓取端不得因单次渲染失败而崩
                    logger.warning("渲染 /metrics 失败", exc_info=True)
                    body, status = b"", 500
                self.send_response(status)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):  # noqa: D102 - 静音访问日志
                return

        server = ThreadingHTTPServer((self._host, self._port), _Handler)
        server.daemon_threads = True
        self._server = server
        self.bound_port = int(server.server_address[1])
        self._thread = threading.Thread(
            target=server.serve_forever, name="plaita-metrics-http", daemon=True
        )
        self._thread.start()
        logger.info("/metrics 抓取端已启动: http://%s:%d/metrics", self._host, self.bound_port)
        return self.bound_port

    def stop(self) -> None:
        if self._server is not None:
            server, self._server = self._server, None
            try:
                server.shutdown()
                server.server_close()
            except Exception as exc:  # noqa: BLE001 - 停机路径 best-effort
                logger.debug("关闭 /metrics 服务失败: %s", exc)
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
