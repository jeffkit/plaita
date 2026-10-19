"""集群级 Prometheus ``/metrics`` 端点（#26）。

worker 自己暴露的 ``/metrics`` 只有本进程的队列计数器；「worker 全灭」这类
判定需要跨副本视野——本端点从 console 已连的 Redis 汇总：

- 各已知队列的 ``XLEN`` / ``XPENDING``（pending）与 DLQ 堆积；
- 服务注册表里的 flow_worker 存活数（``plaita_worker_registered`` /
  ``plaita_workers_registered_total``）——归零即「worker 全灭」。

渲染复用 ``plaita.server.metrics``（核心包，无需 prometheus-client）。
鉴权：随管理面挂载（``X-Admin-API-Key``）——Prometheus 抓取配置里带上即可。
"""
import logging
import threading
import time
from typing import Any, Dict, List, Set

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import PlainTextResponse
from redis import Redis
from redis.exceptions import ResponseError

from plaita.server.metrics import Metric, collect_queue_metrics, render_prometheus
from plaita.server.task_queue import DEFAULT_CONSUMER_GROUP

logger = logging.getLogger("api.metrics")

router = APIRouter()

DLQ_SUFFIX = ":dlq"

# 与 api/queues.py 的 KNOWN_QUEUES 同源；此处只取可直查的 Stream 键
KNOWN_STREAM_KEYS = [
    "plaita:flow:queue",
    "plaita:flow:queue:v2",
]

REGISTRY_WORKER_PATTERN = "plaita:registry:flow_worker:*"


def get_redis(request: Request) -> Redis:
    redis = request.app.state.redis
    if redis is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "当前为本地单机模式（未连接 Redis），该功能不可用。"
                "启动 Redis 并重启 console 可恢复完整集群能力。"
            ),
        )
    return redis


def _decode(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else value


class _Degradation:
    """单次抓取内的部分失败累计（A2R19-04）。

    单个 Redis 命令失败时仍返回 0 保证整页不打崩，但**必须**把「读不到」
    与「真实为零」区分开：``metrics_scrape_degraded`` + 每命令失败计数进
    响应，WARNING 按抓取频率限流。
    """

    def __init__(self) -> None:
        self.failures: Dict[str, int] = {}

    def record(self, command: str) -> None:
        self.failures[command] = self.failures.get(command, 0) + 1

    @property
    def degraded(self) -> bool:
        return bool(self.failures)

    def metrics(self) -> List[Metric]:
        metrics = [
            Metric(
                "metrics_scrape_degraded",
                1 if self.degraded else 0,
                {},
                "gauge",
                "本次抓取是否有 Redis 命令失败（1 = 页面里的 0 值不可信）",
            )
        ]
        for command, count in sorted(self.failures.items()):
            metrics.append(
                Metric(
                    "metrics_failed_commands_total",
                    count,
                    {"command": command},
                    "counter",
                    "本次抓取中该 Redis 命令的失败次数",
                )
            )
        return metrics


#: 降级 WARNING 限流窗口（秒）：Prometheus 抓取通常 15-60s 一次，防止刷屏。
_DEGRADED_WARN_INTERVAL_SECONDS = 60.0
_degraded_warn_lock = threading.Lock()
_last_degraded_warn_at = 0.0


def _warn_degradation(degradation: _Degradation) -> None:
    global _last_degraded_warn_at
    if not degradation.degraded:
        return
    now = time.monotonic()
    with _degraded_warn_lock:
        if now - _last_degraded_warn_at < _DEGRADED_WARN_INTERVAL_SECONDS:
            return
        _last_degraded_warn_at = now
    summary = ", ".join(
        f"{command}={count}" for command, count in sorted(degradation.failures.items())
    )
    logger.warning(
        "集群指标采集降级（%s）：相关队列/worker 指标为 0 但**不代表真实为零**"
        "（已通过 plaita_metrics_scrape_degraded/plaita_metrics_failed_commands_total 导出）",
        summary,
    )


def _xlen(redis: Redis, key: str, degradation: _Degradation) -> int:
    try:
        return int(redis.xlen(key) or 0)
    except Exception:  # noqa: BLE001 - 单键读取失败不整单 500
        degradation.record("xlen")
        return 0


def _pending(redis: Redis, key: str, group: str, degradation: _Degradation) -> int:
    try:
        summary = redis.xpending(key, group)
    except Exception as exc:  # noqa: BLE001 - 组不存在/键不存在按 0
        # 消费组/键尚未创建（NOGROUP / no such key）是**正常空队列**，不是依赖
        # 故障——不得计入降级，否则每个空队列都会触发假告警。
        text = str(exc).upper()
        if isinstance(exc, ResponseError) and (
            "NOGROUP" in text or "NO SUCH KEY" in text
        ):
            logger.debug("xpending(%s, %s) 无消费组/键，按空队列处理", key, group)
            return 0
        degradation.record("xpending")
        return 0
    if isinstance(summary, dict):
        return int(summary.get("pending", 0) or 0)
    if summary:
        return int(summary[0] or 0)
    return 0


def _stream_keys(redis: Redis) -> List[str]:
    """已知队列 + 实际存在的 DLQ Stream（去重排序）。"""
    names: Set[str] = set(KNOWN_STREAM_KEYS)
    for key in redis.keys(f"*{DLQ_SUFFIX}"):
        names.add(_decode(key))
    return sorted(names)


def _registry_metrics(redis: Redis, degradation: _Degradation) -> List[Metric]:
    try:
        instances = [_decode(k) for k in redis.keys(REGISTRY_WORKER_PATTERN)]
    except Exception:  # noqa: BLE001
        degradation.record("registry_scan")
        instances = []
    metrics = [
        Metric(
            "workers_registered_total",
            len(instances),
            {},
            "gauge",
            "存活（未过期）的 flow_worker 实例数——归零 = 全灭",
        )
    ]
    for instance in instances:
        metrics.append(
            Metric(
                "worker_registered",
                1,
                {"instance": instance.rsplit(":", 1)[-1]},
                "gauge",
                "该实例是否在注册表中（1/0）",
            )
        )
    return metrics


def collect_cluster_metrics(redis: Redis) -> List[Metric]:
    degradation = _Degradation()
    metrics: List[Metric] = []
    for key in _stream_keys(redis):
        stats: Dict[str, Any] = {
            "stream_key": key,
            "stream_length": _xlen(redis, key, degradation),
            "dlq_length": _xlen(redis, f"{key}{DLQ_SUFFIX}", degradation),
        }
        if not key.endswith(DLQ_SUFFIX):
            stats["group"] = DEFAULT_CONSUMER_GROUP
            stats["pending"] = _pending(redis, key, DEFAULT_CONSUMER_GROUP, degradation)
        metrics.extend(collect_queue_metrics(stats))
    metrics.extend(_registry_metrics(redis, degradation))
    metrics.extend(degradation.metrics())
    _warn_degradation(degradation)
    return metrics


@router.get("/metrics", response_class=PlainTextResponse)
async def metrics(request: Request, redis: Redis = Depends(get_redis)) -> PlainTextResponse:
    """Prometheus 文本：队列积压 / 死信堆积 / worker 存活（#26）。"""
    body = render_prometheus(collect_cluster_metrics(redis))
    return PlainTextResponse(
        content=body, media_type="text/plain; version=0.0.4; charset=utf-8"
    )
