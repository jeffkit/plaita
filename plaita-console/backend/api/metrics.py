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
from typing import Any, Dict, List, Set

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import PlainTextResponse
from redis import Redis

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


def _xlen(redis: Redis, key: str) -> int:
    try:
        return int(redis.xlen(key) or 0)
    except Exception as exc:  # noqa: BLE001 - 单键读取失败不整单 500
        logger.debug("xlen(%s) failed: %s", key, exc)
        return 0


def _pending(redis: Redis, key: str, group: str) -> int:
    try:
        summary = redis.xpending(key, group)
    except Exception as exc:  # noqa: BLE001 - 组不存在/键不存在按 0
        logger.debug("xpending(%s, %s) failed: %s", key, group, exc)
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


def _registry_metrics(redis: Redis) -> List[Metric]:
    try:
        instances = [_decode(k) for k in redis.keys(REGISTRY_WORKER_PATTERN)]
    except Exception as exc:  # noqa: BLE001
        logger.debug("扫描服务注册表失败: %s", exc)
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
    metrics: List[Metric] = []
    for key in _stream_keys(redis):
        stats: Dict[str, Any] = {
            "stream_key": key,
            "stream_length": _xlen(redis, key),
            "dlq_length": _xlen(redis, f"{key}{DLQ_SUFFIX}"),
        }
        if not key.endswith(DLQ_SUFFIX):
            stats["group"] = DEFAULT_CONSUMER_GROUP
            stats["pending"] = _pending(redis, key, DEFAULT_CONSUMER_GROUP)
        metrics.extend(collect_queue_metrics(stats))
    metrics.extend(_registry_metrics(redis))
    return metrics


@router.get("/metrics", response_class=PlainTextResponse)
async def metrics(request: Request, redis: Redis = Depends(get_redis)) -> PlainTextResponse:
    """Prometheus 文本：队列积压 / 死信堆积 / worker 存活（#26）。"""
    body = render_prometheus(collect_cluster_metrics(redis))
    return PlainTextResponse(
        content=body, media_type="text/plain; version=0.0.4; charset=utf-8"
    )
