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

try:
    from .queues import KNOWN_DLQ_KEYS, KNOWN_QUEUES
    from .redis_scan import scan_keys
except ImportError:  # 平铺布局（cwd=backend）运行时
    from queues import KNOWN_DLQ_KEYS, KNOWN_QUEUES  # type: ignore
    from redis_scan import scan_keys  # type: ignore

logger = logging.getLogger("api.metrics")

router = APIRouter()

DLQ_SUFFIX = ":dlq"

# 与 api/queues.py 的 KNOWN_QUEUES 同源；此处只取可直查的 Stream 键
KNOWN_STREAM_KEYS = [
    "plaita:flow:queue",
    "plaita:flow:queue:v2",
]

REGISTRY_WORKER_PATTERN = "plaita:registry:flow_worker:*"

#: A55-S5：单次抓取最多渲染的 Stream / worker 实例数（防止基数注入）。
MAX_STREAM_KEYS = 200
MAX_REGISTRY_INSTANCES = 500


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


def _stream_keys(redis: Redis) -> tuple[List[str], bool]:
    """已登记队列族 + DLQ 键（去重排序，有界）。

    A55-S5：不再 ``KEYS *:dlq``——事件类型键可以被租户构造成以 ``:dlq``
    结尾，从而被当成队列指标（基数注入）。只认 ``KNOWN_QUEUES`` 键族，
    并用有界 SCAN（#202）发现通配前缀下的实际键。
    """
    names: Set[str] = set(KNOWN_STREAM_KEYS)
    names.update(KNOWN_DLQ_KEYS)
    truncated = False
    for pattern in KNOWN_QUEUES:
        if "*" not in pattern:
            continue
        result = scan_keys(redis, pattern, max_keys=MAX_STREAM_KEYS)
        names.update(result.keys)
        truncated = truncated or result.truncated
    keys = sorted(names)
    if len(keys) > MAX_STREAM_KEYS:
        keys = keys[:MAX_STREAM_KEYS]
        truncated = True
    return keys, truncated


def _registry_metrics(redis: Redis) -> List[Metric]:
    truncated = False
    try:
        result = scan_keys(redis, REGISTRY_WORKER_PATTERN,
                           max_keys=MAX_REGISTRY_INSTANCES)
        instances = list(result.keys)
        truncated = result.truncated
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
    if truncated:
        metrics.append(
            Metric(
                "metrics_truncated",
                1,
                {"kind": "registry"},
                "gauge",
                "指标扫描触界（结果不完整，见 WARNING）",
            )
        )
    return metrics


def collect_cluster_metrics(redis: Redis) -> List[Metric]:
    metrics: List[Metric] = []
    stream_keys, truncated = _stream_keys(redis)
    if truncated:
        metrics.append(
            Metric(
                "metrics_truncated",
                1,
                {"kind": "queue"},
                "gauge",
                "指标扫描触界（结果不完整，见 WARNING）",
            )
        )
    for key in stream_keys:
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
