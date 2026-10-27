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
import json
import os
import threading
import time
from typing import Any, Dict, List, Set

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import PlainTextResponse
from redis import Redis
from redis.exceptions import ResponseError

from plaita.server.metrics import Metric, collect_queue_metrics, render_prometheus
from plaita.server.task_queue import DEFAULT_CONSUMER_GROUP

try:
    # C1（plaita#323）：键登记住 redis_scan（无域）；不再反向 import api.queues。
    from .redis_scan import KNOWN_DLQ_KEYS, KNOWN_QUEUES, scan_keys
except ImportError:  # 平铺布局（cwd=backend）运行时
    from redis_scan import KNOWN_DLQ_KEYS, KNOWN_QUEUES, scan_keys  # type: ignore

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
        # 0.0 = 本进程尚未告警（也作为测试/运维的显式重置哨兵）。
        if (
            _last_degraded_warn_at
            and now - _last_degraded_warn_at < _DEGRADED_WARN_INTERVAL_SECONDS
        ):
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


def _registry_metrics(redis: Redis, degradation: _Degradation) -> List[Metric]:
    truncated = False
    try:
        result = scan_keys(redis, REGISTRY_WORKER_PATTERN,
                           max_keys=MAX_REGISTRY_INSTANCES)
        instances = list(result.keys)
        truncated = result.truncated
    except Exception:  # noqa: BLE001 - 扫描失败不整单 500，但必须标降级
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
    degradation = _Degradation()
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
            "stream_length": _xlen(redis, key, degradation),
            "dlq_length": _xlen(redis, f"{key}{DLQ_SUFFIX}", degradation),
        }
        if not key.endswith(DLQ_SUFFIX):
            stats["group"] = DEFAULT_CONSUMER_GROUP
            stats["pending"] = _pending(
                redis, key, DEFAULT_CONSUMER_GROUP, degradation
            )
        metrics.extend(collect_queue_metrics(stats))
    metrics.extend(_registry_metrics(redis, degradation))
    # B75-01：派发侧 unknown 队列计数（进程内累计，与计划/派发同源判据）。
    from plaita.server.capacity import collect_dispatch_metrics

    metrics.extend(collect_dispatch_metrics())
    metrics.extend(degradation.metrics())
    _warn_degradation(degradation)
    return metrics


def collect_queued_age_metrics(redis: Redis) -> List[Metric]:
    """超龄 queued 行点名（plaita#345，ADR-002 修订跟进单）。

    场景：worker 停机窗口入队是合法运维模式（chaos-wr 契约，受理面放行），
    但 worker 一直没拉起 ⇒ queued 行静默滞留——投递成功无人消费零信号（#300
    门对空池 fail-open、受理面 2026-10-27 起为 WARNING+放行）。本收集器在
    抓取时补上这块观测：扫执行索引 ZSET（score=start epoch 秒）里超过阈值
    的候选、读状态确认为 queued、按目标 runtime 分组输出 gauge
    ``plaita_queued_aged_rows{runtime}``；对应 runtime 池零活 worker 时按
    抓取频率限流 WARNING（复用 _warn_degradation 形状）。

    runtime 口径：状态行 flow_id+flow_version → console 版本行 runtime 列
    （E2 单真相面；解析不到按 python 缺省，与受理面同口径）。上限
    ``PLAITA_QUEUED_AGE_SCAN_MAX``（缺省 200）——超限置 metrics_truncated。
    任何 Redis 读失败走 degradation（整页 0 值不可信语义）。
    """
    try:
        threshold = float(os.environ.get("PLAITA_QUEUED_AGE_WARN_SECONDS", "900") or 900)
    except ValueError:
        threshold = 900.0
    try:
        scan_max = int(os.environ.get("PLAITA_QUEUED_AGE_SCAN_MAX", "200") or 200)
    except ValueError:
        scan_max = 200
    namespace = os.environ.get("PLAITA_NAMESPACE", "plaita")
    now = time.time()
    cutoff = now - max(0.0, threshold)
    index_key = f"{namespace}:execution:index"
    metrics: List[Metric] = []
    degradation = _Degradation()
    try:
        candidates = redis.zrangebyscore(
            index_key, "-inf", cutoff, start=0, num=scan_max
        ) or []
        total_in_window = redis.zcount(index_key, "-inf", cutoff) or 0
    except Exception:  # noqa: BLE001 — 读失败按降级计，整页不 500
        degradation.record("queued_age_index")
        return degradation.metrics() + metrics

    aged_by_runtime: Dict[str, int] = {}
    checked = 0
    for member in candidates:
        if checked >= scan_max:
            break
        execution_id = member.decode() if isinstance(member, bytes) else str(member)
        checked += 1
        try:
            raw = redis.get(f"{namespace}:execution:{execution_id}")
        except Exception:  # noqa: BLE001 — 单键读失败降级计
            degradation.record("queued_age_state")
            continue
        if not raw:
            continue  # 终态 TTL 已清/键不存在——索引陈旧成员，非滞留
        try:
            state = json.loads(raw)
        except Exception:  # noqa: BLE001 — 坏条目跳过
            continue
        if str(state.get("status") or "") != "queued":
            continue  # 只有 queued 是「无人消费」的滞留形态
        runtime = "python"
        flow_id = state.get("flow_id")
        version = state.get("flow_version")
        try:
            from ..services import flow_store as _fs
        except ImportError:  # 平铺布局（cwd=backend）
            from services import flow_store as _fs  # type: ignore
        try:
            row = _fs.get_flow_store().get_version(
                str(flow_id), str(version), tenant_id=None
            ) if (flow_id and version) else None
            if row is not None and getattr(row, "runtime", None):
                runtime = str(row.runtime)
        except Exception:  # noqa: BLE001 — runtime 解析失败按缺省池
            pass
        aged_by_runtime[runtime] = aged_by_runtime.get(runtime, 0) + 1

    for runtime in sorted(aged_by_runtime):
        count = aged_by_runtime[runtime]
        metrics.append(
            Metric(
                "queued_aged_rows",
                count,
                {"runtime": runtime},
                "gauge",
                "排队时长超过阈值的 queued 行数（超龄 = 可能无人消费）",
            )
        )
    if aged_by_runtime:
        try:
            try:
                from ..services.capability_gate import runtime_pool_alive
            except ImportError:  # 平铺布局（cwd=backend）
                from services.capability_gate import runtime_pool_alive  # type: ignore

            for runtime in sorted(aged_by_runtime):
                if runtime_pool_alive(redis, runtime) is False:
                    _warn_queued_aged(runtime, aged_by_runtime[runtime])
        except Exception:  # noqa: BLE001 — 点名失败不炸指标页
            pass
    if total_in_window > checked:
        metrics.append(
            Metric(
                "metrics_truncated",
                1,
                {"kind": "queued_age"},
                "gauge",
                "指标扫描触界（结果不完整，见 WARNING）",
            )
        )
    return degradation.metrics() + metrics


#: 超龄 queued 告警限流窗口（秒）：对齐 _DEGRADED_WARN_INTERVAL_SECONDS。
_QUEUED_AGED_WARN_INTERVAL_SECONDS = 300.0
_queued_aged_warn_lock = threading.Lock()
_last_queued_aged_warn_at: Dict[str, float] = {}


def _warn_queued_aged(runtime: str, count: int) -> None:
    global _last_queued_aged_warn_at
    now = time.monotonic()
    with _queued_aged_warn_lock:
        last = _last_queued_aged_warn_at.get(runtime, 0.0)
        if last and now - last < _QUEUED_AGED_WARN_INTERVAL_SECONDS:
            return
        _last_queued_aged_warn_at[runtime] = now
    logger.warning(
        "queued_aged_rows（放行待消费）：runtime=%s 池有 %d 条排队超过阈值"
        "（PLAITA_QUEUED_AGE_WARN_SECONDS）的执行且**该池零活 worker**——任务"
        "将无人消费。请拉起对应 worker（集群页模板）或切换 runtime；若为有计划"
        "的停机缓冲请忽略本告警",
        runtime, count,
    )


@router.get("/metrics", response_class=PlainTextResponse)
async def metrics(request: Request, redis: Redis = Depends(get_redis)) -> PlainTextResponse:
    """Prometheus 文本：队列积压 / 死信堆积 / worker 存活（#26）。"""
    metrics_out = collect_cluster_metrics(redis) + collect_queued_age_metrics(redis)
    body = render_prometheus(metrics_out)
    return PlainTextResponse(
        content=body, media_type="text/plain; version=0.0.4; charset=utf-8"
    )
