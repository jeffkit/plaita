"""Console 启动期配置漂移预检（B2R31-02）。

单一 resolver 只保证**本进程**的 effective 值一致；跨进程（console 派发 vs
worker 消费）仍需一次启动对账，否则「同一 env 不同语义」会以静默积压的形式
出现（A2R31-03/04）。本模块只做**可观测**：registry 非空且字段不匹配 →
WARNING + drift 列表（fail-open，不阻断启动）；registry 为空 → INFO「尚无
worker 注册」。
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

#: 每进程每 `(field, console_value, worker_value)` 只告警一次。
_WARNED: set = set()


def _redis_target(url: str) -> Dict[str, Any]:
    try:
        parts = urlsplit(url)
    except ValueError:
        return {}
    try:
        db = int((parts.path or "/0").lstrip("/") or 0)
    except ValueError:
        db = 0
    return {"host": parts.hostname, "port": parts.port or 6379, "db": db}


def _warn_once(
    field: str, console_value: Any, worker_value: Any, message: str, *args: Any
) -> None:
    key = (field, str(console_value), str(worker_value))
    if key in _WARNED:
        return
    _WARNED.add(key)
    logger.warning(message, *args)


def preflight_config_drift(redis_client) -> Dict[str, Any]:
    """对比 console effective 配置与 registry worker metadata。

    返回 ``{"status", "workers", "drift": [...]}``；任何读取失败只降级为
    ``status="degraded"`` + WARNING，绝不阻断 console 启动。
    """
    from plaita.runtime_config import resolve_dlq_key, resolve_queue_name, resolve_redis_url

    effective_queue = resolve_queue_name("console")
    effective_redis = resolve_redis_url("console")
    effective_target = _redis_target(effective_redis)
    effective_dlq = resolve_dlq_key(effective_queue, empty_policy="default")
    result: Dict[str, Any] = {
        "status": "ok",
        "workers": 0,
        "effective": {
            "queue_name": effective_queue,
            "redis_target": effective_target,
            "dlq_key": effective_dlq,
        },
        "drift": [],
    }
    if redis_client is None:
        result["status"] = "no_redis"
        return result
    try:
        keys = list(redis_client.scan_iter("plaita:registry:flow_worker:*"))
    except Exception as exc:  # noqa: BLE001 — 预检不阻断启动，但 loud
        result["status"] = "degraded"
        logger.warning("配置漂移预检：读取 registry 失败（跳过）: %s", exc)
        return result
    if not keys:
        logger.info("配置漂移预检：尚无 worker 注册（queue=%s）", effective_queue)
        return result

    for raw_key in keys:
        key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
        try:
            raw = redis_client.get(raw_key)
            info = json.loads(raw) if raw else {}
        except Exception as exc:  # noqa: BLE001 — 单实例坏数据跳过
            logger.warning("配置漂移预检：解析 %s 失败（跳过）: %s", key, exc)
            continue
        metadata = info.get("metadata") if isinstance(info, dict) else None
        if not isinstance(metadata, dict):
            continue
        result["workers"] += 1
        instance = info.get("instance_id") or key.rsplit(":", 1)[-1]

        worker_queue = metadata.get("queue_name")
        if isinstance(worker_queue, str) and worker_queue and worker_queue != effective_queue:
            result["drift"].append(
                {"field": "queue_name", "instance": instance,
                 "console": effective_queue, "worker": worker_queue}
            )
            _warn_once(
                "queue_name", effective_queue, worker_queue,
                "配置漂移：console 派发队列=%s，worker %s 消费队列=%s——"
                "手工启动/调度消息会落入无人消费的 Stream（B2R31-02）",
                effective_queue, instance, worker_queue,
            )

        worker_target = {
            "host": metadata.get("redis_host"),
            "port": metadata.get("redis_port"),
            "db": metadata.get("redis_db"),
        }
        if any(v is not None for v in worker_target.values()):
            mismatch = {
                field: (effective_target.get(field), worker_target.get(field))
                for field in ("host", "port", "db")
                if worker_target.get(field) is not None
                and effective_target.get(field) != worker_target.get(field)
            }
            if mismatch:
                result["drift"].append(
                    {"field": "redis_target", "instance": instance,
                     "console": effective_target, "worker": worker_target}
                )
                _warn_once(
                    "redis_target", effective_target, worker_target,
                    "配置漂移：console Redis=%s，worker %s Redis=%s——事件/队列"
                    "不在同一 keyspace（B2R31-02）",
                    effective_target, instance, worker_target,
                )

        worker_dlq = metadata.get("dlq_key")
        if isinstance(worker_dlq, str) and worker_dlq and worker_dlq != effective_dlq:
            result["drift"].append(
                {"field": "dlq_key", "instance": instance,
                 "console": effective_dlq, "worker": worker_dlq}
            )
            _warn_once(
                "dlq_key", effective_dlq, worker_dlq,
                "配置漂移：console DLQ 键=%s，worker %s DLQ 键=%s——删除租户时"
                "自定义 DLQ 可能漏清（A2R31-08/B2R31-02）",
                effective_dlq, instance, worker_dlq,
            )

        configured_id = metadata.get("instance_id")
        if isinstance(configured_id, str) and configured_id and configured_id != instance:
            result["drift"].append(
                {"field": "instance_id", "instance": instance,
                 "configured": configured_id}
            )
            logger.info(
                "配置漂移（非阻断）：注册键实例 id=%s，metadata instance_id=%s",
                instance, configured_id,
            )
    return result
