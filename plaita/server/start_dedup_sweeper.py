"""start-dedup park 态的有界周期清扫（r62 B108-01）。

#289 的 park 语义把「mark 失败且 carrier 可能已被消费」的映射保留原 id，
靠同 key/同 due 重试/worker 终态自然收敛。但无人重试时，park 的 queued 行/
map/token/配额没有最终清收。本模块提供轻量 janitor：

- 只扫 ``{ns}:start-dedup-park:{key}`` 标记（park 时写入，含 parked_at）；
- 超过 ``PLAITA_START_DEDUP_PARK_TIMEOUT``（默认=claiming TTL 60s）才处理；
- 行已 running/终态 ⇒ 执行已接管，只删标记（不碰映射/配额）；
- 行 queued/缺失 ⇒ 执行未开始：写墓碑+删行、清 map/owner/token、回收配额，
  WARNING 审计；
- 预算按检查的标记数，单条失败 WARNING+计数，不终止循环；游标只在进程内。
"""
from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from typing import Any, Dict, Optional

from plaita.core.url_redaction import _redact_env_value, _scrub_url_text
from plaita.storage.redis import RedisExecutionStorage
from plaita.server import start_dedup as sd
from plaita.server.tenant_quota import release_tenant_start

logger = logging.getLogger("plaita.server.start_dedup_sweeper")

DEFAULT_INTERVAL_SECONDS = 30.0
DEFAULT_BUDGET = 100
INTERVAL_ENV = "PLAITA_START_DEDUP_PARK_SWEEP_INTERVAL"
BUDGET_ENV = "PLAITA_START_DEDUP_PARK_SWEEP_BUDGET"
DISABLE_ENV = "PLAITA_DISABLE_START_DEDUP_PARK_SWEEP"

_METRICS: Dict[str, int] = {
    "start_dedup_park_sweep_examined_total": 0,
    "start_dedup_park_sweep_cleaned_total": 0,
    "start_dedup_park_sweep_skipped_total": 0,
    "start_dedup_park_sweep_errors_total": 0,
}


def park_sweep_metrics() -> Dict[str, int]:
    return dict(_METRICS)


def reset_park_sweep_metrics() -> None:
    for key in _METRICS:
        _METRICS[key] = 0


def park_sweep_disabled() -> bool:
    return os.environ.get(DISABLE_ENV, "").strip() == "1"


def _resolve_positive_float(env: str, default: float) -> float:
    raw = os.environ.get(env)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = -1.0
    if not math.isfinite(value) or value <= 0:
        logger.warning(
            "%s=%s 非法（需有限正数），回退默认 %.0fs",
            env, _redact_env_value(raw), default,
        )
        return default
    return value


def _resolve_budget() -> int:
    raw = os.environ.get(BUDGET_ENV)
    if raw is None or not raw.strip():
        return DEFAULT_BUDGET
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = 0
    if value <= 0:
        logger.warning("%s=%s 非正数，按 1 标记/tick", BUDGET_ENV, _redact_env_value(raw))
        return 1
    return value


def _tenant_from_namespace(namespace: str) -> str:
    prefix = "plaita"
    if namespace == prefix:
        return "default"
    if namespace.startswith(prefix + ":"):
        return namespace[len(prefix) + 1:]
    return "default"


def _parse_marker(raw: Any) -> Optional[Dict[str, Any]]:
    text = raw.decode(errors="replace") if isinstance(raw, bytes) else raw
    if not text:
        return None
    try:
        data = json.loads(text)
    except (TypeError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _row_status(redis, namespace: str, execution_id: str) -> Optional[str]:
    try:
        raw = redis.get(f"{namespace}:execution:{execution_id}")
    except Exception as exc:  # noqa: BLE001 — 状态不可判按缺失（下轮再试）
        logger.warning(
            "start-dedup park 清扫读取执行行失败 execution=%s: %s",
            execution_id, _scrub_url_text(exc),
        )
        return None
    if raw is None:
        return None
    text = raw.decode(errors="replace") if isinstance(raw, bytes) else raw
    try:
        data = json.loads(text)
    except (TypeError, ValueError):
        return None
    return str(data.get("status") or "") or None


def sweep_parks(
    redis,
    *,
    now: Optional[float] = None,
    timeout_seconds: Optional[float] = None,
    budget: Optional[int] = None,
) -> Dict[str, int]:
    """扫描并清收超时 park；返回本 tick 的计数（测试/运维入口）。"""
    if redis is None:
        return {"examined": 0, "cleaned": 0, "skipped": 0, "errors": 0}
    timeout = (
        sd.CLAIM_TTL_SECONDS if timeout_seconds is None else float(timeout_seconds)
    )
    limit = DEFAULT_BUDGET if budget is None else max(1, int(budget))
    current = time.time() if now is None else float(now)
    counts = {"examined": 0, "cleaned": 0, "skipped": 0, "errors": 0}

    try:
        keys = list(redis.scan_iter(match="*:start-dedup-park:*", count=limit * 2))
    except Exception as exc:  # noqa: BLE001 — 下轮重试
        logger.warning("start-dedup park 清扫扫描失败（下轮重试）: %s", _scrub_url_text(exc))
        _METRICS["start_dedup_park_sweep_errors_total"] += 1
        counts["errors"] += 1
        return counts

    for key in keys:
        if counts["examined"] >= limit:
            break
        counts["examined"] += 1
        _METRICS["start_dedup_park_sweep_examined_total"] += 1
        key_text = key.decode(errors="replace") if isinstance(key, bytes) else str(key)
        try:
            raw = redis.get(key_text)
            marker = _parse_marker(raw)
            if marker is None:
                redis.delete(key_text)
                counts["skipped"] += 1
                continue
            execution_id = str(marker.get("id") or "")
            parked_at = float(marker.get("parked_at") or 0)
            if not execution_id or current - parked_at < timeout:
                counts["skipped"] += 1
                _METRICS["start_dedup_park_sweep_skipped_total"] += 1
                continue
            namespace_part, dedup_key = key_text.split(":start-dedup-park:", 1)
            tenant = _tenant_from_namespace(namespace_part)
            mapped = sd.read_mapped(redis, tenant, dedup_key)
            if mapped != execution_id:
                redis.delete(key_text)
                counts["skipped"] += 1
                _METRICS["start_dedup_park_sweep_skipped_total"] += 1
                continue
            status = _row_status(redis, namespace_part, execution_id)
            if status not in (None, "", "queued"):
                # running/终态：执行已接管，映射留给 dedup TTL；只清标记。
                redis.delete(key_text)
                counts["skipped"] += 1
                _METRICS["start_dedup_park_sweep_skipped_total"] += 1
                continue

            # 行 queued/缺失：执行未开始 → 墓碑+清行+清认领+回收配额。
            try:
                RedisExecutionStorage(
                    client=redis, namespace=namespace_part
                ).delete_execution_state(execution_id)
            except Exception as exc:  # noqa: BLE001 — 清理失败保留标记下轮再试
                logger.warning(
                    "start-dedup park 清扫删行失败 execution=%s: %s",
                    execution_id, _scrub_url_text(exc),
                )
                counts["errors"] += 1
                _METRICS["start_dedup_park_sweep_errors_total"] += 1
                continue
            sd.release(redis, tenant, dedup_key, execution_id)
            release_tenant_start(redis, tenant, execution_id)
            redis.delete(key_text)
            counts["cleaned"] += 1
            _METRICS["start_dedup_park_sweep_cleaned_total"] += 1
            logger.warning(
                "start-dedup park 超时清收（审计）：tenant=%s dedup=%s "
                "execution=%s age=%.0fs status=%r——已写墓碑、清 map/token、"
                "回收配额",
                tenant, dedup_key, execution_id,
                current - parked_at, status,
            )
        except Exception as exc:  # noqa: BLE001 — 单条失败不终止循环
            counts["errors"] += 1
            _METRICS["start_dedup_park_sweep_errors_total"] += 1
            logger.warning("start-dedup park 清扫失败 key=%s: %s", key_text, _scrub_url_text(exc))

    return counts


class ParkSweeper:
    """轻量周期清扫器（thread；console lifespan 挂载）。"""

    def __init__(
        self,
        redis,
        *,
        interval_seconds: Optional[float] = None,
        timeout_seconds: Optional[float] = None,
        budget: Optional[int] = None,
    ) -> None:
        self.redis = redis
        self.interval_seconds = (
            _resolve_positive_float(INTERVAL_ENV, DEFAULT_INTERVAL_SECONDS)
            if interval_seconds is None
            else max(0.05, float(interval_seconds))
        )
        self.timeout_seconds = (
            float(sd.CLAIM_TTL_SECONDS)
            if timeout_seconds is None
            else max(0.0, float(timeout_seconds))
        )
        self.budget = _resolve_budget() if budget is None else max(1, int(budget))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._loop, name="start-dedup-park-sweeper", daemon=True
        )
        self._thread.start()
        logger.info(
            "start-dedup park 清扫已启用：每 %.0fs，超时 %.0fs，预算 %d/tick",
            self.interval_seconds, self.timeout_seconds, self.budget,
        )

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        self._thread = None
        if thread is not None:
            thread.join(timeout=2.0)

    def _loop(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                counts = sweep_parks(
                    self.redis,
                    timeout_seconds=self.timeout_seconds,
                    budget=self.budget,
                )
            except Exception as exc:  # noqa: BLE001 — 下轮重试
                logger.warning("start-dedup park 清扫异常（下轮重试）: %s", _scrub_url_text(exc))
                continue
            if counts.get("cleaned") or counts.get("errors"):
                logger.warning(
                    "start-dedup park 清扫：examined=%s cleaned=%s skipped=%s errors=%s",
                    counts.get("examined"), counts.get("cleaned"),
                    counts.get("skipped"), counts.get("errors"),
                )
