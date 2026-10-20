"""DLQ replay helper（A53-S2）——死信人工回灌的唯一入口。

背景（duo-a 第五十三轮）：replay 此前就是「按 runbook 手工 ``enqueue_task``」，
没有一次性标记/审计，也没有 schema/世代校验：

- 同一个 ``dlq_id`` 可无限回灌；无 ``execution_id`` 的旧载体回灌两次就是
  两条真实执行（A53-02）；
- ``enqueue_task`` 缺省写**当前 build** 的 ``schema_version``，v2 载体在 v1
  build 上会被按 v1 语义硬跑（A53-03）；
- 删除租户/执行后回灌会以新世代身份重跑（A53-01/A53-06）。

本模块提供幂等、可审计的回灌契约（CLI/console 共用）：

1. 以 ``(dlq_key, dlq_id)`` 为幂等键写 ``plaita:dlq:replay:<key>:<id>``
   （``SET NX EX 7d``）；第二次回灌返回 ``already_replayed`` 与既有
   ``target_message_id``，不再入队；
2. 无 ``execution_id`` 的 **start** 载体铸造**确定性** execution_id
   （``uuid5(dlq_key, dlq_id)``）并写入载体（同时补 ``dedup_key``）——即使
   标记过期/被 force 覆盖，同一死信也只会收敛到一条执行；
3. ``schema_version`` 按源信封继承（缺失 = ``LEGACY_TASK_SCHEMA_VERSION``），
   超过本 build 理解范围时**入队前**具名拒绝（``schema_too_new``）；
4. 每次回灌（含拒绝）写 ``plaita:dlq:replay:audit``；DLQ 原条目**不删除**
   ——证据保留，真正的清理由保留策略/显式 drain 负责。
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from .task_queue import (
    LEGACY_TASK_SCHEMA_VERSION,
    PAYLOAD_FIELD,
    TASK_SCHEMA_VERSION,
    enqueue_task,
    timestamp_from_stream_id,
)
from ..core.url_redaction import _scrub_url_text

try:
    from ..storage.redis import EXECUTION_TOMBSTONE_PREFIX
except ImportError:  # pragma: no cover - storage.redis 为常规依赖
    EXECUTION_TOMBSTONE_PREFIX = "plaita:exec_tombstone:"

logger = logging.getLogger(__name__)

#: replay 幂等键前缀（``plaita:dlq:replay:{dlq_key}:{dlq_id}``）。
REPLAY_MARKER_PREFIX = "plaita:dlq:replay"
#: 平台级 replay 审计 Stream（只对平台管理员可见）。
REPLAY_AUDIT_STREAM = "plaita:dlq:replay:audit"
#: 与 ``FlowWorker.START_DEDUP_TTL_SECONDS`` 同量级（7 天）。
REPLAY_MARKER_TTL_SECONDS = 7 * 86400

_REPLAYABLE_TYPES = ("start", "resume")


@dataclass(frozen=True)
class ReplayResult:
    """一次 replay 的结果（命名拒绝优先于抛异常）。"""

    replayed: bool
    reason: str
    target_message_id: Optional[str] = None
    execution_id: Optional[str] = None
    tenant_id: Optional[str] = None
    schema_version: Optional[int] = None
    detail: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def marker_key(dlq_key: str, dlq_id: str) -> str:
    return f"{REPLAY_MARKER_PREFIX}:{dlq_key}:{dlq_id}"


def deterministic_execution_id(dlq_key: str, dlq_id: str) -> str:
    """无 ``execution_id`` 载体的确定性 id（同一死信永远得到同一个 id）。"""
    return uuid.uuid5(
        uuid.NAMESPACE_URL, f"{REPLAY_MARKER_PREFIX}:{dlq_key}:{dlq_id}"
    ).hex


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return "" if value is None else str(value)


def _dead_letter_carrier_timestamp(
    envelope: Dict[str, Any], dlq_message_id: str
) -> Optional[str]:
    """DLQ 条目的载体入队时间（A69-04）：``dead_lettered_at`` → DLQ Stream ID。

    replay **不得用 now() 刷新年龄**：运维把一条 40 天前的老载体回灌后，
    它必须仍能被派发闸的年龄闸拦下（否则 replay 就是「洗白老 start」的
    后门）。两源都不可解析 → None，调用方 fail-closed 拒绝回灌。
    """
    raw = envelope.get("dead_lettered_at")
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        try:
            return datetime.fromtimestamp(float(raw)).isoformat()
        except (OverflowError, OSError, ValueError):
            pass
    elif isinstance(raw, str) and raw.strip():
        try:
            return datetime.fromisoformat(raw.strip()).isoformat()
        except ValueError:
            pass
    return timestamp_from_stream_id(dlq_message_id)


def _read_marker(redis, key: str) -> Optional[Dict[str, Any]]:
    try:
        raw = redis.get(key)
    except Exception as exc:  # noqa: BLE001 — 标记不可读按不存在处理（调用方拒绝）
        logger.warning("读取 DLQ replay 标记失败（%s）: %s", key, _scrub_url_text(exc))
        return None
    if not raw:
        return None
    try:
        value = json.loads(_text(raw))
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _result_from_marker(marker: Optional[Dict[str, Any]]) -> ReplayResult:
    marker = marker or {}
    return ReplayResult(
        replayed=False,
        reason="already_replayed",
        target_message_id=marker.get("target_message_id") or None,
        execution_id=marker.get("execution_id") or None,
        tenant_id=marker.get("tenant_id") or None,
        schema_version=marker.get("schema_version"),
        detail="replay marker exists; use force to re-enqueue deliberately",
    )


def _audit(
    redis,
    *,
    actor: str,
    dlq_key: str,
    dlq_id: str,
    result: ReplayResult,
    target_stream: str,
    source_id: str = "",
    force: bool = False,
) -> None:
    """写 replay 审计行（best-effort：失败只 WARNING，不回滚已入队副本）。"""
    fields = {
        "ts": _text(time.time()),
        "action": "dlq.replay",
        "actor": _text(actor),
        "dlq_key": _text(dlq_key),
        "dlq_id": _text(dlq_id),
        "source_id": _text(source_id),
        "target_stream": _text(target_stream),
        "target_message_id": _text(result.target_message_id),
        "execution_id": _text(result.execution_id),
        "tenant_id": _text(result.tenant_id),
        "schema_version": _text(result.schema_version),
        "result": _text(result.reason),
        "force": "1" if force else "0",
    }
    try:
        redis.xadd(REPLAY_AUDIT_STREAM, fields)
    except Exception as exc:  # noqa: BLE001 — 审计旁路
        logger.warning(
            "DLQ replay 审计写入失败（dlq_id=%s, result=%s）: %s",
            dlq_id,
            result.reason,
            _scrub_url_text(exc),
        )


def _reject(
    redis,
    *,
    actor: str,
    dlq_key: str,
    dlq_id: str,
    target_stream: str,
    reason: str,
    detail: str = "",
    schema_version: Optional[int] = None,
    tenant_id: Optional[str] = None,
    execution_id: Optional[str] = None,
    source_id: str = "",
    force: bool = False,
) -> ReplayResult:
    result = ReplayResult(
        replayed=False,
        reason=reason,
        execution_id=execution_id,
        tenant_id=tenant_id,
        schema_version=schema_version,
        detail=detail or None,
    )
    _audit(
        redis,
        actor=actor,
        dlq_key=dlq_key,
        dlq_id=dlq_id,
        result=result,
        target_stream=target_stream,
        source_id=source_id,
        force=force,
    )
    return result


def replay_dead_letter(
    redis,
    *,
    dlq_key: str,
    dlq_id: str,
    target_stream: str,
    actor: str,
    force: bool = False,
    execution_id: Optional[str] = None,
    tenant_id: Optional[str] = None,
    max_schema_version: Optional[int] = None,
    now: Optional[float] = None,
) -> ReplayResult:
    """把一条 DLQ 条目幂等回灌到 ``target_stream``。

    - ``execution_id``：仅用于补全**没有** execution_id 的旧 start 载体；
      载体自带不同 id 时具名拒绝（``execution_id_mismatch``）。
    - ``tenant_id``：仅用于补全缺 ``tenant_id`` 的条目（平台管理员显式指定）；
      与 payload 自带租户不一致时拒绝（``tenant_mismatch``）。
    - ``force=True``：绕过已存在的 replay 标记（仍要求平台管理员）。确定性
      execution_id 保证同一死信的 force 重灌仍收敛到一条执行。
    """
    dlq_key = _text(dlq_key)
    dlq_id = _text(dlq_id)
    target_stream = _text(target_stream)
    actor = _text(actor) or "unknown"
    reason_prefix = "not_replayable"

    if not dlq_key or not dlq_id or not target_stream:
        return _reject(
            redis,
            actor=actor,
            dlq_key=dlq_key,
            dlq_id=dlq_id,
            target_stream=target_stream,
            reason=reason_prefix,
            detail="dlq_key/dlq_id/target_stream are required",
            force=force,
        )

    try:
        rows = redis.xrange(dlq_key, min=dlq_id, max=dlq_id, count=1)
    except Exception as exc:  # noqa: BLE001 — 读取失败具名返回
        logger.warning("DLQ replay 读取失败（%s/%s）: %s", dlq_key, dlq_id, _scrub_url_text(exc))
        return _reject(
            redis,
            actor=actor,
            dlq_key=dlq_key,
            dlq_id=dlq_id,
            target_stream=target_stream,
            reason=reason_prefix,
            detail=f"xrange failed: {_text(exc)}",
            force=force,
        )
    if not rows:
        return _reject(
            redis,
            actor=actor,
            dlq_key=dlq_key,
            dlq_id=dlq_id,
            target_stream=target_stream,
            reason=reason_prefix,
            detail="dlq entry not found",
            force=force,
        )

    msg_id, fields = rows[0]
    msg_id = _text(msg_id)
    raw_payload = ""
    for key in (PAYLOAD_FIELD, PAYLOAD_FIELD.encode()):
        if key in fields:
            raw_payload = _text(fields[key])
            break
    try:
        envelope = json.loads(raw_payload)
    except (TypeError, ValueError):
        envelope = None
    if not isinstance(envelope, dict):
        return _reject(
            redis,
            actor=actor,
            dlq_key=dlq_key,
            dlq_id=msg_id,
            target_stream=target_stream,
            reason=reason_prefix,
            detail="dlq envelope is not a JSON object",
            force=force,
        )
    source_id = _text(envelope.get("source_id"))
    body = envelope.get("payload")
    if not isinstance(body, dict):
        return _reject(
            redis,
            actor=actor,
            dlq_key=dlq_key,
            dlq_id=msg_id,
            target_stream=target_stream,
            reason=reason_prefix,
            detail="dead-letter payload is not a dict (raw/truncated)",
            source_id=source_id,
            force=force,
        )
    if envelope.get("truncated") is True:
        return _reject(
            redis,
            actor=actor,
            dlq_key=dlq_key,
            dlq_id=msg_id,
            target_stream=target_stream,
            reason=reason_prefix,
            detail="dead-letter payload was truncated; cannot replay safely",
            source_id=source_id,
            force=force,
        )
    task_type = body.get("type")
    if task_type not in _REPLAYABLE_TYPES:
        return _reject(
            redis,
            actor=actor,
            dlq_key=dlq_key,
            dlq_id=msg_id,
            target_stream=target_stream,
            reason=reason_prefix,
            detail=f"unsupported task type: {_text(task_type)!r}",
            source_id=source_id,
            force=force,
        )

    # schema_version 按源信封继承（A53-03）；缺失 = v1，绝不臆造当前版本。
    raw_schema = envelope.get("schema_version")
    if raw_schema is None:
        schema_version = LEGACY_TASK_SCHEMA_VERSION
    else:
        try:
            schema_version = int(raw_schema)
        except (TypeError, ValueError):
            return _reject(
                redis,
                actor=actor,
                dlq_key=dlq_key,
                dlq_id=msg_id,
                target_stream=target_stream,
                reason=reason_prefix,
                detail=f"invalid schema_version: {_text(raw_schema)!r}",
                source_id=source_id,
                force=force,
            )
    limit = (
        TASK_SCHEMA_VERSION
        if max_schema_version is None
        else int(max_schema_version)
    )
    if schema_version > limit:
        # 入队前拒绝：先入队再靠消费侧死信会让超版本副本（或 A49-01 克隆）
        # 继续污染 DLQ。
        return _reject(
            redis,
            actor=actor,
            dlq_key=dlq_key,
            dlq_id=msg_id,
            target_stream=target_stream,
            reason="schema_too_new",
            detail=f"schema_version={schema_version} > max={limit}",
            schema_version=schema_version,
            source_id=source_id,
            force=force,
        )

    payload = dict(body)
    # A69-04：replay 不得把老载体洗成新载体。payload 缺 timestamp 时用
    # DLQ 条目的 dead_lettered_at（写入时刻）或 DLQ Stream ID 时间；两源
    # 都不可解析 ⇒ 拒绝回灌（fail-closed），绝不用 now() 兜底。
    existing_ts = payload.get("timestamp")
    if not isinstance(existing_ts, str) or not existing_ts.strip():
        carrier_ts = _dead_letter_carrier_timestamp(envelope, msg_id)
        if carrier_ts is None:
            return _reject(
                redis,
                actor=actor,
                dlq_key=dlq_key,
                dlq_id=msg_id,
                target_stream=target_stream,
                reason="carrier_timestamp_unknown",
                detail=(
                    "dead-letter payload has no timestamp and neither "
                    "dead_lettered_at nor dlq stream id is parseable; "
                    "replay would reset the carrier age"
                ),
                schema_version=schema_version,
                source_id=source_id,
                force=force,
            )
        payload["timestamp"] = carrier_ts
    owner = payload.get("tenant_id")
    if not isinstance(owner, str) or not owner:
        if not tenant_id:
            return _reject(
                redis,
                actor=actor,
                dlq_key=dlq_key,
                dlq_id=msg_id,
                target_stream=target_stream,
                reason=reason_prefix,
                detail="payload has no explicit tenant_id; pass tenant_id explicitly",
                schema_version=schema_version,
                source_id=source_id,
                force=force,
            )
        owner = _text(tenant_id)
        payload["tenant_id"] = owner
    elif tenant_id and _text(tenant_id) != owner:
        return _reject(
            redis,
            actor=actor,
            dlq_key=dlq_key,
            dlq_id=msg_id,
            target_stream=target_stream,
            reason="tenant_mismatch",
            detail=f"payload tenant {owner!r} != explicit tenant {_text(tenant_id)!r}",
            schema_version=schema_version,
            tenant_id=owner,
            source_id=source_id,
            force=force,
        )

    carrier_execution_id = payload.get("execution_id")
    minted = False
    if not isinstance(carrier_execution_id, str) or not carrier_execution_id.strip():
        if execution_id:
            carrier_execution_id = _text(execution_id).strip()
        elif task_type == "start":
            # 无 id 的 start 载体：确定性铸造（重试/force 复用同一 id）。
            carrier_execution_id = deterministic_execution_id(dlq_key, msg_id)
            minted = True
        else:
            return _reject(
                redis,
                actor=actor,
                dlq_key=dlq_key,
                dlq_id=msg_id,
                target_stream=target_stream,
                reason=reason_prefix,
                detail="resume carrier has no execution_id",
                schema_version=schema_version,
                tenant_id=owner,
                source_id=source_id,
                force=force,
            )
    elif execution_id and _text(execution_id).strip() != carrier_execution_id:
        return _reject(
            redis,
            actor=actor,
            dlq_key=dlq_key,
            dlq_id=msg_id,
            target_stream=target_stream,
            reason="execution_id_mismatch",
            detail=(
                f"payload execution_id {carrier_execution_id!r} != "
                f"explicit {_text(execution_id)!r}"
            ),
            schema_version=schema_version,
            tenant_id=owner,
            execution_id=carrier_execution_id,
            source_id=source_id,
            force=force,
        )

    # A53-06：执行已删除（墓碑命中）不再复活。墓碑是平台根键、按 id 键控。
    try:
        if redis.exists(f"{EXECUTION_TOMBSTONE_PREFIX}{carrier_execution_id}"):
            return _reject(
                redis,
                actor=actor,
                dlq_key=dlq_key,
                dlq_id=msg_id,
                target_stream=target_stream,
                reason="execution_deleted",
                detail="execution tombstone exists; replay would resurrect a deleted run",
                schema_version=schema_version,
                tenant_id=owner,
                execution_id=carrier_execution_id,
                source_id=source_id,
                force=force,
            )
    except Exception as exc:  # noqa: BLE001 — 无法确认删除状态 ⇒ fail-closed
        return _reject(
            redis,
            actor=actor,
            dlq_key=dlq_key,
            dlq_id=msg_id,
            target_stream=target_stream,
            reason=reason_prefix,
            detail=f"tombstone check failed: {_text(exc)}",
            schema_version=schema_version,
            tenant_id=owner,
            execution_id=carrier_execution_id,
            source_id=source_id,
            force=force,
        )

    # 一次性标记：先占位再入队（同一条目并发 replay 只有一胜）。
    key = marker_key(dlq_key, msg_id)
    ts = float(now if now is not None else time.time())
    marker = {
        "actor": actor,
        "ts": ts,
        "dlq_key": dlq_key,
        "dlq_id": msg_id,
        "tenant_id": owner,
        "schema_version": schema_version,
        "execution_id": carrier_execution_id,
        "target_stream": target_stream,
        "target_message_id": None,
        "attempts": 1,
        "force": bool(force),
    }
    try:
        encoded = json.dumps(marker, ensure_ascii=False)
        if force:
            redis.set(key, encoded, ex=REPLAY_MARKER_TTL_SECONDS)
        else:
            acquired = redis.set(
                key, encoded, nx=True, ex=REPLAY_MARKER_TTL_SECONDS
            )
            if not acquired:
                result = _result_from_marker(_read_marker(redis, key))
                _audit(
                    redis,
                    actor=actor,
                    dlq_key=dlq_key,
                    dlq_id=msg_id,
                    result=result,
                    target_stream=target_stream,
                    source_id=source_id,
                    force=force,
                )
                return result
    except Exception as exc:  # noqa: BLE001 — 标记写失败 ⇒ fail-closed，不入队
        logger.warning("DLQ replay 标记写入失败（%s）: %s", key, _scrub_url_text(exc))
        return _reject(
            redis,
            actor=actor,
            dlq_key=dlq_key,
            dlq_id=msg_id,
            target_stream=target_stream,
            reason=reason_prefix,
            detail=f"replay marker write failed: {_text(exc)}",
            schema_version=schema_version,
            tenant_id=owner,
            execution_id=carrier_execution_id,
            source_id=source_id,
            force=force,
        )

    payload["execution_id"] = carrier_execution_id
    if minted:
        # 「进 dedup」：确定性 id + 幂等键双保险（标记过期后同 id 仍收敛）。
        payload.setdefault("dedup_key", f"dlq-replay:{carrier_execution_id}")
    try:
        target_message_id = enqueue_task(
            redis, target_stream, payload, schema_version=schema_version
        )
    except Exception as exc:  # noqa: BLE001 — 入队失败：撤销占位并具名返回
        logger.warning(
            "DLQ replay 入队失败（dlq_id=%s）: %s", msg_id, _scrub_url_text(exc)
        )
        try:
            redis.delete(key)
        except Exception as exc:  # noqa: BLE001 — 撤销 best-effort，但必须可观测
            logger.warning(
                "DLQ replay 标记撤销失败（%s）: %s", key, _scrub_url_text(exc)
            )
        return _reject(
            redis,
            actor=actor,
            dlq_key=dlq_key,
            dlq_id=msg_id,
            target_stream=target_stream,
            reason="enqueue_failed",
            detail=_text(exc),
            schema_version=schema_version,
            tenant_id=owner,
            execution_id=carrier_execution_id,
            source_id=source_id,
            force=force,
        )

    marker["target_message_id"] = target_message_id
    try:
        redis.set(
            key, json.dumps(marker, ensure_ascii=False), ex=REPLAY_MARKER_TTL_SECONDS
        )
    except Exception as exc:  # noqa: BLE001 — 占位仍在，重复 replay 已被挡住
        logger.warning(
            "DLQ replay 已入队但标记补写失败（%s; target=%s）: %s",
            key,
            target_message_id,
            _scrub_url_text(exc),
        )

    result = ReplayResult(
        replayed=True,
        reason="ok",
        target_message_id=target_message_id,
        execution_id=carrier_execution_id,
        tenant_id=owner,
        schema_version=schema_version,
    )
    _audit(
        redis,
        actor=actor,
        dlq_key=dlq_key,
        dlq_id=msg_id,
        result=result,
        target_stream=target_stream,
        source_id=source_id,
        force=force,
    )
    return result
