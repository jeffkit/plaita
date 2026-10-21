"""租户状态闸（跨进程共享视图，plaita#27）。

console 是租户状态的权威库（SQLite/PG）；FlowWorker 与 schedule_service 可能
在别的进程/机器，无法直连该库。console 在租户创建/启停/删除时把状态同步进
Redis HASH ``plaita:tenant_status``（``tenant_id -> active|disabled``），
worker 派发任务前、schedule_service 入队前据此拦截停用租户。

- 缺失记录 / 无 Redis 客户端 / 读失败 = active（fail-open）：兼容升级前未发布
  状态的既有部署，避免升级即全租户停摆；也容忍 console 与 worker 不共享 Redis
  的运维缺口（此时闸门不生效，须在 worker 侧 Redis 跑同步脚本补齐）。
- 平台机制键（不按租户 namespace 分区），与任务队列/registry/调度锁同款。
"""
from __future__ import annotations

import json
import logging
import threading
from typing import Any, Optional

from plaita.core.url_redaction import _scrub_exception_text
from plaita.tenant_context import (
    TENANT_STATUS_DISABLED,
    TENANT_STATUS_KEY,
    TENANT_STATUS_META_KEY,
)

logger = logging.getLogger(__name__)

#: 部署纪元锚点（A2R31-02 / B2R31-01 §租户状态纪元）。
#: 值来自部署配置（``PLAITA_DEPLOYMENT_EPOCH``），**不在 Redis 快照里**；
#: Redis-only 恢复回滚了状态 HASH 时，这个键也会一起回滚——因此 worker
#: 用「本进程 expected_epoch vs Redis 里的 epoch」判断状态是否属于当前部署。
TENANT_STATUS_EPOCH_KEY = "plaita:tenant_status:epoch"
STATUS_ACTIVE = "active"
STATUS_DISABLED = TENANT_STATUS_DISABLED
LEGACY_EPOCH = "legacy"

# plaita#237（owner 裁决）：fail-open 语义保持不变（缺失记录 = active，兼容
# default 单租部署），但每次命中必须 WARNING 计数——否则闸门静默失效无法
# 从日志/监控发现。计数是进程内的（多 worker 各自累计，由日志聚合）。
_fail_open_lock = threading.Lock()
_fail_open_total = 0
_stale_epoch_total = 0


def tenant_fail_open_total() -> int:
    """本进程内租户状态闸 fail-open 命中次数（观测/测试用）。"""
    with _fail_open_lock:
        return _fail_open_total


def tenant_stale_epoch_total() -> int:
    """本进程内「状态不属于当前部署纪元」的拒绝次数（A2R31-02）。"""
    with _fail_open_lock:
        return _stale_epoch_total


def _note_fail_open(tenant_id: Optional[str], reason: str) -> None:
    global _fail_open_total
    with _fail_open_lock:
        _fail_open_total += 1
    logger.warning(
        "租户状态闸 fail-open（%s）：tenant=%s 按 active 放行（plaita#237 计数）",
        reason, tenant_id,
    )


def _note_stale_epoch(
    tenant_id: Optional[str], reason: str, *, expected: str, actual: Optional[str]
) -> None:
    global _stale_epoch_total
    with _fail_open_lock:
        _stale_epoch_total += 1
    logger.warning(
        "event=tenant_status.stale_epoch tenant=%s reason=%s expected_epoch=%s "
        "redis_epoch=%s——状态不属于当前部署纪元，拒绝（A2R31-02）",
        tenant_id or "default", reason, expected, actual if actual is not None else "<missing>",
    )


def _status_from_value(raw: Any) -> str:
    """状态值解码：兼容旧纯字符串与 JSON ``{status: ...}``（灰度双写）。"""
    if isinstance(raw, bytes):
        raw = raw.decode(errors="replace")
    text = str(raw).strip()
    if text.startswith("{"):
        try:
            payload = json.loads(text)
        except (TypeError, ValueError):
            return text
        if isinstance(payload, dict) and isinstance(payload.get("status"), str):
            return payload["status"]
    return text


def _epoch_cmp(remote: str, expected: str) -> Optional[int]:
    """纪元比较：数字按数值，其余按字符串；不可比返回 None。"""
    try:
        a, b = int(remote), int(expected)
        return (a > b) - (a < b)
    except (TypeError, ValueError):
        pass
    if not remote or not expected:
        return None
    return (remote > expected) - (remote < expected)


def publish_tenant_status(
    redis_client,
    tenant_id: Optional[str],
    status: str,
    *,
    epoch: Optional[str] = None,
) -> None:
    """把租户状态写入共享视图（无 Redis / 无租户 id 时空操作）。

    A2R31-02：状态值保持**纯字符串**（旧读侧兼容，灰度「旧值双写」），纪元
    写在独立键 ``plaita:tenant_status:epoch``。两者分开是因为状态 HASH 会被
    Redis 快照回滚，而 epoch 的权威锚点是部署配置；只有分开写，worker 才能
    判断「这条状态属于哪个部署」。
    """
    if redis_client is None or not tenant_id:
        return
    try:
        redis_client.hset(TENANT_STATUS_KEY, key=tenant_id, value=status)
        if epoch:
            redis_client.set(TENANT_STATUS_EPOCH_KEY, epoch)
    except Exception as exc:  # noqa: BLE001 — 同步失败不阻断 console 主流程
        logger.warning("租户 %s 状态同步到 Redis 失败: %s", tenant_id, _scrub_exception_text(exc))


def publish_tenant_status_epoch(redis_client, epoch: Optional[str]) -> None:
    """写入/刷新部署纪元（console 每次对账与状态写入都调用）。"""
    if redis_client is None or not epoch or epoch == LEGACY_EPOCH:
        return
    try:
        redis_client.set(TENANT_STATUS_EPOCH_KEY, epoch)
    except Exception as exc:  # noqa: BLE001
        logger.warning("租户状态纪元写入失败: %s", _scrub_exception_text(exc))


def clear_tenant_status(redis_client, tenant_id: Optional[str]) -> None:
    """删除租户时清掉共享视图记录（缺失即视为 active）。"""
    if redis_client is None or not tenant_id:
        return
    try:
        redis_client.hdel(TENANT_STATUS_KEY, tenant_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("租户 %s 状态清理失败: %s", tenant_id, _scrub_exception_text(exc))


def publish_tenant_generation(
    redis_client, tenant_id: Optional[str], generation: Optional[str]
) -> bool:
    """写入租户当前世代（plaita#277）；None 清除该租户的世代记录。

    返回是否成功：Redis 失败 WARNING 并返回 False，由调用方决定语义
    （plaita#284 的建租户/对账按 fail-closed 处理；其余同步路径可忽略）。
    worker 侧读不到世代时按 fail-closed 处理（非 default 且调用方带 expected
    世代时拒绝写）。
    """
    if redis_client is None or not tenant_id:
        return True
    try:
        if generation is None:
            redis_client.hdel(TENANT_STATUS_META_KEY, tenant_id)
            return True
        # 保留 meta 中其它附加字段（当前只有 tenant_generation，合并写更稳）。
        raw = redis_client.hget(TENANT_STATUS_META_KEY, tenant_id)
        meta = {}
        if raw is not None:
            try:
                parsed = json.loads(
                    raw.decode() if isinstance(raw, bytes) else raw
                )
                if isinstance(parsed, dict):
                    meta = parsed
            except (TypeError, ValueError):
                meta = {}
        meta["tenant_generation"] = str(generation)
        redis_client.hset(
            TENANT_STATUS_META_KEY,
            key=tenant_id,
            value=json.dumps(meta, ensure_ascii=False),
        )
        return True
    except Exception as exc:  # noqa: BLE001 — 调用方决定是否 fail-closed
        logger.warning(
            "租户 %s 世代同步到 Redis 失败: %s",
            tenant_id,
            _scrub_exception_text(exc),
        )
        return False


def read_tenant_generation(
    redis_client, tenant_id: Optional[str]
) -> Optional[str]:
    """读租户当前世代；无客户端/记录/读失败 → None。"""
    if redis_client is None or not tenant_id:
        return None
    try:
        raw = redis_client.hget(TENANT_STATUS_META_KEY, tenant_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "租户 %s 世代读取失败: %s", tenant_id, _scrub_exception_text(exc)
        )
        return None
    if raw is None:
        return None
    try:
        meta = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(meta, dict):
        return None
    generation = meta.get("tenant_generation")
    return str(generation) if generation else None


def tenant_write_allowed(
    redis_client,
    tenant_id: Optional[str],
    expected_generation: Optional[str] = None,
) -> tuple[bool, Optional[str]]:
    """DLQ/订阅等未受 fence 保护写面的租户闸（plaita#277）。

    返回 ``(allowed, reason)``。规则：

    - 空/default 租户：放行（#237 单租兼容；default 的删除不在此单范围）。
    - 无 Redis 客户端：非 default 一律拒绝（fail-closed）。
    - 状态 ``disabled``：拒绝（删除墓碑；即使尚未重建也不得写回）。
    - 调用方带 ``expected_generation``（在途 worker 派发时捕获的世代）：
      当前世代缺失或不同 → 拒绝。
    - 调用方不带世代（老消息/队列内部路径）：只有当前世代也缺失才放行；
      一旦 console 发布了世代，无法证明归属的迟到写按 fail-closed 拒绝。
    """
    if not tenant_id or tenant_id == "default":
        return True, None
    if redis_client is None:
        return False, "no_redis"
    try:
        raw_status = redis_client.hget(TENANT_STATUS_KEY, tenant_id)
    except Exception as exc:  # noqa: BLE001 — 状态不可确认 ⇒ 拒绝
        logger.warning(
            "租户 %s 写闸读状态失败（fail-closed）: %s",
            tenant_id,
            _scrub_exception_text(exc),
        )
        return False, "status_read_failed"
    if raw_status is not None and _status_from_value(raw_status) == STATUS_DISABLED:
        return False, "tenant_disabled"
    current = read_tenant_generation(redis_client, tenant_id)
    if expected_generation is None:
        if current is None:
            return True, None
        return False, "generation_missing"
    if current is None:
        return False, "generation_missing"
    if current != expected_generation:
        return False, "generation_changed"
    return True, None


def tenant_is_active(
    redis_client,
    tenant_id: Optional[str],
    expected_epoch: Optional[str] = None,
    *,
    tenant_generation: Optional[str] = None,
) -> bool:
    """租户是否可运行；无客户端 / 读失败 / 无记录 → True（fail-open + 计数）。

    fail-open 语义不变（plaita#237 边界：不改 default 单租部署兼容），但每次
    命中都经 :func:`_note_fail_open` 打 WARNING 并累加进程内计数。

    A2R31-02：``expected_epoch`` 非 legacy 时启用纪元闸——
    - Redis epoch 更旧/缺失 → 非 default 租户拒绝，default 保持 fail-open；
    - Redis epoch 更新 → 本 worker 过旧，拒绝并 WARNING（不用旧代码执行新
      纪元状态）；
    - 相等 → 按状态值判定。
    """
    if not tenant_id:
        return True
    if redis_client is None:
        _note_fail_open(tenant_id, "无 Redis 客户端")
        return True
    try:
        raw = redis_client.hget(TENANT_STATUS_KEY, tenant_id)
    except Exception as exc:  # noqa: BLE001 — Redis 抖动不误伤全租户
        _note_fail_open(
            tenant_id, f"读取失败: {_scrub_exception_text(exc)}")
        return True

    expected = (expected_epoch or "").strip()
    epoch_gate = bool(expected and expected != LEGACY_EPOCH)
    if epoch_gate:
        try:
            redis_epoch_raw = redis_client.get(TENANT_STATUS_EPOCH_KEY)
        except Exception as exc:  # noqa: BLE001 — 读不到纪元按「缺失」保守处理
            logger.warning(
                "event=tenant_status.stale_epoch tenant=%s reason=epoch_read_failed: %s",
                tenant_id, _scrub_exception_text(exc),
            )
            redis_epoch_raw = None
        redis_epoch = None
        if redis_epoch_raw is not None:
            redis_epoch = (
                redis_epoch_raw.decode(errors="replace")
                if isinstance(redis_epoch_raw, bytes)
                else str(redis_epoch_raw)
            ).strip()
        if not redis_epoch:
            # 恢复的旧快照没有纪元键：无法证明状态属于本部署。
            if tenant_id == "default":
                _note_fail_open(tenant_id, "Redis 无纪元键（default 保持 #237）")
            else:
                _note_stale_epoch(
                    tenant_id, "redis_epoch_missing",
                    expected=expected, actual=None,
                )
                return False
        else:
            cmp = _epoch_cmp(redis_epoch, expected)
            if cmp == 0:
                pass
            elif cmp is None:
                if tenant_id == "default":
                    _note_fail_open(tenant_id, "纪元不可比（default 保持 #237）")
                else:
                    _note_stale_epoch(
                        tenant_id, "epoch_uncomparable",
                        expected=expected, actual=redis_epoch,
                    )
                    return False
            elif cmp < 0:
                if tenant_id == "default":
                    _note_fail_open(tenant_id, "Redis 纪元更旧（default 保持 #237）")
                else:
                    _note_stale_epoch(
                        tenant_id, "redis_epoch_older",
                        expected=expected, actual=redis_epoch,
                    )
                    return False
            else:
                _note_stale_epoch(
                    tenant_id, "worker_epoch_older",
                    expected=expected, actual=redis_epoch,
                )
                return False
        if tenant_generation:
            try:
                meta_raw = redis_client.hget(
                    "plaita:tenant_status:meta", tenant_id
                )
            except Exception as exc:  # noqa: BLE001 — meta 只是附加校验
                logger.debug(
                    "读取租户 %s 状态 meta 失败（附加校验跳过）: %s",
                    tenant_id, _scrub_exception_text(exc),
                )
                meta_raw = None
            if meta_raw is not None:
                try:
                    meta = json.loads(
                        meta_raw.decode() if isinstance(meta_raw, bytes) else meta_raw
                    )
                except (TypeError, ValueError):
                    meta = None
                if isinstance(meta, dict):
                    actual_generation = meta.get("tenant_generation")
                    if actual_generation and actual_generation != tenant_generation:
                        _note_stale_epoch(
                            tenant_id, "tenant_generation_mismatch",
                            expected=expected, actual=redis_epoch,
                        )
                        return False

    if raw is None:
        _note_fail_open(tenant_id, "无状态记录")
        return True
    return _status_from_value(raw) != STATUS_DISABLED
