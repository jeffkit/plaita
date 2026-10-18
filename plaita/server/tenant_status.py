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

import logging
import threading
from typing import Optional

from plaita.core.url_redaction import _scrub_exception_text

logger = logging.getLogger(__name__)

TENANT_STATUS_KEY = "plaita:tenant_status"
STATUS_ACTIVE = "active"
STATUS_DISABLED = "disabled"

# plaita#237（owner 裁决）：fail-open 语义保持不变（缺失记录 = active，兼容
# default 单租部署），但每次命中必须 WARNING 计数——否则闸门静默失效无法
# 从日志/监控发现。计数是进程内的（多 worker 各自累计，由日志聚合）。
_fail_open_lock = threading.Lock()
_fail_open_total = 0


def tenant_fail_open_total() -> int:
    """本进程内租户状态闸 fail-open 命中次数（观测/测试用）。"""
    with _fail_open_lock:
        return _fail_open_total


def _note_fail_open(tenant_id: Optional[str], reason: str) -> None:
    global _fail_open_total
    with _fail_open_lock:
        _fail_open_total += 1
    logger.warning(
        "租户状态闸 fail-open（%s）：tenant=%s 按 active 放行（plaita#237 计数）",
        reason, tenant_id,
    )


def publish_tenant_status(redis_client, tenant_id: Optional[str], status: str) -> None:
    """把租户状态写入共享视图（无 Redis / 无租户 id 时空操作）。"""
    if redis_client is None or not tenant_id:
        return
    try:
        redis_client.hset(TENANT_STATUS_KEY, key=tenant_id, value=status)
    except Exception as exc:  # noqa: BLE001 — 同步失败不阻断 console 主流程
        logger.warning("租户 %s 状态同步到 Redis 失败: %s", tenant_id, _scrub_exception_text(exc))


def clear_tenant_status(redis_client, tenant_id: Optional[str]) -> None:
    """删除租户时清掉共享视图记录（缺失即视为 active）。"""
    if redis_client is None or not tenant_id:
        return
    try:
        redis_client.hdel(TENANT_STATUS_KEY, tenant_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("租户 %s 状态清理失败: %s", tenant_id, _scrub_exception_text(exc))


def tenant_is_active(redis_client, tenant_id: Optional[str]) -> bool:
    """租户是否可运行；无客户端 / 读失败 / 无记录 → True（fail-open + 计数）。

    fail-open 语义不变（plaita#237 边界：不改 default 单租部署兼容），但每次
    命中都经 :func:`_note_fail_open` 打 WARNING 并累加进程内计数。
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
    if raw is None:
        _note_fail_open(tenant_id, "无状态记录")
        return True
    if isinstance(raw, bytes):
        raw = raw.decode()
    return raw != STATUS_DISABLED
