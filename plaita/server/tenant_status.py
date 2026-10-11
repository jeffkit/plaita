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
from typing import Optional

logger = logging.getLogger(__name__)

TENANT_STATUS_KEY = "plaita:tenant_status"
STATUS_ACTIVE = "active"
STATUS_DISABLED = "disabled"


def publish_tenant_status(redis_client, tenant_id: Optional[str], status: str) -> None:
    """把租户状态写入共享视图（无 Redis / 无租户 id 时空操作）。"""
    if redis_client is None or not tenant_id:
        return
    try:
        redis_client.hset(TENANT_STATUS_KEY, key=tenant_id, value=status)
    except Exception:  # noqa: BLE001 — 同步失败不阻断 console 主流程
        logger.warning("租户 %s 状态同步到 Redis 失败", tenant_id, exc_info=True)


def clear_tenant_status(redis_client, tenant_id: Optional[str]) -> None:
    """删除租户时清掉共享视图记录（缺失即视为 active）。"""
    if redis_client is None or not tenant_id:
        return
    try:
        redis_client.hdel(TENANT_STATUS_KEY, tenant_id)
    except Exception:  # noqa: BLE001
        logger.warning("租户 %s 状态清理失败", tenant_id, exc_info=True)


def tenant_is_active(redis_client, tenant_id: Optional[str]) -> bool:
    """租户是否可运行；无客户端 / 读失败 / 无记录 → True（fail-open）。"""
    if redis_client is None or not tenant_id:
        return True
    try:
        raw = redis_client.hget(TENANT_STATUS_KEY, tenant_id)
    except Exception:  # noqa: BLE001 — Redis 抖动不误伤全租户
        logger.warning("读取租户 %s 状态失败，按 active 处理", tenant_id, exc_info=True)
        return True
    if raw is None:
        return True
    if isinstance(raw, bytes):
        raw = raw.decode()
    return raw != STATUS_DISABLED
