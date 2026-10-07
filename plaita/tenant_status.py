"""租户停用状态闸（跨进程：console 写、server 读）。

租户状态权威源在 console 的 relational store（``tenants.status``，见
``plaita-console`` 的 ``tenants_svc``）。但运行面（调度服务、FlowWorker）
是独立进程，只连 Redis、读不到该库；因此 console 在改状态时把「停用」
发布到 Redis，server 侧在**入队 / 派发**前读取并阻断——

- ``fire_schedule``（调度服务循环 + console 立即触发）不再入队
- ``FlowWorker._dispatch_task`` 的 start/resume 派发跳过

键用 ``plaita:tenant_status``（租户 slug 不允许下划线，见 console 的
``_SLUG_RE``，故与 ``plaita:{tenant_id}:...`` 命名空间不会碰撞）。

读取语义：Redis 不可用 / 键缺失 / 读失败一律视为**未停用**（fail-open）——
停用闸是加严措施，不应因 Redis 抖动把全部租户的运行面拦死。
"""
from __future__ import annotations

from typing import Optional

from plaita.logger import logger

TENANT_STATUS_KEY = "plaita:tenant_status"  # Redis HASH: tenant_id -> active|disabled
_DISABLED = "disabled"


def publish_tenant_status(redis_client, tenant_id: str, status: str) -> None:
    """把租户状态发布到 Redis（console 侧调用；失败仅告警，不影响主流程）。"""
    if redis_client is None or not tenant_id:
        return
    try:
        redis_client.hset(TENANT_STATUS_KEY, tenant_id, status)
    except Exception:  # noqa: BLE001 — 发布失败不得阻断状态变更
        logger.warning("租户 %s 状态发布到 Redis 失败", tenant_id, exc_info=True)


def is_tenant_disabled(redis_client, tenant_id: Optional[str]) -> bool:
    """租户是否已停用（server 运行面调用）。缺失/异常 = 未停用（fail-open）。"""
    if redis_client is None or not tenant_id:
        return False
    try:
        raw = redis_client.hget(TENANT_STATUS_KEY, tenant_id)
    except Exception:  # noqa: BLE001 — 读失败按未停用放行
        logger.warning("读取租户 %s 停用状态失败（按未停用放行）", tenant_id, exc_info=True)
        return False
    if isinstance(raw, bytes):
        raw = raw.decode()
    return raw == _DISABLED
