"""租户管理服务（平台管理员操作面）。

- 租户 slug 一经创建不可改；contract_secret_* 为对外 HMAC 契约凭证，
  明文仅在创建/轮换的响应中返回一次
- 删除保护：租户内仍有流程时拒绝删除（避免误删带数据的租户）
- 停用（#27）是运行面动作而非仅改一行状态：除落库外，还把租户 ID 发布到
  Redis 停用集合（worker / 调度服务据此把闸，见 ``plaita.tenant_context``）
  并停掉该租户的全部调度定义。会话不删——由 ``users_svc.resolve_session``
  实时判状态 → 403，重新启用后原会话即刻可用。
"""
from __future__ import annotations

import logging
import re
import secrets
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from plaita.tenant_context import set_tenant_disabled

try:
    from ..models.flow import DEFAULT_TENANT_ID, FlowRecord, Tenant, TenantMember
except ImportError:
    from models.flow import (  # type: ignore
        DEFAULT_TENANT_ID,
        FlowRecord,
        Tenant,
        TenantMember,
    )

from . import users_svc

logger = logging.getLogger(__name__)

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$")


class TenantError(ValueError):
    """租户管理操作非法（信息面向管理员展示）。"""


def new_contract_secret() -> tuple[str, str]:
    """生成一组契约密钥（secret_id 可读性较好，secret_key 高熵）。"""
    return f"t-{secrets.token_hex(8)}", secrets.token_hex(24)


def list_tenants(store) -> List[Dict[str, Any]]:
    with store._session_local() as session:
        tenants = session.scalars(select(Tenant).order_by(Tenant.id)).all()
        counts: Dict[str, int] = {}
        for m in session.scalars(select(TenantMember)).all():
            counts[m.tenant_id] = counts.get(m.tenant_id, 0) + 1
        return [
            {
                "id": t.id,
                "name": t.name,
                "status": t.status,
                "contract_secret_id": t.contract_secret_id,
                "member_count": counts.get(t.id, 0),
                "created_at": t.created_at.isoformat() if t.created_at else None,
            }
            for t in tenants
        ]


def get_tenant(store, tenant_id: str) -> Dict[str, Any]:
    with store._session_local() as session:
        row = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        if row is None:
            raise LookupError(f"租户不存在: {tenant_id}")
        return _to_dict(row)


def create_tenant(store, tenant_id: str, name: str = "") -> Dict[str, Any]:
    if not _SLUG_RE.match(tenant_id or ""):
        raise TenantError(
            "租户 ID 需为 3-64 位小写字母/数字/连字符，且字母或数字开头结尾"
        )
    with store._session_local() as session:
        if session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first():
            raise TenantError(f"租户已存在: {tenant_id}")
        secret_id, secret_key = new_contract_secret()
        row = Tenant(
            id=tenant_id,
            name=name or tenant_id,
            status="active",
            contract_secret_id=secret_id,
            contract_secret_key=secret_key,
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        result = _to_dict(row)
        result["contract_secret_key"] = secret_key  # 仅本次响应返回
        return result


def set_tenant_status(
    store, tenant_id: str, status: str, redis_client: Optional[Any] = None
) -> None:
    """启用/停用租户；停用时一并把闸下到运行面（#27）。

    除落库外：把租户 ID 发布到 Redis 停用集合（FlowWorker 与调度服务据此
    不再推进该租户的任务/调度），并停掉该租户全部调度定义。会话不在此撤销
    ——``resolve_session`` 每次实时判状态，停用即 403，重新启用即恢复。
    """
    if status not in ("active", "disabled"):
        raise TenantError(f"非法状态: {status}")
    with store._session_local() as session:
        row = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        if row is None:
            raise TenantError(f"租户不存在: {tenant_id}")
        row.status = status
        session.commit()
    set_tenant_disabled(redis_client, tenant_id, status == "disabled")
    if status == "disabled":
        _disable_tenant_schedules(store, tenant_id, redis_client)


def is_tenant_disabled(store, tenant_id: Optional[str]) -> bool:
    """租户是否已停用（console 进程内路径用；权威库即本表）。

    运行面（worker / 独立调度服务进程）无 SQLite 访问，走 Redis 停用集合
    （``plaita.tenant_context.is_tenant_disabled``）；本函数供 console 进程内
    的本地档调度循环使用。租户行缺失视为未停用（同 runtime 口径，宽松优先）。
    """
    with store._session_local() as session:
        row = session.scalars(
            select(Tenant).where(Tenant.id == (tenant_id or DEFAULT_TENANT_ID))
        ).first()
        return row is not None and row.status != "active"


def sync_tenant_status_to_redis(store, redis_client: Optional[Any]) -> int:
    """把库内停用状态对齐到 Redis 停用集合（启动兜底，幂等）。返回停用数。

    停用状态只在变更时发布，Redis 若被清空/换实例，运行面就会认为「无人
    停用」——安全边界不该这么脆。启动时按权威库（本表）对齐一次。
    """
    if redis_client is None:
        return 0
    with store._session_local() as session:
        rows = session.scalars(select(Tenant)).all()
    disabled = 0
    for row in rows:
        is_disabled = row.status != "active"
        set_tenant_disabled(redis_client, row.id, is_disabled)
        disabled += int(is_disabled)
    return disabled


def _disable_tenant_schedules(store, tenant_id: str, redis_client: Optional[Any]) -> None:
    """停掉某租户的全部调度：本地档改 SQLite，集群档改 Redis HASH。

    两档都尽力而为——调度停用失败不得让「停用租户」整体失败（运行面的租户
    闸仍兜住，调度照样不会入队）。
    """
    try:
        if redis_client is None:
            try:
                from . import local_scheduler
            except ImportError:
                from services import local_scheduler  # type: ignore
            local_scheduler.disable_tenant_schedules(store, tenant_id)
        else:
            from plaita.server.services.schedule_service import disable_tenant_schedules

            disable_tenant_schedules(redis_client, tenant_id)
    except Exception:  # noqa: BLE001 — 排障用；不得阻断停用
        logger.warning("停用租户 %s 时禁用其调度失败", tenant_id, exc_info=True)


def rotate_contract_secret(store, tenant_id: str) -> Dict[str, str]:
    """轮换契约密钥，明文仅本次响应返回。"""
    with store._session_local() as session:
        row = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        if row is None:
            raise TenantError(f"租户不存在: {tenant_id}")
        secret_id, secret_key = new_contract_secret()
        row.contract_secret_id = secret_id
        row.contract_secret_key = secret_key
        session.commit()
        return {
            "tenant_id": tenant_id,
            "contract_secret_id": secret_id,
            "contract_secret_key": secret_key,
        }


def delete_tenant(store, tenant_id: str, redis_client: Optional[Any] = None) -> bool:
    with store._session_local() as session:
        row = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        if row is None:
            return False
        flows = session.scalars(
            select(FlowRecord).where(FlowRecord.tenant_id == tenant_id)
        ).all()
        if flows:
            raise TenantError(
                f"租户 {tenant_id} 内仍有 {len(flows)} 个流程，先清空或迁移后再删除"
            )
        for m in session.scalars(
            select(TenantMember).where(TenantMember.tenant_id == tenant_id)
        ).all():
            session.delete(m)
        session.delete(row)
        session.commit()
    # 清掉停用标记：租户 ID 可被重新创建，残留标记会让新租户一出生就被闸
    set_tenant_disabled(redis_client, tenant_id, False)
    return True


def find_tenant_by_contract_secret_id(store, secret_id: str) -> Dict[str, Any]:
    """契约接口按 secret_id 定位租户（未命中抛 LookupError）。

    返回含 contract_secret_key（供验签内部使用；API 层不得外透）。
    """
    with store._session_local() as session:
        row = session.scalars(
            select(Tenant).where(Tenant.contract_secret_id == secret_id)
        ).first()
        if row is None:
            raise LookupError(f"未知契约 secret_id: {secret_id}")
        out = _to_dict(row)
        out["contract_secret_key"] = row.contract_secret_key
        return out


def _to_dict(row: Tenant) -> Dict[str, Any]:
    return {
        "id": row.id,
        "name": row.name,
        "status": row.status,
        "contract_secret_id": row.contract_secret_id,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


# 复用 users_svc 的成员管理（同一张 tenant_members 表），保持单一入口
list_tenant_members = users_svc.list_tenant_members
add_member = users_svc.add_member
set_member_role = users_svc.set_member_role
remove_member = users_svc.remove_member
