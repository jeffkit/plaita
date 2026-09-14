"""租户管理服务（平台管理员操作面）。

- 租户 slug 一经创建不可改；contract_secret_* 为对外 HMAC 契约凭证，
  明文仅在创建/轮换的响应中返回一次
- 删除保护：租户内仍有流程时拒绝删除（避免误删带数据的租户）
"""
from __future__ import annotations

import re
import secrets
from typing import Any, Dict, List

from sqlalchemy import select

try:
    from ..models.flow import FlowRecord, Tenant, TenantMember
except ImportError:
    from models.flow import FlowRecord, Tenant, TenantMember  # type: ignore

from . import users_svc

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


def set_tenant_status(store, tenant_id: str, status: str) -> None:
    if status not in ("active", "disabled"):
        raise TenantError(f"非法状态: {status}")
    with store._session_local() as session:
        row = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        if row is None:
            raise TenantError(f"租户不存在: {tenant_id}")
        row.status = status
        session.commit()


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


def delete_tenant(store, tenant_id: str) -> bool:
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
