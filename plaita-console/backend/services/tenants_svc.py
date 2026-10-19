"""租户管理服务（平台管理员操作面）。

- 租户 slug 一经创建不可改；contract_secret_* 为对外 HMAC 契约凭证，
  明文仅在创建/轮换的响应中返回一次
- 删除保护：租户内仍有流程时拒绝删除（避免误删带数据的租户）
- 删除清理：凭据（Credential 行 + 引擎导出旁文件）随租户一并删除，
  否则重建同名租户会继承旧密钥（#245）
"""
from __future__ import annotations

import json
import logging
import re
import secrets
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import select

try:
    from ..models.flow import (
        Credential,
        DEFAULT_TENANT_ID,
        FlowRecord,
        Tenant,
        TenantMember,
    )
except ImportError:
    from models.flow import (  # type: ignore
        Credential,
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


def set_tenant_status(store, tenant_id: str, status: str, redis_client=None) -> None:
    """启用/停用租户（#27：停用必须对运行面生效，不能只改 SQLite 行）。

    - 停用：同步停掉该租户的调度（本地 + 集群档）；其已签发会话由
      ``users_svc.resolve_session`` 的租户状态闸按请求实时拦下（403）
    - 状态写入 Redis 短视图，供 worker/调度服务（可能在别的进程）跨进程拦截
    """
    if status not in ("active", "disabled"):
        raise TenantError(f"非法状态: {status}")
    with store._session_local() as session:
        row = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        if row is None:
            raise TenantError(f"租户不存在: {tenant_id}")
        row.status = status
        session.commit()
    if status == "disabled":
        _disable_tenant_schedules(store, tenant_id, redis_client)
    _publish_status(redis_client, tenant_id, status)


def _disable_tenant_schedules(store, tenant_id: str, redis_client) -> None:
    """停用租户时同步停掉其调度（本地 SQLite 档 + 集群 Redis HASH 档）。"""
    try:
        from . import local_scheduler
    except ImportError:
        local_scheduler = None  # type: ignore
    if local_scheduler is not None:
        for s in local_scheduler.list_schedules(store):
            if (s.get("tenant_id") or DEFAULT_TENANT_ID) != tenant_id:
                continue
            if not s.get("enabled"):
                continue
            s["enabled"] = False
            s["next_run_at"] = ""
            local_scheduler.update_schedule(store, s["schedule_id"], s)

    if redis_client is None:
        return
    try:
        from plaita.server.services.schedule_service import SCHEDULES_KEY, list_schedules
    except ImportError:
        return
    for s in list_schedules(redis_client):
        if (s.get("tenant_id") or DEFAULT_TENANT_ID) != tenant_id:
            continue
        if not s.get("enabled"):
            continue
        s["enabled"] = False
        s["next_run_at"] = ""
        s["updated_at"] = datetime.now().isoformat()
        redis_client.hset(
            SCHEDULES_KEY,
            key=s["schedule_id"],
            value=json.dumps(s, ensure_ascii=False),
        )


def _publish_status(redis_client, tenant_id: str, status: str) -> None:
    if redis_client is None:
        return
    try:
        from plaita.server.tenant_status import publish_tenant_status
    except ImportError:
        return
    publish_tenant_status(redis_client, tenant_id, status)


def publish_all_tenant_status(redis_client, store) -> None:
    """启动兜底：把权威库里的全部租户状态回填到 Redis 短视图。

    补齐「console 停机期间改过状态 / Redis 被清空」的漂移——否则 worker 侧
    只有 console 运行时改动的状态可见。失败静默（不阻断启动）。
    """
    if redis_client is None:
        return
    try:
        from plaita.server.tenant_status import publish_tenant_status
    except ImportError:
        return
    for t in list_tenants(store):
        publish_tenant_status(redis_client, t["id"], t["status"])


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


def delete_tenant(store, tenant_id: str, redis_client=None) -> bool:
    purged_credentials = 0
    with store._session_local() as session:
        row = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        if row is None:
            # plaita#257：旧实现先 commit 删租户行、后删凭据旁文件；unlink
            # 失败后租户已不存在，重试只拿到 404（「租户已删、旧密钥仍可被
            # 引擎读到」的死路）。404 路径补清扫这份残留——仍删除失败则显式
            # 上抛，不得把「没清掉」伪装成「已不存在」。
            _remove_tenant_credentials_export(tenant_id)
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
        # #245：凭据行与租户同事务删除——只删 Tenant 行会让重建的同名租户
        # 通过 list/get 直接继承旧密钥。
        credentials = session.scalars(
            select(Credential).where(Credential.tenant_id == tenant_id)
        ).all()
        for c in credentials:
            session.delete(c)
        purged_credentials = len(credentials)
        session.delete(row)
        # plaita#257：删除先落到事务（flush），凭据旁文件（含密钥）清成功
        # 之后才 commit。失败方向 fail-safe：宁可「租户行还在而凭据不可达」
        # （重试可继续清理），也不可「租户已删而旧密钥仍可读」。
        session.flush()
        credentials_export = _remove_tenant_credentials_export(tenant_id)
        session.commit()

    if purged_credentials:
        logger.info(
            "删除租户 %s：已清理 %d 条凭据及导出文件 %s",
            tenant_id,
            purged_credentials,
            credentials_export,
        )

    if redis_client is not None:
        try:
            from plaita.server.tenant_status import clear_tenant_status
        except ImportError:
            clear_tenant_status = None  # type: ignore
        if clear_tenant_status is not None:
            clear_tenant_status(redis_client, tenant_id)
    return True


def _tenant_credentials_export(tenant_id: str) -> Path:
    """租户凭据导出路径（与 console 导出/worker 读取共用同一命名，#215）。"""
    try:
        from . import credentials_svc
    except ImportError:
        from services import credentials_svc  # type: ignore
    return credentials_svc.credentials_file(tenant_id)


def _remove_tenant_credentials_export(tenant_id: str) -> Path:
    path = _tenant_credentials_export(tenant_id)
    try:
        path.unlink()
    except FileNotFoundError:
        return path
    except OSError:
        logger.exception(
            "租户 %s 已删除，但凭据导出文件清理失败: %s", tenant_id, path
        )
        raise
    return path


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
