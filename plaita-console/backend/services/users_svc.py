"""用户/会话/租户成员服务（RBAC 基础）。

- 密码：PBKDF2-SHA256（210k 迭代），存 ``salt$hash``（stdlib，无额外依赖）
- 会话：随机 32 字节 token，落库只存 SHA-256，有效期 7 天；
  会话携带 ``active_tenant``（多租户成员的当前租户上下文）
- 角色：租户内角色以 ``tenant_members`` 为准（实时查，改角色即时生效）；
  ``users.role`` 仅作无租户上下文时的遗留回退；``users.platform_admin``
  为平台级管理员（跨租户管理）
- 引导：users 表为空时创建首个 admin（平台管理员）
"""
from __future__ import annotations

import hashlib
import logging
import secrets
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import select

try:
    from ..models.flow import DEFAULT_TENANT_ID, SessionToken, Tenant, TenantMember, User
except ImportError:
    from models.flow import (  # type: ignore
        DEFAULT_TENANT_ID,
        SessionToken,
        Tenant,
        TenantMember,
        User,
    )

logger = logging.getLogger(__name__)

ROLES = ("admin", "editor", "viewer")
SESSION_TTL_DAYS = 7
_PBKDF2_ITER = 210_000


class UserError(ValueError):
    """用户/租户管理操作非法（信息面向管理员展示）。"""


# ---- 密码 ----

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), _PBKDF2_ITER).hex()
    return f"{salt}${digest}"


def verify_password(password: str, stored: str) -> bool:
    try:
        salt, digest = stored.split("$", 1)
    except ValueError:
        return False
    calc = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), _PBKDF2_ITER).hex()
    return secrets.compare_digest(calc, digest)


# ---- 引导 ----

def has_any_user(store) -> bool:
    """users 表是否已有用户（决定前端走登录页还是首次启动向导）。"""
    with store._session_local() as session:
        return session.scalars(select(User)).first() is not None


def _ensure_default_tenant_row(store) -> None:
    """确保 default 租户行存在（独立于 flow_store.ensure_tenant_bootstrap，
    供 setup/bootstrap 等非 lifespan 路径使用；幂等）。"""
    with store._session_local() as session:
        if session.scalars(
            select(Tenant).where(Tenant.id == DEFAULT_TENANT_ID)
        ).first() is None:
            session.add(Tenant(id=DEFAULT_TENANT_ID, name="默认租户", status="active"))
            session.commit()


def ensure_bootstrap_user(store) -> Optional[str]:
    """无人值守引导：仅在设置了 PLAITA_CONSOLE_ADMIN_PASSWORD 时自动创建 admin。

    未设置该环境变量时不自动创建——前端进入「创建管理员」向导
    （POST /api/auth/setup），避免随机密码打印到日志的粗糙体验。
    返回明文密码（仅环境变量路径），否则 None。
    首个 admin 即平台管理员，并挂到 default 租户（admin 角色）。
    """
    import os

    env_password = os.environ.get("PLAITA_CONSOLE_ADMIN_PASSWORD")
    if not env_password or has_any_user(store):
        return None
    _ensure_default_tenant_row(store)
    with store._session_local() as session:
        session.add(
            User(
                username="admin",
                password_hash=hash_password(env_password),
                role="admin",
                platform_admin=True,
            )
        )
        session.add(
            TenantMember(username="admin", tenant_id=DEFAULT_TENANT_ID, role="admin")
        )
        session.commit()
    logger.info("已创建初始管理员 admin（密码来自 PLAITA_CONSOLE_ADMIN_PASSWORD）")
    return env_password


def setup_admin(store, username: str, password: str) -> Dict[str, Any]:
    """首次启动向导：创建首个 admin 用户（仅 users 表为空时允许）。
    首个 admin 即平台管理员。"""
    if has_any_user(store):
        raise UserError("管理员已存在，禁止重复初始化")
    _ensure_default_tenant_row(store)
    create_user(store, username, password, "admin", platform_admin=True)
    add_member(store, DEFAULT_TENANT_ID, username, "admin")
    return {"username": username, "role": "admin", "platform_admin": True}


# ---- 登录/会话 ----

def _memberships_of(session, username: str) -> List[Dict[str, str]]:
    rows = session.scalars(
        select(TenantMember).where(TenantMember.username == username)
    ).all()
    return [{"tenant_id": r.tenant_id, "role": r.role} for r in rows]


def login(store, username: str, password: str) -> Optional[Dict[str, Any]]:
    """校验用户名密码，签发会话。失败返回 None。

    成功时返回 {token, username, role, expires_at, platform_admin,
    memberships, active_tenant}；active_tenant 默认取第一个成员关系。
    """
    with store._session_local() as session:
        row = session.scalars(select(User).where(User.username == username)).first()
        if row is None or row.disabled or not verify_password(password, row.password_hash):
            return None
        memberships = _memberships_of(session, row.username)
        active_tenant = memberships[0]["tenant_id"] if memberships else None
        token = uuid.uuid4().hex + secrets.token_hex(16)
        expires = datetime.utcnow() + timedelta(days=SESSION_TTL_DAYS)
        session.add(
            SessionToken(
                token_hash=hashlib.sha256(token.encode()).hexdigest(),
                username=row.username,
                role=row.role,
                active_tenant=active_tenant,
                expires_at=expires,
            )
        )
        session.commit()
        role = _resolve_role(session, row, active_tenant)
        return {
            "token": token,
            "username": row.username,
            "role": role,
            "expires_at": expires.isoformat(),
            "platform_admin": bool(row.platform_admin),
            "memberships": memberships,
            "active_tenant": active_tenant,
        }


def _resolve_role(session, user: User, tenant_id: Optional[str]) -> Optional[str]:
    """角色裁决：活跃租户 → membership.role（实时）；平台管理员 → admin；
    遗留回退 users.role。用户被禁用返回 None。"""
    if user is None or user.disabled:
        return None
    if tenant_id:
        member = session.scalars(
            select(TenantMember).where(
                TenantMember.username == user.username,
                TenantMember.tenant_id == tenant_id,
            )
        ).first()
        if member is not None:
            return member.role
    if user.platform_admin:
        return "admin"
    return user.role


def resolve_session(store, token: str) -> Optional[Dict[str, Any]]:
    """token -> {username, role, tenant_id, platform_admin}；无效/过期返回 None。

    - 角色实时取自 tenant_members（改角色即时生效）
    - active_tenant 的成员资格已失效时自动清空（回退平台/遗留视角）
    """
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    with store._session_local() as session:
        row = session.scalars(
            select(SessionToken).where(SessionToken.token_hash == token_hash)
        ).first()
        if row is None:
            return None
        if row.expires_at < datetime.utcnow():
            session.delete(row)
            session.commit()
            return None
        user = session.scalars(select(User).where(User.username == row.username)).first()
        if user is None or user.disabled:
            session.delete(row)
            session.commit()
            return None
        active_tenant = row.active_tenant
        role = _resolve_role(session, user, active_tenant)
        if active_tenant and not _is_member(session, row.username, active_tenant):
            # 成员资格被移除：清空活跃租户，回退角色
            row.active_tenant = None
            active_tenant = None
            role = _resolve_role(session, user, None)
            session.commit()
        return {
            "username": row.username,
            "role": role,
            "tenant_id": active_tenant,
            "platform_admin": bool(user.platform_admin),
        }


def switch_tenant(store, token: str, tenant_id: str) -> Optional[Dict[str, Any]]:
    """切换会话的活跃租户（须为该租户成员且租户未停用）。"""
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    with store._session_local() as session:
        row = session.scalars(
            select(SessionToken).where(SessionToken.token_hash == token_hash)
        ).first()
        if row is None or row.expires_at < datetime.utcnow():
            raise UserError("会话无效或已过期，请重新登录")
        tenant = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        if tenant is None or tenant.status != "active":
            raise UserError(f"租户不可用: {tenant_id}")
        if not _is_member(session, row.username, tenant_id):
            raise UserError(f"不是租户 {tenant_id} 的成员")
        row.active_tenant = tenant_id
        session.commit()
    return _session_context(store, token)


def _session_context(store, token: str) -> Optional[Dict[str, Any]]:
    """token 明文 -> 登录态上下文（复用 resolve_session，供切换后返回）。"""
    return resolve_session(store, token)


def _is_member(session, username: str, tenant_id: str) -> bool:
    return (
        session.scalars(
            select(TenantMember).where(
                TenantMember.username == username,
                TenantMember.tenant_id == tenant_id,
            )
        ).first()
        is not None
    )


def logout(store, token: str) -> bool:
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    with store._session_local() as session:
        row = session.scalars(
            select(SessionToken).where(SessionToken.token_hash == token_hash)
        ).first()
        if row is None:
            return False
        session.delete(row)
        session.commit()
        return True


# ---- 用户管理 ----

def list_users(store) -> List[Dict[str, Any]]:
    with store._session_local() as session:
        rows = session.scalars(select(User).order_by(User.username)).all()
        memberships: Dict[str, List[Dict[str, str]]] = {}
        for m in session.scalars(select(TenantMember)).all():
            memberships.setdefault(m.username, []).append(
                {"tenant_id": m.tenant_id, "role": m.role}
            )
        return [
            {
                "username": r.username,
                "role": r.role,
                "platform_admin": bool(r.platform_admin),
                "disabled": r.disabled,
                "memberships": memberships.get(r.username, []),
                "created_at": r.created_at.isoformat(),
            }
            for r in rows
        ]


def create_user(
    store,
    username: str,
    password: str,
    role: str,
    platform_admin: bool = False,
    memberships: Optional[List[Dict[str, str]]] = None,
) -> None:
    """创建用户；platform_admin / memberships 仅平台管理员路径使用。"""
    if role not in ROLES:
        raise UserError(f"非法角色: {role}（可选 {'/'.join(ROLES)}）")
    if len(password) < 8:
        raise UserError("密码至少 8 位")
    with store._session_local() as session:
        if session.scalars(select(User).where(User.username == username)).first():
            raise UserError(f"用户已存在: {username}")
        session.add(
            User(
                username=username,
                password_hash=hash_password(password),
                role=role,
                platform_admin=platform_admin,
            )
        )
        for m in memberships or []:
            tenant_id, m_role = m.get("tenant_id", ""), m.get("role", role)
            if m_role not in ROLES:
                raise UserError(f"非法角色: {m_role}")
            if not session.scalars(
                select(Tenant).where(Tenant.id == tenant_id)
            ).first():
                raise UserError(f"租户不存在: {tenant_id}")
            session.add(
                TenantMember(username=username, tenant_id=tenant_id, role=m_role)
            )
        session.commit()


def set_role(store, username: str, role: str) -> None:
    """遗留全局角色（users.role）。租户内角色请走 set_member_role。"""
    if role not in ROLES:
        raise UserError(f"非法角色: {role}")
    with store._session_local() as session:
        row = session.scalars(select(User).where(User.username == username)).first()
        if row is None:
            raise UserError(f"用户不存在: {username}")
        row.role = role
        session.commit()
    _revoke_user_sessions(store, username)


def set_password(store, username: str, password: str) -> None:
    if len(password) < 8:
        raise UserError("密码至少 8 位")
    with store._session_local() as session:
        row = session.scalars(select(User).where(User.username == username)).first()
        if row is None:
            raise UserError(f"用户不存在: {username}")
        row.password_hash = hash_password(password)
        session.commit()
    _revoke_user_sessions(store, username)


def set_platform_admin(store, username: str, platform_admin: bool) -> None:
    with store._session_local() as session:
        row = session.scalars(select(User).where(User.username == username)).first()
        if row is None:
            raise UserError(f"用户不存在: {username}")
        row.platform_admin = bool(platform_admin)
        session.commit()
    _revoke_user_sessions(store, username)


def delete_user(store, username: str) -> bool:
    if username == "admin":
        raise UserError("内置 admin 不可删除（可改密/改角色）")
    with store._session_local() as session:
        row = session.scalars(select(User).where(User.username == username)).first()
        if row is None:
            return False
        for m in session.scalars(
            select(TenantMember).where(TenantMember.username == username)
        ).all():
            session.delete(m)
        session.delete(row)
        session.commit()
    _revoke_user_sessions(store, username)
    return True


# ---- 租户成员管理 ----

def list_tenant_members(store, tenant_id: str) -> List[Dict[str, Any]]:
    with store._session_local() as session:
        rows = session.scalars(
            select(TenantMember).where(TenantMember.tenant_id == tenant_id)
        ).all()
        return [
            {"username": r.username, "role": r.role, "tenant_id": r.tenant_id}
            for r in rows
        ]


def add_member(store, tenant_id: str, username: str, role: str) -> None:
    if role not in ROLES:
        raise UserError(f"非法角色: {role}（可选 {'/'.join(ROLES)}）")
    with store._session_local() as session:
        if session.scalars(
            select(Tenant).where(Tenant.id == tenant_id)
        ).first() is None:
            raise UserError(f"租户不存在: {tenant_id}")
        if session.scalars(
            select(User).where(User.username == username)
        ).first() is None:
            raise UserError(f"用户不存在: {username}")
        if _is_member(session, username, tenant_id):
            raise UserError(f"{username} 已是租户 {tenant_id} 的成员")
        session.add(TenantMember(username=username, tenant_id=tenant_id, role=role))
        session.commit()


def set_member_role(store, tenant_id: str, username: str, role: str) -> None:
    if role not in ROLES:
        raise UserError(f"非法角色: {role}")
    with store._session_local() as session:
        row = session.scalars(
            select(TenantMember).where(
                TenantMember.username == username,
                TenantMember.tenant_id == tenant_id,
            )
        ).first()
        if row is None:
            raise UserError(f"{username} 不是租户 {tenant_id} 的成员")
        row.role = role
        session.commit()
    _revoke_user_sessions(store, username)


def remove_member(store, tenant_id: str, username: str) -> bool:
    with store._session_local() as session:
        row = session.scalars(
            select(TenantMember).where(
                TenantMember.username == username,
                TenantMember.tenant_id == tenant_id,
            )
        ).first()
        if row is None:
            return False
        session.delete(row)
        session.commit()
    _revoke_user_sessions(store, username)
    return True


def _revoke_user_sessions(store, username: str) -> None:
    with store._session_local() as session:
        rows = session.scalars(
            select(SessionToken).where(SessionToken.username == username)
        ).all()
        for row in rows:
            session.delete(row)
        session.commit()
