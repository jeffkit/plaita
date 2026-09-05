"""用户与会话服务（RBAC 基础）。

- 密码：PBKDF2-SHA256（210k 迭代），存 ``salt$hash``（stdlib，无额外依赖）
- 会话：随机 32 字节 token，落库只存 SHA-256，有效期 7 天
- 引导：users 表为空时创建首个 admin——密码取 ``PLAITA_CONSOLE_ADMIN_PASSWORD``，
  未设置则随机生成并一次性打印到日志（提醒修改）
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
    from ..models.flow import SessionToken, User
except ImportError:
    from models.flow import SessionToken, User  # type: ignore

logger = logging.getLogger(__name__)

ROLES = ("admin", "editor", "viewer")
SESSION_TTL_DAYS = 7
_PBKDF2_ITER = 210_000


class UserError(ValueError):
    """用户管理操作非法（信息面向管理员展示）。"""


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


def ensure_bootstrap_user(store) -> Optional[str]:
    """无人值守引导：仅在设置了 PLAITA_CONSOLE_ADMIN_PASSWORD 时自动创建 admin。

    未设置该环境变量时不自动创建——前端进入「创建管理员」向导
    （POST /api/auth/setup），避免随机密码打印到日志的粗糙体验。
    返回明文密码（仅环境变量路径），否则 None。
    """
    import os

    env_password = os.environ.get("PLAITA_CONSOLE_ADMIN_PASSWORD")
    if not env_password or has_any_user(store):
        return None
    with store._session_local() as session:
        session.add(
            User(username="admin", password_hash=hash_password(env_password), role="admin")
        )
        session.commit()
    logger.info("已创建初始管理员 admin（密码来自 PLAITA_CONSOLE_ADMIN_PASSWORD）")
    return env_password


def setup_admin(store, username: str, password: str) -> Dict[str, Any]:
    """首次启动向导：创建首个 admin 用户（仅 users 表为空时允许）。"""
    if has_any_user(store):
        raise UserError("管理员已存在，禁止重复初始化")
    create_user(store, username, password, "admin")
    return {"username": username, "role": "admin"}


# ---- 登录/会话 ----

def login(store, username: str, password: str) -> Optional[Dict[str, Any]]:
    """校验用户名密码，签发会话。失败返回 None。"""
    with store._session_local() as session:
        row = session.scalars(select(User).where(User.username == username)).first()
        if row is None or row.disabled or not verify_password(password, row.password_hash):
            return None
        token = uuid.uuid4().hex + secrets.token_hex(16)
        expires = datetime.utcnow() + timedelta(days=SESSION_TTL_DAYS)
        session.add(
            SessionToken(
                token_hash=hashlib.sha256(token.encode()).hexdigest(),
                username=row.username,
                role=row.role,
                expires_at=expires,
            )
        )
        session.commit()
        return {"token": token, "username": row.username, "role": row.role,
                "expires_at": expires.isoformat()}


def resolve_session(store, token: str) -> Optional[Dict[str, Any]]:
    """token -> {username, role}；无效/过期返回 None（并顺手清理过期会话）。"""
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
        # 角色以 users 表当前值为准（改角色后旧会话即时生效）
        user = session.scalars(select(User).where(User.username == row.username)).first()
        role = user.role if user and not user.disabled else None
        if role is None:
            session.delete(row)
            session.commit()
            return None
        return {"username": row.username, "role": role}


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
        return [
            {
                "username": r.username,
                "role": r.role,
                "disabled": r.disabled,
                "created_at": r.created_at.isoformat(),
            }
            for r in rows
        ]


def create_user(store, username: str, password: str, role: str) -> None:
    if role not in ROLES:
        raise UserError(f"非法角色: {role}（可选 {'/'.join(ROLES)}）")
    if len(password) < 8:
        raise UserError("密码至少 8 位")
    with store._session_local() as session:
        if session.scalars(select(User).where(User.username == username)).first():
            raise UserError(f"用户已存在: {username}")
        session.add(User(username=username, password_hash=hash_password(password), role=role))
        session.commit()


def set_role(store, username: str, role: str) -> None:
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


def delete_user(store, username: str) -> bool:
    if username == "admin":
        raise UserError("内置 admin 不可删除（可改密/改角色）")
    with store._session_local() as session:
        row = session.scalars(select(User).where(User.username == username)).first()
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
