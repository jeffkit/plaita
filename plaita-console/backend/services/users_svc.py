"""用户/会话/租户成员服务（RBAC 基础）。

- 密码：PBKDF2-SHA256（210k 迭代），存 ``salt$hash``（stdlib，无额外依赖）
- 会话：随机 32 字节 token，落库只存 SHA-256，有效期 7 天；
  会话携带 ``active_tenant``（多租户成员的当前租户上下文）
- 角色：租户内角色以 ``tenant_members`` 为准（实时查，改角色即时生效）；
  ``users.platform_admin`` 为平台级管理员（跨租户管理）。**无活跃租户的
  普通用户不再回退 ``users.role`` 遗留全局角色（#254）**：无成员关系即无
  角色，鉴权层 fail-closed 403。
- 引导：users 表为空时创建首个 admin（平台管理员）
"""
from __future__ import annotations

import hashlib
import logging
import secrets
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select

try:
    from ..models.flow import DEFAULT_TENANT_ID, SessionToken, Tenant, TenantMember, User
    from .tenant_write_guard import (
        assert_tenant_generation,
        expected_tenant_generation,
        tenant_generation_token,
    )
except ImportError:
    from models.flow import (  # type: ignore
        DEFAULT_TENANT_ID,
        SessionToken,
        Tenant,
        TenantMember,
        User,
    )
    from services.tenant_write_guard import (  # type: ignore
        assert_tenant_generation,
        expected_tenant_generation,
        tenant_generation_token,
    )

logger = logging.getLogger(__name__)

ROLES = ("admin", "editor", "viewer")
SESSION_TTL_DAYS = 7
_PBKDF2_ITER = 210_000


class UserError(ValueError):
    """用户/租户管理操作非法（信息面向管理员展示）。"""


class TenantDisabledError(Exception):
    """会话的活跃租户已停用（#27）——鉴权层据此返回 403，而非按无效会话 401。"""


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
        # 优先钉到一个未停用租户（#27）：唯一成员租户被停用时用户本就会被闸门
        # 拦下；但多租户用户若被钉到已停用的第一个成员关系，会连 switch-tenant
        # （需先过鉴权）都调不动——优先活跃租户避免这种自锁。
        active_tenant = next(
            (
                m["tenant_id"]
                for m in memberships
                if _tenant_active(session, m["tenant_id"])
            ),
            memberships[0]["tenant_id"] if memberships else None,
        )
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
    """角色裁决：活跃租户 → membership.role（实时）；平台管理员 → admin。

    #254 fail-closed：无租户上下文的普通用户**不再回退** ``users.role``
    遗留全局角色——返回 None（无角色）。被移出租户的成员重登后因此无法
    凭遗留角色进入管理面；鉴权层对无活跃租户的普通会话直接 403。
    """
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
    return None


def _tenant_active(session, tenant_id: Optional[str]) -> bool:
    """租户是否可运行（行缺失视为活跃，兼容引导/存量）。"""
    if not tenant_id:
        return True
    row = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
    return row is None or row.status == "active"


def tenant_is_active(store, tenant_id: Optional[str]) -> bool:
    """供 console 侧（本地调度、触发 API）判断租户是否已停用。"""
    if not tenant_id:
        return True
    with store._session_local() as session:
        return _tenant_active(session, tenant_id)


def tenant_is_executable(store, tenant_id: Optional[str]) -> bool:
    """运行面严格租户闸：非 default 租户行缺失视为**已删除**（#255 / A48-04）。

    与 ``tenant_is_active`` 的区别只有「行缺失」一档：后者对缺失行 fail-open
    是 #27/#237 的引导/存量兼容语义；但本地调度快照可能持有删除前的行，此时
    Tenant 行已删、local_schedules 尚未被 404 补清扫，lenient 闸会让已删除
    租户再执行一次。default 行缺失无法区分「已删除」与「尚未 bootstrap」，
    因此 default 保留 fail-open（与 #255 清理口径一致）。
    """
    if not tenant_id or tenant_id == DEFAULT_TENANT_ID:
        return tenant_is_active(store, tenant_id)
    with store._session_local() as session:
        row = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        return row is not None and row.status == "active"


def resolve_session(store, token: str) -> Optional[Dict[str, Any]]:
    """token -> {username, role, tenant_id, tenant_generation, platform_admin}。

    - 角色实时取自 tenant_members（改角色即时生效）
    - **A51-02**：成员资格 + 角色 + ``Tenant.created_at/contract_secret_id``
      收成**一条 outer JOIN**。不能先查成员再单独读 Tenant——两条语句之间
      仍可插入 delete+同名 create，旧请求随后会以「新租户成员」身份通过，
      或把新世代捕获成自己的。单条语句给出同一快照里的成员关系与世代。
    - active_tenant 的成员资格已失效时自动清空；普通用户随后无角色
      （#254 fail-closed，不回落 ``users.role``），需重新登录/重新授权
    - active_tenant 已停用且非平台管理员 → 抛 TenantDisabledError（#27，鉴权层
      转 403）；平台管理员不受限，否则无法把租户重新启用
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
        role = None
        tenant_generation = None
        tenant_status = None
        membership = None
        if active_tenant:
            membership = session.execute(
                select(
                    TenantMember.role,
                    Tenant.created_at,
                    Tenant.contract_secret_id,
                    Tenant.status,
                )
                .select_from(TenantMember)
                .outerjoin(Tenant, Tenant.id == TenantMember.tenant_id)
                .where(
                    TenantMember.username == row.username,
                    TenantMember.tenant_id == active_tenant,
                )
            ).first()
        if membership is not None:
            role = membership[0]
            if membership[2] is not None:
                tenant_generation = (membership[1], membership[2])
            tenant_status = membership[3]
        elif user.platform_admin:
            role = "admin"
        if active_tenant and membership is None:
            # 成员资格被移除：清空活跃租户；普通用户随之无角色（#254）
            row.active_tenant = None
            active_tenant = None
            tenant_generation = None
            role = "admin" if user.platform_admin else None
            session.commit()
        if (
            active_tenant
            and not user.platform_admin
            and tenant_status is not None
            and tenant_status != "active"
        ):
            # 租户已停用：已签发会话立即失效（不落库、不改行，鉴权层转 403）
            raise TenantDisabledError(f"租户已停用: {active_tenant}")
        return {
            "username": row.username,
            "role": role,
            "tenant_id": active_tenant,
            "tenant_generation": tenant_generation,
            "platform_admin": bool(user.platform_admin),
        }


def tenant_table_initialized(store) -> bool:
    """租户表是否已 bootstrap（至少一行）；空表 = 内嵌/测试挂载。

    生产 ``lifespan`` 先 ``ensure_tenant_bootstrap`` 再挂路由，因此空表只可能
    出现在直接装配 router 的测试/内嵌场景。认证侧据此区分「已初始化但租户
    不存在」（拒绝）与「未初始化表」（跳过世代捕获并 WARNING）。
    """
    with store._session_local() as session:
        return session.query(Tenant).count() > 0


def read_tenant_generation(store, tenant_id: str) -> Optional[Tuple[Any, Any]]:
    """只读一次租户行并返回世代令牌（平台身份带 X-Tenant-ID 时用）。

    认证路径不持写锁：SQLite 快照读即可，PG 也不加 ``FOR UPDATE``（读侧不
    参与删除串行化；写路径自己的 flush 后复核才需要锁）。
    """
    with store._session_local() as session:
        row = session.execute(
            select(Tenant.created_at, Tenant.contract_secret_id).where(
                Tenant.id == tenant_id
            )
        ).first()
        if row is None or row[1] is None:
            return None
        return (row[0], row[1])


def switch_tenant(store, token: str, tenant_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """切换会话的活跃租户；``tenant_id=None`` 显式清空为平台全量视角。

    - 具体租户：须为该租户成员且租户未停用（既有语义）。
    - ``None``（A2R17-01）：仅 ``users.platform_admin`` 可调用——把会话
      ``active_tenant`` 置空，后续请求读跨租户、写被 ``tenant_scope(required=True)``
      拒绝并引导选租户。普通用户显式切换全量一律拒绝（绝不把 None 当全量，
      对齐 #254 fail-closed）。
    """
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    with store._session_local() as session:
        row = session.scalars(
            select(SessionToken).where(SessionToken.token_hash == token_hash)
        ).first()
        if row is None or row.expires_at < datetime.utcnow():
            raise UserError("会话无效或已过期，请重新登录")
        if tenant_id is None:
            user = session.scalars(select(User).where(User.username == row.username)).first()
            if user is None or user.disabled:
                raise UserError("会话无效或已过期，请重新登录")
            if not user.platform_admin:
                # 普通用户没有平台全量语义（#254）；具名拒绝，避免把一次
                # 非法切换静默成「保持原租户」或「无租户」。
                raise UserError("仅平台管理员可切换到全量视角")
            row.active_tenant = None
        else:
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

def list_users(store, tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """用户列表。给 ``tenant_id`` 时只返回该租户成员（#260 租户 admin 读面）。

    A2R45-03：成员的 ``memberships`` 同样收窄到该租户——用户行按租户筛过，
    但成员关系若仍是全表，同时属于多租户的用户会把他租户的 (租户 id, 角色)
    带进租户 admin 的视图（#260 只堵了用户名枚举这一半）。
    """
    with store._session_local() as session:
        rows = session.scalars(select(User).order_by(User.username)).all()
        memberships: Dict[str, List[Dict[str, str]]] = {}
        for m in session.scalars(select(TenantMember)).all():
            if tenant_id is not None and m.tenant_id != tenant_id:
                continue
            memberships.setdefault(m.username, []).append(
                {"tenant_id": m.tenant_id, "role": m.role}
            )
        result = [
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
        if tenant_id is None:
            return result
        return [u for u in result if u["memberships"]]


def is_platform_admin(store, username: str) -> bool:
    """目标账号是否为平台管理员（#260 租户 admin 不得触达平台账号）。"""
    with store._session_local() as session:
        row = session.scalars(select(User).where(User.username == username)).first()
        return bool(row is not None and row.platform_admin)


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


def delete_user(store, username: str, *, allow_platform_admin: bool = False) -> bool:
    """删除用户。

    #260 纵深防御：
    - 删除平台管理员账号必须经平台身份路径显式放行（``allow_platform_admin``）
    - 无论操作者是谁，都不允许删掉**最后一个**平台管理员（否则平台失去治理入口）
    """
    if username == "admin":
        raise UserError("内置 admin 不可删除（可改密/改角色）")
    with store._session_local() as session:
        row = session.scalars(select(User).where(User.username == username)).first()
        if row is None:
            return False
        if row.platform_admin:
            if not allow_platform_admin:
                raise UserError("平台管理员账号仅可由平台管理员删除")
            remaining = session.scalars(
                select(User).where(User.platform_admin.is_(True))
            ).all()
            if len(remaining) <= 1:
                raise UserError("不可删除最后一个平台管理员")
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
        tenant = session.scalars(
            select(Tenant).where(Tenant.id == tenant_id)
        ).first()
        if tenant is None:
            raise UserError(f"租户不存在: {tenant_id}")
        # 记住本次校验通过的租户世代；同名重建会换 token（A50-02 ABA）。
        # A51-02：请求级绑定优先——认证时的世代才是本请求的身份；服务首次
        # 读若发生在 delete+create 之后，不能把它捕获成「自己的」世代。
        tenant_token = (
            expected_tenant_generation(tenant_id)
            or (tenant.created_at, tenant.contract_secret_id)
        )
        if session.scalars(
            select(User).where(User.username == username)
        ).first() is None:
            raise UserError(f"用户不存在: {username}")
        if _is_member(session, username, tenant_id):
            raise UserError(f"{username} 已是租户 {tenant_id} 的成员")
        session.add(TenantMember(username=username, tenant_id=tenant_id, role=role))
        session.flush()
        # A50-02：flush 取得写串行化点后在同一事务内复核租户世代。并发
        # delete 已提交 ⇒ None；delete+同名 create 已提交 ⇒ 新 token。两种
        # 情况成员行都随事务回滚；尚未提交 ⇒ 删除侧等本事务结束后会显式
        # 删掉全部成员行（SQLite 的 FK 级联默认不启用，不能依赖声明层）。
        # 上面的租户检查只负责快速拒绝。
        current_token = tenant_generation_token(session, tenant_id)
        if current_token is None or current_token != tenant_token:
            raise UserError(f"租户不存在: {tenant_id}")
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
        # A51-02：写（flush 取得串行化点）之后、同一事务内复核请求世代。
        session.flush()
        assert_tenant_generation(session, tenant_id)
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
        session.flush()
        assert_tenant_generation(session, tenant_id)
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
