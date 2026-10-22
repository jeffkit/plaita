"""登录/会话 与 用户管理 API。

- POST /api/auth/login    用户名密码换会话 token（无需认证）
- GET  /api/auth/me       当前身份（需认证）
- POST /api/auth/switch-tenant  切换会话活跃租户（需认证）
- POST /api/auth/logout   注销当前会话（需认证）
- GET  /api/users         用户列表（admin；租户 admin 仅见本租户成员）
- POST /api/users         创建用户（平台管理员）
- POST /api/users/{u}/role      改遗留全局角色（平台管理员）
- POST /api/users/{u}/password  重置密码（平台管理员；本人可自助改密）
- DELETE /api/users/{u}         删除用户（平台管理员）
"""

import hashlib
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

try:
    from ..request_models import EmptyRequestModel, StrictRequestModel
except ImportError:  # 顶层 ``api`` 包（测试/脚本模式）
    from request_models import EmptyRequestModel, StrictRequestModel  # type: ignore

try:
    from ..auth import (
        NO_ACTIVE_TENANT_DETAIL,
        ensure_platform_admin,
        require_auth,
    )
    from ..services import login_guard, tenants_svc, users_svc
    from ..services.flow_store import get_flow_store
except ImportError:
    from auth import (  # type: ignore
        NO_ACTIVE_TENANT_DETAIL,
        ensure_platform_admin,
        require_auth,
    )
    from services import login_guard, tenants_svc, users_svc  # type: ignore
    from services.flow_store import get_flow_store  # type: ignore

try:  # A2R24-01：统一错误 detail 形状
    from ..error_contract import error_detail
except ImportError:
    from error_contract import error_detail  # type: ignore

# plaita#151：异常/外部文本入日志前统一清洗（共享实现，不在此另写一份）
from plaita.core.url_redaction import _scrub_url_text

logger = logging.getLogger(__name__)

router = APIRouter()

#: 登录失败日志里只出现用户名指纹，不回显明文账号（#208 建议 3）
_USER_FP_LEN = 12


def _client_ip(request: Request) -> str:
    """登录限速维度之一：解析真实客户端地址（B112-01）。

    默认只用 ASGI peer；仅当 peer 在 ``PLAITA_TRUSTED_PROXIES`` 内才读
    ``X-Forwarded-For``，并从右向左取第一个 untrusted 地址。裸信 XFF 等于
    把限速交给攻击者（可伪造）。
    """
    client = getattr(request, "client", None)
    peer = getattr(client, "host", "") or "unknown"
    return login_guard.client_ip_from(
        peer, request.headers.get("x-forwarded-for")
    )


def _user_fingerprint(username: str) -> str:
    """用户名指纹：日志可关联同一账号，但不落明文（口令更不落）。"""
    digest = hashlib.sha256((username or "").encode("utf-8")).hexdigest()
    return digest[:_USER_FP_LEN]


class LoginRequest(StrictRequestModel):
    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)


class CreateUserRequest(StrictRequestModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=8)
    role: str = "viewer"
    platform_admin: bool = False
    memberships: Optional[List[Dict[str, str]]] = Field(
        None, description="租户成员关系 [{tenant_id, role}]（仅平台管理员可设置）"
    )


class RoleRequest(StrictRequestModel):
    role: str


class PasswordRequest(StrictRequestModel):
    password: str = Field(..., min_length=8)


class SwitchTenantRequest(StrictRequestModel):
    # A2R17-01：``null`` 是「平台管理员显式切到全量视角（只读）」的合法值；
    # 字段本身仍必填——缺省/空串都不解释成全量，避免客户端漏传时静默清租户。
    tenant_id: Optional[str] = Field(..., min_length=1)


@router.get("/auth/setup-status")
def setup_status():
    """首次启动向导探测：users 为空 = 需要初始化管理员。"""
    return {"needs_setup": not users_svc.has_any_user(get_flow_store())}


@router.post("/auth/setup")
def setup(req: CreateUserRequest, request: Request):
    """创建首个管理员（仅 users 表为空时可用），成功直接返回会话。"""
    try:
        info = users_svc.setup_admin(get_flow_store(), req.username, req.password)
    except users_svc.UserError as e:
        raise HTTPException(status_code=409, detail=error_detail("user_conflict", str(e)))
    session = users_svc.login(get_flow_store(), req.username, req.password)
    _set_audit_identity(request, session)
    _audit(request, "auth.setup", req.username,
           {"role": session.get("role")},
           tenant_id=session.get("active_tenant") or "")
    return session


def _audit_break_glass(
    request: Request, username: str, client_ip: str, *,
    allowed: bool, reason: str, outcome: str,
) -> None:
    """#294：break-glass 每次尝试独立审计（不按窗口合并）。"""
    try:
        from ..services import audit as audit_svc
    except ImportError:
        from services import audit as audit_svc  # type: ignore
    try:
        audit_svc.record(
            request,
            action="auth.login_break_glass",
            resource="auth",
            resource_id=username,
            detail={
                "break_glass": True,
                "allowed": bool(allowed),
                "reason": reason,
                "outcome": outcome,
                "username": username,
                "ip": client_ip,
            },
            actor=username,
            tenant_id="",
        )
    except Exception as exc:  # noqa: BLE001 — 审计不阻断登录
        logger.warning("break-glass 审计失败: %s", _scrub_url_text(exc))


@router.post("/auth/login")
def login(req: LoginRequest, request: Request):
    """用户名密码换会话 token。

    #208/r64：三维限速；锁定期内正确/错误口令都 429。
    #294：break-glass 必须同时满足用户白名单 + 来源 IP 白名单 + 一次性
    token；失败照常计数，每次尝试独立审计（break_glass=true）。
    """
    client_ip = _client_ip(request)
    redis_client = getattr(request.app.state, "redis", None)
    bg_allowed, bg_reason, bg_requested = login_guard.validate_break_glass(
        req.username,
        request.headers.get("x-break-glass-token"),
        client_ip,
        redis=redis_client,
    )
    if bg_requested and not bg_allowed:
        logger.warning(
            "break-glass 未生效（%s，回落到常规限速）: ip=%s user=%s",
            bg_reason, _scrub_url_text(client_ip), _user_fingerprint(req.username),
        )
    if not bg_allowed:
        retry_after = login_guard.retry_after(
            client_ip, req.username, redis=redis_client
        )
        if retry_after:
            if bg_requested:
                _audit_break_glass(
                    request, req.username, client_ip,
                    allowed=False, reason=bg_reason, outcome="locked",
                )
            _audit_login_failed(request, req.username, client_ip, "locked")
            raise HTTPException(
                status_code=429,
                detail="登录尝试过于频繁，请稍后再试",
                headers={"Retry-After": str(retry_after)},
            )
    info = users_svc.login(get_flow_store(), req.username, req.password)
    if info is None:
        # break-glass 失败也必须计数（不允许无限爆破）
        locked_for = login_guard.record_failure(
            client_ip, req.username, redis=redis_client
        )
        logger.warning(
            "登录失败 ip=%s user=%s%s",
            _scrub_url_text(client_ip),
            _user_fingerprint(req.username),
            f"（失败次数超限，已锁定 {locked_for}s）" if locked_for else "",
        )
        if bg_requested:
            _audit_break_glass(
                request, req.username, client_ip,
                allowed=bg_allowed, reason=bg_reason, outcome="bad_credentials",
            )
        _audit_login_failed(
            request, req.username, client_ip,
            "locked" if locked_for else "bad_credentials",
        )
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    login_guard.reset(client_ip, req.username, redis=redis_client)
    if bg_requested:
        _audit_break_glass(
            request, req.username, client_ip,
            allowed=bg_allowed, reason=bg_reason, outcome="ok",
        )
    _set_audit_identity(request, info)
    _audit(request, "auth.login", info.get("username", req.username),
           {"role": info.get("role")},
           tenant_id=info.get("active_tenant") or "")
    return info


@router.get("/auth/me")
def me(request: Request, identity: Dict = Depends(require_auth)):
    out = dict(identity)
    out["memberships"] = _memberships(identity.get("actor", ""))
    return out


def _memberships(username: str) -> List[Dict[str, str]]:
    """当前用户的全部租户成员关系（切换器数据源）。"""
    try:
        from ..models.flow import TenantMember
    except ImportError:
        from models.flow import TenantMember  # type: ignore
    store = get_flow_store()
    with store._session_local() as session:
        rows = session.query(TenantMember).filter(
            TenantMember.username == username
        ).all()
        return [{"tenant_id": r.tenant_id, "role": r.role} for r in rows]


@router.post("/auth/switch-tenant")
def switch_tenant(req: SwitchTenantRequest, request: Request,
                  identity: Dict = Depends(require_auth)):
    auth_header = request.headers.get("Authorization") or ""
    token = auth_header.strip().split(None, 1)[-1]
    try:
        context = users_svc.switch_tenant(get_flow_store(), token, req.tenant_id)
    except users_svc.UserError as e:
        raise HTTPException(status_code=400, detail=error_detail("tenant_switch_invalid", str(e)))
    _audit(request, "auth.switch_tenant", req.tenant_id or "(full-view)",
           {"full_view": req.tenant_id is None},
           tenant_id=req.tenant_id or "")
    return context


@router.post("/auth/logout")
def logout(request: Request, identity: Dict = Depends(require_auth),
           body: Optional[EmptyRequestModel] = None):
    auth_header = request.headers.get("Authorization") or ""
    token = auth_header.strip().split(None, 1)[-1]
    users_svc.logout(get_flow_store(), token)
    _audit(request, "auth.logout", identity.get("actor", ""),
           {"actor": identity.get("actor", "")},
           tenant_id=identity.get("tenant_id") or "")
    return {"success": True}


@router.get("/users")
def list_users(identity: Dict = Depends(require_auth)):
    """用户列表。平台管理员看全量；租户 admin 只看本租户成员（#260）。

    租户 admin 曾是「全局用户清单」的旁路（可直接枚举平台管理员用户名）；
    ``_ADMIN_PREFIXES`` 仍拦住 editor/viewer，本函数再按租户收窄读面。
    """
    store = get_flow_store()
    if identity.get("platform_admin"):
        return {"users": users_svc.list_users(store)}
    tenant_id = identity.get("tenant_id")
    if not tenant_id:
        # 普通会话无活跃租户在 require_auth 已 fail-closed；这里兜底不降级
        raise HTTPException(status_code=403, detail=NO_ACTIVE_TENANT_DETAIL)
    return {"users": users_svc.list_users(store, tenant_id=tenant_id)}


@router.post("/users")
def create_user(req: CreateUserRequest, request: Request, identity: Dict = Depends(require_auth)):
    ensure_platform_admin(
        request, identity, action="user.create", resource="user", target=req.username
    )
    try:
        users_svc.create_user(
            get_flow_store(), req.username, req.password, req.role,
            platform_admin=req.platform_admin,
            memberships=req.memberships,
        )
    except users_svc.UserError as e:
        raise HTTPException(status_code=400, detail=error_detail("user_invalid", str(e)))
    _audit(request, "user.create", req.username,
           {"role": req.role, "platform_admin": req.platform_admin},
           tenant_id="")
    return {"success": True, "username": req.username}


@router.post("/users/{username}/role")
def set_role(username: str, req: RoleRequest, request: Request,
             identity: Dict = Depends(require_auth)):
    ensure_platform_admin(
        request, identity, action="user.set_role", resource="user", target=username
    )
    try:
        users_svc.set_role(get_flow_store(), username, req.role)
    except users_svc.UserError as e:
        raise HTTPException(status_code=400, detail=error_detail("user_invalid", str(e)))
    _audit(request, "user.set_role", username, {"role": req.role},
           tenant_id="")
    return {"success": True, "username": username, "role": req.role}


@router.post("/users/{username}/password")
def set_password(username: str, req: PasswordRequest, request: Request,
                 identity: Dict = Depends(require_auth)):
    """重置密码：平台管理员可改任意账号；其余角色只能改自己（自助）。"""
    if username != identity.get("actor"):
        ensure_platform_admin(
            request, identity, action="user.set_password", resource="user",
            target=username,
        )
    try:
        users_svc.set_password(get_flow_store(), username, req.password)
    except users_svc.UserError as e:
        raise HTTPException(status_code=400, detail=error_detail("user_invalid", str(e)))
    _audit(request, "user.set_password", username, {}, tenant_id="")
    return {"success": True, "username": username}


@router.delete("/users/{username}")
def delete_user(username: str, request: Request,
                identity: Dict = Depends(require_auth),
                body: Optional[EmptyRequestModel] = None):
    ensure_platform_admin(
        request, identity, action="user.delete", resource="user", target=username
    )
    try:
        deleted = users_svc.delete_user(
            get_flow_store(), username, allow_platform_admin=True
        )
    except users_svc.UserError as e:
        raise HTTPException(status_code=400, detail=error_detail("user_invalid", str(e)))
    if not deleted:
        raise HTTPException(status_code=404, detail=f"用户不存在: {username}")
    _audit(request, "user.delete", username, {}, tenant_id="")
    return {"success": True, "username": username}


def _audit(request: Request, action: str, resource_id: str, detail: Dict,
           *, tenant_id: Optional[str] = None) -> None:
    """审计钩子（users 模块内置埋点；audit 服务缺失时静默跳过）。"""
    try:
        from ..services import audit as audit_svc
    except ImportError:
        try:
            from services import audit as audit_svc  # type: ignore
        except ImportError:
            return
    audit_svc.record(
        request,
        action=action,
        resource="user",
        resource_id=resource_id,
        detail=detail,
        tenant_id=tenant_id,
    )


def _set_audit_identity(request: Request, session: Dict[str, Any]) -> None:
    """登录/初始化成功后把身份写进 state，供审计（actor/租户/角色）。"""
    request.state.actor = session.get("username") or ""
    request.state.tenant_id = session.get("active_tenant") or ""
    request.state.role = session.get("role")
    request.state.auth_source = "session"


def _audit_login_failed(request: Request, username: str, client_ip: str,
                        reason: str) -> None:
    """失败登录审计（A55-S2）：只记 username/ip/reason，绝不记口令。"""
    try:
        from ..services import audit as audit_svc
    except ImportError:
        from services import audit as audit_svc  # type: ignore
    request.state.actor = username
    request.state.tenant_id = ""
    audit_svc.record_denied(
        request,
        action="auth.login_failed",
        resource="auth",
        detail={
            "denied": True,
            "reason": reason,
            "username": username,
            "ip": client_ip,
            "path": request.url.path,
        },
        actor=username,
        tenant_id="",
        # A55-S2：合并键含 reason——「口令错误」与「锁定期拒绝」是两种证据，
        # 不能在同一 60s 窗口里被前一行吞掉（否则锁定尝试在审计面不可见）。
        key_suffix=reason,
    )
