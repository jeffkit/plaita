"""租户管理 API（平台管理员；成员管理额外允许本租户 admin）。

- 租户 CRUD / 状态 / 密钥：仅平台管理员（/api/tenants 在 _ADMIN_PREFIXES 中）
- 成员管理：平台管理员，或**本租户**的 admin（#260；跨租户一律 403）
- 租户 admin 不得添加/修改/移除 ``platform_admin=True`` 的账号
"""
from typing import Dict, Optional

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

try:
    from ..request_models import EmptyRequestModel, StrictRequestModel
except ImportError:  # 顶层 ``api`` 包（测试/脚本模式）
    from request_models import EmptyRequestModel, StrictRequestModel  # type: ignore

try:
    from ..auth import require_auth
    from ..services import tenants_svc, users_svc
    from ..services.flow_store import get_flow_store
except ImportError:
    from auth import require_auth  # type: ignore
    from services import tenants_svc, users_svc  # type: ignore
    from services.flow_store import get_flow_store  # type: ignore

try:  # A2R24-01：统一错误 detail 形状
    from ..error_contract import error_detail
except ImportError:
    from error_contract import error_detail  # type: ignore

logger = logging.getLogger(__name__)

router = APIRouter()

#: #260：租户 admin 的成员管理只在本租户内成立；跨租户仍是平台身份专属。
TENANT_MEMBER_ADMIN_DETAIL = "仅平台管理员或本租户管理员可管理租户成员"
PLATFORM_ACCOUNT_PROTECTED_DETAIL = "仅平台管理员可操作平台管理员账号"


class CreateTenantRequest(StrictRequestModel):
    id: str = Field(..., description="租户 ID（slug，3-64 位小写字母/数字/连字符）")
    name: str = Field("", description="展示名（缺省用 id）")


class TenantStatusRequest(StrictRequestModel):
    status: str = Field(..., description="active | disabled")


class MemberRequest(StrictRequestModel):
    username: str = Field(..., min_length=1, max_length=64)
    role: str = Field("viewer", description="admin | editor | viewer")


def _platform_admin(request: Request, identity: Dict, *, action: str,
                    target: str = "") -> None:
    """平台身份闸（#260/A55-S2）：拒绝写 ``<action>.denied`` + WARNING。"""
    if identity.get("platform_admin"):
        return
    _audit_denied(request, action, target, "platform_admin_required")
    raise HTTPException(status_code=403, detail="仅平台管理员可管理租户")


def _tenant_member_admin(request: Request, identity: Dict, tenant_id: str,
                         *, action: str, target: str = "") -> None:
    """成员管理闸（#260）：平台管理员，或 active tenant 等于目标租户的 admin。"""
    if identity.get("platform_admin"):
        return
    if identity.get("role") == "admin" and identity.get("tenant_id") == tenant_id:
        return
    _audit_denied(request, action, target or tenant_id, TENANT_MEMBER_ADMIN_DETAIL)
    raise HTTPException(status_code=403, detail=TENANT_MEMBER_ADMIN_DETAIL)


def _guard_platform_target(request: Request, identity: Dict, tenant_id: str,
                           target: str, *, action: str) -> None:
    """租户 admin 不得触达平台账号（#260；防成员路径成为接管旁路）。"""
    if identity.get("platform_admin"):
        return
    if users_svc.is_platform_admin(get_flow_store(), target):
        _audit_denied(request, action, f"{tenant_id}/{target}",
                      PLATFORM_ACCOUNT_PROTECTED_DETAIL)
        raise HTTPException(status_code=403, detail=PLATFORM_ACCOUNT_PROTECTED_DETAIL)


def _audit_denied(request: Request, action: str, resource_id: str, detail: str) -> None:
    """拒绝尝试留痕：审计 + WARNING（拒绝必须可观测，不做静默降级）。

    A55-S2 同口径：``record_denied`` 按 ``(action, actor, ip, tenant)`` 合并
    窗口内的重复拒绝，低权流量不能按请求数撑大审计表。
    """
    logger.warning(
        "拒绝租户成员操作 action=%s actor=%s target=%s path=%s",
        action,
        getattr(getattr(request, "state", None), "actor", ""),
        resource_id,
        request.url.path,
    )
    try:
        from ..services import audit as audit_svc
    except ImportError:
        try:
            from services import audit as audit_svc  # type: ignore
        except ImportError:
            return
    audit_svc.record_denied(
        request,
        action=f"{action}.denied",
        resource="tenant",
        resource_id=resource_id,
        detail={
            "denied": True,
            "reason": detail,
            "method": request.method,
            "path": request.url.path,
        },
        key_suffix=detail,
    )


def _redis(request: Request):
    """集群档的 Redis 客户端（本地单机档为 None）。"""
    return getattr(request.app.state, "redis", None)


@router.get("/tenants")
def list_tenants(request: Request, identity: Dict = Depends(require_auth)):
    _platform_admin(request, identity, action="tenant.list")
    return {"tenants": tenants_svc.list_tenants(get_flow_store())}


@router.post("/tenants")
def create_tenant(req: CreateTenantRequest, request: Request,
                  identity: Dict = Depends(require_auth)):
    _platform_admin(request, identity, action="tenant.create", target=req.id)
    try:
        tenant = tenants_svc.create_tenant(
            get_flow_store(), req.id, req.name, redis_client=_redis(request)
        )
    except tenants_svc.TenantError as e:
        raise HTTPException(status_code=409, detail=error_detail("tenant_conflict", str(e)))
    _audit(request, "tenant.create", req.id, {"target_tenant": req.id},
           tenant_id="")
    return tenant  # 含 contract_secret_key（仅此一次）


@router.get("/tenants/{tenant_id}")
def get_tenant(tenant_id: str, request: Request,
               identity: Dict = Depends(require_auth)):
    _platform_admin(request, identity, action="tenant.get", target=tenant_id)
    try:
        return tenants_svc.get_tenant(get_flow_store(), tenant_id)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=error_detail("tenant_not_found", str(e)))


@router.post("/tenants/{tenant_id}/status")
def set_status(tenant_id: str, req: TenantStatusRequest, request: Request,
               identity: Dict = Depends(require_auth)):
    _platform_admin(request, identity, action="tenant.set_status", target=tenant_id)
    try:
        tenants_svc.set_tenant_status(
            get_flow_store(), tenant_id, req.status, redis_client=_redis(request)
        )
    except tenants_svc.TenantError as e:
        raise HTTPException(status_code=400, detail=error_detail("tenant_invalid", str(e)))
    _audit(request, "tenant.set_status", tenant_id,
           {"status": req.status, "target_tenant": tenant_id}, tenant_id="")
    return {"success": True, "tenant_id": tenant_id, "status": req.status}


@router.post("/tenants/{tenant_id}/rotate-secret")
def rotate_secret(tenant_id: str, request: Request,
                  identity: Dict = Depends(require_auth),
                  body: Optional[EmptyRequestModel] = None):
    _platform_admin(request, identity, action="tenant.rotate_secret",
                    target=tenant_id)
    try:
        secret = tenants_svc.rotate_contract_secret(get_flow_store(), tenant_id)
    except tenants_svc.TenantError as e:
        raise HTTPException(status_code=404, detail=error_detail("tenant_not_found", str(e)))
    _audit(request, "tenant.rotate_secret", tenant_id,
           {"target_tenant": tenant_id}, tenant_id="")
    return secret  # 含 contract_secret_key（仅此一次）


@router.delete("/tenants/{tenant_id}")
def delete_tenant(tenant_id: str, request: Request,
                  identity: Dict = Depends(require_auth),
                  body: Optional[EmptyRequestModel] = None):
    _platform_admin(request, identity, action="tenant.delete", target=tenant_id)
    try:
        deleted = tenants_svc.delete_tenant(
            get_flow_store(), tenant_id, redis_client=_redis(request)
        )
    except tenants_svc.TenantError as e:
        raise HTTPException(status_code=409, detail=error_detail("tenant_conflict", str(e)))
    if not deleted:
        raise HTTPException(status_code=404, detail=f"租户不存在: {tenant_id}")
    _audit(request, "tenant.delete", tenant_id, {"target_tenant": tenant_id},
           tenant_id="")
    return {"success": True, "tenant_id": tenant_id}


# ---- 成员管理 ----

@router.get("/tenants/{tenant_id}/members")
def list_members(tenant_id: str, request: Request,
                 identity: Dict = Depends(require_auth)):
    _tenant_member_admin(request, identity, tenant_id, action="tenant.list_members")
    return {"members": tenants_svc.list_tenant_members(get_flow_store(), tenant_id)}


@router.post("/tenants/{tenant_id}/members")
def add_member(tenant_id: str, req: MemberRequest, request: Request,
               identity: Dict = Depends(require_auth)):
    _tenant_member_admin(request, identity, tenant_id, action="tenant.add_member",
                         target=f"{tenant_id}/{req.username}")
    _guard_platform_target(request, identity, tenant_id, req.username,
                           action="tenant.add_member")
    try:
        tenants_svc.add_member(get_flow_store(), tenant_id, req.username, req.role)
    except users_svc.UserError as e:
        raise HTTPException(status_code=400, detail=error_detail("member_invalid", str(e)))
    # A55-S3 残面：成员关系属于目标租户（resource 生效租户），平台管理员
    # 不带 X-Tenant-ID 时 state.tenant_id=None，不能把审计落到 ""——否则该
    # 租户 admin 在自己的 /api/audit 里看不到「谁改了本租户成员」。
    _audit(request, "tenant.add_member", f"{tenant_id}/{req.username}",
           {"role": req.role, "target_tenant": tenant_id},
           tenant_id=tenant_id)
    return {"success": True, "username": req.username, "tenant_id": tenant_id}


@router.put("/tenants/{tenant_id}/members/{username}")
def set_member_role(tenant_id: str, username: str, req: MemberRequest,
                    request: Request, identity: Dict = Depends(require_auth)):
    _tenant_member_admin(request, identity, tenant_id, action="tenant.set_member_role",
                         target=f"{tenant_id}/{username}")
    _guard_platform_target(request, identity, tenant_id, username,
                           action="tenant.set_member_role")
    try:
        tenants_svc.set_member_role(get_flow_store(), tenant_id, username, req.role)
    except users_svc.UserError as e:
        raise HTTPException(status_code=400, detail=error_detail("member_invalid", str(e)))
    _audit(request, "tenant.set_member_role", f"{tenant_id}/{username}",
           {"role": req.role, "target_tenant": tenant_id},
           tenant_id=tenant_id)
    return {"success": True, "username": username, "role": req.role}


@router.delete("/tenants/{tenant_id}/members/{username}")
def remove_member(tenant_id: str, username: str, request: Request,
                  identity: Dict = Depends(require_auth),
                  body: Optional[EmptyRequestModel] = None):
    _tenant_member_admin(request, identity, tenant_id, action="tenant.remove_member",
                         target=f"{tenant_id}/{username}")
    _guard_platform_target(request, identity, tenant_id, username,
                           action="tenant.remove_member")
    if not tenants_svc.remove_member(get_flow_store(), tenant_id, username):
        raise HTTPException(status_code=404, detail=f"{username} 不是租户 {tenant_id} 的成员")
    _audit(request, "tenant.remove_member", f"{tenant_id}/{username}",
           {"target_tenant": tenant_id}, tenant_id=tenant_id)
    return {"success": True, "username": username, "tenant_id": tenant_id}


def _audit(request: Request, action: str, resource_id: str,
           detail: Dict | None = None, *, tenant_id: str | None = None) -> None:
    try:
        from ..services import audit as audit_svc
    except ImportError:
        try:
            from services import audit as audit_svc  # type: ignore
        except ImportError:
            return
    audit_svc.record(request, action=action, resource="tenant", resource_id=resource_id,
                     detail=detail, tenant_id=tenant_id)
