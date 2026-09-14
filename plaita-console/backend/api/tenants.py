"""租户管理 API（仅平台管理员；/api/tenants 在 _ADMIN_PREFIXES 中强制 admin）。

- GET    /api/tenants                          租户列表（含成员数）
- POST   /api/tenants                          创建租户（响应一次性返回契约密钥）
- GET    /api/tenants/{tenant_id}              租户详情
- POST   /api/tenants/{tenant_id}/status       启用/停用
- POST   /api/tenants/{tenant_id}/rotate-secret 轮换契约密钥（一次性返回）
- DELETE /api/tenants/{tenant_id}              删除（租户内有流程时 409）
- GET    /api/tenants/{tenant_id}/members      成员列表
- POST   /api/tenants/{tenant_id}/members      添加成员 {username, role}
- PUT    /api/tenants/{tenant_id}/members/{username}  改成员角色
- DELETE /api/tenants/{tenant_id}/members/{username}  移除成员
"""
from typing import Dict

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

try:
    from ..auth import require_auth
    from ..services import tenants_svc, users_svc
    from ..services.flow_store import get_flow_store
except ImportError:
    from auth import require_auth  # type: ignore
    from services import tenants_svc, users_svc  # type: ignore
    from services.flow_store import get_flow_store  # type: ignore

router = APIRouter()


class CreateTenantRequest(BaseModel):
    id: str = Field(..., description="租户 ID（slug，3-64 位小写字母/数字/连字符）")
    name: str = Field("", description="展示名（缺省用 id）")


class TenantStatusRequest(BaseModel):
    status: str = Field(..., description="active | disabled")


class MemberRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    role: str = Field("viewer", description="admin | editor | viewer")


def _platform_admin(identity: Dict) -> None:
    if not identity.get("platform_admin"):
        raise HTTPException(status_code=403, detail="仅平台管理员可管理租户")


@router.get("/tenants")
def list_tenants(identity: Dict = Depends(require_auth)):
    _platform_admin(identity)
    return {"tenants": tenants_svc.list_tenants(get_flow_store())}


@router.post("/tenants")
def create_tenant(req: CreateTenantRequest, request: Request,
                  identity: Dict = Depends(require_auth)):
    _platform_admin(identity)
    try:
        tenant = tenants_svc.create_tenant(get_flow_store(), req.id, req.name)
    except tenants_svc.TenantError as e:
        raise HTTPException(status_code=409, detail=str(e))
    _audit(request, "tenant.create", req.id)
    return tenant  # 含 contract_secret_key（仅此一次）


@router.get("/tenants/{tenant_id}")
def get_tenant(tenant_id: str, identity: Dict = Depends(require_auth)):
    _platform_admin(identity)
    try:
        return tenants_svc.get_tenant(get_flow_store(), tenant_id)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/tenants/{tenant_id}/status")
def set_status(tenant_id: str, req: TenantStatusRequest, request: Request,
               identity: Dict = Depends(require_auth)):
    _platform_admin(identity)
    try:
        tenants_svc.set_tenant_status(get_flow_store(), tenant_id, req.status)
    except tenants_svc.TenantError as e:
        raise HTTPException(status_code=400, detail=str(e))
    _audit(request, "tenant.set_status", tenant_id, {"status": req.status})
    return {"success": True, "tenant_id": tenant_id, "status": req.status}


@router.post("/tenants/{tenant_id}/rotate-secret")
def rotate_secret(tenant_id: str, request: Request, identity: Dict = Depends(require_auth)):
    _platform_admin(identity)
    try:
        secret = tenants_svc.rotate_contract_secret(get_flow_store(), tenant_id)
    except tenants_svc.TenantError as e:
        raise HTTPException(status_code=404, detail=str(e))
    _audit(request, "tenant.rotate_secret", tenant_id)
    return secret  # 含 contract_secret_key（仅此一次）


@router.delete("/tenants/{tenant_id}")
def delete_tenant(tenant_id: str, request: Request, identity: Dict = Depends(require_auth)):
    _platform_admin(identity)
    try:
        deleted = tenants_svc.delete_tenant(get_flow_store(), tenant_id)
    except tenants_svc.TenantError as e:
        raise HTTPException(status_code=409, detail=str(e))
    if not deleted:
        raise HTTPException(status_code=404, detail=f"租户不存在: {tenant_id}")
    _audit(request, "tenant.delete", tenant_id)
    return {"success": True, "tenant_id": tenant_id}


# ---- 成员管理 ----

@router.get("/tenants/{tenant_id}/members")
def list_members(tenant_id: str, identity: Dict = Depends(require_auth)):
    _platform_admin(identity)
    return {"members": tenants_svc.list_tenant_members(get_flow_store(), tenant_id)}


@router.post("/tenants/{tenant_id}/members")
def add_member(tenant_id: str, req: MemberRequest, request: Request,
               identity: Dict = Depends(require_auth)):
    _platform_admin(identity)
    try:
        tenants_svc.add_member(get_flow_store(), tenant_id, req.username, req.role)
    except users_svc.UserError as e:
        raise HTTPException(status_code=400, detail=str(e))
    _audit(request, "tenant.add_member", f"{tenant_id}/{req.username}", {"role": req.role})
    return {"success": True, "username": req.username, "tenant_id": tenant_id}


@router.put("/tenants/{tenant_id}/members/{username}")
def set_member_role(tenant_id: str, username: str, req: MemberRequest,
                    request: Request, identity: Dict = Depends(require_auth)):
    _platform_admin(identity)
    try:
        tenants_svc.set_member_role(get_flow_store(), tenant_id, username, req.role)
    except users_svc.UserError as e:
        raise HTTPException(status_code=400, detail=str(e))
    _audit(request, "tenant.set_member_role", f"{tenant_id}/{username}", {"role": req.role})
    return {"success": True, "username": username, "role": req.role}


@router.delete("/tenants/{tenant_id}/members/{username}")
def remove_member(tenant_id: str, username: str, request: Request,
                  identity: Dict = Depends(require_auth)):
    _platform_admin(identity)
    if not tenants_svc.remove_member(get_flow_store(), tenant_id, username):
        raise HTTPException(status_code=404, detail=f"{username} 不是租户 {tenant_id} 的成员")
    _audit(request, "tenant.remove_member", f"{tenant_id}/{username}")
    return {"success": True, "username": username, "tenant_id": tenant_id}


def _audit(request: Request, action: str, resource_id: str, detail: Dict | None = None) -> None:
    try:
        from ..services import audit as audit_svc
    except ImportError:
        try:
            from services import audit as audit_svc  # type: ignore
        except ImportError:
            return
    audit_svc.record(request, action=action, resource="tenant", resource_id=resource_id,
                     detail=detail)
