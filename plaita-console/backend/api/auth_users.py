"""登录/会话 与 用户管理 API。

- POST /api/auth/login    用户名密码换会话 token（无需认证）
- GET  /api/auth/me       当前身份（需认证）
- POST /api/auth/logout   注销当前会话（需认证）
- GET  /api/users         用户列表（admin）
- POST /api/users         创建用户（admin）
- POST /api/users/{u}/role      改角色（admin）
- POST /api/users/{u}/password  重置密码（admin）
- DELETE /api/users/{u}         删除用户（admin）
"""
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

try:
    from .auth import require_auth
    from .services import users_svc
    from .services.flow_store import get_flow_store
except ImportError:
    from auth import require_auth  # type: ignore
    from services import users_svc  # type: ignore
    from services.flow_store import get_flow_store  # type: ignore

router = APIRouter()


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)


class CreateUserRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=64)
    password: str = Field(..., min_length=8)
    role: str = "viewer"


class RoleRequest(BaseModel):
    role: str


class PasswordRequest(BaseModel):
    password: str = Field(..., min_length=8)


@router.get("/auth/setup-status")
def setup_status():
    """首次启动向导探测：users 为空 = 需要初始化管理员。"""
    return {"needs_setup": not users_svc.has_any_user(get_flow_store())}


@router.post("/auth/setup")
def setup(req: CreateUserRequest):
    """创建首个管理员（仅 users 表为空时可用），成功直接返回会话。"""
    try:
        info = users_svc.setup_admin(get_flow_store(), req.username, req.password)
    except users_svc.UserError as e:
        raise HTTPException(status_code=409, detail=str(e))
    session = users_svc.login(get_flow_store(), req.username, req.password)
    return session


@router.post("/auth/login")
def login(req: LoginRequest, request: Request):
    info = users_svc.login(get_flow_store(), req.username, req.password)
    if info is None:
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    return info


@router.get("/auth/me")
def me(request: Request, identity: Dict = Depends(require_auth)):
    return identity


@router.post("/auth/logout")
def logout(request: Request, identity: Dict = Depends(require_auth)):
    auth_header = request.headers.get("Authorization") or ""
    token = auth_header.strip().split(None, 1)[-1]
    users_svc.logout(get_flow_store(), token)
    return {"success": True}


@router.get("/users")
def list_users(_: Dict = Depends(require_auth)):
    return {"users": users_svc.list_users(get_flow_store())}


@router.post("/users")
def create_user(req: CreateUserRequest, request: Request, _: Dict = Depends(require_auth)):
    try:
        users_svc.create_user(get_flow_store(), req.username, req.password, req.role)
    except users_svc.UserError as e:
        raise HTTPException(status_code=400, detail=str(e))
    _audit(request, "user.create", req.username, {"role": req.role})
    return {"success": True, "username": req.username}


@router.post("/users/{username}/role")
def set_role(username: str, req: RoleRequest, request: Request, _: Dict = Depends(require_auth)):
    try:
        users_svc.set_role(get_flow_store(), username, req.role)
    except users_svc.UserError as e:
        raise HTTPException(status_code=400, detail=str(e))
    _audit(request, "user.set_role", username, {"role": req.role})
    return {"success": True, "username": username, "role": req.role}


@router.post("/users/{username}/password")
def set_password(username: str, req: PasswordRequest, request: Request,
                 _: Dict = Depends(require_auth)):
    try:
        users_svc.set_password(get_flow_store(), username, req.password)
    except users_svc.UserError as e:
        raise HTTPException(status_code=400, detail=str(e))
    _audit(request, "user.set_password", username, {})
    return {"success": True, "username": username}


@router.delete("/users/{username}")
def delete_user(username: str, request: Request, _: Dict = Depends(require_auth)):
    try:
        deleted = users_svc.delete_user(get_flow_store(), username)
    except users_svc.UserError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not deleted:
        raise HTTPException(status_code=404, detail=f"用户不存在: {username}")
    _audit(request, "user.delete", username, {})
    return {"success": True, "username": username}


def _audit(request: Request, action: str, resource_id: str, detail: Dict) -> None:
    """审计钩子（users 模块内置埋点；audit 服务缺失时静默跳过）。"""
    try:
        from .services import audit as audit_svc
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
    )
