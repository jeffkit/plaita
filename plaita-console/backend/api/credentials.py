"""凭据管理 API（管理面）

- GET    /api/credentials          列表（仅元信息，不返回密文/明文）
- GET    /api/credentials/{name}   详情（返回明文数据，供编辑回填；需管理员）
- POST   /api/credentials          创建/更新（保存后导出引擎凭据文件）
- DELETE /api/credentials/{name}   删除
"""
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

try:
    from .services import credentials_svc
except ImportError:
    from services import credentials_svc  # type: ignore

router = APIRouter()


class CredentialSaveRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128, description="凭据名（节点按名引用）")
    type: str = Field("generic", max_length=64, description="类型标签，如 webhook-bearer / database")
    data: Dict[str, Any] = Field(..., description="凭据数据，如 {url: ...} 或 {username, password}")
    desc: str = Field("", description="用途说明")


class CredentialDetail(BaseModel):
    name: str
    type: str
    desc: str = ""
    data: Dict[str, Any]


@router.get("/credentials")
def list_credentials():
    items = credentials_svc.list_credentials()
    return {"credentials": items, "total": len(items)}


@router.get("/credentials/{name}", response_model=CredentialDetail)
def get_credential(name: str):
    record = credentials_svc.get_credential_record(name)
    if record is None:
        raise HTTPException(status_code=404, detail=f"凭据不存在: {name}")
    return CredentialDetail(**record)


@router.post("/credentials")
def save_credential(req: CredentialSaveRequest, request: Request = None):
    try:
        credentials_svc.save_credential(req.name, req.type, req.data, req.desc)
    except credentials_svc.CredentialsDisabledError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if request is not None:
        try:
            from .services import audit as audit_svc
        except ImportError:
            from services import audit as audit_svc  # type: ignore
        audit_svc.record(request, action="credential.save", resource="credential",
                         resource_id=req.name, detail={"type": req.type})
    return {"success": True, "name": req.name, "message": "凭据已保存并导出"}


@router.delete("/credentials/{name}")
def delete_credential(name: str, request: Request = None):
    if not credentials_svc.delete_credential(name):
        raise HTTPException(status_code=404, detail=f"凭据不存在: {name}")
    if request is not None:
        try:
            from .services import audit as audit_svc
        except ImportError:
            from services import audit as audit_svc  # type: ignore
        audit_svc.record(request, action="credential.delete", resource="credential", resource_id=name)
    return {"success": True, "name": name, "message": "凭据已删除"}
