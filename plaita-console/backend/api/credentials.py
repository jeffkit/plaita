"""凭据管理 API（管理面）

- GET    /api/credentials          列表（仅元信息，不返回密文/明文）
- GET    /api/credentials/{name}   详情（返回明文数据，供编辑回填；需管理员）
- POST   /api/credentials          创建/更新（保存后导出引擎凭据文件）
- DELETE /api/credentials/{name}   删除
"""
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

try:
    from ..request_models import EmptyRequestModel, StrictRequestModel
except ImportError:  # 顶层 ``api`` 包（测试/脚本模式）
    from request_models import EmptyRequestModel, StrictRequestModel  # type: ignore

try:
    from ..services import credentials_svc
    from ..auth import (
        NO_ACTIVE_TENANT_DETAIL,
        has_auth_context,
        is_platform_request,
        tenant_scope,
    )
except ImportError:
    from services import credentials_svc  # type: ignore
    from auth import (  # type: ignore
        NO_ACTIVE_TENANT_DETAIL,
        has_auth_context,
        is_platform_request,
        tenant_scope,
    )

try:  # A2R24-01：统一错误 detail 形状
    from ..error_contract import error_detail
except ImportError:
    from error_contract import error_detail  # type: ignore

router = APIRouter()


def _audit(request: Optional[Request], action: str, resource_id: str,
           detail: Optional[Dict[str, Any]] = None, *,
           tenant_id: Optional[str] = None) -> None:
    """A55-S1/S3：凭据审计（显式生效租户；绝不记录 data/明文）。"""
    if request is None:
        return
    try:
        from ..services import audit as audit_svc
    except ImportError:
        from services import audit as audit_svc  # type: ignore
    audit_svc.record(
        request,
        action=action,
        resource="credential",
        resource_id=resource_id,
        detail=detail,
        tenant_id=tenant_id,
    )


class CredentialSaveRequest(StrictRequestModel):
    name: str = Field(..., min_length=1, max_length=128, description="凭据名（节点按名引用）")
    type: str = Field("generic", max_length=64, description="类型标签，如 webhook-bearer / database")
    data: Dict[str, Any] = Field(..., description="凭据数据，如 {url: ...} 或 {username, password}")
    desc: str = Field("", description="用途说明")


class CredentialDetail(BaseModel):
    name: str
    tenant_id: str = ""
    type: str
    desc: str = ""
    # A2R24-07：列表行已投影 updated_at，详情补齐（编辑页显示最近更新时间）。
    updated_at: Optional[str] = None
    data: Dict[str, Any]


@router.get("/credentials")
def list_credentials(request: Request = None):
    tenant = tenant_scope(request) if request is not None else None
    items = credentials_svc.list_credentials(tenant_id=tenant)
    return {"credentials": items, "total": len(items)}


@router.get("/credentials/{name}", response_model=CredentialDetail)
def get_credential(name: str, request: Request = None):
    tenant = tenant_scope(request) if request is not None else None
    try:
        record = credentials_svc.get_credential_record(name, tenant_id=tenant)
    except credentials_svc.CredentialsKeyMismatchError as e:
        # A2R25-10：错/缺 key 是「服务当前不可用」而不是 500 裸文本；
        # 具名返回可操作指引（恢复原密钥），不把明文/密文带进错误体。
        raise HTTPException(
            status_code=503,
            detail=error_detail(
                "credentials_key_mismatch", str(e),
                error="credentials_key_mismatch", reason=e.reason,
            ),
        )
    except credentials_svc.CredentialsDisabledError as e:
        raise HTTPException(
            status_code=503,
            detail=error_detail(
                "credentials_disabled", str(e), error="credentials_disabled",
            ),
        )
    if record is None:
        raise HTTPException(status_code=404, detail=f"凭据不存在: {name}")
    # #254：明文返回前校验租户归属。租户上下文下记录必须同属该租户；
    # tenant_scope 已保证 None 只可能来自可证明的平台级身份（普通会话
    # 无活跃租户在进入 handler 前就 403），这里再显式复核一次，防止
    # 未来新增直调路径把 None 当全量绕过归属检查。
    if tenant is not None and record.get("tenant_id") != tenant:
        raise HTTPException(status_code=404, detail=f"凭据不存在: {name}")
    if (
        tenant is None
        and request is not None
        and has_auth_context(request)
        and not is_platform_request(request)
    ):
        raise HTTPException(status_code=403, detail=NO_ACTIVE_TENANT_DETAIL)
    if request is not None:
        # A55-S1：读取返回明文，属安全审计（只记 name/tenant，不记 data）。
        _audit(request, "credential.read", name,
               {"name": name, "tenant_id": record.get("tenant_id", "")},
               tenant_id=record.get("tenant_id", "") or "")
    return CredentialDetail(**record)


@router.post("/credentials")
def save_credential(req: CredentialSaveRequest, request: Request = None):
    tenant = tenant_scope(request, required=True) if request is not None else ""
    try:
        credentials_svc.save_credential(req.name, req.type, req.data, req.desc,
                                        tenant_id=tenant)
    except credentials_svc.CredentialsKeyMismatchError as e:
        # 409：与当前库状态冲突（密钥不一致），不是请求体错误；拒绝写入
        # 且原导出文件保持不变（A2R25-10）。
        raise HTTPException(
            status_code=409,
            detail=error_detail(
                "credentials_key_mismatch", str(e),
                error="credentials_key_mismatch", reason=e.reason,
            ),
        )
    except credentials_svc.CredentialsDisabledError as e:
        raise HTTPException(
            status_code=503,
            detail=error_detail(
                "credentials_disabled", str(e), error="credentials_disabled",
            ),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=error_detail("credential_invalid", str(e)))
    _audit(request, "credential.save", req.name,
           {"type": req.type, "tenant_id": tenant}, tenant_id=tenant)
    return {"success": True, "name": req.name, "message": "凭据已保存并导出"}


@router.delete("/credentials/{name}")
def delete_credential(name: str, request: Request = None,
                      body: Optional[EmptyRequestModel] = None):
    tenant = tenant_scope(request) if request is not None else None
    if not credentials_svc.delete_credential(name, tenant_id=tenant):
        raise HTTPException(status_code=404, detail=f"凭据不存在: {name}")
    _audit(request, "credential.delete", name,
           {"name": name, "tenant_id": tenant or ""}, tenant_id=tenant or "")
    return {"success": True, "name": name, "message": "凭据已删除"}
