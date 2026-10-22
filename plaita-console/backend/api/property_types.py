"""
自定义属性类型 API（2026-09 节点管理重设计 C2）

- GET    /api/property-types        列出全部自定义类型（内置类型由前端常量展示）
- POST   /api/property-types        注册/更新（name 唯一，upsert）
- DELETE /api/property-types/{name} 删除

语义约束见 services/node_registry_svc.upsert_property_type：类型是 console 侧
命名别名（base_type + enum/default 约束），生成节点 schema_json 时展开为内置
基础类型，运行时永不接触自定义类型名。
"""
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

try:
    from ..request_models import EmptyRequestModel, StrictRequestModel
except ImportError:  # 顶层 ``api`` 包（测试/脚本模式）
    from request_models import EmptyRequestModel, StrictRequestModel  # type: ignore

try:
    from ..services import flow_store, node_registry_svc
    from ..auth import tenant_scope
except ImportError:
    from services import flow_store, node_registry_svc  # type: ignore
    from auth import tenant_scope  # type: ignore

try:  # A2R24-01：统一错误 detail 形状
    from ..error_contract import error_detail
except ImportError:
    from error_contract import error_detail  # type: ignore

router = APIRouter()


class PropertyTypeView(BaseModel):
    name: str
    base_type: str = "string"
    enum_options: List[Any] = Field(default_factory=list)
    default_value: Optional[Any] = None
    desc: str = ""


class UpsertPropertyTypeRequest(StrictRequestModel):
    name: str = Field(..., description="类型名（唯一，≤64 字符）")
    base_type: str = Field("string", description="基础类型：string/integer/number/boolean/array/object")
    enum_options: List[Any] = Field(default_factory=list, description="枚举选项（base_type=string 时有意义）")
    default_value: Optional[Any] = Field(None, description="默认值")
    desc: str = Field("", description="用途说明")


class PropertyTypeListResponse(BaseModel):
    types: List[PropertyTypeView]
    total: int


def _store() -> flow_store.FlowStore:
    return flow_store.get_flow_store()


def _audit(request: Optional[Request], action: str, resource_id: str,
           detail: Optional[dict] = None, *, tenant_id: Optional[str] = None) -> None:
    """A55-S1：自定义属性类型写操作审计。"""
    if request is None:
        return
    try:
        from ..services import audit as audit_svc
    except ImportError:
        from services import audit as audit_svc  # type: ignore
    audit_svc.record(
        request,
        action=action,
        resource="property_type",
        resource_id=resource_id,
        detail=detail,
        tenant_id=tenant_id,
    )


@router.get("/property-types", response_model=PropertyTypeListResponse)
def list_property_types(request: Request = None):
    """列出本租户自定义属性类型。"""
    tenant: Optional[str] = tenant_scope(request) if request is not None else None
    out = node_registry_svc.list_property_types(_store(), tenant_id=tenant)
    views = [PropertyTypeView(**t.model_dump()) for t in out]
    return PropertyTypeListResponse(types=views, total=len(views))


@router.post("/property-types", response_model=PropertyTypeView)
def upsert_property_type(req: UpsertPropertyTypeRequest, request: Request = None):
    """注册或更新本租户自定义属性类型。"""
    tenant = tenant_scope(request, required=True) if request is not None else ""
    try:
        out = node_registry_svc.upsert_property_type(
            store=_store(),
            name=req.name,
            base_type=req.base_type,
            enum_options=req.enum_options,
            default_value=req.default_value,
            desc=req.desc,
            tenant_id=tenant,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=400, detail=error_detail("property_type_invalid", str(e))
        )
    _audit(request, "property_type.upsert", req.name,
           {"name": req.name, "base_type": req.base_type}, tenant_id=tenant)
    return PropertyTypeView(**out.model_dump())


@router.delete("/property-types/{name}")
def delete_property_type(name: str, request: Request = None,
                         body: Optional[EmptyRequestModel] = None):
    # 写路径 required=True（A2R17-01 全量视角只读；A1 r110 a/P2）：None 不过滤租户。
    tenant: Optional[str] = tenant_scope(request, required=True) if request is not None else None
    try:
        node_registry_svc.delete_property_type(_store(), name, tenant_id=tenant)
    except LookupError as e:
        raise HTTPException(
            status_code=404, detail=error_detail("property_type_not_found", str(e))
        )
    _audit(request, "property_type.delete", name, {"name": name},
           tenant_id=tenant or "")
    return {"success": True, "name": name}
