"""
节点管理 API

- GET    /api/nodes            列出内置 + 自定义节点描述（含 schema）
- POST   /api/nodes            注册/更新自定义节点描述
- DELETE /api/nodes/{node_type} 删除自定义节点描述（内置不可删）
"""
import json
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

try:
    from ..services import flow_store, node_registry_svc
    from ..auth import tenant_scope
except ImportError:
    from services import flow_store, node_registry_svc
    from auth import tenant_scope  # type: ignore

router = APIRouter()


class NodeDescriptorView(BaseModel):
    # 同 NodeDescriptorOut：字段名避开 pydantic v2 schema_json shadow 警告，
    # alias 保持 API 线格式（FastAPI 响应默认按 alias 序列化为 schema_json）。
    model_config = ConfigDict(populate_by_name=True)

    node_type: str
    node_name: str = ""
    category: str = ""
    node_schema_json: str = Field("{}", alias="schema_json")
    is_builtin: bool = False
    # 代码位置：仅内置节点有（Python 模块路径与类名）；控制台自定义节点为空
    source_module: str = ""
    source_class: str = ""


class NodeListResponse(BaseModel):
    nodes: List[NodeDescriptorView]
    total: int


class RegisterNodeRequest(BaseModel):
    # 字段名避开 pydantic v2 schema_json shadow 警告；alias 保持请求体键名不变
    model_config = ConfigDict(populate_by_name=True)

    node_type: str = Field(..., description="节点类型（唯一）")
    node_name: str = Field("", description="展示名")
    category: str = Field("", description="分类")
    node_schema_json: str = Field("{}", alias="schema_json", description="节点字段 schema（JSON 字符串）")


def _store() -> flow_store.FlowStore:
    return flow_store.get_flow_store()


@router.get("/nodes", response_model=NodeListResponse)
def list_nodes(request: Request = None):
    """列出全部可用节点描述（内置 + 当前租户自定义）。"""
    tenant: Optional[str] = tenant_scope(request) if request is not None else None
    descriptors = node_registry_svc.list_descriptors(_store(), tenant_id=tenant)
    views = [NodeDescriptorView(**d.model_dump()) for d in descriptors]
    return NodeListResponse(nodes=views, total=len(views))


@router.post("/nodes", response_model=NodeDescriptorView)
def register_node(req: RegisterNodeRequest, request: Request = None):
    """注册或更新本租户自定义节点描述。与内置 type 冲突时 400。"""
    tenant = tenant_scope(request, required=True) if request is not None else ""
    try:
        out = node_registry_svc.register_custom(
            store=_store(),
            node_type=req.node_type,
            node_name=req.node_name,
            category=req.category,
            schema_json=req.node_schema_json,
            tenant_id=tenant,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return NodeDescriptorView(**out.model_dump())


@router.delete("/nodes/{node_type}")
def delete_node(node_type: str, request: Request = None):
    """删除本租户自定义节点描述。内置节点不可删（400）。不存在返回 404。"""
    tenant: Optional[str] = tenant_scope(request) if request is not None else None
    try:
        node_registry_svc.delete_custom(_store(), node_type, tenant_id=tenant)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"success": True, "node_type": node_type}


def parsed_schema(node_type: str, tenant_id: Optional[str] = None) -> dict:
    """辅助：取某节点 schema_json 并解析为 dict（供其他模块复用）。"""
    out = node_registry_svc.list_descriptors(_store(), tenant_id=tenant_id)
    for d in out:
        if d.node_type == node_type:
            try:
                return json.loads(d.node_schema_json)
            except (json.JSONDecodeError, TypeError):
                return {}
    return {}
