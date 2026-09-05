"""
节点管理 API

- GET    /api/nodes            列出内置 + 自定义节点描述（含 schema）
- POST   /api/nodes            注册/更新自定义节点描述
- DELETE /api/nodes/{node_type} 删除自定义节点描述（内置不可删）
"""
import json
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

try:
    from ..services import flow_store, node_registry_svc
except ImportError:
    from services import flow_store, node_registry_svc

router = APIRouter()


class NodeDescriptorView(BaseModel):
    node_type: str
    node_name: str = ""
    category: str = ""
    schema_json: str = "{}"
    is_builtin: bool = False


class NodeListResponse(BaseModel):
    nodes: List[NodeDescriptorView]
    total: int


class RegisterNodeRequest(BaseModel):
    node_type: str = Field(..., description="节点类型（唯一）")
    node_name: str = Field("", description="展示名")
    category: str = Field("", description="分类")
    schema_json: str = Field("{}", description="节点字段 schema（JSON 字符串）")


def _store() -> flow_store.FlowStore:
    return flow_store.get_flow_store()


@router.get("/nodes", response_model=NodeListResponse)
def list_nodes():
    """列出全部可用节点描述（内置 + 自定义）。"""
    descriptors = node_registry_svc.list_descriptors(_store())
    views = [NodeDescriptorView(**d.model_dump()) for d in descriptors]
    return NodeListResponse(nodes=views, total=len(views))


@router.post("/nodes", response_model=NodeDescriptorView)
def register_node(req: RegisterNodeRequest):
    """注册或更新自定义节点描述。与内置 type 冲突时 400。"""
    try:
        out = node_registry_svc.register_custom(
            store=_store(),
            node_type=req.node_type,
            node_name=req.node_name,
            category=req.category,
            schema_json=req.schema_json,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return NodeDescriptorView(**out.model_dump())


@router.delete("/nodes/{node_type}")
def delete_node(node_type: str):
    """删除自定义节点描述。内置节点不可删（400）。不存在返回 404。"""
    try:
        node_registry_svc.delete_custom(_store(), node_type)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {"success": True, "node_type": node_type}


def parsed_schema(node_type: str) -> dict:
    """辅助：取某节点 schema_json 并解析为 dict（供其他模块复用）。"""
    out = node_registry_svc.list_descriptors(_store())
    for d in out:
        if d.node_type == node_type:
            try:
                return json.loads(d.schema_json)
            except (json.JSONDecodeError, TypeError):
                return {}
    return {}
