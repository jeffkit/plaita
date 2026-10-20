"""
dry-run API

POST /api/flows/dry-run
- 请求：{ flowJson: string, input?: object }
- 响应：{ result, nodes: [{id,type,name,input,output,status,error,depth,flow_path,flow_id}], error }
  其中 depth/flow_path/flow_id 为主/子流程层级（根层 depth=0），供试跑面板子图缩进。
"""
import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

try:
    from ..request_models import StrictRequestModel
except ImportError:  # 顶层 ``api`` 包（测试/脚本模式）
    from request_models import StrictRequestModel  # type: ignore

try:
    from ..services import dryrun as dryrun_svc
    from ..auth import tenant_scope
except ImportError:
    from services import dryrun as dryrun_svc  # type: ignore
    from auth import tenant_scope  # type: ignore

router = APIRouter()


class DryRunRequest(StrictRequestModel):
    flowJson: str = Field(..., description="Flow 定义 JSON 字符串")
    input: Optional[Dict[str, Any]] = Field(default_factory=dict, description="输入参数")
    pinned: Optional[Dict[str, Any]] = Field(
        default=None, description="节点输出固定：{nodeId: value}，命中节点跳过真实执行"
    )
    onlyNode: Optional[str] = Field(
        default=None, description="仅真实执行该节点（其余除 start 外以 mock 代替）"
    )


class NodeResult(BaseModel):
    id: Optional[str] = None
    type: Optional[str] = None
    name: Optional[str] = None
    input: Optional[Any] = None
    output: Optional[Any] = None
    status: str = "success"
    error: Optional[str] = None
    # 主/子流程层级：根层 depth=0；flow_path 自根 flow 起的标签路径；
    # flow_id 为节点所属 flow 的 id（内联子流程为 null）
    depth: int = 0
    flow_path: List[str] = Field(default_factory=list)
    flow_id: Optional[str] = None


class DryRunResponse(BaseModel):
    result: Optional[Any] = None
    nodes: list[NodeResult] = Field(default_factory=list)
    error: Optional[str] = None


@router.post("/flows/dry-run", response_model=DryRunResponse)
def dry_run(req: DryRunRequest, request: Request):
    out = dryrun_svc.dry_run(req.flowJson, req.input, pinned=req.pinned, only_node=req.onlyNode)
    # A55-S1：试跑审计（flow_id/节点数/only_node；不记 definition）。
    flow_id = None
    node_count = 0
    try:
        defn = json.loads(req.flowJson)
        if isinstance(defn, dict):
            flow_id = defn.get("flow_id") or defn.get("id")
            node_count = len(defn.get("nodes") or [])
    except (TypeError, ValueError):
        pass
    try:
        from ..services import audit as audit_svc
    except ImportError:
        from services import audit as audit_svc  # type: ignore
    audit_svc.record(
        request,
        action="flow.dry_run",
        resource="flow",
        resource_id=str(flow_id or ""),
        detail={
            "flow_id": flow_id,
            "nodes": node_count,
            "only_node": req.onlyNode,
        },
        tenant_id=tenant_scope(request, required=True),
    )
    return DryRunResponse(
        result=out["result"],
        nodes=[NodeResult(**n) for n in out["nodes"]],
        error=out["error"],
    )
