"""图（编辑器 GraphDTO）validate API（plaita#305 Phase 1）。

- POST /api/graphs/validate   编辑器 graph 结构化校验（宿主 AI / 嵌入编辑器
                               在保存/发布前调用）

鉴权：既有会话/API key（require_auth，挂载于 main._mount_admin）；embed token
的授权集成留给 #305 Phase 3。租户作用域照 ``tenant_scope(required=True)``：
reference 节点的引用目标只在当前租户内解析。

契约文档：``services/graph_validation.py`` 模块 docstring（GraphDTO 输入形状、
折叠规则、错误项结构、与 publish 门的共享实现与一致性契约）。
"""
from __future__ import annotations
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Request
from redis import Redis

try:
    from ..request_models import StrictRequestModel
    from ..services import flow_store
    from ..services import graph_validation
    from ..services.runtime_pools import normalize_runtime
    from ..auth import tenant_scope
    from ..error_contract import error_detail
except ImportError:  # 顶层 ``api`` 包（测试/脚本模式）
    from request_models import StrictRequestModel  # type: ignore
    from services import flow_store  # type: ignore
    from services import graph_validation  # type: ignore
    from services.runtime_pools import normalize_runtime  # type: ignore
    from auth import tenant_scope  # type: ignore
    from error_contract import error_detail  # type: ignore

from pydantic import Field

router = APIRouter()


class GraphValidateRequest(StrictRequestModel):
    graph: Dict[str, Any] = Field(
        ..., description="编辑器 graph（GraphDTO：{nodes, edges, viewport?, meta?}）"
    )
    # 目标 runtime pool（go/python）：能力门的判定池；缺省 python（与保存路径同默认）
    runtime: Optional[str] = Field(None, description="目标 runtime pool（go/python）")


class GraphValidateResponse(StrictRequestModel):
    valid: bool
    errors: list = Field(default_factory=list)
    definition: Dict[str, Any] = Field(default_factory=dict)


def _redis(request: Request) -> Optional[Redis]:
    """本地单机模式为 None（能力门与 publish 面同口径跳过）。"""
    return getattr(request.app.state, "redis", None)


@router.post("/graphs/validate", response_model=GraphValidateResponse)
def validate_graph(req: GraphValidateRequest, request: Request = None):  # noqa: ANN001
    """校验编辑器 graph，返回结构化 errors 全量（非法 → 422 + 全量，非首错即停）。

    校验面 = publish 门共享实现（字段 schema / #300 能力门）∪ 编辑器图结构
    （单入口 / 可达 end / 无环 / 孤儿）∪ 连线语义 ∪ reference 引用的租户内
    存在性。warning 级条目不阻断（valid=true，随 200 返回）。
    """
    tenant = tenant_scope(request, required=True) if request is not None else ""
    try:
        runtime = normalize_runtime(req.runtime)
    except ValueError as e:
        raise HTTPException(
            status_code=422, detail=error_detail("invalid_runtime", str(e))
        )
    report = graph_validation.validate_graph(
        req.graph, runtime=runtime, redis=_redis(request),
        store=flow_store.get_flow_store(), tenant_id=tenant,
    )
    if not report["valid"]:
        raise HTTPException(
            status_code=422,
            detail=error_detail(
                "invalid_graph",
                f"graph 校验未通过（{sum(1 for e in report['errors'] if e.get('severity') == 'error')} 个错误）",
                valid=False,
                errors=report["errors"],
                definition=report["definition"],
            ),
        )
    return GraphValidateResponse(
        valid=True, errors=report["errors"], definition=report["definition"]
    )
