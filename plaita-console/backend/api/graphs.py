"""图（编辑器 GraphDTO）API（plaita#305）。

- POST /api/graphs/validate   编辑器 graph 结构化校验（Phase 1；宿主 AI /
                               嵌入编辑器在保存/发布前调用）
- POST /api/graphs/generate   自然语言/结构化意图 → GraphDTO 草稿（Phase 2；
                               generate→validate→回喂闭环，纯函数式不落库）

鉴权：既有会话/API key（require_auth，挂载于 main._mount_admin）；写方法
闸 = editor+，租户作用域照 ``tenant_scope(required=True)``（reference 引用
目标只在当前租户内解析）。embed token 的授权集成留给 #305 Phase 3。

契约文档：``services/graph_validation.py`` 模块 docstring（GraphDTO 输入形状、
折叠规则、错误项结构、与 publish 门的共享实现与一致性契约）与
``services/graph_generation.py``（生成闭环、模型接入面、A128-01 自检语义）。
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from redis import Redis

try:
    from ..request_models import StrictRequestModel
    from ..services import flow_store
    from ..services import graph_generation, graph_validation
    from ..services.runtime_pools import normalize_runtime
    from ..auth import tenant_scope
    from ..error_contract import error_detail
except ImportError:  # 顶层 ``api`` 包（测试/脚本模式）
    from request_models import StrictRequestModel  # type: ignore
    from services import flow_store  # type: ignore
    from services import graph_generation, graph_validation  # type: ignore
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


class GraphGenerateHints(StrictRequestModel):
    preferred_node_types: Optional[List[str]] = Field(
        None, description="节点类型偏好（注入生成 prompt，仍须是可用类型）"
    )
    reference_flows: Optional[List[Dict[str, Any]]] = Field(
        None,
        description="租户内可引用的子流程清单（注入生成 prompt；reference "
                    "节点 fields.flow_id 只能从中选择）",
    )


class GraphGenerateRequest(StrictRequestModel):
    # 空/纯空白 prompt 由 handler 具名 422（pydantic min_length 会把空串
    # 变成非具名的校验错误列表，错误契约面不统一）
    prompt: str = Field(..., description="自然语言业务需求")
    # 目标 runtime pool：能力门判定池 + reference 可执行性；缺省 python
    runtime: Optional[str] = Field(None, description="目标 runtime pool（go/python）")
    hints: Optional[GraphGenerateHints] = Field(
        None, description="生成提示：节点类型偏好 / 可引用子流程清单"
    )


class GenerationMeta(StrictRequestModel):
    model: str = Field(..., description="生成模型名")
    duration_ms: int = Field(..., description="总耗时（毫秒，含全部尝试轮）")
    attempts: int = Field(..., description="模型调用轮数（回喂自纠计费面）")
    usage: Optional[Dict[str, int]] = Field(
        None, description="token 用量累积（input/output/total；不可得为 null）"
    )


class GraphGenerateResponse(StrictRequestModel):
    valid: bool
    graph: Optional[Dict[str, Any]] = Field(
        None, description="GraphDTO 草稿（valid=false 时为最后一次草稿或 null）"
    )
    definition: Dict[str, Any] = Field(
        default_factory=dict, description="草稿折叠出的 Flow 定义"
    )
    errors: list = Field(
        default_factory=list,
        description="valid=true 时为 warning 级提示；valid=false 时为最后一轮"
                    " validate errors 原样（或 generation.* 具名错误）",
    )
    generation: GenerationMeta


@router.post("/graphs/generate", response_model=GraphGenerateResponse)
def generate_graph(req: GraphGenerateRequest, request: Request = None):  # noqa: ANN001
    """自然语言/结构化意图 → GraphDTO 草稿（纯函数式，不落库）。

    generate→validate→回喂闭环：草稿必须过 Phase 1 validate，不过则把
    errors 原样回喂模型自纠（有界轮数）；轮数用尽仍不合法 → 200
    ``valid=false`` 且 errors[] 原样带回。模型输出无法解析出 JSON 视为可
    回喂错误；模型/凭证不可用 → 503 ``generation_client_unavailable``；
    生成产物违反 GraphDTO 内部不变量（A128-01：容器投影子节点内容未双写
    进宿主 childFlow）→ 500 ``generation.childflow_not_double_written``，
    不出草稿。保存仍走宿主既有 save（本端点零落库）。
    """
    tenant = tenant_scope(request, required=True) if request is not None else ""
    prompt = (req.prompt or "").strip()
    if not prompt:
        raise HTTPException(
            status_code=422,
            detail=error_detail("empty_prompt",
                                "prompt 不能为空（自然语言业务需求）"),
        )
    try:
        runtime = normalize_runtime(req.runtime)
    except ValueError as e:
        raise HTTPException(
            status_code=422, detail=error_detail("invalid_runtime", str(e))
        )
    try:
        complete, model_name, usage = graph_generation.default_model()
    except Exception as exc:  # noqa: BLE001 — 凭证/依赖缺失具名 503
        raise HTTPException(
            status_code=503,
            detail=error_detail("generation_client_unavailable",
                                f"生成模型不可用：{exc}"),
        )
    try:
        result = graph_generation.generate_graph(
            prompt, runtime=runtime, redis=_redis(request),
            store=flow_store.get_flow_store(), tenant_id=tenant,
            hints=req.hints, complete=complete, model_name=model_name,
            usage=usage,
        )
    except graph_generation.GraphGenerationInvariantError as exc:
        raise HTTPException(
            status_code=500,
            detail=error_detail(
                "generation.childflow_not_double_written",
                "生成产物违反 GraphDTO 不变量：容器投影子节点内容未双写进"
                "宿主 childFlow（A128-01 内容级自检拦截，不出草稿）",
                violations=exc.violations,
            ),
        )
    return GraphGenerateResponse(
        valid=result["valid"], graph=result["graph"],
        definition=result["definition"], errors=result["errors"],
        generation=GenerationMeta(**result["generation"]),
    )
