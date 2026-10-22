"""
流程定义 CRUD + 版本管理 API

- GET    /api/flows                            流程列表
- POST   /api/flows                            新建流程（仅元信息）
- GET    /api/flows/{flow_id}                  流程详情 + 版本列表
- DELETE /api/flows/{flow_id}                  删除流程及其全部版本（#301：同步清目标 Redis 三件）
- GET    /api/flows/{flow_id}/versions/{ver}   取某版本定义（含 layout）
- GET    /api/flows/{flow_id}/versions/{ver}/source   codeflow 源码视图（权威 / emit_source 反编译兜底）
- PUT    /api/flows/{flow_id}/versions/{ver}   保存草稿（Flow.model_validate 强校验）
- DELETE /api/flows/{flow_id}/versions/{ver}   删除版本（#301：同步清目标 Redis 三件）
- POST   /api/flows/{flow_id}/publish          发布版本（draft → published）
- POST   /api/flows/publish-outbox/{id}/resolve  DLQ 人工出口·resolve（c117-06，置终态留痕）
- POST   /api/flows/publish-outbox/{id}/discard   DLQ 人工出口·discard（c117-06，放弃并删行）
"""
import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from redis import Redis
from pydantic import BaseModel, Field

try:
    from ..request_models import EmptyRequestModel, StrictRequestModel
except ImportError:  # 顶层 ``api`` 包（测试/脚本模式）
    from request_models import EmptyRequestModel, StrictRequestModel  # type: ignore

try:
    from ..services import flow_store
    from ..services import validation
    from ..services.engine_sync import (
        remove_flow_from_engine,
        remove_flow_version_from_engine,
        sync_flow_to_engine,
    )
    from ..auth import ensure_platform_admin, require_auth, tenant_scope
except ImportError:
    from services import flow_store
    from services import validation  # type: ignore
    from services.engine_sync import (
        remove_flow_from_engine,
        remove_flow_version_from_engine,
        sync_flow_to_engine,
    )
    from auth import ensure_platform_admin, require_auth, tenant_scope

try:  # A2R24-01：统一错误 detail 形状 + 异常文本脱敏
    from ..error_contract import error_detail, scrubbed_detail
except ImportError:
    from error_contract import error_detail, scrubbed_detail

try:  # plaita#300：runtime 能力门 + 池路由取值域
    from ..services.capability_gate import UnsupportedNodeTypesError, gate_flow_nodes
    from ..services.runtime_pools import normalize_runtime
except ImportError:
    from services.capability_gate import (  # type: ignore
        UnsupportedNodeTypesError,
        gate_flow_nodes,
    )
    from services.runtime_pools import normalize_runtime  # type: ignore

try:  # A2R24-09：SSE 路由的 OpenAPI 媒体类型声明
    from ..streaming import EventStreamResponse
except ImportError:
    from streaming import EventStreamResponse  # type: ignore

logger = logging.getLogger(__name__)

router = APIRouter()


# ============ 请求/响应模型 ============

class FlowSummaryView(BaseModel):
    flow_id: str
    tenant_id: str = ""
    author: str = ""
    desc: str = ""
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class FlowListResponse(BaseModel):
    flows: List[FlowSummaryView]
    total: int


class CreateFlowRequest(StrictRequestModel):
    flow_id: str = Field(..., description="流程 ID")
    author: str = Field("", description="作者")
    desc: str = Field("", description="描述")


class VersionSummaryView(BaseModel):
    """详情内嵌版本摘要（A2R24-07）：与单版本 ``VersionView`` 同名字段的有类型子集。

    ``definition`` / ``layout`` 体积大且详情不消费，仍走单版本端点读取。
    """

    version: str
    status: str
    # plaita#300：目标 runtime pool（go/python），节点面板/列表按池过滤展示
    runtime: str = "python"
    created_at: Optional[str] = None
    published_at: Optional[str] = None
    updated_at: Optional[str] = None


class FlowDetailResponse(BaseModel):
    flow_id: str
    # A2R24-07：详情补齐列表已有的实体字段（平台管理员需要从详情确认归属/
    # 更新时间；列表 ``FlowSummaryView`` 即同一集合）。
    tenant_id: str = ""
    author: str = ""
    desc: str = ""
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    versions: List[VersionSummaryView] = Field(default_factory=list)


class VersionView(BaseModel):
    flow_id: str
    version: str
    status: str
    definition: str = ""
    layout: str = ""
    # plaita#300：目标 runtime pool（go/python）——发布/启动能力门的判定池
    runtime: str = "python"
    created_at: Optional[str] = None
    published_at: Optional[str] = None
    created_by: str = ""
    # C5-1：草稿最近保存时间——前端保存时的乐观锁基准
    updated_at: Optional[str] = None
    # A2R24-07：实体租户（平台视角下详情不再丢失归属）
    tenant_id: str = ""


class SaveVersionRequest(StrictRequestModel):
    definition: str = Field(..., description="Flow 定义 JSON 字符串")
    layout: str = Field("{}", description="画布坐标 JSON 字符串")
    created_by: str = Field("", description="保存人")
    # plaita#300：目标 runtime pool（go/python）。None = 不改（新建为 python，
    # 更新既有草稿保留原值）；非法值 422。
    runtime: Optional[str] = Field(
        None, description="目标 runtime pool（go/python）；缺省不改/默认 python"
    )
    # C5-1 乐观并发：调用方所基于版本的 updated_at（ISO 串）；不一致返回
    # 409（conflict=version_stale，带 latest_updated_at）。force=True 绕过。
    base_updated_at: Optional[str] = Field(None, description="乐观锁基准 updated_at")
    force: bool = Field(False, description="强制覆盖（绕过乐观锁检查）")
    allocate_version: bool = Field(
        False, description="忽略 path 中的版本号，由服务端原子分配下一个 patch 版本"
    )


class PublishRequest(StrictRequestModel):
    version: str = Field(..., description="要发布的版本号")


def _store() -> flow_store.FlowStore:
    return flow_store.get_flow_store()


def get_redis_dep(request: Request) -> Redis:
    """引擎同步用 Redis 客户端。本地单机模式返回 None（发布/删除跳过引擎同步）。"""
    return request.app.state.redis


def _check_semver(version: str) -> None:
    if not validation.is_semver(version):
        raise HTTPException(status_code=422, detail=f"非法 semver 版本号: {version}")


def _check_runtime(runtime: Optional[str]) -> None:
    """runtime 取值域闸（plaita#300）：非法值 422（保存入口 fail-fast）。"""
    if runtime is None:
        return
    try:
        normalize_runtime(runtime)
    except ValueError as e:
        raise HTTPException(
            status_code=422, detail=error_detail("invalid_runtime", str(e))
        )


def _gate_runtime_capability(redis: Optional[Redis], row, *, where: str) -> None:
    """发布链路共用能力门（plaita#300）：不支持的节点集 → 422 结构化错误。

    ``redis=None``（本地单机档：无 registry、执行在 console 进程内）跳过——
    门的数据源是 worker 注册清单，本地档不存在远端 worker。
    """
    if redis is None:
        return
    try:
        gate_flow_nodes(
            redis, getattr(row, "runtime", None) or "python", row.definition,
            flow_id=row.flow_id, version=row.version,
        )
    except UnsupportedNodeTypesError as e:
        detail = dict(e.detail)
        raise HTTPException(
            status_code=422,
            detail=error_detail(
                "unsupported_node_types",
                (
                    f"流程 {row.flow_id}@{row.version} 含目标 runtime "
                    f"{detail.get('runtime')} 不支持的节点类型: "
                    f"{', '.join(detail.get('unsupported') or [])}"
                ),
                **detail,
            ),
        ) from e


def _validate_definition(definition: str) -> None:
    """用 Flow.model_validate 强校验 definition，失败抛 422。"""
    try:
        data = json.loads(definition)
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=422,
            detail=scrubbed_detail(
                logger, "invalid_definition_json", "definition 不是合法 JSON", e
            ),
        )
    try:
        validation.validate_definition(data)
    except ValueError as e:
        raise HTTPException(
            status_code=422, detail=error_detail("invalid_definition", str(e))
        )


# ============ 端点 ============

@router.get("/flows", response_model=FlowListResponse)
def list_flows(request: Request = None):
    tenant = tenant_scope(request) if request is not None else None
    flows = _store().list_flows(tenant_id=tenant)
    views = [
        FlowSummaryView(
            flow_id=f.flow_id,
            tenant_id=f.tenant_id,
            author=f.author,
            desc=f.desc,
            created_at=f.created_at.isoformat() if f.created_at else None,
            updated_at=f.updated_at.isoformat() if f.updated_at else None,
        )
        for f in flows
    ]
    return FlowListResponse(flows=views, total=len(views))


class AiGenerateRequest(StrictRequestModel):
    prompt: str = Field(..., description="自然语言需求描述")
    agent: str = Field("glm-52", description="编码 Agent profile（agentproc/agents.json）")


@router.post("/flows/ai-generate/stream", response_class=EventStreamResponse)
def ai_generate_stream(req: AiGenerateRequest, request: Request,
                       identity: Dict[str, Any] = Depends(require_auth)):
    """AI 生成流程（SSE）：后端作为 agent 宿主跑真实编码 Agent，事件流给前端。

    设计（ADR-2026-08-27 phase2）：后端不直连 LLM 端点；Agent 循环由 agentproc
    执行的编码 Agent 负责，编译校验为宿主的确定性检查，错误回喂下一轮。

    pn#20：在宿主上跑编码 Agent（``--permission-mode auto``）是**平台能力**，
    不是租户编辑功能。``require_auth`` 只保证 editor+，租户 editor/admin 都能
    通过；这里补平台身份闸（对齐 ``/api/users`` 族）——租户角色 403 具名拒绝，
    拒绝尝试进审计，且 agent 生成器零启动。
    """
    ensure_platform_admin(
        request, identity, action="flows.ai_generate.stream", resource="flow",
    )
    if not req.prompt.strip():
        raise HTTPException(status_code=422, detail="prompt 不能为空")
    # 平台管理员可带 X-Tenant-ID 指定工作区命名空间；无则 platform。
    tenant_id = tenant_scope(request)
    from fastapi.responses import StreamingResponse
    try:
        from ..services import ai_flow
    except ImportError:  # 平铺布局（cwd=backend）运行时
        from services import ai_flow

    # A55-S1 残面：r55 时本端点只有 ensure_platform_admin 的 denied 审计，
    # 成功生成（平台宿主上跑编码 Agent）无留痕。只记 agent/工作区租户，
    # prompt 与生成物一律不入库。
    _audit(request, "flow.ai_generate", "",
           {"agent": req.agent, "tenant_id": tenant_id or ""},
           tenant_id=tenant_id or "")

    def event_stream():
        for ev in ai_flow.generate_flow_events(req.prompt.strip(), tenant_id=tenant_id):
            yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


@router.post("/flows", response_model=FlowSummaryView)
def create_flow(req: CreateFlowRequest, request: Request = None):
    if not req.flow_id:
        raise HTTPException(status_code=422, detail="flow_id 不能为空")
    tenant = tenant_scope(request, required=True) if request is not None else ""
    try:
        rec = _store().create_flow(req.flow_id, author=req.author, desc=req.desc,
                                   tenant_id=tenant)
    except ValueError as e:
        raise HTTPException(
            status_code=409, detail=error_detail("flow_conflict", str(e))
        )
    _audit(request, "flow.create", req.flow_id,
           {"author": req.author, "desc": req.desc}, tenant_id=tenant)
    return FlowSummaryView(
        flow_id=rec.flow_id,
        tenant_id=rec.tenant_id,
        author=rec.author,
        desc=rec.desc,
        created_at=rec.created_at.isoformat() if rec.created_at else None,
        updated_at=rec.updated_at.isoformat() if rec.updated_at else None,
    )


def _resolve_platform_tenant(store, flow_id: str, version: Optional[str] = None) -> Optional[str]:
    """平台全量视角（``tenant is None``）下解析实体租户（A2R24-07）。

    ``flow_id`` 只在租户内唯一：两个租户都发布同名 flow 时，旧的
    ``get_flow_record(..., tenant_id=None).first()`` 没有 tie-breaker，详情/
    导出会静默挑一个。这里显式 409 并列出候选租户，调用方带 ``X-Tenant-ID``
    重试即可。单租户（或不存在）时返回该租户，后续读面带租户过滤。
    """
    tenants = (
        store.list_version_tenants(flow_id, version)
        if version is not None
        else store.list_flow_tenants(flow_id)
    )
    if len(tenants) > 1:
        raise HTTPException(
            status_code=409,
            detail=error_detail(
                "ambiguous_flow_id",
                f"{flow_id}"
                + (f"@{version}" if version is not None else "")
                + " 在多个租户中存在，请用 X-Tenant-ID 指定租户",
                available_tenants=tenants,
            ),
        )
    return tenants[0] if tenants else None


@router.get("/flows/{flow_id}", response_model=FlowDetailResponse)
def get_flow(flow_id: str, request: Request = None):
    tenant = tenant_scope(request) if request is not None else None
    store = _store()
    if tenant is None:
        tenant = _resolve_platform_tenant(store, flow_id)
    rec = store.get_flow_record(flow_id, tenant_id=tenant)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"流程不存在: {flow_id}")
    versions = [
        VersionSummaryView(
            version=v.version,
            status=v.status,
            runtime=getattr(v, "runtime", None) or "python",
            created_at=v.created_at.isoformat() if v.created_at else None,
            published_at=v.published_at.isoformat() if v.published_at else None,
            updated_at=v.updated_at.isoformat() if v.updated_at else None,
        )
        for v in store.list_versions(flow_id, tenant_id=tenant)
    ]
    return FlowDetailResponse(
        flow_id=rec.flow_id,
        tenant_id=rec.tenant_id,
        author=rec.author,
        desc=rec.desc,
        created_at=rec.created_at.isoformat() if rec.created_at else None,
        updated_at=rec.updated_at.isoformat() if rec.updated_at else None,
        versions=versions,
    )


@router.delete("/flows/{flow_id}")
def delete_flow(flow_id: str, request: Request = None,
                redis: Redis = Depends(get_redis_dep),
                body: Optional[EmptyRequestModel] = None):
    tenant = tenant_scope(request, required=True) if request is not None else ""
    # #301：跨 Redis 部署先清目标侧（与 outbox 投递同源配置）。先清后删：
    # 若下方删除事务失败回滚，已 delivered 的 outbox 行仍在，对账会把 key
    # 重投自愈；反之（先删后清）清理失败会留下无行覆盖的幽灵 key。
    _outbox_purge_target(flow_id, tenant_id=tenant, request=request)
    try:
        _store().delete_flow(flow_id, tenant_id=tenant)
    except LookupError as e:
        raise HTTPException(
            status_code=404, detail=error_detail("flow_not_found", str(e))
        )
    # 引擎运行时存储同步清理（本地模式无 Redis，无需清理）
    if redis is not None:
        remove_flow_from_engine(redis, flow_id, tenant_id=tenant)
    if request is not None:
        _audit(request, "flow.delete", flow_id, tenant_id=tenant)
    return {"success": True, "flow_id": flow_id}


@router.get("/flows/{flow_id}/versions/{version}", response_model=VersionView)
def get_version(flow_id: str, version: str, request: Request = None):
    tenant = tenant_scope(request) if request is not None else None
    store = _store()
    if tenant is None:
        tenant = _resolve_platform_tenant(store, flow_id, version)
    out = store.get_version(flow_id, version, tenant_id=tenant)
    if out is None:
        raise HTTPException(status_code=404, detail=f"版本不存在: {flow_id}@{version}")
    return VersionView(
        flow_id=out.flow_id,
        version=out.version,
        status=out.status,
        definition=out.definition,
        layout=out.layout,
        runtime=getattr(out, "runtime", None) or "python",
        created_at=out.created_at.isoformat() if out.created_at else None,
        published_at=out.published_at.isoformat() if out.published_at else None,
        created_by=out.created_by,
        updated_at=out.updated_at.isoformat() if out.updated_at else None,
        tenant_id=out.tenant_id,
    )


@router.get("/flows/{flow_id}/versions/{version}/source")
def get_version_source(flow_id: str, version: str, request: Request = None):
    """版本的 codeflow 源码视图（源码面板 @flow 页签数据源）。

    优先 ``definition.metadata.source``——``plaita build --embed-source`` /
    mediaflow publish_console 注入的**仓内权威源码**，行号与节点 source_line
    对应，可跳转；缺失时 ``emit_source`` **反编译兜底**——语义等效但非权威：
    注释与原始 id 不可恢复，行号不对应任何仓内文件，前端据此标注并停用
    行号跳转（见 SourceViewPanel / NodeConfigDrawer 的 sourceKind 分支）。
    """
    tenant = tenant_scope(request) if request is not None else None
    out = _store().get_version(flow_id, version, tenant_id=tenant)
    if out is None:
        raise HTTPException(status_code=404, detail=f"版本不存在: {flow_id}@{version}")
    try:
        defn = json.loads(out.definition or "{}")
    except ValueError:
        defn = None
    if isinstance(defn, dict):
        md = defn.get("metadata")
        if isinstance(md, dict) and isinstance(md.get("source"), str) and md["source"].strip():
            return {"source_kind": "authoritative", "source": md["source"]}
        if defn.get("nodes"):
            try:
                from plaita.dsl.codeflow import emit_source

                return {"source_kind": "decompiled", "source": emit_source(defn)}
            except Exception as e:  # noqa: BLE001 — 不可表达的构造如实回原因
                return {"source_kind": "unavailable", "source": None,
                        "reason": f"emit_source 失败: {e}"}
    return {"source_kind": "unavailable", "source": None,
            "reason": "定义缺失或不可解析"}


@router.put("/flows/{flow_id}/versions/{version}", response_model=VersionView)
def save_version(flow_id: str, version: str, req: SaveVersionRequest,
                 request: Request = None):  # noqa: ANN001 — FastAPI 注入
    tenant = tenant_scope(request, required=True) if request is not None else ""
    _check_semver(version)
    _validate_definition(req.definition)
    _check_runtime(req.runtime)
    store = _store()
    # 确保 flow 记录存在
    if store.get_flow_record(flow_id, tenant_id=tenant) is None:
        raise HTTPException(status_code=404, detail=f"流程不存在: {flow_id}")
    try:
        result = store.save_flow_definition(
            flow_id=flow_id,
            version=version,
            definition=req.definition,
            layout=req.layout,
            status="draft",
            created_by=req.created_by,
            tenant_id=tenant,
            base_updated_at=req.base_updated_at,
            force=req.force,
            allocate_version=req.allocate_version,
            runtime=req.runtime,
        )
    except flow_store.VersionConflictError as e:
        # C5-1：结构化 409——前端据此弹「加载最新 / 强制覆盖」
        raise HTTPException(
            status_code=409,
            detail=error_detail(
                "version_stale", str(e),
                conflict="version_stale",
                latest_updated_at=e.latest_updated_at,
            ),
        )
    except ValueError as e:
        raise HTTPException(
            status_code=409, detail=error_detail("version_conflict", str(e))
        )
    # allocate_version 时服务端可能分配了与 path 不同的版本号：以返回值为准
    out = store.get_version(flow_id, result.version, tenant_id=tenant)
    if request is not None:
        _audit(request, "flow.save_version", f"{flow_id}@{result.version}",
               {"bytes": len(req.definition)}, tenant_id=tenant)
    return VersionView(
        flow_id=out.flow_id,
        version=out.version,
        status=out.status,
        definition=out.definition,
        layout=out.layout,
        runtime=getattr(out, "runtime", None) or "python",
        created_at=out.created_at.isoformat() if out.created_at else None,
        published_at=out.published_at.isoformat() if out.published_at else None,
        created_by=out.created_by,
        updated_at=out.updated_at.isoformat() if out.updated_at else None,
        tenant_id=out.tenant_id,
    )


@router.delete("/flows/{flow_id}/versions/{version}")
def delete_version(flow_id: str, version: str, request: Request = None,
                   redis: Redis = Depends(get_redis_dep),
                   body: Optional[EmptyRequestModel] = None):
    tenant = tenant_scope(request, required=True) if request is not None else ""
    # #301：同 delete_flow——先清目标侧再删行（失败回滚可对账自愈）。
    _outbox_purge_target(flow_id, tenant_id=tenant, version=version,
                         request=request)
    prior = _store().get_version(flow_id, version, tenant_id=tenant)
    try:
        _store().delete_version(flow_id, version, tenant_id=tenant)
    except LookupError as e:
        raise HTTPException(
            status_code=404, detail=error_detail("version_not_found", str(e))
        )
    if redis is not None:
        remove_flow_version_from_engine(redis, flow_id, version, tenant_id=tenant)
    if request is not None:
        _audit(request, "flow.delete_version", f"{flow_id}@{version}",
               {"version": version,
                "prior_status": prior.status if prior is not None else None},
               tenant_id=tenant)
    return {"success": True, "flow_id": flow_id, "version": version}


@router.post("/flows/publish-outbox/{row_id}/resolve")
def resolve_publish_outbox(row_id: int, request: Request = None,
                           body: Optional[EmptyRequestModel] = None):
    """DLQ 人工出口·resolve（c117-06）：认定该行无需再投递，置 resolved 终态。

    resolved 行不进投递扫描、不进对账，行体与失败原因保留作留痕；
    /health.engine_outbox.resolved 计数。delivered 行 409（无需人工出口）。
    """
    return _outbox_manual_exit(row_id, "resolve", request)


@router.post("/flows/publish-outbox/{row_id}/discard")
def discard_publish_outbox(row_id: int, request: Request = None,
                           body: Optional[EmptyRequestModel] = None):
    """DLQ 人工出口·discard（c117-06）：放弃投递并删除该行（无留痕，仅审计）。

    delivered 行 409——删行会失去对账覆盖；若定义也不该留在目标 Redis，
    走删除流程/版本接口（#301 清理三件）。
    """
    return _outbox_manual_exit(row_id, "discard", request)


@router.post("/flows/{flow_id}/publish", response_model=VersionView)
def publish_flow(flow_id: str, req: PublishRequest, request: Request = None,
                 redis: Redis = Depends(get_redis_dep)):
    tenant = tenant_scope(request, required=True) if request is not None else ""
    _check_semver(req.version)
    store = _store()
    if store.get_flow_record(flow_id, tenant_id=tenant) is None:
        raise HTTPException(status_code=404, detail=f"流程不存在: {flow_id}")
    # plaita#300：能力门在状态机 draft→published **之前**——不支持的节点集
    # 结构化拒绝（422），版本保持 draft、引擎存储零写入（不入队后失败）。
    row = store.get_version(flow_id, req.version, tenant_id=tenant)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=error_detail("version_not_found", f"版本不存在: {flow_id}@{req.version}"),
        )
    _gate_runtime_capability(redis, row, where="publish")
    try:
        out = store.publish_version(flow_id, req.version, tenant_id=tenant)
    except LookupError as e:
        raise HTTPException(
            status_code=404, detail=error_detail("version_not_found", str(e))
        )
    # 发布即同步到引擎运行时存储：FlowWorker 从这里解析定义，
    # 不同步则「发布成功、启动必失败」。本地单机模式无 Redis：
    # 执行走 console 进程内（SQLite 定义），无需同步。
    if redis is not None:
        sync_flow_to_engine(redis, flow_id, out.version, out.definition, tenant_id=tenant)
    if request is not None:
        try:
            from ..config import get_settings
        except ImportError:
            from config import get_settings
        env = get_settings().console_env
        _audit(request, "flow.publish", f"{flow_id}@{out.version}",
               {"env": env}, tenant_id=tenant)
        # 部署记录（切片 C）
        try:
            from ..services import deployments as deployments_svc
        except ImportError:
            from services import deployments as deployments_svc  # type: ignore
        deployments_svc.record(
            flow_id=flow_id, version=out.version, environment=env,
            actor=getattr(request.state, "actor", ""),
            definition=out.definition,
            tenant_id=tenant,
        )
    return VersionView(
        flow_id=out.flow_id,
        version=out.version,
        status=out.status,
        definition=out.definition,
        layout=out.layout,
        runtime=getattr(out, "runtime", None) or "python",
        created_at=out.created_at.isoformat() if out.created_at else None,
        published_at=out.published_at.isoformat() if out.published_at else None,
        created_by=out.created_by,
        updated_at=out.updated_at.isoformat() if out.updated_at else None,
        tenant_id=out.tenant_id,
    )


def _audit(request: Request, action: str, resource_id: str,
           detail: Dict | None = None, *, tenant_id: Optional[str] = None) -> None:
    try:
        from ..services import audit as audit_svc
    except ImportError:
        try:
            from services import audit as audit_svc  # type: ignore
        except ImportError:
            return
    audit_svc.record(request, action=action, resource="flow", resource_id=resource_id,
                     detail=detail, tenant_id=tenant_id)


def _outbox_sessions():
    try:
        from ..services.flow_store import session_factory
    except ImportError:
        from services.flow_store import session_factory  # type: ignore
    return session_factory()


def _outbox_purge_target(flow_id: str, *, tenant_id: str,
                         version: Optional[str] = None,
                         request: Request = None) -> None:
    """#301：删除流程/版本时清**目标 Redis** 侧引擎存储三件（尽力而为）。

    与 outbox 投递同源配置（``TARGET_REDIS_URL``）；未启用（同 Redis 部署）
    时 no-op。复用 lifespan 缓存的目标连接（``app.state.engine_outbox_target``）
    避免逐次建连。清理失败只 ERROR 不阻断删除——console 是权威库，但行已
    随删除事务 purge、对账不再覆盖该 key，幽灵 key 需按日志指引人工对齐。
    """
    try:
        try:
            from ..services import engine_outbox as _engine_outbox
        except ImportError:
            from services import engine_outbox as _engine_outbox  # type: ignore
        client = getattr(getattr(request, "app", None), "state", None) \
            if request is not None else None
        client = getattr(client, "engine_outbox_target", None)
        _engine_outbox.purge_target_keys(
            flow_id, tenant_id=tenant_id, version=version, client=client
        )
    except Exception as e:  # noqa: BLE001 — 清理失败不阻断删除，但必须 loud
        logger.error(
            "outbox 目标侧删除清理异常（flow=%s@%s）：目标 Redis 可能残留"
            "幽灵 key，需人工对齐: %s", flow_id, version or "*", e,
        )


def _outbox_manual_exit(row_id: int, action: str, request: Request = None) -> Dict:
    """DLQ 人工出口（c117-06）共用：resolve=置终态留痕 / discard=删行。

    delivered 行拒绝（409）——resolve 失去意义、discard 会失去对账覆盖；
    租户不匹配/行不存在 → 404。
    """
    tenant = tenant_scope(request, required=True) if request is not None else ""
    try:
        from ..services import engine_outbox as _engine_outbox
    except ImportError:
        from services import engine_outbox as _engine_outbox  # type: ignore
    fn = (_engine_outbox.resolve_row if action == "resolve"
          else _engine_outbox.discard_row)
    actor = getattr(getattr(request, "state", None), "actor", "") or ""
    try:
        brief = fn(_outbox_sessions(), row_id, tenant_id=tenant, actor=actor)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=error_detail(
            "outbox_row_not_eligible", str(e)))
    if brief is None:
        raise HTTPException(status_code=404, detail=error_detail(
            "outbox_row_not_found", f"outbox 行不存在或不在当前租户: {row_id}"))
    if request is not None:
        _audit(request, f"outbox.{action}", str(row_id),
               {"flow_id": brief["flow_id"], "version": brief["version"],
                "prior_status": brief["status"]},
               tenant_id=tenant)
    return {"success": True, **brief}


@router.get("/flows/{flow_id}/versions/{version}/export")
def export_version(flow_id: str, version: str, request: Request = None):
    """导出晋升包（定义 + 指纹 + 元信息），跨环境 console 晋升的载体。

    A2R24-07：晋升包的 ``tenant_id`` 取**实体租户**，不再取调用方上下文
    （平台 API-Key 导出 default 租户的版本时此前返回 ``""``，源租户丢失）。
    平台全量视角下同名 flow 跨租户 → 409 ``ambiguous_flow_id``。
    """
    tenant = tenant_scope(request) if request is not None else None
    store = _store()
    if tenant is None:
        tenant = _resolve_platform_tenant(store, flow_id, version)
    try:
        from ..services import deployments as deployments_svc
    except ImportError:
        from services import deployments as deployments_svc  # type: ignore
    try:
        return deployments_svc.build_promotion_package(store, flow_id, version,
                                                       tenant_id=tenant)
    except LookupError as e:
        raise HTTPException(
            status_code=404, detail=error_detail("version_not_found", str(e))
        )


class ImportPromotionRequest(StrictRequestModel):
    package: Dict[str, Any]
    new_version: Optional[str] = None
    publish: bool = False
    # A2R25-02：目标版本已有草稿时默认 409，必须显式 force 才允许覆盖。
    force: bool = Field(False, description="允许覆盖同版本既有草稿（写入审计）")


@router.post("/flows/import-version")
def import_version(req: ImportPromotionRequest, request: Request = None,
                   redis: Redis = Depends(get_redis_dep)):
    """导入晋升包：默认为草稿；publish=true 等价于发布（引擎同步 + 部署记录）。"""
    tenant = tenant_scope(request, required=True) if request is not None else ""
    try:
        from ..services import deployments as deployments_svc
    except ImportError:
        from services import deployments as deployments_svc  # type: ignore
    try:
        result = deployments_svc.import_promotion_package(
            _store(), req.package, new_version=req.new_version,
            tenant_id=tenant, force=req.force,
        )
    except deployments_svc.PromotionConflictError as e:
        raise HTTPException(
            status_code=409,
            detail=error_detail(
                e.conflict, str(e), conflict=e.conflict, **e.details
            ),
        )
    except deployments_svc.PromotionValidationError as e:
        raise HTTPException(
            status_code=422, detail=error_detail("promotion_invalid", str(e))
        )
    except ValueError as e:
        raise HTTPException(
            status_code=400, detail=error_detail("import_invalid", str(e))
        )

    flow_id, version = result["flow_id"], result["version"]
    if req.publish:
        # A2R25-03：发布前再校验一次存储定义——导入链路不因 publish=true
        # 绕过正常保存/发布已有的 Flow schema 闸。
        stored = _store().get_version(flow_id, version, tenant_id=tenant)
        if stored is None:
            raise HTTPException(
                status_code=404, detail=f"版本不存在: {flow_id}@{version}"
            )
        _validate_definition(stored.definition)
        # plaita#300：导入直发的 publish 与普通发布同一能力门（结构化 422，
        # 版本保持 draft）。
        _gate_runtime_capability(redis, stored, where="import.publish")
        try:
            out = _store().publish_version(flow_id, version, tenant_id=tenant)
        except LookupError as e:
            raise HTTPException(
                status_code=404, detail=error_detail("version_not_found", str(e))
            )
        sync_flow_to_engine(redis, flow_id, out.version, out.definition, tenant_id=tenant)
        try:
            from ..config import get_settings
        except ImportError:
            from config import get_settings
        deployments_svc.record(
            flow_id=flow_id, version=out.version,
            environment=get_settings().console_env,
            actor=getattr(request.state, "actor", "") if request else "",
            definition=out.definition,
            tenant_id=tenant,
        )
        result["status"] = "published"

    if request is not None:
        _audit(
            request,
            "flow.import",
            f"{flow_id}@{version}",
            {
                "publish": req.publish,
                "force": req.force,
                "overwritten": result.get("overwritten", False),
                "previous_status": result.get("previous_status"),
                "previous_definition_hash": result.get("previous_definition_hash"),
                "definition_hash": result.get("definition_hash"),
            },
            tenant_id=tenant,
        )
    return result
