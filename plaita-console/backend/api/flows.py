"""
流程定义 CRUD + 版本管理 API

- GET    /api/flows                            流程列表
- POST   /api/flows                            新建流程（仅元信息）
- GET    /api/flows/{flow_id}                  流程详情 + 版本列表
- DELETE /api/flows/{flow_id}                  删除流程及其全部版本（#301：同步清目标 Redis 三件；#304：published/在途引用删除守卫）
- GET    /api/flows/{flow_id}/versions/{ver}   取某版本定义（含 layout）
- GET    /api/flows/{flow_id}/versions/{ver}/source   codeflow 源码视图（权威 / emit_source 反编译兜底）
- PUT    /api/flows/{flow_id}/versions/{ver}   保存草稿（Flow.model_validate 强校验）
- DELETE /api/flows/{flow_id}/versions/{ver}   删除版本（#301：同步清目标 Redis 三件；#304：published/在途引用删除守卫）
- POST   /api/flows/{flow_id}/publish          发布版本（draft → published）
- POST   /api/flows/publish-outbox/{id}/resolve  DLQ 人工出口·resolve（c117-06，置终态留痕）
- POST   /api/flows/publish-outbox/{id}/discard   DLQ 人工出口·discard（c117-06，放弃并删行）
"""
import json
import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
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

try:  # plaita#304：版本删除守卫的在途引用扫描（执行域助手单点在 executions）
    from .executions import (
        VersionInUseScanError,
        VersionInUseScanSaturatedError,
        find_flow_version_active_executions,
    )
except ImportError:  # pragma: no cover - 平铺布局（cwd=backend）运行时
    from executions import (  # type: ignore
        VersionInUseScanError,
        VersionInUseScanSaturatedError,
        find_flow_version_active_executions,
    )

try:  # A2R24-01：统一错误 detail 形状 + 异常文本脱敏
    from ..error_contract import error_detail, scrubbed_detail
except ImportError:
    from error_contract import error_detail, scrubbed_detail

# A126-02：扫描 503 文案按瞬态/持久分流（持久 = 坏行具名，重试无效）
from plaita.storage.redis import ExecutionStateCorruptError

try:  # plaita#300：runtime 能力门 + 池路由取值域；#331：执行要求闸
    from ..services.capability_gate import (
        ExecutionProfileUnsatisfiedError,
        UnsupportedNodeTypesError,
        gate_flow_execution_profile,
        gate_flow_nodes,
    )
    from ..services.runtime_pools import DEFAULT_RUNTIME, normalize_runtime
except ImportError:
    from services.capability_gate import (  # type: ignore
        ExecutionProfileUnsatisfiedError,
        UnsupportedNodeTypesError,
        gate_flow_execution_profile,
        gate_flow_nodes,
    )
    from services.runtime_pools import (  # type: ignore
        DEFAULT_RUNTIME,
        normalize_runtime,
    )

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
    # plaita#300：目标 runtime pool（go/python/rust），节点面板/列表按池过滤展示
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
    # plaita#300：目标 runtime pool（go/python/rust）——发布/启动能力门的判定池
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
    # plaita#300：目标 runtime pool（go/python/rust）。None = 不改（新建为 python，
    # 更新既有草稿保留原值）；非法值 422。
    runtime: Optional[str] = Field(
        None, description="目标 runtime pool（go/python/rust）；缺省不改/默认 python"
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


def _gate_definition_encodable(row) -> None:
    """发布前置编码闸（卡 A-1 / A129-01）：canonical hash 不可计算 → 422。

    与 #300 能力门同层、同在状态翻转之前：孤立代理转义等不可 UTF-8 编码
    的定义，此前 ``enqueue_publish`` 吞 ``UnicodeEncodeError``（⊂ValueError）
    后发布照常成功——outbox 无行、Redis 有键（save 面默认 dumps 不炸）、
    Python 池读回 hash 即炸、Go 池（G-1 后）拒载：两池皆不可执行却拖到
    运行期才炸。拒于发布面：版本保持 draft、引擎存储零写入、outbox 零行。
    不依赖 ``redis``/outbox 是否启用（判据是定义自身）。
    """
    try:
        from ..services.engine_outbox import (
            DefinitionUnencodableError,
            ensure_definition_encodable,
        )
    except ImportError:
        from services.engine_outbox import (  # type: ignore
            DefinitionUnencodableError,
            ensure_definition_encodable,
        )
    try:
        ensure_definition_encodable(row.flow_id, row.version, row.definition or "")
    except DefinitionUnencodableError as e:
        raise HTTPException(
            status_code=422,
            detail=error_detail(
                "definition_unencodable", str(e),
                flow_id=row.flow_id, version=row.version,
            ),
        ) from e


def _gate_execution_profile(redis: Optional[Redis], row, *, where: str) -> None:
    """执行要求闸（plaita#331）：闭包 token × 目标档 profile → 422 结构化拒绝。

    落点在 `_publish_gates` 共享函数（A2 r61 评审 §2.1）：publish 与
    import-version 两端点自动同闸。目标档：本地单机档（``redis=None``，进程内
    执行、无持久队列+延迟排程）按 ``local`` 判——delay 族在此档发布即具名拒；
    集群档发布面向 worker 池（``distributed``，全集）——本闸对内置词表恒过，
    池维度节点支持性由 #300 门负责（同一发布面的两个输入）。
    """
    profile = "local" if redis is None else "distributed"
    # 本地档点名（静态）：两级模型下 managed:queue 子档废除——本地档无 Redis
    # ⇒ wake 唤醒源必不在岗，凡声明 wake_service 的节点发布即 422（错误码与
    # 集群点名一致 queue_service_absent）；集群档发布不点名（先发布后起服务
    # 的时序合法，点名在受理门）。本地档早期拒绝 UX 由此保留。
    if redis is None:
        try:
            from ..services.capability_gate import definition_wake_services
        except ImportError:  # 平铺布局（cwd=backend）
            from services.capability_gate import definition_wake_services  # type: ignore
        wake = definition_wake_services(row.definition)
        if wake:
            raise HTTPException(
                status_code=422,
                detail=error_detail(
                    "queue_service_absent",
                    (
                        f"流程 {row.flow_id}@{row.version} 的节点挂起后需要唤醒服务"
                        f"（{', '.join(wake)}），本地单机档没有唤醒服务进程——发布后"
                        "将无法启动（启动即永久挂起）。请接上 Redis 升级为集群档，"
                        "或把对应节点换成立即可返回的写法"
                    ),
                    wake_services=wake,
                ),
            )
    try:
        gate_flow_execution_profile(
            row.definition, profile=profile,
            flow_id=row.flow_id, version=row.version,
        )
    except ExecutionProfileUnsatisfiedError as e:
        detail = dict(e.detail)
        raise HTTPException(
            status_code=422,
            detail=error_detail(
                str(detail.get("error") or "execution_requirements_unmet"),
                (
                    f"流程 {row.flow_id}@{row.version} 的执行要求 "
                    f"{detail.get('required')} 不被目标执行档 "
                    f"{detail.get('profile')} 满足，缺少 {detail.get('missing')}"
                    f"（{where}）；版本保持 draft。"
                ),
                **detail,
            ),
        ) from e


def _publish_gates(redis: Optional[Redis], row, *, where: str) -> None:
    """发布前置闸合集：状态翻转前与事务内 fresh 行复核（A2R54）共用同一组。

    编码闸（卡 A-1，定义自身判据、零 Redis 往返）先于能力门（#300，
    查 worker 注册清单）与执行要求闸（#331，闭包 × profile）。
    """
    _gate_definition_encodable(row)
    _gate_runtime_capability(redis, row, where=where)
    _gate_execution_profile(redis, row, where=where)


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


# ---- plaita#304：版本删除前置守卫 ----
#
# 口径（二选一已拍板并在此落档）：**状态守卫可 force，引用守卫不可越**。
#   * 状态守卫：published 版本默认 409 ``version_published``——与既有
#     「published 不可覆盖」（flow_store.save_definition ValueError→409）
#     同口径；``force=true`` 显式逃生（同 A43-01 执行删除的 force 形态）。
#   * 引用守卫：存在**非终态执行**（queued/running/suspended）引用该
#     flow(+version) → 409 ``version_in_use``，附 execution 清单；force
#     **不可越**。级联取消刻意不采用：取消是异步意图信号，删行+清定义键
#     之后取消路径自身还需要定义键/状态行收口，竞窗内会造出新的孤儿；
#     「先取消 → 待终态 → 再删」是唯一无窗口序，守卫拒绝即引导该序。
#     定义键清理（remove_*_from_engine）只在守卫通过后发生——「确认无
#     引用后才清真源」，resume/start 的具名终态（worker 侧）仅作竞窗与
#     本地档的兜底，不作为常规路径。
#   * 引用守卫之二（A2R52-01）：**钉版调度**（schedule 是版本的第二类
#     引用方，#307 执行扫描翻页/#308 FK 级联均未覆盖）→ 409
#     ``version_pinned_by_schedule`` 附调度清单；force 语义照 published
#     状态守卫（可越但须显式）——force 放行时被钉调度**级联暂停**并
#     标注 ``pinned_version_missing``（cron 幽灵触发面随删除同步止住，
#     降级可观测），调度行本身不删（保用户配置）。
def _find_pinning_schedules(
    redis: Optional[Redis], tenant: Optional[str], flow_id: str,
    scan_version: Optional[str],
) -> List[tuple]:
    """A2R52-01：扫「钉版调度」——版本的第二类引用方（schedule 系）。

    双面都扫（守卫两档共用）：集群档 ``plaita:schedules`` HASH、本地档
    ``local_schedules`` 表（集群部署下该表为空，扫之无害）。只计**钉版行**
    （version 非空）——version=None 是「跟随最新发布」行，其 dangling 维度
    属发布面（A2R50-03 另单），不在本判据。tenant 非空时按行归属过滤
    （flow_id 跨租户可重名，防他租户同 id 调度误挡）。

    返回 ``[(surface, row)]``，surface ∈ {"cluster", "local"}；扫描自身失败
    fail-closed 503（与 #307 执行扫描同口径——守卫不做 fail-open）。
    """
    rows: List[tuple] = []
    if redis is not None:
        try:
            from plaita.server.services.schedule_service import list_schedules
        except ImportError:  # pragma: no cover - 极简部署无 schedule_service
            logger.warning("schedule_service 不可用，跳过集群档调度引用扫描")
        else:
            try:
                rows.extend(
                    ("cluster", r) for r in list_schedules(redis)
                )
            except HTTPException:
                raise
            except Exception as e:  # noqa: BLE001 — 引用核实失败拒绝删除
                raise HTTPException(
                    status_code=503,
                    detail=error_detail(
                        "version_pinned_by_schedule_scan_failed",
                        f"调度引用核实失败，拒绝删除（fail-closed）：稍后重试；{e}",
                    ),
                ) from e
    try:
        from ..services import local_scheduler
    except ImportError:  # pragma: no cover - 平铺布局（cwd=backend）运行时
        from services import local_scheduler  # type: ignore
    try:
        rows.extend(
            ("local", r)
            for r in local_scheduler.list_schedules(
                flow_store.get_flow_store(), tenant_id=None
            )
        )
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001 — 引用核实失败拒绝删除
        raise HTTPException(
            status_code=503,
            detail=error_detail(
                "version_pinned_by_schedule_scan_failed",
                f"调度引用核实失败，拒绝删除（fail-closed）：稍后重试；{e}",
            ),
        ) from e

    tenant_key = tenant or "default"
    out: List[tuple] = []
    for surface, row in rows:
        if row.get("flow_id") != flow_id:
            continue
        if (row.get("tenant_id") or "default") != tenant_key:
            continue
        version = row.get("version")
        if not version:
            continue
        if scan_version is not None and version != scan_version:
            continue
        out.append((surface, row))
    return out


def _pause_pinned_schedules(redis: Optional[Redis], pinned: List[tuple]) -> List[str]:
    """A2R52-01 force 逃生级联：删版放行时暂停被钉调度并打标注。

    调度行不删（保用户配置），只停触发面（``enabled=False`` +
    ``next_run_at=""``）+ 集群档行上标注 ``pinned_version_missing=true``
    （列表/详情视图透出，``_view`` 原样透传）——「孤儿调度每 tick 产出
    失败执行」的持续伤害随删除放行同步止住。本地档无自由列，标注以
    WARNING 留痕。单行暂停失败不回滚删除（删除已完成），WARNING 留痕。
    """
    paused: List[str] = []
    for surface, row in pinned:
        sid = row.get("schedule_id")
        if not sid:
            continue
        try:
            row["enabled"] = False
            row["next_run_at"] = ""
            if surface == "cluster" and redis is not None:
                from plaita.server.services.schedule_service import SCHEDULES_KEY
                row["pinned_version_missing"] = True
                row["updated_at"] = datetime.now().isoformat()
                redis.hset(
                    SCHEDULES_KEY, sid, json.dumps(row, ensure_ascii=False)
                )
            else:
                try:
                    from ..services import local_scheduler
                except ImportError:  # 平铺布局/顶层包导入（测试脚本模式）
                    from services import local_scheduler  # type: ignore
                # 本地档无自由列，pinned_version_missing 不落行——标注以
                # 上方 WARNING 留痕；enabled/next_run_at 随 update 落库。
                local_scheduler.update_schedule(
                    flow_store.get_flow_store(), sid, row, tenant_id=None
                )
            paused.append(sid)
            logger.warning(
                "版本删除 force 放行：被钉调度已暂停并标注 "
                "pinned_version_missing schedule=%s flow=%s@%s",
                sid, row.get("flow_id"), row.get("version"),
            )
        except Exception as e:  # noqa: BLE001 — 删除已完成，调度面降级留痕
            logger.warning(
                "被钉调度暂停失败（版本已删，调度面遗留孤儿）: %s: %s", sid, e
            )
    return paused


def _restamp_schedules_for_publish(
    redis: Optional[Redis],
    tenant: Optional[str],
    flow_id: str,
    version: str,
    runtime_raw: Any,
) -> int:
    """A2R50-03：发布成功后按发布版本行现值重盖同 flow 调度行的 runtime 章。

    #300 的行上章只在 create/update/enable 盖（publish 全路径零调度触点，
    r50 台账静态+行为双证）——version=None（跟随 latest）的调度在发布不同
    runtime 新版本后，cron 按行上陈旧章选池（``_resolve_schedule_queue``）、
    manual trigger 按版本行现值选池，两面分裂（go 版本被 cron 反复打进
    python 池节点级失败）。publish 是 runtime 变更唯一入口，在此源头闭窗，
    章语义恢复「盖章值 = 生效版本行 runtime」——与 create/update/enable
    同口径，值域仍经 ``normalize_runtime``（runtime_queues 单源）。

    范围：同 flow、同租户、生效版本 = 本次发布版本的行——``version`` 空
    （跟随 latest，生效版本随本次发布切换）与钉在本次发布版本上的行（幂等
    重发布顺带自愈 legacy 缺章行）；钉在**其他**版本的行生效版本未变，不碰。
    集群档专用：本地档调度表无 runtime 列、触发在进程内不走队列，行上章无
    语义（``redis=None`` 直接跳过）。

    写入走 ``cas_update_schedule``（WATCH/MULTI 原子、只回写改动字段），
    **不碰 ``updated_at``**——重章是服务端路由校正而非用户编辑，不参与 #198
    乐观锁竞争。best-effort：扫描/单行失败只 WARNING 不回滚发布（与删除
    级联暂停同口径），陈旧窗留待下次发布/重启用收敛。返回重章行数。
    """
    if redis is None:
        return 0
    try:
        runtime = normalize_runtime(runtime_raw)
    except ValueError:
        logger.warning(
            "发布版本 runtime 非法 flow=%s@%s runtime=%r（重章落默认 %s）",
            flow_id, version, runtime_raw, DEFAULT_RUNTIME,
        )
        runtime = DEFAULT_RUNTIME
    try:
        from plaita.server.services.schedule_service import (
            cas_update_schedule,
            list_schedules,
        )

        rows = list_schedules(redis)
    except Exception as e:  # noqa: BLE001 — 发布已完成，重章降级留痕
        logger.warning(
            "发布后调度行重章扫描失败 flow=%s@%s（陈旧章留待下次发布/重启用"
            "收敛）: %s", flow_id, version, e,
        )
        return 0
    tenant_key = tenant or "default"
    restamped = 0
    for row in rows:
        if row.get("flow_id") != flow_id:
            continue
        if (row.get("tenant_id") or "default") != tenant_key:
            continue
        if row.get("version") not in (None, "", version):
            continue
        sid = row.get("schedule_id")
        if not sid or row.get("runtime") == runtime:
            continue

        def _apply(current: dict) -> None:
            current["runtime"] = runtime

        try:
            if cas_update_schedule(redis, sid, _apply) is not None:
                restamped += 1
        except Exception as e:  # noqa: BLE001 — 单行失败不回滚发布
            logger.warning(
                "调度行重章失败 schedule=%s flow=%s@%s: %s",
                sid, flow_id, version, e,
            )
    if restamped:
        logger.info(
            "发布后调度行重章 flow=%s@%s runtime=%s：重盖 %s 行",
            flow_id, version, runtime, restamped,
        )
    return restamped


def _enforce_version_delete_guard(
    redis: Optional[Redis],
    tenant: Optional[str], flow_id: str,
    versions: List[Any], *, force: bool,
) -> List[tuple]:
    """delete_flow / delete_version 共用的删除前置守卫（#304 + A2R52-01）。

    ``versions`` 为本次删除涉及的版本行（delete_version 单行；delete_flow
    全量行）。不通过即抛 HTTPException，删除动作（含目标侧清理与定义键
    清理）完全不发生。通过时返回钉版调度清单（``[(surface, row)]``）——
    force 放行路径由调用方级联暂停（无钉版时为空表，无需处理）。
    """
    published = [v for v in versions if getattr(v, "status", "") == "published"]
    if published and not force:
        raise HTTPException(
            status_code=409,
            detail=error_detail(
                "version_published",
                "版本 "
                + "、".join(f"{flow_id}@{v.version}" for v in published)
                + " 已发布，不可删除（删除会打断在途执行的恢复链）；"
                "如确需删除请显式传 force=true",
                versions=[v.version for v in published],
            ),
        )
    # 引用判据：delete_version 只扫该版本；delete_flow 聚合该 flow 全部版本
    #（version=None 一遍扫完）。
    scan_version = versions[0].version if len(versions) == 1 else None
    try:
        active = find_flow_version_active_executions(
            redis, tenant, flow_id, scan_version
        )
    except VersionInUseScanError as e:
        # A126-02：文案按瞬态/持久分流。坏行（反序列化失败）是持久缺陷——
        # 「稍后重试」对它是错误指引（连删恒 503、非 strict 列表表面健康）；
        # ``str(e)`` 已含守卫透传的具名 state key / execution_id，运维据此
        # 直接定位，处置程序见 ops-runbook「删除守卫 503 的坏行处置」。
        # A133-01 追补第三类：饱和触顶同为**持久**（数据量 vs 上限配置，
        # 「稍后重试」永不自愈）——具名当前上限与 env 旋钮自助路径。
        if isinstance(e, VersionInUseScanSaturatedError):
            guidance = (
                "饱和=持久：flow 执行数已达删除守卫单次扫描上限（数值见本消息"
                "末尾），重试无效——调大 env PLAITA_GUARD_SCAN_SINGLE_LIMIT 后"
                "重启 console，或先清理该 flow 终态执行后再删"
            )
        elif isinstance(e.__cause__, ExecutionStateCorruptError):
            guidance = (
                "执行状态索引存在损坏行（下方具名 state_key），重试无效——"
                "按 ops-runbook「删除守卫 503 的坏行处置」核实并修复/清理该行后再删"
            )
        else:
            guidance = "稍后重试"
        raise HTTPException(
            status_code=503,
            detail=error_detail(
                "version_in_use_scan_failed",
                f"在途执行引用核实失败，拒绝删除（fail-closed）：{guidance}；{e}",
            ),
        ) from e
    if active:
        listing = [
            {"execution_id": s.execution_id, "status": s.status,
             "flow_version": getattr(s, "flow_version", None)}
            for s in active[:20]
        ]
        raise HTTPException(
            status_code=409,
            detail=error_detail(
                "version_in_use",
                f"存在 {len(active)} 条非终态执行引用 "
                f"{flow_id}"
                + (f"@{scan_version}" if scan_version else "（全部版本）")
                + "，先取消（POST /api/executions/{id}/cancel）待终态后再删除",
                executions=listing,
                total=len(active),
            ),
        )
    # 引用判据之二（A2R52-01）：钉版调度——force 语义照 published 状态守卫
    # （可越但须显式）；force 放行时的级联暂停由调用方在删除成功后执行。
    pinned = _find_pinning_schedules(redis, tenant, flow_id, scan_version)
    if pinned and not force:
        listing = [
            {"schedule_id": r.get("schedule_id"), "name": r.get("name"),
             "version": r.get("version"), "enabled": bool(r.get("enabled"))}
            for _, r in pinned[:20]
        ]
        raise HTTPException(
            status_code=409,
            detail=error_detail(
                "version_pinned_by_schedule",
                f"存在 {len(pinned)} 个调度钉住 "
                f"{flow_id}"
                + (f"@{scan_version}" if scan_version else "（钉住该流程的版本）")
                + "，删除将使调度孤儿化（cron 每 tick 产出失败执行）；"
                "请先改钉/删除相关调度，或显式传 force=true"
                "（被钉调度将被暂停并标注 pinned_version_missing）",
                schedules=listing,
                total=len(pinned),
            ),
        )
    return pinned


@router.delete("/flows/{flow_id}")
def delete_flow(flow_id: str, request: Request = None,
                redis: Redis = Depends(get_redis_dep),
                force: bool = Query(
                    False,
                    description="#304：对含 published 版本的 flow 显式强制删除"
                                "（非终态执行引用仍拒绝，force 不可越）",
                ),
                body: Optional[EmptyRequestModel] = None):
    tenant = tenant_scope(request, required=True) if request is not None else ""
    # #304：删除前置守卫——任何 published 版本（no force）/任何非终态执行
    # 引用（force 也不可越）→ 具名 409。守卫通过（或 flow 不存在交由下方
    # 404）才允许触碰目标侧与定义键。flow 不存在时 list_versions 为空、
    # 守卫自然放行，404 语义由既有 LookupError 路径保留。
    versions = _store().list_versions(flow_id, tenant_id=tenant)
    pinned_schedules = _enforce_version_delete_guard(
        redis, tenant, flow_id, versions, force=force,
    )
    # #301：跨 Redis 部署先清目标侧（与 outbox 投递同源配置）。先清后删：
    # 若下方删除事务失败回滚，已 delivered 的 outbox 行仍在，对账会把 key
    # 重投自愈；反之（先删后清）清理失败会留下无行覆盖的幽灵 key。
    # #303：全程经删除守卫（互斥锁 + deleting tombstone）——清 key 后
    # 投递/对账不可能复活该 key；删除事务失败守卫恢复行原状（自愈保留）。
    # A134-02：删行钉在守卫观察到的 {version: status} 快照上（逐行条件
    # DELETE + 幸存者复核）——守卫判据与删行之间夹进的并发 publish 翻转/
    # 新版本直发 → 具名 409 version_stale、零删除，而非静默删掉刚发布版本
    # 并 purge 其 outbox；force 只越 published 状态守卫、不越快照漂移。
    with _outbox_delete_guard(flow_id, tenant_id=tenant):
        _outbox_purge_target(flow_id, tenant_id=tenant, request=request)
        try:
            _store().delete_flow(
                flow_id, tenant_id=tenant,
                expected_versions={v.version: v.status for v in versions},
            )
        except flow_store.VersionConflictError as e:
            raise HTTPException(
                status_code=409,
                detail=error_detail(
                    "version_stale", str(e),
                    conflict="version_stale",
                    latest_updated_at=e.latest_updated_at,
                ),
            )
        except LookupError as e:
            raise HTTPException(
                status_code=404, detail=error_detail("flow_not_found", str(e))
            )
    # A2R52-01：force 放行的钉版调度在删除落地后级联暂停（删除失败 404
    # 时守卫已拒绝，不会走到这里；暂停失败只 WARNING，不回滚删除）。
    paused_schedules = (
        _pause_pinned_schedules(redis, pinned_schedules)
        if force and pinned_schedules else []
    )
    # 引擎运行时存储同步清理（本地模式无 Redis，无需清理）
    if redis is not None:
        remove_flow_from_engine(redis, flow_id, tenant_id=tenant)
    if request is not None:
        _audit(request, "flow.delete", flow_id,
               {"force": force, **({"pinned_schedules_paused": paused_schedules}
                                   if paused_schedules else {})},
               tenant_id=tenant)
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
                   force: bool = Query(
                       False,
                       description="#304：对 published 版本显式强制删除"
                                   "（非终态执行引用仍拒绝，force 不可越）",
                   ),
                   body: Optional[EmptyRequestModel] = None):
    tenant = tenant_scope(request, required=True) if request is not None else ""
    # #304：删除前置守卫——published 默认 409（force 可越）、非终态执行
    # 引用 409（force 不可越）。前置到目标侧清理与删行之前：守卫拒绝时
    # outbox/引擎键一律不碰。版本不存在（prior=None）直接 404：不存在的
    # 版本没有可守卫的状态/真源，也没有可钉的删除快照（A134-02）——
    # 不再先清目标侧再由 store 抛 404（半截清理无意义）。
    prior = _store().get_version(flow_id, version, tenant_id=tenant)
    if prior is None:
        raise HTTPException(
            status_code=404,
            detail=error_detail("version_not_found", f"版本不存在: {flow_id}@{version}"),
        )
    pinned_schedules = _enforce_version_delete_guard(
        redis, tenant, flow_id, [prior], force=force,
    )
    # #301：同 delete_flow——先清目标侧再删行（失败回滚可对账自愈）。
    # #303：同 delete_flow——全程经删除守卫（互斥锁 + deleting tombstone）。
    # A134-02：删行钉在守卫观察到的 status 上（条件 DELETE）——判据读与
    # 删行之间夹进的并发 publish 翻转 → 具名 409 version_stale、行与刚写
    # 出的 outbox 行原样保留；force 只越 published 状态守卫、不越快照漂移
    # （force+观察 draft+窗内 publish 同样 409，重试 force 即过）。
    with _outbox_delete_guard(flow_id, tenant_id=tenant, version=version):
        _outbox_purge_target(flow_id, tenant_id=tenant, version=version,
                             request=request)
        try:
            _store().delete_version(
                flow_id, version, tenant_id=tenant, expected_status=prior.status,
            )
        except flow_store.VersionConflictError as e:
            raise HTTPException(
                status_code=409,
                detail=error_detail(
                    "version_stale", str(e),
                    conflict="version_stale",
                    latest_updated_at=e.latest_updated_at,
                ),
            )
        except LookupError as e:
            raise HTTPException(
                status_code=404, detail=error_detail("version_not_found", str(e))
            )
    # A2R52-01：同 delete_flow——force 放行的钉版调度删除落地后级联暂停。
    paused_schedules = (
        _pause_pinned_schedules(redis, pinned_schedules)
        if force and pinned_schedules else []
    )
    if redis is not None:
        remove_flow_version_from_engine(redis, flow_id, version, tenant_id=tenant)
    if request is not None:
        _audit(request, "flow.delete_version", f"{flow_id}@{version}",
               {"version": version,
                "force": force,
                "prior_status": prior.status,
                **({"pinned_schedules_paused": paused_schedules}
                   if paused_schedules else {})},
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
    # 卡 A-1：编码闸 + plaita#300 能力门同在状态翻转之前（422 结构化拒绝，
    # 版本保持 draft、引擎存储零写入、outbox 零行）。
    _publish_gates(redis, row, where="publish")
    try:
        # A2R54-01/02：发布事务内对 fresh 行 re-gate + 条件翻转（并发保存落进
        # 竞窗 → 409，不再静默发布未过门定义/打回已发布行）。
        out = store.publish_version(
            flow_id, req.version, tenant_id=tenant,
            gate_fn=lambda fresh: _publish_gates(redis, fresh, where="publish"),
        )
    except LookupError as e:
        raise HTTPException(
            status_code=404, detail=error_detail("version_not_found", str(e))
        )
    except flow_store.VersionConflictError as e:
        raise HTTPException(
            status_code=409,
            detail=error_detail(
                "version_stale", str(e),
                conflict="version_stale",
                latest_updated_at=e.latest_updated_at,
            ),
        )
    # 发布即同步到引擎运行时存储：FlowWorker 从这里解析定义，
    # 不同步则「发布成功、启动必失败」。本地单机模式无 Redis：
    # 执行走 console 进程内（SQLite 定义），无需同步。
    schedules_restamped = 0
    if redis is not None:
        sync_flow_to_engine(redis, flow_id, out.version, out.definition, tenant_id=tenant)
        # A2R50-03：publish 是 runtime 变更唯一入口——随发布重盖同 flow 调度
        # 行的陈旧章（此前只在 create/update/enable 盖，version=None 调度在
        # 换 runtime 发布后 cron 按旧章错池、与 manual 按版本行现值分裂）。
        schedules_restamped = _restamp_schedules_for_publish(
            redis, tenant, flow_id, out.version, getattr(out, "runtime", None)
        )
    if request is not None:
        try:
            from ..config import get_settings
        except ImportError:
            from config import get_settings
        env = get_settings().console_env
        _audit(request, "flow.publish", f"{flow_id}@{out.version}",
               {"env": env, "schedules_restamped": schedules_restamped},
               tenant_id=tenant)
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


@contextmanager
def _outbox_delete_guard(flow_id: str, *, tenant_id: str,
                         version: Optional[str] = None):
    """#303：删除流程/版本的 outbox 串行化守卫（``engine_outbox.deleting_guard``）。

    进程内互斥锁内先标记行 ``deleting``（tombstone，投递/对账扫描与逐行
    动作一律跳过）→ 调用方清目标三件 + 删除事务（purge 行）；删除事务
    失败按快照恢复行原状（#301「先清后删」自愈方向保留）。删除窗口内
    投递/对账不可能写出 key——口径一：删除完成后目标侧不复活。outbox
    未启用（同 Redis 部署）时透传（零负担）。
    """
    try:
        from ..services import engine_outbox as _engine_outbox
    except ImportError:
        from services import engine_outbox as _engine_outbox  # type: ignore
    with _engine_outbox.deleting_guard(_outbox_sessions(), tenant_id=tenant_id,
                                       flow_id=flow_id, version=version):
        yield


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
    schedules_restamped = 0
    if req.publish:
        # A2R25-03：发布前再校验一次存储定义——导入链路不因 publish=true
        # 绕过正常保存/发布已有的 Flow schema 闸。
        stored = _store().get_version(flow_id, version, tenant_id=tenant)
        if stored is None:
            raise HTTPException(
                status_code=404, detail=f"版本不存在: {flow_id}@{version}"
            )
        _validate_definition(stored.definition)
        # plaita#300 能力门 + 卡 A-1 编码闸：导入直发的 publish 与普通发布
        # 同一组前置闸（结构化 422，版本保持 draft）。
        _publish_gates(redis, stored, where="import.publish")
        try:
            # A2R54-01/02：同普通发布——事务内 re-gate + 条件翻转（竞窗 409）。
            out = _store().publish_version(
                flow_id, version, tenant_id=tenant,
                gate_fn=lambda fresh: _publish_gates(redis, fresh, where="import.publish"),
            )
        except LookupError as e:
            raise HTTPException(
                status_code=404, detail=error_detail("version_not_found", str(e))
            )
        except flow_store.VersionConflictError as e:
            raise HTTPException(
                status_code=409,
                detail=error_detail(
                    "version_stale", str(e),
                    conflict="version_stale",
                    latest_updated_at=e.latest_updated_at,
                ),
            )
        sync_flow_to_engine(redis, flow_id, out.version, out.definition, tenant_id=tenant)
        # A2R57-01：import publish=true 与普通 publish 同为 runtime 变更入口
        #（晋升包 runtime 随包落版本行）——发布后同调重章，否则 version=None
        # 调度行留旧章，cron 按旧章错池、与 manual 按版本行现值分裂（A2R50-03
        # 原害从 import 门原样重开，且无 WARNING/审计痕迹）。
        schedules_restamped = _restamp_schedules_for_publish(
            redis, tenant, flow_id, out.version, getattr(out, "runtime", None)
        )
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
                # A2R57-01：与普通 publish 审计同字段，import 直发的重章留痕
                "schedules_restamped": schedules_restamped,
            },
            tenant_id=tenant,
        )
    return result
