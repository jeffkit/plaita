"""
流程定义 CRUD + 版本管理 API

- GET    /api/flows                            流程列表
- POST   /api/flows                            新建流程（仅元信息）
- GET    /api/flows/{flow_id}                  流程详情 + 版本列表
- DELETE /api/flows/{flow_id}                  删除流程及其全部版本
- GET    /api/flows/{flow_id}/versions/{ver}   取某版本定义（含 layout）
- PUT    /api/flows/{flow_id}/versions/{ver}   保存草稿（Flow.model_validate 强校验）
- DELETE /api/flows/{flow_id}/versions/{ver}   删除版本
- POST   /api/flows/{flow_id}/publish          发布版本（draft → published）
"""
import json
import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from redis import Redis
from pydantic import BaseModel, Field

from plaita.core.flow import Flow

try:
    from ..services import flow_store
    from ..services.engine_sync import (
        remove_flow_from_engine,
        remove_flow_version_from_engine,
        sync_flow_to_engine,
    )
except ImportError:
    from services import flow_store
    from services.engine_sync import (
        remove_flow_from_engine,
        remove_flow_version_from_engine,
        sync_flow_to_engine,
    )

router = APIRouter()

_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(?:-[\w.]+)?(?:\+[\w.]+)?$")


# ============ 请求/响应模型 ============

class FlowSummaryView(BaseModel):
    flow_id: str
    author: str = ""
    desc: str = ""
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class FlowListResponse(BaseModel):
    flows: List[FlowSummaryView]
    total: int


class CreateFlowRequest(BaseModel):
    flow_id: str = Field(..., description="流程 ID")
    author: str = Field("", description="作者")
    desc: str = Field("", description="描述")


class FlowDetailResponse(BaseModel):
    flow_id: str
    author: str = ""
    desc: str = ""
    versions: List[dict] = Field(default_factory=list)


class VersionView(BaseModel):
    flow_id: str
    version: str
    status: str
    definition: str = ""
    layout: str = ""
    created_at: Optional[str] = None
    published_at: Optional[str] = None
    created_by: str = ""


class SaveVersionRequest(BaseModel):
    definition: str = Field(..., description="Flow 定义 JSON 字符串")
    layout: str = Field("{}", description="画布坐标 JSON 字符串")
    created_by: str = Field("", description="保存人")


class PublishRequest(BaseModel):
    version: str = Field(..., description="要发布的版本号")


def _store() -> flow_store.FlowStore:
    return flow_store.get_flow_store()


def get_redis_dep(request: Request) -> Redis:
    """引擎同步用 Redis 客户端。本地单机模式返回 None（发布/删除跳过引擎同步）。"""
    return request.app.state.redis


def _check_semver(version: str) -> None:
    if not _SEMVER_RE.match(version or ""):
        raise HTTPException(status_code=422, detail=f"非法 semver 版本号: {version}")


def _validate_definition(definition: str) -> None:
    """用 Flow.model_validate 强校验 definition，失败抛 422。"""
    try:
        data = json.loads(definition)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=422, detail=f"definition 非合法 JSON: {e}")
    try:
        Flow.model_validate(data)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Flow 校验失败: {e}")


# ============ 端点 ============

@router.get("/flows", response_model=FlowListResponse)
def list_flows():
    flows = _store().list_flows()
    views = [
        FlowSummaryView(
            flow_id=f.flow_id,
            author=f.author,
            desc=f.desc,
            created_at=f.created_at.isoformat() if f.created_at else None,
            updated_at=f.updated_at.isoformat() if f.updated_at else None,
        )
        for f in flows
    ]
    return FlowListResponse(flows=views, total=len(views))


class AiGenerateRequest(BaseModel):
    prompt: str = Field(..., description="自然语言需求描述")
    agent: str = Field("glm-52", description="编码 Agent profile（agentproc/agents.json）")


@router.post("/flows/ai-generate/stream")
def ai_generate_stream(req: AiGenerateRequest):
    """AI 生成流程（SSE）：后端作为 agent 宿主跑真实编码 Agent，事件流给前端。

    设计（ADR-2026-08-27 phase2）：后端不直连 LLM 端点；Agent 循环由 agentproc
    执行的编码 Agent 负责，编译校验为宿主的确定性检查，错误回喂下一轮。
    """
    if not req.prompt.strip():
        raise HTTPException(status_code=422, detail="prompt 不能为空")
    from fastapi.responses import StreamingResponse
    try:
        from ..services import ai_flow
    except ImportError:  # 平铺布局（cwd=backend）运行时
        from services import ai_flow

    def event_stream():
        for ev in ai_flow.generate_flow_events(req.prompt.strip()):
            yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


@router.post("/flows", response_model=FlowSummaryView)
def create_flow(req: CreateFlowRequest):
    if not req.flow_id:
        raise HTTPException(status_code=422, detail="flow_id 不能为空")
    try:
        rec = _store().create_flow(req.flow_id, author=req.author, desc=req.desc)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return FlowSummaryView(
        flow_id=rec.flow_id,
        author=rec.author,
        desc=rec.desc,
        created_at=rec.created_at.isoformat() if rec.created_at else None,
        updated_at=rec.updated_at.isoformat() if rec.updated_at else None,
    )


@router.get("/flows/{flow_id}", response_model=FlowDetailResponse)
def get_flow(flow_id: str):
    store = _store()
    rec = store.get_flow_record(flow_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"流程不存在: {flow_id}")
    versions = [
        {
            "version": v.version,
            "status": v.status,
            "created_at": v.created_at.isoformat() if v.created_at else None,
            "published_at": v.published_at.isoformat() if v.published_at else None,
        }
        for v in store.list_versions(flow_id)
    ]
    return FlowDetailResponse(
        flow_id=rec.flow_id, author=rec.author, desc=rec.desc, versions=versions
    )


@router.delete("/flows/{flow_id}")
def delete_flow(flow_id: str, request: Request = None, redis: Redis = Depends(get_redis_dep)):
    try:
        _store().delete_flow(flow_id)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    # 引擎运行时存储同步清理（本地模式无 Redis，无需清理）
    if redis is not None:
        remove_flow_from_engine(redis, flow_id)
    if request is not None:
        _audit(request, "flow.delete", flow_id)
    return {"success": True, "flow_id": flow_id}


@router.get("/flows/{flow_id}/versions/{version}", response_model=VersionView)
def get_version(flow_id: str, version: str):
    out = _store().get_version(flow_id, version)
    if out is None:
        raise HTTPException(status_code=404, detail=f"版本不存在: {flow_id}@{version}")
    return VersionView(
        flow_id=out.flow_id,
        version=out.version,
        status=out.status,
        definition=out.definition,
        layout=out.layout,
        created_at=out.created_at.isoformat() if out.created_at else None,
        published_at=out.published_at.isoformat() if out.published_at else None,
        created_by=out.created_by,
    )


@router.put("/flows/{flow_id}/versions/{version}", response_model=VersionView)
def save_version(flow_id: str, version: str, req: SaveVersionRequest,
                 request: Request = None):  # noqa: ANN001 — FastAPI 注入
    _check_semver(version)
    _validate_definition(req.definition)
    store = _store()
    # 确保 flow 记录存在
    if store.get_flow_record(flow_id) is None:
        raise HTTPException(status_code=404, detail=f"流程不存在: {flow_id}")
    try:
        store.save_flow_definition(
            flow_id=flow_id,
            version=version,
            definition=req.definition,
            layout=req.layout,
            status="draft",
            created_by=req.created_by,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    out = store.get_version(flow_id, version)
    if request is not None:
        _audit(request, "flow.save_version", f"{flow_id}@{version}", {"bytes": len(req.definition)})
    return VersionView(
        flow_id=out.flow_id,
        version=out.version,
        status=out.status,
        definition=out.definition,
        layout=out.layout,
        created_at=out.created_at.isoformat() if out.created_at else None,
        published_at=out.published_at.isoformat() if out.published_at else None,
        created_by=out.created_by,
    )


@router.delete("/flows/{flow_id}/versions/{version}")
def delete_version(flow_id: str, version: str, request: Request = None,
                   redis: Redis = Depends(get_redis_dep)):
    try:
        _store().delete_version(flow_id, version)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    if redis is not None:
        remove_flow_version_from_engine(redis, flow_id, version)
    return {"success": True, "flow_id": flow_id, "version": version}


@router.post("/flows/{flow_id}/publish", response_model=VersionView)
def publish_flow(flow_id: str, req: PublishRequest, request: Request = None,
                 redis: Redis = Depends(get_redis_dep)):
    _check_semver(req.version)
    store = _store()
    if store.get_flow_record(flow_id) is None:
        raise HTTPException(status_code=404, detail=f"流程不存在: {flow_id}")
    try:
        out = store.publish_version(flow_id, req.version)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    # 发布即同步到引擎运行时存储：FlowWorker 从这里解析定义，
    # 不同步则「发布成功、启动必失败」。本地单机模式无 Redis：
    # 执行走 console 进程内（SQLite 定义），无需同步。
    if redis is not None:
        sync_flow_to_engine(redis, flow_id, out.version, out.definition)
    if request is not None:
        try:
            from ..config import get_settings
        except ImportError:
            from config import get_settings
        env = get_settings().console_env
        _audit(request, "flow.publish", f"{flow_id}@{out.version}", {"env": env})
        # 部署记录（切片 C）
        try:
            from ..services import deployments as deployments_svc
        except ImportError:
            from services import deployments as deployments_svc  # type: ignore
        deployments_svc.record(
            flow_id=flow_id, version=out.version, environment=env,
            actor=getattr(request.state, "actor", ""),
            definition=out.definition,
        )
    return VersionView(
        flow_id=out.flow_id,
        version=out.version,
        status=out.status,
        definition=out.definition,
        layout=out.layout,
        created_at=out.created_at.isoformat() if out.created_at else None,
        published_at=out.published_at.isoformat() if out.published_at else None,
        created_by=out.created_by,
    )


def _audit(request: Request, action: str, resource_id: str, detail: Dict | None = None) -> None:
    try:
        from ..services import audit as audit_svc
    except ImportError:
        try:
            from services import audit as audit_svc  # type: ignore
        except ImportError:
            return
    audit_svc.record(request, action=action, resource="flow", resource_id=resource_id, detail=detail)


@router.get("/flows/{flow_id}/versions/{version}/export")
def export_version(flow_id: str, version: str):
    """导出晋升包（定义 + 指纹 + 元信息），跨环境 console 晋升的载体。"""
    try:
        from ..services import deployments as deployments_svc
    except ImportError:
        from services import deployments as deployments_svc  # type: ignore
    try:
        return deployments_svc.build_promotion_package(_store(), flow_id, version)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))


class ImportPromotionRequest(BaseModel):
    package: Dict[str, Any]
    new_version: Optional[str] = None
    publish: bool = False


@router.post("/flows/import-version")
def import_version(req: ImportPromotionRequest, request: Request = None,
                   redis: Redis = Depends(get_redis_dep)):
    try:
        from ..services import deployments as deployments_svc
    except ImportError:
        from services import deployments as deployments_svc  # type: ignore
    """导入晋升包：默认为草稿；publish=true 等价于发布（引擎同步 + 部署记录）。"""
    try:
        result = deployments_svc.import_promotion_package(
            _store(), req.package, new_version=req.new_version, publish=False
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    flow_id, version = result["flow_id"], result["version"]
    if req.publish:
        try:
            out = _store().publish_version(flow_id, version)
        except LookupError as e:
            raise HTTPException(status_code=404, detail=str(e))
        sync_flow_to_engine(redis, flow_id, out.version, out.definition)
        try:
            from ..config import get_settings
        except ImportError:
            from config import get_settings
        deployments_svc.record(
            flow_id=flow_id, version=out.version,
            environment=get_settings().console_env,
            actor=getattr(request.state, "actor", "") if request else "",
            definition=out.definition,
        )
        result["status"] = "published"

    if request is not None:
        _audit(request, "flow.import", f"{flow_id}@{version}", {"publish": req.publish})
    return result
