"""
调度（触发器）管理 API

集群档：调度定义存储在 Redis（与引擎 schedule_service 共享视图）：
- plaita:schedules            hash: schedule_id -> 定义 JSON
- plaita:schedule:fires:{id}  list: 最近触发记录（由 schedule_service 写入）

本地单机档：调度定义存 SQLite（local_schedules），由 console 内置调度循环
触发（services/local_scheduler.py），触发直接进程内执行、不入队。

两档对外结构与语义一致，前端无感。
端点（管理面）：
- GET    /api/schedules                       全部调度（含下次触发时间）
- POST   /api/schedules                       新建（校验 cron / 流程存在）
- GET    /api/schedules/preview               cron 预览（纯计算，无需 Redis）
- GET    /api/schedules/{schedule_id}         调度详情
- PUT    /api/schedules/{schedule_id}         更新（重算 next_run_at）
- DELETE /api/schedules/{schedule_id}         删除（含触发历史）
- POST   /api/schedules/{schedule_id}/enable  启用（立即重算 next_run_at）
- POST   /api/schedules/{schedule_id}/disable 暂停
- POST   /api/schedules/{schedule_id}/trigger 立即触发一次
- GET    /api/schedules/{schedule_id}/history 最近触发记录
"""
import json
from datetime import datetime
from typing import List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

try:
    from ..request_models import EmptyRequestModel, StrictRequestModel
except ImportError:  # 顶层 ``api`` 包（测试/脚本模式）
    from request_models import EmptyRequestModel, StrictRequestModel  # type: ignore
from redis import Redis

try:
    from plaita.server.services.schedule_service import (
        SCHEDULE_FIRES_KEY,
        SCHEDULES_KEY,
        ScheduleConflictError,
        cas_update_schedule,
        fire_schedule,
        list_schedules,
        next_run_after,
        validate_cron,
    )
    from ..services import flow_store
except ImportError:  # 平铺布局（cwd=backend）运行时
    from services import flow_store  # type: ignore
    try:
        from plaita.server.services.schedule_service import (
            SCHEDULE_FIRES_KEY,
            SCHEDULES_KEY,
            ScheduleConflictError,
            cas_update_schedule,
            fire_schedule,
            list_schedules,
            next_run_after,
            validate_cron,
        )
    except ImportError:
        SCHEDULES_KEY = "plaita:schedules"
        SCHEDULE_FIRES_KEY = "plaita:schedule:fires:{schedule_id}"

# get_redis / 队列名与 executions 同源：队列名用 _task_queue_name() 在**使用点**
# 解析（env PLAITA_CONSOLE_TASK_QUEUE > 默认值），不得导入模块级静态别名——
# 静态值在导入时求值，env 切换/新旧并行验证时触发消息会落错队列（A56-02）
from .executions import get_redis_or_none, _task_queue_name

try:
    from ..auth import tenant_scope
except ImportError:  # 平铺布局（cwd=backend）运行时
    from auth import tenant_scope  # type: ignore

try:  # A2R24-01：统一错误 detail 形状
    from ..error_contract import error_detail
except ImportError:
    from error_contract import error_detail  # type: ignore

router = APIRouter()

#: A2R24-05：触发历史 offset 上限（与 executions page≤10000 同量级）。
#: 历史记录本身由 schedule_service 的 ``LTRIM`` 限制在最近 N 条，这里只防
#: 调用方把 OFFSET 推到无界值。
_FIRE_HISTORY_MAX_OFFSET = 10000


# ============ 请求/响应模型 ============

class ScheduleCreateRequest(StrictRequestModel):
    name: str = Field(..., description="调度名称")
    flow_id: str = Field(..., description="目标流程 ID")
    version: Optional[str] = Field(None, description="流程版本；空 = 最新已发布")
    cron: str = Field(..., description="cron 表达式（minute hour day month weekday，本地时区）")
    params: dict = Field(default_factory=dict, description="启动入参")
    enabled: bool = Field(True, description="是否启用")
    created_by: str = Field("", description="创建人")


class ScheduleUpdateRequest(StrictRequestModel):
    name: Optional[str] = None
    version: Optional[str] = None
    cron: Optional[str] = None
    params: Optional[dict] = None
    enabled: Optional[bool] = None
    # plaita#198 乐观锁：调用方读取时的 updated_at；不一致返回 409
    # （conflict=schedule_stale，带 latest_updated_at）。
    expected_updated_at: Optional[str] = None


class CronPreviewRequest(StrictRequestModel):
    cron: str
    count: int = Field(5, ge=1, le=20)


class TriggerScheduleResponse(BaseModel):
    """A2R24-08：本地/集群两模式统一的触发响应。

    历史：本地档只返回 ``execution_id``、集群档只返回 ``msg_id``，前端 TS 只
    声明了 ``msg_id``（本地档读成 undefined）。现在两模式都返回同一字段集，
    用 ``mode`` 说明标识语义，缺省侧为 ``null``（不是字段消失）。
    """

    success: bool
    mode: Literal["local", "cluster"]
    execution_id: Optional[str] = None
    msg_id: Optional[str] = None


class ScheduleFireRecord(BaseModel):
    """触发历史记录（A2R24 契约族余项）。

    集群档历史上写 ``{fired_at, trigger_kind, flow_id, version, enqueue, msg_id}``
    （``schedule_service.fire_schedule``），本地档写 ``{triggered_at, execution_id,
    status, error}``；前端 ``ScheduleFireRecord`` 只声明了前者，本地档的
    ``fired_at``/``enqueue`` 恒为 undefined（时间显示空白、状态恒为「入队失败」）。
    这里把两档收敛到同一字段集：集群键原样保留，本地档补齐映射；本地专有的
    ``execution_id/status/error`` 作为可选诊断字段保留，集群侧为 null。
    """

    fired_at: str = ""
    trigger_kind: str = ""
    flow_id: str = ""
    version: str = ""
    enqueue: str = ""
    msg_id: Optional[str] = None
    execution_id: Optional[str] = None
    status: Optional[str] = None
    error: Optional[str] = None
    # 触发拒绝原因（B44-01）：enqueue="rejected" 时非空
    # （tenant_disabled / tenant_quota_exhausted）——「到期被拒」与「未到期」
    # 在调度面可区分；其余记录为 None。
    rejection_reason: Optional[str] = None


class ScheduleHistoryResponse(BaseModel):
    """A2R24-05：``total`` 是匹配总数，``returned`` 是本页条数。

    ``truncated`` 表示后面还有记录；``next_offset`` 直接回传给 ``?offset=``
    （集群档 ``LRANGE`` / 本地档 SQL ``OFFSET`` 同语义）。历史实现两档都返回
    ``total=len(records)``（本页条数），limit 以上的记录没有任何可达路径。
    """

    schedule_id: str
    records: List[ScheduleFireRecord]
    total: int
    returned: int
    truncated: bool = False
    next_offset: Optional[int] = None


# ============ 内部工具 ============

def _now_iso() -> str:
    return datetime.now().isoformat()


def _get_local(request: Request):
    """本地单机模式 → local_scheduler 模块；集群模式 → None。"""
    if getattr(request.app.state, "local_mode", False):
        try:
            from ..services import local_scheduler
        except ImportError:
            from services import local_scheduler  # type: ignore
        return local_scheduler
    return None


def _tenant_is_active(request: Request, tenant_id: Optional[str]) -> bool:
    """租户状态闸（#27）：停用租户的调度不允许手动触发。"""
    if (local := _get_local(request)) is not None:
        try:
            from ..services import users_svc
        except ImportError:
            from services import users_svc  # type: ignore
        return users_svc.tenant_is_active(flow_store.get_flow_store(), tenant_id)
    try:
        from plaita.server.tenant_status import tenant_is_active
    except ImportError:
        return True
    return tenant_is_active(getattr(request.app.state, "redis", None), tenant_id)


def _get_schedule(redis: Redis, schedule_id: str, *, tenant_id: Optional[str]) -> dict:
    """按 id 取调度定义；tenant_id 非空时做归属校验（#63）。

    - tenant_id=None（平台视角：api-key / 未切换租户的平台管理员）→ 不过滤；
    - 租户视角：与 list_all 同一判据比对 HASH 内 tenant_id，不匹配按
      404 拒绝（与「不存在」同形，不暴露条目是否存在）。
    每个单条端点的集群分支都必须显式传入 tenant_id。
    """
    raw = redis.hget(SCHEDULES_KEY, schedule_id)
    if raw is None:
        raise HTTPException(status_code=404, detail=f"调度不存在: {schedule_id}")
    if isinstance(raw, bytes):
        raw = raw.decode()
    schedule = json.loads(raw)
    if tenant_id is not None and (schedule.get("tenant_id") or "default") != tenant_id:
        raise HTTPException(status_code=404, detail=f"调度不存在: {schedule_id}")
    return schedule


def _save_schedule(redis: Redis, schedule: dict) -> None:
    redis.hset(SCHEDULES_KEY, key=schedule["schedule_id"], value=json.dumps(schedule, ensure_ascii=False))


def _raise_schedule_conflict(latest_updated_at: Optional[str]) -> None:
    """#198：结构化 409（参考 flows.py C5-1 的 version_stale 形状）。"""
    raise HTTPException(
        status_code=409,
        detail=error_detail(
            "schedule_stale",
            "调度已被其他编辑更新，请加载最新后重试",
            conflict="schedule_stale",
            latest_updated_at=latest_updated_at,
        ),
    )


def _view(schedule: dict) -> dict:
    """API 视图：统一给前端的结构（补计算字段）。"""
    out = dict(schedule)
    out["status"] = "running" if schedule.get("enabled") else "paused"
    return out


def _audit(request: Optional[Request], action: str, resource_id: str,
           detail: Optional[dict] = None, *, tenant_id: Optional[str] = None) -> None:
    """A55-S1：调度写操作审计（显式传生效租户，不依赖 state）。"""
    if request is None:
        return
    try:
        from ..services import audit as audit_svc
    except ImportError:
        from services import audit as audit_svc  # type: ignore
    audit_svc.record(
        request,
        action=action,
        resource="schedule",
        resource_id=resource_id,
        detail=detail,
        tenant_id=tenant_id,
    )


def _schedule_detail(schedule: dict) -> dict:
    return {
        "flow_id": schedule.get("flow_id"),
        "version": schedule.get("version"),
        "cron": schedule.get("cron"),
        "enabled": bool(schedule.get("enabled")),
    }


def _validate_flow(flow_id: str, version: Optional[str], tenant_id: Optional[str] = None) -> None:
    store = flow_store.get_flow_store()
    if store.get_flow_record(flow_id, tenant_id=tenant_id) is None:
        raise HTTPException(status_code=422, detail=f"流程不存在: {flow_id}")
    if version:
        published = {v.version for v in store.list_versions(flow_id, tenant_id=tenant_id)}
        if version not in published:
            raise HTTPException(status_code=422, detail=f"流程 {flow_id} 不存在版本 {version}")


def _schedule_from_req(req: ScheduleCreateRequest, tenant_id: str) -> dict:
    return {
        "schedule_id": f"sched-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}",
        "tenant_id": tenant_id,
        "name": req.name.strip(),
        "flow_id": req.flow_id,
        "version": req.version or None,
        "cron": req.cron,
        "params": req.params or {},
        "enabled": bool(req.enabled),
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "created_by": req.created_by,
        # 服务自愈兜底，但创建时就给明确值，避免首扫竞态
        "next_run_at": str(next_run_after(req.cron)) if req.enabled else "",
    }


# ============ 端点 ============

@router.get("/schedules")
def list_all(request: Request, redis: Redis = Depends(get_redis_or_none)):
    tenant = tenant_scope(request)
    if (local := _get_local(request)) is not None:
        schedules = sorted(
            local.list_schedules(flow_store.get_flow_store(), tenant_id=tenant),
            key=lambda s: s.get("created_at") or "",
        )
    else:
        # 集群档：HASH 值内含 tenant_id（console 写入），按租户上下文过滤
        all_schedules = list_schedules(redis)
        if tenant:
            all_schedules = [
                s for s in all_schedules if (s.get("tenant_id") or "default") == tenant
            ]
        schedules = sorted(all_schedules, key=lambda s: s.get("created_at") or "")
    return {"schedules": [_view(s) for s in schedules], "total": len(schedules)}


@router.post("/schedules")
def create(req: ScheduleCreateRequest, request: Request, redis: Redis = Depends(get_redis_or_none)):
    tenant = tenant_scope(request, required=True)
    if not req.name.strip():
        raise HTTPException(status_code=422, detail="name 不能为空")
    if not validate_cron(req.cron):
        raise HTTPException(status_code=422, detail=f"非法 cron 表达式: {req.cron}")
    _validate_flow(req.flow_id, req.version, tenant_id=tenant)
    schedule = _schedule_from_req(req, tenant)

    if (local := _get_local(request)) is not None:
        schedule = local.create_schedule(flow_store.get_flow_store(), schedule)
    else:
        _save_schedule(redis, schedule)
    _audit(request, "schedule.create", schedule["schedule_id"],
           _schedule_detail(schedule), tenant_id=tenant)
    return _view(schedule)


@router.get("/schedules/preview")
def preview_cron(cron: str, count: int = 5):
    """cron 预览：从现在起未来 count 次触发时间（本地时区 ISO 字符串）。

    纯 croniter 计算，不依赖 Redis —— 本地单机档同样可用。
    """
    if not validate_cron(cron):
        raise HTTPException(status_code=422, detail=f"非法 cron 表达式: {cron}")
    count = max(1, min(20, count))
    base = datetime.now()
    out = []
    cursor = base
    for _ in range(count):
        nxt_ms = next_run_after(cron, cursor)
        cursor = datetime.fromtimestamp(nxt_ms / 1000)
        out.append(cursor.isoformat())
    return {"cron": cron, "next": out}


@router.get("/schedules/{schedule_id}")
def get_one(schedule_id: str, request: Request, redis: Redis = Depends(get_redis_or_none)):
    if (local := _get_local(request)) is not None:
        schedule = local.get_schedule(flow_store.get_flow_store(), schedule_id,
                                      tenant_id=tenant_scope(request))
        if schedule is None:
            raise HTTPException(status_code=404, detail=f"调度不存在: {schedule_id}")
        return _view(schedule)
    return _view(_get_schedule(redis, schedule_id, tenant_id=tenant_scope(request)))


@router.put("/schedules/{schedule_id}")
def update(schedule_id: str, req: ScheduleUpdateRequest, request: Request,
           redis: Redis = Depends(get_redis_or_none)):
    # 写路径按 required=True 取租户（A2R17-01 全量视角只读；A1 r110 a/P2）：
    # 非 required 下全量视角解析出 None，get_schedule(tenant_id=None) 不过滤，
    # 平台管理员即可按 id 盲改任意租户的调度。以下 by-id 写端点同理。
    tenant = tenant_scope(request, required=True)
    local = _get_local(request)
    if local is not None:
        store = flow_store.get_flow_store()
        schedule = local.get_schedule(store, schedule_id, tenant_id=tenant)
        if schedule is None:
            raise HTTPException(status_code=404, detail=f"调度不存在: {schedule_id}")
    else:
        schedule = _get_schedule(redis, schedule_id, tenant_id=tenant)

    # 输入校验先行（CAS 的 mutate 只做纯字段赋值）
    if req.name is not None and not req.name.strip():
        raise HTTPException(status_code=422, detail="name 不能为空")
    if req.cron is not None and not validate_cron(req.cron):
        raise HTTPException(status_code=422, detail=f"非法 cron 表达式: {req.cron}")

    # #198：乐观锁基准 = 本次读取的 updated_at。请求显式带 expected_updated_at
    # 时先做一次 409 早判；真正的写回由下面 CAS/WATCH 再做一次（防读后写前
    # 被并发编辑抢先）。
    base_updated_at = schedule.get("updated_at")
    if (req.expected_updated_at is not None
            and base_updated_at != req.expected_updated_at):
        _raise_schedule_conflict(base_updated_at)

    def _apply(current: dict) -> None:
        if req.name is not None:
            current["name"] = req.name.strip()
        if req.version is not None:
            current["version"] = req.version or None
        if req.cron is not None:
            current["cron"] = req.cron
        if req.params is not None:
            current["params"] = req.params
        if req.enabled is not None:
            current["enabled"] = bool(req.enabled)
        # cron/enabled 变化后重算下次触发；编辑即重置节奏。
        # 用 current 的 cron（并发编辑以在库值为准，不回退旧快照）。
        if req.cron is not None or req.enabled is not None:
            cron = current.get("cron") or schedule.get("cron")
            current["next_run_at"] = (
                str(next_run_after(cron)) if current.get("enabled") else ""
            )
        current["updated_at"] = _now_iso()

    if local is not None:
        store = flow_store.get_flow_store()
        # 本地档无跨进程 CAS；读后写前复核 updated_at，冲突同样 409。
        fresh = local.get_schedule(store, schedule_id, tenant_id=tenant)
        if fresh is None:
            raise HTTPException(status_code=404, detail=f"调度不存在: {schedule_id}")
        if fresh.get("updated_at") != base_updated_at:
            _raise_schedule_conflict(fresh.get("updated_at"))
        _apply(fresh)
        try:
            result = local.update_schedule(store, schedule_id, fresh, tenant_id=tenant)
        except LookupError:
            raise HTTPException(status_code=404, detail=f"调度不存在: {schedule_id}")
        _audit(request, "schedule.update", schedule_id,
               _schedule_detail(result), tenant_id=tenant)
        return _view(result)

    try:
        updated = cas_update_schedule(
            redis, schedule_id, _apply,
            expected_updated_at=base_updated_at, check_updated_at=True,
        )
    except ScheduleConflictError as exc:
        _raise_schedule_conflict(exc.latest_updated_at)
    if updated is None:
        raise HTTPException(status_code=404, detail=f"调度不存在: {schedule_id}")
    _audit(request, "schedule.update", schedule_id,
           _schedule_detail(updated), tenant_id=tenant)
    return _view(updated)


@router.delete("/schedules/{schedule_id}")
def remove(schedule_id: str, request: Request,
           redis: Redis = Depends(get_redis_or_none),
           body: Optional[EmptyRequestModel] = None):
    tenant = tenant_scope(request, required=True)
    if (local := _get_local(request)) is not None:
        store = flow_store.get_flow_store()
        schedule = local.get_schedule(store, schedule_id, tenant_id=tenant)
        if schedule is None or not local.delete_schedule(
                store, schedule_id, tenant_id=tenant):
            raise HTTPException(status_code=404, detail=f"调度不存在: {schedule_id}")
        _audit(request, "schedule.delete", schedule_id,
               _schedule_detail(schedule), tenant_id=tenant or "")
        return {"success": True, "schedule_id": schedule_id}
    schedule = _get_schedule(redis, schedule_id, tenant_id=tenant)
    redis.hdel(SCHEDULES_KEY, schedule_id)
    redis.delete(SCHEDULE_FIRES_KEY.format(schedule_id=schedule_id))
    _audit(request, "schedule.delete", schedule_id,
           _schedule_detail(schedule), tenant_id=tenant or "")
    return {"success": True, "schedule_id": schedule_id}


@router.post("/schedules/{schedule_id}/enable")
def enable(schedule_id: str, request: Request,
           redis: Redis = Depends(get_redis_or_none),
           body: Optional[EmptyRequestModel] = None):
    tenant = tenant_scope(request, required=True)
    if (local := _get_local(request)) is not None:
        store = flow_store.get_flow_store()
        schedule = local.get_schedule(store, schedule_id, tenant_id=tenant)
        if schedule is None:
            raise HTTPException(status_code=404, detail=f"调度不存在: {schedule_id}")
        schedule["enabled"] = True
        schedule["next_run_at"] = str(next_run_after(schedule["cron"]))
        result = local.update_schedule(store, schedule_id, schedule, tenant_id=tenant)
        _audit(request, "schedule.enable", schedule_id,
               _schedule_detail(result), tenant_id=tenant or "")
        return _view(result)
    schedule = _get_schedule(redis, schedule_id, tenant_id=tenant)
    schedule["enabled"] = True
    schedule["next_run_at"] = str(next_run_after(schedule["cron"]))
    schedule["updated_at"] = _now_iso()
    _save_schedule(redis, schedule)
    _audit(request, "schedule.enable", schedule_id,
           _schedule_detail(schedule), tenant_id=tenant or "")
    return _view(schedule)


@router.post("/schedules/{schedule_id}/disable")
def disable(schedule_id: str, request: Request,
            redis: Redis = Depends(get_redis_or_none),
            body: Optional[EmptyRequestModel] = None):
    tenant = tenant_scope(request, required=True)
    if (local := _get_local(request)) is not None:
        store = flow_store.get_flow_store()
        schedule = local.get_schedule(store, schedule_id, tenant_id=tenant)
        if schedule is None:
            raise HTTPException(status_code=404, detail=f"调度不存在: {schedule_id}")
        schedule["enabled"] = False
        schedule["next_run_at"] = ""
        result = local.update_schedule(store, schedule_id, schedule, tenant_id=tenant)
        _audit(request, "schedule.disable", schedule_id,
               _schedule_detail(result), tenant_id=tenant or "")
        return _view(result)
    schedule = _get_schedule(redis, schedule_id, tenant_id=tenant)
    schedule["enabled"] = False
    schedule["updated_at"] = _now_iso()
    _save_schedule(redis, schedule)
    _audit(request, "schedule.disable", schedule_id,
           _schedule_detail(schedule), tenant_id=tenant or "")
    return _view(schedule)


@router.post(
    "/schedules/{schedule_id}/trigger", response_model=TriggerScheduleResponse
)
def trigger_now(schedule_id: str, request: Request,
                redis: Redis = Depends(get_redis_or_none),
                body: Optional[EmptyRequestModel] = None):
    """立即触发一次：本地档直接进程内执行；集群档写任务队列 Stream。

    A2R24-08：两模式返回同一字段集（``mode`` 区分标识语义）。
    """
    tenant = tenant_scope(request, required=True)
    if (local := _get_local(request)) is not None:
        store = flow_store.get_flow_store()
        schedule = local.get_schedule(store, schedule_id, tenant_id=tenant)
        if schedule is None:
            raise HTTPException(status_code=404, detail=f"调度不存在: {schedule_id}")
        if not _tenant_is_active(request, schedule.get("tenant_id")):
            raise HTTPException(status_code=403, detail="租户已停用，拒绝触发调度")
        execution_id = local.trigger_now(store, schedule)
        if execution_id is None:
            raise HTTPException(status_code=502, detail="触发失败，请查看 console 日志")
        detail = _schedule_detail(schedule)
        detail["execution_id"] = execution_id
        _audit(request, "schedule.trigger", schedule_id, detail,
               tenant_id=schedule.get("tenant_id") or tenant or "")
        return TriggerScheduleResponse(
            success=True, mode="local", execution_id=execution_id, msg_id=None,
        )

    schedule = _get_schedule(redis, schedule_id, tenant_id=tenant)
    if not _tenant_is_active(request, schedule.get("tenant_id")):
        raise HTTPException(status_code=403, detail="租户已停用，拒绝触发调度")
    # 与手动「启动流程」同源：写任务队列 Stream（队列名在调用点解析 env）
    msg_id = fire_schedule(redis, schedule, _task_queue_name(), trigger_kind="manual")
    if msg_id is None:
        raise HTTPException(status_code=502, detail="入队失败，请检查 Redis 与任务队列配置")
    detail = _schedule_detail(schedule)
    detail["queue_msg_id"] = msg_id
    _audit(request, "schedule.trigger", schedule_id, detail,
           tenant_id=schedule.get("tenant_id") or tenant or "")
    return TriggerScheduleResponse(
        success=True, mode="cluster", execution_id=None, msg_id=msg_id,
    )


@router.get(
    "/schedules/{schedule_id}/history", response_model=ScheduleHistoryResponse
)
def history(
    schedule_id: str,
    request: Request,
    limit: int = Query(default=10, ge=1, le=50),
    offset: int = Query(default=0, ge=0, le=_FIRE_HISTORY_MAX_OFFSET),
    redis: Redis = Depends(get_redis_or_none),
):
    """最近触发记录（A2R24-05：``total``=匹配总数 + ``offset`` 可达全部历史）。

    - **limit**: 本页条数（1..50；超出 422，不再静默夹紧）
    - **offset**: 跳过条数（0..10000；``next_offset`` 直接回传）
    """
    if (local := _get_local(request)) is not None:
        # A48-03：与集群分支同口径先做归属校验（#63/#192）。fire_history 只按
        # schedule_id 取数，不先确认调度属于当前租户就会跨租户泄漏执行 ID/错误。
        store = flow_store.get_flow_store()
        schedule = local.get_schedule(store, schedule_id,
                                      tenant_id=tenant_scope(request))
        if schedule is None:
            raise HTTPException(status_code=404, detail=f"调度不存在: {schedule_id}")
        total = local.fire_history_total(store, schedule_id)
        records = local.fire_history(
            store, schedule_id, limit, offset=offset,
            flow_id=schedule.get("flow_id") or "",
            version=schedule.get("version") or "",
        )
    else:
        _get_schedule(redis, schedule_id, tenant_id=tenant_scope(request))
        fires_key = SCHEDULE_FIRES_KEY.format(schedule_id=schedule_id)
        total = int(redis.llen(fires_key) or 0)
        raw = redis.lrange(fires_key, offset, offset + limit - 1)
        records = []
        for item in raw:
            if isinstance(item, bytes):
                item = item.decode()
            try:
                records.append(json.loads(item))
            except json.JSONDecodeError:
                continue

    truncated = offset + len(records) < total
    return ScheduleHistoryResponse(
        schedule_id=schedule_id,
        records=records,
        total=total,
        returned=len(records),
        truncated=truncated,
        next_offset=(offset + limit) if truncated else None,
    )
