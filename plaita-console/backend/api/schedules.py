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
import logging
import uuid
from datetime import datetime
from typing import List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError

try:
    from ..request_models import EmptyRequestModel, StrictRequestModel
except ImportError:  # 顶层 ``api`` 包（测试/脚本模式）
    from request_models import EmptyRequestModel, StrictRequestModel  # type: ignore
from redis import Redis

try:
    from plaita.server.services.schedule_service import (
        REJECTION_QUOTA_EXHAUSTED,
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
            REJECTION_QUOTA_EXHAUSTED,
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
        REJECTION_QUOTA_EXHAUSTED = "tenant_quota_exhausted"

# get_redis / 队列名与 executions 同源：队列名用 _task_queue_name() 在**使用点**
# 解析（env PLAITA_CONSOLE_TASK_QUEUE > 默认值），不得导入模块级静态别名——
# 静态值在导入时求值，env 切换/新旧并行验证时触发消息会落错队列（A56-02）
# plaita#300：调度触发面与手动 start 同源——受理闸（能力门）与版本行解析
# 共用 executions 的单一实现；池队列按版本行 runtime 解析。
from .executions import (
    _admission_version_row,
    _resolve_start_version,
    _task_queue_name,
    get_redis_or_none,
    StartVersionNotStartable,
)

try:
    from ..services.runtime_pools import (
        DEFAULT_RUNTIME,
        normalize_runtime,
        resolve_runtime_queue,
    )
except ImportError:  # 平铺布局（cwd=backend）运行时
    from services.runtime_pools import (  # type: ignore
        DEFAULT_RUNTIME,
        normalize_runtime,
        resolve_runtime_queue,
    )

try:
    from ..auth import tenant_scope
except ImportError:  # 平铺布局（cwd=backend）运行时
    from auth import tenant_scope  # type: ignore

try:  # A2R24-01：统一错误 detail 形状
    from ..error_contract import error_detail
except ImportError:
    from error_contract import error_detail  # type: ignore

try:  # A2R52-04：manual 触发配额满 429 与 POST /executions 同契约（#28 同源）
    from plaita.server.tenant_quota import tenant_max_running
except ImportError:
    tenant_max_running = None  # type: ignore[assignment]

router = APIRouter()
logger = logging.getLogger(__name__)

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


class ImportLocalSchedulesRequest(StrictRequestModel):
    """「导入本地定时」入参（预留扩展位；当前无必填项）。"""

    enabled_only: bool = Field(True, description="只迁移启用的本地条目（默认；停用条目视为已处理/手动搁置）")


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


def _raise_if_pin_missing(schedule: dict) -> None:
    """A2R57-02：级联暂停标注行禁原样复启（幽灵 cron 复活门）。

    force 删版的级联暂停在行上留 ``pinned_version_missing=true``
    （A2R52-01）：钉版已删，复启即武装幽灵 cron——每 tick 产出
    ``flow_definition_missing`` 垃圾执行（租户 start 配额空转 + 执行列表
    噪音，「幽灵触发面随删除同步止住」被一键绕过）。复启门（POST /enable
    与 PUT {enabled:true}）consult 行上现成标注 → 409 具名拒，零新查询面；
    修复路径 = 重钉有效版本（可随复启同请求提交，update 的 422 版本门先验、
    标注随重钉回清）或先解钉（version 空串，随解钉回清标注）再启。标注
    缺失即健康行，直过。本地档行无该标注（级联暂停时不落自由列），门为
    no-op。
    """
    if not schedule.get("pinned_version_missing"):
        return
    raise HTTPException(
        status_code=409,
        detail=error_detail(
            "schedule_pinned_version_missing",
            "调度钉定的流程版本已删除（级联暂停标注在案），不可直接启用——"
            "请先重钉有效版本（PUT version=<有效版本>）或解钉（version 置空）"
            "后再启用",
            conflict="schedule_pinned_version_missing",
        ),
    )


def _view(schedule: dict) -> dict:
    """API 视图：统一给前端的结构（补计算字段）。"""
    out = dict(schedule)
    out["status"] = "running" if schedule.get("enabled") else "paused"
    # plaita#300：调度行的目标池（旧行无该键 = 历史默认池 python，读面如实）
    out["runtime"] = schedule.get("runtime") or DEFAULT_RUNTIME
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
        # plaita#300：目标池随审计留痕（旧行/本地档为默认 python）
        "runtime": schedule.get("runtime") or DEFAULT_RUNTIME,
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
        # A2R52-02：id 带随机后缀——毫秒时间戳同毫秒两连 create 会生成同 id，
        # 集群档 hset 语义=覆盖即真，第二条把第一条静默抹掉（无 409/无错误面）。
        "schedule_id": (
            f"sched-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}"
            f"-{uuid.uuid4().hex[:6]}"
        ),
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


def _resolve_flow_runtime(flow_id: str, version: Optional[str],
                          tenant_id: Optional[str]) -> str:
    """调度行冗余 runtime 的盖章值（plaita#300 触发面池路由）。

    cron 派发在独立 plaita server 进程里发生，读不到 console 的
    flow_versions.runtime——触发面要与手动 POST /executions 同池就必须把
    runtime 冗余进调度行。解析与 start 同源（``_resolve_start_version``：
    显式 version → 该版本行；``None`` → latest published）。解析不到时落
    默认 ``python``（与历史派发链的池语义一致）：version=None 且尚无已发布
    版本是合法态（静默，等于旧行为）；**钉了 version 却解析不到**不可能是
    正常路径（create/update 已验版本存在），打 WARNING 留痕——降级可观测。
    """
    try:
        row = _resolve_start_version(str(flow_id),
                                     str(version) if version else None,
                                     tenant_id)
    except Exception as exc:  # noqa: BLE001 — 版本库读失败按默认池并留痕
        logger.warning(
            "调度行 runtime 解析失败 flow=%s@%s（落默认 %s）: %s",
            flow_id, version, DEFAULT_RUNTIME, exc,
        )
        return DEFAULT_RUNTIME
    if row is None:
        if version:
            logger.warning(
                "调度行 runtime 解析不到版本行 flow=%s@%s（落默认 %s）",
                flow_id, version, DEFAULT_RUNTIME,
            )
        return DEFAULT_RUNTIME
    try:
        return normalize_runtime(getattr(row, "runtime", None))
    except ValueError:
        logger.warning(
            "调度行 runtime 非法 flow=%s@%s runtime=%r（落默认 %s）",
            flow_id, version, getattr(row, "runtime", None), DEFAULT_RUNTIME,
        )
        return DEFAULT_RUNTIME


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
    # plaita#300 触发面池路由：runtime 冗余进调度行——cron 派发进程读不到
    # console store，跨进程选池只能靠行上盖章（start 同源的版本行解析）。
    schedule["runtime"] = _resolve_flow_runtime(
        req.flow_id, req.version or None, tenant
    )

    if (local := _get_local(request)) is not None:
        try:
            schedule = local.create_schedule(flow_store.get_flow_store(), schedule)
        except IntegrityError as e:
            # A2R52-02：id 唯一约束兜底（随机后缀下实测不可达，唯一约束
            # 语义化）——此前 IntegrityError 裸穿 500。
            raise HTTPException(
                status_code=409,
                detail=error_detail(
                    "schedule_id_conflict",
                    f"调度 ID 冲突: {schedule['schedule_id']}，请重试",
                    schedule_id=schedule["schedule_id"],
                ),
            ) from e
    else:
        # A2R52-02：写入防覆盖（hsetnx）——生成侧已带随机后缀，此处是
        # 结构性兜底：id 撞行时 409 而非静默抹掉已有调度。
        if not redis.hsetnx(
            SCHEDULES_KEY,
            schedule["schedule_id"],
            json.dumps(schedule, ensure_ascii=False),
        ):
            raise HTTPException(
                status_code=409,
                detail=error_detail(
                    "schedule_id_conflict",
                    f"调度 ID 冲突: {schedule['schedule_id']}，请重试",
                    schedule_id=schedule["schedule_id"],
                ),
            )
    _audit(request, "schedule.create", schedule["schedule_id"],
           _schedule_detail(schedule), tenant_id=tenant)
    return _view(schedule)


@router.post("/schedules/import-local")
def import_local_schedules(
    request: Request,
    redis: Redis = Depends(get_redis_or_none),
    body: ImportLocalSchedulesRequest = None,
):
    """「导入本地定时」（迁移刀，2026-10-25）：把 SQLite 里的本地单机档调度
    迁入集群 Redis（``plaita:schedules``），消掉部署文档「切集群后请按相同
    cron 在集群档重建后删除本地条目」的手工账单。

    语义：
    - **集群档专用**：本地单机档（redis 不可达）无需迁移——本地调度本就在
      本进程执行，409 可操作错误指路；
    - 只处理**当前租户**的条目（平台管理员代迁其他租户走逐租户调用）；
    - 逐条 ``hsetnx`` 写入（撞行 = 已导入过，幂等跳过不覆盖）；保留原
      ``schedule_id`` 可追溯；``next_run_at`` 按导入时刻重算（跨机时钟差
      安全），``runtime`` 与手动 start 同源盖章（#300 池路由）；
    - 写入成功 → **原地停用**本地条目（enabled=False，保留行与触发历史——
      切回本地档仍可见，不会静默丢排班）；
    - 流程不存在/cron 非法的条目跳过并逐条点名（不挡整批）；
    - 触发历史（``local_schedule_fires``）不迁移（历史留在本地表，切回可见）。
    """
    tenant = tenant_scope(request, required=True)
    if (local := _get_local(request)) is not None or redis is None:
        raise HTTPException(
            status_code=409,
            detail=error_detail(
                "local_mode",
                "本地单机档无需导入：本地调度就在本进程执行。请接上 Redis"
                "（重启为集群档）后再调用本端点",
            ),
        )
    body = body or ImportLocalSchedulesRequest()
    store = flow_store.get_flow_store()

    try:
        from ..services import schedule_store as local_schedule_store
    except ImportError:
        from services import schedule_store as local_schedule_store  # type: ignore

    rows = local_schedule_store.list_schedules(store, tenant_id=tenant)
    candidates = [r for r in rows if r.get("enabled") or not body.enabled_only]
    imported: List[str] = []
    skipped_existing: List[str] = []
    skipped_invalid: List[dict] = []
    for row in candidates:
        sid = row["schedule_id"]
        if body.enabled_only and not row.get("enabled"):
            continue
        if not validate_cron(row.get("cron") or ""):
            skipped_invalid.append({"schedule_id": sid, "reason": "invalid_cron"})
            continue
        try:
            _validate_flow(row["flow_id"], row.get("version") or None, tenant_id=tenant)
        except HTTPException as e:
            skipped_invalid.append({
                "schedule_id": sid,
                "reason": "flow_unavailable",
                "detail": (e.detail or {}).get("message") if isinstance(e.detail, dict) else str(e.detail),
            })
            continue
        record = {
            # 保留原 id：撞行（hsetnx 失败）= 已导入过 → 幂等跳过，不覆盖
            "schedule_id": sid,
            "tenant_id": tenant,
            "name": row.get("name") or sid,
            "flow_id": row["flow_id"],
            "version": row.get("version") or None,
            "cron": row["cron"],
            "params": row.get("params") or {},
            "enabled": bool(row.get("enabled")),
            "created_at": row.get("created_at") or _now_iso(),
            "updated_at": _now_iso(),
            "created_by": row.get("created_by") or "",
            # 导入时刻重算（本地行的旧 next_run_at 可能已过期；服务侧本有
            # 自愈兜底，这里给明确值避免首扫竞态——与 create 同口径）
            "next_run_at": str(next_run_after(row["cron"])) if row.get("enabled") else "",
            "runtime": _resolve_flow_runtime(
                row["flow_id"], row.get("version") or None, tenant
            ),
        }
        if redis.hsetnx(
            SCHEDULES_KEY, sid, json.dumps(record, ensure_ascii=False)
        ):
            imported.append(sid)
            # 原地停用（保行保历史，幂等重跑只看 enabled 行）；失败只告警——
            # 集群侧已启用同 id 调度，重跑会走 skipped_existing 收敛
            try:
                disable = dict(row)
                disable["enabled"] = False
                disable["next_run_at"] = ""
                local_schedule_store.update_schedule(store, sid, disable, tenant_id=tenant)
            except Exception as e:  # noqa: BLE001 — 停用失败不回滚导入
                logger.warning(
                    "本地调度 %s 已导入集群但停用失败（重跑本端点会收敛）: %s", sid, e
                )
        else:
            skipped_existing.append(sid)
    _audit(
        request, "schedule.import_local",
        ",".join(imported)[:120] or "-",
        {
            "imported": imported,
            "skipped_existing": skipped_existing,
            "skipped_invalid": skipped_invalid,
            "candidates": len(candidates),
        },
        tenant_id=tenant,
    )
    return {
        "imported": imported,
        "imported_count": len(imported),
        "skipped_existing": skipped_existing,
        "skipped_invalid": skipped_invalid,
        "candidates": len(candidates),
    }


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
    # A2R52-01：update 钉版本与 create 同口（此前 _validate_flow 只在 create
    # 调，PUT 钉任意字符串 200 落行，cron 每 tick 产出 flow_definition_missing
    # 垃圾执行）。只拦「钉新版本」：非空 version 必须存在（422）；空串=解钉
    # （回到跟随 latest published 语义）不拦；不触碰 version 的更新不复查
    # 流程存在性（存量孤儿调度仍可改名/暂停/解钉）。
    if req.version:
        _validate_flow(schedule["flow_id"], req.version, tenant_id=tenant)

    # A2R57-02：复启门——级联暂停行（钉版已删、pinned_version_missing 标注
    # 在案）不得原样复活。同请求重钉有效版本（req.version 非空，上面 422 门
    # 已验存在）算修复动作放行；单纯复启/解钉后复启 → 409 先修复钉版。
    # 暂停（enabled=false）与改名/解钉不入此门（A2R52-01 豁免语义不变）。
    if req.enabled and not req.version:
        _raise_if_pin_missing(schedule)

    # plaita#300 触发面池路由：version 变化即重盖章 runtime。解析涉及
    # 版本库读，提前到 CAS mutate 之外算好（mutate 只做纯字段赋值）。
    new_runtime = (
        _resolve_flow_runtime(schedule["flow_id"], req.version or None, tenant)
        if req.version is not None
        else None
    )

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
            # plaita#300：随版本重盖章目标池（上面已按新 version 现算）
            current["runtime"] = new_runtime
            # A2R57-03：显式改钉（重钉过 422 门 / 解钉不再有钉）即解除
            # 「钉版缺失」标注——钉版面已由本次请求重新裁定，健康行不再
            # 永久带病征（此前写后全仓零清理点，视图透出假信号）。
            current.pop("pinned_version_missing", None)
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
        _raise_if_pin_missing(schedule)  # A2R57-02：级联暂停行禁原样复启
        schedule["enabled"] = True
        schedule["next_run_at"] = str(next_run_after(schedule["cron"]))
        # plaita#300：启用即重盖章 runtime（本地档执行在进程内，字段惰性）
        schedule["runtime"] = _resolve_flow_runtime(
            schedule["flow_id"], schedule.get("version"), tenant
        )
        result = local.update_schedule(store, schedule_id, schedule, tenant_id=tenant)
        _audit(request, "schedule.enable", schedule_id,
               _schedule_detail(result), tenant_id=tenant or "")
        return _view(result)
    schedule = _get_schedule(redis, schedule_id, tenant_id=tenant)
    _raise_if_pin_missing(schedule)  # A2R57-02：级联暂停行禁原样复启
    schedule["enabled"] = True
    schedule["next_run_at"] = str(next_run_after(schedule["cron"]))
    schedule["updated_at"] = _now_iso()
    # plaita#300：启用即重盖章 runtime——version=None 的调度按 latest
    # published 现算，新发布版本换池后重启用即校正 cron 路由。
    schedule["runtime"] = _resolve_flow_runtime(
        schedule["flow_id"], schedule.get("version"), tenant
    )
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
        # A2R52-03：暂停调度拒绝手动触发，与集群档同语义（此前本地档走到
        # fire() 的 A48-04 闸返回 None → 502「查看 console 日志」，把用户
        # 配置态误导成基础设施故障）。具名 409，无 fire record——拒绝发生在
        # 受理之前（与 403 停用租户同层）。只拦显式 enabled=False（行缺键的
        # 最小形状/畸形行不在此拦，行为零变化）。
        if schedule.get("enabled", True) is False:
            raise HTTPException(
                status_code=409,
                detail=error_detail(
                    "schedule_paused",
                    f"调度 {schedule_id} 已暂停，手动触发被拒绝；请先启用调度",
                    schedule_id=schedule_id,
                ),
            )
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
    # A2R52-03：暂停调度拒绝手动触发（此前集群档 fire_schedule 不查 enabled
    # 照常入队 200，与本地档 502 分裂）——两档统一为具名 409。cron 面不经过
    # 端点，不受影响（调度循环本就跳过 paused 行）。只拦显式 enabled=False。
    if schedule.get("enabled", True) is False:
        raise HTTPException(
            status_code=409,
            detail=error_detail(
                "schedule_paused",
                f"调度 {schedule_id} 已暂停，手动触发被拒绝；请先启用调度",
                schedule_id=schedule_id,
            ),
        )
    # plaita#300 触发面：与手动 POST /executions 同一受理语义——先过能力门
    # （不支持即 422 结构化拒绝，零入队零占位行），再按版本行 runtime 选池
    # 队列（此前恒落默认 python 队列，go-runtime flow 的调度会串池）。
    # #304×#300 合并口径：start 面对显式不可启动版本（不存在/draft）具名 404
    # ——交互调用方可即时纠正；触发面与 cron（schedule_service）同语义
    # fail-open——版本行缺失/未发布时按存量落默认队列，worker 以
    # flow_definition_missing 具名终态收场（降级留 WARNING 痕，不静默）。
    try:
        version_row = _admission_version_row(
            redis, schedule["flow_id"], schedule.get("version"), tenant
        )
    except StartVersionNotStartable as e:
        logger.warning(
            "调度触发受理降级（版本行不可启动，fail-open 落默认队列）: "
            "schedule=%s flow=%s@%s: %s",
            schedule_id, schedule.get("flow_id"), schedule.get("version"), e,
        )
        version_row = None
    trigger_queue: Optional[str] = None
    if version_row is not None:
        try:
            trigger_queue = resolve_runtime_queue(
                getattr(version_row, "runtime", None)
            )
        except ValueError:
            trigger_queue = None
    # 与手动「启动流程」同源：写任务队列 Stream（队列名在调用点解析 env）
    # A2R52-04：rejection 出参接闸拒绝原因——None 返回有多义（配额满/租户
    # 停用/入队失败/dedup 收敛），据此把「配额满」映射为与 POST /executions
    # 同契约的 429 tenant_quota_exceeded（#28 同一准入闸），其余仍 502。
    fire_rejection: dict = {}
    msg_id = fire_schedule(
        redis, schedule, trigger_queue or _task_queue_name(),
        trigger_kind="manual", rejection=fire_rejection,
    )
    if msg_id is None:
        if fire_rejection.get("reason") == REJECTION_QUOTA_EXHAUSTED:
            tenant_for_quota = schedule.get("tenant_id") or "default"
            max_running = (
                tenant_max_running(redis, tenant_for_quota)
                if tenant_max_running is not None else None
            )
            # None=未配额（fail-open 下本不应拒，防御性呈现「未配置」而非 None）
            cap_text = str(max_running) if max_running is not None else "未配置"
            logger.warning(
                "租户 %s start 配额已满（%s），拒绝调度 %s 手动触发",
                tenant_for_quota, cap_text, schedule_id,
            )
            raise HTTPException(
                status_code=429,
                detail=error_detail(
                    "tenant_quota_exceeded",
                    (
                        f"租户 {tenant_for_quota} 并发 start 数已达上限"
                        f"（{cap_text}），调度未入队；请等待在跑执行终态后重试"
                    ),
                    error="tenant_quota_exceeded",
                    tenant_id=tenant_for_quota,
                    max_running=max_running,
                ),
            )
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
