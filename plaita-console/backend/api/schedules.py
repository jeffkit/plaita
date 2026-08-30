"""
调度（触发器）管理 API

调度定义存储在 Redis（与引擎 schedule_service 共享视图）：
- plaita:schedules            hash: schedule_id -> 定义 JSON
- plaita:schedule:fires:{id}  list: 最近触发记录（由 schedule_service 写入）

端点（管理面，Admin API Key）：
- GET    /api/schedules                       全部调度（含下次触发时间）
- POST   /api/schedules                       新建（校验 cron / 流程存在）
- GET    /api/schedules/{schedule_id}         调度详情
- PUT    /api/schedules/{schedule_id}         更新（重算 next_run_at）
- DELETE /api/schedules/{schedule_id}         删除（含触发历史）
- POST   /api/schedules/{schedule_id}/enable  启用（立即重算 next_run_at）
- POST   /api/schedules/{schedule_id}/disable 暂停
- POST   /api/schedules/{schedule_id}/trigger 立即触发一次（手动入队）
- GET    /api/schedules/{schedule_id}/history 最近触发记录
- POST   /api/schedules/preview               cron 预览：给出未来 N 次触发时间
"""
import json
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from redis import Redis

try:
    from plaita.server.services.schedule_service import (
        SCHEDULES_KEY,
        SCHEDULE_FIRES_KEY,
        fire_schedule,
        list_schedules,
        next_run_after,
        validate_cron,
    )
    from ..services import flow_store
except ImportError:  # 平铺布局（cwd=backend）运行时
    from services import flow_store

# get_redis / TASK_QUEUE_NAME 与 executions 共享同一依赖与队列名
from .executions import get_redis, TASK_QUEUE_NAME

router = APIRouter()


# ============ 请求/响应模型 ============

class ScheduleCreateRequest(BaseModel):
    name: str = Field(..., description="调度名称")
    flow_id: str = Field(..., description="目标流程 ID")
    version: Optional[str] = Field(None, description="流程版本；空 = 最新已发布")
    cron: str = Field(..., description="cron 表达式（minute hour day month weekday，本地时区）")
    params: dict = Field(default_factory=dict, description="启动入参")
    enabled: bool = Field(True, description="是否启用")
    created_by: str = Field("", description="创建人")


class ScheduleUpdateRequest(BaseModel):
    name: Optional[str] = None
    version: Optional[str] = None
    cron: Optional[str] = None
    params: Optional[dict] = None
    enabled: Optional[bool] = None


class CronPreviewRequest(BaseModel):
    cron: str
    count: int = Field(5, ge=1, le=20)


# ============ 内部工具 ============

def _now_iso() -> str:
    return datetime.now().isoformat()


def _get_schedule(redis: Redis, schedule_id: str) -> dict:
    raw = redis.hget(SCHEDULES_KEY, schedule_id)
    if raw is None:
        raise HTTPException(status_code=404, detail=f"调度不存在: {schedule_id}")
    if isinstance(raw, bytes):
        raw = raw.decode()
    return json.loads(raw)


def _save_schedule(redis: Redis, schedule: dict) -> None:
    redis.hset(SCHEDULES_KEY, key=schedule["schedule_id"], value=json.dumps(schedule, ensure_ascii=False))


def _view(schedule: dict) -> dict:
    """API 视图：统一给前端的结构（补计算字段）。"""
    out = dict(schedule)
    out["status"] = "running" if schedule.get("enabled") else "paused"
    return out


def _validate_flow(flow_id: str, version: Optional[str]) -> None:
    store = flow_store.get_flow_store()
    if store.get_flow_record(flow_id) is None:
        raise HTTPException(status_code=422, detail=f"流程不存在: {flow_id}")
    if version:
        published = {v.version for v in store.list_versions(flow_id)}
        if version not in published:
            raise HTTPException(status_code=422, detail=f"流程 {flow_id} 不存在版本 {version}")


# ============ 端点 ============

@router.get("/schedules")
def list_all(redis: Redis = Depends(get_redis)):
    schedules = sorted(list_schedules(redis), key=lambda s: s.get("created_at") or "")
    return {"schedules": [_view(s) for s in schedules], "total": len(schedules)}


@router.post("/schedules")
def create(req: ScheduleCreateRequest, redis: Redis = Depends(get_redis)):
    if not req.name.strip():
        raise HTTPException(status_code=422, detail="name 不能为空")
    if not validate_cron(req.cron):
        raise HTTPException(status_code=422, detail=f"非法 cron 表达式: {req.cron}")
    _validate_flow(req.flow_id, req.version)

    schedule_id = f"sched-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}"
    now = _now_iso()
    schedule = {
        "schedule_id": schedule_id,
        "name": req.name.strip(),
        "flow_id": req.flow_id,
        "version": req.version or None,
        "cron": req.cron,
        "params": req.params or {},
        "enabled": bool(req.enabled),
        "created_at": now,
        "updated_at": now,
        "created_by": req.created_by,
        # 服务自愈兜底，但创建时就给明确值，避免首扫竞态
        "next_run_at": str(next_run_after(req.cron)) if req.enabled else "",
    }
    _save_schedule(redis, schedule)
    return _view(schedule)


@router.get("/schedules/preview")
def preview_cron(cron: str, count: int = 5, redis: Redis = Depends(get_redis)):
    """cron 预览：从现在起未来 count 次触发时间（本地时区 ISO 字符串）。"""
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
def get_one(schedule_id: str, redis: Redis = Depends(get_redis)):
    return _view(_get_schedule(redis, schedule_id))


@router.put("/schedules/{schedule_id}")
def update(schedule_id: str, req: ScheduleUpdateRequest, redis: Redis = Depends(get_redis)):
    schedule = _get_schedule(redis, schedule_id)
    if req.name is not None:
        if not req.name.strip():
            raise HTTPException(status_code=422, detail="name 不能为空")
        schedule["name"] = req.name.strip()
    if req.version is not None:
        schedule["version"] = req.version or None
    if req.cron is not None:
        if not validate_cron(req.cron):
            raise HTTPException(status_code=422, detail=f"非法 cron 表达式: {req.cron}")
        schedule["cron"] = req.cron
    if req.params is not None:
        schedule["params"] = req.params
    if req.enabled is not None:
        schedule["enabled"] = bool(req.enabled)

    # cron/enabled 变化后重算下次触发；编辑即重置节奏
    if req.cron is not None or req.enabled is not None:
        schedule["next_run_at"] = (
            str(next_run_after(schedule["cron"])) if schedule.get("enabled") else ""
        )
    schedule["updated_at"] = _now_iso()
    _save_schedule(redis, schedule)
    return _view(schedule)


@router.delete("/schedules/{schedule_id}")
def remove(schedule_id: str, redis: Redis = Depends(get_redis)):
    _get_schedule(redis, schedule_id)
    redis.hdel(SCHEDULES_KEY, schedule_id)
    redis.delete(SCHEDULE_FIRES_KEY.format(schedule_id=schedule_id))
    return {"success": True, "schedule_id": schedule_id}


@router.post("/schedules/{schedule_id}/enable")
def enable(schedule_id: str, redis: Redis = Depends(get_redis)):
    schedule = _get_schedule(redis, schedule_id)
    schedule["enabled"] = True
    schedule["next_run_at"] = str(next_run_after(schedule["cron"]))
    schedule["updated_at"] = _now_iso()
    _save_schedule(redis, schedule)
    return _view(schedule)


@router.post("/schedules/{schedule_id}/disable")
def disable(schedule_id: str, redis: Redis = Depends(get_redis)):
    schedule = _get_schedule(redis, schedule_id)
    schedule["enabled"] = False
    schedule["updated_at"] = _now_iso()
    _save_schedule(redis, schedule)
    return _view(schedule)


@router.post("/schedules/{schedule_id}/trigger")
def trigger_now(schedule_id: str, redis: Redis = Depends(get_redis)):
    """立即触发一次：消息形状与 cron 触发完全一致（trigger_kind=manual）。"""
    schedule = _get_schedule(redis, schedule_id)
    # 与手动「启动流程」同源：写任务队列 Stream
    try:
        from .executions import TASK_QUEUE_NAME
    except ImportError:
        from api.executions import TASK_QUEUE_NAME
    msg_id = fire_schedule(redis, schedule, TASK_QUEUE_NAME, trigger_kind="manual")
    if msg_id is None:
        raise HTTPException(status_code=502, detail="入队失败，请检查 Redis 与任务队列配置")
    return {"success": True, "msg_id": msg_id}


@router.get("/schedules/{schedule_id}/history")
def history(schedule_id: str, limit: int = 10, redis: Redis = Depends(get_redis)):
    _get_schedule(redis, schedule_id)
    limit = max(1, min(50, limit))
    raw = redis.lrange(SCHEDULE_FIRES_KEY.format(schedule_id=schedule_id), 0, limit - 1)
    records = []
    for item in raw:
        if isinstance(item, bytes):
            item = item.decode()
        try:
            records.append(json.loads(item))
        except json.JSONDecodeError:
            continue
    return {"schedule_id": schedule_id, "records": records, "total": len(records)}
