"""worker 容量 / 派发决策 API（#42：W10-a 期 0 决策面 + W23-a 期 1 派发）。

期 0（只读决策面）：

- ``GET  /api/dispatch/capacity``      各 worker 上报的容量快照 + 可派性
- ``POST /api/dispatch/plan``          给定一批 issue，算确定性派单计划（影子）

期 1（可控派发 + 台账）：

- ``POST /api/dispatch/one``           单次可控派发（**默认 dry_run**）：容量
  路由 → 消息体写 ``target_instance_id`` → 入队 + 落一条派发台账
- ``GET  /api/dispatch/ledger``        派发台账 + 「台账条数 == 入队条数」对账

为什么需要它们：registry 此前只回答「谁活着」（``ServiceInfo`` 无容量字段），
console 因此**看不见**低盘/饱和的 worker——2026-10-06 本机盘触底 0 字节仍被
压单，就是这条盲区的直接后果。本组端点把 worker 自报的容量快照与派发门的
判定（可派 / 拒绝原因 / 陈旧）暴露出来，供运维看板与 intake flow 共用同一份
判据（口径不分叉）。

**影子纪律**（验收 5）：``POST /plan`` 返回 ``enqueued: 0`` 且不写任何 Redis
键；``POST /one`` 默认 ``dry_run=true`` 且影子模式默认开 ⇒ 同样零写入。
切流需另一个显式步骤（金丝雀按仓 → 全量），且金丝雀队列与生产队列
``XLEN`` 物理隔离（期 1 判据③）。
"""
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

try:
    from ..services import dispatcher as dispatcher_svc
except ImportError:  # 平铺布局（cwd=backend）运行时
    from services import dispatcher as dispatcher_svc  # type: ignore

logger = logging.getLogger(__name__)

router = APIRouter()


# ============ 数据模型 ============

class WorkerCapacityRow(BaseModel):
    """单个 worker 的容量快照 + 可派性判定。"""

    instance_id: str = Field(..., description="worker 实例 ID")
    host: str = Field("", description="worker 主机")
    status: str = Field("running", description="注册状态")
    active_tasks: int = Field(0, description="注册表口径的在跑任务数")
    capacity: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "worker 自报容量块（worker_capacity）：free_disk_gib / "
            "min_free_disk_gib / disk_blocked / active_tasks / concurrency / "
            "sampled_at；null = 该 worker 未上报容量（存量 worker）"
        ),
    )
    capacity_stale: bool = Field(False, description="快照是否超出新鲜期（不可采信内容）")
    capacity_age_seconds: Optional[float] = Field(None, description="快照年龄（秒）")
    dispatchable: bool = Field(True, description="本机是否可被派单")
    reject_reason: Optional[str] = Field(None, description="不可派原因（前缀稳定）")


class DispatchPlanRequest(BaseModel):
    issue_ids: List[str] = Field(default_factory=list, description="本轮候选 issue ID")
    queue_name: Optional[str] = Field(
        None, description="计划队列名（缺省跟随 PLAITA_CONSOLE_TASK_QUEUE）"
    )
    shadow: Optional[bool] = Field(
        None, description="影子模式覆盖（缺省跟随 PLAITA_CONSOLE_DISPATCH_SHADOW，默认 true）"
    )


class DispatchPlanResponse(BaseModel):
    shadow: bool = Field(True, description="影子模式（只算不发）")
    shadow_mode: bool = Field(True, description="同 shadow（显式字段，便于看板直读）")
    queue: str = Field(..., description="计划队列名")
    issue_ids: List[str] = Field(default_factory=list)
    target: Optional[str] = Field(None, description="本轮选中的 worker（null = 本轮不派）")
    allowed: List[str] = Field(default_factory=list, description="通过容量门的 worker")
    rejected: List[Dict[str, Any]] = Field(default_factory=list, description="被拒 + 原因")
    stale: List[str] = Field(default_factory=list, description="快照陈旧的 worker")
    candidate_count: int = Field(0, description="注册表里看到的 worker 数")
    enqueued: int = Field(0, description="本接口**恒为 0**（不发单）")


# ============ 工具函数 ============

def get_redis(request: Request):
    """取 Redis；本地单机模式返回 503（容量面只在集群档有意义）。"""
    redis = getattr(request.app.state, "redis", None)
    if redis is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "当前为本地单机模式（未连接 Redis），容量面不可用。"
                "启动 Redis 并重启 console 可恢复完整集群能力。"
            ),
        )
    return redis


# ============ API 端点 ============

@router.get("/dispatch/capacity", response_model=List[WorkerCapacityRow])
async def list_worker_capacity(request: Request) -> List[WorkerCapacityRow]:
    """各 worker 的容量快照 + 可派性（派发侧唯一可见面）。"""
    rows = dispatcher_svc.worker_capacity_rows(get_redis(request))
    return [WorkerCapacityRow(**row) for row in rows]


@router.post("/dispatch/plan", response_model=DispatchPlanResponse)
async def dispatch_plan(req: DispatchPlanRequest, request: Request) -> DispatchPlanResponse:
    """确定性派单计划（影子：只算不发，``enqueued`` 恒 0）。"""
    plan = dispatcher_svc.build_dispatch_plan(
        get_redis(request),
        req.issue_ids,
        queue_name=req.queue_name,
        shadow=req.shadow,
    )
    return DispatchPlanResponse(**plan)


# ============ 期 1：单次可控派发 + 派发台账 ============

class DispatchOneRequest(BaseModel):
    """单次可控派发请求（金丝雀/按仓切流的最小单元）。"""

    issue_id: str = Field(..., description="本轮派发的 issue（台账主键）")
    flow_id: Optional[str] = Field(None, description="要跑的 flow（缺省 = issue_id）")
    execution_id: Optional[str] = Field(
        None, description="预铸执行 ID（缺省 = issue_id；调用方据此立刻可轮询）"
    )
    params: Dict[str, Any] = Field(default_factory=dict, description="调度入参")
    queue_name: Optional[str] = Field(
        None,
        description=(
            "目标队列（缺省跟随 PLAITA_CONSOLE_TASK_QUEUE）。金丝雀期显式传"
            "金丝雀队列 ⇒ 与生产队列 XLEN 物理隔离（期 1 判据③）"
        ),
    )
    tenant_id: Optional[str] = Field(None, description="租户 ID（透传进消息体）")
    dry_run: bool = Field(
        True,
        description=(
            "**默认真** = 只算不发（返回决策与目标，不入队、不写台账）。"
            "真发单必须显式传 false，且 PLAITA_CONSOLE_DISPATCH_SHADOW 未开启"
        ),
    )
    shadow: Optional[bool] = Field(
        None, description="影子模式覆盖（缺省跟随 PLAITA_CONSOLE_DISPATCH_SHADOW，默认 true）"
    )


class DispatchOneResponse(BaseModel):
    shadow: bool = Field(True, description="影子模式（只算不发）")
    dry_run: bool = Field(True, description="调用方是否只算不发")
    issue_id: str = Field(..., description="本轮 issue")
    execution_id: str = Field(..., description="执行 ID")
    queue: str = Field(..., description="目标队列")
    target: Optional[str] = Field(
        None, description="容量路由选中的 worker（null = 无可派目标 ⇒ 广播放行）"
    )
    reason: str = Field("", description="路由稳定原因前缀（route: …）")
    decision: str = Field(
        "", description="targeted（显式派发）| broadcast（放行）| shadow | dry_run"
    )
    allowed: List[str] = Field(default_factory=list)
    rejected: List[Dict[str, Any]] = Field(default_factory=list)
    stale: List[str] = Field(default_factory=list)
    candidate_count: int = Field(0)
    enqueued: bool = Field(False, description="是否真的入队（影子/干跑恒 false）")
    message_id: Optional[str] = Field(None, description="入队后的 Stream 消息 ID")
    ledger_id: Optional[str] = Field(None, description="派发台账条目 ID")
    ledger_recorded: bool = Field(False, description="台账是否落盘成功")
    ledger_key: Optional[str] = Field(None, description="台账 Stream 键")
    error: Optional[str] = Field(None, description="入队/记账失败原因（成功为 null）")


class DispatchLedgerResponse(BaseModel):
    queue: str = Field(..., description="对照的队列名")
    ledger: Optional[int] = Field(None, description="台账条数（XLEN）")
    queue_length: Optional[int] = Field(None, description="队列条数（XLEN）")
    parity: bool = Field(False, description="台账条数 == 队列条数（期 1 判据）")
    error: Optional[str] = Field(None, description="读失败原因（无法证明相等时为非 null）")
    rows: List[Dict[str, Any]] = Field(default_factory=list, description="台账条目（旧 → 新）")


@router.post("/dispatch/one", response_model=DispatchOneResponse)
async def dispatch_one(req: DispatchOneRequest, request: Request) -> DispatchOneResponse:
    """单次可控派发（期 1）：容量路由 → 显式目标 → 入队 + 落台账。

    **默认只算不发**（``dry_run=true`` 且影子模式默认开）：返回目标与原因，
    不写任何 Redis 键。真发单需同时 ``dry_run=false`` 且
    ``PLAITA_CONSOLE_DISPATCH_SHADOW=0``——两道闸都显式打开才会入队。
    """
    result = dispatcher_svc.dispatch_one(
        get_redis(request),
        req.issue_id,
        flow_id=req.flow_id,
        execution_id=req.execution_id,
        params=req.params,
        queue_name=req.queue_name,
        tenant_id=req.tenant_id,
        dry_run=req.dry_run,
        shadow=req.shadow,
    )
    return DispatchOneResponse(**result)


@router.get("/dispatch/ledger", response_model=DispatchLedgerResponse)
async def dispatch_ledger(
    request: Request,
    queue_name: Optional[str] = None,
    count: int = 50,
) -> DispatchLedgerResponse:
    """派发台账 + 与队列的对账（``台账条数 == 入队条数``，期 1 判据②）。"""
    redis = get_redis(request)
    rows = dispatcher_svc.ledger_rows(redis, count=max(1, min(int(count), 500)))
    parity = dispatcher_svc.dispatch_parity(redis, queue_name)
    return DispatchLedgerResponse(
        queue=parity["queue_name"],
        ledger=parity["ledger"],
        queue_length=parity["queue"],
        parity=parity["parity"],
        error=parity["error"],
        rows=rows,
    )
