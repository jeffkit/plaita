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

**服务端权威字段（#262）**：``/dispatch/one`` 的 ``shadow`` / ``dry_run`` /
``tenant_id`` 不再信任请求体——shadow 只取 env，dry_run=false 仅平台身份
可写，tenant_id 取认证上下文（仅平台身份可代传）。跨租户非平台请求 403 +
``dispatch.one.denied`` 审计；台账记录 ``actor`` 与生效租户。
"""
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

try:
    from ..request_models import StrictRequestModel
except ImportError:  # 顶层 ``api`` 包（测试/脚本模式）
    from request_models import StrictRequestModel  # type: ignore

try:
    from ..auth import require_auth, tenant_scope
    from ..services import dispatcher as dispatcher_svc
except ImportError:  # 平铺布局（cwd=backend）运行时
    from auth import require_auth, tenant_scope  # type: ignore
    from services import dispatcher as dispatcher_svc  # type: ignore

logger = logging.getLogger(__name__)

router = APIRouter()

#: #262：租户 editor 试图把派发代传到别的租户时的具名拒绝。
DISPATCH_TENANT_DENIED_DETAIL = "仅平台管理员可指定派发租户"
#: A54-02：队列对账是全局计数，只对平台身份开放。
TENANT_SCOPED_PARITY_DETAIL = "租户视图：队列对账仅平台身份可用"


def _tenant_generation(tenant_id: Optional[str]) -> Optional[str]:
    """当前租户世代（create 时间）；取不到 ⇒ None（读面 fail-closed）。"""
    if not tenant_id:
        return None
    try:
        from ..services import flow_store, tenants_svc
    except ImportError:  # 平铺布局（cwd=backend）
        from services import flow_store, tenants_svc  # type: ignore
    try:
        return tenants_svc.tenant_generation(flow_store.get_flow_store(), tenant_id)
    except Exception:  # noqa: BLE001 — 世代不可证明 ⇒ 不返回租户行（响亮记录）
        logger.exception(
            "读取租户世代失败（tenant=%s），派发台账租户视图按 fail-closed 处理",
            tenant_id,
        )
        return None


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


class DispatchPlanRequest(StrictRequestModel):
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
    # A55-S1：影子计划同样留痕（只算不发，但仍是对全局容量面的访问）。
    try:
        from ..services import audit as audit_svc
    except ImportError:
        from services import audit as audit_svc  # type: ignore
    audit_svc.record(
        request,
        action="dispatch.plan",
        resource="dispatch",
        resource_id=plan.get("queue", ""),
        detail={
            "issues": len(req.issue_ids),
            "queue": plan.get("queue"),
            "shadow": bool(plan.get("shadow")),
            "target": plan.get("target"),
        },
        tenant_id=tenant_scope(request, required=True),
    )
    return DispatchPlanResponse(**plan)


# ============ 期 1：单次可控派发 + 派发台账 ============

class DispatchOneRequest(BaseModel):
    """单次可控派发请求（金丝雀/按仓切流的最小单元）。

    #262：``shadow`` / ``dry_run`` 与 ``tenant_id`` 都是**服务端权威字段**，
    不接受客户端直接覆盖：

    - ``shadow`` 完全由 ``PLAITA_CONSOLE_DISPATCH_SHADOW`` 决定；请求体里的
      同名字段被显式剔除（``extra="ignore"``），传了也不生效；
    - ``dry_run`` 仅对平台身份（platform_admin / API-Key）是可写提示；租户
      editor 传 ``false`` 会被忽略并降为干跑（WARNING 留痕）；
    - ``tenant_id`` 默认取认证上下文（``tenant_scope(required=True)``）；只有
      可证明的平台身份可以代传其它租户，其余身份跨租户即 403 + 审计。
    """

    model_config = ConfigDict(extra="ignore")

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
    tenant_id: Optional[str] = Field(
        None,
        description=(
            "租户代传提示：仅平台身份有效；普通会话必须等于认证上下文，"
            "否则 403。缺省取认证上下文"
        ),
    )
    dry_run: bool = Field(
        True,
        description=(
            "**默认真** = 只算不发（返回决策与目标，不入队、不写台账）。"
            "仅平台身份（platform_admin / API-Key）可显式传 false 真发单；"
            "其它身份传 false 会被忽略并强制干跑"
        ),
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
async def dispatch_one(
    req: DispatchOneRequest,
    request: Request,
    identity: Dict = Depends(require_auth),
) -> DispatchOneResponse:
    """单次可控派发（期 1）：容量路由 → 显式目标 → 入队 + 落台账。

    **默认只算不发**（``dry_run=true`` 且影子模式默认开）：返回目标与原因，
    不写任何 Redis 键。真发单需同时 ``dry_run=false`` 且
    ``PLAITA_CONSOLE_DISPATCH_SHADOW=0``——两道闸都显式打开才会入队。

    #262：服务端权威字段。``shadow`` 只取 env（请求体字段被剔除）；
    ``dry_run=false`` 仅平台身份可写，租户 editor 传 false 会被忽略并
    强制干跑（WARNING）；``tenant_id`` 取认证上下文，只有平台身份可代传。
    台账记录 ``actor`` 与实际生效租户。
    """
    platform_identity = bool(identity.get("platform_admin"))

    # 租户：认证上下文是唯一默认来源；跨租户代传只对平台身份开放。
    auth_tenant = tenant_scope(request, required=True)
    requested_tenant = (req.tenant_id or "").strip() or None
    if requested_tenant and requested_tenant != auth_tenant:
        if not platform_identity:
            logger.warning(
                "拒绝非平台身份的跨租户派发 actor=%s issue=%s auth_tenant=%s "
                "requested_tenant=%s",
                identity.get("actor"), req.issue_id, auth_tenant, requested_tenant,
            )
            try:
                from ..services import audit as audit_svc
            except ImportError:
                from services import audit as audit_svc  # type: ignore
            audit_svc.record(
                request,
                action="dispatch.one.denied",
                resource="dispatch",
                resource_id=req.issue_id,
                detail={
                    "denied": True,
                    "reason": "tenant_delegation_required",
                    "auth_tenant": auth_tenant,
                    "requested_tenant": requested_tenant,
                    "method": request.method,
                    "path": request.url.path,
                },
            )
            raise HTTPException(status_code=403, detail=DISPATCH_TENANT_DENIED_DETAIL)
        tenant_id = requested_tenant
    else:
        tenant_id = auth_tenant

    # dry_run：非平台身份只能干跑。忽略 false 必须留 WARNING（不是静默降级）。
    effective_dry_run = bool(req.dry_run)
    if not effective_dry_run and not platform_identity:
        logger.warning(
            "非平台身份请求真实派发已强制降为干跑 actor=%s issue=%s path=%s",
            identity.get("actor"), req.issue_id, request.url.path,
        )
        effective_dry_run = True

    # shadow 不传：dispatch_one 只读 PLAITA_CONSOLE_DISPATCH_SHADOW（服务端配置）。
    result = dispatcher_svc.dispatch_one(
        get_redis(request),
        req.issue_id,
        flow_id=req.flow_id,
        execution_id=req.execution_id,
        params=req.params,
        queue_name=req.queue_name,
        tenant_id=tenant_id,
        tenant_generation=_tenant_generation(tenant_id),
        dry_run=effective_dry_run,
        actor=identity.get("actor"),
    )
    # A55-S1：派发决策成功路径留痕（真发单必记；干跑同样记录）。
    try:
        from ..services import audit as audit_svc
    except ImportError:
        from services import audit as audit_svc  # type: ignore
    audit_svc.record(
        request,
        action="dispatch.one",
        resource="dispatch",
        resource_id=req.issue_id,
        detail={
            "issue_id": req.issue_id,
            "execution_id": result.get("execution_id"),
            "queue": result.get("queue"),
            "tenant_id": tenant_id,
            "dry_run": bool(result.get("dry_run")),
            "enqueued": bool(result.get("enqueued")),
            "target": result.get("target"),
            "decision": result.get("decision"),
            "allowed": len(result.get("allowed") or []),
            "rejected": len(result.get("rejected") or []),
        },
        tenant_id=tenant_id,
    )
    return DispatchOneResponse(**result)


@router.get("/dispatch/ledger", response_model=DispatchLedgerResponse)
async def dispatch_ledger(
    request: Request,
    queue_name: Optional[str] = None,
    count: int = 50,
    identity: Dict = Depends(require_auth),
) -> DispatchLedgerResponse:
    """派发台账 + 与队列的对账（``台账条数 == 入队条数``，期 1 判据②）。

    A54-02：平台身份可全量审计；租户身份只看到**当前世代**的本租户条目
    （同名重建后旧世代 actor/issue/execution 不再命中）。队列对账是全局
    计数，只对平台身份返回，租户视图显式给出不可用原因（不伪造成 0/相等）。
    """
    redis = get_redis(request)
    limit = max(1, min(int(count), 500))
    if bool(identity.get("platform_admin")):
        rows = dispatcher_svc.ledger_rows(redis, count=limit)
        parity = dispatcher_svc.dispatch_parity(redis, queue_name)
        return DispatchLedgerResponse(
            queue=parity["queue_name"],
            ledger=parity["ledger"],
            queue_length=parity["queue"],
            parity=parity["parity"],
            error=parity["error"],
            rows=rows,
        )

    tenant = tenant_scope(request)
    rows = dispatcher_svc.ledger_rows(
        redis,
        count=limit,
        tenant_id=tenant,
        tenant_generation=_tenant_generation(tenant),
    )
    return DispatchLedgerResponse(
        queue=queue_name or dispatcher_svc.dispatch_queue_from_env(),
        ledger=len(rows),
        queue_length=None,
        parity=False,
        error=TENANT_SCOPED_PARITY_DETAIL,
        rows=rows,
    )
