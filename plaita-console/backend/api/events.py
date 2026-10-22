"""
事件系统管理 API
提供事件订阅查看、事件发布、事件列表等接口

多租户边界（plaita#191 / #192）：
- 发布：租户由服务端从请求上下文绑定，写进事件**受信顶层字段** ``tenant_id``；
  客户端在 ``data`` 里自报的 tenant_id 一律剥离（data 是载荷，不是授权来源）。
- 读/删：事件与订阅按记录里的受信 ``tenant_id`` 收窄；单条资源不匹配按 404
  返回（不暴露存在性）。平台视角（``tenant_scope() is None``）保持全量。

读面成本（A2R45-04，#202 原语）：两列表端点的键枚举走有界 ``scan_keys``
（页大小 ≤ ``PAGE_SIZE_MAX``、键数 / 轮次 / 墙钟三预算），触界时响应
``scan_truncated=true`` + WARNING，绝不 ``KEYS``、绝不无界空转——这两个端点
只要 viewer 的 GET 权限，共享 Redis 的 keyspace 不能被最低权限反复全库遍历。

total 契约（A2R47-04）：触界时 ``total`` 哨兵化为 ``-1``——未扫完的
keyspace 没有可信的「匹配总数」（实现里的 ``total`` 曾是已扫前缀内计数且
逐页递减，naive 客户端把它当全集会静默丢数据）。完整遍历的终止条件是
**合取**：``truncated=false`` **且** ``scan_truncated=false``。
"""
import json
import logging
import re
import time
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field, field_validator

try:
    from ..request_models import EmptyRequestModel, StrictRequestModel
except ImportError:  # 顶层 ``api`` 包（测试/脚本模式）
    from request_models import EmptyRequestModel, StrictRequestModel  # type: ignore
from redis import Redis

try:
    from ..auth import tenant_scope
except ImportError:  # 平铺布局（cwd=backend）运行时
    from auth import tenant_scope  # type: ignore

try:  # A2R24-01：异常文本脱敏
    from ..error_contract import scrubbed_detail
except ImportError:
    from error_contract import scrubbed_detail  # type: ignore

try:  # A2R45-04：有界 SCAN（与 logs / queues / services / executions / metrics 同一原语）
    from .redis_scan import scan_keys
except ImportError:
    from redis_scan import scan_keys  # type: ignore

# A69-02：console 发布事件写引擎正典键形，与回扫/引擎读面同源。
from plaita.event.core import Event
from plaita.event.redis import event_retention_ttl_seconds, store_event_sync

logger = logging.getLogger(__name__)

router = APIRouter()


def _record_tenant(info: Dict[str, Any]) -> str:
    """记录归属租户：受信顶层字段，缺失（升级前旧记录）视为 default。"""
    return info.get("tenant_id") or "default"


def _audit(request: Request, action: str, resource_id: str,
           detail: Optional[Dict[str, Any]] = None, *,
           tenant_id: Optional[str] = None) -> None:
    """A55-S1：事件写操作审计（不记 data/payload）。"""
    try:
        from ..services import audit as audit_svc
    except ImportError:
        from services import audit as audit_svc  # type: ignore
    audit_svc.record(
        request,
        action=action,
        resource="event",
        resource_id=resource_id,
        detail=detail,
        tenant_id=tenant_id,
    )


# ============ 数据模型 ============

class EventInfo(BaseModel):
    event_id: str
    event_type: str
    data: Dict[str, Any] = Field(default_factory=dict)
    timestamp: Optional[float] = None
    correlation_id: Optional[str] = None
    source: Optional[str] = None


class EventListResponse(BaseModel):
    events: List[EventInfo]
    # A2R24-05 / A2R47-04：``total`` 是匹配总数——**只在完整扫描时可信**；
    # ``scan_truncated=true`` 时哨兵化为 ``-1``（键空间未扫完，总数不可知，
    # 不得当总数参与计算）。``returned`` 是本页条数；``next_cursor`` 可直接
    # 回传给 ``?cursor=`` 继续翻页（历史不再在 limit 之外不可达）。
    total: int
    returned: int = 0
    truncated: bool = False
    next_cursor: Optional[str] = None
    # A2R19-04：部分键读取失败时显式标注——空列表不等于「没有事件」。
    degraded: bool = False
    skipped: int = 0
    # A2R45-04：键枚举触到有界 SCAN 预算——列表只是匹配键的一部分。与
    # ``truncated``（翻页：本页之后还有）是两回事，分开表达。完整遍历的
    # 终止条件是合取：``truncated=false`` 且 ``scan_truncated=false``
    # （A2R47-04），此时 ``total=-1``。
    scan_truncated: bool = False


class SubscriptionInfo(BaseModel):
    subscription_id: str
    event_type: str
    filter_condition: Optional[Dict[str, Any]] = None
    correlation_id: Optional[str] = None
    flow_id: Optional[str] = None
    node_id: Optional[str] = None
    created_at: Optional[float] = None
    timeout: Optional[float] = None


class SubscriptionListResponse(BaseModel):
    subscriptions: List[SubscriptionInfo]
    # A2R47-04：同 EventListResponse.total——scan_truncated=true 时哨兵 -1。
    total: int
    degraded: bool = False
    skipped: int = 0
    # A2R45-04：同 EventListResponse.scan_truncated
    scan_truncated: bool = False


#: A55-S5：事件类型是 Redis 键的一部分（``plaita:event:types:{event_type}``），
#: 只能是受限字符集；空白/控制字符会制造怪异键名，``:dlq`` 后缀会被
#: 队列发现逻辑误认成死信。
_EVENT_TYPE_RE = re.compile(r"^[A-Za-z0-9._:/=-]{1,128}$")

#: A69-02：引擎正典事件记录键前缀（``plaita:event:events:{id}``）。同 id 的
#: 旧 console 形（``plaita:event:{id}``）仍可能并存，列表按此判定优先级。
_CANONICAL_EVENT_KEY_PREFIX = "plaita:event:events:"


class PublishEventRequest(StrictRequestModel):
    event_type: str = Field(..., description="事件类型")
    data: Dict[str, Any] = Field(default_factory=dict, description="事件数据")
    correlation_id: Optional[str] = Field(None, description="关联 ID")

    @field_validator("event_type")
    @classmethod
    def _validate_event_type(cls, value: str) -> str:
        if not _EVENT_TYPE_RE.fullmatch(value or ""):
            raise ValueError(
                "event_type 只允许 1-128 位 [A-Za-z0-9._:/=-]，"
                "不得含空白或控制字符"
            )
        if value.lower().endswith(":dlq"):
            raise ValueError("event_type 不得以 :dlq 结尾（会污染 DLQ/指标视图）")
        return value


# ============ 工具函数 ============

def get_redis(request: Request) -> Redis:
    redis = request.app.state.redis
    if redis is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "当前为本地单机模式（未连接 Redis），该功能不可用。"
                "启动 Redis 并重启 console 可恢复完整集群能力。"
            ),
        )
    return redis


# A2R45-04：列表端点的扫描预算。COUNT 取 PAGE_SIZE_MAX（同 logs：同样的 keyspace
# 轮次比默认 500 少一半）；匹配键上限 5000——每个匹配键随后还要各发一次
# TYPE + GET，上限同时约束这段 N+1 的成本。轮次 / 墙钟沿 scan_keys 默认
# （250 轮 / 1s）。触界 fail-safe：返回已收集部分 + ``scan_truncated``，不抛错。
EVENT_SCAN_COUNT = 1000
EVENT_SCAN_MAX_KEYS = 5000


def _bounded_event_keys(redis: Redis, pattern: str) -> tuple[List[str], bool]:
    result = scan_keys(redis, pattern, count=EVENT_SCAN_COUNT, max_keys=EVENT_SCAN_MAX_KEYS)
    return result.keys, result.truncated


# ============ 事件 API ============

def _parse_event_cursor(raw: Optional[str]) -> Optional[tuple]:
    """解析 ``<timestamp>:<event_id>`` 游标（A2R24-05）。

    时间戳是浮点数（无冒号），event_id 可能含冒号——按第一个冒号切分。
    非法游标 422，不静默退回首页（否则翻页会无限重复）。
    """
    if raw is None or not str(raw).strip():
        return None
    ts_part, sep, event_id = str(raw).strip().partition(":")
    if not sep or not event_id:
        raise HTTPException(status_code=422, detail="cursor 非法（需 <timestamp>:<event_id>）")
    try:
        return float(ts_part), event_id
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="cursor 非法（timestamp 不是数字）")


@router.get("/events", response_model=EventListResponse)
async def list_events(
    event_type: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=200),
    cursor: Optional[str] = None,
    request: Request = None,
    redis: Redis = Depends(get_redis),
):
    """获取事件列表（plaita#192：按事件受信 tenant_id 收窄，平台视角全量）。

    A2R24-05：事件按 ``(timestamp, event_id)`` 降序，``next_cursor`` 传回
    ``cursor`` 即可继续翻页（游标是排他键集，同时间戳的多条事件不会被跳过）。

    A2R47-04：``truncated`` 只是**已扫窗口内**的翻页信号；完整遍历的终止
    条件是合取 ``truncated=false`` **且** ``scan_truncated=false``。
    ``scan_truncated=true`` 时 ``total=-1`` 哨兵（匹配总数不可知）。
    """
    cursor_key = _parse_event_cursor(cursor)
    pattern = "plaita:event:*"
    keys, scan_truncated = _bounded_event_keys(redis, pattern)
    tenant = tenant_scope(request) if request is not None else None

    events_by_id: Dict[str, EventInfo] = {}
    # 同 id 两形并存时 canonical 优先（A69-02）：键形决定覆盖顺序，而不是
    # SCAN 的随机顺序。
    canonical_ids: set = set()
    skipped = 0
    for key in keys:
        if any(seg in key for seg in (":subscription:", ":processing:", ":type:", ":types:", ":channel:")):
            continue
        try:
            key_type = redis.type(key)
            if isinstance(key_type, bytes):
                key_type = key_type.decode()
            if key_type != "string":
                continue
            data = redis.get(key)
            if not data:
                continue
            info = json.loads(data)
            if event_type and info.get("event_type") != event_type:
                continue
            # 多租户：只认事件受信顶层 tenant_id（旧事件缺字段 → default）；
            # data 里的 tenant_id 是载荷，绝不作为归属判据（#191）。
            if tenant and _record_tenant(info) != tenant:
                continue
            event_id = info.get("event_id", key.split(":")[-1])
            is_canonical = key.startswith(_CANONICAL_EVENT_KEY_PREFIX)
            if event_id in events_by_id and not is_canonical:
                if event_id in canonical_ids:
                    continue
            events_by_id[event_id] = EventInfo(
                event_id=event_id,
                event_type=info.get("event_type", "unknown"),
                data=info.get("data", {}),
                timestamp=info.get("timestamp"),
                correlation_id=info.get("correlation_id"),
                source=info.get("source"),
            )
            if is_canonical:
                canonical_ids.add(event_id)
            else:
                canonical_ids.discard(event_id)
        except Exception:  # noqa: BLE001 - 单键读取失败计入 degraded
            # A2R19-04：读不到的键不得静默混入「空列表」。
            skipped += 1
            continue

    events = list(events_by_id.values())
    if cursor_key is not None:
        events = [
            event for event in events
            if (event.timestamp or 0.0, event.event_id) < cursor_key
        ]
    if skipped:
        logger.warning(
            "事件列表读取降级：%d/%d 个键读取失败（返回 %d 条，degraded=true）",
            skipped, len(keys), len(events),
        )

    events.sort(key=lambda e: (e.timestamp or 0.0, e.event_id), reverse=True)
    window_total = len(events)
    page = events[:limit]
    truncated = window_total > len(page)
    next_cursor = None
    if truncated and page:
        next_cursor = f"{page[-1].timestamp or 0.0}:{page[-1].event_id}"

    return EventListResponse(
        events=page,
        # A2R47-04：触界即哨兵——已扫前缀内的计数不得冒充匹配总数（naive
        # 客户端会把它当全集静默丢数据）；完整扫描时才是匹配总数。
        total=-1 if scan_truncated else window_total,
        returned=len(page),
        truncated=truncated,
        next_cursor=next_cursor,
        degraded=skipped > 0,
        skipped=skipped,
        scan_truncated=scan_truncated,
    )


@router.post("/events/publish")
async def publish_event(
    request: Request,
    body: PublishEventRequest,
    redis: Redis = Depends(get_redis),
):
    """
    手动发布事件到事件总线

    将事件写入 Redis，并通过 Pub/Sub 通知订阅者。

    多租户（plaita#191）：发布者租户由服务端 ``tenant_scope(required=True)``
    取自请求上下文，写进**受信顶层字段** ``tenant_id``；客户端在 ``data`` 里
    自报的 ``tenant_id`` 一律剥离——否则任意租户 editor 只要知道受害租户的
    execution_id，就能伪造租户给对方的挂起执行注入 resume（审批放行）。
    """
    tenant = tenant_scope(request, required=True)
    payload = dict(body.data or {})
    client_tenant = payload.pop("tenant_id", None)
    if client_tenant is not None:
        logger.warning(
            "事件发布剥离客户端自报租户（event_type=%s，发布者租户=%s，自报=%s）",
            body.event_type, tenant, client_tenant,
        )

    # A69-02：单次构造 Event，记录体与 zset score 共用同一个 timestamp。
    event = Event(
        event_id=str(uuid.uuid4()),
        event_type=body.event_type,
        data=payload,
        timestamp=time.time(),
        correlation_id=body.correlation_id,
        source="plaita-console",
        # 受信租户（服务端字段）：EventFilter 只认它决定执行状态命名空间
        tenant_id=tenant,
    )

    # 正典写：与引擎 ``RedisEventStorage.store_event`` 同键形/TTL（7d 默认），
    # 引擎 get_event/list_events 与 EventReconciler 回扫都可见。
    # 不再写 legacy ``plaita:event:{id}`` / ``plaita:event:type:{type}``
    # （旧键只读兼容、自然过期）。
    store_event_sync(redis, event, ttl=event_retention_ttl_seconds())
    event_json = event.model_dump_json()

    # 引擎 RedisEventBus 的订阅频道是 plaita:events:{type}。
    # legacy 频道 ``plaita:event:channel:{type}`` 保留一个 release（仓内无
    # 订阅者，但可能有外部消费者），下个 release 删除。
    try:
        redis.publish(f"plaita:events:{body.event_type}", event_json)
        redis.publish(f"plaita:event:channel:{body.event_type}", event_json)
    except Exception:
        # 已落盘：回扫可补偿通知丢失，但客户端必须看到失败（不静默成功）。
        logger.error(
            "事件已落盘但 Pub/Sub 通知失败（可经事件回扫补偿）: event_id=%s type=%s",
            event.event_id, body.event_type,
        )
        raise

    _audit(
        request, "event.publish", event.event_id,
        {
            "event_type": body.event_type,
            "correlation_id": body.correlation_id,
            "event_id": event.event_id,
        },
        tenant_id=tenant,
    )
    return {
        "success": True,
        "event_id": event.event_id,
        "event_type": body.event_type,
        "message": "事件已发布",
    }


# ============ 订阅 API ============

@router.get("/events/subscriptions", response_model=SubscriptionListResponse)
async def list_subscriptions(
    event_type: Optional[str] = None,
    flow_id: Optional[str] = None,
    request: Request = None,
    redis: Redis = Depends(get_redis),
):
    """获取事件订阅列表（租户视角按订阅内的 tenant_id 过滤）。

    A2R47-04：``scan_truncated=true`` 时 ``total=-1`` 哨兵——未扫完的
    keyspace 没有可信总数；完整遍历以 ``scan_truncated=false`` 为准。
    """
    tenant = tenant_scope(request) if request is not None else None

    pattern = "plaita:subscription:*"
    keys, scan_truncated = _bounded_event_keys(redis, pattern)

    subscriptions: List[SubscriptionInfo] = []
    skipped = 0
    for key in keys:
        try:
            key_type = redis.type(key)
            if isinstance(key_type, bytes):
                key_type = key_type.decode()
            if key_type != "string":
                continue  # 跳过引擎写的 type:/correlation:/flow: 集合键
            data = redis.get(key)
        except Exception:  # noqa: BLE001 - 单键读取失败计入 degraded
            skipped += 1
            continue
        if not data:
            continue
        try:
            info = json.loads(data)
            if event_type and info.get("event_type") != event_type:
                continue
            if flow_id and info.get("flow_id") != flow_id:
                continue
            # 订阅是流程级数据（流程归租户）；旧订阅无 tenant_id 字段视为 default
            if tenant and _record_tenant(info) != tenant:
                continue
            subscriptions.append(SubscriptionInfo(
                subscription_id=info.get("subscription_id", key.split(":")[-1]),
                event_type=info.get("event_type", "unknown"),
                filter_condition=info.get("filter_condition"),
                correlation_id=info.get("correlation_id"),
                flow_id=info.get("flow_id"),
                node_id=info.get("node_id"),
                created_at=info.get("created_at"),
                timeout=info.get("timeout"),
            ))
        except Exception:  # noqa: BLE001 - 单条解析失败计入 degraded
            skipped += 1
            continue

    if skipped:
        logger.warning(
            "订阅列表读取降级：%d/%d 个键读取失败（返回 %d 条，degraded=true）",
            skipped, len(keys), len(subscriptions),
        )

    subscriptions.sort(key=lambda s: s.created_at or 0, reverse=True)

    return SubscriptionListResponse(
        subscriptions=subscriptions,
        # A2R47-04：同 events——触界即哨兵，前缀计数不得冒充匹配总数。
        total=-1 if scan_truncated else len(subscriptions),
        degraded=skipped > 0,
        skipped=skipped,
        scan_truncated=scan_truncated,
    )


_SUBSCRIPTION_KEY_PREFIX = "plaita:subscription:data:"


def _load_subscription_record(redis: Redis, subscription_id: str) -> Dict[str, Any]:
    raw = redis.get(f"{_SUBSCRIPTION_KEY_PREFIX}{subscription_id}")
    if not raw:
        raise HTTPException(status_code=404, detail=f"订阅不存在: {subscription_id}")
    try:
        info = json.loads(raw)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=scrubbed_detail(logger, "event_data_parse_failed", "数据解析失败", exc),
        )
    return info if isinstance(info, dict) else {}


def _require_owned_subscription(info: Dict[str, Any], subscription_id: str, request: Request,
                                *, required: bool = False) -> None:
    """单条订阅归属校验（plaita#192）：跨租户按 404，不暴露存在性。

    与 ``schedules._get_schedule`` 同口径：先是 404（不存在）还是 404
    （无权看）对外不可区分。

    ``required=True`` 供写路径（DELETE）使用：``tenant_scope(required=True)``
    让会话型平台管理员全量视角在归属判断之前 403（A2R17-01 全量视角只读；
    A1 r110 a/P2）——否则 None 直接放行，平台管理员可按 id 盲删他租户订阅。
    """
    tenant = tenant_scope(request, required=required)
    if tenant is None:
        return
    if _record_tenant(info) != tenant:
        logger.warning(
            "拒绝跨租户访问事件订阅（订阅=%s，请求租户=%s）", subscription_id, tenant
        )
        raise HTTPException(status_code=404, detail=f"订阅不存在: {subscription_id}")


@router.get("/events/subscriptions/{subscription_id}", response_model=SubscriptionInfo)
async def get_subscription(
    subscription_id: str,
    request: Request,
    redis: Redis = Depends(get_redis),
):
    """获取订阅详情（引擎 RedisEventBus 写在 plaita:subscription:data:{id}）"""
    info = _load_subscription_record(redis, subscription_id)
    _require_owned_subscription(info, subscription_id, request)
    try:
        return SubscriptionInfo(**info)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=scrubbed_detail(logger, "event_data_parse_failed", "数据解析失败", e),
        )


@router.delete("/events/subscriptions/{subscription_id}")
async def delete_subscription(
    subscription_id: str,
    request: Request,
    redis: Redis = Depends(get_redis),
    body: Optional[EmptyRequestModel] = None,
):
    """删除事件订阅（含引擎侧的类型/关联/流程索引）。跨租户按 404 且不删除。"""
    # 写路径先解析租户（A2R17-01 全量视角只读；A1 r110 a/P2）：403 发生在读
    # 记录之前，全量视角对「存在 / 不存在」的订阅得到同一响应（无存在性 oracle）。
    tenant_scope(request, required=True)
    info = _load_subscription_record(redis, subscription_id)
    _require_owned_subscription(info, subscription_id, request, required=True)
    key = f"{_SUBSCRIPTION_KEY_PREFIX}{subscription_id}"
    pipe = redis.pipeline()
    pipe.delete(key)
    event_type = info.get("event_type")
    if event_type:
        pipe.srem(f"plaita:subscription:type:{event_type}", subscription_id)
    if info.get("correlation_id"):
        pipe.srem(f"plaita:subscription:correlation:{info['correlation_id']}", subscription_id)
    if info.get("flow_id"):
        pipe.srem(f"plaita:subscription:flow:{info['flow_id']}", subscription_id)
    # A69-03：补齐引擎侧 ``node:`` 索引——console 删除此前漏掉这一桶，
    # 冷桶孤儿只能等 janitor 兜底；与引擎 delete_subscription / 租户 purge
    # 的四索引清理对齐。
    if info.get("node_id"):
        pipe.srem(f"plaita:subscription:node:{info['node_id']}", subscription_id)
    pipe.execute()
    _audit(
        request, "event.subscription_delete", subscription_id,
        {
            "event_type": event_type,
            "flow_id": info.get("flow_id"),
            "correlation_id": info.get("correlation_id"),
        },
        tenant_id=_record_tenant(info),
    )
    return {"success": True, "message": "订阅已删除"}
