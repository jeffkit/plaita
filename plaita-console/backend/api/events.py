"""
事件系统管理 API
提供事件订阅查看、事件发布、事件列表等接口

多租户边界（plaita#191 / #192）：
- 发布：租户由服务端从请求上下文绑定，写进事件**受信顶层字段** ``tenant_id``；
  客户端在 ``data`` 里自报的 tenant_id 一律剥离（data 是载荷，不是授权来源）。
- 读/删：事件与订阅按记录里的受信 ``tenant_id`` 收窄；单条资源不匹配按 404
  返回（不暴露存在性）。平台视角（``tenant_scope() is None``）保持全量。
"""
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from redis import Redis

try:
    from ..auth import tenant_scope
except ImportError:  # 平铺布局（cwd=backend）运行时
    from auth import tenant_scope  # type: ignore

logger = logging.getLogger(__name__)

router = APIRouter()


def _record_tenant(info: Dict[str, Any]) -> str:
    """记录归属租户：受信顶层字段，缺失（升级前旧记录）视为 default。"""
    return info.get("tenant_id") or "default"


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
    total: int


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
    total: int


class PublishEventRequest(BaseModel):
    event_type: str = Field(..., description="事件类型")
    data: Dict[str, Any] = Field(default_factory=dict, description="事件数据")
    correlation_id: Optional[str] = Field(None, description="关联 ID")


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


def _scan_keys(redis: Redis, pattern: str, count: int = 100) -> List[str]:
    """使用 SCAN 替代 KEYS 获取匹配的键"""
    keys = []
    cursor = 0
    while True:
        cursor, batch = redis.scan(cursor=cursor, match=pattern, count=count)
        keys.extend(k if isinstance(k, str) else k.decode() for k in batch)
        if cursor == 0:
            break
    return keys


# ============ 事件 API ============

@router.get("/events", response_model=EventListResponse)
async def list_events(
    event_type: Optional[str] = None,
    limit: int = Query(default=50, le=200),
    request: Request = None,
    redis: Redis = Depends(get_redis),
):
    """获取事件列表（plaita#192：按事件受信 tenant_id 收窄，平台视角全量）"""
    pattern = "plaita:event:*"
    keys = _scan_keys(redis, pattern)
    tenant = tenant_scope(request) if request is not None else None

    events: List[EventInfo] = []
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
            events.append(EventInfo(
                event_id=info.get("event_id", key.split(":")[-1]),
                event_type=info.get("event_type", "unknown"),
                data=info.get("data", {}),
                timestamp=info.get("timestamp"),
                correlation_id=info.get("correlation_id"),
                source=info.get("source"),
            ))
        except Exception:
            continue

    events.sort(key=lambda e: e.timestamp or 0, reverse=True)
    events = events[:limit]

    return EventListResponse(events=events, total=len(events))


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

    event_id = str(uuid.uuid4())
    event_data = {
        "event_id": event_id,
        "event_type": body.event_type,
        "data": payload,
        "timestamp": time.time(),
        "correlation_id": body.correlation_id,
        "source": "plaita-console",
        # 受信租户（服务端字段）：EventFilter 只认它决定执行状态命名空间
        "tenant_id": tenant,
    }

    event_key = f"plaita:event:{event_id}"
    redis.set(event_key, json.dumps(event_data), ex=86400)

    type_key = f"plaita:event:type:{body.event_type}"
    redis.zadd(type_key, {event_id: time.time()})
    redis.expire(type_key, 86400)

    # 引擎 RedisEventBus 的订阅频道是 plaita:events:{type}——
    # 历史上只发 plaita:event:channel:{type}，引擎侧永远收不到
    redis.publish(f"plaita:events:{body.event_type}", json.dumps(event_data))
    redis.publish(f"plaita:event:channel:{body.event_type}", json.dumps(event_data))

    return {
        "success": True,
        "event_id": event_id,
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
    """获取事件订阅列表（租户视角按订阅内的 tenant_id 过滤）"""
    tenant = tenant_scope(request) if request is not None else None

    pattern = "plaita:subscription:*"
    keys = _scan_keys(redis, pattern)

    subscriptions: List[SubscriptionInfo] = []
    for key in keys:
        try:
            key_type = redis.type(key)
            if isinstance(key_type, bytes):
                key_type = key_type.decode()
            if key_type != "string":
                continue  # 跳过引擎写的 type:/correlation:/flow: 集合键
            data = redis.get(key)
        except Exception:
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
        except Exception:
            continue

    subscriptions.sort(key=lambda s: s.created_at or 0, reverse=True)

    return SubscriptionListResponse(subscriptions=subscriptions, total=len(subscriptions))


_SUBSCRIPTION_KEY_PREFIX = "plaita:subscription:data:"


def _load_subscription_record(redis: Redis, subscription_id: str) -> Dict[str, Any]:
    raw = redis.get(f"{_SUBSCRIPTION_KEY_PREFIX}{subscription_id}")
    if not raw:
        raise HTTPException(status_code=404, detail=f"订阅不存在: {subscription_id}")
    try:
        info = json.loads(raw)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"数据解析失败: {exc}")
    return info if isinstance(info, dict) else {}


def _require_owned_subscription(info: Dict[str, Any], subscription_id: str, request: Request) -> None:
    """单条订阅归属校验（plaita#192）：跨租户按 404，不暴露存在性。

    与 ``schedules._get_schedule`` 同口径：先是 404（不存在）还是 404
    （无权看）对外不可区分。
    """
    tenant = tenant_scope(request)
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
        raise HTTPException(status_code=500, detail=f"数据解析失败: {e}")


@router.delete("/events/subscriptions/{subscription_id}")
async def delete_subscription(
    subscription_id: str,
    request: Request,
    redis: Redis = Depends(get_redis),
):
    """删除事件订阅（含引擎侧的类型/关联/流程索引）。跨租户按 404 且不删除。"""
    info = _load_subscription_record(redis, subscription_id)
    _require_owned_subscription(info, subscription_id, request)
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
    pipe.execute()
    return {"success": True, "message": "订阅已删除"}
