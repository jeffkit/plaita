"""
事件系统管理 API
提供事件订阅查看、事件发布、事件列表等接口
"""
import json
import time
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from redis import Redis

router = APIRouter()


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
    return request.app.state.redis


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
    redis: Redis = Depends(get_redis),
):
    """获取事件列表"""
    pattern = "plaita:event:*"
    keys = _scan_keys(redis, pattern)

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
    request: PublishEventRequest,
    redis: Redis = Depends(get_redis),
):
    """
    手动发布事件到事件总线

    将事件写入 Redis，并通过 Pub/Sub 通知订阅者
    """
    event_id = str(uuid.uuid4())
    event_data = {
        "event_id": event_id,
        "event_type": request.event_type,
        "data": request.data,
        "timestamp": time.time(),
        "correlation_id": request.correlation_id,
        "source": "plaita-console",
    }

    event_key = f"plaita:event:{event_id}"
    redis.set(event_key, json.dumps(event_data), ex=86400)

    type_key = f"plaita:event:type:{request.event_type}"
    redis.zadd(type_key, {event_id: time.time()})
    redis.expire(type_key, 86400)

    channel = f"plaita:event:channel:{request.event_type}"
    redis.publish(channel, json.dumps(event_data))

    return {
        "success": True,
        "event_id": event_id,
        "event_type": request.event_type,
        "message": "事件已发布",
    }


# ============ 订阅 API ============

@router.get("/events/subscriptions", response_model=SubscriptionListResponse)
async def list_subscriptions(
    event_type: Optional[str] = None,
    flow_id: Optional[str] = None,
    redis: Redis = Depends(get_redis),
):
    """获取事件订阅列表"""
    pattern = "plaita:subscription:*"
    keys = _scan_keys(redis, pattern)

    subscriptions: List[SubscriptionInfo] = []
    for key in keys:
        data = redis.get(key)
        if not data:
            continue
        try:
            info = json.loads(data)
            if event_type and info.get("event_type") != event_type:
                continue
            if flow_id and info.get("flow_id") != flow_id:
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


@router.get("/events/subscriptions/{subscription_id}", response_model=SubscriptionInfo)
async def get_subscription(
    subscription_id: str,
    redis: Redis = Depends(get_redis),
):
    """获取订阅详情"""
    key = f"plaita:subscription:{subscription_id}"
    data = redis.get(key)
    if not data:
        raise HTTPException(status_code=404, detail=f"订阅不存在: {subscription_id}")
    try:
        info = json.loads(data)
        return SubscriptionInfo(**info)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"数据解析失败: {e}")


@router.delete("/events/subscriptions/{subscription_id}")
async def delete_subscription(
    subscription_id: str,
    redis: Redis = Depends(get_redis),
):
    """删除事件订阅"""
    key = f"plaita:subscription:{subscription_id}"
    if not redis.exists(key):
        raise HTTPException(status_code=404, detail=f"订阅不存在: {subscription_id}")
    redis.delete(key)
    return {"success": True, "message": "订阅已删除"}
