"""
任务队列查看 API
提供队列状态查询接口
"""
import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from redis import Redis

router = APIRouter()


# ============ 数据模型 ============

class QueueInfo(BaseModel):
    """队列信息"""
    name: str = Field(..., description="队列名称")
    length: int = Field(..., description="队列长度")
    queue_type: str = Field(default="list", description="队列类型")


class QueueListResponse(BaseModel):
    """队列列表响应"""
    queues: List[QueueInfo]
    total: int


class QueueTask(BaseModel):
    """队列任务"""
    index: int = Field(..., description="任务索引")
    data: Dict[str, Any] = Field(..., description="任务数据")


class QueueDetailResponse(BaseModel):
    """队列详情响应"""
    name: str
    length: int
    tasks: List[QueueTask]


# ============ 工具函数 ============

def get_redis(request: Request) -> Redis:
    """获取 Redis 客户端"""
    return request.app.state.redis


# ============ 已知队列列表 ============

KNOWN_QUEUES = [
    "plaita:flow:queue",           # 流程任务队列
    "plaita:delay:queue",          # 延迟任务队列
    "plaita:redis_queue:*",        # Redis 队列服务
    "plaita:kafka_queue:*",        # Kafka 队列服务
]


# ============ API 端点 ============

@router.get("/queues", response_model=QueueListResponse)
async def list_queues(
    redis: Redis = Depends(get_redis)
):
    """
    获取所有队列概览
    """
    queues = []
    
    # 检查已知队列。plaita:flow:queue 现在是 Redis Stream（FlowWorker 消费组
    # 消费），长度必须用 XLEN；对 Stream 调 LLEN 会 WRONGTYPE——按实际类型取。
    def _key_type(key: str) -> str:
        t = redis.type(key)
        return t.decode() if isinstance(t, bytes) else t

    def _queue_length(key: str) -> int:
        t = _key_type(key)
        if t == "stream":
            return redis.xlen(key)
        if t == "list":
            return redis.llen(key)
        if t == "zset":
            return redis.zcard(key)
        return 0

    for pattern in KNOWN_QUEUES:
        keys = redis.keys(pattern)
        if "*" not in pattern and not keys:
            # 键尚不存在：保留已知队列的零值行，页面不缺行
            queues.append(QueueInfo(
                name=pattern,
                length=0,
                queue_type="stream" if pattern == "plaita:flow:queue" else "list",
            ))
            continue
        for key in keys:
            key_str = key if isinstance(key, str) else key.decode()
            length = _queue_length(key_str)
            if length > 0 or key_str == "plaita:flow:queue":
                queues.append(QueueInfo(
                    name=key_str,
                    length=length,
                    queue_type=_key_type(key_str),
                ))

    return QueueListResponse(
        queues=queues,
        total=len(queues)
    )


@router.get("/queues/{queue_name:path}", response_model=QueueDetailResponse)
async def get_queue(
    queue_name: str,
    start: int = 0,
    count: int = 20,
    redis: Redis = Depends(get_redis)
):
    """
    获取队列详情
    
    - **queue_name**: 队列名称
    - **start**: 起始索引
    - **count**: 获取数量
    """
    key_type = redis.type(queue_name)
    if isinstance(key_type, bytes):
        key_type = key_type.decode()

    tasks = []
    if key_type == "stream":
        # Stream 队列（plaita:flow:queue）：XRANGE 取消息，payload 字段即任务 JSON
        length = redis.xlen(queue_name)
        entries = redis.xrange(queue_name, min=start, count=count)
        for i, (msg_id, fields) in enumerate(entries):
            if isinstance(msg_id, bytes):
                msg_id = msg_id.decode()
            payload = None
            for k, v in fields.items():
                k_str = k.decode() if isinstance(k, bytes) else k
                if k_str == "payload":
                    payload = v.decode() if isinstance(v, bytes) else v
                    break
            try:
                data = json.loads(payload)
            except Exception:
                data = {"raw": str(payload)}
            data = {"_msg_id": msg_id, **data}
            tasks.append(QueueTask(index=i, data=data))
    elif key_type == "list":
        length = redis.llen(queue_name)
        items = redis.lrange(queue_name, start, start + count - 1)
        for i, item in enumerate(items):
            try:
                item_str = item if isinstance(item, str) else item.decode()
                data = json.loads(item_str)
            except Exception:
                data = {"raw": str(item)}
            tasks.append(QueueTask(
                index=start + i,
                data=data
            ))
    else:
        length = 0

    return QueueDetailResponse(
        name=queue_name,
        length=length,
        tasks=tasks
    )

