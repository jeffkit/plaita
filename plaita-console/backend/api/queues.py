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
    
    # 检查已知队列
    for pattern in KNOWN_QUEUES:
        if "*" in pattern:
            # 模式匹配
            keys = redis.keys(pattern)
            for key in keys:
                key_str = key if isinstance(key, str) else key.decode()
                length = redis.llen(key_str)
                if length > 0 or key_str == "plaita:flow:queue":
                    queues.append(QueueInfo(
                        name=key_str,
                        length=length
                    ))
        else:
            # 精确匹配
            length = redis.llen(pattern)
            queues.append(QueueInfo(
                name=pattern,
                length=length
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
    # 获取队列长度
    length = redis.llen(queue_name)
    
    # 获取任务列表
    items = redis.lrange(queue_name, start, start + count - 1)
    
    tasks = []
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
    
    return QueueDetailResponse(
        name=queue_name,
        length=length,
        tasks=tasks
    )

