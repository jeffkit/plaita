"""
日志查看 API
提供日志查询和实时日志流接口
"""
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from redis import Redis
from sse_starlette.sse import EventSourceResponse

router = APIRouter()


# ============ 数据模型 ============

class LogEntry(BaseModel):
    """日志条目"""
    timestamp: str = Field(..., description="时间戳")
    level: str = Field(..., description="日志级别")
    service_type: Optional[str] = Field(None, description="服务类型")
    instance_id: Optional[str] = Field(None, description="实例 ID")
    message: str = Field(..., description="日志消息")
    context: Optional[Dict[str, Any]] = Field(None, description="上下文信息")


class LogListResponse(BaseModel):
    """日志列表响应"""
    logs: List[LogEntry]
    total: int


class LogStatsEntry(BaseModel):
    """日志统计条目"""
    service_type: str
    instance_id: Optional[str] = None
    level: str
    count: int


class LogStatsResponse(BaseModel):
    """日志统计响应"""
    stats: List[LogStatsEntry]
    total_logs: int


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


# ============ API 端点 ============

@router.get("/logs", response_model=LogListResponse)
async def list_logs(
    service_type: Optional[str] = None,
    instance_id: Optional[str] = None,
    level: Optional[str] = None,
    limit: int = Query(default=100, le=1000),
    redis: Redis = Depends(get_redis)
):
    """
    获取日志列表
    
    - **service_type**: 按服务类型筛选
    - **instance_id**: 按实例 ID 筛选
    - **level**: 按日志级别筛选
    - **limit**: 返回数量限制
    """
    # 构建 Stream key 模式
    if service_type and instance_id:
        pattern = f"plaita:logs:{service_type}:{instance_id}"
        keys = [pattern]
    elif service_type:
        pattern = f"plaita:logs:{service_type}:*"
        keys = redis.keys(pattern)
    else:
        pattern = "plaita:logs:*"
        keys = redis.keys(pattern)
    
    logs = []
    
    for key in keys:
        key_str = key if isinstance(key, str) else key.decode()
        
        try:
            # 从 Redis Stream 读取日志
            entries = redis.xrevrange(key_str, count=limit)
            
            for entry_id, entry_data in entries:
                try:
                    log_entry = LogEntry(
                        timestamp=entry_data.get("timestamp", entry_id),
                        level=entry_data.get("level", "INFO"),
                        service_type=entry_data.get("service_type"),
                        instance_id=entry_data.get("instance_id"),
                        message=entry_data.get("message", ""),
                        context=json.loads(entry_data.get("context", "{}"))
                    )
                    
                    # 级别筛选
                    if level and log_entry.level != level:
                        continue
                    
                    logs.append(log_entry)
                except Exception:
                    continue
        except Exception:
            # 如果不是 Stream，跳过
            continue
    
    # 按时间排序
    logs.sort(key=lambda x: x.timestamp, reverse=True)
    
    # 限制数量
    logs = logs[:limit]
    
    return LogListResponse(
        logs=logs,
        total=len(logs)
    )


@router.get("/logs/instance/{instance_id}", response_model=LogListResponse)
async def get_instance_logs(
    instance_id: str,
    level: Optional[str] = None,
    limit: int = Query(default=100, le=1000),
    order: str = Query(default="desc", description="排序方式: asc 或 desc"),
    redis: Redis = Depends(get_redis)
):
    """
    获取指定服务实例的日志
    
    - **instance_id**: 服务实例 ID
    - **level**: 按日志级别筛选
    - **limit**: 返回数量限制
    - **order**: 排序方式 (asc: 时间正序, desc: 时间倒序)
    """
    logs = []
    
    # 查找所有可能包含该实例日志的 Stream key
    pattern = "plaita:logs:*"
    keys = redis.keys(pattern)
    
    for key in keys:
        key_str = key if isinstance(key, str) else key.decode()
        
        try:
            # 从 Redis Stream 读取日志
            if order == "desc":
                entries = redis.xrevrange(key_str, count=limit * 2)  # 多读一些以便筛选
            else:
                entries = redis.xrange(key_str, count=limit * 2)
            
            for entry_id, entry_data in entries:
                try:
                    entry_instance_id = entry_data.get("instance_id")
                    if entry_instance_id != instance_id:
                        continue
                    
                    log_entry = LogEntry(
                        timestamp=entry_data.get("timestamp", entry_id),
                        level=entry_data.get("level", "INFO"),
                        service_type=entry_data.get("service_type"),
                        instance_id=entry_data.get("instance_id"),
                        message=entry_data.get("message", ""),
                        context=json.loads(entry_data.get("context", "{}"))
                    )
                    
                    # 级别筛选
                    if level and log_entry.level != level:
                        continue
                    
                    logs.append(log_entry)
                except Exception:
                    continue
        except Exception:
            continue
    
    # 按时间排序
    logs.sort(key=lambda x: x.timestamp, reverse=(order == "desc"))
    
    # 限制数量
    logs = logs[:limit]
    
    return LogListResponse(
        logs=logs,
        total=len(logs)
    )


@router.get("/logs/stats", response_model=LogStatsResponse)
async def get_log_stats(
    redis: Redis = Depends(get_redis)
):
    """
    获取日志统计信息
    
    返回按服务类型和日志级别分组的日志数量统计
    """
    stats_dict: Dict[str, Dict[str, int]] = {}  # {service_type: {level: count}}
    total_logs = 0
    
    # 遍历所有日志 Stream
    pattern = "plaita:logs:*"
    keys = redis.keys(pattern)
    
    for key in keys:
        key_str = key if isinstance(key, str) else key.decode()
        
        try:
            # 获取 Stream 长度
            length = redis.xlen(key_str)
            total_logs += length
            
            # 从 key 解析服务类型
            parts = key_str.split(":")
            if len(parts) >= 3:
                service_type = parts[2]
            else:
                service_type = "unknown"
            
            # 采样一些日志来统计级别分布
            entries = redis.xrevrange(key_str, count=100)
            level_counts: Dict[str, int] = {}
            
            for _, entry_data in entries:
                level = entry_data.get("level", "INFO")
                if isinstance(level, bytes):
                    level = level.decode()
                level_counts[level] = level_counts.get(level, 0) + 1
            
            # 按比例估算总数
            if entries:
                ratio = length / len(entries)
                for level, count in level_counts.items():
                    if service_type not in stats_dict:
                        stats_dict[service_type] = {}
                    estimated = int(count * ratio)
                    stats_dict[service_type][level] = stats_dict[service_type].get(level, 0) + estimated
        except Exception:
            continue
    
    # 转换为响应格式
    stats = []
    for service_type, level_counts in stats_dict.items():
        for level, count in level_counts.items():
            stats.append(LogStatsEntry(
                service_type=service_type,
                level=level,
                count=count
            ))
    
    return LogStatsResponse(
        stats=stats,
        total_logs=total_logs
    )


@router.get("/logs/stream")
async def stream_logs(
    request: Request,
    service_type: Optional[str] = None,
    instance_id: Optional[str] = None,
    level: Optional[str] = None,
    redis: Redis = Depends(get_redis)
):
    """
    SSE 端点：实时日志流
    
    - **service_type**: 按服务类型筛选
    - **instance_id**: 按实例 ID 筛选
    - **level**: 按日志级别筛选
    """
    async def event_generator():
        """事件生成器"""
        pubsub = redis.pubsub()
        
        # 订阅日志通道
        if service_type and instance_id:
            channel = f"plaita:logs:stream:{service_type}:{instance_id}"
        elif service_type:
            channel = f"plaita:logs:stream:{service_type}:*"
        else:
            channel = "plaita:logs:stream:*"
        
        if "*" in channel:
            pubsub.psubscribe(channel)
        else:
            pubsub.subscribe(channel)
        
        try:
            while True:
                message = pubsub.get_message(timeout=1.0)
                if message and message["type"] in ("message", "pmessage"):
                    try:
                        data = json.loads(message["data"])
                        
                        # 级别筛选
                        if level and data.get("level") != level:
                            continue
                        
                        yield {
                            "event": "log",
                            "data": json.dumps(data)
                        }
                    except Exception:
                        continue
                
                # 检查客户端是否断开
                if await request.is_disconnected():
                    break
        finally:
            if "*" in channel:
                pubsub.punsubscribe(channel)
            else:
                pubsub.unsubscribe(channel)
            pubsub.close()
    
    return EventSourceResponse(event_generator())

