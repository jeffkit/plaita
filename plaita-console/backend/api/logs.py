"""
日志查看 API
提供日志查询和实时日志流接口
"""
import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from redis import Redis
from sse_starlette.sse import EventSourceResponse

try:
    from .redis_scan import scan_keys
except ImportError:  # 平铺布局（cwd=backend）运行时
    from api.redis_scan import scan_keys  # type: ignore

try:
    from .sse_redis import SSE_REDIS_RETRY_SECONDS, get_sse_async_redis
except ImportError:  # 平铺布局（cwd=backend）运行时
    from api.sse_redis import (  # type: ignore
        SSE_REDIS_RETRY_SECONDS,
        get_sse_async_redis,
    )

try:
    from ..auth import tenant_scope
except ImportError:  # 平铺布局（cwd=backend）运行时
    from auth import tenant_scope  # type: ignore

logger = logging.getLogger(__name__)

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


def get_redis_or_none(request: Request) -> Redis:
    """本地单机模式返回 None——list/instance/stats 有本地分支。"""
    return request.app.state.redis


def _entry_tenant(entry_data: Any) -> str:
    """日志条目归属租户（plaita#192）：条目内 ``tenant_id``，旧条目视为 default。

    ``plaita:logs:*`` Stream 与 Pub/Sub 频道都是平台级共享的（键/频道不含租户
    段，见 log_handler.py），租户边界必须逐条目判——不能靠 key 或频道前缀。
    """
    tenant = entry_data.get("tenant_id") or "default"
    if isinstance(tenant, bytes):
        tenant = tenant.decode()
    return tenant


# 有界扫描的单请求键预算（#202）：日志每个键还要 XREVRANGE，键预算按端点
# 读取代价收紧——触界返回部分结果并记 warning，不阻塞共享 Redis。
_LOG_LIST_MAX_KEYS = 2000
_LOG_STATS_MAX_KEYS = 1000


def _bounded_log_keys(redis: Redis, pattern: str, max_keys: int) -> List[str]:
    """有界 SCAN 代替 ``KEYS``（#202）：单条命令只扫一页，触界 fail-safe。"""
    return scan_keys(redis, pattern, max_keys=max_keys).keys


# ============ API 端点 ============

def _local_store(request: Request):
    """本地单机模式 → (flow_store 模块, True)；集群模式 → (None, False)。"""
    if getattr(request.app.state, "local_mode", False):
        try:
            from ..services import flow_store
        except ImportError:
            from services import flow_store  # type: ignore
        return flow_store, True
    return None, False


# ============ API 端点 ============

@router.get("/logs", response_model=LogListResponse)
async def list_logs(
    service_type: Optional[str] = None,
    instance_id: Optional[str] = None,
    level: Optional[str] = None,
    limit: int = Query(default=100, le=1000),
    request: Request = None,
    redis: Redis = Depends(get_redis_or_none)
):
    """
    获取日志列表
    
    - **service_type**: 按服务类型筛选
    - **instance_id**: 按实例 ID 筛选
    - **level**: 按日志级别筛选
    - **limit**: 返回数量限制
    """
    # 本地单机模式：读 SQLite 执行日志
    local_fs, is_local = _local_store(request) if request else (None, False)
    if is_local:
        rows = local_fs.list_local_logs(level=level, execution_id=instance_id, limit=limit,
                                        tenant_id=tenant_scope(request))
        return LogListResponse(
            logs=[
                LogEntry(
                    timestamp=r["timestamp"],
                    level=r["level"],
                    service_type=r["service_type"],
                    instance_id=r["instance_id"],
                    message=r["message"],
                )
                for r in rows
            ],
            total=len(rows),
        )

    # 构建 Stream key 模式：instance_id 一律不参与 key 拼接，只作条目过滤
    # （#102）。发布侧 key 本就按 service_type:instance_id 分流
    # （plaita/server/log_handler.py），日志页的深链可能只有 instance_id
    # （历史链接、Cluster 卡片深链），仅凭 instance_id 拼不出 key——曾因此
    # 退化为全量键且从不按条目收窄，实例筛选静默失效。
    if service_type:
        pattern = f"plaita:logs:{service_type}:*"
    else:
        pattern = "plaita:logs:*"
    keys = _bounded_log_keys(redis, pattern, _LOG_LIST_MAX_KEYS)

    logs = []
    tenant = tenant_scope(request)
    tenant_id_filter = tenant if tenant else None

    for key in keys:
        key_str = key if isinstance(key, str) else key.decode()

        try:
            # 从 Redis Stream 读取日志
            entries = redis.xrevrange(key_str, count=limit)

            for entry_id, entry_data in entries:
                try:
                    # 多租户：日志键平台级共享，按条目内 tenant_id 过滤
                    # （旧条目无该字段，视为 default 租户）
                    if tenant_id_filter and _entry_tenant(entry_data) != tenant_id_filter:
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

                    # 实例筛选：key 择取已不含 instance_id，必须按条目字段
                    # 收窄，与 /logs/instance/{instance_id} 语义对齐（#102）
                    if instance_id and log_entry.instance_id != instance_id:
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
    request: Request = None,
    redis: Redis = Depends(get_redis_or_none)
):
    """
    获取指定服务实例的日志
    
    - **instance_id**: 服务实例 ID
    - **level**: 按日志级别筛选
    - **limit**: 返回数量限制
    - **order**: 排序方式 (asc: 时间正序, desc: 时间倒序)
    """
    logs = []

    # 本地单机模式：instance_id 即本地执行 ID
    local_fs, is_local = _local_store(request) if request else (None, False)
    if is_local:
        rows = local_fs.list_local_logs(level=level, execution_id=instance_id, limit=limit,
                                        tenant_id=tenant_scope(request))
        if order == "asc":
            rows = list(reversed(rows))
        return LogListResponse(
            logs=[
                LogEntry(
                    timestamp=r["timestamp"],
                    level=r["level"],
                    service_type=r["service_type"],
                    instance_id=r["instance_id"],
                    message=r["message"],
                )
                for r in rows
            ],
            total=len(rows),
        )

    # 查找所有可能包含该实例日志的 Stream key
    pattern = "plaita:logs:*"
    keys = _bounded_log_keys(redis, pattern, _LOG_LIST_MAX_KEYS)
    # 多租户（plaita#192）：实例 id 可能被他租户猜中/从别处取得，逐条目按
    # tenant_id 收窄——不能只按 instance_id 匹配。
    tenant = tenant_scope(request)
    
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
                    if tenant and _entry_tenant(entry_data) != tenant:
                        continue
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
    request: Request = None,
    redis: Redis = Depends(get_redis_or_none)
):
    """
    获取日志统计信息
    
    返回按服务类型和日志级别分组的日志数量统计
    """
    # 本地单机模式：统计本地执行日志
    local_fs, is_local = _local_store(request) if request else (None, False)
    if is_local:
        summary = local_fs.local_log_stats(tenant_id=tenant_scope(request))
        entries = [
            LogStatsEntry(service_type=svc, level=level, count=count)
            for svc, info in summary["stats"].items()
            for level, count in info["levels"].items()
        ]
        return LogStatsResponse(stats=entries, total_logs=summary["total"])

    stats_dict: Dict[str, Dict[str, int]] = {}  # {service_type: {level: count}}
    total_logs = 0
    # 多租户（plaita#192）：Stream 键与长度都是平台级的——租户视角必须逐条目
    # 统计（精确），不能把 xlen 当成租户总量、也不能采平台样本估算。
    tenant = tenant_scope(request)
    
    # 遍历所有日志 Stream
    pattern = "plaita:logs:*"
    keys = _bounded_log_keys(redis, pattern, _LOG_STATS_MAX_KEYS)
    
    for key in keys:
        key_str = key if isinstance(key, str) else key.decode()
        
        try:
            # 从 key 解析服务类型
            parts = key_str.split(":")
            if len(parts) >= 3:
                service_type = parts[2]
            else:
                service_type = "unknown"

            if tenant:
                # 精确统计：只累计本租户条目（日志 Stream 由 log_handler 的
                # maxlen 约束，逐条读取有界；平台视角仍走下方采样快路径）
                level_counts: Dict[str, int] = {}
                matched = 0
                for _, entry_data in redis.xrange(key_str):
                    if _entry_tenant(entry_data) != tenant:
                        continue
                    matched += 1
                    level = entry_data.get("level", "INFO")
                    if isinstance(level, bytes):
                        level = level.decode()
                    level_counts[level] = level_counts.get(level, 0) + 1
                if not matched:
                    continue
                total_logs += matched
                target = stats_dict.setdefault(service_type, {})
                for level, count in level_counts.items():
                    target[level] = target.get(level, 0) + count
                continue

            # 获取 Stream 长度
            length = redis.xlen(key_str)
            total_logs += length
            
            # 采样一些日志来统计级别分布
            entries = redis.xrevrange(key_str, count=100)
            level_counts = {}
            
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

    多租户（plaita#192）：日志频道是平台级通配订阅（``plaita:logs:stream:*``
    等），频道本身不含租户段——漏过滤时租户 A 的流会收到全平台日志。这里在
    事件生成器里逐条目按 ``tenant_id`` 收窄；平台视角（无租户上下文）保持全量。

    非阻塞消费（#259）：pubsub 必须是 ``redis.asyncio`` 客户端（实现在
    ``sse_redis.py``，与 execution SSE 的 C4-4 修法同款）。历史实现在 async
    生成器里调同步 ``get_message(timeout=1.0)``，一次空闲等待就独占事件循环
    1s——单条 viewer 连接即可让 console 所有请求（含 /health）卡顿。``redis``
    依赖只用于「本地单机模式 503」的可用性闸门，不参与流式读取。
    """
    tenant = tenant_scope(request)

    async def event_generator():
        """事件生成器"""
        # 订阅日志通道（载荷语义与历史实现逐字一致）
        if service_type and instance_id:
            channel = f"plaita:logs:stream:{service_type}:{instance_id}"
        elif service_type:
            channel = f"plaita:logs:stream:{service_type}:*"
        else:
            channel = "plaita:logs:stream:*"

        pubsub = None
        try:
            try:
                pubsub = get_sse_async_redis(request).pubsub()
                if "*" in channel:
                    await pubsub.psubscribe(channel)
                else:
                    await pubsub.subscribe(channel)
            except Exception as exc:
                # 订阅失败不静默：给运维留可观测痕迹后结束本条流
                logger.warning("日志 SSE 订阅失败 %s: %s", channel, exc)
                return

            while True:
                try:
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=1.0
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # Redis 抖动不终结 SSE：短暂退避后继续消费
                    logger.warning("日志 SSE 消费异常（保持连接）%s: %s", channel, exc)
                    await asyncio.sleep(SSE_REDIS_RETRY_SECONDS)
                    continue

                if message and message["type"] in ("message", "pmessage"):
                    try:
                        data = json.loads(message["data"])

                        # 租户筛选：频道只有平台级通配，必须逐条目判归属
                        if tenant and _entry_tenant(data) != tenant:
                            continue

                        # 级别筛选
                        if level and data.get("level") != level:
                            continue

                        # 实例筛选：仅给 instance_id 时频道是通配的，须按
                        # 条目 instance_id 收窄（#102）；service_type+
                        # instance_id 齐备时走精确频道，天然已收窄
                        if instance_id and data.get("instance_id") != instance_id:
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
            if pubsub is not None:
                try:
                    if "*" in channel:
                        await pubsub.punsubscribe(channel)
                    else:
                        await pubsub.unsubscribe(channel)
                except Exception:
                    pass
                try:
                    await pubsub.aclose()
                except Exception:
                    pass

    return EventSourceResponse(event_generator())

