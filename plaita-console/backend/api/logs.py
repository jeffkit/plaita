"""
日志查看 API
提供日志查询和实时日志流接口
"""
import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from redis import Redis
from sse_starlette.sse import EventSourceResponse

try:
    from .redis_scan import escape_glob, scan_keys
except ImportError:  # 平铺布局（cwd=backend）运行时
    from api.redis_scan import escape_glob, scan_keys  # type: ignore

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
    # A2R19-04：部分键/条目读取失败时显式标注——空列表不等于「没有日志」。
    degraded: bool = False
    skipped: int = 0
    # A2R20-04：全局条目/墙钟预算触界（或键扫描触界）时只是部分结果。
    truncated: bool = False


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
    # A2R20-04：条目/墙钟预算或键扫描触界，统计只是部分结果（有租户过滤时
    # 计数是下界）。
    truncated: bool = False
    # A2R24-03：平台视角按每键最新 N 条采样再按 xlen/样本比放大，级别分布
    # 是**估算**。此前没有任何字段区分「精确」与「估算」，200 条 ERROR 能被
    # 报成 300 条 INFO。现在显式标注；truncated 仍只表示预算/键扫描触界。
    estimated: bool = Field(
        False,
        description="级别分布是否为采样估算（平台全量视角且未读完整个 Stream）",
    )
    sample_size: Optional[int] = Field(
        None,
        description="估算所用样本条数；estimated=false 时为 null",
    )


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

# ---- A2R20-04：单请求**全局条目预算**（不是「每键 × limit」）----
# 历史：list 每键 `count=limit`（最大 1000）× 最多 2000 键 = 200 万条；
# instance `count=limit*2` × 2000 = 400 万条；stats 的 tenant 分支
# `XRANGE` 不带 count，最坏 1000 键 × maxlen(10000) = 1000 万条。三处都在
# 最后才 `[:limit]`——`limit` 只是返回上限，不是读取预算。
# 现在预算是**前置**的：每键 `count=min(..., remaining)`，边读边扣减，
# 触界即停并把 `truncated=true` 带回响应（同时记 WARNING）。
_LOG_LIST_MAX_ENTRIES = 50_000
_LOG_INSTANCE_MAX_ENTRIES = 50_000
_LOG_STATS_MAX_ENTRIES = 50_000
# 与 server/log_handler.py 的 DEFAULT_MAX_LEN 同值：单流最多这么多条。
_LOG_STATS_PER_KEY_MAX = 10_000
# 墙钟预算：与条目预算共同前置生效。0.3s 约束**条目读取循环**（键 SCAN 另有
# scan_keys 自身的 keys/rounds/墙钟预算）；触界返回 truncated=true，不长时间
# 阻塞 console 事件循环。
_LOG_TIME_BUDGET_S = 0.3


def _bounded_log_keys(redis: Redis, pattern: str, max_keys: int) -> tuple[List[str], bool]:
    """有界 SCAN 代替 ``KEYS``（#202）：单条命令只扫一页，触界 fail-safe。

    COUNT 显式给 1000（PAGE_SIZE_MAX）：同样的 keyspace 轮次比默认 500 少一半，
    留更多墙钟给条目读取。
    """
    result = scan_keys(redis, pattern, count=1000, max_keys=max_keys)
    return result.keys, result.truncated


def _instance_log_keys(
    redis: Redis,
    service_type: Optional[str],
    instance_id: str,
    max_keys: int,
) -> tuple[List[str], bool]:
    """实例日志的**键级下推**（A2R28-01）：先精确键/定向模式，无命中回退。

    发布侧键格式是 ``plaita:logs:{service_type}:{instance_id}``
    （plaita/server/log_handler.py），instance_id 本可直接参与 key 择取。
    此前先枚举 ``plaita:logs:*`` 再按条目字段过滤——800 流 × 0.3s 墙钟下
    只有前 ~170 个流轮得到，绝大多数真实实例返回空（且 ``truncated`` 只
    在服务端 WARNING 可见）。

    - ``service_type`` 已知：键是精确的，``EXISTS`` 后**直读该键**，零 SCAN；
    - 只有 ``instance_id``（深链形态）：``plaita:logs:*:{instance_id}`` 定向
      SCAN，仍按字面量转义 glob 元字符。定向模式的 SCAN 只回传命中键，但
      仍需走完 keyspace 才能确认没有第二个 service 段；用 0.3s 墙钟 + 更大
      轮次预算收口（250 轮在 25 万键下差一轮就误报 truncated）；
    - 零命中回退全量 SCAN 并 WARNING——兼容历史/非规范键格式。
    """
    escaped_instance = escape_glob(instance_id)
    if service_type:
        exact = f"plaita:logs:{service_type}:{instance_id}"
        try:
            if redis.exists(exact):
                return [exact], False
        except Exception as exc:  # noqa: BLE001 - 探测失败退化为 SCAN
            logger.warning(
                "实例日志精确键探测失败（instance=%s key=%s，退化为 SCAN）: %s",
                instance_id, exact, exc,
            )
        logger.warning(
            "实例日志精确键无命中（instance=%s key=%s），回退 service SCAN",
            instance_id, exact,
        )
        return _bounded_log_keys(
            redis, f"plaita:logs:{escape_glob(service_type)}:*", max_keys
        )

    targeted = f"plaita:logs:*:{escaped_instance}"
    result = scan_keys(
        redis,
        targeted,
        count=1000,
        max_keys=max_keys,
        max_rounds=1000,
        time_budget_s=_LOG_TIME_BUDGET_S,
    )
    if result.keys:
        return result.keys, result.truncated

    logger.warning(
        "实例日志精确键零命中（instance=%s pattern=%s），回退全量 SCAN "
        "（兼容旧键格式；若结果仍为空请核对实例 ID）",
        instance_id,
        targeted,
    )
    return _bounded_log_keys(redis, "plaita:logs:*", max_keys)


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

    # 构建 Stream key 模式（A2R28-01）：instance_id 存在时下推到精确键
    # ``plaita:logs:{service}:{instance}`` / ``plaita:logs:*:{instance}``，
    # 只读命中键；条目字段过滤仍然保留（租户边界与深链语义不变）。
    if instance_id:
        keys, truncated = _instance_log_keys(
            redis, service_type, instance_id, _LOG_LIST_MAX_KEYS
        )
    else:
        pattern = f"plaita:logs:{service_type}:*" if service_type else "plaita:logs:*"
        keys, truncated = _bounded_log_keys(redis, pattern, _LOG_LIST_MAX_KEYS)

    logs = []
    skipped = 0
    tenant = tenant_scope(request)
    tenant_id_filter = tenant if tenant else None
    # 全局读取预算（前置）：每键只读到 remaining，读完即扣减。
    remaining = _LOG_LIST_MAX_ENTRIES
    deadline = time.monotonic() + _LOG_TIME_BUDGET_S

    for key in keys:
        key_str = key if isinstance(key, str) else key.decode()
        if remaining <= 0 or time.monotonic() >= deadline:
            truncated = True
            break

        try:
            # 从 Redis Stream 读取日志（每键 count 受全局 remaining 约束）
            count = min(limit, remaining)
            entries = redis.xrevrange(key_str, count=count)
            remaining -= len(entries)
            if remaining <= 0 and len(entries) >= count:
                # 读满了 remaining：该键可能还有未读条目（下一轮循环会 break，
                # 单键场景也必须显式标注不完整）。
                truncated = True

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
                except Exception:  # noqa: BLE001 - 条目解析失败计入 degraded
                    # A2R19-04：条目解析失败计入 skipped，不再无痕迹丢弃
                    skipped += 1
                    continue
        except Exception:  # noqa: BLE001 - 键读取失败计入 degraded
            # 如果不是 Stream/键读取失败：跳过但必须可观测
            skipped += 1
            continue

    if skipped:
        logger.warning(
            "日志列表读取降级：%d 个键/条目读取失败（返回 %d 条，degraded=true）",
            skipped, len(logs),
        )
    if truncated:
        logger.warning(
            "日志列表触界：全局条目预算 %d / 墙钟 %.1fs / 键预算 %d 任一触界，"
            "返回 %d 条局部结果（truncated=true）",
            _LOG_LIST_MAX_ENTRIES, _LOG_TIME_BUDGET_S, _LOG_LIST_MAX_KEYS,
            len(logs),
        )
    
    # 按时间排序
    logs.sort(key=lambda x: x.timestamp, reverse=True)
    
    # 限制数量
    logs = logs[:limit]
    
    return LogListResponse(
        logs=logs,
        total=len(logs),
        degraded=skipped > 0,
        skipped=skipped,
        truncated=truncated,
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

    # A2R28-01：实例键级下推——发布侧键是
    # ``plaita:logs:{service_type}:{instance_id}``，只读命中键；零命中才
    # 回退全量 SCAN（历史/非规范键格式，WARNING 可见）。
    keys, truncated = _instance_log_keys(
        redis, None, instance_id, _LOG_LIST_MAX_KEYS
    )
    # 多租户（plaita#192）：实例 id 可能被他租户猜中/从别处取得，逐条目按
    # tenant_id 收窄——不能只按 instance_id 匹配。
    tenant = tenant_scope(request)
    skipped = 0
    remaining = _LOG_INSTANCE_MAX_ENTRIES
    deadline = time.monotonic() + _LOG_TIME_BUDGET_S
    
    for key in keys:
        key_str = key if isinstance(key, str) else key.decode()
        if remaining <= 0 or time.monotonic() >= deadline:
            truncated = True
            break
        
        try:
            # 从 Redis Stream 读取日志（每键 count 受全局 remaining 约束）
            count = min(limit * 2, remaining)
            if order == "desc":
                entries = redis.xrevrange(key_str, count=count)
            else:
                entries = redis.xrange(key_str, count=count)
            remaining -= len(entries)
            if remaining <= 0 and len(entries) >= count:
                truncated = True
            
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
                except Exception:  # noqa: BLE001 - 条目解析失败计入 degraded
                    # A2R19-04：条目解析失败计入 skipped，不再无痕迹丢弃
                    skipped += 1
                    continue
        except Exception:  # noqa: BLE001 - 键读取失败计入 degraded
            skipped += 1
            continue

    if skipped:
        logger.warning(
            "实例日志读取降级（instance=%s）：%d 个键/条目读取失败（返回 %d 条，degraded=true）",
            instance_id, skipped, len(logs),
        )
    if truncated:
        logger.warning(
            "实例日志触界（instance=%s）：全局条目预算 %d / 墙钟 %.1fs / 键预算 %d "
            "任一触界，返回 %d 条局部结果（truncated=true）",
            instance_id, _LOG_INSTANCE_MAX_ENTRIES, _LOG_TIME_BUDGET_S,
            _LOG_LIST_MAX_KEYS, len(logs),
        )
    
    # 按时间排序
    logs.sort(key=lambda x: x.timestamp, reverse=(order == "desc"))
    
    # 限制数量
    logs = logs[:limit]
    
    return LogListResponse(
        logs=logs,
        total=len(logs),
        degraded=skipped > 0,
        skipped=skipped,
        truncated=truncated,
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
    truncated = False
    # A2R24-03：平台视角是采样估算，必须显式标注（estimated/sample_size）。
    estimated = False
    sampled_entries = 0
    remaining = _LOG_STATS_MAX_ENTRIES
    deadline = time.monotonic() + _LOG_TIME_BUDGET_S
    
    # 遍历所有日志 Stream
    pattern = "plaita:logs:*"
    keys, scan_truncated = _bounded_log_keys(redis, pattern, _LOG_STATS_MAX_KEYS)
    truncated = truncated or scan_truncated
    
    for key in keys:
        key_str = key if isinstance(key, str) else key.decode()
        if remaining <= 0 or time.monotonic() >= deadline:
            truncated = True
            break
        
        try:
            # 从 key 解析服务类型
            parts = key_str.split(":")
            if len(parts) >= 3:
                service_type = parts[2]
            else:
                service_type = "unknown"

            if tenant:
                # 精确统计：只累计本租户条目。XRANGE 必须带 count（A2R20-04：
                # 此前无 count，最坏 1000 键 × maxlen 全读）；全局 remaining
                # 前置扣减，单键再受 maxlen 上限约束。
                level_counts: Dict[str, int] = {}
                matched = 0
                per_key = min(_LOG_STATS_PER_KEY_MAX, remaining)
                entries = redis.xrange(key_str, count=per_key)
                remaining -= len(entries)
                if redis.xlen(key_str) > len(entries):
                    truncated = True
                for _, entry_data in entries:
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
            
            # 采样一些日志来统计级别分布（采样也受全局预算约束）
            sample = min(100, remaining)
            entries = redis.xrevrange(key_str, count=sample) if sample > 0 else []
            remaining -= len(entries)
            sampled_entries += len(entries)
            level_counts = {}
            
            for _, entry_data in entries:
                level = entry_data.get("level", "INFO")
                if isinstance(level, bytes):
                    level = level.decode()
                level_counts[level] = level_counts.get(level, 0) + 1
            
            # 按比例估算总数；样本没覆盖整个 Stream 时是估算（A2R24-03）
            if entries:
                if len(entries) < length:
                    estimated = True
                ratio = length / len(entries)
                for level, count in level_counts.items():
                    if service_type not in stats_dict:
                        stats_dict[service_type] = {}
                    estimated_count = int(count * ratio)
                    stats_dict[service_type][level] = stats_dict[service_type].get(level, 0) + estimated_count
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
    
    if truncated:
        logger.warning(
            "日志统计触界：全局条目预算 %d / 墙钟 %.1fs / 键预算 %d 任一触界，"
            "total_logs=%d 与级别分布只是局部结果（truncated=true）",
            _LOG_STATS_MAX_ENTRIES, _LOG_TIME_BUDGET_S, _LOG_STATS_MAX_KEYS,
            total_logs,
        )

    if estimated:
        # 采样估算不是静默降级：响应带 estimated/sample_size，这里留 WARNING。
        logger.warning(
            "日志统计平台视角使用采样估算：sample_size=%d total_logs=%d "
            "estimated=true（精确计数请带租户视角）",
            sampled_entries, total_logs,
        )

    return LogStatsResponse(
        stats=stats,
        total_logs=total_logs,
        truncated=truncated,
        estimated=estimated,
        sample_size=sampled_entries if estimated else None,
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

