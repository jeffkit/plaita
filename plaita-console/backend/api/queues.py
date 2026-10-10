"""
任务队列查看 API
提供队列状态查询接口
"""
import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from redis import Redis

try:
    from ..auth import tenant_scope
except ImportError:  # 平铺布局（cwd=backend）运行时
    from auth import tenant_scope  # type: ignore

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


class DlqEntry(BaseModel):
    """一条死信（DLQ 条目 payload 的信封字段）"""
    dlq_id: str = Field(..., description="DLQ Stream 条目 id")
    reason: str = Field(default="", description="死信原因")
    source_stream: str = Field(default="", description="原队列 Stream")
    source_id: str = Field(default="", description="原消息 id")
    delivery_count: Optional[int] = Field(default=None, description="投递次数")
    dead_lettered_at: Optional[float] = Field(default=None, description="死信 Unix 时间戳")
    payload: Dict[str, Any] = Field(default_factory=dict, description="原始任务负载")


class DlqStream(BaseModel):
    """单个 DLQ Stream 概览"""
    name: str
    length: int
    entries: List[DlqEntry]


class DlqListResponse(BaseModel):
    """DLQ 列表响应"""
    streams: List[DlqStream]
    total: int


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


# ============ 已知队列列表 ============

# 注意：DLQ（死信）键此前不在本表——队列页永远看不到死信堆积（#26），
# 只能靠 redis-cli 才能发现。`<queue>:dlq` 由 task_queue.dlq_stream_key 生成。
KNOWN_QUEUES = [
    "plaita:flow:queue",           # 流程任务队列
    "plaita:flow:queue:v2",        # 流程任务队列 v2（FlowWorker 实消费流）
    "plaita:flow:queue:dlq",       # 上述队列的死信 Stream
    "plaita:flow:queue:v2:dlq",    # v2 队列的死信 Stream
    "plaita:delay:queue",          # 延迟任务队列
    "plaita:redis_queue:*",        # Redis 队列服务
    "plaita:kafka_queue:*",        # Kafka 队列服务
]

DLQ_SUFFIX = ":dlq"

# 显式登记的死信键：DLQ 是纯记录流，只在有死信时才被创建——空 DLQ 也保留
# 零值行，页面才不缺行（否则「看不到」与「没有死信」无从区分）。
KNOWN_DLQ_KEYS = [
    "plaita:flow:queue:dlq",
    "plaita:flow:queue:v2:dlq",
]

# 单键读取的资源范围：只放行 KNOWN_QUEUES 登记的字面键，或登记通配项的
# 前缀（`plaita:redis_queue:*` → `plaita:redis_queue:`）。否则任意键名都可
# 被当 Redis 键读取（#64）。
_ALLOWED_QUEUE_PREFIXES = tuple(
    p[:-1] for p in KNOWN_QUEUES if p.endswith("*")
)
_ALLOWED_QUEUE_KEYS = frozenset(p for p in KNOWN_QUEUES if not p.endswith("*"))


def _queue_key_allowed(queue_name: str) -> bool:
    return queue_name in _ALLOWED_QUEUE_KEYS or queue_name.startswith(
        _ALLOWED_QUEUE_PREFIXES
    )


def _owned_by_tenant(data: Any, tenant: Optional[str]) -> bool:
    """消息归属：无租户上下文（平台全量视角）一律放行；有则按消息体
    ``tenant_id`` 过滤，缺省视为 default。无法判归属的非 dict 消息在有租户
    上下文时一律不返回。"""
    if tenant is None:
        return True
    if not isinstance(data, dict):
        return False
    return (data.get("tenant_id") or "default") == tenant


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
                queue_type="stream" if _is_stream_key(pattern) else "list",
            ))
            continue
        for key in keys:
            key_str = key if isinstance(key, str) else key.decode()
            length = _queue_length(key_str)
            if length > 0 or _is_stream_key(key_str):
                queues.append(QueueInfo(
                    name=key_str,
                    length=length,
                    queue_type=_key_type(key_str),
                ))

    return QueueListResponse(
        queues=queues,
        total=len(queues)
    )


def _is_stream_key(key: str) -> bool:
    """已知的 Stream 键永远保留零值行——含 DLQ（#26）。"""
    return key in ("plaita:flow:queue", "plaita:flow:queue:v2") or key.endswith(DLQ_SUFFIX)


@router.get("/queues/dlq", response_model=DlqListResponse)
async def list_dlq(
    count: int = 20,
    redis: Redis = Depends(get_redis)
):
    """死信总览（#26）：每个 DLQ Stream 的堆积量与最近若干条死信。

    死信此前只有 worker 的 logger.error——值守看不到堆积，DLQ 被 XTRIM
    裁掉旧条目也无人知晓。本端点把 DLQ 拉进看板。

    - **count**: 每个 DLQ Stream 返回的最近条目数（默认 20）
    """
    count = max(0, min(int(count), 200))
    names = set(KNOWN_DLQ_KEYS)
    for key in redis.keys(f"*{DLQ_SUFFIX}"):
        names.add(key if isinstance(key, str) else key.decode())

    streams = []
    for name in sorted(names):
        if redis.type(name) != "stream":
            streams.append(DlqStream(name=name, length=0, entries=[]))
            continue
        length = redis.xlen(name)
        entries: List[DlqEntry] = []
        if count:
            # 取最近 count 条：XREVRANGE 从尾部倒序拿，再翻回时间正序
            for msg_id, fields in reversed(redis.xrevrange(name, max="+", min="-", count=count)):
                if isinstance(msg_id, bytes):
                    msg_id = msg_id.decode()
                envelope: Dict[str, Any] = {}
                for k, v in fields.items():
                    k_str = k.decode() if isinstance(k, bytes) else k
                    if k_str == "payload":
                        raw = v.decode() if isinstance(v, bytes) else v
                        try:
                            envelope = json.loads(raw)
                        except Exception:
                            envelope = {"raw": str(raw)}
                        break
                if not isinstance(envelope, dict):
                    envelope = {"raw": envelope}
                entries.append(DlqEntry(
                    dlq_id=msg_id,
                    reason=str(envelope.get("reason", "")),
                    source_stream=str(envelope.get("source_stream", "")),
                    source_id=str(envelope.get("source_id", "")),
                    delivery_count=envelope.get("delivery_count"),
                    dead_lettered_at=envelope.get("dead_lettered_at"),
                    payload=envelope.get("payload") or {},
                ))
        streams.append(DlqStream(name=name, length=length, entries=entries))

    return DlqListResponse(streams=streams, total=len(streams))


@router.get("/queues/{queue_name:path}", response_model=QueueDetailResponse)
async def get_queue(
    request: Request,
    queue_name: str,
    start: int = 0,
    count: int = 20,
    redis: Redis = Depends(get_redis)
):
    """
    获取队列详情

    - **queue_name**: 队列名称（须为已登记的 plaita 队列键）
    - **start**: 起始索引（非负）
    - **count**: 获取数量（上限 200）
    """
    # 资源范围：不登记的键一律 404（不泄露键是否存在）
    if not _queue_key_allowed(queue_name):
        raise HTTPException(status_code=404, detail="队列不存在")
    if start < 0:
        raise HTTPException(status_code=422, detail="start 不能为负数")
    count = max(0, min(int(count), 200))
    tenant = tenant_scope(request)

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
            if not _owned_by_tenant(data, tenant):
                continue
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
            if not _owned_by_tenant(data, tenant):
                continue
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

