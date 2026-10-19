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
    from .redis_scan import scan_keys
except ImportError:  # 平铺布局（cwd=backend）运行时
    from api.redis_scan import scan_keys  # type: ignore

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
    schema_version: Optional[int] = Field(default=None, description="源消息信封版本")
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="原始任务负载；畸形消息为 {'raw': 截断原始串}",
    )
    raw_fields: Optional[Dict[str, Any]] = Field(
        default=None, description="畸形消息的原始 Stream 字段（截断）"
    )
    truncated: Optional[bool] = Field(
        default=None, description="畸形 payload 是否被截断"
    )


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


# ============ DLQ 读取（租户收窄） ============

# 租户视角逐页回退补足 count 时的最大扫描条数：死信可能被别租户条目刷屏，
# 但也不能为此无界扫描整条 Stream。
_DLQ_SCAN_LIMIT = 500


def _optional_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _optional_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _payload_shape(value: Any) -> Dict[str, Any]:
    """payload 字段容错（A49-02）。

    正常死信的 ``payload`` 是 dict；``_dead_letter_invalid`` 对畸形消息写的是
    **截断原始串**（task_queue.py）。定型 ``Dict`` 会在构造 DlqEntry 时抛
    ValidationError——一条毒丸让整个端点（含所有租户）500。原始串统一包成
    ``{"raw": ...}``：读面形状不变，证据不丢。
    """
    if isinstance(value, dict):
        return value
    if value is None or value == "":
        return {}
    return {"raw": value if isinstance(value, str) else str(value)}


def _parse_dlq_entry(msg_id: Any, fields: Dict[Any, Any]) -> DlqEntry:
    """DLQ Stream 条目 → DlqEntry（envelope.payload 为原始任务负载）。

    解析必须**对单条毒丸容错且不抛**：该函数在租户过滤/端点之前执行，任何
    异常都会遮蔽全部 DLQ 流与全部租户视角（A49-02）。
    """
    if isinstance(msg_id, bytes):
        msg_id = msg_id.decode()
    envelope: Any = None
    raw_payload: Any = None
    for k, v in fields.items():
        k_str = k.decode() if isinstance(k, bytes) else k
        if k_str == "payload":
            raw_payload = v.decode(errors="replace") if isinstance(v, bytes) else v
            try:
                envelope = json.loads(raw_payload)
            except Exception:
                envelope = None
            break
    if not isinstance(envelope, dict):
        # 整个信封不可解析 / JSON 非对象：原始串即唯一证据，按 raw 形态展示。
        envelope = {}
        payload = _payload_shape(raw_payload)
    else:
        payload = _payload_shape(envelope.get("payload"))
    raw_fields = envelope.get("raw_fields")
    truncated = envelope.get("truncated")
    return DlqEntry(
        dlq_id=msg_id,
        reason=str(envelope.get("reason") or ""),
        source_stream=str(envelope.get("source_stream") or ""),
        source_id=str(envelope.get("source_id") or ""),
        delivery_count=_optional_int(envelope.get("delivery_count")),
        dead_lettered_at=_optional_float(envelope.get("dead_lettered_at")),
        schema_version=_optional_int(envelope.get("schema_version")),
        payload=payload,
        raw_fields=raw_fields if isinstance(raw_fields, dict) else None,
        truncated=bool(truncated) if truncated is not None else None,
    )


def _read_dlq_entries(
    redis: Redis, name: str, *, count: int, tenant: Optional[str]
) -> List[DlqEntry]:
    """取最近 ``count`` 条**本租户可见**死信，按时间正序返回。

    平台视角等价于单次 ``XREVRANGE``；租户视角逐页回退，跳过别租户条目，
    直到补满 count 或扫到 ``_DLQ_SCAN_LIMIT``（plaita#192：DLQ Stream 是
    平台级的，条目 payload 里才带 tenant_id）。
    """
    if count <= 0:
        return []
    collected: List[DlqEntry] = []
    scanned = 0
    batch = max(count, 20)
    cursor = "+"
    while len(collected) < count and scanned < _DLQ_SCAN_LIMIT:
        rows = redis.xrevrange(name, max=cursor, min="-", count=batch)
        if not rows:
            break
        for msg_id, fields in rows:
            scanned += 1
            entry = _parse_dlq_entry(msg_id, fields)
            if tenant is not None and not _owned_by_tenant(entry.payload, tenant):
                continue
            collected.append(entry)
            if len(collected) >= count:
                break
        if len(rows) < batch:
            break
        last_id = rows[-1][0]
        if isinstance(last_id, bytes):
            last_id = last_id.decode()
        cursor = f"({last_id}"  # 排他游标，避免同一页原地重读
    collected.reverse()
    return collected


# ============ API 端点 ============

@router.get("/queues", response_model=QueueListResponse)
async def list_queues(
    redis: Redis = Depends(get_redis)
):
    """
    获取所有队列概览

    已知规模泄露残面（plaita#192 记录）：本端点返回的是**平台级**队列长度
    （键共享、无条目内容）。逐租户精确长度需要对共享 Stream 全量扫描，代价
    随队列深度线性增长，且与 ``GET /api/queues/{name}`` 的 ``length`` 同口径；
    条目内容面（``/queues/{name}`` 与 ``/queues/dlq``）已按租户收窄。
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
        if "*" in pattern:
            # 通配项走有界 SCAN（#202）：单条命令只扫一页，触界 fail-safe
            keys = list(_bounded_queue_keys(redis, pattern))
        else:
            # 字面键直读（#202）：EXISTS 是 O(1)，再没有全库遍历的理由
            keys = [pattern] if redis.exists(pattern) else []
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


# 有界扫描的单请求键预算（#202）：每个键还要 TYPE/LLEN/XLEN（DLQ 再叠
# XREVRANGE），键预算按读取代价收紧；触界返回部分结果并记 warning。
_QUEUE_SCAN_MAX_KEYS = 1000
_DLQ_SCAN_MAX_KEYS = 500


def _bounded_queue_keys(
    redis: Redis, pattern: str, max_keys: int = _QUEUE_SCAN_MAX_KEYS
) -> List[str]:
    """有界 SCAN 代替 ``KEYS``（#202）：单条命令只扫一页，触界 fail-safe。"""
    return scan_keys(redis, pattern, max_keys=max_keys).keys


def _is_stream_key(key: str) -> bool:
    """已知的 Stream 键永远保留零值行——含 DLQ（#26）。"""
    return key in ("plaita:flow:queue", "plaita:flow:queue:v2") or key.endswith(DLQ_SUFFIX)


@router.get("/queues/dlq", response_model=DlqListResponse)
async def list_dlq(
    request: Request,
    count: int = 20,
    redis: Redis = Depends(get_redis)
):
    """死信总览（#26）：每个 DLQ Stream 的堆积量与最近若干条死信。

    死信此前只有 worker 的 logger.error——值守看不到堆积，DLQ 被 XTRIM
    裁掉旧条目也无人知晓。本端点把 DLQ 拉进看板。

    多租户（plaita#192）：DLQ Stream 是平台级的（键里没有租户段），租户归属
    在条目 ``payload.tenant_id``；租户视角只返回本租户条目，平台视角全量。
    每条 Stream 的 ``length`` 仍是平台级总量（与 ``get_queue`` 的 length
    同口径，仅规模不含量）。

    - **count**: 每个 DLQ Stream 返回的最近条目数（默认 20）
    """
    count = max(0, min(int(count), 200))
    tenant = tenant_scope(request)
    names = set(KNOWN_DLQ_KEYS)
    # `*:dlq` 通配必须有界扫（#202）：未知 DLQ 键靠 SCAN 补，只扫一页/触界即停
    names.update(_bounded_queue_keys(redis, f"*{DLQ_SUFFIX}", max_keys=_DLQ_SCAN_MAX_KEYS))

    streams = []
    for name in sorted(names):
        if redis.type(name) != "stream":
            streams.append(DlqStream(name=name, length=0, entries=[]))
            continue
        length = redis.xlen(name)
        entries = _read_dlq_entries(redis, name, count=count, tenant=tenant)
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
    - **count**: 获取数量（1..200；上限外夹紧到边界）
    """
    # 资源范围：不登记的键一律 404（不泄露键是否存在）
    if not _queue_key_allowed(queue_name):
        raise HTTPException(status_code=404, detail="队列不存在")
    if start < 0:
        raise HTTPException(status_code=422, detail="start 不能为负数")
    # 下限 1 而非 0：`count=0` 会让 redis-py 的 `xrange`/`lrange` 客户端直接抛
    # `DataError: value is not an integer or out of range`（`XRANGE ... COUNT 0`
    # 不合法）——端点 500。此前 0 也没被拦（stream 键从未设过上限），#64 收紧
    # 时顺手把两端都夹紧，调用方拿不到 500。
    count = max(1, min(int(count), 200))
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

