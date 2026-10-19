"""
任务队列查看 API
提供队列状态查询接口
"""
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from redis import Redis

try:
    from .redis_scan import scan_keys
except ImportError:  # 平铺布局（cwd=backend）运行时
    from api.redis_scan import scan_keys  # type: ignore

try:
    from ..auth import ensure_platform_admin, require_auth, tenant_scope
except ImportError:  # 平铺布局（cwd=backend）运行时
    from auth import ensure_platform_admin, require_auth, tenant_scope  # type: ignore

try:
    from plaita.server.dlq_replay import replay_dead_letter
except ImportError:  # 最小安装（无引擎）时回灌端点返回 503
    replay_dead_letter = None  # type: ignore

router = APIRouter()
logger = logging.getLogger(__name__)


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
    tenant_id: Optional[str] = Field(
        default=None,
        description="payload 里显式声明的租户（缺字段/畸形为 null，不猜 default）",
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


class DlqReplayRequest(BaseModel):
    """死信回灌请求（A53-S2；平台管理员专属）。"""
    dlq_key: Optional[str] = Field(
        default=None,
        description="来源 DLQ 键；缺省时在已登记/已存在的 DLQ 中按条目 id 查找",
    )
    target_stream: Optional[str] = Field(
        default=None,
        description="目标任务队列；缺省为 dlq_key 去掉 :dlq 后缀",
    )
    force: bool = Field(
        default=False, description="绕过 replay 幂等标记（仅平台管理员，留审计）"
    )
    execution_id: Optional[str] = Field(
        default=None, description="为无 execution_id 的旧 start 载体补 id"
    )
    tenant_id: Optional[str] = Field(
        default=None, description="为缺 tenant_id 的条目显式指定租户"
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


def _text(value: Any) -> str:
    """Redis 字段/键 → str；非法 UTF-8 用 replace 保留可诊断信息（A53-11）。"""
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return str(value)


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
    return {"raw": _text(value)}


def _parse_dlq_entry(msg_id: Any, fields: Dict[Any, Any]) -> DlqEntry:
    """DLQ Stream 条目 → DlqEntry（envelope.payload 为原始任务负载）。

    解析必须**对单条毒丸容错且不抛**：该函数在租户过滤/端点之前执行，任何
    异常都会遮蔽全部 DLQ 流与全部租户视角（A49-02/A53-11）。未知形状统一下
    降为 ``reason=parse_error:...`` 的诊断条目，而不是抛给 FastAPI。
    """
    msg_id = _text(msg_id)
    try:
        envelope: Any = None
        raw_payload: Any = None
        for k, v in fields.items():
            if _text(k) == "payload":
                raw_payload = _text(v)
                try:
                    envelope = json.loads(raw_payload)
                except Exception:  # noqa: BLE001 — 不可解析反正走 raw 兜底
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
        tenant_id = payload.get("tenant_id")
        if not isinstance(tenant_id, str) or not tenant_id:
            tenant_id = None
        return DlqEntry(
            dlq_id=msg_id,
            reason=str(envelope.get("reason") or ""),
            source_stream=str(envelope.get("source_stream") or ""),
            source_id=str(envelope.get("source_id") or ""),
            delivery_count=_optional_int(envelope.get("delivery_count")),
            dead_lettered_at=_optional_float(envelope.get("dead_lettered_at")),
            schema_version=_optional_int(envelope.get("schema_version")),
            payload=payload,
            raw_fields=(
                {_text(k): v for k, v in raw_fields.items()}
                if isinstance(raw_fields, dict)
                else None
            ),
            truncated=bool(truncated) if truncated is not None else None,
            tenant_id=tenant_id,
        )
    except Exception as exc:  # noqa: BLE001 — 单条毒丸不得遮蔽整个读面
        logger.warning(
            "DLQ 条目 %s 解析失败（按诊断条目返回）: %s", msg_id, exc
        )
        return DlqEntry(
            dlq_id=msg_id,
            reason=f"parse_error:{type(exc).__name__}",
            payload={"raw": _text(fields)},
        )


def _dlq_entry_visible(
    entry: DlqEntry,
    tenant: Optional[str],
    generation_epoch: Optional[float],
) -> bool:
    """租户视角的死信可见性（#264，纵深防御）。

    不能只看 ``payload.tenant_id``：删除同名重建后，前代条目仍带着同一个
    slug。判据三条同时成立才可见：

    1. payload 有**显式** ``tenant_id`` 且等于当前租户——缺字段/畸形不再
       回退 ``default``（A49-02 的 raw 包装曾把缺租户条目误归 default，
       B53-03）；
    2. ``dead_lettered_at`` 可解析——缺失/畸形无法证明属于当前世代；
    3. 死信时间不早于租户当前世代起点（``tenant.created_at``）——前代死信
       一律归平台可见，绝不归给同名重建的新租户。

    ``generation_epoch`` 取不到时 fail-closed（租户视角不返回任何条目）。
    平台视角（``tenant is None``）不看本函数。
    """
    if tenant is None:
        return True
    if entry.tenant_id != tenant:
        return False
    if generation_epoch is None or entry.dead_lettered_at is None:
        return False
    try:
        dead_at = float(entry.dead_lettered_at)
    except (TypeError, ValueError):
        return False
    if dead_at != dead_at or dead_at in (float("inf"), float("-inf")):
        return False
    return dead_at >= generation_epoch


def _read_dlq_entries(
    redis: Redis,
    name: str,
    *,
    count: int,
    tenant: Optional[str],
    generation_epoch: Optional[float] = None,
) -> List[DlqEntry]:
    """取最近 ``count`` 条**本租户可见**死信，按时间正序返回。

    平台视角等价于单次 ``XREVRANGE``；租户视角逐页回退，跳过别租户/前代/
    无法归属的条目，直到补满 count 或扫到 ``_DLQ_SCAN_LIMIT``（plaita#192：
    DLQ Stream 是平台级的，条目 payload 里才带 tenant_id）。
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
            if not _dlq_entry_visible(entry, tenant, generation_epoch):
                continue
            collected.append(entry)
            if len(collected) >= count:
                break
        if len(rows) < batch:
            break
        last_id = _text(rows[-1][0])
        cursor = f"({last_id}"  # 排他游标，避免同一页原地重读
    collected.reverse()
    return collected


def _tenant_generation_epoch(
    request: Request, tenant: Optional[str]
) -> Optional[float]:
    """租户当前世代起点（epoch 秒）；取不到返回 None（调用方 fail-closed）。

    优先复用 ``require_auth`` 的请求快照（会话解析的
    ``Tenant.created_at``）；未挂鉴权/平台直调时退回一次 DB 读。DB 里的
    ``created_at`` 是 naive UTC（``datetime.utcnow``），按 UTC 折算，避免
    部署时区把世代起点挪 8 小时。
    """
    if not tenant:
        return None
    created: Any = None
    state = getattr(request, "state", None)
    snapshot = getattr(state, "tenant_generation", None) if state is not None else None
    if isinstance(snapshot, tuple) and snapshot:
        created = snapshot[0]
    if created is None:
        try:
            try:
                from ..services import flow_store, tenants_svc
            except ImportError:
                from services import flow_store, tenants_svc  # type: ignore
            store = getattr(request.app.state, "store", None)
            if store is None:
                store = flow_store.get_flow_store()
            generation = tenants_svc.tenant_generation(store, tenant)
            created = datetime.fromisoformat(generation) if generation else None
        except Exception:  # noqa: BLE001 — 世代不可证明 ⇒ 租户视角不返回
            logger.exception(
                "读取租户 %s 世代失败，DLQ 租户读面按 fail-closed 处理", tenant
            )
            return None
    if created is None:
        return None
    try:
        if getattr(created, "tzinfo", None) is None:
            created = created.replace(tzinfo=timezone.utc)
        return float(created.timestamp())
    except (TypeError, ValueError, OSError):
        logger.warning("租户 %s 世代时间不可解析，DLQ 租户读面 fail-closed", tenant)
        return None


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
            key_str = _text(key)
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


def known_queue_family_keys(
    redis: Redis, max_keys: int = _QUEUE_SCAN_MAX_KEYS
) -> List[str]:
    """已登记的队列键族（A55-S5）：字面键 + 通配前缀的有界 SCAN。

    只认 ``KNOWN_QUEUES``——绝不 ``SCAN *:dlq``。事件键
    （``plaita:event:type:x:dlq``）由此不再被误认为队列/DLQ（#202 同源）。
    """
    names = {p for p in KNOWN_QUEUES if "*" not in p}
    for pattern in KNOWN_QUEUES:
        if "*" in pattern:
            names.update(_bounded_queue_keys(redis, pattern, max_keys=max_keys))
    return sorted(names)


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
    # #264：租户视角还要求条目属于**当前世代**。取不到世代 ⇒ fail-closed
    # （租户读面为空），绝不把前代死信归给同名重建的新租户。
    generation_epoch = _tenant_generation_epoch(request, tenant)
    names = set(KNOWN_DLQ_KEYS)
    # A55-S5：只从**已登记队列族**里发现有界 DLQ 键；不再 SCAN `*:dlq`
    # （事件类型键可被租户构造为 `...:dlq`，会把 DLQ 看板当注入面）。
    names.update(
        key for key in known_queue_family_keys(redis, max_keys=_DLQ_SCAN_MAX_KEYS)
        if key.endswith(DLQ_SUFFIX)
    )

    streams = []
    for name in sorted(names):
        if _text(redis.type(name)) != "stream":
            streams.append(DlqStream(name=name, length=0, entries=[]))
            continue
        length = redis.xlen(name)
        entries = _read_dlq_entries(
            redis,
            name,
            count=count,
            tenant=tenant,
            generation_epoch=generation_epoch,
        )
        streams.append(DlqStream(name=name, length=length, entries=entries))

    return DlqListResponse(streams=streams, total=len(streams))


def _resolve_dlq_key(
    redis: Redis, requested: Optional[str], dlq_id: str
) -> Optional[str]:
    """定位 ``dlq_id`` 所在 DLQ 键（只允许已登记/同族 ``*:dlq`` 键）。"""
    if requested:
        if not requested.endswith(DLQ_SUFFIX) or not _queue_key_allowed(requested):
            return None
        try:
            return (
                requested
                if redis.xrange(requested, min=dlq_id, max=dlq_id, count=1)
                else None
            )
        except Exception:  # noqa: BLE001 — 读失败按不存在处理（具名 404）
            logger.warning("replay 定位 DLQ 条目失败（%s/%s）", requested, dlq_id)
            return None
    names = set(KNOWN_DLQ_KEYS)
    names.update(
        _bounded_queue_keys(redis, f"*{DLQ_SUFFIX}", max_keys=_DLQ_SCAN_MAX_KEYS)
    )
    for name in sorted(names):
        try:
            if _text(redis.type(name)) == "stream" and redis.xrange(
                name, min=dlq_id, max=dlq_id, count=1
            ):
                return name
        except Exception:  # noqa: BLE001 — 单个键读取失败不遮蔽其他候选
            logger.warning("replay 定位 DLQ 键失败（%s）", name, exc_info=True)
    return None


def _record_replay_audit(
    request: Request,
    *,
    dlq_key: str,
    dlq_id: str,
    detail: Dict[str, Any],
    tenant_id: str,
) -> None:
    """DLQ replay 的 DB 审计（成功/具名拒绝统一口径）。

    A55-S3：``tenant_id`` 是**资源生效租户**（死信条目 payload 的租户），不是
    调用者的 header 视角——平台管理员带 ``X-Tenant-ID`` 跨租户回灌时，审计行
    仍落在条目所属租户，``detail.target_tenant`` 同步回显。这样租户 admin 在
    ``/api/audit`` 能看到「谁回灌了本租户的死信」，平台视角也不丢操作人身份
    （``actor``/``key_fingerprint`` 由 ``record`` 从 request.state 取）。
    """
    try:
        try:
            from ..services import audit as audit_svc
        except ImportError:
            from services import audit as audit_svc  # type: ignore
        audit_svc.record(
            request,
            action="dlq.replay",
            resource="queue",
            resource_id=f"{dlq_key}/{dlq_id}",
            detail=detail,
            tenant_id=tenant_id,
        )
    except Exception:  # noqa: BLE001 — 审计失败不得改变回灌结果
        logger.warning("DLQ replay 控制台审计失败（%s/%s）", dlq_key, dlq_id)


@router.post("/queues/dlq/{dlq_id}/replay")
async def replay_dlq(
    dlq_id: str,
    body: DlqReplayRequest,
    request: Request,
    redis: Redis = Depends(get_redis),
    identity: Dict[str, Any] = Depends(require_auth),
):
    """幂等回灌一条死信（A53-S2，平台管理员专属）。

    返回统一 ``{replayed, reason, target_message_id, execution_id,
    tenant_id, schema_version}``；``reason`` 是具名结果（``ok`` /
    ``already_replayed`` / ``schema_too_new`` / ``execution_deleted`` ...）。
    DLQ 原条目**不删除**（证据保留）。

    审计（A55-S3）：成功与具名拒绝都写 ``dlq.replay``；定位失败/目标未登记
    也在抛 4xx 前留痕（A53-S2「每次尝试」契约）。行内 ``tenant_id`` 是条目
    的生效租户，``detail.target_tenant`` 同值回显。
    """
    if replay_dead_letter is None:
        raise HTTPException(status_code=503, detail="引擎未安装，无法回灌死信")
    ensure_platform_admin(
        request,
        identity,
        action="dlq.replay",
        resource="queue",
        target=dlq_id,
    )
    caller_tenant = str(identity.get("tenant_id") or "")
    dlq_key = _resolve_dlq_key(redis, body.dlq_key, dlq_id)
    if dlq_key is None:
        _record_replay_audit(
            request,
            dlq_key=body.dlq_key or "<unresolved>",
            dlq_id=dlq_id,
            detail={
                "replayed": False,
                "reason": "dlq_not_found",
                "requested_tenant": body.tenant_id or None,
            },
            tenant_id=caller_tenant,
        )
        raise HTTPException(status_code=404, detail="死信不存在或 DLQ 键未登记")
    target_stream = (body.target_stream or "").strip() or dlq_key[: -len(DLQ_SUFFIX)]
    if target_stream.endswith(DLQ_SUFFIX) or not _queue_key_allowed(target_stream):
        _record_replay_audit(
            request,
            dlq_key=dlq_key,
            dlq_id=dlq_id,
            detail={
                "replayed": False,
                "reason": "target_not_allowed",
                "target_stream": body.target_stream or "",
                "requested_tenant": body.tenant_id or None,
            },
            tenant_id=caller_tenant,
        )
        raise HTTPException(status_code=422, detail="目标队列未登记或不是任务队列")
    result = replay_dead_letter(
        redis,
        dlq_key=dlq_key,
        dlq_id=dlq_id,
        target_stream=target_stream,
        actor=str(identity.get("actor") or "unknown"),
        force=bool(body.force),
        execution_id=body.execution_id,
        tenant_id=body.tenant_id,
    )
    payload = result.as_dict()
    target_tenant = result.tenant_id or body.tenant_id or caller_tenant
    payload["target_tenant"] = result.tenant_id or body.tenant_id or None
    _record_replay_audit(
        request,
        dlq_key=dlq_key,
        dlq_id=dlq_id,
        detail=payload,
        tenant_id=target_tenant,
    )
    return result.as_dict()


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

    key_type = _text(redis.type(queue_name))

    tasks = []
    if key_type == "stream":
        # Stream 队列（plaita:flow:queue）：XRANGE 取消息，payload 字段即任务 JSON
        length = redis.xlen(queue_name)
        entries = redis.xrange(queue_name, min=start, count=count)
        for i, (msg_id, fields) in enumerate(entries):
            msg_id = _text(msg_id)
            payload = None
            for k, v in fields.items():
                if _text(k) == "payload":
                    payload = _text(v)
                    break
            try:
                data = json.loads(payload)
            except Exception:  # noqa: BLE001 — 非 JSON 保留 raw 证据
                data = {"raw": str(payload)}
            if not _owned_by_tenant(data, tenant):
                continue
            # 非 dict 的 JSON（数组/标量）不能 ``**`` 展开；保持 raw 证据。
            if isinstance(data, dict):
                data = {"_msg_id": msg_id, **data}
            else:
                data = {"_msg_id": msg_id, "raw": data}
            tasks.append(QueueTask(index=i, data=data))
    elif key_type == "list":
        length = redis.llen(queue_name)
        items = redis.lrange(queue_name, start, start + count - 1)
        for i, item in enumerate(items):
            try:
                item_str = _text(item)
                data = json.loads(item_str)
            except Exception:  # noqa: BLE001 — 非 JSON 保留 raw 证据
                data = {"raw": str(item)}
            if not _owned_by_tenant(data, tenant):
                continue
            if not isinstance(data, dict):
                data = {"raw": data}
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

