"""租户管理服务（平台管理员操作面）。

- 租户 slug 一经创建不可改；contract_secret_* 为对外 HMAC 契约凭证，
  明文仅在创建/轮换的响应中返回一次
- 删除保护：租户内仍有流程时拒绝删除（避免误删带数据的租户）
- 删除清理：凭据（Credential 行 + 引擎导出旁文件）随租户一并删除，
  否则重建同名租户会继承旧密钥（#245）
- 运行数据清理（#255）：本地档 SQLite 四表（调度/触发历史/执行/日志）与
  集群档 Redis 租户命名空间（执行状态/取消/fence/租约/重试计数/start-dedup、
  flow 发布定义）+ 平台键里归属该租户的调度/事件/订阅条目一并清理；
  失败显式抛出（可重试），删除后写 ``disabled`` 墓碑阻止在途消息执行
"""
from __future__ import annotations

import json
import logging
import os
import re
import secrets
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

from sqlalchemy import delete, func, select

try:
    from ..models.flow import (
        CopilotThread,
        Credential,
        DEFAULT_TENANT_ID,
        Deployment,
        FlowRecord,
        LocalExecution,
        LocalLog,
        LocalSchedule,
        LocalScheduleFire,
        NodeDescriptor,
        PropertyType,
        Tenant,
        TenantMember,
    )
except ImportError:
    from models.flow import (  # type: ignore
        CopilotThread,
        Credential,
        DEFAULT_TENANT_ID,
        Deployment,
        FlowRecord,
        LocalExecution,
        LocalLog,
        LocalSchedule,
        LocalScheduleFire,
        NodeDescriptor,
        PropertyType,
        Tenant,
        TenantMember,
    )

from . import users_svc

try:
    from .tenant_write_guard import begin_guarded_transaction, lock_tenant_row
except ImportError:  # 平铺布局（cwd=backend，services 为顶层包）
    from services.tenant_write_guard import (  # type: ignore
        begin_guarded_transaction,
        lock_tenant_row,
    )

try:
    from plaita.tenant_context import tenant_namespace as _engine_tenant_namespace
except ImportError:  # 平铺布局/最小安装：与 plaita.tenant_context 同一映射规则
    def _engine_tenant_namespace(tenant_id: str) -> str:
        if not tenant_id or tenant_id == DEFAULT_TENANT_ID:
            return "plaita"
        return f"plaita:{tenant_id}"

logger = logging.getLogger(__name__)

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$")

# ---- 租户运行数据清理（plaita#255） --------------------------------------
# 平台机制键（不按租户 namespace 分区）：只能按记录/字段里的受信 tenant_id
# 逐条收窄后删除，绝不清整个键。
_SCHEDULES_KEY = "plaita:schedules"                     # HASH schedule_id -> 定义
_SCHEDULE_FIRES_KEY = "plaita:schedule:fires:{schedule_id}"
_SCHEDULE_LOCK_KEY = "plaita:schedule:lock:{schedule_id}"
_TENANT_RUNNING_KEY = "plaita:tenant_running:{tenant_id}"  # 配额名额 ZSET
_TENANT_QUOTA_KEY = "plaita:tenant_quota"               # HASH tenant_id -> 上限
_TENANT_STATUS_KEY = "plaita:tenant_status"             # 与 tenant_status 同键
_TENANT_STATUS_DISABLED = "disabled"                    # tenant_is_active 拒行值
# A50-05：共享任务队列（console 派发与 worker 消费同名，env 可覆盖）。
_TASK_QUEUE_DEFAULT = "plaita:flow:queue"

#: 集群日志 Stream（平台级键，条目内 ``tenant_id`` 归属；A54-01）。
_LOG_KEY_PATTERN = "plaita:logs:*"
_LOG_PURGE_BATCH = 256

# default 租户与平台机制键共用 ``plaita:`` 前缀：只按租户数据前缀清理，
# 不触碰队列/registry 等平台键（flow 定义逐键类型判别，见 _purge_tenant_redis）。
_DEFAULT_TENANT_KEY_PATTERNS = ("plaita:execution:*", "plaita:start-dedup:*")
_REDIS_SCAN_COUNT = 200


class TenantError(ValueError):
    """租户管理操作非法（信息面向管理员展示）。"""


def new_contract_secret() -> tuple[str, str]:
    """生成一组契约密钥（secret_id 可读性较好，secret_key 高熵）。"""
    return f"t-{secrets.token_hex(8)}", secrets.token_hex(24)


def list_tenants(store) -> List[Dict[str, Any]]:
    with store._session_local() as session:
        tenants = session.scalars(select(Tenant).order_by(Tenant.id)).all()
        counts: Dict[str, int] = {}
        for m in session.scalars(select(TenantMember)).all():
            counts[m.tenant_id] = counts.get(m.tenant_id, 0) + 1
        return [
            {
                "id": t.id,
                "name": t.name,
                "status": t.status,
                "contract_secret_id": t.contract_secret_id,
                "member_count": counts.get(t.id, 0),
                "created_at": t.created_at.isoformat() if t.created_at else None,
            }
            for t in tenants
        ]


def get_tenant(store, tenant_id: str) -> Dict[str, Any]:
    with store._session_local() as session:
        row = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        if row is None:
            raise LookupError(f"租户不存在: {tenant_id}")
        return _to_dict(row)


def tenant_generation(store, tenant_id: str) -> str | None:
    """租户世代标识（A54-02）：``tenants.created_at`` 的 ISO 时间。

    ``tenant_id`` 是安全主体标识而非数据所有权凭证——删除后同名重建是
    **新世代的新主体**。世代取创建时间（删除行即失去，重建得到新值），
    用于把旧世代派工台账/日志等审计元数据挡在新租户读面之外。与合同密钥
    ``contract_secret_id``（轮换会变）不同，世代不随合同密钥轮换漂移。
    租户不存在 ⇒ ``None``（调用方按 fail-closed 处理，不返回任何旧行）。
    """
    with store._session_local() as session:
        created = session.scalars(
            select(Tenant.created_at).where(Tenant.id == tenant_id)
        ).first()
    if created is None:
        return None
    return created.isoformat()


def create_tenant(store, tenant_id: str, name: str = "",
                  redis_client=None) -> Dict[str, Any]:
    if not _SLUG_RE.match(tenant_id or ""):
        raise TenantError(
            "租户 ID 需为 3-64 位小写字母/数字/连字符，且字母或数字开头结尾"
        )
    with store._session_local() as session:
        if session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first():
            raise TenantError(f"租户已存在: {tenant_id}")
        # A54-06：同名 create 不得继承前代数据（租户行可能已被旧版本/人工删掉
        # 但 flows/资产仍在）。拒绝门下要求显式清理/归档，绝不静默重新归属。
        leftovers = {
            name: count
            for name, count in _orphan_asset_counts(session, tenant_id).items()
            if count
        }
        if leftovers:
            detail = "、".join(f"{name}={count}" for name, count in leftovers.items())
            raise TenantError(
                f"租户 slug {tenant_id} 存在前代遗留数据（{detail}）："
                "请先显式清理/归档后再创建（不允许同名继承）"
            )
        secret_id, secret_key = new_contract_secret()
        row = Tenant(
            id=tenant_id,
            name=name or tenant_id,
            status="active",
            contract_secret_id=secret_id,
            contract_secret_key=secret_key,
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        result = _to_dict(row)
        result["contract_secret_key"] = secret_key  # 仅本次响应返回
    # plaita#255：删除写下的 disabled 墓碑必须被同名重建覆盖，否则新租户
    # 在 Redis 短视图里仍是「已停用」而被 worker/调度拦截。
    _publish_status(redis_client, tenant_id, "active")
    return result


def set_tenant_status(store, tenant_id: str, status: str, redis_client=None) -> None:
    """启用/停用租户（#27：停用必须对运行面生效，不能只改 SQLite 行）。

    - 停用：同步停掉该租户的调度（本地 + 集群档）；其已签发会话由
      ``users_svc.resolve_session`` 的租户状态闸按请求实时拦下（403）
    - 状态写入 Redis 短视图，供 worker/调度服务（可能在别的进程）跨进程拦截
    """
    if status not in ("active", "disabled"):
        raise TenantError(f"非法状态: {status}")
    with store._session_local() as session:
        row = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        if row is None:
            raise TenantError(f"租户不存在: {tenant_id}")
        row.status = status
        session.commit()
    if status == "disabled":
        _disable_tenant_schedules(store, tenant_id, redis_client)
    _publish_status(redis_client, tenant_id, status)


def _disable_tenant_schedules(store, tenant_id: str, redis_client) -> None:
    """停用租户时同步停掉其调度（本地 SQLite 档 + 集群 Redis HASH 档）。"""
    try:
        from . import local_scheduler
    except ImportError:
        local_scheduler = None  # type: ignore
    if local_scheduler is not None:
        for s in local_scheduler.list_schedules(store):
            if (s.get("tenant_id") or DEFAULT_TENANT_ID) != tenant_id:
                continue
            if not s.get("enabled"):
                continue
            s["enabled"] = False
            s["next_run_at"] = ""
            local_scheduler.update_schedule(store, s["schedule_id"], s)

    if redis_client is None:
        return
    try:
        from plaita.server.services.schedule_service import SCHEDULES_KEY, list_schedules
    except ImportError:
        return
    for s in list_schedules(redis_client):
        if (s.get("tenant_id") or DEFAULT_TENANT_ID) != tenant_id:
            continue
        if not s.get("enabled"):
            continue
        s["enabled"] = False
        s["next_run_at"] = ""
        s["updated_at"] = datetime.now().isoformat()
        redis_client.hset(
            SCHEDULES_KEY,
            key=s["schedule_id"],
            value=json.dumps(s, ensure_ascii=False),
        )


def _publish_status(redis_client, tenant_id: str, status: str) -> None:
    if redis_client is None:
        return
    try:
        from plaita.server.tenant_status import publish_tenant_status
    except ImportError:
        return
    publish_tenant_status(redis_client, tenant_id, status)


def publish_all_tenant_status(redis_client, store) -> None:
    """启动兜底：把权威库里的全部租户状态回填到 Redis 短视图。

    补齐「console 停机期间改过状态 / Redis 被清空」的漂移——否则 worker 侧
    只有 console 运行时改动的状态可见。失败静默（不阻断启动）。
    """
    if redis_client is None:
        return
    try:
        from plaita.server.tenant_status import publish_tenant_status
    except ImportError:
        return
    for t in list_tenants(store):
        publish_tenant_status(redis_client, t["id"], t["status"])


def rotate_contract_secret(store, tenant_id: str) -> Dict[str, str]:
    """轮换契约密钥，明文仅本次响应返回。"""
    with store._session_local() as session:
        row = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        if row is None:
            raise TenantError(f"租户不存在: {tenant_id}")
        secret_id, secret_key = new_contract_secret()
        row.contract_secret_id = secret_id
        row.contract_secret_key = secret_key
        session.commit()
        return {
            "tenant_id": tenant_id,
            "contract_secret_id": secret_id,
            "contract_secret_key": secret_key,
        }


def _tenant_row_ids(tenant_id: str) -> tuple:
    """SQLite 行里的租户 id 形态：default 与历史空串同义（bootstrap 前存量）。"""
    if tenant_id == DEFAULT_TENANT_ID:
        return (tenant_id, "")
    return (tenant_id,)


def _purge_local_tenant_runtime(session, tenant_id: str) -> Dict[str, int]:
    """本地单机档四表清理（plaita#255）：调度 → 触发历史 → 执行 → 日志。

    与租户行删除同一事务：任何一步失败整体回滚，租户仍在、可重试。
    ``local_schedule_fires`` 没有 tenant_id 列，按该租户调度的 schedule_id
    收窄（先取 id 再删定义，顺序不能反）。
    """
    ids = _tenant_row_ids(tenant_id)
    counts: Dict[str, int] = {}
    schedule_ids = session.scalars(
        select(LocalSchedule.schedule_id).where(LocalSchedule.tenant_id.in_(ids))
    ).all()
    if schedule_ids:
        counts["local_schedule_fires"] = session.execute(
            delete(LocalScheduleFire).where(
                LocalScheduleFire.schedule_id.in_(schedule_ids)
            )
        ).rowcount or 0
    else:
        counts["local_schedule_fires"] = 0
    for name, model in (
        ("local_schedules", LocalSchedule),
        ("local_executions", LocalExecution),
        ("local_logs", LocalLog),
    ):
        counts[name] = session.execute(
            delete(model).where(model.tenant_id.in_(ids))
        ).rowcount or 0
    return counts


def _orphan_asset_counts(session, tenant_id: str) -> Dict[str, int]:
    """该 slug 下已存在的前代租户数据（租户行不在时也不得同名继承）。

    A54-06 的判据面：``create_tenant`` 与 404 补清扫都先看这里。flow 是可
    执行资产，绝不能静默继承；其余资产（节点/属性/Copilot/部署）同名重建
    也会重新可见，同样在拒绝门下。返回 ``{表名: 行数}``。
    """
    ids = _tenant_row_ids(tenant_id)
    counts: Dict[str, int] = {}
    for name, model in (
        ("flows", FlowRecord),
        ("node_descriptors", NodeDescriptor),
        ("property_types", PropertyType),
        ("copilot_threads", CopilotThread),
        ("deployments", Deployment),
    ):
        counts[name] = int(
            session.scalar(
                select(func.count()).select_from(model).where(
                    model.tenant_id.in_(ids)
                )
            ) or 0
        )
    return counts


def _purge_tenant_db_assets(session, tenant_id: str) -> Dict[str, int]:
    """租户级 DB 资产清理（A54-03/04/05）。

    自定义节点描述、属性类型、Copilot 会话元数据、部署记录都带
    ``tenant_id`` 但不在删除事务里：同名重建后新租户 viewer 经
    ``/api/nodes``、``/api/property-types``、``/api/copilot/threads``、
    ``/api/deployments`` 读到前代资产。它们不是平台审计证据，按租户数据
    随删除事务清理；``audit_logs`` **不删**（合规证据保留），读面另行按
    世代收窄（见 ``api/audit.py``）。
    """
    ids = _tenant_row_ids(tenant_id)
    counts: Dict[str, int] = {}
    for name, model in (
        ("node_descriptors", NodeDescriptor),
        ("property_types", PropertyType),
        ("copilot_threads", CopilotThread),
        ("deployments", Deployment),
    ):
        counts[name] = session.execute(
            delete(model).where(model.tenant_id.in_(ids))
        ).rowcount or 0
    return counts


def _redis_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return value if isinstance(value, str) else str(value)


def _loads_redis_json(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode()
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def _scan_redis_keys(redis_client, pattern: str):
    """SCAN 迭代（有界批量，绝不 KEYS）；兼容 bytes/str 游标。"""
    cursor: Any = 0
    while True:
        cursor, batch = redis_client.scan(
            cursor=cursor, match=pattern, count=_REDIS_SCAN_COUNT
        )
        for key in batch:
            yield _redis_text(key)
        if cursor in (0, "0", b"0"):
            break


def _delete_redis_keys(redis_client, keys: Iterable[str]) -> int:
    batch: List[str] = []
    deleted = 0
    for key in keys:
        batch.append(key)
        if len(batch) >= _REDIS_SCAN_COUNT:
            redis_client.delete(*batch)
            deleted += len(batch)
            batch = []
    if batch:
        redis_client.delete(*batch)
        deleted += len(batch)
    return deleted


def _purge_tenant_redis_schedules(redis_client, tenant_id: str) -> int:
    """平台调度 HASH 里归属该租户的条目 + 各自的触发历史/触发锁。"""
    raw = redis_client.hgetall(_SCHEDULES_KEY)
    matched: List[str] = []
    for sid, value in raw.items():
        info = _loads_redis_json(value)
        if not isinstance(info, dict):
            logger.warning(
                "删除租户 %s：调度 %s 定义无法解析，跳过清理（需人工确认归属）",
                tenant_id, _redis_text(sid),
            )
            continue
        if (info.get("tenant_id") or DEFAULT_TENANT_ID) != tenant_id:
            continue
        matched.append(_redis_text(sid))
    if not matched:
        return 0
    redis_client.hdel(_SCHEDULES_KEY, *matched)
    for sid in matched:
        redis_client.delete(
            _SCHEDULE_FIRES_KEY.format(schedule_id=sid),
            _SCHEDULE_LOCK_KEY.format(schedule_id=sid),
        )
    return len(matched)


def _purge_tenant_redis_events(redis_client, tenant_id: str) -> int:
    """事件记录（平台键，按记录里的受信 tenant_id 收窄）与类型索引成员。

    兼容两个写入形状：console ``plaita:event:{id}`` 与引擎
    ``plaita:event:events:{id}``；两者都可能进 ``type:``/``types:`` zset。
    """
    deleted = 0
    for key in _scan_redis_keys(redis_client, "plaita:event:*"):
        if _redis_text(redis_client.type(key)) != "string":
            continue
        info = _loads_redis_json(redis_client.get(key))
        if not isinstance(info, dict):
            continue
        if (info.get("tenant_id") or DEFAULT_TENANT_ID) != tenant_id:
            continue
        event_id = _redis_text(
            info.get("event_id") or key.rsplit(":", 1)[-1]
        )
        pipe = redis_client.pipeline()
        pipe.delete(key)
        event_type = info.get("event_type")
        if event_type:
            pipe.zrem(f"plaita:event:type:{event_type}", event_id)
            pipe.zrem(f"plaita:event:types:{event_type}", event_id)
        pipe.execute()
        deleted += 1
    return deleted


def _purge_tenant_redis_subscriptions(redis_client, tenant_id: str) -> int:
    """事件订阅记录及其类型/关联/流程/节点索引、超时与去重标记。"""
    deleted = 0
    for key in _scan_redis_keys(redis_client, "plaita:subscription:data:*"):
        if _redis_text(redis_client.type(key)) != "string":
            continue
        info = _loads_redis_json(redis_client.get(key))
        if not isinstance(info, dict):
            continue
        if (info.get("tenant_id") or DEFAULT_TENANT_ID) != tenant_id:
            continue
        sid = _redis_text(info.get("subscription_id") or key.rsplit(":", 1)[-1])
        pipe = redis_client.pipeline()
        pipe.delete(key)
        if info.get("event_type"):
            pipe.srem(f"plaita:subscription:type:{info['event_type']}", sid)
        if info.get("correlation_id"):
            pipe.srem(
                f"plaita:subscription:correlation:{info['correlation_id']}", sid
            )
        if info.get("flow_id"):
            pipe.srem(f"plaita:subscription:flow:{info['flow_id']}", sid)
        if info.get("node_id"):
            pipe.srem(f"plaita:subscription:node:{info['node_id']}", sid)
        pipe.delete(f"plaita:event_filter:timeout:{sid}")
        pipe.execute()
        # 订阅处理记录/去重标记（subscription_id 为服务端 UUID，glob 安全）
        _delete_redis_keys(
            redis_client,
            _scan_redis_keys(
                redis_client, f"plaita:subscription:processed:{sid}:*"
            ),
        )
        _delete_redis_keys(
            redis_client,
            _scan_redis_keys(redis_client, f"plaita:event_filter:dedup:*:{sid}"),
        )
        deleted += 1
    return deleted


def _purge_tenant_redis(redis_client, tenant_id: str) -> Dict[str, int]:
    """集群档租户运行数据清理（plaita#255）。

    租户数据按 ``{ns}:`` 命名空间整体清理（default 与平台机制键共用前缀，
    改用受限模式 + flow 定义逐键类型判别）；平台键只删归属该租户的条目。
    任何 Redis 错误原样上抛——清理失败必须 loud，交由调用方重试。
    """
    if redis_client is None:
        return {}
    counts: Dict[str, int] = {}
    counts["schedules"] = _purge_tenant_redis_schedules(redis_client, tenant_id)

    if tenant_id == DEFAULT_TENANT_ID:
        for pattern in _DEFAULT_TENANT_KEY_PATTERNS:
            counts[pattern] = _delete_redis_keys(
                redis_client, _scan_redis_keys(redis_client, pattern)
            )
        # flow 定义是 string；队列/DLQ（plaita:flow:queue[:dlq]）是 Stream，
        # 不能被 SET 形状误删。flow_list / flow_versions:* 是精确键/索引。
        flow_keys = [
            key for key in _scan_redis_keys(redis_client, "plaita:flow:*:*")
            if _redis_text(redis_client.type(key)) == "string"
        ]
        counts["flow_definitions"] = _delete_redis_keys(redis_client, flow_keys)
        flow_index_keys = ["plaita:flow_list"]
        flow_index_keys.extend(
            _scan_redis_keys(redis_client, "plaita:flow_versions:*")
        )
        counts["flow_index"] = _delete_redis_keys(redis_client, flow_index_keys)
    else:
        counts["namespace"] = _delete_redis_keys(
            redis_client,
            _scan_redis_keys(
                redis_client, f"{_engine_tenant_namespace(tenant_id)}:*"
            ),
        )

    counts["events"] = _purge_tenant_redis_events(redis_client, tenant_id)
    counts["subscriptions"] = _purge_tenant_redis_subscriptions(
        redis_client, tenant_id
    )
    # A54-01：集群日志 Stream 是平台级键，条目内才带 tenant_id。不清理会在
    # 同名重建后让新租户 viewer 读到前代 message（/api/logs 三件套）。
    counts["logs"] = _purge_tenant_logs(redis_client, tenant_id)
    # 配额名额/上限（平台键按租户字段收窄）
    redis_client.delete(_TENANT_RUNNING_KEY.format(tenant_id=tenant_id))
    redis_client.hdel(_TENANT_QUOTA_KEY, tenant_id)
    return counts


def _task_queue_name() -> str:
    """console 派发队列名（与 api.executions._task_queue_name 同一解析规则）。"""
    return os.getenv("PLAITA_CONSOLE_TASK_QUEUE", _TASK_QUEUE_DEFAULT)


#: 显式队列 allowlist（逗号/空白分隔）：worker/调度按自定义名消费时补登记。
_TENANT_PURGE_QUEUES_ENV = "PLAITA_CONSOLE_TENANT_PURGE_QUEUES"


def _tenant_queue_names(redis_client) -> List[str]:
    """删除时要排空的队列键集合（A54-09）。

    单清 console 队列会在「worker 用 ``PLAITA_QUEUE_NAME`` 分流、或 v1/v2
    并行」时留下可执行的旧 start。集合来源：
    - console 队列（``PLAITA_CONSOLE_TASK_QUEUE``）；
    - worker/调度队列（``PLAITA_QUEUE_NAME`` / ``QUEUE_NAME``）；
    - 显式 allowlist（``PLAITA_CONSOLE_TENANT_PURGE_QUEUES``）；
    - Redis 里已存在的同族 Stream（``{base}*``，仅 Stream 类型）——覆盖
      runbook 的 ``plaita:flow:queue:v2`` 等分叉，无需 console 进程知道
      worker 的 env。
    去重保序；任何 Redis 读错误上抛（清理失败必须 loud）。
    """
    names: List[str] = []

    def _add(name: Any) -> None:
        if not isinstance(name, str):
            return
        value = name.strip()
        if value and value not in names:
            names.append(value)

    for env_name in ("PLAITA_CONSOLE_TASK_QUEUE", "PLAITA_QUEUE_NAME", "QUEUE_NAME"):
        _add(os.getenv(env_name))
    _add(_TASK_QUEUE_DEFAULT)
    for item in re.split(r"[,\s]+", os.getenv(_TENANT_PURGE_QUEUES_ENV, "")):
        _add(item)
    if redis_client is not None:
        for base in dict.fromkeys((_TASK_QUEUE_DEFAULT, _task_queue_name())):
            for key in _scan_redis_keys(redis_client, f"{base}*"):
                if _redis_text(redis_client.type(key)) == "stream":
                    _add(key)
    return names


def _purge_tenant_queue(redis_client, tenant_id: str) -> int:
    """删除共享任务队列中归属该租户的 start/resume 消息（A50-05/A54-09）。

    队列是全局 Stream（消息只带 ``tenant_id`` 字段），不在租户命名空间里；
    删除时不清，同名重建把墓碑覆盖回 active 后旧 start 会重新执行。清理
    失败必须 loud（与 #255 的 Redis 清理同一口径），否则删除会静默留下
    可执行的旧消息。A54-09：按 :func:`_tenant_queue_names` 的**键集合**
    逐个排空，不再只看 console 单键。
    """
    if redis_client is None:
        return 0
    try:
        from plaita.server.task_queue import purge_tenant_tasks
    except ImportError:
        logger.exception(
            "删除租户 %s：无法加载任务队列清理器（旧 start 消息可能留存）",
            tenant_id,
        )
        raise
    counts: Dict[str, int] = {}
    for queue_name in _tenant_queue_names(redis_client):
        counts[queue_name] = purge_tenant_tasks(
            redis_client, queue_name, tenant_id
        )
    total = sum(counts.values())
    if total:
        logger.info("删除租户 %s：任务队列清理 %s", tenant_id, counts)
    return total


def _stream_entry_tenant(entry_data: Any) -> str:
    """日志条目归属租户，与读面 ``api.logs._entry_tenant`` 同一判据。

    键/值可能是 bytes（未开 decode_responses 的客户端）；缺失或空串按
    ``default`` 处理——读面就是这么归类的，删除侧必须同口径，否则
    default 租户的旧条目永远清不掉。
    """
    if not isinstance(entry_data, dict):
        return DEFAULT_TENANT_ID
    tenant = entry_data.get("tenant_id")
    if tenant is None:
        tenant = entry_data.get(b"tenant_id")
    if isinstance(tenant, bytes):
        tenant = tenant.decode()
    return tenant or DEFAULT_TENANT_ID


def _purge_tenant_logs(redis_client, tenant_id: str) -> int:
    """删除平台级日志 Stream 中归属该租户的条目（A54-01）。

    ``plaita:logs:{service_type}:{instance_id}`` 是平台级 Stream，键不含
    租户段；租户边界只在条目字段里（``log_handler._format_record`` 写入
    ``current_tenant()``）。同名重建后旧条目的 ``tenant_id`` 重新等于新
    租户 slug ⇒ 新租户 viewer 经 ``/api/logs`` 三件套读到前代 message。
    删除时按条目收窄 XDEL（日志无消费组，不需要 XACK）；无法解析归属的
    条目不删（不误伤），非 Stream 键跳过并 WARNING。Redis 错误上抛——
    清理失败必须 loud，交由调用方重试。
    """
    if redis_client is None:
        return 0
    deleted = 0
    for key in _scan_redis_keys(redis_client, _LOG_KEY_PATTERN):
        key_type = _redis_text(redis_client.type(key))
        if key_type != "stream":
            logger.warning(
                "删除租户 %s：%s 类型为 %s 而非 Stream，跳过日志清理",
                tenant_id, key, key_type,
            )
            continue
        start = "-"
        while True:
            entries = redis_client.xrange(
                key, min=start, max="+", count=_LOG_PURGE_BATCH
            )
            if not entries:
                break
            matched = [
                _redis_text(entry_id)
                for entry_id, entry_data in entries
                if _stream_entry_tenant(entry_data) == tenant_id
            ]
            if matched:
                deleted += int(redis_client.xdel(key, *matched) or 0)
            if len(entries) < _LOG_PURGE_BATCH:
                break
            start = f"({_redis_text(entries[-1][0])}"
    return deleted


def _tombstone_tenant(redis_client, tenant_id: str) -> None:
    """删除墓碑：保留 ``disabled`` 短视图而不是 clear（#255）。

    ``tenant_is_active`` 对缺失记录 fail-open（#27/#237 兼容语义），删除时
    clear 会让仍在队列里的 start/resume 消息按 active 执行并把残留写回
    Redis。墓碑让「显式删除」与「记录缺失」在运行面可区分；同名重建由
    ``create_tenant`` 覆盖回 active。写失败必须 loud。
    """
    if redis_client is None:
        return
    try:
        redis_client.hset(
            _TENANT_STATUS_KEY, key=tenant_id, value=_TENANT_STATUS_DISABLED
        )
    except Exception:
        logger.exception(
            "删除租户 %s：运行面墓碑写入失败（在途消息可能继续执行）", tenant_id
        )
        raise


def _purge_runtime_after_commit(
    redis_client, tenant_id: str
) -> tuple[Dict[str, int], "BaseException | None"]:
    """DB 提交后的运行面清理（A2R13）。

    Redis 全量清理（SCAN 租户命名空间 + 平台键逐条收窄 + 队列遍历）可能耗时
    数秒，**绝不能**放在 SQLite ``BEGIN IMMEDIATE`` 写事务里：写锁跨清理
    期间会令并发写撞 ``database is locked``（默认 5s timeout），清理失败而
    COMMIT 失败时连接还会以 PENDING 状态回池，后续请求全站 500 直到重启。

    两步各自尝试（前一步失败不跳过后一步），汇总后再 loud 上抛。此时租户行
    已删除，重试 ``delete_tenant`` 会走 404 补清扫继续清理（#257 的「失败可
    重试、绝不伪装成功」语义）。
    """
    counts: Dict[str, int] = {}
    first_error: "BaseException | None" = None
    steps = (
        ("redis", lambda: _purge_tenant_redis(redis_client, tenant_id)),
        # 队列必须排空：旧 start 消息会在同名重建后重新执行（A50-05）。
        ("queue", lambda: _purge_tenant_queue(redis_client, tenant_id)),
    )
    for label, purge in steps:
        try:
            result = purge()
        except Exception as exc:  # noqa: BLE001 — 两步都要尝试，最后统一上抛
            logger.exception(
                "删除租户 %s：%s 运行数据清理失败（数据库删除已提交，"
                "重试 DELETE 走 404 补清扫可继续）",
                tenant_id,
                label,
            )
            if first_error is None:
                first_error = exc
        else:
            if label == "queue":
                counts["queue"] = result
            else:
                counts.update(result or {})
    return counts, first_error


def _purge_orphan_tenant_runtime(session, tenant_id: str, redis_client) -> None:
    """404 路径补清扫（#257/#255）：租户行已不在时的历史残留。

    只对非 default 租户做完整运行数据清扫——default 行缺失无法区分
    「已删除」与「尚未 bootstrap 的存量单租部署」，避免 404 误清活跃数据。
    复用调用方 session：此时没有待提交的写，先提交本地清理再清 Redis，
    避免同进程两个 SQLite 连接互相等锁。

    A2R13：墓碑先落（在途消息在清理期间即被拦下），Redis/队列清理失败
    两步都尝试后 loud 上抛——重试 404 路径继续补偿。
    """
    if tenant_id == DEFAULT_TENANT_ID:
        _tombstone_tenant(redis_client, tenant_id)
        return
    # A54-06：租户行已不在但仍有流程 ⇒ 404 补清扫必须是 loud 的拒绝门：
    # 静默返回 404 后同名 create 会把旧流程重新归给新租户（可读、可发布、
    # 可同步执行）。要求先用平台身份显式归档/清除 flows，再重试删除。
    orphan_counts = _orphan_asset_counts(session, tenant_id)
    if orphan_counts.get("flows"):
        raise TenantError(
            f"租户 {tenant_id} 行已不存在但仍有 {orphan_counts['flows']} 个"
            "流程残留：请显式归档/清除 flows 后再删除（不会留给同名重建）"
        )
    _purge_local_tenant_runtime(session, tenant_id)
    _purge_tenant_db_assets(session, tenant_id)
    session.commit()
    _tombstone_tenant(redis_client, tenant_id)
    _, purge_error = _purge_runtime_after_commit(redis_client, tenant_id)
    if purge_error is not None:
        raise purge_error


def delete_tenant(store, tenant_id: str, redis_client=None) -> bool:
    purged_credentials = 0
    local_counts: Dict[str, int] = {}
    redis_counts: Dict[str, int] = {}
    with store._session_local() as session:
        # A50-01/A50-02：写锁必须先于第一次读（SQLite 为 BEGIN IMMEDIATE；
        # PG 为下面的 SELECT ... FOR UPDATE）。删除事务从存在性检查到 commit
        # 全程串行，并发的 save_credential/add_member 要么在删除提交前完成
        # （其行随后被本事务删掉），要么在其 flush 后复检时看到租户已删。
        begin_guarded_transaction(session)
        row = lock_tenant_row(session, tenant_id)
        if row is None:
            # plaita#257：旧实现先 commit 删租户行、后删凭据旁文件；unlink
            # 失败后租户已不存在，重试只拿到 404（「租户已删、旧密钥仍可被
            # 引擎读到」的死路）。404 路径补清扫这份残留——仍删除失败则显式
            # 上抛，不得把「没清掉」伪装成「已不存在」。
            # A54-06：先过 orphan-flow 拒绝门；被拒时凭据/运行数据一字不动。
            _purge_orphan_tenant_runtime(session, tenant_id, redis_client)
            _remove_tenant_credentials_export(tenant_id)
            return False
        flows = session.scalars(
            select(FlowRecord).where(FlowRecord.tenant_id == tenant_id)
        ).all()
        if flows:
            # 拒绝门先于一切清理（#255 判据 2）：有 flow 时不清凭据/运行数据。
            raise TenantError(
                f"租户 {tenant_id} 内仍有 {len(flows)} 个流程，先清空或迁移后再删除"
            )
        for m in session.scalars(
            select(TenantMember).where(TenantMember.tenant_id == tenant_id)
        ).all():
            session.delete(m)
        # #245：凭据行与租户同事务删除——只删 Tenant 行会让重建的同名租户
        # 通过 list/get 直接继承旧密钥。
        credentials = session.scalars(
            select(Credential).where(Credential.tenant_id == tenant_id)
        ).all()
        for c in credentials:
            session.delete(c)
        purged_credentials = len(credentials)
        # #255：本地档运行数据与租户行同一事务（失败整体回滚、可重试）。
        local_counts = _purge_local_tenant_runtime(session, tenant_id)
        # A54-03/04/05：节点/属性类型/Copilot/部署同事务清理（audit 保留）。
        asset_counts = _purge_tenant_db_assets(session, tenant_id)
        session.delete(row)
        # plaita#257：删除先落到事务（flush），凭据旁文件（含密钥）清成功
        # 之后才 commit。失败方向 fail-safe：宁可「租户行还在而凭据不可达」
        # （重试可继续清理），也不可「租户已删而旧密钥仍可读」。
        session.flush()
        credentials_export = _remove_tenant_credentials_export(tenant_id)
        # A2R13：墓碑是廉价 HSET，仍先于 commit——在途消息/调度从这一刻起
        # 即被拦下；Redis 不可用时写墓碑失败 ⇒ 整单回滚（租户保留、可重试）。
        _tombstone_tenant(redis_client, tenant_id)
        session.commit()

    # A2R13：Redis 全量清理移出 SQLite 写事务。DB 删除已提交，写锁在毫秒级
    # 释放；秒级 SCAN/队列遍历不再阻塞并发写。失败 loud，租户行已删 ⇒ 重试
    # 走 404 补清扫继续补偿（#257 语义：可重试、不伪装成功）。
    redis_counts, purge_error = _purge_runtime_after_commit(
        redis_client, tenant_id
    )
    if purge_error is not None:
        logger.error(
            "删除租户 %s：数据库删除已提交，但运行面清理未完成；"
            "重试 DELETE（404 补清扫）可继续，租户不会复活",
            tenant_id,
        )
        raise purge_error

    logger.info(
        "删除租户 %s：凭据 %d 条（导出文件 %s）、本地 %s、DB 资产 %s、Redis %s",
        tenant_id,
        purged_credentials,
        credentials_export,
        local_counts,
        asset_counts,
        redis_counts,
    )
    return True


def _tenant_credentials_export(tenant_id: str) -> Path:
    """租户凭据导出路径（与 console 导出/worker 读取共用同一命名，#215）。"""
    try:
        from . import credentials_svc
    except ImportError:
        from services import credentials_svc  # type: ignore
    return credentials_svc.credentials_file(tenant_id)


def _remove_tenant_credentials_export(tenant_id: str) -> Path:
    path = _tenant_credentials_export(tenant_id)
    try:
        path.unlink()
    except FileNotFoundError:
        return path
    except OSError:
        logger.exception(
            "租户 %s 已删除，但凭据导出文件清理失败: %s", tenant_id, path
        )
        raise
    return path


def find_tenant_by_contract_secret_id(store, secret_id: str) -> Dict[str, Any]:
    """契约接口按 secret_id 定位租户（未命中抛 LookupError）。

    返回含 contract_secret_key（供验签内部使用；API 层不得外透）。
    """
    with store._session_local() as session:
        row = session.scalars(
            select(Tenant).where(Tenant.contract_secret_id == secret_id)
        ).first()
        if row is None:
            raise LookupError(f"未知契约 secret_id: {secret_id}")
        out = _to_dict(row)
        out["contract_secret_key"] = row.contract_secret_key
        return out


def _to_dict(row: Tenant) -> Dict[str, Any]:
    return {
        "id": row.id,
        "name": row.name,
        "status": row.status,
        "contract_secret_id": row.contract_secret_id,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


# 复用 users_svc 的成员管理（同一张 tenant_members 表），保持单一入口
list_tenant_members = users_svc.list_tenant_members
add_member = users_svc.add_member
set_member_role = users_svc.set_member_role
remove_member = users_svc.remove_member
