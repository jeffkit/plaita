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
import re
import secrets
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

from sqlalchemy import delete, select

try:
    from ..models.flow import (
        Credential,
        DEFAULT_TENANT_ID,
        FlowRecord,
        LocalExecution,
        LocalLog,
        LocalSchedule,
        LocalScheduleFire,
        Tenant,
        TenantMember,
    )
except ImportError:
    from models.flow import (  # type: ignore
        Credential,
        DEFAULT_TENANT_ID,
        FlowRecord,
        LocalExecution,
        LocalLog,
        LocalSchedule,
        LocalScheduleFire,
        Tenant,
        TenantMember,
    )

from . import users_svc

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


def create_tenant(store, tenant_id: str, name: str = "",
                  redis_client=None) -> Dict[str, Any]:
    if not _SLUG_RE.match(tenant_id or ""):
        raise TenantError(
            "租户 ID 需为 3-64 位小写字母/数字/连字符，且字母或数字开头结尾"
        )
    with store._session_local() as session:
        if session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first():
            raise TenantError(f"租户已存在: {tenant_id}")
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
    # 配额名额/上限（平台键按租户字段收窄）
    redis_client.delete(_TENANT_RUNNING_KEY.format(tenant_id=tenant_id))
    redis_client.hdel(_TENANT_QUOTA_KEY, tenant_id)
    return counts


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


def _purge_orphan_tenant_runtime(session, tenant_id: str, redis_client) -> None:
    """404 路径补清扫（#257/#255）：租户行已不在时的历史残留。

    只对非 default 租户做完整运行数据清扫——default 行缺失无法区分
    「已删除」与「尚未 bootstrap 的存量单租部署」，避免 404 误清活跃数据。
    复用调用方 session：此时没有待提交的写，先提交本地清理再清 Redis，
    避免同进程两个 SQLite 连接互相等锁。
    """
    if tenant_id == DEFAULT_TENANT_ID:
        _tombstone_tenant(redis_client, tenant_id)
        return
    _purge_local_tenant_runtime(session, tenant_id)
    session.commit()
    _purge_tenant_redis(redis_client, tenant_id)
    _tombstone_tenant(redis_client, tenant_id)


def delete_tenant(store, tenant_id: str, redis_client=None) -> bool:
    purged_credentials = 0
    local_counts: Dict[str, int] = {}
    redis_counts: Dict[str, int] = {}
    with store._session_local() as session:
        row = session.scalars(select(Tenant).where(Tenant.id == tenant_id)).first()
        if row is None:
            # plaita#257：旧实现先 commit 删租户行、后删凭据旁文件；unlink
            # 失败后租户已不存在，重试只拿到 404（「租户已删、旧密钥仍可被
            # 引擎读到」的死路）。404 路径补清扫这份残留——仍删除失败则显式
            # 上抛，不得把「没清掉」伪装成「已不存在」。
            _remove_tenant_credentials_export(tenant_id)
            _purge_orphan_tenant_runtime(session, tenant_id, redis_client)
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
        session.delete(row)
        # plaita#257：删除先落到事务（flush），凭据旁文件（含密钥）清成功
        # 之后才 commit。失败方向 fail-safe：宁可「租户行还在而凭据不可达」
        # （重试可继续清理），也不可「租户已删而旧密钥仍可读」。
        session.flush()
        credentials_export = _remove_tenant_credentials_export(tenant_id)
        # #255：Redis 清理与墓碑同样先于 commit——任一失败都保留租户行，
        # 重试可继续清理；commit 成功后不存在需要 404 才能补清的中间态。
        redis_counts = _purge_tenant_redis(redis_client, tenant_id)
        _tombstone_tenant(redis_client, tenant_id)
        session.commit()

    logger.info(
        "删除租户 %s：凭据 %d 条（导出文件 %s）、本地 %s、Redis %s",
        tenant_id,
        purged_credentials,
        credentials_export,
        local_counts,
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
