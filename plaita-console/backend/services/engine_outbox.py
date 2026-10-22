"""flow publish outbox（#299）：跨 Redis 部署的 durable 发布投递链。

## 为什么需要它

console 发布链路的引擎同步（``engine_sync``）只写 **console 进程所连的
Redis**；console 与 FlowWorker 不同机/不同 Redis 时，「发布成功、worker 找
不到 flow」，只能人工在目标机跑 ``sync_published_to_engine.py`` 批量回填。
本模块把发布事件变成**事务内 durable 记录**：publish 事务里与
``flow_versions`` 同事务写一条 outbox（publish 回滚 ⇒ outbox 不残留），
再由投递 worker **至少一次**送达目标 Redis 的
``{ns}:flow:{flow_id}:{version}``——跨环境部署不再依赖人工脚本。

## 同 Redis 部署零负担

未配置目标 Redis（``PLAITA_CONSOLE_ENGINE_OUTBOX_TARGET_REDIS_URL`` 为空，
默认）时本模块整体关闭：publish 不写 outbox、不起线程、零额外写入——
同 Redis 部署沿用 ``engine_sync`` 的进程内同步（issue 验收条款）。

## 投递语义

- **至少一次**：写入后**读回校验**（key 存在且 hash 一致）才记 delivered；
  网络抖动下最坏重复写（覆盖写同内容，天然幂等）。
- **hash 校验**：投递前对 outbox 行内 definition 重算
  ``flow-definition-json-sortkeys-v2`` 指纹与登记值比对（防写坏数据），
  送达后对目标 Redis 的**存储原文**重算同款指纹（与 worker 侧
  ``compute_flow_definition_hash`` 同口径）。
- **幂等键**：``(tenant_id, flow_id, version, definition_hash)`` 唯一约束
  ——同版本同内容重复发布不重复入队/投递；同版本换内容是新投递。
- **失败重试 + DLQ**：指数退避（2s 起、封顶 120s）；连续失败到
  ``max_attempts`` 转 ``failed``（DLQ 档，/health 暴露），failed 行按
  **慢速重排**（``failed_requeue_seconds`` 起、翻倍封顶 30min）自动重试
  ——目标 Redis 长时间不可用恢复后**自动补齐**，定义不丢。
- **对账**：启动 + 周期扫 delivered 行——目标缺 key / hash 不符（被外部
  删改）打回 pending 重投。版本/流程删除走 ``purge_for`` 显式清理，
  对账不会复活已删版本。

多租户：键空间与引擎侧同一映射（``tenant_namespace``）；投递线程对每行
以**行内租户**显式声明上下文后过 ``guard_tenant_write``（#239 phase 2
写闸），线程本身不继承 ContextVar。
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from sqlalchemy import func, select, delete as sa_delete
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from plaita.core.url_redaction import _redact_env_value
from plaita.tenant_context import (
    DEFAULT_TENANT_ID,
    TENANT_SOURCE_EXPLICIT,
    guard_tenant_write,
    reset_current_tenant,
    set_current_tenant,
)

try:
    from .engine_sync import sync_flow_to_engine
    from ..models.flow import FlowPublishOutbox
except ImportError:  # 平铺布局（cwd=backend）运行时
    from services.engine_sync import sync_flow_to_engine  # type: ignore
    from models.flow import FlowPublishOutbox  # type: ignore

from plaita.server.tenant_context import tenant_namespace

logger = logging.getLogger(__name__)

#: flow 定义指纹算法标记——与引擎 worker 侧（``plaita/server/flow_worker.py``
#: 的 ``FLOW_HASH_ALGO``）同一口径，只读对齐、本模块绝不另立算法。
OUTBOX_HASH_ALGO = "flow-definition-json-sortkeys-v2"

# ---- 配置（env 直读，口径与 engine_sync 相同：非法值 WARNING + 默认） ----

TARGET_URL_ENV = "PLAITA_CONSOLE_ENGINE_OUTBOX_TARGET_REDIS_URL"
POLL_ENV = "PLAITA_CONSOLE_ENGINE_OUTBOX_POLL_SECONDS"
RECONCILE_ENV = "PLAITA_CONSOLE_ENGINE_OUTBOX_RECONCILE_SECONDS"
MAX_ATTEMPTS_ENV = "PLAITA_CONSOLE_ENGINE_OUTBOX_MAX_ATTEMPTS"
BATCH_ENV = "PLAITA_CONSOLE_ENGINE_OUTBOX_BATCH"
FAILED_REQUEUE_ENV = "PLAITA_CONSOLE_ENGINE_OUTBOX_FAILED_REQUEUE_SECONDS"

DEFAULT_POLL_SECONDS = 2.0
DEFAULT_RECONCILE_SECONDS = 300.0
DEFAULT_MAX_ATTEMPTS = 8
DEFAULT_BATCH = 200
DEFAULT_FAILED_REQUEUE_SECONDS = 300.0

#: 快速重试退避：``base * 2^(n-1)``，封顶 120s。
_RETRY_BACKOFF_BASE_SECONDS = 2.0
_RETRY_BACKOFF_MAX_SECONDS = 120.0
#: failed 慢速重排封顶：目标长时间不可用时按此节奏反复补投（30min）。
_FAILED_REQUEUE_MAX_SECONDS = 1800.0

STATUS_PENDING = "pending"
STATUS_DELIVERED = "delivered"
STATUS_FAILED = "failed"

last_error_truncate = 300


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = None  # type: ignore[assignment]
    if value is None or value < minimum:
        logger.warning(
            "%s=%s 非法（需 ≥ %s 的数），使用默认 %r",
            name, _redact_env_value(raw), minimum, default,
        )
        return default
    return value


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = None  # type: ignore[assignment]
    if value is None or value < minimum:
        logger.warning(
            "%s=%s 非法（需 ≥ %d 的整数），使用默认 %r",
            name, _redact_env_value(raw), minimum, default,
        )
        return default
    return value


def target_redis_url() -> str:
    """目标 Redis 地址（跨环境部署的 worker 所连实例）。空串 = outbox 关闭。"""
    return (os.environ.get(TARGET_URL_ENV) or "").strip()


def is_enabled() -> bool:
    """outbox 是否启用：仅当显式配置了目标 Redis（跨 Redis 部署）。"""
    return bool(target_redis_url())


def poll_seconds() -> float:
    """投递线程轮询间隔。≤0 视为非法（投递链停摆）→ WARNING + 默认。"""
    value = _env_float(POLL_ENV, DEFAULT_POLL_SECONDS, minimum=0.0)
    if value <= 0:
        logger.warning(
            "%s<=0 非法（投递链会停摆），使用默认 %r",
            POLL_ENV, DEFAULT_POLL_SECONDS,
        )
        return DEFAULT_POLL_SECONDS
    return value


def reconcile_seconds() -> float:
    """对账间隔；0 = 关闭周期对账（启动对账仍会跑一次）。"""
    return _env_float(RECONCILE_ENV, DEFAULT_RECONCILE_SECONDS, minimum=0.0)


def max_attempts() -> int:
    return _env_int(MAX_ATTEMPTS_ENV, DEFAULT_MAX_ATTEMPTS)


def batch_size() -> int:
    return _env_int(BATCH_ENV, DEFAULT_BATCH)


def failed_requeue_seconds() -> float:
    return _env_float(FAILED_REQUEUE_ENV, DEFAULT_FAILED_REQUEUE_SECONDS, minimum=0.0)


def connect_target(url: Optional[str] = None) -> Redis:
    """建目标 Redis 客户端（decode_responses；显式连接/读超时防半开阻塞）。

    ping 失败/不可达直接抛——调用方决定是启动失败还是线程下一轮重试。
    """
    from plaita.runtime_config import (
        CONSOLE_CONNECT_TIMEOUT_SECONDS,
        CONSOLE_SOCKET_TIMEOUT_SECONDS,
    )

    client = Redis.from_url(
        url or target_redis_url(),
        decode_responses=True,
        encoding_errors="replace",
        socket_connect_timeout=CONSOLE_CONNECT_TIMEOUT_SECONDS,
        socket_timeout=CONSOLE_SOCKET_TIMEOUT_SECONDS,
    )
    client.ping()
    return client


# ---- 指纹 ----


def canonical_definition_json(definition: Any) -> str:
    """定义 → 稳定 JSON 文本。

    与引擎侧 ``plaita.server.flow_worker.canonical_flow_definition_json``
    完全同口径（sort_keys + ensure_ascii=False + default=str + 紧凑分隔符）
    ——等价性有单测钉住（``test_engine_outbox.py::test_hash_algo_parity``），
    任何一侧漂移都会被抓。
    """
    obj = json.loads(definition) if isinstance(definition, str) else definition
    return json.dumps(
        obj, sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":")
    )


def flow_definition_hash_v2(definition: Any) -> str:
    """``OUTBOX_HASH_ALGO`` 口径的 sha256（对存储中的原始定义）。"""
    return hashlib.sha256(
        canonical_definition_json(definition).encode("utf-8")
    ).hexdigest()


def engine_payload(flow_id: str, version: str, definition: Any) -> tuple:
    """复刻引擎同步的落盘工件：定义 + 注入 ``flow_id``/``version``。

    返回 ``(payload_json_str, hash_v2)``。**hash 的输入是注入后的载荷**——
    worker 侧 ``compute_flow_definition_hash`` 校验的是 Redis 里的存储原文，
    而 ``sync_flow_to_engine`` 落盘前会注入这两个键；outbox 登记的指纹必须
    与「投递后工件」一致，否则读回校验永远不等。
    """
    defn = json.loads(definition) if isinstance(definition, str) else dict(definition)
    defn["flow_id"] = flow_id
    defn["version"] = version
    payload = json.dumps(defn)
    return payload, flow_definition_hash_v2(payload)


def _stored_key_hash(raw: str) -> Optional[str]:
    """对目标 Redis 里的**存储原文**算指纹（worker 读到后就是按这个算）。"""
    try:
        return flow_definition_hash_v2(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


# ---- 发布事务内入队（flow_store.publish_version 调用；同事务、不 commit） ----


def enqueue_publish(
    session: Session, *, tenant_id: str, flow_id: str, version: str, definition: str
) -> bool:
    """在 **publish 的数据库事务内**登记一条 outbox（调用方负责 commit）。

    - 未启用（未配目标 Redis）→ no-op（同 Redis 部署零负担）；
    - definition 不可解析 → ERROR 跳过入队（坏定义本就发不出去；不阻断
      发布主流程，console 仍是权威库，修复后重发布会产生新 hash 行）；
    - 幂等键已存在（同版本同内容重复发布）→ 跳过，不重复入队；
    - 并发窗口下的唯一约束冲突经 SAVEPOINT 吞掉（只影响 outbox 行，
      不波及发布本身）。
    """
    if not is_enabled():
        return False
    try:
        # 指纹按「投递后工件」（定义 + 注入 flow_id/version）计算——worker
        # 校验的是 Redis 存储原文，见 engine_payload 的说明。
        _, definition_hash = engine_payload(flow_id, version, definition)
    except (TypeError, ValueError, json.JSONDecodeError) as e:
        logger.error(
            "outbox 入队跳过：%s@%s 定义不可解析（租户 %s）: %s",
            flow_id, version, tenant_id or "default", e,
        )
        return False

    dup = session.scalars(
        select(FlowPublishOutbox).where(
            FlowPublishOutbox.tenant_id == (tenant_id or ""),
            FlowPublishOutbox.flow_id == flow_id,
            FlowPublishOutbox.version == version,
            FlowPublishOutbox.definition_hash == definition_hash,
        )
    ).first()
    if dup is not None:
        return False

    row = FlowPublishOutbox(
        tenant_id=tenant_id or "",
        flow_id=flow_id,
        version=version,
        definition_hash=definition_hash,
        definition=definition or "",
        status=STATUS_PENDING,
    )
    try:
        # SAVEPOINT：唯一约束竞态（并发重复发布）只回滚这一行，发布事务继续。
        with session.begin_nested():
            session.add(row)
            session.flush()
        return True
    except IntegrityError:
        logger.info(
            "outbox 幂等去重：%s@%s（租户 %s）已有同 hash 行，跳过",
            flow_id, version, tenant_id or "default",
        )
        return False


def purge_for(
    session: Session, *, tenant_id: str, flow_id: str, version: Optional[str] = None
) -> int:
    """删除流程/版本时清理对应 outbox 行（防对账复活已删版本）。返回行数。"""
    stmt = sa_delete(FlowPublishOutbox).where(
        FlowPublishOutbox.tenant_id == (tenant_id or ""),
        FlowPublishOutbox.flow_id == flow_id,
    )
    if version is not None:
        stmt = stmt.where(FlowPublishOutbox.version == version)
    result = session.execute(stmt)
    return int(result.rowcount or 0)


# ---- 投递 ----


def _iter_due_rows(session: Session, limit: int, now: datetime):
    """pending / failed 且到期的行（created_at 序，先老后新）。"""
    return session.scalars(
        select(FlowPublishOutbox)
        .where(
            FlowPublishOutbox.status.in_((STATUS_PENDING, STATUS_FAILED)),
            (FlowPublishOutbox.next_attempt_at.is_(None))
            | (FlowPublishOutbox.next_attempt_at <= now),
        )
        .order_by(FlowPublishOutbox.id.asc())
        .limit(limit)
    ).all()


def _deliver_one(target: Redis, row: FlowPublishOutbox, now: datetime) -> Optional[str]:
    """单行投递。返回 None=成功；否则返回具名失败原因（不落库，由调用方记账）。

    读回校验通过才算 delivered——「至少一次」的确认点在目标 Redis 侧，
    不在 write 命令的返回值。
    """
    # 行内定义先自检：hash 对不上 = 库内数据被改坏，绝不写进目标 Redis。
    try:
        _, expected = engine_payload(row.flow_id, row.version, row.definition)
    except (TypeError, ValueError, json.JSONDecodeError) as e:
        return f"definition_unparsable: {e}"
    if expected != row.definition_hash:
        return (
            f"definition_hash mismatch: row登记 {row.definition_hash[:12]}… "
            f"≠ 重算 {expected[:12]}…（库内定义与指纹不一致，拒绝投递）"
        )

    # 投递线程不继承 ContextVar：以行内租户显式声明后过写闸（#239 phase 2）。
    token = set_current_tenant(row.tenant_id or DEFAULT_TENANT_ID,
                               source=TENANT_SOURCE_EXPLICIT)
    try:
        guard_tenant_write("engine_outbox.deliver")
        ok = sync_flow_to_engine(
            target, row.flow_id, row.version, row.definition,
            tenant_id=row.tenant_id or "",
        )
    except (RedisConnectionError, RedisTimeoutError, OSError) as e:
        return f"redis_unavailable: {e}"
    except Exception as e:  # noqa: BLE001 — 单行失败不拖垮整批
        return f"deliver_error: {e}"
    finally:
        reset_current_tenant(token)
    if not ok:
        # sync_flow_to_engine 内部吞异常只回 False：补一次 ping 区分
        # 「连接级不可达」（可重试、last_error 留因）与其它数据级失败。
        try:
            target.ping()
        except (RedisConnectionError, RedisTimeoutError, OSError) as e:
            return f"redis_unavailable: {e}"
        return "sync_flow_to_engine returned False"

    ns = tenant_namespace(row.tenant_id or "")
    key = f"{ns}:flow:{row.flow_id}:{row.version}"
    try:
        raw = target.get(key)
    except (RedisConnectionError, RedisTimeoutError, OSError) as e:
        return f"verify_read_failed: {e}"
    if raw is None:
        return f"verify_missing: {key} 写后读回缺失"
    actual = _stored_key_hash(raw)
    if actual is None:
        return f"verify_unparsable: {key} 存储原文不是合法 JSON"
    if actual != row.definition_hash:
        return (
            f"verify_hash_mismatch: {key} 目标侧指纹 {actual[:12]}… "
            f"≠ 登记 {row.definition_hash[:12]}…"
        )
    return None


def run_delivery_cycle(
    target: Redis,
    session_local: sessionmaker,
    *,
    now: Optional[datetime] = None,
    limit: Optional[int] = None,
    attempts_cap: Optional[int] = None,
) -> Dict[str, Any]:
    """一轮投递：pending/failed(到期) 行逐条送达目标 Redis，逐行提交。

    返回统计（``scanned/delivered/retried/to_dlq/errors``）。任何意外都被
    计入 errors 而不抛出——投递线程按 poll 节奏自愈。
    """
    now = now or datetime.utcnow()
    limit = limit or batch_size()
    attempts_cap = attempts_cap or max_attempts()
    stats: Dict[str, Any] = {
        "scanned": 0, "delivered": 0, "retried": 0, "to_dlq": 0, "errors": 0,
    }
    try:
        with session_local() as session:
            rows = _iter_due_rows(session, limit, now)
            for row in rows:
                stats["scanned"] += 1
                reason = _deliver_one(target, row, now)
                row.attempts = (row.attempts or 0) + 1
                if reason is None:
                    row.status = STATUS_DELIVERED
                    row.delivered_at = now
                    row.next_attempt_at = None
                    row.last_error = ""
                    stats["delivered"] += 1
                    logger.info(
                        "outbox 已投递 %s@%s（租户 %s，attempts=%s）",
                        row.flow_id, row.version, row.tenant_id or "default",
                        row.attempts,
                    )
                else:
                    row.last_error = reason[:last_error_truncate]
                    if row.attempts >= attempts_cap:
                        row.status = STATUS_FAILED
                        # DLQ 档仍按慢速重排自动补投（封顶 30min），不丢定义。
                        row.next_attempt_at = now + timedelta(seconds=min(
                            _failed_requeue_backoff(row.attempts),
                            _FAILED_REQUEUE_MAX_SECONDS,
                        ))
                        stats["to_dlq"] += 1
                        logger.error(
                            "outbox 投递转 DLQ：%s@%s（租户 %s）attempts=%s reason=%s；"
                            "将按慢速重排自动补投，/health.engine_outbox 可观测",
                            row.flow_id, row.version, row.tenant_id or "default",
                            row.attempts, reason,
                        )
                    else:
                        row.next_attempt_at = now + timedelta(seconds=min(
                            _RETRY_BACKOFF_BASE_SECONDS * (2 ** (row.attempts - 1)),
                            _RETRY_BACKOFF_MAX_SECONDS,
                        ))
                        stats["retried"] += 1
                        logger.warning(
                            "outbox 投递失败（将退避重试）：%s@%s（租户 %s）"
                            "attempts=%s/%s reason=%s",
                            row.flow_id, row.version, row.tenant_id or "default",
                            row.attempts, attempts_cap, reason,
                        )
                    stats["errors"] += 1
                session.commit()
    except Exception as e:  # noqa: BLE001 — 读库/会话级失败：下轮重试
        stats["errors"] += 1
        logger.warning("outbox 投递轮次失败（下轮重试）: %s", e, exc_info=True)
    return stats


def _failed_requeue_backoff(attempts: int) -> float:
    """failed 档重排间隔：从 ``failed_requeue_seconds`` 起、每轮翻倍封顶。"""
    base = max(1.0, failed_requeue_seconds())
    over = max(0, attempts - _env_int(MAX_ATTEMPTS_ENV, DEFAULT_MAX_ATTEMPTS))
    return min(base * (2 ** min(over, 8)), _FAILED_REQUEUE_MAX_SECONDS)


# ---- 对账 ----


def run_reconcile_cycle(
    target: Redis,
    session_local: sessionmaker,
    *,
    now: Optional[datetime] = None,
    max_rows: Optional[int] = None,
) -> Dict[str, Any]:
    """一轮对账：delivered 行在目标 Redis 缺 key / hash 不符 → 打回 pending 重投。

    有界（``max_rows``，默认 batch_size）；行多时按 id 序分轮消化。
    """
    now = now or datetime.utcnow()
    max_rows = max_rows or batch_size()
    stats: Dict[str, Any] = {"checked": 0, "requeued": 0, "errors": 0}
    try:
        with session_local() as session:
            rows = session.scalars(
                select(FlowPublishOutbox)
                .where(FlowPublishOutbox.status == STATUS_DELIVERED)
                .order_by(FlowPublishOutbox.id.asc())
                .limit(max_rows)
            ).all()
            ns_cache: Dict[str, str] = {}
            for row in rows:
                stats["checked"] += 1
                tenant = row.tenant_id or ""
                if tenant not in ns_cache:
                    ns_cache[tenant] = tenant_namespace(tenant)
                key = f"{ns_cache[tenant]}:flow:{row.flow_id}:{row.version}"
                try:
                    raw = target.get(key)
                except (RedisConnectionError, RedisTimeoutError, OSError) as e:
                    # 连接级失败：本轮对账到此为止，剩余行留给下轮（不计错）。
                    logger.warning(
                        "outbox 对账中断（目标 Redis 不可达）: %s", e,
                    )
                    break
                if raw is not None:
                    actual = _stored_key_hash(raw)
                    if actual == row.definition_hash:
                        continue
                stats["requeued"] += 1
                row.status = STATUS_PENDING
                row.delivered_at = None
                row.attempts = 0
                row.next_attempt_at = None
                missing = raw is None
                row.last_error = (
                    f"reconcile: 目标 key {'缺失' if missing else 'hash 不符'} "
                    f"({key})，已打回 pending 重投 @ {now.isoformat()}"
                )[:last_error_truncate]
                logger.warning(
                    "outbox 对账重投：%s@%s（租户 %s）目标 %s",
                    row.flow_id, row.version, tenant or "default",
                    "缺 key" if missing else "hash 不符",
                )
            session.commit()
    except Exception as e:  # noqa: BLE001 — 读库失败：下轮重试
        stats["errors"] += 1
        logger.warning("outbox 对账轮次失败（下轮重试）: %s", e, exc_info=True)
    return stats


# ---- 观测 ----


def stats_snapshot(session_local: sessionmaker) -> Dict[str, Any]:
    """/health.engine_outbox 的 backlog 视图（pending/failed 计数 + 最近错误）。"""
    snapshot: Dict[str, Any] = {
        "pending": 0, "failed": 0,
        "last_delivered_at": None, "last_error": None,
    }
    try:
        with session_local() as session:
            for status in (STATUS_PENDING, STATUS_FAILED):
                snapshot[status] = session.scalar(
                    select(func.count())
                    .select_from(FlowPublishOutbox)
                    .where(FlowPublishOutbox.status == status)
                ) or 0
            last_delivered = session.scalars(
                select(FlowPublishOutbox)
                .where(FlowPublishOutbox.status == STATUS_DELIVERED)
                .order_by(FlowPublishOutbox.delivered_at.desc())
                .limit(1)
            ).first()
            if last_delivered is not None and last_delivered.delivered_at is not None:
                snapshot["last_delivered_at"] = last_delivered.delivered_at.isoformat()
            last_error_row = session.scalars(
                select(FlowPublishOutbox)
                .where(FlowPublishOutbox.last_error != "")
                .order_by(FlowPublishOutbox.id.desc())
                .limit(1)
            ).first()
            if last_error_row is not None:
                snapshot["last_error"] = last_error_row.last_error
    except Exception as e:  # noqa: BLE001 — 健康读数失败不抛
        snapshot["error"] = str(e)[:last_error_truncate]
    return snapshot
