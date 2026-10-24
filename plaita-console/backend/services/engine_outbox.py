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
- **对账（#302 扩口径）**：启动 + 周期扫 delivered 行，每行**先对源表**：
  重读 ``flow_versions.definition`` 现值重算指纹与行内 ``definition_hash``
  比对——分歧 = 源表被 DB 级修改（API 面禁止覆盖 published，绕开 API 的
  写入是唯一来源），行内副本已过期：按「console 是权威库」口径以**源表
  现值**更新行并打回 pending 重投（``drift_count`` 累计、/health 暴露
  ``drift_total``）；源表行已删除（删除竞态漏 purge 的残留）则按删除语义
  收尾——清目标三件 + 删行。**后对目标**：目标缺 key / hash 不符（被外部
  删改）打回 pending 重投。版本/流程删除走 ``purge_for`` 显式清理，
  对账不会复活已删版本。
- **删除投递语义（#301/#303）**：删除流程/版本时除清 DB 行外，同步清**目标
  Redis** 侧引擎存储三件（``{ns}:flow:{fid}:{ver}``、``flow_versions:{fid}``
  注册集、``flow_list`` 成员）——复用 ``RedisFlowStorage.delete_flow``、
  与投递同源配置（``TARGET_REDIS_URL``）。尽力而为：目标不可达 ERROR +
  留待人工（此时行已 purge、对账不再覆盖，幽灵 key 需手工清理）。
- **删除协议串行化（#303，口径一：删除完成后目标侧不复活）**：「先清
  目标、后删行」（#301）防住了清理失败留幽灵，防不住对偶方向——清 key
  之后、删行 commit 之前，投递/对账线程把 key 复活（打回 pending → 重投），
  删行 purge 后对账永不覆盖、零日志。收口 = 行级 tombstone + 进程内互斥
  双保险：删除经 ``deleting_guard`` 全程持 ``OUTBOX_DELETE_LOCK``——锁内
  先标记行 ``deleting``（独立事务提交；投递/对账扫描与逐行动作对
  deleting 行一律跳过）→ 清目标三件 → 删除事务（purge 行）；投递/对账
  逐行动作前也在锁内**重读行现态**（``_reload_row``，防跨事务陈旧读）。
  删除事务失败回滚 → 守卫按快照恢复行原状（#301「先清后删」的自愈方向
  保留）；进程死在「已标记、未删行」之间 → 启动恢复
  ``recover_deleting_rows`` 把 deleting 行打回 pending，交还常规
  投递/对账自愈（源表仍在 → 重投收敛；源表已删 → source-missing 收尾）。
- **DLQ 人工出口（c117-06）**：``resolve``（认定无需再投递，置
  ``resolved`` 终态、留痕）/ ``discard``（放弃投递并删行），见
  ``api/flows.py`` 的 ``POST /flows/publish-outbox/{id}/resolve|discard``。
  人工出口同样在 :data:`OUTBOX_DELETE_LOCK` 内行动（A2R50-02）：
  删除协议标记 tombstone 与出口的状态写竞争时，锁内重读现态必见
  ``deleting``/已删，按既有 409/404 语义拒——出口不再可能盖掉
  tombstone 或抢在删除事务前删行（删除失败回滚后行不可自愈的幽灵链）。
- **按行租约（#324，解除单副本约束）**：投递/对账的**逐行动作**前先抢
  ``{ns}:outbox:lease:{行id}``（SET NX PX，TTL 短于最小重试退避）——
  多副本共享同一数据库 + 目标 Redis 时**谁抢到谁投**；他副本持有中则
  本轮跳过该行、不动行态，``attempts``/退避/DLQ 记账只由持有者写
  （单副本时代的互覆盖失效）。租约面不可达 = fail-open 照常行动（投递
  自身对死目标具名失败，记账语义不变）。TTL 过期自愈持有者崩溃，正常
  路径动作完成即显式释放（token 比对防误删他副本租约）。

**多副本约定（#324，取代 b117-03 单副本约定）**：投递/对账经按行租约
支持多 console 副本并发（不做全局 leader，#324 边界）；残余重复投递窗 =
持有者在 TTL 内崩溃且目标写入已生效，或租约面瞬断走 fail-open——读回
校验 + 幂等覆盖写兜底，至少一次语义不变。**删除协议（``deleting_guard``）
仍为进程内互斥**：
:data:`OUTBOX_DELETE_LOCK` 不跨进程，多副本**同时删除**同一流程/版本与
他副本投递之间存在小窗（投递锁内重读早于删除标记提交的陈旧读）——部署
上避免多副本并发删除同一流程/版本；将删除协议纳入跨副本互斥超出 #324
边界（不做全局 leader、不改 outbox 表结构），如需另立单。

多租户：键空间与引擎侧同一映射（``tenant_namespace``）；投递线程对每行
以**行内租户**显式声明上下文后过 ``guard_tenant_write``（#239 phase 2
写闸），线程本身不继承 ContextVar。
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError
from sqlalchemy import func, select, delete as sa_delete
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.exc import ObjectDeletedError

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
    from ..models.flow import FlowPublishOutbox, FlowVersion
except ImportError:  # 平铺布局（cwd=backend）运行时
    from services.engine_sync import sync_flow_to_engine  # type: ignore
    from models.flow import FlowPublishOutbox, FlowVersion  # type: ignore

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
LEASE_TTL_ENV = "PLAITA_CONSOLE_ENGINE_OUTBOX_LEASE_TTL_SECONDS"

DEFAULT_POLL_SECONDS = 2.0
DEFAULT_RECONCILE_SECONDS = 300.0
DEFAULT_MAX_ATTEMPTS = 8
DEFAULT_BATCH = 200
DEFAULT_FAILED_REQUEUE_SECONDS = 300.0
#: 按行租约 TTL（#324）：必须短于最小重试退避（``_RETRY_BACKOFF_BASE_SECONDS``
#: 2s）——持有者崩溃后租约至多滞留 TTL，绝不阻塞到期重试（正文验收口径
#: 「TTL 短于重试间隔」）。
DEFAULT_LEASE_TTL_SECONDS = 1.5

#: 快速重试退避：``base * 2^(n-1)``，封顶 120s。
_RETRY_BACKOFF_BASE_SECONDS = 2.0
_RETRY_BACKOFF_MAX_SECONDS = 120.0
#: failed 慢速重排封顶：目标长时间不可用时按此节奏反复补投（30min）。
_FAILED_REQUEUE_MAX_SECONDS = 1800.0

STATUS_PENDING = "pending"
STATUS_DELIVERED = "delivered"
STATUS_FAILED = "failed"
#: DLQ 人工出口终态（c117-06）：不投递、不对账、只留痕。
STATUS_RESOLVED = "resolved"
#: 删除协议 tombstone（#303）：删除事务 commit 前的行级「删除中」标记。
#: 投递/对账扫描与逐行动作对 deleting 行一律跳过——清目标 key 之后、
#: 删行 commit 之前，任何线程都不得再触碰该行（对偶破口封死）。
STATUS_DELETING = "deleting"

#: 删除协议与投递/对账/人工出口的进程内互斥锁（#303；A2R50-02 收口）。
#: #324 后投递/对账的**跨副本**互斥由按行租约承担，本锁仍是进程内串行面：
#: 删除守卫全程持锁，投递/对账/人工出口（resolve/discard）在**行动粒度**
#: 上持锁并重读行现态——同进程各面在单行动作上串行，删除窗口内不可能有
#: 投递写出 key、也不可能有人工出口盖掉 tombstone。跨副本「同时删除」
#: 与他副本投递的残留小窗见模块文档串「多副本约定」。持锁段均为单行动作
#: （毫秒级），等待方最多让出一个行动位，不饿死。
OUTBOX_DELETE_LOCK = threading.RLock()

last_error_truncate = 300


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = None  # type: ignore[assignment]
    # b117-04：inf/nan 一并具名拒——``value < minimum`` 对两者恒 False，
    # 修前会静默放行（poll=inf → Event.wait(inf) 投递链停摆、reconcile=inf
    # → 周期对账静默失效）；与 PLAITA_HTTP_DNS_TTL 的「nan/inf → 拦截」
    # 先例同口径（非法值矩阵不再漏网）。
    if value is None or not math.isfinite(value) or value < minimum:
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


def lease_ttl_seconds() -> float:
    """按行租约 TTL（秒，#324）。合法域 ``0 < ttl < 快速重试基线``。

    上界=「TTL 短于重试间隔」的规格口径：TTL ≥ 最小退避（2s）时，崩溃
    持有者的滞留租约会顶掉到期重试的首轮扫描（饿死一个 poll 周期以上）
    ——非法（≤0 / ≥2s / inf/nan，:func:`_env_float` 已拒后两类）一律
    WARNING + 回退默认，绝不静默。
    """
    value = _env_float(LEASE_TTL_ENV, DEFAULT_LEASE_TTL_SECONDS, minimum=0.0)
    if value <= 0 or value >= _RETRY_BACKOFF_BASE_SECONDS:
        logger.warning(
            "%s=%s 非法（需 0 < ttl < %s 秒，短于最小重试退避），使用默认 %r",
            LEASE_TTL_ENV, _redact_env_value(os.environ.get(LEASE_TTL_ENV)),
            _RETRY_BACKOFF_BASE_SECONDS, DEFAULT_LEASE_TTL_SECONDS,
        )
        return DEFAULT_LEASE_TTL_SECONDS
    return value


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


class DefinitionUnencodableError(ValueError):
    """定义含不可 UTF-8 编码字符，canonical hash 不可计算（卡 A-1 / A129-01）。

    典型来源：ES2019 well-formed ``JSON.stringify`` 把孤立代理写成
    ``\\udXXX`` 转义——合法 JSON、``json.loads`` 照收，但 canonical 形
    （ensure_ascii=False）``.encode("utf-8")`` 抛 ``UnicodeEncodeError``。
    这类定义 Python 池 ``compute_flow_definition_hash`` 每次必炸（毒丸），
    Go 池 canonicaljson 解码层拒载（plaita-go G-1）——两池皆不可执行，
    必须拒于发布/同步面，而不是落盘后拖到运行期才炸。
    """


def ensure_definition_encodable(flow_id: str, version: str, definition: Any) -> None:
    """对「投递后工件」跑同款 hash 计算，编码不可行即具名上抛。

    复用 :func:`engine_payload`（定义 + 注入 flow_id/version）——worker
    校验的正是这个工件；hash 计算/编码失败（``UnicodeEncodeError``）在此
    收口为 :class:`DefinitionUnencodableError`，发布闸与 save 面据此拒绝。
    不改变 hash 算法本身。定义不可解析（JSONDecodeError/TypeError）**不属
    本闸管辖**：既有「定义不可解析 → 跳过入队」语义由调用方照旧处理。
    """
    try:
        engine_payload(flow_id, version, definition)
    except UnicodeEncodeError as e:
        raise DefinitionUnencodableError(
            f"流程 {flow_id}@{version} 定义含不可 UTF-8 编码字符"
            f"（如孤立代理转义 \\udXXX，位置 {e.start}）：canonical hash "
            "无法计算，Python/Go 池均不可执行，请修正定义后重新保存"
        ) from e
    except (TypeError, ValueError):
        return


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
    - definition **不可 UTF-8 编码**（孤立代理转义，canonical hash 算不出）
      → 具名上抛 :class:`DefinitionUnencodableError`（卡 A-1）：此前它作为
      ``ValueError`` 子类被上一条吞掉——outbox 无行、发布照常成功、Redis
      却有键，Python 池读回即毒丸。发布端点在状态翻转前前置同款闸
      （422 ``definition_unencodable``），此处是绕开 API 调用面的兜底；
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
    except UnicodeEncodeError as e:
        # UnicodeEncodeError ⊂ ValueError：必须排在宽口 except 之前，否则
        # 又被吞成「不可解析」。
        raise DefinitionUnencodableError(
            f"流程 {flow_id}@{version} 定义含不可 UTF-8 编码字符"
            f"（如孤立代理转义 \\udXXX，位置 {e.start}）：canonical hash "
            "无法计算，拒绝入队"
        ) from e
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


# ---- 删除链路的目标侧清理（#301） ----


def _purge_target_storage(
    client: Redis, *, tenant_id: str, flow_id: str, version: Optional[str]
) -> bool:
    """对给定目标 Redis 清引擎存储三件：版本 key、``flow_versions`` 注册集、
    ``flow_list`` 成员——复用引擎存储自身的 ``delete_flow``（与投递落盘
    ``sync_flow_to_engine`` 同一存储抽象，不另立清理逻辑）。

    返回 ``False`` = 目标侧本就无此流程/版本（从未投递或已清过），非错误。
    """
    from plaita.storage.redis import RedisFlowStorage

    storage = RedisFlowStorage(
        client=client, namespace=tenant_namespace(tenant_id or "")
    )
    return bool(storage.delete_flow(flow_id, version=version))


def purge_target_keys(
    flow_id: str,
    *,
    tenant_id: str = "",
    version: Optional[str] = None,
    client: Optional[Redis] = None,
) -> Optional[bool]:
    """删除流程/版本时同步清**目标 Redis**侧引擎存储（#301）。

    outbox 投递过的 key 与 ``RedisFlowStorage.save_flow`` 一并维护的
    ``flow_list`` 成员 / ``flow_versions:{fid}`` 注册集，只清 DB 行会在
    目标侧永驻（幽灵 key）。本函数与投递**同源配置**（
    ``TARGET_REDIS_URL``）建连（调用方可传 lifespan 缓存的连接复用）。

    尽力而为：outbox 未启用（同 Redis 部署）→ ``None`` no-op；目标不可达
    → ERROR（降级必须可观测）+ ``None``，不抛、不阻断删除——此时行已随
    删除事务 purge，对账不再覆盖该 key，恢复后需人工对齐或在目标可达后
    重试同 key 删除。返回 ``None``=no-op/失败，``bool``=``delete_flow`` 结果。
    """
    if not is_enabled():
        return None
    token = set_current_tenant(tenant_id or DEFAULT_TENANT_ID,
                               source=TENANT_SOURCE_EXPLICIT)
    try:
        # 与投递路径同一写闸（#239 phase 2）：删除也是对引擎存储的写。
        guard_tenant_write("engine_outbox.purge_target")
        if client is None:
            try:
                client = connect_target()
            except Exception as e:  # noqa: BLE001 — 建连失败按不可达处理
                logger.error(
                    "outbox 目标侧删除清理失败（目标 Redis 不可达，行已 purge、"
                    "对账不再覆盖；目标恢复后需人工清理幽灵 key %s@%s，租户 %s）: %s",
                    flow_id, version or "*", tenant_id or "default", e,
                )
                return None
        try:
            return _purge_target_storage(
                client, tenant_id=tenant_id, flow_id=flow_id, version=version
            )
        except (RedisConnectionError, RedisTimeoutError, OSError) as e:
            logger.error(
                "outbox 目标侧删除清理失败（目标 Redis 写入异常，行已 purge、"
                "对账不再覆盖；目标恢复后需人工清理幽灵 key %s@%s，租户 %s）: %s",
                flow_id, version or "*", tenant_id or "default", e,
            )
            return None
    finally:
        reset_current_tenant(token)


# ---- 删除协议串行化（#303：tombstone + 互斥锁） ----


def _outbox_scope(tenant_id: str, flow_id: str, version: Optional[str]):
    """按租户/流程（可选版本）过滤 outbox 行的公共 WHERE。"""
    stmt = select(FlowPublishOutbox).where(
        FlowPublishOutbox.tenant_id == (tenant_id or ""),
        FlowPublishOutbox.flow_id == flow_id,
    )
    if version is not None:
        stmt = stmt.where(FlowPublishOutbox.version == version)
    return stmt


def mark_deleting(
    session_local: sessionmaker, *, tenant_id: str, flow_id: str,
    version: Optional[str] = None,
) -> List[Tuple[int, str]]:
    """删除协议第 0 步：把相关 outbox 行标记为 ``deleting``（独立事务提交）。

    tombstone 语义：标记一提交，投递/对账的扫描与逐行动作即全部跳过这些
    行——清目标 key 之后不可能再被重投复活。返回 ``(id, 原状态)`` 快照，
    供删除事务失败时 :func:`restore_deleting` 精确恢复。调用方必须持有
    :data:`OUTBOX_DELETE_LOCK`（``deleting_guard`` 已代持）。
    """
    with session_local() as session:
        rows = session.scalars(
            _outbox_scope(tenant_id, flow_id, version).where(
                FlowPublishOutbox.status != STATUS_DELETING)
        ).all()
        snapshot = [(r.id, r.status) for r in rows]
        for r in rows:
            r.status = STATUS_DELETING
        session.commit()
    if snapshot:
        logger.info(
            "outbox 删除协议：已标记 %d 行 deleting（tombstone）flow=%s@%s（租户 %s）",
            len(snapshot), flow_id, version or "*", tenant_id or "default",
        )
    return snapshot


def restore_deleting(
    session_local: sessionmaker,
    snapshot: List[Tuple[int, str]],
    *,
    tenant_id: str, flow_id: str, version: Optional[str] = None,
) -> int:
    """删除事务失败回滚：仍处 ``deleting`` 的行按快照恢复原状态。

    只恢复快照内且仍为 deleting 的行（已 purge 的行自然无需要恢复）；
    delivered 行恢复 delivered——「先清后删」失败回滚后由对账重投自愈
    （#301 自愈方向保留）。返回恢复行数。调用方须持锁。
    """
    if not snapshot:
        return 0
    prior = dict(snapshot)
    with session_local() as session:
        rows = session.scalars(
            _outbox_scope(tenant_id, flow_id, version).where(
                FlowPublishOutbox.status == STATUS_DELETING)
        ).all()
        n = 0
        for r in rows:
            before = prior.get(r.id)
            if before is not None and before != STATUS_DELETING:
                r.status = before
                n += 1
        session.commit()
    if n:
        logger.info(
            "outbox 删除协议回滚：%d 行 deleting 已恢复原状 flow=%s@%s（租户 %s）",
            n, flow_id, version or "*", tenant_id or "default",
        )
    return n


def recover_deleting_rows(session_local: sessionmaker) -> int:
    """启动恢复：一律把 ``deleting`` 行打回 pending（进程崩溃自愈）。

    进程内删除协议不跨进程存活——启动时仍处 deleting 的行 = 上次进程死在
    「已标记、未删行」之间（删除未 commit，flow_versions/flow_records 源表
    行仍在）。打回 pending 交还常规机器：投递重投收敛（key 被清过则补齐）；
    若源表其实已另行删除，对账 source-missing 分支按删除语义收尾。返回恢复
    行数（供启动日志）。resolved 行被打回属已知取舍：崩溃窗口内无法还原
    「人工出口」语义，重投为幂等覆盖写，需要时人工再 resolve。
    """
    with session_local() as session:
        rows = session.scalars(
            select(FlowPublishOutbox).where(
                FlowPublishOutbox.status == STATUS_DELETING)
        ).all()
        for r in rows:
            r.status = STATUS_PENDING
            r.next_attempt_at = None
            r.last_error = (
                "recovered: 启动时仍处 deleting（上次删除未完成），已打回重投 "
                f"@ {datetime.utcnow().isoformat()}"
            )[:last_error_truncate]
        session.commit()
        n = len(rows)
    if n:
        logger.warning(
            "outbox 启动恢复：%d 行 deleting（上次删除未完成）已打回 pending",
            n,
        )
    return n


@contextmanager
def deleting_guard(
    session_local: sessionmaker, *, tenant_id: str, flow_id: str,
    version: Optional[str] = None,
):
    """删除流程/版本的 outbox 串行化守卫（#303）——api 删除端点必须经此。

    锁内三步：标记 deleting（tombstone）→ 调用方清目标三件 + 删除事务
    （purge 行）→ 异常时按快照恢复行原状再上抛（LookupError→404 等语义
    不变）。与投递/对账线程的逐行动作同锁串行，删除窗口内投递不可能写出
    key——**口径一：删除完成后目标侧不复活**。outbox 未启用（同 Redis
    部署）时透传：无线程、无行，无协议可做。
    """
    if not is_enabled():
        yield
        return
    with OUTBOX_DELETE_LOCK:
        snapshot = mark_deleting(session_local, tenant_id=tenant_id,
                                 flow_id=flow_id, version=version)
        try:
            yield
        except BaseException:
            restore_deleting(session_local, snapshot, tenant_id=tenant_id,
                             flow_id=flow_id, version=version)
            raise


def _reload_row(session: Session, row_id: int) -> Optional[FlowPublishOutbox]:
    """锁内重读行现态（#303 陈旧读防线）：行已删 → None；否则刷新后返回。"""
    row = session.get(FlowPublishOutbox, row_id)
    if row is None:
        return None
    try:
        session.refresh(row)
    except ObjectDeletedError:
        return None
    return row


# ---- 按行租约（#324：多副本谁抢到谁投，解除单副本约束） ----


def _outbox_lease_key(tenant_id: str, row_id: int) -> str:
    """按行租约键：``{ns}:outbox:lease:{行id}``（租户命名空间与投递键同源）。"""
    return f"{tenant_namespace(tenant_id or '')}:outbox:lease:{int(row_id)}"


_LEASE_RELEASE_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then "
    "return redis.call('del', KEYS[1]) else return 0 end"
)


class _RowLease:
    """持有中的按行租约句柄。仅持有者可释放，且 token 不符时不删。"""

    __slots__ = ("_client", "_key", "_token")

    def __init__(self, client: Optional[Redis], key: str, token: str) -> None:
        self._client = client
        self._key = key
        self._token = token

    def release(self) -> None:
        """显式释放（Lua 比对 token 才删）。

        token 比对防误删：本租约 TTL 过期后被他副本抢走时，迟到的释放
        不得删掉他副本的租约（否则第三副本可再抢入，重复投递窗扩大）。
        连接级失败静默（debug 留痕）：TTL 兜底至多滞留一个 TTL（短于
        最小重试退避，不阻塞到期重试），且该行动作的成败已由投递/对账
        自身的失败路径具名记录，释放失败不改变任何行态。
        """
        if self._client is None:
            return
        try:
            self._client.eval(_LEASE_RELEASE_LUA, 1, self._key, self._token)
        except (RedisError, OSError) as e:
            logger.debug(
                "outbox 租约释放失败（TTL 兜底，不阻塞记账路径）key=%s: %s",
                self._key, e,
            )


#: 租约面不可达的 fail-open 哨兵（no-op 租约）：调用方照常行动——投递/
#: 对账自身会对不可达目标具名失败（redis_unavailable → attempts/退避/DLQ
#: 记账与单副本时代一致），绝不因租约面把「不可达重试记账」改写成「静默
#: 跳过」（否则目标 Redis 宕机期间行永不入账、#299 的退避/DLQ 语义失真）。
#: 健康目标上的互斥不受影响；fail-open 窗口内的最坏情形 = 两副本并发
#: 同内容覆盖写（读回校验兜底，规格明言幂等无害）。
LEASE_UNAVAILABLE = _RowLease(None, "", "")


def acquire_row_lease(
    client: Redis, tenant_id: str, row_id: int
) -> Optional[_RowLease]:
    """抢按行租约（#324）：``SET NX PX``——抢到 = 本副本独占该行的本次
    投递/对账动作。

    返回三态：:class:`_RowLease` = 抢到（谁抢到谁投）；``None`` = 他副本
    持有中（调用方本轮跳过该行、**不动行态**——attempts/退避/DLQ 记账只
    由持有者写，单副本时代的计数互覆盖〔b117-03〕就此失效）；
    :data:`LEASE_UNAVAILABLE` 哨兵 = 租约面不可达（fail-open，照常行动）。
    TTL 短于最小重试退避（:func:`lease_ttl_seconds`）：持有者崩溃后至多
    滞留一个 TTL，绝不阻塞到期重试。
    """
    key = _outbox_lease_key(tenant_id, row_id)
    token = uuid.uuid4().hex
    ttl_ms = max(1, int(round(lease_ttl_seconds() * 1000)))
    try:
        acquired = client.set(key, token, nx=True, px=ttl_ms)
    except (RedisError, OSError) as e:
        logger.warning(
            "outbox 租约面不可达（fail-open 行动，行 %s）: %s", row_id, e,
        )
        return LEASE_UNAVAILABLE
    if not acquired:
        return None
    return _RowLease(client, key, token)


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

    返回统计（``scanned/delivered/retried/to_dlq/errors/skipped/
    lease_skipped/lease_open``）。任何意外都被计入 errors 而不抛出——
    投递线程按 poll 节奏自愈。逐行动作前先抢**按行租约**（#324，跨副本
    谁抢到谁投；他副本持有 → ``lease_skipped``，不动行态；租约面不可达 →
    fail-open 行动并计 ``lease_open``），再持 :data:`OUTBOX_DELETE_LOCK`
    重读行现态（#303）：删除协议标记的 deleting 行、以及已删行（陈旧读
    命中）一律跳过——清目标 key 之后不可能再被本循环写回。
    """
    now = now or datetime.utcnow()
    limit = limit or batch_size()
    attempts_cap = attempts_cap or max_attempts()
    stats: Dict[str, Any] = {
        "scanned": 0, "delivered": 0, "retried": 0, "to_dlq": 0, "errors": 0,
        "skipped": 0, "lease_skipped": 0, "lease_open": 0,
    }
    try:
        with session_local() as session:
            rows = _iter_due_rows(session, limit, now)
            for row in rows:
                stats["scanned"] += 1
                # #324：先抢按行租约（跨副本互斥，Redis 面不该持进程内锁
                # 做往返）；他副本持有 → 跳过，租约面不可达 → fail-open。
                lease = acquire_row_lease(target, row.tenant_id or "", row.id)
                if lease is None:
                    stats["lease_skipped"] += 1
                    continue
                if lease is LEASE_UNAVAILABLE:
                    stats["lease_open"] += 1
                try:
                    with OUTBOX_DELETE_LOCK:
                        current = _reload_row(session, row.id)
                        if current is None or current.status not in (
                                STATUS_PENDING, STATUS_FAILED):
                            stats["skipped"] += 1
                            continue
                        reason = _deliver_one(target, current, now)
                        current.attempts = (current.attempts or 0) + 1
                        if reason is None:
                            current.status = STATUS_DELIVERED
                            current.delivered_at = now
                            current.next_attempt_at = None
                            current.last_error = ""
                            stats["delivered"] += 1
                            logger.info(
                                "outbox 已投递 %s@%s（租户 %s，attempts=%s）",
                                current.flow_id, current.version,
                                current.tenant_id or "default",
                                current.attempts,
                            )
                        else:
                            current.last_error = reason[:last_error_truncate]
                            if current.attempts >= attempts_cap:
                                current.status = STATUS_FAILED
                                # DLQ 档仍按慢速重排自动补投（封顶 30min），不丢定义。
                                current.next_attempt_at = now + timedelta(seconds=min(
                                    _failed_requeue_backoff(current.attempts),
                                    _FAILED_REQUEUE_MAX_SECONDS,
                                ))
                                stats["to_dlq"] += 1
                                logger.error(
                                    "outbox 投递转 DLQ：%s@%s（租户 %s）attempts=%s "
                                    "reason=%s；将按慢速重排自动补投，/health."
                                    "engine_outbox 可观测",
                                    current.flow_id, current.version,
                                    current.tenant_id or "default",
                                    current.attempts, reason,
                                )
                            else:
                                current.next_attempt_at = now + timedelta(seconds=min(
                                    _RETRY_BACKOFF_BASE_SECONDS
                                    * (2 ** (current.attempts - 1)),
                                    _RETRY_BACKOFF_MAX_SECONDS,
                                ))
                                stats["retried"] += 1
                                logger.warning(
                                    "outbox 投递失败（将退避重试）：%s@%s（租户 %s）"
                                    "attempts=%s/%s reason=%s",
                                    current.flow_id, current.version,
                                    current.tenant_id or "default",
                                    current.attempts, attempts_cap, reason,
                                )
                            stats["errors"] += 1
                        session.commit()
                finally:
                    lease.release()
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


def _iter_delivered_ids(session: Session, max_rows: int) -> List[Tuple[int, str]]:
    """delivered 行 ``(id, tenant_id)``（id 序、有界）；整行只在锁内动作时
    重读（``_reload_row``）。

    租户随行带出：按行租约键（#324）须在进程内删除锁**之外**、重读之前
    先抢——跨副本互斥是 Redis 往返，不该持进程内锁做网络 I/O。
    """
    return [
        (int(rid), tenant or "")
        for rid, tenant in session.execute(
            select(FlowPublishOutbox.id, FlowPublishOutbox.tenant_id)
            .where(FlowPublishOutbox.status == STATUS_DELIVERED)
            .order_by(FlowPublishOutbox.id.asc())
            .limit(max_rows)
        ).all()
    ]


def _reconcile_one(
    target: Redis,
    session: Session,
    row: FlowPublishOutbox,
    ns_cache: Dict[str, str],
    now: datetime,
    stats: Dict[str, Any],
) -> bool:
    """单 delivered 行对账（#302 扩口径）：先对源表、后对目标。

    返回 ``False`` = 连接级中断（剩余行留待下轮）；其余一律 ``True``
    （含各行分支的记账）。行内变更由调用方在同锁内提交。
    """
    tenant = row.tenant_id or ""

    # ---- 1. 源表对账（#302）----
    source = session.scalars(
        select(FlowVersion).where(
            FlowVersion.tenant_id == tenant,
            FlowVersion.flow_id == row.flow_id,
            FlowVersion.version == row.version,
        )
    ).first()
    if source is None:
        if _reconcile_source_missing(target, session, row, tenant, now):
            stats["source_missing"] += 1
        else:
            stats["errors"] += 1
        return True
    try:
        _, source_hash = engine_payload(
            row.flow_id, row.version, source.definition
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        source_hash = None
    if source_hash != row.definition_hash:
        _reconcile_definition_drift(session, row, source, source_hash, now)
        stats["drifted"] += 1
        return True

    # ---- 2. 目标对账（#299 原口径）----
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
        return False
    if raw is not None:
        actual = _stored_key_hash(raw)
        if actual == row.definition_hash:
            return True
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
    return True


def run_reconcile_cycle(
    target: Redis,
    session_local: sessionmaker,
    *,
    now: Optional[datetime] = None,
    max_rows: Optional[int] = None,
) -> Dict[str, Any]:
    """一轮对账（#302 扩口径）：每行先对源表、后对目标。

    1. **源表对账**（修「行内副本自检不防源表被改」的三路全闭）：重读
       ``flow_versions.definition`` 现值重算指纹，与行内 ``definition_hash``
       比对。分歧 = 源表被 DB 级修改、行内副本过期——按「console 是权威
       库」口径以**源表现值**更新行内副本与指纹、打回 pending 重投
       （``drift_count`` 累计；重投被投递期自检/读回校验兜住）。源表定义
       不可解析时保留旧指纹，投递期自检会具名拒投进 DLQ（可见、不静默）。
       源表行已删除而 outbox 行仍在（删除竞态漏 purge 的残留）→ 按删除
       语义收尾：清目标三件 + 删行；目标清理失败保留行、下轮重试。
    2. **目标对账**（#299 原口径）：delivered 行在目标 Redis 缺 key /
       hash 不符 → 打回 pending 重投。

    有界（``max_rows``，默认 batch_size）；行多时按 id 序分轮消化。逐行
    动作前先抢**按行租约**（#324，跨副本谁抢到谁对账；他副本持有 →
    ``lease_skipped``，不动行态；租约面不可达 → fail-open 行动并计
    ``lease_open``），再持 :data:`OUTBOX_DELETE_LOCK` 重读行现态（#303）：
    已进删除协议（deleting）或已被删的行一律跳过——对账不得复活删除中的
    版本。逐行提交（原为轮末一次提交）。
    """
    now = now or datetime.utcnow()
    max_rows = max_rows or batch_size()
    stats: Dict[str, Any] = {
        "checked": 0, "requeued": 0, "errors": 0,
        "drifted": 0, "source_missing": 0, "skipped": 0,
        "lease_skipped": 0, "lease_open": 0,
    }
    try:
        with session_local() as session:
            ns_cache: Dict[str, str] = {}
            for rid, tenant in _iter_delivered_ids(session, max_rows):
                stats["checked"] += 1
                # #324：先抢按行租约（同投递面，跨副本互斥在锁外）。
                lease = acquire_row_lease(target, tenant, rid)
                if lease is None:
                    stats["lease_skipped"] += 1
                    continue
                if lease is LEASE_UNAVAILABLE:
                    stats["lease_open"] += 1
                try:
                    with OUTBOX_DELETE_LOCK:
                        row = _reload_row(session, rid)
                        if row is None or row.status != STATUS_DELIVERED:
                            stats["skipped"] += 1
                            continue
                        if not _reconcile_one(target, session, row, ns_cache,
                                              now, stats):
                            break
                        session.commit()
                finally:
                    lease.release()
    except Exception as e:  # noqa: BLE001 — 读库失败：下轮重试
        stats["errors"] += 1
        logger.warning("outbox 对账轮次失败（下轮重试）: %s", e, exc_info=True)
    return stats


def _reconcile_definition_drift(
    session: Session,
    row: FlowPublishOutbox,
    source: FlowVersion,
    source_hash: Optional[str],
    now: datetime,
) -> None:
    """源表定义与行内副本分歧：按源表现值更新行并打回 pending 重投（#302）。

    SAVEPOINT 吸收唯一约束竞态（同键同 hash 的另一行已存在时回滚本次
    更新、计 ERROR，不拖垮整轮对账）。源表不可解析（``source_hash`` 为
    ``None``）时保留旧指纹——投递期自检会以「行内定义与指纹不一致」具名
    拒投，最终进 DLQ 可见。
    """
    new_hash = source_hash if source_hash is not None else row.definition_hash
    try:
        with session.begin_nested():
            row.definition = source.definition or ""
            row.definition_hash = new_hash
            row.status = STATUS_PENDING
            row.attempts = 0
            row.delivered_at = None
            row.next_attempt_at = None
            row.drift_count = (row.drift_count or 0) + 1
            row.last_error = (
                f"reconcile: 源表定义与行内副本不一致（DB 级修改），已按源表现值"
                f"重算指纹并打回重投 @ {now.isoformat()}"
            )[:last_error_truncate]
            session.flush()
    except IntegrityError:
        logger.error(
            "outbox 对账发现漂移但行更新冲突（同键同 hash 行已存在），"
            "本行保持原状下轮重试：%s@%s（租户 %s）",
            row.flow_id, row.version, row.tenant_id or "default",
        )
        return
    logger.warning(
        "outbox 对账发现源表定义漂移（#302）：%s@%s（租户 %s）行内副本已过期，"
        "按源表现值重投（drift_count=%s）",
        row.flow_id, row.version, row.tenant_id or "default", row.drift_count,
    )


def _reconcile_source_missing(
    target: Redis,
    session: Session,
    row: FlowPublishOutbox,
    tenant: str,
    now: datetime,
) -> bool:
    """源表行已删除而 outbox 行仍在：按删除语义收尾（#301+#302）。

    清目标三件后删行；目标清理失败则保留行（仍 delivered）待下轮重试。
    返回 ``True`` = 收尾完成（行已删）。
    """
    token = set_current_tenant(row.tenant_id or DEFAULT_TENANT_ID,
                               source=TENANT_SOURCE_EXPLICIT)
    try:
        guard_tenant_write("engine_outbox.reconcile_purge")
        ns = tenant_namespace(tenant)
        key = f"{ns}:flow:{row.flow_id}:{row.version}"
        try:
            # delete_flow 内部吞连接异常只回 False/True，不能作为成功判据；
            # 以「目标版本 key 已消失」为收尾口径（从未投递/已清过也算成功）。
            _purge_target_storage(
                target, tenant_id=tenant, flow_id=row.flow_id, version=row.version
            )
            gone = not target.exists(key)
        except (RedisConnectionError, RedisTimeoutError, OSError) as e:
            logger.warning(
                "outbox 对账发现源表行缺失但目标清理失败（保留行下轮重试）："
                "%s@%s（租户 %s）: %s",
                row.flow_id, row.version, tenant or "default", e,
            )
            row.last_error = (
                f"reconcile: 源表行已删除但目标 Redis 清理失败，保留行重试 "
                f"@ {now.isoformat()}"
            )[:last_error_truncate]
            return False
        if not gone:
            logger.warning(
                "outbox 对账发现源表行缺失但目标 key 清理未生效（保留行下轮"
                "重试）：%s@%s（租户 %s）",
                row.flow_id, row.version, tenant or "default",
            )
            row.last_error = (
                f"reconcile: 源表行已删除但目标 key 清理未生效，保留行重试 "
                f"@ {now.isoformat()}"
            )[:last_error_truncate]
            return False
        brief = (row.flow_id, row.version)
        session.delete(row)
        logger.warning(
            "outbox 对账收尾残留行（源表已删除）：清目标三件 + 删行 %s@%s（租户 %s）@ %s",
            brief[0], brief[1], tenant or "default", now.isoformat(),
        )
        return True
    finally:
        reset_current_tenant(token)


# ---- DLQ 人工出口（c117-06） ----


def _row_brief(row: FlowPublishOutbox) -> Dict[str, Any]:
    return {
        "row_id": row.id,
        "tenant_id": row.tenant_id or "",
        "flow_id": row.flow_id,
        "version": row.version,
        "status": row.status,
        "attempts": row.attempts or 0,
        "drift_count": row.drift_count or 0,
        "last_error": row.last_error or "",
    }


def _get_scoped_row(
    session: Session, row_id: int, tenant_id: Optional[str]
) -> Optional[FlowPublishOutbox]:
    """按 id 取行；``tenant_id`` 非 ``None`` 时限定租户（不匹配视同不存在）。"""
    row = session.get(FlowPublishOutbox, row_id)
    if row is None:
        return None
    if tenant_id is not None and (row.tenant_id or "") != (tenant_id or ""):
        return None
    return row


def resolve_row(
    session_local: sessionmaker,
    row_id: int,
    *,
    tenant_id: Optional[str] = None,
    actor: str = "",
) -> Optional[Dict[str, Any]]:
    """DLQ 人工出口·resolve：认定该行无需再投递，置 ``resolved`` 终态。

    resolved 行不进投递扫描（``_iter_due_rows`` 只取 pending/failed）、
    不进对账（只扫 delivered），保留行体与失败原因作留痕。delivered 行
    无需人工出口（下线请走删除流程/版本接口，#301 会清目标三件）→
    ``ValueError``。租户不匹配/行不存在 → ``None``。

    A2R50-02：检查与写在 :data:`OUTBOX_DELETE_LOCK` 内全程串行（与
    deleting_guard/投递/对账同锁），锁内 ``_reload_row`` 重读现态——
    竞窗内落下的 tombstone / 删除在此显形（deleting → ``ValueError``
    409、已删 → ``None`` 404，陈旧读不放行），resolve 不再可能盖掉
    tombstone 致删除回滚后行不可恢复。
    """
    with session_local() as session:
        with OUTBOX_DELETE_LOCK:
            row = _get_scoped_row(session, row_id, tenant_id)
            if row is None:
                return None
            if _reload_row(session, row_id) is None:
                return None
            if row.status == STATUS_DELETING:
                raise ValueError(
                    "该行处于删除协议中（deleting），不可人工出口；"
                    "删除事务失败会自动恢复行原状，稍后重试"
                )
            if row.status == STATUS_DELIVERED:
                raise ValueError(
                    "已 delivered 的行无需人工出口；如需下线请走删除流程/版本接口"
                )
            if row.status != STATUS_RESOLVED:
                row.status = STATUS_RESOLVED
                row.next_attempt_at = None
                note = f"manually resolved by {actor or 'unknown'} @ {datetime.utcnow().isoformat()}"
                row.last_error = (
                    f"{row.last_error} | {note}" if row.last_error else note
                )[:last_error_truncate]
                session.commit()
            brief = _row_brief(row)
            logger.info(
                "outbox 行人工 resolve：%s@%s（租户 %s，row_id=%s，actor=%s）",
                brief["flow_id"], brief["version"], brief["tenant_id"] or "default",
                row_id, actor or "unknown",
            )
            return brief


def discard_row(
    session_local: sessionmaker,
    row_id: int,
    *,
    tenant_id: Optional[str] = None,
    actor: str = "",
) -> Optional[Dict[str, Any]]:
    """DLQ 人工出口·discard：放弃投递并**删除**该行。

    与 resolve 的差别：不保留行体（无留痕，仅审计日志）。delivered 行
    拒绝——删行会失去对账覆盖（目标 key 幽灵化风险）→ ``ValueError``；
    若定义也不该留在目标 Redis，走删除流程/版本接口（#301 清理三件）。
    租户不匹配/行不存在 → ``None``。

    A2R50-02：与 :func:`resolve_row` 同锁同重读纪律——竞窗内 discard
    不再可能抢在删除事务前删行（删除失败回滚后 restore_deleting 只恢复
    仍为 deleting 的行，被抢删的行 = published 版本永久失去投递/对账
    载体，幽灵不可自愈）；竞窗内已落 tombstone 的行按 409 拒。
    """
    with session_local() as session:
        with OUTBOX_DELETE_LOCK:
            row = _get_scoped_row(session, row_id, tenant_id)
            if row is None:
                return None
            if _reload_row(session, row_id) is None:
                return None
            if row.status == STATUS_DELETING:
                raise ValueError(
                    "该行处于删除协议中（deleting），不可 discard；"
                    "删除事务失败会自动恢复行原状，稍后重试"
                )
            if row.status == STATUS_DELIVERED:
                raise ValueError(
                    "已 delivered 的行不可 discard（会失去对账覆盖）；"
                    "如需下线请走删除流程/版本接口"
                )
            brief = _row_brief(row)
            session.delete(row)
            session.commit()
            logger.info(
                "outbox 行人工 discard（删行）：%s@%s（租户 %s，row_id=%s，actor=%s）",
                brief["flow_id"], brief["version"], brief["tenant_id"] or "default",
                row_id, actor or "unknown",
            )
            return brief


# ---- 观测 ----


def stats_snapshot(session_local: sessionmaker) -> Dict[str, Any]:
    """/health.engine_outbox 的 backlog 视图（pending/failed 计数 + 最近错误）。

    #302：``drift_total`` = 各行 ``drift_count`` 之和（源表定义漂移被对账
    命中的累计次数，>0 即发生过 DB 级定义修改并被自动重投收敛）。
    c117-06：``resolved`` = 人工 resolve 终态行数。
    """
    snapshot: Dict[str, Any] = {
        "pending": 0, "failed": 0, "resolved": 0, "deleting": 0,
        "drift_total": 0,
        "last_delivered_at": None, "last_error": None,
    }
    try:
        with session_local() as session:
            for status in (STATUS_PENDING, STATUS_FAILED, STATUS_RESOLVED,
                           STATUS_DELETING):
                snapshot[status] = session.scalar(
                    select(func.count())
                    .select_from(FlowPublishOutbox)
                    .where(FlowPublishOutbox.status == status)
                ) or 0
            snapshot["drift_total"] = session.scalar(
                select(func.coalesce(func.sum(FlowPublishOutbox.drift_count), 0))
                .select_from(FlowPublishOutbox)
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
