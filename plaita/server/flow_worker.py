from datetime import datetime
from enum import Enum
import hashlib
import inspect
import json
import logging
import os
import signal
import socket
import threading
import time
import uuid
import weakref
from typing import Dict, Any, Mapping, Optional, Set, Tuple

import argparse
import importlib
import sys

from cachetools import TTLCache
from redis import Redis
from redis.exceptions import RedisError
from plaita.core.errors import ResumeType
from plaita.event.core import EventBus
from plaita.core.errors import (
    DEFAULT_NODE_ABORT_CODE,
    FlowCancelledException,
    FlowExecutionException,
    FlowTimeoutError,
    NodeExecutionError,
    NodeTimeoutError,
    ResumeGuardError,
)
from plaita.core.flow import Flow
from plaita.core.executor import FlowExecution, ExecutionMode
from plaita.core.runner import is_retryable_error
from plaita.storage.base import (
    ExecutionState,
    ExecutionStorage,
    FlowStorage,
    StateSerializationError,
)
from plaita.storage.fenced import (
    reset_current_fence_token,
    set_current_fence_token,
)
from plaita.storage.redis import (
    ExecutionStateLoadError,
    TERMINAL_EXECUTION_STATUSES,
    validate_tombstone_retention,
)
from plaita.core.retention import task_max_age_seconds
from plaita.logger import logger
from plaita.redis_client import make_redis_client
from plaita.writefile_jail import apply_writefile_jail
from plaita.runtime_config import (
    ConfigError,
    ConfigGeneration,
    LEGACY_GENERATION,
    envelope_budget_values,
    load_runtime_config,
    log_config_effective,
    preflight_security_and_limits,
    resolve_consumer_group,
    resolve_dlq_key,
    resolve_queue_name,
    resolve_redis_url,
)
from plaita.server.registry import RegistryMixin, ServiceRegistry, ServiceInfo
# #42（W10-a）：worker 容量上报——把「还能接多少活」写进既有 registry metadata
# 的 ``worker_capacity`` 子键（复用 plaita:registry:flow_worker:<id> 键与 TTL）。
# #42（W23-a 期 1）：显式派发目标闸——派发侧按容量路由把目标 worker 写进消息体
# 顶层 ``target_instance_id``，worker 侧复用同一 ``TaskNotForThisWorker`` 交接
# 路径让出（不新增协议）。
from plaita.server.capacity import (
    CAPACITY_METADATA_KEY,
    TARGET_INSTANCE_FIELD,
    build_capacity_block,
    collect_capacity_metrics,
    normalize_target_instance_id,
    target_mismatch,
)
from plaita.server.capability import python_worker_manifest
from plaita.server.control import ControlMixin, ControlListener
from plaita.server.alerts import WebhookAlerter
from plaita.server.metrics import (
    Metric,
    MetricsHttpServer,
    collect_queue_metrics,
    collect_worker_metrics,
    render_prometheus,
)
from plaita.server.log_handler import setup_redis_logging
from plaita.server.node_timings import NodeTimingCallback
from plaita.storage.usage import aggregate_node_usage, node_results_from_context
from plaita.server.execution_timeline import (
    CHANNEL_PREFIX,
    ExecutionTimelineCallback,
    event_payload,
    merge_timeline,
    publish_enabled,
)
from plaita.server.task_queue import (
    DEFAULT_CLAIM_MIN_IDLE_MS,
    DEFAULT_CONSUMER_GROUP,
    DEFAULT_MAX_DELIVERIES,
    RECONNECT_BACKOFF_SECONDS,
    RedisStreamTaskQueue,
    StreamTask,
    START_DEDUP_TTL_SECONDS,
    _dlq_max_len_from_env,
    enqueue_task,
    start_dedup_map_key,
    timestamp_from_stream_id,
)
from plaita.server.execution_lease import (
    DEFAULT_LEASE_TTL_SECONDS,
    ExecutionLease,
    ExecutionLeaseError,
    NullExecutionLease,
    RedisExecutionLease,
    holder_instance_id,
    new_holder_token,
)
from plaita.server.tenant_context import (
    TenantRoutingExecutionLease,
    current_tenant,
    reset_current_tenant,
    set_current_tenant,
    tenant_namespace,
)
from plaita.tenant_context import (
    DEFAULT_TENANT_ID,
    TENANT_SOURCE_EXPLICIT,
    TenantWriteRejected,
    current_tenant_generation,
    reset_current_tenant_generation,
    set_current_tenant_generation,
)
from plaita.server.tenant_status import (
    is_epistemic_reject_reason,
    read_tenant_generation,
    tenant_is_active,
    tenant_start_allowed,
    tenant_write_allowed,
)
# plaita#28：每租户 start 准入配额（最小公平性原语）。console 入口超限 429、
# worker 对无名额的外部生产方消息超限延迟重投；原语细节见 tenant_quota 模块。
from plaita.server.tenant_quota import (
    release_tenant_start,
    try_admit_tenant_start,
)
# plaita#158 / #159：两个持久面（INFO/WARNING 日志、注册元数据）里的 URL 一律经
# ``url_redaction`` 渲染——它**复用** #88 的 ``factory._mask_url``（不另起一套掩码，
# 那正是 #88 的根因形态），并加一层「掩码失败 ⇒ 安全常量、绝不抛」的 fail-closed。
# ``RedisUrlError`` / ``_parse_redis_url`` 仍直接来自 #88 的 factory（唯一解析路径）。
from plaita.server.factory import RedisUrlError, _parse_redis_url
from plaita.core.url_redaction import (
    _mask_url_for_log,
    _mask_webhook_url_for_log,
    _redact_env_value,
    _scrub_exception_text,
    _scrub_url_text,
)


def _env_switch(name: str) -> bool:
    """读 ``=1`` 形式的环境回滚开关（每次调用读取，便于测试注入）。"""
    return os.environ.get(name, "").strip() == "1"


def _cancel_checkpoint_disabled() -> bool:
    """波次①回滚开关：PLAITA_DISABLE_CANCEL_CHECKPOINT=1 时 worker 不查取消标志键。"""
    return _env_switch("PLAITA_DISABLE_CANCEL_CHECKPOINT")


def _cancel_interrupt_disabled() -> bool:
    """波次③回滚开关（步内中断半边）：PLAITA_DISABLE_CANCEL_INTERRUPT=1 时
    不启动取消监听线程——回到「只在步界检查取消标志键」的现状语义。"""
    return _env_switch("PLAITA_DISABLE_CANCEL_INTERRUPT")


def _fencing_disabled() -> bool:
    """波次②回滚开关（fencing 半边）：PLAITA_DISABLE_FENCING=1 时退回 SET NX acquire。"""
    return _env_switch("PLAITA_DISABLE_FENCING")


def _watchdog_disabled() -> bool:
    """波次②回滚开关（看门狗半边）：PLAITA_DISABLE_LEASE_WATCHDOG=1 时不启动续租线程。"""
    return _env_switch("PLAITA_DISABLE_LEASE_WATCHDOG")


def _holder_liveness_disabled() -> bool:
    """#50 回滚开关：PLAITA_DISABLE_HOLDER_LIVENESS=1 时租约冲突分支不做
    持有者存活核算——无条件回到「ack 释放」（2026-10-06 语义，含 A47-F2
    的决议载体收紧）。"""
    return _env_switch("PLAITA_DISABLE_HOLDER_LIVENESS")


# A47-F2：挂起执行的决议载体。subscribe→suspend 提交窗口内到达的
# event/timeout/cancel 不是重复投递，而是 suspended 的唯一唤醒来源（#33）。
_DECISION_RESUME_TYPES = frozenset(
    (ResumeType.EVENT.value, ResumeType.TIMEOUT.value, ResumeType.CANCEL.value)
)


def _affinity_disabled() -> bool:
    """机器亲和闸回滚开关：PLAITA_DISABLE_AFFINITY=1 时不做路径检查
    （回到「领到就跑」的现状；单机/同构环境本就不需要本闸）。"""
    return _env_switch("PLAITA_DISABLE_AFFINITY")


def _dispatch_target_disabled() -> bool:
    """显式派发目标闸回滚开关（#42 期 1）：``PLAITA_DISABLE_DISPATCH_TARGET=1``
    时不再按消息体 ``target_instance_id`` 让出——回到「谁抢到谁跑」。
    独立于 ``PLAITA_DISABLE_AFFINITY``：两者是不同判据（路径 vs 显式目标），
    回滚粒度必须分开，否则回滚一个会顺带关掉另一个。"""
    return _env_switch("PLAITA_DISABLE_DISPATCH_TARGET")


# ---- 节点重试显式重投的退避节奏（plaita#91）----
# 基线（b975109）的显式重投「立即入队 + ack」之间没有任何节奏：新副本 XADD
# 后下一次 read 立即命中，默认 5 次预算在 ~40–60ms 内烧光（相邻间隔 ~1ms，
# #91 复现 A 实测）——「持续数秒的上游抖动本可自愈」被直接打成 error 终态。
# 取值按 issue#91 建议区间（base 1s 量级、cap 30–60s，与 claim_min_idle_ms
# 的 60s 同量级），cap 取保守下限 30s。
NODE_RETRY_BACKOFF_BASE_SECONDS = 1.0
NODE_RETRY_BACKOFF_CAP_SECONDS = 30.0
# S18-01/A46-06：退避分片长度（秒）。整体 `time.sleep(delay)` 会挡住 drain
# 超时后的退出（实测 drain timeout 0.2s，消费线程 ~2s 才退出）；切成小片后
# 每片检查强停 Event，退出延迟上限 ≈ 一个分片。
NODE_RETRY_BACKOFF_SLICE_SECONDS = 0.2


def _node_retry_backoff_seconds(attempt: int) -> float:
    """显式重投的指数退避：``min(cap, base * 2**(attempt-1))``。

    ``attempt`` 取 ``NodeExecutionRetryableError.attempt``（已放行的重试
    次数，计数键 INCR 后的值，首次失败 = 1）→ 延迟序列 1s, 2s, 4s, 8s, …
    （封顶 cap）。``attempt < 1``（未知，旧调用方未透传）返回 0 = 不退避，
    旧签名直接调用方行为零变化。
    """
    try:
        n = int(attempt)
    except (TypeError, ValueError):
        return 0.0
    if n < 1:
        return 0.0
    return min(
        NODE_RETRY_BACKOFF_CAP_SECONDS,
        NODE_RETRY_BACKOFF_BASE_SECONDS * (2 ** (n - 1)),
    )


def _sleep(seconds: float) -> None:
    """退避睡眠的模块级缝隙：测试注入假时钟/记录桩（禁止真 sleep 拉长套件）。

    取舍说明（#91）：退避睡在**消费线程内**（入队前），单 worker 睡住会
    推迟同队列其他任务 ≤ cap（30s）。更稳妥的队列级延迟投递（ZSET/延迟
    Stream）是后续演进项；本修复先以最小面恢复节奏。cap（30s）<
    claim_min_idle_ms（默认 60s）保证睡窗内原条 pending 不会被对端
    XCLAIM 抢走产生并发推进。
    """
    time.sleep(seconds)


def _requeue_envelope_kwargs(source: Any) -> Dict[str, Any]:
    """重投/交接副本继承**源消息**的信封版本（A44-02）。

    worker 既是消费者也是生产者：它在重投/交接/归一化时写出的副本承载的是
    源载体的语义，不能无条件盖本 build 的 ``TASK_SCHEMA_VERSION``——否则混跑
    池里新实例写的恢复副本会被旧实例按「超版本」死信，运维文档的「先升消费者
    再升生产者」对 worker 自身不成立。

    源版本不可得（非 ``StreamTask`` 的测试桩/畸形对象）时返回空 kwargs，写入口
    按当前 build 版本落信封（存量行为）；``bool`` 不是合法版本值。
    """
    version = getattr(source, "schema_version", None)
    kwargs: Dict[str, Any] = {}
    if isinstance(version, int) and not isinstance(version, bool):
        kwargs["schema_version"] = version
    # B2R31-01：重投/交接/克隆/DLQ replay 继承**源**代际，绝不盖当前值。
    # 源缺失/legacy 时不写字段（消费侧按 legacy 处理），因此旧测试的
    # ``kwargs == {"schema_version": ...}`` 形状保持不变。
    for attr in ("config_family", "config_generation", "budget_generation"):
        value = getattr(source, attr, None)
        if isinstance(value, str) and value and value != LEGACY_GENERATION:
            kwargs[attr] = value
    effective_at = getattr(source, "config_effective_at", None)
    if isinstance(effective_at, str) and effective_at:
        kwargs["config_effective_at"] = effective_at
    # plaita#283：源租户世代随副本走；旧副本不得在新租户下被读成当前世代。
    tenant_generation = getattr(source, "tenant_generation", None)
    if isinstance(tenant_generation, str) and tenant_generation:
        kwargs["tenant_generation"] = tenant_generation
    return kwargs


def _load_matching_tombstone(
    storage: Optional[ExecutionStorage],
    execution_id: str,
    tenant_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    """读取并匹配执行墓碑（A51-01）；返回 None = 无墓碑/跨租户同 id。

    墓碑在平台根、值里带 ``tenant``：console 每次 uuid4 新 id，但外部生产者
    可自带 id——只有墓碑租户与载体 ``tenant_id`` 一致才视为「同一执行」，
    避免跨租户同 id 误伤。读取瞬态错误原样上抛（不得当「无墓碑」放行重建）。
    """
    loader = getattr(storage, "load_execution_tombstone", None)
    if not callable(loader):
        return None
    token = set_current_tenant(tenant_id)
    try:
        tombstone = loader(execution_id)
    finally:
        reset_current_tenant(token)
    if not tombstone:
        return None
    tombstone_tenant = tombstone.get("tenant") or DEFAULT_TENANT_ID
    carrier_tenant = tenant_id or DEFAULT_TENANT_ID
    if tombstone_tenant != carrier_tenant:
        logger.info(
            "执行 %s 的墓碑属于租户 %s（载体租户 %s），按未命中处理（外部同 id）",
            execution_id, tombstone_tenant, carrier_tenant,
        )
        return None
    return tombstone


# B52-02：墓碑 TTL 到期后仍有载体的兜底闸。主任务流没有 MAXLEN/时间 TTL，
# pending/积压消息理论上可无限期存活；墓碑过期后同一 start 仍会重建。
# 默认 30 天（与状态/墓碑默认 TTL 同 horizon），0 = 关闭。
# A69-04：env 解析下沉到 ``plaita/core/retention.py``（storage 的墓碑 resolver
# 与 worker 的年龄闸必须同一口径，且 storage 不得反向 import server）。
def _task_max_age_seconds() -> int:
    """消息最大年龄（秒）；未配置/非法回退 30 天，<=0 关闭（见 core.retention）。"""
    return task_max_age_seconds()


def _message_age_seconds(raw_timestamp: Any) -> Optional[float]:
    """消息体 ``timestamp``（ISO 串）→ 年龄秒；缺失/不可解析返回 None。"""
    if not isinstance(raw_timestamp, str) or not raw_timestamp.strip():
        return None
    try:
        timestamp = datetime.fromisoformat(raw_timestamp.strip())
    except (TypeError, ValueError):
        return None
    now = datetime.now(timestamp.tzinfo) if timestamp.tzinfo else datetime.now()
    return (now - timestamp).total_seconds()


def _message_age_from_stream_id(message_id: Any) -> Optional[float]:
    """Stream ID（``<ms>-<seq>``）→ 条目写入至今的秒数；非法返回 None。

    A69-04：外部 raw XADD / 老载体可能没有 payload ``timestamp``，但 Stream
    ID 的毫秒前缀是 Redis 写入时刻的可靠证据。解析统一走
    ``task_queue.timestamp_from_stream_id``（只接受纯数字 ID，不做异常上抛）。
    """
    raw = timestamp_from_stream_id(message_id)
    if raw is None:
        return None
    return (datetime.now() - datetime.fromisoformat(raw)).total_seconds()


def _carrier_timestamp_or_derived(
    body: Dict[str, Any], source_message_id: Any
) -> Optional[str]:
    """载体年龄证据：取 payload timestamp 与源 Stream ID 时间中**年龄更大**者。

    重投/交接/克隆必须保留源年龄，且只能保留或收紧、绝不刷新：
    - payload ``timestamp`` 畸形（外部生产者）不是年龄证据，不得挡住可靠的
      源 Stream ID 时间；
    - payload ``timestamp`` 超前（时钟漂移/伪造）时，源 Stream ID 可能已经
      更老，不能被新值洗白。

    返回 None = 两源都不可用；调用方（重投/克隆）不应补 ``now()``——那会
    把老载体变成「刚入队」，交给 ``enqueue_task`` 的缺失注入只发生在
    「全新载体」路径。
    """
    candidates = []
    for raw in (
        body.get("timestamp"),
        timestamp_from_stream_id(source_message_id),
    ):
        age = _message_age_seconds(raw)
        if age is not None:
            candidates.append((age, raw.strip() if isinstance(raw, str) else str(raw)))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def _copy_carrier_body(
    body: Dict[str, Any], source_message_id: Any = None
) -> Dict[str, Any]:
    """重投/交接/克隆副本：复制 body 并保留源年龄（缺失时由源 Stream ID 派生）。"""
    copy = dict(body)
    derived = _carrier_timestamp_or_derived(body, source_message_id)
    if derived is not None:
        copy["timestamp"] = derived
    return copy


#: 调度触发执行的调用者标记（A2R44-02；触发侧 queued 占位行同款值，接管后保持——
#: 接管落行不再恒写 "worker"，否则调度归因在接管瞬间断链）。
_SCHEDULE_TRIGGER_INVOKER = "schedule"


def invoker_for_trigger(trigger: Any) -> str:
    """start 消息 trigger 归因 → 接管落行的 invoker（A2R44-02）。

    调度触发（cron 与 console「立即触发」manual 均带 schedule_id，形见
    schedule_service.fire_schedule）统一落 ``invoker="schedule"``——周期/手动
    由 ``trigger.kind`` 区分；无 trigger（手动 start / 旧生产方消息）或形状
    不符 ⇒ ``invoker="worker"`` 存量语义零变化。
    """
    if isinstance(trigger, dict) and trigger.get("schedule_id"):
        return _SCHEDULE_TRIGGER_INVOKER
    return "worker"


def _requeue_body_for_retry(
    body: Dict[str, Any],
    storage: Optional[ExecutionStorage],
    source_message_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """#91 载体转换：start 载体重投前查盘，已有行则转 **resume 载体**。

    基线原样复制 start body：每轮经 ``start_flow`` 以 ``context={}`` 覆写
    盘上 running 行并从首个节点整条重放（前序副作用节点被重复执行最多
    5 次、间隔 ~1ms）。转换后重投副本走 resume 路径：

    - 非 start 载体原样返回（resume 本就带 checkpoint 语义）；
    - 盘上无该 execution_id 的行（首持久化前崩溃等）→ 原样 start（全新
      启动语义不变）；
    - 盘上有行 → ``{"type": "resume", "resume_type": "continue"}``：
      running 从 checkpoint 续跑；suspended / 终态由 resume_flow 的幂等
      短路接管——都优于 start 整条重放。形状镜像
      ``_handle_start_dedup_hit`` 的既有 start→resume 转换（#122：非空
      params 随载体透传，checkpoint 为空窗口不丢调度入参）；
    - flow_id 与盘上行不一致 → 原样重投（转换会被 resume_flow 的流程ID
      校验 ValueError 打成 poison ack 丢消息）；
    - 读盘失败（Redis 抖动）→ 原样 start（fail-safe：转换是优化，绝不因
      它失败而丢消息/丢节奏）；
    - **年龄保留（A69-04）**：原样重投的副本保留源 ``timestamp``；源缺失时
      由 ``source_message_id``（Redis Stream ID）派生，绝不补 ``now()``——
      否则每次重投都把老载体洗成新载体，年龄闸失效。start→resume 转换是
      **新的恢复载体**，写 ``now()``（规范语义，不是历史 start 重放）；
    - **删除墓碑命中（A51-01）→ 返回 None**：调用方必须 ack 原条、不再
      入队。删除后把 start 原样重投会让 ``start_flow`` 把执行重建完成；
      墓碑读取瞬态错误同样上抛（不 ack、等重投），绝不 fail-open 重排。
    """
    if body.get("type") != "start":
        return _copy_carrier_body(body, source_message_id)
    execution_id = body.get("execution_id")
    if not isinstance(execution_id, str) or not execution_id or storage is None:
        return _copy_carrier_body(body, source_message_id)
    tombstone = _load_matching_tombstone(
        storage, execution_id, body.get("tenant_id")
    )
    if tombstone is not None:
        logger.warning(
            "执行 %s 已删除（墓碑 reason=%s），节点失败重投不再生成 start 载体"
            "（ack 原条）: tenant=%s",
            execution_id, tombstone.get("reason"), body.get("tenant_id"),
        )
        return None
    token = set_current_tenant(body.get("tenant_id"))
    try:
        state = storage.load_execution_state(execution_id)
        tenant = current_tenant()
    except Exception as e:  # noqa: BLE001 — 读盘失败不拦重投（原样 start）
        logger.warning(
            "执行 %s 重投前查盘失败，start 载体原样重投: %s", execution_id, _scrub_url_text(e)
        )
        return _copy_carrier_body(body, source_message_id)
    finally:
        reset_current_tenant(token)
    if state is None:
        return _copy_carrier_body(body, source_message_id)
    stored_flow_id = getattr(state, "flow_id", None)
    if stored_flow_id and stored_flow_id != body.get("flow_id"):
        logger.warning(
            "执行 %s 重投载体 flow_id 与盘上行不一致（body=%r stored=%r），"
            "start 载体原样重投",
            execution_id, body.get("flow_id"), stored_flow_id,
        )
        return _copy_carrier_body(body, source_message_id)
    resume_body = {
        "type": "resume",
        "flow_id": body.get("flow_id"),
        "execution_id": execution_id,
        "resume_type": "continue",
        "tenant_id": tenant,
        "timestamp": datetime.now().isoformat(),
    }
    params = body.get("params")
    if isinstance(params, dict) and params:
        resume_body["params"] = dict(params)
    logger.info(
        "执行 %s start 载体重投转换为 resume 载体（盘上已有 %s 行，从 checkpoint 续跑）",
        execution_id, getattr(state, "status", "?"),
    )
    return resume_body

def _tenant_quota_disabled() -> bool:
    """每租户 start 配额闸回滚开关（plaita#28）：PLAITA_DISABLE_TENANT_QUOTA=1
    时 worker 不做准入检查（回到「领到就跑」的现状；console 侧开关独立）。"""
    return _env_switch("PLAITA_DISABLE_TENANT_QUOTA")

def _metrics_port_from_env() -> int:
    """``/metrics`` 抓取端端口（#26）；未配置 / 非法 = 0 = 不启动。"""
    raw = (os.environ.get("PLAITA_METRICS_PORT") or "").strip()
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        logger.warning("PLAITA_METRICS_PORT=%s 非法，已禁用 /metrics 端点", _redact_env_value(raw))
        return 0


def _metrics_host_from_env() -> str:
    return (os.environ.get("PLAITA_METRICS_HOST") or "").strip() or "0.0.0.0"


def _alert_webhook_from_env() -> Optional[str]:
    return (os.environ.get("PLAITA_ALERT_WEBHOOK") or "").strip() or None


def _redis_registry_metadata(redis_url: object) -> Dict[str, Any]:
    """注册元数据里的 Redis 端点：**掩码 URL + host/port/db**，绝不含口令。

    plaita#158：``RedisFlowWorker`` 的注册元数据会被 ``ServiceRegistry.register``
    原样 ``SETEX`` 进 ``plaita:registry:flow_worker:<instance_id>``——即**持久化**到
    Redis 里。修复前这里放原始连接串，口令随注册键留存（TTL 到期前，任何能读注册键
    的进程/控制面都能读到）。

    端点三件套走 ``_parse_redis_url``（#88 确立的唯一解析路径），不另写解析——否则
    又是「同一个 URL 两套解析」。URL 畸形时降级为「只带掩码 URL」并记日志，**不让
    注册路径抛**：真正的拒绝发生在 ``create_storage_component`` 的连接前解析。

    两条**不得违反**的性质（plaita#158 r1 定验）：

    * ``_mask_url_for_log`` 保证掩码失败退化为安全常量、绝不抛（r1 的
      ``UnicodeEncodeError`` 曾让 ``RedisFlowWorker.__init__`` 直接炸）；
    * 兜底分支**不打印异常文本**——那可能是任意异常（``UnicodeEncodeError`` 的
      ``str()`` 就带着口令里的那个字符），而 ``RedisUrlError``（#88 契约：消息已
      掩码）才允许回显。
    """
    metadata: Dict[str, Any] = {"redis_url": _mask_url_for_log(redis_url)}
    try:
        target = _parse_redis_url(redis_url)
    except RedisUrlError as exc:
        logger.warning("注册元数据无法解析 Redis 端点，仅登记掩码 URL: %s", _scrub_url_text(exc))
        return metadata
    except Exception:  # noqa: BLE001 — 注册路径绝不为「端点解析」抛
        logger.warning("注册元数据无法解析 Redis 端点，仅登记掩码 URL")
        return metadata
    unix_socket = target.unix_socket_path
    metadata["redis_host"] = unix_socket if unix_socket else target.host
    metadata["redis_port"] = None if unix_socket else target.port
    metadata["redis_db"] = target.db
    return metadata


def _paused_sweeper(max_age_secs: int = 6 * 3600):
    """准备「暂停沙箱清扫」：**全部 import 在主线程完成**，返回无 import 的闭包。

    ⚠️ 为什么死磕 import 位置：线程内 import 与主线程的懒加载 import 并发时会撞
    import 锁——2026-10-08 实测本机 worker 启动即整体卡死（栈停在 e2b/httpx 导入链，
    进程 0% CPU、零日志、连不上 Redis），根因就是清扫线程里那句
    ``from e2b_code_interpreter import ...``。预导入之后线程内只做网络调用。

    为什么需要这条清扫：失败/取消改为一律 pause 保现场后（sandbox_agent.
    _preserve_scene），没人续跑的暂停实例会累积占实例配额（AGS ~20；暂停不计
    计算力费，但配额满会让新建失败）。

    返回 ``None`` 表示无需/不可清扫（未装 plaita-nodes、无注册表、无 ags driver）。
    e2b 预导入是尽力而为的加速项，失败不算「不可清扫」——缺装时照常交出闭包，
    由驱动内部再导入时自报错并吞掉；任何异常只告警，绝不拦启动。
    """
    try:
        from plaita_nodes import sandbox as _sb
        if not _sb.load_sandboxes():
            return None
        import plaita_nodes.sandbox_ags  # noqa: F401 — import 即注册 ags driver
        drv = _sb.get_driver("ags")
        if drv is None or not hasattr(drv, "sweep_paused"):
            return None
    except Exception as exc:  # noqa: BLE001 — 清扫失败不影响启动
        logger.warning("沙箱清扫准备失败（忽略）：%s", _scrub_url_text(exc))
        return None

    try:
        import e2b_code_interpreter  # noqa: F401 — 预导入（sweep 内部再取时已缓存）
    except Exception as exc:  # noqa: BLE001 — 缺装只退化为驱动内部导入
        logger.warning("e2b 预导入失败（忽略，清扫仍会尝试）：%s", _scrub_url_text(exc))

    def _run() -> list:
        try:
            killed = drv.sweep_paused(max_age_secs=max_age_secs)
            if killed:
                logger.info("启动清扫：回收超龄暂停沙箱 %d 个：%s", len(killed), killed)
            return killed
        except Exception as exc:  # noqa: BLE001
            logger.warning("启动清扫沙箱失败（忽略）：%s", _scrub_url_text(exc))
            return []

    return _run


def sweep_paused_sandboxes(max_age_secs: int = 6 * 3600) -> list:
    """同步执行一次暂停清扫（测试/手工调用用）。

    worker 启动路径请用 ``_paused_sweeper()`` + 后台线程（import 前置到主线程）。
    """
    run = _paused_sweeper(max_age_secs)
    return run() if run is not None else []


def _deny_repos() -> set:
    """本机拒跑仓名单（2026-10-07）：PLAITA_WORKER_DENY_REPOS=<仓名,仓名…>。

    用途：把 Rust 重仓（cargo 冷构建）挡在小容量 worker 之外——2 核远端
    跑 cargo 会饱和/超时（实测 load 3.59/2 核），重仓留给大容量 worker。
    命中 → 复用既有「不亲和 → 交接」路径（对端接力；无人接则留 pending
    等 reclaim）。空 = 不拒（默认零行为变化）。"""
    raw = os.environ.get("PLAITA_WORKER_DENY_REPOS", "")
    return {x.strip() for x in raw.split(",") if x.strip()}


def _min_free_disk_gib_from_env() -> float:
    """claim 前本机盘守线（GiB；#49）：PLAITA_WORKER_MIN_FREE_DISK_GIB。

    未配置 / 非法 = 0 = 关闭预检（默认零行为变化，与 ``_deny_repos`` 同口径）。
    部署侧应与 flow 内 preflight 节点的同款阈值对齐（recursive 侧为
    ``RECURSIVE_MIN_FREE_DISK_GIB``，默认 20），否则仍会「worker 放行领取 →
    flow preflight 才判盘不足」白跑一轮。"""
    raw = (os.environ.get("PLAITA_WORKER_MIN_FREE_DISK_GIB") or "").strip()
    if not raw:
        return 0.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        logger.warning("PLAITA_WORKER_MIN_FREE_DISK_GIB=%s 非法，盘预检按关闭处理", _redact_env_value(raw))
        return 0.0


def _disk_guard_path_from_env() -> str:
    """盘预检路径（PLAITA_WORKER_DISK_GUARD_PATH），默认当前工作目录。"""
    return (os.environ.get("PLAITA_WORKER_DISK_GUARD_PATH") or "").strip() or "."


def _free_disk_gib(path: str) -> Optional[float]:
    """``path`` 所在文件系统的可用盘（GiB）；探测失败返回 None。

    ``AttributeError``：无 ``os.statvfs`` 的平台（Windows）——预检不可用按
    「放行」处理（与探针自身的其余失败一致），不得把 AttributeError 冒到
    消费循环（预检开关默认关，但开了也不该炸掉 worker）。
    """
    try:
        st = os.statvfs(path)
    except (OSError, AttributeError):
        return None
    return (st.f_bavail * st.f_frsize) / float(1024 ** 3)


def _node_retry_disabled() -> bool:
    """波次二任务①回滚开关：PLAITA_DISABLE_NODE_RETRY=1 时节点失败直接终态化
    error（完全回到波次前行为）。"""
    return _env_switch("PLAITA_DISABLE_NODE_RETRY")


# A56-01 显式逃生开关：配置后端不可用（Docker daemon 不在）时是否允许降级到
# subprocess。默认**关**——降级改变安全边界（subprocess 不隔离 FS/网络，见
# plaita#256），且必须同时保证 subprocess 已在运营者显式白名单内。
CODE_SUBPROCESS_FALLBACK_ENV = "PLAITA_ALLOW_SUBPROCESS_FALLBACK"


class CodeNodeRegistrationError(RuntimeError):
    """worker 启动期 CodeNode 注册被具名拒绝（A56-01，fail-closed）。"""


def _code_backend_for_worker() -> str:
    """worker 启动时 CodeNode 的沙箱后端（PLAITA_CODE_BACKEND，默认 subprocess）。"""
    raw = os.environ.get("PLAITA_CODE_BACKEND", "").strip()
    return raw or "subprocess"


def _code_subprocess_fallback_allowed() -> bool:
    """A56-01：显式 ``PLAITA_ALLOW_SUBPROCESS_FALLBACK=1`` 才允许降级。"""
    return _env_switch(CODE_SUBPROCESS_FALLBACK_ENV)


def _code_allowed_backends_for_worker(backend: str) -> list:
    """worker 生效的沙箱后端白名单（plaita#22）。

    ``PLAITA_SANDBOX_ALLOWED_BACKENDS`` 未配置时默认 ``(docker,)`` 并入生效后端，
    即流程 JSON 不得逐节点降级到 ``"unsafe"``（进程内 raw exec）——未接线前该白名单
    机制只存在于注释里，任意租户写一行 ``sandbox_backend: "unsafe"`` 即宿主 RCE。
    """
    from plaita.node import resolve_sandbox_allowed_backends

    return list(resolve_sandbox_allowed_backends(backend, "flow-worker"))


def _code_allowed_languages_for_worker() -> list:
    """worker 生效的语言白名单（plaita#29）。

    默认只放行 ``python``：``language: "js"`` 的历史实现（PyExecJS）绕开整个
    ``sandbox_backend`` 档位体系（无隔离/无超时/无取消），放行 js 须经
    ``PLAITA_SANDBOX_ALLOWED_LANGUAGES`` 显式配置。
    """
    from plaita.node import resolve_sandbox_allowed_languages

    return list(resolve_sandbox_allowed_languages("flow-worker"))


def _code_node_enabled() -> bool:
    """PLAITA_DISABLE_CODE_NODE=1 时不注册 CodeNode（含 code 节点的流程会被丢弃）。"""
    return not _env_switch("PLAITA_DISABLE_CODE_NODE")


def _register_code_node_for_worker() -> None:
    """为 worker 注册 CodeNode（生产流程如 self-improve-v2 含 code 节点）。

    默认注册表自 0.4.0 起不含 CodeNode（执行任意用户代码须显式 opt-in），worker
    不注册则整单被丢弃（unRecognized node type: code）。

    **后端不可用时 fail-closed（A56-01）**：显式 ``PLAITA_CODE_BACKEND=docker`` 但
    Docker daemon 不可用时，注册被具名拒绝（:class:`CodeNodeRegistrationError`），
    worker 启动失败——**不**自动降级 ``subprocess``。降级会把运维显式白名单
    ``{docker}`` 扩宽成 ``{docker, subprocess}``，与该白名单"只能更严"的契约相反。
    需要可用性降级的部署必须**同时**显式设置
    ``PLAITA_ALLOW_SUBPROCESS_FALLBACK=1`` 与
    ``PLAITA_SANDBOX_ALLOWED_BACKENDS``（含 ``subprocess``）；探测失败本身不会
    扩集。

    白名单由 ``_code_allowed_backends_for_worker`` 解析（plaita#22）：未显式配置
    ``PLAITA_SANDBOX_ALLOWED_BACKENDS`` 时默认只放行 ``docker`` ∪ 生效后端，流程
    JSON 逐节点覆盖成 ``"unsafe"`` 在解析期被拒。语言白名单由
    ``_code_allowed_languages_for_worker`` 解析（plaita#29）：默认只放行
    ``python``，``language: "js"`` 在解析期被拒。
    """
    from plaita.node import (
        SANDBOX_ALLOWED_BACKENDS_ENV,
        operator_configured_sandbox_allowed_backends,
        register_code_node,
    )

    backend = _code_backend_for_worker()
    allowed = _code_allowed_backends_for_worker(backend)
    languages = _code_allowed_languages_for_worker()
    if backend not in allowed:
        raise CodeNodeRegistrationError(
            f"CodeNode 注册拒绝：生效后端 {backend!r} 不在白名单 {allowed!r} 中"
            f"（PLAITA_CODE_BACKEND / {SANDBOX_ALLOWED_BACKENDS_ENV}），"
            "拒绝在未授权后端上注册。"
        )
    try:
        register_code_node(default_backend=backend,
                           allowed_backends=allowed,
                           allowed_languages=languages)
        logger.info("CodeNode 已注册（sandbox_backend=%s, allowed=%s）", backend, allowed)
        return
    except RuntimeError as e:  # 当前唯一来源：docker daemon 不可用
        detail = _scrub_url_text(e)
        if _code_subprocess_fallback_allowed():
            configured = operator_configured_sandbox_allowed_backends()
            if "subprocess" in configured:
                logger.warning(
                    "CodeNode 降级 subprocess（event=code_node.subprocess_fallback, "
                    "backend=%s, allowed=%s）：%s —— 已显式设置 %s=1 且 "
                    "subprocess 在 %s 中。",
                    backend, allowed, detail, CODE_SUBPROCESS_FALLBACK_ENV,
                    SANDBOX_ALLOWED_BACKENDS_ENV,
                    extra={"event": "code_node.subprocess_fallback",
                           "code_backend": backend,
                           "allowed_backends": allowed},
                )
                # 白名单沿用首次解析结果：subprocess 在 configured 中，allowed
                # 已含它；绝不因探测失败重新 resolve 扩集。
                register_code_node(default_backend="subprocess",
                                   allowed_backends=allowed,
                                   allowed_languages=languages)
                logger.info("CodeNode 已注册（sandbox_backend=subprocess，显式降级）")
                return
            reason = (
                f"{SANDBOX_ALLOWED_BACKENDS_ENV} 未包含 'subprocess'"
                f"（运营者配置={list(configured)}）"
            )
        else:
            reason = f"{CODE_SUBPROCESS_FALLBACK_ENV} 未设置（降级默认关闭）"
        logger.error(
            "CodeNode 注册拒绝（event=code_node.registration_refused, backend=%s, "
            "allowed=%s, fallback=%s）：%s；%s。不会自动降级，worker 停止启动"
            "（A56-01 fail-closed）。修复：启动 Docker daemon；或显式改 "
            "PLAITA_CODE_BACKEND=subprocess；或同时设置 "
            "%s=1 且 %s 含 subprocess。",
            backend, allowed, str(_code_subprocess_fallback_allowed()).lower(),
            detail, reason, CODE_SUBPROCESS_FALLBACK_ENV,
            SANDBOX_ALLOWED_BACKENDS_ENV,
            extra={"event": "code_node.registration_refused",
                   "code_backend": backend,
                   "allowed_backends": allowed,
                   "subprocess_fallback": _code_subprocess_fallback_allowed(),
                   "reason": reason},
        )
        raise CodeNodeRegistrationError(
            f"CodeNode 注册拒绝（backend={backend!r}, allowed={allowed!r}）：{detail}；"
            f"{reason}。不会自动降级 subprocess。修复：启动 Docker daemon；或显式改 "
            f"PLAITA_CODE_BACKEND=subprocess；或同时设置 "
            f"{CODE_SUBPROCESS_FALLBACK_ENV}=1 且 {SANDBOX_ALLOWED_BACKENDS_ENV} "
            "含 subprocess。"
        ) from e



# 节点级重试判据沿 __cause__ 链回溯的最大深度。分布式归一化链固定一层
# （FlowErrorException → NodeExecutionError），多留几层防御双重包装。
_NODE_RETRY_CHAIN_MAX_DEPTH = 5

# 队列残留回收（#43）兜底间隔：worker 启动时扫一次，之后每 N 秒一次
# （best-effort，见 RedisStreamTaskQueue.sweep_acked_residue）。
DEFAULT_RESIDUE_SWEEP_INTERVAL_SECONDS = 300.0


# flow 定义指纹的**算法标记**：只有算法口径本身变化（model_dump 行为、字段规范化）
# 才 bump。resume 时据它分级判定——算法不同但哈希相同说明定义没变（升级导致标签
# 变化），可直接续跑；算法不同且哈希也不同才需要人工显式裁决。
#
# v2（plaita#36）：指纹口径从「解析后 ``Flow.model_dump``」改为「**存储中的原始
# 定义 JSON**」（``model_validate`` 之前）。v1 的 dump 会随模型字段演进漂移——
# 同一份存储定义在新旧引擎下算出不同指纹，滚动升级窗口把在途 run 批量终态化。
# v1 → v2 是**算法口径变更**：老状态（stored_algo=v1）resume 时哈希必然不同，
# 走 ``classify_flow_hash_change`` 的 ``mismatch`` + ``upgrade_suspected`` 分级，
# 由 ``allow_flow_hash_change`` 显式放行（不静默吞掉真实定义变更）。
FLOW_HASH_ALGO = "flow-definition-json-sortkeys-v2"

# v1 算法标记：只用于识别「老状态来自旧口径」，不再用于计算新指纹。
FLOW_HASH_ALGO_V1 = "flow-dump-json-sortkeys-v1"

# resume 显式裁决键：允许在指纹变化时继续续跑（升级导致算法口径变化后的补救入口）。
# 必须由调用方在 resume data 里显式传入，且会打 WARNING + 把新指纹写回状态。
ALLOW_FLOW_HASH_CHANGE_KEY = "allow_flow_hash_change"

# 优雅停机时等待在途任务的上限（秒）；超时则放弃当前步并退出——消息不 ack，
# 由其他 worker 经 XCLAIM 从**步界检查点**续跑（该步会重放，业务节点须幂等）。
DEFAULT_WORKER_DRAIN_TIMEOUT = 30.0

# 盘预检拦下领取时的空转轮询间隔（秒；#49）：低盘期不 XREADGROUP，按该间隔
# 重探一次，盘回线即自动恢复领取（无需重启）。
DEFAULT_DISK_GUARD_POLL_SECONDS = 15.0

# 启动期服务注册失败的重试退避（A2R19-03）：`register_service()` 内部吞异常、
# 失败只返回 False；若启动瞬间 Redis 不可达就此放弃，消费循环的读路径会自愈，
# 注册路径**不会**——worker 恢复后永远缺席注册表（容量/亲和派发/readiness
# 都看不到它）。失败后按指数退避重试到成功（上限 30s），停止时立即退出。
REGISTRY_RETRY_INITIAL_SECONDS = 1.0
REGISTRY_RETRY_MAX_SECONDS = 30.0

# __new__ 构造的骨架 worker（不经 __init__）惰性补建状态写锁登记表时的守卫锁。
_STATE_WRITE_LOCKS_INIT_GUARD = threading.Lock()


def _drain_timeout_from_env() -> float:
    raw = (os.environ.get("PLAITA_WORKER_DRAIN_TIMEOUT") or "").strip()
    if not raw:
        return DEFAULT_WORKER_DRAIN_TIMEOUT
    try:
        value = float(raw)
    except ValueError:
        logger.warning(
            "PLAITA_WORKER_DRAIN_TIMEOUT=%s 非法，回退默认 %ss", _redact_env_value(raw), DEFAULT_WORKER_DRAIN_TIMEOUT
        )
        return DEFAULT_WORKER_DRAIN_TIMEOUT
    return max(0.0, value)


def classify_flow_hash_change(
    *,
    stored_hash: Optional[str],
    current_hash: str,
    stored_algo: Optional[str],
    current_algo: str,
    allow_change: bool,
) -> str:
    """resume 时 flow 指纹变化的判定表（纯函数，便于测试与复用）。

    返回：
    - ``no_guard``  ：老状态没有指纹，跳过校验（存量语义）
    - ``match``     ：指纹相同、算法标签也相同
    - ``refresh``   ：指纹相同但算法标签升级——定义确实没变，续跑并刷新标签
    - ``accepted``  ：指纹不同 + 算法标签不同 + 调用方显式放行（升级后的补救入口）
    - ``mismatch``  ：其余指纹不同——按定义变更处理，拒绝续跑并给可执行的提示
    """
    if not stored_hash:
        return "no_guard"
    if stored_hash == current_hash:
        return "match" if stored_algo == current_algo else "refresh"
    algo_changed = bool(stored_algo) and stored_algo != current_algo
    if algo_changed and allow_change:
        return "accepted"
    return "mismatch"


def _major_minor(version: str) -> str:
    """取 major.minor（'0.6.1' → '0.6'）；非法值原样返回，免得比较时抛错。"""
    parts = str(version).split(".")
    return ".".join(parts[:2]) if len(parts) >= 2 else str(version)


def _engine_version() -> str:
    """当前引擎版本（只用于观测：记录「谁创建了这个执行」）。"""
    try:
        from plaita import __version__

        return __version__
    except Exception as exc:  # noqa: BLE001 — 版本信息拿不到不能影响执行
        logger.debug("读取引擎版本失败，按 unknown 记录: %s", _scrub_exception_text(exc))
        return "unknown"


def canonical_flow_definition_json(flow_definition: Mapping[str, Any]) -> str:
    """把**存储中的原始流程定义**规范化为稳定 JSON 文本（指纹的输入面）。

    plaita#36：指纹只取这一层——``model_validate`` 之前的定义才是「作者写下的
    语义」。解析后的 ``Flow.model_dump(mode="json")`` 会随模型字段演进（新增/
    删除字段、默认值的物化、别名规范化）改变形态，同一份存储定义在新旧引擎下
    算出不同指纹，滚动升级窗口把在途 run 批量终态化。

    规范化口径：``sort_keys``（dict 序无关）+ ``ensure_ascii=False`` +
    ``default=str``（非 JSON 原生值降级为 str，与 ``sanitize_context_for_persist``
    同款兜底，绝不为「日志/指纹路径」抛）。
    """
    return json.dumps(
        flow_definition,
        sort_keys=True,
        ensure_ascii=False,
        default=str,
        separators=(",", ":"),
    )


def compute_flow_definition_hash(flow_definition: Mapping[str, Any]) -> str:
    """原始定义 JSON 的 sha256（跨进程/跨引擎版本确定）。"""
    return hashlib.sha256(
        canonical_flow_definition_json(flow_definition).encode("utf-8")
    ).hexdigest()


def sanitize_context_for_persist(context: Any) -> Optional[dict]:
    """#80 层 1：把不可 JSON 化的值**降级为 str**，返回**副本**。

    **绝不就地改原对象** —— 原 ``context`` 仍被其它路径引用（终态化是收尾动作，
    不该反向改动流程语义）。

    返回 ``None`` 表示「连兜底都失败」（如**循环引用**，``json.dumps`` 会抛
    ``ValueError: Circular reference detected``）⇒ 由调用方走层 2（最小 error 状态）。
    """
    def _default(o: Any) -> str:
        try:
            return str(o)
        except Exception:  # noqa: BLE001 —— __str__ 自己抛异常的对象
            return f"<unstr-able {type(o).__name__}>"

    try:
        return json.loads(json.dumps(context, default=_default))
    except Exception:  # noqa: BLE001 —— 循环引用等，交给层 2
        return None


def _is_retryable_node_failure(exc: BaseException) -> bool:
    """判别「可重试的节点执行失败」vs「协议/图错误/超时/取消」。

    分布式路径上 ``run_distributed`` 把一切异常归一化为 ``FlowErrorException``
    且原始异常挂在 ``__cause__``（core/_error_normalization.py:59-69）。判据：

    - 链中出现 ``NodeExecutionError``（runner.py 节点 abort 时 raise，原始
      异常再挂它的 ``__cause__``）→ 默认**可重试**——LLM/HTTP 网络抖动属
      瞬态，磁盘 state 仍停在最后成功步的 checkpoint（strategies.py
      ``_execute_current_node`` 的 ``runner.run_node`` 抛出时 context 未变）；
    - **A50-03**：``NodeExecutionError`` 只是 abort 包装层，原始节点异常挂在
      它的 ``__cause__``。若更深处是 node-F2 判定的确定性错误（1001 /
      ValueError·TypeError·NameError·SyntaxError 及沙箱信封 ``origin_type``
      落集同型，#43），**不重试**——否则 core 里「执行 1 次」的分类在
      worker 层被包装层抹掉，同一确定性错误还会被重投 5 次。分类直接复用
      ``core.runner.is_retryable_error``（单一来源），保留 #219 输出校验
      异常的可重试例外；
    - 链中出现超时（``NodeTimeoutError``/``FlowTimeoutError``）→ **不重试**
      （确定性信号，重试=再烧一次全款，对齐 v3 宿主 D4 决策），维持现状
      终态化 error；
    - 链中出现取消（``FlowCancelledException``）→ **不重试**（执行级意图）；
    - 其余（``ResumeError`` 等协议类、图结构错误、无 ``__cause__`` 的引擎
      管线异常）→ **不重试**，维持现状终态化。

    超时/取消优先于 NodeExecutionError 判定：防御节点函数把超时包进普通
    异常再被 abort 包装的病态链。
    """
    cause = getattr(exc, "__cause__", None)
    depth = 0
    node_failure_seen = False
    while cause is not None and depth < _NODE_RETRY_CHAIN_MAX_DEPTH:
        if isinstance(cause, (NodeTimeoutError, FlowTimeoutError, FlowCancelledException)):
            return False
        if isinstance(cause, NodeExecutionError):
            node_failure_seen = True
        elif node_failure_seen and not is_retryable_error(cause):
            # abort 包装层之下的原始异常：确定性分类（F2）不得被包装抹掉。
            return False
        cause = cause.__cause__
        depth += 1
    return node_failure_seen


def _chain_has_cancellation(exc: BaseException) -> bool:
    """异常链（含自身）中是否含执行级取消 ``FlowCancelledException``。

    波次③：取消监听在节点执行期置位 ``cancel_requested`` 后，引擎在**下一个
    节点入口**抛 ``FlowCancelledException``；distributed 策略把它归一化为
    ``FlowErrorException`` 并把原始异常挂在 ``__cause__``（_error_normalization）。
    步循环的通用 ``except`` 需要据此把终态写成 ``cancelled`` 而非 ``error``。

    取消优先于一切其他判定：链中一旦出现取消即整链定性为取消（即便更深处
    还有被 abort 包装的节点异常）。
    """
    node: Optional[BaseException] = exc
    depth = 0
    while node is not None and depth <= _NODE_RETRY_CHAIN_MAX_DEPTH + 1:
        if isinstance(node, FlowCancelledException):
            return True
        node = node.__cause__
        depth += 1
    return False


def _chain_has_resume_guard_error(exc: BaseException) -> bool:
    """异常链（含自身）中是否含**恢复守卫** ``ResumeGuardError``。

    #33 配套：``run_distributed`` 把策略层异常归一化为 ``FlowErrorException``
    且原异常挂 ``__cause__``，worker 通用 except 因此需要「豁免」判据。

    豁免面**刻意很窄**——只认 ``ResumeGuardError``（策略层**分发到
    ``current_node.resume()`` 之前**的守卫：continue/retry 想绕过 pending 挂起
    节点、resume 打在无挂起节点/非挂起节点的 checkpoint 上）。这类 ResumeError
    描述的是**调用方协议与执行状态不匹配**：消息类型与执行状态对不上时必须
    保持原状（挂起执行等 event/cancel/timeout 决议），终态化 error 会把它永久
    打封（#33）；且它天然由 at-least-once 重投/竞速产生（重复投递的 event
    resume 打在已推进的 checkpoint 上也是同族），终态化会误杀健康执行。

    **不**豁免裸 ``ResumeError``：``plaita/core/strategies.py`` 的
    ``_handle_resume`` 把 ``current_node.resume()`` 抛出的**任何**异常包成
    ``ResumeError``（事件数据畸形 / 节点恢复逻辑失败…）——那是执行自身失败，
    静默保持原状只会变成一个「无 error 记录、永远等不到决议」的哑执行；
    交给下面的现状路径（可重试判据 → 终态化 error + poison ack）才可观测、
    可人工 retry。
    """
    node: Optional[BaseException] = exc
    depth = 0
    while node is not None and depth <= _NODE_RETRY_CHAIN_MAX_DEPTH + 1:
        if isinstance(node, ResumeGuardError):
            return True
        node = node.__cause__
        depth += 1
    return False


def _node_id_from_chain(exc: BaseException) -> Optional[str]:
    """从异常链取**失败节点 id**（plaita#123：计数键按节点隔离的前提）。

    主来源是 ``NodeExecutionError.node``——runner.py:144 在节点 abort 时
    构造并带上 ``node``（Flow Node 对象），经 ``_error_normalization``
    归一化后原异常挂在 ``__cause__``。取 ``node.id``（退化取 ``node.name``）；
    历史/测试调用方也传过裸节点 id 字符串（``NodeExecutionError(msg, node="a")``），
    一并识别——否则计数会静默退化成执行维度、又回到 #123 的整执行清零形态。

    为何不用 checkpoint 的 ``$LAST_NODE``：失败节点**不写 context**
    （runner.run_node 成功后才 ``update_node_result``/``last_node_id``），
    所以 ``$LAST_NODE`` 指向的是**上一个成功节点**，拿它当失败节点会张冠
    李戴——恰好在「前序节点成功 + 当前节点失败」的事故形态上把计数记到
    错误的键上。链中没有 NodeExecutionError（即非可重试失败）时返回 None，
    调用方退化为执行维度键（保守，不比修复前差）。
    """
    node: Optional[BaseException] = exc
    depth = 0
    while node is not None and depth <= _NODE_RETRY_CHAIN_MAX_DEPTH + 1:
        if isinstance(node, NodeExecutionError):
            target = getattr(node, "node", None)
            if isinstance(target, str) and target:
                return target
            for attr in ("id", "name"):
                value = getattr(target, attr, None)
                if isinstance(value, str) and value:
                    return value
            return None
        node = node.__cause__
        depth += 1
    return None


# ---------------------------------------------------------------------------
# 落盘错误的结构化字段（plaita#93）
# ---------------------------------------------------------------------------
#
# 落盘边界（各处 ``state.error``）历史上只写 ``str(e)``：引擎已经算出的
# ``code`` / ``error_type``（``FlowExecutionException`` 自带）与 HTTP 节点
# 特意挂载的 ``exc.details`` / ``exc.response``（plaita/node/http.py 的
# ``wrap_http_node_err``）在最后一步全部被抹平，执行终态只剩一段中文 message
# ——console / DLQ 巡检 / issue-keeper 之类下游只能 ``startswith`` 解析文本。
#
# 非异常驱动的两处终态化（flow_hash 不匹配 / 死信守卫重试预算耗尽）没有可抄
# 的异常，用引擎自己的**通用错误身份**标注并把 worker 级判据写进 ``error_type``
# （见各站点的注释）——不编造"看起来像节点业务码"的数字。

# ``FlowExecutionException.code`` 的基类默认值（= FlowErrorType.FLOW_ERROR）。
_FLOW_ERROR_CODE = -500


def _iter_error_chain(exc: BaseException):
    """沿 ``__cause__``（缺省退 ``__context__``）上溯异常链。

    深度与重试判据（``_NODE_RETRY_CHAIN_MAX_DEPTH``）同口径，并按对象 id 去环。
    """
    node: Optional[BaseException] = exc
    seen: Set[int] = set()
    depth = 0
    while node is not None and depth <= _NODE_RETRY_CHAIN_MAX_DEPTH:
        if id(node) in seen:
            return
        seen.add(id(node))
        yield node
        node = getattr(node, "__cause__", None) or getattr(node, "__context__", None)
        depth += 1


def _jsonable_error_value(value: Any) -> Any:
    """错误字段 → JSON 可序列化标量（``FlowErrorType`` 等枚举取 ``.value``）。"""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return value.value
    return str(value)


def _error_details_summary(details: Any) -> Optional[Dict[str, Any]]:
    """``exc.details``（HTTP 节点的 ``HttpNodeErrorInfo``）→ 落盘安全的紧凑摘要。

    只走 ``details.redacted()``（#160 的**唯一**掩码视图，JSON 安全）并丢掉
    body：``exc.response`` 已有一份截断（``_ERROR_FRAME_BODY_MAX_CHARS``）且
    脱敏的摘要，而落盘状态是长期资产——status/code/headers 足够定位「连不上」
    还是「远端返回了什么」，不必把 body 再抄一份。任何渲染异常都吞掉：
    明细丢失绝不能拖垮终态化。
    """
    render = getattr(details, "redacted", None)
    if not callable(render):
        return None
    try:
        view = render()
    except Exception as exc:  # noqa: BLE001 — 明细渲染失败不影响终态化
        logger.debug("执行明细渲染失败（不影响终态化）: %s", _scrub_exception_text(exc))
        return None
    if not isinstance(view, dict):
        return None
    summary: Dict[str, Any] = {}
    if view.get("code") is not None:
        summary["code"] = _jsonable_error_value(view["code"])
    if view.get("message") is not None:
        summary["message"] = _jsonable_error_value(view["message"])
    request = view.get("request")
    if isinstance(request, dict):
        summary["request"] = {k: request.get(k) for k in ("method", "url", "headers")}
    response = view.get("response")
    if isinstance(response, dict):
        summary["response"] = {k: response.get(k) for k in ("status", "statusText", "headers")}
    return summary or None


def _error_fields(exc: Optional[BaseException]) -> Dict[str, Any]:
    """从异常链提取可落盘的**结构化错误字段**（plaita#93）。

    取值口径：

    * ``code`` / ``error_type`` **优先取节点级引擎异常**（``NodeExecutionError``
      / ``NodeTimeoutError``）：分布式路径上 ``run_distributed`` 把一切归一化成
      ``FlowErrorException``（外部契约 code=-500，见
      ``docs-site/docs/guide/error-handling.md``），节点身份挂在 ``__cause__``
      上——只取外壳会把「哪种错误」也一并抹平。链上没有节点级异常时取**最深的**
      引擎异常（``FlowExecutionException`` 子类；外壳只是归一化容器，语义在
      ``__cause__``），再退回链首——真正终态化那次异常。
    * 链上没有任何 ``error_type`` 时以异常类名兜底：落盘字段必须**永远存在**
      且机器可判别，否则下游又只能回去解析文本。
    * ``details`` / ``response`` 取链上第一份（HTTP 节点把它们挂在链尾的
      ``NodeException`` 上）；``response`` 已是脱敏 + 截断的摘要，可直接落盘。
    * 返回值只用 JSON 可序列化标量；取不到任何字段时返回 ``{}``，调用方
      ``**_error_fields(e)`` 的字典形状与修复前逐字一致（老消费方零影响）。
    """
    if exc is None:
        return {}
    chain = list(_iter_error_chain(exc))
    if not chain:
        return {}
    identity: Optional[BaseException] = next(
        (node for node in chain
         if isinstance(node, (NodeExecutionError, NodeTimeoutError))), None)
    if identity is None:
        engine = [node for node in chain if isinstance(node, FlowExecutionException)]
        identity = engine[-1] if engine else chain[0]
    fields: Dict[str, Any] = {}
    code = getattr(identity, "code", None)
    if code is not None:
        fields["code"] = _jsonable_error_value(code)
    error_type = getattr(identity, "error_type", None)
    fields["error_type"] = (_jsonable_error_value(error_type)
                            if error_type is not None else type(identity).__name__)
    response = next((node.response for node in chain
                     if isinstance(getattr(node, "response", None), dict)), None)
    if response:
        fields["response"] = response
    details = next((node.details for node in chain
                    if getattr(node, "details", None) is not None), None)
    summary = _error_details_summary(details)
    if summary:
        fields["details"] = summary
    return fields


class StatePersistError(RuntimeError):
    """执行状态落盘失败（``save_execution_state`` 返回 False）——**瞬态**那一支。

    **#80 起本类只表示「可重试的落盘失败」**（Redis 瞬断 ⇒ 返回 False）。
    「不可序列化」是**永久**失败、已从本类**分家**为
    ``plaita.storage.base.StateSerializationError``（存储层直接抛，不返回 False），
    由调用点转成可观测终态——**别把两者再合回一个类型**：那正是 #80 的病灶。
    （本条注释原写『网络瞬断/序列化失败均如此』，已随契约分家同步。）

    历史：终态/挂起/步进
    落盘失败若被静默放行，消息会被 ack——节点副作用已发生而执行状态永远
    停在旧值（僵尸执行）。此处把 False 升级为异常，让 ``RedisFlowWorker.run``
    的兜底路径不 ack 消息、走 at-least-once 重投。

    刻意**不是** ValueError 子类：run() 把 ValueError 当畸形消息 poison ack，
    继承它会让存储瞬态错误仍被丢弃（ReviewFix D1 同款教训）。
    """


# G1 唤醒记账原子收口（第七十一轮任务①）：清 noderetry 全族（执行维度缺省键
# + 节点维度 ``{id}:*`` 子键，与 _delete_node_retry_keys 的 SCAN 语义一致）与
# INCR/EXPIRE g1wakeups 必须同轮生效——修复前两步散调，清预算失败被吞而记数
# 照常执行 ⇒ 配额被消耗但没拿到新预算，半生效唤醒净烧唤醒额度。
# KEYS[1]=noderetry 基键；KEYS[2]=g1wakeups 键；ARGV[1]=g1wakeups TTL 秒。
_G1_WAKEUP_COMMIT_LUA = """
local base = KEYS[1]
local cursor = '0'
repeat
  local scan = redis.call('SCAN', cursor, 'MATCH', base .. ':*', 'COUNT', 100)
  cursor = scan[1]
  for i = 1, #scan[2] do
    redis.call('DEL', scan[2][i])
  end
until cursor == '0'
redis.call('DEL', base)
redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[1]))
return 1
"""


class G1WakeupAccountingError(StatePersistError):
    """G1 唤醒记账原子收口失败（第七十一轮任务①）——瞬态，StatePersistError 子族。

    ``retry_wakeup`` 的「清 noderetry 预算 + 记 g1wakeups 配额」两步已收敛为
    单段 Lua（``_commit_g1_wakeup``），脚本未生效即抛本类。此时状态行已翻
    running，但**预算未清、配额未记（整体不生效，配额守恒）**——绝不许出现
    修复前的「配额已消耗、预算没拿到」半生效态。消息不 ack 走 at-least-once
    重投；重投见 running 行按「从断点续跑」正常推进（预算仍是耗尽态，下次
    失败照常终态化，届时 retry 唤醒不占额度——记账从未发生），收敛。
    继承 StatePersistError 是契约的一部分：resume_flow 对该族原样上抛、
    不终态化，记账失败绝不能把已翻 running 的执行打成 error。
    """


class ServiceDispatchError(RuntimeError):
    """挂起服务任务投递失败（有 redis 客户端时不再吞）。

    挂起时先 save(suspended) 再 rpush 服务队列；rpush 失败若被吞，执行已
    suspended、消息被 ack → delay/approval 任务永无人接，永久挂起。抛出后
    消息走重投、下轮 resume 重新执行挂起节点再派发（重复注册的订阅由
    EventFilter 终态 GC / TTL 兜底）。suspended 状态保留，不翻 error。
    """


class ExecutionIdConflictError(ValueError):
    """预铸 ``execution_id`` 已属于另一条 flow：拒绝启动，绝不覆盖盘上状态。

    台账第三十一轮 V1（worker P2 批）：盘上 execution_id=X 属于 flow A 时，
    一条 ``start_flow(flow_id=B, execution_id=X)`` 的载体（畸形/误重投/ID
    复用）会让 start 新建 ``context={}`` 的 B running 行**覆写 X**——A 的
    checkpoint/状态永久丢失。现在 start 载体查盘发现 flow_id 不一致即抛本
    异常，不落任何行、不动 A 的状态。

    刻意继承 ``ValueError``：run() 把 ValueError 当畸形消息 poison ack——
    同一 execution_id 换 flow 重投永远不会自愈，重投只烧 delivery。
    状态保留 + 具名日志是这条路径的可观测面。
    """


class FlowHashMismatchError(ValueError):
    """flow 定义自启动后已变更（hash 不匹配），执行无法安全续跑（波次二任务②）。

    resume_flow 在终态化 error **之后**抛出本异常：run() 把它当 ValueError
    poison ack（终态已可观测，重投只会命中 already_terminal 短路）。刻意
    用独立子类而非裸 ValueError——resume 的通用 except 需要区分「指纹
    不匹配（已终态化，原样上抛）」与「引擎内部其他 ValueError（维持现状
    终态化路径）」。
    """


class FlowDefinitionMissingError(ValueError):
    """流程定义在引擎存储中缺失（版本被删/未同步/不存在），执行无法推进（#304）。

    刻意继承 ``ValueError``：既有语义下 run() 把它当畸形消息 poison ack。
    与裸 ValueError 的分界在**捕获方义务**——start/resume 入口捕获到本异常
    后必须先把执行终态化 error（``error.error_type="flow_definition_missing"``
    机器可判身份，经 ``_terminalize_definition_missing``）再原样上抛；否则
    执行行永驻 queued/suspended、start 准入名额不释放——#304 病灶正是
    「裸 ValueError 直接 poison ack，状态行无人写」。终态化用 error 而非
    cancelled 与 flow_hash mismatch 同口径：版本恢复/重发后仍可
    ``resume_type=retry`` 从断点唤醒（G1「error 态可反复唤醒」语义保留）。
    """


class NodeFailureTerminalizedError(ValueError):
    """节点失败已**终态化 error**（非瞬态/预算耗尽）——消息必须 poison ack。

    为什么要有这个独立异常（plaita#73，2026-10-10 实证）：

    终态化路径原本抛裸 ``RuntimeError``。``RuntimeError`` **不是**
    ``ValueError``，于是一路落到 ``run()`` 的兜底 ``except Exception``
    ——那条分支**不 ack**、留 pending 等超时回收 ⇒ 消息被反复重投 ⇒
    ``resume_flow`` 见 ``status=error`` 只放行 ``resume_type=retry`` ⇒
    再次撞同一确定性失败 ⇒ 再次终态化 + 再次 ``RuntimeError``……
    **状态早已 error，消息却永远在转**（实测某日同一执行重投 598 次、
    沙箱实例持续占位 5.66 实例小时）。

    更糟的是这条路径**不碰重试计数键**：``INCR`` 位于
    ``_node_failure_retry_decision`` 内、在 ``_is_retryable_node_failure``
    判据**之后**，确定性失败（``exited 1`` / ``AgsError: sync_in`` /
    超时）直接 ``return None`` ⇒ 计数键恒为 1（实测全库 74 个键**全为 1**）
    ⇒ 「预算耗尽」分支从未触发，重投**无界**。

    语义与既有的 ``FlowHashMismatchError`` 同族：**raise 前状态已终态化**，
    run() 按 ``ValueError`` poison ack（终态已可观测，重投只会命中
    ``already_terminal`` 短路，纯属浪费）。刻意用独立子类而非裸
    ``ValueError``，以便 resume 的通用 except 区分「已终态化」与
    「引擎内部其他 ValueError（维持现状终态化路径）」。
    """


class TenantQuotaDeferred(RuntimeError):
    """start 消息超出租户并发配额（plaita#28）：**延迟**，不是失败。

    消费入口抛出——由 run() 的专门分支**重投**（见 _defer_over_quota_start）：
    入队一份同体新副本到 stream 尾部（让位给更早入队的其他租户消息）+ ack
    原条。刻意不「留 pending」：XCLAIM 回收每轮让 deliveries +1，烧到
    max_deliveries 会误死信——配额已满不是消息的错，绝不能因排队而死信。
    刻意**不是** ValueError 子类（防 poison ack 语义误伤）。

    命中前提：消息不带名额（非 console 入口生产方）。console start 在入口
    即占名额（try_admit_tenant_start），其消息抵达 worker 时已是成员，闸
    幂等放行——worker 闸只拦「绕过入口的第三方生产方」的洪峰。

    **有界**（防重投空转）：副本带 ``quota_defers`` 计数，达
    ``PLAITA_TENANT_QUOTA_DEFER_MAX``（默认 64）后带 ``quota_bypass`` 放行
    ——宁可短暂突破软配额，不可让消息在队列里无限空转烧 CPU。
    """


class TenantGateEpistemicReject(RuntimeError):
    """新 start 的租户状态闸认识论性拒绝（plaita#326，#312 词汇表）。

    读失败型（status_read_failed / generation_read_failed）——「不知道租户
    状态」，不是确认态。消费入口抛出，由 run() 的专门分支**留 pending**：
    不 ack、不重投、不收容、不终局化，等 XCLAIM 重投，瞬断自愈后下轮过闸。
    重投烧到 max_deliveries 触死信守卫时，守卫对读失败型原因同样不收容
    （#312 修后语义），闭环无终局化出口。刻意不是 ValueError 子类（防
    poison ack 语义误伤），也不是 TenantWriteRejected 子类（那是走收容
    分流的确认态通道，处置语义相反）。
    """

    def __init__(self, tenant_id: Optional[str], reason: str):
        self.tenant_id = tenant_id
        self.reason = reason
        super().__init__(
            f"tenant start gate epistemic reject: tenant={tenant_id!r} "
            f"reason={reason}; carrier stays pending until the status view "
            "is readable again"
        )


class TaskNotForThisWorker(RuntimeError):
    """任务与本机不亲和（repo/run_dir 指向别的机器的绝对路径）。

    消费入口抛出——由 run() 的专门分支**交接**（见 _handover_non_affine）：
    ack 原消息 + 重入队一份同体新副本（delivery 归 1）。不终态化、不死信：
    任务本身没毛病，只是不该在本机跑。

    为何要「重入队」而非「留 pending」：留 pending 的消息会被对端 worker
    按 claim_min_idle_ms 反复 XCLAIM，而 XCLAIM 每次让 Redis 的
    deliveries +1——两 worker 环境下 60s 一轮，5 轮即触顶 max_deliveries
    → 误死信（2026-10-06 双机实测：非亲和任务被让出 5 次后进 DLQ）。
    重入队新副本把计数清零，交接无损耗。

    **但有界**（2026-10-11 无限交接风暴）：重入队使 `times_delivered` 恒为 1
    ⇒ `max_deliveries` 永不触发 ⇒ 若判据不可满足，交接**没有**终止条件。
    故交接次数由 worker 侧按 `execution_id` 计数约束（`AFFINITY_HANDOVER_MAX`），
    达上限即**终态化 error**（可观测），不再交接——见
    `_handover_non_affine` / `_terminalize_handover_exhausted`。
    """


class ResumeProtocolError(RuntimeError):
    """resume 协议错误（#33）：执行状态与 resume_type 不匹配，执行保持原状。

    策略层守卫 ``ResumeGuardError``（continue/retry 试图绕过 pending 挂起节点、
    resume 打在无挂起/非挂起 checkpoint 上）经 ``_chain_has_resume_guard_error``
    判出后，resume_flow 不终态化、把执行留在原状态（suspended/running），
    以本异常上抛。**裸 ``ResumeError`` 不在豁免面内**（节点 ``resume()`` 自身
    抛错是执行失败，走终态化 error 的现状路径，见该判据 docstring）。
    run() 主循环按
    「消费失败」处理：消息走 ack（执行状态原样可查、事件订阅仍在，真正的
    决议路径 event/cancel/timeout 随时可入），重投只会重复命中同一守卫，
    无意义。**例外（plaita#258）**：event/timeout 载体在 ``running`` 中间态
    已错过挂起点（订阅已注销）时，ack 前先状态感知并归一化 ``continue``
    重投——见 ``_handle_resume_protocol_error``；直接 ack 会丢掉唯一载体。
    刻意**不是** ValueError 子类（防 poison ack 语义误伤）。
    """

    def __init__(self, message: str, execution_id: Optional[str] = None):
        super().__init__(message)
        self.execution_id = execution_id


class NodeExecutionRetryableError(RuntimeError):
    """节点执行可重试失败（波次二任务①）：执行**未**终态化，消息等重投。

    节点异常经 ``_is_retryable_node_failure`` 判为瞬态且重试预算未耗尽时，
    处理函数跳过终态化改抛本异常——磁盘 state 停在最后成功步 checkpoint，
    ``run()`` 据此**退避后显式重投**一份新副本（``_requeue_retryable_failure``：
    按 ``attempt`` 指数退避后先入队、后 ack 原条，plaita#91），新副本从
    checkpoint 重跑失败节点；start 载体且盘上已有 checkpoint 时重投副本
    转换为 resume 载体（不整条重放、不覆写 checkpoint）。

    2026-10-09 变更（plaita#52 真因）：此前是「不 ack、留 pending 等
    claim_min_idle_ms 回收」。该语义在**消息已被竞争者 ack 释放**时不成立
    （ExecutionLeaseError 分支为避免烧 delivery 会显式 ack），实测导致执行
    既不重试也不终态、卡在 running 58 分钟。现改为显式重投；仅当重投本身
    失败（Redis 抖动等）才退化为留 pending 交 reclaim。2026-10-16（#91）：
    显式重投补上指数退避（旧实现无节奏，5 次预算 ~40ms 烧光）与 start
    载体的 resume 转换。

    刻意**不是** ValueError 子类（同 StatePersistError 的理由）：ValueError
    会被 run() 当畸形消息 poison ack。

    Attributes:
        execution_id: 所属执行（日志与守卫后备判据用）。
        attempt: 已放行的重试次数（重试计数键 INCR 后的值）。
        delivery_count: 消息当前投递次数（仅可观测；预算判定用重试计数键，
            见 ``_record_node_retry`` 的说明）。
    """

    def __init__(self, message: str, execution_id: Optional[str] = None,
                 attempt: int = 0, delivery_count: Optional[int] = None):
        super().__init__(message)
        self.execution_id = execution_id
        self.attempt = attempt
        self.delivery_count = delivery_count


class FlowWorker:
    """
    流程工作器：把 ``run_distributed`` 与 ExecutionStorage / FlowStorage / EventBus 串起来。

    这是 **suspend/resume 编排器**，不是具备至少一次投递或崩溃安全的工作流引擎。
    可靠性边界见 docs-site ``distributed/flow-worker.md`` 与类常量
    ``PERSIST_EVERY_N_STEPS`` / ``RedisFlowWorker.run`` 的队列语义说明。
    """

    # 连续推进时每隔 N 步落一次中间态。挂起 / 结束 / 出错始终立即持久化。
    # 默认 1 = 每步落盘，崩溃不丢步进进度（Wave 3）。
    PERSIST_EVERY_N_STEPS = 1

    def __init__(
        self,
        execution_storage: ExecutionStorage,
        flow_storage: FlowStorage,
        event_bus: EventBus = None,
        cache_size: int = 100,
        cache_ttl: int = 300,
        callback_handlers: Optional[list] = None,
        execution_lease: Optional[ExecutionLease] = None,
        lease_ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
    ):
        """
        初始化流程工作器

        Args:
            execution_storage: 执行状态存储实例
            flow_storage: 流程定义存储实例
            event_bus: 事件总线实例
            cache_size: 缓存大小，默认100
            cache_ttl: 缓存过期时间(秒)，默认300秒
            callback_handlers: 分布式执行期间贯穿所有步骤的回调列表
            execution_lease: resume 租约（默认 NullExecutionLease）；RedisFlowWorker 注入 Redis 实现
            lease_ttl_seconds: resume 租约 TTL（秒），推进过程中会 renew
        """
        self.execution_storage = execution_storage
        self.flow_storage = flow_storage
        self.event_bus = event_bus
        self.callback_handlers = list(callback_handlers) if callback_handlers else []
        # 节点耗时采集：**按执行**一个采集器（并发处理多个执行时不串场），只监听
        # on_node_start/on_node_end；落盘时由 _persist_state_or_raise 写入
        # ExecutionState.node_timings。
        self._node_timings: Dict[str, NodeTimingCallback] = {}
        # run 级事件时间线（plaita#25）：**按执行**一个采集器，与 _node_timings
        # 同一对钩子、同一落盘收口（_collect_execution_timeline）。它同时是本进程
        # 向 SSE 频道 ``plaita:execution:events:{id}`` 发布事件的**唯一发布者**
        # （修复前该频道全仓零发布者，订阅者除 initial_state 外永远静默）。
        self._timelines: Dict[str, ExecutionTimelineCallback] = {}
        # 执行状态写的**每执行互斥锁**（见 _state_write_lock）：同一 execution_id
        # 的所有状态写（推进路径 _persist_state_or_raise ↔ 看门狗心跳
        # _publish_node_progress）必须串行，否则心跳的「读-改-写」会把并发终态写
        # 整行回滚（2026-10-10 评审复现：completed 被改成 running + 旧 checkpoint）。
        # WeakValueDictionary：没有线程再持有该锁时自动回收（长跑 worker 不泄漏）。
        self._state_write_locks: "weakref.WeakValueDictionary[str, threading.RLock]" = (
            weakref.WeakValueDictionary()
        )
        self._state_write_locks_guard = threading.Lock()
        # 沙箱生命周期回调：**按执行**缓存（同一执行的多个 step 必须复用同一个
        # 实例，否则 agent 节点在早先 step 产生的 workspace 快照到 flow 结束那一步
        # 已经丢了）；终态落盘时按持久化上下文释放并回收（_release_sandboxes）。
        self._sandbox_callbacks: Dict[str, Any] = {}
        # 优雅停机（无损升级）：draining = 不再领新任务、等 in-flight 收尾；
        # 超时由 _force_stop_after_drain 兜底退出（消息留 pending 待接管）。
        self._draining = threading.Event()
        self._drain_timer: Optional[threading.Timer] = None
        self._drain_started_at: Optional[float] = None
        # 注意：_draining 在 _drain_event 里惰性兜底，__new__ 构造的骨架 worker 也安全
        # 初始化流程定义缓存，使用TTL缓存
        self.flow_definition_cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)
        # 原始定义指纹缓存（plaita#36）：与 flow_definition_cache 同窗口，
        # 在 get_flow_definition 里随解析结果一起写入。
        self.flow_definition_hash_cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)
        self.execution_lease = execution_lease or NullExecutionLease()
        self.lease_ttl_seconds = lease_ttl_seconds
        # 取消监听（波次③：步内可中断）：基类先建好登记表与停止位，内存 worker
        # 不启动线程（_cancel_requested 无 redis 客户端时恒为 False，登记为空转）。
        self._cancel_watch_lock = threading.Lock()
        self._cancel_watch: Dict[str, Tuple[Any, str]] = {}
        self._cancel_thread: Optional[threading.Thread] = None
        self._cancel_stop = threading.Event()
        self._cancel_poll_seconds: Optional[float] = None
        # A69-04：年龄闸「两源都不可判」的 fail-closed 计数（/metrics 暴露为
        # carrier_age_unknown_total；只增不减，运维据此判断是否有生产者缺 timestamp）。
        self._carrier_age_unknown_total = 0
        # A69-04：启动期保留期配置校验（每进程一次；可疑配置只 WARNING 不阻断）。
        validate_tombstone_retention()

    # ---- 节点级有界重试（波次二任务①）----

    # 重试计数键 TTL：7 天自清理（与取消标志键同款「带 TTL 意图键」模式）。
    NODE_RETRY_COUNTER_TTL_SECONDS = 7 * 86400

    # G1 唤醒上限（plaita#73 补丁）：同一执行最多被 `resume_type=retry` 唤醒
    # 这么多次，防「唤醒→重置预算→再耗尽→再唤醒」无限循环。默认 2（人工救
    # 一次 + 自动救一次足够；再多说明该失败是确定性的，救不回来）。
    G1_MAX_WAKEUPS = 2
    G1_WAKEUP_COUNTER_TTL_SECONDS = 7 * 86400

    # 确定性失败**连续**次数上限（plaita#73 遗留层，2026-10-10）：`exited 1` /
    # `AgsError` / 超时 / 协议错等**不可重试**的失败也要有界。此前它们完全不
    # 进计数器（2026-10-10 本机 Redis db1 快照：全库 noderetry 键 74/74 恒为 1
    # 即此故；键数随时间变化，勿当常量），唯一收敛机制是消息层
    # 重投、而消息层无次数概念 ⇒ 同一执行被重投数百次、沙箱持续占位。
    # 达上限即判「不可救」，与可达上限分开记账（noderetry 是可重试预算，语义不同）。
    #
    # 计数语义是「**连续**失败」：任一节点成功推进即清零（见
    # `_reset_deterministic_failure_counter` 的调用点）。只增不减时，一个跑了几天、
    # 在**不同**节点各撞过一次确定性失败的长寿命执行会被累计判死——那远没到
    # 「该放弃」的程度（2026-10-10 评审）；连续 12 次才等于「同一位置反复撞墙」。
    # 刻意**不**在 G1 retry 唤醒时清零（那是收不上来的自毁循环：唤醒→清零→再撞
    # →再唤醒，见 G1_MAX_WAKEUPS 的来历）。
    DETERMINISTIC_FAILURE_MAX = 12
    DETERMINISTIC_FAILURE_TTL_SECONDS = 7 * 86400

    # 兜底上限（plaita#123 评审建议，2026-10-10）：**不看键名、只看次数**。
    # 节点维度预算（noderetry:{id}:{node}）与执行维度预算都可能因「键空间
    # 变化」而失效——例如 flow 里节点数很多、节点 id 每轮漂移（动态 id /
    # 定义被改），或将来又把 execution_id 引回键里却漏了某条路径。该计数
    # **不含 node_id、不含任何会漂移的段**，只按 execution_id 累计「本执行
    # 一共放行过多少次节点重试」，达上限即终态化 error——保证任何键空间
    # 异常都不会再退化成「无限重投」。
    #
    # 取值远大于单节点预算 × 合理节点数（默认 5 × 20 = 100），只在
    # 「预算机制整体失效」时才兜住；正常执行永远碰不到。
    NODE_RETRY_TOTAL_MAX = 100
    NODE_RETRY_TOTAL_TTL_SECONDS = 7 * 86400

    # 不亲和交接的**有界上限**（2026-10-11 无限交接风暴，plaita#68 等）。
    #
    # 事故机制：`aa74ab6` 的宿主归属判据对**每台** worker 都成立（`runs/` 两边
    # 都在、run 目录两边都没建）⇒ 交接永远无法让判据变为满足；而每次交接都是
    # `ack` 原条 + `enqueue_task` **新 msg id** ⇒ `times_delivered` 恒为 1 ⇒
    # 队列层 `max_deliveries=5`（`task_queue.py`）**永不触发** ⇒ 无界自锁
    # （实测双机各 2600+ 次、持续 ≥85 分钟，日志 26 MB）。
    #
    # 因此「同一执行被交接几次」必须由 worker 侧**独立计数**约束，且达上限即
    # **终态化**（不是留 pending——留 pending 的消息仍会在 `claim_min_idle_ms`
    # =60s 后被对端 `XCLAIM` 抢回，实测 2356 次留 pending 全部被回收，是无限
    # reclaim 循环，不是界）。
    AFFINITY_HANDOVER_MAX = 3
    # 计数键 TTL 必须**远长于**回收周期（claim_min_idle_ms=60s）：TTL 短于回收
    # 周期时计数会在一圈交接回来前过期，重置为 1 ⇒ 又是无界（旧实现的 30s
    # 布尔标记即如此：60s > 30s）。取 7 天与其余计数键同款（自清理）。
    AFFINITY_HANDOVER_TTL_SECONDS = 7 * 86400

    # 超配额 start 的**重投延迟有界上限**（plaita#28，与 AFFINITY_HANDOVER_MAX
    # 同一教训：无界重投 = 自锁）。重投副本落 stream 尾部，一轮 ≈ 当前队列
    # 排空时间；达上限后带 quota_bypass 放行（存活性 > 软配额）。
    # env PLAITA_TENANT_QUOTA_DEFER_MAX 可调（≥1）。
    TENANT_QUOTA_DEFER_MAX = 64

    def _node_retry_total_key(self, execution_id: str) -> str:
        """兜底总重试计数键：``{ns}:execution:noderefetch:{id}``。

        刻意**不叫** ``noderetry:*``：那个前缀是「按节点预算」的语义空间
        （console 的 ``_is_mechanism_key`` 按子串排除，混进去会让运维误读
        为节点预算键）；独立前缀也让「预算」与「兜底」在 redis 里一眼可分。
        """
        return f"{tenant_namespace(current_tenant())}:execution:noderefetch:{execution_id}"

    def _record_node_retry_total(self, execution_id: str) -> int:
        """自增兜底总重试计数（INCR + 7d TTL），返回自增后的值。"""
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "incr"):
            return 1
        try:
            key = self._node_retry_total_key(execution_id)
            pipe = redis_client.pipeline(transaction=True)
            pipe.incr(key)
            pipe.expire(key, self.NODE_RETRY_TOTAL_TTL_SECONDS)
            return int(pipe.execute()[0])
        except Exception as e:  # noqa: BLE001 — 计数失败按 1（不阻断现状语义）
            logger.warning("兜底总重试计数自增失败（按 1）: %s: %s", execution_id, _scrub_url_text(e))
            return 1

    def _reset_node_retry_total(self, execution_id: str) -> None:
        """清零兜底总重试计数（DEL；键不存在为幂等 no-op）。

        与 ``_reset_node_retry_counter`` / ``_reset_deterministic_failure_counter``
        同一个收口：**任一节点成功推进**即清零。计数语义是「连续失败」——
        长寿命执行跨节点的偶发可重试失败不应终身累计到
        ``NODE_RETRY_TOTAL_MAX``（plaita#242：100 次重试在长跑流程里可达），
        否则累计达限后任何一条超投递消息都会让死信守卫误判执行搁浅。

        G1 retry 唤醒**不**清零（与确定性失败计数同款理由）：唤醒后若仍连续
        撞墙，兜底上限必须还能收敛——唤醒即清会形成「唤醒→清零→再撞→再
        唤醒」的无界循环。
        """
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "delete"):
            return
        try:
            redis_client.delete(self._node_retry_total_key(execution_id))
        except Exception as e:  # noqa: BLE001 — 清零失败无害（下次失败继续累计）
            logger.warning("兜底总重试计数清零失败（忽略）: %s: %s", execution_id, _scrub_url_text(e))

    def _node_retry_total_exhausted(self, execution_id: str) -> bool:
        """该执行是否已达兜底总重试上限。"""
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "get"):
            return False
        try:
            raw = redis_client.get(self._node_retry_total_key(execution_id))
        except Exception as e:  # noqa: BLE001 — 读失败按未达限（保守，不误杀）
            logger.warning("兜底总重试计数读取失败（按未达限）: %s: %s", execution_id, _scrub_url_text(e))
            return False
        if raw is None:
            return False
        try:
            return int(raw) >= self.NODE_RETRY_TOTAL_MAX
        except (TypeError, ValueError):
            return False

    def _deterministic_failure_key(self, execution_id: str) -> str:
        """确定性失败计数键：``{ns}:execution:nofail:{id}``（租户路由同上）。"""
        return f"{tenant_namespace(current_tenant())}:execution:nofail:{execution_id}"

    def _read_deterministic_failure_count(self, execution_id: str) -> int:
        """读确定性失败计数；无 redis / 键缺失 / 异常 → 0（放行，保守）。"""
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "get"):
            return 0
        try:
            raw = redis_client.get(self._deterministic_failure_key(execution_id))
            return int(raw) if raw is not None else 0
        except Exception as e:  # noqa: BLE001 — 读失败按 0（不误判不可救）
            logger.warning("确定性失败计数读取失败（按 0）: %s: %s", execution_id, _scrub_url_text(e))
            return 0

    def _record_deterministic_failure(self, execution_id: str) -> int:
        """自增确定性失败计数（INCR + 7d TTL），返回自增后的值。"""
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "incr"):
            return 1
        try:
            key = self._deterministic_failure_key(execution_id)
            pipe = redis_client.pipeline(transaction=True)
            pipe.incr(key)
            pipe.expire(key, self.DETERMINISTIC_FAILURE_TTL_SECONDS)
            return int(pipe.execute()[0])
        except Exception as e:  # noqa: BLE001 — 计数失败按 1（不阻断现状语义）
            logger.warning("确定性失败计数自增失败（按 1）: %s: %s", execution_id, _scrub_url_text(e))
            return 1

    def _deterministic_failure_exhausted(self, execution_id: str) -> bool:
        """该执行是否已判「确定性失败不可救」（连续失败达上限）。"""
        return (self._read_deterministic_failure_count(execution_id)
                >= self.DETERMINISTIC_FAILURE_MAX)

    def _reset_deterministic_failure_counter(self, execution_id: str) -> None:
        """清零确定性失败计数（DEL；键不存在为幂等 no-op）。

        调用时机只有一处：**任一节点成功推进**（与
        ``_reset_node_retry_counter`` 同一个收口）——计数语义是「连续失败」，
        推进过就说明不是「同一位置反复撞墙」，长寿命执行的跨节点偶发失败不应
        累计判死。G1 retry 唤醒时**不**清零（否则「唤醒→清零→再撞→再唤醒」
        永不收敛）。
        """
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "delete"):
            return
        try:
            redis_client.delete(self._deterministic_failure_key(execution_id))
        except Exception as e:  # noqa: BLE001 — 清零失败无害（下次失败继续累计）
            logger.warning("确定性失败计数清零失败（忽略）: %s: %s", execution_id, _scrub_url_text(e))

    def _g1_wakeup_key(self, execution_id: str) -> str:
        """G1 唤醒计数键：``{ns}:execution:g1wakeups:{id}``（租户路由同上）。"""
        return f"{tenant_namespace(current_tenant())}:execution:g1wakeups:{execution_id}"

    def _read_g1_wakeup_count(self, execution_id: str) -> int:
        """读 G1 唤醒计数；无 redis / 键不存在 / 异常 → 0（放行，保守）。"""
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "get"):
            return 0
        try:
            raw = redis_client.get(self._g1_wakeup_key(execution_id))
            return int(raw) if raw is not None else 0
        except Exception as e:  # noqa: BLE001 — 读失败按 0（不阻断人工救援）
            logger.warning("G1 唤醒计数读取失败（按 0 放行）: %s: %s", execution_id, _scrub_url_text(e))
            return 0

    def _g1_wakeup_budget_exhausted(self, execution_id: str) -> bool:
        """唤醒是否已达上限——达限即拒绝（执行保持 error 终态）。"""
        return self._read_g1_wakeup_count(execution_id) >= self.G1_MAX_WAKEUPS

    def _record_g1_wakeup(self, execution_id: str) -> None:
        """记一次 G1 唤醒（INCR + 7d TTL）。"""
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "incr"):
            return
        try:
            key = self._g1_wakeup_key(execution_id)
            pipe = redis_client.pipeline(transaction=True)
            pipe.incr(key)
            pipe.expire(key, self.G1_WAKEUP_COUNTER_TTL_SECONDS)
            pipe.execute()
        except Exception as e:  # noqa: BLE001 — 计数失败不阻断唤醒（人工救援优先）
            logger.warning("G1 唤醒计数失败（忽略）: %s: %s", execution_id, _scrub_url_text(e))

    def _commit_g1_wakeup(self, execution_id: str) -> None:
        """原子收口 G1 唤醒的「清零节点预算 + 记一次唤醒」（第七十一轮任务①）。

        此前两步散调（``_reset_node_retry_counter`` → ``_record_g1_wakeup``）
        非原子且失败各被吞：清计数失败时记数照常执行 ⇒ **g1 配额被消耗但
        执行没拿到新预算**——下一次失败直接终态化，每次半生效唤醒净烧一次
        唤醒额度。现收敛为单段 Lua（SCAN 清 noderetry 全族 + INCR/EXPIRE
        g1wakeups），整体生效或整体不生效：

        - 脚本失败 ⇒ ERROR 留痕 + 抛 :class:`G1WakeupAccountingError`
          （StatePersistError 子族：消息不 ack 重投，绝不终态化）——配额守恒，
          唤醒方可安全重来；
        - 落 running（``_persist_state_or_raise``）刻意**不**并入脚本：它走
          fenced 世代 CAS + timeline/usage 收口，先行失败时两个计数键分毫
          未动，本就守恒；
        - 无 redis 客户端 → no-op（与原两步同）；不支持 eval 的受限客户端
          → 降级为旧两步序列，WARNING 留痕（可观测降级纪律）。
        """
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "incr"):
            return
        if not hasattr(redis_client, "eval"):
            logger.warning(
                "执行 %s G1 唤醒记账降级：redis 客户端不支持 eval，"
                "退回非原子两步序列（清预算失败仍可能烧配额）",
                execution_id,
            )
            self._reset_node_retry_counter(execution_id)
            self._record_g1_wakeup(execution_id)
            return
        try:
            redis_client.eval(
                _G1_WAKEUP_COMMIT_LUA,
                2,
                self._retry_counter_key(execution_id),
                self._g1_wakeup_key(execution_id),
                self.G1_WAKEUP_COUNTER_TTL_SECONDS,
            )
        except Exception as e:  # noqa: BLE001 — 整体未生效，具名上抛走重投
            logger.error(
                "执行 %s G1 唤醒记账失败（清预算+记配额原子脚本未生效，"
                "配额守恒，消息将重投）: %s",
                execution_id, _scrub_url_text(e),
            )
            raise G1WakeupAccountingError(
                f"G1 唤醒记账原子收口失败: execution_id={execution_id}, "
                f"{_scrub_url_text(e)}"
            ) from e

    def _node_retry_budget(self) -> int:
        """节点重试预算：沿用队列 max_deliveries 语义（默认 5）。

        RedisFlowWorker 以其队列配置为准；基类（内存 worker / 单测）回退
        DEFAULT_MAX_DELIVERIES。
        """
        configured = getattr(self, "_max_deliveries", None)
        try:
            return max(1, int(configured)) if configured else DEFAULT_MAX_DELIVERIES
        except (TypeError, ValueError):
            return DEFAULT_MAX_DELIVERIES

    def _retry_counter_key(self, execution_id: str, node_id: Optional[str] = None) -> str:
        """节点重试计数键：``{ns}:execution:noderetry:{id}:{node}``（租户路由同租约键）。

        **按节点维度隔离**（plaita#123 真因，2026-10-10）：键的粒度必须与它
        声称的语义一致——「当前节点的**连续**失败次数」。此前键只含
        execution_id（执行维度），而成功推进时按注释「任一节点成功推进即清零」
        清掉整执行的键 ⇒ 在「前序节点稳定成功 + 某节点稳定失败」的 flow 上
        每轮重投都被前序节点的成功清零，计数永远到不了预算 ⇒ 永不终态化 ⇒
        无限重投（实测 2026-10-10 13:0x 本机 Redis db1：keeper-watch 累积
        667 个悬停 running 执行；**该数字随时间增长，勿当常量引用**）。

        ``node_id`` 为 None/空（无法定位失败节点时）退回**执行维度**旧键形态
        ——保守但安全：语义退化为「本执行累计已放行重试次数」，绝不会比修复
        前更差（修复前就是这个行为）。

        **退化契约（显式声明）**：取不到 node_id 时退回执行维度键
        ``noderetry:{id}``（无节点后缀）。触发条件：异常链中没有
        ``NodeExecutionError``、或它的 ``node`` 既非 id 字符串也无 ``id``/
        ``name``（见 ``_node_id_from_chain``）。此时：
          * 预算判定仍在**该退化键**上计数，不会静默变成「永不耗尽」；
          * 但如果该执行**同时**存在按节点的键，两者是**互相独立**的计数，
            退化键不参与节点键的预算（可能多给该节点若干次重试）；
          * 宁可多给预算也不误杀——「取不到 id」本身是观测缺陷，不应升级成
            把可恢复的执行提前终态化。

        **存量旧键影响声明**：旧扁平键（``noderetry:{id}``，无节点维度）**不被
        新逻辑读取**——新节点键名字不同（带 ``:{node}``），故在途执行升级后
        其旧键归零、相当于**白送 5 次重试预算**。这是刻意的取舍：不迁移旧键
        （靠 7 天 TTL 自然过期）、不读旧键（避免旧语义污染新语义），代价是
        升级瞬间在途执行多拿一轮预算，收益是语义干净、零迁移风险。
        """
        base = f"{tenant_namespace(current_tenant())}:execution:noderetry:{execution_id}"
        return f"{base}:{node_id}" if node_id else base

    def _read_node_retry_counter(
        self, execution_id: str, node_id: Optional[str] = None
    ) -> int:
        """读重试计数；无 redis 客户端 / 键不存在 / 读取异常 → 0（按未重试过）。"""
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "get"):
            return 0
        try:
            raw = redis_client.get(self._retry_counter_key(execution_id, node_id))
        except Exception as e:  # noqa: BLE001 — 瞬断按 0 处理（保守放行死信重入队）
            logger.warning("读取节点重试计数失败（按 0 处理）: %s: %s", execution_id, _scrub_url_text(e))
            return 0
        if raw is None:
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def _max_node_retry_counter(self, execution_id: str) -> int:
        """该执行下**所有节点**重试计数的最大值（plaita#123 起键按节点隔离）。

        死信守卫用（见 ``_dead_letter_guard``）：它按「消息在某执行上被反复
        重投」判死信，而预算键现在按节点分片——任一节点达预算即说明该执行
        已经搁浅，取 max 与守卫的原始意图一致。读不到任何键 → 0。
        """
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "get"):
            return 0
        base = self._retry_counter_key(execution_id)
        keys = [base]
        scan = getattr(redis_client, "scan_iter", None)
        if callable(scan):
            try:
                keys.extend(list(scan(match=f"{base}:*")))
            except Exception as e:  # noqa: BLE001 — 扫描失败退化为单键读
                logger.warning("扫描节点重试计数键失败（按单键读）: %s: %s", execution_id, _scrub_url_text(e))
        best = 0
        for key in keys:
            try:
                raw = redis_client.get(key)
            except Exception as e:  # noqa: BLE001 — 瞬断按 0 处理（保守放行死信重入队）
                logger.warning("读取节点重试计数失败（按 0 处理）: %s: %s", execution_id, _scrub_url_text(e))
                continue
            if raw is None:
                continue
            try:
                best = max(best, int(raw))
            except (TypeError, ValueError):
                continue
        return best

    def _record_node_retry(self, execution_id: str, node_id: Optional[str] = None) -> int:
        """重试放行前自增计数键（INCR + 滑动 7 天 EX），返回自增后的值。

        预算判定**不**用消息 delivery_count：核实发现队列在 reclaim 路径
        上报的是 XCLAIM **前**的 times_delivered（task_queue.py:402，与该处
        注释 "count + 1" 相悖）、fresh 读取恒报 1，且达限消息在队列层就地
        死信、根本不会进 handler（task_queue.py:404）——按 delivery_count
        判预算永远不会触发，只会造成「死信守卫重入队 → delivery 归 1 →
        再耗尽 → 再重入队」的无限循环。计数键在 worker 侧自增，语义是
        「**该节点**累计已放行的重试次数」（键含 node_id，见
        ``_retry_counter_key``），预算耗尽即终态化。

        无 redis 客户端（内存 worker / 单测）→ 返回 1 不设上限：预算由
        调用方/重投机制兜底（直连调用没有 run() 循环，异常直接冒给调用方）。
        """
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "incr"):
            return 1
        key = self._retry_counter_key(execution_id, node_id)
        try:
            pipe = redis_client.pipeline(transaction=True)
            pipe.incr(key)
            pipe.expire(key, self.NODE_RETRY_COUNTER_TTL_SECONDS)
            return int(pipe.execute()[0])
        except Exception as e:  # noqa: BLE001 — 计数失败按 1 处理，不阻断重试放行
            logger.warning("节点重试计数自增失败（按 1 处理）: %s: %s", execution_id, _scrub_url_text(e))
            return 1

    def _reset_node_retry_counter(
        self, execution_id: str, node_id: Optional[str] = None
    ) -> None:
        """清零节点重试计数键（DEL；键不存在为幂等 no-op）。

        两个调用时机（rebase 组合语义，2026-10 二波 vs G1 43828aa）：

        - G1 retry 唤醒放行时（``node_id=None``）：预算耗尽终态化的执行经人工
          retry 唤醒后拿全新预算——否则唤醒的执行第一次失败就立刻再耗尽，
          G1 形同虚设。唤醒是**执行级**意图 ⇒ 清**全部**节点计数（``node_id``
          缺省键 + 该执行下所有 ``{id}:*`` 子键，见 ``_delete_node_retry_keys``）；
        - 节点成功推进后（**必须传 node_id**）：计数语义是「当前节点的**连续**
          失败次数」，不同节点的失败不共享预算——节点 A 抖一次花掉的预算不应
          让之后节点 B 的第一次失败就少一次重试机会。**只清该节点**的计数，
          不得波及他节点（plaita#123：此前清整个执行 ⇒ 前序节点每轮成功都把
          失败节点的计数抹平 ⇒ 预算永不耗尽 ⇒ 无限重投）。
        """
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "delete"):
            return
        try:
            self._delete_node_retry_keys(redis_client, execution_id, node_id)
        except Exception as e:  # noqa: BLE001 — 清零失败无害（下次失败继续累计）
            logger.warning("节点重试计数清零失败（忽略）: %s: %s", execution_id, _scrub_url_text(e))

    def _delete_node_retry_keys(
        self, redis_client: Any, execution_id: str, node_id: Optional[str]
    ) -> None:
        """删除计数键：给 node_id → 精确 DEL 该节点的键；不给 → DEL 该执行的**全部**
        节点计数键（缺省执行维度键 + ``{id}:*`` 子键，供 G1 唤醒清预算）。

        G1 唤醒的「清全部」用 SCAN 而非 ``KEYS``：worker 与生产执行共用实例，
        阻塞式 ``KEYS`` 会拖停整个 Redis。SCAN 不可用（受限代理/桩客户端）时
        退化为「逐个已知形态 + 缺省键」的精确删除，宁可少清也不阻塞。
        """
        base = self._retry_counter_key(execution_id)
        if node_id:
            redis_client.delete(self._retry_counter_key(execution_id, node_id))
            return
        targets = [base]
        scan = getattr(redis_client, "scan_iter", None)
        if callable(scan):
            targets.extend(list(scan(match=f"{base}:*")))
        redis_client.delete(*targets)

    def _node_failure_retry_decision(
        self,
        exc: Exception,
        execution_id: str,
        delivery_count: Optional[int],
        node_id: Optional[str] = None,
    ) -> Optional[NodeExecutionRetryableError]:
        """节点失败重试决策：返回 NodeExecutionRetryableError（调用方 raise，
        状态不终态化）或 None（按现状终态化 error）。

        - 回滚开关 / 判据不符 → None（现状）；
        - 重试计数达预算 → None（现状终态化；error 里带重试次数供观测）；
        - 其余 → 计数自增后返回重试异常。

        ``node_id`` = 失败节点 id（plaita#123）：计数键按节点隔离的前提。
        调用方拿不到时传 None，计数退化为执行维度（保守，不比修复前差）。
        传入时也用于日志，让运维一眼看出「哪个节点在反复失败」。
        """
        if _node_retry_disabled():
            return None
        if not isinstance(execution_id, str) or not execution_id:
            return None
        if not _is_retryable_node_failure(exc):
            # 确定性失败（`exited 1` / `AgsError: sync_in` / 超时 / 协议错）：
            # 不重试，但仍要**记账**（plaita#73 遗留层，2026-10-10 实证）。
            #
            # 此前直接 return None ⇒ 这些失败**完全不进任何计数器**（实测全库
            # `noderetry` 键 74/74 恒为 1（2026-10-10 本机 db1 快照）），
            # 于是它们唯一的收敛机制只剩「消息层
            # 重投」，而消息层没有次数概念 ⇒ 同一执行可被重投数百次、沙箱实例
            # 持续占位（实测 5 小时烧 5.66 实例小时、产出 0）。
            #
            # 这里用**独立**计数键（`nofail`）而不是 noderetry：noderetry 的语义
            # 是「可重试预算」（会被 G1 唤醒/PROGRESS 清零），塞进确定性失败会污染
            # 该语义。确定性失败**只增不减**，达上限即判该执行不可救、触发终态化 +
            # 后续重投一律短路（见 `_deterministic_failure_exhausted`）。
            n = self._record_deterministic_failure(execution_id)
            if n >= self.DETERMINISTIC_FAILURE_MAX:
                logger.error(
                    "执行 %s 确定性失败累计达上限（%s/%s）——判定不可救，终态化 error"
                    "（后续重投将短路，不再空转）: %s",
                    execution_id, n, self.DETERMINISTIC_FAILURE_MAX, _scrub_url_text(exc),
                )
            else:
                logger.warning(
                    "执行 %s 确定性失败（不重试）第 %s/%s 次: %s",
                    execution_id, n, self.DETERMINISTIC_FAILURE_MAX, _scrub_url_text(exc),
                )
            return None
        attempt = self._record_node_retry(execution_id, node_id)
        if attempt >= self._node_retry_budget():
            logger.error(
                "执行 %s 节点 %s 重试预算耗尽（%s/%s），终态化 error: %s",
                execution_id, node_id or "?", attempt, self._node_retry_budget(), _scrub_url_text(exc),
            )
            return None
        # 兜底上限（plaita#123 评审建议）：**不看键名、只看次数**。节点维度
        # 预算依赖「键里的节点段稳定」，任何键空间漂移（节点 id 动态变化、
        # 定义被改、未来又把 execution_id 引回键）都会让单节点预算失效而
        # 退化成无界重投。该计数只按 execution_id 累计，达上限即终态化——
        # 保证「预算机制整体失效」时仍有界。放在节点预算判定**之后**：
        # 正常执行永远碰不到，只在异常形态下兜住。
        total = self._record_node_retry_total(execution_id)
        if total >= self.NODE_RETRY_TOTAL_MAX:
            logger.error(
                "执行 %s 节点重试达**兜底总上限**（%s/%s，节点 %s）——判定键空间"
                "异常/预算机制失效，终态化 error 防止无界重投: %s",
                execution_id, total, self.NODE_RETRY_TOTAL_MAX, node_id or "?", _scrub_url_text(exc),
            )
            return None
        logger.warning(
            "执行 %s 节点 %s 执行失败，不终态化等待显式重投后重跑失败节点"
            "（第 %s/%s 次重试，重投退避 %.0fs，plaita#91）: %s",
            execution_id, node_id or "?", attempt, self._node_retry_budget(),
            _node_retry_backoff_seconds(attempt), _scrub_url_text(exc),
        )
        return NodeExecutionRetryableError(
            f"节点执行失败（可重试，第 {attempt}/{self._node_retry_budget()} 次重试，"
            f"execution_id={execution_id}）: {exc}",
            execution_id=execution_id,
            attempt=attempt,
            delivery_count=delivery_count,
        )

    # ---- 取消检查点（波次①，设计稿 §3.1 选型 B：独立取消标志键）----

    # 取消标志键 TTL：7 天自清理（设计稿 §3.1；复用 event_filter 去重键的
    # 同类「带 TTL 意图键」模式）。BFF 侧写入（executions.py._cancel_key）。
    CANCEL_FLAG_TTL_SECONDS = 7 * 86400

    def _cancel_flag_key(self, execution_id: str) -> str:
        """取消标志键：``{ns}:execution:cancel:{id}``（租户路由，与 _exec_key 同规则）。"""
        return f"{tenant_namespace(current_tenant())}:execution:cancel:{execution_id}"

    def _cancel_requested(self, execution_id: str) -> bool:
        """步间取消检查：控制面写的意图标志键是否存在。

        - 无 redis 客户端（内存 worker / 单测派生类）或查询异常 → 容错降级
          视为未取消（设计稿 §3.5 兼容红线）；
        - ``PLAITA_DISABLE_CANCEL_CHECKPOINT=1``（波次①回滚开关）→ 跳过检查。
        """
        if _cancel_checkpoint_disabled():
            return False
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "exists"):
            return False
        try:
            return bool(redis_client.exists(self._cancel_flag_key(execution_id)))
        except Exception as e:
            logger.warning(
                "取消标志检查失败（视为未取消，下个步界重试）: %s: %s", execution_id, _scrub_url_text(e)
            )
            return False

    # ---- 取消监听（波次③：步内可中断）----
    #
    # 现状缺口：取消标志键只在**步界**（start/resume 入口、步循环顶）被检查，
    # 一个节点一旦跑起来就打断不了——agentrun 默认 timeout_secs=1800，取消后
    # 最多白等 30 分钟。本监听线程在**节点执行期间**轮询同一标志键，命中即
    # ``execution.cancel()``（同时置位 cancel_event/cancel_requested）：
    #   - code 沙箱等待循环（code._popen_wait_cancellable）当场 killpg 进程树；
    #   - 引擎在**下一个节点入口**抛 FlowCancelledException 拒绝继续；
    #   - 若节点自身协作（agentrun 等）消费 cancel_event，则连在途进程一起中止。
    # 语义与 §3.3 一致：默认软中断，协作节点即时击杀。
    #
    # 基类持有登记表/停止位与轮询逻辑（start/resume/步循环都在基类）；线程
    # 启动由 RedisFlowWorker.run() 触发——无 redis 客户端的内存 worker 登记
    # 为空转（``_cancel_requested`` 恒 False），永不误中。

    def _register_cancel_watch(self, execution_id: str, execution: Any) -> None:
        """登记活跃执行：取消监听线程据此轮询标志键并中止在途节点。"""
        with self._cancel_watch_lock:
            self._cancel_watch[execution_id] = (execution, current_tenant())

    def _unregister_cancel_watch(self, execution_id: str) -> None:
        with self._cancel_watch_lock:
            self._cancel_watch.pop(execution_id, None)

    def _cancel_poll_interval(self) -> float:
        if self._cancel_poll_seconds is not None:
            return max(0.05, float(self._cancel_poll_seconds))
        return 1.0

    def _start_cancel_watcher(self) -> None:
        """启动取消监听线程（RedisFlowWorker.run() 调用）。

        ``PLAITA_DISABLE_CANCEL_INTERRUPT=1``（波次③回滚开关）不启动——回到
        「只在步界检查取消标志键」的现状语义。无 redis 客户端（内存 worker）
        不启动（无标志键可读）。
        """
        if _cancel_interrupt_disabled():
            logger.info("取消监听已禁用（PLAITA_DISABLE_CANCEL_INTERRUPT=1）")
            return
        if getattr(self, "redis_client", None) is None:
            return
        if self._cancel_thread is not None and self._cancel_thread.is_alive():
            return
        self._cancel_stop.clear()
        self._cancel_thread = threading.Thread(
            target=self._cancel_watch_loop,
            name="plaita-cancel-watcher",
            daemon=True,
        )
        self._cancel_thread.start()

    def _stop_cancel_watcher(self) -> None:
        self._cancel_stop.set()
        watcher = self._cancel_thread
        if (
            watcher is not None
            and watcher is not threading.current_thread()
            and watcher.is_alive()
        ):
            watcher.join(timeout=2.0)
        self._cancel_thread = None

    def _cancel_watch_loop(self) -> None:
        interval = self._cancel_poll_interval()
        logger.info("取消监听已启动（轮询间隔 %.2fs）", interval)
        # 首次立即轮询一次，再按间隔等待——缩短「进入长跑节点」到「被中止」
        # 的最坏延迟（最长 ≈ interval + 一次 Redis 往返）。
        self._cancel_poll_safely()
        while not self._cancel_stop.wait(interval):
            self._cancel_poll_safely()

    def _cancel_poll_safely(self) -> None:
        try:
            self._cancel_poll_once()
        except Exception as exc:  # noqa: BLE001 — 监听线程自身绝不能带崩 worker
            logger.error("取消监听周期异常: %s", _scrub_exception_text(exc))

    def _cancel_poll_once(self) -> None:
        """对全部活跃执行轮询一次取消标志键，命中即中止在途执行。"""
        with self._cancel_watch_lock:
            entries = list(self._cancel_watch.items())
        for execution_id, (execution, tenant_id) in entries:
            token = set_current_tenant(tenant_id)
            try:
                requested = self._cancel_requested(execution_id)
            finally:
                reset_current_tenant(token)
            if not requested:
                continue
            # 命中：撤登记（幂等——重复轮询不再对同一执行重复 cancel），
            # 调 execution.cancel() 置位双 Event，在途协作节点当场收手。
            self._unregister_cancel_watch(execution_id)
            cancel = getattr(execution, "cancel", None)
            if callable(cancel):
                try:
                    cancel()
                except Exception as exc:  # noqa: BLE001 — cancel 失败仍有步界兜底
                    logger.warning(
                        "取消监听调 execution.cancel() 失败: %s: %s",
                        execution_id, _scrub_exception_text(exc),
                    )
            logger.info("取消监听命中标志键，已请求中止在途节点: %s", execution_id)

    # ---- 租约看门狗挂钩（波次②；基类 no-op，RedisFlowWorker 覆写）----

    def _acquire_lease(self, execution_id: str, holder: str) -> Tuple[Optional[str], Optional[int]]:
        """取得执行租约，返回 ``(lease_value, fence_token)``。

        fencing 开启且 lease 实现支持时走世代号 acquire（execution_lease.
        try_acquire_fenced）：租约值变为 ``{holder}:{gen}``，renew/release 与
        fenced storage CAS 均以完整 value 串比较。回滚开关
        ``PLAITA_DISABLE_FENCING=1`` 或 lease 为 NullExecutionLease（内存/
        单测）时退回 SET NX，fence_token=None（设计稿 §4.2 / §6 波次②回滚）。
        lease_value 为 None 表示 acquire 失败（已被他人持有）。
        """
        if not _fencing_disabled():
            fenced = getattr(self.execution_lease, "try_acquire_fenced", None)
            if callable(fenced):
                generation = fenced(execution_id, holder, self.lease_ttl_seconds)
                if generation is not None:
                    return f"{holder}:{generation}", generation
                return None, None
        acquired = self.execution_lease.try_acquire(
            execution_id, holder, self.lease_ttl_seconds
        )
        return (holder if acquired else None), None

    def _register_lease_watch(
        self,
        execution_id: str,
        lease_value: str,
        execution: Any,
        fence_token: Optional[int] = None,
    ) -> None:
        """登记活跃执行供看门狗续租（基类 no-op）。

        ``fence_token`` 与租户一起供心跳（``_publish_node_progress``）使用：
        看门狗线程没有本执行的 ContextVar，不带就会写错 namespace 并绕开
        世代 CAS（见 RedisFlowWorker 的同名覆写）。
        """

    def _unregister_lease_watch(self, execution_id: str, lease_value: str) -> None:
        """注销看门狗登记（基类 no-op）。"""

    def _execution_still_advancing(self, execution: Any) -> bool:
        """该执行对象是否仍在推进（基类默认：保守判「是」）。

        基类无租约机制，无法也不该判定活性；保守返回 True 只影响
        「是否把执行转入 orphan 观测」，而基类的 `_register_lease_orphan`
        是 no-op，故行为与改动前等价（不会误放走健康 run）。
        """
        return True

    def _register_lease_orphan(
        self,
        execution_id: str,
        execution: Any,
        tenant_id: Optional[str] = None,
        fence_token: Optional[int] = None,
    ) -> None:
        """转入失租后观测集合（基类 no-op：无租约机制即无此需要）。"""

    def _raise_if_lease_lost(self, execution_id: Optional[str]) -> None:
        """看门狗已标记失租则抛 ExecutionLeaseError（基类 no-op）。"""

    def _renew_lease_if_held(self, lease_execution_id: Optional[str], lease_holder: Optional[str]) -> None:
        if not lease_execution_id or not lease_holder:
            return
        # 看门狗已判死（含注入式 renew 失败）：步界立即自爆，不再续期
        self._raise_if_lease_lost(lease_execution_id)
        if not self.execution_lease.renew(lease_execution_id, lease_holder, self.lease_ttl_seconds):
            raise ExecutionLeaseError(
                f"lost lease for execution {lease_execution_id}; aborting resume"
            )

    def _state_write_lock(self, execution_id: str) -> "threading.RLock":
        """取本执行的**状态写互斥锁**（同 id 恒返回同一把；RLock 可重入）。

        为什么需要：同一 execution_id 有两个写者——推进线程的
        ``_persist_state_or_raise``（终态/步进/挂起/错误态）与看门狗线程的
        ``_publish_node_progress``（长节点期间的心跳）。心跳是「load → 合并
        node_timings → save」，**整行写回**：若它读到的是终态化**之前**的行，
        然后把终态写覆盖掉，就得到一个「status=running + 旧 checkpoint + 新
        last_update_time」的假活僵尸——既不会被 keeper 回收（open node + 新鲜
        last_update_time 恰是它的「活着」判据），后续 resume 还会从旧 checkpoint
        重放节点副作用（消息已被 ack，没有写者会再回来修）。两处都持本锁即把
        「读-改-写」与终态写串行化。

        实现要点：``WeakValueDictionary`` —— 只有正在写/正在排队的线程持有强引用，
        写完即自动回收，长跑 worker 不随执行数泄漏；取锁/建锁在 guard 锁内完成，
        保证两个线程拿到的是**同一个**锁对象（否则互斥不成立）。__new__ 构造的
        骨架 worker 没有 ``__init__`` 里的登记表，这里惰性补齐。
        """
        locks = self.__dict__.get("_state_write_locks")
        guard = self.__dict__.get("_state_write_locks_guard")
        if locks is None or guard is None:
            with _STATE_WRITE_LOCKS_INIT_GUARD:
                locks = self.__dict__.setdefault(
                    "_state_write_locks", weakref.WeakValueDictionary()
                )
                guard = self.__dict__.setdefault("_state_write_locks_guard", threading.Lock())
        with guard:
            lock = locks.get(execution_id)
            if lock is None:
                lock = threading.RLock()
                locks[execution_id] = lock
            return lock

    def _persist_state_or_raise(
        self,
        execution_id: str,
        state: ExecutionState,
        phase: str,
        *,
        attempt_end: Optional[str] = None,
    ) -> None:
        """落盘执行状态并检查返回值：False → 抛 ``StatePersistError``。

        **#80 起契约已分家（本条注释此前是错的，必须同步）**：``save_execution_state``
        **不再**吞掉一切异常返回 False——
          * **Redis 瞬断** ⇒ 仍返回 False ⇒ 本方法抛 ``StatePersistError``
            （消息不 ack、走 at-least-once 重投，这是瞬态该走的路）；
          * **状态不可序列化** ⇒ 存储层**抛** ``StateSerializationError``
            （永久失败，重投不会让它变好）⇒ 它**绕过**本方法的 False 分支直接上抛，
            由调用点的 ``except StateSerializationError`` 转成**可观测终态**。
        留着旧措辞就是把「声明与实现脱钩」（#140 同族）埋回代码里。

        历史：start 路径历史上检查过返回值，但
        终态/挂起/取消/步进/错误态保存与 resume 两条路径此前不检查——
        is_end 落盘失败 → break → 正常返回 → 消息被 ack，节点副作用已发生
        而执行状态永远停在 running/suspended（僵尸执行）。统一在此收口：
        失败即抛，消息不 ack，走 at-least-once 重投。

        注意：fenced 世代失配是 storage 层 raise ``ExecutionLeaseError``、
        不经本方法的 False 路径——既有失租链路不受影响。

        ``attempt_end``（plaita#25 P2）：**非终态**路径上的「本次投递尝试收口」
        状态（当前仅 start 失败路径传 ``"error"``）——落盘收口据此在时间线里
        补一条 ``run_end(final=False)``，而**不改状态行**（status/context/error
        原样）。终态路径（completed/error/cancelled）忽略该参数，照旧由
        ``_collect_execution_timeline`` 收口成 ``run_end(final=True)``。

        全程持**每执行互斥锁**（``_state_write_lock``）：与本执行的看门狗心跳
        （``_publish_node_progress``）串行，否则心跳的「读-改-写」会把这里的
        终态/步进写整行回滚（2026-10-10 评审复现）。
        """
        with self._state_write_lock(execution_id):
            self._collect_node_timings(execution_id, state)
            self._collect_execution_timeline(execution_id, state, attempt_end=attempt_end)
            self._collect_usage(execution_id, state)
            # 沙箱回收挂在同一收口（覆盖 start/步进/挂起/终态所有路径）：
            # 非终态是 no-op，终态按上下文快照释放实例
            self._release_sandboxes(execution_id, state)
            saved = self.execution_storage.save_execution_state(execution_id, state)
        if not saved:
            logger.error(
                "保存执行状态失败 (%s): execution_id=%s, status=%s——消息将不 ack 等待重投",
                phase,
                execution_id,
                getattr(state, "status", "?"),
            )
            raise StatePersistError(
                f"保存执行状态失败 ({phase}): execution_id={execution_id}, "
                f"status={getattr(state, 'status', '?')}"
            )
        # start 准入名额释放（plaita#28）：终态（completed/error/cancelled）
        # 即释放——**单一收口**，覆盖 start/resume 两条路径上的全部终态写
        # （完成/取消各分支/错误态/交接耗尽终态化）。挂起不释放：执行仍在
        # 被 admitted 的生命周期内，resume 复用名额。按 ``state.tenant_id``
        # 路由（而非 ContextVar）——看门狗等无租户上下文线程同样放对账。
        # 释放是幂等 ZREM、尽力而为：失败由名额条目 TTL 兜底自愈。
        self._release_quota_slot_if_terminal(execution_id, state)

    def _release_quota_slot_if_terminal(self, execution_id: str, state: ExecutionState) -> None:
        """终态执行释放 start 准入名额（plaita#28）；非终态 no-op。"""
        if getattr(state, "status", None) not in TERMINAL_EXECUTION_STATUSES:
            return
        release_tenant_start(
            getattr(self, "redis_client", None),
            getattr(state, "tenant_id", None),
            execution_id,
        )

    def _terminalize_definition_missing(
        self,
        execution_id: str,
        flow_id: str,
        version: Optional[str],
        state: Optional[ExecutionState],
        phase: str,
    ) -> None:
        """定义缺失的执行终态化 error（#304）：可观测终态 + 配额名额释放。

        调用方（start/resume 入口）捕获 ``FlowDefinitionMissingError`` 后、
        原样上抛前调用。幂等：``state`` 缺省时现读盘；行不存在或已终态
        （活 worker 的推进写 / 前次终态化已落）一律不覆写。写盘走
        ``_persist_state_or_raise``——终态经 ``_release_quota_slot_if_terminal``
        单一收口释放 start 准入名额，queued 幽灵行与名额泄漏一并收口。

        终态化用 error 与 flow_hash mismatch 同口径：版本恢复/重发后仍可
        ``resume_type=retry`` 从断点唤醒，不做不可逆处置。
        """
        if state is None:
            try:
                state = self.execution_storage.load_execution_state(execution_id)
            except Exception as exc:  # noqa: BLE001 — 读盘瞬断交还重投路径
                logger.warning(
                    "定义缺失终态化前读盘失败（交还重投，不终态化）: "
                    "execution=%s: %s", execution_id, _scrub_exception_text(exc),
                )
                # #310：必须以非 ValueError 瞬态上抛——裸 return 会让调用方
                # 原样 raise FlowDefinitionMissingError（⊂ValueError），run()
                # 按 ValueError poison ack：消息被吃、重投从未发生，「交还
                # 重投」注释成谎，执行行永驻非终态（start 名额等 24h TTL
                # 兜底）。抛 StatePersistError（RuntimeError 族）让消息留
                # pending 等 at-least-once 重投；重投后读盘恢复且定义仍缺失
                # → 走上方正常终态化。
                raise StatePersistError(
                    f"定义缺失终态化前读盘失败（交还重投）: "
                    f"execution={execution_id}"
                ) from exc
            if state is None:
                # 无状态行（未走 console 占位、就地铸造 id 的 start）：没有
                # 可观测对象，poison ack 即终局，无幽灵行可言。
                logger.warning(
                    "定义缺失且无状态行，跳过终态化: execution=%s flow=%s@%s",
                    execution_id, flow_id, version or "latest",
                )
                return
        if getattr(state, "status", None) in TERMINAL_EXECUTION_STATUSES:
            return
        message = (
            f"流程定义缺失，执行无法推进（flow_id={flow_id}, "
            f"version={version or 'latest'}）。版本可能已被删除或未同步到引擎存储。"
        )
        logger.error(
            "执行 %s 的 flow 定义缺失，终态化 error: %s", execution_id, message
        )
        state.status = "error"
        state.error = {
            "message": message,
            # 机器可判身份（同 flow_hash mismatch 的落盘纪律）：code 用引擎
            # 通用 flow 级错误码，error_type 是本判据的 worker 级身份。
            "code": _FLOW_ERROR_CODE,
            "error_type": "flow_definition_missing",
            "flow_id": flow_id,
            "flow_version": version,
            "hint": (
                "确认版本存在且已发布（或重新 publish/重发定义）后，可 "
                "resume_type=retry 从断点唤醒本执行"
            ),
        }
        state.end_time = datetime.now().isoformat()
        self._persist_state_or_raise(execution_id, state, phase)

    def _handlers_for(self, execution_id: str, enqueued_at: Optional[str] = None) -> list:
        """本次执行要挂的回调列表：用户/观测回调 + 节点耗时采集器 + 事件时间线
        （+ 沙箱生命周期回收，条件装配）。

        采集器**按执行**新建并登记，避免同一 worker 并发处理多个执行时耗时串场；
        登记表在终态落盘后回收（见 ``_collect_node_timings`` /
        ``_collect_execution_timeline``）。同一执行重复调用本方法**复用**已登记
        的采集器（分布式步进会多次构造 handlers，复用语义与 ``_sandbox_lifecycle_for``
        同因：新建会丢掉先前采到的事件）。

        ``enqueued_at``（plaita#25）：console 入队时写进消息体的 ``timestamp``，
        用于回答「排队多久」。老消息/本地档没有该键 ⇒ 时间线里
        ``queue_wait_ms=None``（未采集），绝不折算成 0。

        沙箱回收（2026-10-07）：存在沙箱注册表时挂 `SandboxLifecycleCallback`
        —**flow 结束/挂起/异常三条路径都精确释放实例**，不再干等 AGS 侧 timeout
        兜底（AGS 无闲置自动处置，"总寿命"比实际需要长得多）。plaita-nodes 为
        可选依赖，缺装/无注册表时静默跳过（非沙箱部署零行为变化）。
        """
        timing = NodeTimingCallback()
        self._node_timings[execution_id] = timing
        timeline = self._timelines.get(execution_id)
        if timeline is None:
            timeline = ExecutionTimelineCallback(
                enqueued_at=enqueued_at,
                publisher=self._timeline_publisher(execution_id),
            )
            self._timelines[execution_id] = timeline
        handlers = [*self.callback_handlers, timing, timeline]
        sandbox_cb = self._sandbox_lifecycle_for(execution_id)
        if sandbox_cb is not None:
            handlers.append(sandbox_cb)
        return handlers

    def _timeline_publisher(self, execution_id: str):
        """把事件实时发布到 SSE 频道的回调（best-effort，绝不反压执行）。"""

        def _publish(event: Dict[str, Any]) -> None:
            self._publish_execution_event(execution_id, event)

        return _publish

    def _publish_execution_event(self, execution_id: str, event: Dict[str, Any]) -> None:
        """向 ``plaita:execution:events:{id}`` 发布一条执行事件（plaita#25）。

        这是该频道的**唯一发布者**（console 侧只有订阅与删除）。三条纪律：

        - **best-effort**：Redis 抖动/无客户端/开关关闭都只返回，绝不影响执行
          结果（观测面不得把执行面带崩）；
        - **可关**：``PLAITA_EXECUTION_EVENTS_PUBLISH=0`` 静默；内存档
          （无 ``redis_client``）天然 no-op；
        - **载荷自描述**：事件里带 ``execution_id``，订阅方无需解析频道名。
        """
        if not publish_enabled():
            return
        client = getattr(self, "redis_client", None)
        if client is None:
            return
        try:
            client.publish(f"{CHANNEL_PREFIX}{execution_id}", event_payload(execution_id, event))
        except Exception as exc:  # noqa: BLE001 — 发布失败绝不影响执行
            logger.warning(
                "执行事件发布失败（忽略，不影响执行）: execution_id=%s: %s",
                execution_id,
                _scrub_exception_text(exc),
            )

    def _sandbox_lifecycle_for(self, execution_id: str):
        """按执行取沙箱生命周期回调（缓存复用）。

        ⚠️ 分布式步进下**每步都会新建 handlers 列表**——若每步都造一个新回调，
        它只在当步收集快照，到 flow 结束那一步早已空空如也，终态释放静默失效
        （2026-10-08 实测：plaita#22 跑完 24 分钟后沙箱仍 running、日志零条
        ``sandbox lifecycle``）。因此同一执行必须复用同一实例。
        """
        cb = self._sandbox_callbacks.get(execution_id)
        if cb is None:
            cb = self._sandbox_lifecycle_handler()
            if cb is not None:
                self._sandbox_callbacks[execution_id] = cb
        return cb

    def _release_sandboxes(self, execution_id: str, state: ExecutionState) -> None:
        """终态落盘时释放本执行用过的沙箱实例（跨 step/跨进程都成立）。

        快照来源=**持久化上下文**的 ``$NODE.*.workspace``（
        ``collect_workspace_snapshots``）——进程内累积在分布式路径不可靠（见
        ``_sandbox_lifecycle_for``）。成功（completed）→ kill 不留现场；失败/取消
        → pause 保现场（可恢复）。best-effort：任何异常只告警，绝不拖累落盘。
        """
        if getattr(state, "status", "") not in TERMINAL_EXECUTION_STATUSES:
            return
        cb = self._sandbox_lifecycle_for(execution_id)
        self._sandbox_callbacks.pop(execution_id, None)
        if cb is None:
            return
        try:
            from plaita_nodes.sandbox import collect_workspace_snapshots
        except Exception:  # noqa: BLE001 — 可选依赖
            return
        try:
            snaps = collect_workspace_snapshots(getattr(state, "context", None) or {})
        except Exception as exc:  # noqa: BLE001
            logger.warning("沙箱快照收集失败（忽略）: %s", _scrub_url_text(exc))
            return
        if not snaps:
            return
        keep = getattr(state, "status", "") != "completed"   # 失败/取消→保现场
        try:
            results = cb.drain(snaps, phase="terminal", keep_data=keep)
            if results:
                logger.info("终态沙箱释放 %s: %s", execution_id, results)
        except Exception as exc:  # noqa: BLE001 — 回收失败由 AGS TTL 兜底
            logger.warning("终态沙箱释放失败（忽略）: %s", _scrub_url_text(exc))

    @staticmethod
    def _sandbox_lifecycle_handler():
        """惰性装配沙箱生命周期回调；未装 plaita-nodes 或无沙箱注册表 → None。"""
        if os.environ.get("PLAITA_SANDBOX_LIFECYCLE", "1") == "0":
            return None
        try:
            from plaita_nodes.lifecycle import SandboxLifecycleCallback
        except Exception:  # noqa: BLE001 — 可选依赖/导入差异都跳过
            return None
        try:
            from plaita_nodes import sandbox as _sb
            specs = _sb.load_sandboxes()
        except Exception:  # noqa: BLE001 — 无注册表=非沙箱部署
            return None
        if not specs:
            return None
        try:
            return SandboxLifecycleCallback(sandboxes=specs)
        except Exception:  # noqa: BLE001 — 装配失败绝不拖累执行
            return None

    def _collect_node_timings(self, execution_id: str, state: ExecutionState) -> None:
        """把本次执行的节点耗时并进状态（在**唯一的落盘收口**上做，覆盖 start/步进/
        挂起/终态所有路径）。

        - 与状态里已有的 ``node_timings`` **合并**而不是覆盖：resume 可能发生在
          另一个进程，旧节点的时间不能被新进程的采集器抹掉；
        - 终态落盘后回收采集器，避免长跑 worker 泄漏。
        """
        timing = self._node_timings.get(execution_id)
        if timing is None:
            return
        merged = dict(state.node_timings or {})
        merged.update(timing.snapshot())
        state.node_timings = merged
        if getattr(state, "status", "") in TERMINAL_EXECUTION_STATUSES:
            self._node_timings.pop(execution_id, None)

    def _collect_execution_timeline(
        self,
        execution_id: str,
        state: ExecutionState,
        *,
        attempt_end: Optional[str] = None,
    ) -> None:
        """把本次执行的 run 级事件时间线并进状态（与 ``_collect_node_timings``
        同一落盘收口、同一「合并而非覆盖」语义）。

        - 终态时先补一条 ``run_end(final=True)``（含 status）再快照——「这次跑完
          没有、以什么状态收的」是时间线的收口事件；
        - **非终态**的失败尝试（``attempt_end`` 非 None，plaita#25 P2）补一条
          ``run_end(final=False)``：状态行保持非终态（既有契约：可重投/可
          cancel），但这次尝试必须留下收口锚点，否则失败 run 的落盘时间线停在
          半途（失败节点有、收口没有）；
        - 合并语义（``merge_timeline``）：resume 跨进程时旧事件与旧排队时长
          必须保留，新进程的 None **不得**把已有答案擦掉；
        - 终态落盘后回收采集器，避免长跑 worker 泄漏（与 node_timings 同款）；
          非终态不回收——同一执行的后续尝试还要继续往同一条流里追加。
        """
        timeline = self._timelines.get(execution_id)
        if timeline is None:
            return
        terminal = getattr(state, "status", "") in TERMINAL_EXECUTION_STATUSES
        if terminal:
            timeline.finish_run(getattr(state, "status", None))
        elif attempt_end is not None:
            timeline.finish_attempt(attempt_end)
        drained = timeline.drain()
        if not drained["events"] and not drained["dropped"] and state.timeline is None:
            # 还没有任何事件（resume 进程在首个节点前）：保持 None = 未采集，
            # 不要落一个「采集到了但是空」的假象。
            return
        state.timeline = merge_timeline(state.timeline, drained)
        if terminal:
            self._timelines.pop(execution_id, None)

    def _collect_usage(self, execution_id: str, state: ExecutionState) -> None:
        """终态时把每节点 ``usage`` 聚合进 ``ExecutionState.usage``（plaita#37）。

        四条纪律：

        - **只在终态**聚合：半截/非最终数字被读成「本次成本」比没有数字更坏；
        - **不覆写**：``state.usage`` 已有值（resume / 历史 patch_count 回填）
          原样保留——与 ``node_timings``/``flow_hash`` 同款零回归语义；
        - **best-effort**：聚合失败只打 WARNING（响亮、不静默），**绝不影响
          执行结果**——落盘口径不得把执行面拖下水；
        - **未采集 ≠ 0**：没有任何节点报 usage ⇒ 保持 ``None``；节点明确报了
          ``total_tokens=0`` ⇒ 落一个含 0 的 usage。
        """
        if getattr(state, "usage", None) is not None:
            return
        if getattr(state, "status", "") not in TERMINAL_EXECUTION_STATUSES:
            return
        try:
            usage = aggregate_node_usage(node_results_from_context(state.context))
        except Exception as exc:  # noqa: BLE001 — 观测/口径失败绝不影响执行结果
            logger.warning(
                "usage 聚合失败（忽略，不影响执行结果）: execution_id=%s: %s",
                execution_id,
                _scrub_exception_text(exc),
            )
            return
        if usage is None:
            return
        try:
            state.usage = usage
        except Exception as exc:  # noqa: BLE001 — 赋值/序列化前失败同样不得外抛
            logger.warning(
                "usage 落盘失败（忽略，不影响执行结果）: execution_id=%s: %s",
                execution_id,
                _scrub_exception_text(exc),
            )
    
    def get_flow_definition(self, flow_id: str, version: Optional[str] = None) -> Flow:
        """
        获取流程定义，支持按ID和版本获取
        
        Args:
            flow_id: 流程ID
            version: 流程版本，如果不指定则获取最新版本
            
        Returns:
            Dict[str, Any]: 流程定义
            
        Raises:
            ValueError: 如果找不到流程定义或版本不匹配
        """
        # 生成缓存键（含租户段：同名流程可共存于不同租户 namespace）
        cache_key = f"{current_tenant()}:{flow_id}:{version or 'latest'}"
        
        # 尝试从缓存获取
        if cache_key in self.flow_definition_cache:
            logger.info("从缓存获取流程定义: %s", cache_key)
            return self.flow_definition_cache[cache_key]
        
        # 缓存未命中，从存储获取
        logger.info("从存储获取流程定义: %s, 版本: %s", flow_id, version or 'latest')
        flow_definition = self.flow_storage.get_flow(flow_id, version)
        
        if not flow_definition:
            # Delegate diagnostic details to the storage layer via an optional
            # diagnose() method — FlowWorker must not peek at storage internals
            # (e.g. Redis keys / namespaces) directly.
            if hasattr(self.flow_storage, "diagnose_missing_flow"):
                try:
                    self.flow_storage.diagnose_missing_flow(flow_id)
                except Exception as exc:
                    logger.warning("flow_storage.diagnose_missing_flow(%s) failed: %s", flow_id, _scrub_exception_text(exc))

            error_msg = f"找不到流程定义: {flow_id}"
            logger.error(error_msg)
            # #304：具名子类（仍继承 ValueError）——start/resume 入口据此
            # 把受影响执行终态化 error，而不是裸 poison ack 留下永驻
            # queued/suspended 行。
            raise FlowDefinitionMissingError(error_msg)

        # 解析流程定义
        try:
            logger.info("成功获取流程定义: %s, 数据: %s", flow_id, flow_definition.get('name', 'unknown'))
            flow = Flow.model_validate(flow_definition)
        except Exception as e:
            error_msg = f"解析流程定义失败: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        self.flow_definition_cache[cache_key] = flow
        # 指纹随定义一起缓存（plaita#36）：指纹只取 model_validate **之前**的
        # 原始定义，与缓存里的 Flow 同窗口，避免 start/resume 各自重算走叉。
        self._flow_definition_hash_cache()[cache_key] = compute_flow_definition_hash(
            flow_definition
        )
        
        return flow

    def _flow_definition_hash_cache(self) -> "TTLCache":
        """指纹缓存（惰性兜底：``__new__`` 构造的骨架 worker 不经 ``__init__``）。"""
        cache = getattr(self, "flow_definition_hash_cache", None)
        if cache is None:
            cache = TTLCache(maxsize=100, ttl=300)
            self.flow_definition_hash_cache = cache
        return cache

    def get_flow_definition_hash(self, flow_id: str, version: Optional[str] = None) -> str:
        """当前存储定义的指纹（plaita#36：原始定义 JSON 的 sha256）。

        与 ``get_flow_definition`` 共用缓存窗口：定义加载时就算好（同一次
        ``flow_storage.get_flow``），调用方（start/resume）不自己从 ``Flow``
        对象反推——那正是版本漂移的入口。缓存未命中（定义缓存被清 / 加载器被
        测试替身替换）时回源到**同一个 FlowStorage** 直读原始定义，绝不回退
        到「解析后 dump」口径。
        """
        cache_key = f"{current_tenant()}:{flow_id}:{version or 'latest'}"
        cache = self._flow_definition_hash_cache()
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        flow_definition = self.flow_storage.get_flow(flow_id, version)
        if not flow_definition:
            raise ValueError(f"找不到流程定义: {flow_id}")
        digest = compute_flow_definition_hash(flow_definition)
        cache[cache_key] = digest
        return digest

    # ---- flow 定义指纹（波次二任务②）----

    def _compute_flow_hash(self, flow_definition: Mapping[str, Any]) -> str:
        """对**存储中的原始流程定义**计算稳定指纹（plaita#36 口径 v2）。

        入参必须是 ``model_validate`` **之前**的原始定义（dict/Mapping）：只有
        它跨 pydantic/plaita 版本稳定。传 ``Flow`` 对象（或任何非 Mapping）一律
        ``TypeError`` —— 对解析后的 ``model_dump`` 求指纹会让「引擎升级」被误判
        成「定义变更」，滚动升级窗口把在途 run 批量终态化（#36 的根因）。
        """
        if not isinstance(flow_definition, Mapping):
            raise TypeError(
                "_compute_flow_hash 只接受存储中的原始流程定义（Mapping）；"
                f"收到 {type(flow_definition).__name__}——对解析后的模型 dump "
                "求指纹会随模型字段演进漂移（plaita#36），禁止回退到该口径。"
            )
        return compute_flow_definition_hash(flow_definition)

    # ---- start 幂等键（波次二任务③）----

    # start 去重映射键 TTL：7 天自清理（与取消标志/重试计数键同款模式）。
    # plaita#274：单源在 task_queue.START_DEDUP_TTL_SECONDS（console 生产侧
    # 预认领共用同一 TTL），类属性保留为兼容别名。
    START_DEDUP_TTL_SECONDS = START_DEDUP_TTL_SECONDS

    def _load_execution_tombstone(
        self, execution_id: str, tenant_id: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """经 storage API 读取并匹配执行墓碑（A51-01）。"""
        return _load_matching_tombstone(
            getattr(self, "execution_storage", None), execution_id, tenant_id
        )

    def _carrier_tombstoned(self, body: Dict[str, Any]) -> bool:
        """start/resume 载体是否指向已删除执行（同租户 + 墓碑命中）。

        A51-01 原先只拦 ``start``；A2R25-04 起 ``resume`` 同样在派发闸被拦
        ——备份恢复把状态键带回来不会撤销墓碑，「删除即不可复活」在 resume
        载体上必须与 start 同口径。
        """
        execution_id = body.get("execution_id")
        if not isinstance(execution_id, str) or not execution_id:
            return False
        tombstone = self._load_execution_tombstone(
            execution_id, body.get("tenant_id")
        )
        if tombstone is None:
            return False
        logger.warning(
            "执行 %s 已删除（墓碑 tenant=%s reason=%s deleted_at=%s），"
            "丢弃 %s 载体（不重建/不推进）: tenant=%s",
            execution_id,
            tombstone.get("tenant"),
            tombstone.get("reason"),
            tombstone.get("deleted_at"),
            body.get("type"),
            body.get("tenant_id") or DEFAULT_TENANT_ID,
        )
        return True

    def _start_carrier_too_old(
        self, body: Dict[str, Any], stream_message_id: Optional[str] = None
    ) -> bool:
        """消息最大年龄闸（B52-02/A69-04）：墓碑 TTL 到期后的历史载体兜底丢弃。

        年龄取**双源最大值**：payload ``timestamp``（ISO）与 Stream ID 的
        毫秒前缀。两者都有时取大——未来时间戳不能把老 Stream ID 洗白，重投
        后 Stream ID 变新也不能让 payload 保留的旧年龄被忽略。

        无法判定年龄（两源都缺失/畸形）→ **fail-closed** 丢弃并计数
        （``carrier_age_unknown_total``）：放行等于把「删后压队熬过墓碑
        horizon」的载体重新放回执行面。``max_age <= 0`` 仍表示关闭年龄闸，
        但启动时会由 ``validate_tombstone_retention`` WARNING。
        """
        max_age = _task_max_age_seconds()
        if max_age <= 0:
            return False
        ages = [
            age
            for age in (
                _message_age_seconds(body.get("timestamp")),
                _message_age_from_stream_id(stream_message_id),
            )
            if age is not None
        ]
        if not ages:
            self._carrier_age_unknown_total = (
                getattr(self, "_carrier_age_unknown_total", 0) + 1
            )
            logger.warning(
                "载体 %s 年龄无法判定（timestamp=%r stream_id=%r execution=%s "
                "type=%s），fail-closed 丢弃（A69-04）",
                stream_message_id or body.get("message_id"),
                body.get("timestamp"),
                stream_message_id,
                body.get("execution_id"),
                body.get("type"),
            )
            return True
        age = max(ages)
        if age < max_age:
            return False
        logger.warning(
            "消息 %s 年龄 %.0fs 达到/超过最大年龄 %ss（execution=%s type=%s，"
            "payload_age=%s stream_age=%s），丢弃（墓碑/状态 TTL 到期后的历史"
            "载体不重建执行）",
            stream_message_id or body.get("message_id") or body.get("timestamp"),
            age,
            max_age,
            body.get("execution_id"),
            body.get("type"),
            _message_age_seconds(body.get("timestamp")),
            _message_age_from_stream_id(stream_message_id),
        )
        return True

    def _start_dedup_key(self, dedup_key: str) -> str:
        """start 去重映射键：``{ns}:start-dedup:{key}``（租户路由，与租约键同规则）。

        键形状单源在 ``task_queue.start_dedup_map_key``（#274：console 生产侧
        的入队前预认领必须与消费侧认领命中**同一把键**，否则「同键必同 id」
        契约在某一侧漂移即失效）。
        """
        return start_dedup_map_key(tenant_namespace(current_tenant()), dedup_key)

    def _claim_start_dedup(self, dedup_key: Optional[str], execution_id: str) -> Tuple[str, Optional[str]]:
        """原子认领 start 幂等键（SET NX + EX 7 天）。

        返回 ``(action, mapped_execution_id)``：

        - ``("claimed", None)``：认领成功，本消息是这条逻辑 start 的首次执行
          （含 #274 自命中：映射已指向本载体 id——console 生产侧预认领或同
          消息先前投递的认领，本次投递按归属者继续）；
        - ``("hit", mapped)``：键已存在（重投/双开），mapped 为首次执行映射的
          execution_id，调用方**绝不二次 start**；mapped 为 None 表示键刚好
          过期/被清（罕见竞态），调用方按孤儿映射处理；
        - ``("inactive", None)``：未提供 dedup_key、无 redis 客户端或认领
          瞬断——按未启用处理，行为与存量完全一致。

        映射在 ``run_distributed`` **之前**认领（execution_id 在
        ``FlowExecution``/``ExecutionContext`` 构造时即生成，core/context.py:220），
        首节点副作用受保护；映射值指向的执行状态此后由 ``start_flow`` 正常落盘。
        """
        redis_client = getattr(self, "redis_client", None)
        if not dedup_key or redis_client is None or not hasattr(redis_client, "set"):
            return ("inactive", None)
        key = self._start_dedup_key(dedup_key)
        try:
            claimed = redis_client.set(
                key, execution_id, ex=self.START_DEDUP_TTL_SECONDS, nx=True
            )
        except Exception as e:  # noqa: BLE001 — 认领瞬断：不阻塞启动（退化为存量行为）
            logger.warning("start 幂等键认领失败（按未启用处理）: %s: %s", key, _scrub_url_text(e))
            return ("inactive", None)
        if claimed:
            return ("claimed", None)
        try:
            raw = redis_client.get(key)
        except Exception as e:  # noqa: BLE001 — 读取瞬断同上
            logger.warning("start 幂等键读取失败（按孤儿映射处理）: %s: %s", key, _scrub_url_text(e))
            return ("hit", None)
        mapped = raw.decode() if isinstance(raw, bytes) else raw
        if mapped == execution_id:
            # plaita#274 自命中：映射已指向**本载体** id——console 在 POST 时
            # 以同键预认领（生产侧「同键必同 id」），或同一消息先前投递留下的
            # 认领（at-least-once 重投）。本次投递就是这条逻辑 start 的归属者，
            # 按认领成功继续；绝不走 hit 分支把唯一执行哑掉（映射行是 queued
            # 占位/初始快照时，hit 的现状回报会让 start 不被消费、执行永不
            # 推进）。重投安全由 preminted 载体转换（#234：非空 checkpoint 转
            # resume）与 start 入口取消检查兜底。
            return ("claimed", None)
        return ("hit", mapped or None)

    def _release_start_dedup(self, dedup_key: str, execution_id: str) -> None:
        """删除孤儿映射（映射指向的执行状态从未落盘）。GET==id 才 DEL，
        防误删并发的后来者认领；比较与删除间的微小竞窗最多导致重复启动，
        与存量 at-least-once 语义一致。"""
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "get"):
            return
        key = self._start_dedup_key(dedup_key)
        try:
            raw = redis_client.get(key)
            current = raw.decode() if isinstance(raw, bytes) else raw
            if current == execution_id:
                redis_client.delete(key)
        except Exception as e:  # noqa: BLE001 — 释放失败无害（键 7 天自动过期）
            logger.warning("start 幂等键孤儿释放失败（忽略）: %s: %s", key, _scrub_url_text(e))

    def _handle_start_dedup_hit(
        self,
        flow_id: str,
        mapped_execution_id: Optional[str],
        params: Optional[Dict[str, Any]] = None,
        *,
        schema_version: Optional[int] = None,
        carrier_execution_id: Optional[str] = None,
        dedup_key: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """处理幂等键命中：绝不二次 start。

        ``schema_version`` = 触发本次命中的 start 消息信封版本（A44-02）。重入队
        的 resume 副本继承它；``None``（未从队列派发/老调用方）按当前 build
        写版本落信封。

        - 映射缺失/指向的执行状态不存在（孤儿：认领后首次执行从未落盘，
          如 crash 在 claim 与 save 之间）→ 返回 None，调用方释放映射后按
          新启动继续（首节点可能重跑——首节点须幂等是既有文档约定）；
        - 已终态 → 返回 already 形状（镜像 resume 终态短路）；
        - 非终态 running（start 消息处理中 worker 崩溃/任务①重试搁浅）→
          **重入队一份 resume 消息**把执行接续起来再 ack start（若只返回
          现状形状，执行将无人推进永久卡 running）；入队失败抛
          ServiceDispatchError 让消息重投再来；
        - 非终态 suspended → 只返回现状形状不重入队：挂起执行自有
          delay/approval 服务的 resume 链路，重入队 continue 会被
          ``_handle_resume`` 的 pending 校验拒绝（ResumeError）；#33 起
          resume_flow 对 suspended+continue 直接幂等短路，重入队更是无谓。

        ``params``（调度/手动 start 消息的入参，#122）：命中发生在「首次
        处理未完成」的窗口——盘上 running 行仍是 ``start_flow`` 刚落的初始
        快照（``context={}``，首节点还没成功过）。此时 resume 走
        ``saved_context={}`` 的**假值分支**，会以 ``params or {}`` 重新
        ``setup_flow``：重入队的 resume 若不带上原 start 的 params，
        ``$INPUT`` 就被绑成 ``{}``——调度 params（如
        ``watch_agent``）静默丢失，且此后该执行整条生命周期都拿不到它。
        故重入队的 resume 消息携带 params（非空 dict 才加该键，空的与
        存量消息形状一致）；``resume_flow`` 把它透传给引擎，只在
        checkpoint 为空时生效（非空 checkpoint 走真值分支，params 不参与
        ——续跑语义零变化）。
        """
        if not mapped_execution_id:
            return None
        state = self.execution_storage.load_execution_state(mapped_execution_id)
        if state is None:
            # A51-01/A52-01 scenario 6：映射指向的执行已被删除。必须区分
            # 「同一 id 重放」（拒绝并释放映射）与「新 id 复用旧 dedup_key」
            # （释放映射后按新 id 正常启动）——用载体 id 是否等于映射 id 判定。
            tombstone = self._load_execution_tombstone(
                mapped_execution_id, current_tenant()
            )
            if tombstone is not None:
                if carrier_execution_id == mapped_execution_id:
                    logger.warning(
                        "start 幂等键命中已删除执行 %s（墓碑），拒绝同 id 重放；"
                        "释放映射供新启动复用: dedup_key 命中",
                        mapped_execution_id,
                    )
                    return {
                        "execution_id": mapped_execution_id,
                        "status": "deleted",
                        "discarded": True,
                        "release_dedup": True,
                    }
                logger.warning(
                    "start 幂等键命中已删除执行 %s（墓碑），但载体是新 id %s"
                    "（复用旧 dedup_key）→ 释放孤儿映射后按新 id 启动",
                    mapped_execution_id, carrier_execution_id,
                )
                return None
            logger.warning(
                "start 幂等键命中但映射的执行无状态（孤儿映射）: %s",
                mapped_execution_id,
            )
            return None
        status = getattr(state, "status", "") or ""
        if status in ("completed", "error", "cancelled"):
            # #289 消费点最后防线：载体命中的映射执行已终态——本载体具名
            # 放弃（消费循环随后 ack），绝不因 mark 失败后的重试再次执行。
            # B108-02：独立指标 + 执行详情可见的审计记录（不只一条 WARNING）。
            self._dedup_discarded_total = (
                getattr(self, "_dedup_discarded_total", 0) + 1
            )
            audit_record = {
                "execution_id": mapped_execution_id,
                "carrier_execution_id": carrier_execution_id,
                "dedup_key": dedup_key,
                "status": status,
                "reason": "already_terminal",
                "discarded_at": datetime.now().isoformat(),
            }
            redis_client = getattr(self, "redis_client", None)
            if redis_client is not None:
                try:
                    redis_client.set(
                        f"plaita:execution:dedup-discard:{mapped_execution_id}",
                        json.dumps(audit_record, ensure_ascii=False),
                        ex=self.START_DEDUP_TTL_SECONDS,
                    )
                except Exception as exc:  # noqa: BLE001 — 审计旁路，仍保留 WARNING
                    logger.warning(
                        "start 载体放弃审计写入失败 execution=%s: %s",
                        mapped_execution_id, _scrub_url_text(exc),
                    )
            logger.warning(
                "start 载体 %s 命中已终态执行 %s (%s)：具名放弃执行并 ack"
                "（dedup_key=%s）",
                carrier_execution_id or "?", mapped_execution_id, status,
                dedup_key or "?",
            )
            return {
                "execution_id": mapped_execution_id,
                "status": status,
                "already_terminal": True,
                "deduplicated": True,
                "discarded": True,
                "result": getattr(state, "result", None),
                "error": getattr(state, "error", None),
            }
        if status == "running":
            # 崩溃/重试搁浅的 start：重入队 resume 接续执行。若原 start 持有者
            # 仍在推进，重入队的 resume 会被执行租约拦下（ExecutionLeaseError，
            # run() 对该分支 ack 释放）；持有者已死则租约过期后由重入队的
            # resume 从 checkpoint 接管——两条路都不产生并发推进（#23）。
            redis_client = getattr(self, "redis_client", None)
            queue_name = getattr(self, "queue_name", None)
            if redis_client is not None and queue_name:
                resume_msg = {
                    "type": "resume",
                    "flow_id": flow_id,
                    "execution_id": mapped_execution_id,
                    "resume_type": "continue",
                    "tenant_id": current_tenant(),
                    "timestamp": datetime.now().isoformat(),
                }
                # #122：把原 start 的 params 带到 resume 载体上。命中窗口里盘上
                # running 行的 context 还是 ``start_flow`` 落的 ``{}``（首节点
                # 未成功），resume 会走「无 checkpoint → setup_flow(params or
                # {})」分支；不带 params 就等于把调度入参清成 ``{}``。空 dict
                # 不加键——存量消息形状与测试断言零变化。
                if isinstance(params, dict) and params:
                    resume_msg["params"] = dict(params)
                try:
                    # A44-02：resume 副本继承触发命中的 start 消息版本，混跑池
                    # 里的旧实例仍能接续；不知道源版本时才写当前 build 版本。
                    enqueue_kwargs = (
                        {} if schema_version is None
                        else {"schema_version": schema_version}
                    )
                    source_generation = current_tenant_generation()
                    if source_generation:
                        enqueue_kwargs["tenant_generation"] = source_generation
                    enqueue_task(redis_client, queue_name, resume_msg, **enqueue_kwargs)
                except Exception as e:  # noqa: BLE001 — 入队失败让 start 消息重投
                    raise ServiceDispatchError(
                        f"start 幂等命中后重入队 resume 失败 "
                        f"(execution_id={mapped_execution_id}): {e}"
                    ) from e
                logger.info(
                    "start 幂等键命中：执行 %s 仍在 running，已重入队 resume 接续",
                    mapped_execution_id,
                )
                return {
                    "execution_id": mapped_execution_id,
                    "status": status,
                    "deduplicated": True,
                    "resume_requeued": True,
                }
            logger.warning(
                "start 幂等键命中：执行 %s 仍在 running，但无队列可重入队 resume"
                "（内存 worker），执行可能停滞", mapped_execution_id,
            )
        # suspended：只回报现状（绝不二次 start）；但若落盘留着未清的派发
        # 凭据（plaita#233 的「落盘→派发」窗口），这条 start 重投是仅存的
        # 载体——在返回前补派一次（不重跑挂起节点，不重入队 continue）。
        if status == "suspended":
            compensated = self._compensate_suspend_dispatch(
                state, mapped_execution_id, source="start_dedup_hit"
            )
            result = {
                "execution_id": mapped_execution_id,
                "status": status,
                "deduplicated": True,
            }
            if compensated:
                result["dispatch_compensated"] = True
            return result
        # 无队列的 running：只回报现状（绝不二次 start）
        return {
            "execution_id": mapped_execution_id,
            "status": status,
            "deduplicated": True,
        }

    def _existing_start_checkpoint(
        self, flow_id: str, execution_id: str
    ) -> Optional[ExecutionState]:
        """start 重投时的查盘判定（plaita#234）：返回非 None = 应转 resume 语义。

        判据与 #91 ``_requeue_body_for_retry`` 一致，但覆盖**所有** start 重投
        来源（XCLAIM 回收 / 死信重入队 / 显式重投），而不只是显式重投那条：

        - 盘上无行 → None（首投，行为逐字不变）；
        - ``flow_id`` 不一致 → 抛 ``ExecutionIdConflictError``：X 属于另一条
          flow，转 resume 会被流程 ID 校验 poison ack，保持原样 start 则会
          用 ``context={}`` 覆写 A 的 checkpoint。两种转换都错，只能拒绝
          （台账第三十一轮 V1）；
        - 终态（completed/error/cancelled）→ 返回状态：resume_flow 终态短路
          幂等回报现状，**不重跑**（重放会把已终态行覆写回 running）；
        - suspended → 返回状态：#33 幂等短路 + #233 挂起补派，**不重跑挂起
          节点**；
        - running 且 checkpoint 非空 → 返回状态：从断点续跑（``saved_context``
          由 resume_flow 携带）；
        - running 且 checkpoint 为空 → None：start→resume 转换的空 checkpoint
          窗口（首节点尚未成功过），现有行为不变。

        读盘失败**原样上抛**（``ExecutionStateLoadError`` 非 ValueError，
        run() 走不 ack 重投）：判定不了有没有断点时，重投一次的成本远低于
        把已有副作用整条重放。
        """
        try:
            state = self.execution_storage.load_execution_state(execution_id)
        except Exception as exc:  # noqa: BLE001 — 查盘失败不改写语义，只上抛重投
            logger.error(
                "start 载体查盘失败（无法判定是否已有断点，消息将不 ack 重投）: "
                "execution_id=%s: %s", execution_id, _scrub_exception_text(exc),
            )
            raise
        if state is None:
            return None
        stored_flow_id = getattr(state, "flow_id", None)
        if stored_flow_id and stored_flow_id != flow_id:
            logger.error(
                "start 拒绝跨 flow 覆写 execution_id（execution_id=%s 盘上属于 "
                "flow_id=%r，本次 start flow_id=%r）：保留原状态，消息按畸形"
                "载体 poison ack（台账第三十一轮 V1）",
                execution_id, stored_flow_id, flow_id,
            )
            raise ExecutionIdConflictError(
                f"execution_id {execution_id} already belongs to flow "
                f"{stored_flow_id!r}; refuse to start flow {flow_id!r} with "
                "the same execution_id"
            )
        status = (getattr(state, "status", "") or "").strip()
        if status in TERMINAL_EXECUTION_STATUSES:
            return state
        if status == "suspended":
            return state
        if state.context:
            return state
        return None

    def _finalize_observers(self) -> None:
        """run 终结（completed/failed）时收尾观测回调。

        distributed 模式下内核不发 on_flow_end（is_end 由宿主循环判定）。
        观测回调实现 finalize()（如 LangfuseCallback：收口根 span + flush）
        则终态调用之，仅有 flush() 的退化为直接 flush。
        """
        for handler in self.callback_handlers:
            finalizer = getattr(handler, "finalize", None)
            flusher = getattr(handler, "flush", None)
            try:
                if callable(finalizer):
                    finalizer()
                elif callable(flusher):
                    flusher()
            except Exception as exc:
                logger.warning("观测回调终态收尾失败: %r: %s", handler, _scrub_exception_text(exc))

    def _bind_observers(self, execution) -> None:
        """把执行实例通知给支持 ``bind_execution`` 的观测回调（plaita.obs）。

        LangfuseCallback 等观测回调以此拿到运行时 ``$EXECUTION_ID`` 作跨进程
        trace id；绑定同时重置其 run 状态，防止上一执行的 trace 串入。绑定
        失败只告警，不影响执行。
        """
        for handler in self.callback_handlers:
            binder = getattr(handler, "bind_execution", None)
            if callable(binder):
                try:
                    binder(execution)
                except Exception as exc:
                    logger.warning("观测回调 bind_execution 失败: %r: %s", handler, _scrub_exception_text(exc))

    def start_flow(self, flow_id: str, params: Dict[str, Any], version: Optional[str] = None,
                   execution_id: Optional[str] = None,
                   dedup_key: Optional[str] = None,
                   delivery_count: Optional[int] = None,
                   enqueued_at: Optional[str] = None,
                   schema_version: Optional[int] = None,
                   trigger: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        启动流程执行

        全程持有 ``execution_id`` 执行租约（先行落 running 行之前取得，处理
        结束 finally 释放）：队列按 ``claim_min_idle_ms``（默认 60s）把长任务
        的 pending 消息 XCLAIM 重派给空闲 worker——无租约的 start 在多 worker
        池下跑超过 60s 必被重派双跑（#23）。另一 worker 已持租约（消息重派/
        并发投递）时抛 ``ExecutionLeaseError``，本消息不 ack 留待处理。

        Args:
            flow_id: 流程ID
            params: 流程输入参数
            version: 流程版本，如果不指定则使用最新版本
            execution_id: 预铸 id（BFF start 时铸造、随消息透传）——提交方
                即刻拿 id 返给调用方，无需等 worker 消费；缺省就地铸造
            dedup_key: 可选 start 幂等键（消息重投/双开收敛；未提供则行为
                与存量完全一致，见 ``_claim_start_dedup``）
            delivery_count: 消息投递次数（可观测；见 _process_execution_result）
            enqueued_at: console 入队时间（消息体 ``timestamp``，ISO 串）。用于
                时间线回答「排队多久」（plaita#25）：缺失/不可解析 ⇒
                ``queue_wait_ms=None``（未采集），不折算成 0。
            schema_version: 触发本次 start 的消息信封版本（A44-02）。仅用于
                start 幂等命中后重入队的 resume 副本继承源版本；直接调用方
                缺省 ``None`` = 按当前 build 写版本。
            trigger: 触发归因（A2R44-02，随消息体透传）。调度触发带
                {kind, schedule_id, schedule_name}——接管落行写 invoker=
                "schedule" 并原样随行（贯穿终态写）；None/畸形 = 手动或旧
                消息，invoker 保持 "worker" 存量语义。

        Returns:
            Dict[str, Any]: 流程执行结果
        """
        # 获取流程定义
        try:
            flow = self.get_flow_definition(flow_id, version)
        except FlowDefinitionMissingError:
            # #304：定义缺失（版本被删/未发布/未同步）不再裸 ValueError
            # poison ack——console 预铸 id 的 queued 占位行在此终态化 error
            # （可观测 + start 准入名额经单一收口释放），否则占位行永驻
            # queued、名额等 TTL 兜底。未预铸 id 的调用没有状态行可写，
            # 终态化助手内部跳过，poison ack 即终局。
            if isinstance(execution_id, str) and execution_id.strip():
                self._terminalize_definition_missing(
                    execution_id, flow_id, version, None,
                    phase="flow_definition_missing_start",
                )
            raise

        logger.info("开始执行流程: %s, 版本: %s", flow_id, version or '最新版本')
        
        try:
            # P0 可见性修复（keeper 迁移设计稿 §5.5 清单①，43828aa）：
            # execution_id 优先吃 BFF 预铸（随消息透传，提交方即刻可轮询），
            # 否则就地铸造；行 id 与 result.execution_id 天然一致。异常遗留
            # 的 running 行是 zombie，交 reaper/心跳年龄判定处置。
            # 位置前移到执行器构造之前：节点耗时采集器按执行登记，需要 id 已定。
            # preminted（plaita#234）：只有自带 id 的载体才可能命中盘上已有
            # 断点；就地铸造的 id 天然是首次执行，不必多读一次盘。
            preminted_execution_id = bool(
                isinstance(execution_id, str) and execution_id.strip()
            )
            execution_id = execution_id or uuid.uuid4().hex

            # A51-01：删除墓碑复核（五种纵深之一）。分派闸之外，直达
            # ``start_flow`` 的调用同样不得把已删执行重建；新铸造的 id 天然
            # 不可能有墓碑，只有预铸 id 才需要多读一次。命中即 ack 语义
            # （返回值供调用方确认），并释放同 id 的 dedup 映射。
            if preminted_execution_id:
                tombstone = self._load_execution_tombstone(
                    execution_id, current_tenant()
                )
                if tombstone is not None:
                    if dedup_key:
                        self._release_start_dedup(dedup_key, execution_id)
                    logger.warning(
                        "执行 %s 已删除（墓碑 tenant=%s reason=%s deleted_at=%s），"
                        "start_flow 拒绝重建（flow=%s）",
                        execution_id,
                        tombstone.get("tenant"),
                        tombstone.get("reason"),
                        tombstone.get("deleted_at"),
                        flow_id,
                    )
                    return {
                        "execution_id": execution_id,
                        "status": "deleted",
                        "discarded": True,
                    }

            # 创建流程执行器并执行流程
            # 复用同一个 FlowExecution 贯穿所有分布式步骤, 保留用户回调
            handlers = self._handlers_for(execution_id, enqueued_at=enqueued_at)
            # 时间线开篇（plaita#25）：记 run_start + 由 enqueued_at 算出排队时长。
            # 位置在**落 running 行之前**——排队时长的终点是「worker 开始处理」，
            # 不是「首节点开始跑」，否则排队时长会被流程定义加载/租约获取计入。
            _timeline = self._timelines.get(execution_id)
            if _timeline is not None:
                _timeline.start_run()
            execution = FlowExecution(
                event_bus=self.event_bus,
                callback_handlers=handlers,
            )
            execution.mode = ExecutionMode.DISTRIBUTED
            self._bind_observers(execution)

            # start 幂等键（波次二任务③，与 G1 预铸 id 组合）：在先行落行
            # 之前认领，映射值=预铸或就地铸造的 execution_id——重投/双开
            # 在**任何新行落盘之前**即被拦截，首节点副作用由此受重投保护。
            dedup_action, mapped_execution_id = self._claim_start_dedup(
                dedup_key, execution_id
            )
            if dedup_action == "hit":
                hit_result = self._handle_start_dedup_hit(
                    flow_id, mapped_execution_id, params=params,
                    schema_version=schema_version,
                    carrier_execution_id=execution_id,
                    dedup_key=dedup_key,
                )
                if hit_result is not None:
                    # 本次没有真正启动：回收按本 id 装配的时间线采集器
                    # （否则节流命中会在长跑 worker 里留一条无人认领的采集器）。
                    self._timelines.pop(execution_id, None)
                    if hit_result.get("release_dedup") and dedup_key:
                        self._release_start_dedup(
                            dedup_key, hit_result.get("execution_id") or mapped_execution_id
                        )
                    return hit_result
                # 孤儿映射（首次执行从未落盘）：释放后重新认领，让本次启动
                # 继续受保护；并发后来者抢先认领则放弃设防（存量语义）
                if dedup_key:
                    self._release_start_dedup(dedup_key, mapped_execution_id)
                    dedup_action, mapped_execution_id = self._claim_start_dedup(
                        dedup_key, execution_id
                    )
                    if dedup_action == "hit":
                        logger.warning(
                            "start 幂等键孤儿释放后被并发认领，本次启动不设防: %s",
                            dedup_key,
                        )

            # 载体转换（plaita#234）：start 消息**不是只能从首节点起跑**。
            # 盘上已有该 execution_id 的断点时（worker 崩溃/滚动重启、
            # StatePersistError、ServiceDispatchError 后消息不 ack 被 XCLAIM
            # 原样回收——这条回收路径**不经过** #91 的
            # ``_requeue_body_for_retry``），历史实现会新建 `context={}` 的
            # 行无条件覆写盘上 checkpoint，再以**不带 saved_context** 的
            # ``run_distributed`` 从首节点整条重放：已完成节点的外部副作用
            # 最多重复 max_deliveries 次，且 $LAST_NODE/$NODE/$INPUT 被永久
            # 抹成 {}，此后连正确的 event/continue resume 也失去续跑现场。
            #
            # 修法：把 start 载体**转成 resume 语义**（判定口径与 #91 一致，
            # 但入口收在 start_flow，覆盖「重投不经 _requeue_body_for_retry」
            # 的那条路），复用 resume_flow 已有的全部保护：终态/挂起幂等短路
            # （不重跑）、flow_hash 校验、取消检查、租约，以及 #233 的挂起
            # 补派。此时尚未取得 start 租约（两者共用同一 execution_id 租约
            # 键），转调不会自撞锁。
            if preminted_execution_id:
                existing_state = self._existing_start_checkpoint(flow_id, execution_id)
                if existing_state is not None:
                    logger.warning(
                        "start 载体命中盘上已有断点（execution_id=%s, status=%s, "
                        "checkpoint=%s）：转 resume 语义，绝不整条重放/覆写 checkpoint",
                        execution_id,
                        existing_state.status,
                        "有" if existing_state.context else "空",
                    )
                    # 时间线采集器**保留**：resume 路径的 _handlers_for 会复用
                    # 同一采集器，从而保住本消息的 enqueued_at/run_start（#25）。
                    return self.resume_flow(
                        flow_id,
                        execution_id,
                        "continue",
                        delivery_count=delivery_count,
                        params=params,
                    )

            # 先落 running 行再执行（P0 可见性）：首节点期间 /api/executions
            # 查得到此行、cancel 有锚。flow_hash：对**存储中的原始定义**计算
            # 指纹（plaita#36 口径 v2）随行落盘——resume 时与当前定义比对，防
            # 运行中改定义后续跑走错分支；原始定义口径保证同一份存储定义在
            # 不同引擎版本下算同一个指纹。
            state = ExecutionState(
                execution_id=execution_id,
                flow_id=flow_id,
                flow_version=version,
                flow_hash=self.get_flow_definition_hash(flow_id, version),
                flow_hash_algo=FLOW_HASH_ALGO,
                engine_version=_engine_version(),
                tenant_id=current_tenant(),
                context={},
                status="running",
                start_time=datetime.now().isoformat(),
                # 调度触发归因（A2R44-02）：带 schedule_id 的 trigger ⇒
                # invoker=schedule + trigger 原样随行；手动/旧消息保持 worker。
                invoker=invoker_for_trigger(trigger),
                trigger=trigger if isinstance(trigger, dict) and trigger else None,
            )

            # start 入口取消检查点（与 resume 入口对称）：BFF cancel 可发生在
            # 「已入队、未消费」窗口，更关键的是 start 消息 at-least-once 重投
            # 时（在途失败→未 ack→重投）同 id 重跑整条 flow——不查标志则
            # 已取消意图永远等不到步界（E2E drill 3 实证：超时 flow 空转
            # N×900s）。命中即终态化，不再执行。
            if self._cancel_requested(execution_id):
                state.status = "cancelled"
                state.end_time = datetime.now().isoformat()
                self._persist_state_or_raise(execution_id, state, "cancelled_at_start_entry")
                self._finalize_observers()
                logger.info("执行 %s 在 start 入口命中取消标志，终态化", execution_id)
                return {
                    "execution_id": execution_id,
                    "status": "cancelled",
                    "cancelled_at_start": True,
                }

            # 租约（#23 扩展 A′）：**在落 running 行之前 acquire**，持有整个
            # start 处理窗口。A′（2026-10-06）只把租约对齐到 run_distributed
            # 之前，仍在先行落行**之后**——「落行 → acquire」间可被别的 worker
            # 抢先 XCLAIM 同一 start 消息（>claim_min_idle_ms 即可），抢占者
            # 先 acquire 成租约，原持有者 acquire 失败礼貌退出，但其 running
            # 行已被抢占者覆写。移动到落行前同时消除 A′ 的三个后果：
            # ①多 worker 抢到同一 start 消息时无闸可拦，双方同时跑同 id（双跑）；
            # ②死信守卫按「租约是否被持有」判活，start 执行恒"租约空"→ 长步
            #   误判持有者已死 → 误死信 + 重入队，长任务被反复打断；
            # ③抢占者白烧 5 次 delivery 配额。
            # 对齐 resume：acquire → 看门狗续租 → finally 释放。acquire 失败
            # 即抛 ExecutionLeaseError（run() 走不 ack/重投语义），抢占者礼貌退出。
            # holder 嵌入注册表 instance id（#50）：其他 worker 在租约冲突时
            # 可反解持有者并核其在册与否，区分「活持有者」与「死后 TTL 尾巴」。
            holder = new_holder_token(
                prefix="start", instance_id=getattr(self, "instance_id", None)
            )
            lease_value, fence_token = self._acquire_lease(execution_id, holder)
            if lease_value is None:
                raise ExecutionLeaseError(
                    f"execution {execution_id} is leased by another worker; "
                    "refuse concurrent start"
                )
            fence_token_reset = None
            if fence_token is not None:
                fence_token_reset = set_current_fence_token(fence_token)

            # 落 running 行（持租约窗口内）：与本执行此后所有写同 fence 世代，
            # 不会被后续接管者的 fenced CAS 拒绝序列排斥。
            self._persist_state_or_raise(execution_id, state, "start")

            # 取消监听（波次③）：登记整个推进窗口——取消标志键命中即在途节点被
            # execution.cancel() 中止（code 沙箱当场 killpg；协作节点如 agentrun
            # 消费 cancel_event 收手）。租约看门狗同窗口。
            self._register_cancel_watch(execution_id, execution)
            self._register_lease_watch(
                execution_id, lease_value, execution, fence_token=fence_token
            )
            # 标记「本次处理是否正常跑完」：只有正常跑完才说明该 execution
            # 这一轮真的结束了；从 run_distributed 里抛异常（尤其
            # ExecutionLeaseError 的 ack 释放路径）时执行对象可能仍在推进，
            # 见 finally 里的条件释放。
            completed_normally = False
            try:
                result = execution.run_distributed(flow, params=params, execution_id=execution_id)

                # 处理执行结果
                final_result = self._process_execution_result(
                    flow, result, state, execution, delivery_count=delivery_count,
                    lease_execution_id=execution_id,
                )

                completed_normally = True
                return final_result
            finally:
                self._unregister_cancel_watch(execution_id)
                self._unregister_lease_watch(execution_id, lease_value)
                if fence_token_reset is not None:
                    reset_current_fence_token(fence_token_reset)
                # release 对整个租约 value 串 compare（fencing 档 = {holder}:{gen}）
                #
                # ⚠️ **释放租约 ≠ 执行已结束**（2026-10-10 二波，生产实证）：
                # 「执行被他人持租约 → ack 释放」这条路径让本消息处理走到
                # finally，但**该 execution 对象仍在沙箱里推进长节点**
                # （实测 exec 339c3262：19:20:31 复用沙箱跑 impl，19:21:25 因
                # 「他人持租约」被 ack 释放 ⇒ 此后心跳停 80 分钟 ⇒ keeper 判
                # zombie 收尸）。一旦 release，「无租约」成事实：看门狗登记已被
                # 上面 unregister 摘除，也从未走过 renew 失败分支 ⇒ **没有任何
                # 组件再发布它的进度** ⇒ 健康 run 必然被判死。
                #
                # 因此：**仅当执行确已停止推进时才真正 release**；否则交给
                # `_lease_orphan_watch` 继续观测（只发心跳，不续租、不重复
                # cancel），由它在该执行结束或重新登记时摘除。
                if not completed_normally and self._execution_still_advancing(execution):
                    # tenant 由上下文取（该作用域无 tenant_id 形参，见上方
                    # `tenant_id=current_tenant()` 的同款用法）。
                    self._register_lease_orphan(
                        execution_id, execution, current_tenant(), fence_token
                    )
                else:
                    self.execution_lease.release(execution_id, lease_value)

        except ExecutionIdConflictError:
            # 冲突的 start 未落任何行（查盘在落 running 行之前）。若幂等映射
            # 是**本次**认领的，释放它——否则 7 天内同 dedup_key 会被指向
            # 另一条 flow 的 execution_id，后续修正后的 start 也走不到这里。
            # 原样上抛：run() 按 ValueError poison ack（换 flow 重投不会自愈）。
            if dedup_key and locals().get("dedup_action") == "claimed":
                self._release_start_dedup(
                    dedup_key, mapped_execution_id or execution_id
                )
            raise

        except (NodeExecutionRetryableError, ServiceDispatchError, ExecutionLeaseError):
            # 节点级可重试失败（波次二任务①）/ 幂等命中后重入队 resume 失败
            # （波次二任务③）/ **租约被他人持有**（A′ + #23，租约覆盖整个
            # start 窗口含先行落行）：原样上抛给 run() 按不 ack 语义处理——
            # ExecutionLeaseError 尤其关键：run() 的
            # `except ExecutionLeaseError` 分支据此 note_lease_conflict 并把消息
            # 留在 pending 等租约过期后 reclaim；若被下方通用 except 包成
            # RuntimeError，run() 会误 ack 消息 → 抢占者把别人的活执行 ack 掉，
            # 且真实持有者崩溃后无人 reclaim（执行永久失联）。绝不能包。
            raise

        except Exception as e:
            logger.error("执行流程出错: %s", _scrub_exception_text(e))
            # 波次③步内取消：start 路径首个节点在途命中取消监听 →
            # 引擎自 run_distributed 归一化抛出（挂 __cause__）或协作节点自杀
            # 抛普通异常。两条判据任一命中即终态化 cancelled，绝不包成
            # RuntimeError 让执行停在 running（取消是控制面意图）。
            if (
                _chain_has_cancellation(e) or self._cancel_requested(execution_id)
            ) and "state" in locals() and state is not None:
                state.status = "cancelled"
                state.end_time = datetime.now().isoformat()
                self._persist_state_or_raise(execution_id, state, "cancelled_in_start_node")
                self._finalize_observers()
                logger.info(
                    "执行 %s 在 start 首节点执行期响应取消，终态化 cancelled",
                    execution_id,
                )
                return {"execution_id": execution_id, "status": "cancelled"}
            # plaita#25 P2：错误路径落盘。确定性失败此前只打日志 + 实时发布事件
            # ⇒ 状态行停在 start 时落的 running，**落盘时间线只有 run_start**：
            # 失败节点（runner 已补发 on_node_end(error)）与收口事件都只存在于
            # 内存/SSE 里，数小时后复盘时没有第二来源。这里走**唯一落盘收口**
            # 把这次尝试的事件补进 state.timeline，并在流里补一条
            # run_end(final=False, status=error)。
            #
            # 状态行**刻意不动**：错误路径的 running 行是既有契约（可重投、
            # 可 cancel、zombie 可见——见
            # tests/unit/test_g1_resume_retry.py::test_running_row_survives_start_failure），
            # 也保证 ack/重投与死信守卫的判据逐字不变。
            # best-effort：落盘失败只告警，下面的 RuntimeError 照旧上抛 ⇒
            # 消息仍不 ack 走 at-least-once 重投。
            self._record_failed_start_attempt(execution_id, locals().get("state"))
            self._finalize_observers()
            raise RuntimeError(f"执行流程出错: {e}")

    def _record_failed_start_attempt(
        self,
        execution_id: str,
        state: Optional[ExecutionState],
    ) -> None:
        """start 路径**确定性失败**的时间线落盘（plaita#25 P2，best-effort）。

        只覆盖非可重试异常分支（可重试族 ``NodeExecutionRetryableError`` /
        ``ServiceDispatchError`` / ``ExecutionLeaseError`` 在上方原样上抛，
        执行保持非终态等待重投——那是重试语义，不被观测落盘改写）。

        边界（逐条写清，避免被读成「终态化」）：

        - **不改状态行**：status/context/error/end_time 一个都不动——错误路径的
          非终态行是既有契约（可重投、可 cancel，见
          ``tests/unit/test_g1_resume_retry.py`` 的预铸 id 守卫），也保证
          ack/重投、G1 唤醒矩阵与死信守卫的判据逐字不变；
        - **只补观测面**：唯一被写的是 ``state.timeline``（本次尝试的
          ``node_end(ok=false)`` 与 ``run_end(final=False, status=error)``）；
        - **best-effort**：落盘异常（瞬断/不可序列化/任何异常）只告警、绝不上抛
          ——上抛会替换掉调用方要抛的 RuntimeError，改写重投语义；
        - **不回收采集器**：状态非终态，同一执行的后续尝试还要继续往同一条流里
          追加（终态收口与回收由 ``_collect_execution_timeline`` 负责）。
        """
        if state is None:
            return
        try:
            self._persist_state_or_raise(
                execution_id, state, "start_error_attempt", attempt_end="error"
            )
        except Exception as persist_exc:  # noqa: BLE001 — 观测落盘绝不改写 ack 语义
            logger.warning(
                "start 失败路径的时间线落盘失败（忽略，消息仍不 ack 走重投）: "
                "execution_id=%s: %s",
                execution_id,
                _scrub_exception_text(persist_exc),
            )

    @staticmethod
    def _resume_carrier_stale_reason(state: Any, flow: Any) -> Optional[str]:
        """event/timeout 载体是否已错过可决议的挂起点（plaita#258）。

        返回 None 的两种情形：

        * 盘上不是 ``status=running``——终态/挂起幂等短路与 #33 的既有语义
          负责处置，不进本判据；
        * ``running`` 且 ``$LAST_NODE`` 仍是 ``pending`` 的 suspending 节点——
          事件/超时决议仍有处可落，载体仍有效。

        返回非 None = 执行仍活（running），但流内已经推进过至少一个节点
        （``$LAST_NODE`` 是非挂起节点），或挂起节点已被决议并落盘
        （``$LAST_NODE`` 是 suspending 但 ``status != pending``）：此时事件
        订阅已注销、resume 的决议语义用不上，继续 ack 唯一载体就是 #258 的
        永久悬挂。调用方应把意图归一化为 ``continue``（从 checkpoint 续跑）。
        """
        if (getattr(state, "status", "") or "") != "running":
            return None
        context = getattr(state, "context", None)
        if not isinstance(context, dict):
            return None
        last_node_id = context.get("$LAST_NODE")
        if not last_node_id:
            return None
        node = flow.find_node_by_id(last_node_id) if flow is not None else None
        if node is None:
            return None
        if getattr(node, "is_suspending", False) is True:
            node_results = context.get("$NODE")
            prev_state = node_results.get(last_node_id) if isinstance(node_results, dict) else None
            if isinstance(prev_state, dict) and prev_state.get("status", "") == "pending":
                return None
        return (
            f"盘上 status=running、$LAST_NODE={last_node_id!r} 不是 pending "
            "挂起点，event/timeout 决议无处可落"
        )

    def _load_and_guard_resume_state(
        self, flow_id: str, execution_id: str, resume_type: str,
        *, lease_held: bool = False,
    ) -> Tuple[ExecutionState, Optional[Dict[str, Any]]]:
        """加载执行状态并跑 resume 前置判定，返回 ``(state, early_result)``。

        ``early_result`` 非 None 表示本次 resume **不推进**（终态幂等 /
        挂起幂等短路 / 唤醒预算耗尽），调用方原样返回即可。

        两处调用（plaita#241）：租约**之前**一次，保留低成本前置短路与
        #33「suspended + continue 不取租约」契约；取到租约后**必须再调
        一次**并以这次为准——load 与 acquire 之间另一 worker 可能已把执行
        推到终态并释放租约，旧快照不能用于推进（fencing 只拒绝旧世代写，
        本 worker 拿到的是新一代，拦不住旧 state）。

        ``lease_held``（plaita#243）：第二次调用时外层 resume 已持有该执行
        租约。suspended 幂等分支的补派必须**复用这份所有权**，不能再次
        ``_acquire_lease``（自撞 ⇒ 补派跳过 + 消息 ack ⇒ 永久 suspended）。
        """
        # 加载执行状态。None 仅表示键不存在——Redis 后端的读取瞬断/反序列化
        # 失败现以上抛 ExecutionStateLoadError，由 run() 的重投递路径处理，
        # 不再被吞成 None 触发 poison ack（ReviewFix D1）。
        state = self.execution_storage.load_execution_state(execution_id)
        if not state:
            error_msg = f"找不到执行状态: {execution_id}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # 验证流程ID是否匹配
        stored_flow_id = state.flow_id
        if stored_flow_id and stored_flow_id != flow_id:
            error_msg = f"流程ID不匹配: 期望 {flow_id}, 实际 {stored_flow_id}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # A2R25-04：恢复路径墓碑闸（与 A51-01 的 start 闸同口径）。删除执行
        # 清掉状态键后，备份恢复可以把状态键带回来，但墓碑在平台根、同 TTL
        # 窗口内仍然有效——「删除即不可复活」不能只在 start 载体上成立。
        # 位置在 load 之后、取租约之前（调用方的租约内复核会再走一遍本函数，
        # 覆盖 load→acquire 窗口）；命中即具名返回 deleted，绝不 run_distributed。
        tombstone = self._load_execution_tombstone(execution_id, current_tenant())
        if tombstone is not None:
            logger.warning(
                "执行 %s 已删除（墓碑 tenant=%s reason=%s deleted_at=%s），"
                "拒绝 resume 推进（resume_type=%s）：恢复的状态键不撤销删除",
                execution_id,
                tombstone.get("tenant"),
                tombstone.get("reason"),
                tombstone.get("deleted_at"),
                resume_type,
            )
            return state, {
                "execution_id": execution_id,
                "status": "deleted",
                "discarded": True,
            }

        # 终态短路（2026-09 分布式评审 P1-1）：at-least-once 下重复投递的
        # resume 任务会命中已完成的执行。历史实现走完正常 resume 再在异常
        # 处理里把终态改写成 error——监控按 status 查询会得出错误结论。
        # 幂等语义：已终态的执行直接原样返回，不再推进。
        # cancelled 同为终态（console cancel 端点先写 cancelled 再投
        # resume_type=cancel 消息；若此处不放行，挂起执行会被 on_cancel
        # 续跑到 end 翻成 completed、非挂起执行被 ResumeError 翻成 error
        # ——「已取消」跳回「已完成/失败」。E2E cancel 回归用例钉住此语义）。
        # G1 例外：error + resume_type=retry 放行——error 不再是绝对终态，
        # 从断点（上一成功节点的后继）步进重跑失败节点（keeper 迁移设计稿
        # §G1，验收=失败节点恰重跑一次、已完成不重放）。
        state_status = getattr(state, "status", "") or ""
        retry_wakeup = state_status == "error" and \
            ResumeType.coerce(resume_type) is ResumeType.RETRY
        if retry_wakeup and self._g1_wakeup_budget_exhausted(execution_id):
            # G1 唤醒**有界**（plaita#73 补丁，2026-10-10 实证）：
            # 每次唤醒都会清零节点重试预算（下方 retry_wakeup 分支），于是
            # 「唤醒 → 重置预算 → 跑 5 次 → 又终态化 → 再唤醒」可以无限循环。
            # 实测某执行 07:20/07:21/07:23 连续被唤醒、每轮烧 5 次 impl；
            # 调用方是 inflight-watch 的自动 resume（flow 侧 `resume_type=retry`，
            # 每 15 分钟一轮，账本「同 exec 只 resume 一次」挡不住跨轮重复）。
            # 达上限即拒绝唤醒、保持 error 终态（幂等返回，不抛异常——
            # 抛异常会让调用方以为失败并重试）。
            logger.error(
                "执行 %s G1 唤醒次数达上限（%s/%s），拒绝再次唤醒——"
                "error 终态保留，避免「唤醒重置预算」无限循环",
                execution_id, self._read_g1_wakeup_count(execution_id),
                self.G1_MAX_WAKEUPS,
            )
            return state, {
                "execution_id": execution_id,
                "status": state_status,
                "already_terminal": True,
                "g1_wakeups_exhausted": True,
            }
        if retry_wakeup and self._deterministic_failure_exhausted(execution_id):
            # 确定性失败已达上限（plaita#73 遗留层）：该执行的失败是**确定性**的
            # （`exited 1` / `sync_in` / 超时…），重跑只会再烧一次全额成本。
            # 与 G1 上限同理返回幂等结果、不抛异常（抛异常会让调用方当失败重试），
            # 让消息层重投在此短路、不再空转沙箱。
            #
            # **必须与 ``retry_wakeup``（status=error）绑定**（plaita#53）：计数
            # 键寿命长于状态行（终态化写盘失败/状态被回滚都可能留下
            # 「running + 计数达限」的组合），而本分支返回的
            # ``already_terminal`` 是**谎报终态 + 一行不推进**——对 running 执行
            # 就是这个工单里的「resume 假受理」：API 返回已受理、node_timings /
            # last_update_time 双双不动、无 error 无告警。running 执行没有
            # 「失败节点」可重跑，retry 的语义就是断点续跑，必须真实重派。
            logger.error(
                "执行 %s 确定性失败已判不可救（%s/%s），拒绝唤醒重跑——"
                "error 终态保留",
                execution_id, self._read_deterministic_failure_count(execution_id),
                self.DETERMINISTIC_FAILURE_MAX,
            )
            return state, {
                "execution_id": execution_id,
                "status": state_status,
                "already_terminal": True,
                "deterministic_failure_exhausted": True,
            }
        if state_status in ("completed", "error", "cancelled") and not retry_wakeup:
            logger.info(
                "执行 %s 已是终态 (%s)，跳过重复 resume", execution_id, state_status,
            )
            return state, {
                "execution_id": execution_id,
                "status": state_status,
                "already_terminal": True,
                "result": getattr(state, "result", None),
                "error": getattr(state, "error", None),
            }

        # 挂起幂等短路（#33）：suspended 执行收到 resume_type=continue ——策略层
        # 的 pending 守卫必抛 ResumeError（continue 不允许绕过挂起节点），历史上
        # 这条路会被下方通用 except 终态化成 error：一次重复投递把本可等
        # 事件/审批/延迟恢复的执行永久打封（连 resume_type=event 都被终态短路
        # 拒绝，retry 也救不回）。continue 对挂起执行不携带任何推进语义——
        # checkpoint 未变、事件订阅仍在，正确动作是幂等跳过（ack 消息，状态
        # 原样保留），等真正的决议路径（event/cancel/timeout）来唤醒。
        # resume_type=retry 对挂起执行同样过不了策略层守卫，一并短路——
        # retry 的对象是 error 态断点，不是挂起节点。
        if state_status == "suspended" and ResumeType.coerce(resume_type) in (
            ResumeType.CONTINUE, ResumeType.RETRY,
        ):
            # 取消意图优先于挂起幂等短路（台账第三十四轮 ④）：挂起之前写的
            # 取消标志键（控制面在 running 时只写意图）若在此早退，消息会被
            # ack、盘上保持 suspended、flag 仍在——独立 cancel 载体丢失时
            # 执行带着取消意图长期挂起。命中标志即**不早退**：交给调用方
            # 走正常路径（取租约 → 复核 → 取消终态化），与 resume 入口既有
            # 取消检查点共用同一收口。仅在确有取消意图时取租约，#33 的
            # 「suspended + continue 不取租约」契约对其余路径不变。
            if self._cancel_requested(execution_id):
                logger.warning(
                    "执行 %s 处于 suspended 但取消标志已置位：resume_type=%s "
                    "不按挂起幂等短路，转正常路径在租约内终态化 cancelled"
                    "（台账第三十四轮 ④）",
                    execution_id, resume_type,
                )
                return state, None
            # plaita#233：短路**不等于什么都不做**。挂起行上若留着未清的
            # 派发凭据（「落盘 suspended → 派发」窗口里派发失败/进程被杀），
            # 这条 continue/retry 重投就是唯一还在的载体——不补派的话
            # delay/approval 任务永远发不出去，执行永久 suspended。补派
            # **不重跑挂起节点**（#33 契约不变：不取租约、不造 FlowExecution、
            # 状态原样），只把 service_config 幂等重投一次并清凭据；
            # 补派失败抛 ServiceDispatchError → 消息不 ack 继续重投。
            compensated = self._compensate_suspend_dispatch(
                state, execution_id, source=f"resume:{resume_type}",
                lease_held=lease_held,
            )
            logger.info(
                "执行 %s 处于 suspended（挂起节点待事件决议），resume_type=%s "
                "不携带推进语义，幂等跳过%s", execution_id, resume_type,
                "（已补派未投递的挂起服务任务）" if compensated else "",
            )
            result = {
                "execution_id": execution_id,
                "status": state_status,
                "already_suspended": True,
            }
            if compensated:
                result["dispatch_compensated"] = True
            return state, result

        return state, None

    def resume_flow(self, flow_id: str, execution_id: str, resume_type: str,
                    data: Optional[Dict[str, Any]] = None,
                    delivery_count: Optional[int] = None,
                    params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        恢复流程执行。

        取得 ``execution_id`` 租约后才推进；另一 worker 已持有租约时抛
        ``ExecutionLeaseError``（RedisFlowWorker 对此**不** XACK，待租约过期后回收）。

        Args:
            flow_id: 流程ID
            execution_id: 执行ID
            resume_type: 恢复类型
            data: 恢复数据
            delivery_count: 消息投递次数（可观测；见 _process_execution_result）
            params: 可选入参（#122）。只在盘上 checkpoint 为空时被引擎用来
                重新 ``setup_flow``（start→resume 载体转换会撞上这种窗口）；
                checkpoint 非空（正常 resume）时**不参与**——续跑一律以盘上
                context 为准，续跑语义零变化。
        """
        # 前置判定（租约之前）：终态/挂起幂等短路在此完成，保持 #33 的
        # 「suspended + continue 不取租约」契约。这里拿到的 state 只是
        # **预读快照**——取到租约后必须重读并以盘上状态为准（plaita#241）。
        state, early_result = self._load_and_guard_resume_state(
            flow_id, execution_id, resume_type
        )
        if early_result is not None:
            return early_result

        # 获取流程版本
        version = state.flow_version

        # 获取流程定义
        try:
            flow = self.get_flow_definition(flow_id, version)
        except FlowDefinitionMissingError:
            # #304：定义缺失（版本被删/未同步）不再裸 ValueError poison ack
            # ——先终态化 error（可观测 + 配额名额释放）再上抛。位置在租约
            # 前（#33「suspended + continue 不取租约」契约保持）。不传上面
            # 的预读快照（B123-01）：load→取定义的窗口内，持缓存定义的活
            # worker 可能已把执行推进到 completed 终态，快照（suspended +
            # 旧 context）的 error 写会无条件覆写终态——状态回退 + 整行
            # 倒灌旧断点（retry 重放已成功节点）+ 按快照 context 误 pause
            # 活跑沙箱 + 假终态 run_end 先行双发。传 None 让助手**现读盘**：
            # 已终态一律不覆写，非终态才终态化且落在现读行（与 start 路径
            # 同款）。现读后的写仍走普通写（未持租约）——写的是刚读到的
            # 行，窗口收窄到读-写之间。
            self._terminalize_definition_missing(
                execution_id, flow_id, version, None,
                phase="flow_definition_missing_resume",
            )
            raise

        # flow 定义指纹（波次二任务②）：worker 的定义 TTLCache 有 300s 窗口、
        # console engine_sync 可直接覆盖 Redis 定义——挂起执行 resume 用
        # latest 版本定义时，若定义自启动后被改过，``_get_next_from_last``
        # 找不到节点/走错分支且无任何告警。指纹在 lease 内判定（与活
        # worker 的推进写串行化，防「他人持租约推进中却被误终态化」），
        # 不一致 → 终态化 error + raise ValueError（poison ack）——意图是
        # 可观测的终态而不是重投风暴。老状态 flow_hash 为 None → 跳过校验
        # （零回归）。指纹取**存储中的原始定义**（plaita#36 口径 v2）：同一份
        # 存储定义不受引擎版本/模型字段演进影响。
        current_flow_hash = self.get_flow_definition_hash(flow_id, version)

        # 解析流程定义
        logger.info("恢复流程执行: %s, 执行ID: %s, 恢复类型: %s", flow_id, execution_id, resume_type)

        # holder 嵌入注册表 instance id（#50），语义同 start 路径。
        holder = new_holder_token(
            prefix="resume", instance_id=getattr(self, "instance_id", None)
        )
        lease_value, fence_token = self._acquire_lease(execution_id, holder)
        if lease_value is None:
            raise ExecutionLeaseError(
                f"execution {execution_id} is leased by another worker; refuse concurrent resume"
            )

        fence_token_reset = None
        if fence_token is not None:
            # fencing 写侧守门（设计稿 §4.2）：本 resume 的所有状态落盘带
            # 世代号，FencedExecutionStorage 据此 CAS；finally 复位。
            fence_token_reset = set_current_fence_token(fence_token)

        try:
            # TOCTOU 复核（plaita#241）：上面的 load 发生在取租约**之前**。
            # 若窗口内另一 worker 已把执行推到终态并释放租约，本 worker 随后
            # 取得的是**新一代**租约——fencing 只拒绝旧世代写，不会拦它拿
            # 旧 state 继续 run_distributed（节点副作用重放、终态行被二次
            # 改写）。租约内重读并重跑前置判定；state/early_result 均以本次
            # 为准，不复用 acquire 前的快照。
            state, early_result = self._load_and_guard_resume_state(
                flow_id, execution_id, resume_type, lease_held=True
            )
            if early_result is not None:
                logger.info(
                    "执行 %s 租约内复核发现状态已变更（status=%s）：拒绝本次 "
                    "resume，不复用 acquire 前的旧 state（plaita#241）",
                    execution_id, early_result.get("status"),
                )
                # 具名拒绝：区别于入口即终态的重复投递——本次是**赢了租约
                # 竞争、但盘上状态已由上一持有者推进**，测试/值守可据此
                # 识别 TOCTOU 窗口，而不是当成普通幂等短路。
                early_result["rejected_after_lease"] = True
                return early_result

            # plaita#247：#241 的复核只替换了 state，version/flow/current_flow_hash
            # 仍是 acquire 前快照算出的值。窗口内 flow_version 变更（同 flow_id 新
            # 版本 / 另一 start 载体回写）时，继续用旧 flow 执行并把结果写回新
            # version 的状态行 ⇒ 执行定义与状态身份不一致。复核后的 state 才是
            # 权威：版本变了就用新版本重算定义与指纹；hash 冲突仍走下方既有
            # mismatch 终态路径（绝不带着旧定义/旧指纹续跑）。
            if state.flow_version != version:
                logger.warning(
                    "执行 %s 在 load→acquire 窗口内 flow_version 变更（%r → %r），"
                    "按租约内复核的新版本重算 flow 定义与指纹（plaita#247）",
                    execution_id, version, state.flow_version,
                )
                version = state.flow_version
                try:
                    flow = self.get_flow_definition(flow_id, version)
                except FlowDefinitionMissingError:
                    # #304：租约内复核换到的新版本定义缺失——同款终态化
                    # （此时持租约，fenced 写生效），具名上抛走下方专属
                    # except（不进通用 except 二次终态化）。
                    self._terminalize_definition_missing(
                        execution_id, flow_id, version, state,
                        phase="flow_definition_missing_resume_recheck",
                    )
                    raise
                current_flow_hash = self.get_flow_definition_hash(flow_id, version)

            state_status = getattr(state, "status", "") or ""
            retry_wakeup = state_status == "error" and \
                ResumeType.coerce(resume_type) is ResumeType.RETRY
            if state_status == "running" and \
                    ResumeType.coerce(resume_type) is ResumeType.RETRY:
                # running 执行没有失败节点可重跑（plaita#53 验收 2）：retry 在此的
                # 语义 = 从 checkpoint 派发后继节点（与 continue 同路），**必须真实
                # 重派**——禁止「受理后无动作」。显式留痕，值守从日志即可区分
                # 「真的推进了」与「白等」（租约被他人持有等不生效情形另见 run()
                # 的租约冲突日志）。
                logger.warning(
                    "执行 %s 处于 running，resume_type=retry 按「从断点续跑」处理"
                    "（running 执行无失败节点可重跑），本次将从 checkpoint 派发后继节点",
                    execution_id,
                )

            # 陈旧 event/timeout 载体的归一化（plaita#258）：resume 在流内
            # 推进并落过 step_persist 后，若发生瞬态 StatePersistError / 进程
            # 崩溃，消息按 at-least-once 重投，但 resume_type 仍是 event/timeout
            # ———而挂起订阅已在第一次 resume 时注销、$LAST_NODE 已不是 pending
            # 挂起点。策略层会抛 ResumeGuardError（协议错误），旧 run() 分支据此
            # ack 唯一载体 ⇒ 执行永久停在 running。这里在**取得租约并重读状态
            # 之后**确认「执行仍活、但决议无处可落」，把本次意图改写为 continue
            # （剩余工作就是从 checkpoint 续跑，已完成节点不重放）。租约内
            # 复核保证：另一 worker 已推进到**新的挂起点**时 status=suspended，
            # 不会走本分支，陈旧 resume 仍由策略层的订阅绑定守卫拒绝（#221）。
            if resume_type in (ResumeType.EVENT, ResumeType.TIMEOUT, "event", "timeout"):
                stale_reason = self._resume_carrier_stale_reason(state, flow)
                if stale_reason:
                    logger.warning(
                        "执行 %s 的 %s resume 载体已错过挂起点（%s）；订阅早已注销、"
                        "执行仍 running，本次归一化为 continue 从 checkpoint 续跑"
                        "（plaita#258）",
                        execution_id, resume_type, stale_reason,
                    )
                    resume_type = ResumeType.CONTINUE.value

            # XCLAIM 恢复路径取消检查点（设计稿 §3.1）：crash 后 cancel 消息
            # 或任一 resume 消息被重投，若取消标志在（控制面对运行中执行写的
            # 意图键），直接终态化而非继续推进。上一 checkpoint 已落盘，
            # cancelled 的 context 即取消点前一步的完整快照（§3.3）。
            if self._cancel_requested(execution_id):
                state.status = "cancelled"
                state.end_time = datetime.now().isoformat()
                self._persist_state_or_raise(execution_id, state, "cancelled_at_resume_entry")
                logger.info("执行 %s 在 resume 入口命中取消标志，终态化", execution_id)
                return {
                    "execution_id": execution_id,
                    "status": "cancelled",
                    "cancelled_at_resume": True,
                }

            # flow 定义指纹校验（波次二任务②）在 G1 retry 唤醒**之前**：
            # 定义被改时执行保持 error（hash 不匹配信息落盘），修复定义后
            # 仍可再 retry——G1 的「error 态可反复唤醒」语义不被破坏。
            # 取消是用户意图，已在上方优先放行。写盘走 _persist_state_or_raise
            # （带本 resume 的 fence 世代），随后 raise FlowHashMismatchError
            # → run() 按 ValueError poison ack（终态已可观测，重投无意义）。
            stored_flow_hash = getattr(state, "flow_hash", None)
            stored_algo = getattr(state, "flow_hash_algo", None)
            algo_changed = bool(stored_algo) and stored_algo != FLOW_HASH_ALGO
            allow_hash_change = bool(
                isinstance(data, dict) and data.get(ALLOW_FLOW_HASH_CHANGE_KEY)
            )
            hash_decision = classify_flow_hash_change(
                stored_hash=stored_flow_hash,
                current_hash=current_flow_hash,
                stored_algo=stored_algo,
                current_algo=FLOW_HASH_ALGO,
                allow_change=allow_hash_change,
            )
            if hash_decision in ("mismatch", "accepted"):
                if hash_decision == "accepted":
                    # 升级后的人工裁决入口：显式承认「换算法/换定义」并续跑，
                    # 必须留痕（WARNING + 状态里刷新指纹），不静默放行。
                    logger.warning(
                        "执行 %s 的 flow 指纹变化被显式放行续跑（%s）：stored=%s(%s) current=%s(%s)",
                        execution_id,
                        ALLOW_FLOW_HASH_CHANGE_KEY,
                        stored_flow_hash[:12],
                        stored_algo or "unknown",
                        current_flow_hash[:12],
                        FLOW_HASH_ALGO,
                    )
                    state.flow_hash = current_flow_hash
                    state.flow_hash_algo = FLOW_HASH_ALGO
                    self._persist_state_or_raise(execution_id, state, "flow_hash_change_accepted")
                else:
                    mismatch_msg = (
                        "flow 定义自启动后已变更，hash 不匹配，执行无法安全续跑 "
                        f"(execution_id={execution_id}, flow_id={flow_id}, "
                        f"stored_hash={stored_flow_hash[:12]}..., "
                        f"current_hash={current_flow_hash[:12]}..., "
                        f"stored_algo={stored_algo or 'unknown'}, current_algo={FLOW_HASH_ALGO})"
                    )
                    logger.error(mismatch_msg)
                    state.status = "error"
                    state.error = {
                        "message": mismatch_msg,
                        # plaita#93：本处终态化不由异常驱动（有专门的字段承载
                        # 细节），但落盘面同样要给下游一个机器可判别的身份：
                        # code 用引擎的通用 flow 级错误码（-500），error_type
                        # 是本处的 worker 级判据（不是节点业务码）。
                        "code": _FLOW_ERROR_CODE,
                        "error_type": "flow_hash_mismatch",
                        "stored_flow_hash": stored_flow_hash,
                        "current_flow_hash": current_flow_hash,
                        "stored_flow_hash_algo": stored_algo,
                        "current_flow_hash_algo": FLOW_HASH_ALGO,
                        # 升级疑似（算法标记变了且哈希也不同）：给运维明确下一步，
                        # 而不是只丢一句「hash 不匹配」。
                        "upgrade_suspected": algo_changed,
                        "hint": (
                            "引擎升级可能改变指纹算法；确认流程定义未变且可接受后，"
                            f"在 resume data 里带 {ALLOW_FLOW_HASH_CHANGE_KEY}: true 显式放行"
                        ) if algo_changed else None,
                    }
                    state.end_time = datetime.now().isoformat()
                    self._persist_state_or_raise(execution_id, state, "flow_hash_mismatch")
                    raise FlowHashMismatchError(mismatch_msg)
            elif hash_decision == "refresh":
                # 哈希相同 → 定义确实没变，只是算法标记变了（引擎升级）：续跑并刷新标记
                logger.info(
                    "执行 %s 的 flow 指纹一致但算法标记升级（%s → %s），续跑并刷新标记",
                    execution_id,
                    stored_algo,
                    FLOW_HASH_ALGO,
                )
                state.flow_hash_algo = FLOW_HASH_ALGO

            # 引擎版本跨 minor：只告警不拦截（硬门由 flow_hash 承担，这里给可观测性）
            stored_engine = getattr(state, "engine_version", None)
            current_engine = _engine_version()
            if stored_engine and _major_minor(stored_engine) != _major_minor(current_engine):
                logger.warning(
                    "执行 %s 由引擎 %s 创建，当前 %s：跨 minor 续跑，"
                    "若流程行为与预期不符请复核定义与节点实现",
                    execution_id,
                    stored_engine,
                    current_engine,
                )

            # G1 retry 唤醒：error → running 翻转并先落盘（观察者/监控立即
            # 可见；若本 worker 接着硬死，行是 running 而非 error——再一轮
            # retry 仍可放行，zombie 判定也不误报「终态」）。checkpoint 原样，
            # saved_context 由下方 run_distributed 携带。
            # 与任务①组合语义：唤醒即清零节点重试计数键——预算耗尽终态化的
            # 执行经人工 retry 唤醒后拿全新预算，否则唤醒的执行第一次失败就
            # 立刻再耗尽，G1 形同虚设。翻转落盘成功后才清零（落盘失败抛
            # StatePersistError 走重投，唤醒未生效不清预算）。
            # 不传 node_id ⇒ 清**该执行下所有节点**的计数键（plaita#123 起键按
            # 节点隔离，唤醒是执行级意图，必须整体重来）。
            if retry_wakeup and not state.context:
                # plaita#240：retry 的能力前提是盘上有 checkpoint。此前先翻
                # running 再落盘、等 run_distributed 才由策略层抛
                # ResumeGuardError；协议错误分支虽「保持原状」，但原状已经被
                # 改成 running（error/end_time 也被清空）——非法 error→running
                # 迁移 + 失败证据丢失。必须在**任何状态变更之前**拒绝。
                # 保持 error 原状（不落盘）、不创建 FlowExecution、不触发节点。
                message = (
                    "resume_type='retry' requires a saved checkpoint, but the "
                    f"execution has none; execution {execution_id} remains error "
                    "(status=error, error/end_time preserved)"
                )
                logger.warning(
                    "执行 %s 收到 retry 但 checkpoint 为空，拒绝唤醒（保持 error 原状）",
                    execution_id,
                )
                raise ResumeProtocolError(message, execution_id=execution_id)
            if retry_wakeup:
                state.status = "running"
                state.error = None
                state.end_time = None
                self._persist_state_or_raise(execution_id, state, "retry_wakeup")
                # 清预算 + 记配额原子收口（第七十一轮任务①）：两步散调在清
                # 计数失败时会留下「配额已消耗、预算没拿到」的半生效态（记数
                # 照常执行、失败被吞）——下一次失败直接终态化，每次半生效唤醒
                # 净烧一次额度。收敛为单段 Lua：整体生效或具名抛
                # G1WakeupAccountingError（StatePersistError 子族，消息不 ack
                # 重投、不终态化，配额守恒）。达上限的拒绝仍在上方入口处
                # （`_g1_wakeup_budget_exhausted`）。
                self._commit_g1_wakeup(execution_id)
                logger.info("执行 %s error 态经 retry 放行（重试计数已清零），从断点步进", execution_id)

            # 复用同一个 FlowExecution 贯穿恢复后的所有分布式步骤
            execution = FlowExecution(
                event_bus=self.event_bus,
                callback_handlers=self._handlers_for(execution_id),
            )
            execution.mode = ExecutionMode.DISTRIBUTED
            self._bind_observers(execution)
            # 登记看门狗（波次②）：持租约期间每 TTL/3 续租，防长步 > TTL
            # 被 XCLAIM 抢占双跑（fence 世代随登记带上，心跳写要过世代 CAS）
            self._register_lease_watch(
                execution_id, lease_value, execution, fence_token=fence_token
            )
            # 登记取消监听（波次③）：命中标志键即中止在途节点（与租约登记
            # 同窗口；finally 一并撤销）
            self._register_cancel_watch(execution_id, execution)

            # 直接使用 run_distributed 恢复执行
            # ``execution_id`` 必须显式透传（plaita#123 配套）：不传时引擎侧
            # 的 execution_id 与 worker 脱钩——resume 走 ``context.context =
            # saved_context`` 整体替换 checkpoint，若 checkpoint 缺
            # ``$EXECUTION_ID``（resume 常见），引擎 execution_id 为空串 ⇒
            # ``result.execution_id`` 回空、且重试计数键退化成
            # ``noderetry::{node}``（无执行 id），运维无法从键反查执行。
            # start 路径（本文件 ``start_flow``）早已是同款正确范式。
            #
            # ``params``（#122）：仅在 ``state.context`` 为空（假值）时被引擎
            # 用于 ``setup_flow``——即 start→resume 载体转换撞上的「首节点还
            # 没成功过」窗口；checkpoint 非空时引擎走真值分支，params 不参与
            # （续跑语义零变化，见 ``DistributedStrategy.execute``）。
            result = execution.run_distributed(
                flow,
                params=params,
                saved_context=state.context,
                resume_type=resume_type,
                resume_data=data,
                execution_id=execution_id,
            )

            # 处理执行结果
            final_result = self._process_execution_result(
                flow,
                result,
                state,
                execution,
                lease_execution_id=execution_id,
                lease_holder=lease_value,
                delivery_count=delivery_count,
            )

            return final_result

        except ExecutionLeaseError:
            raise
        except (StatePersistError, ServiceDispatchError, NodeExecutionRetryableError):
            # 落盘/派发失败：执行状态保持原样（is_end 前失败 → 仍 running，
            # 挂起派发失败 → 仍 suspended），消息不 ack 走 at-least-once 重投，
            # 下轮从 checkpoint / 挂起节点重入。绝不落 error 终态——重投消息
            # 会命中上方 already_terminal 短路被 ack，执行永久卡死。
            # NodeExecutionRetryableError（波次二任务①）同理：节点失败未
            # 终态化，重投后从 checkpoint 重跑失败节点；finally 释放租约，
            # 重投消息可重新取得租约。
            raise
        except FlowHashMismatchError:
            # flow 定义指纹不匹配（波次二任务②）：执行已在 raise 前终态化
            # error（可观测终态），原样上抛给 run() 按 ValueError poison
            # ack——重投只会命中 already_terminal 短路，终态化已完成，无意义。
            # 不落进下面的通用 except 二次终态化/包 RuntimeError。
            raise
        except FlowDefinitionMissingError:
            # #304：租约内复核路径的定义缺失——raise 前已终态化 error
            # （同 FlowHashMismatchError 的纪律），原样上抛 poison ack，
            # 不进通用 except 二次终态化。
            raise
        except ResumeProtocolError:
            # plaita#240：预检在**状态变更前**抛出的协议错误，必须原样上抛给
            # run()（ack、不终态化）——不能被下面通用 except 当作执行失败
            # 二次终态化 error。策略层守卫路径仍由 _chain_has_resume_guard_error
            # 在通用 except 内包成同类异常。
            raise
        except Exception as e:
            logger.error("恢复流程执行出错: %s", _scrub_exception_text(e))

            # 波次③步内取消：被恢复的节点在途命中取消监听 → 引擎抛
            # FlowCancelledException（归一化后挂 __cause__）或协作节点自杀抛
            # 普通异常。两条判据任一命中即终态化 cancelled，绝不写 error。
            if _chain_has_cancellation(e) or self._cancel_requested(execution_id):
                state.status = "cancelled"
                state.end_time = datetime.now().isoformat()
                self._persist_state_or_raise(execution_id, state, "cancelled_in_node")
                self._finalize_observers()
                logger.info(
                    "执行 %s 在恢复节点执行期响应取消，终态化 cancelled", execution_id
                )
                return {
                    "execution_id": execution_id,
                    "status": "cancelled",
                }

            # 挂起守卫豁免（#33）：**只**豁免策略层守卫 ``ResumeGuardError``
            # （continue/retry 试图绕过 pending 挂起节点、resume 打在无挂起/
            # 非挂起 checkpoint 上）——那是调用方协议与执行状态不匹配，不是
            # 执行自身失败。终态化 error 会把本可被 event/cancel/timeout 唤醒的
            # 挂起执行永久打封（终态短路从此拒绝一切 resume 类型，死局），且这类
            # 错误天然由 at-least-once 重投产生（重复投递的 event resume 打在已
            # 推进的 checkpoint 上同族），终态化会误杀健康执行。
            # 豁免为「保持原状 + 上抛」：suspended 执行保持 suspended，消息被
            # ack（重投也只会再次命中同一守卫，重投无意义）；running 执行保持
            # checkpoint 现状。
            # 裸 ``ResumeError``（节点 ``resume()`` 抛错被策略层包的那个，
            # 如事件数据畸形）**不豁免**：静默保持原状 = 无 error 记录、永远等
            # 不到决议的哑执行；交给下方现状路径（可重试判据 → 终态化 error）
            # 才可观测、可人工 retry。
            if _chain_has_resume_guard_error(e):
                logger.warning(
                    "执行 %s (status=%s) 收到与挂起状态不匹配的 resume 请求，"
                    "协议错误不终态化，执行保持原状: %s",
                    execution_id, getattr(state, "status", "?"), _scrub_url_text(e),
                )
                self._finalize_observers()
                raise ResumeProtocolError(
                    f"resume 协议错误（执行保持原状 status={getattr(state, 'status', '?')}）: {e}",
                    execution_id=execution_id,
                ) from e

            # 节点级有界重试（波次二任务①）：同 _process_execution_result，
            # 瞬态节点失败不终态化、消息等重投；否则现状终态化 error。
            # 失败节点 id 取自异常链（plaita#123：计数键按节点隔离）；取不到
            # 时计数退化为执行维度，终态化 error 里也就没有按节点的次数。
            failed_node = _node_id_from_chain(e)
            retry_exc = self._node_failure_retry_decision(
                e, execution_id, delivery_count, failed_node
            )
            if retry_exc is not None:
                raise retry_exc from e

            # 更新执行状态为错误
            state.status = "error"
            retries = max(
                0, self._read_node_retry_counter(execution_id, failed_node) - 1
            )
            if retries > 0:
                state.error = {
                    "message": f"节点执行失败（重试 {retries} 次后仍失败）: {e}",
                    "node_retries": retries,
                    # plaita#93：终态化那一刻把引擎已算出的 code / error_type 与
                    # HTTP 明细摘要如实落盘（message 保留，老消费方零影响）。
                    **_error_fields(e),
                }
            else:
                state.error = {"message": str(e), **_error_fields(e)}
            if failed_node:
                state.error["failed_node"] = failed_node
            state.end_time = datetime.now().isoformat()

            self._persist_state_or_raise(execution_id, state, "resume_error_handler")
            self._finalize_observers()

            # plaita#73：终态化**已完成**，必须 poison ack 终止重投循环。
            # 原先抛裸 RuntimeError → 落到 run() 兜底 except → 不 ack → 重投
            # → status=error 只放行 retry → 再撞同一确定性失败 → 无限循环
            # （且全程不碰重试计数键，预算永不耗尽）。
            raise NodeFailureTerminalizedError(
                f"恢复流程执行出错（已终态化 error，消息 poison ack）: {e}"
            )
        finally:
            self._unregister_lease_watch(execution_id, lease_value)
            self._unregister_cancel_watch(execution_id)
            if fence_token_reset is not None:
                reset_current_fence_token(fence_token_reset)
            # release 对整个租约 value 串 compare（fencing 档 = {holder}:{gen}）
            self.execution_lease.release(execution_id, lease_value)

    def _process_execution_result(
        self,
        flow: Flow,
        result: Dict[str, Any],
        state: ExecutionState,
        execution: Optional[FlowExecution] = None,
        lease_execution_id: Optional[str] = None,
        lease_holder: Optional[str] = None,
        delivery_count: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        处理流程执行结果

        Args:
            flow: 流程对象
            result: 执行结果
            state: 执行状态
            execution: 贯穿本执行全过程的 FlowExecution (复用以保留回调);
                为 None 时按需创建, 仅供向后兼容的简单调用场景使用
            lease_execution_id / lease_holder: resume 租约续期参数
            delivery_count: 消息投递次数（可观测/日志用；节点重试预算判定
                用重试计数键，见 ``_record_node_retry``）

        Returns:
            Dict[str, Any]: 最终的执行结果
        """

        # 初始化状态
        # 凭据可见性（升级演练 P2-1 + MIGRATION 轮换提醒）：expose_env 白名单
        # 命中的变量会随 checkpoint 持久化到 Redis——每个执行只告警一次。
        context_for_warn = result.get("context") or {}
        env_snapshot = context_for_warn.get("$ENV") if isinstance(context_for_warn, dict) else None
        if env_snapshot:
            logger.warning(
                "执行 %s 的 checkpoint 包含 $ENV 快照（%d 个变量，可能含密钥）。"
                "这些值会在 Redis 中存活至 TTL/删除——请在升级/退役窗口轮换凭据。",
                state.execution_id if state is not None else "?", len(env_snapshot),
            )
        # id 以「被加载记录的 execution_id」为准（升级演练 P2-2）：老 checkpoint
        # 的 context 可能缺 $EXECUTION_ID，信任 result 会把终态写到
        # plaita:execution: 空键，原记录永久停留 suspended。
        execution_id = result.get("execution_id") or (
            state.execution_id if state is not None else None
        )
        context = result.get("context")

        if execution is None:
            execution = FlowExecution(
                event_bus=self.event_bus,
                callback_handlers=self._handlers_for(execution_id),
            )
            execution.mode = ExecutionMode.DISTRIBUTED
            self._bind_observers(execution)

        steps_since_persist = 0

        while True:
            is_end = result.get("is_end", False)
            is_suspend = result.get("is_suspend", False)

            # plaita#233：任何**非挂起**去向（完成/取消/步进/错误）都意味着
            # 挂起节点已被越过或决议，残留的「待派发」凭据必须就地清掉——
            # 否则执行早已续跑/终态化，之后任何一次 continue 重投还会拿旧
            # 凭据补派一条早已过期的外延任务。
            if not is_suspend:
                state.dispatch_pending = None

            if is_end:
                # 落盘前失租检查（波次② §4.1）：看门狗已判死则不写状态，
                # 消息不 ack 走重投——防止与接管者双写终态
                self._raise_if_lease_lost(lease_execution_id)
                state.status = "completed"
                state.context = context
                state.end_time = datetime.now().isoformat()
                self._persist_state_or_raise(execution_id, state, "completed")
                self._finalize_observers()
                break
            elif is_suspend:
                self._raise_if_lease_lost(lease_execution_id)
                state.status = "suspended"
                state.context = context
                # plaita#233：「落盘 → 派发」是两段非原子操作，中间死掉时
                # 没有任何载体能补派（#33 对 suspended+continue 幂等短路、
                # start 只回报现状、无巡检），执行永久 suspended 且无
                # error/end_time。修法 = **落盘时带上待派发凭据**
                # （dispatch_pending，与 suspended 同一次写），派发成功后
                # 再清；重投载体（resume/start）据此补派一次。
                #
                # 为什么是「先落盘 + 凭据」而不是「先派发再落盘」：
                # ① 先派发后落盘时，「派发成功、落盘失败」是**常态窗口**
                #    （StatePersistError 正是设计上要不 ack 重投的瞬态），
                #    每次重投都会重跑挂起节点再派一条 → 重复任务 + 节点副作用
                #    重放；
                # ② 先落盘后派发时，失败窗口的代价只是「任务没发出去」，由
                #    凭据补派收敛到恰好一次（极端残留见
                #    ``_clear_suspend_dispatch_pending``）。
                # 即：把「已发生的外部投递」放在「可重放的落盘」之后，让
                # 不确定窗口只覆盖可补偿的一侧。
                has_dispatch = self._mark_suspend_dispatch_pending(state, result, context)
                self._persist_state_or_raise(execution_id, state, "suspended")
                self._dispatch_service_task(result, context, execution_id)
                if has_dispatch:
                    self._clear_suspend_dispatch_pending(
                        execution_id, state, source="suspend"
                    )
                break
            else:
                # 取消检查点（波次① §3.1 选型 B）：控制面对运行中执行只写
                # 取消标志键不再直写 cancelled；worker 在步界消费意图——
                # 上一节点结果已随 PERSIST_EVERY_N_STEPS=1 落盘，此处
                # state.context 即取消点前一步的完整 checkpoint（§3.3）。
                # 默认语义：在途节点跑完（软中断），code 沙箱类由引擎层
                # cancel_event 即时击杀（§3.3，波次③）。
                if self._cancel_requested(execution_id):
                    self._raise_if_lease_lost(lease_execution_id)
                    state.status = "cancelled"
                    state.context = context
                    state.end_time = datetime.now().isoformat()
                    self._persist_state_or_raise(execution_id, state, "cancelled")
                    self._finalize_observers()
                    logger.info("执行 %s 已在步界响应取消（标志键命中），终态化", execution_id)
                    break
                state.status = "running"
                try:
                    self._renew_lease_if_held(lease_execution_id, lease_holder)
                    # ``execution_id`` 显式透传（plaita#123 配套，同 resume 路径）：
                    # 本函数内该变量取自「被加载记录的 execution_id」且整个步进
                    # 循环内**不变**，把引擎 execution_id 钉死到它上面，重试计数
                    # 键的 execution_id 段才不会退化成空串。
                    result = execution.run_distributed(
                        flow,
                        saved_context=context,
                        resume_type="continue",
                        execution_id=execution_id,
                    )

                    context = result.get("context", context)
                    steps_since_persist += 1
                    # 重试计数按「当前节点的连续失败」计（组合语义）：**刚成功
                    # 推进的那个节点**清零——不同节点的失败不共享预算（节点 A
                    # 抖一次花掉的预算不应让之后节点 B 的第一次失败就少一次机会）。
                    # 只清这一个节点（plaita#123 真因）：此前清掉整个执行的计数，
                    # 在「前序节点稳定成功 + 某节点稳定失败」的 flow 上，前序
                    # 节点每轮成功都把失败节点的计数抹平 ⇒ 永远到不了预算 ⇒
                    # 永不终态化 ⇒ 无限重投（实测 2026-10-10 13:0x 本机
                    # Redis db1：keeper-watch 累积 667 个悬停 running 执行）。
                    # 成功节点 id 取自本次步进返回的 result.id
                    # （distributed 每步执行**一个**节点，_create_lazy_output /
                    # _create_end_output 都带 id）；取不到则退化为执行维度键。
                    self._reset_node_retry_counter(
                        execution_id, result.get("id")
                    )
                    # 确定性失败计数同款「连续」语义：推进过就不是「同一位置
                    # 反复撞墙」，长寿命执行跨节点的偶发确定性失败不应累计判死。
                    self._reset_deterministic_failure_counter(execution_id)
                    # 兜底总重试计数同样按「连续」语义清零（plaita#242）：它只
                    # 该兜住「预算机制整体失效后的连续重试」，不能终身累计——
                    # 否则长寿命执行达 100 次后，任意一条超投递消息都会把
                    # suspended（等审批/事件）的执行误终态化。
                    self._reset_node_retry_total(execution_id)

                    is_end = result.get("is_end", False)
                    is_suspend = result.get("is_suspend", False)

                    if (
                        not is_end
                        and not is_suspend
                        and steps_since_persist >= self.PERSIST_EVERY_N_STEPS
                    ):
                        self._raise_if_lease_lost(lease_execution_id)
                        # plaita#280 顺带 P2：取消可能落在本步顶部的取消检查
                        # 之后、running 落盘之前。此时若直接写 running，会把
                        # console 已 CAS 的 cancelled 覆盖回去（配额也已释放）。
                        # 落盘前再复核一次取消标志，命中即终态化。
                        if self._cancel_requested(execution_id):
                            state.status = "cancelled"
                            state.context = context
                            state.end_time = datetime.now().isoformat()
                            self._persist_state_or_raise(
                                execution_id, state, "cancelled_before_step_persist"
                            )
                            self._finalize_observers()
                            logger.info(
                                "执行 %s 取消落在步界检查与 running 落盘之间，"
                                "复核命中，终态化 cancelled", execution_id,
                            )
                            break
                        state.context = context
                        state.last_update_time = datetime.now().isoformat()
                        self._persist_state_or_raise(execution_id, state, "step_persist")
                        steps_since_persist = 0
                        logger.info("流程步骤执行完成，继续下一步: %s", execution_id)

                except ExecutionLeaseError:
                    raise
                except StateSerializationError as exc:
                    # #80：**永久失败，重投无用** ⇒ 转可观测终态（不落进下面的
                    # 通用 except，那会把永久失败送进重试判别）。
                    # 必须**先净化**：导致不可序列化的那个值还在 context 里，
                    # 直接终态化再落盘会**再抛同一个异常**（终态永远落不下去）。
                    logger.error(
                        "执行状态不可序列化，终态化 error（不重投）: execution_id=%s, err=%s",
                        execution_id, _scrub_url_text(exc),
                    )
                    _sanitized = sanitize_context_for_persist(context)
                    if _sanitized is not None:
                        # 层 1：只改**持久化副本**，原 context 不动
                        state.context = _sanitized
                        state.status = "error"
                        state.error = {
                            "context_sanitized": True,
                            "rule": "non-JSON values coerced to str via default=str (#80)",
                            "exception": str(exc)[:200],
                            # plaita#93：异常身份也落盘（可序列化失败本身没有
                            # 引擎错误码，error_type 退化为异常类名）。
                            **_error_fields(exc),
                        }
                        state.end_time = datetime.now().isoformat()
                        try:
                            self._persist_state_or_raise(
                                execution_id, state, "serialization_error"
                            )
                        except StateSerializationError:
                            logger.error(
                                "层 1（净化后）仍无法落盘，降级最小 error 状态: execution_id=%s",
                                execution_id,
                            )
                            _sanitized = None
                    if _sanitized is None:
                        # 层 2（C）：最小 error 状态，**不含 context** —— 丢现场是
                        # 最后一道闸，不是第一选择
                        logger.error(
                            "降级最小 error 状态落盘（不含 context）: execution_id=%s",
                            execution_id,
                        )
                        state.context = {}
                        state.status = "error"
                        state.error = {
                            "message": (
                                "执行状态不可序列化，且净化后仍无法落盘"
                                f"（永久失败，不重投）: {str(exc)[:200]}"
                            ),
                            **_error_fields(exc),
                        }
                        state.end_time = datetime.now().isoformat()
                        self._persist_state_or_raise(
                            execution_id, state, "serialization_error_minimal"
                        )
                    break
                except StatePersistError:
                    # 步进落盘失败必须原样上抛：若落进下面的通用 except，
                    # 会把可重投的瞬态失败改写成 error 终态（消息被 ack 后
                    # 重投命中 already_terminal 短路），执行永久卡死。
                    raise
                except Exception as e:
                    logger.error("流程执行出错: %s", _scrub_exception_text(e))
                    # 波次③步内取消：两条判据任一命中即终态化 cancelled（= 取消
                    # 点前 checkpoint），绝不写 error：
                    #   ① 异常链含 FlowCancelledException（引擎在下一节点入口
                    #      抛出的执行级取消）；
                    #   ② 取消标志键命中——协作节点（如 agentrun）收到
                    #      cancel_event 后自杀抛出普通异常，异常类型无取消
                    #      特征，但控制面意图键仍在，以键为准最稳。
                    # 放行前做失租检查：与步界取消分支同规则（看门狗已判死
                    # 则不写状态，让接管者来写）。
                    if _chain_has_cancellation(e) or self._cancel_requested(execution_id):
                        self._raise_if_lease_lost(lease_execution_id)
                        state.status = "cancelled"
                        state.context = context
                        state.end_time = datetime.now().isoformat()
                        self._persist_state_or_raise(execution_id, state, "cancelled_in_node")
                        self._finalize_observers()
                        logger.info(
                            "执行 %s 在节点执行期响应取消（取消监听命中），终态化 cancelled",
                            execution_id,
                        )
                        break
                    # 节点级有界重试（波次二任务①）：瞬态节点失败不终态化，
                    # 抛重试异常让消息走 at-least-once 重投，从 checkpoint
                    # 重跑失败节点；预算耗尽/判据不符/回滚开关 → 现状终态化。
                    # 失败节点 id 取自异常链（plaita#123：计数键按节点隔离）。
                    failed_node = _node_id_from_chain(e)
                    retry_exc = self._node_failure_retry_decision(
                        e, execution_id, delivery_count, failed_node
                    )
                    if retry_exc is not None:
                        raise retry_exc from e
                    state.status = "error"
                    state.context = context
                    retries = max(
                        0, self._read_node_retry_counter(execution_id, failed_node) - 1
                    )
                    if retries > 0:
                        state.error = {
                            "message": f"节点执行失败（重试 {retries} 次后仍失败）: {e}",
                            "node_retries": retries,
                            # plaita#93：步进失败终态化同样落结构化字段。
                            **_error_fields(e),
                        }
                    else:
                        state.error = {"message": str(e), **_error_fields(e)}
                    if failed_node:
                        state.error["failed_node"] = failed_node
                    state.end_time = datetime.now().isoformat()
                    self._persist_state_or_raise(execution_id, state, "error_state")
                    break

        return result

    @staticmethod
    def _resolve_service_task(
        result: Dict[str, Any], context: Dict[str, Any]
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        """从挂起结果解析 ``(service_config, subtype)``；非服务节点返回 ``(None, "")``。

        单一解析口（plaita#233）：派发路径与**落盘凭据**必须看到同一份配置——
        两处各写一遍解析逻辑，一旦分叉就会出现「凭据记了 A、实际派发 B」的
        补偿错投。
        """
        service_config = (result or {}).get("service_config")
        if not isinstance(service_config, dict) or not service_config:
            nodes = (context or {}).get("$NODE") or {}
            last_node = (context or {}).get("$LAST_NODE")
            node_result = nodes.get(last_node) if last_node else None
            if isinstance(node_result, dict):
                service_config = node_result.get("service_config")
        if not isinstance(service_config, dict) or not service_config:
            return None, ""
        subtype = str(service_config.get("type") or (result or {}).get("node_subtype") or "").strip()
        if not subtype:
            return None, ""
        return dict(service_config), subtype

    @staticmethod
    def _pending_suspend_dispatch(state: Any) -> Optional[Dict[str, Any]]:
        """读取未清的挂起派发凭据（plaita#233）；None = 无可补偿对象。

        老状态没有该字段（pydantic 默认 None）⇒ 一律按「无可补偿」处理：
        升级前已卡住的 suspended 执行维持原状，零回归（无法凭空知道它是否
        派发过——猜错会造重复任务）。
        """
        marker = getattr(state, "dispatch_pending", None)
        if isinstance(marker, dict) and marker.get("pending"):
            return marker
        return None

    def _mark_suspend_dispatch_pending(
        self, state: ExecutionState, result: Dict[str, Any], context: Dict[str, Any]
    ) -> bool:
        """把「待派发」凭据写进状态对象（plaita#233），由调用方与 suspended
        **同一次落盘**写下——凭据与状态原子同写，才不会出现「状态已挂起、
        凭据没记」的不可补偿窗口。

        返回 True = 本次有需要派发的任务（凭据已记）；False = 非服务节点挂起，
        不需要派发，也不留凭据（避免每挂起一次都多一次无意义的二写）。
        """
        service_config, subtype = self._resolve_service_task(result, context)
        if service_config is None:
            state.dispatch_pending = None
            return False
        state.dispatch_pending = {
            "pending": True,
            "service_type": subtype,
            "service_config": service_config,
            "node_id": (context or {}).get("$LAST_NODE"),
            "recorded_at": datetime.now().isoformat(),
        }
        return True

    def _clear_suspend_dispatch_pending(
        self, execution_id: str, state: ExecutionState, *, source: str
    ) -> None:
        """派发成功后清凭据（plaita#233）：投递完成与「已派发」标记同一收口。

        清不清得掉决定**幂等性**：清掉后同一载体的再次重投不会再补派第二条；
        清失败（Redis 瞬断）必须上抛不 ack——吞掉会让标记永久留在盘上，
        下一次任何 resume/start 都会再补派一次。残留窗口（rpush 已成功、
        这次清标记的落盘失败）里重投**可能补派重复任务**，由服务侧
        幂等/租约语义吸收，代价远小于「永久 suspended」。
        """
        if getattr(state, "dispatch_pending", None) is None:
            return
        state.dispatch_pending = None
        try:
            self._persist_state_or_raise(
                execution_id, state, f"suspend_dispatched_{source}"
            )
        except StatePersistError:
            logger.error(
                "执行 %s 挂起派发已成功但清除派发凭据失败（%s）——消息将不 ack 重投，"
                "重投可能**重复派发一次**该外延任务（服务侧需幂等）",
                execution_id, source,
            )
            raise

    def _compensate_suspend_dispatch(
        self, state: ExecutionState, execution_id: str, *, source: str,
        lease_held: bool = False,
    ) -> bool:
        """挂起补偿派发（plaita#233）：把落盘凭据里的服务任务幂等重投一次。

        触发面（三条载体共用本函数）：``resume_flow`` 的 suspended 幂等短路
        （continue/retry，两处调用点：租约前 / 租约内复核）、start 载体
        （非 dedup 走 resume 委托 / dedup 命中分支）。**不重跑挂起节点**
        （#33 契约不变），只补「落盘→派发」窗口丢掉的投递。

        ``lease_held``（plaita#243）：调用方**已持有**该执行租约（#241 的
        租约内复核分支）。此时直接复用外层所有权，不再 ``_acquire_lease``
        ——同一 worker 重入 acquire 必然失败（自撞），补派会被跳过，随后
        消息 ack、``dispatch_pending`` 执行永久 suspended。独立载体
        （start dedup / 租约前短路）保持 ``lease_held=False``，仍走短期
        租约 + 持锁重读。

        返回 True = 本次确实补派了；False = 无凭据 / 已被他人推进 / 持有者
        活跃（跳过，由持有者推进并重新派发）。

        为什么独立载体要**短暂持执行租约 + 持锁后重读**（而不是直接拿入口
        加载的快照去写）：

        - 补派成功后要「清凭据 = 再写一次状态行」，无租约的写是 last-writer-
          wins——若此刻另一 worker 正持租约把该执行推向终态，这次写会把旧
          ``suspended`` 快照**倒灌回盘**（唤醒一个已终态的执行）；
        - 持锁重读顺带消掉「并发重复载体各补派一条」的竞态：只有租约持有者
          能补派，另一个直接跳过。租约只覆盖「补派 + 清凭据」这一小段（毫秒
          级），不改变 #33 短路「不重跑、不推进」的语义（无凭据时根本不取租约
          ——见 ``test_issue33_suspended_continue_guard`` 的零租约断言）。
        - 租约被活跃持有者占着 ⇒ 跳过而不是抛：对方要么正在推进（会在自己的
          挂起分支重新派发），要么其载体仍留在 PEL 会被重投，凭据不会被遗忘。
        """
        if self._pending_suspend_dispatch(state) is None:
            return False
        if lease_held:
            # plaita#243：外层 resume 的租约就是所有权凭据，直接投递；
            # 仍重读盘上状态，避免拿复核前的旧快照覆写。
            return self._dispatch_suspend_pending(execution_id, source=source)
        holder = new_holder_token(
            prefix="dispatch", instance_id=getattr(self, "instance_id", None)
        )
        lease_value, fence_token = self._acquire_lease(execution_id, holder)
        if lease_value is None:
            logger.warning(
                "执行 %s 挂起补派跳过（source=%s）：执行租约被另一 worker 持有，"
                "由其推进/重新派发", execution_id, source,
            )
            return False
        fence_token_reset = None
        if fence_token is not None:
            fence_token_reset = set_current_fence_token(fence_token)
        try:
            return self._dispatch_suspend_pending(execution_id, source=source)
        finally:
            if fence_token_reset is not None:
                reset_current_fence_token(fence_token_reset)
            self.execution_lease.release(execution_id, lease_value)

    def _dispatch_suspend_pending(self, execution_id: str, *, source: str) -> bool:
        """持租约（自身取得或外层已持）时重读盘上状态并补派一次。

        调用方负责租约生命周期；本函数只做「重读 → 校验仍 suspended 且有
        凭据 → 投递 → 清凭据」。投递失败上抛（消息不 ack 可重投）；清凭据
        失败同样上抛（由服务侧幂等吸收重复投递）。
        """
        # 持锁后重读：入口快照可能已被他人推进/终态化，绝不能拿旧快照覆写。
        fresh = self.execution_storage.load_execution_state(execution_id)
        marker = (
            self._pending_suspend_dispatch(fresh) if fresh is not None else None
        )
        if marker is None or (getattr(fresh, "status", "") or "") != "suspended":
            logger.info(
                "执行 %s 挂起补派跳过（source=%s）：盘上已无待派凭据或状态已变"
                "（status=%r）", execution_id, source,
                getattr(fresh, "status", None) if fresh is not None else None,
            )
            return False
        service_config = marker.get("service_config")
        service_config = (
            dict(service_config) if isinstance(service_config, dict) else {}
        )
        subtype = str(marker.get("service_type") or service_config.get("type") or "")
        logger.warning(
            "执行 %s 挂起但派发凭据未清（source=%s, service=%s, node=%s）："
            "补派一次外延服务任务",
            execution_id, source, subtype or "?", marker.get("node_id"),
        )
        self._dispatch_service_task(
            {"service_config": service_config}, fresh.context or {}, execution_id
        )
        self._clear_suspend_dispatch_pending(execution_id, fresh, source=source)
        return True

    def _dispatch_service_task(
        self, result: Dict[str, Any], context: Dict[str, Any], execution_id: str
    ) -> None:
        """挂起时把扩展节点的 service_config 投递给对应外延服务。

        历史上挂起只落执行状态，service_config 无人消费——
        delay/approval 等节点的任务永远不会被服务接走，执行会永久挂起。
        service_config 通常埋在 ``context.$NODE.{最后节点}.service_config``，
        顶层偶有直接携带；任务按 subtype 投递到 ``plaita:{subtype}:queue``，
        由对应外延服务消费（DelayService 等）。
        无 redis 客户端（单测/纯内存派生类）时跳过投递。
        """
        service_config, subtype = self._resolve_service_task(result, context)
        if service_config is None:
            return
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "rpush"):
            logger.warning(
                "挂起任务投递跳过（无 redis 客户端）: %s (execution_id=%s)",
                subtype, execution_id,
            )
            return
        queue_key = f"plaita:{subtype}:queue"
        try:
            task = dict(service_config)
            task.setdefault("execution_id", execution_id)
            # 挂起服务消费后要 resume 原执行——透传租户，服务侧据此路由
            task.setdefault("tenant_id", current_tenant())
            redis_client.rpush(queue_key, json.dumps(task, ensure_ascii=False))
            logger.info(
                "挂起任务已投递: %s → %s (execution_id=%s)", subtype, queue_key, execution_id
            )
        except Exception as e:
            # 有 redis 客户端时投递失败必须上抛（任务②）：此刻执行已落盘为
            # suspended，吞掉异常会让消息被 ack——delay/approval 任务永无人
            # 接，执行永久挂起。抛出后消息不 ack 走重投，下轮 resume 重新
            # 执行挂起节点再派发；suspended 状态经 resume_flow 的定向放行
            # 保留（不翻 error）。代价：重投会重复注册订阅，EventFilter 的
            # 终态 GC 只回收终态，孤儿订阅留到 TTL 过期——可接受。
            # ServiceDispatchError 非 ValueError 子类，不会被 run() 毒丸 ack。
            logger.error(
                "挂起任务投递失败: %s → %s: %s", subtype, queue_key, _scrub_exception_text(e)
            )
            raise ServiceDispatchError(
                f"挂起任务投递失败: {subtype} → {queue_key} "
                f"(execution_id={execution_id}): {e}"
            ) from e

class RedisFlowWorker(RegistryMixin, ControlMixin, FlowWorker):
    """
    基于 Redis Stream 队列的流程工作器（服务注册 / 心跳 / 远程控制）。

    任务队列语义为 **at-least-once**：consumer group + ``XACK``；处理成功前
    崩溃则 pending 可被其他 consumer 在 ``claim_min_idle_ms`` 后回收重投。
    resume 通过 Redis execution lease 保证同一 ``execution_id`` 最多一个 worker 推进。
    控制面硬依赖 Redis。
    """

    SERVICE_TYPE = "flow_worker"
    
    def __init__(
        self,
        redis_url: str,
        queue_name: str,
        execution_storage: ExecutionStorage,
        flow_storage: FlowStorage,
        event_bus: EventBus = None,
        cache_size: int = 100,
        cache_ttl: int = 300,
        enable_registry: bool = True,
        registry_ttl: int = ServiceRegistry.DEFAULT_TTL,
        heartbeat_interval: int = ServiceRegistry.DEFAULT_HEARTBEAT_INTERVAL,
        enable_redis_logging: bool = True,
        redis_client: Redis = None,
        callback_handlers=None,
        consumer_group: str = DEFAULT_CONSUMER_GROUP,
        consumer_name: Optional[str] = None,
        claim_min_idle_ms: int = DEFAULT_CLAIM_MIN_IDLE_MS,
        lease_ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
        execution_lease: Optional[ExecutionLease] = None,
        max_deliveries: int = DEFAULT_MAX_DELIVERIES,
        dlq_key: Optional[str] = None,
        read_block_ms: int = 1_000,
        concurrency: int = 1,
        min_free_disk_gib: Optional[float] = None,
        disk_guard_path: Optional[str] = None,
        disk_guard_poll_seconds: float = DEFAULT_DISK_GUARD_POLL_SECONDS,
        watchdog_interval_seconds: Optional[float] = None,
        cancel_poll_seconds: Optional[float] = None,
        residue_sweep_interval_seconds: Optional[float] = None,
        metrics_port: Optional[int] = None,
        metrics_host: Optional[str] = None,
        alert_webhook: Optional[str] = None,
    ):
        # b94-02：预算哈希必须与实际生效值一致——构造参数是 worker 真正
        # acquire/renew 的 TTL，先按与 overrides 相同的口径钳制，避免
        # lease_ttl_seconds<=0 时「实际 0 / 哈希 1」的再次分叉。
        if int(lease_ttl_seconds) < 1:
            logger.warning(
                "lease_ttl_seconds=%s ≤0，钳制为 1（启动预检会拒绝该配置，B2R31-03）",
                lease_ttl_seconds,
            )
        lease_ttl_seconds = max(1, int(lease_ttl_seconds))
        # A2R20-05：队列/租约共用客户端——connect 5s / read 30s 显式声明
        # （read 必须大于 XREADGROUP BLOCK 窗口，见 task_queue）。
        if redis_client is None:
            redis_client = make_redis_client(redis_url)
            pool_kwargs = redis_client.connection_pool.connection_kwargs
            logger.info(
                "Redis 超时预算: connect=%ss read=%ss（PLAITA_REDIS_* 可覆盖）",
                pool_kwargs.get("socket_connect_timeout"),
                pool_kwargs.get("socket_timeout"),
            )
        super().__init__(
            execution_storage,
            flow_storage,
            event_bus,
            cache_size,
            cache_ttl,
            callback_handlers=callback_handlers,
            execution_lease=execution_lease or TenantRoutingExecutionLease(redis_client),
            lease_ttl_seconds=lease_ttl_seconds,
        )
        self.redis_url = redis_url
        self.redis_client = redis_client
        self.queue_name = queue_name
        self._running = False
        # 消费阻塞窗口上限（毫秒）——见 run() 主循环内的分片说明
        self.read_block_ms = max(100, int(read_block_ms))
        # 并发消费线程数（2026-10-07）：>1 时 run() 起 N 个消费线程共享本实例，
        # 单机可同时处理 N 个任务（原为严格串行——一次 read 一条、处理完再读）。
        # 每线程各自持有一个独立 queue 对象（独立 Redis 连接），避免连接竞争；
        # 消息级 at-least-once 与执行级租约语义不变（并发下同一 execution 仍由
        # 租约串行化）。默认 1 = 零行为变化。
        self.concurrency = max(1, int(concurrency))
        # claim 前本机盘预检（#49）：多 worker 池下，低盘机器的 flow preflight
        # 要等「领到手」才判盘不足 → retry-later 白跑一轮（实测一夜 11 次），
        # 且把瞬时低盘放大成数小时退避停机；健康 worker 全程闲置。阈值 >0 时，
        # 本机可用盘低于它就不 XREADGROUP（空转按 disk_guard_poll_seconds
        # 重探，盘回线自动恢复，无需重启）。默认 0 = 关闭 = 零行为变化。
        self._min_free_disk_gib = (
            _min_free_disk_gib_from_env()
            if min_free_disk_gib is None
            else max(0.0, float(min_free_disk_gib))
        )
        self._disk_guard_path = disk_guard_path or _disk_guard_path_from_env()
        self._disk_guard_poll_seconds = max(0.1, float(disk_guard_poll_seconds))
        self._disk_guard_active = False
        self._active_task_count = 0
        self._active_count_lock = threading.Lock()
        self._log_handler = None
        self._consumer_group = consumer_group
        self._claim_min_idle_ms = claim_min_idle_ms
        self._consumer_name = consumer_name
        if int(max_deliveries) < 1:
            logger.warning(
                "max_deliveries=%s ≤0，钳制为 1（启动预检会拒绝该配置，B2R31-03）",
                max_deliveries,
            )
        self._max_deliveries = max(1, int(max_deliveries))
        self._dlq_key = dlq_key
        self._task_queue: Optional[RedisStreamTaskQueue] = None

        # B2R31-01/02/03：本进程的配置视图与三维代际。库内构造用非 strict
        # （非法值 WARNING + 安全默认，不阻断嵌入式使用）；CLI 入口另走
        # preflight 严格校验。显式构造参数优先于 env。
        resolved_dlq = dlq_key
        if resolved_dlq is None:
            try:
                resolved_dlq = resolve_dlq_key(queue_name, empty_policy="default")
            except ConfigError as exc:
                logger.warning(
                    "DLQ 键解析失败，按默认键继续（配置预检会拒绝启动）: %s",
                    _scrub_exception_text(exc),
                )
                resolved_dlq = f"{queue_name}:dlq"
        self._runtime_config = load_runtime_config(
            "worker",
            strict=False,
            overrides={
                "queue_name": queue_name,
                "redis_url": redis_url,
                "consumer_group": consumer_group,
                "dlq_key": resolved_dlq,
                "max_deliveries": max(1, int(max_deliveries)),
                "lease_ttl_seconds": max(1, int(lease_ttl_seconds)),
                "task_max_age_seconds": task_max_age_seconds(),
                # 与队列构造实际采用的钳制值同源，避免代际哈希与实际预算分叉。
                "dlq_max_len": _dlq_max_len_from_env(),
            },
        )
        self._config_generation: Optional[ConfigGeneration] = (
            self._runtime_config.generation
        )
        if self._config_generation is not None:
            self._config_family = self._config_generation.family
            self._config_generation_id = self._config_generation.generation
            self._budget_generation = self._config_generation.budget
            self._config_effective_at = self._config_generation.effective_at
        else:  # pragma: no cover - build_generation always returns an object
            self._config_family = LEGACY_GENERATION
            self._config_generation_id = LEGACY_GENERATION
            self._budget_generation = LEGACY_GENERATION
            self._config_effective_at = None
        self._tenant_status_epoch = self._runtime_config.tenant_status_epoch
        self._dlq_key = resolved_dlq
        log_config_effective(logger, self._runtime_config)

        # #26 观测导出：``/metrics`` 抓取端（0 = 不启动）与死信告警 webhook。
        # 队列计数器（enqueued/acked/dead_lettered…）是**进程内**状态，只有
        # worker 自己持有——console 无法代它导出，故端点必须跑在本进程里。
        self._metrics_port = (
            _metrics_port_from_env() if metrics_port is None else max(0, int(metrics_port))
        )
        self._metrics_host = metrics_host or _metrics_host_from_env()
        self._metrics_server: Optional[MetricsHttpServer] = None
        self._alert_webhook = (
            alert_webhook if alert_webhook is not None else _alert_webhook_from_env()
        )
        self._alerter: Optional[WebhookAlerter] = None

        # 启动期注册重试（A2R19-03）：注册失败时后台线程按有界退避重试；
        # 成功前 ``_registry_registered`` 保持 False（状态/就绪侧不宣称 ready）。
        self._registry_registered = False
        self._registry_retry_thread: Optional[threading.Thread] = None
        self._registry_retry_stop = threading.Event()
        # S18-02A：生命周期状态即时落盘失败计数（/metrics 可观测量）。
        self._registry_status_flush_failures = 0
        # B108-02：start 载体命中已终态被具名放弃的累计数（prometheus counter）。
        self._dedup_discarded_total = 0

        # 租约看门狗（波次② §4.1）：登记活跃 (execution_id → 租约值串/
        # FlowExecution/租户)，每 watchdog_interval_seconds（默认 TTL/3）
        # renew 一次，保证存活 worker 的租约永不空窗——XCLAIM 只会捡到
        # 真死 worker 的消息，抢占双跑被根除，fencing 是其下第二道保险。
        self._watchdog_interval_seconds = watchdog_interval_seconds
        self._watchdog_thread: Optional[threading.Thread] = None
        self._watchdog_stop = threading.Event()
        self._lease_watch_lock = threading.Lock()
        # execution_id -> (lease_value, execution, tenant_id)
        self._lease_watch: Dict[str, tuple] = {}
        self._lease_lost: Set[str] = set()
        # 失租后仍在跑的执行：只发布进度心跳，不续租、不重复 cancel。
        # 存在的原因见 `_watchdog_renew_once`：租约被别的消息处理释放后，
        # 执行对象仍会在沙箱里正常推进（实测 6caf8289），若停止发布心跳，
        # keeper 会按「末节点停滞 >1800s」判死一个健康 run。
        self._lease_orphan_watch: Dict[str, tuple] = {}

        # 取消监听（波次③ §3.3 步内中断）：登记活跃 (execution_id →
        # FlowExecution/租户)，每 cancel_poll_seconds（默认 1s）轮询取消标志键，
        # 命中即 execution.cancel() 中止在途节点。登记表/停止位已在基类建好，
        # 此处只接轮询间隔旋钮（单测可缩短）。
        if cancel_poll_seconds is not None:
            self._cancel_poll_seconds = cancel_poll_seconds

        # 队列残留回收（#43）：启动时扫一次 + 每 residue_sweep_interval_seconds
        # 兜底一次，清掉「已 ack 未 XDEL」的 Stream 残留条目（它们只增不减地
        # 计入 XLEN，让运维把队列读数读成假阳性）。非阻塞锁保证并发消费下
        # 同一时刻只有一个扫描在跑（幂等，重复扫描只是浪费一次往返）。
        self._residue_sweep_interval_seconds = (
            DEFAULT_RESIDUE_SWEEP_INTERVAL_SECONDS
            if residue_sweep_interval_seconds is None
            else float(residue_sweep_interval_seconds)
        )
        self._last_residue_sweep: Optional[float] = None
        self._residue_sweep_lock = threading.Lock()
        
        # 服务注册
        self._enable_registry = enable_registry
        if self._enable_registry:
            self.init_registry(
                redis_client=self.redis_client,
                service_type=self.SERVICE_TYPE,
                metadata={
                    "queue_name": queue_name,
                    # B75-01 §6：队列声明的契约版本。派发侧据此区分「新
                    # worker 明确声明消费队列」与 pre-#158/自定义 worker 的
                    # 缺失 metadata（unknown ≠ default）。
                    "queue_contract_version": 2,
                    "consumer_group": self._consumer_group,
                    "dlq_key": self._dlq_key,
                    # B2R31-01：代际与脱敏关键值进注册 metadata——控制面
                    # （/api/cluster/instances、capacity、drift 预检）据此
                    # 发现混跑；旧 build 忽略未知键。
                    "config_family": self._config_family,
                    "config_generation": self._config_generation_id,
                    "budget_generation": self._budget_generation,
                    "config_effective_at": self._config_effective_at,
                    "max_deliveries": self._max_deliveries,
                    "lease_ttl_seconds": lease_ttl_seconds,
                    "task_max_age_seconds": self._runtime_config.task_max_age_seconds,
                    "dlq_max_len": self._runtime_config.dlq_max_len,
                    "deployment_epoch": self._runtime_config.deployment_epoch,
                    "tenant_status_epoch": self._tenant_status_epoch,
                    # plaita#300：worker 能力清单（engine/IR 契约代际/节点白名单）。
                    # supported 由进程默认 NodeRegistry **自动生成**（本构造点之前
                    # 已完成 PLAITA_NODE_MODULES / CodeNode 注册，清单如实反映本
                    # 进程可执行集合）；console 读面透出、publish/start 能力门消费。
                    "capability_manifest": python_worker_manifest(),
                    # plaita#158：本字典会被 registry.register() 原样 ``SETEX`` **持久化**
                    # 进 ``plaita:registry:flow_worker:<instance_id>``——注册键名与
                    # metadata 键语义不变（仍是 ``redis_url``），只有它的**值**是掩码
                    # 形态，另加 host/port/db 三件套保住运维诊断价值。
                    **_redis_registry_metadata(redis_url),
                    "cache_size": cache_size,
                    "cache_ttl": cache_ttl,
                },
                ttl=registry_ttl,
                heartbeat_interval=heartbeat_interval,
                instance_id=self._runtime_config.instance_id,
            )
            
            # 初始化控制监听
            self.init_control(
                redis_client=self.redis_client,
                instance_id=self._service_info.instance_id if self._service_info else "unknown"
            )

            # #42（W10-a）：把容量快照写进 metadata（并立即落一次注册键）。
            # 放在 init_control 之后：`_service_info` 已建好；此时 `_registry`
            # 尚未 start_heartbeat，直推一次让派发侧**首个心跳前**就能看到容量
            # （registry.register() 在 init_registry 内还没执行，本次推送顺带
            # 完成注册键写入——幂等，register() 之后再覆盖一次同样的值）。
            self._report_capacity()
        
        # Redis 日志处理器
        self._enable_redis_logging = enable_redis_logging
        if self._enable_redis_logging and self._service_info:
            self._log_handler = setup_redis_logging(
                redis_client=self.redis_client,
                service_type=self.SERVICE_TYPE,
                instance_id=self._service_info.instance_id,
                logger_name="plaita",
                level=logging.INFO
            )

    def _resolve_consumer_name(self) -> str:
        if self._consumer_name:
            return self._consumer_name
        if self._enable_registry and getattr(self, "_service_info", None):
            return self._service_info.instance_id
        return f"worker-{os.getpid()}"

    def _get_task_queue(self) -> RedisStreamTaskQueue:
        if self._task_queue is None:
            kwargs: Dict[str, Any] = dict(
                group_name=self._consumer_group,
                consumer_name=self._resolve_consumer_name(),
                claim_min_idle_ms=self._claim_min_idle_ms,
                max_deliveries=self._max_deliveries,
                dlq_key=self._dlq_key,
            )
            queue_params = inspect.signature(
                RedisStreamTaskQueue.__init__
            ).parameters
            # B2R31-01：本进程代际进队列；family 不匹配的载体不执行、
            # 不 XCLAIM（留 pending 等匹配代际的 worker）。按签名探测，
            # 兼容只实现旧参数的队列桩/并行合入版本。
            for key, value in (
                ("config_family", self._config_family),
                ("config_generation", self._config_generation_id),
                ("budget_generation", self._budget_generation),
                ("config_effective_at", self._config_effective_at),
                ("task_max_age_seconds", self._runtime_config.task_max_age_seconds),
            ):
                if key in queue_params:
                    kwargs[key] = value
            # 死信守卫接线（Track B 契约）：队列类已支持 ``dead_letter_guard``
            # 参数才传入（并行合入期向后兼容——旧类不收该参数，TypeError 会
            # 炸掉 _get_task_queue 的全部调用方）。Track B 合入前守卫不生效，
            # 合入后由集成联测覆盖。
            if (
                "dead_letter_guard"
                in inspect.signature(RedisStreamTaskQueue.__init__).parameters
            ):
                kwargs["dead_letter_guard"] = self._dead_letter_guard
            # #26 死信告警接线：同样按签名探测向后兼容（并行合入期的旧队列类
            # 不收该参数）。
            if (
                "on_dead_letter"
                in inspect.signature(RedisStreamTaskQueue.__init__).parameters
            ):
                kwargs["on_dead_letter"] = self._on_dead_letter
            self._task_queue = RedisStreamTaskQueue(self.redis_client, self.queue_name, **kwargs)
        return self._task_queue

    def _on_dead_letter(self, event: Dict[str, Any]) -> None:
        """死信事件出口：未配置 webhook 时空操作（只保留原始 error 日志）。"""
        if self._alerter is not None:
            self._alerter.send(event)

    def _dead_letter_guard(self, task: StreamTask) -> bool:
        """死信守卫（Track B 契约）：True=允许死信；False/抛异常=跳过。

        判据是**执行状态**而非仅租约（合并评审修正）：delivery_count 被
        XCLAIM 抢占虚增后不可逆——若只看「租约在」，持有者崩溃后租约过期，
        守卫放行 → 消息立即死信，非终态执行的唯一恢复路径被切断；若只返回
        False 拒绝，超限消息在队列回收循环里只会被反复 XCLAIM/跳过、**永远
        不会再被派发**，同样救不回执行。因此：

        - 执行状态缺失或已终态（completed/error/cancelled）→ 放行死信
          （终态消息本就会被 already_terminal 短路 ack，死信记录更可查）；
        - 状态加载失败（Redis 瞬断/损坏）→ 保守跳过死信；
        - 非终态 + 租约在 → 有活 worker 正持有（长步骤处理中）→ 跳过死信；
        - 非终态 + 租约空（持有者已死）→ **重新入队一份同体消息**（delivery
          归 1，恢复路径重生）后放行原消息死信。并发守卫双入队无害：两条
          新消息被 resume 租约串行化，第二条命中 already_terminal 短路。
        - 例外（A49-01）：消息因信封版本超出本 build 而被拒收
          （``task.schema_rejected``）→ **不克隆**，直接放行死信一次。副本会
          继承同一个超版本，本 build 仍然无法消费：每轮克隆+死信是自持热环
          （流内恒 1 条、DLQ 线性增长）。租约仍在时仍拒绝死信（留 pending
          等新 build）。

        租户路由：状态存储经 ContextVar 路由、租约键手工拼装，两者都用
        消息体 ``tenant_id``（守卫在 run() 主循环内 ``_dispatch_task`` 之外
        被调用，ContextVar 已复位）。键格式与 ``TenantRoutingExecutionLease``
        同规则：``{ns}:execution:lease:{id}``，ns = ``plaita``（default/空
        租户）或 ``plaita:{tenant_id}``。
        """
        body = getattr(task, "body", None)
        if not isinstance(body, dict):
            return True
        execution_id = body.get("execution_id")
        if not execution_id:
            # 消息体没有 execution_id ⇒ 无执行可保护（放行死信）。
            # 更正（2026-10-11）：此处旧注释写「start 任务入队时还没有
            # execution_id」**与生产现实不符**——console 在**提交时就铸造**
            # execution_id 并随消息透传（`executions.py`），实测生产在途消息
            # 的 body 全部带 execution_id。该陈旧注释曾被误引为「start 消息
            # 无 execution_id ⇒ 按执行维度计数必然恒为 1」的错误依据；实际上
            # 新副本**原样重投同一 body**（`enqueue_task(..., dict(body))`），
            # execution_id 不变，是可用的稳定计数维度。此处保留早退仅作为
            # 「无 id 的老消息/畸形消息」的兜底。
            return True

        token = set_current_tenant(body.get("tenant_id"))
        try:
            try:
                state = self.execution_storage.load_execution_state(execution_id)
            except Exception as exc:  # noqa: BLE001 — 瞬断保守跳过，不误杀活执行
                logger.warning(
                    "死信守卫读执行状态失败（保守跳过死信）: %s: %s", execution_id, _scrub_url_text(exc)
                )
                return False
            retry_count = self._max_node_retry_counter(execution_id)
        finally:
            reset_current_tenant(token)

        if state is None:
            return True
        if getattr(state, "status", "") in TERMINAL_EXECUTION_STATUSES:
            return True

        namespace = tenant_namespace(body.get("tenant_id"))
        lease_key = f"{namespace}:execution:lease:{execution_id}"
        try:
            lease_held = bool(self.redis_client.exists(lease_key))
        except Exception as exc:  # noqa: BLE001 — 瞬断保守跳过，不误杀活执行
            logger.warning(
                "死信守卫查询租约失败（保守跳过死信）: %s: %s", lease_key, _scrub_url_text(exc)
            )
            return False
        if lease_held:
            logger.warning(
                "死信跳过：执行 %s 租约仍在（活 worker 处理中），消息留 pending: %s",
                execution_id,
                getattr(task, "message_id", "?"),
            )
            return False

        # A51-01：状态快照可能是删除前读的（守卫窗口：读 state → 删除 →
        # 探测租约），墓碑必须在租约探测之后、终态化/克隆之前再判一次。
        # 读取瞬态错误保守跳过死信（消息留 pending），绝不 fail-open 克隆。
        try:
            tombstone = _load_matching_tombstone(
                self.execution_storage, execution_id, body.get("tenant_id")
            )
        except Exception as exc:  # noqa: BLE001 — 瞬断保守跳过，不误杀活执行
            logger.warning(
                "死信守卫读执行墓碑失败（保守跳过死信）: %s: %s",
                execution_id, _scrub_url_text(exc),
            )
            return False

        # A51-01：执行已删除（墓碑）。原消息放行死信留证，但**不克隆**
        # 恢复副本——克隆 body 仍是 start/同 id，会在删除后重建执行；也
        # 不终态化（执行已不存在）。比 state 快照更权威：守卫窗口里 state
        # 是删除前读的 running。
        if tombstone is not None:
            logger.warning(
                "执行 %s 已删除（墓碑 tenant=%s reason=%s），死信守卫只放行 DLQ、"
                "不克隆恢复副本: %s",
                execution_id,
                tombstone.get("tenant"),
                tombstone.get("reason"),
                getattr(task, "message_id", "?"),
            )
            return True

        # A49-01：超版本拒收不走「克隆恢复」。副本继承同一超版本，本 build
        # 仍会在下一轮拒收并再次克隆——流内恒 1 条、DLQ 每轮 +1，永不收敛。
        # 允许死信一次（留证+告警），执行状态不终态化：suspended 仍可被后续
        # event/cancel/timeout 唤醒；运维升级后可从 DLQ 重放（ops-runbook）。
        # 也刻意跳过下方「节点重试预算耗尽 → 终态化」分支：版本拒收不是节点
        # 反复失败，不能据此把执行打成 error。
        if getattr(task, "schema_rejected", False) or getattr(
            task, "config_rejected", False
        ):
            logger.error(
                "执行 %s 的消息信封版本/配置代际（schema=%s, family=%s, budget=%s）"
                "与本进程不兼容，拒收后不再克隆恢复副本（克隆继承同代际，会自持"
                "循环）；放行死信一次: %s",
                execution_id,
                getattr(task, "schema_version", "?"),
                getattr(task, "config_family", "?"),
                getattr(task, "budget_generation", "?"),
                getattr(task, "message_id", "?"),
            )
            return True

        # 节点重试预算耗尽后备判据（波次二任务①）：达限消息在队列层被就地
        # 死信、不经过处理函数；此刻执行非终态、租约已空且重试计数已达预算
        # ——说明这是节点反复失败的搁浅（含「预算耗尽判定后、终态化写盘前
        # worker 崩溃」窗口），终态化 error 后放行死信。**不**重入队：重入队
        # delivery 归 1 → 再耗尽 → 再重入队 = 无限循环。终态化写盘失败 →
        # 保守跳过死信（消息留 pending，下轮再处置）。
        # 计数取**该执行下所有节点键的最大值**（plaita#123 起键按节点隔离，
        # 守卫看不到「是哪个节点」——任一节点达预算即该执行搁浅，与守卫原
        # 意图一致）。
        # 兜底总上限（#123 评审建议）也在此生效：键空间漂移（节点 id 变化等）
        # 会让单节点预算失效，此时靠「不看键名、只看次数」的总计数兜住——
        # 守卫是消息层的最后一道闸，不能只有会失效的那一条判据。
        # plaita#242：终态化**只允许 status == running**（真正卡住的推进态）。
        # suspended 在等外部事件/审批/delay/超时，重试计数达限不代表搁浅——
        # 按 error 终态化会永久封死 resume(event/cancel/timeout) 的唤醒路径。
        # 非 running 一律保持原状、走下方重入队恢复路径（消息不丢）。
        # plaita#57：终态化分支（兜底计数读 + 置 error + persist）必须整体在
        # 租户上下文内——原实现在 reset_current_tenant 之后，非 default 租户的
        # state 被写进 default 命名空间、兜底计数也读不到自己的键（读写同病）。
        token = set_current_tenant(body.get("tenant_id"))
        try:
            total_exhausted = self._node_retry_total_exhausted(execution_id)
            if (
                getattr(state, "status", "") == "running"
                and (retry_count >= self._node_retry_budget() or total_exhausted)
            ):
                logger.error(
                    "执行 %s 非终态但节点重试计数已达预算（%s/%s）%s，终态化 error "
                    "后放行死信，不再重入队: %s",
                    execution_id, retry_count, self._node_retry_budget(),
                    "或已达兜底总上限" if total_exhausted else "",
                    getattr(task, "message_id", "?"),
                )
                state.status = "error"
                state.error = {
                    "message": (
                        f"节点执行失败：重试预算耗尽（{retry_count} 次），"
                        "执行无法继续推进"
                    ),
                    "node_retries": retry_count,
                    # plaita#93：守卫只能看到计数键（#123 起键按节点隔离，
                    # 守卫看不到「是哪个节点」、更看不到原始异常），无法抄
                    # 节点业务码——用引擎的节点 abort 默认码标注「这是节点
                    # 反复失败」的类别，具体判据写进 error_type。
                    "code": DEFAULT_NODE_ABORT_CODE,
                    "error_type": "node_retry_budget_exhausted",
                }
                state.end_time = datetime.now().isoformat()
                try:
                    self._persist_state_or_raise(
                        execution_id, state, "dlq_guard_node_retry_exhausted"
                    )
                except Exception as exc:  # noqa: BLE001 — 写盘失败不丢消息
                    logger.warning(
                        "死信守卫终态化失败（保守跳过死信）: %s: %s", execution_id, _scrub_url_text(exc)
                    )
                    return False
                return True
        finally:
            reset_current_tenant(token)

        # A125-01：租户写闸关闭（停用墓碑/世代变更/legacy×已发布世代）时，
        # 恢复副本注定过不了派发闸（tenant_is_active / #283 世代闸），克隆只
        # 会形成「每轮泵 1 条垃圾副本」的泵循环（DLQ 写闸同闭 ⇒ 原消息留
        # pending ⇒ 下轮再泵）。不克隆，放行死信——DLQ 写闸虽同样拒绝，队列
        # 层会收容到租户隔离流并 ack 原消息（A125 终局出口）；执行态保持不
        # 动，租户恢复后经 console resume / 隔离流按源世代重放可救。
        # #312：关闭分两种性质——确认态照上段放行收容；**读失败型关闭**
        # （status/generation hget 瞬断，「不知道租户状态」）不得放行收容：
        # 那会把健康租户的可救载体终局化进 console 不可见的隔离流。改走本
        # 函数既有纪律（执行状态读失败 :5276 同款）：保守跳过死信，消息留
        # pending，瞬断自愈后下一轮按真实状态处置。
        gate_allowed, gate_reason = tenant_write_allowed(
            self.redis_client,
            body.get("tenant_id"),
            getattr(task, "tenant_generation", None),
        )
        if not gate_allowed:
            if is_epistemic_reject_reason(gate_reason):
                logger.warning(
                    "死信守卫：租户 %s 写闸读失败（%s，认识论性拒绝）——保守跳过"
                    "死信（不克隆不收容），消息留 pending 等重投: %s",
                    body.get("tenant_id") or "default",
                    gate_reason,
                    getattr(task, "message_id", "?"),
                )
                return False
            logger.warning(
                "死信守卫：租户 %s 写闸关闭（%s），不克隆恢复副本（派发闸必丢），"
                "放行死信交队列层收容终局: %s",
                body.get("tenant_id") or "default",
                gate_reason,
                getattr(task, "message_id", "?"),
            )
            return True

        try:
            # A44-02：恢复副本继承源信封版本（混跑池里旧 worker 仍可接管）。
            # A69-04：副本保留源年龄（缺失时由源 Stream ID 派生）——老载体
            # 不能因为一次死信守卫克隆就变成「刚入队」。
            enqueue_task(
                self.redis_client,
                self.queue_name,
                _copy_carrier_body(body, getattr(task, "message_id", None)),
                **_requeue_envelope_kwargs(task),
            )
            logger.warning(
                "执行 %s 非终态且租约已空（持有者已死），原消息 delivery 已超限："
                "已重新入队恢复路径，放行原消息死信: %s",
                execution_id,
                getattr(task, "message_id", "?"),
            )
        except Exception as exc:  # noqa: BLE001 — 重入队失败则不能丢原消息
            logger.warning(
                "死信守卫重新入队失败（保守跳过死信）: %s: %s", execution_id, _scrub_url_text(exc)
            )
            return False
        return True

    # ---- 租约看门狗（波次② §4.1）----

    def _register_lease_watch(
        self,
        execution_id: str,
        lease_value: str,
        execution: Any,
        fence_token: Optional[int] = None,
    ) -> None:
        """登记活跃执行：看门狗据此续租；失租鸭子调 execution.cancel()。

        ``fence_token``/租户一并登记：心跳（``_publish_node_progress``）在
        **看门狗线程**里跑，那里既没有本执行的 fence 世代 ContextVar 也没有
        租户上下文——不带上就会退化成「默认租户 + 无 CAS 的裸写」（跨进程写错
        namespace，且绕开 fenced 世代门）。
        """
        with self._lease_watch_lock:
            # 同一执行的新租约（重投 resume 重入）清除陈旧失租标记
            self._lease_lost.discard(execution_id)
            self._lease_watch[execution_id] = (
                lease_value,
                execution,
                current_tenant(),
                fence_token,
            )

    def _unregister_lease_watch(self, execution_id: str, lease_value: str) -> None:
        with self._lease_watch_lock:
            entry = self._lease_watch.get(execution_id)
            # 只撤自己的登记（防误撤后来者的新世代租约）
            if entry is not None and entry[0] == lease_value:
                self._lease_watch.pop(execution_id, None)
            self._lease_lost.discard(execution_id)

    def _raise_if_lease_lost(self, execution_id: Optional[str]) -> None:
        if not execution_id:
            return
        with self._lease_watch_lock:
            lost = execution_id in self._lease_lost
        if lost:
            raise ExecutionLeaseError(
                f"execution {execution_id} lease lost (watchdog flagged); aborting"
            )

    def _start_lease_watchdog(self) -> None:
        """启动看门狗线程（run() 调用）；PLAITA_DISABLE_LEASE_WATCHDOG=1 不启动。"""
        if _watchdog_disabled():
            logger.info("租约看门狗已禁用（PLAITA_DISABLE_LEASE_WATCHDOG=1）")
            return
        if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
            return
        self._watchdog_stop.clear()
        self._watchdog_thread = threading.Thread(
            target=self._lease_watchdog_loop,
            name="plaita-lease-watchdog",
            daemon=True,
        )
        self._watchdog_thread.start()

    def _stop_lease_watchdog(self) -> None:
        self._watchdog_stop.set()
        watchdog = self._watchdog_thread
        if (
            watchdog is not None
            and watchdog is not threading.current_thread()
            and watchdog.is_alive()
        ):
            watchdog.join(timeout=2.0)
        self._watchdog_thread = None

    def _watchdog_interval(self) -> float:
        if self._watchdog_interval_seconds is not None:
            return max(0.01, float(self._watchdog_interval_seconds))
        # 默认 TTL/3（设计稿 §4.1：120s TTL → 40s renew）
        return max(1.0, self.lease_ttl_seconds / 3.0)

    def _lease_watchdog_loop(self) -> None:
        interval = self._watchdog_interval()
        logger.info(
            "租约看门狗已启动（renew 间隔 %.2fs, ttl=%ss）", interval, self.lease_ttl_seconds
        )
        while not self._watchdog_stop.wait(interval):
            try:
                self._watchdog_renew_once()
            except Exception as exc:  # noqa: BLE001 — 看门狗自身绝不能带崩 worker
                logger.error("租约看门狗周期异常: %s", _scrub_exception_text(exc))

    def _publish_node_progress(
        self,
        execution_id: str,
        fence_token: Optional[int] = None,
        tenant_id: Optional[str] = None,
    ) -> None:
        """把**在跑节点**的进度发布到执行状态（长节点期间的活性心跳）。

        为什么需要（2026-10-10，plaita#27/#31/#55/#32/#62 五单误杀事故）：
        执行状态的落盘只发生在**节点边界**（``_persist_state_or_raise`` 的
        step_persist / 终态等路径）。而 ``sandbox_agent`` 节点（impl）是**单个
        长节点**——实测在 AGS 沙箱里跑 35 分钟，期间**一次落盘都不发生**：

        - ``runner._execute_node`` 在节点开跑**前**调 ``on_node_start``，
          于是采集器 ``_open['impl']`` 一直存在；
        - 但 ``snapshot()`` 只在 ``_collect_node_timings``（= 落盘收口）里被调；
        - ⇒ ``node_timings`` 永远停在进入 impl 前的最后一次落盘。

        keeper 的 ``console_exec.worker_alive`` 判据① 是「有 started_at、无
        ended_at 的节点 = 活证据」；它在长节点期间**读不到这个节点**，于是
        落到判据③「末节点 ended_at 停滞超 1800s」→ **把健康 run 判死 cancel**。
        铁证：产出落盘与误杀同一秒（#27 产 10:19:18 / 判 10:19:15）。

        这里复用看门狗周期（默认 TTL/3 ≈ 40s）做一次**合并 + 落盘**：
        不推进流程、不改 context，只把采集器的当前快照并进 ``node_timings``
        与 ``last_update_time``，让宿主侧「看得见沙箱里在跑」。

        与并发写者的关系（2026-10-10 评审修，**本函数是全局第二类写者**）：
        整行写回必须与推进路径的 ``_persist_state_or_raise`` 串行——同进程内持
        ``_state_write_lock``（读也在锁内：读在锁外则「读-改-写」窗口里插进终态
        写，本函数会把终态整行回滚成 running + 旧 checkpoint 的假活僵尸）；
        跨进程由 fencing 承接（``fence_token``/``tenant_id`` 由租约登记处带上，
        此前看门狗线程的 ContextVar 是 None → 退化**无 CAS** 的裸写、绕开世代门，
        且默认租户会写错 namespace）。另外任务在租约期内才登记在
        ``_lease_watch``，失租/终态后看门狗不再调本函数。

        失败**只告警不抛**：这是观测路径，绝不能因落盘失败而打断正在跑的
        节点（真正的落盘失败仍由 ``_persist_state_or_raise`` 在节点边界负责）。
        """
        timing = self._node_timings.get(execution_id)
        if timing is None:
            return
        tenant_scope = (
            set_current_tenant(tenant_id) if tenant_id is not None else None
        )
        fence_scope = (
            set_current_fence_token(fence_token) if fence_token is not None else None
        )
        try:
            with self._state_write_lock(execution_id):
                state = self.execution_storage.load_execution_state(execution_id)
                if state is None:
                    return
                if getattr(state, "status", "") in TERMINAL_EXECUTION_STATUSES:
                    return
                # 与 _collect_node_timings 同款合并语义：不抹掉别的进程写下的旧节点
                merged = dict(state.node_timings or {})
                merged.update(timing.snapshot())
                state.node_timings = merged
                state.last_update_time = datetime.now().isoformat()
                self.execution_storage.save_execution_state(execution_id, state)
        except ExecutionLeaseError:
            # fenced 世代不符 = 租约已被接管者夺走（本进程的心跳不再可信），
            # 不是观测路径的「落盘失败」：下周期 renew 会失败并走失租链路。
            logger.info(
                "在跑节点进度发布被 fencing 拒绝（租约已被接管，停止心跳）: %s",
                execution_id,
            )
        except Exception as exc:  # noqa: BLE001 — 观测路径不得影响执行
            logger.warning(
                "在跑节点进度发布失败（忽略，节点边界仍会落盘）: %s: %s",
                execution_id,
                _scrub_exception_text(exc),
            )
        finally:
            if fence_scope is not None:
                reset_current_fence_token(fence_scope)
            if tenant_scope is not None:
                reset_current_tenant(tenant_scope)

    def _watchdog_renew_once(self) -> None:
        """对全部活跃执行续租一轮；renew 失败（Lua compare 不符 = 已被他人
        持有/过期）→ 标记 lease_lost + 鸭子调 execution.cancel() 中止当前步
        （引擎 cancel() 为波次③实现，缺席时跳过——失租兜底是步界
        _renew_lease_if_held / persist 前失租检查抛 ExecutionLeaseError 且
        不写状态，消息不 ack）。Redis 瞬断（renew 抛异常）不判死，下周期重试。

        **进度发布与续租解耦**（2026-10-10 根治「为什么总靠抢救」）：
        此前发布写在 ``if renewed:`` 分支内 ⇒ **无租约 = 不发心跳**。而
        「无租约但仍在跑」正是生产常见形态——`#50` 的租约冲突 ack 会让
        重投载体消失、租约不再续，**但执行进程继续在沙箱里正常推进**。
        实测（exec 6caf8289）：17:49 被 ack 后仍于 18:45/18:47 正常完步，
        却因 17:49 起无租约 ⇒ 心跳停 ⇒ node_timings 永久停在 17:44:56
        ⇒ keeper 见「末节点停滞 >1800s」⇒ **判死一个正在干活的 run**，
        产出只能靠值守抢救。

        现在：**只要登记在册且租约未判失效，就发布进度**（发布本身是只读
        观测，不依赖租约；租约只用于竞争仲裁）。renew 失败的当轮仍会发布
        一次——在此之后该执行被摘出登记表（失租兜底路径不受影响）。
        """
        with self._lease_watch_lock:
            entries = list(self._lease_watch.items())
        for execution_id, (lease_value, execution, tenant_id, fence_token) in entries:
            token = set_current_tenant(tenant_id)
            try:
                renewed = self.execution_lease.renew(
                    execution_id, lease_value, self.lease_ttl_seconds
                )
            except Exception as exc:  # noqa: BLE001 — 瞬断不判死
                logger.warning(
                    "看门狗 renew 异常（下周期重试）: %s: %s", execution_id, _scrub_url_text(exc)
                )
                continue
            finally:
                reset_current_tenant(token)
            if renewed:
                # 租约确认在手 → 发布在跑节点进度（带租户与 fence 世代：
                # 看门狗线程没有本执行的上下文，不带会写错 namespace、
                # 绕开 fenced CAS）。
                self._publish_node_progress(
                    execution_id, fence_token=fence_token, tenant_id=tenant_id
                )
                continue
            # 失租：**先发布一次进度**（该执行可能仍在跑，见 docstring），
            # 再走既有失租兜底（标记 lease_lost + 请求中止 + 摘出登记表）。
            # 不发布会让 keeper 在「租约已丢但 impl 仍在推进」的长窗口里
            # 把健康 run 判成 zombie。
            self._publish_node_progress(
                execution_id, fence_token=fence_token, tenant_id=tenant_id
            )
            # 摘出登记表后本方法不再遍历它 ⇒ 后续轮次不再续租、**也不再发布**。
            # 但执行对象仍在跑（实测 6caf8289 失租后仍于 18:45/18:47 正常完步），
            # 若就此停止发布，keeper 仍会在 1800s 后判死一个健康 run。
            # 因此把它转交给**失租后的持续观测**集合：只发布进度、不续租、
            # 不重复 cancel，直到该执行离开活跃表（终态/被取消）。
            with self._lease_watch_lock:
                self._lease_lost.add(execution_id)
                self._lease_watch.pop(execution_id, None)
                self._lease_orphan_watch[execution_id] = (
                    execution, tenant_id, fence_token, 0
                )
            cancel = getattr(execution, "cancel", None)
            if callable(cancel):
                try:
                    cancel()
                except Exception as exc:  # noqa: BLE001 — cancel 失败不影响失租兜底
                    logger.warning(
                        "看门狗调 execution.cancel() 失败: %s: %s", execution_id, _scrub_exception_text(exc)
                    )
            else:
                logger.debug("执行对象无 cancel()（引擎侧波次③前），仅步界兜底")
            logger.error(
                "执行 %s 租约续期失败（已被他人持有/过期），已标记 lease_lost "
                "并请求中止当前步；消息将在步界以 ExecutionLeaseError 退出不 ack",
                execution_id,
            )
        self._publish_orphan_progress()

    def _execution_still_advancing(self, execution: Any) -> bool:
        """该执行对象是否仍在推进（= 释放租约前必须判定的活性）。

        用于 `finally` 里的条件释放：ack 释放路径会走到 finally，但执行
        对象可能仍在沙箱里跑长节点（见该处注释的生产实证）。判据取**保守
        方向**——只有能确证「已停止」才返回 False（允许 release）：

        - 执行对象缺失 / 无 `cancel_requested` 也无法判定 → 视为**在推进**
          （宁可多观测一轮，不可放走一个健康 run）；
        - `cancel_requested` 为真 → 已被要求停止，可释放；
        - 取不到任何信号时一律返回 True。

        注意：本判定**不查 Redis 状态**。状态可能滞后，而这里要回答的是
        「这个内存里的执行对象还会不会继续干活」，状态字段答不了。
        """
        if execution is None:
            return False
        try:
            if getattr(execution, "cancel_requested", False):
                return False
        except Exception:  # noqa: BLE001 — 判定不出就当仍在推进
            return True
        return True

    def _register_lease_orphan(
        self,
        execution_id: str,
        execution: Any,
        tenant_id: Optional[str] = None,
        fence_token: Optional[int] = None,
    ) -> None:
        """把「租约已释放但执行仍在跑」的执行转入持续观测集合。

        与 `_watchdog_renew_once` 失租分支同款语义（见 `_publish_orphan_progress`）：
        只发布进度心跳，不续租、不重复 cancel。存在的唯一理由是——
        释放租约不等于执行结束，停止发布心跳会让 keeper 把健康 run 判死。

        条目形状 ``(execution, tenant_id, fence_token, rounds)``：``rounds`` 是
        已发布心跳轮数，仅用于摘除时的泄漏观测日志（plaita#197）。
        """
        with self._lease_watch_lock:
            self._lease_orphan_watch[execution_id] = (
                execution, tenant_id, fence_token, 0
            )
            # 该执行已不再持有租约，但仍在跑：标记为失租态（步界
            # `_raise_if_lease_lost` 据此中止，不再继续推进）。
            self._lease_lost.add(execution_id)

    @staticmethod
    def _orphan_entry(entry: Any) -> tuple:
        """兼容三元/四元条目（旧调用方/旧测试直接写成三元组）。"""
        execution, tenant_id, fence_token = entry[0], entry[1], entry[2]
        rounds = entry[3] if len(entry) > 3 else 0
        return execution, tenant_id, fence_token, rounds

    def _orphan_execution_alive(self, execution_id: str, execution: Any) -> bool:
        """失租 orphan 是否仍在推进（plaita#197 的摘除判据）。

        历史判据是「不在 `_lease_watch` 且**不在 `_lease_lost`** 才摘除」，
        但 `_register_lease_orphan` 自己就往 `_lease_lost` 里写 ⇒ 条件恒假，
        条目只增不减、已结束的执行被永久续写 `last_update_time`（keeper 的
        stale 判据永远看不到停滞）。

        现在改为**对象/节点存活**判据（不再让 `_lease_lost` 自我续命）：

        - ``execution._running``：引擎 run 窗口内为真（``run_distributed``
          每步持有）；取不到该属性视为**未在跑**（``object()``/已回收对象）；
        - 本 worker 的节点耗时采集器仍有「已开始、未结束」的节点 ⇒ 长节点
          仍在跑（#123 的生产形态：租约已释放但 impl 仍在沙箱里推进）；
        - 取不到任何信号 ⇒ 判「已结束」，让观测集有界（#197 的规格）。
        """
        if execution is None:
            return False
        try:
            if bool(getattr(execution, "_running", False)):
                return True
        except Exception as exc:  # noqa: BLE001 — 判定失败按未知处理，不摘除
            logger.debug(
                "失租 orphan %s 的 _running 判定失败（保守保留）: %s",
                execution_id, _scrub_exception_text(exc),
            )
            return True
        timings = getattr(self, "_node_timings", None) or {}
        timing = timings.get(execution_id)
        if timing is None:
            return False
        try:
            return bool(timing.has_open_nodes())
        except Exception as exc:  # noqa: BLE001 — 同上
            logger.debug(
                "失租 orphan %s 的节点活性判定失败（保守保留）: %s",
                execution_id, _scrub_exception_text(exc),
            )
            return True

    def _drop_lease_orphan(self, execution_id: str, reason: str, rounds: int) -> None:
        """摘除 orphan 观测条目并清失租标记（plaita#197）。

        清 `_lease_lost` 是关键：历史实现只在「新租约登记」/「显式 unregister」
        时 discard，已结束的 orphan 两者都不会发生 ⇒ 失租集合也永久驻留。
        """
        with self._lease_watch_lock:
            self._lease_orphan_watch.pop(execution_id, None)
            self._lease_lost.discard(execution_id)
        logger.info(
            "失租 orphan 观测摘除: %s（%s，驻留 %d 轮）", execution_id, reason, rounds
        )

    def _publish_orphan_progress(self) -> None:
        """对**失租后仍在跑**的执行发布进度心跳；已结束/已交回的摘除。

        为什么需要（2026-10-10 根治「为什么总靠抢救」）：
        `_watchdog_renew_once` 在续租失败时把执行摘出 `_lease_watch`，此后
        不再遍历它。但「租约已丢、执行仍在推进」是生产常见形态——租约会被
        **另一个消息处理的 finally** 释放（`_process` 的 finally 无条件
        `release`），而原执行对象还在沙箱里跑长节点。实测 exec `6caf8289`：
        17:49 租约被释放 → 18:45/18:47 仍正常完步 → 18:51 因「末节点停滞
        >1800s」被 keeper 判死（**一个健康 run**），产出只能靠值守抢救。

        本方法只做**只读观测**：不续租、不 cancel、不改执行状态。

        plaita#197：摘除判据从「`_lease_lost` 是否仍在」（永远为真）改为
        ``_orphan_execution_alive``（对象/在跑节点）。执行结束即摘除、清
        `_lease_lost`、停止心跳——观测集有界，keeper 重新看得到停滞：
        否则已结束的执行被永久续写 `last_update_time`，stale 判据永不成立。

        ``_publish_node_progress`` 自身对「非 running / 无采集器 / 查询失败」
        都是 no-op 或只告警，因此这里无需再判状态。
        """
        with self._lease_watch_lock:
            orphans = list(self._lease_orphan_watch.items())
        if not orphans:
            return
        for execution_id, entry in orphans:
            execution, tenant_id, fence_token, rounds = self._orphan_entry(entry)
            with self._lease_watch_lock:
                handed_back = execution_id in self._lease_watch
            if handed_back:
                # 已被新租约重新登记 ⇒ 交回正常路径，不重复发布
                self._drop_lease_orphan(
                    execution_id, "已被新租约接管，交回正常路径", rounds
                )
                continue
            if not self._orphan_execution_alive(execution_id, execution):
                self._drop_lease_orphan(
                    execution_id, "执行已结束/不再推进", rounds
                )
                continue
            token = set_current_tenant(tenant_id)
            try:
                self._publish_node_progress(
                    execution_id, fence_token=fence_token, tenant_id=tenant_id
                )
            finally:
                reset_current_tenant(token)
            with self._lease_watch_lock:
                if execution_id in self._lease_orphan_watch:
                    self._lease_orphan_watch[execution_id] = (
                        execution, tenant_id, fence_token, rounds + 1
                    )

    def _detect_affinity_mismatch(self, message_data: Dict[str, Any]) -> Optional[str]:
        """任务的 repo/run_dir 是否指向本机不存在的路径（机器亲和判定）。

        返回 None = 亲和（可跑）；返回字符串 = 不亲和的原因（供日志）。
        判据：消息 params（或顶层）里的 repo / run_dir 是绝对路径且本机
        **不存在** → 不亲和。只查 start 类消息（resume 任务路径已由首次
        start 验过，且 resume 可能不带完整 params）。

        宽松原则：拿不到路径、相对路径、路径存在 → 一律判亲和（不拦）。
        宁可多跑一次失败，不可误拦本机该跑的任务。
        """
        if str(message_data.get("type") or "start") not in ("start", ""):
            return None
        params = message_data.get("params")
        if not isinstance(params, dict):
            return None
        for field in ("repo", "run_dir"):
            raw = params.get(field)
            if not isinstance(raw, str) or not raw.strip():
                continue
            path = raw.strip()
            if not path.startswith("/"):
                continue  # 相对路径/标识符：无法判定，放过
            # run_dir 首次运行时尚未建（<repo>/.flowcast/runs/<run_id>），
            # 故判它的**仓根**（run_dir 上溯到 .flowcast 的父目录）是否存在。
            # 找不到 .flowcast 段则退回查 run_dir 自身（保守）。
            probe = path
            if field == "run_dir":
                head = path
                while head and head != "/":
                    if os.path.basename(head) == ".flowcast":
                        probe = os.path.dirname(head)
                        break
                    head = os.path.dirname(head)
                # ── 已删除的「宿主归属补判」（2026-10-11 事故，勿再加回）──────
                # `aa74ab6` 曾在此加过 `isdir(dirname(path)) and not exists(path)
                # ⇒ 判不亲和（run 属别的宿主）`。该判据**不可满足且必然自锁**：
                #   1. keeper 只**预告** run_dir、不创建它（`issue_keeper/keeper.py`
                #      拼路径后直接 `start_execution`，中间无 `mkdir`），目录由执行
                #      期才建 ⇒ 对「尚未开跑的 run」，`runs/` 在**两台机器都**存在而
                #      `run_dir` 在**两台机器都不存在**，于是**每台** worker（含单机）
                #      都判「不亲和」；
                #   2. 目录只能由「被放行者」创建，而放行又要求目录已存在 ⇒ 鸡生蛋；
                #      交接**不产生目录** ⇒ 判据永远无法被满足；
                #   3. 叠加 `_handover_non_affine` 每次 `enqueue_task` 铸**新 msg id**
                #      ⇒ `times_delivered` 恒为 1 ⇒ `max_deliveries=5` 永不触发 ⇒
                #      **无界自锁**（实测双机各 2600+ 次交接、持续 ≥85 分钟）。
                # 根本原因：「目录不存在」**无法**区分「属别的宿主」与「还没建」，
                # 且经 `/Users/kong/projects → /home/ubuntu/projects` 软链后两边解析
                # 到同一条路径 ⇒ 宿主归属信号在本层**根本不可得**。归属应由派发侧
                # 注入字段表达（见 `docs/` 规划），不在 worker 侧用目录反推。
                # 故此处**退回**「查仓根是否存在」的原判据（：3396 之前的 probe）。
                if not os.path.exists(probe):
                    return f"{field}={path} 在本机不存在（本机无此仓路径）"
                continue
            if not os.path.exists(probe):
                return f"{field}={path} 在本机不存在（本机无此仓路径）"
        # 按仓拒跑名单（2026-10-07）：与路径亲和独立——路径在本机存在（大仓有
        # 软链、双机都能解析）时仍可按仓名拒收，把重仓挡在小容量机器外。
        deny = _deny_repos()
        if deny:
            raw_repo = params.get("repo")
            if isinstance(raw_repo, str) and raw_repo.strip():
                name = os.path.basename(raw_repo.strip().rstrip("/"))
                if name in deny:
                    return (f"repo={name} 在拒跑名单（PLAITA_WORKER_DENY_REPOS）"
                            "——重仓留给大容量 worker")
        return None

    def _lease_conflict_ack_safe(self, body: Any) -> bool:
        """ExecutionLeaseError 时 ack 是否安全（#50 加固）。

        「租约冲突 ack 释放」（2026-10-06 修）的前提是**有人在跑**：租约被
        持有 = 持有者正常推进，ack 只释放重复载体。但租约键在持有者进程
        死后仍可存活 ≤TTL（看门狗最后一次续期的尾巴，120s）——维护窗重启
        /崩溃后的这段窗口里，另一 worker 按 claim_min_idle（60s）回收重投
        消息，命中该分支把**非终态执行唯一的重投载体** ack 掉：执行停在
        running，恢复退化成 zombie reap（plaita#50 双机两例实证）。

        判据：从租约值反解持有者 instance id（``new_holder_token`` 嵌入）
        → 查服务注册表在册与否。注册表心跳 10s / TTL 30s，持有者死后
        ≤30s 消失，早于 60s 的首次回收——在册 = 活持有者，ack 安全；
        不在册 = TTL 尾巴，ack 会吞掉重试载体。

        拿不到任何信号（旧格式租约 / 读租约或注册表瞬断）按「存活未知」返回
        True 保持 ack——宁可回到 zombie reap 兜底，不回归 2026-10-06 修掉的
        假死信风暴（对端每 60s XCLAIM 烧 delivery）。
        **例外：未启用注册表（``--no-registry``）不在此列**（B65-03）：该部署
        形态下存活信号静态不可得，把「存活未知」当「活持有者」会在持有者被
        冻结（SIGSTOP/GC/调度饥饿，租约仍有效）时 ack 掉唯一消息——冻结者
        恢复后 ``lease_lost`` 退出，执行停在 running 且零载体。故该分支一律
        **不 ack**，留 pending 等租约过期后 reclaim；租约在时死信守卫拒绝
        死信，不会回归假死信风暴。
        回滚开关 PLAITA_DISABLE_HOLDER_LIVENESS=1 整体旁路（回到无条件 ack）。

        A47-F2（A46-04 根因）：持有者存活只证明「进程在」，不证明「会消费
        这条消息」。suspend 提交窗口 [订阅注册, 租约释放) 内到达的
        ``resume_type=event/timeout/cancel`` 是挂起执行的**唯一决议载体**
        （#33：挂起只能被这三者唤醒），持有者落盘 suspended 后不会再读它。
        旧判据在持有者在册时把它当重复载体 ack ⇒ 释放租约后零载体、执行
        永久 suspended（默认 event 节点的自动恢复网为 0，见台账 A47-02）。
        故决议载体**不看任何存活信号、一律不 ack**，留 pending 到租约释放后
        reclaim；#242 死信守卫在租约仍在时拒绝死信，释放后重入队副本推进。
        ``continue/retry/start`` 不携带推进语义，保持上面的原判据。本判据
        只覆盖租约冲突窗口；#258 的 ``ResumeProtocolError`` 状态感知分支是
        另一个窗口（协议错配），两套判据互不影响。
        """
        if _holder_liveness_disabled():
            return True
        # A46-04 / A47-F2：决议载体无条件留 pending（含信号缺失分支——
        # 对「唯一载体」fail-open 就是永久挂起）。
        if (
            isinstance(body, dict)
            and body.get("type") == "resume"
            and body.get("resume_type") in _DECISION_RESUME_TYPES
        ):
            logger.warning(
                "执行 %s 的 %s 决议载体在租约冲突窗口内到达（持有者可能正在落盘 "
                "suspended）——不 ack，留 pending 等租约释放后重投推进 "
                "(A46-04/A47-F2)",
                body.get("execution_id"),
                body.get("resume_type"),
            )
            return False
        if not (isinstance(body, dict) and body.get("execution_id")):
            return True
        if not getattr(self, "_enable_registry", False):
            # B65-03：--no-registry 没有存活信号，不能把「存活未知」当「活
            # 持有者」。持有者冻结时租约仍有效；ack 掉本消息后冻结者恢复
            # 即 lease_lost 退出，执行停在 running 且无任何载体。留 pending，
            # 等租约过期后由 reclaim 续跑（租约在时死信守卫拒绝死信）。
            logger.warning(
                "执行 %s 租约冲突，但服务注册表未启用（--no-registry）无法核实"
                "持有者存活——不 ack，留 pending 等租约过期后 reclaim 续跑 "
                "(B65-03)",
                body.get("execution_id"),
            )
            return False
        token = set_current_tenant(body.get("tenant_id"))
        try:
            try:
                lease_value = self.execution_lease.get_holder(body["execution_id"])
            except Exception as exc:  # noqa: BLE001 — 瞬断按存活未知处理
                logger.debug("租约持有者存活核算读租约失败（按存活未知）: %s", _scrub_url_text(exc))
                return True
        finally:
            reset_current_tenant(token)
        instance = holder_instance_id(lease_value)
        if not instance:
            return True
        registry_key = (
            f"{ServiceRegistry.REGISTRY_PREFIX}:{self.SERVICE_TYPE}:{instance}"
        )
        try:
            registered = bool(self.redis_client.exists(registry_key))
        except Exception as exc:  # noqa: BLE001 — 瞬断按存活未知处理
            logger.debug("租约持有者存活核算查注册表失败（按存活未知）: %s", _scrub_url_text(exc))
            return True
        if registered:
            # 持有者在册但处于 draining（优雅停机中）：它**不保证**会收尾——
            # _force_stop_after_drain 超时即放弃当前步退出。此时若 ack 释放
            # 消息，执行唯一的重投载体就没了（2026-10-11 维护窗：Mac 重启时
            # 4 个在途执行全部停 running、PEL=0，见 plaita#52 同族）。
            # 故 draining 持有者不算「安全 ack」，消息留 pending，等租约过期后
            # 由 reclaim 续跑。读注册表失败/无 status 字段 → 保持原语义（ack）。
            if self._registry_entry_not_accepting(registry_key):
                logger.warning(
                    "执行 %s 租约持有者 %s 处于非 running 状态（停机/启动中，不保证收尾）"
                    "——不 ack，留 pending 等租约过期后 reclaim 续跑",
                    body.get("execution_id"),
                    instance,
                )
                return False
            return True
        logger.warning(
            "执行 %s 租约持有者 %s 已不在服务注册表（持有者已死、租约处 ≤%ss "
            "TTL 尾巴）——不 ack，留 pending 等租约过期后 reclaim 续跑 (#50)",
            body.get("execution_id"),
            instance,
            getattr(self, "lease_ttl_seconds", "?"),
        )
        return False

    def _registry_entry_not_accepting(self, registry_key: str) -> bool:
        """注册表条目的 status 是否**不保证收尾**（S18-02A / A45-03/A46-04）。

        对 ``status not in {"running"}`` 返回 True：draining/stopping/stopped/
        starting 与未知值都视为「持有者可能随时退出」——此时 ack 会释放执行的
        唯一重投载体（2026-10-11 维护窗事故同族）。

        **保持既有红线**：读失败 / 值非 JSON / **无 status 字段**（存量格式）
        → False（不改变既有 ack 语义）；缺字段不等于「非 running」。
        """
        try:
            raw = self.redis_client.get(registry_key)
        except Exception as exc:  # noqa: BLE001 — 瞬断按「可收尾」处理（既有语义）
            logger.debug("读注册表条目失败（按可收尾处理）: %s", _scrub_url_text(exc))
            return False
        if not raw:
            return False
        try:
            data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001 — 非 JSON 值按可收尾处理
            logger.debug("注册表条目非 JSON（按可收尾处理）: %s", _scrub_url_text(exc))
            return False
        if not isinstance(data, dict) or "status" not in data:
            return False
        return data.get("status") != "running"

    def _affinity_handover_key(self, body: Dict[str, Any]) -> str:
        """交接计数键：按 **execution_id** 维度（全局稳定，见下）。

        为什么是 `execution_id` 而不是 body 指纹/`run_dir`：

        - 新副本**原样重投同一 body**（`enqueue_task(..., dict(body))`），
          `execution_id` 不变 ⇒ 能跨「新 msg id」连续累计，正是旧实现缺的维度；
        - `run_dir` 是 keeper 的私有约定，不是 plaita 协议字段；
        - body 指纹在带 `delivery_count`/临时字段时可能漂移，退化为键空间爆炸。

        退化契约：body **无** `execution_id`（老消息/畸形消息）时退回 body 指纹
        ——与旧实现同款键，仍**足以**抑制同副本立刻回弹（TTL 由 30s 提到 7 天）；
        此时计数只在「同 body」内有效，不同 body 的同一任务不会被合并计数。
        """
        execution_id = body.get("execution_id")
        if isinstance(execution_id, str) and execution_id.strip():
            return (f"{tenant_namespace(current_tenant())}"
                    f":execution:affinityhandover:{execution_id.strip()}")
        import hashlib as _hl
        digest = _hl.sha1(
            json.dumps(body, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
        return f"plaita:affinity:handover:{digest}"

    def _record_affinity_handover(self, key: str) -> Optional[int]:
        """自增交接计数（INCR + TTL），返回自增后的值（含本次）。

        TTL 取 `AFFINITY_HANDOVER_TTL_SECONDS`（7 天）——**必须远长于**回收
        周期 `claim_min_idle_ms`(60s)：TTL 短于回收周期时计数会在同一任务绕
        一圈回来前过期 ⇒ 计数重置为 1 ⇒ 「有界」形同虚设（旧实现 30s 布尔标记
        即此病：60s > 30s，抑制间歇失效、事故时约 47% 未命中）。

        **返回 None = 计数不可用**（区别于「计数=1」）。调用方**必须**把
        None 当「界已失效 ⇒ 不再交接」处理——见 `_handover_non_affine`。

        为什么这里**必须 fail-closed**（2026-10-11 验收者指出的部署阻断级洞）：
        本计数是自锁场景下**唯一的**天然终止条件（`max_deliveries` 因「新副本
        delivery 恒为 1」够不着）。计数一旦失效而仍按「未知 = 1」放行，界就
        等于不存在 ⇒ 无界风暴原样复发。实测（修复前）：`pipeline()` 抛错时
        **500 次交接 / 500 份副本**、键被写成非整数（`WRONGTYPE` 键污染）时
        **50/50 无界**。

        ⚠️ **与本仓 `noderetry` 的同款 `return 1` 形似而实不同**：那里
        `return 1` 只是「少拦一次重试」，另有总上限/终态等兜底；这里计数是
        **唯一**终止条件 ⇒ 「保守不误杀」在此恰是**危险方向**。故本方法
        与其他计数助手**刻意不一致**，勿为「统一风格」改回 `return 1`。

        无 redis 客户端（内存 worker / 直连单测）→ 同样返回 None（fail-closed）。
        **生产可达性已核实（2026-10-11 验收要求）**：`RedisFlowWorker.__init__`
        的 `redis_client = redis_client or make_redis_client(redis_url)` 保证实例
        恒有真客户端，`run()` 消费循环也只能经该实例进入 ⇒ 本分支在生产
        **不可达**，只有 `__new__` 骨架 / 内存 worker / 直连单测会走到。仍按
        fail-closed 处理是**纵深防御**：即便将来有人绕开 `__init__`，也不会
        静默退化成无界。
        """
        redis_client = getattr(self, "redis_client", None)
        if redis_client is None or not hasattr(redis_client, "incr"):
            logger.error(
                "本机无可用 redis 客户端，交接计数不可用——"
                "界已失效，本次**不再交接**（退化为留 pending）: %s", key,
            )
            return None
        try:
            pipe = redis_client.pipeline(transaction=True)
            pipe.incr(key)
            pipe.expire(key, self.AFFINITY_HANDOVER_TTL_SECONDS)
            return int(pipe.execute()[0])
        except Exception as e:  # noqa: BLE001 — ★ fail-closed：界失效 ⇒ 不交接
            logger.error(
                "交接计数自增失败（含 WRONGTYPE 键污染/瞬断）——**界已失效，"
                "本机不再交接**（退化为留 pending，等对端 reclaim 或运维介入）: "
                "%s: %s", key, _scrub_url_text(e),
            )
            return None

    def _terminalize_handover_exhausted(
        self, body: Dict[str, Any], count: int, exc: Exception
    ) -> bool:
        """交接达上限：把执行**终态化 error**（可观测），返回是否已终态化。

        为什么是「终态化」而不是「留 pending」（裁决见 review-verdict §A3）：

        - 「留 pending」**不是界**：消息留在 PEL 里，`claim_min_idle_ms`(60s)
          后被对端 `XCLAIM` 抢回（实测 2356 次留 pending 全部被回收，风暴持续
          85+ 分钟）⇒ 只是把「无限交接」换成「无限 reclaim」，两者都无终止条件；
        - 终态化的**真实收口机制**（已逐条核对源码，勿想当然）：
          1. **计数键是终止的真正承担者**：达上限后本机**不 ack、不重投**，
             且计数键 TTL(7 天) 远长于回收周期 ⇒ 消息即便被对端 reclaim 绕回，
             本机读到的计数仍 > 上限 ⇒ **永远不再产新副本**。对端首次收到时
             计数从 1 起算、同样有界 ⇒ 总交接次数上界 ≈ 上限 × 机器数；
          2. **但不要声称「对端 reclaim 后会被终态短路」**——那是 **resume**
             路径的语义（`resume_flow` 的 `state_status in ("completed",
             "error","cancelled")` 分支），**start 路径没有**这条判据：核对
             `start_flow` 确认 `status="running"` 行是**无条件重写**的，入口
             只查**取消标志**（`_cancel_requested`），不查终态状态；
          3. 故**兜底边界**如实为：那条绕回的消息最多被对端再消费一次
             （届时按宽松原则能跑就跑，跑不起来是**有界**的一次失败），
             此后不再产生副本。**「次数有上界」即「有界」，本修复正是把
             无界自旋收敛为有界**——它不保证任务不再被执行；
          4. 死信守卫（`_dead_letter_guard`）见终态 → 放行死信，不再重入队。
        - 可观测：error 里带 `affinity_handovers`/`affinity_reason`，运维与
          console 一眼看出「没有任何宿主能跑该任务」，而非静默空转。

        ⚠️ **边界声明**：真正消除误判需派发侧注入权威归属字段
        （`target_worker`）——那是独立改动，且需先定义「哪个 repo 归哪台」。

        终态化前先读状态：**已经终态**（人工已 cancel / 已被别的路径终态化）
        不覆写，直接按「无需再交接」处理。读不到状态（None/异常）时降级为
        **不写盘**但返回 True —— 达上限即**不再重投**是安全方向（宁可让 keeper
        的宽限线接手，也不再产副本）。
        """
        execution_id = body.get("execution_id")
        if not isinstance(execution_id, str) or not execution_id.strip():
            return False  # 无执行 id：无从终态化，退化为留 pending（由调用方处理）
        execution_id = execution_id.strip()
        try:
            state = self.execution_storage.load_execution_state(execution_id)
        except Exception as e:  # noqa: BLE001 — 读失败不覆写状态，但仍停止重投
            logger.error(
                "执行 %s 交接达上限但读执行状态失败——不再重投（留 pending 待"
                "运维介入），不覆写状态: %s", execution_id, _scrub_url_text(e),
            )
            return True
        if state is None:
            logger.error(
                "执行 %s 交接达上限（%s 次）但执行状态不存在（可能已被 console "
                "清理/从未创建）——不再重投，留 pending 待运维介入: %s",
                execution_id, count, _scrub_url_text(exc),
            )
            return True
        if getattr(state, "status", "") in TERMINAL_EXECUTION_STATUSES:
            logger.error(
                "执行 %s 交接达上限（%s 次）且已是终态 %s——不再交接: %s",
                execution_id, count, state.status, _scrub_url_text(exc),
            )
            # 已是终态 ⇒ 该消息**永远不可能**再被处理，留在 PEL 只会被对端每
            # `claim_min_idle_ms`(60s) 反复 XCLAIM（实测 count 5→6→7→8 递增），
            # 形成「有界但不停」的低频噪声。故此处返回 `"ack"` 让调用方**消费掉**
            # 这条死信（ack 是有损的，仅对**已终态**消息使用——不会丢任何活工作）。
            return "ack"
        state.status = "error"
        state.error = {
            "message": (
                f"任务在 {count} 次 worker 间交接后仍不亲和：没有任何 worker "
                f"能认领该任务（repo/run_dir 路径在全部宿主上都不可用）。"
                f"最后一次不亲和原因：{exc}"
            ),
            "type": "AffinityHandoverExhausted",
            "affinity_handovers": count,
            "affinity_reason": str(exc),
        }
        state.end_time = datetime.now().isoformat()
        try:
            self._persist_state_or_raise(
                execution_id, state, "affinity_handover_exhausted"
            )
        except Exception as e:  # noqa: BLE001 — 写盘失败不丢消息，但仍不重投
            logger.error(
                "执行 %s 交接达上限但终态化落盘失败——不再重投（留 pending 待"
                "运维介入）: %s", execution_id, _scrub_url_text(e),
            )
            return True
        logger.error(
            "执行 %s 交接达上限（%s/%s）——判定「无宿主可跑」，已终态化 error，"
            "不再交接（终止自旋）。最后一次不亲和原因: %s",
            execution_id, count, self.AFFINITY_HANDOVER_MAX, _scrub_url_text(exc),
        )
        return True

    def _unavailable_dispatch_target(
        self, body: Dict[str, Any]
    ) -> Optional[Tuple[str, str]]:
        """A46-02：复核显式派发目标是否仍可用；不可用 → ``(target, status)``。

        只有「本机不是目标」的交接才复核（本机就是目标时，这次交接是亲和性
        让出，目标机可能仍是本机，不能剥掉目标）。判定口径：

        - 目标注册键不存在 → ``missing``（探针里的「幽灵目标」）；
        - 键存在但 status != ``running``（draining/stopping/stopped/未知）
          → 剥掉 target 按广播重投，避免 4 次交接后
          ``AffinityHandoverExhausted``；
        - **读注册表失败 → 保留 target**（fail-open，与派发侧同口径）：
          一次 Redis 瞬断不得把可跑的定向任务广播给不亲和的机器；
        - 目标就是 running → 保留 target（正常定向交接）。
        """
        target = normalize_target_instance_id(body.get(TARGET_INSTANCE_FIELD))
        if target is None:
            return None
        try:
            if target_mismatch(body, getattr(self, "instance_id", None)) is None:
                return None
        except Exception as exc:  # noqa: BLE001 — 判据自身异常时保持原交接语义
            logger.warning(
                "dispatch-target-check-failed:keep-target target=%s reason=mismatch-error: %s",
                target,
                _scrub_url_text(exc),
            )
            return None

        redis_client = getattr(self, "redis_client", None)
        if redis_client is None:
            logger.warning(
                "dispatch-target-check-failed:keep-target target=%s reason=no-redis-client",
                target,
            )
            return None
        key = f"{ServiceRegistry.REGISTRY_PREFIX}:{self.SERVICE_TYPE}:{target}"
        try:
            raw = redis_client.get(key)
        except Exception as exc:  # noqa: BLE001 — 读失败 fail-open，保留 target
            logger.warning(
                "dispatch-target-check-failed:keep-target target=%s: %s",
                target,
                _scrub_url_text(exc),
            )
            return None
        if not raw:
            return target, "missing"
        try:
            data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001 — 无法证明 running ⇒ 按不可用处理
            logger.warning(
                "dispatch-target-check-failed:target-key-not-json target=%s: %s",
                target,
                _scrub_url_text(exc),
            )
            return target, "invalid"
        if not isinstance(data, dict):
            return target, "invalid"
        status = data.get("status")
        if not isinstance(status, str) or not status:
            return target, "unknown"
        if status == "running":
            return None
        return target, status

    def _handover_non_affine(self, task: Any, queue: Any, exc: Exception) -> bool:
        """把不亲和的 task 交接给其他 worker：**先入队**同体新副本，成功后 ack 原条。

        S18-03（A45-05）：顺序必须是 enqueue→ack——旧实现先 ack 再入队，一旦
        ``enqueue`` 失败，原条已移出 PEL 且从 Stream 删除，执行唯一载体凭空
        消失（日志却仍声称「绝不丢消息」）。enqueue 失败 ⇒ 不 ack，原条留
        pending 由对端 reclaim；ack 失败 ⇒ 新副本已在队列、原条仍在 pending，
        重复由租约/终态短路吸收。

        返回 True = 原条已 ack（交接完成）；False = 原条留 pending（由 reclaim
        接管，或已达上限终态化）。``_consume_loop`` 用返回值回写 ``acked``，
        避免日志把「已交接」与「留 pending」混为一谈。

        重入队而非留 pending 的原因见 TaskNotForThisWorker docstring（留
        pending 会被对端反复 XCLAIM 虚增 delivery → 误死信）。

        **有界性**（2026-10-11 无限交接风暴后的硬约束）：交接次数按
        `execution_id` 计数（`INCR` + 7 天 TTL），达 `AFFINITY_HANDOVER_MAX`
        即**终态化 error**、不再 ack/重投——见 `_terminalize_handover_exhausted`
        的裁决说明（「留 pending」不是界）。

        **旧实现的病**（勿改回布尔标记）：`SET key 1 EX 30` + `EXISTS`——
        标记 TTL 30s 短于回收周期 60s，抑制间歇失效；且判据不可满足时
        交接永无终止条件，而每次 `enqueue_task` 铸新 msg id 使
        `times_delivered` 恒为 1、`max_deliveries` 永不触发 ⇒ 无界自旋。

        **fail-closed 契约**（2026-10-11 验收者指出的部署阻断级洞）：计数
        不可用（`_record_affinity_handover` 返回 `None`：redis 瞬断 /
        `WRONGTYPE` 键污染 / 无 redis 客户端）时**不再交接**，退化为留
        pending（不 ack、不重投、绝不丢消息）。**不得**把「计数不可用」当成
        「计数 = 1」——本计数是自锁场景下唯一的终止条件，按 1 放行即等于
        无界（实测 500 次交接 / 500 份副本）。此处**不**终态化：计数失效是
        本机基础设施故障，不是「该执行无宿主可跑」，误终态化会判死可跑任务。
        """
        body = getattr(task, "body", None)
        if not isinstance(body, dict):
            # 拿不到消息体：退化为留 pending（对端 reclaim）
            logger.info("任务 %s 不亲和但无体可交接，留 pending: %s",
                        getattr(task, "message_id", "?"), _scrub_url_text(exc))
            return False
        # A46-02：目标机器已不可用（键消失 / 不再是 running）时剥掉
        # target_instance_id，按广播重投——否则集群里每台非目标机都会交接一次，
        # 4 次后 AffinityHandoverExhausted 把可跑任务终态化。
        # A69-04：交接副本保留源年龄（缺失时由源 Stream ID 派生）。
        copy_body = _copy_carrier_body(body, getattr(task, "message_id", None))
        unavailable_target = self._unavailable_dispatch_target(body)
        stripped_target = unavailable_target is not None
        if stripped_target:
            target, target_status = unavailable_target
            copy_body.pop(TARGET_INSTANCE_FIELD, None)
            logger.warning(
                "dispatch-target-unavailable:broadcast target=%s status=%s "
                "(执行 %s 不再定向，按广播重投)",
                target,
                target_status,
                body.get("execution_id"),
            )
        try:
            key = self._affinity_handover_key(body)
            count = self._record_affinity_handover(key)
            if count is None:
                # ★ fail-closed（2026-10-11 验收者的部署阻断级洞）：计数不可用
                # ⇒ **界不可用** ⇒ 绝不能按「未知 = 1」放行（那等于无界）。
                # 与「拿不到 body」同款退化为留 pending：不 ack、不重投，
                # **绝不丢消息**（消息留在 PEL，由对端 reclaim 或运维介入）。
                # 注意：此处**不**终态化——计数失效是**本机基础设施故障**，
                # 不是「该执行无宿主可跑」；误终态化会把可跑的任务判死。
                logger.error(
                    "任务 %s 的交接计数不可用（界已失效）——本机不再交接，"
                    "退化为留 pending（绝不丢消息；请查 redis 瞬断/键污染）: %s",
                    getattr(task, "message_id", "?"), _scrub_url_text(exc),
                )
                return False
            if count > self.AFFINITY_HANDOVER_MAX:
                # 达上限：**不 ack、不重投**——ack 会移出 PEL 让消息凭空消失，
                # 重投会让自旋继续。留 pending + 终态化 error：终态化的作用是
                # **可观测**（运维立刻看见「无宿主可跑」），而**终止自旋靠计数键**
                # ——它 7 天不过期，故对端 reclaim 绕回来时本机仍读到 count > 上限、
                # 永不再产副本（详见 _terminalize_handover_exhausted 说明；
                # 注意 start 路径**没有**「已终态即短路」的判据）。
                verdict = self._terminalize_handover_exhausted(body, count, exc)
                if verdict == "ack":
                    # 执行**已是终态** ⇒ 该消息永远不可能再被处理。ack 消费掉它，
                    # 否则它会留在 PEL 被对端每 `claim_min_idle_ms`(60s) 反复
                    # XCLAIM（实测 count 5→6→7→8 递增）——「有界但不停」的噪声。
                    # 仅对**已终态**消息 ack（有损操作），不会丢任何活工作。
                    queue.ack(getattr(task, "message_id"))
                    logger.error(
                        "任务 %s 交接达上限（%s/%s）且执行已终态——已 ack 消费该"
                        "死信，终止自旋: %s",
                        getattr(task, "message_id", "?"), count,
                        self.AFFINITY_HANDOVER_MAX, _scrub_url_text(exc),
                    )
                    return True
                if verdict:
                    return False
                # 无 execution_id 可终态化（老消息/畸形消息）：退化为留 pending，
                # 绝不丢消息（对端 reclaim 或死信守卫兜底）。
                logger.error(
                    "任务 %s 交接达上限（%s/%s）但无 execution_id 可终态化——"
                    "不再交接，留 pending 待运维介入: %s",
                    getattr(task, "message_id", "?"), count,
                    self.AFFINITY_HANDOVER_MAX, _scrub_url_text(exc),
                )
                return False
            # S18-03（A45-05）：**先入队、后 ack**。旧序 ack→enqueue 在入队
            # 失败时让原条既出 PEL 又从 Stream 删除 = 唯一载体凭空消失。
            # A46-02 的目标不可用回退（copy_body 已剥 target）走同一安全序。
            # A44-02：交接副本继承源信封版本（否则新 build 写的副本会被
            # 同组旧 worker 按超版本死信，交接反而丢失恢复载体）。
            mid = getattr(task, "message_id", "?")
            try:
                queue.enqueue(copy_body, **_requeue_envelope_kwargs(task))
            except Exception as e:  # noqa: BLE001 — 入队失败：不 ack，留 pending
                logger.warning(
                    "任务 %s 交接重投失败（原条留 pending，由对端 reclaim）: %s",
                    mid, _scrub_url_text(e),
                )
                return False
            try:
                queue.ack(mid)
            except Exception as e:  # noqa: BLE001 — 新副本已入队，原条仍 pending
                logger.warning(
                    "任务 %s 交接副本已入队但 ack 原条失败（重复投递，"
                    "由租约/终态短路去重）: %s",
                    mid, _scrub_url_text(e),
                )
                return False
            logger.info(
                "任务 %s 与本机不亲和，已交接（enqueue→ack）给其他 worker"
                "（第 %s/%s 次）: %s",
                mid, count, self.AFFINITY_HANDOVER_MAX, _scrub_url_text(exc),
            )
            return True
        except Exception as e:  # noqa: BLE001 — 交接失败退化为留 pending（绝不丢消息）
            logger.warning("不亲和任务交接失败（退化为留 pending）: %s: %s", _scrub_url_text(e), _scrub_url_text(exc))
            return False

    def _interruptible_backoff(self, seconds: float) -> bool:
        """分片退避；返回 True = 被 D2（drain 超时/强停）打断。

        只在**强停**（``_force_stop_event``）时打断：仅请求 draining（D1）
        仍让在途步完整跑完，包括它的退避重投（S18-01 I5）。分片数按固定
        片长预先算出（不依赖真实时钟），测试注入的 ``_sleep`` 假时钟不会让
        循环空转。
        """
        if seconds <= 0:
            return False
        slices = max(1, int(seconds / NODE_RETRY_BACKOFF_SLICE_SECONDS))
        if slices * NODE_RETRY_BACKOFF_SLICE_SECONDS < seconds:
            slices += 1
        chunk = seconds / slices
        for _ in range(slices):
            if self._force_stop_event.is_set():
                return True
            _sleep(chunk)
        return self._force_stop_event.is_set()

    def _requeue_retryable_failure(
        self, task: Any, queue: Any,
        retry_exc: Optional[NodeExecutionRetryableError] = None,
    ) -> bool:
        """节点可重试失败：**退避后显式重投**一份新副本（ack 原条）。

        为什么不能只依赖「不 ack 留 pending」的回收语义（2026-10-09 实证，
        执行 030ef793981743f6a9c217187022bb38 / plaita#28 卡 58 分钟）：

        - 同一执行的消息可能已被**竞争者的 ExecutionLeaseError 分支 ack 掉**
          （该分支为「避免烧 delivery」显式 ack，见 run() 内注释）；
        - 此时消息不在 PEL 里，reclaim/XCLAIM 无从回收 → 依赖它的重投
          **永不发生**；而处理函数又刻意不终态化 → 执行既不重试也不落终态，
          一直挂在 running，只能等 keeper 的 zombie 线（默认 2h）或人工 resume。

        新副本 delivery 归 1（`enqueue_task` 语义），节点重试预算由独立计数键
        约束，因此不会无限重投。顺序为**先入队、后 ack**：中间崩溃只会产生
        重复投递（租约机制天然去重），绝不会丢消息。

        plaita#91 补上的两件事（2026-10-16）：

        - **退避**：``retry_exc.attempt``（无则 0）→
          ``_node_retry_backoff_seconds`` 的指数延迟（1s/2s/4s/8s…封顶 30s），
          入队前在消费线程内睡——旧实现无任何节奏，5 次预算 ~40ms 烧光
          （#91 复现 A）。attempt 未知（<1，如旧签名直接调用）不退避，
          旧行为零变化。
        - **载体转换**：原样复制 start body 会从首节点整条重放 + 用
          ``context={}`` 覆写盘上 checkpoint（#91 复现 D）；改经
          ``_requeue_body_for_retry`` 查盘，已有行则转 resume 载体
          （``_handle_start_dedup_hit`` 同款形状），读盘失败/无行原样 start。

        返回 True=已重投并 ack；False=退化为留 pending（由 reclaim 回收）。
        """
        body = getattr(task, "body", None)
        mid = getattr(task, "message_id", "?")
        if not isinstance(body, dict):
            logger.info("任务 %s 可重试失败但无体可重投，留 pending 交 reclaim", mid)
            return False
        attempt = getattr(retry_exc, "attempt", 0) or 0
        delay = _node_retry_backoff_seconds(attempt)
        if delay > 0:
            logger.warning(
                "任务 %s 节点可重试失败，退避 %.0fs 后显式重投（attempt=%s）",
                mid, delay, attempt,
            )
            # A46-06/S18-01：分片退避，D2（drain 超时强停）时立即放弃——
            # 不 enqueue、不 ack，原条留 pending（I5）。
            if self._interruptible_backoff(delay):
                logger.warning(
                    "retry-backoff-aborted-by-drain：任务 %s 退避期间进入强停，"
                    "不重投、不 ack，消息留 pending（attempt=%s）",
                    mid, attempt,
                )
                return False
        requeue_body = _requeue_body_for_retry(
            body,
            getattr(self, "execution_storage", None),
            # A69-04：源载体缺失 timestamp 时由源 Stream ID 派生，不得补 now。
            source_message_id=getattr(task, "message_id", None),
        )
        if requeue_body is None:
            # A51-01：执行已被删除（墓碑命中）——ack 原条、不生成任何新
            # 载体。原样重投 start 会被 start_flow 当「无状态=全新执行」重建。
            logger.warning(
                "任务 %s 的执行 %s 已删除（墓碑），节点失败重投被丢弃（ack 原条）",
                mid, body.get("execution_id"),
            )
            try:
                queue.ack(mid)
            except Exception as e:  # noqa: BLE001 — ack 失败=重复投递，墓碑会再次拦截
                logger.warning(
                    "任务 %s 墓碑丢弃后 ack 失败（将产生重复投递）: %s",
                    mid, _scrub_url_text(e),
                )
            return True
        try:
            # A44-02：重投副本继承源信封版本（载体语义属于源消息）。
            queue.enqueue(requeue_body, **_requeue_envelope_kwargs(task))
        except Exception as e:  # noqa: BLE001 — 重投失败退化为 reclaim（绝不丢消息）
            logger.warning("任务 %s 显式重投失败（退化为留 pending 回收）: %s", mid, _scrub_url_text(e))
            return False
        try:
            queue.ack(mid)
        except Exception as e:  # noqa: BLE001 — ack 失败=重复投递，不致命
            logger.warning("任务 %s 重投后 ack 失败（将产生重复投递，租约去重）: %s", mid, _scrub_url_text(e))
            return True
        return True

    def _quota_defer_max(self) -> int:
        """超配额重投上限（env 可调，≥1）：达上限后带 quota_bypass 放行。"""
        raw = (os.environ.get("PLAITA_TENANT_QUOTA_DEFER_MAX") or "").strip()
        if not raw:
            return self.TENANT_QUOTA_DEFER_MAX
        try:
            value = int(raw)
        except (TypeError, ValueError):
            logger.warning(
                "PLAITA_TENANT_QUOTA_DEFER_MAX=%s 非法，回退默认 %s（B2R31-03）",
                _redact_env_value(raw), self.TENANT_QUOTA_DEFER_MAX,
            )
            return self.TENANT_QUOTA_DEFER_MAX
        if value < 1:
            logger.warning(
                "PLAITA_TENANT_QUOTA_DEFER_MAX=%s <1，回退默认 %s（不是无限延迟）",
                value, self.TENANT_QUOTA_DEFER_MAX,
            )
            return self.TENANT_QUOTA_DEFER_MAX
        return value

    def _defer_over_quota_start(self, task: Any, queue: Any, exc: Exception) -> bool:
        """超配额 start 的**延迟**（plaita#28）：重投尾部 + ack 原条。

        与 ``_requeue_retryable_failure`` 同款安全序（先入队、后 ack：中间
        崩溃只会产生重复投递，租约机制天然去重，绝不丢消息）；副本带
        ``quota_defers`` 计数（deliveries 归 1 ⇒ 不烧 delivery、不死信）。
        重投把消息放到 stream **尾部**——先入队的其他租户消息获得优先，
        这就是单 stream FIFO 下能做到的「排队」语义（按租户分 stream +
        加权轮询是 issue #28 的进阶项）。

        有界（与 AFFINITY_HANDOVER_MAX 同一教训：无界重投=自锁）：达
        ``_quota_defer_max()`` 后副本带 ``quota_bypass=True`` 放行——软配额
        让位存活性。返回 True=已重投并 ack；False=退化为留 pending。
        """
        body = getattr(task, "body", None)
        mid = getattr(task, "message_id", "?")
        if not isinstance(body, dict):
            logger.info("任务 %s 超配额但无体可重投，留 pending 交 reclaim", mid)
            return False
        try:
            defers = int(body.get("quota_defers") or 0) + 1
        except (TypeError, ValueError):
            defers = 1
        body = _copy_carrier_body(body, getattr(task, "message_id", None))
        body["quota_defers"] = defers
        tenant = body.get("tenant_id") or "default"
        if defers > self._quota_defer_max():
            body["quota_bypass"] = True
            logger.warning(
                "租户 %s start 配额持续已满，execution=%s 已延迟 %d 次达上限，"
                "带 bypass 放行（存活性优先于软配额）",
                tenant, body.get("execution_id"), defers - 1,
            )
        else:
            logger.info(
                "租户 %s start 配额已满，execution=%s 延迟第 %d 次（重投尾部排队）: %s",
                tenant, body.get("execution_id"), defers, exc,
            )
        try:
            # A44-02：延迟副本继承源信封版本。
            queue.enqueue(body, **_requeue_envelope_kwargs(task))
        except Exception as e:  # noqa: BLE001 — 重投失败退化为 reclaim（绝不丢消息）
            logger.warning("任务 %s 超配额重投失败（退化为留 pending 回收）: %s", mid, _scrub_exception_text(e))
            return False
        try:
            queue.ack(mid)
        except Exception as e:  # noqa: BLE001 — ack 失败=重复投递，不致命
            logger.warning("任务 %s 超配额重投后 ack 失败（将产生重复投递）: %s", mid, _scrub_exception_text(e))
            return True
        return True

    def _dispatch_task(self, message_data: Dict[str, Any], delivery_count: Optional[int] = None,
                       schema_version: Optional[int] = None,
                       stream_message_id: Optional[str] = None) -> None:
        message_type = message_data.get("type")

        # 租户状态闸（plaita#27）：停用租户的 start/resume 一律不执行。消息已
        # 入队才停用（或挂起执行的 resume 来自 delay/approval/event 服务）时，
        # 若照跑则「停用」对运行面是 no-op。
        # plaita#326：**新 start** 升级为 fail-closed 三格闸——读失败（认识论
        # 性，#312 词汇表）抛 TenantGateEpistemicReject 由消费循环留 pending
        # 自愈；确认态（disabled / A133-03 的 status 缺+世代在残留）照旧丢弃；
        # status 缺+世代缺维持 #237 fail-open。resume 与其他在飞载体保持
        # #27 fail-open（kill switch 是准入控制，抖动时拽飞中执行会放大而
        # 非收敛爆炸半径）。
        if message_type == "start":
            start_allowed, start_gate_reason = tenant_start_allowed(
                getattr(self, "redis_client", None),
                message_data.get("tenant_id"),
                getattr(self, "_tenant_status_epoch", None),
            )
            if not start_allowed:
                if is_epistemic_reject_reason(start_gate_reason):
                    raise TenantGateEpistemicReject(
                        message_data.get("tenant_id"), start_gate_reason
                    )
                logger.warning(
                    "租户 %s start 被状态闸拒绝（%s），丢弃任务 %s"
                    "（flow=%s execution=%s）",
                    message_data.get("tenant_id") or "default",
                    start_gate_reason,
                    message_type,
                    message_data.get("flow_id"),
                    message_data.get("execution_id"),
                )
                return
        elif not tenant_is_active(
            getattr(self, "redis_client", None),
            message_data.get("tenant_id"),
            getattr(self, "_tenant_status_epoch", None),
        ):
            logger.warning(
                "租户 %s 已停用，丢弃任务 %s（flow=%s execution=%s）",
                message_data.get("tenant_id") or "default",
                message_data.get("type"),
                message_data.get("flow_id"),
                message_data.get("execution_id"),
            )
            return

        # plaita#283：消费循环已按载体源世代设置 ContextVar；dispatch 入口
        # 必须与当前租户世代一致——同名重建后的旧 requeue 副本不得按新世代
        # 正常执行（不能等 dead_letter 才拒绝）。无世代的存量载体按 legacy
        # 宽限（写闸仍 fail-closed）。
        source_generation = current_tenant_generation()
        if source_generation:
            current_generation = read_tenant_generation(
                getattr(self, "redis_client", None),
                message_data.get("tenant_id"),
            )
            if current_generation is None or source_generation != current_generation:
                logger.warning(
                    "丢弃旧世代任务载体 %s（tenant=%s source_gen=%r current_gen=%r, "
                    "flow=%s execution=%s）——不按当前世代执行",
                    message_data.get("type"),
                    message_data.get("tenant_id") or "default",
                    source_generation,
                    current_generation,
                    message_data.get("flow_id"),
                    message_data.get("execution_id"),
                )
                return

        # A51-01/A2R25-04：执行删除墓碑闸。放在配额闸**之前**——已删执行的
        # start 载体不得占用 tenant_running 名额（A51-03）；放在租户状态闸
        # 之后，停用租户消息仍走既有丢弃路径。命中即返回（消费循环随后
        # ack），同 id 的 dedup 映射一并释放（新 id 复用旧 dedup_key 不受
        # 影响）。resume 与 start 同口径：恢复状态键不会撤销墓碑，已删执行
        # 的决议/续跑载体不得再推进。
        if message_type in ("start", "resume") and self._carrier_tombstoned(
            message_data
        ):
            dedup_key = (
                message_data.get("dedup_key") if message_type == "start" else None
            )
            if dedup_key:
                token = set_current_tenant(message_data.get("tenant_id"))
                try:
                    self._release_start_dedup(
                        dedup_key, message_data.get("execution_id")
                    )
                finally:
                    reset_current_tenant(token)
            return

        # B52-02/A69-04：消息最大年龄闸。主任务流没有时间 TTL，墓碑 TTL 到期后
        # 同一 start 仍可重建；超过 horizon 的历史载体直接 ack+WARNING。年龄取
        # payload 与 Stream ID 双源最大值；两源都不可判 → fail-closed。
        # 只闸 start/resume（决议/控制载体不做年龄判断，避免误丢唤醒源）。
        if message_type in ("start", "resume") and self._start_carrier_too_old(
            message_data, stream_message_id
        ):
            return

        # 每租户 start 准入配额（plaita#28，最小公平性原语）：在租户状态闸
        # 之后、亲和闸之前。只闸 ``start``——resume 是救援流量，不受 start
        # 配额拦（挂起执行本就占着名额，闸它只会放大积压）。带名额的消息
        # （console 入口 429 前已 try_admit）幂等放行；无名额的外部生产方
        # 消息（schedule 旧形状/第三方直投）超限时抛 TenantQuotaDeferred，
        # 由消费循环重投尾部排队（不烧 delivery、不死信）。回滚开关
        # PLAITA_DISABLE_TENANT_QUOTA=1 整闸旁路；消息带 quota_bypass（延迟
        # 预算耗尽的放行副本）同样旁路——存活性 > 软配额。
        if (
            message_type == "start"
            and not _tenant_quota_disabled()
            and not message_data.get("quota_bypass")
            and not try_admit_tenant_start(
                getattr(self, "redis_client", None),
                message_data.get("tenant_id"),
                message_data.get("execution_id"),
            )
        ):
            raise TenantQuotaDeferred(
                f"租户 {message_data.get('tenant_id') or 'default'} start 配额已满，"
                f"execution={message_data.get('execution_id')}"
            )

        # 机器亲和性闸（路线二首版，2026-10-06 多机验证）：任务参数里的 repo/
        # run_dir 是**派发方所在机器**的绝对路径。本机不具备该路径 = 跑不了，
        # 应让给有它的 worker（或等它出现）。不拦的话本机抢到就跑 → 秒失败
        # → 烧 delivery 配额 → 死信（plaita#41 双机实证）。抛
        # TaskNotForThisWorker（下方 except 分支不 ack、留 pending，由队列
        # 回收交给别的 consumer）。
        # 回滚/放行：PLAITA_DISABLE_AFFINITY=1，或消息无 repo 参数（不自带
        # 路径的任务不受影响）。
        if not _affinity_disabled():
            missing = self._detect_affinity_mismatch(message_data)
            if missing is not None:
                raise TaskNotForThisWorker(missing)

        # 显式派发目标闸（#42 期 1）：派发侧按容量路由把目标 worker 写进消息体
        # 顶层 ``target_instance_id``。本机不是目标 ⇒ 抛**同一个**
        # ``TaskNotForThisWorker``，由 run() 既有交接分支 ack 原条 + 重入队新副本
        # （副本原样带上该字段）让给目标机——复用既有机制，不新增协议/键/队列。
        # 为什么不是「本地丢弃」：本机不是目标仅是**本机**的判断，消息必须留在
        # 池里才有机会落到目标机；丢弃即丢单（见 TaskNotForThisWorker docstring）。
        # 回滚/放行：PLAITA_DISABLE_DISPATCH_TARGET=1；消息体无该字段（存量广播
        # 派发）、字段空白/非字符串、本机 instance_id 不可知 ⇒ 一律放行——
        # 默认零行为变化，且不因注册表缺失把可跑的任务误拦在本机之外。
        if not _dispatch_target_disabled():
            not_mine = target_mismatch(message_data, getattr(self, "instance_id", None))
            if not_mine is not None:
                raise TaskNotForThisWorker(not_mine)

        # 租户上下文：消息携带 tenant_id（缺省 = default，兼容旧生产方）；
        # 存储路由包装器/日志 handler/租约据此选租户 namespace。
        token = set_current_tenant(message_data.get("tenant_id"))
        try:
            # message_type 已在配额闸处求值（plaita#28），此处直接复用
            if message_type == "start":
                self.start_flow(
                    message_data.get("flow_id"),
                    message_data.get("params"),
                    message_data.get("version"),
                    execution_id=message_data.get("execution_id"),
                    dedup_key=message_data.get("dedup_key"),
                    delivery_count=delivery_count,
                    # plaita#25：console 入队时写进消息体的 timestamp ⇒ 时间线
                    # 由它算「排队多久」（老消息无此键 ⇒ None，不折算成 0）。
                    enqueued_at=message_data.get("timestamp"),
                    # A44-02：start 幂等命中的 resume 副本继承源信封版本。
                    schema_version=schema_version,
                    # A2R44-02：调度触发归因随消息体透传（老消息无此键 ⇒ None）。
                    trigger=message_data.get("trigger"),
                )
            elif message_type == "resume":
                self.resume_flow(
                    message_data.get("flow_id"),
                    message_data.get("execution_id"),
                    message_data.get("resume_type"),
                    message_data.get("data"),
                    delivery_count=delivery_count,
                    # #122：start→resume 载体转换（start 幂等命中）会把原 start
                    # 的 params 带过来。它只在盘上 checkpoint 为空时生效——
                    # 那时不带上就等于把调度入参清成 ``{}``（$INPUT 丢失）。
                    params=message_data.get("params"),
                )
            else:
                raise ValueError(f"unknown task type: {message_type!r}")
        finally:
            reset_current_tenant(token)

    def _ack_resume_protocol_error(self, task: Any, queue: Any, exc: Exception) -> None:
        """#33 既有处置：协议错误且无推进语义 ⇒ ack 释放，不终态化。

        这是 #258 修复**未命中**时的原样行为（真协议错配：suspended +
        continue、陈旧 event 打在仍有活跃订阅的挂起点等）。保留独立方法只为
        让上面的状态感知分支与原语义一眼可对照。
        """
        queue.ack(task.message_id)
        queue.note_poison()
        logger.warning(
            "任务 %s resume 协议错误（执行保持原状不终态化）: %s",
            task.message_id,
            _scrub_url_text(exc),
        )

    def _handle_resume_protocol_error(
        self, task: Any, queue: Any, exc: ResumeProtocolError
    ) -> bool:
        """``run()`` 的 ``ResumeProtocolError`` 分支：ack 前做状态感知处置（#258）。

        旧逻辑无条件 ``ack`` + ``note_poison``，其注释前提是「非终态执行不需要
        重投载体：决议路径（event/cancel/timeout resume）各有独立消息」。该前提
        在 #258 的窗口内不成立——事件 resume 已经消费并注销订阅、流内已推进并
        落盘（``status=running`` + ``$LAST_NODE`` 非挂起点），此后 step_persist
        瞬断/进程崩溃让同一 event/timeout 消息重投：策略层守卫正确判协议错误，
        但 ack 掉的是**唯一**还能推进该执行的载体 ⇒ 永久悬挂。与 #50（租约冲突
        分支 ack 吃掉持有者已死的重试载体）同族：ack 前必须先确认执行是否还有
        活跃载体，而不是只看「当前这条消息此刻推进不了」。

        处置：

        - 盘上 ``running`` 且载体已错过挂起点 ⇒ 归一化为 ``continue`` 重投一份
          新副本（先入队、后 ack；任一步失败都**不 ack**，原消息留 pending），
          把「事件决议」载体切换为「断点续跑」载体；
        - 其余情况（真 #33 错配 / 终态 / suspended / 状态读不到）⇒ 原样 ack，
          重投只会重复命中同一守卫或进 poison 循环；
        - 状态/定义读取瞬断 ⇒ 不 ack，消息留 pending 等回收（与兜底异常分支
          同口径：绝不能因一次读盘抖动丢掉恢复载体）。

        返回 True = 已 ack（含归一化重投后 ack 原条）；False = 未 ack。
        """
        body = getattr(task, "body", None)
        if not isinstance(body, dict):
            self._ack_resume_protocol_error(task, queue, exc)
            return True
        resume_type = body.get("resume_type")
        execution_id = body.get("execution_id")
        flow_id = body.get("flow_id")
        if resume_type not in ("event", "timeout") or not execution_id or not flow_id:
            self._ack_resume_protocol_error(task, queue, exc)
            return True

        token = set_current_tenant(body.get("tenant_id"))
        try:
            # 新副本的租户必须取**读盘时**的上下文（body 缺省时 current_tenant()
            # 才是 load 用的那个 namespace）；reset 之后再取会落回外层默认租户。
            tenant_id = body.get("tenant_id") or current_tenant()
            state = self.execution_storage.load_execution_state(execution_id)
            if state is None:
                # 状态行不存在：没有可续跑的 checkpoint；沿用旧处置
                logger.warning(
                    "任务 %s resume 协议错误且执行 %s 无状态行，按既有语义 ack",
                    task.message_id, execution_id,
                )
                self._ack_resume_protocol_error(task, queue, exc)
                return True
            flow = self.get_flow_definition(
                flow_id, getattr(state, "flow_version", None)
            )
            reason = self._resume_carrier_stale_reason(state, flow)
        except Exception as state_exc:  # noqa: BLE001 — 读盘瞬断不丢载体
            logger.warning(
                "任务 %s（execution=%s）resume 协议错误的状态感知失败，"
                "不 ack，消息留 pending 等回收（plaita#258）: %s",
                task.message_id, execution_id, _scrub_exception_text(state_exc),
            )
            return False
        finally:
            reset_current_tenant(token)

        if reason is None:
            self._ack_resume_protocol_error(task, queue, exc)
            return True

        requeue_body = {
            "type": "resume",
            "flow_id": flow_id,
            "execution_id": execution_id,
            "resume_type": "continue",
            "tenant_id": tenant_id,
            "timestamp": datetime.now().isoformat(),
        }
        params = body.get("params")
        if isinstance(params, dict) and params:
            requeue_body["params"] = dict(params)
        try:
            # 先入队、后 ack（与 _requeue_retryable_failure 同款安全序）：
            # 中间崩溃只会产生重复投递（租约串行化），绝不丢消息。
            # A44-02：归一化 continue 副本继承源信封版本。
            queue.enqueue(requeue_body, **_requeue_envelope_kwargs(task))
        except Exception as enqueue_exc:  # noqa: BLE001 — 重投失败退化为留 pending
            logger.warning(
                "任务 %s（execution=%s）归一化 continue 重投失败，不 ack、"
                "消息留 pending 等回收（plaita#258）: %s",
                task.message_id, execution_id, _scrub_exception_text(enqueue_exc),
            )
            return False
        try:
            queue.ack(task.message_id)
        except Exception as ack_exc:  # noqa: BLE001 — ack 失败=重复投递，不致命
            logger.warning(
                "任务 %s（execution=%s）归一化重投后 ack 失败（将产生重复投递，"
                "租约去重）: %s",
                task.message_id, execution_id, _scrub_exception_text(ack_exc),
            )
            return True
        logger.warning(
            "任务 %s 的 %s resume 载体已错过挂起点（%s）；执行 %s 仍 running，"
            "已归一化重投 continue 载体从 checkpoint 续跑（plaita#258）",
            task.message_id, resume_type, reason, execution_id,
        )
        return True

    def run(self, wait_for_inflight: bool = False):
        """
        从 Redis Stream consumer group 拉取 ``start`` / ``resume`` 任务。

        成功处理后 ``XACK``；崩溃或未 ack 的消息留在 pending，超时后可被
        其他 consumer ``XCLAIM`` 回收（at-least-once，业务侧应幂等）。

        ``wait_for_inflight=True``（S18-01）：退出时 ``stop(wait=True)``，
        在看门狗/注销之前有界等待在途步收尾（库模式推荐）；CLI 默认 False，
        以进程退出为硬中止边界。
        """
        # plaita#239 phase 2（owner 2026-10-18 裁决）：单租户部署迁移——
        # 服务入口**显式**声明 default 租户；写路径对隐式 default 已
        # fail-closed（TenantContextMissingError）。每条任务消息仍由
        # `_dispatch_task` 按其 tenant_id 覆盖（显式/derived）。
        set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)
        self._running = True
        queue = self._get_task_queue()
        queue.ensure_group()

        # 队列残留回收（#43）：启动时先扫一次，之后由消费循环按间隔兜底
        self._sweep_residue_if_due(queue)

        # 租约看门狗（波次②）：持租约执行每 TTL/3 续租，防长步被 XCLAIM 抢占
        self._start_lease_watchdog()
        # 取消监听（波次③）：轮询取消标志键，命中即中止在途节点
        self._start_cancel_watcher()

        # 注册服务：失败不放弃——后台按有界退避重试，Redis 恢复后自愈（A2R19-03）
        if self._enable_registry:
            self._ensure_service_registration()
            # 启动控制监听
            self.start_control_listener()

        # #26 观测导出：/metrics 抓取端 + 死信告警 webhook（未配置 = 无操作）
        self._start_metrics_server()
        self._start_alerting()

        logger.info(
            "流程工作器已启动，监听 stream: %s (group=%s, consumer=%s, concurrency=%d)",
            self.queue_name,
            self._consumer_group,
            queue.consumer_name,
            self.concurrency,
        )

        # 并发消费（2026-10-07）：concurrency=1 时原地串行（零行为变化）；
        # >1 时起 N 个消费线程共享本实例。线程各自跑 _consume_loop，靠
        # XREADGROUP 的天然互斥（Redis 保证一条消息只投给一个 consumer 的一次读）
        # 分到不同任务；共享 queue 对象线程安全（redis-py 连接池）。
        threads: list = []
        try:
            if self.concurrency <= 1:
                self._consume_loop(queue)
            else:
                for i in range(self.concurrency):
                    t = threading.Thread(
                        target=self._consume_loop,
                        args=(queue,),
                        name=f"flow-consume-{i}",
                        daemon=True,
                    )
                    t.start()
                    threads.append(t)
                logger.info("已启动 %d 个并发消费线程", len(threads))
                # 主线程等任一消费线程退出（正常只在 stop() 后发生）
                for t in threads:
                    while t.is_alive():
                        t.join(timeout=1.0)
                        if not self._running:
                            break
        finally:
            self.stop(wait=wait_for_inflight)

    def _sweep_residue_if_due(self, queue: RedisStreamTaskQueue) -> None:
        """按间隔兜底回收「已 ack 未 XDEL」的队列残留条目（#43）。

        与消费循环同线程执行，只在到期时（`_last_residue_sweep` 为空 = 启动
        后第一次）扫一轮；并发消费下多线程共享实例，非阻塞锁保证不并发扫。
        队列类已实现 `sweep_acked_residue` 才调用（并行合入期向后兼容——注入/
        子类化的旧队列没有该方法，AttributeError 会炸掉消费线程）。
        """
        interval = self._residue_sweep_interval_seconds
        if interval <= 0:
            return
        sweep = getattr(queue, "sweep_acked_residue", None)
        if not callable(sweep):
            return
        now = time.monotonic()
        last = self._last_residue_sweep
        if last is not None and now - last < interval:
            return
        if not self._residue_sweep_lock.acquire(blocking=False):
            return
        try:
            self._last_residue_sweep = time.monotonic()
            sweep()
        finally:
            self._residue_sweep_lock.release()

    def _disk_guard_blocks_claim(self) -> bool:
        """claim 前的本机盘预检（#49）：True = 本机盘低于守线，本轮不领任务。

        阈值 ``0``（默认）恒 False，零行为变化——与 ``TaskNotForThisWorker``
        那类「领到再让」不同，本闸在 ``XREADGROUP`` **之前**生效：低盘机器
        根本不产生 claim，也就不烧 delivery、不产生 retry-later 回评。

        探测失败（路径不存在 / 无权限）一律放行：宁可能白跑一次，不可因
        预检自身故障让整池 worker 停摆。状态迁移只打一次日志（进入低盘
        与盘回线各一条），避免空转期刷屏。
        """
        min_free = getattr(self, "_min_free_disk_gib", 0.0) or 0.0
        if min_free <= 0:
            return False
        path = getattr(self, "_disk_guard_path", ".") or "."
        free = _free_disk_gib(path)
        if free is None:
            return False
        active = bool(self.__dict__.get("_disk_guard_active"))
        if free >= min_free:
            if active:
                logger.info(
                    "本机可用盘恢复 %.1fGiB ≥ 守线 %.1fGiB（%s），恢复领取任务",
                    free, min_free, path,
                )
                self.__dict__["_disk_guard_active"] = False
                # #42（W10-a）：盘回线立刻撤回「被拦」标记——否则派发侧在
                # 心跳周期内仍按低盘快照拒单（保守方向，但白等一轮额度）。
                self._refresh_registry_capacity()
            return False
        if not active:
            logger.warning(
                "本机可用盘 %.1fGiB < 守线 %.1fGiB（%s）：暂停领取任务，"
                "盘回线自动恢复（不产生 claim，也不烧任务 delivery）",
                free, min_free, path,
            )
            self.__dict__["_disk_guard_active"] = True
            # #42（W10-a）：**这条是缺口核心**——盘闸此前只在本进程内空转，
            # 派发方看不见。进入低盘态即上报，让 capacity 门当轮就避开本机。
            self._refresh_registry_capacity()
        return True

    def _wait_disk_guard(self) -> None:
        """低盘期空转等待：按 ``_disk_guard_poll_seconds`` 休眠后重探。

        切小片休眠以便 ``stop()`` / ``request_drain()`` 能及时中断（低盘
        worker 停机不该再等一个完整轮询周期）。
        """
        deadline = time.monotonic() + getattr(
            self, "_disk_guard_poll_seconds", DEFAULT_DISK_GUARD_POLL_SECONDS
        )
        while self._running and not self._drain_event.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(0.2, remaining))

    def _consume_loop(self, queue: RedisStreamTaskQueue) -> None:
        """单条消费循环（原 run() 主体）。可被 1 或 N 个线程并发执行。

        ``draining``（无损升级）后不再领新任务：循环条件的检查发生在**任务边界**，
        所以在途任务会跑完，随后各消费线程自然退出；残留 sweep 也一并停——
        它属于「领任务」的配套动作，draining 期间不该再触发。
        """
        while self._running and not self._drain_event.is_set():
            # 盘预检（#49）在 sweep 之前：低盘期本进程不做任何与领取相关的
            # 动作（残留 sweep 由池内其他健康 worker 承担），空转等盘回线。
            try:
                if self._disk_guard_blocks_claim():
                    self._wait_disk_guard()
                    continue
                self._sweep_residue_if_due(queue)
                # 分片阻塞读取（2026-09 分布式评审 P2-2）：XREADGROUP 的
                # BLOCK 无法被信号中断出循环，整块 10s 会让 SIGTERM 后的
                # worker 继续抢任务最长 10s。切成 ≤1s 的窗口，停机延迟
                # 上限 ≈1s，空轮询的 Redis 往返开销可忽略。
                task = queue.read(block_ms=min(self.read_block_ms, 1_000))
                if not task:
                    continue
                # S18-01（A45-02）post-read gate：read 在 drain 前发出、drain 后
                # 才返回时，任务已被本 consumer 从 Stream 领走（PEL 有记录）。
                # 此时不得 dispatch/ack/bump——否则停机窗口里又启动一个新步，
                # 且 ack 后进程退出会让该步既没跑完也无 pending 可回收。
                # 消息留在 PEL，由其他 worker 在 claim_min_idle_ms 后 XCLAIM。
                # 该 gate 同时覆盖 read 内部 `_reclaim_one` 返回的孤儿。
                if self._drain_event.is_set():
                    logger.warning(
                        "drain-read-gate：drain 后收到任务 %s（read 已发出），"
                        "不 dispatch、不 ack，消息留 pending 待其他 worker 回收",
                        task.message_id,
                    )
                    break

                self._bump_active(+1)
                acked = False
                # plaita#277：捕获本在执行所属租户世代。订阅存储在节点执行
                # 期间读取该 ContextVar；在途执行跨租户删除/重建后，旧世代
                # 的订阅写会被具名拒绝。task 在 read 时已从 Redis meta 取过
                # 一次；StreamTask 是 frozen dataclass，这里不再回写。
                gen_token = set_current_tenant_generation(task.tenant_generation)
                try:
                    self._dispatch_task(
                        task.body,
                        delivery_count=task.delivery_count,
                        # A44-02：派发侧把源信封版本带到 start 幂等命中的
                        # resume 副本上（同版本时写入口行为不变）。
                        schema_version=task.schema_version,
                        # A69-04：载体年龄的第二数据源（外部 raw XADD 没有
                        # payload timestamp 时由 Stream ID 兜底）。
                        stream_message_id=task.message_id,
                    )
                    queue.ack(task.message_id)
                    acked = True
                except TaskNotForThisWorker as exc:
                    # 任务不亲和本机（repo/run_dir 指向别的机器的路径）：**交接**
                    # 给其他 worker——先入队新副本（delivery 归 1），成功后 ack 原条。
                    # 不能只「留 pending」：对端会按 claim_min_idle_ms 反复 XCLAIM，
                    # 每次让 deliveries +1，两 worker 下 5 轮即触顶 → 误死信
                    # （2026-10-06 双机实测）。交接无损耗，且新副本仍会回到
                    # 共享队列由亲和的 worker 领走。
                    # **有界**：交接次数按 execution_id 计数（AFFINITY_HANDOVER_MAX），
                    # 达上限即终态化 error——否则「重入队使 delivery 恒为 1」会让
                    # 交接永无终止条件（2026-10-11 无限交接风暴，见
                    # _handover_non_affine 与 _terminalize_handover_exhausted）。
                    queue.note_lease_conflict()  # 复用「让给别人」计数口径
                    # S18-03：交接现在返回「原条是否已 ack」——enqueue 失败时
                    # 原条留 pending，acked 必须保持 False 让 finally 的
                    # 「留在 pending」日志与事实一致。
                    acked = self._handover_non_affine(task, queue, exc)
                except TenantQuotaDeferred as exc:
                    # 每租户 start 配额已满（plaita#28）：**延迟**，不是失败。
                    # 入队同体新副本到 stream 尾部（让位给更早入队的其他租户
                    # 消息——这就是单 stream 下的「排队」语义）+ ack 原条。
                    # 不留 pending：XCLAIM 回收每轮让 deliveries +1，烧到
                    # max_deliveries 会误死信——配额已满不是消息的错。
                    # 重投失败 → 退化为留 pending（绝不丢消息）。
                    if self._defer_over_quota_start(task, queue, exc):
                        acked = True
                except TenantGateEpistemicReject as exc:
                    # plaita#326：新 start 的租户状态闸读失败（认识论性，#312
                    # 词汇表）。不 ack、不重投、不收容——消息留 pending 等
                    # XCLAIM 重投，瞬断自愈后下轮过闸正常执行。重投烧
                    # deliveries 至多触死信守卫，#312 修后的守卫对读失败型
                    # 原因同样不收容（留 pending），闭环无终局化出口。
                    queue.note_failed()
                    # #151 卫生：异常字段先投影成标量再进 logger（不裸传）。
                    reject_tenant = str(exc.tenant_id or "default")
                    reject_reason = str(exc.reason)
                    logger.warning(
                        "任务 %s start 的租户状态读失败（tenant=%s reason=%s）"
                        "——载体不动（不 ack 不终局化），留 pending 等重投自愈"
                        "（plaita#326 认识论性拒绝）",
                        task.message_id,
                        reject_tenant,
                        reject_reason,
                    )
                except ExecutionLeaseError as exc:
                    # 另一 worker 持有该执行的租约（A′ 起点后的正常竞争态）：
                    # 持有者**活着**时 ack 释放本消息（2026-10-06 修）。
                    #
                    # 原行为「不 ack 留 pending」的实测问题：持有者跑长节点
                    # （agentrun 分钟级）期间，对端每 claim_min_idle_ms(60s)
                    # XCLAIM 一次，**每次让 Redis deliveries +1** → 烧到超限
                    # 触发死信（实测 delivery 达 22/31），死信守卫虽正确拦下
                    # （租约仍在→跳过）并最终重入队，但产生大量假死信污染 DLQ、
                    # 浪费队列往返。
                    #
                    # 为何 ack 对活持有者是安全的：任务**没有丢**——它在持有者
                    # 手里正常推进。但「租约在」≠「持有者在」：持有者进程死后
                    # 租约键还有 ≤TTL 的尾巴（#50 双机实证：维护窗 SIGTERM 杀
                    # 持有者 → 节点失败重试的重投消息在 60s 被对端回收 → 命中
                    # 本分支被 ack → 非终态执行唯一重投载体被吞，执行停 running
                    # 退化成 2h zombie reap）。故 ack 前先经
                    # ``_lease_conflict_ack_safe`` 核实持有者仍在注册表：
                    # 在册（活持有者）→ ack；不在册（TTL 尾巴）→ 不 ack 留
                    # pending，等租约过期后下一轮 reclaim 正常取得租约从
                    # checkpoint 续跑（代价 ≤TTL+60s，远小于 2h reap）。
                    # 亲和闸路径（TaskNotForThisWorker）保留"交接重入队"是对的：
                    # 那里**没人**能跑该任务，必须留给对端。决议载体
                    # （event/timeout/cancel）另有 A47-F2 守卫：即使持有者在册
                    # 也不 ack——它们不是重复载体，而是挂起执行的唯一唤醒来源。
                    if self._lease_conflict_ack_safe(task.body):
                        queue.note_lease_conflict()
                        queue.ack(task.message_id)
                        acked = True
                        logger.info(
                            "任务 %s 执行被他人持租约且载体无推进语义"
                            "（continue/retry/start，持有者在册），已 ack 释放避免烧 delivery: %s",
                            task.message_id,
                            _scrub_url_text(exc),
                        )
                        body = task.body if isinstance(task.body, dict) else {}
                        if body.get("type") == "resume":
                            # plaita#53：resume 被 ack 丢弃 = **本次救援未生效**。上面
                            # 那句 INFO 只说「消息已释放」，调用方（BFF/值守）看到的仍
                            # 是「恢复请求已加入队列」而执行一行没推进——即工单里的
                            # 「假受理」：值守无法区分「已救活」与「白等」。所以这里
                            # 必须补一句显式的「未生效」+ 可判别的下一步。
                            logger.warning(
                                "任务 %s 是 resume（execution=%s, resume_type=%s），但推进租约"
                                "被在册 worker 持有，本消息已 ack 丢弃——**本次 resume 未生效**"
                                "（不产生任何节点活动）。若该执行的 node_timings / "
                                "last_update_time 长时间不动，说明持有者已卡住：反复 resume 无效，"
                                "需人工 cancel 后按需重开",
                                task.message_id,
                                body.get("execution_id"),
                                body.get("resume_type"),
                            )
                    else:
                        queue.note_lease_conflict()
                        logger.warning(
                            "任务 %s 租约冲突但守卫判定不 ack（持有者已死/停机中，"
                            "或载体是 event/timeout/cancel 决议载体），消息留 pending "
                            "等租约释放后重投，防唯一载体被吞 (#50/A46-04): %s",
                            task.message_id,
                            _scrub_url_text(exc),
                        )
                except NodeExecutionRetryableError as exc:
                    # 节点级可重试失败（波次二任务①）：执行状态停在最后成功
                    # 步 checkpoint，不计 poison。预算（重试计数键）已在处理
                    # 函数内判定，耗尽时已在处理函数内终态化 error，不会走到
                    # 这里无限重投。
                    queue.note_failed()
                    # 2026-10-09 修复（plaita#52 真因）：**显式重投**，不再把
                    # 重投的成败押在「消息仍在 PEL」上——同执行可能已被竞争者的
                    # ExecutionLeaseError 分支 ack 释放（"避免烧 delivery"），
                    # 此时 reclaim 无条目可回收，重投永不发生而执行又不终态化，
                    # 结果卡在 running 直到 zombie 线/人工 resume（实测 58 分钟）。
                    # plaita#91（2026-10-16）：重投带**指数退避**（1s/2s/4s/8s…
                    # 封顶 30s，attempt 取 exc.attempt——旧实现无任何节奏，
                    # 5 次预算 ~40ms 烧光）；start 载体且盘上已有 checkpoint
                    # 时转换为 **resume 载体**，从 checkpoint 续跑，不再整条
                    # 重放/覆写 checkpoint（见 _requeue_retryable_failure）。
                    if self._requeue_retryable_failure(task, queue, exc):
                        acked = True
                        logger.warning(
                            "任务 %s 节点执行失败：已退避后显式重投新副本"
                            "（载体 type=%s，原 delivery=%s）: %s",
                            task.message_id,
                            task.body.get("type") if isinstance(task.body, dict) else "?",
                            task.delivery_count,
                            _scrub_url_text(exc),
                        )
                    else:
                        logger.warning(
                            "任务 %s 节点执行失败待重投 (delivery=%s): %s",
                            task.message_id,
                            task.delivery_count,
                            _scrub_url_text(exc),
                        )
                except ResumeProtocolError as exc:
                    # resume 协议错误（#33）：执行状态与 resume_type 不匹配（如
                    # continue 打在挂起执行上）。执行**未**终态化、保持原状——
                    # 错误已可观测（状态行/日志），消息重投只会重复命中同一守卫
                    # 且烧 delivery，ack 掉。
                    #
                    # plaita#258：旧注释的前提「非终态执行不需要重投载体：决议
                    # 路径（event/cancel/timeout resume）各有独立消息」只在执行
                    # 仍停在**可决议的挂起点**时成立。event/timeout resume 已经
                    # 消费并注销订阅、流内又推进并落过 step_persist（status=
                    # running、$LAST_NODE 非挂起点）后，这条消息就是唯一载体；
                    # 直接 ack 会把执行永久留在 running。故 ack 前先做状态感知：
                    # 仍可续跑 ⇒ 归一化重投 continue 副本；真 #33 错配/终态/
                    # 挂起 ⇒ 原样 ack。
                    acked = self._handle_resume_protocol_error(task, queue, exc)
                except TenantWriteRejected as rejected:
                    # plaita#282：派发过程中（如订阅写入）的旧世代拒写不是
                    # 消息畸形——不 ack、不 poison（否则唯一恢复载体被吞），
                    # 计数 + ERROR 后留 pending，循环继续；后续正常消息照常处理。
                    rejected_tenant = str(rejected.tenant_id or "?")
                    rejected_reason = str(rejected.reason or "unknown")
                    queue.note_tenant_write_rejected()
                    logger.error(
                        "任务 %s 旧世代写拒绝（留 pending，继续消费）: "
                        "tenant=%s reason=%s: %s",
                        task.message_id,
                        rejected_tenant,
                        rejected_reason,
                        _scrub_url_text(rejected),
                    )
                except ValueError as exc:
                    # 畸形消息：ack 掉避免 poison pill 无限重投
                    logger.error("丢弃无效任务 %s: %s", task.message_id, _scrub_url_text(exc))
                    queue.ack(task.message_id)
                    queue.note_poison()
                    acked = True
                except Exception as exc:
                    # 兜底重投递路径（at-least-once）：不 ack，等超时回收。
                    # 刻意放在 except ValueError 之后——存储层抛出的
                    # ExecutionStateLoadError（读取瞬断/状态损坏，ReviewFix D1）
                    # 由此处理：消息留在 pending 重投，超过 max_deliveries 才
                    # 进 DLQ；绝不能像 ValueError 一样被当畸形消息 poison ack，
                    # 否则 Redis 抖动一次 = 挂起执行永久失去恢复机会。
                    queue.note_failed()
                    if task.delivery_count >= queue.max_deliveries:
                        reason = f"processing_failed:{type(exc).__name__}:{exc}"[:500]
                        # plaita#252：死信前必须过与 _read_once/_reclaim_one
                        # 相同的守卫（队列层 dead_letter_if_allowed 单一入口）。
                        # 本分支恰是 ExecutionStateLoadError/StatePersistError
                        # 等瞬态失败的落点：守卫拒绝（活租约/状态读取瞬断）时
                        # **不得 ack**——消息留 pending，否则唯一恢复载体被
                        # ack 出 PEL；守卫 None 恒放行，存量行为零变化。
                        if queue.dead_letter_if_allowed(task, reason=reason):
                            acked = True
                        else:
                            logger.warning(
                                "任务 %s（execution=%s）处理失败且 delivery=%s/%s "
                                "已触顶，但死信守卫拒绝/异常（执行仍可恢复）；"
                                "不 ack，消息留 pending 等下一轮处置: %s",
                                task.message_id,
                                task.body.get("execution_id")
                                if isinstance(task.body, dict)
                                else None,
                                task.delivery_count,
                                queue.max_deliveries,
                                _scrub_exception_text(exc),
                            )
                    else:
                        logger.error(
                            "任务处理失败 %s (delivery=%s/%s)，未 ack（将重投）: %s",
                            task.message_id,
                            task.delivery_count,
                            queue.max_deliveries,
                            _scrub_exception_text(exc),
                        )
                finally:
                    reset_current_tenant_generation(gen_token)
                    self._bump_active(-1)
                    if not acked:
                        logger.debug("任务 %s 留在 pending 等待回收", task.message_id)
            except TenantWriteRejected as rejected:
                # plaita#282：#277 的世代拒写是 fail-closed 的写闸，不是消费
                # 循环故障。残留的旧世代超投递载体可能在 read() 内部被拒；这里
                # 兜底计数 + 短退避继续，绝不退出消费线程/进程。消息留 pending
                # （dead_letter_if_allowed 已不 ack），等有界年龄闸/人工处置。
                rejected_tenant = str(rejected.tenant_id or "?")
                rejected_reason = str(rejected.reason or "unknown")
                queue.note_tenant_write_rejected()
                logger.error(
                    "消费循环捕获旧世代写拒绝（消息留 pending，继续消费）: "
                    "tenant=%s reason=%s: %s",
                    rejected_tenant, rejected_reason, _scrub_url_text(rejected),
                )
                time.sleep(0.05)
                continue
            except (RedisError, socket.timeout) as exc:
                # #48：**消费循环级兜底，绝不退出进程**。上面的 read() 已就地吞掉
                # 瞬时故障族；但循环体内还有 ack/handover/sweep 等 Redis 调用，
                # 任何一处冒出的 Redis 异常若穿出本函数，就会经 run()（无 except）
                # 走到 main() 的 sys.exit(1) —— worker 进程整体退出、在跑 run 的
                # 监视线程全断，新实例启动又 XCLAIM 存量引发重投风暴。
                # 故在**循环最外层**兜住：记 error + 退避 + continue，Redis 恢复
                # 后同一循环继续消费（停机语义不变：while 条件仍在下一轮检查）。
                # 注：`socket.timeout`（py3.10+ 即内建 TimeoutError）**不是**
                # `RedisError` 的子类，必须与它并列 catch —— 否则从别处冒出的 socket
                # 超时仍会杀死进程（我第一版就是漏了这一支，被守卫测试当场逮住）。
                logger.error("消费循环遇到 Redis 故障（退避 %.1fs 后继续轮询）: %s",
                             RECONNECT_BACKOFF_SECONDS, _scrub_url_text(exc))
                time.sleep(RECONNECT_BACKOFF_SECONDS)
                continue

    def _bump_active(self, delta: int) -> None:
        """原子调整在跑任务数并刷新注册表 active_tasks（并发下多线程安全）。"""
        with self._active_count_lock:
            self._active_task_count = max(0, self._active_task_count + delta)
            count = self._active_task_count
        if self._enable_registry and not self._registry_is_closed():
            # 内存计数照常（metrics/join 需要）；closed 后只到这里为止。
            self.update_registry_info(active_tasks=count)
            # #42（W10-a）：容量门按**新鲜**快照判饱和，故在跑数一变就直推一次
            # 注册键（不等心跳周期）——否则最多 CAPACITY_STALE 级的窗口里，
            # 派发侧会按「上一轮在跑 0 条」把并发额度压给同一台机器。
            # S18-02A：draining/stopping/stopped 由 _refresh_registry_capacity
            # 自行跳过（容量上报仅 running；starting 仅构造期预上报）。
            self._refresh_registry_capacity()

    # ------------------------------------------------- #42 worker 容量上报

    def capacity_snapshot(self) -> Dict[str, Any]:
        """本机当前容量快照（纯函数式取值，不写注册表）。

        ``free_disk_gib`` 为 ``None`` = 盘探针失败/未启用盘闸路径不可读——与
        #49 盘闸同口径「探不到就放行」，**不**写成 0（0 会被派发侧当成
        「真的没盘」而拒单，把探针故障放大成停摆）。
        """
        block = build_capacity_block(
            free_disk_gib=_free_disk_gib(getattr(self, "_disk_guard_path", ".") or "."),
            min_free_disk_gib=getattr(self, "_min_free_disk_gib", 0.0) or 0.0,
            disk_blocked=bool(self.__dict__.get("_disk_guard_active")),
            active_tasks=getattr(self, "_active_task_count", 0) or 0,
            concurrency=getattr(self, "concurrency", 1) or 1,
            config_family=getattr(self, "_config_family", None),
            config_generation=getattr(self, "_config_generation_id", None),
            budget_generation=getattr(self, "_budget_generation", None),
            deployment_epoch=(
                self._runtime_config.deployment_epoch
                if getattr(self, "_runtime_config", None) is not None
                else None
            ),
        )
        return block

    def _refresh_registry_capacity(self) -> None:
        """把最新容量快照写进注册键（失败只告警，绝不打断消费循环）。

        复用 ``ServiceRegistry.heartbeat``：同键、同 TTL、同 JSON 形状，只是
        立刻刷一次而不是等心跳线程。注册未启用 / 未初始化 / Redis 瞬断
        → 静默降级为「无容量信息」（派发侧放行，与存量行为一致）。
        """
        if not getattr(self, "_enable_registry", False):
            return
        if self._registry_is_closed():
            # 注销后心跳/容量都不得把键写回（A46-03）；在跑数只更新内存。
            return
        registry = getattr(self, "_registry", None)
        service_info = getattr(self, "_service_info", None)
        if registry is None or service_info is None:
            return
        status = getattr(service_info, "status", "")
        if status not in ("starting", "running"):
            # S18-02A：容量上报仅 running（starting 只允许构造期的一次预上报）。
            # draining/stopping/stopped 继续写容量会让派发侧看到「状态停机但
            # 容量新鲜」的漂移快照，也没有任何派发价值。
            logger.debug("容量上报跳过：注册状态 %s 不允许刷新容量", status)
            return
        try:
            service_info.metadata[CAPACITY_METADATA_KEY] = json.dumps(
                self.capacity_snapshot(), ensure_ascii=False
            )
            registry.heartbeat(service_info)
        except Exception as exc:  # noqa: BLE001 — 上报失败不得影响领取/执行
            logger.warning("worker 容量上报失败（注册表按无容量信息处理）: %s", _scrub_exception_text(exc))

    def _flush_registry_status(self, status: str) -> bool:
        """S18-02A：生命周期状态**立即**落盘（不再等 10s 心跳）。

        ``flush_registry_status`` 内含 2 次短退避重试；仍失败则 ERROR +
        ``registry_status_flush_failed_total`` 指标——不得静默。失败不阻断
        drain/stop 流程（本地状态已经是权威，派发侧最坏在 TTL 内看到旧状态）。
        """
        if not getattr(self, "_enable_registry", False):
            return True
        if self._registry_is_closed():
            return True
        if getattr(self, "_registry", None) is None:
            return True
        try:
            ok = self.flush_registry_status()
        except Exception as exc:  # noqa: BLE001 — 落盘失败绝不能阻断停机
            logger.warning("注册状态 %s 立即落盘异常: %s", status, _scrub_exception_text(exc))
            ok = False
        if ok:
            return True
        self._registry_status_flush_failures = (
            getattr(self, "_registry_status_flush_failures", 0) + 1
        )
        logger.error(
            "注册状态 %s 立即落盘失败（已重试）：派发侧可能在 TTL 内继续按旧状态选择本机",
            status,
        )
        return False

    def _report_capacity(self) -> None:
        """``__init__`` 期的容量上报入口（注册表尚未 start_heartbeat 时直推一次）。"""
        self._refresh_registry_capacity()

    # ------------------------------------------- 启动期注册重试（A2R19-03）

    def _ensure_service_registration(self) -> bool:
        """启动期注册一次；失败则启动后台有界退避重试。

        ``register_service()`` 内部吞掉 Redis 异常、失败只返回 False。启动
        瞬间 Redis 不可达时，消费循环的读路径会自愈（``read()`` 对瞬时故障
        退避重试），注册路径不会——进程会**永久**缺席注册表。失败后由重试
        线程补注册；``register_service()`` 成功时会顺带 ``start_heartbeat``。
        """
        if not getattr(self, "_enable_registry", False):
            return True
        if self.register_service():
            self._registry_registered = True
            return True
        self._start_registry_retry()
        return False

    def _start_registry_retry(self) -> None:
        """启动注册重试线程（幂等；已存活则复用）。"""
        stop_event = getattr(self, "_registry_retry_stop", None)
        if stop_event is None:
            stop_event = threading.Event()
            self._registry_retry_stop = stop_event
        thread = getattr(self, "_registry_retry_thread", None)
        if thread is not None and thread.is_alive():
            return
        stop_event.clear()
        self._registry_retry_thread = threading.Thread(
            target=self._registry_retry_loop,
            name="plaita-registry-retry",
            daemon=True,
        )
        self._registry_retry_thread.start()
        logger.warning(
            "worker 启动期服务注册失败：已进入后台重试（退避 %.0fs→%.0fs，"
            "上限封顶）；Redis 恢复后自动注册并启动心跳",
            REGISTRY_RETRY_INITIAL_SECONDS,
            REGISTRY_RETRY_MAX_SECONDS,
        )

    def _registry_retry_loop(self) -> None:
        """指数退避重试注册，直到成功或 worker 停止。

        失败日志由 ``ServiceRegistry.register()`` 逐次打 ERROR；本循环只按
        退避节奏重试（封顶 30s，Redis 全断时不刷屏），成功后回到心跳线程。
        """
        stop_event = self._registry_retry_stop
        delay = REGISTRY_RETRY_INITIAL_SECONDS
        attempt = 0
        while not stop_event.wait(delay):
            if not getattr(self, "_running", False):
                return
            attempt += 1
            try:
                ok = self.register_service()
            except Exception as exc:  # noqa: BLE001 — 重试线程不能因单次异常退出
                logger.warning(
                    "worker 注册重试第 %d 次异常: %s", attempt, _scrub_exception_text(exc)
                )
                ok = False
            if ok:
                self._registry_registered = True
                logger.warning(
                    "worker 服务注册已恢复（第 %d 次重试）：注册键已写入、心跳线程已启动",
                    attempt,
                )
                return
            delay = min(delay * 2, REGISTRY_RETRY_MAX_SECONDS)

    def _stop_registry_retry(self) -> None:
        """停止注册重试线程（stop() 调用；先于 unregister，避免退出后又被注册）。"""
        stop_event = getattr(self, "_registry_retry_stop", None)
        if stop_event is not None:
            stop_event.set()
        thread = getattr(self, "_registry_retry_thread", None)
        if (
            thread is not None
            and thread is not threading.current_thread()
            and thread.is_alive()
        ):
            thread.join(timeout=2.0)
        self._registry_retry_thread = None

    @property
    def registry_registered(self) -> bool:
        """本进程当前是否已在注册表中（重试成功前为 False）。"""
        return bool(getattr(self, "_registry_registered", False))

    @property
    def _drain_event(self) -> threading.Event:
        """draining 标志（惰性创建）。

        某些路径会以 ``__new__`` 构造「骨架 worker」（例如只测消费循环调度的单测），
        不走 ``__init__``。把状态做成惰性属性后，这些路径同样安全——否则会在
        循环条件里直接 AttributeError。
        """
        event = self.__dict__.get("_draining")
        if event is None:
            event = threading.Event()
            self.__dict__["_draining"] = event
        return event

    @property
    def _force_stop_event(self) -> threading.Event:
        """D2 强停标记（惰性创建，``__new__`` 骨架 worker 同样安全）。

        与 ``_drain_event``（D1：只停止领取）区分：退避重投只在 D2 被打断；
        仅请求 drain 时在途步必须完整跑完（S18-01 I5）。
        """
        event = self.__dict__.get("_force_stopping")
        if event is None:
            event = threading.Event()
            self.__dict__["_force_stopping"] = event
        return event

    @property
    def draining(self) -> bool:
        return self._drain_event.is_set()

    def request_drain(self, reason: str = "manual") -> None:
        """进入 draining（D1）：不再领新任务、注册表标 draining，并启动有界等待。

        有界是关键：单个节点步可能跑几十分钟（如 HITL gate），等它做完等于把
        停机窗口拉到不可接受。``PLAITA_WORKER_DRAIN_TIMEOUT`` 是**停止领取/
        停止等待**的上界（advisory），不是「节点中止」的上界：超时后
        ``_force_stop_after_drain`` 尽力 cancel 在途执行并让 run() 收尾，但不
        承诺节点在 N 秒内停下。若步在进程退出前完成，仍允许 ack；若进程先
        退出，消息留 pending 由其他 worker 经 XCLAIM 从**步界检查点**续跑
        （那一步会重放，业务节点须幂等）。硬中断是 B18-01，未实现。
        """
        with self._active_count_lock:
            if self._drain_event.is_set():
                return
            self._drain_event.set()
            self._drain_started_at = time.time()
        if self._enable_registry and not self._registry_is_closed():
            # 单一状态源：register() 之前（starting）也要让 drain 生效，
            # 否则 update_service_info 在 _registered_service is None 时直接
            # return，内存状态都没置上（S9 前置坑）。
            info = getattr(self, "_service_info", None)
            if info is not None:
                info.status = "draining"
            # 注册表可见：编排/K8s 就绪探针据此把这台从流量里摘掉
            self.update_registry_info(status="draining")
            # 立即落盘（S18-02A / A45-03）：不再等 10s 心跳，否则派发侧在
            # 这个窗口里仍按 running 选中本机（探针 S3）。
            self._flush_registry_status("draining")
        timeout = _drain_timeout_from_env()
        logger.info(
            "worker 进入 draining（%s）：不再领新任务，等待 %d 个在途任务收尾（上限 %.0fs）",
            reason,
            self._active_task_count,
            timeout,
        )
        if timeout <= 0:
            self._force_stop_after_drain(timeout)
            return
        timer = threading.Timer(timeout, self._force_stop_after_drain, args=(timeout,))
        timer.daemon = True
        self._drain_timer = timer
        timer.start()

    def _force_stop_after_drain(self, timeout: float = 0.0) -> None:
        """drain 超时兜底（D2）：停止等待、尽力取消在途执行，让 run() 收尾。

        两个必须点（S18-01/A45-01+A45-02）：
        - **无论 active 是否为 0 都置 ``_running=False``**。旧实现的
          ``active <= 0: return`` 让「定时器先到、任务恰好收尾」的窗口里
          ``_running`` 保持 True，消费线程继续 read/dispatch（探针
          ``[new-task-after-drain]``）。
        - 对在途执行做 best-effort cancel（复用取消监听的登记表）；cancel 不
          保证节点立刻中止（B18-01 未落地），步在进程退出前完成仍可能 ack。
        """
        if not self._drain_event.is_set():
            return
        # D2 标记先置位：可中断退避（A46-06）据此当场放弃 enqueue/ack。
        self._force_stop_event.set()
        active = self._active_task_count
        self._running = False
        if active <= 0:
            logger.info(
                "drain 超时（%.0fs）：无在途任务，停止领取并退出", timeout
            )
            return
        logger.warning(
            "drain 超时（%.0fs）仍有 %d 个任务在跑：停止等待并尽力取消在途执行；"
            "不承诺节点在 N 秒内中止（硬中断见 B18-01）。若进程在步返回前退出，"
            "消息留 pending，由其他 worker XCLAIM 后从步界检查点续跑（该步会重放）",
            timeout,
            active,
        )
        self._cancel_inflight_after_drain()

    def _cancel_inflight_after_drain(self) -> int:
        """best-effort 取消在途执行（D2）；返回实际调用 cancel 的数量。

        只读快照登记表后在锁外调用（与取消监听同款），避免 cancel 回调里
        再取 ``_cancel_watch_lock`` 造成自锁。租户上下文按登记项恢复，确保
        cancel 内的存储/标志键写入落在正确 namespace。
        """
        lock = getattr(self, "_cancel_watch_lock", None)
        if lock is None:
            return 0
        with lock:
            entries = list(getattr(self, "_cancel_watch", {}).items())
        cancelled = 0
        for execution_id, entry in entries:
            try:
                execution, tenant_id = entry
            except (TypeError, ValueError):
                continue
            cancel = getattr(execution, "cancel", None)
            if not callable(cancel):
                continue
            token = set_current_tenant(tenant_id)
            try:
                cancel()
            except Exception as exc:  # noqa: BLE001 — 取消失败仍有步界兜底
                logger.warning(
                    "drain-force-stop-cancel 失败 execution_id=%s: %s",
                    execution_id, _scrub_exception_text(exc),
                )
                continue
            finally:
                reset_current_tenant(token)
            cancelled += 1
            logger.warning(
                "drain-force-stop-cancel execution_id=%s（best-effort，"
                "不保证节点立即中止）",
                execution_id,
            )
        return cancelled

    def wait_for_idle(self, timeout: Optional[float] = None) -> bool:
        """等所有在途任务结束；超时返回 False（调用方决定是否强退）。"""
        deadline = None if timeout is None else time.time() + max(0.0, timeout)
        while self._active_task_count > 0:
            if deadline is not None and time.time() >= deadline:
                return False
            time.sleep(0.05)
        return True

    def stop(self, wait: bool = False):
        """停止流程工作器。

        顺序对无损升级很重要：先 draining（摘流量：注册表标 draining + 停止
        领新任务）→ 等 in-flight 收尾（有界）→ 再注销服务。反过来的话，注销
        之后仍可能领到任务，编排会看到「已下线但仍消费」的矛盾状态。

        ``wait=True``（S18-01/B45-06）：在停看门狗/注销之前**有界**等待在途
        任务收尾（预算沿用 ``PLAITA_WORKER_DRAIN_TIMEOUT`` 的剩余量），库模式
        调用方应传 True；CLI 默认 False——进程退出即硬中止，daemon 消费线程
        可能仍在跑，未完成步的消息留 pending 由 XCLAIM 重放。无论哪种模式，
        本方法都会置 D2 标记，可中断退避据此不再 enqueue/ack。
        """
        if self._drain_timer is not None:
            self._drain_timer.cancel()
            self._drain_timer = None
        if self._running and not self._drain_event.is_set():
            # 直接 stop（非 drain 路径）也保证「先摘流量再退出」
            with self._active_count_lock:
                self._drain_event.set()
                self._drain_started_at = self._drain_started_at or time.time()
            if self._enable_registry and not self._registry_is_closed():
                info = getattr(self, "_service_info", None)
                if info is not None:
                    info.status = "draining"
                self.update_registry_info(status="draining")
                self._flush_registry_status("draining")
        logger.info("正在停止流程工作器...")
        self._running = False
        # D2/D3：退避重投不再 enqueue/ack；未完成消息留 pending。
        self._force_stop_event.set()
        if wait:
            budget = _drain_timeout_from_env()
            started = getattr(self, "_drain_started_at", None)
            if started is not None:
                budget = max(0.0, budget - (time.time() - started))
            if not self.wait_for_idle(budget):
                logger.warning(
                    "stop(wait=True) 在 %.0fs 内未等齐在途任务（active=%d）："
                    "继续收尾退出；未完成步的消息留 pending，由 XCLAIM 重放",
                    budget, self._active_task_count,
                )

        # 停止注册重试（A2R19-03）：先于 unregister，避免退出后又被重试线程写回
        self._stop_registry_retry()

        # 停止租约看门狗（波次②）
        self._stop_lease_watchdog()

        # 停止取消监听（波次③）
        self._stop_cancel_watcher()

        # 停止 #26 观测导出（抓取端线程 + 告警 webhook 后台线程）
        self._stop_metrics_server()
        self._stop_alerting()

        # 停止控制监听
        if self._enable_registry:
            self.stop_control_listener()
        
        # 注销服务（先标 stopping：编排侧能看到「正在下线」而非突然消失）
        if self._enable_registry:
            info = getattr(self, "_service_info", None)
            if info is not None:
                info.status = "stopping"
            self.update_registry_info(status="stopping")
            # S18-02A：stopping 也要**立即**落盘（不再等心跳）。
            self._flush_registry_status("stopping")
            # 写完 stopping 之后、注销之前置 closed：这一次 flush 是最后一次
            # 注册表写入；之后心跳/容量/迟到 bump 全部 no-op（A46-03）。
            self._mark_registry_closed()
            if not self.unregister_service():
                logger.error(
                    "registry_unregister_failed: 服务注销未完全成功"
                    "（instance=%s；stale 键由 stopped 墓碑/TTL 兜底）",
                    getattr(self, "instance_id", None),
                )
        
        # 关闭日志处理器
        if self._log_handler:
            self._log_handler.close()
        
        logger.info("流程工作器已停止")
    
    def _on_stop_command(self, graceful: bool):
        """响应停止命令。

        优雅路径走 ``request_drain``：不再领新任务、等 in-flight 收尾，
        超时由 drain 定时器兜底退出；非优雅路径直接 ``stop()``（尽快退出，
        消息留 pending 待接管）。
        """
        logger.info("收到远程停止命令，优雅停止: %s", graceful)
        if graceful:
            self.request_drain("remote stop command")
        else:
            self.stop()
    
    def _on_status_command(self) -> Dict[str, Any]:
        """响应状态查询命令"""
        status = {
            "status": (
                "draining" if self._drain_event.is_set()
                else ("running" if self._running else "stopped")
            ),
            "draining": self._drain_event.is_set(),
            "active_tasks": self._active_task_count,
            "queue_name": self.queue_name,
            "consumer_group": self._consumer_group,
            "consumer_name": self._resolve_consumer_name(),
        }
        try:
            status["queue"] = self._get_task_queue().stats()
        except Exception as exc:
            status["queue_error"] = str(exc)
        return status

    # ---------------------------------------------------------- #26 观测导出

    def metrics_text(self) -> str:
        """Prometheus 文本快照（``/metrics`` 每次抓取时调用）。

        队列 stats 读失败（Redis 瞬断）不整单 500：退化为只报 worker 自身
        指标——抓不到队列读数本身也是信号，不该让整张看板空白。
        """
        try:
            stats = self._get_task_queue().stats()
        except Exception as exc:
            logger.warning("读取队列 stats 失败（/metrics 仅返回 worker 指标）: %s", _scrub_url_text(exc))
            stats = {"stream_key": self.queue_name}
        metrics = collect_queue_metrics(stats)
        service_info = getattr(self, "_service_info", None)
        heartbeat = None
        if service_info is not None and service_info.last_heartbeat:
            try:
                heartbeat = datetime.fromisoformat(service_info.last_heartbeat).timestamp()
            except ValueError:
                heartbeat = None
        metrics.extend(collect_worker_metrics(
            active_tasks=self._active_task_count,
            draining=self._drain_event.is_set(),
            running=self._running,
            heartbeat_timestamp=heartbeat,
            instance=self._resolve_consumer_name(),
            registered=service_info is not None,
            dedup_discarded_total=getattr(
                self, "_dedup_discarded_total", 0
            ),
        ))
        # S18-02A：draining/stopping 立即落盘失败必须可观测（不得静默）。
        metrics.append(Metric(
            "registry_status_flush_failed_total",
            getattr(self, "_registry_status_flush_failures", 0),
            {},
            "counter",
            "生命周期状态立即落盘失败次数（S18-02A）",
        ))
        # A69-04：载体年龄两源都不可判 → fail-closed 丢弃次数（可能意味着
        # 某个生产者既没写 timestamp、消息也不来自 Redis Stream ID）。
        metrics.append(Metric(
            "carrier_age_unknown_total",
            getattr(self, "_carrier_age_unknown_total", 0),
            {},
            "counter",
            "start/resume 载体年龄无法判定而 fail-closed 丢弃的次数（A69-04）",
        ))
        # A2R31-02：租户状态纪元拒绝次数（进程内；Redis-only 恢复后非 default
        # 租户被拒的直接信号）。
        from plaita.server.tenant_status import tenant_stale_epoch_total

        metrics.append(Metric(
            "tenant_status_stale_epoch_total",
            tenant_stale_epoch_total(),
            {},
            "counter",
            "租户状态不属于当前部署纪元而被拒绝的次数（A2R31-02）",
        ))
        # #42（W10-a）：容量指标与「派发侧看到的容量」同源（同一快照函数），
        # 否则看板与派发门会各报一套（口径分叉是最难查的一类事故）。
        try:
            capacity_block = self.capacity_snapshot()
        except Exception as exc:  # noqa: BLE001 — 容量探针故障不该打空整张看板
            logger.debug("容量快照失败（本次 /metrics 不含容量指标）: %s", _scrub_exception_text(exc))
            capacity_block = None
        if capacity_block is not None:
            metrics.extend(collect_capacity_metrics(
                capacity_block, instance=self._resolve_consumer_name()
            ))
        if self._alerter is not None:
            alert_stats = self._alerter.stats()
            for key, help_text in (
                ("sent", "已成功投递的告警数"),
                ("failed", "投递失败的告警数"),
                ("dropped", "告警队列满而丢弃的事件数"),
                ("pending", "告警队列中待投递的事件数"),
            ):
                metrics.append(Metric(
                    f"alert_webhook_{key}_total" if key != "pending" else "alert_webhook_pending",
                    alert_stats[key],
                    {},
                    "counter" if key != "pending" else "gauge",
                    help_text,
                ))
        return render_prometheus(metrics)

    def _start_metrics_server(self) -> None:
        """按配置起 ``/metrics`` 抓取端（未配置端口 = 无操作）。"""
        if self._metrics_port <= 0 or self._metrics_server is not None:
            return
        try:
            self._metrics_server = MetricsHttpServer(
                self.metrics_text, host=self._metrics_host, port=self._metrics_port
            )
            self._metrics_server.start()
        except Exception as exc:  # noqa: BLE001 - 端口占用等不得拖垮 worker 启动
            self._metrics_server = None
            logger.error("启动 /metrics 抓取端失败（端口 %s）: %s", self._metrics_port, _scrub_url_text(exc))

    def _stop_metrics_server(self) -> None:
        if self._metrics_server is not None:
            server, self._metrics_server = self._metrics_server, None
            server.stop()

    # ------------------------------------------------------------------ 告警

    def _start_alerting(self) -> None:
        """配置了 webhook 才建 alerter（未配置 = 死信只走日志，存量行为）。"""
        if self._alert_webhook and self._alerter is None:
            self._alerter = WebhookAlerter(self._alert_webhook)
            # plaita#159：webhook 的凭据就在 path/query 里（Slack/飞书/钉钉/企微），
            # 所以「能不能读这行日志」等价于「能不能往该 webhook 投任意内容」。
            # ``self._alert_webhook`` 本体仍持原文（要 POST），只有渲染进日志的这一份
            # 被抹成 scheme://host + 占位。
            logger.info(
                "死信告警 webhook 已启用: %s",
                _mask_webhook_url_for_log(self._alert_webhook),
            )

    def _stop_alerting(self) -> None:
        if self._alerter is not None:
            alerter, self._alerter = self._alerter, None
            alerter.close()

# 新增命令行入口


def _request_graceful_stop(worker: "RedisFlowWorker", signum: int) -> None:
    """信号处理器入口：只请求优雅停机，绝不立即退出进程（ReviewFix D3）。

    旧实现在 handler 里 ``sys.exit(0)``——SystemExit 会在 ``_dispatch_task``
    的任意字节码边界抛出（可能正卡在 LLM 调用/存储写入之间），在途任务当场
    腰斩、消息未 ack，退化为 crash 式重投；与控制通道停止命令的 drain 语义
    （``_on_stop_command`` 只置位、等当前任务做完）不一致。现在 handler 只调
    ``worker.stop()`` 置位，由 ``run()`` 主循环在任务边界自然退出——read 已切
    成 ≤1s 分片，空转停机延迟上限 ≈1s；强制退出兜底交给 systemd/K8s 的
    SIGKILL 超时。
    """
    logger.info("收到信号 %s，请求优雅停机（当前任务完成后退出）...", signum)
    worker.request_drain(f"signal {signum}")


from plaita.server.factory import create_storage_component, create_event_bus  # noqa: F401


def _cli_startup_problems(args) -> list:
    """#269/#270: reject CLI values that bypass startup fail-fast (B1 r52).

    - ``--lease-ttl-seconds`` / ``--max-deliveries`` below 1: the env knobs
      already fail fast; a CLI 0/negative must not be silently clamped to 1.
    - ``--consumer-group`` / ``--dlq-key``: producers (console/schedule/
      event_filter) derive ``config_family`` from env only. A worker-only CLI
      override makes the family gate reject every foreign task (no ack/DLX,
      pending until the 30-day age gate). Require the whole deployment to use
      the env value; a matching CLI value is accepted.
    """
    problems = []
    # #275：判定前把 CLI 值规范化并**写回实际使用的 args**。此前只 strip 比较
    # 副本，``--consumer-group "plaita-workers "`` 会被放行而 worker 实际加入
    # 带空格的独立 consumer group（同一消息两组各投一次）；DLQ/queue 同理。
    # family 的 canonical_json 又会 strip，哈希相同但 Redis 键/组不同，更隐蔽。
    args.queue_name = str(args.queue_name or "").strip()
    args.consumer_group = str(args.consumer_group or "").strip()
    args.dlq_key = str(args.dlq_key or "").strip()
    invalid_budget_keys = set()
    if int(args.lease_ttl_seconds) < 1:
        problems.append(
            f"--lease-ttl-seconds: {args.lease_ttl_seconds} < minimum 1 "
            "(resume lease TTL; 0/negative makes acquire/renew invalid, and a "
            "silent clamp to 1 leaves no renewal headroom)"
        )
        invalid_budget_keys.add("lease_ttl_seconds")
    if int(args.max_deliveries) < 1:
        problems.append(
            f"--max-deliveries: {args.max_deliveries} < minimum 1 "
            "(a silent clamp to 1 dead-letters on the first failure)"
        )
        invalid_budget_keys.add("max_deliveries")

    # #272：生产者（console/schedule/event_filter）只从 env 算 budget_generation。
    # worker 的 CLI 预算覆盖若与 env-resolved 值不同，新任务信封仍带 env 预算；
    # 首投可执行，但失败/崩溃留下的 PEL 会被 worker 按不同预算判 stale，永久
    # 跳过 reclaim（直到 30 天年龄闸）。要求预算四键全部署同源：不一致即拒启。
    producer_budget = envelope_budget_values()
    worker_budget = {
        "lease_ttl_seconds": int(args.lease_ttl_seconds),
        "max_deliveries": int(args.max_deliveries),
        "task_max_age_seconds": task_max_age_seconds(),
        "dlq_max_len": _dlq_max_len_from_env(),
    }
    budget_labels = (
        ("lease_ttl_seconds", "--lease-ttl-seconds", "PLAITA_LEASE_TTL_SECONDS"),
        ("max_deliveries", "--max-deliveries", "PLAITA_MAX_DELIVERIES"),
        ("task_max_age_seconds", "task_max_age_seconds", "PLAITA_TASK_MAX_AGE_SECONDS"),
        ("dlq_max_len", "dlq_max_len", "PLAITA_DLQ_MAX_LEN"),
    )
    for key, label, env_key in budget_labels:
        if key in invalid_budget_keys:
            continue
        actual = worker_budget[key]
        expected = producer_budget[key]
        if actual != expected:
            problems.append(
                f"{label}: worker actual {actual} != producer/env/default "
                f"{expected} ({env_key}); console/schedule/event_filter hash "
                f"env only — set {env_key} for the whole deployment instead "
                "of a worker-only CLI override"
            )

    queue_name = args.queue_name
    if not queue_name:
        problems.append(
            "--queue-name: empty value is not allowed (identity/routing)"
        )
    else:
        producer_queue = resolve_queue_name("worker")
        if queue_name != producer_queue:
            problems.append(
                f"--queue-name {queue_name!r} != env/default {producer_queue!r}: "
                "console/schedule/event_filter write the env stream; set "
                "PLAITA_QUEUE_NAME for the whole deployment instead of a "
                "worker-only CLI override"
            )

    group = args.consumer_group
    if not group:
        problems.append(
            "--consumer-group: empty value is not allowed (identity/routing)"
        )
    else:
        producer_group = resolve_consumer_group()
        if group != producer_group:
            problems.append(
                f"--consumer-group {group!r} != env/default {producer_group!r}: "
                "console/schedule/event_filter derive config_family from env; "
                "set PLAITA_CONSUMER_GROUP for the whole deployment instead of "
                "a worker-only CLI override"
            )

    dlq = args.dlq_key
    if not dlq:
        problems.append("--dlq-key: empty value is not allowed (identity/routing)")
    else:
        producer_dlq = resolve_dlq_key(queue_name, empty_policy="default")
        if dlq != producer_dlq:
            problems.append(
                f"--dlq-key {dlq!r} != env/default {producer_dlq!r}: "
                "producers derive config_family from env; set PLAITA_DLQ_KEY "
                "for the whole deployment instead of a worker-only CLI override"
            )
    return problems


def main():
    """命令行入口程序"""
    # env-only preflight 必须在 argparse 之前：parser 的 default 自己会读 env
    # （int(...)/identity resolver），非法环境不能等到 parse 后才炸成 traceback。
    # CLI 覆盖面的校验在 parse_args() 之后（#269/#270）。
    try:
        preflight_warnings = preflight_security_and_limits()
        load_runtime_config("worker", strict=True)
    except ConfigError as exc:
        print(f"配置预检失败，worker 拒绝启动：{exc}", file=sys.stderr)
        raise SystemExit(2)
    parser = argparse.ArgumentParser(description="Plaita流程工作器")
    
    # Redis参数（统一 resolver：PLAITA_REDIS_URL > REDIS_URL > 默认）
    parser.add_argument("--redis-url",
                      default=resolve_redis_url("worker"),
                      help="Redis连接URL")
    parser.add_argument("--queue-name",
                      default=resolve_queue_name("worker"),
                      help="Redis Stream 键名（任务队列，需 Redis 5+）")
    parser.add_argument("--consumer-group",
                      default=resolve_consumer_group(),
                      help="Stream consumer group 名称")
    parser.add_argument("--consumer-name",
                      default=os.environ.get("PLAITA_CONSUMER_NAME"),
                      help="本 worker 的 consumer 名称（默认 instance_id 或 worker-pid）")
    parser.add_argument("--claim-min-idle-ms", type=int,
                      default=int(os.environ.get("PLAITA_CLAIM_MIN_IDLE_MS", str(DEFAULT_CLAIM_MIN_IDLE_MS))),
                      help="pending 消息最短空闲毫秒数后才可被其他 consumer 回收")
    parser.add_argument("--lease-ttl-seconds", type=int,
                      default=int(os.environ.get("PLAITA_LEASE_TTL_SECONDS", str(DEFAULT_LEASE_TTL_SECONDS))),
                      help="resume execution lease TTL（秒），推进中会 renew")
    parser.add_argument("--max-deliveries", type=int,
                      default=int(os.environ.get("PLAITA_MAX_DELIVERIES", str(DEFAULT_MAX_DELIVERIES))),
                      help="任务最大投递次数，超过后写入 DLQ 并 ack")
    parser.add_argument("--dlq-key",
                      default=resolve_dlq_key(resolve_queue_name("worker"), empty_policy="default"),
                      help="死信 Stream 键（默认 <queue-name>:dlq）")
    
    # 数据库参数
    parser.add_argument("--database-url", default="sqlite:///flow.db",
                      help="数据库连接URL")
    
    # 存储组件类型（execution/flow 仅 memory|redis；db 与同步 ABC 不兼容，见 factory）
    parser.add_argument("--execution-storage-type", choices=["memory", "redis"], default="redis",
                      help="执行状态存储类型（memory|redis；db 已下架）")
    parser.add_argument("--flow-storage-type", choices=["memory", "redis"], default="redis",
                      help="流程定义存储类型（memory|redis；db 已下架）")
    
    # 事件总线参数（默认启用：分布式挂起/恢复依赖订阅写入；
    # 历史 --use-event-bus 为 opt-in，导致默认部署订阅落内存、EventFilter 读 Redis）
    parser.add_argument("--event-bus-type", choices=["memory", "redis"], default="redis",
                      help="事件总线类型（生产用 redis；db/sqlalchemy 已标 experimental，见 factory）")
    parser.add_argument("--use-event-bus", action="store_true",
                      help=argparse.SUPPRESS)  # 已默认启用，保留兼容旧脚本
    parser.add_argument("--no-event-bus", action="store_true",
                      help="禁用事件总线（仅无挂起节点的纯同步场景）")
    
    # 缓存参数
    parser.add_argument("--cache-size", type=int, default=100,
                      help="缓存大小")
    parser.add_argument("--cache-ttl", type=int, default=300,
                      help="缓存TTL(秒)")
    
    # 服务注册参数
    parser.add_argument("--enable-registry", action="store_true", default=True,
                      help="启用服务注册")
    parser.add_argument("--no-registry", action="store_true",
                      help="禁用服务注册")
    parser.add_argument("--registry-ttl", type=int, default=30,
                      help="服务注册TTL(秒)")
    parser.add_argument("--read-block-ms", type=int, default=1_000,
                        help="XREADGROUP 阻塞窗口上限（毫秒）。默认 1000：让 SIGTERM "
                             "后的停机延迟 ≤1s；调大可略降 Redis 往返次数")
    parser.add_argument("--concurrency", type=int,
                        default=int(os.environ.get("PLAITA_WORKER_CONCURRENCY", "1")),
                        help="并发消费线程数（默认 1=串行）。>1 时单机可同时处理多个任务；"
                             "轻任务（TS/Python/Node）可放大，Rust 重构建任务建议保持 1。"
                             "也可用环境变量 PLAITA_WORKER_CONCURRENCY")
    parser.add_argument("--quiet", action="store_true",
                        help="关闭 INFO 级控制台日志（等价 PLAITA_LOG_LEVEL=WARNING）")
    parser.add_argument("--min-free-disk-gib", type=float,
                        default=_min_free_disk_gib_from_env(),
                        help="claim 前的本机可用盘守线（GiB）：低于它就不领任务，"
                             "空转重探、盘回线自动恢复（不产生白跑 claim）。"
                             "默认 0=关闭；跟随 PLAITA_WORKER_MIN_FREE_DISK_GIB，"
                             "应与 flow 内 preflight 节点的阈值对齐")
    parser.add_argument("--disk-guard-path", default=_disk_guard_path_from_env(),
                        help="盘预检路径（默认当前工作目录或 PLAITA_WORKER_DISK_GUARD_PATH）")
    parser.add_argument("--heartbeat-interval", type=int, default=10,
                      help="心跳间隔(秒)")
    parser.add_argument("--metrics-port", type=int,
                      default=_metrics_port_from_env(),
                      help="Prometheus /metrics 抓取端端口（默认跟随 PLAITA_METRICS_PORT；"
                           "0 = 不启动）。暴露 stream/pending/dlq/计数器/心跳")
    parser.add_argument("--metrics-host", default=_metrics_host_from_env(),
                      help="/metrics 抓取端监听地址（默认 0.0.0.0 或 PLAITA_METRICS_HOST）")
    parser.add_argument("--alert-webhook", default=_alert_webhook_from_env(),
                      help="死信告警 webhook URL（JSON POST）；默认跟随 PLAITA_ALERT_WEBHOOK")
    parser.add_argument("--langfuse", action="store_true", default=None,
                        help="启用 Langfuse 观测（需 pip install plaita[langfuse]；"
                             "凭据走 LANGFUSE_PUBLIC_KEY/SECRET_KEY/HOST 环境变量）。"
                             "也可用环境变量 PLAITA_WORKER_LANGFUSE=1 开启")

    args = parser.parse_args()

    # B2R31-03 / #269 / #270：身份/安全/资源类非法值在启动期一次聚合、
    # fail-fast。必须先 parse_args，CLI 的 0/负值不能绕过预检；family/代际
    # 相关覆盖也必须纳入同一配置视图（与 worker 构造时的 overrides 同源）。
    try:
        cli_problems = _cli_startup_problems(args)
        if cli_problems:
            raise ConfigError(cli_problems)
        _runtime_cfg = load_runtime_config(
            "worker",
            strict=True,
            overrides={
                "queue_name": args.queue_name,
                "redis_url": args.redis_url,
                "consumer_group": args.consumer_group,
                "dlq_key": args.dlq_key,
                "max_deliveries": args.max_deliveries,
                "lease_ttl_seconds": args.lease_ttl_seconds,
                "task_max_age_seconds": task_max_age_seconds(),
                "dlq_max_len": _dlq_max_len_from_env(),
            },
        )
    except ConfigError as exc:
        print(f"配置预检失败，worker 拒绝启动：{exc}", file=sys.stderr)
        raise SystemExit(2)
    # CLI 默认给控制台日志：库内 logger 无 handler，原样跑运维看到的是
    # 零输出（2026-09 分布式评审 P2-6）。--quiet / PLAITA_LOG_LEVEL 可调。
    logging.basicConfig(
        level=os.environ.get("PLAITA_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log_config_effective(logger, _runtime_cfg)
    # preflight 的 WARNING 发生在 logging.basicConfig 之前（NullHandler），
    # 这里补打一次，确保可选调优降级在启动日志可见（B2R31-03）。
    for warning in preflight_warnings:
        logger.warning("配置预检：%s", warning)
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    # writefile 写入 jail（plaita#39）：writefile 节点默认任意路径可写，而部署入口
    # 此前从不设置 PLAITA_NODES_WORKSPACE_ROOT——机制在节点侧，门在运营侧缺省开着。
    # worker 启动即注入（未显式配置则 fail-closed 推导默认根）。
    apply_writefile_jail("flow-worker")

    callback_handlers = []
    if args.langfuse or os.environ.get("PLAITA_WORKER_LANGFUSE") == "1":
        try:
            from plaita.obs import LangfuseCallback

            callback_handlers.append(LangfuseCallback())
            logger.info("Langfuse 观测已启用（trace id = 运行时 $EXECUTION_ID）")
        except ImportError as e:
            logger.warning("Langfuse 观测未启用（缺依赖）: %s", _scrub_url_text(e))
        except Exception as e:  # noqa: BLE001 — SDK 初始化失败（如缺凭据）只降级不退出
            logger.warning("Langfuse 观测未启用: %s", _scrub_url_text(e))

    # 外部业务节点模块加载（与 console 的 PLAITA_CONSOLE_NODE_MODULES 约定对齐）：
    # PLAITA_NODE_PATH 冒号分隔追加 sys.path；PLAITA_NODE_MODULES 逗号分隔，
    # 逐个 import 并调用其 register_all()（或 register）。业务仓（如 mediaflow
    # 的 plaita_flows.nodes）由此在 console 拉起的 worker 内生效。
    import importlib

    for extra in [p for p in os.environ.get("PLAITA_NODE_PATH", "").split(os.pathsep) if p]:
        if extra not in sys.path:
            sys.path.insert(0, extra)
    for mod_path in [m.strip() for m in os.environ.get("PLAITA_NODE_MODULES", "").split(",") if m.strip()]:
        try:
            mod = importlib.import_module(mod_path)
            register = getattr(mod, "register_all") or getattr(mod, "register")
            register()
            logger.info("已加载外部节点模块: %s", mod_path)
        except Exception as e:
            logger.error("外部节点模块加载失败 %s: %s", mod_path, _scrub_exception_text(e))
            raise SystemExit(f"外部节点模块加载失败: {mod_path}")

    # CodeNode 按需注册：0.4.0 起移出默认注册表（其执行任意用户代码，需显式 opt-in）。
    # 生产流程（如 self-improve-v2）含 code 节点，worker 必须注册否则整单被丢弃
    # （「unRecognized node type: code」）。后端由 PLAITA_CODE_BACKEND 选择，默认
    # subprocess（无需 docker daemon）；声明值非法或配置后端不可用时**具名拒绝并
    # 停止启动**（A56-01 fail-closed，不自动降级；显式逃生开关见下方 helper）。
    # 后端白名单由 PLAITA_SANDBOX_ALLOWED_BACKENDS 配置（见 _register_code_node_for_worker）。
    if _code_node_enabled():
        _register_code_node_for_worker()

    # 沙箱暂停实例清扫（启动一次性、后台线程）：**import 已在主线程完成**
    # （`_paused_sweeper`）——线程内 import 会与主线程懒加载撞 import 锁并整体卡死
    # （2026-10-08 实测）。线程内只做网络调用。
    _sweeper = _paused_sweeper()
    if _sweeper is not None:
        threading.Thread(target=_sweeper, name="sandbox-sweep", daemon=True).start()

    # 处理注册开关
    enable_registry = args.enable_registry and not args.no_registry
    
    try:
        # 创建执行状态存储
        storage_kwargs = {
            "redis_url": args.redis_url,
            "database_url": args.database_url
        }
        
        execution_storage = create_storage_component(
            args.execution_storage_type,
            "execution",
            tenant_routing=True,
            **storage_kwargs
        )
        logger.info("已创建执行状态存储: %s类型（多租户路由）", args.execution_storage_type)

        # 创建流程定义存储
        flow_storage = create_storage_component(
            args.flow_storage_type,
            "flow",
            tenant_routing=True,
            **storage_kwargs
        )
        logger.info("已创建流程定义存储: %s类型（多租户路由）", args.flow_storage_type)
        
        # 创建事件总线（默认启用；--no-event-bus 显式关闭）
        event_bus = None
        if not args.no_event_bus:
            event_bus = create_event_bus(
                args.event_bus_type,
                **storage_kwargs
            )
            logger.info("已创建事件总线: %s类型", args.event_bus_type)
        else:
            logger.warning("已禁用事件总线（--no-event-bus）；含挂起节点的流程将无法恢复")
        
        # 创建Redis流程工作器
        worker = RedisFlowWorker(
            redis_url=args.redis_url,
            queue_name=args.queue_name,
            execution_storage=execution_storage,
            flow_storage=flow_storage,
            event_bus=event_bus,
            callback_handlers=callback_handlers or None,
            cache_size=args.cache_size,
            cache_ttl=args.cache_ttl,
            enable_registry=enable_registry,
            registry_ttl=args.registry_ttl,
            heartbeat_interval=args.heartbeat_interval,
            consumer_group=args.consumer_group,
            consumer_name=args.consumer_name or None,
            claim_min_idle_ms=args.claim_min_idle_ms,
            lease_ttl_seconds=args.lease_ttl_seconds,
            max_deliveries=args.max_deliveries,
            dlq_key=args.dlq_key or None,
            read_block_ms=args.read_block_ms,
            concurrency=args.concurrency,
            min_free_disk_gib=args.min_free_disk_gib,
            disk_guard_path=args.disk_guard_path,
            metrics_port=args.metrics_port,
            metrics_host=args.metrics_host,
            alert_webhook=args.alert_webhook or None,
        )
        
        # 注册信号处理器以支持优雅关闭。只置位、由 run() 主循环在任务边界
        # 自然退出；不再 sys.exit 腰斩在途任务（ReviewFix D3，语义见
        # _request_graceful_stop）。
        def signal_handler(signum, frame):
            _request_graceful_stop(worker, signum)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # 启动工作器。历史上这里有 --debug-mode 分支硬编码 flow_id="event_flow_demo"
        # 直接读 Redis + 手动 lrem 队列消息, 是开发期临时脚本——已删除。需要类似
        # 调试请用 Redis CLI 或独立 dev 脚本, 不要留在生产 CLI 入口里。
        logger.info("流程工作器启动成功，监听队列: %s", args.queue_name)
        worker.run()

    except Exception as e:
        logger.error("流程工作器启动失败: %s", _scrub_url_text(e))
        sys.exit(1)

if __name__ == "__main__":
    main()


