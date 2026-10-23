"""worker 容量上报 + 派发侧容量门（#42 / W10-a 全局调度层第一竖切）。

## 为什么需要这一层

`plaita/server/registry.py` 的服务注册表原本只回答「**谁活着**」：
``ServiceInfo`` 有 ``status`` / ``active_tasks`` / ``metadata``，但没有任何
「**还能接多少活**」的字段。派发侧（console / intake flow）因此无法：

- 避开低盘机器——worker 自己的盘闸（#49
  ``_disk_guard_blocks_claim``）只在**本进程内**空转，对派发方完全不可见
  （2026-10-06 本机盘触底 0 字节仍在被压单）；
- 按负载均衡——即使本机排队、远端空转，也不知道该把单发给谁。

## 本模块提供什么

1. **规格化容量块**（``build_capacity_block`` / ``parse_capacity_block``）：
   一段 JSON，写进既有 ``plaita:registry:flow_worker:<id>`` 键的 metadata
   之子键 ``worker_capacity``——**复用**既有 registry 键与 TTL，
   不引入新基础设施、不新增 Redis 键族。
2. **新鲜度**（``capacity_age_seconds`` / ``is_capacity_stale``）：块带
   ``sampled_at``；超过 ``CAPACITY_STALE_SECONDS`` 视为不可信。
3. **纯函数派发门**（``filter_dispatchable`` / ``select_dispatch_target`` /
   ``decide_route``）：输入候选快照、输出「可派 + 拒绝原因 + 目标」，供
   intake flow / console / 单测共用同一份判据（策略改定义不改代码）。
   A64-05 起带**队列维度**：``queue_name`` 给定时先按候选消费队列过滤，
   跨队列候选以 ``queue: mismatch`` 拒绝。B75-01 起队列是**三态**：
   声明（known）/ 未声明（unknown）/ 默认队列假设——unknown 不再等价于
   default，默认只拒绝定向、不拒绝广播（legacy default worker 仍可消费）。
4. **影子模式**（``shadow_dispatch_plan``）：只算不发——plan 里只包含目标与
   队列名，**绝不写任何 Redis 键**（验收 5：影子期生产队列零新增）。
5. **显式派发目标闸**（``TARGET_INSTANCE_FIELD`` / ``target_mismatch``）：
   期 1 把上面的路由结论接进真实派发——目标实例 id 写进既有消息 payload，
   非目标 worker 复用既有 ``TaskNotForThisWorker`` 交接路径让出（不新增协议）。
   字段缺失/空白/本机 id 不可知 ⇒ **放行**（存量广播消息零行为变化）。

## 红线（与 #49 盘闸同口径）

- 容量块缺失 / 非法 / 无 ``sampled_at`` → **一律放行**。上报机制自身故障
  不得让整池 worker 停摆；宁可白跑一次，不可误拦。
- 陈旧快照**不采信其内容**（不按陈旧容量误派），但**不把机器踢出池**
  （``capacity_stale=True`` 后按「无容量信息」处理 = 放行）。
- 无可用目标（池空 / 全被拒）→ 派发**不带**目标字段 = 广播抢单（存量行为）。
- 本模块的读侧对 Redis 故障返回空/放行，绝不抛出。
"""
from __future__ import annotations

import json
import logging
import math
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from plaita.core.url_redaction import _scrub_exception_text
from plaita.core.url_redaction import _redact_env_value

logger = logging.getLogger("plaita.server.capacity")

#: registry metadata 里承载容量块的子键（写在既有 metadata 字典内，
#: 不新增 Redis 键族）。
CAPACITY_METADATA_KEY = "worker_capacity"

#: 容量块 schema 版本（语义版本：字段含义变化才 bump）。解析侧对未知版本
#: 采取**放行**（不猜着用）。
CAPACITY_SCHEMA_VERSION = 1

#: registry 里 worker 的 service_type（与 ``RedisFlowWorker.SERVICE_TYPE`` 一致，
#: 此处不做导入以避免循环依赖）。
DEFAULT_WORKER_SERVICE_TYPE = "flow_worker"

#: 容量快照的新鲜期（秒）。registry TTL 默认 30s、心跳 10s，即正常 worker 的
#: 快照至少每 10s 刷新一次；给三倍余量（90s）后判陈旧——比 TTL 宽松，
#: 因为注册键过期另有「存在性」语义兜底，本守线只用于「内容可信度」。
DEFAULT_CAPACITY_STALE_SECONDS = 90.0

#: 默认新鲜期的短别名（测试/调用方按「守线常量」引用；env 可覆盖）。
CAPACITY_STALE_SECONDS = DEFAULT_CAPACITY_STALE_SECONDS

#: 影子模式环境变量（验收 5：影子 → 金丝雀 → 全量）。**默认开**——
#: 本竖切不改变任何既有派发行为，只新增可观测与可计算的决策面。
SHADOW_MODE_ENV = "PLAITA_CONSOLE_DISPATCH_SHADOW"

#: 派发队列名 env（与 ``api/executions.py`` 的 ``PLAITA_CONSOLE_TASK_QUEUE`` 同源）。
DISPATCH_QUEUE_ENV = "PLAITA_CONSOLE_TASK_QUEUE"

#: 派发队列默认名（与 FlowWorker / console 缺省一致）。
DEFAULT_DISPATCH_QUEUE = "plaita:flow:queue"

#: B75-01：worker 未声明 ``queue_name`` 时的定向策略。
#: ``skip``（默认）= 不把 unknown 当默认队列，只拒绝定向；消息仍按调用方
#: 队列广播，legacy default worker 仍可消费。``assume-default`` = 仅在
#: ``requested_queue == DEFAULT_DISPATCH_QUEUE`` 时把 unknown 纳入定向，
#: 并留 WARNING + ``queue_source=assumed-default``（显式兼容旧部署）。
UNKNOWN_QUEUE_POLICY_ENV = "PLAITA_DISPATCH_UNKNOWN_QUEUE"
UNKNOWN_QUEUE_SKIP = "skip"
UNKNOWN_QUEUE_ASSUME_DEFAULT = "assume-default"

#: 台账/响应里的队列来源（B75-01 §5：只有这两个值）。
QUEUE_SOURCE_DECLARED = "declared"
QUEUE_SOURCE_ASSUMED_DEFAULT = "assumed-default"

#: 拒绝原因前缀：**稳定字符串**，供 flow 侧 assignment 节点按前缀分支
#: （改文案不算改语义，改前缀算）。
#: A64-05：候选消费的队列（registry metadata ``queue_name``）与本次派发队列
#: 不一致。容量路由必须带队列维度——跨队列选中 target 会让消息落到 target
#: 永远读不到的队列，worker 交接耗尽后把执行误终态化为 error。
REASON_QUEUE_MISMATCH = "queue: mismatch"
#: B75-01：候选没有可证明的消费队列（缺失/空白/非字符串）——**不等于**
#: 默认队列。unknown 在默认 ``skip`` 策略下永不进入 ``allowed``/``target``。
REASON_QUEUE_UNKNOWN = "queue: unknown"
#: plaita#328：B2R31-01 的 ``REASON_CONFIG_FAMILY_MISMATCH`` 随逐消息 family
#: 闸一起删除——拓扑一致性（queue/group/dlq/namespace）由 worker 启动校验
#: 把守，本模块只保留队列维度（REASON_QUEUE_MISMATCH/UNKNOWN）。
REASON_DISK = "disk: low free space"
REASON_SATURATED = "saturated: active_tasks >= concurrency"
REASON_DRAINING = "draining"
#: S18-02A：status 是唯一派发白名单——非 running 一律拒，且在容量判定之前。
REASON_STARTING = "status: starting"
REASON_STOPPING = "status: stopping"
REASON_STOPPED = "status: stopped"
REASON_STATUS_UNKNOWN = "status: unknown"

#: 路由结论前缀（同样稳定，供台账/看板按前缀聚合）。
#: ``route: capacity`` = 按容量排序选出了目标；``route: no-dispatchable-worker``
#: = 本轮没有可用目标 ⇒ **放行**（广播抢单，即存量行为；不因上报故障停摆）。
REASON_ROUTE_CAPACITY = "route: capacity"
REASON_ROUTE_NO_TARGET = "route: no-dispatchable-worker"

#: 显式派发目标字段（#42 期 1）：派发侧按容量路由选中的 worker 实例 id，
#: 写在**既有任务消息 payload 的顶层**——不新增队列/Redis 键/握手协议；
#: worker 侧比对自身 ``instance_id``，不等则复用既有 ``TaskNotForThisWorker``
#: 交接路径让给目标机。
TARGET_INSTANCE_FIELD = "target_instance_id"


def stale_seconds_from_env() -> float:
    """新鲜期（``PLAITA_CAPACITY_STALE_SECONDS``），非法/未设回默认。"""
    raw = (os.environ.get("PLAITA_CAPACITY_STALE_SECONDS") or "").strip()
    if not raw:
        return DEFAULT_CAPACITY_STALE_SECONDS
    try:
        value = float(raw)
    except ValueError:
        logger.warning("PLAITA_CAPACITY_STALE_SECONDS=%s 非法，回退默认 %s",
                       _redact_env_value(raw), DEFAULT_CAPACITY_STALE_SECONDS)
        return DEFAULT_CAPACITY_STALE_SECONDS
    if value <= 0 or math.isnan(value) or math.isinf(value):
        logger.warning("PLAITA_CAPACITY_STALE_SECONDS=%s 非正，回退默认 %s",
                       _redact_env_value(raw), DEFAULT_CAPACITY_STALE_SECONDS)
        return DEFAULT_CAPACITY_STALE_SECONDS
    return value


def shadow_mode_from_env() -> bool:
    """影子模式开关（默认 ``True`` = 只算不发）。

    显式关闭需 ``PLAITA_CONSOLE_DISPATCH_SHADOW=0``（或 false/no/off）。
    """
    raw = (os.environ.get(SHADOW_MODE_ENV) or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    return True


def dispatch_queue_from_env() -> str:
    """派发队列名（``PLAITA_CONSOLE_TASK_QUEUE``，与 console 发单同源）。

    B2R31-02：与 manual start/schedule 共用单一 resolver；空串是 identity
    配置错误，不再静默回落默认队列（A2R31-03）。
    """
    from plaita.runtime_config import resolve_queue_name

    return resolve_queue_name("console")


def candidate_queue_name(metadata: Any) -> Optional[str]:
    """候选**声明**的消费队列名（registry metadata ``queue_name``）。

    B75-01：队列是三态，不是二态——

    - 非空、非纯空白字符串 ⇒ known，返回原字符串（不做去空白：worker 用
      原值作为 Stream 键，改写会让判据与事实漂移）；
    - 缺失 / 空白 / 纯空白 / 非字符串 ⇒ **unknown**，返回 ``None``。

    unknown **绝不等于** ``DEFAULT_DISPATCH_QUEUE``。未升级 worker 的注册
    metadata 可能没有该键、也可能有畸形值；把假设当事实正是 B74-02 的
    「消息定向到 target 读不到的队列、静默滞留」根因。未知只影响**定向**，
    不影响**广播**（见 :func:`decide_route`）。
    """
    if isinstance(metadata, dict):
        raw = metadata.get("queue_name")
        if isinstance(raw, str) and raw.strip():
            return raw
    return None


def resolve_unknown_queue_policy(value: Optional[str] = None) -> str:
    """unknown 队列策略（``PLAITA_DISPATCH_UNKNOWN_QUEUE``）。

    ``value=None`` 读 env；显式传入便于单测/调用方固定判据。非法值按
    ``skip`` 处理并留 WARNING（不能静默把兼容开关开成默认）。
    """
    raw = value if value is not None else os.environ.get(UNKNOWN_QUEUE_POLICY_ENV)
    raw = (raw or "").strip().lower()
    if raw in ("", UNKNOWN_QUEUE_SKIP):
        return UNKNOWN_QUEUE_SKIP
    if raw == UNKNOWN_QUEUE_ASSUME_DEFAULT:
        return UNKNOWN_QUEUE_ASSUME_DEFAULT
    logger.warning(
        "%s=%s 非法（允许 %s/%s），按 %s 处理",
        UNKNOWN_QUEUE_POLICY_ENV, _redact_env_value(raw),
        UNKNOWN_QUEUE_SKIP, UNKNOWN_QUEUE_ASSUME_DEFAULT, UNKNOWN_QUEUE_SKIP,
    )
    return UNKNOWN_QUEUE_SKIP


#: B75-01 §5：``dispatch_unknown_queue_total{queue}`` 的进程内累计。
#: 只统计「本次派发带队列过滤且候选未声明队列」的次数——广播/不做队列
#: 过滤的调用（看板）不计，避免把正常的存量调用报成异常。
_unknown_queue_counts: Dict[str, int] = {}
_unknown_queue_counts_lock = threading.Lock()

_UNKNOWN_QUEUE_METRIC_HELP = (
    "派发时遇到未声明 queue_name 的 worker 次数（unknown ≠ default；"
    "skip 时只拒绝定向，assume-default 时显式纳入默认队列）"
)


def record_unknown_queue(queue_name: str, count: int = 1) -> None:
    """累加 unknown 队列计数（空队列名忽略；进程内、线程安全）。"""
    if not queue_name or count <= 0:
        return
    with _unknown_queue_counts_lock:
        _unknown_queue_counts[queue_name] = (
            _unknown_queue_counts.get(queue_name, 0) + int(count)
        )


def unknown_queue_counts() -> Dict[str, int]:
    """当前进程内 ``queue → count`` 快照（prometheus 导出/单测）。"""
    with _unknown_queue_counts_lock:
        return dict(_unknown_queue_counts)


def reset_unknown_queue_counts() -> None:
    """清空进程内计数（单测隔离用；生产不调用）。"""
    with _unknown_queue_counts_lock:
        _unknown_queue_counts.clear()


def collect_dispatch_metrics() -> List[Any]:
    """派发侧指标（``dispatch_unknown_queue_total{queue}``）。

    与 worker 的 ``collect_capacity_metrics`` 同属「派发侧可见面」；console
    的集群 ``/metrics`` 会把它并入（惰性导入 ``metrics`` 避免环依赖）。
    """
    counts = unknown_queue_counts()
    if not counts:
        return []
    from .metrics import Metric  # 局部导入：避免模块级环依赖

    return [
        Metric(
            "dispatch_unknown_queue_total",
            count,
            {"queue": queue},
            "counter",
            _UNKNOWN_QUEUE_METRIC_HELP,
        )
        for queue, count in sorted(counts.items())
    ]


# ------------------------------------------------------------------ 容量块


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_float(value: Any, default: float = 0.0) -> float:
    """宽松转 float：非法/None/NaN/inf → default。"""
    if value is None:
        return default
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(out) or math.isinf(out):
        return default
    return out


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(_as_float(value, float(default)))
    except (TypeError, ValueError):
        return default


def build_capacity_block(
    *,
    free_disk_gib: Optional[float] = None,
    min_free_disk_gib: float = 0.0,
    disk_blocked: bool = False,
    active_tasks: int = 0,
    concurrency: int = 1,
    sampled_at: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
    deployment_epoch: Optional[str] = None,
) -> Dict[str, Any]:
    """构造容量块（纯函数，可直接断言）。

    ``free_disk_gib=None`` 表示探针失败/未启用盘闸——保留为 ``null`` 而不是
    0，让下游能区分「真的没盘」与「探不到盘」（后者按放行处理）。
    数值一律非负钳制，避免探测异常把 ``-1`` 写进注册表。
    """
    free = None if free_disk_gib is None else max(0.0, _as_float(free_disk_gib))
    block: Dict[str, Any] = {
        "schema_version": CAPACITY_SCHEMA_VERSION,
        "free_disk_gib": free,
        "min_free_disk_gib": max(0.0, _as_float(min_free_disk_gib)),
        "disk_blocked": bool(disk_blocked),
        "active_tasks": max(0, _as_int(active_tasks)),
        "concurrency": max(1, _as_int(concurrency, 1)),
        "sampled_at": sampled_at or _now_iso(),
    }
    # plaita#328：B2R31-01 三哈希（config_family/config_generation/
    # budget_generation）不再随容量块上报——版本维度收敛为 wire schema + IR；
    # deployment_epoch 是部署纪元（另一维度），保留。
    if deployment_epoch is not None:
        block["deployment_epoch"] = deployment_epoch
    if extra:
        for key, value in extra.items():
            block.setdefault(key, value)
    return block


def parse_capacity_block(raw: Any) -> Optional[Dict[str, Any]]:
    """宽松解析容量块；无法解析返回 ``None``（**绝不抛**）。

    接受形态：JSON 字符串 / bytes / 已解析的 dict / registry metadata 字典
    （内含 ``worker_capacity`` 子键，其值再递归解析一层）。
    """
    data = _coerce_to_mapping(raw)
    if data is None:
        return None
    if CAPACITY_METADATA_KEY in data and isinstance(data[CAPACITY_METADATA_KEY], (str, bytes, dict)):
        nested = _coerce_to_mapping(data[CAPACITY_METADATA_KEY])
        if nested is not None:
            data = nested
    if "schema_version" not in data and "free_disk_gib" not in data and "active_tasks" not in data:
        return None
    if "schema_version" in data and _as_int(data.get("schema_version"), 0) != CAPACITY_SCHEMA_VERSION:
        # 未知语义版本：不猜着用（调用方按「无容量信息」放行）。
        logger.debug("容量块 schema_version=%r 未知，按无容量信息处理", data.get("schema_version"))
        return None
    free = data.get("free_disk_gib", None)
    stale_at = data.get("sampled_at")
    parsed = {
        "schema_version": CAPACITY_SCHEMA_VERSION,
        "free_disk_gib": None if free is None else max(0.0, _as_float(free)),
        "min_free_disk_gib": max(0.0, _as_float(data.get("min_free_disk_gib"), 0.0)),
        "disk_blocked": bool(data.get("disk_blocked", False)),
        "active_tasks": max(0, _as_int(data.get("active_tasks"), 0)),
        "concurrency": max(1, _as_int(data.get("concurrency"), 1)),
        "sampled_at": stale_at if isinstance(stale_at, str) else None,
    }
    # deployment_epoch 是增量：旧容量块没有它时保持缺失。plaita#328：
    # config_family/config_generation/budget_generation 三哈希已随逐消息
    # family 闸一起删除，解析侧不再透传。
    value = data.get("deployment_epoch")
    if value is not None:
        parsed["deployment_epoch"] = value
    return parsed


def _coerce_to_mapping(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text or text[0] not in "{[":
        return None
    try:
        parsed = json.loads(text)
    except (ValueError, TypeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def capacity_from_metadata(metadata: Any) -> Optional[Dict[str, Any]]:
    """从 registry metadata 提取容量块（无则 ``None``）。"""
    if not isinstance(metadata, dict):
        return None
    return parse_capacity_block(metadata.get(CAPACITY_METADATA_KEY))


def capacity_age_seconds(block: Optional[Dict[str, Any]], *, now: Optional[datetime] = None) -> Optional[float]:
    """容量块年龄（秒）；无 ``sampled_at``/不可解析返回 ``None``。"""
    if not block:
        return None
    raw = block.get("sampled_at")
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        sampled = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if sampled.tzinfo is None:
        sampled = sampled.replace(tzinfo=timezone.utc)
    ref = now or datetime.now(timezone.utc)
    return (ref - sampled).total_seconds()


def is_capacity_stale(
    block: Optional[Dict[str, Any]],
    *,
    max_age_seconds: Optional[float] = None,
    now: Optional[datetime] = None,
) -> bool:
    """快照是否陈旧到不可采信。

    无 ``sampled_at`` / 时间戳不可解析 → **判陈旧**（诚实：无法证明新鲜）。
    无容量块（``block is None``）→ ``False``（那是「无信息」，走放行分支，
    不等价于「陈旧信息」）。
    """
    if block is None:
        return False
    age = capacity_age_seconds(block, now=now)
    if age is None:
        return True
    limit = stale_seconds_from_env() if max_age_seconds is None else float(max_age_seconds)
    return age > limit


def collect_capacity_metrics(
    block: Optional[Dict[str, Any]],
    *,
    instance: Optional[str] = None,
    age_seconds: Optional[float] = None,
) -> List[Any]:
    """容量块 → Prometheus ``Metric`` 列表（``/metrics`` 导出用）。

    ``collect_worker_metrics`` 报的是「worker 自己在跑什么」，本函数报的是
    「**派发侧能看到的容量**」——两者同源不同视角：前者进程内直读，后者
    走 registry 上报。低盘/饱和要能被看板直接看见，否则容量门拒单时
    运维只能看日志。

    惰性导入 ``metrics`` 以避免 ``metrics`` ↔ ``capacity`` 的导入环
    （``metrics`` 目前不依赖 capacity，但保持单向更稳）。
    """
    if block is None:
        return []
    from .metrics import Metric  # 局部导入：避免模块级环依赖

    labels: Dict[str, str] = {}
    if instance:
        labels["instance"] = str(instance)
    out: List[Any] = []
    free = block.get("free_disk_gib")
    if free is not None:
        out.append(Metric(
            "worker_free_disk_gib", _as_float(free), dict(labels), "gauge",
            "worker 上报的可用磁盘（GiB）——派发容量门的输入",
        ))
    out.append(Metric(
        "worker_min_free_disk_gib", _as_float(block.get("min_free_disk_gib"), 0.0),
        dict(labels), "gauge", "worker 自报的盘守线（GiB）",
    ))
    out.append(Metric(
        "worker_disk_blocked", 1 if block.get("disk_blocked") else 0,
        dict(labels), "gauge", "worker 自报是否被盘闸拦下（1/0）",
    ))
    out.append(Metric(
        "worker_capacity_concurrency", max(1, _as_int(block.get("concurrency"), 1)),
        dict(labels), "gauge", "worker 自报并发上限",
    ))
    out.append(Metric(
        "worker_capacity_active_tasks", max(0, _as_int(block.get("active_tasks"), 0)),
        dict(labels), "gauge", "worker 上报的在跑任务数",
    ))
    if age_seconds is not None:
        out.append(Metric(
            "worker_capacity_age_seconds", _as_float(age_seconds), dict(labels), "gauge",
            "容量快照年龄（秒）——持续增长 = 上报停摆",
        ))
    if is_capacity_stale(block):
        out.append(Metric(
            "worker_capacity_stale", 1, dict(labels), "gauge",
            "容量快照已超新鲜期（1/0）",
        ))
    return out


# ------------------------------------------------------------------ 候选快照


@dataclass
class DispatchCandidate:
    """一个可派发候选（worker 快照 + 解析后的容量）。"""

    instance_id: str
    host: str = ""
    service_type: str = DEFAULT_WORKER_SERVICE_TYPE
    status: str = "running"
    active_tasks: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    #: registry metadata 里的原始容量块（未解析形态，仅用于诊断/透出）
    capacity_raw: Any = None
    #: 解析后的容量块；``None`` = 无容量信息（放行）
    capacity: Optional[Dict[str, Any]] = None
    #: 快照是否陈旧（陈旧 → 不采信内容，但仍在池内）
    capacity_stale: bool = False

    def __post_init__(self) -> None:
        """由 ``capacity_raw`` 补齐解析结果。

        允许调用方只给原始块（HTTP/测试/stub 三处入口形状不一），解析与
        新鲜度判定集中在**一个**地方——判据不能有第二份实现。
        """
        if self.capacity is None and self.capacity_raw is not None:
            self.capacity = parse_capacity_block(self.capacity_raw)
        if not self.capacity_stale and self.capacity is not None:
            self.capacity_stale = is_capacity_stale(self.capacity)

    @classmethod
    def from_service_info(cls, info: Any) -> "DispatchCandidate":
        """从 ``ServiceInfo``（或同形对象）构造候选。"""
        metadata = getattr(info, "metadata", None) or {}
        raw = metadata.get(CAPACITY_METADATA_KEY) if isinstance(metadata, dict) else None
        block = parse_capacity_block(raw) if raw is not None else None
        return cls(
            instance_id=str(getattr(info, "instance_id", "") or ""),
            host=str(getattr(info, "host", "") or ""),
            service_type=str(getattr(info, "service_type", "") or DEFAULT_WORKER_SERVICE_TYPE),
            status=str(getattr(info, "status", "") or "running"),
            active_tasks=max(0, _as_int(getattr(info, "active_tasks", 0), 0)),
            metadata=dict(metadata) if isinstance(metadata, dict) else {},
            capacity_raw=raw,
            capacity=block,
            capacity_stale=is_capacity_stale(block),
        )

    @property
    def queue_name(self) -> Optional[str]:
        """候选**声明**的消费队列名；``None`` = unknown（缺 metadata/畸形值）。

        B75-01：unknown 绝不回落到默认队列——那是假设，不是事实。
        """
        return candidate_queue_name(self.metadata)

    @property
    def queue_source(self) -> str:
        """候选队列来源：``declared`` / ``unknown``（供诊断/台账分类）。"""
        return QUEUE_SOURCE_DECLARED if self.queue_name is not None else "unknown"

    def resolve_queue(
        self,
        requested_queue: str,
        unknown_queue_policy: Optional[str] = None,
    ) -> Tuple[Optional[str], str]:
        """把候选队列解析为定向用的 ``(队列名, queue_source)``。

        - 声明了队列 ⇒ ``(declared, "declared")``；
        - 未声明 + ``assume-default`` + 请求队列是默认队列 ⇒
          ``(DEFAULT_DISPATCH_QUEUE, "assumed-default")``（显式兼容开关）；
        - 其余未声明 ⇒ ``(None, "unknown")``——不得成为定向目标。
        """
        declared = self.queue_name
        if declared is not None:
            return declared, QUEUE_SOURCE_DECLARED
        policy = resolve_unknown_queue_policy(unknown_queue_policy)
        if (
            policy == UNKNOWN_QUEUE_ASSUME_DEFAULT
            and requested_queue == DEFAULT_DISPATCH_QUEUE
        ):
            return DEFAULT_DISPATCH_QUEUE, QUEUE_SOURCE_ASSUMED_DEFAULT
        return None, "unknown"

    def reject_reason(
        self,
        queue_name: Optional[str] = None,
        unknown_queue_policy: Optional[str] = None,
    ) -> Optional[str]:
        """不可派发原因（前缀稳定，见 ``REASON_*``）；可派发返回 ``None``。

        判定顺序即优先级：**队列维度** → status 白名单 → 容量缺失/陈旧
        （放行）→ 盘 → 饱和。``queue_name`` 为 ``None`` 时不做队列过滤
        （纯函数调用方/看板保持原语义）；给定时，候选**声明**的队列必须与
        本次派发队列一致——A64-05：跨队列 target 的消息会投到 target 读不到
        的队列，worker 交接耗尽后把执行误终态化为 error。
        B75-01：未声明队列的候选默认以 ``queue: unknown`` 拒绝定向；只有
        ``assume-default`` 兼容开关（且请求默认队列）才把 unknown 当默认。
        S18-02A：只有 ``running`` 参与派发；``starting/draining/stopping/
        stopped`` 与未知状态在容量判定**之前**就被拒——即使它上报了完美容量，
        也不得成为派发目标（A46-01）。
        """
        if queue_name is not None:
            resolved, source = self.resolve_queue(queue_name, unknown_queue_policy)
            if resolved is None:
                return (
                    f"{REASON_QUEUE_UNKNOWN} "
                    "(worker metadata lacks queue_name)"
                )
            if source == QUEUE_SOURCE_DECLARED and resolved != queue_name:
                return f"{REASON_QUEUE_MISMATCH} (worker consumes {resolved})"
        # plaita#328：B2R31-01 的 family 硬兼容键已删除——拓扑一致性在 worker
        # 启动校验（连错队列的 worker 起不来），逐消息 family 闸不复存在。
        if self.status != "running":
            return {
                "starting": REASON_STARTING,
                "draining": REASON_DRAINING,
                "stopping": REASON_STOPPING,
                "stopped": REASON_STOPPED,
            }.get(self.status, REASON_STATUS_UNKNOWN)
        block = self.capacity
        if block is None:
            # 红线：无容量信息（存量 worker / 上报故障 / schema 未知）→ 放行。
            return None
        if self.capacity_stale:
            # 陈旧快照不采信其内容，按「无信息」放行——不按陈旧容量误派，
            # 也不因上报停摆把机器踢出池。
            return None
        if block.get("disk_blocked"):
            return REASON_DISK
        free = block.get("free_disk_gib")
        min_free = _as_float(block.get("min_free_disk_gib"), 0.0)
        if free is not None and min_free > 0 and _as_float(free) < min_free:
            return REASON_DISK
        if _as_int(block.get("active_tasks"), 0) >= max(1, _as_int(block.get("concurrency"), 1)):
            return REASON_SATURATED
        return None

    @property
    def dispatchable(self) -> bool:
        return self.reject_reason() is None

    def sort_key(self) -> Tuple:
        """确定性排序键：剩余盘降序 → 在跑数升序 → instance_id 升序。

        最后一项保证「同输入同输出」（验收 1）：并列候选不会因 Redis
        ``KEYS`` 返回顺序不同而抖动。
        """
        block = self.capacity or {}
        free = block.get("free_disk_gib")
        disk_key = -_as_float(free, 0.0) if free is not None else 0.0
        # 无容量信息的存量 worker 排在「有容量但盘很少」之后、正常 worker 之前？
        # 不：红线是放行，故其排序按「未知」处理——盘键取 0（即排在负数之后、
        # 与 0GiB 同档），并靠在跑数（registry 的 active_tasks）次级排序。
        active = _as_int(block.get("active_tasks"), self.active_tasks)
        return (0 if self.capacity is not None else 1, disk_key, active, self.instance_id)


@dataclass
class DispatchRejection:
    """被容量门拒绝的候选（带稳定原因，供台账/看板）。"""

    instance_id: str
    host: str = ""
    reason: str = ""
    capacity: Optional[Dict[str, Any]] = None


def filter_dispatchable(
    candidates: Iterable[Any],
    *,
    queue_name: Optional[str] = None,
    unknown_queue_policy: Optional[str] = None,
) -> Tuple[List[DispatchCandidate], List[DispatchRejection]]:
    """容量门：``(可派候选, 被拒候选)``。

    接受 ``DispatchCandidate`` 或任意同形对象（``ServiceInfo`` / duck-typed
    stub）。**纯函数**——不读 Redis、不写 Redis、不改入参。

    ``queue_name`` 给定时候选必须先过队列维度：只有**能证明**消费该队列的
    worker 才有资格成为目标（B75-01：缺 metadata 是 unknown，不是默认队列；
    见 :func:`candidate_queue_name`）。``None`` = 不做队列过滤（看板/存量调用）。
    ``unknown_queue_policy`` 缺省读 env（见
    :func:`resolve_unknown_queue_policy`）。
    """
    allowed: List[DispatchCandidate] = []
    rejected: List[DispatchRejection] = []
    for item in candidates:
        cand = item if isinstance(item, DispatchCandidate) else DispatchCandidate.from_service_info(item)
        reason = cand.reject_reason(queue_name, unknown_queue_policy)
        if reason is None:
            allowed.append(cand)
        else:
            rejected.append(
                DispatchRejection(
                    instance_id=cand.instance_id,
                    host=cand.host,
                    reason=reason,
                    capacity=cand.capacity,
                )
            )
    return allowed, rejected


def select_dispatch_target(
    candidates: Iterable[Any],
    *,
    queue_name: Optional[str] = None,
    unknown_queue_policy: Optional[str] = None,
) -> Optional[DispatchCandidate]:
    """在容量门之上做确定性选择：剩余盘最多 → 在跑最少 → id 字典序。

    无可派候选返回 ``None``（调用方据此走「本轮不派单」而非「随便派一台」）。
    ``queue_name`` / ``unknown_queue_policy`` 语义同
    :func:`filter_dispatchable`。
    """
    allowed, _ = filter_dispatchable(
        candidates,
        queue_name=queue_name,
        unknown_queue_policy=unknown_queue_policy,
    )
    if not allowed:
        return None
    return sorted(allowed, key=lambda c: c.sort_key())[0]


@dataclass
class RouteDecision:
    """一次容量路由的**完整**结论（目标 + 可派/被拒 + 稳定原因）。

    ``target is None`` 有两种语义：池里没有候选、或有候选但全被容量门拒。
    两者的对外处置**相同**——放行（广播抢单），故合并为同一个原因前缀
    ``route: no-dispatchable-worker``（派发侧据此**不带** target 字段）。

    B75-01：``queue_source`` 说明目标队列是 worker **声明**的还是按
    ``assume-default`` 兼容开关**假设**的（``declared`` / ``assumed-default``）。
    ``target_queue_name`` 是定向实际使用的队列（无目标时 ``None``）。
    """

    target: Optional[DispatchCandidate] = None
    reason: str = REASON_ROUTE_NO_TARGET
    allowed: List[DispatchCandidate] = field(default_factory=list)
    rejected: List[DispatchRejection] = field(default_factory=list)
    queue_source: str = QUEUE_SOURCE_DECLARED
    target_queue_name: Optional[str] = None

    @property
    def target_instance_id(self) -> Optional[str]:
        return self.target.instance_id if self.target else None


def decide_route(
    candidates: Iterable[Any],
    *,
    queue_name: Optional[str] = None,
    unknown_queue_policy: Optional[str] = None,
) -> RouteDecision:
    """容量路由（**唯一**实现）：队列维度 + 容量门 + 确定性排序 + 稳定原因。

    ``build_dispatch_plan`` / 真实派发 / flow 侧 assignment 节点都走这里——
    排序键在 ``DispatchCandidate.sort_key`` 里只有一份，本函数不再复制判据。

    ``queue_name`` 给定时**先按队列过滤再排序**：``active_tasks`` 等容量
    指标只在同一队列内可比（A64-05）。跨队列候选以
    ``REASON_QUEUE_MISMATCH`` 进入 ``rejected``，未声明队列的候选以
    ``REASON_QUEUE_UNKNOWN`` 进入 ``rejected``（默认策略 skip），不静默消失。
    B75-01：unknown 只在 ``assume-default`` + 默认队列时被显式纳入；该假设
    必须留 WARNING，并通过 ``queue_source=assumed-default`` 透出。
    """
    policy = resolve_unknown_queue_policy(unknown_queue_policy)
    candidate_list = list(candidates)
    if queue_name is not None:
        unknown_ids: List[str] = []
        for item in candidate_list:
            cand = (
                item if isinstance(item, DispatchCandidate)
                else DispatchCandidate.from_service_info(item)
            )
            if cand.queue_name is None:
                unknown_ids.append(cand.instance_id)
        if unknown_ids:
            record_unknown_queue(queue_name, len(unknown_ids))
            assume = (
                policy == UNKNOWN_QUEUE_ASSUME_DEFAULT
                and queue_name == DEFAULT_DISPATCH_QUEUE
            )
            if assume:
                logger.warning(
                    "派发队列 %s：%d 个 worker 未声明 queue_name，按 %s=%s "
                    "显式当作默认队列参与定向（queue_source=assumed-default）: %s",
                    queue_name, len(unknown_ids), UNKNOWN_QUEUE_POLICY_ENV,
                    UNKNOWN_QUEUE_ASSUME_DEFAULT, unknown_ids,
                )
            else:
                logger.warning(
                    "派发队列 %s：%d 个 worker 未声明 queue_name（%s），按 %s=%s "
                    "跳过定向（unknown 不等于默认队列；消息仍按调用方队列广播）: %s",
                    queue_name, len(unknown_ids), REASON_QUEUE_UNKNOWN,
                    UNKNOWN_QUEUE_POLICY_ENV, policy, unknown_ids,
                )
    allowed, rejected = filter_dispatchable(
        candidate_list,
        queue_name=queue_name,
        unknown_queue_policy=policy,
    )
    if not allowed:
        return RouteDecision(
            target=None, reason=REASON_ROUTE_NO_TARGET,
            allowed=allowed, rejected=rejected,
        )
    target = sorted(allowed, key=lambda c: c.sort_key())[0]
    if target.queue_name is not None:
        queue_source = QUEUE_SOURCE_DECLARED
        target_queue_name: Optional[str] = target.queue_name
    elif queue_name == DEFAULT_DISPATCH_QUEUE:
        queue_source = QUEUE_SOURCE_ASSUMED_DEFAULT
        target_queue_name = DEFAULT_DISPATCH_QUEUE
    else:
        # 不做队列过滤（queue_name=None）时不存在「假设默认队列」的语义。
        queue_source = QUEUE_SOURCE_DECLARED
        target_queue_name = queue_name
    return RouteDecision(
        target=target,
        reason=REASON_ROUTE_CAPACITY,
        allowed=allowed,
        rejected=rejected,
        queue_source=queue_source,
        target_queue_name=target_queue_name,
    )


# ------------------------------------------------------------------ 显式派发目标闸


def normalize_target_instance_id(raw: Any) -> Optional[str]:
    """规范化显式派发目标：非字符串 / 空白 ⇒ ``None``（= 无目标 = 放行）。"""
    if not isinstance(raw, str):
        return None
    return raw.strip() or None


def target_mismatch(message_data: Any, instance_id: Any) -> Optional[str]:
    """显式派发目标闸：消息体指向**别的** worker ⇒ 返回原因，否则 ``None``。

    纯函数（无 Redis、无副作用），供 worker 消费入口与单测共用同一份判据。

    **红线（与盘闸同口径，缺一不可）**——下列情形一律返回 ``None``（放行）：

    - 消息体不是 dict / 无 ``target_instance_id`` / 该值为空串或非字符串
      （存量广播消息、老生产方、畸形消息）；
    - 本机 ``instance_id`` 不可知（注册表未启用 / 骨架 worker）——**无法证明**
      「不该我跑」时不得拦（宁可白跑一次，不可误拦本机该跑的任务）；
    - 目标就是本机。
    """
    if not isinstance(message_data, dict):
        return None
    target = normalize_target_instance_id(message_data.get(TARGET_INSTANCE_FIELD))
    if target is None:
        return None
    me = normalize_target_instance_id(instance_id)
    if me is None or target == me:
        return None
    return (
        f"{TARGET_INSTANCE_FIELD}={target} 非本机（{me}）："
        "显式派发给别的 worker（交接让出）"
    )


# ------------------------------------------------------------------ registry 读侧


def load_worker_capacity(
    redis_client: Any,
    *,
    service_type: str = DEFAULT_WORKER_SERVICE_TYPE,
) -> List[DispatchCandidate]:
    """读取注册表里全部 worker 的容量快照（Redis 故障 → 空列表）。

    走既有 ``ServiceRegistry.list_services``（同一 Redis 键族，零新增键）。
    """
    if redis_client is None:
        return []
    try:
        from .registry import ServiceRegistry

        registry = ServiceRegistry(redis_client)
        services = registry.list_services(service_type)
    except Exception as exc:  # noqa: BLE001 — 派发侧读不到容量不阻塞派发
        logger.warning("读取 worker 容量快照失败（按无容量信息处理）: %s", _scrub_exception_text(exc))
        return []
    return [DispatchCandidate.from_service_info(info) for info in services]


def shadow_dispatch_plan(
    redis_client: Any,
    issue_ids: Optional[Iterable[str]] = None,
    *,
    queue_name: Optional[str] = None,
    service_type: str = DEFAULT_WORKER_SERVICE_TYPE,
) -> Dict[str, Any]:
    """影子决策：**只算不发**（验收 5：生产队列零新增）。

    返回一份可观测的决策记录——目标 worker、本轮可派候选、被拒候选及原因、
    计划队列名。**不调用 ``enqueue_task``、不写任何 Redis 键**（连台账都不写，
    影子期唯一输出是 HTTP 响应/日志）。
    """
    shadow = shadow_mode_from_env()
    queue = queue_name or dispatch_queue_from_env()
    candidates = load_worker_capacity(redis_client, service_type=service_type)
    route = decide_route(candidates, queue_name=queue)
    return {
        "shadow": shadow,
        "queue": queue,
        "queue_source": route.queue_source,
        "issue_ids": list(issue_ids or []),
        "target": route.target_instance_id,
        "reason": route.reason,
        "allowed": [c.instance_id for c in route.allowed],
        "rejected": [
            {"instance_id": r.instance_id, "host": r.host, "reason": r.reason}
            for r in route.rejected
        ],
        "stale": [c.instance_id for c in candidates if c.capacity_stale],
        "enqueued": 0,
    }
