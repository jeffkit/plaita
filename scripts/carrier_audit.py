"""执行载体对账器（plaita#316 Stage 0：只读观测，「非终态 × 0 载体」的统一信号）。

「载体」= *会自行触发推进的线索*（无人介入、到期即燃）。按此判据（A2 r59
评审钉死）：schedule 行不算（tick 只创建新执行）、checkpoint/fence/start-dedup
不算（被动状态或推进阻碍物）。任何「自行推进」必须先落成四类存放之一——
分类对执行模型闭合：

1. 队列消息：stream 本体 ∪ PEL（``task_queue.enqueue_task`` XADD / 消费组
   ``plaita-workers``）；queued 占位行的消息在 stream 里**等首投**（未进
   PEL）——只查 PEL 会把「刚 tick 的调度执行 + 消费积压」误报 none（A2
   修正①），故本审计查 stream 本体（XRANGE）。PEL 成员必然仍在 stream
   本体里（XACK 不删条目；body 缺失的 PEL 幽灵——「只 XDEL 会在 PEL 留下
   pending 幽灵」`task_queue` 清理注释——无载荷不可归属、也不再会被投递，
   不算载体），故归属一律以 stream 本体为准。
2. 事件订阅：``{prefix}data:{subscription_id}``（``event/redis.py``
   RedisEventSubscriptionStorage；key 存在 = 未过期 = 活载体，过期判定交给
   Redis TTL，不复制 worker 的超时语义）。
3. 定时器：``plaita:{subtype}:queue`` LIST（``flow_worker`` 挂起投递，delay/
   approval 等；条目 JSON 带 execution_id）。
4. 死信/隔离流：``{stream}:dlq`` 与 ``{dlq}:quarantine:{tenant}``（A2 补充
   存放处：消息形状、地址已知、一条命令可重放——这类执行按三载体规格会误报
   none，但 runbook 动作完全不同（replay vs 从头排查））。

指标（全部 gauge——systemd timer 每轮拉起新进程，counter 每轮归零 rate()
全废，A2 修正③；gauge 不带 ``_total`` 后缀，metrics.py 家规）：

- ``plaita_execution_carrier{status,carrier,tenant}``：非终态执行计数。
  每轮对 status×carrier 各 cell **显式发 0 或 N**（尤其 none cell 恒发），
  「持续 2 分钟」的持久性判定交给 PromQL ``for: 2m``，不放本进程（timer
  拉起的进程无跨轮状态）。carrier=unknown（工具/数据故障）不入 page 规则
  ——把工具故障翻页成全量 none 风暴是最坏的失败形态（A2）。
- ``plaita_execution_carrier_age_seconds{status,tenant}``：该 cell 内最老
  非终态执行的年龄（now − last_update_time，缺失回退 start_time；步界才
  写，长 AGENTRUN 期间可陈旧数小时——**纯信息面**，mode 5「租约新鲜 +
  无步界推进」的检测面是 reap_zombie 明确跳过的形态，本单不吞（A2 出圈
  声明），Stage 1 reconciler 再收）。
- ``plaita_carrier_violations{reason}``：reason ∈ {missing（非终态×none，
  六类失联统一落点；第六类幽灵 queued 占位行 = schedule 入队失败且清理
  失败的残留，``schedule_service._discard_queued_placeholder`` 自证「将
  残留 queued 行，需人工清理」——落点 status="queued"×none）/ duplicate
  （同一执行多条队列消息）/ orphan_terminal（终态执行仍挂载体）/
  unbound_subscription（活订阅 correlation_id 不指向任何在册执行——外部
  订阅者若用执行外 correlation id 会抬升此值，信息面）}。
- 自指标：``plaita_carrier_audit_last_success_timestamp`` /
  ``_duration_seconds`` / ``_check_errors``（单条/单键级检查错误计数）/
  ``_sources_failed``（整源读取失败数）。meta-alert 判据 = last_success
  停止推进 **或** sources_failed>0——audit 自身挂掉不能静默全零。

只读纪律（A2 清单 7 条的全条对照）：
  1. Redis 命令白名单由 :class:`ReadOnlyRedis` 包装层强制，白名单外一律
     异常（fail-loud 优于静默降级）；
  2. 禁 import ``flow_worker``；共享只走 ``plaita.storage.redis`` /
     ``plaita.tenant_context`` 的键构造纯函数，其余键拼接就地复制并注释
     来源行号；
  3. 本模块 except 只允许「单源/单条」隔离粒度：log + 计数，禁 except:
     pass，禁复制 worker 各 except 分支的语义判断；
  4. 不写任何 Redis 键（含自身游标/去重键，扫描态全在内存）；快照 JSON
     落文件 tmp+rename 原子写；不写 DLQ/隔离流（重放属 Stage 1 恢复面）；
  5. 指标在单轮扫描收口统一 flush（扫一半 crash 不留半套 gauge）；age 用
     单次 now() 快照；
  6. 状态枚举/键形状硬编码在本文件并注释来源；不读运行时配置（防配置代
    际漂移悄悄改判定面）——env 只提供**部署拓扑**（租户/stream/队列清单
     与连接参数），缺失即 fail-visible（AuditConfigError，退出码 2），
     绝不静默按空清单扫出「全零健康」；
  7. diff 门：``plaita/server/**`` 与 ``plaita/storage/**`` 零改动；探针
     全部合成数据（fakeredis + 临时 ns），不碰真实流。

用法：
    export PLAITA_CARRIER_AUDIT_REDIS_URL=redis://localhost:6379/0
    export PLAITA_CARRIER_AUDIT_TENANTS=default              # 逗号分隔，必填
    export PLAITA_CARRIER_AUDIT_STREAMS=plaita:flow:queue    # 逗号分隔，必填（v2-console 每仓自定义）
    python scripts/carrier_audit.py --once                   # 单轮（systemd timer 用）
    python scripts/carrier_audit.py --loop                   # 常驻循环（间隔 env 可调，缺省 60s）

输出：``$PLAITA_CARRIER_AUDIT_OUTPUT_DIR/carrier_metrics.prom``（Prometheus
textfile 约定）+ ``carrier_snapshot.json``（schema 2：含 per-execution 段，
#321 reconciler shadow 消费——单分类器、双视输出）。失败轮次**不覆写**上一轮产物
（last_success 停止推进即 meta-alert 信号）。

回滚 = 停掉 timer，无残留键（全程只读）。
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from plaita.core.url_redaction import _scrub_exception_text  # noqa: E402
from plaita.storage.redis import (  # noqa: E402
    TERMINAL_EXECUTION_STATUSES,
    execution_index_key,
)
from plaita.tenant_context import tenant_namespace  # noqa: E402

logger = logging.getLogger("plaita.scripts.carrier_audit")

# ---------------------------------------------------------------------------
# 硬编码判定面（纪律 6：来源行号注释，不读运行时配置）
# ---------------------------------------------------------------------------

#: 非终态状态枚举 = 全状态 − TERMINAL_EXECUTION_STATUSES（storage/redis.py:325，
#: {completed, error, cancelled}）。queued 占位行见 schedule_service._write_queued_placeholder。
NON_TERMINAL_STATUSES: Tuple[str, ...] = ("queued", "running", "suspended")

#: 载体种类（存放处）与 gauge carrier 标签全集。
CARRIER_KINDS: Tuple[str, ...] = ("pel", "subscription", "timer", "dlq")
CARRIER_LABELS: Tuple[str, ...] = CARRIER_KINDS + ("multiple", "none", "unknown")

#: 违例 reason 枚举（owner #316 表 + A2 r59 分类学补充的落点说明见模块 docstring）。
VIOLATION_REASONS: Tuple[str, ...] = (
    "missing",
    "duplicate",
    "orphan_terminal",
    "unbound_subscription",
)

# 键形状复制区（纪律 2：宁可复制 3 行拼接并注释来源，不背整个 worker 的 import 面）。
PAYLOAD_FIELD = "payload"  # task_queue.py:35（信封载荷字段名）
DEFAULT_DLQ_SUFFIX = ":dlq"  # task_queue.py:38 DEFAULT_DLQ_SUFFIX / :328 dlq_stream_key
QUARANTINE_FMT = "{dlq_key}:quarantine:{tenant_id}"  # task_queue.py:1329（隔离流键）
STATE_KEY_FMT = "{ns}:execution:{execution_id}"  # storage/redis.py _index_member_id 同源
SUBSCRIPTION_DATA_FMT = "{prefix}data:{subscription_id}"  # event/redis.py:1716

#: 环境变量名（部署拓扑；判定面常量一律不进 env——纪律 6）。
ENV_REDIS_URL = "PLAITA_CARRIER_AUDIT_REDIS_URL"
ENV_TENANTS = "PLAITA_CARRIER_AUDIT_TENANTS"
ENV_STREAMS = "PLAITA_CARRIER_AUDIT_STREAMS"
ENV_TIMER_QUEUES = "PLAITA_CARRIER_AUDIT_TIMER_QUEUES"
ENV_SUBSCRIPTION_PREFIX = "PLAITA_CARRIER_AUDIT_SUBSCRIPTION_PREFIX"
ENV_OUTPUT_DIR = "PLAITA_CARRIER_AUDIT_OUTPUT_DIR"
ENV_INTERVAL = "PLAITA_CARRIER_AUDIT_INTERVAL_SECONDS"

DEFAULT_TIMER_QUEUES = "plaita:delay:queue,plaita:approval:queue"  # flow_worker 挂起投递注释所列 subtype
DEFAULT_SUBSCRIPTION_PREFIX = "plaita:subscription:"  # event/redis.py:2332 缺省
DEFAULT_OUTPUT_DIR = "/tmp/plaita-carrier-audit"
DEFAULT_INTERVAL_SECONDS = 60.0  # 与「2 分钟内可见」SLO 对齐（timer 周期应同值）

#: 快照里 carrier=none 样本上限（只进 JSON 快照，不进 Prometheus——标签基数有界）。
MISSING_SAMPLES_CAP = 20
#: MGET 批大小（状态行批量读）。
MGET_BATCH = 500


class AuditConfigError(RuntimeError):
    """部署拓扑配置缺失/非法——fail-visible（退出码 2），绝不静默按空清单扫。"""


class AuditCommandNotAllowed(RuntimeError):
    """只读白名单外的 Redis 命令被拒（纪律 1：fail-loud）。"""


class ReadOnlyRedis:
    """Redis 只读白名单包装层（A2 纪律 1）。

    白名单 = 本审计全部合法读取面（SCAN 类/GET 类/ZRANGE/XLEN/XRANGE/
    LRANGE/TYPE/PTTL）。白名单外命令（含一切写命令与 XPENDING 之外的管理
    命令）一律 :class:`AuditCommandNotAllowed`——Stage 0 工具，fail-loud
    优于静默降级。
    """

    _ALLOWED = frozenset(
        {
            "get",
            "mget",
            "exists",
            "type",
            "pttl",
            "scan_iter",
            "zrange",
            "zcard",
            "xlen",
            "xrange",
            "lrange",
        }
    )

    def __init__(self, client: Any) -> None:
        self._client = client

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        if name not in self._ALLOWED:
            raise AuditCommandNotAllowed(
                f"carrier_audit 只读白名单外命令被拒: {name}"
            )
        return getattr(self._client, name)


@dataclass(frozen=True)
class AuditConfig:
    """部署拓扑（env/CLI 提供）；判定面常量不在此处（纪律 6）。"""

    redis_url: str
    tenants: Tuple[str, ...]
    streams: Tuple[str, ...]
    timer_queues: Tuple[str, ...]
    subscription_prefix: str
    output_dir: Path
    interval_seconds: float


def _csv_env(value: str) -> Tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def load_config(
    *,
    redis_url: Optional[str] = None,
    tenants: Optional[str] = None,
    streams: Optional[str] = None,
    timer_queues: Optional[str] = None,
    subscription_prefix: Optional[str] = None,
    output_dir: Optional[str] = None,
    interval_seconds: Optional[str] = None,
) -> AuditConfig:
    """env（可被 CLI 参数覆盖）→ :class:`AuditConfig`。

    租户与 stream 清单**必填**（A2 修正①②：stream 清单缺失会让 queued 误报
    none；租户清单决定扫描命名空间）——缺失即 :class:`AuditConfigError`，
    由调用方转成退出码 2（systemd 日志可见），绝不静默扫出全零健康。
    """

    def _require(value: Optional[str], env_name: str, what: str) -> str:
        raw = value if value is not None else os.environ.get(env_name, "")
        raw = raw.strip()
        if not raw:
            raise AuditConfigError(
                f"carrier_audit 配置缺失：{what}（env {env_name}）——"
                "fail-visible，不按空清单扫描"
            )
        return raw

    interval_raw = (
        interval_seconds
        if interval_seconds is not None
        else os.environ.get(ENV_INTERVAL, "")
    ).strip()
    interval = DEFAULT_INTERVAL_SECONDS
    if interval_raw:
        try:
            interval = float(interval_raw)
        except ValueError as exc:
            raise AuditConfigError(
                f"{ENV_INTERVAL}={interval_raw!r} 非法（期望秒数）: {exc}"
            ) from exc
        if interval <= 0:
            raise AuditConfigError(f"{ENV_INTERVAL}={interval_raw!r} 必须为正数")

    return AuditConfig(
        redis_url=_require(redis_url, ENV_REDIS_URL, "Redis URL"),
        tenants=_csv_env(_require(tenants, ENV_TENANTS, "租户清单")),
        streams=_csv_env(_require(streams, ENV_STREAMS, "stream 清单")),
        timer_queues=_csv_env(
            timer_queues
            if timer_queues is not None
            else os.environ.get(ENV_TIMER_QUEUES, DEFAULT_TIMER_QUEUES)
        )
        or _csv_env(DEFAULT_TIMER_QUEUES),
        subscription_prefix=(
            subscription_prefix
            if subscription_prefix is not None
            else os.environ.get(ENV_SUBSCRIPTION_PREFIX, DEFAULT_SUBSCRIPTION_PREFIX)
        ),
        output_dir=Path(
            output_dir
            if output_dir is not None
            else os.environ.get(ENV_OUTPUT_DIR, DEFAULT_OUTPUT_DIR)
        ),
        interval_seconds=interval,
    )


# ---------------------------------------------------------------------------
# 扫描
# ---------------------------------------------------------------------------


@dataclass
class _Row:
    """一条可读执行状态行的审计视图。"""

    execution_id: str
    tenant: str
    status: str
    last_update_time: Optional[str]
    start_time: Optional[str]
    corrupt: bool = False


@dataclass
class RoundResult:
    """单轮扫描结果（gauge 族的唯一事实源，收口统一 flush——纪律 5）。"""

    generated_at: str
    duration_seconds: float
    tenants: List[str] = field(default_factory=list)
    cells: Dict[Tuple[str, str, str], int] = field(default_factory=dict)
    ages: Dict[Tuple[str, str], float] = field(default_factory=dict)
    violations: Dict[str, int] = field(default_factory=dict)
    check_errors: int = 0
    sources_failed: List[str] = field(default_factory=list)
    missing_samples: List[Dict[str, str]] = field(default_factory=list)
    observed_statuses: Set[str] = field(default_factory=set)
    #: schema 2 快照的 per-execution 段（#321 shadow 同源消费：非终态全集 +
    #: 带载体终态行〔orphan 候选〕）。verdict 复用 ``_verdict``（含源失败轮的
    #: unknown），消费方按它聚合可逐格重放 cells——双向互证的数据前提。
    executions: List[Dict[str, Any]] = field(default_factory=list)


def _source(
    failed: List[str],
    counter: List[int],
    name: str,
    fn: Callable[[], Any],
    default: Any,
) -> Any:
    """单源隔离执行（纪律 3）：失败 = log + 计数 + 该源按「不可判」处理。

    整源读取失败的语义：该源**观察到的在场**仍可信（有就是有），**缺席**
    不可信（可能因读失败漏看）——缺席判 none 只在全部相关源健康时成立，
    见 ``_verdict``。
    """
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 — 单源隔离：log+计数，不上抛不吞声音
        counter[0] += 1
        failed.append(name)
        logger.error(
            "carrier_audit 数据源 %s 读取失败（本轮该源缺席不可判）: %s",
            name,
            _scrub_exception_text(exc),
        )
        return default


def _item_error(counter: List[int], what: str, exc: Exception) -> None:
    """单条/单键级隔离（纪律 3）：log + ``_check_errors`` 计数。"""
    counter[0] += 1
    logger.warning("carrier_audit 单条不可读 %s: %s", what, _scrub_exception_text(exc))


def _parse_epoch(value: Optional[str]) -> Optional[float]:
    """ISO 时间串 → epoch 秒；缺失/不可解析返回 None（调用方决定回退）。"""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except (TypeError, ValueError):
        return None


def scan_round(client: ReadOnlyRedis, config: AuditConfig, *, now: Optional[float] = None) -> RoundResult:
    """一轮全量对账：非终态执行 × 四存放处载体存在性（全程只读）。

    任何一轮内 ``now`` 只取一次（纪律 5）。输出 :class:`RoundResult`——
    调用方在扫描**收口后**统一渲染落盘；本函数不写任何外部状态。
    """
    started = time.monotonic()
    now_ts = now if now is not None else time.time()
    failed: List[str] = []
    counter = [0]  # check_errors（list 装可变计数，便于 _source/_item_error 闭包共享）

    # ---- 源 1：执行状态行（按租户 ns 分扫，A2 修正②）----
    rows: List[_Row] = []
    executions_healthy = True
    for tenant in config.tenants:
        ns = tenant_namespace(tenant)
        index_key = execution_index_key(ns)

        def _scan_tenant(tenant=tenant, ns=ns, index_key=index_key) -> List[_Row]:
            member_ids = client.zrange(index_key, 0, -1)
            found: List[_Row] = []
            for offset in range(0, len(member_ids), MGET_BATCH):
                batch = member_ids[offset : offset + MGET_BATCH]
                keys = [
                    STATE_KEY_FMT.format(ns=ns, execution_id=eid) for eid in batch
                ]
                raws = client.mget(keys)
                for eid, raw in zip(batch, raws):
                    # 索引成员无行 = 终态行 TTL 到期后的索引残影：与存储回填
                    # 同口径跳过（行是存在性的真相，索引只是查找辅助）。
                    if not raw:
                        continue
                    try:
                        data = json.loads(raw)
                    except (TypeError, ValueError) as exc:
                        # 行存在但不可读：执行在册却不可评估——按 A2 记
                        # carrier=unknown（不从 cells 蒸发，也不误报 none）。
                        _item_error(counter, f"状态行 {ns}/execution/{eid}", exc)
                        found.append(
                            _Row(
                                execution_id=str(eid),
                                tenant=tenant,
                                status="",
                                last_update_time=None,
                                start_time=None,
                                corrupt=True,
                            )
                        )
                        continue
                    if not isinstance(data, dict):
                        _item_error(
                            counter,
                            f"状态行 {ns}/execution/{eid}",
                            ValueError(f"非对象: {type(data).__name__}"),
                        )
                        found.append(
                            _Row(
                                execution_id=str(eid),
                                tenant=tenant,
                                status="",
                                last_update_time=None,
                                start_time=None,
                                corrupt=True,
                            )
                        )
                        continue
                    found.append(
                        _Row(
                            execution_id=str(eid),
                            tenant=tenant,
                            status=str(data.get("status") or ""),
                            last_update_time=data.get("last_update_time"),
                            start_time=data.get("start_time"),
                        )
                    )
            return found

        tenant_rows = _source(
            failed, counter, f"executions:{tenant}", _scan_tenant, None
        )
        if tenant_rows is None:
            executions_healthy = False
            continue
        rows.extend(tenant_rows)

    all_eids = {row.execution_id for row in rows}

    # ---- 源 2：队列消息（stream 本体；A2 修正①，PEL ⊆ 本体见模块 docstring）----
    queue_counts: Dict[str, int] = {}

    def _scan_queue() -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for stream in config.streams:
            for _msg_id, fields in client.xrange(stream, min="-", max="+"):
                raw = None
                for key in (PAYLOAD_FIELD, b"payload"):
                    if key in fields:
                        raw = fields[key]
                        break
                if raw is None:
                    continue
                try:
                    body = json.loads(raw)
                except (TypeError, ValueError):
                    # 不可归属的畸形消息（DLQ 候选）不归属任何执行，跳过。
                    continue
                eid = body.get("execution_id") if isinstance(body, dict) else None
                if eid:
                    counts[str(eid)] = counts.get(str(eid), 0) + 1
        return counts

    queue_counts = _source(failed, counter, "queue", _scan_queue, {})

    # ---- 源 3：事件订阅（key 存在 = 活载体，过期判定归 Redis TTL）----
    subs_by_cid: Dict[str, int] = {}

    def _scan_subscriptions() -> Dict[str, int]:
        counts: Dict[str, int] = {}
        keys = list(client.scan_iter(match=f"{config.subscription_prefix}data:*"))
        for offset in range(0, len(keys), MGET_BATCH):
            batch = keys[offset : offset + MGET_BATCH]
            for key, raw in zip(batch, client.mget(batch)):
                if not raw:
                    continue
                try:
                    sub = json.loads(raw)
                except (TypeError, ValueError) as exc:
                    _item_error(counter, f"订阅行 {key}", exc)
                    continue
                cid = sub.get("correlation_id") if isinstance(sub, dict) else None
                if cid:
                    counts[str(cid)] = counts.get(str(cid), 0) + 1
        return counts

    subs_by_cid = _source(failed, counter, "subscriptions", _scan_subscriptions, {})

    # ---- 源 4：定时器服务队列（LIST，条目 JSON 带 execution_id）----
    timer_counts: Dict[str, int] = {}

    def _scan_timer() -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for queue_key in config.timer_queues:
            for raw in client.lrange(queue_key, 0, -1):
                try:
                    task = json.loads(raw)
                except (TypeError, ValueError):
                    continue
                eid = task.get("execution_id") if isinstance(task, dict) else None
                if eid:
                    counts[str(eid)] = counts.get(str(eid), 0) + 1
        return counts

    timer_counts = _source(failed, counter, "timer", _scan_timer, {})

    # ---- 源 5：死信/隔离流（A2 补充的第四存放处）----
    dlq_counts: Dict[str, int] = {}

    def _scan_dlq() -> Dict[str, int]:
        counts: Dict[str, int] = {}

        def _absorb(stream_key: str) -> None:
            for _msg_id, fields in client.xrange(stream_key, min="-", max="+"):
                raw = fields.get(PAYLOAD_FIELD)
                if raw is None:
                    continue
                try:
                    envelope = json.loads(raw)
                except (TypeError, ValueError):
                    continue
                body = envelope.get("payload") if isinstance(envelope, dict) else None
                eid = body.get("execution_id") if isinstance(body, dict) else None
                if eid:
                    counts[str(eid)] = counts.get(str(eid), 0) + 1

        for stream in config.streams:
            dlq_key = f"{stream}{DEFAULT_DLQ_SUFFIX}"
            _absorb(dlq_key)
            for tenant in config.tenants:
                _absorb(QUARANTINE_FMT.format(dlq_key=dlq_key, tenant_id=tenant))
        return counts

    dlq_counts = _source(failed, counter, "dlq", _scan_dlq, {})

    # ---- 装配：执行 × 载体 → cell / 违例 / 年龄 ----
    result = RoundResult(
        generated_at=datetime.fromtimestamp(now_ts).astimezone().isoformat(),
        duration_seconds=0.0,
        tenants=list(config.tenants),
    )
    result.sources_failed = list(failed)
    present_by_eid: Dict[str, Set[str]] = {}
    for eid in queue_counts:
        present_by_eid.setdefault(eid, set()).add("pel")
    for eid in subs_by_cid:
        present_by_eid.setdefault(eid, set()).add("subscription")
    for eid in timer_counts:
        present_by_eid.setdefault(eid, set()).add("timer")
    for eid in dlq_counts:
        present_by_eid.setdefault(eid, set()).add("dlq")

    def _verdict(row: _Row, present: Set[str]) -> str:
        if present:
            if len(present) == 1:
                return next(iter(present))
            return "multiple"
        # 缺席判 none 的前提：全部相关源健康（有失败的源时缺席不可判）。
        if failed:
            return "unknown"
        return "none"

    cell_counts: Dict[Tuple[str, str, str], int] = {}
    ages: Dict[Tuple[str, str], float] = {}
    missing_samples: List[Dict[str, str]] = []
    duplicate_count = 0
    orphan_terminal_count = 0
    for row in rows:
        present = present_by_eid.get(row.execution_id, set())
        terminal = row.status in TERMINAL_EXECUTION_STATUSES
        if terminal:
            if present:
                orphan_terminal_count += 1
                # schema 2：带载体终态行入 per-execution 段（gc 意图候选）；
                # 无载体终态行不入段（对账面纯噪声）。
                result.executions.append(
                    {
                        "execution_id": row.execution_id,
                        "tenant": row.tenant,
                        "status": row.status,
                        "carriers": sorted(present),
                        "messages": queue_counts.get(row.execution_id, 0),
                        "verdict": _verdict(row, present),
                    }
                )
            continue
        if row.corrupt or not row.status:
            # 行不可读/状态为空：carrier 与 status 均不可判 → 落 unknown cell
            #（不入 page 规则，见 missing_help）；per-execution 段同步收
            #（verdict=unknown，消费方聚合可重放 unknown cell）。
            cell_counts[(row.tenant, "unknown", "unknown")] = (
                cell_counts.get((row.tenant, "unknown", "unknown"), 0) + 1
            )
            result.observed_statuses.add("unknown")
            result.executions.append(
                {
                    "execution_id": row.execution_id,
                    "tenant": row.tenant,
                    "status": "",
                    "carriers": sorted(present),
                    "messages": queue_counts.get(row.execution_id, 0),
                    "verdict": "unknown",
                }
            )
            continue
        if row.status not in NON_TERMINAL_STATUSES:
            # 非终态枚举外的状态：仍按非终态判（不漏 missing），告警并入渲染
            # 集合（不静默蒸发）。
            logger.warning(
                "carrier_audit: 枚举外非终态状态 %r（execution=%s）——入账并告警",
                row.status,
                row.execution_id,
            )
        verdict = _verdict(row, present)
        cell_counts[(row.tenant, row.status, verdict)] = (
            cell_counts.get((row.tenant, row.status, verdict), 0) + 1
        )
        result.observed_statuses.add(row.status)
        result.executions.append(
            {
                "execution_id": row.execution_id,
                "tenant": row.tenant,
                "status": row.status,
                "carriers": sorted(present),
                "messages": queue_counts.get(row.execution_id, 0),
                "verdict": verdict,
            }
        )
        if verdict == "none" and len(missing_samples) < MISSING_SAMPLES_CAP:
            missing_samples.append(
                {
                    "execution_id": row.execution_id,
                    "tenant": row.tenant,
                    "status": row.status,
                }
            )
        if verdict == "none" and row.status == "queued":
            # 第六类（幽灵 queued 占位行）等 queued×none 形态的信息样本。
            logger.info(
                "carrier_audit: 非终态 0 载体 execution=%s tenant=%s status=queued",
                row.execution_id,
                row.tenant,
            )
        if "pel" in present and queue_counts.get(row.execution_id, 0) > 1:
            duplicate_count += 1
        # 年龄（信息面）：last_update_time 缺失回退 start_time，皆不可解析则不计。
        epoch = _parse_epoch(row.last_update_time) or _parse_epoch(row.start_time)
        if epoch is not None:
            age = max(0.0, now_ts - epoch)
            key = (row.tenant, row.status)
            if age > ages.get(key, -1.0):
                ages[key] = age

    result.cells = cell_counts
    result.ages = ages
    result.missing_samples = missing_samples
    result.check_errors = counter[0]
    result.violations = {
        "missing": sum(
            count
            for (_t, _s, carrier), count in cell_counts.items()
            if carrier == "none"
        ),
        # duplicate/unbound 只在全源健康时报告（读失败轮的 0 不可信）。
        "duplicate": duplicate_count if "queue" not in failed else 0,
        "orphan_terminal": orphan_terminal_count,
        "unbound_subscription": (
            sum(
                count
                for cid, count in subs_by_cid.items()
                if cid not in all_eids
            )
            if executions_healthy and "subscriptions" not in failed
            else 0
        ),
    }
    result.duration_seconds = time.monotonic() - started
    return result


# ---------------------------------------------------------------------------
# 渲染与落盘
# ---------------------------------------------------------------------------


def _escape_label(value: str) -> str:
    """Prometheus label 值转义（\\、"、换行）。"""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render_prom(result: RoundResult) -> str:
    """RoundResult → Prometheus text exposition（gauge 族，无 ``_total``）。"""
    lines: List[str] = []

    missing_help = (
        "非终态执行×载体存在性计数（gauge）。carrier=none 且非终态持续 2 分钟应 page"
        "（PromQL for: 2m）；carrier=unknown 为工具/数据故障轮次，不入 page 规则。"
        "第六类幽灵 queued 占位行落在 status=queued×carrier=none。"
    )

    def _series_header(name: str, help_text: str) -> None:
        lines.append(f"# HELP {name} {_escape_label(help_text)}")
        lines.append(f"# TYPE {name} gauge")

    _series_header("plaita_execution_carrier", missing_help)
    # 租户取配置全集 ∪ 观测到的租户：degraded/空轮也恒发 0（A2 修正④——
    # none cell 恒发，持久性判定交给 PromQL for: 2m）。
    tenants = sorted(set(result.tenants) | {t for (t, _s, _c) in result.cells})
    # 状态取非终态枚举 ∪ 观测到的枚举外状态（告警过，不静默蒸发）。
    statuses = tuple(NON_TERMINAL_STATUSES) + tuple(
        sorted(result.observed_statuses - set(NON_TERMINAL_STATUSES))
    )
    for tenant in tenants:
        for status in statuses:
            for carrier in CARRIER_LABELS:
                count = result.cells.get((tenant, status, carrier), 0)
                lines.append(
                    "plaita_execution_carrier"
                    f'{{status="{_escape_label(status)}",'
                    f'carrier="{_escape_label(carrier)}",'
                    f'tenant="{_escape_label(tenant)}"}} {count}'
                )
    _series_header(
        "plaita_execution_carrier_age_seconds",
        "cell 内最老非终态执行年龄（now−last_update_time，缺失回退 start_time；"
        "步界才写，长节点期间可陈旧——纯信息面，mode 5 出圈声明见 carrier_audit 模块注释）。",
    )
    for tenant in tenants:
        for status in NON_TERMINAL_STATUSES:
            age = result.ages.get((tenant, status))
            if age is None:
                continue
            lines.append(
                "plaita_execution_carrier_age_seconds"
                f'{{status="{_escape_label(status)}",'
                f'tenant="{_escape_label(tenant)}"}} {age:.3f}'
            )
    _series_header(
        "plaita_carrier_violations",
        "违例计数（gauge，timer 拉起进程计数器无意义故不带 _total）。missing=非终态×0 载体；"
        "duplicate=同一执行多条队列消息；orphan_terminal=终态执行仍挂载体；"
        "unbound_subscription=活订阅 correlation_id 不指向任何在册执行（外部订阅者可抬升，信息面）。",
    )
    for reason in VIOLATION_REASONS:
        lines.append(
            "plaita_carrier_violations"
            f'{{reason="{_escape_label(reason)}"}} {result.violations.get(reason, 0)}'
        )
    _series_header(
        "plaita_carrier_audit_last_success_timestamp",
        "最近一次完整扫描收口的 unix 时间戳（停推 = audit 挂掉/配置错，meta-alert 判据一）。",
    )
    lines.append(
        f"plaita_carrier_audit_last_success_timestamp {datetime.fromisoformat(result.generated_at).timestamp():.3f}"
    )
    _series_header(
        "plaita_carrier_audit_duration_seconds", "最近一轮扫描耗时（秒）。"
    )
    lines.append(f"plaita_carrier_audit_duration_seconds {result.duration_seconds:.6f}")
    _series_header(
        "plaita_carrier_audit_check_errors",
        "最近一轮单条/单键级检查错误数（损坏行/不可解析条目；对应执行记 unknown 不入 page）。",
    )
    lines.append(f"plaita_carrier_audit_check_errors {result.check_errors}")
    _series_header(
        "plaita_carrier_audit_sources_failed",
        "最近一轮整源读取失败数（>0 时缺席不可判，全部 none 判定改记 unknown；meta-alert 判据二）。",
    )
    lines.append(f"plaita_carrier_audit_sources_failed {len(result.sources_failed)}")
    return "\n".join(lines) + "\n"


def render_snapshot(result: RoundResult, config: AuditConfig) -> Dict[str, Any]:
    """RoundResult → console JSON 快照形状（含部署拓扑、none 样本与 per-execution 段）。

    schema 2（#321 起）：新增 ``executions`` 段——非终态全集 + 带载体终态行
    的逐执行明细（carriers/messages/verdict）。#321 reconciler shadow 以此为
    唯一候选集与 actual 侧来源（单分类器、双视输出）；既有各键与 gauge 族
    语义不变（加法扩展）。
    """
    return {
        "schema": 2,
        "generated_at": result.generated_at,
        "duration_seconds": round(result.duration_seconds, 6),
        "config": {
            "tenants": list(config.tenants),
            "streams": list(config.streams),
            "timer_queues": list(config.timer_queues),
            "subscription_prefix": config.subscription_prefix,
        },
        "cells": [
            {"tenant": tenant, "status": status, "carrier": carrier, "count": count}
            for (tenant, status, carrier), count in sorted(result.cells.items())
        ],
        "ages": [
            {"tenant": tenant, "status": status, "max_age_seconds": round(age, 3)}
            for (tenant, status), age in sorted(result.ages.items())
        ],
        "violations": {reason: result.violations.get(reason, 0) for reason in VIOLATION_REASONS},
        "check_errors": result.check_errors,
        "sources_failed": result.sources_failed,
        "missing_samples": result.missing_samples,
        "executions": result.executions,
    }


def _atomic_write(path: Path, content: str) -> None:
    """tmp + rename 原子写（纪律 4：不留半套产物）。"""
    import tempfile

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=path.name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def flush(result: RoundResult, config: AuditConfig) -> None:
    """单轮收口统一落盘（纪律 4/5）：prom textfile + JSON 快照，tmp+rename。"""
    _atomic_write(config.output_dir / "carrier_metrics.prom", render_prom(result))
    _atomic_write(
        config.output_dir / "carrier_snapshot.json",
        json.dumps(render_snapshot(result, config), ensure_ascii=False, indent=2) + "\n",
    )


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------


def build_client(redis_url: str) -> ReadOnlyRedis:
    """Redis URL → 只读包装客户端（连接超时口径与 reap 脚本一致）。"""
    import redis

    raw = redis.Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=5.0,
        socket_timeout=30.0,
    )
    return ReadOnlyRedis(raw)


def run_round(config: AuditConfig) -> RoundResult:
    """一轮扫描 + 收口落盘。落盘失败按轮级失败上抛（不覆写半套产物）。"""
    result = scan_round(build_client(config.redis_url), config)
    flush(result, config)
    logger.info(
        "carrier_audit 轮完成: %.3fs violations=%s sources_failed=%s check_errors=%d",
        result.duration_seconds,
        result.violations,
        result.sources_failed,
        result.check_errors,
    )
    return result


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="执行载体对账器（plaita#316 Stage 0，只读）")
    parser.add_argument("--once", action="store_true", help="单轮扫描后退出（缺省；systemd timer 用）")
    parser.add_argument("--loop", action="store_true", help="常驻循环（间隔 PLAITA_CARRIER_AUDIT_INTERVAL_SECONDS，缺省 60s）")
    parser.add_argument("--redis-url", default=None, help="覆盖 env PLAITA_CARRIER_AUDIT_REDIS_URL")
    parser.add_argument("--tenants", default=None, help="覆盖 env PLAITA_CARRIER_AUDIT_TENANTS（逗号分隔）")
    parser.add_argument("--streams", default=None, help="覆盖 env PLAITA_CARRIER_AUDIT_STREAMS（逗号分隔）")
    parser.add_argument("--output-dir", default=None, help="覆盖 env PLAITA_CARRIER_AUDIT_OUTPUT_DIR")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        config = load_config(
            redis_url=args.redis_url,
            tenants=args.tenants,
            streams=args.streams,
            output_dir=args.output_dir,
        )
    except AuditConfigError as exc:
        logger.error("%s", exc)
        return 2

    if args.loop:
        while True:
            try:
                run_round(config)
            except Exception as exc:  # noqa: BLE001 — 轮级失败：log 后续轮重试（不覆写产物）
                logger.error("carrier_audit 轮失败（保留上一轮产物）: %s", _scrub_exception_text(exc))
            time.sleep(config.interval_seconds)
        return 0  # pragma: no cover
    try:
        run_round(config)
    except Exception as exc:  # noqa: BLE001 — 单轮模式失败以非零码 fail-visible
        logger.error("carrier_audit 轮失败: %s", _scrub_exception_text(exc))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
