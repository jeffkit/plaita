"""执行载体对账器 shadow（plaita#321 A2 阶段 1：零控制动作，只记录 + gauge）。

定位（A2 r138 评审钉死的规格修正）：#316 ``carrier_audit`` 是「按存放处枚举」
的违规信号视图；本脚本是**同一分类器**之上的「按执行重建」意图视图——从状态行
（plaita#320 write-ahead state）重建「该执行应有的载体清单」，与快照里的实际
四存放处在场对差，输出 shadow 报告（若阶段 2/3 接管会补哪条、动作属 enqueue
类还是控制类）。**不补发、不终态化、不写任何状态行/Redis 键**——控制类动作
（fence+cancel / pending_lost / 终局化）明确留阶段 3（worker fenced CAS 通道，
#322 之后）；本脚本全程只读。

同源纪律（勿自造第二分类器）：候选集与 actual 侧一律消费 #316 的 console
JSON 快照（schema 2 的 per-execution 段，见 ``scripts/carrier_audit.py``）——
快照缺失/低版/过期 = 该源失败，缺席不可判（loss 类差异不进 gauge，意图记录
``verifiable=false``），与 #316 的 degraded 语义同构；双向互证（shadow 按执行
聚合重放快照 cells / missing / orphan_terminal，逐格对账）作为每轮固定校验
写进报告——「shadow 与 gauge 分歧」从第一天起就是可观测信号而非暗雷。

expected 侧推导（按状态 × ``pending.kind``；``PENDING_KINDS`` 五 kind 已全部进
schema，本轮写路径只产 event/service——timer/start/cancel 为 #321 预留，
推导面先按 schema 全集闭合）：

- ``queued`` → {message}（start 派发；占位行 ghost 同形——补发=阶段 2 enqueue；
  flow 已删型永败的终局化路径=阶段 3 控制类，此处只记意图）；
- ``running`` → pending ``None``（在途消息假设）/``service``/``cancel`` →
  {message}；``event`` → {subscription}；``timer`` → {timer}；
- ``suspended`` → pending kind 决定：event→{subscription} / timer→{timer} /
  service→{message} / cancel→{message}；``None`` = 老行 orphan（不可重建，
  #316 missing 面接管，不猜 expected）；
- ``terminal`` → {}（任何在场 = extra_carrier，意图=gc）；
- 枚举外非终态 → {}（保守：只报 extra_carrier，不猜 expected）。

「message」期望由 stream 本体/PEL（pel）**或** DLQ/隔离流（dlq）满足——
custody 在押 = 有载体不补（A2 r138 规范性义务：恢复走隔离流重放通道，
reconciler 不碰），该行产 ``custody_replay`` 信息意图、绝不产 lost_message。

到期分支（disarmed 判据，A2 r138「重建或 error(pending_lost)」的 shadow
预演）：``kind=event`` 的 deadline / ``kind=timer`` 的 due_at 已过且对应载体
缺席 → 意图 ``finalize_pending_lost``（控制类，阶段 3）；未过期 / 不可解析
（老行 migrated 缺 deadline）→ 重建类意图（enqueue 类，阶段 2）。

progress_at 停滞面（mode 5 → fence+cancel）不在本脚本扫描面：候选判据是
**载体存在性**（A2 r138 缺陷修正②——progress_at 阈值扫不到新鲜 queued 行），
mode 5 检测面仍归 reap_zombie，fence+cancel 属控制类动作留阶段 3。

gauge 族（与 #316 同形制：全格显式 0/N、无 ``_total`` 后缀、自指标四件、
收口统一 flush）：

- ``plaita_reconciler_shadow_candidates{status,tenant}``：候选集规模（快照
  per-execution 段：非终态全集 + 带载体终态行）。
- ``plaita_reconciler_shadow_diffs{kind}``：kind ∈ {lost_message,
  lost_subscription, lost_timer, extra_carrier}（只计 verifiable 差异；
  degraded 轮次全 0——把工具故障报成差异风暴是最坏的失败形态，同 #316
  unknown 不入 page 的理由）。
- ``plaita_reconciler_shadow_intents{intent,action_class}``：意图 × 动作类
  （enqueue=不写状态行的物化动作，阶段 2；control=fenced CAS 状态行写，
  阶段 3；observe=信息面）。
- 自指标：``plaita_reconciler_shadow_last_success_timestamp`` /
  ``_duration_seconds`` / ``_check_errors``（单条级错误计数）/
  ``_sources_failed``（快照源失败数；meta-alert 语义同 #316）。

只读纪律（对照 #316 清单）：复用 ``carrier_audit.ReadOnlyRedis`` 白名单
（状态行只 GET）；不写任何 Redis 键（无游标/守卫键，扫描态全在内存）；
报告/指标落盘 tmp+rename 原子写（复用 ``carrier_audit._atomic_write``）；
禁 import ``flow_worker``；except 只允许单条隔离粒度。

用法（与 #316 同形制）：
    export PLAITA_RECONCILER_SHADOW_REDIS_URL=redis://localhost:6379/0
    # 快照路径缺省指向 #316 的缺省产出（PLAITA_CARRIER_AUDIT_OUTPUT_DIR）
    python scripts/reconciler_shadow.py --once   # 单轮（systemd timer 用）
    python scripts/reconciler_shadow.py --loop   # 常驻循环（缺省 10s，#321 正文节奏）

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
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))  # scripts/ → carrier_audit
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # 仓根 → plaita.*

import carrier_audit  # noqa: E402 — 同目录脚本模块（单分类器来源）
from plaita.core.url_redaction import _scrub_exception_text  # noqa: E402
from plaita.storage.base import ExecutionState  # noqa: E402
from plaita.storage.redis import (  # noqa: E402
    TERMINAL_EXECUTION_STATUSES,
)
from plaita.tenant_context import tenant_namespace  # noqa: E402

logger = logging.getLogger("plaita.scripts.reconciler_shadow")

# ---------------------------------------------------------------------------
# 判定面常量（expected 推导与意图枚举；实际侧判据全部来自 carrier_audit）
# ---------------------------------------------------------------------------

#: 差异 kind 枚举（任务书四格：丢消息/丢订阅/丢定时器/多余载体）。
DIFF_KINDS: Tuple[str, ...] = (
    "lost_message",
    "lost_subscription",
    "lost_timer",
    "extra_carrier",
)

#: 意图枚举（阶段 2/3 接管时的动作面预演；shadow 只记录不执行）。
INTENT_KINDS: Tuple[str, ...] = (
    "start",  # queued×丢消息 → 补 start 派发（阶段 2，enqueue）
    "continue",  # running×丢消息 → continue 重投（阶段 2，enqueue）
    "redispatch_service",  # suspended×service×丢消息 → 补偿派发（阶段 2，enqueue）
    "rebuild_subscription",  # event 等待×丢订阅 → 重建订阅（阶段 2，enqueue）
    "rebuild_timer",  # timer 等待×丢定时器 → 重建定时器（阶段 2，enqueue）
    "finalize_pending_lost",  # 到期失装 → error(pending_lost)（阶段 3，控制类）
    "gc",  # 终态×多余载体 → 清载体（阶段 3，控制类）
    "custody_replay",  # DLQ/隔离流在押 → 恢复走重放通道（reconciler 出圈，observe）
    "inspect",  # 其余形态（含枚举外状态/预留 kind）→ 人工/阶段 3 定夺（observe）
)

#: 动作类（A2 r138 缺陷修正①的口径：enqueue 类不写状态行；控制类复用
#: worker 同款 fenced CAS 通道——不发明第二写路径）。
ACTION_CLASSES: Tuple[str, ...] = ("enqueue", "control", "observe")

#: ``pending.kind`` → 期望载体（引用 plaita/storage/base.py PENDING_KINDS 全集）。
_PENDING_KIND_CARRIER: Dict[str, str] = {
    "event": "subscription",
    "timer": "timer",
    "service": "message",
    "start": "message",
    "cancel": "message",
}

#: ``message`` 期望被哪些实际存放处满足（custody 判据：dlq 在押也算有载体）。
_MESSAGE_SATISFIED_BY = frozenset({"pel", "dlq"})

#: 环境变量名（部署拓扑；判定面常量一律不进 env——同 #316 纪律 6）。
ENV_REDIS_URL = "PLAITA_RECONCILER_SHADOW_REDIS_URL"
ENV_SNAPSHOT = "PLAITA_RECONCILER_SHADOW_SNAPSHOT"
ENV_OUTPUT_DIR = "PLAITA_RECONCILER_SHADOW_OUTPUT_DIR"
ENV_SNAPSHOT_MAX_AGE = "PLAITA_RECONCILER_SHADOW_SNAPSHOT_MAX_AGE_SECONDS"
ENV_INTERVAL = "PLAITA_RECONCILER_SHADOW_INTERVAL_SECONDS"

DEFAULT_OUTPUT_DIR = "/tmp/plaita-reconciler-shadow"
DEFAULT_INTERVAL_SECONDS = 10.0  # #321 正文节奏（每 10s）
#: 快照最大可接受陈旧度 = 2 个审计周期（A2 r138 §三：60s 快照 + 扫描时长
#: 擦线，「≤60s 被补」在阶段 1 观测面放宽为 ≤2 周期）。
DEFAULT_SNAPSHOT_MAX_AGE_SECONDS = 120.0
#: 报告里意图样本上限（超出计数进 totals，truncated=true）。
INTENT_SAMPLES_CAP = 200


class ShadowConfigError(RuntimeError):
    """部署拓扑配置缺失/非法——fail-visible（退出码 2），同 #316 语义。"""


@dataclass(frozen=True)
class ShadowConfig:
    """部署拓扑（env/CLI 提供）；判定面常量不在此处。"""

    redis_url: str
    snapshot_path: Path
    output_dir: Path
    snapshot_max_age_seconds: float
    interval_seconds: float


def default_snapshot_path() -> Path:
    """#316 快照缺省路径（跟随 ``PLAITA_CARRIER_AUDIT_OUTPUT_DIR`` 部署面）。"""
    audit_dir = os.environ.get(
        "PLAITA_CARRIER_AUDIT_OUTPUT_DIR", carrier_audit.DEFAULT_OUTPUT_DIR
    )
    return Path(audit_dir) / "carrier_snapshot.json"


def load_config(
    *,
    redis_url: Optional[str] = None,
    snapshot: Optional[str] = None,
    output_dir: Optional[str] = None,
    snapshot_max_age_seconds: Optional[str] = None,
    interval_seconds: Optional[str] = None,
) -> ShadowConfig:
    """env（可被 CLI 参数覆盖）→ :class:`ShadowConfig`。

    Redis URL **必填**（状态行只读 GET 面）——缺失即 :class:`ShadowConfigError`，
    由调用方转退出码 2。快照路径缺省指向 #316 缺省产出。
    """

    def _positive_float(raw: str, env_name: str, default: float) -> float:
        if not raw:
            return default
        try:
            value = float(raw)
        except ValueError as exc:
            raise ShadowConfigError(
                f"{env_name}={raw!r} 非法（期望秒数）: {exc}"
            ) from exc
        if value <= 0:
            raise ShadowConfigError(f"{env_name}={raw!r} 必须为正数")
        return value

    url = (redis_url if redis_url is not None else os.environ.get(ENV_REDIS_URL, "")).strip()
    if not url:
        raise ShadowConfigError(
            f"reconciler_shadow 配置缺失：Redis URL（env {ENV_REDIS_URL}）——"
            "fail-visible，状态行读取面不可缺"
        )
    return ShadowConfig(
        redis_url=url,
        snapshot_path=Path(
            snapshot
            if snapshot is not None
            else os.environ.get(ENV_SNAPSHOT, "")
            or str(default_snapshot_path())
        ),
        output_dir=Path(
            output_dir
            if output_dir is not None
            else os.environ.get(ENV_OUTPUT_DIR, DEFAULT_OUTPUT_DIR)
        ),
        snapshot_max_age_seconds=_positive_float(
            (
                snapshot_max_age_seconds
                if snapshot_max_age_seconds is not None
                else os.environ.get(ENV_SNAPSHOT_MAX_AGE, "")
            ).strip(),
            ENV_SNAPSHOT_MAX_AGE,
            DEFAULT_SNAPSHOT_MAX_AGE_SECONDS,
        ),
        interval_seconds=_positive_float(
            (
                interval_seconds
                if interval_seconds is not None
                else os.environ.get(ENV_INTERVAL, "")
            ).strip(),
            ENV_INTERVAL,
            DEFAULT_INTERVAL_SECONDS,
        ),
    )


# ---------------------------------------------------------------------------
# expected 侧推导（按执行重建视角；actual 侧全部来自快照）
# ---------------------------------------------------------------------------


def _pending_expired(pending: Dict[str, Any], now: float) -> bool:
    """挂起引用是否已到期失装（disarmed 判据的到期半边）。

    ``kind=event`` 看 ``deadline``、``kind=timer`` 看 ``due_at``；不可解析/缺失
    （老行 migrated 不编造）按**未到期**处理——重建意图优先于终局化，与 #320
    「不编造」的迁移口径一致。
    """
    kind = pending.get("kind")
    raw = pending.get("deadline") if kind == "event" else pending.get("due_at")
    epoch = carrier_audit._parse_epoch(raw)
    if epoch is None:
        return False
    return epoch < now


def expected_carriers(
    status: str, pending: Optional[Dict[str, Any]], *, now: float
) -> Tuple[Set[str], bool]:
    """从状态行重建期望载体集合 → (集合 ⊆ {message, subscription, timer}, 是否到期失装)。

    推导表见模块 docstring；快照 actual 侧不在此函数出现（纯状态行函数，
    可独立对拍）。
    """
    kind = pending.get("kind") if isinstance(pending, dict) else None
    if status == "queued":
        # start 派发期望（含占位行 ghost；kind=start 为 schema 预留，同形）。
        return {"message"}, False
    if status == "running":
        if kind == "event":
            return {"subscription"}, _pending_expired(pending, now)
        if kind == "timer":
            return {"timer"}, _pending_expired(pending, now)
        # None = 在途消息假设（mode 1：ack 后崩溃则消息缺席）；service/cancel
        # 意图同走队列消息。
        return {"message"}, False
    if status == "suspended":
        carrier = _PENDING_KIND_CARRIER.get(kind)
        if carrier is None:
            # 老行 orphan（effective_pending 推不出）：不可重建，不猜 expected。
            return set(), False
        return {carrier}, _pending_expired(pending, now)
    # terminal / 枚举外非终态：不猜 expected（终态任何在场由 extra_carrier 收）。
    return set(), False


def _reconcile_row(
    record: Dict[str, Any], pending: Optional[Dict[str, Any]], *, now: float
) -> Tuple[List[str], Optional[Dict[str, Any]]]:
    """单执行对差 → (差异 kind 列表, 意图记录 or None)。

    actual 侧只读 ``record``（快照 per-execution 段）；expected 侧来自
    :func:`expected_carriers`。
    """
    status = str(record.get("status") or "")
    carriers = {str(c) for c in record.get("carriers") or []}
    expected, expired = expected_carriers(status, pending, now=now)

    have: Set[str] = set()
    if "message" in expected and carriers & _MESSAGE_SATISFIED_BY:
        have.add("message")
    if "subscription" in expected and "subscription" in carriers:
        have.add("subscription")
    if "timer" in expected and "timer" in carriers:
        have.add("timer")
    lost = expected - have

    # 期望已满足的存放处被「消费」，剩余在场 = 期望之外的多余载体。
    consumed: Set[str] = set()
    if "message" in expected and carriers & _MESSAGE_SATISFIED_BY:
        consumed |= carriers & _MESSAGE_SATISFIED_BY
    for carrier in ("subscription", "timer"):
        if carrier in expected and carrier in carriers:
            consumed.add(carrier)
    extra_locations = carriers - consumed

    diffs = []
    if "message" in lost:
        diffs.append("lost_message")
    if "subscription" in lost:
        diffs.append("lost_subscription")
    if "timer" in lost:
        diffs.append("lost_timer")
    if extra_locations:
        diffs.append("extra_carrier")

    intent: Optional[Dict[str, Any]] = None

    def _intent(name: str, note: str) -> Dict[str, Any]:
        action_class = {
            "start": "enqueue",
            "continue": "enqueue",
            "redispatch_service": "enqueue",
            "rebuild_subscription": "enqueue",
            "rebuild_timer": "enqueue",
            "finalize_pending_lost": "control",
            "gc": "control",
            "custody_replay": "observe",
            "inspect": "observe",
        }[name]
        return {
            "execution_id": record.get("execution_id"),
            "tenant": record.get("tenant"),
            "status": status,
            "pending_kind": pending.get("kind") if isinstance(pending, dict) else None,
            "diffs": diffs,
            "intent": name,
            "action_class": action_class,
            "note": note,
        }

    if lost:
        if "message" in lost:
            if status == "queued":
                intent = _intent("start", "queued 无载体：若接管将补 start 派发（阶段 2）")
            elif status == "running":
                intent = _intent("continue", "running 在途消息缺席：若接管将补 continue（阶段 2）")
            elif status == "suspended" and isinstance(pending, dict) and pending.get("kind") == "service":
                intent = _intent(
                    "redispatch_service", "service 派发凭据在而消息缺席：若接管将补偿派发（阶段 2）"
                )
            else:
                intent = _intent("inspect", "丢消息但形态不在阶段 2 动作面（预留 kind/枚举外状态）")
        elif "subscription" in lost:
            if expired:
                intent = _intent(
                    "finalize_pending_lost", "event 等待已过 deadline 且订阅缺席：阶段 3 error(pending_lost)"
                )
            else:
                intent = _intent("rebuild_subscription", "event 等待在而订阅缺席：若接管将重建订阅（阶段 2）")
        elif "timer" in lost:
            if expired:
                intent = _intent(
                    "finalize_pending_lost", "timer 等待已过 due_at 且定时器缺席：阶段 3 error(pending_lost)"
                )
            else:
                intent = _intent("rebuild_timer", "timer 等待在而定时器缺席：若接管将重建定时器（阶段 2）")
    elif status in TERMINAL_EXECUTION_STATUSES and extra_locations:
        # 终态行先于 custody：DLQ 里挂着已完成执行的消息，重放=双执行，只能清。
        intent = _intent("gc", f"终态执行仍挂载体 {sorted(extra_locations)}：阶段 3 清理")
    elif "dlq" in carriers:
        intent = _intent(
            "custody_replay", "载体在 DLQ/隔离流在押：恢复走重放通道，reconciler 不碰（A2 规范性义务）"
        )
    elif extra_locations:
        intent = _intent("inspect", f"期望之外载体在场 {sorted(extra_locations)}：人工/阶段 3 定夺")
    return diffs, intent


# ---------------------------------------------------------------------------
# 双向互证（shadow 按执行聚合重放快照聚合面）
# ---------------------------------------------------------------------------


def mutual_verify(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """per-execution 段逐格重放快照 cells / missing / orphan_terminal。

    三项全真 = 「按执行重建」与「按存放处枚举」双视同源自洽；任何失配
    进 ``mismatches``（截 20 条）——shadow 与 gauge 分歧从第一天可观测。
    """
    executions = snapshot.get("executions")
    cells_index = {
        (cell.get("tenant"), cell.get("status"), cell.get("carrier")): int(
            cell.get("count", 0)
        )
        for cell in snapshot.get("cells") or []
    }
    rollup: Dict[Tuple[Any, Any, Any], int] = {}
    missing = 0
    orphans = 0
    for record in executions or []:
        status = str(record.get("status") or "")
        tenant = record.get("tenant")
        if not status:
            key = (tenant, "unknown", "unknown")
            rollup[key] = rollup.get(key, 0) + 1
            continue
        if status in TERMINAL_EXECUTION_STATUSES:
            if record.get("carriers"):
                orphans += 1
            continue  # 终态行不入 cells（audit 同款）
        verdict = str(record.get("verdict") or "unknown")
        key = (tenant, status, verdict)
        rollup[key] = rollup.get(key, 0) + 1
        if verdict == "none":
            missing += 1
    mismatch_keys = sorted(set(cells_index) | set(rollup))
    mismatches = [
        f"cells[{k[0]}/{k[1]}/{k[2]}] snapshot={cells_index.get(k, 0)} shadow={rollup.get(k, 0)}"
        for k in mismatch_keys
        if cells_index.get(k, 0) != rollup.get(k, 0)
    ]
    violations = snapshot.get("violations") or {}
    return {
        "checked": True,
        "cells_reproduced": not mismatches,
        "missing_matched": missing == int(violations.get("missing", 0)),
        "orphan_terminal_matched": orphans == int(violations.get("orphan_terminal", 0)),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches[:20],
    }


# ---------------------------------------------------------------------------
# shadow 轮
# ---------------------------------------------------------------------------


@dataclass
class ShadowResult:
    """单轮 shadow 结果（gauge/report 的唯一事实源，收口统一 flush）。"""

    generated_at: str
    duration_seconds: float
    snapshot_path: str
    snapshot_age_seconds: Optional[float]
    sources_failed: List[str] = field(default_factory=list)
    candidates: Dict[Tuple[str, str], int] = field(default_factory=dict)
    diffs: Dict[str, int] = field(default_factory=dict)
    intents: List[Dict[str, Any]] = field(default_factory=list)
    intents_total: int = 0
    intents_truncated: bool = False
    unverifiable_intents: int = 0
    intent_counts: Dict[Tuple[str, str], int] = field(default_factory=dict)
    mutual: Dict[str, Any] = field(default_factory=dict)
    check_errors: int = 0
    executions_seen: int = 0


def shadow_round(
    snapshot: Dict[str, Any],
    client: Any,
    config: ShadowConfig,
    *,
    now: Optional[float] = None,
) -> ShadowResult:
    """一轮 shadow 对账：快照候选集 × 状态行 expected 推导 → 差异 + 意图。

    全程只读：Redis 面 = 复用 :class:`carrier_audit.ReadOnlyRedis` 白名单的
    状态行 GET；扫描态全在内存；落盘由调用方收口（:func:`flush`）。
    """
    started = time.monotonic()
    now_ts = now if now is not None else time.time()
    counter = [0]
    failed: List[str] = []

    # ---- 快照源健康（schema × 新鲜度）----
    raw_executions = snapshot.get("executions")
    schema_ok = isinstance(raw_executions, list)
    executions: List[Dict[str, Any]] = raw_executions if schema_ok else []
    if not schema_ok:
        failed.append("snapshot_schema")
    snapshot_age: Optional[float] = None
    epoch = carrier_audit._parse_epoch(snapshot.get("generated_at"))
    if epoch is None:
        failed.append("snapshot_generated_at")
    else:
        snapshot_age = max(0.0, now_ts - epoch)
        if snapshot_age > config.snapshot_max_age_seconds:
            failed.append("snapshot_stale")
    healthy = not failed

    result = ShadowResult(
        generated_at=datetime.fromtimestamp(now_ts).astimezone().isoformat(),
        duration_seconds=0.0,
        snapshot_path=str(config.snapshot_path),
        snapshot_age_seconds=snapshot_age,
        sources_failed=failed,
        mutual=(
            mutual_verify(snapshot)
            if schema_ok
            else {"checked": False, "reason": "snapshot_schema"}
        ),
    )

    for record in executions:
        eid = record.get("execution_id")
        tenant = record.get("tenant")
        status = str(record.get("status") or "")
        if not eid or not tenant:
            counter[0] += 1
            logger.warning(
                "reconciler_shadow 快照记录缺 execution_id/tenant，跳过: %r",
                {k: record.get(k) for k in ("execution_id", "tenant")},
            )
            continue
        result.executions_seen += 1
        cell = (str(tenant), status or "unknown")
        result.candidates[cell] = result.candidates.get(cell, 0) + 1

        # ---- expected 侧：状态行读取（只 GET）+ #320 读面归一 ----
        ns = tenant_namespace(str(tenant))
        try:
            raw = client.get(
                carrier_audit.STATE_KEY_FMT.format(ns=ns, execution_id=str(eid))
            )
        except Exception as exc:  # noqa: BLE001 — 单条隔离：log+计数
            counter[0] += 1
            logger.warning(
                "reconciler_shadow 状态行读取失败 %s/%s: %s",
                ns,
                eid,
                _scrub_exception_text(exc),
            )
            continue
        if not raw:
            counter[0] += 1
            logger.warning(
                "reconciler_shadow 状态行缺失（扫描间隙消失）%s/%s", ns, eid
            )
            continue
        try:
            state = ExecutionState.model_validate(json.loads(raw))
            pending = state.effective_pending()
        except Exception as exc:  # noqa: BLE001 — 单条隔离：log+计数
            counter[0] += 1
            logger.warning(
                "reconciler_shadow 状态行不可归一 %s/%s: %s",
                ns,
                eid,
                _scrub_exception_text(exc),
            )
            continue

        diffs, intent = _reconcile_row(record, pending, now=now_ts)
        if intent is None:
            continue
        intent["verifiable"] = healthy
        result.intents_total += 1
        if not healthy:
            result.unverifiable_intents += 1
            continue
        result.intents.append(intent)
        result.intent_counts[(intent["intent"], intent["action_class"])] = (
            result.intent_counts.get((intent["intent"], intent["action_class"]), 0) + 1
        )
        for kind in diffs:
            result.diffs[kind] = result.diffs.get(kind, 0) + 1

    if len(result.intents) > INTENT_SAMPLES_CAP:
        result.intents_truncated = True
        result.intents = result.intents[:INTENT_SAMPLES_CAP]
    result.check_errors = counter[0]
    result.duration_seconds = time.monotonic() - started
    return result


# ---------------------------------------------------------------------------
# 渲染与落盘（与 #316 同形制：全格显式 0/N、无 _total、收口统一 flush）
# ---------------------------------------------------------------------------


def render_prom(result: ShadowResult) -> str:
    """ShadowResult → Prometheus text exposition（gauge 族）。"""
    def _header(name: str, help_text: str) -> None:
        lines.append(f"# HELP {name} {carrier_audit._escape_label(help_text)}")
        lines.append(f"# TYPE {name} gauge")

    lines: List[str] = []
    _header(
        "plaita_reconciler_shadow_candidates",
        "对账候选集规模（#316 快照 per-execution 段：非终态全集 + 带载体终态行）。",
    )
    tenants = sorted({t for (t, _s) in result.candidates})
    observed = sorted({s for (_t, s) in result.candidates})
    statuses = tuple(carrier_audit.NON_TERMINAL_STATUSES) + tuple(
        sorted(set(observed) - set(carrier_audit.NON_TERMINAL_STATUSES))
    )
    for tenant in tenants:
        for status in statuses:
            count = result.candidates.get((tenant, status), 0)
            lines.append(
                "plaita_reconciler_shadow_candidates"
                f'{{status="{carrier_audit._escape_label(status)}",'
                f'tenant="{carrier_audit._escape_label(tenant)}"}} {count}'
            )
    _header(
        "plaita_reconciler_shadow_diffs",
        "expected×actual 差异计数（gauge，只计 verifiable 差异；degraded 轮次全 0）。"
        "lost_message/lost_subscription/lost_timer=期望载体缺席；extra_carrier=期望之外载体在场。",
    )
    for kind in DIFF_KINDS:
        lines.append(
            f'plaita_reconciler_shadow_diffs{{kind="{carrier_audit._escape_label(kind)}"}} '
            f"{result.diffs.get(kind, 0)}"
        )
    _header(
        "plaita_reconciler_shadow_intents",
        "接管动作意图计数（gauge）。enqueue=不写状态行的物化动作（阶段 2）；"
        "control=fenced CAS 状态行写（阶段 3）；observe=信息面。shadow 本体零控制动作。",
    )
    for intent in INTENT_KINDS:
        for action_class in ACTION_CLASSES:
            count = result.intent_counts.get((intent, action_class), 0)
            lines.append(
                "plaita_reconciler_shadow_intents"
                f'{{intent="{carrier_audit._escape_label(intent)}",'
                f'action_class="{carrier_audit._escape_label(action_class)}"}} {count}'
            )
    _header(
        "plaita_reconciler_shadow_last_success_timestamp",
        "最近一次 shadow 轮收口的 unix 时间戳（停推 = shadow 挂掉/配置错，meta-alert 判据一）。",
    )
    lines.append(
        f"plaita_reconciler_shadow_last_success_timestamp "
        f"{datetime.fromisoformat(result.generated_at).timestamp():.3f}"
    )
    _header(
        "plaita_reconciler_shadow_duration_seconds", "最近一轮 shadow 对账耗时（秒）。"
    )
    lines.append(f"plaita_reconciler_shadow_duration_seconds {result.duration_seconds:.6f}")
    _header(
        "plaita_reconciler_shadow_check_errors",
        "最近一轮单条级错误数（状态行缺失/不可读/不可归一，对应执行不出意图）。",
    )
    lines.append(f"plaita_reconciler_shadow_check_errors {result.check_errors}")
    _header(
        "plaita_reconciler_shadow_sources_failed",
        "最近一轮快照源失败数（缺失/低版/过期；>0 时 loss 类差异不进 gauge——缺席不可判）。",
    )
    lines.append(f"plaita_reconciler_shadow_sources_failed {len(result.sources_failed)}")
    return "\n".join(lines) + "\n"


def render_report(result: ShadowResult, config: ShadowConfig) -> Dict[str, Any]:
    """ShadowResult → shadow 报告 JSON 形状（含互证段与意图样本）。"""
    return {
        "schema": 1,
        "generated_at": result.generated_at,
        "duration_seconds": round(result.duration_seconds, 6),
        "snapshot": {
            "path": result.snapshot_path,
            "age_seconds": (
                round(result.snapshot_age_seconds, 3)
                if result.snapshot_age_seconds is not None
                else None
            ),
            "max_age_seconds": config.snapshot_max_age_seconds,
            "sources_failed": result.sources_failed,
        },
        "executions_seen": result.executions_seen,
        "candidates": [
            {"tenant": tenant, "status": status, "count": count}
            for (tenant, status), count in sorted(result.candidates.items())
        ],
        "diffs": {kind: result.diffs.get(kind, 0) for kind in DIFF_KINDS},
        "intent_counts": [
            {"intent": intent, "action_class": action_class, "count": count}
            for (intent, action_class), count in sorted(result.intent_counts.items())
        ],
        "intents_total": result.intents_total,
        "intents_truncated": result.intents_truncated,
        "unverifiable_intents": result.unverifiable_intents,
        "intents": result.intents,
        "mutual_verification": result.mutual,
        "check_errors": result.check_errors,
    }


def flush(result: ShadowResult, config: ShadowConfig) -> None:
    """单轮收口统一落盘：shadow 报告 + prom textfile，tmp+rename 原子写。"""
    carrier_audit._atomic_write(
        config.output_dir / "reconciler_shadow_report.json",
        json.dumps(render_report(result, config), ensure_ascii=False, indent=2) + "\n",
    )
    carrier_audit._atomic_write(
        config.output_dir / "reconciler_shadow_metrics.prom", render_prom(result)
    )


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------


def build_client(redis_url: str) -> Any:
    """Redis URL → 只读包装客户端（复用 #316 的白名单与连接口径）。"""
    return carrier_audit.build_client(redis_url)


def load_snapshot(path: Path) -> Dict[str, Any]:
    """读 #316 快照（只读文件面；失败按轮级失败上抛，不覆写上一轮产物）。"""
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def run_round(config: ShadowConfig) -> ShadowResult:
    """一轮 shadow：读快照 → 对账 → 收口落盘。落盘失败按轮级失败上抛。"""
    snapshot = load_snapshot(config.snapshot_path)
    result = shadow_round(snapshot, build_client(config.redis_url), config)
    flush(result, config)
    logger.info(
        "reconciler_shadow 轮完成: %.3fs candidates=%d diffs=%s sources_failed=%s "
        "check_errors=%d mutual=%s",
        result.duration_seconds,
        result.executions_seen,
        {k: v for k, v in result.diffs.items() if v},
        result.sources_failed,
        result.check_errors,
        result.mutual.get("cells_reproduced"),
    )
    return result


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="执行载体对账器 shadow（plaita#321 A2 阶段 1，只读零控制动作）"
    )
    parser.add_argument("--once", action="store_true", help="单轮后退出（缺省；systemd timer 用）")
    parser.add_argument("--loop", action="store_true", help="常驻循环（间隔 PLAITA_RECONCILER_SHADOW_INTERVAL_SECONDS，缺省 10s）")
    parser.add_argument("--redis-url", default=None, help=f"覆盖 env {ENV_REDIS_URL}")
    parser.add_argument("--snapshot", default=None, help=f"覆盖 env {ENV_SNAPSHOT}（#316 快照路径）")
    parser.add_argument("--output-dir", default=None, help=f"覆盖 env {ENV_OUTPUT_DIR}")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        config = load_config(
            redis_url=args.redis_url,
            snapshot=args.snapshot,
            output_dir=args.output_dir,
        )
    except ShadowConfigError as exc:
        logger.error("%s", exc)
        return 2

    if args.loop:
        while True:
            try:
                run_round(config)
            except Exception as exc:  # noqa: BLE001 — 轮级失败：log 后续轮重试（不覆写产物）
                logger.error(
                    "reconciler_shadow 轮失败（保留上一轮产物）: %s",
                    _scrub_exception_text(exc),
                )
            time.sleep(config.interval_seconds)
        return 0  # pragma: no cover
    try:
        run_round(config)
    except Exception as exc:  # noqa: BLE001 — 单轮模式失败以非零码 fail-visible
        logger.error("reconciler_shadow 轮失败: %s", _scrub_exception_text(exc))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
