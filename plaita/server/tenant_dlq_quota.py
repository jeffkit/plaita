"""per-tenant DLQ 配额计数（plaita#327）。

blast radius：队列/DLQ/event bus 是平台共享的（``tenant_context`` 平台机制
键不分区），平台 DLQ 单 Stream、全局 xtrim（``DEFAULT_DLQ_MAX_LEN``）——
单租户洪水可灌满全平台死信配额，把其他租户的死信证据挤掉。本模块给收容
写面一个**写路径计数**的 per-tenant 配额闸：

- **计数键**：平台机制键 HASH ``plaita:tenant_dlq_count``（tenant_id → 收容
  条数）。平台 DLQ 与租户隔离流（``{dlq}:quarantine:{tenant}``）**共用**该
  计数——隔离流按键隔离但 console 不可见（DLQ 读面只认 ``*:dlq``），配额若
  只管 ``*:dlq`` 它就是绕行暗面。
- **上限**：env ``PLAITA_TENANT_DLQ_MAX_ENTRIES``（缺省 200 = 平台 maxlen
  1000 的 20% 份额收敛）。``0`` = 不限（关闭配额回现状）；非法/负值 loud
  回默认（B2R31-03 不静默）。
- **满格行为**（owner 裁决 2026-10-24①）：拒新收容 + 留 pending + 具名
  reason ``dlq_quota_full`` + 告警。**不挤旧**——挤旧 = 静默销毁恢复载体，
  与 DLQ=证据哲学冲突；满格就是「无着陆点」，与 #312「无着陆点不 ack」
  纪律同构（守卫对满格拒绝不终局化，留 pending 等水位回落）。
- **隔离流同受配额**（owner 裁决②）：两流共用本计数实现；告警带落点
  分流名（``platform_dlq`` / ``quarantine``），隔离流不可见于 console，
  其告警必须自带落点名。
- **两键成对**：``delete_tenant`` 的 DLQ purge 必须与本计数清零成对
  （:func:`clear_tenant_dlq_count`）——计数只按 tenant_id，purge 后同 slug
  重建的新世代会继承幻影计数，新租户开局即配额吃紧，爆炸半径保护反噬受害
  租户（A2R58-05 status+世代两键成对的配额版）。
- **计数分工**（owner 裁决③）：闸门 = 写路径计数（HINCRBY 原子）；观测 =
  周期扫描对账纠偏（:func:`reconcile_tenant_dlq_counts`，复用 #316
  ``carrier_audit`` 扫描器骨架——XRANGE 解析 payload tenant_id，不新增
  进程）。xtrim 挤旧不通知计数，计数只朝虚高漂（闸门越收越紧，不朝放任
  方向）；纠偏把计数拉回真实水位，满格留 pending 的载体据此「等水位回落」
  ——纠偏不是纯观测，是配额语义的收敛半边。纠偏 HSET 与并发 HINCRBY 有
  竞态（扫描后新进的条目可能被回退 1），下一轮自愈，方向有界。
- **trim 互挤 = 明示可接受**（owner 裁决④）：DLQ 经 A125 修后为证据
  quarantine 性质，被挤条目仍在隔离流可查，跨租户挤最旧的残留显式接受；
  租户感知 trim（trim 前按 tenant 分桶挤）留作 T0 后续，不在本单。兜底 =
  总量告警线：平台 DLQ 长度超过 maxlen 20%（:data:`DLQ_TOTAL_ALERT_RATIO`）
  即具名 WARNING（:func:`reconcile_tenant_dlq_counts` 周期扫描顺带面），
  让运维在 collective fill 挤旧发生之前看见水位。
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

from plaita.core.url_redaction import _redact_env_value, _scrub_exception_text

logger = logging.getLogger(__name__)

#: 平台机制键 HASH（tenant_id → 收容条数），与 plaita:tenant_status 同款。
TENANT_DLQ_COUNT_KEY = "plaita:tenant_dlq_count"
#: per-tenant 收容上限 env（0 = 不限/关闭）。
TENANT_DLQ_MAX_ENTRIES_ENV = "PLAITA_TENANT_DLQ_MAX_ENTRIES"
#: 缺省上限 = 平台 DLQ maxlen 1000 的 20% 份额收敛。
DEFAULT_TENANT_DLQ_MAX_ENTRIES = 200
#: 具名满格拒绝原因（确认态——计数与上限都是读到了的事实，非读失败）。
DLQ_QUOTA_FULL = "dlq_quota_full"
#: 配额周期扫描对账的间隔 env（秒；<=0 = 关闭扫描纠偏）。
TENANT_DLQ_RECONCILE_INTERVAL_ENV = "PLAITA_TENANT_DLQ_RECONCILE_INTERVAL_SECONDS"
#: 扫描纠偏缺省间隔（秒）：10 分钟一轮，O(depth) 有界（平台 DLQ maxlen 内）。
DEFAULT_TENANT_DLQ_RECONCILE_INTERVAL_SECONDS = 600.0
#: 总量告警线（owner 裁决④「20% 阈值」）：平台 DLQ 长度超过 maxlen 的该
#: 比例即具名 WARNING——collective fill 挤旧发生之前的早警兜底。
DLQ_TOTAL_ALERT_RATIO = 0.2
#: 隔离流 SCAN 前缀形制（与 task_queue 写面 ``{dlq_key}:quarantine:{tenant}``
#: 同源；租户段取 ``:quarantine:`` 之后的整段）。
QUARANTINE_SCAN_FMT = "{dlq_key}:quarantine:*"


class TenantDlqQuotaFull(RuntimeError):
    """per-tenant DLQ 配额已满，拒新收容（plaita#327，具名 fail-closed）。

    调用方（dead_letter_if_allowed）接住后**留 pending**：不 ack、不写、
    不终局化——满格是「无着陆点」，载体等水位回落（xtrim 挤旧/清理/扫描
    纠偏）后重投重走闸。刻意不是 ValueError 子类（防 poison ack 误伤）。
    """

    def __init__(self, tenant_id: Optional[str], current: int, cap: int):
        self.tenant_id = tenant_id
        self.current = current
        self.cap = cap
        super().__init__(
            f"tenant dlq quota full: tenant={tenant_id!r} "
            f"entries={current}/{cap} ({DLQ_QUOTA_FULL}); "
            "rejection stays pending until the watermark recedes"
        )


def tenant_dlq_max_entries() -> Optional[int]:
    """解析 per-tenant DLQ 收容上限；None = 不限（env=0 显式关闭）。

    缺省 200（份额收敛占位，owner 三选一后改 env 即切）；非法/负值 loud
    回默认——绝不静默。
    """
    raw = os.environ.get(TENANT_DLQ_MAX_ENTRIES_ENV, "")
    if not raw:
        return DEFAULT_TENANT_DLQ_MAX_ENTRIES
    try:
        value = int(raw.strip())
    except (TypeError, ValueError):
        logger.warning(
            "%s=%s 非法，回退默认 %d（B2R31-03：不静默）",
            TENANT_DLQ_MAX_ENTRIES_ENV,
            _redact_env_value(raw),
            DEFAULT_TENANT_DLQ_MAX_ENTRIES,
        )
        return DEFAULT_TENANT_DLQ_MAX_ENTRIES
    if value < 0:
        logger.warning(
            "%s=%s 负值非法，回退默认 %d（B2R31-03：不静默）",
            TENANT_DLQ_MAX_ENTRIES_ENV,
            _redact_env_value(raw),
            DEFAULT_TENANT_DLQ_MAX_ENTRIES,
        )
        return DEFAULT_TENANT_DLQ_MAX_ENTRIES
    if value == 0:
        return None
    return value


def tenant_dlq_count(redis_client, tenant_id: Optional[str]) -> int:
    """读租户当前收容计数（观测用；故障/无客户端 = 0，绝不抛）。"""
    if redis_client is None or not tenant_id:
        return 0
    try:
        raw = redis_client.hget(TENANT_DLQ_COUNT_KEY, tenant_id)
    except Exception as exc:  # noqa: BLE001 — 观测口径不抛
        logger.warning(
            "读取租户 %s DLQ 计数失败: %s", tenant_id, _scrub_exception_text(exc)
        )
        return 0
    if raw is None:
        return 0
    try:
        return int(raw.decode() if isinstance(raw, bytes) else raw)
    except (TypeError, ValueError):
        return 0


def tenant_dlq_quota_allows(
    redis_client, tenant_id: Optional[str], cap: int
) -> tuple[bool, int]:
    """配额闸：租户收容计数是否仍低于上限。

    返回 ``(allowed, current)``。读失败按「计数不可知」**放行**——配额闸
    是 blast-radius 保护，闸门自身故障不得放大成全局死信停摆（与
    tenant_quota fail-open 同哲学；漂移由扫描纠偏，不靠单次读）。
    """
    if redis_client is None or not tenant_id:
        return True, 0
    try:
        raw = redis_client.hget(TENANT_DLQ_COUNT_KEY, tenant_id)
    except Exception as exc:  # noqa: BLE001 — 闸门故障不放大成全局拒绝
        logger.warning(
            "租户 %s DLQ 配额闸读取失败（fail-open 放行）: %s",
            tenant_id,
            _scrub_exception_text(exc),
        )
        return True, -1
    current = 0
    if raw is not None:
        try:
            current = int(raw.decode() if isinstance(raw, bytes) else raw)
        except (TypeError, ValueError):
            current = 0
    return current < cap, current


def bump_tenant_dlq_count(
    redis_client,
    tenant_id: Optional[str],
    cap: Optional[int],
    *,
    surface: str,
) -> int:
    """收容写成功后计数 +1（HINCRBY）；过半程阈值告警一次。

    ``surface`` 是落点分流名（``platform_dlq`` / ``quarantine``），进告警
    文案与阈值判定——两流同配额但告警可辨（隔离流不可见于 console，是它
    的告警必须自带落点名的原因）。计数失败只 WARNING 不上抛：条目已落盘，
    计数是辅助记账。
    """
    if redis_client is None or not tenant_id:
        return 0
    try:
        new_count = int(redis_client.hincrby(TENANT_DLQ_COUNT_KEY, tenant_id, 1))
    except Exception as exc:  # noqa: BLE001 — 条目已落盘，计数尽力而为
        logger.warning(
            "租户 %s DLQ 计数递增失败（%s 落点已落盘，计数虚低待扫描纠偏）: %s",
            tenant_id,
            surface,
            _scrub_exception_text(exc),
        )
        return -1
    if cap is not None:
        threshold = max(1, cap // 2)
        if new_count == threshold:
            logger.warning(
                "event=tenant_dlq.quota_half tenant=%s surface=%s "
                "entries=%d/%d（过半程）——收容增速异常，请核查该租户",
                tenant_id,
                surface,
                new_count,
                cap,
            )
    return new_count


def clear_tenant_dlq_count(redis_client, tenant_id: Optional[str]) -> bool:
    """清掉租户的 DLQ 配额计数（delete_tenant 的 DLQ purge 配对面）。

    返回是否确实清掉了非零计数（观测/测试用）。失败只 WARNING 不上抛——
    幻影计数的最坏后果是新世代配额吃紧（保守方向），不应阻断删除主流程；
    与「清理失败必须 loud 上抛」的 DLQ 条目清理不同，这里是记账不是证据。
    """
    if redis_client is None or not tenant_id:
        return False
    try:
        removed = int(redis_client.hdel(TENANT_DLQ_COUNT_KEY, tenant_id) or 0)
    except Exception as exc:  # noqa: BLE001 — 记账失败不阻断删除
        logger.warning(
            "清除租户 %s DLQ 配额计数失败（同名重建可能继承幻影计数）: %s",
            tenant_id,
            _scrub_exception_text(exc),
        )
        return False
    return removed > 0


def tenant_dlq_reconcile_interval_seconds() -> float:
    """扫描纠偏间隔（秒）；``<=0`` = 关闭周期纠偏。非法值 loud 回默认。

    与 :func:`tenant_dlq_max_entries` 同款纪律：绝不静默吞错值。
    """
    raw = os.environ.get(TENANT_DLQ_RECONCILE_INTERVAL_ENV, "")
    if not raw:
        return DEFAULT_TENANT_DLQ_RECONCILE_INTERVAL_SECONDS
    try:
        value = float(raw.strip())
    except (TypeError, ValueError):
        logger.warning(
            "%s=%s 非法，回退默认 %g（B2R31-03：不静默）",
            TENANT_DLQ_RECONCILE_INTERVAL_ENV,
            _redact_env_value(raw),
            DEFAULT_TENANT_DLQ_RECONCILE_INTERVAL_SECONDS,
        )
        return DEFAULT_TENANT_DLQ_RECONCILE_INTERVAL_SECONDS
    return value


def _count_platform_dlq_by_tenant(client, dlq_key: str) -> tuple:
    """XRANGE 平台 DLQ，按信封 payload 的 ``tenant_id`` 分桶计数。

    解析形制与 #316 ``carrier_audit._scan_dlq`` 同源（信封 → ``payload`` →
    ``tenant_id``）；解析失败条目跳过（不进任何桶）。返回
    ``(per_tenant_dict, total, parse_failures)``。
    """
    per_tenant: dict = {}
    total = 0
    failures = 0
    for _msg_id, fields in client.xrange(dlq_key, min="-", max="+"):
        total += 1
        raw = None
        if isinstance(fields, dict):
            raw = fields.get("payload", fields.get(b"payload"))
        if raw is None:
            failures += 1
            continue
        try:
            envelope = json.loads(raw)
        except (TypeError, ValueError):
            failures += 1
            continue
        body = envelope.get("payload") if isinstance(envelope, dict) else None
        tenant_id = body.get("tenant_id") if isinstance(body, dict) else None
        if not tenant_id:
            continue
        key = str(tenant_id)
        per_tenant[key] = per_tenant.get(key, 0) + 1
    return per_tenant, total, failures


def _quarantine_counts(client, dlq_key: str) -> tuple:
    """SCAN 隔离流（``{dlq_key}:quarantine:*``），按流长计数。

    隔离流每租户一条流（写面 ``xadd`` 后 xtrim），``XLEN`` 即该租户隔离流
    收容数——不需要解析 payload。返回 ``(per_tenant_dict, stream_count)``。
    """
    pattern = QUARANTINE_SCAN_FMT.format(dlq_key=dlq_key)
    prefix = pattern[:-1]  # 去掉尾部 '*' 得 ``...:quarantine:``
    per_tenant: dict = {}
    stream_count = 0
    scan = getattr(client, "scan_iter", None)
    keys = (
        scan(match=pattern)
        if callable(scan)
        else client.keys(pattern)
    )
    for key in keys:
        key = key.decode() if isinstance(key, bytes) else key
        if not key.startswith(prefix):
            continue
        tenant_id = key[len(prefix):]
        if not tenant_id:
            continue
        stream_count += 1
        try:
            per_tenant[tenant_id] = int(client.xlen(key) or 0)
        except Exception as exc:  # noqa: BLE001 — 单流失败隔离，不影响其余
            logger.warning(
                "隔离流 %s 长度读取失败（该流本轮不纠偏）: %s",
                key, _scrub_exception_text(exc),
            )
    return per_tenant, stream_count


def reconcile_tenant_dlq_counts(
    client,
    dlq_key: str,
    *,
    dlq_max_len: Optional[int] = None,
    cap: Optional[int] = None,
) -> dict:
    """周期扫描对账：把写路径计数拉回真实水位（plaita#327 owner 裁决③）。

    闸门用写路径计数（HINCRBY 原子），观测面用扫描对账纠偏——xtrim 挤旧
    不知道归属，计数只朝虚高漂；虚高不纠，满格留 pending 的载体永无
    「水位回落」可等。本函数扫描平台 DLQ（解析 payload ``tenant_id``）与
    各租户隔离流（XLEN），按流分桶得出真实 per-tenant 收容数，与
    ``plaita:tenant_dlq_count`` HASH 差异处 HSET/HDEL 纠偏。

    - **告警**：任一租户两流合计 >50% 上限 → 具名 WARNING（带落点分流名，
      owner 裁决②）；平台 DLQ 总长 > ``dlq_max_len`` 的
      :data:`DLQ_TOTAL_ALERT_RATIO`（20%，owner 裁决④）→ 总量早警。
    - **竞态语义**：纠偏 HSET 与并发收容 HINCRBY 互踩时可能回退 1，下一轮
      自愈；多 worker 同时纠偏 last-write-wins，同方向有界。
    - **fail-open**：任何 Redis 故障降级为 ``degraded`` 报告（计数原样），
      绝不上抛——观测路径不得打断消费循环。

    返回报告 dict（观测/测试用）：``scanned`` / ``platform_total`` /
    ``parse_failures`` / ``quarantine_streams`` / ``per_tenant`` /
    ``corrections`` / ``alerts`` / ``total_watermark`` / ``degraded``。
    """
    report: dict = {
        "scanned": True,
        "platform_total": 0,
        "parse_failures": 0,
        "quarantine_streams": 0,
        "per_tenant": {},
        "corrections": [],
        "alerts": [],
        "total_watermark": False,
        "degraded": False,
    }
    if client is None or not dlq_key:
        return report
    try:
        platform_by_tenant, platform_total, parse_failures = (
            _count_platform_dlq_by_tenant(client, dlq_key)
        )
        quarantine_by_tenant, stream_count = _quarantine_counts(client, dlq_key)
    except Exception as exc:  # noqa: BLE001 — 观测路径 fail-open
        logger.warning(
            "DLQ 配额扫描对账失败（本轮降级，计数原样）: %s",
            _scrub_exception_text(exc),
        )
        report["degraded"] = True
        return report
    report["platform_total"] = platform_total
    report["parse_failures"] = parse_failures
    report["quarantine_streams"] = stream_count

    tenants = set(platform_by_tenant) | set(quarantine_by_tenant)
    for tenant_id in sorted(tenants):
        platform_n = platform_by_tenant.get(tenant_id, 0)
        quarantine_n = quarantine_by_tenant.get(tenant_id, 0)
        desired = platform_n + quarantine_n
        report["per_tenant"][tenant_id] = {
            "platform_dlq": platform_n,
            "quarantine": quarantine_n,
            "desired": desired,
        }
        # 纠偏：只在差异处写（最小写面）。
        current = tenant_dlq_count(client, tenant_id)
        if current != desired:
            try:
                if desired == 0:
                    client.hdel(TENANT_DLQ_COUNT_KEY, tenant_id)
                else:
                    client.hset(TENANT_DLQ_COUNT_KEY, tenant_id, desired)
                report["corrections"].append(
                    {"tenant": tenant_id, "from": current, "to": desired}
                )
                logger.info(
                    "tenant_dlq.reconcile tenant=%s 计数纠偏 %d → %d"
                    "（platform_dlq=%d quarantine=%d）",
                    tenant_id, current, desired, platform_n, quarantine_n,
                )
            except Exception as exc:  # noqa: BLE001 — 单租户纠偏失败不放大
                logger.warning(
                    "tenant=%s DLQ 计数纠偏写失败（下轮重试）: %s",
                    tenant_id, _scrub_exception_text(exc),
                )
        # 分流名告警（owner 裁决②）：两流合计过半 → 具名 WARNING。
        if cap is not None and cap > 0 and desired * 2 > cap:
            logger.warning(
                "event=tenant_dlq.quota_watermark tenant=%s surface="
                "platform_dlq:%d+quarantine:%d entries=%d/%d（>50%%）"
                "——收容增速异常，请核查该租户（plaita#327 扫描对账面）",
                tenant_id, platform_n, quarantine_n, desired, cap,
            )
            report["alerts"].append(
                {"tenant": tenant_id, "platform_dlq": platform_n,
                 "quarantine": quarantine_n, "cap": cap}
            )

    # 总量告警线（owner 裁决④）：trim 互挤明示可接受，但 collective fill
    # 挤旧之前必须可见——平台 DLQ 总长过 maxlen 20% 即早警。
    if dlq_max_len is not None and dlq_max_len > 0:
        watermark = DLQ_TOTAL_ALERT_RATIO * dlq_max_len
        if platform_total > watermark:
            logger.warning(
                "event=tenant_dlq.total_watermark platform_dlq=%d > %.0f%% "
                "of maxlen=%d——全平台死信水位过线；trim 互挤（跨租户挤最旧）"
                "为明示可接受残留（owner 2026-10-24 裁决），水位继续上行将"
                "开始挤掉旧证据，请核查租户配额或调大 PLAITA_DLQ_MAX_LEN",
                platform_total, DLQ_TOTAL_ALERT_RATIO * 100, dlq_max_len,
            )
            report["total_watermark"] = True
    return report
