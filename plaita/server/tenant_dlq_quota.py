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
  回默认（B2R31-03 不静默）。数值本身待 owner 三选一（份额收敛 / trim 租户
  感知 / 明示互挤残留可接受），切换 = 改 env，零代码成本。
- **满格行为**：拒新收容 + 留 pending + 具名 reason ``dlq_quota_full`` +
  告警。**不挤旧**——挤旧 = 静默销毁恢复载体，与 DLQ=证据哲学冲突；满格就
  是「无着陆点」，与 #312「无着陆点不 ack」纪律同构（守卫对满格拒绝不
  终局化，留 pending 等水位回落）。
- **两键成对**：``delete_tenant`` 的 DLQ purge 必须与本计数清零成对
  （:func:`clear_tenant_dlq_count`）——计数只按 tenant_id，purge 后同 slug
  重建的新世代会继承幻影计数，新租户开局即配额吃紧，爆炸半径保护反噬受害
  租户（A2R58-05 status+世代两键成对的配额版）。
- **漂移语义**：计数是写路径记账；全局 xtrim 挤掉最旧条目时不知道归属，
  计数只朝虚高方向漂（闸门越收越紧，不朝放任方向）。周期扫描对账（复用
  #316 扫描器骨架，后续单）纠偏；配额是爆炸半径闸门不是精确账本，env=0
  随时可关。
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from plaita.core.url_redaction import _redact_env_value, _scrub_exception_text

logger = logging.getLogger(__name__)

#: 平台机制键 HASH（tenant_id → 收容条数），与 plaita:tenant_status 同款。
TENANT_DLQ_COUNT_KEY = "plaita:tenant_dlq_count"
#: per-tenant 收容上限 env（0 = 不限/关闭）。
TENANT_DLQ_MAX_ENTRIES_ENV = "PLAITA_TENANT_DLQ_MAX_ENTRIES"
#: 缺省上限 = 平台 DLQ maxlen 1000 的 20% 份额（owner 三选一前的占位值）。
DEFAULT_TENANT_DLQ_MAX_ENTRIES = 200
#: 具名满格拒绝原因（确认态——计数与上限都是读到了的事实，非读失败）。
DLQ_QUOTA_FULL = "dlq_quota_full"


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
