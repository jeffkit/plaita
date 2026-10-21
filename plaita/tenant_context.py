"""多租户上下文（顶层模块，无仓内依赖）。

历史沿革：本模块的 ContextVar 与 namespace 纯函数原本定义在
``plaita.server.tenant_context``。但租户标识需要在**挂起时**就写进事件
订阅（P3 修复，2026-10-02 评审遗留 #6），而挂起点在 ``core/strategies``
的 ``_subscribe_event``——core 禁止 import server（import 分层检查，
tests/e2e/test_import_layering.py）。因此把**纯部分**（ContextVar +
tenant/current/set/reset/namespace 函数，仅依赖标准库）下沉到本顶层
模块；``plaita.server.tenant_context`` 逐名 re-export 保持全仓既有
``from plaita.server.tenant_context import ...`` 调用点零改动，并把
依赖 storage 层的租户路由包装器留在 server 侧。

键空间约定（与 console 侧 engine_sync 共用同一映射规则）：
- 租户数据键跟随租户 namespace：``{ns}:execution:{id}``、
  ``{ns}:flow:{id}:{ver}``、``{ns}:flow_list``、``{ns}:flow_versions:{id}``、
  ``{ns}:execution:lease:{id}``；``tenant_namespace`` 把 default/空租户
  映射回历史前缀 ``plaita``（存量数据与旧版本 worker 兼容），其余租户为
  ``plaita:{tenant_id}``。
- 平台机制键（任务队列、registry、control、event_filter 去重、调度锁）不分区。

租户上下文用 ContextVar 承载：FlowWorker 每处理一条任务消息前 set、处理后
reset（``_dispatch_task``）；租户路由存储包装器据此选择（并按租户缓存）
底层 RedisStorage 实例。挂起侧（core/strategies._subscribe_event）据此把
租户写进事件订阅，超时恢复路径（event_filter._on_subscription_timeout）
再从订阅取回。
"""
from __future__ import annotations

import logging
import threading
from contextvars import ContextVar, Token
from typing import Dict, NamedTuple, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_TENANT_ID = "default"
LEGACY_NAMESPACE = "plaita"

# plaita#239（先行，只观测）：租户上下文来源三态。
# - explicit：当前上下文显式 `set_current_tenant(...)` 设定；
# - derived：经执行边界（线程/进程/asyncio 桥）从上一上下文派生；
# - default：从未设置（`current_tenant()` 回退 default）。
TENANT_SOURCE_EXPLICIT = "explicit"
TENANT_SOURCE_DERIVED = "derived"
TENANT_SOURCE_DEFAULT = "default"

# plaita#277：租户状态/世代平台键（console 写、worker/event 读）。定义在
# foundation，event 层可直接引用常量做写闸，不必向上 import server。
TENANT_STATUS_KEY = "plaita:tenant_status"
TENANT_STATUS_META_KEY = "plaita:tenant_status:meta"
TENANT_STATUS_DISABLED = "disabled"

_tenant_ctx: ContextVar[str] = ContextVar("plaita_tenant", default=DEFAULT_TENANT_ID)
_tenant_source_ctx: ContextVar[str] = ContextVar(
    "plaita_tenant_source", default=TENANT_SOURCE_DEFAULT)


class TenantToken(NamedTuple):
    """`set_current_tenant` 的复合 token：同时复位租户与来源标记。"""

    tenant: Token
    source: Token


def tenant_namespace(tenant_id: Optional[str]) -> str:
    """租户 → Redis 键 namespace。default/空 = 历史前缀 plaita（兼容）。"""
    if not tenant_id or tenant_id == DEFAULT_TENANT_ID:
        return LEGACY_NAMESPACE
    return f"{LEGACY_NAMESPACE}:{tenant_id}"


def current_tenant() -> str:
    return _tenant_ctx.get()


def current_tenant_source() -> str:
    """当前上下文的租户来源：explicit / derived / default（plaita#239）。"""
    return _tenant_source_ctx.get()


def set_current_tenant(
    tenant_id: Optional[str], *, source: str = TENANT_SOURCE_EXPLICIT,
) -> TenantToken:
    """设置当前租户与来源；返回复合 token（`reset_current_tenant` 可复位）。"""
    if source not in (
        TENANT_SOURCE_EXPLICIT, TENANT_SOURCE_DERIVED, TENANT_SOURCE_DEFAULT,
    ):
        raise ValueError(f"unknown tenant source: {source!r}")
    return TenantToken(
        tenant=_tenant_ctx.set(tenant_id or DEFAULT_TENANT_ID),
        source=_tenant_source_ctx.set(source),
    )


def reset_current_tenant(token) -> None:
    """复位 `set_current_tenant` 的复合 token；兼容历史的裸 Token。"""
    if isinstance(token, TenantToken):
        _tenant_ctx.reset(token.tenant)
        _tenant_source_ctx.reset(token.source)
    else:
        _tenant_ctx.reset(token)


def mark_tenant_derived() -> None:
    """执行边界 worker 内调用：把 explicit 来源降级为 derived。

    default（从未设置）保持 default——边界的产物仍属 default 来源，写探针
    才会把它统计为 default-origin。derived 幂等。
    """
    if _tenant_source_ctx.get() != TENANT_SOURCE_DEFAULT:
        _tenant_source_ctx.set(TENANT_SOURCE_DERIVED)


# ---------------------------------------------------------------------------
# 租户世代上下文（plaita#277）：DLQ/订阅等未受 execution fencing 保护的写面
# 需要知道「这条在途执行属于哪个租户世代」。worker 派发时把当前世代写入本
# ContextVar；引擎挂起创建订阅时随订阅落库，写面据此拒绝旧世代迟到写。
# 纯 ContextVar，无仓内依赖。
# ---------------------------------------------------------------------------
_tenant_generation_ctx: ContextVar[Optional[str]] = ContextVar(
    "plaita_tenant_generation", default=None
)


class TenantWriteRejected(RuntimeError):
    """旧租户世代/已删租户的迟到写被拒绝（plaita#277，具名 fail-closed）。"""

    def __init__(self, tenant_id: Optional[str], reason: str):
        self.tenant_id = tenant_id
        self.reason = reason
        super().__init__(
            f"tenant write rejected: tenant={tenant_id!r} reason={reason}; "
            "the carrier belongs to a deleted/stale tenant generation"
        )


def current_tenant_generation() -> Optional[str]:
    return _tenant_generation_ctx.get()


def set_current_tenant_generation(generation: Optional[str]) -> Token:
    return _tenant_generation_ctx.set(generation)


def reset_current_tenant_generation(token: Token) -> None:
    _tenant_generation_ctx.reset(token)


# ---------------------------------------------------------------------------
# 写调用点审计探针 + fail-closed 守卫（plaita#239：先行观测 → owner 裁决强制）
# ---------------------------------------------------------------------------

_write_probe_lock = threading.Lock()
_write_probe_counts: Dict[Tuple[str, str], int] = {}


class TenantContextMissingError(RuntimeError):
    """写路径租户上下文缺失（plaita#239 fail-closed）。

    来源为 ``default``（进程/线程从未显式/派生设置租户）时拒绝写：
    「上下文丢失」不可再被静默当成有意 default——必须由服务入口显式
    ``set_current_tenant(DEFAULT_TENANT_ID, source="explicit")``，或经
    #237 的传播链以 ``derived`` 到达。``explicit`` / ``derived`` 照常放行。
    """

    def __init__(self, site: str, tenant_id: Optional[str] = None):
        self.site = site
        self.tenant_id = tenant_id
        super().__init__(
            f"tenant context missing for write site={site!r} "
            f"(tenant={tenant_id!r}, source={TENANT_SOURCE_DEFAULT!r}): "
            "set_current_tenant(DEFAULT_TENANT_ID, source='explicit') at the "
            "service entry, or propagate tenant context across the boundary"
        )


def record_write_probe(site: str) -> None:
    """在写调用点记录 (site, 来源) 计数；default 来源额外 DEBUG。

    观测原语：不抛错、不改变返回值（phase 2 的强制由
    :func:`guard_tenant_write` 执行）。
    """
    source = current_tenant_source()
    with _write_probe_lock:
        key = (site, source)
        _write_probe_counts[key] = _write_probe_counts.get(key, 0) + 1
    if source == TENANT_SOURCE_DEFAULT:
        logger.debug(
            "tenant write probe: site=%s tenant=%s source=default",
            site, current_tenant(),
        )


def guard_tenant_write(site: str) -> None:
    """写入口 fail-closed 守卫（plaita#239 phase 2，owner 裁决）。

    先记录分布（含被拒的 default 尝试），再在来源为 ``default`` 时抛具名
    :class:`TenantContextMissingError`；``explicit`` / ``derived`` 放行。
    """
    source = current_tenant_source()
    record_write_probe(site)
    if source == TENANT_SOURCE_DEFAULT:
        raise TenantContextMissingError(site, current_tenant())


def write_probe_snapshot() -> Dict[Tuple[str, str], int]:
    """当前进程的写探针计数快照（测试/观测用）。"""
    with _write_probe_lock:
        return dict(_write_probe_counts)


def reset_write_probe() -> None:
    """清空写探针计数（测试用）。"""
    with _write_probe_lock:
        _write_probe_counts.clear()
