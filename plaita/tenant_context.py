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
- 平台机制键（任务队列、registry、control、event_filter 去重、调度锁、
  停用租户集合）不分区。

租户上下文用 ContextVar 承载：FlowWorker 每处理一条任务消息前 set、处理后
reset（``_dispatch_task``）；租户路由存储包装器据此选择（并按租户缓存）
底层 RedisStorage 实例。挂起侧（core/strategies._subscribe_event）据此把
租户写进事件订阅，超时恢复路径（event_filter._on_subscription_timeout）
再从订阅取回。
"""
from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any, Optional

DEFAULT_TENANT_ID = "default"
LEGACY_NAMESPACE = "plaita"

# 停用租户集合（Redis SET，成员为租户 ID）。租户状态的权威库是 console 侧的
# tenants 表；runtime（FlowWorker / 调度服务）不在 console 进程内、无 SQLite
# 访问，故由 console 在状态变更时写本键，runtime 据此把闸（见
# plaita-console backend services/tenants_svc.set_tenant_status）。键缺席 =
# 无租户停用——与未接入该闸的存量部署行为一致。
DISABLED_TENANTS_KEY = "plaita:tenants:disabled"

_tenant_ctx: ContextVar[str] = ContextVar("plaita_tenant", default=DEFAULT_TENANT_ID)


def tenant_namespace(tenant_id: Optional[str]) -> str:
    """租户 → Redis 键 namespace。default/空 = 历史前缀 plaita（兼容）。"""
    if not tenant_id or tenant_id == DEFAULT_TENANT_ID:
        return LEGACY_NAMESPACE
    return f"{LEGACY_NAMESPACE}:{tenant_id}"


def current_tenant() -> str:
    return _tenant_ctx.get()


def set_current_tenant(tenant_id: Optional[str]) -> Token:
    return _tenant_ctx.set(tenant_id or DEFAULT_TENANT_ID)


def reset_current_tenant(token: Token) -> None:
    _tenant_ctx.reset(token)


def is_tenant_disabled(redis_client: Any, tenant_id: Optional[str]) -> bool:
    """租户是否已停用（DISABLED_TENANTS_KEY 命中即真）。

    空/缺省租户 ID 视为 default（同 ``set_current_tenant`` 口径）。宽松优先
    （同机器亲和闸口径）：无 Redis client 一律放行——闸的失效不得改变存量
    行为。逐次实时查 Redis（不设进程内 TTL 缓存）：停用是安全边界，宁可多
    一次 SISMEMBER，不可让已停用租户残留可跑窗口。
    """
    if redis_client is None:
        return False
    return bool(
        redis_client.sismember(DISABLED_TENANTS_KEY, tenant_id or DEFAULT_TENANT_ID)
    )


def set_tenant_disabled(redis_client: Any, tenant_id: str, disabled: bool) -> None:
    """发布/撤销租户停用标记（console 侧调用，幂等）。"""
    if redis_client is None or not tenant_id:
        return
    if disabled:
        redis_client.sadd(DISABLED_TENANTS_KEY, tenant_id)
    else:
        redis_client.srem(DISABLED_TENANTS_KEY, tenant_id)
