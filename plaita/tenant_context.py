"""多租户上下文（顶层模块，仅依赖标准库与同层的 ``plaita.env_context``）。

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

节点执行离开驱动线程（同步节点池 / 分支池 / 进程池）时租户经
:mod:`plaita.env_context` 的显式快照传播——节点内 ``current_tenant()`` 因此
与驱动侧一致，凭据解析不会错读 default 租户的文件。
"""
from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Optional

from plaita.env_context import register_contextvar

DEFAULT_TENANT_ID = "default"
LEGACY_NAMESPACE = "plaita"

_tenant_ctx: ContextVar[str] = ContextVar("plaita_tenant", default=DEFAULT_TENANT_ID)
# 租户要穿过节点执行线程 / 分支池 / 进程池（见 plaita.env_context）。
register_contextvar(_tenant_ctx)


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
