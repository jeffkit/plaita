"""集群档多租户上下文（server 侧：兼容 re-export + 租户路由存储包装器）。

P3（2026-10-02 评审遗留 #6）分层下沉：ContextVar 与 namespace 纯函数
（``DEFAULT_TENANT_ID`` / ``LEGACY_NAMESPACE`` / ``tenant_namespace`` /
``current_tenant`` / ``set_current_tenant`` / ``reset_current_tenant`` /
``_tenant_ctx``）移至顶层 ``plaita.tenant_context``——core 层禁止 import
server（import 分层检查），而挂起点 ``core/strategies._subscribe_event``
需要读租户。本模块逐名 re-export 保持全仓既有
``from plaita.server.tenant_context import ...`` 调用点零改动；依赖
storage 层的租户路由包装器（``_TenantRoutingMixin`` /
``TenantRoutingExecutionStorage`` / ``TenantRoutingFlowStorage`` /
``TenantRoutingExecutionLease``）留在原处不动。

键空间约定（与 console 侧 engine_sync 共用同一映射规则）见顶层模块
docstring；平台机制键（任务队列、registry、control、event_filter 去重、
调度锁）不分区。
"""
from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from plaita.tenant_context import (  # noqa: F401 — 兼容 re-export
    DEFAULT_TENANT_ID,
    LEGACY_NAMESPACE,
    _tenant_ctx,
    current_tenant,
    reset_current_tenant,
    set_current_tenant,
    tenant_namespace,
)

from ..storage.base import ExecutionStorage, FlowStorage
from ..storage.redis import RedisExecutionStorage, RedisFlowStorage

__all__ = [
    "DEFAULT_TENANT_ID",
    "LEGACY_NAMESPACE",
    "TenantRoutingExecutionLease",
    "TenantRoutingExecutionStorage",
    "TenantRoutingFlowStorage",
    "_tenant_ctx",
    "current_tenant",
    "reset_current_tenant",
    "set_current_tenant",
    "tenant_namespace",
]


class _TenantRoutingMixin:
    """按 ContextVar 租户选择底层存储实例（按租户缓存，共享连接参数）。"""

    _storage_cls: type

    def __init__(self, **factory_kwargs: Any) -> None:
        self._factory_kwargs = factory_kwargs
        self._instances: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def _storage_for(self, tenant_id: Optional[str]):
        tid = tenant_id or DEFAULT_TENANT_ID
        with self._lock:
            instance = self._instances.get(tid)
            if instance is None:
                instance = self._storage_cls(
                    namespace=tenant_namespace(tid), **self._factory_kwargs
                )
                self._instances[tid] = instance
            return instance

    def _current_storage(self):
        return self._storage_for(current_tenant())

    def __getattr__(self, name: str) -> Any:
        # 接口之外的方法/属性透传到当前租户实例（get_namespace_key 等）
        return getattr(self._current_storage(), name)


def _fenced_execution_storage_cls(**kwargs: Any):
    """ExecutionStorage 的 ``_storage_cls`` 同位注入点（设计稿 §4.2/§6 波次②）。

    默认给 RedisExecutionStorage 套 ``FencedExecutionStorage``（save 变世代
    CAS 写，fencing token 经 ContextVar 由 worker 注入）；``PLAITA_DISABLE_
    FENCING=1`` 时回滚为裸存储（§6 波次②回滚门：摘除包装器）。
    """
    from ..storage.fenced import build_fenced_execution_storage

    return build_fenced_execution_storage(**kwargs)


class TenantRoutingExecutionStorage(_TenantRoutingMixin, ExecutionStorage):
    """ExecutionStorage 租户路由包装器。"""

    # staticmethod：mixin 以 ``self._storage_cls(namespace=..., **kwargs)``
    # 调用——普通函数经实例访问会变成绑定方法，需显式静态化。
    _storage_cls = staticmethod(_fenced_execution_storage_cls)

    def save_execution_state(self, execution_id: str, state: Any) -> bool:
        return self._current_storage().save_execution_state(execution_id, state)

    def load_execution_state(self, execution_id: str) -> Optional[Any]:
        return self._current_storage().load_execution_state(execution_id)

    def delete_execution_state(self, execution_id: str) -> bool:
        return self._current_storage().delete_execution_state(execution_id)

    def list_executions(
        self,
        query: Optional[Any] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Any]:
        return self._current_storage().list_executions(
            query=query, order_by=order_by, limit=limit, offset=offset
        )


class TenantRoutingFlowStorage(_TenantRoutingMixin, FlowStorage):
    """FlowStorage 租户路由包装器。"""

    _storage_cls = RedisFlowStorage

    def get_flow(self, flow_id: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        return self._current_storage().get_flow(flow_id, version)

    def save_flow(self, flow: Dict[str, Any]) -> bool:
        return self._current_storage().save_flow(flow)


class TenantRoutingExecutionLease:
    """resume 租约按租户路由键前缀：``{ns}:execution:lease:{id}``。
    default/空租户保持历史前缀 ``plaita:execution:lease:``（兼容存量锁）。
    fence 世代键跟随同一 namespace：``{ns}:execution:fence:{id}``。"""

    def __init__(self, redis_client: Any) -> None:
        self._redis_client = redis_client
        self._instances: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def _lease(self):
        tid = current_tenant()
        with self._lock:
            lease = self._instances.get(tid)
            if lease is None:
                from .execution_lease import DEFAULT_KEY_PREFIX, RedisExecutionLease

                ns = tenant_namespace(tid)
                prefix = DEFAULT_KEY_PREFIX if ns == LEGACY_NAMESPACE else f"{ns}:execution:lease:"
                lease = RedisExecutionLease(self._redis_client, key_prefix=prefix)
                self._instances[tid] = lease
            return lease

    def try_acquire(self, execution_id: str, holder: str, ttl_seconds: int) -> bool:
        return self._lease().try_acquire(execution_id, holder, ttl_seconds)

    def try_acquire_fenced(
        self, execution_id: str, holder: str, ttl_seconds: int
    ) -> Optional[int]:
        """fencing 注入点接线（设计稿 §4.2）：世代号 acquire 按租户路由。"""
        return self._lease().try_acquire_fenced(execution_id, holder, ttl_seconds)

    def release(self, execution_id: str, holder: str) -> bool:
        return self._lease().release(execution_id, holder)

    def renew(self, execution_id: str, holder: str, ttl_seconds: int) -> bool:
        return self._lease().renew(execution_id, holder, ttl_seconds)
