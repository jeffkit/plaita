"""集群档多租户上下文。

键空间约定（与 console 侧 engine_sync 共用同一映射规则）：
- 租户数据键跟随租户 namespace：``{ns}:execution:{id}``、``{ns}:flow:{id}:{ver}``、
  ``{ns}:flow_list``、``{ns}:flow_versions:{id}``、``{ns}:execution:lease:{id}``；
  ``tenant_namespace`` 把 default/空租户映射回历史前缀 ``plaita``（存量数据与
  旧版本 worker 兼容），其余租户为 ``plaita:{tenant_id}``。
- 平台机制键（任务队列、registry、control、event_filter 去重、调度锁）不分区。

租户上下文用 ContextVar 承载：FlowWorker 每处理一条任务消息前 set、处理后
reset；租户路由存储包装器据此选择（并按租户缓存）底层 RedisStorage 实例。
"""
from __future__ import annotations

import threading
from contextvars import ContextVar, Token
from typing import Any, Dict, List, Optional

from ..storage.base import ExecutionStorage, FlowStorage
from ..storage.redis import RedisExecutionStorage, RedisFlowStorage

DEFAULT_TENANT_ID = "default"
LEGACY_NAMESPACE = "plaita"

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


class TenantRoutingExecutionStorage(_TenantRoutingMixin, ExecutionStorage):
    """ExecutionStorage 租户路由包装器。"""

    _storage_cls = RedisExecutionStorage

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
    default/空租户保持历史前缀 ``plaita:execution:lease:``（兼容存量锁）。"""

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

    def release(self, execution_id: str, holder: str) -> bool:
        return self._lease().release(execution_id, holder)

    def renew(self, execution_id: str, holder: str, ttl_seconds: int) -> bool:
        return self._lease().renew(execution_id, holder, ttl_seconds)
