"""多租户上下文单测：namespace 映射、ContextVar、租户路由包装器。"""
import pytest

from plaita.server.tenant_context import (
    DEFAULT_TENANT_ID,
    TenantRoutingExecutionLease,
    TenantRoutingExecutionStorage,
    TenantRoutingFlowStorage,
    current_tenant,
    reset_current_tenant,
    set_current_tenant,
    tenant_namespace,
)


class TestTenantNamespace:
    def test_default_and_empty_map_to_legacy_prefix(self):
        assert tenant_namespace("default") == "plaita"
        assert tenant_namespace("") == "plaita"
        assert tenant_namespace(None) == "plaita"

    def test_named_tenant_gets_prefixed_namespace(self):
        assert tenant_namespace("acme") == "plaita:acme"
        assert tenant_namespace("t-1234") == "plaita:t-1234"


class TestContextVar:
    def test_default_tenant(self):
        assert current_tenant() == DEFAULT_TENANT_ID

    def test_set_and_reset(self):
        token = set_current_tenant("acme")
        try:
            assert current_tenant() == "acme"
        finally:
            reset_current_tenant(token)
        assert current_tenant() == DEFAULT_TENANT_ID

    def test_none_normalizes_to_default(self):
        token = set_current_tenant(None)
        try:
            assert current_tenant() == DEFAULT_TENANT_ID
        finally:
            reset_current_tenant(token)


class _RecordingStorage:
    """记录构造参数的桩存储（替代 RedisStorage，隔离单测与 Redis）。"""

    instances = []

    def __init__(self, namespace="plaita", **kwargs):
        self.namespace = namespace
        self.kwargs = kwargs
        type(self).instances.append(self)

    def get_flow(self, flow_id, version=None):
        return {"namespace": self.namespace, "flow_id": flow_id}


class _RecordingLease:
    instances = []

    def __init__(self, redis_client, key_prefix="plaita:execution:lease:"):
        self.redis_client = redis_client
        self.key_prefix = key_prefix
        type(self).instances.append(self)

    def try_acquire(self, execution_id, holder, ttl_seconds):
        return True


class TestTenantRoutingStorage:
    def test_routes_to_per_tenant_instances(self, monkeypatch):
        # _storage_cls 是类属性（导入时绑定），patch 类属性而非模块属性
        monkeypatch.setattr(TenantRoutingFlowStorage, "_storage_cls", _RecordingStorage)
        _RecordingStorage.instances = []
        wrapper = TenantRoutingFlowStorage(host="h", port=1)

        token = set_current_tenant("acme")
        try:
            out = wrapper.get_flow("demo", "1.0.0")
        finally:
            reset_current_tenant(token)
        assert out == {"namespace": "plaita:acme", "flow_id": "demo"}

        token = set_current_tenant("default")
        try:
            wrapper.get_flow("demo")
        finally:
            reset_current_tenant(token)

        namespaces = sorted(s.namespace for s in _RecordingStorage.instances)
        assert namespaces == ["plaita", "plaita:acme"]

    def test_unknown_attribute_passthrough(self, monkeypatch):
        monkeypatch.setattr(
            TenantRoutingExecutionStorage, "_storage_cls", _RecordingStorage
        )
        _RecordingStorage.instances = []
        wrapper = TenantRoutingExecutionStorage(host="h")
        assert wrapper.namespace == "plaita"  # __getattr__ 透传当前租户实例


class TestTenantRoutingLease:
    def test_lease_prefix_follows_tenant(self, monkeypatch):
        # _lease() 惰性 import execution_lease.RedisExecutionLease，patch 其源模块
        import plaita.server.execution_lease as lease_mod

        monkeypatch.setattr(lease_mod, "RedisExecutionLease", _RecordingLease)
        _RecordingLease.instances = []
        lease = TenantRoutingExecutionLease(redis_client=object())

        token = set_current_tenant("acme")
        try:
            assert lease.try_acquire("e1", "h1", 60) is True
        finally:
            reset_current_tenant(token)
        token = set_current_tenant("default")
        try:
            assert lease.try_acquire("e1", "h1", 60) is True
        finally:
            reset_current_tenant(token)

        prefixes = sorted(l.key_prefix for l in _RecordingLease.instances)
        assert prefixes == ["plaita:acme:execution:lease:", "plaita:execution:lease:"]
