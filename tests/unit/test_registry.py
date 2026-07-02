"""
服务注册模块测试
"""
import json
import time
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from plaita.server.registry import (
    ServiceInfo,
    ServiceRegistry,
    RegistryMixin
)


class TestServiceInfo:
    """ServiceInfo 类测试"""
    
    def test_create_service_info(self):
        """测试创建服务信息"""
        info = ServiceInfo(
            instance_id="test-instance-1",
            service_type="flow_worker",
            host="localhost/127.0.0.1",
            status="running",
            metadata={"queue_name": "test-queue"}
        )
        
        assert info.instance_id == "test-instance-1"
        assert info.service_type == "flow_worker"
        assert info.host == "localhost/127.0.0.1"
        assert info.status == "running"
        assert info.metadata["queue_name"] == "test-queue"
        assert info.active_tasks == 0
    
    def test_to_dict(self):
        """测试转换为字典"""
        info = ServiceInfo(
            instance_id="test-instance-1",
            service_type="delay_service",
            host="localhost"
        )
        
        data = info.to_dict()
        
        assert data["instance_id"] == "test-instance-1"
        assert data["service_type"] == "delay_service"
        assert data["host"] == "localhost"
        assert "start_time" in data
        assert "last_heartbeat" in data
    
    def test_from_dict(self):
        """测试从字典创建"""
        data = {
            "instance_id": "test-123",
            "service_type": "redis_queue_service",
            "host": "server1",
            "status": "running",
            "start_time": "2024-12-30T10:00:00",
            "metadata": {"port": 6379},
            "active_tasks": 5
        }
        
        info = ServiceInfo.from_dict(data)
        
        assert info.instance_id == "test-123"
        assert info.service_type == "redis_queue_service"
        assert info.active_tasks == 5
    
    def test_json_serialization(self):
        """测试 JSON 序列化"""
        info = ServiceInfo(
            instance_id="json-test",
            service_type="test_service",
            host="localhost"
        )
        
        json_str = info.to_json()
        restored = ServiceInfo.from_json(json_str)
        
        assert restored.instance_id == info.instance_id
        assert restored.service_type == info.service_type


class TestServiceRegistry:
    """ServiceRegistry 类测试"""
    
    @pytest.fixture
    def mock_redis(self):
        """创建模拟的 Redis 客户端"""
        redis_mock = MagicMock()
        redis_mock.setex = MagicMock(return_value=True)
        redis_mock.get = MagicMock(return_value=None)
        redis_mock.delete = MagicMock(return_value=1)
        redis_mock.keys = MagicMock(return_value=[])
        return redis_mock
    
    def test_generate_instance_id(self):
        """测试生成实例 ID"""
        id1 = ServiceRegistry.generate_instance_id()
        id2 = ServiceRegistry.generate_instance_id()
        
        assert id1 != id2
        assert "-" in id1
    
    def test_get_host_address(self):
        """测试获取主机地址"""
        host = ServiceRegistry.get_host_address()
        assert host is not None
        assert len(host) > 0
    
    def test_register_service(self, mock_redis):
        """测试注册服务"""
        registry = ServiceRegistry(redis_client=mock_redis)
        
        info = ServiceInfo(
            instance_id="test-1",
            service_type="flow_worker",
            host="localhost"
        )
        
        result = registry.register(info)
        
        assert result is True
        mock_redis.setex.assert_called_once()
        
        # 验证 key 格式
        call_args = mock_redis.setex.call_args
        key = call_args[0][0]
        assert key == "plaita:registry:flow_worker:test-1"
    
    def test_unregister_service(self, mock_redis):
        """测试注销服务"""
        registry = ServiceRegistry(redis_client=mock_redis)
        
        result = registry.unregister("flow_worker", "test-1")
        
        assert result is True
        mock_redis.delete.assert_called_once()
    
    def test_heartbeat(self, mock_redis):
        """测试心跳"""
        registry = ServiceRegistry(redis_client=mock_redis)
        
        info = ServiceInfo(
            instance_id="test-1",
            service_type="flow_worker",
            host="localhost"
        )
        
        result = registry.heartbeat(info)
        
        assert result is True
        mock_redis.setex.assert_called_once()
    
    def test_list_services(self, mock_redis):
        """测试列出服务"""
        # 模拟有两个注册的服务
        mock_redis.keys.return_value = [
            b"plaita:registry:flow_worker:instance-1",
            b"plaita:registry:flow_worker:instance-2"
        ]
        
        info1 = ServiceInfo(
            instance_id="instance-1",
            service_type="flow_worker",
            host="host1"
        )
        info2 = ServiceInfo(
            instance_id="instance-2",
            service_type="flow_worker",
            host="host2"
        )
        
        mock_redis.get.side_effect = [info1.to_json(), info2.to_json()]
        
        registry = ServiceRegistry(redis_client=mock_redis)
        services = registry.list_services("flow_worker")
        
        assert len(services) == 2
        assert services[0].instance_id == "instance-1"
        assert services[1].instance_id == "instance-2"
    
    def test_get_service(self, mock_redis):
        """测试获取单个服务"""
        info = ServiceInfo(
            instance_id="test-1",
            service_type="delay_service",
            host="localhost"
        )
        mock_redis.get.return_value = info.to_json()
        
        registry = ServiceRegistry(redis_client=mock_redis)
        result = registry.get_service("delay_service", "test-1")
        
        assert result is not None
        assert result.instance_id == "test-1"
        assert result.service_type == "delay_service"
    
    def test_update_service_info(self, mock_redis):
        """测试更新服务信息"""
        registry = ServiceRegistry(redis_client=mock_redis)
        
        info = ServiceInfo(
            instance_id="test-1",
            service_type="flow_worker",
            host="localhost"
        )
        registry._registered_service = info
        
        registry.update_service_info(active_tasks=5, metadata={"new_key": "value"})
        
        assert registry._registered_service.active_tasks == 5
        assert registry._registered_service.metadata["new_key"] == "value"


class TestRegistryMixin:
    """RegistryMixin 测试"""
    
    class TestService(RegistryMixin):
        """用于测试的服务类"""
        __test__ = False  # helper class, not a pytest test case

        def __init__(self, redis_client):
            self.init_registry(
                redis_client=redis_client,
                service_type="test_service",
                metadata={"test": True}
            )
    
    @pytest.fixture
    def mock_redis(self):
        """创建模拟的 Redis 客户端"""
        redis_mock = MagicMock()
        redis_mock.setex = MagicMock(return_value=True)
        redis_mock.get = MagicMock(return_value=None)
        redis_mock.delete = MagicMock(return_value=1)
        return redis_mock
    
    def test_init_registry(self, mock_redis):
        """测试初始化注册"""
        service = self.TestService(mock_redis)
        
        assert service._registry is not None
        assert service._service_info is not None
        assert service._service_info.service_type == "test_service"
    
    def test_register_and_unregister(self, mock_redis):
        """测试注册和注销"""
        service = self.TestService(mock_redis)
        
        # 注册
        result = service.register_service()
        assert result is True
        
        # 注销
        result = service.unregister_service()
        assert result is True
    
    def test_instance_id_property(self, mock_redis):
        """测试实例 ID 属性"""
        service = self.TestService(mock_redis)
        
        assert service.instance_id is not None
        assert len(service.instance_id) > 0


@pytest.mark.integration
class TestServiceRegistryIntegration:
    """服务注册集成测试（需要 Redis）"""
    
    @pytest.fixture
    def redis_client(self):
        """创建真实的 Redis 客户端"""
        try:
            from redis import Redis
            client = Redis.from_url("redis://localhost:6379/0")
            client.ping()
            yield client
            # 清理测试数据
            keys = client.keys("plaita:registry:test_*")
            if keys:
                client.delete(*keys)
        except Exception:
            pytest.skip("Redis 不可用")
    
    def test_full_lifecycle(self, redis_client):
        """测试完整的服务生命周期"""
        registry = ServiceRegistry(
            redis_client=redis_client,
            ttl=5,
            heartbeat_interval=2
        )
        
        info = ServiceInfo(
            instance_id="integration-test-1",
            service_type="test_service",
            host="localhost"
        )
        
        # 注册
        assert registry.register(info) is True
        
        # 验证可以获取
        result = registry.get_service("test_service", "integration-test-1")
        assert result is not None
        assert result.status == "running"
        
        # 列出服务
        services = registry.list_services("test_service")
        assert len(services) >= 1
        
        # 心跳
        info.active_tasks = 3
        assert registry.heartbeat(info) is True
        
        # 验证更新
        result = registry.get_service("test_service", "integration-test-1")
        assert result.active_tasks == 3
        
        # 注销
        assert registry.unregister("test_service", "integration-test-1") is True
        
        # 验证已删除
        result = registry.get_service("test_service", "integration-test-1")
        assert result is None

