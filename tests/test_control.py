"""
服务控制模块测试
"""
import json
import pytest
from unittest.mock import MagicMock, patch

from plaita.server.control import (
    ControlCommand,
    ControlListener,
    ControlMixin
)


class TestControlCommand:
    """ControlCommand 测试"""
    
    def test_create_command(self):
        """测试创建命令"""
        cmd = ControlCommand(
            command="stop",
            graceful=True
        )
        
        assert cmd.command == "stop"
        assert cmd.graceful is True
        assert cmd.timestamp is not None
    
    def test_from_dict(self):
        """测试从字典创建"""
        data = {
            "command": "status",
            "graceful": False,
            "timestamp": "2024-12-30T10:00:00"
        }
        
        cmd = ControlCommand.from_dict(data)
        
        assert cmd.command == "status"
        assert cmd.graceful is False
        assert cmd.timestamp == "2024-12-30T10:00:00"
    
    def test_from_json(self):
        """测试从 JSON 创建"""
        json_str = '{"command": "reload_config", "graceful": true}'
        
        cmd = ControlCommand.from_json(json_str)
        
        assert cmd.command == "reload_config"
        assert cmd.graceful is True


class TestControlListener:
    """ControlListener 测试"""
    
    @pytest.fixture
    def mock_redis(self):
        """创建模拟的 Redis 客户端"""
        redis_mock = MagicMock()
        pubsub_mock = MagicMock()
        pubsub_mock.subscribe.return_value = None
        pubsub_mock.unsubscribe.return_value = None
        pubsub_mock.get_message.return_value = None
        pubsub_mock.close.return_value = None
        redis_mock.pubsub.return_value = pubsub_mock
        return redis_mock
    
    def test_create_listener(self, mock_redis):
        """测试创建监听器"""
        listener = ControlListener(
            redis_client=mock_redis,
            instance_id="test-instance-1"
        )
        
        assert listener.instance_id == "test-instance-1"
        assert listener.control_channel == "plaita:control:test-instance-1"
    
    def test_register_handler(self, mock_redis):
        """测试注册自定义处理器"""
        listener = ControlListener(
            redis_client=mock_redis,
            instance_id="test-1"
        )
        
        handler = MagicMock()
        listener.register_handler("custom_command", handler)
        
        assert "custom_command" in listener._handlers
    
    def test_handle_stop_command(self, mock_redis):
        """测试处理停止命令"""
        on_stop = MagicMock()
        
        listener = ControlListener(
            redis_client=mock_redis,
            instance_id="test-1",
            on_stop=on_stop
        )
        
        # 模拟接收停止命令
        cmd = ControlCommand(command="stop", graceful=True)
        listener._handle_stop(cmd)
        
        on_stop.assert_called_once_with(True)
    
    def test_handle_status_command(self, mock_redis):
        """测试处理状态命令"""
        on_status = MagicMock(return_value={"status": "running"})
        
        listener = ControlListener(
            redis_client=mock_redis,
            instance_id="test-1",
            on_status=on_status
        )
        
        cmd = ControlCommand(command="status")
        listener._handle_status(cmd)
        
        on_status.assert_called_once()


class TestControlMixin:
    """ControlMixin 测试"""
    
    class TestService(ControlMixin):
        """测试服务类"""

        __test__ = False  # 嵌套辅助类，不参与 pytest 采集

        def __init__(self, redis_client, instance_id):
            self.stopped = False
            self.init_control(redis_client, instance_id)
        
        def _on_stop_command(self, graceful: bool):
            self.stopped = True
        
        def _on_status_command(self):
            return {"status": "test"}
    
    @pytest.fixture
    def mock_redis(self):
        redis_mock = MagicMock()
        pubsub_mock = MagicMock()
        pubsub_mock.subscribe.return_value = None
        pubsub_mock.get_message.return_value = None
        pubsub_mock.close.return_value = None
        redis_mock.pubsub.return_value = pubsub_mock
        return redis_mock
    
    def test_init_control(self, mock_redis):
        """测试初始化控制"""
        service = self.TestService(mock_redis, "test-1")
        
        assert service._control_listener is not None
    
    def test_stop_callback(self, mock_redis):
        """测试停止回调"""
        service = self.TestService(mock_redis, "test-1")
        
        # 模拟调用停止回调
        service._on_stop_command(True)
        
        assert service.stopped is True

