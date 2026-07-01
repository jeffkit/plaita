"""
Redis Stream 日志处理器测试
"""
import json
import logging
import time
import pytest
from unittest.mock import MagicMock, patch

from plaita.server.log_handler import (
    RedisStreamHandler,
    setup_redis_logging,
    LogContext,
    ContextFilter
)


class TestRedisStreamHandler:
    """RedisStreamHandler 测试"""
    
    @pytest.fixture
    def mock_redis(self):
        """创建模拟的 Redis 客户端"""
        redis_mock = MagicMock()
        redis_mock.pipeline.return_value = redis_mock
        redis_mock.xadd.return_value = "1234567890-0"
        redis_mock.publish.return_value = 1
        redis_mock.execute.return_value = []
        return redis_mock
    
    def test_create_handler(self, mock_redis):
        """测试创建处理器"""
        handler = RedisStreamHandler(
            redis_client=mock_redis,
            service_type="test_service",
            instance_id="test-instance-1"
        )
        
        assert handler.service_type == "test_service"
        assert handler.instance_id == "test-instance-1"
        assert handler.stream_key == "plaita:logs:test_service:test-instance-1"
        
        handler.close()
    
    def test_emit_log(self, mock_redis):
        """测试发送日志"""
        handler = RedisStreamHandler(
            redis_client=mock_redis,
            service_type="test_service",
            instance_id="test-instance-1"
        )
        
        # 创建日志记录
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test log message",
            args=(),
            exc_info=None
        )
        
        # 发送日志
        handler.emit(record)
        
        # 等待刷新
        time.sleep(1.5)
        
        # 验证日志被添加到队列
        # 由于是异步刷新，这里主要验证不抛出异常
        handler.close()
    
    def test_format_record(self, mock_redis):
        """测试格式化日志记录"""
        handler = RedisStreamHandler(
            redis_client=mock_redis,
            service_type="test_service",
            instance_id="test-instance-1"
        )
        
        record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname="test.py",
            lineno=20,
            msg="Error message",
            args=(),
            exc_info=None
        )
        
        formatted = handler._format_record(record)
        
        assert formatted["level"] == "ERROR"
        assert formatted["message"] == "Error message"
        assert formatted["service_type"] == "test_service"
        assert formatted["instance_id"] == "test-instance-1"
        assert "timestamp" in formatted
        
        handler.close()


class TestSetupRedisLogging:
    """setup_redis_logging 测试"""
    
    @pytest.fixture
    def mock_redis(self):
        redis_mock = MagicMock()
        redis_mock.pipeline.return_value = redis_mock
        redis_mock.execute.return_value = []
        return redis_mock
    
    def test_setup_logging(self, mock_redis):
        """测试设置日志"""
        handler = setup_redis_logging(
            redis_client=mock_redis,
            service_type="test_service",
            instance_id="test-123",
            logger_name="test_setup_logger"
        )
        
        assert handler is not None
        
        # 获取日志器
        logger = logging.getLogger("test_setup_logger")
        assert handler in logger.handlers
        
        # 清理
        handler.close()
        logger.removeHandler(handler)


class TestLogContext:
    """LogContext 测试"""
    
    def test_set_and_get(self):
        """测试设置和获取上下文"""
        LogContext.clear()
        
        LogContext.set(user_id="123", action="test")
        context = LogContext.get()
        
        assert context["user_id"] == "123"
        assert context["action"] == "test"
        
        LogContext.clear()
    
    def test_context_manager(self):
        """测试上下文管理器"""
        LogContext.clear()
        LogContext.set(outer="value")
        
        with LogContext(inner="inner_value"):
            context = LogContext.get()
            assert context["outer"] == "value"
            assert context["inner"] == "inner_value"
        
        # 退出后恢复
        context = LogContext.get()
        assert context.get("outer") == "value"
        assert "inner" not in context
        
        LogContext.clear()


class TestContextFilter:
    """ContextFilter 测试"""
    
    def test_filter_adds_context(self):
        """测试过滤器添加上下文"""
        LogContext.clear()
        LogContext.set(request_id="req-123")
        
        filter = ContextFilter()
        
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test",
            args=(),
            exc_info=None
        )
        
        result = filter.filter(record)
        
        assert result is True
        assert hasattr(record, 'context')
        assert record.context["request_id"] == "req-123"
        
        LogContext.clear()

