"""
扩展节点单元测试
覆盖所有扩展节点的基础功能和配置验证
"""
import pytest
import time
from unittest.mock import Mock, MagicMock

from plaita.flow import FlowExecution
from plaita.event.memory import InMemoryEventBus
from plaita.server.nodes.base_extended_node import BaseExtendedNode
from plaita.server.nodes.delay_node import DelayNode
from plaita.server.nodes.redis_queue_node import RedisQueueNode
from plaita.server.nodes.http_callback_node import HttpCallbackNode
from plaita.server.nodes.approval_node import ApprovalNode
from plaita.server.nodes.kafka_queue_node import KafkaQueueNode


class TestBaseExtendedNode:
    """BaseExtendedNode 基类测试"""
    
    def test_default_retry_config(self):
        """测试默认重试配置"""
        # 创建一个具体实现类进行测试
        delay_node = DelayNode(
            id="test",
            delay_seconds=1
        )
        
        retry_config = delay_node.get_default_retry_config()
        
        assert retry_config["max_retries"] == 3
        assert retry_config["retry_delay_ms"] == 1000
        assert retry_config["exponential_backoff"] is True


class TestDelayNode:
    """DelayNode 延迟节点测试"""
    
    @pytest.fixture
    def execution(self):
        """创建执行上下文"""
        event_bus = InMemoryEventBus()
        execution = FlowExecution(event_bus=event_bus)
        execution.clean()
        execution._set_state("$FLOW_ID", "test_flow")
        execution._set_state("$EXECUTION_ID", "exec_123")
        return execution
    
    def test_create_with_seconds(self):
        """测试使用秒创建"""
        node = DelayNode(
            id="delay_seconds",
            delay_seconds=30,
            delay_unit="seconds"
        )
        
        assert node.delay_seconds == 30
        assert node.delay_unit == "seconds"
    
    def test_create_with_minutes(self):
        """测试使用分钟创建"""
        node = DelayNode(
            id="delay_minutes",
            delay_seconds=5,
            delay_unit="minutes"
        )
        
        assert node._convert_to_milliseconds(5, "minutes") == 300000
    
    def test_create_with_hours(self):
        """测试使用小时创建"""
        node = DelayNode(
            id="delay_hours",
            delay_seconds=2,
            delay_unit="hours"
        )
        
        assert node._convert_to_milliseconds(2, "hours") == 7200000
    
    def test_create_with_days(self):
        """测试使用天创建"""
        node = DelayNode(
            id="delay_days",
            delay_seconds=1,
            delay_unit="days"
        )
        
        assert node._convert_to_milliseconds(1, "days") == 86400000
    
    def test_variable_reference(self, execution):
        """测试变量引用延迟时间"""
        node = DelayNode(
            id="delay_var",
            delay_seconds="$INPUT.delay",
            delay_unit="seconds"
        )
        
        execution._set_state("$INPUT", {"delay": 10})
        delay_value = node._resolve_delay_time(execution)
        
        assert delay_value == 10
    
    def test_variable_reference_fallback(self, execution):
        """测试变量引用失败时的默认值"""
        node = DelayNode(
            id="delay_var",
            delay_seconds="$NONEXISTENT.delay",
            delay_unit="seconds"
        )
        
        delay_value = node._resolve_delay_time(execution)
        
        # 应该返回默认值 60 秒
        assert delay_value == 60
    
    def test_generate_config(self, execution):
        """测试生成服务配置"""
        node = DelayNode(
            id="delay_config",
            delay_seconds=5,
            delay_unit="seconds"
        )
        
        config = node.generate_service_config(execution)
        
        assert config["type"] == "delay"
        assert config["delay_ms"] == 5000
        assert config["node_id"] == "delay_config"
        assert "trigger_timestamp" in config
        
        # 验证触发时间戳合理
        now_ms = int(time.time() * 1000)
        assert config["trigger_timestamp"] >= now_ms + 4500
        assert config["trigger_timestamp"] <= now_ms + 6000
    
    def test_validate_valid_config(self):
        """测试验证有效配置"""
        node = DelayNode(id="test", delay_seconds=1)
        
        config = {
            "delay_ms": 1000,
            "trigger_timestamp": int(time.time() * 1000) + 1000,
            "node_id": "test",
            "event_type": "delay_trigger"
        }
        
        assert node.validate_service_config(config) is True
    
    def test_validate_invalid_config_missing_field(self):
        """测试验证缺少字段的配置"""
        node = DelayNode(id="test", delay_seconds=1)
        
        config = {"delay_ms": 1000}
        
        assert node.validate_service_config(config) is False
    
    def test_validate_invalid_config_negative_delay(self):
        """测试验证负延迟时间"""
        node = DelayNode(id="test", delay_seconds=1)
        
        config = {
            "delay_ms": -1,
            "trigger_timestamp": int(time.time() * 1000),
            "node_id": "test",
            "event_type": "delay_trigger"
        }
        
        assert node.validate_service_config(config) is False


class TestRedisQueueNode:
    """RedisQueueNode Redis队列节点测试"""
    
    @pytest.fixture
    def execution(self):
        """创建执行上下文"""
        event_bus = InMemoryEventBus()
        execution = FlowExecution(event_bus=event_bus)
        execution.clean()
        execution._set_state("$FLOW_ID", "test_flow")
        execution._set_state("$EXECUTION_ID", "exec_123")
        return execution
    
    def test_create_with_defaults(self):
        """测试使用默认值创建"""
        node = RedisQueueNode(
            id="redis_queue",
            event_type="redis_message",
            queue_name="test_queue"
        )
        
        assert node.redis_host == "localhost"
        assert node.redis_port == 6379
        assert node.redis_db == 0
        assert node.queue_type == "list"
        assert node.message_format == "json"
    
    def test_create_with_custom_config(self):
        """测试使用自定义配置创建"""
        node = RedisQueueNode(
            id="redis_queue",
            event_type="redis_message",
            queue_name="custom_queue",
            redis_host="redis.example.com",
            redis_port=6380,
            redis_db=1,
            redis_password="secret",
            queue_type="stream",
            timeout_seconds=30
        )
        
        assert node.redis_host == "redis.example.com"
        assert node.redis_port == 6380
        assert node.redis_db == 1
        assert node.redis_password == "secret"
        assert node.queue_type == "stream"
        assert node.timeout_seconds == 30
    
    def test_generate_config(self, execution):
        """测试生成服务配置"""
        node = RedisQueueNode(
            id="redis_queue",
            event_type="redis_message",
            queue_name="test_queue"
        )
        
        config = node.generate_service_config(execution)
        
        assert config["type"] == "redis_queue"
        assert config["node_id"] == "redis_queue"
        assert "redis_config" in config
        assert "queue_config" in config
        assert "listen_config" in config
        
        assert config["redis_config"]["host"] == "localhost"
        assert config["queue_config"]["name"] == "test_queue"
    
    def test_connection_string(self):
        """测试连接字符串生成"""
        node = RedisQueueNode(
            id="redis_queue",
            event_type="redis_message",
            queue_name="test_queue",
            redis_host="redis.example.com",
            redis_port=6379,
            redis_db=2
        )
        
        conn_str = node.get_connection_string()
        assert conn_str == "redis://redis.example.com:6379/2"
        
        # 带密码
        node_with_pass = RedisQueueNode(
            id="redis_queue",
            event_type="redis_message",
            queue_name="test_queue",
            redis_host="redis.example.com",
            redis_port=6379,
            redis_db=0,
            redis_password="secret"
        )
        
        conn_str_with_pass = node_with_pass.get_connection_string()
        assert conn_str_with_pass == "redis://:secret@redis.example.com:6379/0"
    
    def test_validate_valid_config(self):
        """测试验证有效配置"""
        node = RedisQueueNode(id="test", event_type="redis_message", queue_name="test")
        
        config = {
            "redis_config": {"host": "localhost", "port": 6379},
            "queue_config": {"name": "test", "type": "list"},
            "listen_config": {"timeout_seconds": 0},
            "node_id": "test",
            "event_type": "redis_message"
        }
        
        assert node.validate_service_config(config) is True
    
    def test_validate_invalid_queue_type(self):
        """测试验证无效队列类型"""
        node = RedisQueueNode(id="test", event_type="redis_message", queue_name="test")
        
        config = {
            "redis_config": {"host": "localhost", "port": 6379},
            "queue_config": {"name": "test", "type": "invalid"},
            "listen_config": {},
            "node_id": "test",
            "event_type": "redis_message"
        }
        
        assert node.validate_service_config(config) is False


class TestHttpCallbackNode:
    """HttpCallbackNode HTTP回调节点测试"""
    
    @pytest.fixture
    def execution(self):
        """创建执行上下文"""
        event_bus = InMemoryEventBus()
        execution = FlowExecution(event_bus=event_bus)
        execution.clean()
        execution._set_state("$FLOW_ID", "test_flow")
        execution._set_state("$EXECUTION_ID", "exec_123")
        return execution
    
    def test_create_with_defaults(self):
        """测试使用默认值创建"""
        node = HttpCallbackNode(id="http_callback", event_type="http_callback")
        
        assert node.callback_method == "POST"
        assert node.callback_timeout_minutes == 60
        assert node.require_auth is True
        assert node.auth_type == "token"
    
    def test_create_with_custom_path(self):
        """测试使用自定义路径创建"""
        node = HttpCallbackNode(
            id="http_callback",
            event_type="http_callback",
            callback_path="/custom/callback",
            callback_method="PUT"
        )
        
        assert node.callback_path == "/custom/callback"
        assert node.callback_method == "PUT"
    
    def test_auto_generate_callback_path(self, execution):
        """测试自动生成回调路径"""
        node = HttpCallbackNode(id="http_callback", event_type="http_callback")
        
        path = node._generate_callback_path(execution)
        
        assert "/callback/test_flow/http_callback/" in path
    
    def test_generate_auth_token(self):
        """测试生成认证令牌"""
        node = HttpCallbackNode(id="http_callback", event_type="http_callback")
        
        token1 = node._generate_auth_token()
        token2 = node._generate_auth_token()
        
        assert len(token1) == 32
        assert token1 != token2
    
    def test_generate_config(self, execution):
        """测试生成服务配置"""
        node = HttpCallbackNode(
            id="http_callback",
            event_type="http_callback",
            callback_method="POST"
        )
        
        config = node.generate_service_config(execution)
        
        assert config["type"] == "http_callback"
        assert config["node_id"] == "http_callback"
        assert "callback_config" in config
        assert "auth_config" in config
        assert "validation_config" in config
        
        assert config["callback_config"]["method"] == "POST"
        assert config["auth_config"]["enabled"] is True
    
    def test_auth_config_disabled(self, execution):
        """测试禁用认证配置"""
        node = HttpCallbackNode(
            id="http_callback",
            event_type="http_callback",
            require_auth=False
        )
        
        auth_config = node._generate_auth_config(execution)
        
        assert auth_config["enabled"] is False
    
    def test_validate_valid_config(self):
        """测试验证有效配置"""
        node = HttpCallbackNode(id="test", event_type="http_callback")
        
        config = {
            "callback_config": {"path": "/callback", "method": "POST"},
            "auth_config": {"enabled": True},
            "validation_config": {"headers": {}, "params": {}},
            "node_id": "test",
            "event_type": "http_callback"
        }
        
        assert node.validate_service_config(config) is True
    
    def test_validate_invalid_method(self):
        """测试验证无效HTTP方法"""
        node = HttpCallbackNode(id="test", event_type="http_callback")
        
        config = {
            "callback_config": {"path": "/callback", "method": "INVALID"},
            "auth_config": {},
            "validation_config": {},
            "node_id": "test",
            "event_type": "http_callback"
        }
        
        assert node.validate_service_config(config) is False
    
    def test_get_callback_url(self, execution):
        """测试获取回调URL"""
        node = HttpCallbackNode(
            id="http_callback",
            event_type="http_callback",
            callback_path="/custom/path"
        )
        
        url = node.get_callback_url(execution)
        
        assert "http://localhost:8080/custom/path" == url


class TestApprovalNode:
    """ApprovalNode 审批节点测试"""
    
    @pytest.fixture
    def execution(self):
        """创建执行上下文"""
        event_bus = InMemoryEventBus()
        execution = FlowExecution(event_bus=event_bus)
        execution.clean()
        execution._set_state("$FLOW_ID", "test_flow")
        execution._set_state("$EXECUTION_ID", "exec_123")
        return execution
    
    def test_create_basic(self):
        """测试基本创建"""
        node = ApprovalNode(
            id="approval",
            event_type="approval_decision",
            approval_title="测试审批",
            approval_content="请审批",
            approvers=["user1"]
        )
        
        assert node.approval_title == "测试审批"
        assert node.approvers == ["user1"]
        assert node.approval_strategy == "any"
        assert node.event_type == "approval_decision"
    
    def test_create_with_all_options(self):
        """测试使用所有选项创建"""
        node = ApprovalNode(
            id="approval",
            event_type="approval_decision",
            approval_title="完整审批",
            approval_content="详细内容",
            approval_type="manual",
            approvers=["user1", "user2"],
            approval_strategy="all",
            auto_escalation=True,
            escalation_timeout_hours=48,
            escalation_approvers=["manager1"],
            form_fields=[{"name": "reason", "type": "text"}],
            allow_comments=True,
            require_comments=True
        )
        
        assert node.approval_type == "manual"
        assert node.approval_strategy == "all"
        assert node.auto_escalation is True
        assert node.escalation_timeout_hours == 48
        assert len(node.form_fields) == 1
        assert node.require_comments is True
    
    def test_generate_config(self, execution):
        """测试生成服务配置"""
        node = ApprovalNode(
            id="approval",
            event_type="approval_decision",
            approval_title="配置测试",
            approval_content="内容",
            approvers=["admin"]
        )
        
        config = node.generate_service_config(execution)
        
        assert config["type"] == "approval"
        assert config["node_id"] == "approval"
        assert "approval_id" in config
        assert "approval_config" in config
        assert "approver_config" in config
        assert "form_config" in config
        
        assert config["approval_config"]["title"] == "配置测试"
        assert config["approver_config"]["approvers"] == ["admin"]
    
    def test_variable_reference_in_title(self, execution):
        """测试标题中的变量引用"""
        node = ApprovalNode(
            id="approval",
            event_type="approval_decision",
            approval_title="$INPUT.title",
            approval_content="内容",
            approvers=["admin"]
        )
        
        execution._set_state("$INPUT", {"title": "动态标题"})
        
        config = node.generate_service_config(execution)
        
        assert config["approval_config"]["title"] == "动态标题"
    
    def test_variable_reference_in_approvers(self, execution):
        """测试审批人中的变量引用"""
        node = ApprovalNode(
            id="approval",
            event_type="approval_decision",
            approval_title="测试",
            approval_content="内容",
            approvers=["$INPUT.approver"]
        )
        
        execution._set_state("$INPUT", {"approver": "dynamic_user"})
        
        config = node._resolve_approval_config(execution)
        
        assert "dynamic_user" in config["approvers"]["approvers"]
    
    def test_validate_valid_config(self):
        """测试验证有效配置"""
        node = ApprovalNode(
            id="test",
            event_type="approval_decision",
            approval_title="测试",
            approval_content="内容",
            approvers=["user1"]
        )
        
        config = {
            "approval_config": {"title": "测试", "content": "内容"},
            "approver_config": {"approvers": ["user1"], "strategy": "any"},
            "form_config": {},
            "node_id": "test",
            "event_type": "approval_decision"
        }
        
        assert node.validate_service_config(config) is True
    
    def test_validate_empty_approvers(self):
        """测试验证空审批人列表"""
        node = ApprovalNode(
            id="test",
            event_type="approval_decision",
            approval_title="测试",
            approval_content="内容",
            approvers=["user1"]
        )
        
        config = {
            "approval_config": {"title": "测试", "content": "内容"},
            "approver_config": {"approvers": [], "strategy": "any"},
            "form_config": {},
            "node_id": "test",
            "event_type": "approval_decision"
        }
        
        assert node.validate_service_config(config) is False
    
    def test_validate_invalid_strategy(self):
        """测试验证无效审批策略"""
        node = ApprovalNode(
            id="test",
            event_type="approval_decision",
            approval_title="测试",
            approval_content="内容",
            approvers=["user1"]
        )
        
        config = {
            "approval_config": {"title": "测试", "content": "内容"},
            "approver_config": {"approvers": ["user1"], "strategy": "invalid"},
            "form_config": {},
            "node_id": "test",
            "event_type": "approval_decision"
        }
        
        assert node.validate_service_config(config) is False


class TestKafkaQueueNode:
    """KafkaQueueNode Kafka队列节点测试"""
    
    @pytest.fixture
    def execution(self):
        """创建执行上下文"""
        event_bus = InMemoryEventBus()
        execution = FlowExecution(event_bus=event_bus)
        execution.clean()
        execution._set_state("$FLOW_ID", "test_flow")
        execution._set_state("$EXECUTION_ID", "exec_123")
        return execution
    
    def test_create_with_defaults(self):
        """测试使用默认值创建"""
        node = KafkaQueueNode(
            id="kafka_queue",
            event_type="kafka_message",
            bootstrap_servers=["localhost:9092"],
            topic="test_topic",
            group_id="default_group"
        )
        
        assert node.topic == "test_topic"
        assert node.bootstrap_servers == ["localhost:9092"]
        assert node.group_id == "default_group"
        assert node.auto_offset_reset == "latest"
    
    def test_create_with_custom_config(self):
        """测试使用自定义配置创建"""
        node = KafkaQueueNode(
            id="kafka_queue",
            event_type="kafka_message",
            topic="custom_topic",
            bootstrap_servers=["kafka1:9092", "kafka2:9092"],
            group_id="my_group",
            auto_offset_reset="earliest",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass"
        )
        
        assert node.bootstrap_servers == ["kafka1:9092", "kafka2:9092"]
        assert node.group_id == "my_group"
        assert node.auto_offset_reset == "earliest"
        assert node.security_protocol == "SASL_SSL"
    
    def test_generate_config(self, execution):
        """测试生成服务配置"""
        node = KafkaQueueNode(
            id="kafka_queue",
            event_type="kafka_message",
            bootstrap_servers=["localhost:9092"],
            topic="test_topic",
            group_id="test_group"
        )
        
        config = node.generate_service_config(execution)
        
        assert config["type"] == "kafka_queue"
        assert config["node_id"] == "kafka_queue"
        assert "kafka_config" in config
        assert "topic_config" in config
        assert "consumer_config" in config
        
        assert config["topic_config"]["topic"] == "test_topic"
    
    def test_validate_valid_config(self):
        """测试验证有效配置"""
        node = KafkaQueueNode(
            id="test",
            event_type="kafka_message",
            bootstrap_servers=["localhost:9092"],
            topic="test",
            group_id="test_group"
        )
        
        config = {
            "kafka_config": {"bootstrap_servers": ["localhost:9092"]},
            "topic_config": {"topic": "test"},
            "consumer_config": {"group_id": "test_group"},
            "node_id": "test",
            "event_type": "kafka_message"
        }
        
        assert node.validate_service_config(config) is True
    
    def test_validate_missing_topic(self):
        """测试验证缺少主题"""
        node = KafkaQueueNode(
            id="test",
            event_type="kafka_message",
            bootstrap_servers=["localhost:9092"],
            topic="test",
            group_id="test_group"
        )
        
        config = {
            "kafka_config": {"bootstrap_servers": ["localhost:9092"]},
            "topic_config": {},
            "consumer_config": {"group_id": "test_group"},
            "node_id": "test",
            "event_type": "kafka_message"
        }
        
        assert node.validate_service_config(config) is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

