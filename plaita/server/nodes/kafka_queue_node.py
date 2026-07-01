"""
Kafka队列节点实现
支持监听Kafka主题消息并触发流程继续执行
"""
from typing import Any, ClassVar, Dict, List, Optional, Union
from pydantic import Field

from .base_extended_node import BaseExtendedNode
from ...logger import logger


class KafkaQueueNode(BaseExtendedNode):
    """
    Kafka队列节点
    监听Kafka主题消息，接收到消息时触发事件继续流程
    """
    
    node_type: ClassVar[str] = "kafka_queue"
    node_name: ClassVar[str] = "Kafka队列节点"
    
    # Kafka配置
    bootstrap_servers: Union[str, List[str]] = Field(description="Kafka服务器地址，支持多个服务器")
    security_protocol: str = Field(default="PLAINTEXT", description="安全协议: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL")
    sasl_mechanism: Optional[str] = Field(default=None, description="SASL机制: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512")
    sasl_username: Optional[str] = Field(default=None, description="SASL用户名")
    sasl_password: Optional[str] = Field(default=None, description="SASL密码")
    
    # 主题配置
    topic: str = Field(description="Kafka主题名称")
    partition: Optional[int] = Field(default=None, description="指定分区，None表示监听所有分区")
    
    # 消费者配置
    group_id: str = Field(description="消费者组ID")
    auto_offset_reset: str = Field(default="latest", description="偏移量重置策略: earliest, latest")
    enable_auto_commit: bool = Field(default=True, description="是否自动提交偏移量")
    max_poll_records: int = Field(default=1, description="单次拉取的最大记录数")
    
    # 消息处理配置
    message_format: str = Field(default="json", description="消息格式: json, text, avro")
    timeout_ms: int = Field(default=30000, description="消费超时时间（毫秒）")
    
    def __init__(self, **data):
        super().__init__(**data)
        # Kafka队列节点默认监听kafka_message事件
        self.event_type = "kafka_message"
        
    def generate_service_config(self, execution) -> Dict[str, Any]:
        """
        生成Kafka队列服务配置
        
        Args:
            execution: 执行上下文
            
        Returns:
            Dict[str, Any]: Kafka队列服务配置
        """
        # 解析可能的变量引用
        resolved_config = self._resolve_kafka_config(execution)
        
        config = {
            "type": "kafka_queue",
            "node_id": self.id,
            "execution_id": execution._get_execution_id() if hasattr(execution, '_get_execution_id') else None,
            "flow_id": execution._get_state(f"{execution.express_prefix}FLOW_ID", None) if hasattr(execution, '_get_state') else None,
            "event_type": self.event_type,
            "event_filter": self.event_filter,
            "kafka_config": resolved_config["kafka"],
            "topic_config": resolved_config["topic"],
            "consumer_config": resolved_config["consumer"],
            "retry_config": self.get_default_retry_config()
        }
        
        logger.info(f"Kafka队列节点 [{self.id}] 配置: {resolved_config}")
        
        return config
    
    def _resolve_kafka_config(self, execution) -> Dict[str, Any]:
        """
        解析Kafka配置，支持变量引用
        
        Args:
            execution: 执行上下文
            
        Returns:
            Dict[str, Any]: 解析后的配置
        """
        # 处理bootstrap_servers
        bootstrap_servers = self._resolve_value(execution, self.bootstrap_servers)
        if isinstance(bootstrap_servers, str):
            bootstrap_servers = [s.strip() for s in bootstrap_servers.split(',')]
        
        kafka_config = {
            "bootstrap_servers": bootstrap_servers,
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self._resolve_value(execution, self.sasl_mechanism),
            "sasl_username": self._resolve_value(execution, self.sasl_username),
            "sasl_password": self._resolve_value(execution, self.sasl_password)
        }
        
        topic_config = {
            "topic": self._resolve_value(execution, self.topic),
            "partition": self.partition,
            "message_format": self.message_format
        }
        
        consumer_config = {
            "group_id": self._resolve_value(execution, self.group_id),
            "auto_offset_reset": self.auto_offset_reset,
            "enable_auto_commit": self.enable_auto_commit,
            "max_poll_records": self.max_poll_records,
            "timeout_ms": self.timeout_ms
        }
        
        return {
            "kafka": kafka_config,
            "topic": topic_config,
            "consumer": consumer_config
        }
    
    def _resolve_value(self, execution, value):
        """
        解析单个值，支持变量引用
        
        Args:
            execution: 执行上下文
            value: 要解析的值
            
        Returns:
            Any: 解析后的值
        """
        if isinstance(value, str) and value.startswith('$'):
            try:
                resolved = execution.evaluate(value)
                return resolved if resolved is not None else value
            except Exception as e:
                logger.warning(f"解析变量引用失败 {value}: {e}")
                return value
        
        return value
    
    def validate_service_config(self, config: Dict[str, Any]) -> bool:
        """
        验证Kafka队列服务配置
        
        Args:
            config: 服务配置
            
        Returns:
            bool: 配置是否有效
        """
        required_fields = ["kafka_config", "topic_config", "consumer_config", "node_id", "event_type"]
        for field in required_fields:
            if field not in config:
                logger.error(f"Kafka队列节点配置缺少必要字段: {field}")
                return False
        
        # 验证Kafka配置
        kafka_config = config["kafka_config"]
        if not kafka_config.get("bootstrap_servers"):
            logger.error("Kafka服务器地址不能为空")
            return False
        
        # 验证主题配置
        topic_config = config["topic_config"]
        if not topic_config.get("topic"):
            logger.error("Kafka主题名称不能为空")
            return False
        
        # 验证消费者配置
        consumer_config = config["consumer_config"]
        if not consumer_config.get("group_id"):
            logger.error("消费者组ID不能为空")
            return False
            
        return True
    
    def get_consumer_properties(self) -> Dict[str, Any]:
        """
        获取Kafka消费者属性配置
        
        Returns:
            Dict[str, Any]: 消费者属性
        """
        props = {
            'bootstrap.servers': ','.join(self.bootstrap_servers) if isinstance(self.bootstrap_servers, list) else self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': self.enable_auto_commit,
            'max.poll.records': self.max_poll_records,
            'security.protocol': self.security_protocol
        }
        
        # 添加SASL配置（如果需要）
        if self.sasl_mechanism:
            props['sasl.mechanism'] = self.sasl_mechanism
        if self.sasl_username:
            props['sasl.username'] = self.sasl_username
        if self.sasl_password:
            props['sasl.password'] = self.sasl_password
            
        return props 