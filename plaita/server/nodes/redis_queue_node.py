"""
Redis队列节点实现
支持监听Redis队列消息并触发流程继续执行
"""
from typing import Any, ClassVar, Dict, List, Literal, Optional, Union
from pydantic import Field, model_validator

from .base_extended_node import BaseExtendedNode
from ...logger import logger


class RedisQueueNode(BaseExtendedNode):
    """
    Redis队列节点
    监听Redis队列消息，接收到消息时触发事件继续流程
    """

    node_type: ClassVar[str] = "redis_queue"
    node_name: ClassVar[str] = "Redis队列节点"

    # 运行时契约：固定订阅 redis_message 事件。历史上 event_type 必填而用户值
    # 必被 __init__ 覆盖（伪必填地雷，2026-09 表单评审）；现在由 _set_event_type
    # 在校验前注入，schema 不再 required
    event_type: str = Field(default="redis_message", description="内部事件类型标识，由节点自动设定，请勿修改")

    # Redis配置
    redis_host: str = Field(default="localhost", description="Redis主机地址")
    redis_port: int = Field(default=6379, description="Redis端口")
    redis_db: int = Field(default=0, description="Redis数据库")
    redis_password: Optional[str] = Field(default=None, description="Redis密码")

    # 队列配置
    queue_name: str = Field(description="队列名称")
    # Literal 生成 schema enum（console 表单渲染下拉）；未知历史值此前静默
    # 按消费服务的兜底分支处理，现在解析期即拦截
    queue_type: Literal["list", "stream", "pubsub"] = Field(
        default="list", description="队列类型: list / stream / pubsub"
    )

    # 监听配置
    timeout_seconds: int = Field(default=0, description="监听超时时间，0表示无限等待")
    batch_size: int = Field(default=1, description="批量处理大小")

    # 消息处理配置
    message_format: Literal["json", "text", "raw"] = Field(
        default="json", description="消息格式: json / text / raw"
    )

    @model_validator(mode="before")
    @classmethod
    def _set_event_type(cls, values):
        # 强制事件订阅契约（原 __init__ 无条件覆盖行为的等价迁移）
        if isinstance(values, dict):
            values["event_type"] = "redis_message"
        return values
        
    def generate_service_config(self, execution) -> Dict[str, Any]:
        """
        生成Redis队列服务配置
        
        Args:
            execution: 执行上下文
            
        Returns:
            Dict[str, Any]: Redis队列服务配置
        """
        # 解析可能的变量引用
        resolved_config = self._resolve_redis_config(execution)
        
        config = {
            "type": "redis_queue",
            "node_id": self.id,
            "execution_id": execution.execution_id,
            "flow_id": execution.state.flow_id,
            "event_type": self.event_type,
            "event_filter": self.event_filter,
            "redis_config": resolved_config["redis"],
            "queue_config": resolved_config["queue"],
            "listen_config": resolved_config["listen"],
            "retry_config": self.get_default_retry_config()
        }
        
        logger.info("Redis队列节点 [%s] 配置: %s", self.id, resolved_config)
        
        return config
    
    def _resolve_redis_config(self, execution) -> Dict[str, Any]:
        """
        解析Redis配置，支持变量引用
        
        Args:
            execution: 执行上下文
            
        Returns:
            Dict[str, Any]: 解析后的配置
        """
        redis_config = {
            "host": self._resolve_value(execution, self.redis_host),
            "port": self._resolve_value(execution, self.redis_port),
            "db": self._resolve_value(execution, self.redis_db),
            "password": self._resolve_value(execution, self.redis_password)
        }
        
        queue_config = {
            "name": self._resolve_value(execution, self.queue_name),
            "type": self.queue_type,
            "message_format": self.message_format
        }
        
        listen_config = {
            "timeout_seconds": self.timeout_seconds,
            "batch_size": self.batch_size
        }
        
        return {
            "redis": redis_config,
            "queue": queue_config,
            "listen": listen_config
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
                logger.warning("解析变量引用失败 %s: %s", value, e)
                return value
        
        return value
    
    def validate_service_config(self, config: Dict[str, Any]) -> bool:
        """
        验证Redis队列服务配置
        
        Args:
            config: 服务配置
            
        Returns:
            bool: 配置是否有效
        """
        required_fields = ["redis_config", "queue_config", "listen_config", "node_id", "event_type"]
        for field in required_fields:
            if field not in config:
                logger.error("Redis队列节点配置缺少必要字段: %s", field)
                return False
        
        # 验证Redis配置
        redis_config = config["redis_config"]
        if not redis_config.get("host") or not redis_config.get("port"):
            logger.error("Redis主机和端口配置不能为空")
            return False
        
        # 验证队列配置
        queue_config = config["queue_config"]
        if not queue_config.get("name"):
            logger.error("队列名称不能为空")
            return False
        
        if queue_config.get("type") not in ["list", "stream", "pubsub"]:
            logger.error("不支持的队列类型")
            return False
            
        return True
    
    def get_connection_string(self) -> str:
        """
        获取Redis连接字符串
        
        Returns:
            str: 连接字符串
        """
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        else:
            return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}" 