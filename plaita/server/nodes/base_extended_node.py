"""
扩展节点基础类
为所有基于EventNode的扩展节点提供通用功能和接口
"""
from abc import abstractmethod
from typing import Any, ClassVar, Dict, Optional

from ...node.event_node import EventNode
from ...logger import logger


class BaseExtendedNode(EventNode):
    """
    扩展节点基础类
    所有基于EventNode的扩展节点都应该继承这个类
    """
    
    # 子类需要重写的类变量
    node_type: ClassVar[str] = "extended"
    node_name: ClassVar[str] = "扩展节点"
    
    def execute(self, execution):
        """
        扩展节点的执行逻辑
        """
        logger.info("开始执行扩展节点 [%s] - %s", self.id, self.node_name)
        
        # 调用子类的具体配置生成逻辑
        service_config = self.generate_service_config(execution)
        
        # 调用父类的执行逻辑
        result = super().execute(execution)
        
        # 将服务配置添加到结果中
        result.update({
            "service_config": service_config,
            "node_subtype": self.node_type
        })
        
        logger.info("扩展节点 [%s] 配置生成完成: %s", self.id, service_config)
        
        return result
    
    @abstractmethod
    def generate_service_config(self, execution) -> Dict[str, Any]:
        """
        生成外延服务所需的配置
        子类必须实现这个方法来提供具体的服务配置
        
        Args:
            execution: 执行上下文
            
        Returns:
            Dict[str, Any]: 服务配置字典
        """
        pass
    
    def get_default_retry_config(self) -> Dict[str, Any]:
        """
        获取默认重试配置
        子类可以重写这个方法来提供特定的重试配置
        
        Returns:
            Dict[str, Any]: 重试配置
        """
        return {
            "max_retries": 3,
            "retry_delay_ms": 1000,
            "exponential_backoff": True
        } 