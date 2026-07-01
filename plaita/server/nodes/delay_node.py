"""
延迟节点实现
支持指定延迟时间后触发事件继续流程
"""
import time
from typing import Any, ClassVar, Dict, Optional, Union
from pydantic import Field

from .base_extended_node import BaseExtendedNode
from ...logger import logger


class DelayNode(BaseExtendedNode):
    """
    延迟节点
    在指定的延迟时间后触发事件，继续流程执行
    """
    
    node_type: ClassVar[str] = "delay"
    node_name: ClassVar[str] = "延迟节点"
    
    # 延迟节点特有配置
    delay_seconds: Union[int, float, str] = Field(description="延迟秒数，支持变量引用")
    delay_unit: str = Field(default="seconds", description="时间单位: seconds, minutes, hours, days")
    event_type: str = Field(default="delay_trigger", description="事件类型")
    
    def generate_service_config(self, execution) -> Dict[str, Any]:
        """
        生成延迟服务配置
        
        Args:
            execution: 执行上下文
            
        Returns:
            Dict[str, Any]: 延迟服务配置
        """
        # 解析延迟时间
        delay_value = self._resolve_delay_time(execution)
        
        # 计算实际的延迟毫秒数
        delay_ms = self._convert_to_milliseconds(delay_value, self.delay_unit)
        
        # 计算触发时间戳
        trigger_timestamp = int(time.time() * 1000) + delay_ms
        
        config = {
            "type": "delay",
            "delay_ms": delay_ms,
            "trigger_timestamp": trigger_timestamp,
            "node_id": self.id,
            "execution_id": execution._get_execution_id() if hasattr(execution, '_get_execution_id') else None,
            "flow_id": execution._get_state(f"{execution.express_prefix}FLOW_ID", None) if hasattr(execution, '_get_state') else None,
            "event_type": self.event_type,
            "event_filter": self.event_filter,
            "retry_config": self.get_default_retry_config()
        }
        
        logger.info(f"延迟节点 [{self.id}] 配置: 延迟{delay_ms}ms，触发时间戳{trigger_timestamp}")
        
        return config
    
    def _resolve_delay_time(self, execution) -> Union[int, float]:
        """
        解析延迟时间，支持变量引用
        
        Args:
            execution: 执行上下文
            
        Returns:
            Union[int, float]: 解析后的延迟时间
        """
        if isinstance(self.delay_seconds, str) and self.delay_seconds.startswith('$'):
            try:
                resolved = execution.evaluate(self.delay_seconds)
                if resolved is not None and isinstance(resolved, (int, float)):
                    return resolved
                else:
                    logger.warning(f"延迟时间变量引用解析失败，使用默认值: {self.delay_seconds}")
                    return 60  # 默认60秒
            except Exception as e:
                logger.error(f"解析延迟时间变量引用时出错: {e}")
                return 60  # 默认60秒
        
        return float(self.delay_seconds)
    
    def _convert_to_milliseconds(self, value: Union[int, float], unit: str) -> int:
        """
        将时间值转换为毫秒
        
        Args:
            value: 时间值
            unit: 时间单位
            
        Returns:
            int: 毫秒数
        """
        multipliers = {
            "seconds": 1000,
            "minutes": 60 * 1000,
            "hours": 60 * 60 * 1000,
            "days": 24 * 60 * 60 * 1000
        }
        
        multiplier = multipliers.get(unit.lower(), 1000)
        return int(value * multiplier)
    
    def validate_service_config(self, config: Dict[str, Any]) -> bool:
        """
        验证延迟服务配置
        
        Args:
            config: 服务配置
            
        Returns:
            bool: 配置是否有效
        """
        required_fields = ["delay_ms", "trigger_timestamp", "node_id", "event_type"]
        for field in required_fields:
            if field not in config:
                logger.error(f"延迟节点配置缺少必要字段: {field}")
                return False
        
        if config["delay_ms"] <= 0:
            logger.error("延迟时间必须大于0")
            return False
            
        return True 