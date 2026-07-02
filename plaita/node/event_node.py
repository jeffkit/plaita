"""
事件节点实现，用于在Flow流程中等待事件
"""
import time
from typing import Any, ClassVar, Dict
from enum import Enum

from pydantic import Field

from ..logger import logger
from .basic import Node


class EventNodeStatus(Enum):
    """事件节点状态枚举"""
    PENDING = "pending"       # 等待事件
    COMPLETED = "completed"   # 事件正常完成
    ERROR = "error"           # 处理出错
    TIMEOUT = "timeout"       # 等待超时
    CANCELLED = "cancelled"   # 监听取消


class EventNode(Node):
    """
    事件节点，等待特定事件发生后继续执行流程
    """
    
    node_type: ClassVar[str] = "event"
    node_name: ClassVar[str] = "事件节点"
    async_node: ClassVar[bool] = True
    is_suspending: ClassVar[bool] = True
    
    # 只保留配置属性
    event_type: str
    event_filter: Dict[str, Any] = Field(default_factory=dict)
    
    def _get_node_state(self, execution, default=None):
        """
        从执行上下文中获取节点状态
        
        Args:
            execution: 执行上下文
            default: 默认返回值
            
        Returns:
            dict: 节点状态
        """
        try:
            # 首先尝试使用get_node_state方法
            if hasattr(execution, 'get_node_state'):
                return execution.get_node_state(self.id)
            
            # 然后尝试通过context[$NODE]获取
            if hasattr(execution, 'context'):
                prefix = getattr(execution, 'express_prefix', '$')
                node_key = f"{prefix}NODE"
                
                if node_key in execution.context:
                    node_results = execution.context.get(node_key, {})
                    return node_results.get(self.id, default or {})
            
            # 如果都不存在，返回默认值
            return default or {}
        except Exception as e:
            logger.warning(f"获取节点状态失败: {e}")
            return default or {}
    
    def execute(self, execution):
        """
        执行事件节点，返回需要监听的事件信息
        不再直接与EventBus交互，而是由FlowExecution负责订阅
        
        Args:
            execution: 执行上下文
            
        Returns:
            dict: 包含事件监听信息和当前状态的字典
        """
        # 记录节点开始执行
        logger.info(f"开始执行事件节点 [{self.id}]，配置信息: 事件类型={self.event_type}, 过滤条件={self.event_filter}")
        
        # 尝试解析变量引用的事件类型
        resolved_event_type = self.event_type
        if isinstance(self.event_type, str) and self.event_type.startswith('$'):
            try:
                # 尝试从上下文中解析变量引用
                if hasattr(execution, 'evaluate'):
                    resolved = execution.evaluate(self.event_type)
                    if resolved and isinstance(resolved, str):
                        resolved_event_type = resolved
                        logger.info(f"成功解析事件类型变量引用: {self.event_type} -> {resolved}")
                    else:
                        logger.warning(f"解析事件类型变量引用失败，结果非字符串或为空: {self.event_type} -> {resolved}")
                        resolved_event_type = self.event_type  # 保留原始引用
                else:
                    logger.warning(f"执行上下文不支持evaluate方法，无法解析变量引用: {self.event_type}")
                    resolved_event_type = self.event_type  # 保留原始引用
            except Exception as e:
                logger.error(f"解析事件类型变量引用时出错: {e}")
                resolved_event_type = self.event_type  # 保留原始引用
        
        # 创建新的状态
        state = {}
        
        # 创建事件ID
        state["event_id"] = f"event_{self.id}_{int(time.time() * 1000)}"
            
        # 设置状态为等待
        state["status"] = EventNodeStatus.PENDING.value
        
        # 使用解析后的事件类型
        result = self._create_result(state)
        result["event_type"] = resolved_event_type
        
        logger.info(f"事件节点 [{self.id}] 执行结果: {result}")
        
        return result
    
    def on_event(self, execution, event_data: Dict[str, Any]):
        """
        当事件到达时被调用
        
        Args:
            event_data: 事件数据
            execution: 执行上下文
            
        Returns:
            dict: 更新后的状态
        """
        # 获取当前状态
        state = self._get_node_state(execution, {})
        
        # 检查回调数据
        if not event_data:
            return self._create_result(state)
            
        # 更新状态为已完成
        state["status"] = EventNodeStatus.COMPLETED.value
        
        # 添加事件数据
        state["event_data"] = event_data
        
        # 返回更新后的状态
        return self._create_result(state)
    
    def on_timeout(self, execution):
        """
        处理超时情况
        
        Args:
            execution: 执行上下文
            
        Returns:
            dict: 更新后的状态
        """
        # 获取当前状态
        state = self._get_node_state(execution, {})
        
        # 更新状态为超时
        state["status"] = EventNodeStatus.TIMEOUT.value
        
        # 返回更新后的状态
        return self._create_result(state)
    
    def on_error(self, execution, error_message: str):
        """
        处理错误情况
        
        Args:
            error_message: 错误信息
            execution: 执行上下文
            
        Returns:
            dict: 更新后的状态
        """
        # 获取当前状态
        state = self._get_node_state(execution, {})
        
        # 更新状态为错误
        state["status"] = EventNodeStatus.ERROR.value
        state["error_message"] = error_message
        
        # 返回更新后的状态
        return self._create_result(state)
    
    def on_cancel(self, execution):
        """
        处理取消情况
        
        Args:
            execution: 执行上下文
            
        Returns:
            dict: 更新后的状态
        """
        # 获取当前状态
        state = self._get_node_state(execution, {})
        
        # 更新状态为取消
        state["status"] = EventNodeStatus.CANCELLED.value
        
        # 返回更新后的状态
        return self._create_result(state)

    def resume(self, execution, resume_type, resume_data=None):
        """多态 resume 入口: 按 resume_type 分发到 on_cancel/on_timeout/on_event。

        由内核 ``DistributedStrategy._handle_resume`` 调用, 让 core 层不必
        ``isinstance`` EventNode、也不必知道 on_cancel/on_timeout/on_event 的
        分发表, 从而切断 core -> plaita.node.event_node 反向依赖。
        """
        from plaita.core.errors import ResumeType
        if resume_type is ResumeType.CANCEL:
            return self.on_cancel(execution)
        if resume_type is ResumeType.TIMEOUT:
            return self.on_timeout(execution)
        return self.on_event(execution, resume_data)
    
    def can_handle_event(self, event_type, event_data):
        """
        检查事件是否可以被此节点处理
        """
        # 检查事件类型是否匹配
        if event_type != self.event_type:
            return False
            
        # 如果没有过滤条件，则所有该类型事件都匹配
        if not self.event_filter:
            return True
            
        # 检查事件过滤条件
        for key, value in self.event_filter.items():
            keys = key.split('.')
            current_data = event_data
            
            # 遍历嵌套属性
            for k in keys:
                if isinstance(current_data, dict) and k in current_data:
                    current_data = current_data[k]
                else:
                    return False
                    
            # 检查最终值是否匹配
            if current_data != value:
                return False
                
        # 所有条件都匹配
        return True
    
    def _create_result(self, state: Dict):
        """
        创建包含当前状态的结果字典
        
        Args:
            state: 当前状态
            
        Returns:
            dict: 结果字典
        """
        # 添加基本信息
        result = {
            "event_type": self.event_type,
            "event_filter": self.event_filter,
            "is_async": True  # 使用is_async而不是_async，与FlowCoordinator期望的格式一致
        }
        
        # 合并状态信息
        result.update(state)
        
        return result 