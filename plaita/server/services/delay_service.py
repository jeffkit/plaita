"""
延迟服务实现
负责处理延迟任务，在指定时间后触发事件
"""
import asyncio
import time
from typing import Any, Dict

from .base_service import BaseExtendedService
from ...logger import logger


class DelayService(BaseExtendedService):
    """
    延迟服务
    负责处理延迟任务，在指定时间后触发事件
    """
    
    def get_service_type(self) -> str:
        """
        获取服务类型
        
        Returns:
            str: 服务类型
        """
        return "delay"
    
    def start_service(self) -> bool:
        """
        启动延迟服务
        
        Returns:
            bool: 启动是否成功
        """
        try:
            self.is_running = True
            logger.info("延迟服务已启动")
            return True
        except Exception as e:
            logger.error(f"启动延迟服务失败: {e}", exc_info=True)
            return False
    
    def stop_service(self) -> bool:
        """
        停止延迟服务
        
        Returns:
            bool: 停止是否成功
        """
        try:
            self.is_running = False
            logger.info("延迟服务已停止")
            return True
        except Exception as e:
            logger.error(f"停止延迟服务失败: {e}", exc_info=True)
            return False
    
    async def handle_task(self, task_config: Dict[str, Any]) -> bool:
        """
        处理延迟任务
        
        Args:
            task_config: 任务配置
            
        Returns:
            bool: 处理是否成功
        """
        try:
            # 从配置中获取延迟信息
            delay_ms = task_config.get("delay_ms", 0)
            trigger_timestamp = task_config.get("trigger_timestamp")
            node_id = task_config.get("node_id")
            execution_id = task_config.get("execution_id")
            flow_id = task_config.get("flow_id")
            event_type = task_config.get("event_type")
            
            logger.info(f"开始处理延迟任务: node_id={node_id}, delay_ms={delay_ms}")
            
            # 计算实际需要等待的时间
            current_time = int(time.time() * 1000)
            if trigger_timestamp:
                # 使用绝对时间戳
                wait_ms = max(0, trigger_timestamp - current_time)
            else:
                # 使用相对延迟时间
                wait_ms = delay_ms
            
            # 如果需要等待的时间太长，可以考虑分段等待
            if wait_ms > 0:
                wait_seconds = wait_ms / 1000.0
                logger.info(f"延迟任务等待中: {wait_seconds}秒")
                
                # 分段等待，每次最多等待60秒，以便及时响应关闭请求
                while wait_seconds > 0 and not self.is_shutdown_requested():
                    chunk_wait = min(60, wait_seconds)
                    await asyncio.sleep(chunk_wait)
                    wait_seconds -= chunk_wait
                
                # 检查是否被要求关闭
                if self.is_shutdown_requested():
                    logger.info(f"延迟任务被中断: node_id={node_id}")
                    return False
            
            # 构造事件数据
            event_data = {
                "node_id": node_id,
                "execution_id": execution_id,
                "flow_id": flow_id,
                "trigger_type": "delay_completed",
                "delay_ms": delay_ms,
                "actual_trigger_timestamp": int(time.time() * 1000),
                "planned_trigger_timestamp": trigger_timestamp,
                "success": True
            }
            
            # 触发事件
            await self.trigger_event(event_type, event_data)
            
            logger.info(f"延迟任务完成: node_id={node_id}")
            return True
            
        except Exception as e:
            logger.error(f"处理延迟任务失败: {e}", exc_info=True)
            
            # 触发错误事件
            try:
                error_event_data = {
                    "node_id": task_config.get("node_id"),
                    "execution_id": task_config.get("execution_id"),
                    "flow_id": task_config.get("flow_id"),
                    "trigger_type": "delay_error",
                    "error_message": str(e),
                    "success": False
                }
                await self.trigger_event(task_config.get("event_type"), error_event_data)
            except:
                pass
            
            return False
    
    def validate_task_config(self, task_config: Dict[str, Any]) -> bool:
        """
        验证延迟任务配置
        
        Args:
            task_config: 任务配置
            
        Returns:
            bool: 配置是否有效
        """
        # 调用父类验证
        if not super().validate_task_config(task_config):
            return False
        
        # 验证延迟特定字段
        delay_ms = task_config.get("delay_ms")
        trigger_timestamp = task_config.get("trigger_timestamp")
        
        if delay_ms is None and trigger_timestamp is None:
            logger.error("延迟任务必须指定 delay_ms 或 trigger_timestamp")
            return False
        
        if delay_ms is not None and delay_ms < 0:
            logger.error("延迟时间不能为负数")
            return False
        
        if trigger_timestamp is not None and trigger_timestamp <= int(time.time() * 1000):
            logger.warning("触发时间戳已过期，将立即触发")
        
        return True
    
    def get_pending_tasks_info(self) -> Dict[str, Any]:
        """
        获取待处理任务信息
        
        Returns:
            Dict[str, Any]: 任务信息
        """
        return {
            "service_type": self.get_service_type(),
            "active_task_count": self.get_active_task_count(),
            "is_running": self.is_running,
            "max_workers": self.get_max_workers()
        } 