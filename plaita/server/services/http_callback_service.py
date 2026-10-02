"""
HTTP回调服务实现（简化版本）
负责处理HTTP回调注册和监听
"""
import time
from typing import Any, Dict

from .base_service import BaseExtendedService
from ...logger import logger


class HttpCallbackService(BaseExtendedService):
    """
    HTTP回调服务
    负责注册HTTP回调路径并等待回调触发
    """
    
    def __init__(self, event_bus, service_config=None, redis_client=None):
        # 签名对齐基类（同 DelayService）：(event_bus, service_config, redis_client)
        # ——位置传参 HttpCallbackService(bus, {...}) 时第二位是 service_config。
        # redis_client 用于：resume 事件直发 plaita:events:{type} 频道 + 回调
        # 注册跨实例共享（无 redis 时回退进程内存，单测/内存模式不受影响）。
        super().__init__(
            event_bus=event_bus,
            service_config=service_config,
            redis_client=redis_client,
        )
        self.registered_callbacks = {}  # 存储注册的回调信息（无 redis 回退用）
    
    def get_service_type(self) -> str:
        """获取服务类型"""
        return "http_callback"

    async def trigger_event(self, event_type: str, event_data: Dict[str, Any]):
        """触发事件：带 correlation_id（=execution_id），EventFilter 才能关联到挂起执行。

        基类 trigger_event 构造的 Event 不带 correlation_id，会被
        EventFilter.handle_event 直接丢弃——回调到达后挂起执行永远等不到
        resume。统一走基类 publish_resume_event（与 DelayService 的
        trigger_event override 同手法：有 redis 直发 plaita:events:{type}
        频道，无 redis 回退 event_bus.publish）。
        """
        await self.publish_resume_event(event_type, event_data)

    def start_service(self) -> bool:
        """启动HTTP回调服务"""
        try:
            self.is_running = True
            logger.info("HTTP回调服务已启动")
            return True
        except Exception as e:
            logger.error("启动HTTP回调服务失败: %s", e, exc_info=True)
            return False
    
    def stop_service(self) -> bool:
        """停止HTTP回调服务"""
        try:
            self.is_running = False
            self.registered_callbacks.clear()
            logger.info("HTTP回调服务已停止")
            return True
        except Exception as e:
            logger.error("停止HTTP回调服务失败: %s", e, exc_info=True)
            return False
    
    async def handle_task(self, task_config: Dict[str, Any]) -> bool:
        """处理HTTP回调注册任务"""
        try:
            callback_config = task_config.get("callback_config", {})
            node_id = task_config.get("node_id")
            
            # 注册回调路径
            callback_path = callback_config.get("path")
            if callback_path:
                self.registered_callbacks[callback_path] = {
                    "task_config": task_config,
                    "registered_time": int(time.time() * 1000)
                }
                
                logger.info("HTTP回调路径已注册: %s for node %s", callback_path, node_id)
                
                # 在实际实现中，这里会启动HTTP服务器或注册路由
                # 目前只是简单存储配置
                
                return True
            else:
                logger.error("回调路径不能为空")
                return False
                
        except Exception as e:
            logger.error("处理HTTP回调任务失败: %s", e, exc_info=True)
            return False
    
    async def handle_callback_request(self, path: str, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理HTTP回调请求
        
        Args:
            path: 回调路径
            request_data: 请求数据
            
        Returns:
            Dict[str, Any]: 响应数据
        """
        try:
            if path not in self.registered_callbacks:
                return {"status": "error", "message": "回调路径未注册"}
            
            callback_info = self.registered_callbacks[path]
            task_config = callback_info["task_config"]
            
            # 构造事件数据
            event_data = {
                "node_id": task_config.get("node_id"),
                "execution_id": task_config.get("execution_id"),
                "flow_id": task_config.get("flow_id"),
                "tenant_id": task_config.get("tenant_id") or "default",
                "trigger_type": "http_callback",
                "callback_path": path,
                "request_data": request_data,
                "timestamp": int(time.time() * 1000),
                "success": True
            }
            
            # 触发事件
            await self.trigger_event(task_config.get("event_type"), event_data)
            
            # 移除已处理的回调
            del self.registered_callbacks[path]
            
            logger.info("HTTP回调已处理: %s", path)
            
            # 返回成功响应
            response_config = task_config.get("response_config", {})
            return response_config.get("success_response", {"status": "success"})
            
        except Exception as e:
            logger.error("处理HTTP回调请求失败: %s", e, exc_info=True)
            return {"status": "error", "message": str(e)}
    
    def validate_task_config(self, task_config: Dict[str, Any]) -> bool:
        """验证HTTP回调任务配置"""
        if not super().validate_task_config(task_config):
            return False
        
        callback_config = task_config.get("callback_config")
        if not callback_config:
            logger.error("缺少回调配置")
            return False
        
        if not callback_config.get("path"):
            logger.error("缺少回调路径")
            return False
        
        return True
    
    def get_registered_callbacks(self) -> Dict[str, Any]:
        """获取已注册的回调信息"""
        return self.registered_callbacks.copy() 