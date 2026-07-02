"""
外延服务基础类
为所有外延服务提供通用功能和接口
"""
import asyncio
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Set
from concurrent.futures import ThreadPoolExecutor

from redis import Redis

from ...logger import logger
from ...event.core import EventBus, Event
from ..registry import RegistryMixin, ServiceRegistry
from ..control import ControlMixin


class BaseExtendedService(RegistryMixin, ControlMixin, ABC):
    """
    外延服务基础类
    所有外延服务都应该继承这个类
    
    支持 Redis 服务注册和心跳机制
    """
    
    def __init__(
        self, 
        event_bus: EventBus, 
        service_config: Optional[Dict[str, Any]] = None,
        redis_client: Optional[Redis] = None,
        enable_registry: bool = True
    ):
        """
        初始化服务
        
        Args:
            event_bus: 事件总线实例
            service_config: 服务配置
            redis_client: Redis 客户端（用于服务注册）
            enable_registry: 是否启用服务注册
        """
        self.event_bus = event_bus
        self.service_config = service_config or {}
        self.is_running = False
        self.active_tasks: Set[str] = set()
        self.thread_pool = ThreadPoolExecutor(max_workers=self.get_max_workers())
        self._shutdown_event = threading.Event()
        
        # 服务注册
        self._enable_registry = enable_registry and redis_client is not None
        self._redis_client = redis_client
        if self._enable_registry:
            self.init_registry(
                redis_client=redis_client,
                service_type=self.get_service_type(),
                metadata=self._get_registry_metadata(),
                ttl=self.service_config.get("registry_ttl", ServiceRegistry.DEFAULT_TTL),
                heartbeat_interval=self.service_config.get(
                    "heartbeat_interval", 
                    ServiceRegistry.DEFAULT_HEARTBEAT_INTERVAL
                )
            )
            
            # 初始化控制监听
            if self._service_info:
                self.init_control(
                    redis_client=redis_client,
                    instance_id=self._service_info.instance_id
                )
    
    def _get_registry_metadata(self) -> Dict[str, Any]:
        """
        获取注册元数据
        子类可以重写此方法提供额外的元数据
        
        Returns:
            Dict[str, Any]: 元数据字典
        """
        return {
            "max_workers": self.get_max_workers(),
            "config": {
                k: v for k, v in self.service_config.items() 
                if k not in ("registry_ttl", "heartbeat_interval")
            }
        }
        
    def get_max_workers(self) -> int:
        """
        获取最大工作线程数
        
        Returns:
            int: 最大工作线程数
        """
        return self.service_config.get("max_workers", 10)
    
    @abstractmethod
    def get_service_type(self) -> str:
        """
        获取服务类型
        
        Returns:
            str: 服务类型
        """
        pass
    
    @abstractmethod
    def start_service(self) -> bool:
        """
        启动服务
        
        Returns:
            bool: 启动是否成功
        """
        pass
    
    @abstractmethod
    def stop_service(self) -> bool:
        """
        停止服务
        
        Returns:
            bool: 停止是否成功
        """
        pass
    
    @abstractmethod
    async def handle_task(self, task_config: Dict[str, Any]) -> bool:
        """
        处理任务
        
        Args:
            task_config: 任务配置
            
        Returns:
            bool: 处理是否成功
        """
        pass
    
    def submit_task(self, task_config: Dict[str, Any]) -> str:
        """
        提交任务
        
        Args:
            task_config: 任务配置
            
        Returns:
            str: 任务ID
        """
        task_id = self._generate_task_id(task_config)
        
        if not self.validate_task_config(task_config):
            logger.error(f"任务配置验证失败: {task_config}")
            return ""
        
        # 将任务添加到活跃任务集合
        self.active_tasks.add(task_id)
        
        # 更新注册信息中的活跃任务数
        if self._enable_registry:
            self.update_registry_info(active_tasks=len(self.active_tasks))
        
        # 异步处理任务
        if asyncio.iscoroutinefunction(self.handle_task):
            # 异步任务
            future = self.thread_pool.submit(self._run_async_task, task_config, task_id)
        else:
            # 同步任务
            future = self.thread_pool.submit(self._run_sync_task, task_config, task_id)
        
        logger.info(f"任务 {task_id} 已提交到 {self.get_service_type()} 服务")
        
        return task_id
    
    def _run_async_task(self, task_config: Dict[str, Any], task_id: str):
        """
        在新的事件循环中运行异步任务
        
        Args:
            task_config: 任务配置
            task_id: 任务ID
        """
        try:
            # 创建新的事件循环
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # 运行异步任务
            result = loop.run_until_complete(self.handle_task(task_config))
            
            logger.info(f"异步任务 {task_id} 执行完成: {result}")
            
        except Exception as e:
            logger.error("异步任务 %s 执行失败: %s", task_id, e, exc_info=True)
            self._handle_task_error(task_config, e)
        finally:
            # 从活跃任务集合中移除
            self.active_tasks.discard(task_id)
            # 更新注册信息
            if self._enable_registry:
                self.update_registry_info(active_tasks=len(self.active_tasks))
            try:
                loop.close()
            except:
                pass
    
    def _run_sync_task(self, task_config: Dict[str, Any], task_id: str):
        """
        运行同步任务
        
        Args:
            task_config: 任务配置
            task_id: 任务ID
        """
        try:
            result = self.handle_task(task_config)
            logger.info(f"同步任务 {task_id} 执行完成: {result}")
            
        except Exception as e:
            logger.error("同步任务 %s 执行失败: %s", task_id, e, exc_info=True)
            self._handle_task_error(task_config, e)
        finally:
            # 从活跃任务集合中移除
            self.active_tasks.discard(task_id)
            # 更新注册信息
            if self._enable_registry:
                self.update_registry_info(active_tasks=len(self.active_tasks))
    
    def _generate_task_id(self, task_config: Dict[str, Any]) -> str:
        """
        生成任务ID
        
        Args:
            task_config: 任务配置
            
        Returns:
            str: 任务ID
        """
        timestamp = int(time.time() * 1000)
        node_id = task_config.get("node_id", "unknown")
        return f"{self.get_service_type()}_{node_id}_{timestamp}"
    
    def validate_task_config(self, task_config: Dict[str, Any]) -> bool:
        """
        验证任务配置
        子类可以重写这个方法来实现特定的验证逻辑
        
        Args:
            task_config: 任务配置
            
        Returns:
            bool: 配置是否有效
        """
        required_fields = ["node_id", "event_type"]
        for field in required_fields:
            if field not in task_config:
                logger.error(f"任务配置缺少必要字段: {field}")
                return False
        return True
    
    def _handle_task_error(self, task_config: Dict[str, Any], error: Exception):
        """
        处理任务错误
        
        Args:
            task_config: 任务配置
            error: 错误对象
        """
        try:
            # 构造错误事件数据
            error_event_data = {
                "node_id": task_config.get("node_id"),
                "execution_id": task_config.get("execution_id"),
                "flow_id": task_config.get("flow_id"),
                "error_type": "service_error",
                "error_message": str(error),
                "service_type": self.get_service_type(),
                "timestamp": int(time.time() * 1000)
            }
            
            self.event_bus.publish_sync("service_error", **error_event_data)
            
        except Exception as e:
            logger.error("处理任务错误时出错: %s", e, exc_info=True)
    
    async def trigger_event(self, event_type: str, event_data: Dict[str, Any]):
        """
        触发事件
        
        Args:
            event_type: 事件类型
            event_data: 事件数据
        """
        try:
            # 创建Event对象
            event = Event(event_type=event_type, data=event_data)
            
            if asyncio.iscoroutinefunction(self.event_bus.publish):
                await self.event_bus.publish(event)
            else:
                self.event_bus.publish(event)
            logger.info(f"事件总线: {self.event_bus}")
            logger.info(f"事件已触发: {event_type}, 数据: {event_data}")
            
        except Exception as e:
            logger.error("触发事件失败: %s", e, exc_info=True)
    
    def get_active_task_count(self) -> int:
        """
        获取活跃任务数量
        
        Returns:
            int: 活跃任务数量
        """
        return len(self.active_tasks)
    
    def is_task_active(self, task_id: str) -> bool:
        """
        检查任务是否活跃
        
        Args:
            task_id: 任务ID
            
        Returns:
            bool: 任务是否活跃
        """
        return task_id in self.active_tasks
    
    def shutdown(self, timeout: Optional[float] = None):
        """
        关闭服务
        
        Args:
            timeout: 超时时间（秒）
        """
        logger.info(f"开始关闭 {self.get_service_type()} 服务...")
        
        # 设置关闭标志
        self._shutdown_event.set()
        
        # 停止服务
        self.stop_service()
        
        # 停止控制监听
        if self._enable_registry:
            self.stop_control_listener()
        
        # 注销服务
        if self._enable_registry:
            self.unregister_service()
        
        # 等待所有任务完成
        try:
            self.thread_pool.shutdown(wait=True)
        except Exception as e:
            logger.warning(f"关闭线程池时出错: {e}")
        
        logger.info(f"{self.get_service_type()} 服务已关闭")

    def start_with_registry(self) -> bool:
        """
        启动服务并注册到服务中心
        
        Returns:
            bool: 启动是否成功
        """
        # 注册服务
        if self._enable_registry:
            if not self.register_service():
                logger.warning("服务注册失败，但服务仍将启动")
            # 启动控制监听
            self.start_control_listener()
        
        # 启动服务
        return self.start_service()
    
    def _on_stop_command(self, graceful: bool):
        """响应停止命令"""
        logger.info(f"收到远程停止命令，优雅停止: {graceful}")
        self.shutdown()
    
    def _on_status_command(self) -> Dict[str, Any]:
        """响应状态查询命令"""
        return {
            "status": "running" if self.is_running else "stopped",
            "active_tasks": len(self.active_tasks),
            "service_type": self.get_service_type()
        }

    def is_shutdown_requested(self) -> bool:
        """
        检查是否请求了关闭
        
        Returns:
            bool: 是否请求了关闭
        """
        return self._shutdown_event.is_set()