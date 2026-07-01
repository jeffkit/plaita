"""
服务管理器
负责管理所有外延服务的生命周期和任务分发
"""
import threading
from typing import Any, Dict, List, Optional, Type

from .base_service import BaseExtendedService
from .delay_service import DelayService
from .redis_queue_service import RedisQueueService
from .kafka_queue_service import KafkaQueueService
from .http_callback_service import HttpCallbackService
from .approval_service import ApprovalService
from ...logger import logger
from ...event.core import EventBus


class ServiceManager:
    """
    服务管理器
    负责管理所有外延服务的生命周期和任务分发
    """
    
    def __init__(self, event_bus: EventBus):
        """
        初始化服务管理器
        
        Args:
            event_bus: 事件总线实例
        """
        self.event_bus = event_bus
        self.services: Dict[str, BaseExtendedService] = {}
        self.service_classes: Dict[str, Type[BaseExtendedService]] = {
            "delay": DelayService,
            "redis_queue": RedisQueueService,
            "kafka_queue": KafkaQueueService,
            "http_callback": HttpCallbackService,
            "approval": ApprovalService,
        }
        self._lock = threading.RLock()
        self.is_running = False
        
    def register_service_class(self, service_type: str, service_class: Type[BaseExtendedService]):
        """
        注册服务类
        
        Args:
            service_type: 服务类型
            service_class: 服务类
        """
        with self._lock:
            self.service_classes[service_type] = service_class
            logger.info(f"注册服务类: {service_type} -> {service_class.__name__}")
    
    def start_all_services(self, service_configs: Optional[Dict[str, Dict[str, Any]]] = None) -> bool:
        """
        启动所有服务
        
        Args:
            service_configs: 服务配置字典
            
        Returns:
            bool: 是否全部启动成功
        """
        with self._lock:
            if self.is_running:
                logger.warning("服务管理器已在运行中")
                return True
            
            service_configs = service_configs or {}
            success_count = 0
            
            for service_type, service_class in self.service_classes.items():
                try:
                    # 获取服务配置
                    config = service_configs.get(service_type, {})
                    
                    # 创建服务实例
                    service = service_class(self.event_bus, config)
                    
                    # 启动服务
                    if service.start_service():
                        self.services[service_type] = service
                        success_count += 1
                        logger.info(f"服务 {service_type} 启动成功")
                    else:
                        logger.error(f"服务 {service_type} 启动失败")
                        
                except Exception as e:
                    logger.error(f"启动服务 {service_type} 时出错: {e}", exc_info=True)
            
            self.is_running = success_count > 0
            logger.info(f"服务管理器启动完成，成功启动 {success_count}/{len(self.service_classes)} 个服务")
            
            return success_count == len(self.service_classes)
    
    def stop_all_services(self, timeout: Optional[float] = None) -> bool:
        """
        停止所有服务
        
        Args:
            timeout: 超时时间（秒）
            
        Returns:
            bool: 是否全部停止成功
        """
        with self._lock:
            if not self.is_running:
                logger.warning("服务管理器未在运行中")
                return True
            
            success_count = 0
            
            for service_type, service in self.services.items():
                try:
                    # 关闭服务
                    service.shutdown(timeout)
                    success_count += 1
                    logger.info(f"服务 {service_type} 停止成功")
                    
                except Exception as e:
                    logger.error(f"停止服务 {service_type} 时出错: {e}", exc_info=True)
            
            # 清空服务字典
            self.services.clear()
            self.is_running = False
            
            logger.info(f"服务管理器停止完成，成功停止 {success_count} 个服务")
            
            return success_count > 0
    
    def submit_task(self, service_type: str, task_config: Dict[str, Any]) -> str:
        """
        提交任务到指定服务
        
        Args:
            service_type: 服务类型
            task_config: 任务配置
            
        Returns:
            str: 任务ID，空字符串表示提交失败
        """
        with self._lock:
            if service_type not in self.services:
                logger.error(f"服务类型 {service_type} 不存在或未启动")
                return ""
            
            service = self.services[service_type]
            return service.submit_task(task_config)
    
    def get_service(self, service_type: str) -> Optional[BaseExtendedService]:
        """
        获取指定类型的服务
        
        Args:
            service_type: 服务类型
            
        Returns:
            Optional[BaseExtendedService]: 服务实例
        """
        with self._lock:
            return self.services.get(service_type)
    
    def get_all_services_status(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有服务状态
        
        Returns:
            Dict[str, Dict[str, Any]]: 服务状态字典
        """
        with self._lock:
            status = {}
            
            for service_type, service in self.services.items():
                try:
                    status[service_type] = {
                        "is_running": service.is_running,
                        "active_task_count": service.get_active_task_count(),
                        "max_workers": service.get_max_workers(),
                        "service_class": service.__class__.__name__
                    }
                except Exception as e:
                    status[service_type] = {
                        "error": str(e)
                    }
            
            return status
    
    def restart_service(self, service_type: str, service_config: Optional[Dict[str, Any]] = None) -> bool:
        """
        重启指定服务
        
        Args:
            service_type: 服务类型
            service_config: 服务配置
            
        Returns:
            bool: 重启是否成功
        """
        with self._lock:
            try:
                # 停止现有服务
                if service_type in self.services:
                    old_service = self.services[service_type]
                    old_service.shutdown()
                    del self.services[service_type]
                
                # 启动新服务
                if service_type not in self.service_classes:
                    logger.error(f"未知的服务类型: {service_type}")
                    return False
                
                service_class = self.service_classes[service_type]
                config = service_config or {}
                
                new_service = service_class(self.event_bus, config)
                
                if new_service.start_service():
                    self.services[service_type] = new_service
                    logger.info(f"服务 {service_type} 重启成功")
                    return True
                else:
                    logger.error(f"服务 {service_type} 重启失败")
                    return False
                    
            except Exception as e:
                logger.error(f"重启服务 {service_type} 时出错: {e}", exc_info=True)
                return False
    
    def handle_node_config(self, node_config: Dict[str, Any]) -> str:
        """
        根据节点配置处理任务
        
        Args:
            node_config: 节点配置（来自扩展节点的service_config）
            
        Returns:
            str: 任务ID，空字符串表示处理失败
        """
        service_type = node_config.get("type")
        if not service_type:
            logger.error("节点配置中缺少服务类型")
            return ""
        
        return self.submit_task(service_type, node_config)
    
    def get_available_service_types(self) -> List[str]:
        """
        获取可用的服务类型列表
        
        Returns:
            List[str]: 服务类型列表
        """
        with self._lock:
            return list(self.service_classes.keys())
    
    def is_service_running(self, service_type: str) -> bool:
        """
        检查指定服务是否在运行
        
        Args:
            service_type: 服务类型
            
        Returns:
            bool: 服务是否在运行
        """
        with self._lock:
            service = self.services.get(service_type)
            return service is not None and service.is_running 