"""
服务注册模块
为 FlowWorker 和扩展服务提供 Redis 注册机制
"""
import json
import socket
import threading
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

from redis import Redis

from ..logger import logger


class ServiceInfo:
    """服务信息数据类"""
    
    def __init__(
        self,
        instance_id: str,
        service_type: str,
        host: str,
        status: str = "starting",
        start_time: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        active_tasks: int = 0,
        last_heartbeat: Optional[str] = None
    ):
        """
        初始化服务信息
        
        Args:
            instance_id: 实例唯一标识
            service_type: 服务类型
            host: 主机地址
            status: 状态 (starting, running, stopping, stopped)
            start_time: 启动时间
            metadata: 配置信息
            active_tasks: 当前处理的任务数
            last_heartbeat: 最后心跳时间
        """
        self.instance_id = instance_id
        self.service_type = service_type
        self.host = host
        self.status = status
        self.start_time = start_time or datetime.now().isoformat()
        self.metadata = metadata or {}
        self.active_tasks = active_tasks
        self.last_heartbeat = last_heartbeat or datetime.now().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "instance_id": self.instance_id,
            "service_type": self.service_type,
            "host": self.host,
            "status": self.status,
            "start_time": self.start_time,
            "metadata": self.metadata,
            "active_tasks": self.active_tasks,
            "last_heartbeat": self.last_heartbeat
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ServiceInfo":
        """从字典创建实例"""
        return cls(
            instance_id=data.get("instance_id", ""),
            service_type=data.get("service_type", ""),
            host=data.get("host", ""),
            status=data.get("status", "unknown"),
            start_time=data.get("start_time"),
            metadata=data.get("metadata", {}),
            active_tasks=data.get("active_tasks", 0),
            last_heartbeat=data.get("last_heartbeat")
        )
    
    def to_json(self) -> str:
        """转换为 JSON 字符串"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> "ServiceInfo":
        """从 JSON 字符串创建实例"""
        data = json.loads(json_str)
        return cls.from_dict(data)


class ServiceRegistry:
    """
    服务注册中心
    使用 Redis 作为注册中心存储
    """
    
    # 注册 key 前缀
    REGISTRY_PREFIX = "plaita:registry"
    # 默认 TTL（秒）
    DEFAULT_TTL = 30
    # 默认心跳间隔（秒）
    DEFAULT_HEARTBEAT_INTERVAL = 10
    
    def __init__(
        self,
        redis_client: Redis,
        ttl: int = DEFAULT_TTL,
        heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL
    ):
        """
        初始化服务注册中心
        
        Args:
            redis_client: Redis 客户端实例
            ttl: 注册信息过期时间（秒）
            heartbeat_interval: 心跳间隔（秒）
        """
        self.redis_client = redis_client
        self.ttl = ttl
        self.heartbeat_interval = heartbeat_interval
        
        # 心跳线程管理
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop_event = threading.Event()
        self._registered_service: Optional[ServiceInfo] = None
    
    def _get_registry_key(self, service_type: str, instance_id: str) -> str:
        """
        生成注册 key
        
        格式: plaita:registry:{service_type}:{instance_id}
        """
        return f"{self.REGISTRY_PREFIX}:{service_type}:{instance_id}"
    
    def _get_registry_pattern(self, service_type: Optional[str] = None) -> str:
        """
        生成注册 key 匹配模式
        
        Args:
            service_type: 服务类型，如果为 None 则匹配所有
        """
        if service_type:
            return f"{self.REGISTRY_PREFIX}:{service_type}:*"
        return f"{self.REGISTRY_PREFIX}:*"
    
    @staticmethod
    def generate_instance_id() -> str:
        """生成唯一的实例 ID"""
        hostname = socket.gethostname()
        unique_id = str(uuid.uuid4())[:8]
        return f"{hostname}-{unique_id}"
    
    @staticmethod
    def get_host_address() -> str:
        """获取主机地址"""
        try:
            hostname = socket.gethostname()
            ip_address = socket.gethostbyname(hostname)
            return f"{hostname}/{ip_address}"
        except Exception:
            return socket.gethostname()
    
    def register(self, service_info: ServiceInfo) -> bool:
        """
        注册服务
        
        Args:
            service_info: 服务信息
            
        Returns:
            bool: 注册是否成功
        """
        try:
            key = self._get_registry_key(
                service_info.service_type, 
                service_info.instance_id
            )
            
            # 更新状态和心跳时间
            service_info.status = "running"
            service_info.last_heartbeat = datetime.now().isoformat()
            
            # 写入 Redis，设置 TTL
            self.redis_client.setex(
                key,
                self.ttl,
                service_info.to_json()
            )
            
            logger.info(
                f"服务已注册: {service_info.service_type}:{service_info.instance_id}"
            )
            
            # 保存当前注册的服务信息
            self._registered_service = service_info
            
            return True
            
        except Exception as e:
            logger.error("服务注册失败: %s", e, exc_info=True)
            return False
    
    def unregister(self, service_type: str, instance_id: str) -> bool:
        """
        注销服务
        
        Args:
            service_type: 服务类型
            instance_id: 实例 ID
            
        Returns:
            bool: 注销是否成功
        """
        try:
            key = self._get_registry_key(service_type, instance_id)
            
            # 先更新状态为 stopping
            service_data = self.redis_client.get(key)
            if service_data:
                service_info = ServiceInfo.from_json(service_data)
                service_info.status = "stopped"
                self.redis_client.setex(key, 5, service_info.to_json())  # 5秒后过期
            
            # 删除注册信息
            self.redis_client.delete(key)
            
            logger.info(f"服务已注销: {service_type}:{instance_id}")
            
            self._registered_service = None
            
            return True
            
        except Exception as e:
            logger.error("服务注销失败: %s", e, exc_info=True)
            return False
    
    def heartbeat(self, service_info: ServiceInfo) -> bool:
        """
        发送心跳，刷新 TTL
        
        Args:
            service_info: 服务信息（包含最新状态）
            
        Returns:
            bool: 心跳是否成功
        """
        try:
            key = self._get_registry_key(
                service_info.service_type, 
                service_info.instance_id
            )
            
            # 更新心跳时间
            service_info.last_heartbeat = datetime.now().isoformat()
            
            # 刷新 TTL
            self.redis_client.setex(
                key,
                self.ttl,
                service_info.to_json()
            )
            
            logger.debug(
                f"心跳成功: {service_info.service_type}:{service_info.instance_id}"
            )
            
            return True
            
        except Exception as e:
            logger.error("心跳失败: %s", e, exc_info=True)
            return False
    
    def start_heartbeat(self, service_info: ServiceInfo):
        """
        启动心跳线程
        
        Args:
            service_info: 服务信息
        """
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            logger.warning("心跳线程已在运行")
            return
        
        self._heartbeat_stop_event.clear()
        self._registered_service = service_info
        
        def heartbeat_loop():
            while not self._heartbeat_stop_event.is_set():
                if self._registered_service:
                    self.heartbeat(self._registered_service)
                self._heartbeat_stop_event.wait(self.heartbeat_interval)
        
        self._heartbeat_thread = threading.Thread(
            target=heartbeat_loop,
            daemon=True,
            name=f"heartbeat-{service_info.instance_id}"
        )
        self._heartbeat_thread.start()
        
        logger.info(
            f"心跳线程已启动: {service_info.service_type}:{service_info.instance_id}"
        )
    
    def stop_heartbeat(self):
        """停止心跳线程"""
        self._heartbeat_stop_event.set()
        
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=5)
            self._heartbeat_thread = None
        
        logger.info("心跳线程已停止")
    
    def update_service_info(
        self, 
        active_tasks: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        status: Optional[str] = None
    ):
        """
        更新已注册服务的信息
        
        Args:
            active_tasks: 活跃任务数
            metadata: 元数据更新
            status: 状态更新
        """
        if not self._registered_service:
            return
        
        if active_tasks is not None:
            self._registered_service.active_tasks = active_tasks
        
        if metadata:
            self._registered_service.metadata.update(metadata)
        
        if status:
            self._registered_service.status = status
    
    def get_service(
        self, 
        service_type: str, 
        instance_id: str
    ) -> Optional[ServiceInfo]:
        """
        获取指定服务信息
        
        Args:
            service_type: 服务类型
            instance_id: 实例 ID
            
        Returns:
            ServiceInfo 或 None
        """
        try:
            key = self._get_registry_key(service_type, instance_id)
            data = self.redis_client.get(key)
            
            if data:
                return ServiceInfo.from_json(data)
            
            return None
            
        except Exception as e:
            logger.error("获取服务信息失败: %s", e, exc_info=True)
            return None
    
    def list_services(
        self, 
        service_type: Optional[str] = None
    ) -> List[ServiceInfo]:
        """
        列出所有服务
        
        Args:
            service_type: 按服务类型筛选，None 表示所有
            
        Returns:
            服务信息列表
        """
        try:
            pattern = self._get_registry_pattern(service_type)
            keys = self.redis_client.keys(pattern)
            
            services = []
            for key in keys:
                data = self.redis_client.get(key)
                if data:
                    services.append(ServiceInfo.from_json(data))
            
            return services
            
        except Exception as e:
            logger.error("列出服务失败: %s", e, exc_info=True)
            return []
    
    def get_service_types(self) -> List[str]:
        """
        获取所有服务类型
        
        Returns:
            服务类型列表
        """
        try:
            pattern = self._get_registry_pattern()
            keys = self.redis_client.keys(pattern)
            
            service_types = set()
            for key in keys:
                # 解析 key: plaita:registry:{service_type}:{instance_id}
                parts = key.decode() if isinstance(key, bytes) else key
                parts = parts.split(":")
                if len(parts) >= 3:
                    service_types.add(parts[2])
            
            return list(service_types)
            
        except Exception as e:
            logger.error("获取服务类型失败: %s", e, exc_info=True)
            return []


class RegistryMixin:
    """
    服务注册混入类
    为服务类提供注册功能
    """
    
    # 子类需要实现的属性
    _registry: Optional[ServiceRegistry] = None
    _service_info: Optional[ServiceInfo] = None
    
    def init_registry(
        self,
        redis_client: Redis,
        service_type: str,
        metadata: Optional[Dict[str, Any]] = None,
        ttl: int = ServiceRegistry.DEFAULT_TTL,
        heartbeat_interval: int = ServiceRegistry.DEFAULT_HEARTBEAT_INTERVAL
    ):
        """
        初始化服务注册
        
        Args:
            redis_client: Redis 客户端
            service_type: 服务类型
            metadata: 服务元数据
            ttl: TTL（秒）
            heartbeat_interval: 心跳间隔（秒）
        """
        self._registry = ServiceRegistry(
            redis_client=redis_client,
            ttl=ttl,
            heartbeat_interval=heartbeat_interval
        )
        
        self._service_info = ServiceInfo(
            instance_id=ServiceRegistry.generate_instance_id(),
            service_type=service_type,
            host=ServiceRegistry.get_host_address(),
            status="starting",
            metadata=metadata or {}
        )
    
    def register_service(self) -> bool:
        """注册服务并启动心跳"""
        if not self._registry or not self._service_info:
            logger.warning("服务注册未初始化")
            return False
        
        success = self._registry.register(self._service_info)
        if success:
            self._registry.start_heartbeat(self._service_info)
        
        return success
    
    def unregister_service(self) -> bool:
        """注销服务并停止心跳"""
        if not self._registry or not self._service_info:
            return False
        
        self._registry.stop_heartbeat()
        
        return self._registry.unregister(
            self._service_info.service_type,
            self._service_info.instance_id
        )
    
    def update_registry_info(
        self,
        active_tasks: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """更新注册信息"""
        if self._registry:
            self._registry.update_service_info(
                active_tasks=active_tasks,
                metadata=metadata
            )
    
    @property
    def instance_id(self) -> Optional[str]:
        """获取实例 ID"""
        return self._service_info.instance_id if self._service_info else None

