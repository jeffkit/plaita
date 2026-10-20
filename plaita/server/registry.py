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
from plaita.core.url_redaction import _scrub_exception_text


#: 注册表合法生命周期状态（S18-02A）。``closed`` 是**进程内终态**，不落键。
STATUS_STARTING = "starting"
STATUS_RUNNING = "running"
STATUS_DRAINING = "draining"
STATUS_STOPPING = "stopping"
STATUS_STOPPED = "stopped"
STATUS_CLOSED = "closed"

VALID_STATUSES = frozenset({
    STATUS_STARTING,
    STATUS_RUNNING,
    STATUS_DRAINING,
    STATUS_STOPPING,
    STATUS_STOPPED,
    STATUS_CLOSED,
})

#: 唯一可派发状态——派发门（``capacity.DispatchCandidate``）只认这一个。
DISPATCHABLE_STATUS = STATUS_RUNNING


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
            status: 状态 (starting, running, draining, stopping, stopped)
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
            
            # 状态机（S18-02A）：只有 starting/running 可迁移到 running。
            # - draining/stopping：启动期 SIGTERM 可能早于 run()，register 必须
            #   保留 drain 状态，否则会把「正在停机」的实例复活成可派发（S9）。
            # - stopped/closed：终态，注册不得复活（返回 False，不写键）。
            # - 未知状态：保留原值（仍不可派发），不猜测成 running。
            status = service_info.status
            if status in (STATUS_STOPPED, STATUS_CLOSED):
                logger.warning(
                    "拒绝注册终态服务: %s:%s status=%s",
                    service_info.service_type,
                    service_info.instance_id,
                    status,
                )
                return False
            if status in (STATUS_STARTING, STATUS_RUNNING, "", None):
                service_info.status = STATUS_RUNNING
            elif status in (STATUS_DRAINING, STATUS_STOPPING):
                logger.info(
                    "注册保留服务生命周期状态 status=%s: %s:%s",
                    status,
                    service_info.service_type,
                    service_info.instance_id,
                )
            else:
                logger.warning(
                    "注册遇到未知状态 status=%r：保留原值（不可派发）: %s:%s",
                    status,
                    service_info.service_type,
                    service_info.instance_id,
                )
            service_info.last_heartbeat = datetime.now().isoformat()
            
            # 写入 Redis，设置 TTL
            self.redis_client.setex(
                key,
                self.ttl,
                service_info.to_json()
            )
            
            logger.info(
                "服务已注册: %s:%s",
                service_info.service_type,
                service_info.instance_id
            )
            
            # 保存当前注册的服务信息
            self._registered_service = service_info
            
            return True
            
        except Exception as e:
            logger.error("服务注册失败: %s", _scrub_exception_text(e))
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
        key = self._get_registry_key(service_type, instance_id)

        # 注销原子化/可观测（S18-02A / A46-01 S5/S6）：
        #   1) 读现有条目拿完整快照；**读失败仍继续**（旧实现直接跳出，
        #      连 delete 都不做，留下 running 残影）；
        #   2) setex(stopped, 5) 写墓碑——delete 失败时键仍是 stopped，
        #      派发门不会把注销中的实例当 running；
        #   3) delete；任一步失败都 ERROR + 返回 False（调用方据此可观测）。
        raw = None
        read_ok = True
        try:
            raw = self.redis_client.get(key)
        except Exception as e:
            read_ok = False
            logger.error(
                "服务注销读注册信息失败（仍继续写墓碑/删除）: %s:%s: %s",
                service_type,
                instance_id,
                _scrub_exception_text(e),
            )

        service_info = None
        if raw:
            try:
                service_info = ServiceInfo.from_json(raw)
            except Exception as e:
                logger.warning(
                    "服务注销时注册信息非 JSON（写最小 stopped 墓碑）: %s:%s: %s",
                    service_type,
                    instance_id,
                    _scrub_exception_text(e),
                )
        if service_info is None:
            service_info = ServiceInfo(
                instance_id=instance_id,
                service_type=service_type,
                host="",
                status=STATUS_STOPPED,
            )
        service_info.status = STATUS_STOPPED
        service_info.last_heartbeat = datetime.now().isoformat()

        tombstone_ok = False
        try:
            self.redis_client.setex(key, 5, service_info.to_json())  # 5秒后过期
            tombstone_ok = True
        except Exception as e:
            logger.error(
                "服务注销写 stopped 墓碑失败: %s:%s: %s",
                service_type,
                instance_id,
                _scrub_exception_text(e),
            )

        delete_ok = False
        try:
            self.redis_client.delete(key)
            delete_ok = True
        except Exception as e:
            logger.error(
                "服务注销删除注册键失败（墓碑 stopped 保留至 TTL）: %s:%s: %s",
                service_type,
                instance_id,
                _scrub_exception_text(e),
            )

        # 注销是终态：无论两步是否完全成功，都不再允许这个对象继续心跳/更新。
        self._registered_service = None

        if read_ok and tombstone_ok and delete_ok:
            logger.info("服务已注销: %s:%s", service_type, instance_id)
            return True

        logger.error(
            "服务注销未完全成功: %s:%s read_ok=%s tombstone_ok=%s delete_ok=%s",
            service_type,
            instance_id,
            read_ok,
            tombstone_ok,
            delete_ok,
        )
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
                "心跳成功: %s:%s",
                service_info.service_type,
                service_info.instance_id
            )
            
            return True
            
        except Exception as e:
            logger.error("心跳失败: %s", _scrub_exception_text(e))
            return False
    
    def start_heartbeat(self, service_info: ServiceInfo, should_write=None):
        """
        启动心跳线程
        
        Args:
            service_info: 服务信息
            should_write: 可选回调；返回 False 时本轮跳过写入。用于注销后的
                closed 世代闸（迟到的最后一次心跳不得把键写回）。
        """
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            logger.warning("心跳线程已在运行")
            return
        
        self._heartbeat_stop_event.clear()
        self._registered_service = service_info
        
        def heartbeat_loop():
            while not self._heartbeat_stop_event.is_set():
                if self._registered_service and (
                    should_write is None or should_write()
                ):
                    self.heartbeat(self._registered_service)
                self._heartbeat_stop_event.wait(self.heartbeat_interval)
        
        self._heartbeat_thread = threading.Thread(
            target=heartbeat_loop,
            daemon=True,
            name=f"heartbeat-{service_info.instance_id}"
        )
        self._heartbeat_thread.start()
        
        logger.info(
            "心跳线程已启动: %s:%s",
            service_info.service_type,
            service_info.instance_id
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
            logger.error("获取服务信息失败: %s", _scrub_exception_text(e))
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
            logger.error("列出服务失败: %s", _scrub_exception_text(e))
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
            key_prefix = f"{self.REGISTRY_PREFIX}:"
            for key in keys:
                # 解析 key: plaita:registry:{service_type}:{instance_id}
                # #110/F3（同一 split(':') 家族，验收者补的点位）：按**已知前缀**
                # 剥离，再从右侧去掉唯一的 {instance_id}。此前 `parts[2]` 在
                # service_type 自身含 ':' 时被截断（`a:b` → `a`）。当前所有
                # service_type 都是无冒号常量（flow_worker/…_service），故生产
                # 未暴露；这里是同一截断家族的顺手收口。
                key_str = key.decode() if isinstance(key, bytes) else key
                if not key_str.startswith(key_prefix):
                    continue
                rest = key_str[len(key_prefix):]
                service_type = rest.rsplit(":", 1)[0] if ":" in rest else rest
                if service_type:
                    service_types.add(service_type)
            
            return list(service_types)
            
        except Exception as e:
            logger.error("获取服务类型失败: %s", _scrub_exception_text(e))
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
        heartbeat_interval: int = ServiceRegistry.DEFAULT_HEARTBEAT_INTERVAL,
        instance_id: Optional[str] = None,
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
        # 注销世代闸（S18-02A / A46-03）：注销成功后所有写入 no-op。
        # 惰性 helper 在 ``_registry_is_closed`` / ``_mark_registry_closed``，
        # 骨架 worker（不走 init_registry 的 __new__ 单测）同样安全。
        self._registry_closed = threading.Event()
        
        self._service_info = ServiceInfo(
            # B2R31-02：显式实例 id（service_manager 注入的 PLAITA_INSTANCE_ID）
            # 与注册键必须同值；未注入才生成 hostname-uuid。
            instance_id=instance_id or ServiceRegistry.generate_instance_id(),
            service_type=service_type,
            host=ServiceRegistry.get_host_address(),
            status="starting",
            metadata=metadata or {}
        )

    # ---------------------------------------------------------- closed 世代闸

    def _registry_is_closed(self) -> bool:
        """注销后写入闸：True = 本对象已注销，不得再写注册表。"""
        event = getattr(self, "_registry_closed", None)
        return bool(event is not None and event.is_set())

    def _mark_registry_closed(self) -> None:
        """置 closed（幂等）。注销是终态，之后 register/heartbeat/update 全部 no-op。"""
        event = getattr(self, "_registry_closed", None)
        if event is None:
            event = threading.Event()
            self._registry_closed = event
        event.set()

    def _registry_heartbeat_allowed(self) -> bool:
        """心跳线程每轮写入前的 closed 检查（迟到的最后一次写入也拦）。"""
        return not self._registry_is_closed()

    def flush_registry_status(
        self, retries: int = 3, backoff: float = 0.1
    ) -> bool:
        """**立即**把当前 ``ServiceInfo``（含 status/active/capacity）落盘。

        ``request_drain`` / ``stop`` 不再等 10s 心跳：内存置位后直接写一次。
        Redis 瞬断时短退避重试（默认 3 次，0.1s/0.2s），仍失败返回 False
        （调用方负责 ERROR + 指标，不得静默）。
        """
        if self._registry_is_closed():
            return True  # 已注销：无键可写，不算失败
        registry = getattr(self, "_registry", None)
        service_info = getattr(self, "_service_info", None)
        if registry is None or service_info is None:
            return False
        attempts = max(1, int(retries))
        for attempt in range(1, attempts + 1):
            if registry.heartbeat(service_info):
                return True
            if attempt < attempts:
                time.sleep(max(0.0, float(backoff)) * attempt)
        return False
    
    def register_service(self) -> bool:
        """注册服务并启动心跳。"""
        if self._registry_is_closed():
            logger.warning("服务注册已关闭（注销后不得复活），忽略本次注册")
            return False
        if not self._registry or not self._service_info:
            logger.warning("服务注册未初始化")
            return False
        
        success = self._registry.register(self._service_info)
        if success:
            self._registry.start_heartbeat(
                self._service_info,
                should_write=self._registry_heartbeat_allowed,
            )
        
        return success
    
    def unregister_service(self) -> bool:
        """注销服务并停止心跳（幂等；入口先置 closed 封住迟到写入）。"""
        if not self._registry or not self._service_info:
            return False
        
        self._mark_registry_closed()
        self._registry.stop_heartbeat()
        
        return self._registry.unregister(
            self._service_info.service_type,
            self._service_info.instance_id
        )
    
    def update_registry_info(
        self,
        active_tasks: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        status: Optional[str] = None
    ):
        """更新注册信息（status 用于 draining 等生命周期状态，None = 不改）。

        直接改 ``_service_info``（单一状态源）——``register()`` 之前调用的
        ``request_drain`` 也要能在内存中生效，否则 ``update_service_info``
        在 ``_registered_service is None`` 时直接 return（S9 前置坑）。
        注销后（closed）任何更新都是 no-op。
        """
        if self._registry_is_closed():
            logger.debug("注册表已关闭，忽略状态更新 status=%s", status)
            return
        service_info = getattr(self, "_service_info", None)
        if service_info is not None:
            if active_tasks is not None:
                service_info.active_tasks = active_tasks
            if metadata:
                service_info.metadata.update(metadata)
            if status:
                service_info.status = status
        if self._registry:
            self._registry.update_service_info(
                active_tasks=active_tasks,
                metadata=metadata,
                status=status,
            )
    
    @property
    def instance_id(self) -> Optional[str]:
        """获取实例 ID"""
        return self._service_info.instance_id if self._service_info else None

