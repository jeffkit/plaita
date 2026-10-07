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

# resume 事件尽力落盘的键前缀与 TTL（Track P2 任务2）：与 RedisEventStorage
# （event/redis.py，key_prefix 默认 "plaita:event:"、DEFAULT_TTL 7 天）键格式
# 耦合——EventReconciler 靠同一格式扫描补偿；若那边改前缀/键形状，这里必须
# 同步，否则服务直发的 resume 事件回扫不到。
RESUME_EVENT_STORAGE_PREFIX = "plaita:event:"
RESUME_EVENT_TTL_SECONDS = 7 * 86400


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
            logger.error("任务配置验证失败: %s", task_config)
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
        
        logger.info("任务 %s 已提交到 %s 服务", task_id, self.get_service_type())
        
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
            
            logger.info("异步任务 %s 执行完成: %s", task_id, result)
            
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
            except Exception:
                logger.warning("event loop close failed during task cleanup", exc_info=True)
    
    def _run_sync_task(self, task_config: Dict[str, Any], task_id: str):
        """
        运行同步任务
        
        Args:
            task_config: 任务配置
            task_id: 任务ID
        """
        try:
            result = self.handle_task(task_config)
            logger.info("同步任务 %s 执行完成: %s", task_id, result)
            
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
                logger.error("任务配置缺少必要字段: %s", field)
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
            logger.info("事件总线: %s", self.event_bus)
            logger.info("事件已触发: %s, 数据: %s", event_type, event_data)

        except Exception as e:
            logger.error("触发事件失败: %s", e, exc_info=True)

    def _persist_resume_event_best_effort(self, event: Event) -> None:
        """尽力把直发频道的 resume 事件写进事件存储（fail-open）。

        publish_resume_event 用同步 redis 直发 plaita:events:{type} 频道，
        不经 RedisEventBus.publish → 事件不落存储 → EventReconciler（扫事件
        存储 zset 索引做兜底）补偿不到，Pub/Sub 丢通知窗口内 resume 照样丢。
        这里按 RedisEventStorage.store_event 的键格式（plaita:event:events:
        {id} SET + plaita:event:types:{type} ZADD，score=事件时间戳，TTL
        7 天）同步直写。

        选同步直写而非复用 store_event：其一，handle_task 运行在线程池新开
        的 loop 里，asyncio.run 包 async store_event 会嵌套运行中 loop 直接
        RuntimeError；其二，store_event 内部 initialize() 会把共享
        RedisEventBus 的 aioredis 客户端重绑到本任务的临时 loop（从服务线程
        改总线共享状态，与引擎 loop 并发使用相互拆台）。同步客户端本就在
        手边（resume 直发用的就是它），管道三命令写完即走。

        任何失败只 warning——落盘是回扫兜底链路，绝不阻塞 resume 主链路
        （与 EventReconciler 扫描失败 fail-open 同风格）。
        """
        if self._redis_client is None:
            return
        try:
            event_key = f"{RESUME_EVENT_STORAGE_PREFIX}events:{event.event_id}"
            type_key = f"{RESUME_EVENT_STORAGE_PREFIX}types:{event.event_type}"
            pipe = self._redis_client.pipeline()
            pipe.set(event_key, event.model_dump_json(), ex=RESUME_EVENT_TTL_SECONDS)
            pipe.zadd(type_key, {event.event_id: event.timestamp})
            pipe.expire(type_key, RESUME_EVENT_TTL_SECONDS)
            pipe.execute()
        except Exception as e:  # noqa: BLE001 — 兜底链路失败不外溢
            logger.warning(
                "resume 事件落盘失败（不影响直发主链路，回扫补偿缺失窗口）: %s",
                e, exc_info=True,
            )

    async def publish_resume_event(self, event_type: str, event_data: Dict[str, Any]):
        """触发 resume 链路事件：带 correlation_id（=execution_id）。

        历史缺陷（2026-10 分布式可靠性修复）：trigger_event 构造的 Event 不带
        correlation_id，而 EventFilter.handle_event 开头即丢弃无 correlation_id
        的事件——审批完成 / HTTP 回调触发的事件永远到不了挂起执行的 resume，
        恢复链路是断的。DelayService 已先行修复（其 trigger_event override，
        手法与本方法一致）；approval / http_callback 的同名 override 统一收敛
        到这里复用。

        发布通道与 DelayService 同款：
        - 有 Redis 客户端时，直接用同步 redis 客户端发布到引擎 RedisEventBus
          的频道（plaita:events:{type}）。不要走 self.event_bus.publish——
          它的 aioredis 连接绑定在创建时的 event loop 上，而 handle_task
          运行在线程池新开的 loop 里，跨 loop 使用会静默失败。
        - 无 Redis 客户端（进程内 InMemoryEventBus 场景，如 examples/server_demo）
          时回退到 self.event_bus.publish。

        直发不经 RedisEventBus.publish、不落事件存储，故直发前按
        RedisEventStorage 键格式尽力落盘（_persist_resume_event_best_effort，
        fail-open）——EventReconciler 的回扫兜底才能覆盖 Pub/Sub 丢通知窗口。

        发布失败**上抛**（不再自吞）：这是「挂起执行最后一跳」的唤醒凭据，
        吞掉会让调用方以为成功。调用方据此决定处置——DelayService 失败时
        不 ZREM、留排程 ZSET 下轮重试（见 delay_service._run_scheduled_task）；
        审批/回调把它转成错误响应而非静默成功。落盘仍是 fail-open（仅 warning），
        它只是补偿链路，不改变主链路的成败判定。
        """
        event = Event(
            event_type=event_type,
            data=event_data,
            correlation_id=event_data.get("execution_id"),
        )
        try:
            if self._redis_client is None:
                if asyncio.iscoroutinefunction(self.event_bus.publish):
                    await self.event_bus.publish(event)
                else:
                    self.event_bus.publish(event)
            else:
                # 先尽力落盘再直发频道：落盘成功而 Pub/Sub 通知丢失时，
                # EventReconciler 仍能从事件存储补偿（直发不经
                # RedisEventBus.publish，落盘没人代劳，见 _persist_resume_
                # event_best_effort）；落盘失败只 warning，不阻塞直发。
                self._persist_resume_event_best_effort(event)
                self._redis_client.publish(
                    f"plaita:events:{event_type}", event.model_dump_json()
                )
            logger.info(
                "resume 事件已触发: %s (correlation_id=%s)",
                event_type,
                event.correlation_id,
            )
        except Exception as e:  # noqa: BLE001 — 记录后上抛，由调用方决定重试/保留排程
            logger.error("触发 resume 事件失败: %s", e, exc_info=True)
            raise
    
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
        logger.info("开始关闭 %s 服务...", self.get_service_type())
        
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
            logger.warning("关闭线程池时出错: %s", e)
        
        logger.info("%s 服务已关闭", self.get_service_type())

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
        logger.info("收到远程停止命令，优雅停止: %s", graceful)
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