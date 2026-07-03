"""
Redis队列服务实现
负责监听Redis队列消息并触发相应事件
"""
import asyncio
import json
import time
from enum import Enum
from typing import Any, Dict, Optional

from .base_service import BaseExtendedService
from ...logger import logger

try:
    import redis
    import redis.asyncio as aioredis
    from redis.retry import Retry
    from redis.backoff import NoBackoff
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("Redis库未安装，Redis队列服务将不可用")


class ConnectionState(Enum):
    """Redis连接状态"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"


class RetryConfig:
    """重试配置"""
    def __init__(
        self,
        max_retries: int = 5,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0
    ):
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
    
    def get_delay(self, attempt: int) -> float:
        """计算第n次重试的延迟时间"""
        delay = self.initial_delay * (self.backoff_factor ** attempt)
        return min(delay, self.max_delay)


class RedisQueueService(BaseExtendedService):
    """
    Redis队列服务
    负责监听Redis队列消息并触发相应事件
    
    特性:
    - 支持多种队列类型 (list, stream, pubsub)
    - 自动重连机制
    - 指数退避重试
    - 优雅关闭
    """
    
    def __init__(self, event_bus=None, retry_config: Optional[RetryConfig] = None):
        """
        初始化Redis队列服务
        
        Args:
            event_bus: 事件总线实例
            retry_config: 重试配置
        """
        super().__init__(event_bus)
        self.retry_config = retry_config or RetryConfig()
        self._connection_state = ConnectionState.DISCONNECTED
        self._active_clients: Dict[str, Any] = {}  # execution_id -> redis_client
        self._reconnect_tasks: Dict[str, asyncio.Task] = {}
    
    def get_service_type(self) -> str:
        """获取服务类型"""
        return "redis_queue"
    
    @property
    def connection_state(self) -> ConnectionState:
        """获取连接状态"""
        return self._connection_state
    
    def start_service(self) -> bool:
        """启动Redis队列服务"""
        if not REDIS_AVAILABLE:
            logger.error("Redis库未安装，无法启动Redis队列服务")
            return False
        
        try:
            self.is_running = True
            self._connection_state = ConnectionState.DISCONNECTED
            logger.info("Redis队列服务已启动")
            return True
        except Exception as e:
            logger.error("启动Redis队列服务失败: %s", e, exc_info=True)
            return False
    
    def stop_service(self) -> bool:
        """停止Redis队列服务"""
        try:
            self.is_running = False
            
            # 取消所有重连任务
            for task in self._reconnect_tasks.values():
                if not task.done():
                    task.cancel()
            self._reconnect_tasks.clear()
            
            # 关闭所有活跃连接
            for execution_id, client in list(self._active_clients.items()):
                try:
                    asyncio.get_event_loop().run_until_complete(client.close())
                except Exception:
                    logger.warning("redis client close failed during stop (execution_id=%s)", execution_id, exc_info=True)
            self._active_clients.clear()
            
            self._connection_state = ConnectionState.DISCONNECTED
            logger.info("Redis队列服务已停止")
            return True
        except Exception as e:
            logger.error("停止Redis队列服务失败: %s", e, exc_info=True)
            return False
    
    async def handle_task(self, task_config: Dict[str, Any]) -> bool:
        """
        处理Redis队列监听任务
        
        Args:
            task_config: 任务配置
            
        Returns:
            bool: 处理是否成功
        """
        if not REDIS_AVAILABLE:
            logger.error("Redis库未安装，无法处理Redis队列任务")
            return False
        
        execution_id = task_config.get("execution_id")
        
        try:
            redis_config = task_config.get("redis_config", {})
            queue_config = task_config.get("queue_config", {})
            
            node_id = task_config.get("node_id")
            
            logger.info("开始监听Redis队列: node_id=%s, queue=%s", node_id, queue_config.get('name'))
            
            # 创建Redis连接（带重试）
            redis_client = await self._create_redis_client_with_retry(redis_config, execution_id)
            
            if not redis_client:
                return False
            
            # 存储活跃连接
            self._active_clients[execution_id] = redis_client
            
            try:
                # 根据队列类型选择监听方式
                queue_type = queue_config.get("type", "list")
                
                # 设置任务完成标志位
                task_complete_key = f"task_complete:{execution_id}"
                await redis_client.set(task_complete_key, "0", ex=3600)  # 1小时过期
                
                result = False
                if queue_type == "list":
                    result = await self._listen_list_queue(redis_client, task_config)
                elif queue_type == "stream":
                    result = await self._listen_stream_queue(redis_client, task_config)
                elif queue_type == "pubsub":
                    result = await self._listen_pubsub_queue(redis_client, task_config)
                else:
                    logger.error("不支持的队列类型: %s", queue_type)
                    return False
                
                # 删除任务完成标志键
                await redis_client.delete(task_complete_key)
                return result
                
            finally:
                # 清理连接
                await self._cleanup_client(execution_id)
                
        except asyncio.CancelledError:
            logger.info("Redis队列任务被取消: %s", execution_id)
            await self._cleanup_client(execution_id)
            raise
        except Exception as e:
            logger.error("处理Redis队列任务失败: %s", e, exc_info=True)
            await self._trigger_error_event(task_config, str(e))
            await self._cleanup_client(execution_id)
            return False
    
    async def _create_redis_client_with_retry(
        self, 
        redis_config: Dict[str, Any], 
        execution_id: str
    ) -> Optional[Any]:
        """
        创建Redis客户端（带重试机制）
        
        Args:
            redis_config: Redis配置
            execution_id: 执行ID
            
        Returns:
            Redis客户端实例，失败返回None
        """
        self._connection_state = ConnectionState.CONNECTING
        
        for attempt in range(self.retry_config.max_retries + 1):
            try:
                client = await self._create_redis_client(redis_config)
                self._connection_state = ConnectionState.CONNECTED
                logger.info("Redis连接成功: execution_id=%s", execution_id)
                return client
            except Exception as e:
                if attempt < self.retry_config.max_retries:
                    delay = self.retry_config.get_delay(attempt)
                    self._connection_state = ConnectionState.RECONNECTING
                    logger.warning(
                        "Redis连接失败 (尝试 %d/%d): %s, %.1f秒后重试",
                        attempt + 1, self.retry_config.max_retries + 1, e, delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    self._connection_state = ConnectionState.FAILED
                    logger.error("Redis连接失败，已达到最大重试次数: %s", e)
                    return None
        
        return None
    
    async def _create_redis_client(self, redis_config: Dict[str, Any]) -> Any:
        """
        创建Redis客户端
        
        Args:
            redis_config: Redis配置
            
        Returns:
            Redis客户端实例
        """
        host = redis_config.get("host", "localhost")
        port = redis_config.get("port", 6379)
        db = redis_config.get("db", 0)
        password = redis_config.get("password")
        socket_timeout = redis_config.get("socket_timeout", 5.0)
        socket_connect_timeout = redis_config.get("socket_connect_timeout", 5.0)
        
        # 创建异步Redis客户端
        # redis-py 6.0 起 retry_on_timeout 已废弃（TimeoutError 默认就在重试白名单里），
        # 改用显式的 Retry 对象保留「超时重试 1 次」的语义。
        retry = Retry(NoBackoff(), 1) if REDIS_AVAILABLE else None
        client = aioredis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            retry=retry,
            health_check_interval=30
        )
        
        # 测试连接
        await client.ping()
        
        return client
    
    async def _cleanup_client(self, execution_id: str):
        """清理Redis客户端连接"""
        client = self._active_clients.pop(execution_id, None)
        if client:
            try:
                await client.close()
            except Exception as e:
                logger.debug("关闭Redis连接时出错: %s", e)
    
    async def _trigger_error_event(self, task_config: Dict[str, Any], error_message: str):
        """触发错误事件"""
        try:
            error_event_data = {
                "node_id": task_config.get("node_id"),
                "execution_id": task_config.get("execution_id"),
                "flow_id": task_config.get("flow_id"),
                "trigger_type": "redis_error",
                "error_message": error_message,
                "success": False,
                "timestamp": int(time.time() * 1000)
            }
            await self.trigger_event(task_config.get("event_type"), error_event_data)
        except Exception as e:
            logger.error("触发错误事件失败: %s", e)
    
    async def _listen_list_queue(self, redis_client, task_config: Dict[str, Any]) -> bool:
        """
        监听Redis列表队列
        
        Args:
            redis_client: Redis客户端
            task_config: 任务配置
            
        Returns:
            bool: 是否成功
        """
        queue_config = task_config.get("queue_config", {})
        listen_config = task_config.get("listen_config", {})
        
        queue_name = queue_config.get("name")
        message_format = queue_config.get("message_format", "json")
        execution_id = task_config.get("execution_id")
        
        task_complete_key = f"task_complete:{execution_id}"
        max_wait_after_message = listen_config.get("max_wait_after_message", 10)
        
        message_processed = False
        message_processed_time = 0
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while not self.is_shutdown_requested():
            try:
                # 检查任务是否已完成
                complete_flag = await redis_client.get(task_complete_key)
                if complete_flag == "1":
                    logger.info("Redis队列任务已标记为完成，停止监听: %s", execution_id)
                    return True
                
                # 如果消息已处理，检查是否超时
                if message_processed and (time.time() - message_processed_time) > max_wait_after_message:
                    logger.info("Redis队列任务处理后等待超时，自动完成: %s", execution_id)
                    await redis_client.set(task_complete_key, "1")
                    return True
                
                # 使用短超时的BLPOP
                result = await redis_client.blpop(queue_name, timeout=2)
                
                if result:
                    queue_name_result, message = result
                    parsed_message = self._parse_message(message, message_format)
                    
                    event_data = {
                        "node_id": task_config.get("node_id"),
                        "execution_id": execution_id,
                        "flow_id": task_config.get("flow_id"),
                        "trigger_type": "redis_message",
                        "queue_type": "list",
                        "queue_name": queue_name,
                        "message": parsed_message,
                        "raw_message": message,
                        "timestamp": int(time.time() * 1000),
                        "success": True
                    }
                    
                    await self.trigger_event(task_config.get("event_type"), event_data)
                    
                    logger.info("从Redis列表队列 %s 接收到消息", queue_name)
                    message_processed = True
                    message_processed_time = time.time()
                    consecutive_errors = 0
                    
                    await asyncio.sleep(0.5)
                    
                    if await self._check_flow_completion(execution_id):
                        logger.info("流程 %s 已完成，标记Redis任务为完成", execution_id)
                        await redis_client.set(task_complete_key, "1")
                        return True
                
                consecutive_errors = 0
                        
            except asyncio.TimeoutError:
                continue
            except (redis.ConnectionError, redis.TimeoutError) as e:
                consecutive_errors += 1
                logger.warning("Redis连接错误 (%s/%s): %s", consecutive_errors, max_consecutive_errors, e)
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("达到最大连续错误次数，停止监听")
                    return False
                
                delay = self.retry_config.get_delay(consecutive_errors - 1)
                await asyncio.sleep(delay)
            except Exception as e:
                consecutive_errors += 1
                logger.error("监听Redis列表队列出错: %s", e, exc_info=True)
                
                if consecutive_errors >= max_consecutive_errors:
                    return False
                
                await asyncio.sleep(5)
        
        return False
    
    async def _check_flow_completion(self, execution_id: str) -> bool:
        """检查流程是否已完成

        通过 event_bus 的订阅存储来检查流程是否仍有活跃的订阅。
        如果没有活跃订阅，则认为流程已完成。

        Args:
            execution_id: 流程执行ID（correlation_id）

        Returns:
            bool: 如果流程已完成（无活跃订阅）返回 True，否则返回 False
        """
        if not self.event_bus:
            logger.warning("event_bus 未设置，无法检查流程完成状态")
            return False

        try:
            if hasattr(self.event_bus, 'subscription_storage') and self.event_bus.subscription_storage:
                subscriptions = await self.event_bus.subscription_storage.list_subscriptions(
                    correlation_id=execution_id
                )
                return len(subscriptions) == 0
            return False
        except Exception as e:
            logger.warning("检查流程完成状态时出错: %s", e)
            return False
    
    async def _listen_stream_queue(self, redis_client, task_config: Dict[str, Any]) -> bool:
        """监听Redis流队列"""
        queue_config = task_config.get("queue_config", {})
        listen_config = task_config.get("listen_config", {})
        
        stream_name = queue_config.get("name")
        timeout_ms = listen_config.get("timeout_seconds", 60) * 1000
        message_format = queue_config.get("message_format", "json")
        execution_id = task_config.get("execution_id")
        
        task_complete_key = f"task_complete:{execution_id}"
        last_id = "$"
        
        message_processed = False
        message_processed_time = 0
        max_wait_after_message = listen_config.get("max_wait_after_message", 10)
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while not self.is_shutdown_requested():
            try:
                complete_flag = await redis_client.get(task_complete_key)
                if complete_flag == "1":
                    logger.info("Redis流队列任务已标记为完成，停止监听: %s", execution_id)
                    return True
                
                if message_processed and (time.time() - message_processed_time) > max_wait_after_message:
                    logger.info("Redis流队列任务处理后等待超时，自动完成: %s", execution_id)
                    await redis_client.set(task_complete_key, "1")
                    return True
                
                streams = {stream_name: last_id}
                result = await redis_client.xread(streams, block=min(timeout_ms, 2000))
                
                if result:
                    for stream_name_result, messages in result:
                        for message_id, fields in messages:
                            parsed_message = self._parse_stream_message(fields, message_format)
                            
                            event_data = {
                                "node_id": task_config.get("node_id"),
                                "execution_id": task_config.get("execution_id"),
                                "flow_id": task_config.get("flow_id"),
                                "trigger_type": "redis_message",
                                "queue_type": "stream",
                                "stream_name": stream_name,
                                "message_id": message_id,
                                "message": parsed_message,
                                "raw_fields": fields,
                                "timestamp": int(time.time() * 1000),
                                "success": True
                            }
                            
                            await self.trigger_event(task_config.get("event_type"), event_data)
                            last_id = message_id
                            
                            logger.info("从Redis流 %s 接收到消息: %s", stream_name, message_id)
                            message_processed = True
                            message_processed_time = time.time()
                            consecutive_errors = 0
                            
                            await asyncio.sleep(0.5)
                            
                            if await self._check_flow_completion(execution_id):
                                logger.info("流程 %s 已完成，标记Redis流任务为完成", execution_id)
                                await redis_client.set(task_complete_key, "1")
                                return True
                
                consecutive_errors = 0
                            
            except asyncio.TimeoutError:
                continue
            except (redis.ConnectionError, redis.TimeoutError) as e:
                consecutive_errors += 1
                logger.warning("Redis流连接错误 (%s/%s): %s", consecutive_errors, max_consecutive_errors, e)
                
                if consecutive_errors >= max_consecutive_errors:
                    return False
                
                delay = self.retry_config.get_delay(consecutive_errors - 1)
                await asyncio.sleep(delay)
            except Exception as e:
                consecutive_errors += 1
                logger.error("监听Redis流队列出错: %s", e, exc_info=True)
                
                if consecutive_errors >= max_consecutive_errors:
                    return False
                
                await asyncio.sleep(5)
        
        return False
    
    async def _listen_pubsub_queue(self, redis_client, task_config: Dict[str, Any]) -> bool:
        """监听Redis发布/订阅队列"""
        queue_config = task_config.get("queue_config", {})
        listen_config = task_config.get("listen_config", {})
        
        channel_name = queue_config.get("name")
        message_format = queue_config.get("message_format", "json")
        execution_id = task_config.get("execution_id")
        
        task_complete_key = f"task_complete:{execution_id}"
        
        message_processed = False
        message_processed_time = 0
        max_wait_after_message = listen_config.get("max_wait_after_message", 10)
        
        pubsub = redis_client.pubsub()
        
        try:
            await pubsub.subscribe(channel_name)
            logger.info("已订阅Redis频道: %s", channel_name)
            
            async for message in pubsub.listen():
                if self.is_shutdown_requested():
                    break
                
                complete_flag = await redis_client.get(task_complete_key)
                if complete_flag == "1":
                    logger.info("Redis发布/订阅任务已标记为完成，停止监听: %s", execution_id)
                    break
                
                if message_processed and (time.time() - message_processed_time) > max_wait_after_message:
                    logger.info("Redis发布/订阅任务处理后等待超时，自动完成: %s", execution_id)
                    await redis_client.set(task_complete_key, "1")
                    break
                
                if message["type"] == "message":
                    parsed_message = self._parse_message(message["data"], message_format)
                    
                    event_data = {
                        "node_id": task_config.get("node_id"),
                        "execution_id": execution_id,
                        "flow_id": task_config.get("flow_id"),
                        "trigger_type": "redis_message",
                        "queue_type": "pubsub",
                        "channel_name": channel_name,
                        "message": parsed_message,
                        "raw_message": message["data"],
                        "timestamp": int(time.time() * 1000),
                        "success": True
                    }
                    
                    await self.trigger_event(task_config.get("event_type"), event_data)
                    
                    logger.info("从Redis频道 %s 接收到消息", channel_name)
                    message_processed = True
                    message_processed_time = time.time()
                    
                    await asyncio.sleep(0.5)
                    
                    if await self._check_flow_completion(execution_id):
                        logger.info("流程 %s 已完成，标记Redis发布/订阅任务为完成", execution_id)
                        await redis_client.set(task_complete_key, "1")
                        break
            
            return True
                    
        finally:
            try:
                await pubsub.unsubscribe(channel_name)
                await pubsub.close()
            except Exception:
                logger.debug("pubsub unsubscribe/close failed", exc_info=True)
    
    def _parse_message(self, message: str, message_format: str) -> Any:
        """解析消息"""
        try:
            if message_format == "json":
                return json.loads(message)
            elif message_format == "text":
                return str(message)
            else:
                return message
        except Exception as e:
            logger.warning("解析消息失败: %s, 返回原始消息", e)
            return message
    
    def _parse_stream_message(self, fields: Dict[str, str], message_format: str) -> Any:
        """解析流消息字段"""
        try:
            if message_format == "json":
                data = fields.get("data", "")
                if data:
                    return json.loads(data)
                else:
                    return fields
            else:
                return fields
        except Exception as e:
            logger.warning("解析流消息失败: %s, 返回原始字段", e)
            return fields
    
    def validate_task_config(self, task_config: Dict[str, Any]) -> bool:
        """验证Redis队列任务配置"""
        if not REDIS_AVAILABLE:
            logger.error("Redis库未安装")
            return False
        
        if not super().validate_task_config(task_config):
            return False
        
        redis_config = task_config.get("redis_config")
        queue_config = task_config.get("queue_config")
        
        if not redis_config:
            logger.error("缺少Redis配置")
            return False
        
        if not queue_config:
            logger.error("缺少队列配置")
            return False
        
        if not queue_config.get("name"):
            logger.error("缺少队列名称")
            return False
        
        return True
    
    def get_active_connections_count(self) -> int:
        """获取活跃连接数"""
        return len(self._active_clients)
    
    def get_service_status(self) -> Dict[str, Any]:
        """获取服务状态"""
        status = super().get_service_status() if hasattr(super(), 'get_service_status') else {}
        status.update({
            "connection_state": self._connection_state.value,
            "active_connections": len(self._active_clients),
            "retry_config": {
                "max_retries": self.retry_config.max_retries,
                "initial_delay": self.retry_config.initial_delay,
                "max_delay": self.retry_config.max_delay,
                "backoff_factor": self.retry_config.backoff_factor
            }
        })
        return status
