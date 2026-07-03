"""
Kafka队列服务实现
负责监听Kafka主题消息
支持完善的消费者组管理、偏移量提交和错误处理
"""
import asyncio
import json
import time
import threading
from typing import Any, Dict, List, Optional, Callable, Awaitable
from dataclasses import dataclass, field

from .base_service import BaseExtendedService
from ...logger import logger

try:
    # 导入Kafka相关库
    from kafka import KafkaConsumer, KafkaAdminClient
    from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError
    from kafka import KafkaProducer
    from kafka.admin import NewTopic
    from kafka.structs import TopicPartition
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logger.warning("Kafka库未安装，Kafka队列服务将不可用. 请使用 pip install kafka-python 安装")


@dataclass
class ConsumerGroupInfo:
    """消费者组信息"""
    group_id: str
    topic: str
    consumer: Any = None
    is_active: bool = False
    created_time: float = field(default_factory=time.time)
    last_poll_time: float = 0
    messages_processed: int = 0
    offsets: Dict[int, int] = field(default_factory=dict)  # partition -> offset


class KafkaQueueService(BaseExtendedService):
    """
    Kafka队列服务
    负责监听Kafka主题消息并触发相应事件
    支持:
    - 消费者组管理（创建、销毁、监控）
    - 偏移量手动/自动提交
    - 连接健康检查和自动重连
    - 消费者再平衡处理
    """
    
    def __init__(self, event_bus, service_config=None):
        super().__init__(event_bus, service_config)
        self.consumer_groups: Dict[str, ConsumerGroupInfo] = {}
        self.admin_client: Optional[Any] = None
        self._health_check_task: Optional[asyncio.Task] = None
        self._lock = threading.Lock()
        
        # 配置参数
        config = service_config or {}
        self.default_bootstrap_servers = config.get("bootstrap_servers", ["localhost:9092"])
        self.health_check_interval = config.get("health_check_interval", 30)
        self.max_reconnect_attempts = config.get("max_reconnect_attempts", 3)
        self.reconnect_delay = config.get("reconnect_delay", 5)
    
    def get_service_type(self) -> str:
        """获取服务类型"""
        return "kafka_queue"
    
    def start_service(self) -> bool:
        """启动Kafka队列服务"""
        if not KAFKA_AVAILABLE:
            logger.error("Kafka库未安装，无法启动Kafka队列服务")
            return False
            
        try:
            self.is_running = True
            
            # 尝试初始化管理客户端
            self._init_admin_client()
            
            logger.info("Kafka队列服务已启动")
            return True
        except Exception as e:
            logger.error("启动Kafka队列服务失败: %s", e, exc_info=True)
            return False
    
    def _init_admin_client(self):
        """初始化Kafka管理客户端"""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.default_bootstrap_servers,
                request_timeout_ms=10000
            )
            logger.info("Kafka管理客户端初始化成功")
        except NoBrokersAvailable:
            logger.warning("无法连接到Kafka Broker，管理功能将不可用")
            self.admin_client = None
        except Exception as e:
            logger.warning("初始化Kafka管理客户端失败: %s", e)
            self.admin_client = None
    
    def stop_service(self) -> bool:
        """停止Kafka队列服务"""
        try:
            self.is_running = False
            
            # 关闭所有消费者组
            self._close_all_consumer_groups()
            
            # 关闭管理客户端
            if self.admin_client:
                try:
                    self.admin_client.close()
                except Exception as e:
                    logger.warning("关闭管理客户端失败: %s", e)
            
            logger.info("Kafka队列服务已停止")
            return True
        except Exception as e:
            logger.error("停止Kafka队列服务失败: %s", e, exc_info=True)
            return False
    
    def _close_all_consumer_groups(self):
        """关闭所有消费者组"""
        with self._lock:
            for group_id, group_info in self.consumer_groups.items():
                try:
                    if group_info.consumer:
                        group_info.consumer.close()
                        logger.info("消费者组 %s 已关闭", group_id)
                except Exception as e:
                    logger.warning("关闭消费者组 %s 失败: %s", group_id, e)
            
            self.consumer_groups.clear()
    
    def create_consumer_group(self, group_id: str, topic: str, 
                              consumer_config: Dict[str, Any] = None) -> Optional[ConsumerGroupInfo]:
        """
        创建消费者组
        
        Args:
            group_id: 消费者组ID
            topic: 订阅的主题
            consumer_config: 消费者配置
            
        Returns:
            ConsumerGroupInfo: 消费者组信息，创建失败返回None
        """
        if not KAFKA_AVAILABLE:
            logger.error("Kafka库未安装")
            return None
        
        with self._lock:
            if group_id in self.consumer_groups:
                logger.warning("消费者组 %s 已存在", group_id)
                return self.consumer_groups[group_id]
            
            try:
                config = consumer_config or {}
                
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=config.get("bootstrap_servers", self.default_bootstrap_servers),
                    group_id=group_id,
                    auto_offset_reset=config.get("auto_offset_reset", "latest"),
                    enable_auto_commit=config.get("enable_auto_commit", False),  # 默认手动提交
                    max_poll_records=config.get("max_poll_records", 10),
                    session_timeout_ms=config.get("session_timeout_ms", 30000),
                    heartbeat_interval_ms=config.get("heartbeat_interval_ms", 10000),
                    max_poll_interval_ms=config.get("max_poll_interval_ms", 300000),
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
                )
                
                group_info = ConsumerGroupInfo(
                    group_id=group_id,
                    topic=topic,
                    consumer=consumer,
                    is_active=True
                )
                
                self.consumer_groups[group_id] = group_info
                logger.info("消费者组 %s 创建成功，订阅主题: %s", group_id, topic)
                
                return group_info
                
            except Exception as e:
                logger.error("创建消费者组 %s 失败: %s", group_id, e, exc_info=True)
                return None
    
    def destroy_consumer_group(self, group_id: str) -> bool:
        """
        销毁消费者组
        
        Args:
            group_id: 消费者组ID
            
        Returns:
            bool: 是否成功
        """
        with self._lock:
            if group_id not in self.consumer_groups:
                logger.warning("消费者组 %s 不存在", group_id)
                return False
            
            try:
                group_info = self.consumer_groups[group_id]
                if group_info.consumer:
                    group_info.consumer.close()
                
                del self.consumer_groups[group_id]
                logger.info("消费者组 %s 已销毁", group_id)
                return True
                
            except Exception as e:
                logger.error("销毁消费者组 %s 失败: %s", group_id, e, exc_info=True)
                return False
    
    def get_consumer_group_status(self, group_id: str = None) -> Dict[str, Any]:
        """
        获取消费者组状态
        
        Args:
            group_id: 消费者组ID，为空则返回所有消费者组状态
            
        Returns:
            Dict: 消费者组状态信息
        """
        with self._lock:
            if group_id:
                if group_id not in self.consumer_groups:
                    return {"error": f"消费者组 {group_id} 不存在"}
                
                group_info = self.consumer_groups[group_id]
                return {
                    "group_id": group_info.group_id,
                    "topic": group_info.topic,
                    "is_active": group_info.is_active,
                    "created_time": group_info.created_time,
                    "last_poll_time": group_info.last_poll_time,
                    "messages_processed": group_info.messages_processed,
                    "offsets": group_info.offsets
                }
            else:
                return {
                    gid: {
                        "group_id": info.group_id,
                        "topic": info.topic,
                        "is_active": info.is_active,
                        "messages_processed": info.messages_processed
                    }
                    for gid, info in self.consumer_groups.items()
                }
    
    def commit_offsets(self, group_id: str, offsets: Dict[int, int] = None) -> bool:
        """
        提交偏移量
        
        Args:
            group_id: 消费者组ID
            offsets: 要提交的偏移量 {partition: offset}，为空则提交当前偏移量
            
        Returns:
            bool: 是否成功
        """
        with self._lock:
            if group_id not in self.consumer_groups:
                logger.error("消费者组 %s 不存在", group_id)
                return False

            group_info = self.consumer_groups[group_id]
            if not group_info.consumer:
                logger.error("消费者组 %s 的消费者未初始化", group_id)
                return False
            
            try:
                if offsets:
                    # 构建偏移量对象
                    offset_map = {
                        TopicPartition(group_info.topic, partition): offset
                        for partition, offset in offsets.items()
                    }
                    group_info.consumer.commit(offset_map)
                else:
                    # 提交当前偏移量
                    group_info.consumer.commit()
                
                logger.info("消费者组 %s 偏移量已提交", group_id)
                return True
                
            except Exception as e:
                logger.error("提交偏移量失败: %s", e, exc_info=True)
                return False
    
    def seek_to_offset(self, group_id: str, partition: int, offset: int) -> bool:
        """
        将消费者定位到指定偏移量
        
        Args:
            group_id: 消费者组ID
            partition: 分区
            offset: 偏移量
            
        Returns:
            bool: 是否成功
        """
        with self._lock:
            if group_id not in self.consumer_groups:
                logger.error("消费者组 %s 不存在", group_id)
                return False

            group_info = self.consumer_groups[group_id]
            if not group_info.consumer:
                return False

            try:
                tp = TopicPartition(group_info.topic, partition)
                group_info.consumer.seek(tp, offset)
                logger.info("消费者组 %s 已定位到分区 %s 偏移量 %s", group_id, partition, offset)
                return True
                
            except Exception as e:
                logger.error("定位偏移量失败: %s", e, exc_info=True)
                return False
    
    async def handle_task(self, task_config: Dict[str, Any]) -> bool:
        """处理Kafka队列监听任务"""
        if not KAFKA_AVAILABLE:
            logger.error("Kafka库未安装，无法处理Kafka队列任务")
            return False
            
        try:
            kafka_config = task_config.get("kafka_config", {})
            topic_config = task_config.get("topic_config", {})
            consumer_config = task_config.get("consumer_config", {})
            
            node_id = task_config.get("node_id")
            topic = topic_config.get("topic")
            execution_id = task_config.get("execution_id")
            
            logger.info("开始监听Kafka主题: node_id=%s, topic=%s", node_id, topic)
            
            # 检查流程完成的回调函数
            flow_completion_callback = task_config.get("completion_callback")
            
            # 创建任务完成标记
            task_complete = False
            
            # 创建Redis客户端以存储任务完成状态(可选)
            redis_client = None
            task_complete_key = None
            
            try:
                # 尝试导入Redis客户端，用于存储任务状态
                import redis.asyncio as aioredis
                redis_client = aioredis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
                task_complete_key = f"task_complete:kafka:{execution_id}"
                await redis_client.set(task_complete_key, "0", ex=60)
            except ImportError:
                logger.info("Redis客户端不可用，将使用内存状态跟踪任务完成情况")
                
            # 执行Kafka监听任务
            try:
                await self._listen_kafka_topic(
                    task_config,
                    flow_completion_checker=lambda: self._check_flow_completion(execution_id),
                    mark_task_complete=lambda: self._mark_task_complete(task_complete_key, redis_client)
                )
                return True
            except Exception as e:
                logger.error("Kafka监听任务执行失败: %s", e, exc_info=True)
                
                # 触发错误事件
                error_event_data = {
                    "node_id": task_config.get("node_id"),
                    "execution_id": execution_id,
                    "flow_id": task_config.get("flow_id"),
                    "trigger_type": "kafka_error",
                    "error_message": str(e),
                    "success": False
                }
                await self.trigger_event(task_config.get("event_type"), error_event_data)
                
                if redis_client and task_complete_key:
                    await redis_client.set(task_complete_key, "1")
                    await redis_client.close()
                    
                return False
                
        except Exception as e:
            logger.error("处理Kafka队列任务失败: %s", e, exc_info=True)
            
            # 触发错误事件
            try:
                error_event_data = {
                    "node_id": task_config.get("node_id"),
                    "execution_id": task_config.get("execution_id"),
                    "flow_id": task_config.get("flow_id"),
                    "trigger_type": "kafka_error",
                    "error_message": str(e),
                    "success": False
                }
                await self.trigger_event(task_config.get("event_type"), error_event_data)
            except Exception:
                logger.warning("kafka error-event trigger failed", exc_info=True)

            return False
    
    async def _check_flow_completion(self, execution_id: str) -> bool:
        """
        检查流程是否已完成

        通过事件总线的订阅存储查询流程状态，避免使用 sys.modules 和 inspect
        """
        try:
            # 方式1: 通过事件总线查询订阅状态
            if self.event_bus:
                # 检查是否还有针对该执行ID的活跃订阅
                if hasattr(self.event_bus, 'subscription_storage') and self.event_bus.subscription_storage:
                    subscriptions = await self.event_bus.subscription_storage.list_subscriptions()
                    for sub in subscriptions:
                        if isinstance(sub, dict) and sub.get("execution_id") == execution_id:
                            # 还有活跃订阅，流程未完成
                            return False
                    # 没有找到活跃订阅，流程可能已完成
                    return True

                # 方式2: 检查事件总线的处理器注册情况
                if hasattr(self.event_bus, 'handlers'):
                    handlers = self.event_bus.handlers
                    for event_type, handler_list in handlers.items():
                        for handler_info in handler_list:
                            if isinstance(handler_info, dict):
                                if handler_info.get("execution_id") == execution_id:
                                    return False
                            elif hasattr(handler_info, 'execution_id'):
                                if handler_info.execution_id == execution_id:
                                    return False

            # 方式3: 通过流程完成回调检查（如果配置了）
            if hasattr(self, '_flow_completion_callbacks'):
                for callback in self._flow_completion_callbacks:
                    try:
                        if callback(execution_id):
                            return True
                    except Exception:
                        logger.warning("flow completion callback raised", exc_info=True)

            # 默认返回False，保守策略不提前结束
            return False

        except Exception as e:
            logger.warning("检查流程完成状态失败: %s", e)
            return False
    
    def register_flow_completion_callback(self, callback: Callable[[str], bool]):
        """
        注册流程完成检查回调
        
        Args:
            callback: 回调函数，接收 execution_id，返回是否完成
        """
        if not hasattr(self, '_flow_completion_callbacks'):
            self._flow_completion_callbacks = []
        self._flow_completion_callbacks.append(callback)
        
    async def _mark_task_complete(self, task_complete_key: str, redis_client) -> None:
        """标记任务完成"""
        if redis_client and task_complete_key:
            try:
                await redis_client.set(task_complete_key, "1")
            except Exception as e:
                logger.warning("标记任务完成失败: %s", e)
    
    async def _listen_kafka_topic(self, task_config: Dict[str, Any],
                                flow_completion_checker: Callable[[], Awaitable[bool]] = None,
                                mark_task_complete: Callable[[], Awaitable[None]] = None):
        """
        实际的Kafka主题监听逻辑
        
        Args:
            task_config: 任务配置
            flow_completion_checker: 检查流程是否完成的回调函数
            mark_task_complete: 标记任务完成的回调函数
        """
        topic_config = task_config.get("topic_config", {})
        kafka_config = task_config.get("kafka_config", {})
        consumer_config = task_config.get("consumer_config", {})
        
        topic = topic_config.get("topic")
        execution_id = task_config.get("execution_id")
        
        # 获取Kafka服务器配置
        bootstrap_servers = kafka_config.get("bootstrap_servers", ["localhost:9092"])
        security_protocol = kafka_config.get("security_protocol", "PLAINTEXT")
        sasl_mechanism = kafka_config.get("sasl_mechanism")
        sasl_username = kafka_config.get("sasl_username")
        sasl_password = kafka_config.get("sasl_password")
        
        # 获取消费者配置
        group_id = consumer_config.get("group_id", "default_group")
        auto_offset_reset = consumer_config.get("auto_offset_reset", "latest")
        enable_auto_commit = consumer_config.get("enable_auto_commit", True)
        max_poll_records = consumer_config.get("max_poll_records", 1)
        message_format = topic_config.get("message_format", "json")
        
        # 超时配置 - 确保配置正确性
        # connections_max_idle_ms > request_timeout_ms > fetch_max_wait_ms
        connections_max_idle_ms = consumer_config.get("connections_max_idle_ms", 30000)  # 30秒
        request_timeout_ms = consumer_config.get("request_timeout_ms", 20000)            # 20秒
        session_timeout_ms = consumer_config.get("session_timeout_ms", 10000)            # 10秒
        max_poll_interval_ms = consumer_config.get("max_poll_interval_ms", 300000)
        
        # 记录任务是否完成
        task_completed = False
        
        redis_client = None
        task_complete_key = None
        
        try:
            # 尝试导入Redis客户端，如果可用，用于标记任务完成
            import redis.asyncio as aioredis
            redis_client = aioredis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
            
            # 设置任务完成标志键
            task_complete_key = f"task_complete:kafka:{execution_id}"
            await redis_client.set(task_complete_key, "0", ex=60)
            
        except ImportError:
            logger.info("Redis客户端不可用，将使用内存状态跟踪任务完成情况")
            redis_client = None
            task_complete_key = None
        
        # 创建Kafka消费者配置
        consumer_kwargs = {
            'bootstrap_servers': bootstrap_servers,
            'group_id': group_id,
            'auto_offset_reset': auto_offset_reset,
            'enable_auto_commit': enable_auto_commit,
            'max_poll_records': max_poll_records,
            'value_deserializer': lambda x: self._parse_message(x.decode('utf-8'), message_format),
            'request_timeout_ms': request_timeout_ms,
            'session_timeout_ms': session_timeout_ms,
            'connections_max_idle_ms': connections_max_idle_ms,
            'max_poll_interval_ms': max_poll_interval_ms
        }
        
        # 如果配置了SASL认证，添加认证参数
        if security_protocol and security_protocol != "PLAINTEXT":
            consumer_kwargs['security_protocol'] = security_protocol
            
            if sasl_mechanism and sasl_username and sasl_password:
                consumer_kwargs['sasl_mechanism'] = sasl_mechanism
                consumer_kwargs['sasl_plain_username'] = sasl_username
                consumer_kwargs['sasl_plain_password'] = sasl_password
        
        # 创建消费者并监听消息
        try:
            # 由于KafkaConsumer不是异步的，我们需要在执行器中运行它
            loop = asyncio.get_event_loop()
            
            # 创建一个共享的事件，用于标记消息接收完成
            message_received_event = asyncio.Event()
            
            # 创建一个消息队列，用于存储接收到的消息
            message_queue = asyncio.Queue()
            
            # 在单独的线程中运行Kafka消费者
            async def kafka_consumer_task():
                nonlocal task_completed
                try:
                    # 创建一个新的消费者
                    logger.info("创建Kafka消费者: %s, topic=%s", bootstrap_servers, topic)
                    consumer = KafkaConsumer(
                        topic,
                        **consumer_kwargs
                    )
                    
                    try:
                        # 设置轮询超时
                        poll_timeout_ms = 1000  # 1秒
                        
                        # 轮询消息直到超时或收到消息
                        start_time = time.time()
                        max_time = 20  # 最多等待20秒
                        
                        logger.info("开始轮询Kafka消息: topic=%s, auto_offset_reset=%s", topic, auto_offset_reset)
                        while not self.is_shutdown_requested() and not task_completed and time.time() - start_time < max_time:
                            # 轮询消息
                            records = consumer.poll(timeout_ms=poll_timeout_ms)
                            
                            if records:
                                logger.info("轮询到Kafka消息: %s", len(records))
                                for tp, messages in records.items():
                                    for message in messages:
                                        # 将消息放入队列
                                        await message_queue.put({
                                            "tp": tp,
                                            "message": message
                                        })
                                        # 设置消息接收事件
                                        message_received_event.set()
                                        logger.info("接收到Kafka消息: partition=%s, offset=%s", tp.partition, message.offset)
                            
                            # 短暂等待以减轻CPU负担
                            await asyncio.sleep(0.1)
                            
                            # 检查流程是否已完成
                            if flow_completion_checker and await flow_completion_checker():
                                logger.info("检测到流程 %s 已完成，停止轮询", execution_id)
                                task_completed = True
                                if mark_task_complete:
                                    await mark_task_complete()
                                break
                        
                        if time.time() - start_time >= max_time:
                            logger.warning("Kafka消息轮询超时: %s", topic)
                        
                    finally:
                        # 关闭消费者
                        consumer.close()
                        logger.info("Kafka消费者已关闭")
                
                except Exception as e:
                    logger.error("Kafka消费任务出错: %s", e)
            
            # 启动Kafka消费任务
            consumer_task = asyncio.create_task(kafka_consumer_task())
            
            # 等待接收到消息或超时
            try:
                # 最多等待25秒
                await asyncio.wait_for(message_received_event.wait(), timeout=25)
                
                # 如果收到消息，处理它
                if not message_queue.empty():
                    message_data = await message_queue.get()
                    tp = message_data["tp"]
                    message = message_data["message"]
                    
                    # 构造事件数据
                    event_data = {
                        "node_id": task_config.get("node_id"),
                        "execution_id": execution_id,
                        "flow_id": task_config.get("flow_id"),
                        "trigger_type": "kafka_message",
                        "topic": topic,
                        "message": message.value,
                        "partition": tp.partition,
                        "offset": message.offset,
                        "timestamp": message.timestamp,
                        "success": True
                    }
                    
                    # 触发事件
                    await self.trigger_event(task_config.get("event_type"), event_data)
                    
                    logger.info("从Kafka主题 %s 处理消息: partition=%s, offset=%s", topic, tp.partition, message.offset)
                    
                    # 等待流程完成
                    await self._wait_for_flow_completion(execution_id, flow_completion_checker, mark_task_complete)
                    
            except asyncio.TimeoutError:
                logger.warning("等待Kafka消息超时: %s", topic)
            
            # 清理任务
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
            
            if redis_client:
                await redis_client.close()
                
        except Exception as e:
            logger.error("Kafka消息监听错误: %s", e, exc_info=True)
            if redis_client:
                await redis_client.close()
            raise
    
    def _parse_message(self, message: str, message_format: str) -> Any:
        """
        解析消息
        
        Args:
            message: 原始消息
            message_format: 消息格式
            
        Returns:
            Any: 解析后的消息
        """
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
    
    async def _wait_for_flow_completion(self, execution_id: str,
                                       flow_completion_checker: Callable[[], Awaitable[bool]] = None,
                                       mark_task_complete: Callable[[], Awaitable[None]] = None):
        """等待流程完成"""
        # 等待短暂时间让流程处理事件
        await asyncio.sleep(1)
        
        # 检查流程是否已完成
        if flow_completion_checker and await flow_completion_checker():
            logger.info("流程 %s 已完成", execution_id)
            if mark_task_complete:
                await mark_task_complete()
            return

        # 最大等待5秒钟后自动结束
        for _ in range(10):  # 检查10次，每次0.5秒
            await asyncio.sleep(0.5)
            if flow_completion_checker and await flow_completion_checker():
                logger.info("流程 %s 已完成", execution_id)
                if mark_task_complete:
                    await mark_task_complete()
                return
        
        logger.info("等待超时，强制标记Kafka任务 %s 为完成", execution_id)
        if mark_task_complete:
            await mark_task_complete()
    
    async def send_test_message(self, topic: str, message: Any) -> bool:
        """
        发送测试消息到Kafka主题
        
        Args:
            topic: 主题名称
            message: 消息内容（会自动转换为JSON）
            
        Returns:
            bool: 发送是否成功
        """
        if not KAFKA_AVAILABLE:
            logger.error("Kafka库未安装，无法发送消息")
            return False
            
        try:
            # 创建生产者
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            # 发送消息
            future = producer.send(topic, message)
            
            # 等待发送完成
            try:
                record_metadata = future.get(timeout=10)
                logger.info("消息发送成功: topic=%s, partition=%s, offset=%s", topic, record_metadata.partition, record_metadata.offset)
                producer.flush()
                producer.close()
                return True
            except Exception as e:
                logger.error("发送消息超时: %s", e)
                producer.close()
                return False
                
        except Exception as e:
            logger.error("发送Kafka消息失败: %s", e)
            return False
    
    def validate_task_config(self, task_config: Dict[str, Any]) -> bool:
        """验证Kafka队列任务配置"""
        # 调用父类验证
        if not super().validate_task_config(task_config):
            return False
        
        # 验证Kafka特定字段
        kafka_config = task_config.get("kafka_config")
        topic_config = task_config.get("topic_config")
        consumer_config = task_config.get("consumer_config")
        
        if not kafka_config:
            logger.error("缺少Kafka配置")
            return False
        
        if not topic_config:
            logger.error("缺少主题配置")
            return False
        
        if not consumer_config:
            logger.error("缺少消费者配置")
            return False
        
        if not topic_config.get("topic"):
            logger.error("缺少主题名称")
            return False
        
        if not consumer_config.get("group_id"):
            logger.error("缺少消费者组ID")
            return False
        
        return True 