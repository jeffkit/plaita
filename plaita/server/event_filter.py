"""
事件过滤器，用于接收事件并过滤出与当前事件相关的订阅
将订阅信息组装成flow worker任务放入队列
"""
import json
import logging
import asyncio
import argparse
import sys
from typing import Optional, Dict, Any, List

from redis import Redis

from plaita.event.core import Event, EventSubscriptionStorage, EventBus
from plaita.storage.base import ExecutionStorage
from plaita.server.task_queue import enqueue_task

# 获取logger
logger = logging.getLogger("plaita.server.event_filter")


class EventFilter:
    """
    事件过滤器，接收事件并处理与流程执行相关的订阅
    将匹配的订阅组装成任务放入flow worker队列
    """
    
    def __init__(
        self, 
        execution_storage: ExecutionStorage,
        subscription_storage: EventSubscriptionStorage,
        redis_client: Redis,
        event_bus: EventBus,
        queue_name: str = "plaita:flow:queue"
    ):
        """
        初始化事件过滤器
        
        Args:
            execution_storage: 执行状态存储
            subscription_storage: 事件订阅存储
            redis_client: Redis客户端
            event_bus: 事件总线
            queue_name: 流程工作器队列名称
        """
        self.execution_storage = execution_storage
        self.subscription_storage = subscription_storage
        self.redis_client = redis_client
        self.event_bus = event_bus
        self.queue_name = queue_name
        self._running = False
        self._subscription_id = None
    
    async def handle_event(self, event: Event) -> None:
        """
        处理事件，实现EventHandler接口
        
        Args:
            event: 接收到的事件对象
        """
        try:
            logger.info("接收到事件: %s, 类型: %s", event.event_id, event.event_type)
            
            # 如果事件没有correlation_id，无法关联到流程执行
            if not event.correlation_id:
                logger.debug("事件没有correlation_id，跳过处理: %s", event.event_id)
                return
            
            # 使用correlation_id作为execution_id查询执行状态
            execution_id = event.correlation_id
            state = self.execution_storage.load_execution_state(execution_id)
            
            if not state:
                logger.debug("找不到关联的执行状态，跳过处理: %s", execution_id)
                return
            
            # 查询与事件匹配的订阅
            subscriptions = await self.subscription_storage.find_matching_subscriptions(event, state.context)
            
            if not subscriptions:
                logger.debug("没有匹配的订阅，跳过处理: %s", event.event_id)
                return
            
            for subscription in subscriptions:
                if (subscription.flow_id and subscription.flow_id == state.flow_id) or \
                   (subscription.correlation_id and subscription.correlation_id == execution_id):
                    
                    dedup_key = f"plaita:event_filter:dedup:{event.event_id}:{subscription.subscription_id}"
                    if not self.redis_client.set(dedup_key, "1", nx=True, ex=3600):
                        logger.debug("事件已被其他实例处理，跳过: %s/%s", event.event_id, subscription.subscription_id)
                        continue
                    
                    resume_task = {
                        "type": "resume",
                        "flow_id": state.flow_id,
                        "execution_id": execution_id,
                        "resume_type": "event",
                        "data": {
                            "event_id": event.event_id,
                            "event_type": event.event_type,
                            "event_data": event.data,
                            "subscription_id": subscription.subscription_id
                        }
                    }
                    
                    enqueue_task(self.redis_client, self.queue_name, resume_task)
                    
                    logger.info("已将事件 %s 入队 stream %s，关联订阅: %s", event.event_id, self.queue_name, subscription.subscription_id)
                    
                    await self.subscription_storage.mark_event_processed(
                        subscription.subscription_id, 
                        event.event_id
                    )
                    
                else:
                    logger.debug("订阅与当前执行无关，跳过: %s", subscription.subscription_id)
            
        except Exception as e:
            logger.error("处理事件出错: %s", e, exc_info=True)
    
    async def start(self, event_type: Optional[str] = None):
        """
        启动事件过滤器，开始监听事件
        
        Args:
            event_type: 要监听的事件类型，默认监听所有事件
        """
        if self._running:
            logger.warning("事件过滤器已经在运行中")
            return
            
        self._running = True
        
        try:
            # 订阅事件，如果未指定event_type则监听所有事件
            self._subscription_id = await self.event_bus.register_handler(
                event_type=event_type,  # None表示监听所有事件
                handler=self.handle_event
            )
            
            event_type_desc = event_type if event_type else "所有事件类型"
            logger.info("事件过滤器已启动，订阅ID: %s, 监听事件类型: %s", self._subscription_id, event_type_desc)
            
            # 保持运行直到停止
            while self._running:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error("启动事件过滤器时出错: %s", e)
            self._running = False
    
    async def stop(self):
        """停止事件过滤器"""
        if not self._running:
            return
            
        self._running = False
        
        # 取消事件订阅
        if self._subscription_id and self.event_bus:
            try:
                # 修正：使用unregister_handler而不是unregister_subscription
                if hasattr(self.event_bus, 'unregister_handler'):
                    await self.event_bus.unregister_handler(self._subscription_id)
                else:
                    # 兼容性处理
                    await self.event_bus.unregister_subscription(self._subscription_id)
                logger.info("已取消事件订阅: %s", self._subscription_id)
            except Exception as e:
                logger.error("取消事件订阅时出错: %s", e)
    
    @staticmethod
    def create_event_filter(
        execution_storage: ExecutionStorage,
        subscription_storage: EventSubscriptionStorage,
        event_bus: EventBus,
        redis_url: str,
        queue_name: Optional[str] = None
    ) -> "EventFilter":
        """
        工厂方法：创建事件过滤器实例
        
        Args:
            execution_storage: 执行状态存储
            subscription_storage: 事件订阅存储
            event_bus: 事件总线
            redis_url: Redis连接URL
            queue_name: 队列名称，默认为"plaita:flow:queue"
            
        Returns:
            EventFilter: 事件过滤器实例
        """
        redis_client = Redis.from_url(redis_url)
        return EventFilter(
            execution_storage=execution_storage,
            subscription_storage=subscription_storage,
            redis_client=redis_client,
            event_bus=event_bus,
            queue_name=queue_name or "plaita:flow:queue"
        )


from plaita.server.factory import create_storage_component, create_event_bus  # noqa: F401


async def main_async(args):
    """异步主函数"""
    try:
        # 创建执行状态存储
        storage_kwargs = {
            "redis_url": args.redis_url,
            "database_url": args.database_url
        }
        
        # 创建执行状态存储
        execution_storage = create_storage_component(
            args.execution_storage_type,
            "execution",
            **storage_kwargs
        )
        logger.info("已创建执行状态存储: %s类型", args.execution_storage_type)
        
        # 创建事件总线（先于订阅存储：优先复用 bus 自带的 subscription_storage，
        # 避免 worker register_subscription 写入与 filter 读取落在不同后端/实例）
        event_bus = create_event_bus(
            args.event_bus_type,
            **storage_kwargs
        )
        logger.info("已创建事件总线: %s类型", args.event_bus_type)

        bus_storage = getattr(event_bus, "subscription_storage", None)
        if bus_storage is not None:
            subscription_storage = bus_storage
            logger.info(
                "使用事件总线自带的订阅存储（与 register_subscription 同实例/同 keyspace）"
            )
        else:
            subscription_storage = create_storage_component(
                args.subscription_storage_type,
                "subscription",
                **storage_kwargs
            )
            logger.info("已创建事件订阅存储: %s类型", args.subscription_storage_type)
        
        # 创建事件过滤器
        event_filter = EventFilter.create_event_filter(
            execution_storage=execution_storage,
            subscription_storage=subscription_storage,
            event_bus=event_bus,
            redis_url=args.redis_url,
            queue_name=args.queue_name
        )
        
        # 启动事件过滤器
        logger.info("事件过滤器启动中，队列名称: %s", args.queue_name)
        await event_filter.start(event_type=args.event_type)
        
    except Exception as e:
        logger.error("事件过滤器启动失败: %s", e, exc_info=True)
        sys.exit(1)

def main():
    """命令行入口程序"""
    parser = argparse.ArgumentParser(description="Plaita事件过滤器")
    
    # Redis参数
    parser.add_argument("--redis-url", default="redis://localhost:6379/0",
                      help="Redis连接URL")
    parser.add_argument("--queue-name", default="plaita:flow:queue",
                      help="Redis队列名称")
    
    # 数据库参数
    parser.add_argument("--database-url", default="sqlite:///flow.db",
                      help="数据库连接URL")
    
    # 存储组件类型（execution 仅 memory|redis；subscription 可为 db，调用方为 async）
    parser.add_argument("--execution-storage-type", choices=["memory", "redis"], default="redis",
                      help="执行状态存储类型（memory|redis；db 已下架）")
    parser.add_argument("--subscription-storage-type", choices=["memory", "redis"], default="redis",
                      help="事件订阅存储类型（db/sqlalchemy 为 experimental，见 factory）")
    
    # 事件总线参数
    parser.add_argument("--event-bus-type", choices=["memory", "redis"], default="redis",
                      help="事件总线类型（生产用 redis）")
    
    # 事件类型过滤
    parser.add_argument("--event-type", type=str, default="",
                      help="要监听的事件类型")
    
    args = parser.parse_args()
    
    # 运行异步主函数
    asyncio.run(main_async(args))

if __name__ == "__main__":
    main() 