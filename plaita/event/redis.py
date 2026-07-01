"""
基于Redis的事件总线和存储实现
"""
import asyncio
import json
import time
import uuid
import sys
import os
from typing import Any, Callable, Dict, List, Optional, Set, Union, Awaitable

# 临时修改sys.path以避免本地文件冲突
original_path = list(sys.path)
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir in sys.path:
    sys.path.remove(current_dir)

# 需要安装的第三方库
try:
    import redis.asyncio as aioredis
except ImportError:
    raise ImportError("请安装redis.asyncio依赖: pip install redis")
finally:
    # 恢复原始路径
    sys.path = original_path

from .core import (
    Event, EventBus, EventHandler, EventStorage, EventSubscription, 
    EventSubscriptionStorage, EventProcessingTracker, RetryPolicy
)
from .exceptions import EventError, EventNotFoundError, EventStorageError, EventTimeoutError


class RedisEventStorage(EventStorage):
    """
    基于Redis的事件存储实现
    """
    DEFAULT_TTL = 60 * 60 * 24 * 7  # 7 days

    def __init__(self, redis_url: str, key_prefix: str = "plaita:event:", ttl: Optional[int] = None):
        """
        初始化Redis事件存储
        
        Args:
            redis_url: Redis连接URL
            key_prefix: Redis键前缀
            ttl: 事件数据过期时间（秒），默认 7 天
        """
        self.redis_url = redis_url
        self.key_prefix = key_prefix
        self.redis = None
        self.ttl = ttl if ttl is not None else self.DEFAULT_TTL
    
    async def initialize(self):
        """初始化Redis连接"""
        if self.redis is None:
            self.redis = await aioredis.from_url(self.redis_url)
    
    async def store_event(self, event: Event) -> str:
        """存储事件"""
        await self.initialize()
        
        # 将事件序列化为JSON
        event_data = event.model_dump_json()
        
        event_key = f"{self.key_prefix}events:{event.event_id}"
        await self.redis.set(event_key, event_data, ex=self.ttl)
        
        type_key = f"{self.key_prefix}types:{event.event_type}"
        await self.redis.zadd(type_key, {event.event_id: event.timestamp})
        await self.redis.expire(type_key, self.ttl)
        
        return event.event_id
    
    async def batch_store_events(self, events: List[Event]) -> List[str]:
        """批量存储事件"""
        await self.initialize()
        
        # 使用Redis管道批量操作提高性能
        pipe = self.redis.pipeline()
        event_ids = []
        
        for event in events:
            event_data = event.model_dump_json()
            event_key = f"{self.key_prefix}events:{event.event_id}"
            type_key = f"{self.key_prefix}types:{event.event_type}"
            
            pipe.set(event_key, event_data, ex=self.ttl)
            pipe.zadd(type_key, {event.event_id: event.timestamp})
            pipe.expire(type_key, self.ttl)
            event_ids.append(event.event_id)
        
        # 执行管道
        await pipe.execute()
        return event_ids
    
    async def get_event(self, event_id: str) -> Optional[Event]:
        """获取事件"""
        await self.initialize()
        
        event_key = f"{self.key_prefix}events:{event_id}"
        event_data = await self.redis.get(event_key)
        
        if not event_data:
            return None
            
        try:
            return Event.model_validate_json(event_data)
        except Exception as e:
            raise EventError(f"解析事件数据失败: {e}")
    
    async def list_events(self, event_type: Optional[str] = None, 
                         start_time: Optional[float] = None,
                         end_time: Optional[float] = None,
                         limit: int = 100) -> List[Event]:
        """列出事件"""
        await self.initialize()
        
        result = []
        
        # 如果指定了事件类型，则只搜索该类型
        if event_type:
            type_key = f"{self.key_prefix}types:{event_type}"
            
            # 构建时间范围
            min_score = start_time if start_time is not None else "-inf"
            max_score = end_time if end_time is not None else "+inf"
            
            # 使用ZRANGEBYSCORE查询指定时间范围内的事件ID
            event_ids = await self.redis.zrangebyscore(
                type_key, min_score, max_score, 
                start=0, num=limit
            )
            
            # 批量获取事件数据
            if event_ids:
                event_keys = [f"{self.key_prefix}events:{event_id}" for event_id in event_ids]
                event_data_list = await self.redis.mget(*event_keys)
                
                for event_data in event_data_list:
                    if event_data:
                        try:
                            result.append(Event.model_validate_json(event_data))
                        except Exception:
                            pass
        else:
            type_keys = []
            cursor = 0
            while True:
                cursor, keys = await self.redis.scan(cursor, match=f"{self.key_prefix}types:*", count=100)
                type_keys.extend(keys)
                if cursor == 0:
                    break
            
            for type_key in type_keys[:10]:
                type_key_str = type_key.decode('utf-8') if isinstance(type_key, bytes) else type_key
                event_type = type_key_str.split(':')[-1]
                
                # 修复: 直接使用单类型查询逻辑而不是递归调用
                sub_limit = max(1, limit // len(type_keys))
                type_key = f"{self.key_prefix}types:{event_type}"
                
                # 构建时间范围
                min_score = start_time if start_time is not None else "-inf"
                max_score = end_time if end_time is not None else "+inf"
                
                # 使用ZRANGEBYSCORE查询指定时间范围内的事件ID
                event_ids = await self.redis.zrangebyscore(
                    type_key, min_score, max_score, 
                    start=0, num=sub_limit
                )
                
                # 批量获取事件数据
                if event_ids:
                    event_keys = [f"{self.key_prefix}events:{event_id}" for event_id in event_ids]
                    event_data_list = await self.redis.mget(*event_keys)
                    
                    for event_data in event_data_list:
                        if event_data:
                            try:
                                result.append(Event.model_validate_json(event_data))
                            except Exception:
                                pass
                
                if len(result) >= limit:
                    break
        
        return result[:limit]
    
    async def delete_event(self, event_id: str) -> bool:
        """删除事件"""
        await self.initialize()
        
        # 获取事件数据，以确定其类型
        event = await self.get_event(event_id)
        if not event:
            return False
            
        # 从事件类型索引中移除
        type_key = f"{self.key_prefix}types:{event.event_type}"
        await self.redis.zrem(type_key, event_id)
        
        # 删除事件数据
        event_key = f"{self.key_prefix}events:{event_id}"
        result = await self.redis.delete(event_key)
        
        return result > 0

    async def close(self):
        """关闭 Redis 连接"""
        if self.redis:
            await self.redis.aclose()
            self.redis = None


class RedisEventSubscriptionStorage(EventSubscriptionStorage):
    """
    基于Redis的事件订阅存储实现
    """
    DEFAULT_TTL = 60 * 60 * 24 * 7  # 7 days

    def __init__(self, redis_url: str, key_prefix: str = "plaita:subscription:", ttl: Optional[int] = None):
        """
        初始化Redis事件订阅存储
        
        Args:
            redis_url: Redis连接URL
            key_prefix: Redis键前缀
            ttl: 订阅数据过期时间（秒），默认 7 天
        """
        self.redis_url = redis_url
        self.ttl = ttl if ttl is not None else self.DEFAULT_TTL
        self.key_prefix = key_prefix
        self.redis = None
        self.lock = asyncio.Lock()  # 用于原子操作

    async def initialize(self):
        """初始化Redis连接"""
        if self.redis is None:
            self.redis = await aioredis.from_url(self.redis_url)
    
    async def store_subscription(self, subscription: EventSubscription) -> str:
        """存储事件订阅"""
        await self.initialize()
        
        subscription_data = subscription.model_dump_json()
        pipe = self.redis.pipeline()
        
        subscription_key = f"{self.key_prefix}data:{subscription.subscription_id}"
        pipe.set(subscription_key, subscription_data, ex=self.ttl)
        
        type_key = f"{self.key_prefix}type:{subscription.event_type}"
        pipe.sadd(type_key, subscription.subscription_id)
        pipe.expire(type_key, self.ttl)
        
        if subscription.correlation_id:
            correlation_key = f"{self.key_prefix}correlation:{subscription.correlation_id}"
            pipe.sadd(correlation_key, subscription.subscription_id)
            pipe.expire(correlation_key, self.ttl)
        
        if subscription.flow_id:
            flow_key = f"{self.key_prefix}flow:{subscription.flow_id}"
            pipe.sadd(flow_key, subscription.subscription_id)
            pipe.expire(flow_key, self.ttl)
        
        if subscription.node_id:
            node_key = f"{self.key_prefix}node:{subscription.node_id}"
            pipe.sadd(node_key, subscription.subscription_id)
            pipe.expire(node_key, self.ttl)
        
        await pipe.execute()
        return subscription.subscription_id
    
    async def get_subscription(self, subscription_id: str) -> Optional[EventSubscription]:
        """获取事件订阅"""
        await self.initialize()
        
        subscription_key = f"{self.key_prefix}data:{subscription_id}"
        data = await self.redis.get(subscription_key)
        
        if not data:
            return None
            
        try:
            return EventSubscription.model_validate_json(data)
        except Exception as e:
            raise EventError(f"解析订阅数据失败: {e}")
    
    async def list_subscriptions(self, 
                               event_type: Optional[str] = None,
                               correlation_id: Optional[str] = None,
                               flow_id: Optional[str] = None,
                               node_id: Optional[str] = None) -> List[EventSubscription]:
        """列出事件订阅"""
        await self.initialize()
        
        # 确定使用哪个索引
        if event_type:
            key = f"{self.key_prefix}type:{event_type}"
            subscription_ids = await self.redis.smembers(key)
        elif correlation_id:
            key = f"{self.key_prefix}correlation:{correlation_id}"
            subscription_ids = await self.redis.smembers(key)
        elif flow_id:
            key = f"{self.key_prefix}flow:{flow_id}"
            subscription_ids = await self.redis.smembers(key)
        elif node_id:
            key = f"{self.key_prefix}node:{node_id}"
            subscription_ids = await self.redis.smembers(key)
        else:
            data_keys = []
            cursor = 0
            while True:
                cursor, keys = await self.redis.scan(cursor, match=f"{self.key_prefix}data:*", count=100)
                data_keys.extend(keys)
                if cursor == 0:
                    break
            subscription_ids = [
                (key.decode('utf-8') if isinstance(key, bytes) else key).split(':')[-1]
                for key in data_keys
            ]
        
        # 处理空集合的情况
        if not subscription_ids:
            return []
            
        # 将set转换为list
        subscription_ids_list = list(subscription_ids)
        
        # 转换字节字符串为普通字符串
        str_ids = []
        for sid in subscription_ids_list:
            if isinstance(sid, bytes):
                str_ids.append(sid.decode('utf-8'))
            else:
                str_ids.append(sid)
        
        # 批量获取订阅数据
        result = []
        for subscription_id in str_ids:
            subscription = await self.get_subscription(subscription_id)
            if subscription:
                result.append(subscription)
        
        return result
    
    async def delete_subscription(self, subscription_id: str) -> bool:
        """删除事件订阅"""
        await self.initialize()
        
        # 获取订阅数据，以移除索引
        subscription = await self.get_subscription(subscription_id)
        if not subscription:
            return False
            
        # 从各索引中移除
        pipe = self.redis.pipeline()
        
        # 事件类型索引
        type_key = f"{self.key_prefix}type:{subscription.event_type}"
        pipe.srem(type_key, subscription_id)
        
        # 关联ID索引
        if subscription.correlation_id:
            correlation_key = f"{self.key_prefix}correlation:{subscription.correlation_id}"
            pipe.srem(correlation_key, subscription_id)
        
        # 流程ID索引
        if subscription.flow_id:
            flow_key = f"{self.key_prefix}flow:{subscription.flow_id}"
            pipe.srem(flow_key, subscription_id)
        
        # 节点ID索引
        if subscription.node_id:
            node_key = f"{self.key_prefix}node:{subscription.node_id}"
            pipe.srem(node_key, subscription_id)
        
        # 删除主数据
        subscription_key = f"{self.key_prefix}data:{subscription_id}"
        pipe.delete(subscription_key)
        
        # 执行所有操作
        results = await pipe.execute()
        
        # 最后一个结果是删除主数据的结果
        return results[-1] > 0
        
    async def mark_event_processed(self, subscription_id: str, event_id: str) -> bool:
        """原子操作：标记事件为已处理状态"""
        await self.initialize()
        
        # 获取订阅
        subscription = await self.get_subscription(subscription_id)
        if not subscription:
            return False
            
        # 标记事件为已处理
        subscription.mark_event_processed(event_id)
        
        # 更新订阅数据
        subscription_key = f"{self.key_prefix}data:{subscription_id}"
        await self.redis.set(subscription_key, subscription.model_dump_json())
        
        return True
    
    async def batch_mark_processed(self, subscription_id: str, event_ids: List[str]) -> bool:
        """批量标记事件为已处理"""
        await self.initialize()
        
        # 获取订阅
        subscription = await self.get_subscription(subscription_id)
        if not subscription:
            return False
            
        # 标记所有事件为已处理
        for event_id in event_ids:
            subscription.mark_event_processed(event_id)
        
        # 更新订阅数据
        subscription_key = f"{self.key_prefix}data:{subscription_id}"
        await self.redis.set(subscription_key, subscription.model_dump_json())
        
        return True
    
    async def close(self):
        """关闭 Redis 连接"""
        if self.redis:
            await self.redis.aclose()
            self.redis = None

    async def find_unprocessed_matching_subscriptions(self, event: Event) -> List[EventSubscription]:
        """查找未处理过此事件的匹配订阅，并原子地标记为已处理"""
        # 这个方法需要保证原子性，所以使用锁
        async with self.lock:
            await self.initialize()
            
            # 找出所有匹配的订阅
            matching_subscriptions = await self.find_matching_subscriptions(event)
            result = []
            
            # 使用管道批量更新
            pipe = self.redis.pipeline()
            
            for subscription in matching_subscriptions:
                if event.event_id not in subscription.processed_events:
                    # 标记为已处理
                    subscription.mark_event_processed(event.event_id)
                    
                    # 准备更新
                    subscription_key = f"{self.key_prefix}data:{subscription.subscription_id}"
                    pipe.set(subscription_key, subscription.model_dump_json())
                    
                    result.append(subscription)
            
            # 执行所有更新
            if result:
                await pipe.execute()
            
            return result


class RedisProcessingTracker(EventProcessingTracker):
    """基于Redis的事件处理记录跟踪器"""
    
    def __init__(self, redis_url: str, key_prefix: str = "plaita:processed:"):
        """
        初始化Redis事件处理记录
        
        Args:
            redis_url: Redis连接URL
            key_prefix: Redis键前缀
        """
        self.redis_url = redis_url
        self.key_prefix = key_prefix
        self.redis = None
    
    async def initialize(self):
        """初始化Redis连接"""
        if self.redis is None:
            self.redis = await aioredis.from_url(self.redis_url)
    
    async def mark_event_processed(self, event_id: str, handler_id: str) -> bool:
        """标记事件已由特定处理器处理"""
        await self.initialize()
        
        key = f"{self.key_prefix}events:{event_id}"
        timestamp = time.time()
        
        # 使用HSETNX确保只设置一次，返回1表示新设置，0表示已存在
        is_new = await self.redis.hsetnx(key, handler_id, str(timestamp))
        
        # 更新最后更新时间
        await self.redis.hset(key, "last_updated", str(timestamp))
        
        # 设置过期时间（默认7天）
        await self.redis.expire(key, 60 * 60 * 24 * 7)
        
        return bool(is_new)
    
    async def is_event_processed(self, event_id: str, handler_id: str) -> bool:
        """检查事件是否已由特定处理器处理"""
        await self.initialize()
        
        key = f"{self.key_prefix}events:{event_id}"
        result = await self.redis.hexists(key, handler_id)
        
        return bool(result)
    
    async def cleanup_old_records(self, max_age_seconds: int = 86400) -> int:
        """清理超过指定时间的处理记录"""
        await self.initialize()
        
        now = time.time()
        oldest_time = now - max_age_seconds
        
        pattern = f"{self.key_prefix}*"
        all_keys = []
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor, match=pattern, count=100)
            all_keys.extend(keys)
            if cursor == 0:
                break
        
        removed = 0
        
        for key in all_keys:
            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
            
            # 检查键类型是否为哈希表
            key_type = await self.redis.type(key)
            key_type_str = key_type.decode('utf-8') if isinstance(key_type, bytes) else key_type
            
            if key_type_str != 'hash':
                continue  # 跳过非哈希表类型的键
                
            try:
                # 安全地获取最后更新时间
                if not await self.redis.hexists(key, "last_updated"):
                    continue
                    
                last_updated_str = await self.redis.hget(key, "last_updated")
                last_updated = float(last_updated_str) if last_updated_str else None
                
                if last_updated and last_updated < oldest_time:
                    # 获取处理器IDs
                    handlers = {}
                    cursor = 0
                    
                    # 使用hscan替代hgetall来分批处理
                    while True:
                        cursor, items = await self.redis.hscan(key, cursor)
                        for hk, hv in items.items():
                            hk_str = hk.decode('utf-8') if isinstance(hk, bytes) else hk
                            if hk_str != "last_updated":
                                handlers[hk_str] = hv
                        
                        if cursor == 0:
                            break
                    
                    if handlers:
                        # 删除每个处理器的历史记录
                        for handler_id in handlers:
                            history_key = f"{self.key_prefix}history:{handler_id}"
                            await self.redis.delete(history_key)
                    
                    # 删除记录本身
                    await self.redis.delete(key)
                    removed += 1
            except (ValueError, TypeError) as e:
                # 处理解析错误
                print(f"清理记录时出错: {e}, key={key_str}")
        
        return removed
    
    async def close(self):
        """关闭 Redis 连接"""
        if self.redis:
            await self.redis.aclose()
            self.redis = None

    async def record_processing_attempt(self, event_id: str, handler_id: str, 
                                  status: str, error: Optional[str] = None) -> None:
        """记录处理尝试的历史"""
        await self.initialize()
        
        history_key = f"{self.key_prefix}history:{event_id}"
        timestamp = time.time()
        
        # 创建记录
        record = {
            "handler_id": handler_id,
            "timestamp": timestamp,
            "status": status
        }
        
        if error:
            record["error"] = error
            
        # 序列化记录
        record_json = json.dumps(record)
        
        # 新增历史记录时创建有序集合
        await self.redis.zadd(history_key, {record_json: timestamp})
        
        # 设置记录的过期时间
        await self.redis.expire(history_key, 86400)  # 24小时
    
    async def get_processing_history(self, event_id: str) -> List[Dict[str, Any]]:
        """获取事件的处理历史"""
        await self.initialize()
        
        history_key = f"{self.key_prefix}history:{event_id}"
        
        # 检查键是否存在
        if not await self.redis.exists(history_key):
            return []
            
        # 检查键类型
        key_type = await self.redis.type(history_key)
        key_type_str = key_type.decode('utf-8') if isinstance(key_type, bytes) else key_type
        
        # 根据存储类型读取数据
        result = []
        if key_type_str == 'zset':
            # 从有序集合读取
            items = await self.redis.zrange(history_key, 0, -1, withscores=True)
            for item, score in items:
                item_str = item.decode('utf-8') if isinstance(item, bytes) else item
                try:
                    record = json.loads(item_str)
                    result.append(record)
                except json.JSONDecodeError:
                    continue
        elif key_type_str == 'list':
            # 从列表读取 (兼容旧格式)
            items = await self.redis.lrange(history_key, 0, -1)
            for item in items:
                item_str = item.decode('utf-8') if isinstance(item, bytes) else item
                try:
                    record = json.loads(item_str)
                    result.append(record)
                except json.JSONDecodeError:
                    continue
        
        # 按时间戳排序
        result.sort(key=lambda x: x.get('timestamp', 0))
        return result


class RedisEventBus(EventBus):
    """
    基于Redis的事件总线实现
    """
    
    def __init__(self, redis_url: str, event_key_prefix: str = "plaita:event:", 
                subscription_key_prefix: str = "plaita:subscription:",
                processing_key_prefix: str = "plaita:processed:"):
        """
        初始化Redis事件总线
        
        Args:
            redis_url: Redis连接URL
            event_key_prefix: 事件存储的键前缀
            subscription_key_prefix: 订阅存储的键前缀
            processing_key_prefix: 处理记录的键前缀
        """
        self.redis_url = redis_url
        
        # 存储和记录
        self.event_storage = RedisEventStorage(redis_url, event_key_prefix)
        self.subscription_storage = RedisEventSubscriptionStorage(redis_url, subscription_key_prefix)
        self.processing_tracker = RedisProcessingTracker(redis_url, processing_key_prefix)
        
        # Redis pubsub对象
        self.redis = None
        self.pubsub = None
        
        # 事件类型到等待future的映射
        self.waiting_futures = {}
        
        # 事件监听器任务
        self.listeners = []

        # 事件处理器字典 {handler_id: (handler_func, event_type, filter_condition, retry_policy)}
        self.handlers = {}
        self.lock = asyncio.Lock()
    
    async def initialize(self):
        """初始化Redis连接"""
        if self.redis is None:
            self.redis = await aioredis.from_url(self.redis_url)
            self.pubsub = self.redis.pubsub()
    
    async def publish(self, event: Union[Event, str, Dict[str, Any]], 
                    prevent_duplicate_consumption: bool = True,
                    **kwargs) -> str:
        """发布事件"""
        await self.initialize()
        
        # 标准化事件对象
        if isinstance(event, str):
            event = Event(event_type=event, data=kwargs)
        elif isinstance(event, dict):
            if 'event_type' not in event:
                raise ValueError("事件字典必须包含event_type字段")
                
            event = dict(event)
            event_type = event.pop('event_type')
            event = Event(event_type=event_type, data=event)
        
        # 存储事件
        event_id = await self.event_storage.store_event(event)
        
        # 使用PubSub发布事件通知
        channel = f"plaita:events:{event.event_type}"
        await self.redis.publish(channel, event.model_dump_json())
        
        # 通知等待的future
        if event.event_type in self.waiting_futures:
            futures = self.waiting_futures[event.event_type]
            for future in futures:
                if not future.done():
                    future.set_result(event)
            # 清理已完成的future
            self.waiting_futures[event.event_type] = [f for f in futures if not f.done()]
        
        return event_id
    
    async def batch_publish(self, events: List[Union[Event, str, Dict[str, Any]]],
                          prevent_duplicate_consumption: bool = True) -> List[str]:
        """批量发布事件"""
        await self.initialize()
        
        normalized_events = []
        
        # 标准化所有事件
        for event in events:
            if isinstance(event, str):
                normalized_events.append(Event(event_type=event, data={}))
            elif isinstance(event, dict):
                if 'event_type' not in event:
                    raise ValueError("事件字典必须包含event_type字段")
                event = dict(event)
                event_type = event.pop('event_type')
                normalized_events.append(Event(event_type=event_type, data=event))
            else:
                normalized_events.append(event)
        
        # 批量存储事件
        event_ids = await self.event_storage.batch_store_events(normalized_events)
        
        # 使用管道批量发布通知
        pipe = self.redis.pipeline()
        
        for event in normalized_events:
            channel = f"plaita:events:{event.event_type}"
            pipe.publish(channel, event.model_dump_json())
            
            # 通知等待的future
            if event.event_type in self.waiting_futures:
                futures = self.waiting_futures[event.event_type]
                for future in futures:
                    if not future.done():
                        future.set_result(event)
                # 清理已完成的future
                self.waiting_futures[event.event_type] = [f for f in futures if not f.done()]
        
        # 执行所有发布
        await pipe.execute()
        
        return event_ids
    
    async def register_subscription(self, 
                                  event_type: str, 
                                  filter_condition: Optional[Dict[str, Any]] = None,
                                  correlation_id: Optional[str] = None,
                                  flow_id: Optional[str] = None,
                                  node_id: Optional[str] = None,
                                  timeout: Optional[float] = None) -> str:
        """注册事件订阅"""
        subscription = EventSubscription(
            event_type=event_type,
            filter_condition=filter_condition or {},
            correlation_id=correlation_id,
            flow_id=flow_id,
            node_id=node_id,
            timeout=timeout
        )
        
        subscription_id = await self.subscription_storage.store_subscription(subscription)
        return subscription_id
    
    async def unregister_subscription(self, subscription_id: str) -> bool:
        """取消事件订阅"""
        return await self.subscription_storage.delete_subscription(subscription_id)
    
    async def wait_for_event(self, event_type: str, 
                           timeout: Optional[float] = None,
                           condition: Optional[Callable[[Event], bool]] = None) -> Event:
        """等待特定类型的事件发生"""
        await self.initialize()
        
        # 创建future用于等待事件
        future = asyncio.get_running_loop().create_future()
        
        if event_type not in self.waiting_futures:
            self.waiting_futures[event_type] = []
        
        self.waiting_futures[event_type].append(future)
        
        # 创建一个监听器来接收事件
        async def listen_for_event():
            try:
                # 订阅事件类型对应的频道
                await self.pubsub.subscribe(f"plaita:events:{event_type}")
                
                while True:
                    # 接收消息
                    message = await self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if message and message['type'] == 'message':
                        try:
                            # 解析事件数据
                            event_data = message['data']
                            event = Event.model_validate_json(event_data)
                            
                            # 检查条件
                            if not condition or condition(event):
                                if not future.done():
                                    future.set_result(event)
                                break
                        except Exception as e:
                            print(f"解析事件数据失败: {e}")
                    
                    # 检查future是否已完成（可能来自其他来源）
                    if future.done():
                        break
            finally:
                # 取消订阅
                await self.pubsub.unsubscribe(f"plaita:events:{event_type}")
        
        # 启动监听器
        listen_task = asyncio.create_task(listen_for_event())
        
        try:
            if timeout:
                # 带超时等待
                event = await asyncio.wait_for(future, timeout=timeout)
            else:
                # 无限等待
                event = await future
                
            return event
        except asyncio.TimeoutError:
            # 超时时从等待列表中移除
            if event_type in self.waiting_futures:
                self.waiting_futures[event_type].remove(future)
            raise EventTimeoutError(event_type, timeout)
        finally:
            # 取消监听任务
            if not listen_task.done():
                listen_task.cancel()
    
    async def register_handler(self, event_type: Optional[str] = None, 
                             handler: EventHandler = None,
                             filter_condition: Optional[Dict[str, Any]] = None,
                             retry_policy: Optional[RetryPolicy] = None) -> str:
        """注册事件处理器"""
        await self.initialize()
        
        handler_id = str(uuid.uuid4())
        
        async with self.lock:
            self.handlers[handler_id] = (handler, event_type, filter_condition, retry_policy)
            
            # 根据event_type决定监听策略
            if not event_type or event_type == "*":
                # 监听所有事件 - 使用模式订阅
                listener_task = asyncio.create_task(
                    self._listen_for_all_events(handler_id)
                )
            elif "*" in event_type:
                # 包含通配符 - 使用模式订阅  
                listener_task = asyncio.create_task(
                    self._listen_for_pattern_events(event_type, handler_id)
                )
            else:
                # 精确匹配 - 使用普通订阅
                listener_task = asyncio.create_task(
                    self._listen_for_events(event_type, handler_id)
                )
            
            self.listeners.append(listener_task)
        
        return handler_id
    
    async def unregister_handler(self, handler_id: str) -> bool:
        """取消注册事件处理器"""
        async with self.lock:
            if handler_id in self.handlers:
                del self.handlers[handler_id]
                return True
            return False
    
    async def _listen_for_events(self, event_type: str, handler_id: str):
        """监听特定类型的事件并分发到处理器"""
        await self.initialize()
        pubsub = self.redis.pubsub()
        
        try:
            await pubsub.subscribe(f"plaita:events:{event_type}")
            
            while handler_id in self.handlers:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message['type'] == 'message':
                    await self._process_message(message, handler_id)
        finally:
            await pubsub.unsubscribe(f"plaita:events:{event_type}")
    
    async def _listen_for_all_events(self, handler_id: str):
        """监听所有事件类型"""
        await self.initialize()
        pubsub = self.redis.pubsub()
        
        try:
            # 使用模式订阅监听所有事件
            await pubsub.psubscribe("plaita:events:*")
            
            while handler_id in self.handlers:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message['type'] == 'pmessage':
                    await self._process_message(message, handler_id)
        finally:
            await pubsub.punsubscribe("plaita:events:*")
    
    async def _listen_for_pattern_events(self, event_pattern: str, handler_id: str):
        """监听模式匹配的事件类型"""
        await self.initialize()
        pubsub = self.redis.pubsub()
        
        try:
            # 转换fnmatch模式为Redis模式（简单的*映射）
            redis_pattern = f"plaita:events:{event_pattern}"
            await pubsub.psubscribe(redis_pattern)
            
            while handler_id in self.handlers:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message['type'] == 'pmessage':
                    await self._process_message(message, handler_id)
        finally:
            await pubsub.punsubscribe(redis_pattern)
    
    async def _process_message(self, message, handler_id: str):
        """处理收到的消息"""
        try:
            # 解析事件数据
            event_data = message['data']
            event = Event.model_validate_json(event_data)
            
            # 获取处理器信息
            handler_info = self.handlers.get(handler_id)
            if not handler_info:
                return
                
            handler, event_type, filter_condition, retry_policy = handler_info
            
            # 使用统一的通配符匹配逻辑
            if not EventBus.matches_event_type(event_type, event.event_type):
                return
            
            # 检查过滤条件
            if filter_condition:
                subscription = EventSubscription(
                    event_type=event_type or "*",
                    filter_condition=filter_condition
                )
                if not subscription.matches_event(event, {}):
                    return
            
            # 检查是否已处理
            is_new = await self.processing_tracker.mark_event_processed(event.event_id, handler_id)
            if not is_new:
                return
            
            # 处理事件
            if retry_policy:
                asyncio.create_task(self._process_with_retry(handler, event, handler_id, retry_policy))
            else:
                asyncio.create_task(self._process_event(handler, event, handler_id))
                
        except Exception as e:
            print(f"处理事件消息失败: {e}")
    
    async def _process_event(self, handler: EventHandler, event: Event, handler_id: str) -> None:
        """处理事件并记录结果"""
        try:
            await handler(event)
            await self.processing_tracker.record_processing_attempt(
                event.event_id, handler_id, "success"
            )
        except Exception as e:
            await self.processing_tracker.record_processing_attempt(
                event.event_id, handler_id, "error", str(e)
            )
    
    async def _process_with_retry(self, handler: EventHandler, event: Event, 
                                handler_id: str, retry_policy: RetryPolicy) -> None:
        """带重试机制的事件处理"""
        retries = 0
        delay = retry_policy.initial_delay
        
        while True:
            try:
                await handler(event)
                await self.processing_tracker.record_processing_attempt(
                    event.event_id, handler_id, "success"
                )
                break  # 成功处理，退出循环
            except Exception as e:
                retries += 1
                await self.processing_tracker.record_processing_attempt(
                    event.event_id, handler_id, f"error (retry {retries})", str(e)
                )
                
                # 检查是否达到最大重试次数
                if retries >= retry_policy.max_retries:
                    await self.processing_tracker.record_processing_attempt(
                        event.event_id, handler_id, "failed", f"达到最大重试次数 ({retry_policy.max_retries})"
                    )
                    break
                
                # 计算下次重试的延迟
                delay = min(delay * retry_policy.backoff_factor, retry_policy.max_delay)
                await asyncio.sleep(delay)
    
    async def close(self):
        """关闭所有子组件的 Redis 连接"""
        for listener in self.listeners:
            if not listener.done():
                listener.cancel()
        self.listeners.clear()
        
        if self.pubsub:
            await self.pubsub.aclose()
            self.pubsub = None
        if self.redis:
            await self.redis.aclose()
            self.redis = None
        
        await self.event_storage.close()
        await self.subscription_storage.close()
        await self.processing_tracker.close()

    async def get_event(self, event_id: str) -> Event:
        """获取事件"""
        event = await self.event_storage.get_event(event_id)
        if not event:
            raise EventNotFoundError(f"事件 {event_id} 不存在")
        return event 