"""
基于内存的事件总线和存储实现

内存总线语义（与 Redis/SQLAlchemy 后端不同，使用前请知悉）：
- ``wait_for_event`` 只能看到**注册之后**发布的事件——未来先注册、后 publish；
  先发布的事件不会回放（即使还在 event_storage 里）。
- ``register_subscription`` 只把订阅写入订阅存储，**不驱动任何分发**；
  总线分发只认 ``register_handler``。订阅记录供 EventFilter 等外部组件
  按 correlation_id 检索消费。
- ``publish`` 是 fire-and-forget：handler 在独立 task 中异步分发。
  publish 返回后立即关闭事件循环，**不保证** handler 已经执行。
"""
import asyncio
import time
import uuid
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set, Union, Awaitable

from .core import (
    Event, EventBus, EventHandler, EventStorage, EventSubscription, 
    EventSubscriptionStorage, EventProcessingTracker, RetryPolicy
)
from .exceptions import EventError, EventNotFoundError, EventTimeoutError
from .utils import normalize_event

from plaita.logger import logger

class MemoryEventStorage(EventStorage):
    """
    基于内存的事件存储实现
    """
    def __init__(self):
        self.events: Dict[str, Event] = {}
        self.event_types: Dict[str, List[str]] = defaultdict(list)
        self._lock: Optional[asyncio.Lock] = None

    @property
    def lock(self) -> asyncio.Lock:
        # Lazily create the lock inside an async context (Python 3.9 compat).
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock
    
    async def store_event(self, event: Event) -> str:
        """存储事件"""
        async with self.lock:
            self.events[event.event_id] = event
            self.event_types[event.event_type].append(event.event_id)
            return event.event_id
    
    async def get_event(self, event_id: str) -> Optional[Event]:
        """获取事件"""
        async with self.lock:
            return self.events.get(event_id)
    
    async def list_events(self, event_type: Optional[str] = None, 
                         start_time: Optional[float] = None,
                         end_time: Optional[float] = None,
                         limit: int = 100) -> List[Event]:
        """列出事件"""
        async with self.lock:
            result = []
            event_ids = []
            
            if event_type:
                event_ids = self.event_types.get(event_type, [])
            else:
                event_ids = list(self.events.keys())
            
            for event_id in event_ids:
                if len(result) >= limit:
                    break
                    
                event = self.events.get(event_id)
                if not event:
                    continue
                    
                if start_time and event.timestamp < start_time:
                    continue
                    
                if end_time and event.timestamp > end_time:
                    continue
                    
                result.append(event)
            
            return result
    
    async def delete_event(self, event_id: str) -> bool:
        """删除事件"""
        async with self.lock:
            if event_id not in self.events:
                return False
                
            event = self.events.pop(event_id)
            if event.event_id in self.event_types.get(event.event_type, []):
                self.event_types[event.event_type].remove(event.event_id)
            
            return True
    
    async def batch_store_events(self, events: List[Event]) -> List[str]:
        """批量存储事件（优化实现）"""
        async with self.lock:
            event_ids = []
            for event in events:
                self.events[event.event_id] = event
                self.event_types[event.event_type].append(event.event_id)
                event_ids.append(event.event_id)
            return event_ids


class InMemoryEventSubscriptionStorage(EventSubscriptionStorage):
    """
    内存中的事件订阅存储实现
    """
    def __init__(self):
        self.subscriptions: Dict[str, EventSubscription] = {}
        self._lock: Optional[asyncio.Lock] = None

    @property
    def lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def store_subscription(self, subscription: EventSubscription) -> str:
        async with self.lock:
            self.subscriptions[subscription.subscription_id] = subscription
            return subscription.subscription_id
    
    async def get_subscription(self, subscription_id: str) -> Optional[EventSubscription]:
        async with self.lock:
            return self.subscriptions.get(subscription_id)
    
    async def list_subscriptions(self, 
                               event_type: Optional[str] = None,
                               correlation_id: Optional[str] = None,
                               flow_id: Optional[str] = None,
                               node_id: Optional[str] = None) -> List[EventSubscription]:
        result = []
        async with self.lock:
            for subscription in self.subscriptions.values():
                # 事件类型过滤
                if event_type and subscription.event_type != event_type:
                    continue
                
                # 关联ID过滤
                if correlation_id and subscription.correlation_id != correlation_id:
                    continue
                
                # 流程ID过滤
                if flow_id and subscription.flow_id != flow_id:
                    continue
                
                # 节点ID过滤
                if node_id and subscription.node_id != node_id:
                    continue
                
                result.append(subscription)
        
        return result
    
    async def delete_subscription(self, subscription_id: str) -> bool:
        async with self.lock:
            if subscription_id in self.subscriptions:
                del self.subscriptions[subscription_id]
                return True
            return False
        
    async def mark_event_processed(self, subscription_id: str, event_id: str) -> bool:
        """标记事件为已处理状态"""
        async with self.lock:
            if subscription_id not in self.subscriptions:
                return False
                
            subscription = self.subscriptions[subscription_id]
            subscription.mark_event_processed(event_id)
            return True
    
    async def batch_mark_processed(self, subscription_id: str, event_ids: List[str]) -> bool:
        """批量标记事件为已处理（优化实现）"""
        async with self.lock:
            if subscription_id not in self.subscriptions:
                return False
                
            subscription = self.subscriptions[subscription_id]
            for event_id in event_ids:
                subscription.mark_event_processed(event_id)
            return True
    
    async def find_unprocessed_matching_subscriptions(self, event: Event) -> List[EventSubscription]:
        """查找未处理此事件的匹配订阅"""
        result = []
        async with self.lock:
            for subscription in self.subscriptions.values():
                if subscription.matches_event(event, {}) and not subscription.is_event_processed(event.event_id):
                    subscription.mark_event_processed(event.event_id)
                    result.append(subscription)
        
        return result


class InMemoryProcessingTracker(EventProcessingTracker):
    """内存中的事件处理记录跟踪器"""
    
    def __init__(self):
        # 存储格式: {event_id: {handler_id: timestamp}}
        self.processed_records: Dict[str, Dict[str, float]] = {}
        # 处理历史记录: {event_id: [{handler_id, status, timestamp, error}]}
        self.processing_history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._lock: Optional[asyncio.Lock] = None

    @property
    def lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock
    
    async def mark_event_processed(self, event_id: str, handler_id: str) -> bool:
        """标记事件为已处理状态"""
        async with self.lock:
            if event_id not in self.processed_records:
                self.processed_records[event_id] = {}
                
            if handler_id in self.processed_records[event_id]:
                return False  # 已处理过
            
            self.processed_records[event_id][handler_id] = time.time()
            return True  # 成功标记
            
    async def is_event_processed(self, event_id: str, handler_id: str) -> bool:
        """检查事件是否已被处理"""
        return (event_id in self.processed_records and 
                handler_id in self.processed_records[event_id])
    
    async def cleanup_old_records(self, max_age_seconds: int = 86400) -> int:
        """清理旧的处理记录"""
        now = time.time()
        count = 0
        
        async with self.lock:
            for event_id in list(self.processed_records.keys()):
                handlers = list(self.processed_records[event_id].keys())
                for handler_id in handlers:
                    timestamp = self.processed_records[event_id][handler_id]
                    if now - timestamp > max_age_seconds:
                        del self.processed_records[event_id][handler_id]
                        count += 1
                
                # 如果事件没有任何处理记录，则删除整个事件记录
                if not self.processed_records[event_id]:
                    del self.processed_records[event_id]
            
            # 清理历史记录
            for event_id in list(self.processing_history.keys()):
                self.processing_history[event_id] = [
                    record for record in self.processing_history[event_id]
                    if now - record['timestamp'] <= max_age_seconds
                ]
                
                if not self.processing_history[event_id]:
                    del self.processing_history[event_id]
                    
        return count
    
    async def record_processing_attempt(self, event_id: str, handler_id: str, 
                                      status: str, error: Optional[str] = None) -> None:
        """记录处理尝试，包括成功和失败"""
        async with self.lock:
            self.processing_history[event_id].append({
                'handler_id': handler_id,
                'status': status,
                'timestamp': time.time(),
                'error': error
            })
    
    async def get_processing_history(self, event_id: str) -> List[Dict[str, Any]]:
        """获取事件的处理历史"""
        return self.processing_history.get(event_id, [])


class InMemoryEventBus(EventBus):
    """
    内存中的事件总线实现
    """
    def __init__(self):
        """
        初始化内存事件总线
        """
        # fire-and-forget 分发任务的强引用集合（见 _track_task）
        self._pending_tasks: set = set()
        # 存储等待特定事件的future
        self.waiting_futures: Dict[str, List[asyncio.Future]] = {}
        # 事件存储
        self.event_storage = MemoryEventStorage()
        # 事件订阅存储
        self.subscription_storage = InMemoryEventSubscriptionStorage()
        # 事件处理记录
        self.processing_tracker = InMemoryProcessingTracker()
        # 事件处理器
        self.handlers = {}
        # 处理器与事件类型映射关系
        self.handler_event_types = defaultdict(list)
        # 处理器重试策略
        self.handler_retry_policies = {}
        # 处理器过滤条件
        self.handler_filters = {}
        # 线程安全锁（懒初始化，Python 3.9 兼容）
        self._lock: Optional[asyncio.Lock] = None

    @property
    def lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _track_task(self, task: "asyncio.Task") -> None:
        """持住 fire-and-forget 分发任务的强引用。

        裸 ``create_task`` 的任务没有任何引用时可能被 GC 中途取消，且 loop
        关闭竞态下静默不执行（仅 'Task was destroyed' 警告）。完成回调自动
        从集合丢弃，不阻碍回收。
        """
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def publish(self, event: Union[Event, str, Dict[str, Any]],
                    prevent_duplicate_consumption: bool = True,
                    **kwargs) -> str:
        """发布事件"""
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
        await self.event_storage.store_event(event)
        
        # 通知等待的future
        if event.event_type in self.waiting_futures:
            futures = self.waiting_futures[event.event_type]
            for future in futures:
                if not future.done():
                    future.set_result(event)
            # 清理已完成的future
            self.waiting_futures[event.event_type] = [f for f in futures if not f.done()]
        
        # 分发事件到所有匹配的处理器
        self._track_task(asyncio.create_task(self._dispatch_event(event, prevent_duplicate_consumption)))
        
        return event.event_id
    
    async def batch_publish(self, events: List[Union[Event, str, Dict[str, Any]]],
                          prevent_duplicate_consumption: bool = True) -> List[str]:
        """批量发布事件（优化实现）"""
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
        
        # 分发所有事件
        for event in normalized_events:
            # 通知等待的future
            if event.event_type in self.waiting_futures:
                futures = self.waiting_futures[event.event_type]
                for future in futures:
                    if not future.done():
                        future.set_result(event)
                # 清理已完成的future
                self.waiting_futures[event.event_type] = [f for f in futures if not f.done()]
                
            # 分发事件
            self._track_task(asyncio.create_task(self._dispatch_event(event, prevent_duplicate_consumption)))
        
        return event_ids
    
    async def _dispatch_event(self, event: Event, prevent_duplicate_consumption: bool) -> None:
        """将事件分发到所有匹配的处理器"""
        async with self.lock:
            # 遍历所有处理器
            handlers_to_process = []
            for handler_id, (handler, event_type, filter_condition, retry_policy) in self.handlers.items():
                # 使用统一的通配符匹配逻辑
                if not EventBus.matches_event_type(event_type, event.event_type):
                    continue
                
                # 检查过滤条件
                if filter_condition:
                    subscription = EventSubscription(
                        event_type=event_type or "*",
                        filter_condition=filter_condition
                    )
                    if not subscription.matches_event(event, {}):
                        continue
                
                # 去重：仅跳过已成功处理过的 (event, handler)。
                # 标记在 handler 成功之后写入，避免「先 mark 再执行 → 失败永久丢事件」。
                if prevent_duplicate_consumption:
                    if await self.processing_tracker.is_event_processed(event.event_id, handler_id):
                        continue
                
                # 添加到要处理的处理器列表
                handlers_to_process.append((handler, handler_id, retry_policy))
        
        # 在锁外处理事件，避免长时间占用锁
        for handler, handler_id, retry_policy in handlers_to_process:
            # 创建处理任务
            if retry_policy:
                # 使用一个单独的任务来处理重试，但确保它被等待
                task = asyncio.create_task(self._process_with_retry(handler, event, handler_id, retry_policy))
                # 添加完成回调处理异常
                task.add_done_callback(lambda t: t.exception() if t.done() and not t.cancelled() else None)
            else:
                # 直接处理，不创建新任务
                try:
                    await self._process_event(handler, event, handler_id)
                except Exception as e:
                    logger.error("处理事件 %s 出错: %s", event.event_id, e, exc_info=True)
    
    async def _process_event(self, handler: EventHandler, event: Event, handler_id: str) -> None:
        """处理事件并记录结果；成功后才 mark 去重。"""
        try:
            import asyncio
            import inspect
            
            # 检查处理器是否是协程函数
            if inspect.iscoroutinefunction(handler):
                # 异步处理器
                logger.info("异步处理器: %s 处理事件: %s", handler_id, event.event_id)
                await handler(event)
            else:
                # 同步处理器，在执行器中运行
                loop = asyncio.get_running_loop()
                logger.info("同步处理器: %s 处理事件: %s", handler_id, event.event_id)
                await loop.run_in_executor(None, handler, event)
                
            await self.processing_tracker.mark_event_processed(event.event_id, handler_id)
            await self.processing_tracker.record_processing_attempt(
                event.event_id, handler_id, "success"
            )
        except Exception as e:
            await self.processing_tracker.record_processing_attempt(
                event.event_id, handler_id, "error", str(e)
            )
            # 与历史行为一致：记录错误后不向上抛（调用方已有 try/except 兜底）
    
    async def _process_with_retry(self, handler: EventHandler, event: Event, 
                                handler_id: str, retry_policy: RetryPolicy) -> None:
        """带重试机制的事件处理"""
        retries = 0
        delay = retry_policy.initial_delay
        
        while True:
            try:
                import asyncio
                import inspect
                
                # 检查处理器是否是协程函数
                if inspect.iscoroutinefunction(handler):
                    # 异步处理器
                    await handler(event)
                else:
                    # 同步处理器，在执行器中运行
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, handler, event)
                    
                await self.processing_tracker.mark_event_processed(event.event_id, handler_id)
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
        deadline = (time.time() + timeout) if timeout else None
        
        while True:
            remaining = (deadline - time.time()) if deadline else None
            if remaining is not None and remaining <= 0:
                raise EventTimeoutError(event_type, timeout)
            
            future = asyncio.get_running_loop().create_future()
            
            if event_type not in self.waiting_futures:
                self.waiting_futures[event_type] = []
            self.waiting_futures[event_type].append(future)
            
            try:
                if remaining is not None:
                    event = await asyncio.wait_for(future, timeout=remaining)
                else:
                    event = await future
                    
                if condition and not condition(event):
                    continue
                    
                return event
            except asyncio.TimeoutError:
                if event_type in self.waiting_futures and future in self.waiting_futures[event_type]:
                    self.waiting_futures[event_type].remove(future)
                raise EventTimeoutError(event_type, timeout)
    
    async def register_handler(self, event_type: Optional[str] = None, 
                            handler: EventHandler = None,
                            filter_condition: Optional[Dict[str, Any]] = None,
                            retry_policy: Optional[RetryPolicy] = None) -> str:
        """注册事件处理器"""
        handler_id = str(uuid.uuid4())
        
        async with self.lock:
            self.handlers[handler_id] = (handler, event_type, filter_condition, retry_policy)
        
        return handler_id
    
    async def unregister_handler(self, handler_id: str) -> bool:
        """取消注册事件处理器"""
        async with self.lock:
            if handler_id in self.handlers:
                del self.handlers[handler_id]
                return True
            return False
    
    async def get_event(self, event_id: str) -> Event:
        """获取事件"""
        event = await self.event_storage.get_event(event_id)
        if not event:
            raise EventNotFoundError(f"事件 {event_id} 不存在")
        return event 