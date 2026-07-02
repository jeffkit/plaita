"""
事件系统的核心定义和接口
"""
import asyncio
import time
import uuid
import logging
import fnmatch
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Set, Union

from pydantic import BaseModel, Field

# sync/async 桥接工具下沉到 core 层, 避免 core → event 反向依赖。
# 这里保留旧名做向后兼容 (历史上有调用方 import _run_async_from_sync)。
from plaita.core.async_utils import run_async_from_sync as _run_async_from_sync

# 获取logger
logger = logging.getLogger("plaita.event")

class Event(BaseModel):
    """
    事件对象，包含事件类型、数据和元数据
    """
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    data: Dict[str, Any]
    timestamp: float = Field(default_factory=time.time)
    source: str = ""
    correlation_id: Optional[str] = None


class EventSubscription(BaseModel):
    """
    事件订阅信息
    """
    subscription_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    filter_condition: Optional[Dict[str, Any]] = None
    correlation_id: Optional[str] = None
    flow_id: Optional[str] = None
    node_id: Optional[str] = None
    created_at: float = Field(default_factory=time.time)
    timeout: Optional[float] = None
    processed_events: Set[str] = Field(default_factory=set)
    
    def mark_event_processed(self, event_id: str) -> None:
        """Mark an event as processed by this subscription."""
        self.processed_events.add(event_id)
    
    def is_event_processed(self, event_id: str) -> bool:
        """Check if an event has already been processed."""
        return event_id in self.processed_events
    
    def matches_event(self, event: Event, context: Dict[str, Any]) -> bool:
        """检查事件是否匹配此订阅"""

        # 检查事件类型
        if self.event_type and event.event_type != self.event_type:
            return False
        
        # 检查correlation_id
        if self.correlation_id and event.correlation_id != self.correlation_id:
            return False
        
        express_prefix = context.get("EXPRESS_PREFIX", "$")
        flow_id = context.get(f"{express_prefix}FLOW_ID")
        node_id = context.get(f"{express_prefix}LAST_NODE")
        # 检查flow_id,在context中
        if self.flow_id and flow_id and self.flow_id != flow_id:
            return False
        
        # 检查node_id,在context中
        if self.node_id and node_id and self.node_id != node_id:
            return False
        
        # copy一份context
        context_copy = context.copy()
        context_copy[f"{express_prefix}EVENT_DATA"] = event.data

        # 检查过滤条件 - 暂时简化为基本字典匹配
        if self.filter_condition:
            for key, expected_value in self.filter_condition.items():
                if key in event.data:
                    if event.data[key] != expected_value:
                        return False
                elif hasattr(event, key):
                    if getattr(event, key) != expected_value:
                        return False
                else:
                    # 如果事件中没有指定的字段，则不匹配
                    return False
        
        return True


# 定义处理器类型
EventHandler = Callable[[Event], Any]


class EventStorage(ABC):
    """
    事件存储接口，定义了如何存储和检索事件
    """
    @abstractmethod
    async def store_event(self, event: Event) -> str:
        """存储事件并返回事件ID"""
        pass
    
    @abstractmethod
    async def get_event(self, event_id: str) -> Optional[Event]:
        """根据ID获取事件"""
        pass
    
    @abstractmethod
    async def list_events(self, 
                         event_type: Optional[str] = None, 
                         start_time: Optional[float] = None,
                         end_time: Optional[float] = None,
                         limit: int = 100) -> List[Event]:
        """列出符合条件的事件"""
        pass
    
    @abstractmethod
    async def delete_event(self, event_id: str) -> bool:
        """删除事件"""
        pass
    
    async def batch_store_events(self, events: List[Event]) -> List[str]:
        """批量存储事件（默认实现，可被覆盖以提高性能）"""
        event_ids = []
        for event in events:
            event_id = await self.store_event(event)
            event_ids.append(event_id)
        return event_ids


class EventSubscriptionStorage(ABC):
    """
    事件订阅存储接口，定义了如何存储和检索事件订阅信息
    """
    @abstractmethod
    async def store_subscription(self, subscription: EventSubscription) -> str:
        """存储事件订阅并返回订阅ID"""
        pass
    
    @abstractmethod
    async def get_subscription(self, subscription_id: str) -> Optional[EventSubscription]:
        """根据ID获取事件订阅"""
        pass
    
    @abstractmethod
    async def list_subscriptions(self, 
                               event_type: Optional[str] = None,
                               correlation_id: Optional[str] = None,
                               flow_id: Optional[str] = None,
                               node_id: Optional[str] = None) -> List[EventSubscription]:
        """列出符合条件的事件订阅"""
        pass
    
    @abstractmethod
    async def delete_subscription(self, subscription_id: str) -> bool:
        """删除事件订阅"""
        pass
    

    async def find_matching_subscriptions(self, event: Event, context: Dict[str, Any] = None) -> List[EventSubscription]:
        """查找与事件匹配的所有订阅"""
        subscriptions = await self.list_subscriptions(correlation_id=event.correlation_id, event_type=event.event_type)
        return [sub for sub in subscriptions if sub.matches_event(event, context)]
        
    @abstractmethod
    async def mark_event_processed(self, subscription_id: str, event_id: str) -> bool:
        """原子操作：标记事件为已处理状态"""
        pass
    
    async def batch_mark_processed(self, subscription_id: str, event_ids: List[str]) -> bool:
        """批量标记事件为已处理（默认实现）"""
        results = []
        for event_id in event_ids:
            result = await self.mark_event_processed(subscription_id, event_id)
            results.append(result)
        return all(results)


# 统一的事件处理记录接口
class EventProcessingTracker(ABC):
    """统一的事件处理记录接口，用于全局去重和状态追踪"""
    
    @abstractmethod
    async def mark_event_processed(self, event_id: str, handler_id: str) -> bool:
        """标记事件为已处理状态"""
        pass
    
    @abstractmethod
    async def is_event_processed(self, event_id: str, handler_id: str) -> bool:
        """检查事件是否已被处理"""
        pass
    
    @abstractmethod
    async def cleanup_old_records(self, max_age_seconds: int = 86400) -> int:
        """清理旧的处理记录"""
        pass
    
    @abstractmethod
    async def record_processing_attempt(self, event_id: str, handler_id: str, 
                                      status: str, error: Optional[str] = None) -> None:
        """记录处理尝试，包括成功和失败"""
        pass
    
    @abstractmethod
    async def get_processing_history(self, event_id: str) -> List[Dict[str, Any]]:
        """获取事件的处理历史"""
        pass


class RetryPolicy(BaseModel):
    """事件处理重试策略"""
    max_retries: int = 3
    initial_delay: float = 1.0  # 秒
    backoff_factor: float = 2.0  # 退避因子
    max_delay: float = 60.0  # 最大延迟


class EventBus(ABC):
    """
    事件总线接口，定义了事件发布和订阅的核心功能
    """
    
    @staticmethod
    def matches_event_type(handler_event_type: Optional[str], actual_event_type: str) -> bool:
        """
        检查事件类型是否匹配处理器的事件类型模式
        
        支持的匹配规则：
        1. None 或 "*" - 匹配所有事件类型
        2. 精确匹配 - "user.login" 只匹配 "user.login"
        3. 前缀通配符 - "user.*" 匹配 "user.login", "user.logout" 等
        4. 后缀通配符 - "*.login" 匹配 "user.login", "admin.login" 等
        5. 中间通配符 - "*.user.*" 匹配 "app.user.login", "sys.user.update" 等
        
        Args:
            handler_event_type: 处理器注册的事件类型模式
            actual_event_type: 实际事件类型
            
        Returns:
            bool: 是否匹配
        """
        # None 或 "*" 匹配所有事件
        if not handler_event_type or handler_event_type == "*":
            return True
        
        # 精确匹配
        if handler_event_type == actual_event_type:
            return True
        
        # 通配符匹配，使用fnmatch实现Unix shell风格的通配符
        return fnmatch.fnmatch(actual_event_type, handler_event_type)
    
    @abstractmethod
    async def publish(self, event: Union[Event, str, Dict[str, Any]], 
                    prevent_duplicate_consumption: bool = True, 
                    **kwargs) -> str:
        """发布事件"""
        pass
    
    async def batch_publish(self, events: List[Union[Event, str, Dict[str, Any]]],
                          prevent_duplicate_consumption: bool = True) -> List[str]:
        """批量发布事件（默认实现）"""
        event_ids = []
        for event in events:
            event_id = await self.publish(event, prevent_duplicate_consumption)
            event_ids.append(event_id)
        return event_ids
    
    @abstractmethod
    async def register_subscription(self, 
                                  event_type: str, 
                                  filter_condition: Optional[Dict[str, Any]] = None,
                                  correlation_id: Optional[str] = None,
                                  flow_id: Optional[str] = None,
                                  node_id: Optional[str] = None,
                                  timeout: Optional[float] = None) -> str:
        """注册事件订阅信息"""
        pass
    
    @abstractmethod
    async def unregister_subscription(self, subscription_id: str) -> bool:
        """取消事件订阅"""
        pass
    
    @abstractmethod
    async def wait_for_event(self, event_type: str, 
                           timeout: Optional[float] = None,
                           condition: Optional[Callable[[Event], bool]] = None) -> Event:
        """等待特定类型的事件发生"""
        pass
    
    @abstractmethod
    async def register_handler(self, event_type: Optional[str] = None, 
                             handler: EventHandler = None,
                             filter_condition: Optional[Dict[str, Any]] = None,
                             retry_policy: Optional[RetryPolicy] = None) -> str:
        """
        注册事件处理器
        
        Args:
            event_type: 事件类型模式，支持通配符：
                       - None 或 "*": 监听所有事件
                       - "prefix.*": 监听prefix开头的所有事件
                       - "*.suffix": 监听以suffix结尾的所有事件
                       - "*.middle.*": 监听包含middle的所有事件
            handler: 事件处理器函数
            filter_condition: 额外的过滤条件
            retry_policy: 重试策略
            
        Returns:
            str: 处理器ID
        """
        pass
    
    @abstractmethod
    async def get_event(self, event_id: str) -> Event:
        """获取事件"""
        pass

    def publish_sync(self, event: Union[Event, str, Dict[str, Any]],
                     prevent_duplicate_consumption: bool = True,
                     **kwargs) -> str:
        """Synchronous publish — delegates to the async ``publish`` method.

        Detects whether an event loop is already running and uses
        ``_run_async_from_sync`` to bridge appropriately.
        """
        return _run_async_from_sync(
            self.publish(event, prevent_duplicate_consumption, **kwargs)
        )


# 装饰器，简化事件处理器注册
def event_handler(event_bus: EventBus,
                 event_type: Optional[str] = None,
                 filter_condition: Optional[Dict[str, Any]] = None,
                 retry_policy: Optional[RetryPolicy] = None):
    """事件处理器装饰器

    注册是异步的 (``EventBus.register_handler`` 是 coroutine)。本装饰器在
    模块导入期被触发, 当时未必有 running loop, 直接 ``asyncio.create_task``
    会抛 ``RuntimeError`` 或 (即使成功) task 引用丢失被 GC 回收。

    处理策略:
    1. 检测到 running loop —— ``create_task`` 并把引用存进模块级集合, 由 loop
       自然驱动; 任务完成后从集合移除 (``done_callback``)。
    2. 没 running loop (典型场景: 模块导入期) —— 把 register coroutine 存进
       模块级待办列表, 暴露 ``flush_pending_handler_registrations()`` 让用户
       在 loop 起来后显式 await。
    """

    def decorator(func: EventHandler):
        async def register():
            await event_bus.register_handler(
                event_type=event_type,
                handler=func,
                filter_condition=filter_condition,
                retry_policy=retry_policy
            )

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 没运行中的 loop —— 暂存, 等用户在 async 上下文里 flush
            _pending_handler_registrations.append(register)
            return func

        task = loop.create_task(register())
        _handler_registration_tasks.add(task)
        task.add_done_callback(_handler_registration_tasks.discard)
        return func

    return decorator


# 模块级引用持有: 防 GC 回收 fire-and-forget 注册任务
_handler_registration_tasks: Set[asyncio.Task] = set()
# 没 running loop 时暂存的注册 coroutine, 等 loop 起来后由
# ``flush_pending_handler_registrations`` 显式驱动。
_pending_handler_registrations: List[Callable[[], "asyncio.Awaitable"]] = []


async def flush_pending_handler_registrations() -> None:
    """驱动所有在无 running loop 期间被 ``@event_handler`` 排队的注册任务。

    典型用法: 应用启动、``asyncio.run`` 入口里 ``await flush_...()`` 一次。
    """
    pending = list(_pending_handler_registrations)
    _pending_handler_registrations.clear()
    for register in pending:
        await register()