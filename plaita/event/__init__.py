"""
事件系统包

基于发布/订阅模式的事件驱动系统，支持内存、Redis和SQLAlchemy实现。
"""
from .core import (
    Event, EventBus, EventHandler, EventStorage, EventSubscription, EventSubscriptionStorage,
    EventProcessingTracker, RetryPolicy, event_handler
)
from .exceptions import EventError, EventNotFoundError, EventStorageError, EventTimeoutError
from .memory import (
    InMemoryEventBus, MemoryEventStorage, InMemoryEventSubscriptionStorage,
    InMemoryProcessingTracker
)
from ..node.event_node import EventNode
from .utils import normalize_event
from .timeout import SubscriptionTimeoutChecker

# 导入Redis实现
try:
    from .redis import (
        RedisEventBus, RedisEventStorage, RedisEventSubscriptionStorage,
        RedisProcessingTracker
    )
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

# 导入SQLAlchemy实现
try:
    from .sqlalchemy import (
        SqlalchemyEventBus, SqlalchemyEventStorage, SqlalchemyEventSubscriptionStorage,
        SqlalchemyEventProcessingTracker
    )
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False

# 全局默认事件总线实例
_default_event_bus = None

def get_default_event_bus() -> EventBus:
    """
    获取默认的事件总线实例
    
    Returns:
        EventBus: 默认事件总线实例
    """
    global _default_event_bus
    if _default_event_bus is None:
        _default_event_bus = InMemoryEventBus()
    return _default_event_bus

def set_default_event_bus(event_bus: EventBus) -> None:
    """
    设置默认的事件总线实例
    
    Args:
        event_bus: 要设为默认的事件总线实例
    """
    global _default_event_bus
    _default_event_bus = event_bus

# 定义__all__列表
__all__ = [
    'Event',
    'EventBus',
    'EventStorage',
    'EventSubscription',
    'EventSubscriptionStorage',
    'EventProcessingTracker',
    'RetryPolicy',
    'event_handler',
    'EventError',
    'EventNotFoundError',
    'EventStorageError',
    'EventTimeoutError',
    'FlowEventHandler',
    'InMemoryEventBus',
    'MemoryEventStorage',
    'InMemoryEventSubscriptionStorage',
    'InMemoryProcessingTracker',
    'EventNode',
    'get_default_event_bus',
    'set_default_event_bus',
    'HAS_REDIS',
    'HAS_SQLALCHEMY',
]

# 如果Redis可用，添加Redis相关类到__all__
if HAS_REDIS:
    __all__.extend([
        'RedisEventBus',
        'RedisEventStorage',
        'RedisEventSubscriptionStorage',
        'RedisProcessingTracker'
    ])

# 如果SQLAlchemy可用，添加SQLAlchemy相关类到__all__
if HAS_SQLALCHEMY:
    __all__.extend([
        'SqlalchemyEventBus',
        'SqlalchemyEventStorage',
        'SqlalchemyEventSubscriptionStorage',
        'SqlalchemyEventProcessingTracker'
    ])

# 提供便捷的默认实现
DefaultEventBus = InMemoryEventBus
