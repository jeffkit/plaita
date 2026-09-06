#!/usr/bin/env python
"""
Plaita事件系统统一演示程序

本示例程序展示了Plaita事件系统的完整工作流程，支持三种不同的后端实现：
1. 内存实现: 适用于单机环境和开发测试
2. Redis实现: 适用于分布式环境
3. SQLAlchemy实现: 适用于需要持久化的场景

运行示例（在仓库根目录执行）:
python -m examples.event_demo.demo_eventbus --backend memory  # 使用内存后端
python -m examples.event_demo.demo_eventbus --backend redis   # 使用Redis后端
python -m examples.event_demo.demo_eventbus --backend db      # 使用SQLAlchemy后端
"""

import argparse
import asyncio
import logging
import random
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Type
from abc import ABC, abstractmethod

# 本 demo 的所有文件副作用（日志 / SQLite 数据库）都固定写到本目录内，
# 不污染运行命令时所在的 cwd。见同目录 README.md。
_DEMO_DIR = Path(__file__).resolve().parent
LOG_FILE = _DEMO_DIR / "plaita.log"
DATABASE_URL = f"sqlite+aiosqlite:///{_DEMO_DIR / 'event_demo.db'}"

# 根据需要导入不同后端的库
try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None

try:
    import sqlalchemy as sa
    from sqlalchemy.ext.asyncio import create_async_engine
except ImportError:
    sa = None

from plaita.event.core import Event, EventBus, EventStorage, EventSubscription, RetryPolicy

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("plaita.event.demo")

# 延迟导入的组件
memory_components = None  # 内存组件
redis_components = None   # Redis组件
db_components = None      # SQLAlchemy组件

from plaita.event.memory import InMemoryEventBus, MemoryEventStorage, InMemoryEventSubscriptionStorage, InMemoryProcessingTracker

class EventDemoApp(ABC):
    """事件示例应用基类"""
    
    def __init__(self):
        """初始化应用"""
        # 事件总线和相关组件将在子类中初始化
        self.event_bus = None
        self.event_storage = None
        self.subscription_storage = None
        self.processing_tracker = None
        
        # 储存处理器ID以便后续使用
        self.handler_ids = {}
        self.subscription_ids = {}
        
        # 计数器，用于演示
        self.processed_events = 0
        self.failed_events = 0
    
    @abstractmethod
    async def initialize(self):
        """初始化组件，子类需要实现"""
        pass
    
    async def start(self):
        """启动事件系统演示"""
        # 确定应用类型
        app_type = "Mixed"
        if isinstance(self, MemoryEventDemoApp):
            app_type = "内存"
        elif isinstance(self, RedisEventDemoApp):
            app_type = "Redis"
        elif isinstance(self, SQLAlchemyEventDemoApp):
            app_type = "SQLAlchemy"
        
        log.info(f"=== {app_type}事件系统演示开始 ===")
        
        # 初始化组件
        await self.initialize()
        
        # 注册处理器和订阅
        await self.register_handlers()
        await self.register_subscriptions()
        
        # 运行所有演示
        await self.run_demos()
        
        # 运行后端特有的演示
        await self.run_backend_specific_demos()
        
        log.info(f"=== {app_type}事件系统演示结束 ===")
    
    async def register_handlers(self):
        """注册事件处理器"""
        log.info("注册事件处理器...")
        
        # 1. 基本处理器
        basic_handler_id = await self.event_bus.register_handler(
            event_type="demo.basic",
            handler=self.basic_event_handler
        )
        self.handler_ids["basic"] = basic_handler_id
        log.info(f"基本事件处理器已注册, ID: {basic_handler_id}")
        
        # 2. 带过滤条件的处理器
        filtered_handler_id = await self.event_bus.register_handler(
            event_type="demo.filtered",
            handler=self.filtered_event_handler,
            filter_condition={"priority": "high"}
        )
        self.handler_ids["filtered"] = filtered_handler_id
        log.info(f"带过滤条件的处理器已注册, ID: {filtered_handler_id}")
        
        # 3. 带重试策略的处理器
        retry_policy = RetryPolicy(
            max_retries=3,
            initial_delay=0.5,
            backoff_factor=2.0,
            max_delay=5.0
        )
        
        retry_handler_id = await self.event_bus.register_handler(
            event_type="demo.retry",
            handler=self.failing_event_handler,
            retry_policy=retry_policy
        )
        self.handler_ids["retry"] = retry_handler_id
        log.info(f"带重试策略的处理器已注册, ID: {retry_handler_id}")
        
        # 4. 多个单独的类型处理器 (替代原来的多类型处理器)
        for event_type in ["demo.type1", "demo.type2", "demo.type3"]:
            handler_id = await self.event_bus.register_handler(
                event_type=event_type,
                handler=self.multi_type_event_handler
            )
            self.handler_ids[f"multi_type_{event_type}"] = handler_id
            log.info(f"{event_type}事件处理器已注册, ID: {handler_id}")
    
    async def register_subscriptions(self):
        """注册事件订阅"""
        log.info("注册事件订阅...")
        
        # 1. 基本订阅
        basic_subscription_id = await self.event_bus.register_subscription(
            event_type="demo.subscription",
            correlation_id="demo_correlation",
            flow_id="demo_flow",
            node_id="demo_node"
        )
        self.subscription_ids["basic"] = basic_subscription_id
        log.info(f"基本订阅已注册, ID: {basic_subscription_id}")
        
        # 2. 带过滤条件的订阅
        filtered_subscription_id = await self.event_bus.register_subscription(
            event_type="demo.subscription",
            filter_condition={"category": "important"},
            correlation_id="demo_correlation",
            flow_id="demo_flow",
            node_id="demo_node"
        )
        self.subscription_ids["filtered"] = filtered_subscription_id
        log.info(f"带过滤条件的订阅已注册, ID: {filtered_subscription_id}")
    
    async def run_demos(self):
        """运行各种示例场景"""
        # 1. 基本事件
        await self.demo_basic_event()
        
        # 2. 事件过滤
        await self.demo_event_filtering()
        
        # 3. 事件等待
        await self.demo_wait_for_event()
        
        # 4. 重试机制
        await self.demo_retry_mechanism()
        
        # 5. 批量发布
        await self.demo_batch_publish()
        
        # 6. 查询功能
        await self.demo_query_features()
    
    async def run_backend_specific_demos(self):
        """运行特定后端的示例，子类可以覆盖此方法"""
        pass
    
    async def demo_basic_event(self):
        """演示基本事件发布和处理"""
        log.info("\n=== 基本事件演示 ===")
        
        event_id = await self.event_bus.publish(
            "demo.basic",
            message="这是一个基本事件",
            timestamp=time.time()
        )
        
        log.info(f"已发布基本事件, ID: {event_id}")
        
        await asyncio.sleep(0.5)
        
        event = await self.event_bus.get_event(event_id)
        log.info(f"事件详情: 类型={event.event_type}, 数据={event.data}")
        
        history = await self.processing_tracker.get_processing_history(event_id)
        log.info(f"事件处理历史: {history}")
    
    async def demo_event_filtering(self):
        """演示事件过滤功能"""
        log.info("\n=== 事件过滤演示 ===")
        
        match_event_id = await self.event_bus.publish(
            "demo.filtered",
            priority="high",
            message="这是一个高优先级事件"
        )
        log.info(f"发布了匹配过滤条件的事件, ID: {match_event_id}")
        
        non_match_event_id = await self.event_bus.publish(
            "demo.filtered",
            priority="low",
            message="这是一个低优先级事件"
        )
        log.info(f"发布了不匹配过滤条件的事件, ID: {non_match_event_id}")
        
        await asyncio.sleep(0.5)
    
    async def demo_wait_for_event(self):
        """演示事件等待功能"""
        log.info("\n=== 事件等待演示 ===")
        
        wait_task = asyncio.create_task(
            self.wait_for_specific_event()
        )
        
        await asyncio.sleep(0.5)
        
        event_id = await self.event_bus.publish(
            "demo.wait",
            trigger="wait_demo",
            value=random.randint(1, 100)
        )
        log.info(f"发布了触发等待的事件, ID: {event_id}")
        
        await wait_task
    
    async def demo_retry_mechanism(self):
        """演示重试机制"""
        log.info("\n=== 重试机制演示 ===")
        
        event_id = await self.event_bus.publish(
            "demo.retry",
            message="这个事件将会失败并重试",
            fail_count=2
        )
        log.info(f"发布了将触发重试的事件, ID: {event_id}")
        
        await asyncio.sleep(4)
        
        history = await self.processing_tracker.get_processing_history(event_id)
        log.info(f"重试事件处理历史: {history}")
    
    async def demo_batch_publish(self):
        """演示批量发布事件"""
        log.info("\n=== 批量发布演示 ===")
        
        events = []
        for i in range(5):
            events.append(
                Event(
                    event_type="demo.type" + str(i % 3 + 1),
                    data={"index": i, "message": f"批量事件 #{i}"}
                )
            )
        
        event_ids = await self.event_bus.batch_publish(events)
        log.info(f"批量发布了 {len(event_ids)} 个事件, IDs: {event_ids}")
        
        await asyncio.sleep(0.5)
    
    async def demo_query_features(self):
        """演示查询功能"""
        log.info("\n=== 查询功能演示 ===")
        
        events = await self.event_storage.list_events(limit=5)
        log.info(f"最近5个事件: {[f'{e.event_id}:{e.event_type}' for e in events]}")
        
        type_events = await self.event_storage.list_events(event_type="demo.basic", limit=3)
        log.info(f"类型'demo.basic'的事件: {[e.event_id for e in type_events]}")
        
        subscriptions = await self.subscription_storage.list_subscriptions()
        log.info(f"所有订阅: {[s.subscription_id for s in subscriptions]}")
        
        if events:
            first_event = events[0]
            history = await self.processing_tracker.get_processing_history(first_event.event_id)
            log.info(f"事件 {first_event.event_id} 的处理历史: {history}")
    
    # 事件处理器实现
    
    async def basic_event_handler(self, event: Event):
        """基本事件处理器"""
        handler_id = self.handler_ids.get("basic", "unknown")
        log.info(f"🟢 基本处理器(ID:{handler_id})处理事件: {event.event_id}, 数据: {event.data}")
        self.processed_events += 1
        await asyncio.sleep(0.1)
    
    async def filtered_event_handler(self, event: Event):
        """带过滤条件的事件处理器"""
        handler_id = self.handler_ids.get("filtered", "unknown")
        log.info(f"🔵 过滤处理器(ID:{handler_id})处理事件: {event.event_id}, 优先级: {event.data.get('priority')}")
        self.processed_events += 1
        await asyncio.sleep(0.1)
    
    async def failing_event_handler(self, event: Event):
        """会失败的事件处理器"""
        handler_id = self.handler_ids.get("retry", "unknown")
        fail_count = event.data.get("fail_count", 0)
        
        if self.failed_events < fail_count:
            self.failed_events += 1
            log.info(f"🔴 重试处理器(ID:{handler_id})处理事件失败: {event.event_id}, 失败次数: {self.failed_events}/{fail_count}")
            raise ValueError(f"模拟失败 #{self.failed_events}")
        
        log.info(f"🟢 重试处理器(ID:{handler_id})处理事件成功: {event.event_id}, 经过 {self.failed_events} 次失败")
        self.processed_events += 1
        self.failed_events = 0
        await asyncio.sleep(0.1)
    
    async def multi_type_event_handler(self, event: Event):
        """多类型事件处理器"""
        handler_id = self.handler_ids.get(f"multi_type_{event.event_type}", "unknown")
        log.info(f"⭐ 多类型处理器(ID:{handler_id})处理事件: {event.event_id}, 类型: {event.event_type}, 数据: {event.data}")
        self.processed_events += 1
        await asyncio.sleep(0.1)
    
    async def wait_for_specific_event(self):
        """等待特定事件的演示"""
        log.info("开始等待'demo.wait'类型的事件...")
        
        try:
            event = await self.event_bus.wait_for_event(
                event_type="demo.wait",
                timeout=5.0,
                condition=lambda e: e.data.get("trigger") == "wait_demo"
            )
            log.info(f"✅ 成功接收到等待的事件: {event.event_id}, 值: {event.data.get('value')}")
        except Exception as e:
            log.error(f"❌ 等待事件失败: {e}")


class MemoryEventDemoApp(EventDemoApp):
    """内存实现的事件演示应用"""
    
    async def initialize(self):
        """初始化内存实现的组件"""
        from plaita.event.memory import InMemoryEventBus
        self.event_bus = InMemoryEventBus()
        self.event_storage = self.event_bus.event_storage
        self.subscription_storage = self.event_bus.subscription_storage
        self.processing_tracker = self.event_bus.processing_tracker


class RedisEventDemoApp(EventDemoApp):
    """Redis实现的事件演示应用"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        """初始化Redis事件演示应用"""
        super().__init__()
        self.redis_url = redis_url
        
        # 导入Redis组件
        global redis_components
        if redis_components is None:
            from plaita.event.redis import (
                RedisEventBus, RedisEventStorage, 
                RedisEventSubscriptionStorage, RedisProcessingTracker
            )
            redis_components = {
                "EventBus": RedisEventBus,
                "EventStorage": RedisEventStorage,
                "EventSubscriptionStorage": RedisEventSubscriptionStorage,
                "EventProcessingTracker": RedisProcessingTracker
            }
    
    async def initialize(self):
        """初始化Redis实现的组件"""
        RedisEventBus = redis_components["EventBus"]
        
        self.event_bus = RedisEventBus(
            redis_url=self.redis_url,
            event_key_prefix="demo:event:",
            subscription_key_prefix="demo:subscription:",
            processing_key_prefix="demo:processed:"
        )
        
        # 初始化事件总线
        await self.event_bus.initialize()
        
        self.event_storage = self.event_bus.event_storage
        self.subscription_storage = self.event_bus.subscription_storage
        self.processing_tracker = self.event_bus.processing_tracker
        
        log.info(f"使用Redis: {self.redis_url}")
    
    async def run_backend_specific_demos(self):
        """运行Redis特有功能演示"""
        await self.demo_redis_cleanup()
    
    async def demo_redis_cleanup(self):
        """演示Redis清理功能"""
        log.info("\n=== Redis清理功能演示 ===")
        
        cleaned_count = await self.processing_tracker.cleanup_old_records(max_age_seconds=1)
        log.info(f"已清理 {cleaned_count} 条旧处理记录")
        
        redis = await aioredis.from_url(self.redis_url)
        keys = await redis.keys("demo:*")
        await redis.close()
        
        log.info(f"当前演示相关的Redis键数量: {len(keys)}")
        log.info(f"Redis键示例: {keys[:5] if len(keys) > 5 else keys}")


class SQLAlchemyEventDemoApp(EventDemoApp):
    """SQLAlchemy实现的事件演示应用"""
    
    def __init__(self, database_url: str = DATABASE_URL):
        """初始化SQLAlchemy事件演示应用"""
        super().__init__()
        self.database_url = database_url

        # 导入SQLAlchemy组件
        global db_components
        if db_components is None:
            from plaita.event.sqlalchemy import (
                SqlalchemyEventBus, SqlalchemyEventStorage,
                SqlalchemyEventSubscriptionStorage, SqlalchemyEventProcessingTracker
            )
            db_components = {
                "EventBus": SqlalchemyEventBus,
                "EventStorage": SqlalchemyEventStorage,
                "EventSubscriptionStorage": SqlalchemyEventSubscriptionStorage,
                "EventProcessingTracker": SqlalchemyEventProcessingTracker
            }
    
    async def initialize(self):
        """初始化SQLAlchemy实现的组件"""
        SqlalchemyEventBus = db_components["EventBus"]

        # 当前 API 接收异步 engine（而非 database_url），建表走 create_tables/ensure_tables
        engine = create_async_engine(self.database_url)
        self.event_bus = SqlalchemyEventBus(
            engine=engine,
            create_tables=True  # 自动创建表结构
        )
        await self.event_bus.ensure_tables()
        
        self.event_storage = self.event_bus.event_storage
        self.subscription_storage = self.event_bus.subscription_storage
        self.processing_tracker = self.event_bus.processing_tracker
        
        log.info(f"使用数据库: {self.database_url}")
    
    async def run_backend_specific_demos(self):
        """运行SQLAlchemy特有功能演示"""
        await self.demo_db_stats()
    
    async def demo_db_stats(self):
        """演示数据库统计信息"""
        log.info("\n=== 数据库统计信息演示 ===")
        
        try:
            # 获取各表记录数量
            async with self.event_bus.engine.connect() as conn:
                # 查询事件表
                result = await conn.execute(sa.text("SELECT COUNT(*) FROM events"))
                events_count = result.scalar()
                
                # 查询订阅表
                result = await conn.execute(sa.text("SELECT COUNT(*) FROM event_subscriptions"))
                subscriptions_count = result.scalar()
                
                # 查询处理记录表
                result = await conn.execute(sa.text("SELECT COUNT(*) FROM processed_events"))
                processed_count = result.scalar()
                
                # 查询历史记录表
                result = await conn.execute(sa.text("SELECT COUNT(*) FROM event_processing_history"))
                history_count = result.scalar()
                
            log.info(f"数据库表记录统计:")
            log.info(f"- 事件表: {events_count} 条记录")
            log.info(f"- 订阅表: {subscriptions_count} 条记录")
            log.info(f"- 处理记录表: {processed_count} 条记录")
            log.info(f"- 历史记录表: {history_count} 条记录")
            
            # 清理旧记录
            cleaned_count = await self.processing_tracker.cleanup_old_records(max_age_seconds=1)
            log.info(f"已清理 {cleaned_count} 条旧处理记录")
            
        except Exception as e:
            log.error(f"获取数据库统计信息时出错: {e}")
            log.info("提示: 确保已安装所需依赖: pip install sqlalchemy[asyncio] aiosqlite")


class MixedEventDemoApp(EventDemoApp):
    """混合不同后端实现的事件演示应用"""
    
    def __init__(self, 
                 bus_type: str = "memory",
                 storage_type: str = "memory", 
                 subscription_type: str = "memory",
                 tracker_type: str = "memory",
                 redis_url: str = "redis://localhost:6379/0",
                 database_url: str = DATABASE_URL):
        """
        初始化混合后端事件演示应用
        
        Args:
            bus_type: 事件总线类型 (memory, redis, db)
            storage_type: 事件存储类型 (memory, redis, db) 
            subscription_type: 订阅存储类型 (memory, redis, db)
            tracker_type: 处理跟踪器类型 (memory, redis, db)
            redis_url: Redis连接URL
            database_url: 数据库连接URL
        """
        super().__init__()
        self.bus_type = bus_type
        self.storage_type = storage_type
        self.subscription_type = subscription_type
        self.tracker_type = tracker_type
        self.redis_url = redis_url
        self.database_url = database_url
        
        # 导入所有类型的组件
        self._import_all_components()
    
    def _import_all_components(self):
        """导入所有类型的组件"""
        # 导入内存组件
        global memory_components
        if memory_components is None:
            from plaita.event.memory import (
                InMemoryEventBus, MemoryEventStorage,
                InMemoryEventSubscriptionStorage, InMemoryProcessingTracker
            )
            memory_components = {
                "EventBus": InMemoryEventBus,
                "EventStorage": MemoryEventStorage,
                "EventSubscriptionStorage": InMemoryEventSubscriptionStorage,
                "EventProcessingTracker": InMemoryProcessingTracker
            }
        
        # 导入Redis组件
        global redis_components
        if redis_components is None:
            try:
                from plaita.event.redis import (
                    RedisEventBus, RedisEventStorage,
                    RedisEventSubscriptionStorage, RedisProcessingTracker
                )
                redis_components = {
                    "EventBus": RedisEventBus,
                    "EventStorage": RedisEventStorage,
                    "EventSubscriptionStorage": RedisEventSubscriptionStorage,
                    "EventProcessingTracker": RedisProcessingTracker
                }
            except ImportError:
                log.warning("Redis组件导入失败，请确保安装了redis依赖")
                redis_components = {}
        
        # 导入SQLAlchemy组件
        global db_components
        if db_components is None:
            try:
                from plaita.event.sqlalchemy import (
                    SqlalchemyEventBus, SqlalchemyEventStorage,
                    SqlalchemyEventSubscriptionStorage, SqlalchemyEventProcessingTracker
                )
                db_components = {
                    "EventBus": SqlalchemyEventBus,
                    "EventStorage": SqlalchemyEventStorage,
                    "EventSubscriptionStorage": SqlalchemyEventSubscriptionStorage,
                    "EventProcessingTracker": SqlalchemyEventProcessingTracker
                }
            except ImportError:
                log.warning("SQLAlchemy组件导入失败，请确保安装了sqlalchemy依赖")
                db_components = {}
    
    async def _create_component(self, component_type: str, component_name: str):
        """
        创建指定类型的组件
        
        Args:
            component_type: 组件类型 (memory, redis, db)
            component_name: 组件名称 (bus, storage, subscription, tracker)
        
        Returns:
            创建的组件实例
        """
        if component_type == "memory":
            components = memory_components
            if component_name == "bus":
                return memory_components["EventBus"]()
            elif component_name == "storage":
                return memory_components["EventStorage"]()
            elif component_name == "subscription":
                return memory_components["EventSubscriptionStorage"]()
            elif component_name == "tracker":
                return memory_components["EventProcessingTracker"]()
        
        elif component_type == "redis":
            components = redis_components
            if component_name == "bus":
                component = redis_components["EventBus"](
                    redis_url=self.redis_url,
                    event_key_prefix="demo:event:",
                    subscription_key_prefix="demo:subscription:",
                    processing_key_prefix="demo:processed:"
                )
                await component.initialize()
                return component
            elif component_name == "storage":
                component = redis_components["EventStorage"](
                    redis_url=self.redis_url,
                    key_prefix="demo:event:"
                )
                await component.initialize()
                return component
            elif component_name == "subscription":
                component = redis_components["EventSubscriptionStorage"](
                    redis_url=self.redis_url,
                    key_prefix="demo:subscription:"
                )
                await component.initialize()
                return component
            elif component_name == "tracker":
                component = redis_components["EventProcessingTracker"](
                    redis_url=self.redis_url,
                    key_prefix="demo:processed:"
                )
                await component.initialize()
                return component
        
        elif component_type == "db":
            components = db_components
            if component_name == "bus":
                engine = create_async_engine(self.database_url)
                component = db_components["EventBus"](
                    engine=engine,
                    create_tables=True
                )
                await component.ensure_tables()
                return component
            elif component_name == "storage":
                engine = create_async_engine(self.database_url)
                component = db_components["EventStorage"](
                    engine=engine,
                    create_tables=True
                )
                return component
            elif component_name == "subscription":
                engine = create_async_engine(self.database_url)
                return db_components["EventSubscriptionStorage"](engine=engine)
            elif component_name == "tracker":
                engine = create_async_engine(self.database_url)
                return db_components["EventProcessingTracker"](engine=engine)
        
        raise ValueError(f"不支持的组件类型: {component_type}，组件名称: {component_name}")
    
    async def initialize(self):
        """初始化混合后端实现的组件"""
        # 记录使用的后端类型
        backend_types = {
            "事件总线": self.bus_type,
            "事件存储": self.storage_type,
            "订阅存储": self.subscription_type,
            "处理跟踪器": self.tracker_type
        }
        log.info(f"使用混合后端: {backend_types}")
        
        # 创建事件总线组件
        self.event_bus = await self._create_component(self.bus_type, "bus")
        
        # 如果使用自定义组件，替换事件总线中的组件
        if self.storage_type != self.bus_type:
            custom_storage = await self._create_component(self.storage_type, "storage")
            # 替换事件总线中的存储组件
            self.event_bus.event_storage = custom_storage
        
        if self.subscription_type != self.bus_type:
            custom_subscription = await self._create_component(self.subscription_type, "subscription")
            # 替换事件总线中的订阅存储组件
            self.event_bus.subscription_storage = custom_subscription
        
        if self.tracker_type != self.bus_type:
            custom_tracker = await self._create_component(self.tracker_type, "tracker")
            # 替换事件总线中的处理跟踪器组件
            self.event_bus.processing_tracker = custom_tracker
        
        # 获取各组件的引用
        self.event_storage = self.event_bus.event_storage
        self.subscription_storage = self.event_bus.subscription_storage
        self.processing_tracker = self.event_bus.processing_tracker
    
    async def run_backend_specific_demos(self):
        """运行特定后端的演示功能"""
        # 根据不同的后端组件运行特定的演示
        backends_used = set([
            self.bus_type, self.storage_type, 
            self.subscription_type, self.tracker_type
        ])
        
        # 如果使用了Redis组件，运行Redis特有功能演示
        if "redis" in backends_used and aioredis is not None:
            await self.demo_redis_cleanup()
        
        # 如果使用了SQLAlchemy组件，运行数据库特有功能演示
        if "db" in backends_used and sa is not None:
            await self.demo_db_stats()
    
    async def demo_redis_cleanup(self):
        """演示Redis清理功能"""
        if not hasattr(self.processing_tracker, "cleanup_old_records"):
            log.info("当前处理跟踪器不支持清理功能，跳过Redis清理演示")
            return
            
        log.info("\n=== Redis清理功能演示 ===")
        
        cleaned_count = await self.processing_tracker.cleanup_old_records(max_age_seconds=1)
        log.info(f"已清理 {cleaned_count} 条旧处理记录")
        
        try:
            redis = await aioredis.from_url(self.redis_url)
            keys = await redis.keys("demo:*")
            await redis.close()
            
            log.info(f"当前演示相关的Redis键数量: {len(keys)}")
            log.info(f"Redis键示例: {keys[:5] if len(keys) > 5 else keys}")
        except Exception as e:
            log.error(f"Redis连接失败: {e}")
    
    async def demo_db_stats(self):
        """演示数据库统计信息"""
        # 检查是否使用了SQLAlchemy实现
        has_db_components = False
        for component in [self.event_bus, self.event_storage, self.subscription_storage, self.processing_tracker]:
            if hasattr(component, "engine"):
                has_db_components = True
                engine = component.engine
                break
        
        if not has_db_components or sa is None:
            log.info("当前未使用SQLAlchemy组件，跳过数据库统计演示")
            return
            
        log.info("\n=== 数据库统计信息演示 ===")
        
        try:
            # 获取各表记录数量
            async with engine.connect() as conn:
                # 查询事件表
                result = await conn.execute(sa.text("SELECT COUNT(*) FROM events"))
                events_count = result.scalar()
                
                # 查询订阅表
                result = await conn.execute(sa.text("SELECT COUNT(*) FROM event_subscriptions"))
                subscriptions_count = result.scalar()
                
                # 查询处理记录表
                result = await conn.execute(sa.text("SELECT COUNT(*) FROM processed_events"))
                processed_count = result.scalar()
                
                # 查询历史记录表
                result = await conn.execute(sa.text("SELECT COUNT(*) FROM event_processing_history"))
                history_count = result.scalar()
                
            log.info(f"数据库表记录统计:")
            log.info(f"- 事件表: {events_count} 条记录")
            log.info(f"- 订阅表: {subscriptions_count} 条记录")
            log.info(f"- 处理记录表: {processed_count} 条记录")
            log.info(f"- 历史记录表: {history_count} 条记录")
            
            # 清理旧记录
            if hasattr(self.processing_tracker, "cleanup_old_records"):
                cleaned_count = await self.processing_tracker.cleanup_old_records(max_age_seconds=1)
                log.info(f"已清理 {cleaned_count} 条旧处理记录")
            
        except Exception as e:
            log.error(f"获取数据库统计信息时出错: {e}")
            log.info("提示: 确保已安装所需依赖: pip install sqlalchemy[asyncio] aiosqlite")


def create_event_demo_app(backend: str, **kwargs) -> EventDemoApp:
    """创建事件演示应用的工厂函数"""
    if backend == "memory":
        return MemoryEventDemoApp()
    elif backend == "redis":
        return RedisEventDemoApp(**kwargs)
    elif backend == "db":
        return SQLAlchemyEventDemoApp(**kwargs)
    elif backend == "mixed":
        return MixedEventDemoApp(**kwargs)
    else:
        raise ValueError(f"不支持的后端类型: {backend}")


async def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Plaita事件系统演示程序")
    parser.add_argument("--backend", choices=["memory", "redis", "db", "mixed"], default="memory",
                      help="事件系统后端，可选: memory, redis, db, mixed")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0",
                      help="Redis连接URL，当使用Redis组件时有效")
    parser.add_argument("--db-url", default=DATABASE_URL,
                      help="数据库连接URL，当使用数据库组件时有效（默认写到本示例目录内）")
                      
    # 混合后端专用参数
    parser.add_argument("--bus-type", choices=["memory", "redis", "db"], default="memory",
                      help="事件总线类型，当backend=mixed时有效")
    parser.add_argument("--storage-type", choices=["memory", "redis", "db"], default="memory",
                      help="事件存储类型，当backend=mixed时有效")
    parser.add_argument("--subscription-type", choices=["memory", "redis", "db"], default="memory",
                      help="订阅存储类型，当backend=mixed时有效")
    parser.add_argument("--tracker-type", choices=["memory", "redis", "db"], default="memory",
                      help="处理跟踪器类型，当backend=mixed时有效")
    
    args = parser.parse_args()
    
    # 根据参数创建演示应用
    kwargs = {}
    if args.backend == "redis":
        kwargs["redis_url"] = args.redis_url
    elif args.backend == "db":
        kwargs["database_url"] = args.db_url
    elif args.backend == "mixed":
        kwargs["bus_type"] = args.bus_type
        kwargs["storage_type"] = args.storage_type
        kwargs["subscription_type"] = args.subscription_type
        kwargs["tracker_type"] = args.tracker_type
        kwargs["redis_url"] = args.redis_url
        kwargs["database_url"] = args.db_url
    
    app = create_event_demo_app(args.backend, **kwargs)
    await app.start()


if __name__ == "__main__":
    # 运行演示
    asyncio.run(main())