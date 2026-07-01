"""
基于 SQLAlchemy 的事件总线和存储实现
"""
import asyncio
import json
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Union, cast, Awaitable

try:
    import sqlalchemy as sa
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from sqlalchemy.orm import declarative_base
    from sqlalchemy.future import select
    from sqlalchemy.orm import relationship, sessionmaker
except ImportError:
    raise ImportError("请安装 SQLAlchemy 依赖: pip install sqlalchemy sqlalchemy[asyncio]")

from .core import (
    Event, EventBus, EventHandler, EventStorage, EventSubscription,
    EventSubscriptionStorage, EventProcessingTracker, RetryPolicy
)
from .exceptions import EventError, EventNotFoundError, EventStorageError, EventTimeoutError
from .utils import normalize_event

# 创建基础模型
Base = declarative_base()


class EventModel(Base):
    """事件数据库模型"""
    __tablename__ = "events"
    
    id = sa.Column(sa.String(36), primary_key=True)
    event_type = sa.Column(sa.String(255), nullable=False, index=True)
    data = sa.Column(sa.JSON, nullable=False)
    timestamp = sa.Column(sa.Float, nullable=False, index=True)
    source = sa.Column(sa.String(255), nullable=True)
    correlation_id = sa.Column(sa.String(36), nullable=True, index=True)
    created_at = sa.Column(sa.Float, nullable=False, default=time.time)


class EventSubscriptionModel(Base):
    """事件订阅数据库模型"""
    __tablename__ = "event_subscriptions"
    
    id = sa.Column(sa.String(36), primary_key=True)
    event_type = sa.Column(sa.String(255), nullable=False, index=True)  # 改为单个事件类型
    filter_condition = sa.Column(sa.JSON, nullable=True)  # 存储为JSON对象
    correlation_id = sa.Column(sa.String(36), nullable=True, index=True)
    flow_id = sa.Column(sa.String(36), nullable=True, index=True)
    node_id = sa.Column(sa.String(36), nullable=True)
    created_at = sa.Column(sa.Float, nullable=False, default=time.time)
    timeout = sa.Column(sa.Float, nullable=True)  # 订阅超时时间，NULL表示无限等待


class ProcessedEventModel(Base):
    """已处理事件记录数据库模型"""
    __tablename__ = "processed_events"
    
    subscription_id = sa.Column(sa.String(36), primary_key=True)
    event_id = sa.Column(sa.String(36), primary_key=True)
    processed_at = sa.Column(sa.Float, nullable=False, default=time.time)
    
    # 复合索引
    __table_args__ = (
        sa.Index('idx_subscription_event', 'subscription_id', 'event_id'),
    )


class EventProcessingHistoryModel(Base):
    """事件处理历史数据库模型"""
    __tablename__ = "event_processing_history"
    
    id = sa.Column(sa.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    event_id = sa.Column(sa.String(36), nullable=False, index=True)
    handler_id = sa.Column(sa.String(36), nullable=False, index=True)
    status = sa.Column(sa.String(20), nullable=False)  # success, failure, retry
    error = sa.Column(sa.Text, nullable=True)
    timestamp = sa.Column(sa.Float, nullable=False, default=time.time)
    
    # 复合索引
    __table_args__ = (
        sa.Index('idx_event_handler', 'event_id', 'handler_id'),
    )


class SqlalchemyEventStorage(EventStorage):
    """基于 SQLAlchemy 的事件存储实现"""
    
    def __init__(self, engine, create_tables: bool = False):
        """
        初始化 SQLAlchemy 事件存储

        Args:
            engine: SQLAlchemy 引擎
            create_tables: 是否在首次写操作时自动创建表结构。
                历史上这会 fire-and-forget ``asyncio.create_task(_create_tables)``，
                但 ``__init__`` 是同步的: 没有运行中的事件循环时会抛
                ``RuntimeError``, 有循环时任务无人 await 可能被 GC, 且与显式
                ``_create_tables()`` 并发会撞 "table already exists"。
                现在改为惰性: 设置 ``create_tables=True`` 后, 首次写操作会
                先 ``await ensure_tables()``; 也可显式 ``await ensure_tables()``。
        """
        self.engine = engine
        self.async_session = sessionmaker(
            engine, expire_on_commit=False, class_=AsyncSession
        )

        self._tables_pending = bool(create_tables)

    async def ensure_tables(self) -> None:
        """显式创建表结构 (幂等)。推荐构造后 await 一次。"""
        await self._create_tables()
        self._tables_pending = False

    async def _ensure_tables_if_pending(self) -> None:
        if self._tables_pending:
            await self.ensure_tables()

    async def _create_tables(self):
        """创建数据库表结构"""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    
    async def store_event(self, event: Event) -> str:
        """存储事件"""
        await self._ensure_tables_if_pending()
        async with self.async_session() as session:
            db_event = EventModel(
                id=event.event_id,
                event_type=event.event_type,
                data=event.data,
                timestamp=event.timestamp,
                source=event.source,
                correlation_id=event.correlation_id,
                created_at=time.time()
            )
            session.add(db_event)
            try:
                await session.commit()
                return event.event_id
            except Exception as e:
                await session.rollback()
                raise EventStorageError(f"存储事件失败: {e}")
    
    async def get_event(self, event_id: str) -> Optional[Event]:
        """根据ID获取事件"""
        async with self.async_session() as session:
            result = await session.execute(
                select(EventModel).where(EventModel.id == event_id)
            )
            db_event = result.scalars().first()
            
            if not db_event:
                return None
            
            return Event(
                event_id=db_event.id,
                event_type=db_event.event_type,
                data=db_event.data,
                timestamp=db_event.timestamp,
                source=db_event.source,
                correlation_id=db_event.correlation_id
            )
    
    async def list_events(self, 
                         event_type: Optional[str] = None, 
                         start_time: Optional[float] = None,
                         end_time: Optional[float] = None,
                         limit: int = 100) -> List[Event]:
        """列出符合条件的事件"""
        query = select(EventModel)
        
        if event_type:
            query = query.where(EventModel.event_type == event_type)
        
        if start_time:
            query = query.where(EventModel.timestamp >= start_time)
        
        if end_time:
            query = query.where(EventModel.timestamp <= end_time)
        
        query = query.order_by(EventModel.timestamp.desc()).limit(limit)
        
        async with self.async_session() as session:
            result = await session.execute(query)
            db_events = result.scalars().all()
            
            return [
                Event(
                    event_id=db_event.id,
                    event_type=db_event.event_type,
                    data=db_event.data,
                    timestamp=db_event.timestamp,
                    source=db_event.source,
                    correlation_id=db_event.correlation_id
                )
                for db_event in db_events
            ]
    
    async def delete_event(self, event_id: str) -> bool:
        """删除事件"""
        async with self.async_session() as session:
            result = await session.execute(
                select(EventModel).where(EventModel.id == event_id)
            )
            db_event = result.scalars().first()
            
            if not db_event:
                return False
            
            await session.delete(db_event)
            await session.commit()
            return True
    
    async def batch_store_events(self, events: List[Event]) -> List[str]:
        """批量存储事件"""
        await self._ensure_tables_if_pending()
        event_ids = []
        async with self.async_session() as session:
            for event in events:
                db_event = EventModel(
                    id=event.event_id,
                    event_type=event.event_type,
                    data=event.data,
                    timestamp=event.timestamp,
                    source=event.source,
                    correlation_id=event.correlation_id,
                    created_at=time.time()
                )
                session.add(db_event)
                event_ids.append(event.event_id)
                
            try:
                await session.commit()
                return event_ids
            except Exception as e:
                await session.rollback()
                raise EventStorageError(f"批量存储事件失败: {e}")


class SqlalchemyEventSubscriptionStorage(EventSubscriptionStorage):
    """基于 SQLAlchemy 的事件订阅存储实现"""
    
    def __init__(self, engine):
        """
        初始化 SQLAlchemy 事件订阅存储
        
        Args:
            engine: SQLAlchemy 引擎
        """
        self.engine = engine
        self.async_session = sessionmaker(
            engine, expire_on_commit=False, class_=AsyncSession
        )
    
    async def store_subscription(self, subscription: EventSubscription) -> str:
        """存储事件订阅"""
        async with self.async_session() as session:
            # 检查是否已存在
            result = await session.execute(
                select(EventSubscriptionModel).where(
                    EventSubscriptionModel.id == subscription.subscription_id
                )
            )
            existing = result.scalars().first()
            
            if existing:
                # 更新现有订阅
                existing.event_type = subscription.event_type
                existing.filter_condition = subscription.filter_condition
                existing.correlation_id = subscription.correlation_id
                existing.flow_id = subscription.flow_id
                existing.node_id = subscription.node_id
                existing.timeout = subscription.timeout
            else:
                # 创建新订阅
                db_subscription = EventSubscriptionModel(
                    id=subscription.subscription_id,
                    event_type=subscription.event_type,
                    filter_condition=subscription.filter_condition or {},
                    correlation_id=subscription.correlation_id,
                    flow_id=subscription.flow_id,
                    node_id=subscription.node_id,
                    created_at=time.time(),
                    timeout=subscription.timeout
                )
                session.add(db_subscription)
                
            try:
                await session.commit()
                return subscription.subscription_id
            except Exception as e:
                await session.rollback()
                raise EventStorageError(f"存储订阅失败: {e}")
    
    async def get_subscription(self, subscription_id: str) -> Optional[EventSubscription]:
        """根据ID获取事件订阅"""
        async with self.async_session() as session:
            result = await session.execute(
                select(EventSubscriptionModel).where(
                    EventSubscriptionModel.id == subscription_id
                )
            )
            db_subscription = result.scalars().first()
            
            if not db_subscription:
                return None
            
            # 查询该订阅已处理的事件
            proc_result = await session.execute(
                select(ProcessedEventModel).where(
                    ProcessedEventModel.subscription_id == subscription_id
                )
            )
            processed_events = {
                record.event_id for record in proc_result.scalars().all()
            }
            
            return EventSubscription(
                subscription_id=db_subscription.id,
                event_type=db_subscription.event_type,
                filter_condition=db_subscription.filter_condition,
                correlation_id=db_subscription.correlation_id,
                flow_id=db_subscription.flow_id,
                node_id=db_subscription.node_id,
                created_at=db_subscription.created_at,
                processed_events=processed_events,
                timeout=db_subscription.timeout
            )
    
    async def list_subscriptions(self, 
                               event_type: Optional[str] = None,
                               correlation_id: Optional[str] = None,
                               flow_id: Optional[str] = None,
                               node_id: Optional[str] = None) -> List[EventSubscription]:
        """列出符合条件的事件订阅"""
        query = select(EventSubscriptionModel)
        
        if correlation_id:
            query = query.where(EventSubscriptionModel.correlation_id == correlation_id)
        
        if flow_id:
            query = query.where(EventSubscriptionModel.flow_id == flow_id)
        
        if node_id:
            query = query.where(EventSubscriptionModel.node_id == node_id)
        
        async with self.async_session() as session:
            result = await session.execute(query)
            db_subscriptions = result.scalars().all()
            
            subscriptions = []
            for db_sub in db_subscriptions:
                # 如果指定了事件类型，我们需要检查订阅的事件类型是否匹配
                if event_type and event_type != db_sub.event_type:
                    continue
                
                # 查询该订阅已处理的事件
                proc_result = await session.execute(
                    select(ProcessedEventModel).where(
                        ProcessedEventModel.subscription_id == db_sub.id
                    )
                )
                processed_events = {
                    record.event_id for record in proc_result.scalars().all()
                }
                
                subscription = EventSubscription(
                    subscription_id=db_sub.id,
                    event_type=db_sub.event_type,
                    filter_condition=db_sub.filter_condition,
                    correlation_id=db_sub.correlation_id,
                    flow_id=db_sub.flow_id,
                    node_id=db_sub.node_id,
                    created_at=db_sub.created_at,
                    processed_events=processed_events,
                    timeout=db_sub.timeout
                )
                subscriptions.append(subscription)
            
            return subscriptions
    
    async def delete_subscription(self, subscription_id: str) -> bool:
        """删除事件订阅"""
        async with self.async_session() as session:
            result = await session.execute(
                select(EventSubscriptionModel).where(
                    EventSubscriptionModel.id == subscription_id
                )
            )
            db_subscription = result.scalars().first()
            
            if not db_subscription:
                return False
            
            # 同时删除该订阅的处理记录
            await session.execute(
                sa.delete(ProcessedEventModel).where(
                    ProcessedEventModel.subscription_id == subscription_id
                )
            )
            
            await session.delete(db_subscription)
            await session.commit()
            return True
    
    async def mark_event_processed(self, subscription_id: str, event_id: str) -> bool:
        """原子操作：标记事件为已处理状态"""
        async with self.async_session() as session:
            # 检查记录是否已存在
            result = await session.execute(
                select(ProcessedEventModel).where(
                    ProcessedEventModel.subscription_id == subscription_id,
                    ProcessedEventModel.event_id == event_id
                )
            )
            existing = result.scalars().first()
            
            if existing:
                return False  # 已处理过
                
            # 创建新记录
            processed_event = ProcessedEventModel(
                subscription_id=subscription_id,
                event_id=event_id,
                processed_at=time.time()
            )
            session.add(processed_event)
            
            # 更新订阅对象的内部状态
            result = await session.execute(
                select(EventSubscriptionModel).where(
                    EventSubscriptionModel.id == subscription_id
                )
            )
            db_subscription = result.scalars().first()
            
            if not db_subscription:
                await session.rollback()
                return False
                
            try:
                await session.commit()
                return True
            except Exception as e:
                await session.rollback()
                return False
    
    async def batch_mark_processed(self, subscription_id: str, event_ids: List[str]) -> bool:
        """批量标记事件为已处理"""
        all_success = True
        async with self.async_session() as session:
            try:
                for event_id in event_ids:
                    # 检查记录是否已存在
                    result = await session.execute(
                        select(ProcessedEventModel).where(
                            ProcessedEventModel.subscription_id == subscription_id,
                            ProcessedEventModel.event_id == event_id
                        )
                    )
                    existing = result.scalars().first()
                    
                    if not existing:
                        # 创建新记录
                        processed_event = ProcessedEventModel(
                            subscription_id=subscription_id,
                            event_id=event_id,
                            processed_at=time.time()
                        )
                        session.add(processed_event)
                
                await session.commit()
                return True
            except Exception:
                await session.rollback()
                return False


class SqlalchemyEventProcessingTracker(EventProcessingTracker):
    """基于 SQLAlchemy 的事件处理记录实现"""
    
    def __init__(self, engine):
        """
        初始化 SQLAlchemy 事件处理记录
        
        Args:
            engine: SQLAlchemy 引擎
        """
        self.engine = engine
        self.async_session = sessionmaker(
            engine, expire_on_commit=False, class_=AsyncSession
        )
    
    async def mark_event_processed(self, event_id: str, handler_id: str) -> bool:
        """标记事件为已处理状态"""
        async with self.async_session() as session:
            # 检查是否已有成功的处理记录
            result = await session.execute(
                select(EventProcessingHistoryModel).where(
                    EventProcessingHistoryModel.event_id == event_id,
                    EventProcessingHistoryModel.handler_id == handler_id,
                    EventProcessingHistoryModel.status == "success"
                )
            )
            existing = result.scalars().first()
            
            if existing:
                return False  # 已处理过
            
            # 创建新记录
            record = EventProcessingHistoryModel(
                event_id=event_id,
                handler_id=handler_id,
                status="success",
                timestamp=time.time()
            )
            session.add(record)
            
            try:
                await session.commit()
                return True
            except Exception:
                await session.rollback()
                return False
    
    async def is_event_processed(self, event_id: str, handler_id: str) -> bool:
        """检查事件是否已被处理"""
        async with self.async_session() as session:
            result = await session.execute(
                select(EventProcessingHistoryModel).where(
                    EventProcessingHistoryModel.event_id == event_id,
                    EventProcessingHistoryModel.handler_id == handler_id,
                    EventProcessingHistoryModel.status == "success"
                )
            )
            existing = result.scalars().first()
            return existing is not None
    
    async def cleanup_old_records(self, max_age_seconds: int = 86400) -> int:
        """清理旧的处理记录"""
        cutoff_time = time.time() - max_age_seconds
        
        async with self.async_session() as session:
            delete_stmt = sa.delete(EventProcessingHistoryModel).where(
                EventProcessingHistoryModel.timestamp < cutoff_time
            )
            result = await session.execute(delete_stmt)
            await session.commit()
            return result.rowcount
    
    async def record_processing_attempt(self, event_id: str, handler_id: str, 
                                      status: str, error: Optional[str] = None) -> None:
        """记录处理尝试，包括成功和失败"""
        async with self.async_session() as session:
            record = EventProcessingHistoryModel(
                event_id=event_id,
                handler_id=handler_id,
                status=status,
                error=error,
                timestamp=time.time()
            )
            session.add(record)
            await session.commit()
    
    async def get_processing_history(self, event_id: str) -> List[Dict[str, Any]]:
        """获取事件的处理历史"""
        async with self.async_session() as session:
            result = await session.execute(
                select(EventProcessingHistoryModel)
                .where(EventProcessingHistoryModel.event_id == event_id)
                .order_by(EventProcessingHistoryModel.timestamp)
            )
            records = result.scalars().all()
            
            return [
                {
                    "event_id": record.event_id,
                    "handler_id": record.handler_id,
                    "status": record.status,
                    "error": record.error,
                    "timestamp": record.timestamp
                }
                for record in records
            ]


class SqlalchemyEventBus(EventBus):
    """基于 SQLAlchemy 的事件总线实现"""
    
    def __init__(self, 
                engine,
                create_tables: bool = False,
                min_retry_interval: float = 1.0):
        """
        初始化 SQLAlchemy 事件总线
        
        Args:
            engine: SQLAlchemy 异步引擎
            create_tables: 是否自动创建表结构
            min_retry_interval: 最小重试间隔(秒)
        """
        self.engine = engine
        self.min_retry_interval = min_retry_interval
        
        # 创建存储组件
        self.event_storage = SqlalchemyEventStorage(engine, create_tables)
        self.subscription_storage = SqlalchemyEventSubscriptionStorage(engine)
        self.processing_tracker = SqlalchemyEventProcessingTracker(engine)
        
        # 事件处理器
        self.handlers = {}
        self.lock = asyncio.Lock()

        # 等待中的 future
        self.waiting_futures = {}

    async def ensure_tables(self) -> None:
        """显式创建所有事件相关表结构 (幂等)。

        委托给 ``event_storage.ensure_tables()``; 因为所有事件模型共享同一个
        ``Base.metadata``, 一次 ``create_all`` 会建出 events / subscriptions /
        processing_records 全部表。构造时传 ``create_tables=True`` 的调用方,
        也可显式 ``await bus.ensure_tables()`` 替代。
        """
        await self.event_storage.ensure_tables()

    async def _ensure_tables_if_pending(self) -> None:
        if self.event_storage._tables_pending:
            await self.ensure_tables()
    
    async def publish(self, event: Union[Event, str, Dict[str, Any]], 
                    prevent_duplicate_consumption: bool = True, 
                    **kwargs) -> str:
        """发布事件"""
        # 规范化事件对象
        if not isinstance(event, Event):
            event = normalize_event(event, **kwargs)
        
        # 存储事件
        event_id = await self.event_storage.store_event(event)
        
        # 分发事件给等待的监听器
        await self._notify_waiters(event)
        
        # 创建分发任务
        asyncio.create_task(self._dispatch_event(event, prevent_duplicate_consumption))
        
        return event_id
    
    async def _notify_waiters(self, event: Event):
        """通知所有等待此类型事件的监听器"""
        event_type = event.event_type
        if event_type in self.waiting_futures:
            waiters = self.waiting_futures[event_type]
            for waiter in waiters[:]:  # 创建副本以避免修改迭代中的列表
                if not waiter.done():
                    waiter.set_result(event)
            # 清理已完成的等待器
            self.waiting_futures[event_type] = [w for w in waiters if not w.done()]
    
    async def _dispatch_event(self, event: Event, prevent_duplicate_consumption: bool):
        """分发事件到所有匹配的处理器"""
        # 查找所有匹配的处理器
        matching_handlers = []
        
        for event_type, handlers in self.handlers.items():
            # 跳过配置存储（handler_id作为key的配置）
            if isinstance(handlers, dict) and 'event_type' in handlers:
                continue
                
            # 使用统一的通配符匹配逻辑
            if EventBus.matches_event_type(event_type, event.event_type):
                for handler_id, handler in handlers.items():
                    config = self.handlers.get(handler_id, {})
                    filter_condition = config.get('filter_condition')
                    
                    # 检查过滤条件
                    if filter_condition:
                        temp_sub = EventSubscription(
                            subscription_id="temp",
                            event_type=event_type,
                            filter_condition=filter_condition
                        )
                        if not temp_sub.matches_event(event, {}):
                            continue
                    
                    matching_handlers.append((handler_id, handler))
        
        # 处理所有匹配的处理器
        for handler_id, handler in matching_handlers:
            # 检查是否需要去重
            if prevent_duplicate_consumption:
                is_new = await self.processing_tracker.mark_event_processed(
                    event.event_id, handler_id
                )
                if not is_new:
                    continue  # 已处理过，跳过
            
            # 获取处理器配置
            config = self.handlers.get(handler_id, {})
            retry_policy = config.get('retry_policy')
            
            # 如果有重试策略，使用它
            if retry_policy:
                await self._execute_with_retry(handler, event, handler_id, retry_policy)
            else:
                # 直接执行，不重试
                try:
                    await handler(event)
                    await self.processing_tracker.record_processing_attempt(
                        event.event_id, handler_id, "success"
                    )
                except Exception as e:
                    await self.processing_tracker.record_processing_attempt(
                        event.event_id, handler_id, "error", str(e)
                    )
    
    async def _execute_with_retry(self, handler: EventHandler, event: Event, 
                                handler_id: str, retry_policy: RetryPolicy):
        """使用重试策略执行处理器"""
        retries = 0
        last_error = None
        
        while retries <= retry_policy.max_retries:
            try:
                await handler(event)
                # 成功处理
                await self.processing_tracker.record_processing_attempt(
                    event.event_id, handler_id, "success"
                )
                return
            except Exception as e:
                last_error = e
                retries += 1
                
                # 记录重试尝试
                await self.processing_tracker.record_processing_attempt(
                    event.event_id, handler_id, "retry", str(e)
                )
                
                # 计算下一次重试的延迟
                if retries <= retry_policy.max_retries:
                    delay = min(
                        retry_policy.initial_delay * (retry_policy.backoff_factor ** (retries - 1)),
                        retry_policy.max_delay
                    )
                    await asyncio.sleep(delay)
        
        # 所有重试都失败了
        await self.processing_tracker.record_processing_attempt(
            event.event_id, handler_id, "failure", str(last_error)
        )
    
    async def register_subscription(self, 
                                  event_type: str, 
                                  filter_condition: Optional[Dict[str, Any]] = None,
                                  correlation_id: Optional[str] = None,
                                  flow_id: Optional[str] = None,
                                  node_id: Optional[str] = None,
                                  timeout: Optional[float] = None) -> str:
        """注册事件订阅"""
        await self._ensure_tables_if_pending()
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
        await self._ensure_tables_if_pending()
        return await self.subscription_storage.delete_subscription(subscription_id)
    
    async def wait_for_event(self, event_type: str, 
                           timeout: Optional[float] = None,
                           condition: Optional[Callable[[Event], bool]] = None) -> Event:
        """等待特定类型的事件发生"""
        future = asyncio.get_running_loop().create_future()
        
        if event_type not in self.waiting_futures:
            self.waiting_futures[event_type] = []
        
        self.waiting_futures[event_type].append(future)
        
        try:
            if timeout:
                # 带超时等待
                event = await asyncio.wait_for(future, timeout=timeout)
            else:
                # 无限等待
                event = await future
                
            # 如果有附加条件，确保事件满足条件
            if condition and not condition(event):
                # 重新等待符合条件的事件
                return await self.wait_for_event(event_type, timeout, condition)
                
            return event
        except asyncio.TimeoutError:
            # 超时时从等待列表中移除
            if event_type in self.waiting_futures:
                self.waiting_futures[event_type].remove(future)
            raise EventTimeoutError(event_type, timeout)
    
    async def register_handler(self, event_type: Optional[str] = None, 
                             handler: EventHandler = None,
                             filter_condition: Optional[Dict[str, Any]] = None,
                             retry_policy: Optional[RetryPolicy] = None) -> str:
        """注册事件处理器"""
        # 生成处理器ID
        handler_id = str(uuid.uuid4())
        
        # 存储处理器配置
        self.handlers[handler_id] = {
            'filter_condition': filter_condition,
            'retry_policy': retry_policy,
            'event_type': event_type
        }
        
        # 为事件类型注册处理器
        effective_event_type = event_type or "*"
        if effective_event_type not in self.handlers:
            self.handlers[effective_event_type] = {}
        self.handlers[effective_event_type][handler_id] = handler
        
        return handler_id
    
    async def unregister_handler(self, handler_id: str) -> bool:
        """取消注册事件处理器"""
        if handler_id not in self.handlers:
            return False
        
        # 从事件类型中移除此处理器
        config = self.handlers.get(handler_id, {})
        event_type = config.get('event_type')
        if event_type and event_type in self.handlers and handler_id in self.handlers[event_type]:
            del self.handlers[event_type][handler_id]
        
        # 移除处理器配置
        del self.handlers[handler_id]
        return True
    
    async def get_event(self, event_id: str) -> Event:
        """获取事件"""
        event = await self.event_storage.get_event(event_id)
        if not event:
            raise EventNotFoundError(f"事件不存在: {event_id}")
        return event
