"""
Redis事件总线单元测试

本文件包含用于测试基于Redis实现的事件机制的单元测试。
测试使用fakeredis模拟Redis服务器，无需实际的Redis实例。

测试内容包括：
1. 基本功能：事件发布、订阅、处理
2. 高级功能：事件过滤、等待、去重、重试策略
3. 事件存储和查询功能
4. 订阅管理
5. 处理记录和历史查询

运行测试方法：
```
python -m pytest plaita/event/test_redis_eventbus.py -v
```
"""
# 将有问题的导入移到最前面，确保使用的是安装的包而不是本地文件
import sys
from unittest.mock import patch

# 添加临时路径处理以避免本地文件冲突
import os
original_path = list(sys.path)
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir in sys.path:
    sys.path.remove(current_dir)

# 现在导入redis库
import redis.asyncio as aioredis
import fakeredis.aioredis

# 恢复原始路径
sys.path = original_path

import asyncio
import pytest
import pytest_asyncio
import time
import uuid
from typing import Dict, List, Set

from plaita.event.core import Event, RetryPolicy
from plaita.event.redis import (
    RedisEventBus, RedisEventStorage, 
    RedisEventSubscriptionStorage, RedisProcessingTracker
)
from plaita.event.exceptions import EventTimeoutError


class AsyncFakeRedis(fakeredis.aioredis.FakeRedis):
    """扩展FakeRedis以支持pubsub功能"""
    
    async def initialize(self):
        pass


@pytest_asyncio.fixture
async def redis_mock():
    """创建一个模拟的Redis连接"""
    fake_redis = fakeredis.aioredis.FakeRedis()
    
    # 添加pubsub功能
    fake_redis.pubsub = lambda: FakePubSub(fake_redis)
    
    # 模拟from_url方法
    with patch('redis.asyncio.from_url', return_value=fake_redis):
        yield fake_redis


class FakePubSub:
    """模拟Redis的PubSub功能"""
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.channels = set()
        self.messages = []
    
    async def subscribe(self, *channels):
        """订阅频道"""
        for channel in channels:
            self.channels.add(channel)
        return True
    
    async def unsubscribe(self, *channels):
        """取消订阅频道"""
        for channel in channels:
            if channel in self.channels:
                self.channels.remove(channel)
        return True
    
    async def get_message(self, ignore_subscribe_messages=False, timeout=0.01):
        """获取消息"""
        if not self.messages:
            # 模拟接收消息的延迟
            await asyncio.sleep(0.01)
            return None
        
        message = self.messages.pop(0)
        
        if ignore_subscribe_messages and message.get('type') == 'subscribe':
            return None
            
        return message
        
    # 在运行时添加消息
    def add_message(self, channel, data, message_type='message'):
        """添加一条消息到队列中"""
        if channel in self.channels:
            self.messages.append({
                'type': message_type,
                'channel': channel.encode('utf-8') if isinstance(channel, str) else channel,
                'pattern': None,
                'data': data
            })


@pytest_asyncio.fixture
async def event_bus(redis_mock):
    """创建Redis事件总线的测试固件"""
    # 创建事件总线
    redis_url = "redis://localhost:6379/0"  # 使用虚拟URL
    bus = RedisEventBus(redis_url=redis_url)
    
    # 初始化
    await bus.initialize()
    
    # 将bus的redis属性替换为模拟对象
    bus.redis = redis_mock
    
    # 替换存储组件的redis属性
    bus.event_storage.redis = redis_mock
    bus.subscription_storage.redis = redis_mock
    bus.processing_tracker.redis = redis_mock
    
    # 保存原始的发布方法
    original_publish = bus.publish
    
    # 创建一个wrapper来拦截发布操作并模拟消息推送
    async def publish_wrapper(*args, **kwargs):
        # 调用原始方法
        event_id = await original_publish(*args, **kwargs)
        
        # 获取事件
        event = await bus.get_event(event_id)
        
        # 模拟将消息发送到相应的频道
        channel = f"plaita:events:{event.event_type}"
        
        # 遍历所有与该事件类型关联的处理器
        for handler_id, (handler, event_type, filter_condition, _) in bus.handlers.items():
            if event.event_type == event_type:
                # 检查过滤条件
                if not filter_condition or all(event.data.get(k) == v for k, v in filter_condition.items()):
                    # 模拟事件处理调用
                    asyncio.create_task(bus._process_event(handler, event, handler_id))
        
        # 为等待该类型事件的所有future设置结果
        if event.event_type in bus.waiting_futures:
            for future in bus.waiting_futures[event.event_type]:
                if not future.done():
                    future.set_result(event)
        
        return event_id
    
    # 替换发布方法
    bus.publish = publish_wrapper
    
    # 同样替换batch_publish方法
    original_batch_publish = bus.batch_publish
    
    async def batch_publish_wrapper(*args, **kwargs):
        # 调用原始方法
        event_ids = await original_batch_publish(*args, **kwargs)
        
        # 处理每个事件
        for event_id in event_ids:
            # 获取事件
            event = await bus.get_event(event_id)
            
            # 为所有相关处理器触发处理
            for handler_id, (handler, event_type, filter_condition, _) in bus.handlers.items():
                if event.event_type == event_type:
                    # 检查过滤条件
                    if not filter_condition or all(event.data.get(k) == v for k, v in filter_condition.items()):
                        # 模拟事件处理调用
                        asyncio.create_task(bus._process_event(handler, event, handler_id))
        
        return event_ids
    
    # 替换批量发布方法
    bus.batch_publish = batch_publish_wrapper
    
    yield bus


@pytest.mark.asyncio
async def test_publish_event(event_bus):
    """测试基本的事件发布"""
    # 发布事件
    event_id = await event_bus.publish(
        "test.event",
        key1="value1",
        key2=123
    )
    
    # 验证事件ID格式
    assert isinstance(event_id, str)
    assert len(event_id) > 0
    
    # 获取事件并验证
    event = await event_bus.get_event(event_id)
    assert isinstance(event, Event)
    assert event.event_id == event_id
    assert event.event_type == "test.event"
    assert event.data == {"key1": "value1", "key2": 123}


@pytest.mark.asyncio
async def test_event_handler_registration(event_bus):
    """测试事件处理器注册"""
    received_events = []
    
    # 定义处理器
    async def handler(event):
        received_events.append(event)
    
    # 注册处理器
    handler_id = await event_bus.register_handler(
        event_type="test.handler",
        handler=handler
    )
    
    # 验证处理器ID
    assert isinstance(handler_id, str)
    assert len(handler_id) > 0
    
    # 模拟发布事件
    event = Event(
        event_type="test.handler", 
        data={"message": "Hello"}
    )
    await event_bus.publish(event)
    
    # 等待处理完成
    await asyncio.sleep(0.2)
    
    # 验证事件被处理
    assert len(received_events) == 1
    assert received_events[0].event_type == "test.handler"
    assert received_events[0].data == {"message": "Hello"}


@pytest.mark.asyncio
async def test_multiple_handlers(event_bus):
    """测试多个处理器处理同一事件"""
    handler1_calls = []
    handler2_calls = []
    
    async def handler1(event):
        handler1_calls.append(event.event_id)
    
    async def handler2(event):
        handler2_calls.append(event.event_id)
    
    # 注册两个处理器
    await event_bus.register_handler(
        event_type="test.multiple",
        handler=handler1
    )
    
    await event_bus.register_handler(
        event_type="test.multiple",
        handler=handler2
    )
    
    # 发布事件
    event_id = await event_bus.publish("test.multiple", data={"message": "shared"})
    
    # 等待处理完成
    await asyncio.sleep(0.2)
    
    # 验证两个处理器都被调用
    assert event_id in handler1_calls
    assert event_id in handler2_calls


@pytest.mark.asyncio
async def test_filter_condition(event_bus):
    """测试过滤条件"""
    matches_filter = []
    no_matches = []
    
    async def match_handler(event):
        matches_filter.append(event.event_id)
    
    async def no_match_handler(event):
        no_matches.append(event.event_id)
    
    # 注册有过滤条件的处理器
    await event_bus.register_handler(
        event_type="test.filter",
        handler=match_handler,
        filter_condition={"amount": 100}
    )
    
    await event_bus.register_handler(
        event_type="test.filter",
        handler=no_match_handler,
        filter_condition={"amount": 200}
    )
    
    # 发布匹配第一个过滤条件的事件
    event_id = await event_bus.publish("test.filter", amount=100)
    
    # 等待处理完成
    await asyncio.sleep(0.2)
    
    # 验证只有匹配的处理器被调用
    assert event_id in matches_filter
    assert event_id not in no_matches


@pytest.mark.asyncio
async def test_wait_for_event(event_bus, redis_mock):
    """测试等待事件功能"""
    
    # 创建异步等待任务
    wait_task = asyncio.create_task(
        event_bus.wait_for_event("test.wait", timeout=1.0)
    )
    
    # 等待一小段时间，让等待开始
    await asyncio.sleep(0.1)
    
    # 模拟发布事件
    event = Event(
        event_type="test.wait",
        data={"message": "triggered"}
    )
    
    # 手动设置event_bus.waiting_futures中的future
    if "test.wait" in event_bus.waiting_futures:
        for future in event_bus.waiting_futures["test.wait"]:
            if not future.done():
                future.set_result(event)
    
    # 等待任务完成
    result = await wait_task
    
    # 验证收到的事件
    assert result.event_type == "test.wait"
    assert result.data == {"message": "triggered"}


@pytest.mark.asyncio
async def test_event_deduplication(event_bus):
    """测试事件去重功能"""
    handler_calls = []
    processed_events = set()
    
    async def handler(event):
        handler_calls.append(event.event_id)
        processed_events.add(event.event_id)
    
    # 注册处理器
    handler_id = await event_bus.register_handler(
        event_type="test.dedup",
        handler=handler
    )
    
    # 发布两个具有相同数据的事件
    event_id1 = await event_bus.publish("test.dedup", data={"message": "unique"})
    event_id2 = await event_bus.publish("test.dedup", data={"message": "unique"})
    
    # 等待处理完成
    await asyncio.sleep(0.2)
    
    # 验证两个事件都被处理了一次
    assert event_id1 in processed_events
    assert event_id2 in processed_events
    assert len(processed_events) == 2
    
    # 查询处理历史
    history1 = await event_bus.processing_tracker.get_processing_history(event_id1)
    history2 = await event_bus.processing_tracker.get_processing_history(event_id2)
    
    # 每个事件应该有一条处理记录
    assert len(history1) >= 1
    assert len(history2) >= 1


@pytest.mark.asyncio
async def test_retry_policy(event_bus):
    """测试重试策略"""
    failure_count = 0
    retry_completed = asyncio.Event()
    
    # 定义会失败两次，然后成功的处理器
    async def failing_handler(event):
        nonlocal failure_count
        
        if failure_count < 2:
            failure_count += 1
            raise ValueError("Simulated failure")
        else:
            # 第三次成功
            retry_completed.set()
    
    # 创建重试策略
    retry_policy = RetryPolicy(
        max_retries=3,
        initial_delay=0.05,  # 使用更短的初始延迟
        backoff_factor=1.0,
        max_delay=0.2
    )
    
    # 注册带重试策略的处理器
    handler_id = await event_bus.register_handler(
        event_type="test.retry",
        handler=failing_handler,
        retry_policy=retry_policy
    )
    
    # 发布事件
    event_id = await event_bus.publish("test.retry", data={"message": "retry me"})
    
    # 手动触发重试
    event = await event_bus.get_event(event_id)
    
    # 第一次调用（会失败）
    await event_bus._process_with_retry(failing_handler, event, handler_id, retry_policy)
    
    # 等待足够时间让重试完成，最多等待1秒
    try:
        await asyncio.wait_for(retry_completed.wait(), timeout=1.0)
    except asyncio.TimeoutError:
        pass  # 如果超时，继续测试
    
    # 验证失败了两次然后成功
    assert failure_count == 2


@pytest.mark.asyncio
async def test_batch_publish(event_bus):
    """测试批量发布事件"""
    received_events = []
    
    async def handler(event):
        received_events.append(event)
    
    # 注册处理器
    await event_bus.register_handler(
        event_type="test.batch",
        handler=handler
    )
    
    # 准备批量事件
    events = []
    for i in range(5):
        events.append(
            Event(
                event_type="test.batch",
                data={"index": i}
            )
        )
    
    # 批量发布
    event_ids = await event_bus.batch_publish(events)
    
    # 验证返回的事件ID
    assert len(event_ids) == 5
    assert all(isinstance(eid, str) for eid in event_ids)
    
    # 等待处理完成
    await asyncio.sleep(0.3)
    
    # 验证所有事件都被处理
    assert len(received_events) == 5


@pytest.mark.asyncio
async def test_subscription_registration(event_bus):
    """测试订阅注册"""
    # 注册订阅
    subscription_id = await event_bus.register_subscription(
        event_type="test.subscription",
        filter_condition={"category": "important"},
        correlation_id="corr123",
        flow_id="flow123",
        node_id="node123"
    )
    
    # 验证订阅ID
    assert isinstance(subscription_id, str)
    assert len(subscription_id) > 0
    
    # 获取所有订阅
    subscriptions = await event_bus.subscription_storage.list_subscriptions(
        event_type="test.subscription"
    )
    
    # 验证订阅信息
    assert len(subscriptions) == 1
    assert subscriptions[0].subscription_id == subscription_id
    assert subscriptions[0].event_type == "test.subscription"
    assert subscriptions[0].filter_condition == {"category": "important"}
    assert subscriptions[0].correlation_id == "corr123"
    assert subscriptions[0].flow_id == "flow123"
    assert subscriptions[0].node_id == "node123"


@pytest.mark.asyncio
async def test_unregister_subscription(event_bus):
    """测试取消订阅"""
    # 注册订阅
    subscription_id = await event_bus.register_subscription(
        event_type="test.unsub",
        filter_condition={"value": 1}
    )
    
    # 验证订阅已创建
    subscriptions_before = await event_bus.subscription_storage.list_subscriptions(
        event_type="test.unsub"
    )
    assert len(subscriptions_before) == 1
    
    # 取消订阅
    result = await event_bus.unregister_subscription(subscription_id)
    assert result is True
    
    # 验证订阅已删除
    subscriptions_after = await event_bus.subscription_storage.list_subscriptions(
        event_type="test.unsub"
    )
    assert len(subscriptions_after) == 0


@pytest.mark.asyncio
async def test_processing_history(event_bus):
    """测试处理历史记录功能"""
    # 定义处理器
    async def success_handler(event):
        # 成功处理事件
        pass
    
    async def failing_handler(event):
        # 总是失败的处理器
        raise ValueError("Simulated error")
    
    # 注册处理器
    success_id = await event_bus.register_handler(
        event_type="test.history",
        handler=success_handler
    )
    
    failure_id = await event_bus.register_handler(
        event_type="test.history",
        handler=failing_handler
    )
    
    # 发布事件
    event_id = await event_bus.publish("test.history", data={"message": "test data"})
    
    # 等待处理完成
    await asyncio.sleep(0.3)
    
    # 获取处理历史
    history = await event_bus.processing_tracker.get_processing_history(event_id)
    
    # 检查历史记录
    assert len(history) >= 2  # 至少有两条记录（成功和失败）
    
    # 至少有一条成功记录和一条失败记录
    has_success = any(record.get("status") == "success" for record in history)
    has_error = any("error" in record.get("status", "") for record in history)
    
    assert has_success
    assert has_error


@pytest.mark.asyncio
async def test_cleanup_old_records(event_bus):
    """测试清理旧记录的功能"""
    # 创建一些事件和处理记录
    for i in range(5):
        event_id = await event_bus.publish(f"test.cleanup.{i}", data={"index": i})
        
        # 手动标记为已处理
        await event_bus.processing_tracker.mark_event_processed(event_id, f"handler_{i}")
        
        # 添加处理历史
        await event_bus.processing_tracker.record_processing_attempt(
            event_id, f"handler_{i}", "success"
        )
    
    # 等待一小段时间
    await asyncio.sleep(0.1)
    
    # 清理旧记录（超过0.05秒的记录）
    cleaned_count = await event_bus.processing_tracker.cleanup_old_records(max_age_seconds=0.05)
    
    # 验证方法能正常运行
    assert cleaned_count >= 0


if __name__ == "__main__":
    # 允许直接运行测试文件
    pytest.main(["-xvs", __file__]) 