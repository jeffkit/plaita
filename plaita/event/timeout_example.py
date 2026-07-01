"""
演示如何使用独立的SubscriptionTimeoutChecker
"""
import asyncio
import time
from typing import Dict, Any

from plaita.event import (
    Event, EventSubscription, InMemoryEventBus, SubscriptionTimeoutChecker, 
    get_eventbus
)

async def timeout_callback(subscription: EventSubscription) -> None:
    """订阅超时的回调函数"""
    print(f"订阅已超时: {subscription.subscription_id}")
    print(f"订阅类型: {subscription.event_type}")
    print(f"超时时间: {subscription.timeout} 秒")
    print(f"创建时间: {time.ctime(subscription.created_at)}")
    
    # 可以在这里进行处理，例如发送通知或记录日志
    # 也可以根据需要自动取消订阅
    event_bus = get_eventbus()
    await event_bus.unregister_subscription(subscription.subscription_id)


async def main():
    # 创建事件总线
    event_bus = InMemoryEventBus()
    
    # 创建独立的超时检查器
    timeout_checker = SubscriptionTimeoutChecker(
        event_bus.subscription_storage,
        check_interval=1.0  # 每秒检查一次，用于演示
    )
    
    # 注册超时回调
    timeout_checker.register_timeout_callback(timeout_callback)
    
    # 启动超时检查器
    await timeout_checker.start()
    
    print("创建带超时的订阅...")
    
    # 创建一个3秒超时的订阅
    subscription_id = await event_bus.register_subscription(
        event_type="test.event",
        filter_condition={"key": "value"},
        timeout=3.0  # 3秒后超时
    )
    
    print(f"已创建订阅: {subscription_id}")
    print("等待订阅超时...")
    
    # 等待足够长的时间以触发超时
    await asyncio.sleep(5)
    
    # 停止超时检查器
    await timeout_checker.stop()
    print("超时检查器已停止")


if __name__ == "__main__":
    asyncio.run(main()) 