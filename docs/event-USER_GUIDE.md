# Plaita 事件系统使用手册

## 1. 概述

Plaita 事件系统是一个灵活、可靠的事件驱动架构实现，支持多种事件发布和订阅模式。本指南将帮助您快速上手并有效利用事件系统的各项功能。

![Plaita 事件系统工作流程](./images/usage-workflow.svg)

### 1.1 主要功能

- 事件发布与订阅
- 基于条件的事件过滤
- 事件处理的重试机制
- 支持内存、Redis和SQLAlchemy三种实现方式
- 支持混合后端模式，自由组合不同组件
- 异步事件处理
- 防止重复消费

### 1.2 核心概念

- **事件(Event)**: 系统中发生的一个动作或状态变化的通知
- **事件总线(EventBus)**: 负责事件的分发和管理
- **事件处理器(EventHandler)**: 响应并处理特定类型事件的函数
- **事件订阅(EventSubscription)**: 定义对哪些事件感兴趣以及如何过滤事件
- **事件存储(EventStorage)**: 负责事件的持久化存储
- **事件订阅存储(EventSubscriptionStorage)**: 管理事件订阅信息
- **事件处理跟踪器(EventProcessingTracker)**: 跟踪事件处理状态和历史

## 2. 快速入门

### 2.1 安装依赖

```python
# Redis实现需要额外安装redis.asyncio
pip install redis

# SQLAlchemy实现需要安装SQLAlchemy和异步驱动
pip install sqlalchemy aiosqlite  # 或其他异步数据库驱动
```

### 2.2 基本使用示例

```python
import asyncio
from plaita.event import InMemoryEventBus, Event

# 创建事件总线
event_bus = InMemoryEventBus()

# 定义事件处理函数
async def handle_user_created(event: Event):
    print(f"用户创建事件: {event.data}")

# 注册事件处理器
async def main():
    # 注册处理器
    handler_id = await event_bus.register_handler(
        event_type="user.created",
        handler=handle_user_created
    )
    
    # 发布事件
    event_id = await event_bus.publish(
        "user.created",
        user_id="123",
        username="张三"
    )
    
    print(f"已发布事件: {event_id}")
    
    # 等待事件处理完成
    await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
```

### 2.3 使用统一演示程序

Plaita提供了统一的演示程序，可以方便地测试不同后端的功能：

```bash
# 使用内存后端
python -m examples.event_demo.demo_eventbus --backend memory

# 使用Redis后端
python -m examples.event_demo.demo_eventbus --backend redis --redis-url redis://localhost:6379/0

# 使用SQLAlchemy后端
python -m examples.event_demo.demo_eventbus --backend db --database-url sqlite+aiosqlite:///event_demo.db

# 使用混合后端
python -m examples.event_demo.demo_eventbus --backend mixed --bus-type redis --storage-type db
```

## 3. 发布事件

### 3.1 发布单个事件

事件可以通过多种方式发布：

```python
# 方式1: 使用事件类型字符串和关键字参数
event_id = await event_bus.publish("order.created", order_id="A12345", amount=100.0)

# 方式2: 使用事件字典
event_id = await event_bus.publish({
    "event_type": "product.updated",
    "product_id": "P789",
    "price": 29.99
})

# 方式3: 使用Event对象
from plaita.event import Event
event = Event(
    event_type="user.login",
    data={"user_id": "U001", "ip": "192.168.1.1"}
)
event_id = await event_bus.publish(event)
```

### 3.2 批量发布事件

对于需要批量发布的场景，可以使用`batch_publish`方法：

```python
events = [
    Event(event_type="stock.updated", data={"item_id": "A001", "quantity": 100}),
    Event(event_type="stock.updated", data={"item_id": "A002", "quantity": 50}),
    Event(event_type="price.updated", data={"item_id": "A001", "price": 19.99})
]

event_ids = await event_bus.batch_publish(events)
```

### 3.3 发布事件时禁用重复消费检查

在某些场景下，可能需要允许同一个事件被多次处理：

```python
event_id = await event_bus.publish(
    "notification.sent",
    user_id="U123",
    message="Hello",
    prevent_duplicate_consumption=False  # 允许重复消费
)
```

## 4. 订阅和处理事件

### 4.1 使用装饰器注册处理器

最简单的方式是使用`event_handler`装饰器：

```python
from plaita.event import event_handler

@event_handler(event_bus, ["user.created", "user.updated"])
async def handle_user_events(event: Event):
    user_id = event.data.get("user_id")
    print(f"处理用户事件: {event.event_type}, 用户ID: {user_id}")
```

### 4.2 直接注册处理器

更灵活的方式是使用`register_handler`方法：

```python
async def process_order(event: Event):
    order_id = event.data.get("order_id")
    print(f"处理订单: {order_id}")

handler_id = await event_bus.register_handler(
    event_type="order.created",
    handler=process_order
)

# 稍后可以取消注册
await event_bus.unregister_handler(handler_id)
```

### 4.3 使用过滤条件

可以添加过滤条件，只处理符合特定条件的事件：

```python
# 只处理VIP用户的事件
handler_id = await event_bus.register_handler(
    event_type="user.login",
    handler=handle_vip_login,
    filter_condition={"user_type": "vip"}
)

# 支持嵌套属性和正则表达式
handler_id = await event_bus.register_handler(
    event_type="payment.received",
    handler=handle_large_payment,
    filter_condition={
        "amount": 1000.0,  # 金额大于等于1000
        "payment.method": "credit_card",  # 嵌套属性
        "transaction_id": "regex:^TX\\d{10}$"  # 正则表达式匹配
    }
)
```

### 4.4 设置重试策略

对于重要的事件处理器，可以设置重试策略：

```python
from plaita.event import RetryPolicy

# 创建重试策略
retry_policy = RetryPolicy(
    max_retries=3,        # 最大重试次数
    initial_delay=1.0,    # 初始延迟(秒)
    backoff_factor=2.0,   # 退避因子
    max_delay=30.0        # 最大延迟(秒)
)

# 注册带重试的处理器
handler_id = await event_bus.register_handler(
    event_type="order.payment",
    handler=process_payment,
    retry_policy=retry_policy
)
```

## 5. 等待特定事件

在某些场景下，可能需要等待特定事件的发生：

```python
try:
    # 等待订单完成事件，最多等待60秒
    order_event = await event_bus.wait_for_event(
        "order.completed", 
        timeout=60.0,
        condition=lambda e: e.data.get("order_id") == "12345"
    )
    print(f"订单已完成: {order_event.data}")
except EventTimeoutError:
    print("等待订单完成超时")
```

## 6. 使用不同的后端实现

### 6.1 内存实现

适用于单机、单进程应用和测试环境：

```python
from plaita.event import InMemoryEventBus

# 创建基于内存的事件总线
memory_event_bus = InMemoryEventBus()

# 使用方式与基本示例相同
await memory_event_bus.publish("user.login", user_id="U123")
```

### 6.2 Redis实现

对于分布式环境，建议使用Redis实现：

```python
from plaita.event import RedisEventBus

# 创建基于Redis的事件总线
redis_event_bus = RedisEventBus(
    redis_url="redis://localhost:6379/0",
    event_key_prefix="myapp:event:",
    subscription_key_prefix="myapp:subscription:",
    processing_key_prefix="myapp:processed:"
)

# 使用方式与内存实现相同
await redis_event_bus.publish("user.login", user_id="U123")
```

### 6.3 SQLAlchemy实现

对于需要强持久化和复杂查询的场景：

```python
from plaita.event import SqlalchemyEventBus

# 创建基于SQLAlchemy的事件总线
db_event_bus = SqlalchemyEventBus(
    database_url="sqlite+aiosqlite:///events.db",
    create_tables=True,  # 首次运行时自动创建表结构
    min_retry_interval=1.0
)

# 需要确保初始化完成
await db_event_bus.initialize()

# 使用方式与其他实现相同
await db_event_bus.publish("user.login", user_id="U123")
```

### 6.4 混合后端模式

可以根据需求自由组合不同组件实现：

```python
from plaita.event import (
    RedisEventBus, SqlalchemyEventStorage,
    RedisEventSubscriptionStorage, SqlalchemyEventProcessingTracker
)

# 创建Redis事件总线
redis_bus = RedisEventBus(redis_url="redis://localhost:6379/0")

# 创建SQLAlchemy事件存储
db_storage = SqlalchemyEventStorage(
    database_url="sqlite+aiosqlite:///events.db",
    create_tables=True
)

# 初始化组件
await db_storage.initialize()

# 配置事件总线使用DB存储
redis_bus.event_storage = db_storage

# 使用配置好的混合事件总线
await redis_bus.publish("hybrid.event", message="使用混合后端")
```

#### 6.4.1 使用MixedEventBus简化混合模式

更简单的方式是直接使用`MixedEventBus`类：

```python
from plaita.event import MixedEventBus

# 创建混合后端事件总线
mixed_bus = MixedEventBus(
    bus_type="redis",           # 选择Redis作为事件总线实现
    storage_type="db",          # 选择数据库作为事件存储
    subscription_type="redis",  # 选择Redis作为订阅存储
    tracker_type="db",          # 选择数据库作为处理跟踪器
    redis_url="redis://localhost:6379/0",
    database_url="sqlite+aiosqlite:///events.db"
)

# 初始化
await mixed_bus.initialize()

# 使用混合事件总线
await mixed_bus.publish("mixed.event", data="测试混合模式")
```

## 7. 高级用法

### 7.1 使用订阅API

除了处理器API外，还可以使用较低级别的订阅API：

```python
# 注册订阅
subscription_id = await event_bus.register_subscription(
    event_types=["product.price_changed"],
    filter_condition={"category": "electronics"},
    correlation_id="batch-123"
)

# 稍后取消订阅
await event_bus.unregister_subscription(subscription_id)
```

### 7.2 集成到FlowEventHandler

对于工作流场景，可以使用`FlowEventHandler`：

```python
from plaita.event import FlowEventHandler

# 创建Flow事件处理器
flow_handler = FlowEventHandler(
    event_bus=event_bus,
    subscription_storage=event_bus.subscription_storage,
    flow_runner=async_execute_flow_node
)

# 启动处理器
await flow_handler.start()

# 注册Flow事件
subscription_id = await flow_handler.register_flow_event(
    event_types=["document.approved"],
    flow_id="flow-123",
    node_id="approval-node"
)
```

### 7.3 查询事件历史

可以查询事件的处理历史：

```python
# 获取特定事件的处理历史
history = await event_bus.processing_tracker.get_processing_history("event-123")

for record in history:
    print(f"处理时间: {record['timestamp']}")
    print(f"处理状态: {record['status']}")
    if record['error']:
        print(f"错误信息: {record['error']}")
```

### 7.4 混合后端最佳实践

根据应用场景选择最合适的组件组合：

1. **高并发事件处理 + 持久化存储**
   ```python
   # 使用Redis事件总线和Redis订阅存储处理高并发事件
   # 使用DB存储事件数据和处理历史以便于分析和审计
   mixed_bus = MixedEventBus(
       bus_type="redis", 
       storage_type="db",
       subscription_type="redis", 
       tracker_type="db"
   )
   ```

2. **单服务部署 + 集中存储**
   ```python
   # 使用内存事件总线处理本地事件
   # 使用Redis存储事件，方便其他服务查询
   mixed_bus = MixedEventBus(
       bus_type="memory", 
       storage_type="redis",
       subscription_type="memory", 
       tracker_type="redis"
   )
   ```

3. **性能优先 + 分析需求**
   ```python
   # 所有实时组件使用Redis
   # 仅跟踪器使用DB，方便后续分析处理记录
   mixed_bus = MixedEventBus(
       bus_type="redis", 
       storage_type="redis",
       subscription_type="redis", 
       tracker_type="db"
   )
   ```

### 7.5 命令行工具使用

使用内置的演示程序体验不同后端：

```bash
# 基本用法
python -m examples.event_demo.demo_eventbus --help

# 不同后端选项示例
python -m examples.event_demo.demo_eventbus --backend memory
python -m examples.event_demo.demo_eventbus --backend redis --redis-url redis://localhost:6379/0
python -m examples.event_demo.demo_eventbus --backend db --database-url postgresql+asyncpg://user:pass@localhost/db

# 混合后端示例
python -m examples.event_demo.demo_eventbus --backend mixed \
  --bus-type redis \
  --storage-type db \
  --subscription-type redis \
  --tracker-type db \
  --redis-url redis://localhost:6379/0 \
  --database-url sqlite+aiosqlite:///events.db
```

## 8. 最佳实践

![事件系统最佳实践](./images/best-practices.svg)

### 8.1 事件命名约定

采用清晰的事件命名约定：`{领域}.{动作}`，例如：

- `user.created`
- `order.payment_received`
- `product.price_updated`

### 8.2 事件数据设计

- 保持事件数据简洁，仅包含必要信息
- 避免在事件中包含敏感信息
- 考虑使用ID引用而非完整对象拷贝

### 8.3 性能优化

- 批量发布频繁产生的事件
- 使用具体的事件类型而非过于宽泛的类型
- 定期清理历史数据避免存储膨胀

### 8.4 错误处理

- 对重要事件处理器设置重试策略
- 记录事件处理失败并设置告警
- 考虑实现死信队列处理无法成功处理的事件

### 8.5 测试策略

- 单独测试事件处理器逻辑
- 使用内存实现进行集成测试
- 模拟事件发布进行端到端测试

## 9. 常见问题解答

**Q: 事件系统是否支持多个实例并发运行？**

A: 是的，使用Redis实现时，多个实例可以并发运行并共享事件。

**Q: 如何确保事件不会丢失？**

A: 事件总线会持久化存储事件，即使系统崩溃，在恢复后也能处理未完成的事件。

**Q: 处理器抛出异常会发生什么？**

A: 默认情况下，异常会被记录但不会中断处理流程。如果设置了重试策略，则会按照策略进行重试。

**Q: 如何在大量事件下保持性能？**

A: 使用批量操作、精确的过滤条件、合理的索引设计，以及适当的清理策略。

**Q: 如何选择合适的后端组合？**

A: 根据应用场景特点选择：
- 高并发场景优先使用Redis实现
- 需要可靠存储和复杂查询使用SQLAlchemy实现
- 对事件历史有分析需求时使用DB作为跟踪器
- 开发测试环境可使用内存实现或混合模式

**Q: 混合模式会带来额外开销吗？**

A: 混合模式确实会引入不同后端之间的转换开销，但通过合理组合可最大化各后端优势，总体收益通常大于开销。

**Q: 如何从单一后端迁移到混合后端？**

A: 系统设计支持平滑迁移：
1. 先引入MixedEventBus，配置为全部使用原后端
2. 逐步替换单个组件到新后端
3. 验证系统稳定性后继续替换其他组件 

# 使用独立的超时检查器

对于需要处理订阅超时的场景，我们提供了一个独立的`SubscriptionTimeoutChecker`类。这个类可以与任何`EventBus`实现一起使用，只需共享同一个订阅存储接口。

## 基本用法

```python
from plaita.event import InMemoryEventBus, SubscriptionTimeoutChecker

# 创建事件总线
event_bus = InMemoryEventBus()

# 创建超时检查器
timeout_checker = SubscriptionTimeoutChecker(
    event_bus.subscription_storage,  # 使用事件总线的订阅存储接口
    check_interval=10.0  # 每10秒检查一次超时
)

# 注册超时回调函数
async def on_subscription_timeout(subscription):
    print(f"订阅 {subscription.subscription_id} 已超时")
    # 可以在这里执行超时后的操作，例如发送通知、清理资源等
    
    # 可选：自动取消已超时的订阅
    await event_bus.unregister_subscription(subscription.subscription_id)

timeout_checker.register_timeout_callback(on_subscription_timeout)

# 启动超时检查器
await timeout_checker.start()

# 注册带超时的订阅
subscription_id = await event_bus.register_subscription(
    event_type="order.created",
    filter_condition={"customer_id": "12345"},
    timeout=60.0  # 60秒超时
)

# 应用程序结束时停止超时检查器
await timeout_checker.stop()
```

## 适用场景

超时检查器特别适用于以下场景：

1. 等待特定事件的时间有限制，超过时间需要采取措施
2. 实现订阅的自动清理，防止无用订阅长期占用资源
3. 实现业务流程中的超时处理，如订单支付超时自动取消

## 与wait_for_event的区别

`