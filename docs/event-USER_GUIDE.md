# Plaita 事件系统使用手册

> **诚实性更正（2026-07-14）**  
> 生产控制面请用 **redis**（或 memory 单测）。`db`/SQLAlchemy 为 experimental（`PLAITA_ALLOW_EXPERIMENTAL_DB=1`）。  
> FlowWorker 任务队列为 Redis Stream（at-least-once），不是 List。详见 docs-site `distributed/ops-runbook.md`。

## 1. 概述

Plaita 事件系统是一个灵活的事件驱动架构实现，支持多种事件发布和订阅模式。本指南将帮助您快速上手并有效利用事件系统的各项功能。

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

可以根据需求自由组合不同组件实现（替换 `event_storage` / `subscription_storage` /
`processing_tracker` 三个属性即可）：

```python
from sqlalchemy.ext.asyncio import create_async_engine

from plaita.event import (
    RedisEventBus, SqlalchemyEventStorage, SqlalchemyEventProcessingTracker
)

# 创建Redis事件总线
redis_bus = RedisEventBus(redis_url="redis://localhost:6379/0")
await redis_bus.initialize()

# 创建SQLAlchemy事件存储（第一个参数是 SQLAlchemy **异步引擎**，不是 URL）
engine = create_async_engine("sqlite+aiosqlite:///events.db")
db_storage = SqlalchemyEventStorage(engine, create_tables=True)

# 建表（方法名是 ensure_tables；create_tables=True 时首次写操作也会惰性建表）
await db_storage.ensure_tables()

# 配置事件总线使用DB存储与DB跟踪器
redis_bus.event_storage = db_storage
redis_bus.processing_tracker = SqlalchemyEventProcessingTracker(engine)

# 使用配置好的混合事件总线
await redis_bus.publish("hybrid.event", message="使用混合后端")
```

#### 6.4.1 `MixedEventBus` 不存在（历史文档臆造，未实现）

早期文档里的 `MixedEventBus(bus_type=..., storage_type=..., subscription_type=...,
tracker_type=...)` **在代码中并不存在**：`plaita.event` 没有这个类，`bus_type` /
`storage_type` / `subscription_type` / `tracker_type` 这四个参数也是臆造的
（`grep -rn MixedEventBus plaita/` 零命中）。

混合后端**目前唯一**的做法就是上面 §6.4 的手工组合：分别构造各组件，再把
`event_storage` / `subscription_storage` / `processing_tracker` 属性替换掉。

## 7. 高级用法

### 7.1 使用订阅API

除了处理器API外，还可以使用较低级别的订阅API：

```python
# 注册订阅（注意：形参是单数 event_type，一个订阅一个事件类型）
subscription_id = await event_bus.register_subscription(
    event_type="product.price_changed",
    filter_condition={"category": "electronics"},
    correlation_id="batch-123"
)

# 稍后取消订阅
await event_bus.unregister_subscription(subscription_id)
```

### 7.2 在 Flow 中等待事件（`EventNode`）

事件与工作流的集成**不是**靠某个"Flow 事件处理器"类完成的（历史文档里的
`FlowEventHandler` 在代码中**不存在**：`plaita.event` 没有这个类，全仓
`grep -rn FlowEventHandler plaita/` 零命中），而是把 **`EventNode`** 放进 `Flow`：
节点执行即挂起并订阅事件，事件到达后由 `resume_type="event"` 唤醒。

```python
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.event import EventNode, InMemoryEventBus
from plaita.node import End, Start

bus = InMemoryEventBus()
flow = Flow(
    flow_id="approval-flow",
    nodes=[
        Start(id="start", next="wait"),
        EventNode(id="wait", event_type="document.approved", next="done",
                  subscription_timeout=3600),          # 秒；缺省 None = 无限等待
        End(id="done"),
    ],
)

# 分布式模式下复用同一个 FlowExecution 实例（回调才能跨步骤保留）
execution = FlowExecution(event_bus=bus)

# 第 1 步：推进到 EventNode 即挂起（is_suspend=True，节点状态 pending）
step1 = execution.run_distributed(flow)

# 第 2 步：事件到达 —— 唤醒挂起的节点本身（节点状态 completed）
step2 = execution.run_distributed(
    flow,
    saved_context=step1["context"],
    resume_type="event",
    resume_data={"approved": True},
)

# 第 3 步：从挂起节点之后继续推进
step3 = execution.run_distributed(flow, saved_context=step2["context"],
                                  resume_type="continue")
```

`EventNode` 的字段（均为可选 camelCase 别名 `eventType` / `eventFilter` /
`subscriptionTimeout`）：

| 字段 | 语义 |
|------|------|
| `event_type` | **必填**。订阅的事件类型，须与事件发布方的 type 精确匹配；支持 `$` 表达式 |
| `event_filter` | 事件过滤器：按点路径匹配事件负载，如 `{"data.status": "ok"}`；缺省 `{}` |
| `subscription_timeout` | 订阅超时秒数（须 `> 0`）。超时由 `SubscriptionTimeoutChecker` 触发 `resume_type="timeout"`，节点落 `timeout` 状态；缺省 `None` = 无限等待 |

`resume_type` 的取值：

| `resume_type` | 行为 |
|---------------|------|
| `event` | 事件到达：`node.on_event(execution, resume_data)` |
| `timeout` | 等待超时：`node.on_timeout(execution)` |
| `cancel` | 取消等待：`node.on_cancel(execution)` |
| `continue` | 不唤醒，从上次 `LAST_NODE` 之后继续推进下一节点 |

`event` / `timeout` / `cancel` **只恢复挂起节点本身**（该次调用的返回即该节点状态）；
后续节点要用 `resume_type="continue"` 逐次推进，直到 `is_end`。

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

混合 = 选一个 `EventBus` 实现（决定分发路径与各组件默认值），再把不需要的组件
替换掉。可替换的三个普通属性：`event_storage`、`subscription_storage`、
`processing_tracker`。

按场景选择：

1. **高并发事件处理 + 持久化存储**：`RedisEventBus` + 把 `event_storage` 与
   `processing_tracker` 换成 SQLAlchemy 实现（完整写法见 §6.4）。
2. **单服务部署 + 集中存储**：`InMemoryEventBus` 处理本地事件 + 把 `event_storage`
   换成 `RedisEventStorage`，让其他服务能查到事件。
3. **性能优先 + 分析需求**：事件与订阅都留在 Redis，只把 `processing_tracker`
   换成 `SqlalchemyEventProcessingTracker`，便于离线分析处理记录：

   ```python
   from sqlalchemy.ext.asyncio import create_async_engine

   from plaita.event import RedisEventBus, SqlalchemyEventProcessingTracker

   engine = create_async_engine("sqlite+aiosqlite:///events.db")

   redis_bus = RedisEventBus(redis_url="redis://localhost:6379/0")
   await redis_bus.initialize()

   # 事件与订阅留在 Redis，仅处理记录落库
   redis_bus.processing_tracker = SqlalchemyEventProcessingTracker(engine)
   ```

> **`MixedEventBus` 不存在（2026-10-14 更正）**：历史文档里的
> `MixedEventBus(bus_type=..., storage_type=..., subscription_type=...,
> tracker_type=...)` 类**未实现**（见 §6.4.1）。上面的手工组合就是混合后端唯一
> 的官方做法。§7.5 的 `--backend mixed --bus-type ... --tracker-type ...` 是
> 真实的（`examples/event_demo/demo_eventbus.py` 的 CLI 参数），它只是把同样的
> 组合逻辑包了一层。

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
1. 先建好一个 `EventBus`，并把 `event_storage` / `subscription_storage` /
   `processing_tracker` 三个属性**全部配成原后端**（等价于单后端）
2. 逐步把单个属性替换成新后端（如先换 `event_storage`，见 §6.4）
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