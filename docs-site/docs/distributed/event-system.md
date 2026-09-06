# 事件系统

事件系统是断点续执的基础：它把「挂起的流程」与「外部触发源」解耦。`EventBus` 负责发布与订阅，`Event` / `EventSubscription` 是数据模型。**生产路径：memory（单测）+ redis（部署）**；sqlalchemy 为 experimental（需 `PLAITA_ALLOW_EXPERIMENTAL_DB=1`）。控制面（Worker 队列 / Registry / EventFilter）硬绑 Redis，换 EventBus 后端不等于换部署拓扑。

## 核心抽象

| 抽象 | 模块 | 职责 |
|------|------|------|
| `Event` | `plaita.event.core` | 事件对象：`event_type` / `data` / `correlation_id` / `source` / `timestamp` |
| `EventSubscription` | `plaita.event.core` | 订阅信息：`event_type` / `filter_condition` / `correlation_id` / `flow_id` / `node_id` |
| `EventBus` | `plaita.event.core` | 发布/订阅/等待/注册处理器接口 |
| `EventStorage` | `plaita.event.core` | 事件持久化接口 |
| `EventSubscriptionStorage` | `plaita.event.core` | 订阅持久化接口 |
| `EventProcessingTracker` | `plaita.event.core` | 全局去重与处理记录 |

## Event

```python
Event(
    event_type="approval_decision",
    data={"approved": True, "request_id": "r-123"},
    correlation_id="execution-id-xxx",
    source="approval_service",
)
```

`event_id` 与 `timestamp` 自动生成；`correlation_id` 通常用 `execution_id`，用于把事件路由回正确的挂起流程。

## EventSubscription 与匹配 {#eventsubscription-与匹配}

订阅记录「我关心什么样的事件」。`EventSubscription.matches_event` 按以下顺序匹配：

1. **event_type**：**全等**（`!=` 即不匹配）。订阅路径**不**支持 fnmatch 通配
2. **correlation_id**：若订阅指定了，必须与事件的相等
3. **flow_id / node_id**：从上下文取 `$FLOW_ID` / `$LAST_NODE` 比对
4. **filter_condition**：对 `event.data` 做**浅层键值全等**（注释仍标「暂时简化」；非表达式求值）

Handler 注册（`register_handler`）另走 `EventBus.matches_event_type`，**那里**才用 fnmatch（`user.*` 等）。不要把 handler 通配语义套到挂起订阅上。

## EventBus 接口

| 方法 | 用途 |
|------|------|
| `publish(event, prevent_duplicate_consumption=True)` | 发布事件，返回 event_id |
| `register_subscription(event_type, filter_condition, correlation_id, flow_id, node_id, timeout)` | 注册订阅，返回 subscription_id |
| `unregister_subscription(subscription_id)` | 取消订阅 |
| `wait_for_event(event_type, timeout, condition)` | 同步等待某事件 |
| `register_handler(event_type, handler, filter_condition, retry_policy)` | 注册处理器（带重试策略） |
| `get_event(event_id)` | 取事件 |
| `publish_sync(event, ...)` | 同步发布（桥接异步 `publish`） |

`publish` 支持 `prevent_duplicate_consumption`：在 **handler 成功之后** 才 `mark_event_processed`（失败可重试）。这是 handler 路径的去重，**不是** FlowWorker 任务队列的至少一次投递保证。

## 后端实现

| 后端 | 模块 | 需要 extra | 适用场景 |
|------|------|-----------|---------|
| memory | `plaita.event.memory` | — | 单进程、开发调试 |
| redis | `plaita.event.redis` | `redis` | **生产推荐** |
| sqlalchemy | `plaita.event.sqlalchemy` | `server` | **experimental**；factory 需 env 门闩 |

```python
from plaita.event.memory import InMemoryEventBus
# from plaita.event.redis import RedisEventBus  # 需 redis extra

bus = InMemoryEventBus()
execution = FlowExecution(event_bus=bus)
```

未显式注入时，`ExecutionContext.get_or_create_event_bus()` 会在函数体内 lazy import `plaita.event.get_default_event_bus` 作为 fallback（见 [分层约束](../architecture/layering.md#默认-eventbus函数体内-lazy-import非全局-provider) 与 [状态管理](../architecture/state-management.md#event-bus-获取)）。

### 分布式挂起 / EventFilter 接线（重要）

- **订阅失败禁止挂起**：`DistributedStrategy` 在 `register_subscription` 失败（无 bus / 异常）时抛 `FlowExecutionException`，**不会**返回 `is_suspend=True`，避免僵尸执行。
- **FlowWorker 默认启用 EventBus**：`--use-event-bus` 已废弃（兼容保留）；仅 `--no-event-bus` 可显式关闭。
- **EventFilter 优先复用 bus 的 `subscription_storage`**：与 worker 写入同一实例/同 Redis keyspace，避免「写内存、读 Redis」导致事件永不 resume。

## 重试策略

`RetryPolicy` 控制处理器失败时的重试：

```python
RetryPolicy(max_retries=3, initial_delay=1.0, backoff_factor=2.0, max_delay=60.0)
```

指数退避：`delay = min(initial_delay * backoff_factor^n, max_delay)`。

## 装饰器注册处理器

```python
from plaita.event.core import event_handler

@event_handler(bus, event_type="user.*")
def on_user_event(event):
    print(event.event_type, event.data)
```

## 内存总线语义（必读）

`InMemoryEventBus` 为零依赖演示与本地开发而生，与 Redis/SQLAlchemy 后端有三点关键差异：

1. **`wait_for_event` 只能看到注册之后的事件。** 它靠"先注册 future、后 publish 唤醒"实现，不会扫描历史事件回放——哪怕事件还在 `event_storage` 里。所以顺序永远是：先起 `wait_for_event`（或 `register_handler`），再 `publish`。
2. **`register_subscription` 只记录、不分发。** 它仅把订阅写入订阅存储；总线的分发只认 `register_handler`。订阅记录供 `EventFilter` 之类的外部组件按 `correlation_id` 检索消费（断点续执正是这样用它的，见下节）。
3. **`publish` 后立即关闭事件循环，不保证 handler 执行。** handler 在独立的 asyncio task 中异步分发（fire-and-forget），`publish` 返回只代表事件已入库、分发任务已创建。演示脚本在 `publish` 之后要 `await asyncio.sleep(...)` 给 handler 留出执行时间，最后再关循环。

## 与断点续执的协作

`DistributedStrategy._subscribe_event` 在 `EventNode` 挂起时调用 `register_subscription`：

```python
subscription_params = {
    "event_type": resolved_event_type,
    "filter_condition": node.event_filter,
    "correlation_id": context.execution_id,   # 关键：用 execution_id 路由
    "flow_id": flow.flow_id,
    "node_id": node.id,
}
subscription_id = await event_bus.register_subscription(**subscription_params)
```

外延服务监听到外部触发（如审批通过）后 `publish` 一个 `correlation_id=execution_id` 的事件，`FlowWorker` 据此找到挂起流程并恢复（见 [FlowWorker](flow-worker.md)）。

## 下一步

- [Checkpoint 概念](checkpoint.md)
- [API: plaita.event.core](../api/event.md)
