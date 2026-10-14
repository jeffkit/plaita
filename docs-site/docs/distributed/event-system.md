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

## Redis 分发任务生命周期（#109）

`RedisEventBus._process_message` 对每条入站消息建一个 fire-and-forget 分发任务，
并用 `_pending_tasks`（`{task: (event_id, handler_id)}`）持强引用——与
`InMemoryEventBus._track_task` 同一机制。关停契约（实测见
`tests/unit/test_issue109_110_event_redis_parity.py` 与
`scripts/selfclose_109_110.py`）：

- `close()` 先取消监听任务，再等**已登记**的在飞分发任务，最多
  `DISPATCH_DRAIN_TIMEOUT_SECONDS = 5.0` 秒；仍未完成的逐个 `cancel()`，并对每个
  被取消的任务打一条 `WARNING`，含 `event_id` 与 `handler_id`。
- `close()` 返回后不存在「inflight 且无处理记录」的第三态：每个分发要么
  handler 完成并 `mark_event_processed`，要么被取消且已点名。
- `close()` 开始后到达的分发被**拒绝并点名**（`_closing` 围栏），不会绕过 drain
  静默创建。
- **取消窗口也点名**（F2）：`close()` 先取消监听任务，而监听器是内联 `await`
  `_process_message` 的；若在首个 await（`is_event_processed` 的 Redis 往返）处被
  取消，`CancelledError`（BaseException）会被显式捕获、打出含 `event_id` /
  `handler_id` 的 WARNING 后再抛出——不允许「既不完成、也不点名」。
- **`close()` 不得从自己的分发 handler 内调用**（F2/验收者补的形状）：那会把自身
  纳入 drain 并自取消，最终 `RecursionError`。命中时抛具名 `RuntimeError`（含可
  执行出口：在 handler 外关停）。
- `EventFilter` CLI（`plaita/server/event_filter.py:main_async`）在 `finally` 中
  `await event_bus.close()`；调用方若直接 `asyncio.run` 后不调用 `close()`，
  在飞分发仍会被 loop 取消——**契约要求调用方 close**。

覆盖边界（不宣称「任何情况都不丢」）：GC 压力下任务不被回收（实测）；常规运行
（不关停）分发照常跑完；SIGTERM 无信号处理时进程直接结束（`event_filter.py`
不注册信号处理器），本条不含信号处理；`asyncio.run` 后未调用 `close()` 的路径
不在契约内。**副作用口径**：契约是「要么完成、要么被点名」，不是「handler 副作用
必为 0」——被取消/被拒的分发里，handler 自己 `create_task` 的子任务或用
`asyncio.shield` 包住的片段可能在 `close()` 返回后照常产生副作用；本条只保证
**不静默**。

## Redis 订阅查询索引与代价界（#110）

`RedisEventSubscriptionStorage.list_subscriptions` 的索引优先级是
**correlation_id → event_type → flow_id → node_id → 全量 SCAN**。给出
`correlation_id` 时走 `{prefix}correlation:{id}` 索引：

- 代价（Redis 往返 + `model_validate_json`）与**该 correlation 的订阅数**成正比，
  不再与该 `event_type` 的全部订阅数成正比（100 条同类型只 1 次 `data:` GET，
  实测见 `test_correlation_index_cost_is_bounded_by_matches`）。
- 同条件的 `list_subscriptions(event_type=…, correlation_id=…)` 与
  `list_subscriptions(correlation_id=…)` 返回同一集合。
- **`event_type` 必须非空**（F1）：`EventNode.event_type` 与
  `EventSubscription.event_type` 都 `min_length=1`，`EventNode` 另给具名中文原因
  （空类型会被 `matches_event` 的 falsy 语义当通配，而 memory/sqlalchemy 的 list
  过滤是严格 `!=`，三后端会对「空类型订阅」分叉；如需通配请显式写 `"*"`）。
  构造期即 `ValidationError`，作者/编译器与存储写入口都拒。
- 三后端对齐**只在生产路径** `EventSubscriptionStorage.find_matching_subscriptions`
  成立（`scripts/selfclose_109_110.py` 的 parity 格：`['hit']`）。原始
  `list_subscriptions` 层有一个**已写明**的差异：给了 `correlation_id` 时 Redis 返回
  **该 correlation 的全部订阅**（含别的 event_type；list 层不预过滤 event_type，
  实测 `returned=2`），memory / sqlalchemy 在 list 层同时过滤两个条件（实测
  `returned=1`）；两者经 `matches_event` 后一致。
- `correlation_id` 为空时既有单条件路径行为不变（type/flow/node 索引、全量 SCAN）；
  全量 SCAN 路径按**已知前缀** `{prefix}data:` 剥离 id（不再 `split(':')[-1]`）——
  后者在 `subscription_id` 含 `:` 时取到截断值、GET 落空而**静默返回空**。


## 下一步

- [Checkpoint 概念](checkpoint.md)
- [API: plaita.event.core](../api/event.md)

