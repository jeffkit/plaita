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
- **`close()` 不得从自己分发的任务图内部调用**（F2 + r3/F4 + r3/F1 验收者补的形状）：
  直接形状（handler 内 `await bus.close()`）与**嵌套形状**——handler
  `await asyncio.create_task(nested())` / `gather` / `asyncio.shield(bus.close())` /
  spawned-not-awaited，只要 `nested` 调用 `close()`——都会让 drain 等待自己的祖先
  （task-wait cycle）：`asyncio.wait` 超时后 `Task.cancel` 沿 `_fut_waiter` 无界
  递归 ⇒ 进程级 `RecursionError`，且 `close()` 返回后 `_pending_tasks` 仍非空、
  任务永久悬置（r2 只挡直接形状；验收者 V2 形状实测复现）。判据由**两层**组成：
  1. **marker**：分发任务（`_process_event` / `_process_with_retry`）置
     `contextvars.ContextVar("plaita_redis_in_dispatch_bus")`（值是总线实例）；
     `create_task` / `ensure_future`（含 `asyncio.shield`）复制 context，因此
     **在未显式换 context 的前提下**，await / create_task / shield 出的子任务
     无论嵌套多少层都继承该标记。
  2. **完成唤醒边遍历**（r5 结构性口径，取代 r3 的 `_waits_for()` 写法枚举）：
     `close()` 先取**当前任务**（正在执行 close 的 close-task），只沿
     **事件循环真的会用来唤醒等待者的引用**做可达性遍历——
     `Future._callbacks` 的每个表项 → 回调引用的对象 → 其中牵动的 Future/Task：
     * 绑定方法 `__self__`（`Task.__wakeup` 的 `__self__` 就是被唤醒的任务；
       `TaskGroup._on_task_done` 的 `__self__` 是 group，group 持有
       `_on_completed_fut`——`__aexit__` 真正 await 的 future）；
     * `functools.partial` 的 `func` / `args` / `keywords` **递归**（含嵌套 partial；
       `partial(waiter.set_result)` 的 future 在 `func.__self__`）；
     * 可执行函数的 `__closure__`（回调里 `ensure_future(notify())` 时真正唤醒
       等待者的是 `notify` 的闭包状态）；
     * 容器递归（`Condition._waiters` 的表项、`asyncio.wait` 的 `todo` 集合）；
     * 只对**asyncio 自身的唤醒队列属性**下钻：`Queue._getters` / `Queue._putters`
       / `Event._waiters` / `Condition._waiters` / `Semaphore._waiters` /
       `Lock._waiters` / `TaskGroup._on_completed_fut` / `TaskGroup._tasks`
       （`asyncio.as_completed` 的完成边就是 `_on_completion` 闭包 → `done` Queue →
       `_getters` 里的 getter future → 消费任务）。
     **不扫 `__dict__`**：handler 闭包里的总线对象会经 `_track_task` 的完成回调把
     全部在飞任务拖进来，会把合法外部关停误判成图内关停。
     **闭包 cell 里的容器不展开**：`asyncio.wait` / `as_completed` 的 `todo` 集合同时
     装着 close 任务与全部被等待者，它是兄弟簿记而不是唤醒边；真唤醒目标是同一闭包
     里的 `waiter` future 或 `Queue`/`Event`/`Condition` 原语（原语的等待队列仍展开）。
     否则 700 个兄弟会把唤醒锥推过预算、把合法关停误拒
     （`test_sibling_set_in_wait_does_not_inflate_cone`）。
     方向与完备性口径（下一位评审可据此查漏）：**「close 任务完成后会被唤醒的人」
     里若含本实例登记的在飞分发任务，就必然构成 task-wait cycle**——因为该分发
     任务能恢复，前提正是 close 任务先完成。遍历不依赖 contextvars。
     **代价**只与当前任务自己的唤醒锥有关，与在飞分发条数无关（合法外部关停
     实测访问 O(1) 节点：600 个在飞分发 + 外部 close ⇒ `visited=1`）。
     **预算 fail-closed**：唤醒锥在 `_DISPATCH_GRAPH_MAX_NODES = 512` 个节点的
     预算内**未闭合**时，`close()` 抛第二条具名 `RuntimeError`
     （`_SELF_CLOSE_BUDGET_REFUSAL`，文案含预算与已访问节点数），
     **绝不把「证明不出来」当成「允许」**（r4 的「节点上限用满即当作未命中」正是
     该缺陷家族第四次复发：pads=1500 时 12 个危险形状 12 个漏检）。
  `close()` 命中 marker / 遍历口径任一即抛 `_SELF_CLOSE_REFUSAL`（具名
  `RuntimeError`，含可执行出口：在 handler 外关停），命中预算则抛
  `_SELF_CLOSE_BUDGET_REFUSAL`；**两条都发生在任何关停副作用之前**
  （`_closing` 保持 `False`、listener 不取消、不进 `_drain_pending_tasks`）。
  被判据拒绝的 `close()` 也**不再让调用方 teardown 悬置**：X1 的
  `asyncio.TaskGroup` + `context=Context()` 形状在 r4 会让 `asyncio.run` 的
  `_cancel_all_tasks` 无法收尾（外部看门狗 SIGKILL/137），r5 拒绝后
  `asyncio.run` 正常返回（探针 `TEARDOWN_OK`）。
  `asyncio.shield(bus.close())` 因此从 r2 的「bypass-but-named（drain 超时→取消
  在飞分发）」收紧为**具名拒绝**（测
  `Test109NestedSelfCloseGuard::test_shield_close_is_named_refusal`）：该形状的
  副作用计数从「1 条超时 WARNING + 1 个 handler 被取消」变为「0 关停副作用」。
  **日志口径修正**（r3 验收者第 3 处过度声明）：该拒绝只有在**未被 handler 接住**
  时才经 `_process_event` 打出 1 条 handler 失败日志；作者自己的 shield 测试在
  handler 内接住，因此**该日志为 0 条**——两种口径都由
  `Test109R3ResidualHoles::test_shield_refusal_log_scope_declared` 钉住。
- **跨事件循环/线程的关停也具名拒绝**（r3/F2）：`threading.Thread` /
  `run_in_executor` 里 `asyncio.run(bus.close())` 不继承 marker，但当本实例仍有
  **属于另一个事件循环的在飞分发任务**时，`close()` 在副作用前抛具名
  `RuntimeError`（含「另一个事件循环」，出口是 `loop.call_soon_threadsafe`）。
  计数：主循环还有 1 个在飞分发 ⇒ 线程侧 1 条具名拒绝、`_closing` 保持 `False`、
  `_pending_tasks` 仍是 1（`test_cross_loop_close_with_inflight_is_named_refusal`）。
  **无在飞分发时不拦**（没有分发可丢；跨循环 aclose 由 redis-py 自身报错）。
- `EventFilter` CLI 收尾的 `close()` **按能力**调用（`plaita/server/event_filter.py`
  的 `_close_event_bus`）：`RedisEventBus` 有 `close()`（取消监听 / drain 在飞分发 /
  释放连接）；`InMemoryEventBus` / `SqlalchemyEventBus` **没有** `close()`
  （`EventBus` 基类也未声明）——r2 的无条件 `await event_bus.close()` 对
  `--event-bus-type=memory`（合法 CLI 选项）每次关停都抛 `AttributeError`，被记成
  **假** WARNING「事件总线关停失败: 'InMemoryEventBus' object has no attribute
  'close'」。现只在总线**声明**了 `close` 时调用，memory/sqlalchemy 跳过。
  **声明**：memory 总线的 `_pending_tasks` 收尾排空**不在**本契约内（其
  `_track_task` 只持引用，无 drain、无点名）——生产 #109 关注的是 Redis 后端；
  改造 memory 为 drain+点名是独立工作项，不在 r3 范围。
- `EventFilter` CLI（`plaita/server/event_filter.py:main_async`）在 `finally` 中
  调用上述收尾；调用方若直接 `asyncio.run` 后不调用 `close()`，在飞分发仍会被
  loop 取消——**契约要求调用方 close**（Redis 后端）。

覆盖边界（**T1/T2 口径**，不宣称「任何情况都不丢」）：

- **T1（本契约覆盖）**：同一事件循环里、且调用任务不位于本实例任何在飞分发任务
  等待图下游的 `close()`——先取消监听，再等已登记的在飞分发最多
  `DISPATCH_DRAIN_TIMEOUT_SECONDS = 5.0` 秒；超时逐个 `cancel()` 并打含
  `event_id` / `handler_id` 的 WARNING；返回后不存在「inflight 且无记录」的第三态。
  从分发任务图内部（marker 或完成唤醒边遍历任一命中）的 `close()` 一律具名拒绝，且
  拒绝在任何关停副作用之前。
- **T2（声明不在契约内 / 明知的保守行为）**：
  1. **显式换 context**（`create_task(coro, context=contextvars.Context())`）绕过
     marker，但由完成唤醒边遍历覆盖（r5 实测覆盖：`TaskGroup.create_task(...,
     context=Context())`、`asyncio.as_completed`、`partial(waiter.set_result)`、
     `Event._waiters` / `Queue._getters` / `Condition._waiters`）；遍历只认
     **事件循环自己的唤醒机制**（`_callbacks` + asyncio 的唤醒队列属性），不认
     「作者用其它同步原语自行耦合同一循环」的形状。
  2. **唤醒锥预算**（r5 fail-closed 的声明边界）：`close()` 的唤醒锥在 512 节点内
     **枚举不完**时按「无法证明安全」具名拒绝（`_SELF_CLOSE_BUDGET_REFUSAL`，报出
     预算）。合法外部关停只访问 O(1) 节点（600 在飞实测 `visited=1`），因此这条
     边界不会命中合法形状；只有「故意把完成回调链拉到 512 层以上」的病态/对抗形状
     才会撞上——撞上也是**拒绝**（fail-closed），不是放行。
  3. **线程 / executor**：marker 不跨线程；**有在飞分发**时由跨循环围栏具名拒绝，
     **无在飞分发时不拦**。
  4. **两处保守过度拒绝**（r3 验收者 F3：无 task-wait cycle 也被拒；今天无生产
     调用方）：(a) `call_soon` 分离的 `close()`（handler 已返回、`pending == 0`）；
     (b) 分发结束后 detached 任务才调用的 `close()`（marker 随 context 活到该任务
     结束）。二者由 `test_call_soon_detached_close_after_dispatch_is_refused_declared`
     与 `test_detached_task_after_dispatch_still_refused_declared` 钉住；修掉它们会
     削弱已确认的 spawned-not-awaited 拒绝。
  5. **SIGTERM**：`event_filter.py` 不注册信号处理器，进程直接结束，本条不含信号
     处理；`asyncio.run` 后未调用 `close()` 的路径不在契约内。
- **副作用口径**：契约是「要么完成、要么被点名」，不是「handler 副作用必为 0」——
  被取消/被拒的分发里，handler 自己 `create_task` 的子任务、或用 `asyncio.shield`
  包住的**其它** work 片段，可能在 `close()` 返回后照常产生副作用；守卫拒绝的嵌套
  形状里，`nested` 子任务也已执行到 `close()` 调用点（此前可能有副作用）才被拒。
  本条只保证**不静默**。
- GC 压力下任务不被回收（实测）；常规运行（不关停）分发照常跑完。

## Redis 事件存储 `list_events`（#110 相邻缺陷，r3）

验收者补的相邻缺陷，都在**生产可达**路径上（生产调用方
`plaita/server/event_reconcile.py:EventReconciler.scan_once` 调
`list_events(start_time=…, end_time=…, limit=…)`，**不带** `event_type` ⇒ 必走全量分支）：

- **event_type 截断**：全量分支此前 `type_key.split(':')[-1]`——`order:created` →
  `created`，重建的 `{prefix}types:created` 落空，该类型事件**静默丢失**。现按
  已知前缀 `{prefix}types:` 剥离（fakeredis 实测：修前全量只回 `['plain']`，
  修前 `list_events('order:created')` 显式分支在解码连接下正常）。
- **bytes event_id（更严重的一半）**：`initialize()` 用 `aioredis.from_url(url)`
  未带 `decode_responses`（redis-py 默认 `False`）⇒ `zrangebyscore` 返回 **bytes**
  member，键被拼成 `{prefix}events:b'<id>'`，`mget` 永远落空。**真实 Redis
  （redis:7-alpine）实测**：生产配置下显式类型分支与全量分支**都返回 `[]`
  （`list_events` 整体失效）；注入 `decode_responses=True` 客户端后显式分支恢复、
  全量分支仍丢含 `:` 的类型。现把 member 显式归一化为 `str`，两种连接配置都正确。
  经生产调用方实测：修复前 `EventReconciler.scan_once()` 回放 **0** 条，修复后
  **2** 条（`tests/unit/test_issue109_110_r3_parity.py`）。
- `plaita/server/registry.py:get_service_types` 属同一 `split(':')` 家族
  （`parts[2]`）：当前所有 `service_type` 都是无冒号常量（`flow_worker`、
  `*_service`），`instance_id = f"{hostname}-{uuid4()[:8]}"`，故**生产未暴露**；
  r3 一并按「已知前缀 + 右侧去掉 instance_id」收口并加测试，消除该家族的最后一位。

## Redis 订阅查询索引与代价界（#110）

`RedisEventSubscriptionStorage.list_subscriptions` 的索引优先级是
**correlation_id → event_type → flow_id → node_id → 全量 SCAN**。给出
`correlation_id` 时走 `{prefix}correlation:{id}` 索引：

- 代价（Redis 往返 + `model_validate_json`）与**该 correlation 的订阅数**成正比，
  不再与该 `event_type` 的全部订阅数成正比（100 条同类型只 1 次 `data:` GET，
  实测见 `test_correlation_index_cost_is_bounded_by_matches`）。
- **合取语义（#110 r5 决定，取代 r3 的「与 correlation 单条件同集合」声明）**：
  索引只用来**选候选集**，调用方给出的**每一个**过滤条件都必须真正生效（AND）。
  判据：`list_subscriptions` 是列表/内省查询，memory 与 sqlalchemy 都按 AND 过滤，
  且本文件一直声称三后端同一集合；r3 及以前 redis 的 correlation 分支只按索引取行、
  **不再校验 `event_type`** ⇒ `list_subscriptions(event_type='shared.type',
  correlation_id='CID')` 会把同 correlation 但 **event_type 不同**的行也返回。
  **真实 Redis（redis:7-alpine，生产配置 `decode_responses=False`）实测**：修复前
  2 行（`sub-hit` + `sub-wrong-type`），修复后 1 行；memory/sqlalchemy 一直是 1 行。
  选择「让三后端一致（AND）」而不是把错答写成声明边界：要 `shared.type` 的订阅却
  静默拿到 `other.type` 是**错答**，而列表查询的合取口径是三后端与调用方直觉的
  共识。索引选择不变（correlation 优先，`data:` GET 仍只与该 correlation 的候选数
  成正比、不回落 type 索引、不全量 SCAN）；只是对已取回的候选行做其余条件过滤，
  不产生额外 Redis 往返。钉住：`tests/unit/test_issue109_110_r5_parity.py`
  与 `scripts/selfclose_109_110.py` 的 R5 格（三后端 `returned == 1` 且排除
  `wrongtype` 行、redis `data_GET == 2`）。
- **`event_type` 必须非空**（F1，r3 对称化）：`EventNode.event_type` 给具名中文原因 +
  `min_length=1`；`EventSubscription.event_type` 由**本类自己的**具名 validator
  （`mode="before"`）承担（拒绝 `""` 与纯空白）——r2 只有 `min_length=1`（pydantic
  默认英文串）。两个 validator **共享前缀** `event_type 不能为空：`、**正文不同**
  （`订阅必须写明要等待的事件类型` vs `event 节点必须写明要等待的事件类型`），
  旧文案「同一条具名中文原因」是过度声明，r3 已按此改写并加测
  （`test_prefix_shared_but_body_differs_from_event_node`）。构造期即
  `ValidationError`（含「event_type 不能为空…如需通配请显式写 `"*"`」），作者/编译器
  入口（`EventNode`）与存储写入口（`store_subscription`）都拒。
- **`model_construct` 绕过校验的分叉——按实测计数声明**（不宣称 uniform，也不宣称
  「memory 一律静默排除」——r3 验收者第 4 处过度声明）：`EventSubscription.model_construct(event_type="")`
  可绕过 pydantic 直接进后端；三后端行为**不同**，且**同一后端不同 list 条件也不同**
  （`Test110ModelConstructDivergenceDeclared` 钉住）：
  - memory：`list_subscriptions(event_type=…)` **静默排除**（0 命中、0 日志）；但
    `list_subscriptions(correlation_id="CID")` 与 `list_subscriptions()` 全量 SCAN
    **静默返回该行**（1 命中、0 日志）——旧文案只写了第一种条件，属过度声明。
  - Redis：`list_subscriptions(correlation_id=…)` 命中该行时 `get_subscription`
    反序列化抛 **`EventError`（loud、无名）**；`list_subscriptions(event_type=…)`
    走 type 索引落空、**静默空**（0 命中、0 日志）。
  - sqlalchemy：构造被 `ValidationError` 拦下后 **跳过并具名点名**（0 命中）。
  正常构造入口已拒空类型，此分叉只在显式 `model_construct` 后可达。
- **sqlalchemy 历史脏行**（F1 前的存量数据）：r2 之前 `event_type` 允许空，库里可能
  躺着空类型行。`list_subscriptions()` **与** `list_subscriptions(correlation_id=…)`
  此前会在构造 `EventSubscription` 时抛 `ValidationError` 并**丢弃整条查询**
  （同一批里的合法订阅一起丢）。现 `except ValidationError` **跳过坏行**并打具名
  WARNING（`id` / `event_type`），其余订阅照常返回（`Test110SqlalchemyDirtyRowSkipped`）。
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

