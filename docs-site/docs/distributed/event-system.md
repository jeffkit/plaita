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
- **`close()` 不得从自己分发的任务图内部调用**（F2 + r3/F4 + r3/F1 + r5/X1–X4 + r6 决定性反例）：
  直接形状（handler 内 `await bus.close()`）与**嵌套/间接形状**——handler
  `await asyncio.create_task(nested())` / `gather` / `asyncio.shield(bus.close())` /
  spawned-not-awaited / `TaskGroup.create_task(..., context=Context())` +
  `await asyncio.sleep(0)`——都会让 drain 等待自己的祖先（task-wait cycle）：
  `asyncio.wait` 超时后 `Task.cancel` 沿 `_fut_waiter` 无界递归 ⇒ 进程级
  `RecursionError` + 任务永久悬置。**r6 换架构**：判据内核拆到
  `plaita/event/_selfclose_guard.py`（只依赖 stdlib，可在别的 CPython 上按文件加载
  驱动）。不再往「私有属性名白名单」里加形状——那是 **fail-open by construction**
  （属性缺失/改名即静默跳过；验收者的属性缺失表：present ⇒ in_graph，absent ⇒ clean）
  ——改成两层 + 一个运行时自检：
  1. **L1 dispatch lineage**（主判据，廉价，不点名任何私有属性）：分发 handler 运行时置
     `contextvars.ContextVar("plaita_redis_in_dispatch_bus")`（值是总线实例）；
     `create_task` / `TaskGroup.create_task` / `ensure_future`（含 `shield`）复制当前
     context ⇒ handler 里创建的任务、以及这些任务再创建的任务**传递地**继承该标记。
     `close()` 在任何副作用之前具名拒绝：`current_task()` 就是本实例登记的在飞分发
     任务，或当前 context 的标记就是本总线。**声明作用域**：显式换 context
     （`context=Context()`）或换线程/executor 不继承，走 L2 / 跨循环围栏。
  2. **L2 完成唤醒锥（fail-closed）**（defence in depth）：从正在执行 `close()` 的
     任务出发，沿「事件循环真的会用来唤醒等待者」的引用做可达性遍历——
     `Future._callbacks` 的每个表项 → 回调牵动的 Future/Task：绑定方法 `__self__`
     （`Task.__wakeup` / `TaskGroup._on_task_done`）、`functools.partial` 的
     `func`/`args`/`keywords` 递归、函数 `__closure__` 的 cell、以及**已知 asyncio
     唤醒簿记类型**的期望属性（`Task`/`Future._callbacks`、`Queue._getters`·`_putters`、
     `Event`·`Condition`·`Semaphore`·`Lock._waiters`、`Barrier._cond._waiters`、
     `StreamReader._waiter`（单数）、`TaskGroup._tasks`·`_on_completed_fut`）。
     **不再有 `getattr(..., None)` 静默跳过**：已知属性读不到 ⇒ 具名拒绝
     （`_WAKE_GRAPH_INCOMPLETE_REFUSAL`，文案含 `error_kind=wake-graph-incomplete`）；
     `TaskGroup._on_completed_fut is None` 而 `_tasks` 非空 ⇒ 拒绝（验收者决定性反例：
     `__aexit__` **懒创建**该 future，r5 在此刻读不到「group 被谁 await」的边 ⇒ clean
     ⇒ close 进 drain ⇒ 与在飞分发任务互等、70 s 后被 SIGKILL）；未知持有者**直接**
     挂着已武装的唤醒原语（未完成 Future/Task、有 getter 的 Queue）⇒ 拒绝（3.13
     `as_completed` 的 `_AsCompletedIterator._done`、自定义 awaitable 的 `self._f`）；
     展开深度 / 节点预算耗尽 ⇒ 拒绝。闭包 cell 里的**容器**不展开（`asyncio.wait` /
     `as_completed` 的 `todo` 是兄弟簿记，展开会误拒 600+ 在飞的合法关停——声明残差 R2）。
     **不扫 `__dict__`**，但对未知持有者只扫**一层**直接属性找已武装原语：因此
     `RedisEventBus` 自己经 `_track_task` 回调闭包出现时（`_pending_tasks`/`listeners`
     是容器、`lock` 未武装）不会误拒跨总线关停。
  3. **stdlib 属性自检**（把「未来 Python 改名」从 fail-open 变 fail-closed）：首次
     `close()` 在 running loop 内对 `Task`/`Future`/`TaskGroup`/`Queue`/`Event`/
     `Condition`/`Semaphore`/`Lock`/`Barrier`/`StreamReader` **新建实例**并检查期望
     属性；缺任一 ⇒ 本进程内 `close()` 一律具名拒绝
     （`_SELF_CLOSE_UNSUPPORTED_RUNTIME_REFUSAL`，文案点名缺哪个属性）。
  `close()` 命中 L1 或在飞分发任务即抛 `_SELF_CLOSE_REFUSAL`；命中 fail-closed 情形抛
  `_WAKE_GRAPH_INCOMPLETE_REFUSAL`；命中预算抛 `_SELF_CLOSE_BUDGET_REFUSAL`；自检未过抛
  `_SELF_CLOSE_UNSUPPORTED_RUNTIME_REFUSAL`。**四条都在任何关停副作用之前**
  （`_closing` 保持 `False`、listener 不取消、不进 `_drain_pending_tasks`）。拒绝后
  `asyncio.run` 正常返回（`Test109R6AsyncioRunTeardown`）。**r5 的对照实测**：同一
  反例在 r5 上 guard 判 `('clean', 1)` 且进入 drain（`_closing=True`、listener 已取消）；
  验收者在完整总线上实测 70 s 仍被外部看门狗 SIGKILL/137，本仓 stub 复现里它被 5 s
  drain 超时取消——两种结局都证明 r5 **不拒绝**。
  `asyncio.shield(bus.close())` 自 r5 起从
  「bypass-but-named」收紧为**具名拒绝**。**日志口径修正**（r3 验收者第 3 处过度声明）：
  该拒绝只有在**未被 handler 接住**时才经 `_process_event` 打 1 条 handler 失败日志；
  handler 内接住时为 0 条——两种口径都由
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

覆盖边界（**T1/T2 口径**，不宣称「任何情况都不丢」，也不宣称围栏完备）：

- **T1（本契约覆盖）**：同一事件循环里、且 `close()` 的调用任务不在本实例在飞分发
  任务图内的关停——先取消监听，再等已登记的在飞分发最多
  `DISPATCH_DRAIN_TIMEOUT_SECONDS = 5.0` 秒；超时逐个 `cancel()` 并打含
  `event_id` / `handler_id` 的 WARNING；返回后不存在「inflight 且无记录」的第三态。
  L1（marker / 在飞分发任务身份）**或** L2（完成唤醒锥）任一命中的 `close()` 一律
  具名拒绝，且拒绝在任何关停副作用之前；L2 证明不出来（缺属性 / 懒创建 / 未知持有者 /
  预算或深度耗尽）同样具名拒绝（fail-closed）。
- **T2（声明不在契约内 / 明知的保守行为 / 声明残差）**：
  1. **显式换 context**（`create_task(coro, context=contextvars.Context())`）绕过
     L1，由 L2 覆盖。r6 形状表实测：`TaskGroup` + `await asyncio.sleep(0)`（懒创建
     窗口）、两级 `TaskGroup`、`Barrier`、`StreamReader._waiter`、
     `partial(waiter.set_result)`、`as_completed` 都被具名拒绝。
  2. **唤醒锥预算**（fail-closed 的声明边界）：`_DISPATCH_GRAPH_MAX_NODES = 65536`
     个节点内**枚举不完**时按「无法证明安全」具名拒绝。**边界精确且经过测量**：唤醒锥
     65536 节点放行、65537 节点拒绝；对纯 future 链即链长 65535 放行 / 65536 拒绝
     （`Test109R6BudgetBoundary::test_documented_budget_boundary_65536`）。r5 的 512
     预算被验收者证伪——「合法 ≥510 深的 await 链被拒」而 r5 文档却称只有病态/对抗
     形状会撞上；r6 实测合法 600 / 10000 层 await 链**完全枚举、判 clean**
     （`test_legit_600_and_10000_deep_await_chain_not_refused`）。预算只把「无法证明」
     变成具名拒绝，**任何超过 65536 节点的唤醒锥都会被拒**——这是明知的保守边界，
     不是「只有对抗形状才撞上」。把预算分支改回 `clean` 的变异
     `m109-r5-redis-budget-fail-open` 被 `test_over_budget_chain_is_named_budget_refusal`
     杀死。
  3. **线程 / executor**：L1 不跨线程（各自独立 context）；**有在飞分发**时由跨循环
     围栏具名拒绝，**无在飞分发时不拦**。
  4. **两处保守过度拒绝**（r3 验收者 F3：无 task-wait cycle 也被拒；今天无生产
     调用方）：(a) `call_soon` 分离的 `close()`（handler 已返回、`pending == 0`）；
     (b) 分发结束后 detached 任务才调用的 `close()`（marker 随 context 活到该任务
     结束）。二者由 `test_call_soon_detached_close_after_dispatch_is_refused_declared`
     与 `test_detached_task_after_dispatch_still_refused_declared` 钉住；修掉它们会
     削弱已确认的 spawned-not-awaited 拒绝。(b) 同时是 L1 独立检验力的死法
     （`Test109R6LineageMarker::test_delayed_detached_close_refused_by_marker_only`）。
  5. **声明残差 R1（两层机制盖不住的形状）**：`close()` 由一个**在 dispatch 之外
     创建**、却被某个分发 handler 等待的任务发起。L1 对该任务没有标记（它不是任何
     handler context 的后代）；L2 是 **best-effort**——它只认 asyncio 自己的完成唤醒
     边，若唤醒边由作者自建的同步原语承载、且藏在 L2 看不到的地方，`close()` 会被
     放行并进入 drain（自等祖先）。**最小复现**（`__slots__` 对象把 future 藏起来：
     L2 的未知持有者只扫 `__dict__`，`vars()` 在 slots 对象上抛 `TypeError`）：

     ```python
     class SlotGate:
         __slots__ = ("f",)          # 无 __dict__ ⇒ 未知持有者扫描看不到 f

     async def scenario(bus):
         loop = asyncio.get_running_loop()
         gate = SlotGate()
         gate.f = loop.create_future()

         async def handler(event):   # D：在飞分发任务
             await gate.f            # D 等的就是这个 future

         bus.handlers["h1"] = (handler, "*", None, None)
         await bus._process_message(msg, "h1")

         close_task = loop.create_task(bus.close())   # 在 dispatch 之外创建
         close_task.add_done_callback(lambda t: gate.f.set_result(None))
         await close_task            # close 完成会唤醒 D ⇒ drain 自等
     ```

     该形状被判 **clean 放行**（`Test109R6HonestBoundary` 钉住当前行为，是**声明残差**
     而不是 completeness 声明）。为什么两层都够不到：L1 判的是 **context 后代关系**，
     这个 close 任务不是任何 handler 的后代；L2 判的是 **asyncio 自己的唤醒边**，
     而作者用自建原语耦合同一循环时，那条边不在 asyncio 的簿记属性里。
  6. **声明残差 R2**：唤醒边**只由一个存在回调闭包 cell 里的容器**持有的形状不展开
     （闭包 cell 的兄弟簿记规则，见 L2）；这是为了不把 `asyncio.wait` /
     `as_completed` 的 700 个兄弟当成唤醒边而误拒合法关停（声明盲区，不是完备性）。
  7. **SIGTERM**：`event_filter.py` 不注册信号处理器，进程直接结束，本条不含信号
     处理；`asyncio.run` 后未调用 `close()` 的路径不在契约内。
- **版本边界（r6 实测；判据内核 `plaita/event/_selfclose_guard.py` 只依赖 stdlib，
  可独立抽出驱动）**：

  | CPython | stdlib 属性自检 | 形状表（`scripts/selfclose_109_110_versions.py`） | 备注 |
  |---|---|---|---|
  | 3.10.21 | 通过 | 7/7 | 无 `TaskGroup`/`Barrier`（类型不存在 ⇒ 对应形状不可达，自检按版本跳过） |
  | 3.11.16（仓库 venv） | 通过 | 11/11 | `as_completed` 经 `Queue._getters` 证明出 in_graph |
  | 3.12.14 | 通过 | 11/11 | 同 3.11 |
  | 3.13.15 | 通过 | 11/11 | `as_completed` 返回 `_AsCompletedIterator`：未知持有者持 `_done`（Queue）⇒ **fail-closed（incomplete）**，不再漏检 |

  运行方式：3.11 用仓库 venv；3.10/3.12/3.13 用
  `uv run --no-project --python <v> python scripts/selfclose_109_110_versions.py`
  （不装 plaita 依赖，按文件路径加载判据内核，用真实 asyncio 对象驱动）。四个版本
  **都安全**；自检在每个运行时重新验证属性表，若未来 Python 改名 ⇒ 该运行时 `close()`
  全拒（fail-closed），不会静默。
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

- **代价（真实 Redis 复算，单条路径口径）**：correlation 索引只取该 correlation 的
  候选行，`data:` GET 数 = 候选数，与该 `event_type` 的全部订阅数无关
  （`test_correlation_index_cost_is_bounded_by_matches`：100 条同类型只有 1 个候选
  ⇒ 1 次 `data:` GET）。**数字对齐（r6 复算）**：在「`(shared.type, CID)` +
  `(other.type, CID)` + `(shared.type, OTHER)`」三行语料上，**单条路径**的 `data:`
  GET 都是 **2**——两条件查询 2（候选 = CID 的 2 行，AND 后返回 1 行）、
  correlation-only 2（返回 2 行）、type-only 2（返回 2 行）。**旧数字 6 是三条查询
  累计的计数口径，不是单条路径的代价**。复算命令（自起真实 Redis，free port）：
  `PLAITA_TEST_REDIS_URL=redis://127.0.0.1:<port>/0 python -m pytest -q
  tests/unit/test_issue109_110_r6_parity.py::Test110RealRedisConjunction`
  ⇒ 断言 `(two-condition, correlation-only, type-only) == (2, 2, 2)` 且和 = 6。
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
- 三后端对齐**在生产路径与 list 层都成立**（`scripts/selfclose_109_110.py` 的 parity
  格：`['hit']`；r5 起 redis 的 correlation 分支也对候选行做 `event_type` 过滤——AND
  语义）。**已修正的旧声明**：r5 之前 redis 的 `list_subscriptions(event_type=…,
  correlation_id=…)` 在 list 层**不预过滤** `event_type`，会返回同 correlation 但
  `event_type` 不同的行（真实 Redis 实测 `returned=2`），memory / sqlalchemy 返回 1；
  r5 起三后端 list 层一致（真实 Redis 实测 `returned=1`，见
  `Test110RealRedisConjunction`）。单条件路径（`correlation_id` only / `event_type`
  only / 全量 SCAN）行为不变（r6 真实 Redis 实测：correlation-only 2 行、
  type-only 2 行）。
- `correlation_id` 为空时既有单条件路径行为不变（type/flow/node 索引、全量 SCAN）；
  全量 SCAN 路径按**已知前缀** `{prefix}data:` 剥离 id（不再 `split(':')[-1]`）——
  后者在 `subscription_id` 含 `:` 时取到截断值、GET 落空而**静默返回空**。


## 下一步

- [Checkpoint 概念](checkpoint.md)
- [API: plaita.event.core](../api/event.md)

