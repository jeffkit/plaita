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

- **`close()` 是总体、非阻塞、幂等的关停请求**（r7）：受理时同步置 `_closing`
  （拒绝新分发），在**总线构造 context**（`_owner_context`）里创建 teardown 任务，
  返回同一个 close future；`close()` 用 `asyncio.shield` 等它 ⇒ teardown **不是
  调用者的 context 后代**，调用者被取消也不打断关停。重复 `close()` 返回同一个
  future（幂等；`_teardown_runs == 1`）。
- **受理顺序（r8/F1，曾导致挂起）**：`request_close()` **先**落 `_close_future` /
  `_closing` / `_teardown_task` 占位，**再** `create_task` teardown。
  `loop.set_task_factory(asyncio.eager_task_factory)`（3.12+ 的**公开**配置项）下
  `create_task` 会同步把 coroutine 跑到第一个挂起点；空闲总线（无在飞分发、无监听、
  后端 `close` 不 await）会一路跑到 teardown 的 `finally`。r7 在 spawn **之后**才赋
  `_close_future`，`finally` 读到 `None`，稍后赋值的 future **永不 resolve** ⇒
  `await bus.close()` 永久挂起。3.12.14 实测矩阵：eager + 空闲 + 旧顺序 ⇒ `HANG`
  （`teardown_runs=1, close_future_done=False`，五个后端各关 1 次）；eager + 在飞分发
  ⇒ 正常（在 drain 的 `asyncio.wait` 处先挂起，侥幸避开）；非 eager 两种都正常。凡是
  teardown 会读的属性都必须加在 spawn 之前。钉住：
  `tests/unit/test_issue109_110_r8_parity.py` 的顺序/同步完成用例 + 3.12+ 上真
  `asyncio.eager_task_factory` 的端到端用例。
- **teardown 顺序**（P0 是**顺序**的后果，不是检测的后果）：① 有界等待（默认
  `DISPATCH_DRAIN_TIMEOUT_SECONDS = 5.0`）已登记的在飞分发，**不取消**它们；超时的
  逐个打含 `event_id` / `handler_id` 的 `WARNING`（「未在 N 秒内完成，放手让它跑完并
  记录」），登记由完成回调回收；② **最后**才逐个 `cancel()` 监听任务并等落地；③ 释放
  pubsub / redis / 存储。
  **释放按能力探测（r8/G2 起，r9/Finding 2 修正并实测）**：解析顺序 `aclose`
  （redis-py ≥5 / fakeredis）→ `close`（redis-py 4.x 与自定义后端）→ `disconnect`
  （**兜底，且在 redis-py 上是死分支**：4.5.5 与 8.1.0 的 `Redis`/`PubSub` 都**没有**
  该属性；r8 文档把它写成「redis-py ≥4.2 的 aclose 前身」是错的，只有别的后端可能
  命中），**解析与调用都在同一步的 try 内**，单步失败（含三个方法都缺失）具名
  WARNING 后继续释放其余组件。**光按能力选方法还不够**：`close()` / `aclose()` 只有在
  **签名接受**时显式传 `close_connection_pool=True` 才真的关连接池——不传时它们回落到
  `auto_close_connection_pool`，而该标志由**客户端的构造方式**决定（4.5.5 的
  `Redis.from_url()` ⇒ `False`；5.x+ 的 `from_url()` 事后把它改成 `True`，但**外部注入
  `connection_pool=` 的客户端仍是 `False`**）。`_pool_closing` 用 `inspect.signature`
  判断（签名不可知按**不支持**处理，绝不硬塞关键字——4.5.5 的 `PubSub.close(self)` 会
  `TypeError`；返回值非 awaitable 时也不裸 `await`，4.5.5 的 `PubSub.close()` 是**同步**
  方法）。**「`PubSub.close()` 在没建过 connection 时返回 `None`」是 r9 的错误说法**
  （r10 更正）：真实构造的 `PubSub` 上 `connection is None` 时它照样返回
  `reset()` 的协程（实测 `isawaitable` 为 True）；只有构造中途崩溃、连 `connection`
  属性都不存在的半成品才 `return None`（那个对象连 `__del__` 都会炸）。`isawaitable`
  检查仍然必要，理由不同：**关闭方法是同步的**，别的后端也可以同步返回 `None`。
  三个存储各自的 `close()` 也走同一条能力路径（旧实现直接
  `await self.redis.aclose()`，4.x 下三个存储的连接**一个都没释放**）。

  **实测支持范围**（连接数由服务端 `CLIENT LIST` 按 `client_name` 计数得出；redis 7）：

  | redis-py | 客户端形状 | 裸关停 | 带 `close_connection_pool=True` |
  |---|---|---|---|
  | 4.5.5（真装） | `from_url` | `close()` 后 1 → **1** | `close(True)` 后 1 → **0** |
  | 4.5.5（真装） | 外部注入池 | `close()` 后 1 → **1** | `close(True)` 后 1 → **0** |
  | 8.1.0（安装版） | `from_url`（`auto_close=True`） | `aclose()` 后 1 → **0** | `aclose(True)` 后 1 → **0** |
  | 8.1.0（安装版） | 外部注入池（`auto_close=False`） | `aclose()` 后 1 → **1** | `aclose(True)` 后 1 → **0** |
  | 4.5.5（真装，四组件总线） | `RedisEventBus` | r8 式裸 `close()`：4 → **3**（pubsub 的共享连接**确实**被释放；「零释放」只对三个存储槽位成立——三条各带一次 `'Redis' object has no attribute 'aclose'` WARNING） | `bus.close()`（r9/r10）：4 → **0**，0 WARNING |

  即：**4.5.5 上裸 `close()`、5.x+ 上裸 `aclose()` 都可能静默不释放**；保证是
  「签名接受则连池一起关，不接受则裸调用，全缺失则具名 WARNING」。测试覆盖**安装版
  真实客户端 + 真实 `ConnectionPool` + 记录型 `Connection`**（钉「池真的被拆」，不是
  方法名）与 `aclose` / 仅 `close` / 仅 `disconnect` / 三者全缺 / 签名不可知五种形状。
  r8 前 `_close_backends` 里 `self.pubsub.aclose` 的属性访问在 try **之外**，
  redis-py 4.5.5（无 `aclose`）下 `AttributeError` 逃出 teardown，调用方却看到成功、
  五个组件一个都没释放。
- **失败不伪装成功（r8/G3）**：teardown 没完整跑完（被 supervisor / 批量
  `all_tasks()` 取消、或步骤抛异常）时，`close()` 抛具名
  `RuntimeError(error_kind=teardown-incomplete)`，并清空幂等凭据 ⇒ 下一次 `close()`
  真正重试（`_teardown_runs` 递增），而不是早返回「成功」。r7 的 finally 无条件
  `set_result(None)`，会把「被取消」报成成功（`comp_closes == [0,0,0,0,0]`）且此后
  每次 `close()` 都幂等早返回 ⇒ 总线永不释放。
- **无运行循环具名拒绝（r8/G4）**：循环外调用 `request_close()` 给出
  `error_kind=no-running-loop`（不再裸 `RuntimeError: no running event loop`）；
  跨循环拒绝是 `error_kind=cross-loop-close`。
- **spawn 失败整体回滚（r9/Finding 1）**：「先落凭据、再 spawn」的顺序自身有个反面——
  `_spawn_teardown` 抛异常时（自定义/抛异常的 task factory、已关闭的循环）凭据在而
  teardown 任务根本不存在 ⇒ close future 永不 resolve 且 `_closing=True`，
  **第二次 `close()` 永久挂起**（r8 实测 2 s watchdog 超时；r7 的旧顺序反而能重试）。
  现在受理是**全有或全无**：失败时把 `_closing` / `_close_future` / `_teardown_task`
  恢复到受理前的值并抛具名 `error_kind=teardown-spawn-failed`；且只有**仍在飞**的
  teardown 任务才登记进 `_teardown_task`（eager factory 同步跑完的 done 句柄不回填，
  监督者按 `_teardown_task is not None` / `.cancel()` 不会被误导）。
- **回滚必须是真的（r10，r9 判负项）**：r9 把「spawn 抛异常」等同于「什么都没发生」。
  但工厂可以先 `t = asyncio.Task(coro)` 再抛（非 eager），或先让
  `asyncio.eager_task_factory` 同步跑完再抛（eager）。r9 在这些形状下：
  **非 eager** 回滚成「未受理、总线仍可用」，却留下**无人取用**的孤儿 Task——它被
  r9 无条件的 `coro.close()` 弄死在 `cannot reuse already awaited coroutine`，并在 GC 时
  打印 `Task exception was never retrieved`；**eager** 组件**已经全部关掉**
  （`_teardown_runs=1`、五个组件各 1 次）而 `_closing` 被回滚成 `False`，第二次
  `close()` 把三个存储**再关一遍**（`closes=[1,1,2,2,2]`）——这是**双重释放**。
  r10 把归属判定下沉到 `_spawn_teardown`：**已启动就收养**（返回那个 Task，
  `_closing` 保持已受理、`_teardown_runs` 如实为 1），**从未托管才回滚**；
  `coro.close()` **只在从未启动时**调用（启动过就保留其归属与取用回调）。
  工厂返回**非 Task**（已完成的 `Future` / 没有 `add_done_callback` 的 awaitable /
  coroutine 本身）时，close future 会永远悬着且 coroutine 打
  `was never awaited`（r9 实测两次 close 都超时）——r10 绕开工厂用 `asyncio.Task`
  直接接管，真跑一次关停（`_teardown_runs=1`，不留 PENDING）。
  `_spawn_teardown` 的返回类型因此收紧为「真正的 `asyncio.Task`，否则抛具名错」：
  `request_close` 里对返回值的**使用**（`add_done_callback` / `done()`）对任意工厂
  行为都不会失败，会失败的都在 spawn 内部并走同一条回滚。
  **r11 更正**：这条保证的前提「真 Task 就安全」并不成立——见下面 r11 两条。
- **「收养了但入口没跑」不再挂起（r11/A1、A3；**r12/A 已把该判据换掉，见下**）**：
  `isinstance(x, asyncio.Task)` 对**任何**真 Task 都为真。工厂交回一个包着**别的**
  coroutine 的真 Task（同类还有「返回前就被 cancel」「建在**另一个**事件循环上」）时，
  r10 会静默收养：我们的 `_teardown_entry` 从未被 await、`_teardown` 从未跑过 ⇒
  `_close_future` **永不结算**，两次 `close()` 都永久挂起
  （`(True, PENDING, True, 0)`、五个组件 `[0×5]`，外加
  `RuntimeWarning: coroutine ... _teardown_entry was never awaited`）。
  r11 的判据是 `_teardown_entry` **第一行**置的「已进入」标记（标记 holder 绑在该次受理
  上、holder 又绑在任务自己身上）：收养的 Task 结束时标记仍未置位 ⇒ 具名结算
  `error_kind=teardown-never-entered`（`_teardown_runs` **如实为 0**；入口 coroutine
  若从未启动则一并 `close()`，避免 "was never awaited"）。判据是**「任务结束」这一
  事件**——没有 busy-wait，也没有比它更弱的计时假设；返回时已 done/cancelled 的 Task
  走同一条同步判据。**合法包装协程**（`async def wrap(): await <我们的 coro>`）跑到入口
  那一行即置位 ⇒ 照常完成（判据是「入口有没有跑」，不是「coro 是不是同一个对象」）。
  **r12/A 更正**：这个标记**可被工厂一行伪造**（`holder` 就在交给工厂的 coroutine 的
  frame 里）⇒ r12 删除标记通道、判据改为「受理的 Task 结束时本轮 future 是否仍未结算」，
  见下一条。具名 kind / 状态元组 / 「合法包装照常完成」的口径**保持不变**。
- **不是我们的 Task 一律拒收（r11/A2、B；**r12/B 收紧判据**）**：跨事件循环建出来的
  Task（close future 只在本循环结算，跨循环收养会让它永远悬着），以及覆写了
  `add_done_callback` / `done` / `cancelled` / `exception` 的**敌对 `Task` 子类**
  （`isinstance` 放行 ⇒ `request_close` 对返回值的使用**裸抛且不回滚**），都由
  `_spawn_teardown` 结构性拒收：`error_kind=teardown-spawn-failed` + **整体回滚**
  （`_closing` 回受理前、凭据清空、`_teardown_runs=0`），reason
  （`teardown-foreign-loop` / `teardown-hostile-task-methods`）写在异常的 `__cause__`，
  并尽力中和那个被拒任务（挂取用回调 + `cancel()`）。于是 `request_close` 对返回值的
  **使用**对任意 factory 行为仍然不会失败——但保证方式从 r10 的「类型是 Task」升级为
  **结构性检查**：「`request_close` 用到的四个方法（`add_done_callback` / `done` /
  `cancelled` / `exception`）以及 `_accept_spawned_task` 自用的 `get_loop` 都是 stdlib
  原版，且任务绑在本循环上」。
  **r12/B 更正**：r11 的「原版」是用对象自己提供的东西证的（`__eq__`/`__func__`/逐次
  动态访问），被「实例影子 + `__eq__` 恒真」与「类级 TOCTOU 描述符」两个反例击败；
  r12 改成 `inspect.getattr_static`（类表 + 实例表）+ 绑定类型身份，见下一条 r12/B。
  钉住：`tests/unit/test_issue109_110_r11_parity.py`（15 用例，全部 2 s 有界看门狗；
  断言具名结局 + 精确状态元组/组件关闭次数 + 无孤儿 / 无 `never awaited` /
  无 `never retrieved` / 无双关；另含 r10 五行回归与 `_spawn_teardown` 返回契约）。
  r12 追加：`tests/unit/test_issue109_110_r12_parity.py`（12 用例，同样全部 2 s 有界
  看门狗）——5 条 A 负控（伪造一切 / 入口 frame 枚举 / 伪造总线计数器 / 伪造句柄槽 /
  直接结算 future 的 R-12a 边界）+ 4 条 B 负控（影子 / TOCTOU / 影子+别的 coroutine /
  判据不再信 `__eq__`）+ 合法形状全集（默认 Task、纯子类、带属性、`__slots__=()`、
  `__getattr__`、uvloop）。
- **r12/A：「入口跑过」的证据从**可写标记**换成**事件**（r11 的标记通道被**删除**）**：
  r11 把「已进入」标记放进交给工厂的 coroutine 的 frame（`holder` 参数）。工厂拿到的
  就是这个 coroutine，而 `coro.cr_frame.f_locals` 在**未启动**时就已含
  `{'holder': {...}, 'self': <RedisEventBus>}`（3.10/3.11/3.12/3.13 实测；
  **自由变量也出现在 `f_locals` 里**——`co_freevars` 里的 cell 同样可读）。于是一行

  ```python
  coro.cr_frame.f_locals["holder"]["entered"] = True     # 伪造「入口跑过」
  return asyncio.Task(some_other_coro(), loop=loop)      # 真 Task，但从不跑我们的
  ```

  即可绕过收尾判定：**两次 `close()` 永久挂起**（r12 实测四个版本一致的
  `(True, PENDING, True, 0)`、五个组件 `[0×5]`、`runs=0`、无具名错误）。把标记挪到
  `self` 上的字段（`_teardown_runs` 等）或闭包自由变量都**不是**修法：两者都在 frame
  里可达。r12 因此**不设标记**——判据是**事件**：受理的 Task 结束时，本轮
  `close future` 是否**仍未**结算（只有我们自己的关停路径会结算它）。

  **不变量（r12/A，可复算）**：*本轮结算凭据 `_TeardownRound`（future + 入口 coro +
  句柄 + judged）只存活在 `_spawn_teardown` 的闭包（看门狗 `_watch.__closure__`）与
  受理 Task 的属性上，**不作为参数/自由变量出现在交给工厂的 coroutine 的 frame
  里**；入口 `_teardown_entry()` 因此无参、无标记、不结算。*
  可达性枚举（`coro.cr_frame.f_locals` / `co_freevars`）：

  | | `f_locals` | `co_freevars` | 伪造后结局 |
  |---|---|---|---|
  | r11（修前） | `['holder', 'self']`（holder 里含 `entered`/`future`/`task`/`coro`） | `()` | `HANG/TIMEOUT` ×2，`(True,PENDING,True,0)`、`[0×5]`、`runs=0` |
  | r12（修后） | `['self']` | `()` | 具名 `teardown-never-entered`，`(True,'None',False,0)`、`[0×5]`、`runs=0`，两次 close 均有界 |

  负控：工厂枚举 `cr_frame.f_locals` 并**伪造其中每一个对象**（dict 写
  `entered`/`task`/`future`、list 追加、总线标记字段清零），再交回一个跑**别的**
  coroutine 的真 Task ⇒ 仍具名、仍不挂起、组件一个不关（
  `test_issue109_110_r12_parity.py::Test109R12ForgeChannelIsGone`）。
- **r12/B：判据不再问**对象自己**（r11 的 `__eq__`/`__func__`/逐次动态访问被替换）**：
  r11 的 `_task_uses_stdlib_methods` 用对象提供的 `__eq__`/`__func__` 做「绑定比较」，
  被两个反例击败：①**实例影子**（`__self__` 指向本任务、无 `__func__`、`__eq__` 恒真的
  假方法）；②**类级 TOCTOU 描述符**（类访问给 baseline、实例第 1 次访问给真 stdlib
  绑定方法、第 2 次抛）。两者都让判据放行，随后 `request_close` 对返回值的
  `add_done_callback` **裸抛且不回滚**（`(True, PENDING, False, 0)`）；①再叠加
  「交回跑别的 coroutine 的 Task」⇒ 第二次 `close()` 拿到陈旧的 PENDING future ⇒
  **挂起**（r12 四个版本实测一致）。r12/B 改为**结构判据 + 身份判据**，一次都不问
  「这两个对象相等吗」：`inspect.getattr_static` 分别查**类表**与**实例表**（不触发
  描述符、不触发 `__getattribute__`/`__getattr__`）都必须**就是** `asyncio.Task` 上的
  本体，再真绑定一次并要求 `type(bound) is type(baseline.__get__(task, cls))` 且
  `bound.__self__ is task`。两个反例都在**静态表**这一步出局（TOCTOU 描述符在
  **任何**实例访问之前就被拒，`calls == 0`）。
  合法形状逐一实测放行：默认 `Task`、纯子类、带额外实例属性的子类、`__slots__=()` 子类、
  覆写 `__getattr__` 的子类、**uvloop 的 task 类型**（3.10–3.13 上 `type(task) is
  asyncio.Task`）。
- **r12 具名残差（frame 之外；与 r12/A、B 同处声明）**：
  * **R-12a（`self` 必然可达）**：入口必须跑总线代码，`self` 因此在
    `coro.cr_frame.f_locals` 里。工厂可以绕过一切判据**直接结算**
    `self._close_future`（或改写 `_teardown_runs` / 留档）。r12 实测的后果是**假成功**
    （`close()` 正常返回、`comps=[0,0,0,0,0]`、`runs` 为工厂写入的值），**不是挂起**。
    无法关闭：`request_close` 的可执行体逐字节冻结（见下「三处口径更正」），
    `self._close_future` 必须是返回给调用方的那个真 future；`asyncio.Future.set_result`
    对任何持有者都可用。**边界**：伪造 frame 可达的**任何**对象不再改变判定。
  * **R-12b（句柄槽）**：`self._teardown_entry_task` 同属 `self`，可被伪造成别的 Task；
    但 `_started_teardown_task` 要求候选**确实托管着我们的 coro**（`get_coro() is coro`）
    或**已结束且 coro 已回收**（`done() and get_coro() is None`），伪造一个「永不结束的
    任务」不再能被收养（否则会命中 R-1）。伪造一个「已结束的无关 Task」只会走一遍
    R-1/never-entered 判定。
  * **R-12c（有状态 `__getattribute__`）**：r12/B 的第 3 步需要真取一次属性；若
    `type(task).__getattribute__` 对**这一次**给真方法、对 `request_close` 稍后那次给
    敌对方法，判据无法在两个时刻之间钉住对象。这类必须先通过「静态表 + 类型身份」两道
    闸，且只在合成形状（生产路径 0 处 `set_task_factory`）里可达。**不声称已关闭。**
  * **为什么不引入计时器**：能拿到的最强完成事件就是「受理的 Task 结束」（以及
    「future 已结算」）；比它更弱的只能是自己设超时——那会把「还在关」误报成「关完」，
    与 r8/G3「失败不伪装成功」的口径冲突。所以 R-1/R-4 这类**真正的长等**被声明为
    out of contract，而不是用计时器掩盖。
- **out of contract（r11/D；**r12/C 更正「唯一」**：合成形状；生产路径不使用
  `set_task_factory`，实测仓库内 0 处调用）**：
  ① 工厂交回的真 Task 既不跑我们的 coroutine、又**永不结束** ⇒ 没有完成事件可依，
  close future 会一直等到那个任务结束——**工厂侧**的长等（R-1）。
  r11 把它写成「**唯一**仍可能长等的形状」是**错的**：见 R-4。
  ② 我们的 coroutine 被别处驱动、而交回的 Task 又不跑它 ⇒ 按①的完成判据具名失败；
  若别处随后真的跑完 teardown，组件是关掉的、调用方却已看到 `teardown-never-entered`（R-2）；
  ③ 覆写 `cancel` / `__setattr__` / `__getattribute__` 等**未纳入检查**的钩子 ⇒
  拒收路径的「中和」只是尽力而为，可能留下一个在别处/别循环上仍会跑的 teardown
  （具名拒绝本身不变；R-3）。
  ④ **组件自己的 `aclose` / `close` 永不返回 ⇒ `close()` 无界等待**（**R-4**，与工厂
  **无关**）：入口**已经跑过**（`runs=1`）之后，`_teardown` 的第 3 步逐个 `await` 组件
  释放；只要其中一个永不返回，`close()` 就一直在等。r12 实测（redis 槽位的 `aclose`
  挂起）：2 s 看门狗触发，`(True, PENDING, True, 1)`、`comps=[1, 1, 0, 0, 0]`、
  `_teardown_error_kind=None`、两次 close 都等同一个 PENDING future。
  `DISPATCH_DRAIN_TIMEOUT_SECONDS`（5.0 s）只界住**在飞分发**那一步，**不界组件释放**。
  这是**声明的不在契约内**：我们没有权利替用户的组件设超时（超时后强行走下一步会把
  「关了一半」伪装成「关完」）。有界性由组件实现负责。

  **等待的有界性逐段列清**（`close()` 的等待只由这四段构成）：

  | 段 | 上界 | 依据 |
  |---|---|---|
  | 受理（`request_close` 同步段） | 无等待 | 同步执行，`spawn` 失败当场具名回滚 |
  | ① `_drain_pending_tasks` | `DISPATCH_DRAIN_TIMEOUT_SECONDS = 5.0 s` | `asyncio.wait(..., timeout=...)`，超时**不取消**、只点名 |
  | ② `_stop_listeners` | 同上 5.0 s | 逐个 `cancel()` 后 `asyncio.wait(..., timeout=...)` |
  | ③ `_close_backends` | **无界（R-4）** | 逐个 `await` 组件的 `aclose`/`close`/`disconnect` |
  | ④ 工厂交回的 Task 永不结束 | **无界（R-1）** | 只有「任务结束」这一事件可依 |

- **r10 报告的三处口径更正（r11/C，命令可复现）**：
  ① 「`request_close` 可执行体 **2070 B**」不可复现（验收方在不同切片下量到
  2722 / 2362 / 2711 / 1486 B）。改为**方法定义**的数字：`request_close` 去掉 docstring
  后的可执行体在 `5f6e308`(r9) → `8eaf3f6` → `789e070` → `bbf0d2b` → `e1b5356` → `HEAD`
  上**逐字节相同、2770 B / 47 行**。切片命令与实测输出：

  ```bash
  python3 - <<'PY'
  import re, subprocess
  def body(rev):
      L = subprocess.run(["git","show",f"{rev}:plaita/event/redis.py"],
                         capture_output=True, text=True, check=True).stdout.splitlines(True)
      i = next(k for k,l in enumerate(L) if re.match(r"\s*def request_close\(", l))
      end = next((j for j in range(i+1,len(L)) if L[j].strip()
                  and len(L[j])-len(L[j].lstrip()) <= 4
                  and re.match(r"\s*(def |async def |@)", L[j])), len(L))
      out, k = [], i
      while k < end:
          if L[k].strip().startswith('"""'):
              k += 1
              while k < end and '"""' not in L[k]: k += 1
              k += 1
          else:
              out.append(L[k]); k += 1
      return "".join(out)
  revs = ["5f6e308","8eaf3f6","789e070","bbf0d2b","e1b5356","HEAD"]
  g = {r: body(r) for r in revs}
  print(" | ".join(f"{r}={len(g[r].encode())}B/{len(g[r].splitlines())}L" for r in revs))
  print("identical:", len(set(g.values())) == 1)
  PY
  ```

  实测输出：`5f6e308=2770B/47L | 8eaf3f6=2770B/47L | 789e070=2770B/47L |
  bbf0d2b=2770B/47L | e1b5356=2770B/47L | HEAD=2770B/47L` 与 `identical: True`
  （r11 只改 `_spawn_teardown` / `_on_teardown_done` 等方法体与 docstring，
  `request_close` 的可执行体一个字节没动）。**r12 复算（四个版本、五个历史 rev +
  新 HEAD）**：仍是 `2770 B / 47 L`、sha256 `8b38c2bd044c563d…`，逐字节相同——
  r12 只改 `request_close` 的 **docstring**（方法体改动都在 `_TeardownRound` /
  `_watch_round` / `_task_uses_stdlib_methods` / `_spawn_teardown` / `_bind_round`），
  可执行体不动，故 5 条锚在该区间的变异仍**零重锚**。
  ② r10 报告的 (A) 行声称「收养的场景 `_teardown_task is None`」**是错的**：收养瞬间它
  是**真的在飞 Task**（状态元组第三元素 ≠ `None`；上面 `(True, PENDING, True, 0)`
  就是那个瞬间，修后由 `teardown-never-entered` 结算）。r11 用例
  `test_task_wrapping_other_coro_is_named_not_hung` 直接断言该瞬间；r12 负控
  `test_forging_every_reachable_object_is_named_not_hung` 断言同一个瞬间。
  ③ 锚在 `request_close` 块、r10 声称「零重锚」的变异是 **5** 条
  （`m109-r8-loopless-unnamed`、`m109-r8-spawn-order-legacy`、
  `m109-r9-backfill-done-teardown-task`、`m109-r9-spawn-failure-no-rollback`、
  `m109-r9-teardown-task-exception-unretrieved`），**不是 4** 条；r11 上它们仍是零重锚
  （r11 未触碰 `request_close` 的文件区间），**r12 上同样零重锚**（同上）。
- **记录口径一致（r10）**：`_teardown_error_kind` / `_teardown_task_error` 成对改写
  （取消 ⇒ `('cancelled', None)`；异常 ⇒ `(type(exc).__name__, exc)`；正常完成 ⇒
  `(None, None)`）。r9 在取消分支只写 kind 不碰 error，会出现 `kind='cancelled'`
  却仍挂着上一轮 `ValueError` 的不一致对。
- **幂等分支的跨循环也具名（r10）**：`close()` 在**另一个**事件循环里发现
  `_close_future` 已受理时，拒绝文案补上 `error_kind=cross-loop-close`
  （此前只有「有在飞分发」的分支带这个 kind）。
- **teardown Task 的异常被取用（r9/Finding 3）**：r8 只给 close **Future** 挂了取用
  回调；步骤抛异常时 Task 一侧无人取用，解释器在 GC 时打印
  `Task exception was never retrieved — RuntimeError('boom-step')`（3.11/3.12 实测；
  in-process 反复跑 396 次出现 12 次）。现在 done 回调取用并具名留档到
  `_teardown_task_error` / `_teardown_error_kind`（r10 起收养来的任务也挂它，
  eager 同步跑完的路径由入口包装的 holder 找回句柄）；测试在同一进程内连跑 40 次失败
  路径，`never retrieved` 计 **0**。
- **重试的重复释放口径（r9/Finding 4）**：teardown 未完成而重试时，`pubsub` / `redis`
  槽位已置 `None` ⇒ 不再释放；三个存储槽位保留对象引用 ⇒ **会再调用一次它们的
  `close()`**（真实实现有 `if self.redis:` 幂等闸，第二次是 no-op）。这是**有意**的：
  straggler 可能在两次尝试之间经 `initialize()` 把存储的客户端**复活**，第二次才关得掉
  那个复活出来的连接。测试把计数钉死（`(2, 2, 1)`）。
- **口径更正（r9/Finding 4）**：曾有说法称「把 `_teardown` 的 `finally` 结算去掉，
  r8 之前的套件仍全绿」——**不实**。r9 复测：在 r8 **之前**的树（`7cf3674`）上做等价
  变异（把 r7 `_teardown` 的 `finally: future.set_result(None)` 换成 `pass`；r8 的
  `m109-r8-teardown-finally-resolve-off` 定义原文只存在于 r8 之后的代码里，**不能**
  直接套到 r8 之前的树上），5 个 issue 套件里 **4 个在 240 s 界内仍未结束**
  （`rc=124` 真挂死：`event_redis` / `r3` / `r5` / `r6`），剩下 `r7` 套件 57 s 跑完也是
  `12 failed, 2 passed`；**没有一个是「绿」**。抓到它的是 **timeout**，不是断言。
  对照：同一棵 pre-r8 树不施加变异时 5 个套件全部 `rc=0`。r8 的真实价值是把这种
  「超时红」变成两条**可归因**的 `FAILED`；「变异存活/套件仍绿」的说法是错的。
- 因此 `close()` 返回后不存在「既未处理、也无处理记录」的静默第三态：每个分发要么在
  界内完成，要么**已点名**且允许继续跑完（`_pending_tasks` 里可能仍留有被点名的慢任务）。
- `close()` 开始后到达的分发被**拒绝并点名**（`_closing` 围栏），不会绕过本次 drain
  静默创建。
- **取消窗口也点名**（F2）：监听器内联 `await _process_message`，若在首个 await
  （`is_event_processed` 的 Redis 往返）处被 `close()` 取消，`CancelledError`
  （BaseException）会被显式捕获、打出含 `event_id` / `handler_id` 的 WARNING 后再
  抛出——不允许「既不完成、也不点名」。
- **自关停（#109/F1/F4…r7）：`close()` 不因调用者是 handler 或其 context 后代而自取消 /
  递归，判据只是提前拒绝。** 「安全」是本契约列出的**症状表口径**（无自取消、无
  `RecursionError`、无 destroyed-pending、无 handler 连坐取消），**不等于**任何环境下
  都必有界返回——r8/F1 修掉的 `eager_task_factory` + 空闲总线挂起就是反例（已修并钉住；
  见上文「受理顺序」）。下列 r6 反例只说明**判据**不完备：
  r6 把安全性押在两层判据上，被独立验收者两个决定性反例判负（两者**都不再是安全性
  残差**，见 T2）：
  * **CE-1** 两层未知持有者 `Outer(Gate(f))`：L2 的未知持有者扫描只进一层 `__dict__`，
    看不到内层已武装的 future；
  * **CE-2** 在 dispatch 之外创建、被某个 handler 等待的 close 任务：`create_task`
    的 `call_soon` 先于 awaiter 注册 `__wakeup`，L2 的首 tick 快照结构性看不到这条
    **纯 asyncio 簿记**的唤醒边。

  r7 的结论：**让 `close()` 按构造安全，不再让判据承载安全性**（顺序见上：不取消在飞
  分发、监听最后取消、请求在 owner context 调度、close future 由 teardown 收敛）。
  判据保留为**提前、响亮**的拒绝，且只在**确有在飞分发任务**时生效（无在飞分发时直接
  放行，见 T2.4）。两层判据（内核 `plaita/event/_selfclose_guard.py`，只依赖 stdlib，
  可在别的 CPython 上按文件加载驱动）：
  1. **L1 dispatch lineage**（廉价，不点名任何私有属性）：分发 handler 运行时置
     `contextvars.ContextVar("plaita_redis_in_dispatch_bus")`（值是总线实例）；
     `create_task` / `TaskGroup.create_task` / `ensure_future`（含 `shield`）复制当前
     context ⇒ handler 里创建的任务、以及它们再创建的任务**传递地**继承该标记。
     `close()` 在任何副作用之前具名拒绝：`current_task()` 就是本实例登记的在飞分发
     任务，或当前 context 的标记就是本总线。**显式换 context（`context=Context()`）
     或换线程/executor 不继承**——**不再声称 L2「覆盖」这些绕过**：L2 命中就提前拒绝，
     命不中就走安全关停（更晚），两种结局都安全。
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
     `as_completed` 的 `todo` 是兄弟簿记，展开会误拒 600+ 在飞的合法关停——判据盲区 R2）。
     **不扫 `__dict__`**，但对未知持有者只扫**一层**直接属性找已武装原语：因此
     `RedisEventBus` 自己经 `_track_task` 回调闭包出现时（`_pending_tasks`/`listeners`
     是容器、`lock` 未武装）不会误拒跨总线关停。
     **r7：判据内部任何非具名异常（例如自定义描述符抛 `ValueError`）都在
     `close_graph_verdict` 内收敛成具名 `incomplete`**，绝不裸逃到 `close()`。
  3. **stdlib 属性自检**（把「未来 Python 改名」从 fail-open 变 fail-closed）：首次
     `close()` 在 running loop 内对 `Task`/`Future`/`TaskGroup`/`Queue`/`Event`/
     `Condition`/`Semaphore`/`Lock`/`Barrier`/`StreamReader` **新建实例**并检查期望
     属性；缺任一 ⇒ **有在飞分发时** `close()` 具名拒绝
     （`_SELF_CLOSE_UNSUPPORTED_RUNTIME_REFUSAL`，文案点名缺哪个属性）。
     **没有 running loop 时不缓存结果**（r7 缺陷修复：把「没跑成」缓存成失败会让本
     进程此后每次 `close()` 都被拒，是 fail-closed DoS）。
  `close()` 命中 L1 或在飞分发任务即抛 `_SELF_CLOSE_REFUSAL`；命中 fail-closed 情形抛
  `_WAKE_GRAPH_INCOMPLETE_REFUSAL`；命中预算抛 `_SELF_CLOSE_BUDGET_REFUSAL`；自检未过抛
  `_SELF_CLOSE_UNSUPPORTED_RUNTIME_REFUSAL`。**四条都在任何关停副作用之前**
  （`_closing` 保持 `False`、listener 不取消、不进 `_drain_pending_tasks`）。拒绝后
  `asyncio.run` 正常返回（`Test109R6AsyncioRunTeardown`）。**历史对照**：同一反例在
  r5 上 guard 判 `('clean', 1)` 且进入 drain、被 5 s drain 超时取消；在 r6 上 CE-1 被
  放行并取消在飞任务、CE-2 触发 `Task.cancel` 递归（见 T2.1）。
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

覆盖边界（**T1/T2 口径**，不宣称「任何情况都不丢」，也不宣称判据完备）：

- **T1（本契约覆盖）**：同一事件循环里的关停——受理后停新分发，有界等待已登记的
  在飞分发（最多 `DISPATCH_DRAIN_TIMEOUT_SECONDS = 5.0` 秒）；超时**不取消**、只打含
  `event_id` / `handler_id` 的 WARNING 并放手让它跑完；监听**最后**取消。
  **下表列出的**调用者形状都安全（无自取消、无 `RecursionError`、无「Task was destroyed but
  it is pending」、无 handler 被 `_closing` 连坐取消、总线恰好关停一次），且在**受理顺序
  正确**的前提下有界返回（r8/F1 修掉了 `eager_task_factory` + 空闲总线下的挂起，该前提由
  `tests/unit/test_issue109_110_r8_parity.py` 钉住）：handler 内
  `await close()`、handler 后代带 `context=Context()`、两层未知持有者
  `Outer(Gate(f))`、线程 / executor + `run_coroutine_threadsafe` + `wrap_future`、
  CE-2 形状、`__slots__` 持有者、只有 close 任务的 `TaskGroup`、嵌套 `TaskGroup`——
  见 `tests/unit/test_issue109_110_r7_parity.py`（逐形状断言症状）。
  **「安全」是症状表口径，不是「任意环境 / 任意 task factory 下都必有界」**（r8 前
  `eager_task_factory` + 空闲总线正是反例）。
  L1（marker / 在飞分发身份）**或** L2（完成唤醒锥）命中的 `close()` **提前**具名
  拒绝，且在任何关停副作用之前；L2 证明不出来（缺属性 / 懒创建 / 未知持有者 / 预算或
  深度耗尽 / 判据内部非具名异常）同样具名拒绝。**判据只在确有在飞分发任务时生效。**
- **T2（声明不在契约内 / 仅**延迟**差异，不是安全差异）**：
  1. **判据漏判的形状**（L1 无标记、L2 认不出）：显式 `context=Context()` 换掉
     context、两层未知持有者（**CE-1**）、`__slots__` 藏 future（R1）、在 dispatch
     之外创建却被 handler 等待的 **CE-2** 形状。r6 把它们当**安全**残差并因此在完整
     总线上出现自取消 / `RecursionError`；**r7 起漏判只意味着关停更晚**（有界：
     drain 超时后放手，teardown 不取消在飞分发），安全性由关停顺序保证、不依赖判据。
     **属性缺失表 10/10 仍成立**：每个已知私有属性 present ⇒ `in_graph`，删掉 ⇒ 具名
     `incomplete`（`Test109R6AttributeAbsence`）。
  2. **唤醒锥预算**（提前拒绝的声明边界）：`_DISPATCH_GRAPH_MAX_NODES = 65536` 个节点
     内**枚举不完**时按「无法证明安全」具名拒绝 `budget`。**边界精确且经过测量**：唤醒锥
     65536 节点放行、65537 节点拒绝；对纯 future 链即链长 65535 放行 / 65536 拒绝
     （`Test109R6BudgetBoundary::test_documented_budget_boundary_65536`）。r5 的 512
     预算被验收者证伪；r6/r7 实测合法 600 / 10000 层 await 链**完全枚举、判 clean**
     （`test_legit_600_and_10000_deep_await_chain_not_refused`）。预算只把「无法证明」
     变成具名拒绝，**任何超过 65536 节点的唤醒锥都会被拒**——这是明知的保守边界，
     不是「只有对抗形状才撞上」。把预算分支改回 `clean` 的变异
     `m109-r5-redis-budget-fail-open` 被 `test_over_budget_chain_is_named_budget_refusal`
     杀死。
  3. **线程 / executor**：L1 不跨线程（各自独立 context）；**有在飞分发**时由跨循环
     围栏具名拒绝，**无在飞分发时不拦**；**同一循环**的 `run_coroutine_threadsafe` +
     `wrap_future` 属于 T1（r7 形状表钉住）。
  4. **过度拒绝已消失**（r7）：r3 曾声明的两处（`call_soon` 分离的 close、分发结束后
     detached 任务里的 close）与验收者报告的**未声明**一处（只有 close 任务的
     `TaskGroup`）现在都**放行**——没有在飞分发就没有可保护对象，判据不适用。
     这些形状与「被提前拒绝」只差**延迟**（提前拒绝更快失败），不是安全差异；合法
     TaskGroup 调用者不再需要任何绕行。钉住：
     `Test109R3ResidualHoles::test_call_soon_detached_close_after_dispatch_is_now_allowed`、
     `..._detached_task_after_dispatch_is_now_allowed`、
     `Test109R7EndToEndShapes::test_shape_taskgroup_with_only_close_task_works`。
  5. **判据盲区 R2**：唤醒边**只由一个存在回调闭包 cell 里的容器**持有时不展开（闭包
     cell 的兄弟簿记规则，见 L2）；这是为了不把 `asyncio.wait` / `as_completed` 的 700
     个兄弟当成唤醒边而误拒合法关停。同属**延迟差异**，不是安全差异。
  6. **SIGTERM**：`event_filter.py` 不注册信号处理器，进程直接结束；`asyncio.run` 后
     未调用 `close()` 的路径不在契约内。
- **版本边界（r6/r7 实测；判据内核 `plaita/event/_selfclose_guard.py` 只依赖 stdlib，
  可独立抽出驱动。范围：该表**只驱动判据内核**；r7 的关停顺序属于 `RedisEventBus`，
  由仓库 venv（3.11）上的 `test_issue109_110_r7_parity.py` 端到端覆盖）**：

  | CPython | stdlib 属性自检 | 形状表（`scripts/selfclose_109_110_versions.py`） | 备注 |
  |---|---|---|---|
  | 3.10.21 | 通过 | 7/7 | 无 `TaskGroup`/`Barrier`（类型不存在 ⇒ 对应形状不可达，自检按版本跳过） |
  | 3.11.16（仓库 venv） | 通过 | 11/11 | `as_completed` 经 `Queue._getters` 证明出 in_graph |
  | 3.12.14 | 通过 | 11/11 | 同 3.11 |
  | 3.13.15 | 通过 | 11/11 | `as_completed` 返回 `_AsCompletedIterator`：未知持有者持 `_done`（Queue）⇒ **fail-closed（incomplete）**，不再漏检 |

  运行方式：3.11 用仓库 venv；3.10/3.12/3.13 用
  `uv run --no-project --python <v> python scripts/selfclose_109_110_versions.py`
  （不装 plaita 依赖，按文件路径加载判据内核，用真实 asyncio 对象驱动）。四个版本的
  判据形状表都 OK；自检在每个运行时重新验证属性表，若未来 Python 改名 ⇒ 该运行时
  **有在飞分发时** `close()` 具名拒绝（fail-closed），不会静默；**无运行循环时自检
  结果不缓存**（r7 缺陷修复）。
- **副作用口径**：契约是「要么完成、要么被点名」，不是「handler 副作用必为 0」——
  被提前拒绝的分发根本没建任务；**超时被点名的分发会继续跑完**，因此 handler 及其
  自己 `create_task` 的子任务、`asyncio.shield` 包住的**其它** work 片段，可能在
  `close()` 返回后照常产生副作用。本条只保证**不静默**（r7 起还保证**不取消**）。
  **真实 Redis 实测（r7 验收者复算）**：`close_elapsed≈0.2s`（有界）；straggler 在
  WARNING 里点名且**不被取消**。两个必须知道的后果：
  1. **关停后客户端被复活**：straggler 继续跑 handler，handler 里的
     `mark_event_processed` 会经 `initialize()`；`initialize()` 见到
     `self.redis is None`（本进程自建客户端已在关停里关掉并被置 `None`）就**重建**
     客户端——实测 `tracker_redis_resurrected=True` ⇒ `close()` 之后又出现一个
     **没有人会再关**的连接（本次关停只覆盖关停那一刻存在的实例）。
  2. **可能重复投递**：若这次迟到的 `mark_event_processed` **失败**，事件不会落去重
     记录——实测 `marked_on_redis_after_straggler=False` ⇒ 同一事件会**再次投递**
     （至少一次投递语义的必然面）。
  **运维动作**：把「超时被点名」当**需要人工确认**的信号处理（WARNING 含 `event_id` /
  `handler_id`）——确认该事件是否已在本进程处理完、去重记录是否落地；必要时手工补
  `mark_event_processed`，并检查 redis 客户端/连接是否比关停前多（复活）。在 straggler
  真正结束前不要直接复用/销毁进程池。
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

