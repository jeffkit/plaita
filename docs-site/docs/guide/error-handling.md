# 错误处理与超时

plaita 提供节点级/流程级的超时控制、错误策略与重试，并把错误事件传播到所有注册的回调。

## 超时

### 流程级超时

在 `Flow` 上设 `timeout` 作为流程级超时上限。口径**按执行模式不同**：

| 模式 | 口径 |
|------|------|
| `normal` / `generator`（`Flow.run` / `FlowExecution.run_compatible` 单次运行） | **累计**上限——同一次运行内所有节点共享同一份预算，已耗时长从预算里扣除后作为后续节点的最大预算下传（`NormalStrategy`/`GeneratorStrategy` 用一个 `start_time` 扣减） |
| `distributed`（`FlowWorker` / console 逐步推进） | **每步上限，不跨步累计**——每个 `run_distributed` 调用是独立预算，引擎不把已耗时长写进 checkpoint；需要"整个流程累计不超过 X"的调用方必须自行从 checkpoint 恢复剩余量，作为调用期 `timeout` 传入 |

!!! warning "与历史表述的有意偏离"

    本页旧文案称"整个流程的累计执行时间不得超过该值"——该**累计口径在 distributed 上从未实现**。plaita#71 让 distributed 拿到与 normal 一致的「取严 + 解析」，plaita#139 补上畸形输入硬错（非法类型/负值/非法串 → `FlowErrorException`，不静默降级成无限额），但**累计语义仍只存在于 normal/generator 的单次运行内**。是否要在 distributed 落地累计由维护者裁决；在那之前请按上表理解。

格式为 **ISO 8601 时长字符串**或**毫秒整数**：

```json
{
    "flow_id": "timed",
    "timeout": "PT5S",
    "nodes": [ ... ]
}
```

| 写法 | 含义 |
|------|------|
| `PT5S` | 5 秒 |
| `PT1M` | 1 分钟 |
| `PT1H` | 1 小时 |
| `5000` | 5000 毫秒 |
| `0` / `"0"` / `PT0S` / `PT` | **显式零预算**（最严，立即超时）。plaita#138 起不再被当作"不限额" |
| 未设 / `""` / `None` | **执行级默认 deadline**（1800s；`PLAITA_EXECUTION_DEADLINE_SECONDS` 可调，见下） |

`0` 与"没给超时"是两种不同语义：前者是**最严预算**，后者交给执行级默认预算。
历史上的解析把 `0` 与零时长 ISO 串一并归成"不限额"，于是"取最严"在取到最严值（`0`）
时反而完全失效——`flow.timeout="PT0S"` 曾让一个 0.309s 的节点跑满（plaita#138）。

#### 执行级 deadline（plaita#267）

`flow.timeout` / `execution.timeout` / 调用期 `timeout` **全部缺省**时，引擎给每个
执行补一个具名总预算：

- 默认 **1800s**（与运维 runbook「末节点停滞 >1800s 判死」同口径）；
- 环境变量 `PLAITA_EXECUTION_DEADLINE_SECONDS`（秒，支持小数）由运维调整；
  非数字 / 非有限 / `<=0` **不会**变成无限额——回退默认并打 WARNING；
- 显式 timeout（三者任一）仍优先，不再叠加默认值；
- 超时抛 **`ExecutionDeadlineError`**（`FlowTimeoutError` 子类，`code=-1` /
  `error_type=flow_error`）。它**不经过**节点的 `timeoutHandler`/`errorHandler`
  ——执行级预算到期是终态，不能被 `continue` 策略吞掉；
- worker 侧按 `FlowTimeoutError` 家族判定「不重试」，执行终态化 `error`（可收尸），
  不会再重投一次把 38.6h 的配置跑两遍；
- distributed 与 `flow.timeout` 同口径：**每步**一个预算（挂起/等待事件不占
  worker，也不计入本步墙钟）。

默认预算的作用是兜住"没有任何 timeout 配置"的执行：此前该场景完全无限额，
`requestTimeout=600` × `maxRedirects=20` × `retryTimes=10` 的合法配置可让单个
执行占用共享 worker 38.6 小时（issue #267）。

> 注：`Flow.timeout` 字段当前类型为 `Optional[str]`，故上表的"毫秒整数"写法在 JSON 里会被 pydantic 以 `string_type` 拒绝（传字符串 `"5000"` 可用）；这是既有的模型/文档不一致，另单跟踪。

### 节点级超时

每个节点可单独设 `timeout`：

```json
{
    "id": "slow",
    "type": "code",
    "timeout": "PT10S",
    "...": "..."
}
```

`node.timeout` 在**构造期**校验（`Flow.model_validate` / 直接实例化节点）：
负值、非有限值、超过 `sys.float_info.max` 的毫秒数、含年/月分量的 ISO 时长、
非法串，以及超过 **24h（`MAX_NODE_TIMEOUT_MS`）** 的取值都会被具名拒绝
（plaita#267）。`0` / `PT0S` 仍是合法的显式零预算。需要单节点跑超过 24h 的
场景请改用 flow 级 `timeout`（执行 deadline）。

### 取严原则

调用方传入的 `timeout`、节点自身 `timeout`、流程 `timeout` 之间取**更严（更小）者**生效，不会静默丢弃。流程级剩余时间会作为节点执行的最大预算下传。

distributed（worker / console）逐步推进路径同样按此取严（plaita#71），但**每一步调用独立计预算**——引擎不把已耗时长写进 checkpoint，所以「整个流程累计不超过 X」只在 normal/generator 的单次运行内是硬保证；distributed 下 `flow.timeout` 是**每步上限**，需要跨步累计的调用方请从 checkpoint 恢复剩余量后作为调用期 `timeout` 传入。

非法/畸形的 timeout 一律**响亮失败**，不静默降级：非数值非字符串类型（`list`/`dict`/任意对象）、负值、非有限 `float`（`inf`/`nan`）、超过 `sys.float_info.max` 的毫秒整数、含年/月分量的 ISO 时长（`P1Y`、`P1M`——没有固定秒数）、非法串。normal/generator 路径直接抛 `ValueError`（点名违规值与受支持的写法）；distributed 入口按既有契约归一成 `FlowErrorException`。历史上它们被静默当成"无限额"（`_parse_timeout` 返回 `None`），调用方预算被无声丢弃——plaita#139。

### 协作式取消

同步节点在 daemon 线程上执行，超时时 plaita 会设置 `ExecutionContext.cancel_event`（一个 `threading.Event`），节点可在长任务里 poll 它提前退出。这是**协作式、尽力而为**的取消；跨进程取消不支持（子进程会拿到全新的未触发事件）。

执行级取消（`FlowExecution.cancel()` / worker 的取消监听）另有粘滞的
`cancel_requested`：引擎在**每个重试尝试入口**检查，退避等待按 ≤0.5s 分片轮询
——`retryTimes=10` + 30s 退避不会再把取消拖到下一次尝试（plaita#267，cancel
在 ≤1s 内打断重试/退避）。退避本身也计入执行预算，不会睡过 deadline。

```python
import time
from plaita import Node

class PollableNode(Node):
    node_type = "pollable"
    def execute(self, execution):
        for _ in range(100):
            if execution.cancel_event.is_set():
                return "cancelled"
            time.sleep(0.01)
        return "done"
```

## 错误处理策略

每个节点可配 `errorHandler`（`RecoverableErrorHandler`），决定节点抛异常时怎么办：

```json
{
    "id": "risky",
    "type": "code",
    "errorHandler": {
        "strategy": "continue_with",
        "retryTimes": 3,
        "defaultValue": null,
        "code": -500,
        "message": "节点执行失败"
    }
}
```

| strategy | 行为 |
|----------|------|
| `abort` | 中止流程，抛 `FlowExecutionException`（默认） |
| `continue` | 忽略错误，本节点返回 `None`，继续往下 |
| `continue_with` | 用 `defaultValue` 作为本节点输出，继续往下 |

`defaultValue` 接受**任意类型**——标量、字符串、对象均可，不再限定 dict。

`retryTimes` 控制重试次数：在重试耗尽前不触发策略，重试全部失败后才按 `strategy` 处理。**合法域为 0..10**——构造期校验，超限或负值在保存/发布/加载时直接失败（`ValueError` 具名拒绝）。共享 worker 场景下，无上界的重试次数会让单个节点长期占用 worker。**口径变更（2026-10）**：负数此前构造期接受、运行期被 `max(0, …)` 静默钳 0（无任何告警，`retryTimes=-1` 与 `0` 无差别）；现在负值构造期具名拒绝。布尔 / 数字字符串（`true`→1、`"5"`→5 的隐式强转）同样构造期拒绝——五个定义入口（JSON / Builder / @flow / console / S-expression）同口径。显式 `None` 也与省略不同（省略即用默认）：S-expr 的 `:retry nil` / `None` / `null`、`@flow` 的具名 `retry_times=None` 与 `**` dict 字面量里的 `None` 一律具名拒绝（三个 caps 字段同口径）。

只有瞬态错误会被重试（HTTP 节点的 `NodeException` 1002/1003，含 429 限流），重试间隔为指数退避 `min(30s, 1s × 2^n)`。确定性配置错误不消耗重试次数：`NodeException` 1001（请求生成失败）与非 `NodeException` 的 `ValueError` 会立即按 `strategy` 处理（节点输出校验钩子 `_validate_output` 抛出的异常除外——它们仍按普通节点异常重试）。

### 超时处理器

节点可单独配 `timeoutHandler`（`ErrorHandler`），决定节点**超时**时怎么办，策略同上：

```json
{
    "id": "maybe_slow",
    "type": "code",
    "timeout": "PT2S",
    "timeoutHandler": { "strategy": "continue_with", "defaultValue": "fallback" }
}
```

- `abort`：抛 `TimeoutError` 包装为 `FlowExecutionException`
- `continue`：返回 `None`
- `continue_with`：返回 `defaultValue`

## 异常类型

plaita 用一套规范异常类型（定义在 `plaita.core.errors`）：

| 异常 | 含义 |
|------|------|
| `FlowExecutionException` | 流程执行异常，带 `code` / `message` / `error_type` / `node` |
| `FlowResultError` | 在 `End` 节点主动抛出，携带业务 `code` / `message` |
| `NodeException` | 节点内部抛出的异常 |

`FlowErrorType` 枚举区分错误来源：`NODE_ERROR` / `FLOW_ERROR` / `ERROR_RESULT` / `NODE_NOT_FOUND` / `EXECUTION_TIMEOUT` 等。

### 主动返回错误

`End` 节点 `resultType: "error"` 会以业务错误终止流程：运行时把节点内的
`FlowResultError` 包装为 `ErrorResultException`（`plaita.core.errors`，携带同样的
`code` / `message`）抛给调用方——**捕获时用 `ErrorResultException`**（它同时是
`FlowExecutionException` 子类）：

```json
{
    "type": "end",
    "id": "bad",
    "resultType": "error",
    "error": { "code": 4001, "message": "参数非法" }
}
```

`resultType` 三种：`success`（正常返回 `output`，**不写时的默认值**）/ `nop`（返回 `None`）/ `error`（包装为 `ErrorResultException` 抛出，见上节）。写了无法识别的值会打 `logger.warning` 并按 `success` 处理。

## 分布式模式的错误契约

Normal / Generator 模式抛出的异常保持具体类型（`NodeNotFoundError` /
`NodeTimeoutError` / `ErrorResultException` …）；**Distributed 模式把它们统一归一为
`FlowErrorException`（code=-500）**——这是分布式执行的外部契约：跨进程恢复时调用方
拿到的错误形状只有一种，原始类型的细节只保留在 `message` 文本里。

按 `error_type` / 异常子类分支处理的代码在分布式路径上不生效；需要区分错误类别时
从 `message` 解析，或改在 Normal 模式做细粒度错误处理。

```python
from plaita.core.errors import FlowErrorException

try:
    execution.run_distributed(flow, params)
except FlowErrorException as e:
    # 分布式模式下所有错误都是这个形状；具体原因看 e.message
    ...
```

## 回调通知

无论超时还是错误，相关事件都会经 `CallbackManager` 传播到所有注册的 `FlowCallback`（见 [回调机制](callbacks.md)）。`on_node_end` / `on_flow_end` 会带上 `error` 与 `exception` 参数。

## 下一步

- [回调机制](callbacks.md)
- [API: plaita.core.errors](../api/errors.md)
- [API: plaita.core.runner](../api/runner.md)
