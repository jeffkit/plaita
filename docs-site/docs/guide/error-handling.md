# 错误处理与超时

plaita 提供节点级/流程级的超时控制、错误策略与重试，并把错误事件传播到所有注册的回调。

## 超时

### 流程级超时

在 `Flow` 上设 `timeout`，整个流程的累计执行时间不得超过该值。格式为 **ISO 8601 时长字符串**或**毫秒整数**：

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

### 取严原则

调用方传入的 `timeout`、节点自身 `timeout`、流程 `timeout` 之间取**更严（更小）者**生效，不会静默丢弃。流程级剩余时间会作为节点执行的最大预算下传。

### 协作式取消

同步节点在 daemon 线程上执行，超时时 plaita 会设置 `ExecutionContext.cancel_event`（一个 `threading.Event`），节点可在长任务里 poll 它提前退出。这是**协作式、尽力而为**的取消；跨进程取消不支持（子进程会拿到全新的未触发事件）。

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

`retryTimes` 控制重试次数：在重试耗尽前不触发策略，重试全部失败后才按 `strategy` 处理。

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

`End` 节点 `resultType: "error"` 会抛 `FlowResultError`，把业务错误码透传给调用方：

```json
{
    "type": "end",
    "id": "bad",
    "resultType": "error",
    "error": { "code": 4001, "message": "参数非法" }
}
```

`resultType` 三种：`success`（正常返回 `output`）/ `nop`（返回 `None`）/ `error`（抛 `FlowResultError`）。

## 回调通知

无论超时还是错误，相关事件都会经 `CallbackManager` 传播到所有注册的 `FlowCallback`（见 [回调机制](callbacks.md)）。`on_node_end` / `on_flow_end` 会带上 `error` 与 `exception` 参数。

## 下一步

- [回调机制](callbacks.md)
- [API: plaita.core.errors](../api/errors.md)
- [API: plaita.core.runner](../api/runner.md)
