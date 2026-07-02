# 回调机制

回调让你在流程生命周期的关键节点注入自定义逻辑：日志、监控、追踪、调试埋点等。

## 生命周期事件

`FlowCallback` 提供 8 个钩子，默认全为 no-op，子类只需覆写关心的那几个：

| 钩子 | 触发时机 |
|------|---------|
| `on_flow_start(flow)` | 流程开始 |
| `on_flow_end(flow, result, error, exception)` | 流程结束（成功/失败） |
| `on_flow_suspend(flow)` | Distributed 模式下流程挂起 |
| `on_flow_resume(flow)` | Distributed 模式下流程恢复 |
| `on_node_start(flow, node)` | 节点开始执行 |
| `on_node_end(flow, node, result, error, exception)` | 节点结束 |
| `on_node_suspend(flow, node)` | 事件节点挂起 |
| `on_node_resume(flow, node)` | 事件节点恢复 |

## 自定义回调

```python
from plaita import Flow, FlowExecution, FlowCallback

class TraceCallback(FlowCallback):
    def on_flow_start(self, flow, **kwargs):
        print(f"[flow start] {flow.flow_id}")

    def on_node_start(self, flow, node, **kwargs):
        print(f"[node start] {node.id}")

    def on_node_end(self, flow, node, result=None, error=None, **kwargs):
        print(f"[node end] {node.id} -> {result}")

    def on_flow_end(self, flow, result=None, error=None, exception=None, **kwargs):
        print(f"[flow end] {flow.flow_id} -> {result}, error={error}")

flow = Flow.from_string(open("flow.json").read())
execution = FlowExecution(callback_handlers=[TraceCallback()])
result = execution.run_compatible(flow, False, name="test")
```

## 内置 LoggerCallback

`LoggerCallback` 把生命周期事件打到 `plaita.core.callback` logger。两种启用方式：

```python
from plaita import FlowExecution, LoggerCallback

# 方式一：构造时传 verbose=True，自动加 LoggerCallback
execution = FlowExecution(verbose=True)

# 方式二：显式添加
execution = FlowExecution(callback_handlers=[LoggerCallback()])
```

记得配置 logging 才能看到输出：

```python
import logging
logging.basicConfig(level=logging.INFO)
```

## CallbackManager

`CallbackManager` 负责把事件分发给多个 handler，单个 handler 抛异常会被捕获并记录为警告，**不会阻断**其它 handler 或流程执行。

```python
from plaita import FlowExecution, FlowCallback

class A(FlowCallback):
    def on_node_end(self, flow, node, result=None, **kwargs):
        raise ValueError("oops")  # 会被捕获、记录，不影响流程

class B(FlowCallback):
    def on_node_end(self, flow, node, result=None, **kwargs):
        print(f"B got {result}")  # 仍会执行

execution = FlowExecution(callback_handlers=[A(), B()])
```

### 子流程回调继承

子流程（`InlineFlow` / `Loop` / `Parallel` 等）通过 `get_child_execution()` 创建子 `FlowExecution`，其 `CallbackManager` 会**继承父级 handler**，避免双发。所以子流程的节点事件会冒泡到顶层回调。

### Distributed 模式跨步骤保留

Distributed 模式下，每次 `run_distributed` 推进一个节点。若希望回调**贯穿所有步骤**，必须**复用同一个 `FlowExecution` 实例**（见 [执行模式 - Distributed](execution-modes.md#distributed)）；用 `FlowExecution.run(..., mode='distributed')` 类方法每次会新建实例，回调不保留。

## 手动触发回调

自定义节点需要手动触发回调时，直接调用 `CallbackManager` 上的 `on_*` 方法：

```python
def execute(self, execution):
    execution.callback_manager.on_node_end(flow, node, result)
```

`FlowExecution` 不再提供 `trigger_*` 魔法捷径——所有可用的 facade 方法都是显式声明的，避免属性查找的黑箱。

## 下一步

- [调试](debugging.md) —— 回调 + Generator 模式构建调试器
- [API: plaita.core.callback](../api/callback.md)
