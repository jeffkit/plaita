# 调试

plaita 提供两种主要调试手段：**Generator 模式单步执行** 与 **LoggerCallback 日志**。两者可叠加使用。

## Generator 模式单步

`flow.debug(...)` 返回一个生成器，每节点 yield 一次，可在步骤间检查/修改上下文：

```python
import logging
logging.basicConfig(level=logging.INFO)

from plaita import Flow

flow = Flow.from_string(open("flow.json").read())

for step in flow.debug(name="test"):
    print(f"[{step['type']}] {step['id']}: result={step['result']}")
    print(f"  context = {step['context']}")

    if step["is_end"]:
        print("流程完成")
        break

    # 交互式单步：按 Enter 继续
    input("按 Enter 继续下一步...")
```

每次 `step` 的结构见 [执行模式 - Generator](execution-modes.md#generator)。

### 检查与注入

因为 `step["context"]` 是当前上下文快照，你可以在两次 yield 之间观察中间状态、定位表达式求值问题、确认分支走向。这是构建可视化调试器的基础（见 [生成器调试器场景](../scenarios/debug-with-generator.md)）。

## LoggerCallback 日志

开启 `verbose` 让 plaita 自己打印生命周期事件：

```python
import logging
logging.basicConfig(level=logging.INFO)

from plaita import FlowExecution

execution = FlowExecution(verbose=True)
execution.run_compatible(flow, False, name="test")
```

输出形如：

```
[flow start] echo
[node start] start @ flow echo
[node end] start @ flow echo with result: None
[node start] end @ flow echo
[node end] end @ flow echo with result: kongjie
[flow end] echo with result: kongjie
```

## 自定义回调埋点

需要更结构化的调试信息时，写一个 `FlowCallback` 把事件送到你的观测系统：

```python
from plaita import FlowExecution, FlowCallback

class DebugCallback(FlowCallback):
    def on_node_end(self, flow, node, result, error, exception, **kwargs):
        # 发到 tracing / metrics / 文件...
        print(f"NODE {node.id} done: {result!r} err={error!r}")

execution = FlowExecution(callback_handlers=[DebugCallback()])
```

回调签名必须接受框架传入的全部位置参数，否则会被捕获为 warning 且回调静默失效——详见 [回调机制](callbacks.md)。

## 调用方超时诊断

若流程 hang，可用节点级/流程级 `timeout`（见 [错误处理与超时](error-handling.md)）配合 `abort` 策略快速失败，并在 `on_flow_end` 的 `error` / `exception` 里拿到堆栈。

## 异步调试

异步上下文用 `arun` 的 lazy 变体拿到异步生成器：

```python
async for step in flow.arun_compatible_lazy(...):
    ...
```

或直接 `await flow.arun(...)` 配合 `LoggerCallback` 观察日志。

## 下一步

- [执行模式](execution-modes.md)
- [回调机制](callbacks.md)
- [生成器调试器场景](../scenarios/debug-with-generator.md)
