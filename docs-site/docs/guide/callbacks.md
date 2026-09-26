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

!!! warning "签名不匹配 = 回调静默失效"

    覆写时必须接受框架传入的**全部位置参数**（签名如表中所列；不需要的参数用 `**kwargs` 吸收也可以，但不能少写位置参数）。签名不匹配时框架按**位置**传参会抛 `TypeError`，被 `CallbackManager` 捕获为 warning——你的回调**静默失效**，流程照常跑。

## 自定义回调

```python
from plaita import Flow, FlowExecution, FlowCallback

class TraceCallback(FlowCallback):
    def on_flow_start(self, flow, **kwargs):
        print(f"[flow start] {flow.flow_id}")

    def on_node_start(self, flow, node, **kwargs):
        print(f"[node start] {node.id}")

    def on_node_end(self, flow, node, result, error, exception, **kwargs):
        print(f"[node end] {node.id} -> {result}, error={error}")

    def on_flow_end(self, flow, result, error, exception, **kwargs):
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
    def on_node_end(self, flow, node, result, error, exception, **kwargs):
        raise ValueError("oops")  # 会被捕获、记录，不影响流程

class B(FlowCallback):
    def on_node_end(self, flow, node, result, error, exception, **kwargs):
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

## 集成：Langfuse（`plaita.obs.LangfuseCallback`）

官方观测适配器把 8 个钩子映射到 Langfuse 的 trace / span / generation 模型：

| 钩子 | Langfuse 对象 |
|------|---------------|
| `on_flow_start` | trace（name = flow_id，tags 自动附 `flow:<flow_id>`） |
| `on_node_start` / `on_node_end` | span；输出形如 `{"model", "usage", ...}`（llm 节点契约）时额外记 generation，OpenAI 用量键自动换算为 `{input, output, total}` |
| `on_flow_suspend` / `on_node_suspend` | 立即 flush（挂起进程随时可能消失） |
| `on_flow_end` | trace 收口（output / level）+ flush |

```bash
pip install plaita[langfuse]
export LANGFUSE_PUBLIC_KEY=... LANGFUSE_SECRET_KEY=... LANGFUSE_HOST=https://cloud.langfuse.com
```

```python
from plaita import FlowExecution
from plaita.obs import LangfuseCallback

execution = FlowExecution(callback_handlers=[LangfuseCallback()])
execution.run_compatible(flow, False)
```

### Distributed 模式的跨进程续写

trace id 解析链：`flow.global_context["langfuse_trace_id"]`（显式 key，最高优先）→
绑定 execution 的运行时 `$EXECUTION_ID` → 随机生成。`$EXECUTION_ID` 由运行时 fresh start
生成、随 checkpoint 持久化，因此**宿主只需在每次新建 `FlowExecution` 后调用
`cb.bind_execution(execution)`**，同一条分布式流程的所有步骤（含另一进程的 resume）自动
落在同一条 trace 上，无需注入流程定义。plaita-console 与 FlowWorker 的内建接线即此模式。

### 终态收尾的宿主责任

distributed 模式下内核不发 `on_flow_end`（is_end 由宿主循环判定）。v4 的流程根是
真 OTel span，不 end 不导出——宿主在 run 终结（completed / failed）时应调用
`cb.finalize()`（收口根 span + flush）；挂起场景用 `cb.flush()`（根保持 open 供
resume 续写）。FlowWorker 与 console 的内建接线已内置 finalize。

### 错误语义

内核只在节点**成功**路径发 `on_node_end`；abort 策略的节点失败以 `NodeExecutionError`
穿透、不触发 `on_flow_end`——此时 trace/span 保持 open，由宿主收尾（Langfuse TTL 兜底）。
`on_node_end(error=...)` / `on_flow_end(error=...)` 的 ERROR 标记契约保留，供未来内核补发
或自定义节点手动触发。适配器内部任何异常都吞掉记 warning，不影响流程执行。

## 下一步

- [调试](debugging.md) —— 回调 + Generator 模式构建调试器
- [API: plaita.core.callback](../api/callback.md)
