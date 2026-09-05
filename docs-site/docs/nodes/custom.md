# 自定义节点

内置节点不够用时，编写自定义节点即可扩展 plaita。自定义节点与内置节点走同一套 `NodeRegistry` 机制，使用上无差别。

## 实现一个自定义节点

继承 `Node`，设 `node_type` / `node_name`，实现 `execute(self, execution=None)`：

```python
from plaita import Node

class GreetNode(Node):
    node_type = "greet"        # 流程 JSON 里 "type" 字段的值
    node_name = "打招呼"

    def execute(self, execution=None):
        # execution.evaluate 解析其中可能含的表达式
        name = execution.evaluate(self.output)
        return f"hello, {name}"
```

!!! tip "实现 execute，不要覆写 run"

    `Node.run()` 会调用 `execute()` 再做 `_validate_output`。请实现 `execute(self, execution=None)`，
    覆写 `run` 会绕过输出校验。这是旧文档示例的常见错误。

## 注册节点

使用自定义节点前要先注册到 `NodeRegistry`：

```python
from plaita import Flow, NodeRegistry, get_default_registry

# 推荐：用独立的 NodeRegistry
registry = NodeRegistry()
registry.register(GreetNode)

# 或注册到默认注册表
get_default_registry().register(GreetNode)
```

`register` 返回节点类本身，可作装饰器用：

```python
@get_default_registry().register
class GreetNode(Node):
    node_type = "greet"
    ...
```

!!! warning "注册时机"

    必须在执行流程**之前**注册。`Flow` 在解析 `nodes` 时会通过 `get_default_registry().parse_node` 查找节点类，未注册会抛 `unRecognized node type`。

## 在流程中使用

```json
{
    "flow_id": "greet_flow",
    "inputType": { "dataType": "object" },
    "nodes": [
        { "type": "start", "id": "start", "next": "greet" },
        { "type": "greet", "id": "greet", "output": "$INPUT.name", "next": "end" },
        { "type": "end", "id": "end", "output": "$NODE.greet", "resultType": "success" }
    ]
}
```

```python
get_default_registry().register(GreetNode)
flow = Flow.from_string(open("greet_flow.json").read())
print(flow.run(name="kongjie"))  # => "hello, kongjie"
```

## 访问上下文

`execution`（`FlowExecution` facade）代理到 `ExecutionContext`，提供：

| 能力 | 用法 |
|------|------|
| 求值表达式 | `execution.evaluate(value)` |
| 读状态 | `execution.get_state("$INPUT")` 或 `execution._ctx.context` |
| 写状态 | `execution.set_state(key, value)` |
| 全局变量 | `execution.get_global_variable(key)` |
| 子流程 | `execution.get_child_execution()` |
| event bus | `execution.get_or_create_event_bus()` |
| 协作取消 | `execution.cancel_event.is_set()` |

```python
class SumNode(Node):
    node_type = "sum"

    def execute(self, execution=None):
        nums = execution.evaluate(self.output)  # 期望是数组
        return sum(nums)
```

## 异步节点

需要异步 I/O 的节点设 `async_node = True` 并实现 `arun`：

```python
import asyncio
from plaita import Node

class AsyncFetchNode(Node):
    node_type = "async_fetch"
    async_node = True

    async def arun(self, execution=None):
        url = execution.evaluate(self.output)
        # 用 aiohttp 之类异步拉取
        await asyncio.sleep(0.1)
        return {"url": url, "data": "..."}
```

`NodeRunner` 会用 `asyncio.wait_for` 驱动 `arun`，超时同样生效。

## 事件节点（可挂起）

要支持断点续执挂起，继承 `EventNode` 并实现 `on_event` / `on_timeout` / `on_cancel`。详见 [断点续执 - 扩展节点](../distributed/extended-nodes.md)。

## 输入/输出校验

可在 `validate()`（构造期调用）与 `_validate_input` / `_validate_output`（运行期）加校验：

```python
class SafeNode(Node):
    node_type = "safe"

    def validate(self):
        assert self.output is not None, "output is required"

    def _validate_output(self, result):
        assert isinstance(result, str), "result must be str"
```

## 最佳实践

1. **用 `execution.evaluate` 处理表达式**，不要假设字段值是字面量。
2. **节点保持无状态**，所有状态写进 `execution` 上下文，便于分布式持久化。
3. **用 `plaita.logger`** 记日志，与系统日志集成；避免 `print`。
4. **明确定义 `node_type` / `node_name`**，前者是流程 JSON 的键。
5. **错误处理交给框架**：节点抛异常会被 `NodeRunner` 按 `errorHandler` 策略处理，无需自己 try/except 吞错。

## 分发节点库

把领域节点打包成独立 Python 库，通过 `plaita.nodes` entry_points 实现**自动发现**，无需用户手动 `register`。详见 [节点注册表与插件](registry.md)。

## 自定义 Node vs 数据源工具

两件事容易混在一起，选型时先分清：

| | **自定义 Node**（本页） | **plaita-ai 工具层** |
|--|------------------------|----------------------|
| 包 | `plaita` | `plaita-ai` |
| 何时用 | 新控制流语义、领域专用节点、要进 JSON/`type` 字段 | 把 HTTP/SQL/向量/Python 函数暴露给 Agent / `@flow` |
| 写法 | 继承 `Node`，设 `node_type`，实现 `execute` | `@tool` / `HttpToolSource` / YAML 清单 → 动态节点 |
| `@flow` 调用 | `MY_NODE(...)`（`node_type` 大写） | `GET_USER(...)` 或兼容 `TOOL(action=...)` |
| 文档 | 本页 | [工具节点与数据源](../ai/tools.md) |

`examples/agent/` 里的 `ToolNode` 是**教学用自定义节点**，用来演示「怎么写 Node」；生产集成请走 plaita-ai 工具层，不要把两套 `ToolNode` 当成同一个类。

## 完整可运行示例：Agent 编排

仓库里的 `examples/agent/` 给出三个**开箱即跑**的 LLM 相关节点（`LLMNode` 调 LLM、`ToolNode` 注册 Python 函数为工具、`RetrieverNode` 内存检索），并串成 RAG / Tool-use / Router 三个端到端 Agent 案例。内置 `FakeLLM` 无需 API key 即可运行，也可换真实 LLM。

```bash
# examples/ 不随 wheel 分发，需 clone 仓库后在仓库根目录运行
git clone https://github.com/jeffkit/plaita.git
cd plaita
python -m examples.agent.demo
```

详见 [`examples/agent/README.md`](https://github.com/jeffkit/plaita/tree/main/examples/agent) 与 [应用场景 - Agent 编排](../scenarios/agent-orchestration.md)。

## 下一步

- [节点注册表与插件](registry.md)
- [工具节点与数据源](../ai/tools.md) —— 自定义 Node 不够用、或要接 HTTP/SQL/向量时
- [应用场景 - Agent 编排](../scenarios/agent-orchestration.md)
- [API: plaita.node](../api/node.md)
