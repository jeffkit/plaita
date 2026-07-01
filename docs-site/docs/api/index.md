# API 参考

本章节由 [mkdocstrings](https://mkdocstrings.github.io/) 从源码 docstring 与 Pydantic 字段自动生成，覆盖 plaita 的核心模块。

!!! tip "公共 API 速查"

    日常使用只需从顶层导入：`from plaita import Flow, Node, FlowExecution, FlowCallback, NodeRegistry, get_default_registry, parse, parse_and_run, types`。
    顶层包通过 `__getattr__` 懒加载到下述各模块（见 [架构 - 分层约束](../architecture/layering.md)）。

## 模块索引

| 模块 | 内容 |
|------|------|
| [`plaita.core.flow`](flow.md) | `Flow`、`parse`、`parse_and_run` |
| [`plaita.core.executor`](executor.md) | `FlowExecution`、`ExecutionMode`、三种策略 |
| [`plaita.core.context`](context.md) | `ExecutionContext`、默认 event bus provider |
| [`plaita.core.runner`](runner.md) | `NodeRunner`（超时/重试/错误策略） |
| [`plaita.core.callback`](callback.md) | `FlowCallback`、`CallbackManager`、`LoggerCallback` |
| [`plaita.core.errors`](errors.md) | 异常类、`ErrorStrategy`、`ErrorHandler` |
| [`plaita.core.expression`](expression.md) | `ExpressionRegistry`、`ExpressionEvaluator` |
| [`plaita.node`](node.md) | `Node`、`NodeRegistry`、内置节点 |
| [`plaita.event.core`](event.md) | `EventBus`、`Event`、`EventSubscription` |
| [`plaita.storage.base`](storage.md) | `ExecutionStorage`、`FlowStorage`、`ExecutionState` |
| [`plaita.server.flow_worker`](server.md) | `FlowWorker`（需 `server` extra） |

## 可选模块

下列模块需要对应 extra，访问顶层符号时缺失会抛**可操作的 ImportError**：

| 符号 | 模块 | extra |
|------|------|-------|
| `FlowWorker` / `ManagementAPI` | `plaita.server.*` | `server` |
| `RedisStorage` | `plaita.storage.redis` | `redis` |
| `RedisEventBus` | `plaita.event.redis` | `redis` |
| `CodeNode` | `plaita.node.code` | `code` |
| `HTTP` | `plaita.node.http` | `http` |
