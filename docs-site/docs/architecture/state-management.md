# 状态管理

`ExecutionContext` 是流程的"内存"，承载所有运行时状态。理解它有助于写好自定义节点、调试表达式、正确使用分布式持久化。

## 命名空间

状态存在一个内部 dict 里，用 `$` 前缀的命名空间键组织（前缀可配置，默认 `$`）：

| 键 | 含义 | 写入时机 |
|----|------|---------|
| `$INPUT` | 输入参数 | `setup_flow` 时根据 `input_type` 填入 |
| `$NODE` | `{node_id: result}` | 每个节点执行后由 `NodeRunner.update_node_result` 累加 |
| `$GLOBAL` | 全局变量 | `setup_flow` 时从 `flow.global_context` 拷贝并注入 `flow_id` |
| `$PARENT` | 父流程上下文 | `setup_flow` 时填 `parent.context`（无父则为 `{}`） |
| `$ENV` | 环境变量 | `clean` 时从 `os.environ` 拷贝并过滤敏感前缀 |
| `$LAST_NODE` | 最近执行的节点 id | 每节点执行后写入，供 Distributed 续跑 |
| `$BRANCH` | 最近分支命中 | 分支节点写入 |
| `$EXECUTION_ID` | 执行 ID | 首次访问 `execution_id` 时惰性生成（uuid4 hex） |
| `$FLOW_ID` | 流程 id | `setup_flow` 时写入 |
| `EXPRESS_PREFIX` | 表达式前缀 | `setup_flow` 时写入，供 `EventSubscription.matches_event` 等使用 |

## 输入归一化

`_get_context_value` 按 `input_type.dataType` 决定 `$INPUT` 形态：

- `object` / `map`：关键字参数 dict，或位置首参（dict）
- `array`：位置参数元组
- 其它：位置首参单值

## 表达式求值

`ExecutionContext.evaluate(value)` 递归求值（list/dict 逐项），字符串走 `ExpressionEvaluator`（详见 [表达式](../guide/expressions.md)）。

**父级回溯**：当某字符串表达式在当前上下文求值为 `None`，且存在父上下文，会自动 `parent.evaluate(value)` 再试一次。这让子流程能"穿透"引用父级数据。

## 父子链

`ExecutionContext.child()` 创建子上下文，继承父的所有 `express_*` 配置、`event_bus`、`evaluator`，并把 `parent` 指向自己。`get_global_variable(key)` 也会沿父子链向上查找。

`FlowExecution.get_child_execution()` 创建子 facade，其 `CallbackManager` 通过 `inherit_handlers` 继承父级 handler 一份，避免事件双发。

```mermaid
flowchart TD
    P["父 ExecutionContext"]
    C1["子 ctx (InlineFlow/Loop/Parallel)"]
    C2["孙 ctx (嵌套)"]
    P -->|child()| C1
    C1 -->|child()| C2
    C2 -.->|未命中时回溯求值| C1
    C1 -.->|未命中时回溯求值| P
```

## event bus 获取

`get_or_create_event_bus()` 按优先级解析 event bus：

1. 自身已设的 `event_bus`
2. 父链上的 `event_bus`
3. 由顶层 `plaita` 包注册的**默认 event bus provider**（惰性，首次调用才 import `plaita.event`）
4. 都没有则返回 `None`

这种 provider 注入是为了让 `plaita.core` 不直接 import `plaita.event`，保住分层（见 [分层约束](layering.md)）。

## 敏感环境变量过滤

`clean()` 时按前缀过滤 `os.environ`，避免泄漏到 `$ENV`：

```
AWS_SECRET, AWS_SESSION, DATABASE_, DB_PASSWORD,
SECRET, TOKEN, API_KEY, PRIVATE_KEY, CREDENTIAL,
PASSWORD, PASS_, REDIS_PASSWORD
```

匹配规则是键名**大写**后 `startswith` 任一前缀。

## 协作取消 { #cancellation }

`cancel_event` 是一个 `threading.Event`：

- `clean()` 时重建，避免上次取消信号串到下次
- `NodeRunner` 在同步节点超时时 set 它，节点可 poll 提前退出
- **不可 pickle**：`__getstate__` 会剔除它，子进程得到全新未触发事件——因此跨进程取消不支持

## 序列化（分布式）

```python
ctx.to_dict()          # => dict(self._context)
ExecutionContext.from_dict(data, **kwargs)
```

分布式模式下，把 `to_dict()` 的结果存入 `ExecutionStorage`，下次 `run_distributed(saved_context=...)` 时恢复。

## 下一步

- [执行引擎](execution-engine.md)
- [断点续执](../distributed/checkpoint.md)
- [API: plaita.core.context](../api/context.md)
