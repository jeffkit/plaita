# 状态管理

`ExecutionContext` 是流程的"内存"，承载所有运行时状态。理解它有助于写好自定义节点、调试表达式、正确使用分布式持久化。

## 命名空间

状态存在 `CheckpointState`（`plaita.core.state`，Pydantic `BaseModel`）里，用 `$` 前缀的命名空间键组织（前缀可配置，默认 `$`）。`CheckpointState` 暴露 dict-like 视图（`__getitem__`/`__contains__`/`get`/`keys`...），表达式引擎与节点插件仍像用 dict 一样访问 `execution.context`，底层却是 typed model——key schema 集中在 `CheckpointSchema` 单一事实源。节点经 `set_state("任意键", ...)` 写的非 schema 字段落到 model 的 `extra`。

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
3. 惰性 import `plaita.event.get_default_event_bus`（函数体内 import，不污染 `core` 的 import-time 依赖）
4. 都没有则返回 `None`

`core` 仍不持有 `EventBus` 实例；函数体内 lazy import 保住 `core → event` 分层（顶层包不再做 provider 注入，历史的全局 `_default_event_bus_provider` singleton 已删除）。

## 敏感环境变量过滤

`$ENV` 采用 **allowlist 模型**（2026-07 重构）：`clean()` / `setup_flow()` 时只把 Flow 显式声明的 `expose_env` 中、且真实存在于 `os.environ` 的 key 放进 `$ENV`；未声明时 `$ENV` 为空。

历史上这里曾叠了一份「敏感前缀黑名单」作为 allowlist 之上的「第二层防御」，但它的 `startswith` 匹配挡不住 vendor 前缀的真实密钥名（如 `OPENAI_API_KEY`、`STRIPE_KEY`、`PG_CONN`），「挡不住却给人安全感」比没有更危险，已移除。现在的策略：**allowlist 即用户责任**，每个命中的 key 会打一条 `logger.warning` 做审计可见性，但不做任何启发式拦截。如果确实需要暴露敏感变量，自行把关 `expose_env` 内容即可。

## 协作取消 { #cancellation }

`cancel_event` 是一个 `threading.Event`：

- **进程内传播**：子 `ExecutionContext`（经 `__init__` 传入 parent，或 `child()`）共享父的 `Event`，父 flow 取消能传到进程内子 flow / 并行分支（thread 模式）；`clean()` 时根上下文重建新 `Event`，子上下文重新同步到父当前 `Event`，取消链跨 clean 周期不丢
- `NodeRunner` 在同步节点超时时 set 它，节点可 poll 提前退出
- **不可 pickle**：`__getstate__` 会剔除它，子进程得到全新未触发事件——因此**跨进程取消不支持**（process 模式仅做启动前 `is_set()` 检查，详见 `ParallelExecutor.supports_cancel_propagation`）

## 序列化（分布式）

```python
ctx.to_dict()          # => CheckpointState.to_checkpoint_dict()（与旧 dict 格式逐键兼容）
ExecutionContext.from_dict(data, **kwargs)
```

`to_dict()` 会顺带跑 `validate_checkpoint` 漂移扫描——节点经 `set_state` 写的"看起来是 system key 但不在 `CheckpointSchema`"的字段会 warning，让新加 magic key 在 review/日志被发现。

分布式模式下，把 `to_dict()` 的结果存入 `ExecutionStorage`，下次 `run_distributed(saved_context=...)` 时恢复。

## 下一步

- [执行引擎](execution-engine.md)
- [断点续执](../distributed/checkpoint.md)
- [API: plaita.core.context](../api/context.md)
