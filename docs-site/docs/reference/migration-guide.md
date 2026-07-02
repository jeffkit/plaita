# 迁移指南

本页帮你从 plaita 0.2.x 的扁平 API 迁移到 0.3.x 的分层架构。详细的旧→新对照见 [节点系统 - 弃用迁移](../nodes/migration.md)，本页侧重整体迁移思路。

## 0.3.x 的核心变化

1. **分层架构** —— 引入 `plaita.core` 执行核心层，`event` / `storage` / `server` 依赖它而非反向。
2. **`FlowExecution` 重构为 facade** —— 拆出 `ExecutionContext` / `NodeRunner` / `CallbackManager` + 三种执行策略。
3. **`NodeRegistry` 取代全局 node dict** —— 节点注册走作用域化注册表，支持 entry_points 插件发现。
4. **顶层懒 re-export** —— `from plaita import Flow, Node, ...` 通过 `__getattr__` 懒加载，旧路径变 shim。
5. **内核全异步** —— 同步 API 经 `async_utils` 桥接驱动异步内核。
6. **依赖反转** —— `core` 不再 import `event`，由顶层包注入默认 event bus provider。

## 导入路径迁移

| 0.2.x | 0.3.x（推荐） |
|-------|--------------|
| `from plaita.flow import Flow` | `from plaita import Flow` |
| `from plaita.flow import FlowExecution` | `from plaita import FlowExecution` |
| `from plaita.errors import FlowExecutionException` | `from plaita import FlowExecutionException` |
| `from plaita.types import STRING` | `from plaita import types` → `types.STRING` |

旧路径仍可用，但触发 `DeprecationWarning`，未来大版本移除。

> **退役时间表**：`plaita.flow` / `plaita.errors` / `plaita.types` 三个 shim 模块将在 **0.6.0** 移除。请尽快迁到上表右侧的推荐路径（或更精确的 `plaita.core.*` 子模块）。

## 节点注册迁移

```python
# 0.2.x
from plaita import node_register
node_register(MyNode)

# 0.3.x
from plaita import get_default_registry
get_default_registry().register(MyNode)

# 或用独立注册表
from plaita import NodeRegistry
registry = NodeRegistry()
registry.register(MyNode)
```

## 节点实现迁移

```python
# 0.2.x（覆写 run）
class MyNode(Node):
    def run(self, execution):
        ...

# 0.3.x（实现 execute）
class MyNode(Node):
    node_type = "my_node"
    def execute(self, execution=None):
        ...
```

`Node.run()` 现在调 `execute()` 再做 `_validate_output`，覆写 `run` 会绕过校验。

## 表达式语法修正

如果你沿用旧文档的 `${INPUT.name}`，请改为正确语法：

| 错误 | 正确 |
|------|------|
| `${INPUT.name}` | `$INPUT.name` |
| `${NODE.x.field}` | `$NODE.x.field` |
| 字符串内 `${X}` 插值 | `{% $X %}` |

函数调用：`$F.funcName(args)`。

## 分布式执行迁移

```python
# 0.2.x（每次新建实例，回调不保留）
FlowExecution.run(flow, params, mode='distributed', context=saved)

# 0.3.x（复用实例，回调贯穿）
execution = FlowExecution(callback_handlers=[MyCallback()])
step = execution.run_distributed(flow, params)
step = execution.run_distributed(flow, None, saved_context=ctx,
                                 resume_type="event", resume_data=data)
```

## 抑制过渡期警告

迁移完成前可临时抑制：

```python
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="plaita")
```

## 迁移检查清单

- [ ] 导入改为顶层 `from plaita import ...`
- [ ] `node_register` → `get_default_registry().register`
- [ ] 节点覆写 `run` → 实现 `execute`
- [ ] 表达式 `${...}` → `$...` / `{% $... %}`
- [ ] 分布式执行改用 `run_distributed` 并复用实例
- [ ] 按 extra 检查可选依赖（`server` / `redis` / `http` / `code`）

## 下一步

- [节点系统 - 弃用迁移](../nodes/migration.md)
- [架构 - 分层约束](../architecture/layering.md)
- [更新日志](../about/changelog.md)
