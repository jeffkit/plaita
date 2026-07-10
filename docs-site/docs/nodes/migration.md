# 弃用迁移

plaita 在演进中保留了大量向后兼容 shim，旧写法仍可用但会触发 `DeprecationWarning`。本页给出旧 → 新的对照，建议尽早迁移。

## 节点注册

| 旧（弃用） | 新（推荐） |
|-----------|-----------|
| `from plaita import node_register` + `node_register(MyNode)` | `get_default_registry().register(MyNode)` 或 `NodeRegistry().register(...)` |
| `from plaita import parse_node` + `parse_node(d)` | `get_default_registry().parse_node(d)` |
| `from plaita import nodes`（dict 风格） | `get_default_registry()` + `register/get/list_types` |

```python
# 旧
from plaita import node_register
node_register(MyNode)  # DeprecationWarning

# 新
from plaita import get_default_registry
get_default_registry().register(MyNode)
```

`node_register` / 模块级 `parse_node` / `nodes` dict 代理仍保留以不破坏存量代码，但新代码请用 `NodeRegistry`。

## 导入路径

| 旧 | 新（推荐） |
|----|------------|
| `from plaita.flow import Flow` | `from plaita import Flow`（**0.5.0 起 `plaita.flow` 已删除**） |
| `from plaita.flow import FlowExecution` | `from plaita import FlowExecution`（或 `from plaita.core.executor import FlowExecution`） |
| `from plaita.errors import FlowExecutionException` | `from plaita import FlowExecutionException`（或 `from plaita.core.errors import ...`） |
| `from plaita.types import STRING` | `from plaita import types` 后 `types.STRING`（或 `from plaita.core import types`） |

`plaita.errors` / `plaita.types` 仍是 shim，转发到 `plaita.core.*` 并触发 `DeprecationWarning`。`plaita.flow` 在 0.5.0 **已删除**，旧 import 会直接 `ImportError`。

## Flow 字段

| 旧字段 | 新字段 |
|--------|--------|
| `id` / `flowId` | `flow_id`（`flow_id` 优先，其余仍兼容） |
| `Flow.id` 属性 | `Flow.flow_id`（访问 `Flow.id` 发 `DeprecationWarning`） |
| `inputType` / `outputType` / `globalContext` | `input_type` / `output_type` / `global_context`（驼峰仍兼容） |

## 节点实现签名

| 旧写法 | 新写法 |
|--------|--------|
| 覆写 `def run(self, execution):` | 实现 `def execute(self, execution=None):` |
| `from plaita.flow import Flow, FlowCallback, FlowExecution` | `from plaita import Flow, FlowCallback, FlowExecution` |

旧文档示例常覆写 `run`，这会绕过 `Node.run()` 的输出校验。新代码请实现 `execute`。

## 表达式语法

| 旧文档写法（错误） | 正确写法 |
|------------------|---------|
| `${INPUT.name}` | `$INPUT.name` |
| `${NODE.x.field}` | `$NODE.x.field` |
| 字符串内 `${X}` 插值 | `{% $X %}` 插值 |

plaita 的表达式前缀是 `$`（可配置），不是 `${}`。函数调用是 `$F.func(...)`。

## plaita dist-node

`plaita dist-node` 命令行工具已从本运行时仓库移除，不再内置节点分发能力。

## 分布式执行入口

| 旧 | 新 |
|----|----|
| `FlowExecution.run(flow, params, mode='distributed', context=...)` | `execution.run_distributed(flow, params, saved_context=..., resume_type=..., resume_data=...)`（复用同一实例，保留回调） |

类方法 `run(..., mode='distributed')` 仍可用，但每次会新建 execution 实例，导致用户回调无法跨步骤保留。需要保留回调时直接用 `run_distributed`。

## 如何抑制 DeprecationWarning

迁移完成前若需临时抑制：

```python
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="plaita")
```

但建议尽早按本页迁移，未来大版本会移除 shim。

## 下一步

- [自定义节点](custom.md)
- [执行模式](../guide/execution-modes.md)
