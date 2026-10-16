# 迁移指南

本页帮你从旧版扁平 API 迁到当前分层架构，并标出 **0.5.0 / 0.6.x 已落地的 break**。详细旧→新对照见 [节点系统 - 弃用迁移](../nodes/migration.md)；破坏性变更全文见仓库根目录 [`MIGRATION.md`](https://github.com/jeffkit/plaita/blob/main/MIGRATION.md)。

## 0.5.0 必看（已 break）

| 变更 | 影响 |
|------|------|
| **`plaita.flow` 模块已删除** | `from plaita.flow import ...` → `ImportError`；改用 `from plaita import Flow, FlowExecution, ...` |
| **CodeNode 移出默认注册表** | 使用前须 `register_code_node()`；默认沙箱后端为 `docker` |
| **订阅失败拒绝挂起** | EventBus 订阅失败抛异常，不再返回 `is_suspend=True` |
| **FlowWorker 默认启用 EventBus** | 仅 `--no-event-bus` 可关闭；`--use-event-bus` 为无操作兼容 |

`plaita.errors` / `plaita.types` 仍为兼容 shim（`DeprecationWarning`），计划在 **0.7.0** 移除；新代码请用 `from plaita import ...` 或 `plaita.core.*`。

## 0.6.x 必看（已 break）

0.6.0 带入的主要迁移动作（全文见根目录 `MIGRATION.md` 的 `## 0.6.0` 段；0.6.1 为修复版本，无新增破坏性变更）：

| 变更 | 影响 |
|------|------|
| **`db` 执行/流程存储从公开路径下架** | `--execution-storage-type db` / `--flow-storage-type db` 不再是公开入口；改用 redis / 内存等支持档，experimental SQLAlchemy 需显式门闩 |
| **任务队列 Redis List → Stream** | 存量队列需迁移到 Stream 语义（at-least-once）；按旧 List 语义读队列会失配 |
| **Resume：execution lease** | 并发 resume 被租约拒绝；防多 worker 同时推进同一执行 |
| **任务队列：DLQ + max deliveries** | 超过投递上限的消息进 DLQ，不再无限重投 |
| **编排内核行为收紧** | switch/if 未命中且无 default → 抛错；http 节点失败 → raise（`errorHandler` 生效）；`FlowExecution` 实例不可重入；未知配置键打 warning |

## 0.3.x 起的核心变化

1. **分层架构** —— 引入 `plaita.core` 执行核心层，`event` / `storage` / `server` 依赖它而非反向。
2. **`FlowExecution` 重构为 facade** —— 拆出 `ExecutionContext` / `NodeRunner` / `CallbackManager` + 三种执行策略。
3. **`NodeRegistry` 取代全局 node dict** —— 节点注册走作用域化注册表，支持 entry_points 插件发现。
4. **顶层懒 re-export** —— `from plaita import Flow, Node, ...` 通过 `__getattr__` 懒加载。
5. **内核全异步** —— 同步 API 经 `async_utils` 桥接驱动异步内核。
6. **依赖反转** —— `core` 不再 import `event`，由顶层包注入默认 event bus provider。

## 导入路径迁移

| 旧 | 新（推荐） |
|----|------------|
| `from plaita.flow import Flow` | `from plaita import Flow`（**0.5.0 起旧路径已删除**） |
| `from plaita.flow import FlowExecution` | `from plaita import FlowExecution` |
| `from plaita.errors import FlowExecutionException` | `from plaita import FlowExecutionException` |
| `from plaita.types import STRING` | `from plaita import types` → `types.STRING` |

## 节点注册迁移

```python
# 旧
from plaita import node_register
node_register(MyNode)

# 新
from plaita import get_default_registry
get_default_registry().register(MyNode)

# 或用独立注册表
from plaita import NodeRegistry
registry = NodeRegistry()
registry.register(MyNode)
```

## 节点实现迁移

```python
# 旧（覆写 run）
class MyNode(Node):
    def run(self, execution):
        ...

# 新（实现 execute）
class MyNode(Node):
    node_type = "my_node"
    def execute(self, execution=None):
        ...
```

`Node.run()` 现在调 `execute()` 再做 `_validate_output`，覆写 `run` 会绕过校验。

## CodeNode（0.5.0）

```python
from plaita.node import register_code_node

register_code_node()                              # 默认 docker，需本机 Docker daemon
register_code_node(default_backend="subprocess")  # 半信任
register_code_node(default_backend="unsafe")      # 完全信任代码作者
```

未调用 `register_code_node()` 时，流程里的 `"type": "code"` 会报未识别节点类型。

## 表达式语法修正

| 错误 | 正确 |
|------|------|
| `${INPUT.name}` | `$INPUT.name` |
| `${NODE.x.field}` | `$NODE.x.field` |
| 字符串内 `${X}` 插值 | `{% $X %}` |

函数调用：`$F.funcName(args)`。

## 分布式执行迁移

```python
# 旧（每次新建实例，回调不保留）
FlowExecution.run(flow, params, mode='distributed', context=saved)

# 新（复用实例，回调贯穿）
execution = FlowExecution(callback_handlers=[MyCallback()])
step = execution.run_distributed(flow, params)
step = execution.run_distributed(flow, None, saved_context=ctx,
                                 resume_type="event", resume_data=data)
```

## 抑制过渡期警告

迁移完成前可临时抑制（**无法**恢复已删除的 `plaita.flow`）：

```python
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="plaita")
```

## 迁移检查清单

- [ ] 导入改为顶层 `from plaita import ...`（确认无 `plaita.flow`）
- [ ] `node_register` → `get_default_registry().register`
- [ ] 节点覆写 `run` → 实现 `execute`
- [ ] 使用 `code` 节点前调用 `register_code_node(...)`
- [ ] 表达式 `${...}` → `$...` / `{% $... %}`
- [ ] 分布式执行改用 `run_distributed` 并复用实例
- [ ] 按 extra 检查可选依赖（`server` / `redis` / `http` / `code`）

## 下一步

- [节点系统 - 弃用迁移](../nodes/migration.md)
- [架构 - 分层约束](../architecture/layering.md)
- [更新日志](../about/changelog.md)
