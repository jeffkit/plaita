# 节点注册表与插件

`NodeRegistry` 是节点类型的注册表。plaita 提供一个进程级默认注册表，也支持独立实例与 entry_points 插件发现。

## NodeRegistry

```python
from plaita import NodeRegistry, get_default_registry

# 独立实例（默认只含内置节点，不扫描插件）
registry = NodeRegistry()

# 带 parent，初始化为父注册表的副本
child_registry = NodeRegistry(parent=get_default_registry())

# 显式开启 entry_points 自动发现
registry = NodeRegistry(auto_discover=True)
```

| 方法 | 用途 |
|------|------|
| `register(node_cls)` | 注册节点类（返回该类，可作装饰器） |
| `unregister(node_type)` | 取消注册 |
| `get(node_type)` | 查节点类 |
| `parse_node(node_dict)` | 从 dict 构造 `Node` 实例 |
| `list_types()` | 列出所有 `node_type` |
| `copy()` | 独立浅拷贝 |
| `discover()` | 惰性扫描 `plaita.nodes` entry_points（幂等） |

## 默认注册表

`get_default_registry()` 返回进程级单例，**首次调用**时惰性扫描 entry_points（不在 import 期触发，避免把可选插件依赖拖进 `import plaita.node`）：

```python
from plaita import get_default_registry

registry = get_default_registry()
registry.register(MyNode)
```

解析 `Flow` 时用的就是默认注册表（`Flow.parse_flow` 调 `get_default_registry().parse_node`）。

## 插件发现（entry_points）

第三方节点库通过 `plaita.nodes` entry_points 暴露节点类，用户安装该库后 plaita 自动发现并注册。

### 发布节点库

在你的 `pyproject.toml` 声明 entry_points（plaita 自身的 `server` 扩展节点就是这么发布的）：

```toml
[project.entry-points."plaita.nodes"]
greet = "my_plaita_nodes.greet:GreetNode"
fetch = "my_plaita_nodes.fetch:FetchNode"
```

或 `setup.py`：

```python
entry_points={
    "plaita.nodes": [
        "greet = my_plaita_nodes.greet:GreetNode",
    ],
}
```

用户 `pip install my-plaita-nodes` 后，plaita 在首次 `get_default_registry()` 时自动加载这些节点，无需在代码里 `register`。

!!! note "加载失败容错"

    entry_points 加载失败（如缺依赖）只会记 warning，不会阻断注册表初始化，保证其它节点可用。

### 推荐的节点库结构

```
my_plaita_nodes/
├── pyproject.toml          # 声明 plaita.nodes entry_points
├── my_plaita_nodes/
│   ├── __init__.py
│   ├── greet.py            # class GreetNode(Node): node_type = "greet"
│   └── fetch.py
```

`__init__.py` 可选地提供显式注册函数，供不想用 entry_points 的用户手动注册：

```python
# my_plaita_nodes/__init__.py
from plaita import get_default_registry
from .greet import GreetNode
from .fetch import FetchNode

def register_all():
    reg = get_default_registry()
    reg.register(GreetNode)
    reg.register(FetchNode)
```

## 下一步

- [自定义节点](custom.md)
- [弃用迁移](migration.md)
- [配置与可选依赖](../reference/configuration.md)
