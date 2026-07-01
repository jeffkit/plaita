# 扩展指南

本指南介绍如何通过创建自定义节点来扩展 `plaita`。

## 创建自定义节点

要创建新的节点类型，需要继承 `Node` 基类并实现 `execute` 方法。

### 1. 定义节点类

```python
from plaita import Node, types

class MyCustomNode(Node):
    # 节点的类型标识符
    node_type = "my_custom_node"
    
    # 定义输入 schema（可选但推荐）
    input_schema = {
        "text": types.String
    }

    def run(self, execution):
        """
        执行节点逻辑。
        
        Args:
            execution: FlowExecution 实例，提供对上下文的访问。
        """
        # 1. 求值输入
        # 假设节点配置中有一个 'text' 字段，可能是表达式
        text_value = execution.evaluate(self.config.get('text'))
        
        # 2. 执行业务逻辑
        result = f"处理结果: {text_value}"
        
        # 3. 返回结果
        return result
```

**注意**：当前 `plaita` 中的 `Node` 实现可能需要特定的属性定义。请参考 `plaita/node/basic.py` 了解基类 `Node` 的确切签名。

### 2. 注册节点

在执行使用自定义节点的流程之前，必须先注册它。

```python
from plaita import node_register

node_register(MyCustomNode)
```

### 3. 在流程中使用

现在你可以在 JSON 定义中使用自定义节点了。

```json
{
    "nodes": [
        {
            "id": "node1",
            "type": "my_custom_node",
            "config": {
                "text": "你好，自定义节点"
            },
            "next": "end"
        }
    ]
}
```

## 完整示例

下面是一个完整的自定义节点示例，实现一个简单的文本转换节点：

```python
from plaita import Node, types, node_register, Flow

class TextTransformNode(Node):
    """文本转换节点"""
    
    node_type = "text_transform"
    node_name = "文本转换"
    
    # 配置 schema - 节点创建时的静态配置
    config = {
        "type": "object",
        "properties": {
            "operation": {
                "type": types.String,
                "description": "转换操作：upper, lower, title"
            }
        }
    }
    
    # 输入 schema - 每次执行时的动态输入
    input = {
        "type": "object",
        "properties": {
            "text": {
                "type": types.String,
                "description": "待转换的文本"
            }
        }
    }
    
    # 输出 schema
    output = {
        "type": types.String,
        "description": "转换后的文本"
    }
    
    def run(self, execution):
        # 获取配置
        operation = self.config.get('operation', 'upper')
        
        # 获取输入（支持表达式）
        text = execution.evaluate(self.config.get('text', ''))
        
        # 执行转换
        if operation == 'upper':
            return text.upper()
        elif operation == 'lower':
            return text.lower()
        elif operation == 'title':
            return text.title()
        else:
            return text

# 注册节点
node_register(TextTransformNode)

# 使用示例
flow_json = '''
{
    "id": "transform_flow",
    "inputType": {
        "type": "object",
        "properties": {
            "text": {"type": "string"}
        }
    },
    "nodes": [
        {"id": "start", "type": "start", "next": "transform"},
        {
            "id": "transform",
            "type": "text_transform",
            "config": {
                "operation": "upper",
                "text": "${INPUT.text}"
            },
            "next": "end"
        },
        {
            "id": "end",
            "type": "end",
            "response": {
                "type": "success",
                "value": "${NODE.transform}"
            }
        }
    ]
}
'''

flow = Flow.model_validate_json(flow_json)
result = flow.run(text="hello world")
print(result)  # 输出: HELLO WORLD
```

## 插件系统

`plaita` 自动打包标准节点。对于领域特定的逻辑，建议将你的节点打包为单独的 Python 库，并在应用启动时注册它们。

### 推荐的项目结构

```
my_plaita_nodes/
├── __init__.py
├── nodes/
│   ├── __init__.py
│   ├── custom_node_a.py
│   └── custom_node_b.py
└── setup.py
```

### 自动注册

在 `__init__.py` 中自动注册所有节点：

```python
# my_plaita_nodes/__init__.py
from plaita import node_register
from .nodes.custom_node_a import CustomNodeA
from .nodes.custom_node_b import CustomNodeB

def register_all():
    """注册所有自定义节点"""
    node_register(CustomNodeA)
    node_register(CustomNodeB)

# 导入时自动注册
register_all()
```

### 分发节点

使用 `plaita dist-node` 命令（如果已配置）或标准 PyPI 分发方法来分享你的自定义节点库。

```bash
# 发布节点到 Plaita 平台
plaita dist-node my_custom_node.py
```

## 节点生命周期

节点执行的生命周期如下：

1. **初始化**：节点从 JSON 定义创建
2. **验证**：检查配置和输入是否符合 schema
3. **执行**：调用 `run(execution)` 方法
4. **结果处理**：结果存储到 `$NODE` 上下文
5. **回调通知**：触发 `on_node_end` 回调

## 最佳实践

1. **明确定义 Schema**：始终定义 `config`、`input` 和 `output` schema，这有助于验证和文档生成。

2. **使用表达式求值**：通过 `execution.evaluate()` 处理可能包含表达式的值。

3. **错误处理**：在 `run` 方法中妥善处理异常，或依赖流程的错误处理机制。

4. **保持无状态**：节点应该是无状态的，所有状态都应存储在 `execution.context` 中。

5. **日志记录**：使用 `plaita.logger` 进行日志记录，以便与系统日志集成。

```python
from plaita.logger import logger

class MyNode(Node):
    def run(self, execution):
        logger.info(f"执行节点 {self.id}")
        # ...
```

