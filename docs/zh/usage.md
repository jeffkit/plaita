# 使用指南

本指南介绍如何安装、配置和使用 `plaita` 执行流程。

## 安装

### 从 PyPI 安装

```bash
pip install plaita
```

### 从源码安装

```bash
git clone <repository_url>
cd plaita
python3 setup.py install
```

### 验证安装

```bash
python3 -m plaita
```

如果输出版本号，则安装成功。

## 定义流程

流程使用 JSON 格式定义。一个基本的流程包含 `input`、`output`、`nodes` 和 `links`（通常通过节点内的 `next` 属性隐式定义）。

### 示例：回声流程

```json
{
    "id": "echo_flow",
    "inputType": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "required": true
            }
        }
    },
    "nodes": [
        {
            "id": "start",
            "type": "start",
            "next": "end"
        },
        {
            "id": "end",
            "type": "end",
            "response": {
                "type": "success",
                "value": "${INPUT.name}"
            }
        }
    ]
}
```

## 执行流程

### 本地执行

你可以加载 JSON 文件并在 Python 代码中直接执行。

```python
import json
from plaita import Flow

# 加载流程定义
with open('echo.json', 'r') as f:
    content = f.read()

# 初始化 Flow 对象
flow = Flow.model_validate_json(content)

# 执行流程
result = flow.run(name="世界")
print(result)
# 输出: 世界
```

### 使用 Plaita 客户端（远程执行）

如果你部署了 Plaita 服务端，可以远程执行流程。`PlaitaClient` 的 `url` 默认指向本仓库
`plaita-console` 控制台提供的 `/api/flowVersion/semver/detail` 契约接口（本地部署
`http://localhost:8080/api/flowVersion/semver/detail`）；生产环境请通过 `url` 参数指向
你部署的控制台地址。

```python
from plaita.client import PlaitaClient

# 本地控制台部署时 url 可省略（默认即指向控制台契约接口）
client = PlaitaClient('your secret id', 'your secret key')

# 或显式指定远程地址
# client = PlaitaClient(
#     'your secret id',
#     'your secret key',
#     url='https://your-plaita-server/api/flowVersion/semver/detail',
# )

# 执行流程 ID 为 '259' 的流程
result = client.run_flow('259', '0.0.2', {"age": 14})
print(result)
```

## 高级功能

### 超时控制

你可以为整个流程或单个节点设置超时。

```json
{
    "id": "my_flow",
    "timeout": "PT5S",
    "nodes": [
        {
            "id": "slow_node",
            "type": "code",
            "timeout": "PT10S",
            "...": "..."
        }
    ]
}
```

超时格式遵循 ISO 8601 持续时间格式：
- `PT5S` - 5 秒
- `PT1M` - 1 分钟
- `PT1H` - 1 小时

### 错误处理

每个节点都可以配置错误处理策略：

```json
{
    "id": "risky_node",
    "type": "code",
    "errorHandler": {
        "strategy": "continue_with",
        "retryTimes": 3,
        "defaultValue": null,
        "errorCode": -500,
        "errorMessage": "节点执行失败"
    }
}
```

错误策略：
- `abort`：中止流程执行（默认）
- `continue`：忽略错误继续执行
- `continue_with`：使用默认值继续执行

### 调试模式

使用 `debug` 方法以调试模式运行流程，可以单步执行：

```python
import logging
logging.basicConfig(level=logging.INFO)

from plaita import Flow

flow = Flow.model_validate_json(content)

# 使用生成器模式调试
for step in flow.debug(name="test"):
    print(f"[{step['type']}] {step['id']}: {step['result']}")
    
    if step['is_end']:
        break
```

### 表达式语法

在流程定义中，可以使用表达式引用上下文数据：

| 表达式 | 说明 |
|--------|------|
| `${INPUT.name}` | 获取输入参数 `name` |
| `${NODE.nodeId.field}` | 获取节点 `nodeId` 的输出字段 |
| `${GLOBAL.key}` | 获取全局变量 |
| `${ENV.PATH}` | 获取环境变量 |

### 回调集成

你可以注册回调来监控流程执行：

```python
from plaita.flow import Flow, FlowCallback, FlowExecution

class MyCallback(FlowCallback):
    def on_flow_start(self, flow, **kwargs):
        print(f"流程开始: {flow.flow_id}")
    
    def on_node_start(self, flow, node, **kwargs):
        print(f"节点开始: {node.id}")
    
    def on_node_end(self, flow, node, result=None, error=None, **kwargs):
        print(f"节点结束: {node.id}, 结果: {result}")
    
    def on_flow_end(self, flow, result=None, error=None, **kwargs):
        print(f"流程结束: {flow.flow_id}, 结果: {result}")

# 使用回调执行
execution = FlowExecution(callback_handlers=[MyCallback()])
result = execution.run_compatible(flow, False, name="test")
```

