Plaita —— 通用逻辑编排系统。 本项目是Plaita的Python运行时。

# 使用指南

## 安装

前置依赖：要求系统已安装python版本>=3.6，推荐3.10。

### pip安装plaita

```shell
$pip install plaita
```

验证安装成功与否，执行以下命令，如果输出版本号，则安装成功。

```shell
$python3 -m plaita
```

### 源码安装
    
```shell
$git clone
$cd plaita
$python3 setup.py install
```

## 准备流程文件

有两种方式：
- 通过 `plaita-console` 控制台可视化编排（流程编排页拖拽节点 → 保存版本 → 发布），完成后记录流程 ID 与版本号。控制台同时提供 `/api/flowVersion/semver/detail` 契约接口供 `PlaitaClient` 拉取。
- 手写一份流程文件。

示范的流程文件内容如下，实现一个回声功能，把输入的内容原样返回输出：

```json
{
    "flow_id": "echo",
    "inputType": { "dataType": "object" },
    "nodes": [
        {
            "type": "start",
            "id": "start",
            "next": "end"
        },
        {
            "type": "end",
            "id": "end",
            "output": "$INPUT.name",
            "resultType": "success"
        }
    ]
}

```
echo.json

## 执行流程

根据流程文件的提供方式，我们提供了本地及远程流程的执行方法：

### 执行本地流程

```python
from plaita import Flow

with open('echo.json', 'r') as f:
    flow = Flow.from_string(f.read())   # 或 from plaita import parse; flow = parse(f.read())
    result = flow.run(name="kongjie")
    print(result)                       # => "kongjie"
```

### 执行远程流程

若流程托管在远程 Plaita 服务端，可用 `PlaitaClient` 按流程 ID 远程执行。`url` 默认指向本仓库
`plaita-console` 控制台的 `/api/flowVersion/semver/detail` 契约接口（本地部署可省略）：

```python
from plaita.client import PlaitaClient

# 本地控制台部署时 url 可省略
client = PlaitaClient('your secret id', 'your secret key')
# 或显式指定：
# client = PlaitaClient('your secret id', 'your secret key',
#                     url='https://your-plaita-server/api/flowVersion/semver/detail')
result = client.run_flow('259', '0.0.2', {"age": 14})  # 259是流程ID，0.0.2是流程版本，{"age": 14}是流程输入参数
print(result)
```

## 扩展节点

当Plaita内置节点不能满足你的需求时，可以编写自定义节点。

### 编写自定义节点代码
```python
from plaita import Node

class MyNode(Node):
    node_type = "myNode"   # 流程 JSON 里 "type" 字段对应的值
    node_name = "我的节点"  # 展示名

    def execute(self, execution=None):
        # 节点业务逻辑
        # execution 提供上下文/输入访问；返回值即节点输出
        return {"result": "ok"}
```

基类 `Node` 封装了运行时校验逻辑，子类实现 `execute(self, execution=None)` 即可。`node_type` / `node_name` 为类变量，分别对应流程定义中的节点类型与展示名称。

### 注册自定义节点

如果逻辑里使用了自定义节点，那在执行逻辑前，需要先把节点注册到系统中，再执行流程。

```python
from plaita import Flow, NodeRegistry

# 方式一（推荐）：使用独立的 NodeRegistry
registry = NodeRegistry()
registry.register(MyNode)

# 方式二（兼容旧用法，会触发 DeprecationWarning）：
# from plaita import node_register
# node_register(MyNode)

# 注册完再执行流程，确保流程中使用的自定义节点有效。
flow = Flow.from_string('....')
result = flow.run()
print(result)
```

### 推送自定义节点到平台

为了在远程平台上使用自定义节点，开发者可以到对应平台的节点管理界面手动添加自定义节点。

> 说明：本运行时仓库不再内置 `plaita dist-node` 命令行工具，不再提供节点分发能力。

### 自定义配置节点表单

Plaita根据自定义节点中的input, config, output字段的内容，自动生成的节点配置表单。

如果自动生成的节点不能满足需求，开发者可以自行开发一个表单组件，嵌入到Plaita的在线编排系统中。

# 开发指南

## 发布新版本至 PyPI

使用 `build` + `twine` 发布到官方 PyPI：

```shell
$python -m build
$python -m twine upload dist/*
```

首次发布需在 [PyPI](https://pypi.org/account/register/) 注册账号并配置 API Token，
通过 `~/.pypirc` 或环境变量 `TWINE_USERNAME` / `TWINE_PASSWORD` 提供凭据。
