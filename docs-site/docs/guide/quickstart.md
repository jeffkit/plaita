# 快速开始

本页用一个最小可运行示例带你跑通 plaita：定义一个回声流程，本地执行，再演示远程执行。

## 1. 准备流程文件

plaita 的流程用 JSON 描述。下面这个 `echo.json` 实现回声：把输入的 `name` 原样返回。

```json
{
    "flow_id": "echo",
    "inputType": { "dataType": "object" },
    "nodes": [
        { "type": "start", "id": "start", "next": "end" },
        {
            "type": "end",
            "id": "end",
            "output": "$INPUT.name",
            "resultType": "success"
        }
    ]
}
```

- `flow_id`：流程唯一标识（旧字段 `id` / `flowId` 仍兼容，但推荐用 `flow_id`）
- `inputType`：输入类型声明，`dataType: object` 表示输入是一个对象
- `nodes`：节点列表，通过每个节点的 `next` 串成控制流
- `$INPUT.name`：表达式，引用输入参数 `name`（详见 [表达式](expressions.md)）

!!! note "流程从哪来"

    流程文件可以手写 JSON，也可以由可视化编排工具导出后使用。

## 2. 本地执行

```python
from plaita import Flow

with open("echo.json", "r") as f:
    flow = Flow.from_string(f.read())

result = flow.run(name="kongjie")
print(result)  # => "kongjie"
```

`Flow.from_string` 解析 JSON 字符串为 `Flow` 对象；`flow.run(...)` 以 Normal 模式同步执行，参数 `name="kongjie"` 成为流程输入 `$INPUT`。

等价的写法：

```python
from plaita import parse, parse_and_run

flow = parse(open("echo.json").read())          # 解析
print(flow.run(name="kongjie"))                 # 执行

# 一步到位：解析并执行
print(parse_and_run(open("echo.json").read(), name="kongjie"))
```

## 3. 异步执行

plaita 内核全异步。在异步上下文中用 `arun`：

```python
import asyncio
from plaita import Flow

async def main():
    flow = Flow.from_string(open("echo.json").read())
    result = await flow.arun(name="kongjie")
    print(result)  # => "kongjie"

asyncio.run(main())
```

## 4. 远程执行

如果你的流程托管在远程 Plaita 服务端，可以用 `PlaitaClient` 按流程 ID 远程执行：

```python
from plaita.client import PlaitaClient

client = PlaitaClient(
    "your secret id",
    "your secret key",
    url="https://your-plaita-server/api/flowVersion/semver/detail",
)

# 259 是流程 ID，0.0.2 是版本，第三个参数是流程输入
result = client.run_flow("259", "0.0.2", {"age": 14})
print(result)
```

## 下一步

- [流程定义](flow-definition.md) —— 完整字段与字段名兼容
- [表达式](expressions.md) —— `$INPUT` / `$NODE` / `$F.func(...)`
- [执行模式](execution-modes.md) —— Normal / Generator / Distributed
- [回声流程实战](../scenarios/echo.md)
