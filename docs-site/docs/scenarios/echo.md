# 回声流程

最小可运行示例：把输入原样返回。演示流程定义、本地执行、远程执行三种形态。

## 流程定义

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

存为 `echo.json`。

## 本地执行

```python
from plaita import Flow

flow = Flow.from_string(open("echo.json").read())
print(flow.run(name="kongjie"))  # => "kongjie"
```

## 加一层处理

在 `start` 和 `end` 之间插一个 `assignment` 节点做处理：

```json
{
    "flow_id": "greet",
    "inputType": { "dataType": "object" },
    "nodes": [
        { "type": "start", "id": "start", "next": "upper" },
        {
            "type": "assignment",
            "id": "upper",
            "output": "$F.concat('hello, ', $F.upper($INPUT.name))",
            "outputType": { "dataType": "string" },
            "next": "end"
        },
        { "type": "end", "id": "end", "output": "$NODE.upper", "resultType": "success" }
    ]
}
```

```python
flow = Flow.from_string(open("greet.json").read())
print(flow.run(name="kongjie"))  # => "hello, KONGJIE"
```

## 远程执行

若流程托管在远程 Plaita 服务端，可用 `PlaitaClient` 按流程 ID 执行：

```python
from plaita.client import PlaitaClient

client = PlaitaClient("secret_id", "secret_key", url="https://your-plaita-server/api/flowVersion/semver/detail")
print(client.run_flow("259", "0.0.2", {"name": "kongjie"}))
```

## 异步执行

```python
import asyncio
from plaita import Flow

async def main():
    flow = Flow.from_string(open("echo.json").read())
    print(await flow.arun(name="kongjie"))

asyncio.run(main())
```

## 要点回顾

- `inputType.dataType: object` + 关键字参数 → `$INPUT` 是 dict
- `end.output` 用 `$INPUT.name` 引用输入，`$NODE.upper` 引用上游节点结果
- `$F.concat` / `$F.upper` 是内置表达式函数

## 下一步

- [HTTP 集成](http-integration.md)
- [表达式](../guide/expressions.md)
