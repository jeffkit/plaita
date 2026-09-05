# HTTP 集成

用 `http` 节点调用外部 API，配合超时与错误策略做健壮集成。需安装 `http` extra：

```bash
pip install plaita[http]
```

## 流程定义

调用一个用户查询接口，把返回的 `name` 作为流程结果。

```json
{
    "flow_id": "fetch_user",
    "inputType": { "dataType": "object" },
    "nodes": [
        { "type": "start", "id": "start", "next": "fetch" },
        {
            "type": "http",
            "id": "fetch",
            "method": "GET",
            "url": "https://api.example.com/users/{% $INPUT.user_id %}",
            "headers": { "Accept": "application/json" },
            "timeout": "PT3S",
            "errorHandler": {
                "strategy": "continue_with",
                "defaultValue": { "data": { "name": "unknown" } },
                "retryTimes": 2,
                "code": -500,
                "message": "用户查询失败"
            },
            "next": "end"
        },
        {
            "type": "end",
            "id": "end",
            "output": "$NODE.fetch.data.name",
            "resultType": "success"
        }
    ]
}
```

## 执行

```python
from plaita import Flow

flow = Flow.from_string(open("fetch_user.json").read())
print(flow.run(user_id=42))  # => 用户名（失败时 => "unknown"）
```

## 响应结构

`http` 节点输出形如：

```json
{
    "data": { ... },          // 响应体
    "status": 200,
    "statusText": "OK",
    "headers": { ... }
}
```

用 `$NODE.fetch.data`、`$NODE.fetch.status` 引用。响应体若为 JSON 会自动解析。

## 超时与错误策略

- `timeout: "PT3S"` —— 节点级 3 秒超时
- `errorHandler.strategy: continue_with` —— 失败时用 `defaultValue` 作为节点输出，流程继续
- `errorHandler.retryTimes: 2` —— 失败前重试 2 次

若希望失败即中止，改用 `"strategy": "abort"`（默认），流程抛 `FlowExecutionException`。

## 带请求体的 POST

```json
{
    "type": "http",
    "id": "create_user",
    "method": "POST",
    "url": "https://api.example.com/users",
    "headers": { "Content-Type": "application/json" },
    "body": { "name": "$INPUT.name", "email": "$INPUT.email" },
    "timeout": "PT5S",
    "next": "end"
}
```

`body` 中可混合字面量与表达式，plaita 会递归求值。

## 用回调监控调用

```python
from plaita import FlowExecution, FlowCallback

class HttpTrace(FlowCallback):
    def on_node_end(self, flow, node, result, error, exception, **kwargs):
        if node.node_type == "http":
            print(f"http {node.id} -> status={result.get('status') if result else None} err={error}")

execution = FlowExecution(callback_handlers=[HttpTrace()])
print(execution.run_compatible(Flow.from_string(open("fetch_user.json").read()), False, user_id=42))
```

## 要点回顾

- `http` 节点需 `http` extra；`url` / `headers` / `body` 都支持表达式
- 用 `timeoutHandler` / `errorHandler` 控制超时与失败行为
- 响应通过 `$NODE.<id>.data` 引用

## 下一步

- [错误处理与超时](../guide/error-handling.md)
- [审批流](approval-flow.md) —— 进入分布式场景
