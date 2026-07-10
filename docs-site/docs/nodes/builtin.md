# 内置节点

下表列出 plaita **默认注册**的内置节点（见 `plaita.node.__init__._BUILTIN_NODES`，共 16 种）。`node_type` 即流程 JSON 中 `type` 字段的值。

| type | 类 | 展示名 | 分支 | 异步 | 需 extra | 用途 |
|------|------|--------|:----:|:----:|:--------|------|
| `start` | `Start` | 开始 | | | | 流程起点，无逻辑 |
| `end` | `End` | 结束 | | | | 流程终点，返回结果或抛业务错误 |
| `assignment` | `Assignment` | 赋值 | | | | 求值并产出值，支持上游输出选择 |
| `switch` | `Switch` | 判断 | ✓ | | | 按 `branches` 条件多路跳转 |
| `if` | `Bool` | IF | ✓ | | | 二分支：条件真走 `next`，假走 `else_next` |
| `case` | `SwitchLegacy` | CASE | ✓ | | | 按 `target` 等值匹配 `cases` |
| `loop` | `Loop` | 重复 | | | | 对集合循环执行子流程，可带中止条件 |
| `map` | `Map` | 映射 | | | | 对集合映射，可并发 |
| `filter` | `Filter` | 过滤 | | | | 按子流程返回 bool 过滤集合 |
| `find` | `Find` | 查找 | | | | 返回首个子流程返回真的元素 |
| `reduce` | `Reduce` | 归纳 | | | | 对集合逐项归约 |
| `child` | `InlineFlow` | 内联子逻辑 | | | | 内联子流程，可共享父上下文 |
| `reference` | `ReferenceFlow` | 引用逻辑 | | | | 引用独立子流程（不共享父上下文） |
| `parallel` | `Parallel` | 并行 | | | | 多分支并行执行（thread/process/coroutine） |
| `http` | `HTTP` | http | | | `http` | 发起 HTTP 请求 |
| `event` | `EventNode` | 事件节点 | | ✓ | | 等待事件，用于断点续执挂起 |

!!! note "未默认注册的节点"

    - **`code`（`CodeNode`）**：0.5.0 起移出默认注册表。使用前须显式：

      ```python
      from plaita.node import register_code_node
      register_code_node()  # 默认 docker 沙箱；无 Docker 时传 default_backend="subprocess"|"unsafe"|"restricted"
      ```

    - **`calculate`** / **`redis`**（需 `redis` extra）：存在于源码但不在 `_BUILTIN_NODES`，自行 `register`。

## start / end

`start` 无逻辑，仅作为起点标识。

`end` 的 `resultType` 决定行为：

| resultType | 行为 |
|------------|------|
| `success` | 求值 `output` 作为流程结果返回 |
| `nop` | 返回 `None` |
| `error` | 抛 `FlowResultError(error.code, error.message)`，透传业务错误码 |

```json
{ "type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success" }
{ "type": "end", "id": "bad", "resultType": "error", "error": { "code": 4001, "message": "参数非法" } }
```

## assignment

求值 `output`（或从 `upstreamOutput` 选择上游输出）作为节点结果。

```json
{
    "type": "assignment",
    "id": "greet",
    "output": "$F.concat('hi ', $INPUT.name)",
    "outputType": { "dataType": "string" },
    "next": "end"
}
```

`upstreamOutput` 用于多上游汇聚时按 `upstream`（上一个节点 id）挑选对应输出。

## switch / if / case

详见 [流程定义 - 节点与控制流](../guide/flow-definition.md#control-flow) 的分支示例。

- `switch`：`branches[].condition` + `priority` + `isDefault`，按优先级匹配
- `if`：`condition` + `next`（真）+ `else_next`（假）
- `case`：`target` + `cases[]`（等值匹配）+ `default`

条件 `operator` 见 [流程定义 - 条件与运算符](../guide/flow-definition.md#condition-operators)。

## loop / map / filter / find / reduce

这类节点对**集合**操作，每个元素跑一个**子流程**（`childFlow`）。子流程输入为 `item` 与 `index`（`reduce` 为 `first`/`second`）。

| 节点 | 子流程输入 | 子流程输出 | 节点输出 |
|------|-----------|-----------|---------|
| `loop` | `item`, `index` | 任意 | 最后一次结果（满足 `condition` 时中止） |
| `map` | `item`, `index` | 任意 | 结果列表（`concurrent: true` 可并发） |
| `filter` | `item`, `index` | bool | 命中元素组成的子集 |
| `find` | `item`, `index` | bool | 首个命中元素 |
| `reduce` | `first`, `second` | 同元素类型 | 归约结果（可带 `initial`） |

```json
{
    "type": "map",
    "id": "double_all",
    "collection": "$INPUT.numbers",
    "concurrent": true,
    "childFlow": {
        "inputType": { "dataType": "object" },
        "nodes": [
            { "type": "start", "id": "s", "next": "e" },
            { "type": "end", "id": "e", "output": "$F.mul($INPUT.item, 2)", "resultType": "success" }
        ]
    },
    "next": "end"
}
```

!!! warning "并发与副作用"

    `map` 的 `concurrent` 与 `parallel` 节点会并发执行。并发中不要使用带副作用的表达式函数（`pop`/`set`/`clear` 等），也不要在共享上下文上写竞争数据。

## child / reference

两者都执行子流程，区别在于**上下文共享**：

| 节点 | 父上下文 | 类比 |
|------|---------|------|
| `child` (InlineFlow) | 子流程可引用父级及更高级数据 | 匿名函数闭包 |
| `reference` (ReferenceFlow) | 不使用父上下文，子流程由上层调度器注入 | 调用独立函数 |

```json
{
    "type": "child",
    "id": "sub",
    "input": "$INPUT.payload",
    "childFlow": { "inputType": { "dataType": "object" }, "nodes": [ ... ] },
    "next": "end"
}
```

## parallel

多分支并行执行，`mode` 选择执行方式：`thread`（默认）/ `process` / `coroutine`。`joinBranches` 列出的分支会汇聚结果（`{branchName: result}`），其余作为后台分支 fire-and-forget。`isConditional` 为真时按 `branches[].condition` 过滤要执行的分支。

```json
{
    "type": "parallel",
    "id": "fanout",
    "mode": "thread",
    "joinBranches": ["a", "b"],
    "branches": [
        { "name": "a", "flow": { "nodes": [ ... ] }, "input": "$INPUT.x" },
        { "name": "b", "flow": { "nodes": [ ... ] }, "input": "$INPUT.y" }
    ],
    "next": "end"
}
```

!!! note "process 模式的可序列化要求"

    `mode: "process"` 使用进程池，分支 flow 与节点必须可 pickle。`threading.Event` 等不可序列化对象无法跨进程。

## code

执行用户代码（需 `code` extra 才能跑 JS；Python 走沙箱后端）。**0.5.0 起不在默认注册表**，使用前须：

```python
from plaita.node import register_code_node
register_code_node()  # 默认 docker；无 Docker 时显式传 default_backend
```

`language` 为 `js` 或 `python`，代码需定义一个 `run` 函数，`input` 作为参数传入。

```json
{
    "type": "code",
    "id": "transform",
    "language": "python",
    "code": "def run(x):\n    return x.upper()",
    "input": "$INPUT.text",
    "next": "end"
}
```

## http

发起 HTTP 请求，需 `http` extra（requests + aiohttp）。支持表达式寻址、headers、body、代理等。响应可通过 `$NODE.http_id.data` / `.status` / `.headers` 引用。

```json
{
    "type": "http",
    "id": "call_api",
    "method": "POST",
    "url": "https://api.example.com/users",
    "headers": { "Content-Type": "application/json" },
    "body": { "name": "$INPUT.name" },
    "timeout": "PT5S",
    "errorHandler": { "strategy": "continue_with", "defaultValue": { "data": null } },
    "next": "end"
}
```

## event

事件节点，用于断点续执。`async_node=True`，在 Distributed 模式下执行后挂起，等待外部事件到达恢复。详见 [断点续执](../distributed/checkpoint.md)。

```json
{
    "type": "event",
    "id": "wait_approval",
    "eventType": "approval.{{ $INPUT.flow_id }}",
    "eventFilter": { "request_id": "$INPUT.request_id" }
}
```

`on_event` / `on_timeout` / `on_cancel` / `on_error` 分别处理恢复/超时/取消/错误情况。

## 下一步

- [自定义节点](custom.md)
- [API: plaita.node](../api/node.md)
