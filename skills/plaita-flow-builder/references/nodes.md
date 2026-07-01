# 内置节点参考

plaita 默认注册的全部内置节点。`type` 即流程 JSON 中 `type` 字段的值。

## 速查表

| type | 分支 | 异步 | 需 extra | 用途 |
|------|:----:|:----:|:--------|------|
| `start` | | | | 流程起点，无逻辑 |
| `end` | | | | 流程终点，返回结果或抛业务错误 |
| `assignment` | | | | 求值并产出值 |
| `switch` | ✓ | | | 按 `branches` 条件多路跳转 |
| `if` | ✓ | | | 二分支：真走 `next`，假走 `else_next` |
| `case` | ✓ | | | 按 `target` 等值匹配 `cases` |
| `loop` | | | | 对集合循环执行子流程，可带中止条件 |
| `map` | | | | 对集合映射，可并发 |
| `filter` | | | | 按子流程返回 bool 过滤集合 |
| `find` | | | | 返回首个子流程返回真的元素 |
| `reduce` | | | | 对集合逐项归约 |
| `child` | | | | 内联子流程，可共享父上下文 |
| `reference` | | | | 引用独立子流程（不共享父上下文） |
| `parallel` | | | | 多分支并行（thread/process/coroutine） |
| `code` | | | `code` | 执行 JS / Python 代码 |
| `http` | | | `http` | 发起 HTTP 请求 |
| `event` | | ✓ | | 等待事件，用于断点续执挂起 |

> `calculate` 与 `redis` 存在但**不在默认注册表**，需自行 `register`。

## start / end

`start` 无逻辑，仅作起点标识。

`end` 的 `resultType` 决定行为：

| resultType | 行为 |
|------------|------|
| `success` | 求值 `output` 作为流程结果返回 |
| `nop` | 返回 `None` |
| `error` | 抛 `FlowResultError(error.code, error.message)` |

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

### if（二分支）

```json
{
  "type": "if",
  "id": "check",
  "condition": { "field": "$INPUT.age", "operator": "gte", "value": 18 },
  "next": "adult",
  "else_next": "minor"
}
```

条件为真走 `next`，否则走 `else_next`。

### switch（多路条件）

```json
{
  "type": "switch",
  "id": "route",
  "branches": [
    { "name": "a", "next": "nodeA", "priority": 0, "condition": { "field": "$INPUT.type", "operator": "eq", "value": "A" } },
    { "name": "b", "next": "nodeB", "condition": { "field": "$INPUT.type", "operator": "eq", "value": "B" } },
    { "name": "default", "next": "nodeDefault", "isDefault": true }
  ]
}
```

按 `priority` 降序匹配 `condition`，命中跳转到对应 `next`；都未命中走 `isDefault` 分支。

### case（等值匹配）

按 `target` 求值结果与 `cases[].value` 等值匹配，命中走对应 `next`；可选 `default`。

## 条件与运算符

分支条件（`Condition`）由 `field` / `operator` / `value` 三元组组成，`field` 与 `value` 都支持表达式。多个条件用 `ConditionGroup`（`relation: and|or`）组合。

| operator | 含义 |
|----------|------|
| `eq` / `ne` | 等于 / 不等于 |
| `gt` / `gte` / `lt` / `lte` | 大于 / 大于等于 / 小于 / 小于等于 |
| `in` / `notIn` | 属于 / 不属于 |
| `contains` / `notContains` | 包含 / 不包含 |

## loop / map / filter / find / reduce

对**集合**操作，每个元素跑一个**子流程**（`childFlow`）。子流程输入为 `item` 与 `index`（`reduce` 为 `first`/`second`）。

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

> 并发中不要使用带副作用的表达式函数（`pop`/`set`/`clear`），也不要在共享上下文写竞争数据。

## child / reference

两者都执行子流程，区别在于**上下文共享**：

| 节点 | 父上下文 | 类比 |
|------|---------|------|
| `child` (InlineFlow) | 子流程可引用父级及更高级数据 | 匿名函数闭包 |
| `reference` (ReferenceFlow) | 不使用父上下文，由上层调度器注入 | 调用独立函数 |

```json
{
  "type": "child",
  "id": "sub",
  "input": "$INPUT.payload",
  "childFlow": { "inputType": { "dataType": "object" }, "nodes": [ "..." ] },
  "next": "end"
}
```

## parallel

多分支并行执行，`mode` 选择执行方式：`thread`（默认）/ `process` / `coroutine`。`joinBranches` 列出的分支汇聚结果（`{branchName: result}`），其余 fire-and-forget。`isConditional` 为真时按 `branches[].condition` 过滤要执行的分支。

```json
{
  "type": "parallel",
  "id": "fanout",
  "mode": "thread",
  "joinBranches": ["a", "b"],
  "branches": [
    { "name": "a", "flow": { "nodes": [ "..." ] }, "input": "$INPUT.x" },
    { "name": "b", "flow": { "nodes": [ "..." ] }, "input": "$INPUT.y" }
  ],
  "next": "end"
}
```

> `mode: "process"` 使用进程池，分支 flow 与节点必须可 pickle。

## code

执行用户代码，需 `code` extra（JS 用 PyExecJS）。`language` 为 `js` 或 `python`，代码需定义 `run` 函数，`input` 作为参数传入。

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

发起 HTTP 请求，需 `http` extra（requests + aiohttp）。响应可通过 `$NODE.http_id.data` / `.status` / `.headers` 引用。

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

事件节点，用于断点续执。`async_node=True`，在 Distributed 模式下执行后挂起，等待外部事件到达恢复。

```json
{
  "type": "event",
  "id": "wait_approval",
  "eventType": "approval.{{ $INPUT.flow_id }}",
  "eventFilter": { "request_id": "$INPUT.request_id" }
}
```

`on_event` / `on_timeout` / `on_cancel` / `on_error` 分别处理恢复/超时/取消/错误情况。

## 公共字段

所有节点都可配：

| 字段 | 说明 |
|------|------|
| `id` | 节点唯一标识 |
| `type` | 节点类型 |
| `next` | 下一节点 id |
| `timeout` | 节点级超时，ISO 8601 |
| `errorHandler` | 错误处理策略，含 `strategy`/`retryTimes`/`defaultValue`/`errorCode`/`errorMessage` |

错误策略：
- `abort`：中止流程执行（默认）
- `continue`：忽略错误继续执行
- `continue_with`：使用 `defaultValue` 继续执行

## Flow 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `flow_id` | `str?` | 流程唯一标识（推荐） |
| `version` | `str?` | 流程版本 |
| `runtime` | `str` | 运行时，固定 `"python"` |
| `input_type` | `Property?` | 输入类型声明 |
| `output_type` | `Property?` | 输出类型声明 |
| `nodes` | `Node[]?` | 节点列表 |
| `timeout` | `str?` | 流程级超时 |
| `global_context` | `dict?` | 全局上下文初始值（`$GLOBAL` 引用） |
| `metadata` | `dict?` | 元数据 |
| `author` / `desc` | `str?` | 作者 / 描述 |

字段名兼容：`id`/`flowId` → `flow_id`；`inputType` → `input_type`；`outputType` → `output_type`；`globalContext` → `global_context`。节点字段同样支持驼峰。

## 起点推断

若流程没有显式 `start` 节点，plaita 自动找**入度为 0** 的节点作为起点；若都没有，取 `nodes[0]`。
