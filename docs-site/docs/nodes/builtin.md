# 内置节点

下表列出 plaita **默认注册表**中的内置节点，共 **23** 种：18 种随 `plaita.node` 内置（`_BUILTIN_NODES`），另 5 种扩展节点经 `plaita.nodes` entry_points 随包自动注册（无需额外安装）。`node_type` 即流程 JSON 中 `type` 字段的值。

| type | 类 | 展示名 | 分支 | 异步 | 需 extra | 用途 |
|------|------|--------|:----:|:----:|:--------|------|
| `start` | `Start` | 开始 | | | | 流程起点，无逻辑 |
| `end` | `End` | 结束 | | | | 流程终点，返回结果或抛业务错误 |
| `assignment` | `Assignment` | 赋值 | | | | 求值并产出值，支持上游输出选择 |
| `switch` | `Switch` | 判断 | ✓ | | | 按 `branches` 条件多路跳转 |
| `if` | `Bool` | IF | ✓ | | | 二分支：条件真走 `next`，假走 `else_next` |
| `case` | `SwitchLegacy` | CASE | ✓ | | | 按 `target` 等值匹配 `cases` |
| `loop` | `Loop` | 重复 | | | | 对集合循环执行子流程，可带中止条件 |
| `while` | `While` | 条件循环 | | | | 按条件反复执行子流程，带 `max_iterations` 上限保护 |
| `map` | `Map` | 映射 | | | | 对集合映射，可并发 |
| `filter` | `Filter` | 过滤 | | | | 按子流程返回 bool 过滤集合 |
| `find` | `Find` | 查找 | | | | 返回首个子流程返回真的元素 |
| `reduce` | `Reduce` | 归纳 | | | | 对集合逐项归约 |
| `child` | `InlineFlow` | 内联子逻辑 | | | | 内联子流程，可共享父上下文 |
| `reference` | `ReferenceFlow` | 引用逻辑 | | | | 引用独立子流程（不共享父上下文） |
| `parallel` | `Parallel` | 并行 | | | | 多分支并行执行（thread/process） |
| `http` | `HTTP` | http | | | `http` | 发起 HTTP 请求 |
| `event` | `EventNode` | 事件节点 | | ✓ | | 等待事件，用于断点续执挂起 |
| `mock` | `Mock` | 数据固定 | | | | 占位节点，原样返回 `value` 字段（试跑固定下游输入用） |
| `delay` | `DelayNode` | 延迟节点 | | | | 延迟指定时间后触发事件继续流程 |
| `approval` | `ApprovalNode` | 审批节点 | | | | 发起人工审批，等待审批决策后继续 |
| `redis_queue` | `RedisQueueNode` | Redis队列节点 | | | | 与 Redis 队列交互（入队/出队触发） |
| `kafka_queue` | `KafkaQueueNode` | Kafka队列节点 | | | | 与 Kafka 队列交互 |
| `http_callback` | `HttpCallbackNode` | HTTP回调节点 | | | `http` | 发起 HTTP 回调并等待响应 |

!!! note "扩展节点的运行前提"

    `delay` / `approval` / `redis_queue` / `kafka_queue` / `http_callback` 这 5 种扩展节点经 entry_points 自动注册、可直接解析；但它们是**事件驱动**的——实际执行需相应基础设施（Redis / Kafka / 审批与回调服务），通常配合 `server` extra 的[外延服务](../distributed/services.md)使用。详见[扩展节点](../distributed/extended-nodes.md)。

!!! note "未默认注册的节点"

    - **`code`（`CodeNode`）**：0.5.0 起移出默认注册表。使用前须显式：

      ```python
      from plaita.node import register_code_node
      register_code_node()  # 默认 docker 沙箱；无 Docker 时传 default_backend="subprocess"|"unsafe"|"restricted"
      ```

    - **`calculate`** / **`redis`**（需 `redis` extra）：存在于源码但不在 `_BUILTIN_NODES`，自行 `register`。

## start / end

`start` 无逻辑，仅作为起点标识。

`end` 的 `resultType` 决定行为（**不写时默认 `success`**；写了无法识别的值会打 `logger.warning` 并按 `success` 处理，不会静默返回 `None`）：

| resultType | 行为 |
|------------|------|
| `success`（默认） | 求值 `output` 作为流程结果返回 |
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

`upstreamOutput` 用于多上游汇聚时按 `upstream`（上一个节点 id）挑选对应输出，
类型是**列表**，每项形如 `{"upstream": "<上游节点id>", "output": "<表达式>"}`：

```json
{
  "type": "assignment", "id": "merge",
  "upstreamOutput": [
    {"upstream": "fetch_user",  "output": "$NODE.fetch_user"},
    {"upstream": "fetch_order", "output": "$NODE.fetch_order"}
  ],
  "next": "e"
}
```

> 多分支汇总裁决的更简惯用法：`End` 节点的 `output` 直接写对象字面量聚合各分支
> 结果，例如 `"output": {"user": "$NODE.fetch_user", "order": "$NODE.fetch_order"}`
> ——未实际执行的分支其节点结果解析为 `None`，不需要 `upstreamOutput` 逐项挑选。

## switch / if / case

详见 [流程定义 - 节点与控制流](../guide/flow-definition.md#control-flow) 的分支示例。

- `switch`：`branches[].condition` + `priority` + `isDefault`，按优先级匹配
- `if`：`condition` + `next`（真）+ `else_next`（假）
- `case`：`target` + `cases[]`（等值匹配）+ `default`

条件 `operator` 见 [流程定义 - 条件与运算符](../guide/flow-definition.md#condition-operators)。

## loop / while / map / filter / find / reduce

`loop` / `map` / `filter` / `find` / `reduce` 这类节点对**集合**操作，每个元素跑一个**子流程**（`childFlow`）。子流程输入为 `item` 与 `index`（`reduce` 为 `first`/`second`）。

| 节点 | 子流程输入 | 子流程输出 | 节点输出 |
|------|-----------|-----------|---------|
| `loop` | `item`, `index` | 任意 | 最后一次结果（满足 `condition` 时中止） |
| `while` | `item`, `index` | 任意 | 最后一轮结果（条件不满足即退出；未执行任何轮次时为 `None`） |
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

## while

条件循环节点：以 `condition` 为**继续条件**反复执行子流程——条件满足则进入下一轮，不满足即退出。`max_iterations`（默认 1000，JSON 亦可写驼峰 `maxIterations`）是迭代上限保护，达到上限强制停止并打 `logger.warning`。子流程输入为 `item`（上一轮结果，首轮为 `None`）与 `index`（轮次，从 0 起）；`condition` 上下文可引用 `$LOOP-ITEM` / `$LOOP-INDEX`。节点输出为最后一轮子流程结果。

```json
{
    "type": "while",
    "id": "retry_until_three",
    "condition": { "field": "$LOOP-INDEX", "operator": "lt", "value": 3 },
    "maxIterations": 100,
    "childFlow": {
        "inputType": { "dataType": "object" },
        "nodes": [
            { "type": "start", "id": "s", "next": "e" },
            { "type": "end", "id": "e", "output": "$INPUT.index" }
        ]
    },
    "next": "end"
}
```

`condition` 的字段结构同分支条件（`field` / `operator` / `value`，operator 取值见[条件与运算符](../guide/flow-definition.md#condition-operators)）。`condition` 缺省时不循环（仅执行一轮）。

## mock

数据固定（pin）占位节点：原样返回 `value` 字段作为节点输出。主要用于编排台调试——把某次试运行的节点输出「固定」下来，后续试跑直接以固定值作为该节点结果，从而跳过真实执行（如 HTTP 调用）反复调试下游；也可手工充当测试桩。

```json
{
    "type": "mock",
    "id": "stub_user",
    "value": { "name": "alice", "age": 30 },
    "next": "end"
}
```

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

多分支并行执行，`mode` 选择执行方式：`thread`（默认）/ `process`（`coroutine` 模式已于 0.4.0 下线）。`joinBranches` 列出的分支会汇聚结果（`{branchName: result}`），其余作为后台分支 fire-and-forget。`isConditional` 为真时按 `branches[].condition` 过滤要执行的分支。

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
    "event_type": "$INPUT.approval_topic",
    "event_filter": { "request_id": "req-42" }
}
```

字段说明：

- 字段名为 **`event_type` / `event_filter`**（无驼峰归一化，JSON/YAML 里必须写小写下划线形式）。
- 字段值**不支持 `{{ }}` 模板插值**；`event_type` 支持 `$` 前缀表达式——以 `$` 开头时执行期对上下文整体求值（上例在运行时解析为 `$INPUT.approval_topic` 的值，如 `"approval.leave"`）。
- `event_filter` 为字面量字典等值匹配。

`on_event` / `on_timeout` / `on_cancel` / `on_error` 分别处理恢复/超时/取消/错误情况。

## 下一步

- [自定义节点](custom.md)
- [API: plaita.node](../api/node.md)
