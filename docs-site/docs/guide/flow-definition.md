# JSON 流程定义

!!! tip "这是 JSON 格式的参考页"

    plaita 支持 JSON / YAML / Python Builder / S-expr / @flow 五种方式定义流程，本页只覆盖 **JSON 字段参考**。如果你还没选定格式，先看 [流程编写方式](flow-authoring.md) 做选型。

一个 `Flow` 是一段 JSON 描述的静态流程定义。本页列出全部字段、字段名兼容规则，以及如何用节点与分支描述控制流。

## Flow 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `flow_id` | `str?` | 流程唯一标识（推荐） |
| `version` | `str?` | 流程版本 |
| `runtime` | `str` | 运行时，固定 `"python"` |
| `input_type` | `Property?` | 输入类型声明 |
| `output_type` | `Property?` | 输出类型声明 |
| `nodes` | `Node[]?` | 节点列表 |
| `timeout` | `str?` | 流程级超时，ISO 8601 时长或毫秒数 |
| `global_context` | `dict?` | 全局上下文初始值（运行时可由 `$GLOBAL` 引用） |
| `metadata` | `dict?` | 元数据 |
| `author` / `desc` | `str?` | 作者 / 描述 |

## 字段名兼容

plaita 在解析时自动归一化常见驼峰字段，便于与可视化编排工具导出的流程互通：

| 平台字段 | 归一化到 |
|----------|---------|
| `id` / `flowId` | `flow_id`（`flow_id` 优先） |
| `inputType` | `input_type` |
| `outputType` | `output_type` |
| `globalContext` | `global_context` |

节点字段同样支持驼峰，例如 `timeoutHandler` / `errorHandler` / `retryTimes` / `defaultValue` / `resultType` / `upstreamOutput` / `childFlow` 等。

## 输入类型

`inputType.dataType` 决定 `$INPUT` 的形态与传参方式：

| dataType | `flow.run(...)` 传参 | `$INPUT` 形态 |
|----------|---------------------|--------------|
| `object` / `map` | 关键字参数 `flow.run(name="x")` 或位置首参（dict） | 关键字参数组成的 dict |
| `array` | 位置参数 `flow.run(a, b, c)` | 参数元组 |
| 其它（`string` / `number` …） | 位置首参 `flow.run("x")` | 该单值 |

## 节点与控制流 { #control-flow }

节点通过 `next` 串成线性流；分支节点（`branching=True`，如 `switch` / `if` / `case`）用 `branches` 描述多路跳转。

=== "线性流程"

    ```json
    {
        "flow_id": "linear",
        "inputType": { "dataType": "object" },
        "nodes": [
            { "type": "start", "id": "start", "next": "assign" },
            {
                "type": "assignment",
                "id": "assign",
                "output": "$F.upper($INPUT.name)",
                "outputType": { "dataType": "string" },
                "next": "end"
            },
            { "type": "end", "id": "end", "output": "$NODE.assign", "resultType": "success" }
        ]
    }
    ```

=== "分支流程（if）"

    ```json
    {
        "flow_id": "branch",
        "inputType": { "dataType": "object" },
        "nodes": [
            { "type": "start", "id": "start", "next": "check" },
            {
                "type": "if",
                "id": "check",
                "condition": {
                    "field": "$INPUT.age",
                    "operator": "gte",
                    "value": 18
                },
                "next": "adult",
                "else_next": "minor"
            },
            { "type": "end", "id": "adult", "output": "成年", "resultType": "success" },
            { "type": "end", "id": "minor", "output": "未成年", "resultType": "success" }
        ]
    }
    ```

    `if` 节点条件为真走 `next`，否则走 `else_next`。

=== "多路分支（switch）"

    ```json
    {
        "type": "switch",
        "id": "route",
        "branches": [
            {
                "name": "a",
                "next": "nodeA",
                "priority": 0,
                "condition": { "field": "$INPUT.type", "operator": "eq", "value": "A" }
            },
            {
                "name": "b",
                "next": "nodeB",
                "condition": { "field": "$INPUT.type", "operator": "eq", "value": "B" }
            },
            { "name": "default", "next": "nodeDefault", "isDefault": true }
        ]
    }
    ```

    `switch` 按 `priority` 降序匹配 `condition`，命中则跳转到对应 `next`；都未命中走 `isDefault` 分支。

## 条件与运算符 { #condition-operators }

分支条件（`Condition`）由 `field` / `operator` / `value` 三元组组成，`field` 与 `value` 都支持表达式。多个条件用 `ConditionGroup`（`relation: and|or`）组合。

| operator | 含义 |
|----------|------|
| `eq` / `ne` | 等于 / 不等于 |
| `gt` / `gte` / `lt` / `lte` | 大于 / 大于等于 / 小于 / 小于等于 |
| `in` / `notIn` | 属于 / 不属于 |
| `contains` / `notContains` | 包含 / 不包含 |

## 起点推断

若流程没有显式 `start` 节点，plaita 会自动找**入度为 0** 的节点（未被任何节点的 `next` 或 `branch.next` 引用）作为起点；若都没有，则取 `nodes[0]`。

## 解析入口

```python
from plaita import Flow, parse

# 任选其一
flow = Flow.from_string(json_str)
flow = Flow.model_validate_json(json_str)
flow = Flow.model_validate(dict_data)
flow = parse(json_str)        # str 或 dict 均可，会校验 runtime == "python"
```

## 下一步

- [内置节点](../nodes/builtin.md) —— 每种节点的字段与行为
- [表达式](expressions.md) —— 如何在 `output` / `condition` 中引用数据
- [执行模式](execution-modes.md)
