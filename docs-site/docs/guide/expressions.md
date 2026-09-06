# 表达式

表达式是 plaita 的"数据胶水"：在节点的 `output`、`condition`、`input` 等字段中引用上下文数据、调用内置函数。本页给出完整语法。

## 速查：最常用的 5 个写法

| 需求 | 写法 | 示例 |
|------|------|------|
| 引用输入字段 | `$INPUT.字段名` | `"$INPUT.name"` |
| 引用某节点的输出 | `$NODE.节点id` | `"$NODE.assign"` |
| 调用内置函数 | `$F.函数名(参数)` | `"$F.upper($INPUT.name)"` |
| 字符串中嵌入变量 | `{% $INPUT.字段名 %}` | `"你好，{% $INPUT.name %}"` |
| 引用环境变量 | `$ENV.变量名` | `"$ENV.API_BASE"`（须在 `exposeEnv` 白名单内） |

---

!!! warning "重要：表达式前缀是 `$`，不是 `${}`

    plaita 的变量引用写作 **`$INPUT.name`**，函数调用写作 **`$F.upper($INPUT.name)`**。
    不要写成 `${INPUT.name}`——那是错误的语法，会被当作普通字符串原样返回。

## 变量引用

用 `$` 前缀引用执行上下文中的命名空间：

| 表达式 | 含义 | 引用不存在的键时 |
|--------|------|------------------|
| `$INPUT` / `$INPUT.name` | 整个输入对象 / 其 `name` 字段 | `None`（DEBUG 日志留痕） |
| `$NODE.assign` / `$NODE.assign.field` | 节点 `assign` 的输出 / 其字段 | `None` |
| `$GLOBAL.key` | 全局上下文变量（含 `flow_id`） | `None` |
| `$FLOW_ID` | 当前流程的 `flow_id` | — |
| `$FLOW` | `$FLOW_ID` 的别名（0.5.x 起，历史写法 `$FLOW` 会 KeyError 崩） | — |
| `$PARENT.x` | 父流程上下文（子流程中可用） | `None` |
| `$ENV.key` | 环境变量（须在 `exposeEnv` 白名单内，见下文） | `None`（未声明时另有解析期 warning） |

缺省口径统一为"缺键返回 `None`"；唯一的例外是整个根不存在（如上下文尚未建立 `$NODE` 状态）时保留 `KeyError`——那通常意味着流程逻辑错误，静默反而危险。

支持的命名空间由 `ExecutionContext` 维护：`INPUT` / `NODE` / `GLOBAL` / `PARENT` / `ENV`。命名空间名本身可通过 `express_input_name` 等参数自定义。

!!! warning "$ENV 采用 allowlist 模型：不声明就读不到"

    0.4.0 起 `$ENV` 是**白名单（allowlist）模型**，且**不存在任何敏感前缀自动过滤**：

    - 默认情况下流程**一个环境变量都读不到**——`$ENV.XXX` 解析不到值。
    - Flow 必须显式声明 `exposeEnv`（JSON/YAML，Python 侧 `expose_env`）后才可读：

      ```json
      { "flow_id": "demo", "exposeEnv": ["HOME", "API_BASE"], "nodes": [ ... ] }
      ```

      ```python
      Flow(flow_id="demo", expose_env=["HOME", "API_BASE"], ...)
      ```

    - 未声明时，0.5.0 起解析期会 `logger.warning` 一次列出被引用的 key 名与修复指引（不报错，让沉默变可见）。
    - allowlist 命中即暴露，请自行确认列表里没有不该暴露的密钥。

    升级迁移与 0.4.0 的行为对照见 [迁移指南](../reference/migration-guide.md)。

### 数组索引

支持数字索引与 `[n]` 语法：

```
$NODE.list.0           # 列表第 0 项
$NODE.list[0]          # 等价写法
$NODE.list[-1]         # 末项
```

## 字符串插值

当表达式只是字符串的**一部分**时，用 `{% ... %}` 包裹表达式做插值：

```json
"output": "你好，{% $INPUT.name %}，今年 {% $INPUT.age %} 岁"
```

!!! tip "纯表达式 vs 插值"

    - 整个值就是表达式：`"output": "$INPUT.name"` —— 直接写 `$INPUT.name`
    - 表达式是字符串的一部分：`"output": "hi {% $INPUT.name %}"` —— 用 `{% %}`

## 函数调用

用 `$F.funcName(args)` 调用内置函数，参数本身也可以是表达式：

```json
"output": "$F.upper($INPUT.name)"
"output": "$F.concat($INPUT.first, '-', $INPUT.last)"
"output": "$F.len($NODE.items)"
```

### 内置函数一览

plaita 内置 60+ 函数，按类别分布在 `ExpressionRegistry` 中：

=== "数学 math"

    `add` `sub` `mul` `div` `mod` `pow` `abs` `ceil` `floor` `round` `trunc` `sqrt`

=== "字符串 string"

    `lower` `upper` `capitalize` `title` `strip` `lstrip` `rstrip` `replace` `split` `join` `startswith` `endswith` `concat` `isDigit`

=== "逻辑 logic"

    `and` `or` `not`

=== "数组 array"

    纯函数：`len` `length` `index` `slice` `append` `extend` `insert` `remove` `reverse` `sort` `getListItem` `addListItem` `insertListItem`

    带副作用（就地修改，非线程安全）：`pop` `delListItem` `setListItem`

=== "字典 dict"

    纯函数：`keys` `values` `items` `get` `getDictValue` `getDictKeys` `getDictValues`

    带副作用：`set` `delete` `clear` `setDictValue` `delDictValue` `clearDict`

=== "日期时间 datetime"

    `now` `today`（接受可选 `fmt` 参数）

=== "JSON"

    `json_loads` `json_dumps`

!!! warning "副作用函数非线程安全"

    `pop` / `set` / `delete` / `clear` 等会就地修改输入。在 `Parallel` 并行或共享上下文的异步场景中使用它们可能造成数据竞争——要么串行化访问，要么先拷贝再修改。

!!! warning "未注册的函数名求值为 'undefined' 字符串"

    `$F` 是作用域注册表：调用未注册（或拼写错）的函数名**不会报错**，而是把整
    个调用求值为字符串 `'undefined'`（默认 registry 未注册该名字时也一样）。例如
    `$F.uppr($INPUT.name)`（拼错）静默得到 `'undefined'`，下游拿到的就是这串文本。
    排查时先核对函数注册名是否与[内置函数一览](#内置函数一览)或你的
    `registry.register(...)` 名字完全一致。

## 自定义函数

通过 `ExpressionRegistry` 注册自定义函数，再传给 `ExpressionEvaluator`：

```python
from plaita.core.expression import (
    ExpressionEvaluator, ExpressionRegistry, FunctionCategory,
)

registry = ExpressionRegistry()
registry.register(
    "greet",
    lambda name: f"hello {name}",
    FunctionCategory.STRING,
    description="打招呼",
    override=True,
)

evaluator = ExpressionEvaluator(registry=registry)
```

之后在流程中即可用 `$F.greet($INPUT.name)`。

## 求值 API

在自定义节点里，用 `execution.evaluate(value)` 对任意值求值（递归处理 list/dict）：

```python
def execute(self, execution):
    text = execution.evaluate(self.output)  # 解析其中的表达式
    return text.upper()
```

!!! warning "子流程中的静默回退：`$INPUT.x` 解析为 `None` 时会回溯父上下文"

    当某表达式在当前上下文解析为 `None` 且存在父上下文时，plaita 会自动向上
    回溯到父级再求值。这是一个**跨流程边界的隐式数据通路**：子流程里写
    `$INPUT.name`，若子流程输入没有 `name`，实际可能拿到的是**父流程**的
    `name`。它让嵌套流程少写传参，但也意味着"字段不存在"与"来自父流程"
    在结果上无法区分——需要严格隔离时，给子流程输入显式传齐字段。

## 下一步

- [内置节点](../nodes/builtin.md) 看各节点如何使用 `output` / `condition`
- [API: plaita.core.expression](../api/expression.md) 查看 `ExpressionRegistry` 完整接口
