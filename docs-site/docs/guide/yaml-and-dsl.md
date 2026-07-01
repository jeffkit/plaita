# YAML 与 Python DSL 编排

除了 JSON，plaita 还支持两种更简洁的 flow 编写方式：

- **YAML**：去掉引号/逗号/花括号的噪音，支持注释，多行 `code` 字段天然友好。
- **Python Builder DSL**（`plaita.dsl`）：在代码里声明 flow，IDE 自动补全 + 构建期静态校验。

两者产出的都是同一个 `Flow` 对象，运行时行为与 JSON 完全一致——只是序列化/抽象层不同。

> 想要更接近真实代码的写法？参见 [类代码 DSL：S-expr 与 @flow](code-dsl.md)——S-表达式前端，以及用纯 Python 函数 + `@flow` 装饰器（AST 编译）的极致形态。

## YAML

YAML 是可选依赖：

```bash
pip install plaita[yaml]
```

`Flow.from_string` / `parse` / `parse_and_run` 都会自动识别 JSON 或 YAML；
`Flow.from_file(path)` 按文件后缀（`.json` / `.yaml` / `.yml`）选择解析器。

同一个「判断成年」flow，JSON 版：

```json
{
  "flow_id": "adult_check",
  "inputType": { "dataType": "object" },
  "nodes": [
    { "type": "start", "id": "start", "next": "check_age" },
    { "type": "if", "id": "check_age",
      "condition": { "field": "$INPUT.age", "operator": "gte", "value": 18 },
      "next": "end_adult", "else_next": "end_minor" },
    { "type": "end", "id": "end_adult", "output": "成年", "resultType": "success" },
    { "type": "end", "id": "end_minor", "output": "未成年", "resultType": "success" }
  ]
}
```

YAML 版：

```yaml
flow_id: adult_check
desc: 判断成年：输入 age，>=18 返回成年，否则未成年
inputType: { dataType: object }
nodes:
  - { type: start, id: start, next: check_age }
  - type: if
    id: check_age
    condition: { field: $INPUT.age, operator: gte, value: 18 }
    next: end_adult
    else_next: end_minor
  - { type: end, id: end_adult, output: 成年, resultType: success }
  - { type: end, id: end_minor, output: 未成年, resultType: success }
```

加载与执行：

```python
from plaita.flow import Flow

flow = Flow.from_file("flows/adult_check.yaml")
flow.run(age=20)  # -> "成年"
```

YAML 的额外收益：

- **注释**：JSON 不支持注释，YAML 用 `#` 写明每个节点意图，对业务可读性意义重大。
- **多行代码**：`code` 节点的 `code` 字段用 YAML `|` 块标量，不再需要 `\n` 转义。

  ```yaml
  - type: code
    id: transform
    language: python
    code: |
      def run(x):
          return x.upper()
    input: $INPUT.text
    next: end
  ```

## Python Builder DSL

`plaita.dsl` 适合在代码里声明 flow、写测试、生成模板。它带来两个核心价值：

1. **字段名即关键字参数**：拼写错误在调用期暴露，IDE 自动补全。
2. **构建期校验**：`next` 指向不存在的节点、`switch` 没有默认分支、节点 id 重复等反模式在 `build()` 时直接抛错。

### 基础：if 分支

```python
from plaita.dsl import build, start, end, if_, cond

flow = (
    build("adult_check", input_type="object", desc="判断成年")
    .add(start(next="check_age"))
    .add(if_(
        id="check_age",
        condition=cond("$INPUT.age", ">=", 18),
        next="end_adult",
        else_next="end_minor",
    ))
    .add(end("end_adult", output="成年"))
    .add(end("end_minor", output="未成年"))
    .build()
)
flow.run(age=20)  # -> "成年"
```

`cond` 的 `operator` 支持符号写法：`>=` / `==` / `!=` / `in` … 会被规范化成
`gte` / `eq` / `ne` / `in`；也直接接受 `gte` / `eq` 等规范名。
多条件用 `cond_group("and", [cond(...), cond(...)])`。

### 集合节点 + 子流程装饰器

`map` / `filter` / `find` / `loop` / `reduce` / `child` 的 `child_flow`
用 `@child_flow` 装饰器写成普通 Python 函数，缩进层级 = 逻辑层级：

```python
from plaita.dsl import build, start, end, map, child_flow

@child_flow(input_type="object")
def double_each(c):
    c.add(start(next="e"))
    c.add(end("e", output="$F.mul($INPUT.item, 2)"))

flow = (
    build("double_numbers", input_type="object")
    .add(start(next="double_all"))
    .add(map(id="double_all", collection="$INPUT.numbers",
             child_flow=double_each, next="end"))
    .add(end("end", output="$NODE.double_all"))
    .build()
)
flow.run(numbers=[1, 2, 3, 4])  # -> [2, 4, 6, 8]
```

`filter` / `find` 的子流程需要返回 bool，惯例是用 `if_` 分支到输出 `True`/`False`
的两个 `end`：

```python
@child_flow(input_type="object")
def is_even(c):
    c.add(start(next="check"))
    c.add(if_(id="check",
              condition=cond("$F.mod($INPUT.item, 2)", "==", 0),
              next="yes", else_next="no"))
    c.add(end("yes", output=True))
    c.add(end("no", output=False))
```

### switch / case

```python
from plaita.dsl import build, start, end, switch, case, branch

# switch：按 priority 匹配 condition，必须有 is_default 分支
flow = (
    build("router", input_type="object")
    .add(start(next="route"))
    .add(switch(id="route", branches=[
        branch(name="a", next="end_a", condition=cond("$INPUT.type", "==", "A")),
        branch(name="b", next="end_b", condition=cond("$INPUT.type", "==", "B")),
        branch(name="dft", next="end_dft", is_default=True),
    ]))
    .add(end("end_a", output="A"))
    .add(end("end_b", output="B"))
    .add(end("end_dft", output="other"))
    .build()
)

# case：等值匹配，每条用 next 指定跳转目标
flow2 = (
    build("case_router", input_type="object")
    .add(start(next="route"))
    .add(case(id="route", target="$INPUT.n", cases=[
        {"name": "one", "value": 1, "next": "end1"},
        {"name": "two", "value": 2, "next": "end2"},
    ], default="endd"))
    .add(end("end1", output="一"))
    .add(end("end2", output="二"))
    .add(end("endd", output="其它"))
    .build()
)
```

### http / 错误处理

```python
from plaita.dsl import build, start, end, http, error_handler

flow = (
    build("create_user", input_type="object")
    .add(start(next="call_api"))
    .add(http(
        id="call_api",
        method="POST",
        url="https://api.example.com/users",
        headers={"Content-Type": "application/json"},
        body={"name": "$INPUT.name"},
        timeout="PT5S",
        error_handler=error_handler("continue_with", default_value={"data": None}),
        next="end",
    ))
    .add(end("end", output="$NODE.call_api"))
    .build()
)
```

> `http` 节点需要 `pip install plaita[http]`，`code` 节点需要 `plaita[code]`。

### 序列化与互转

Builder 可一键导出 JSON / YAML，与文本格式双向互通：

```python
builder = build("adult_check", input_type="object").add(...).add(...)
builder.to_json()   # -> JSON 字符串
builder.to_yaml()   # -> YAML 字符串（需 plaita[yaml]）
builder.build()     # -> Flow 对象，可直接 .run()
```

### 隐式 next：`linear`

线性流程用 `plaita.dsl.linear` 可以**完全省掉 `next` 和非分支节点的 `id`**：节点按
声明顺序自动串接，分支用 `then`/`else_` 标签跳转，`id` 只在被分支引用或被
`$NODE.<id>` 引用时才显式给出。

```python
from plaita.dsl import linear, cond

flow = (
    linear("adult_check", input_type="object", desc="判断成年")
    .start()                                          # id 自动 _n1
    .if_(condition=cond("$INPUT.age", ">=", 18),
         then="adult", else_="minor")                 # id 自动 _n2
    .end("adult", output="成年")                       # 显式 id（分支目标）
    .end("minor", output="未成年")                     # 显式 id（分支目标）
    .build()
)
flow.run(age=20)  # -> "成年"
```

纯线性管道连 `id` 都不用写：

```python
flow = (
    linear("echo_upper", input_type="object")
    .start()                                         # _n1 -> _n2
    .assignment(output="$F.upper($INPUT.x)")         # _n2 -> _n3
    .end(output="$NODE._n2")                         # _n3 (terminal)
    .build()
)
flow.run(x="hi")  # -> "HI"
```

`if_` 的 `then` 也可省略——默认走下一个声明节点（「条件成立则继续」），只有
`else_` 需要显式给出：

```python
flow = (
    linear("guard", input_type="object")
    .start()
    .if_(condition=cond("$INPUT.ok", "==", True), else_="bad")  # then 默认 -> ok_end
    .end("ok_end", output="ok")
    .end("bad", output="bad")
    .build()
)
```

> `linear` 与 `build` 可以混用：`linear` 适合线性主干，`build` 适合需要精确控制
> 每个节点 id / `next` 的复杂拓扑。两者底层都是 `FlowBuilder`，`.build()` 都会
> 跑同一套构建期校验。

### 构建期校验

`build()` 会先跑 `validate()`，把常见反模式提前拦掉：

| 反模式 | 报错 |
|--------|------|
| 节点 `id` 重复 | `节点 id 重复: [...]` |
| `next` 指向不存在的节点 | `节点 'x' 的 next 指向不存在的节点 id 'y'` |
| `if` 的 `else_next` 指向不存在 | 同上（`else_next`） |
| `switch` 缺 `isDefault` 分支 | `switch 节点 'x' 缺少 isDefault 分支…` |

只想校验不构建：`builder.validate()`。

## 节点工厂速查

| 工厂 | 对应 JSON `type` | 关键参数 |
|------|------------------|----------|
| `start` | `start` | `next` |
| `end` | `end` | `output`, `result_type`, `error` |
| `assignment` | `assignment` | `output`, `next` |
| `if_` | `if` | `condition`, `next`, `else_next` |
| `switch` | `switch` | `branches`（用 `branch`） |
| `case` | `case` | `target`, `cases`, `default` |
| `loop` | `loop` | `collection`, `child_flow`, `condition` |
| `map` | `map` | `collection`, `child_flow`, `concurrent` |
| `filter` | `filter` | `collection`, `child_flow` |
| `find` | `find` | `collection`, `child_flow` |
| `reduce` | `reduce` | `collection`, `child_flow`, `initial` |
| `child` | `child` | `input`, `child_flow` |
| `reference` | `reference` | `input`, `child_flow` |
| `parallel` | `parallel` | `branches`（用 `parallel_branch`）, `mode`, `join_branches` |
| `code` | `code` | `language`, `code`, `input` |
| `http` | `http` | `method`, `url`, `body`, `timeout`, `error_handler` |
| `event` | `event` | `event_type`, `event_filter` |

## API 速查

| 入口 | 作用 |
|------|------|
| `plaita.flow.Flow.from_string(s)` | 从 JSON/YAML 字符串解析（自动识别） |
| `plaita.flow.Flow.from_file(path)` | 按后缀加载文件 |
| `plaita.flow.parse(content)` | 字符串/dict → `Flow` |
| `plaita.flow.parse_and_run(content, ...)` | 解析并执行 |
| `plaita.dsl.build(flow_id, ...)` | 创建顶层 `FlowBuilder`（显式 id/next） |
| `plaita.dsl.linear(flow_id, ...)` | 创建 `LinearBuilder`（隐式 next，id 按需） |
| `plaita.dsl.child_flow(input_type=...)` | 子流程装饰器 |
| `plaita.dsl.cond(field, op, value)` | 分支条件 |
| `plaita.dsl.cond_group(relation, conditions)` | 条件组 |
| `plaita.dsl.error_handler(strategy, ...)` | 错误处理策略 |
