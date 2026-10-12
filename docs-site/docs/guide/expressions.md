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
| `$NODE.raw.<节点>[.<路径>…]` | 节点结果 / 字段的**原文**——字符串不重新求值（见下文） | `None` |
| `$GLOBAL.key` | 全局上下文变量（含 `flow_id`） | `None` |
| `$FLOW_ID` | 当前流程的 `flow_id` | — |
| `$FLOW` | `$FLOW_ID` 的别名（0.5.x 起，历史写法 `$FLOW` 会 KeyError 崩） | — |
| `$PARENT.x` | 父流程上下文（子流程中可用） | `None` |
| `$ENV.key` | 环境变量（须在 `exposeEnv` 白名单内，见下文） | `None`（未声明时另有解析期 warning） |

缺省口径统一为"缺键返回 `None`"；例外有二：整个根不存在（如上下文尚未建立 `$NODE` 状态）时保留 `KeyError`——那通常意味着流程逻辑错误，静默反而危险；字段名以 `_` 开头时按下文「属性遍历与下划线名」抛 `ValueError`。

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

### 属性遍历与下划线名（安全限制）

字段段读的是**数据**：`dict` / 映射按**键**取，其它对象按**属性**取。下划线名在
**属性读取**上被拒绝，并且是**硬报错**（不是返回 `None`）：

| 表达式 | 结果 |
|--------|------|
| `$NODE.obj.text` | `"hello"`（公开属性） |
| `$NODE.obj.__class__` | **`ValueError`**：`'__class__'` 不是允许的字段名 |
| `$NODE.obj._private` | **`ValueError`** |
| `$NODE.d._foo`（`d` 是 dict） | 键 `_foo` 的值——**键**不受限 |
| `$NODE._n2`（自动生成的节点 id） | 该节点结果——**键**不受限 |
| `$NODE.d.__class__`（`d` 是 dict） | **`ValueError`**（dunder **路径段**同样拒绝） |
| `$F.get($NODE.d, '__class__')` | 该键的**数据值**——显式映射辅助不受限（见下） |

规则：

* **属性读取**：任何 `_` 开头的段名一律拒绝；
* **路径段取值**：单下划线键保持可读（`{"_foo": …}`、builder 自动生成的 `_n2` 节点 id）；
  `__` 开头的**路径段**（`$NODE.d.__class__`）拒绝——路径形态无法与属性遍历尝试区分，
  静默 `None` 只会掩盖越权尝试；
* 拒绝是**硬错误**：抛 `ValueError`（消息点名该段并说明原因），在 `end` 节点的
  `output` 等路径上表现为节点错误。不要指望"被挡住 → 得到 `None`"。

**刻意的不对称（映射辅助不受限）**：`$NODE.d.__class__` 抛错，但
`$F.get($NODE.d, '__class__')` / `$F.getDictValue($NODE.d, '__class__')` 仍返回该键的
**数据值**，`$F.keys($NODE.d)` 也仍列出它。映射辅助只是"按键取数据"——键不是属性、
不产生反射语义；路径形态的拒绝针对的是与属性遍历无法区分、且会静默 `None` 的写法。

原因：`$NODE.<节点>.__class__.__init__.__globals__.os.environ` 这类链在节点返回
dataclass / pydantic / 普通实例时能读出执行进程的全部环境变量，绕开 `$ENV` 的
`exposeEnv` allowlist（见下文）。数组索引、公开属性、`dict` 键、`{% … %}` 插值与
`$NODE.raw.…` 都不受影响。迁移说明见[迁移指南](../reference/migration-guide.md)。

运行时错误文案（**逐字**，与代码、迁移指南一致）：

```
expression field name '__class__' is not allowed in an expression path: names starting with '_' are reserved for Python internals (for example __class__ / __globals__ / __init__), so the attribute form reaches process state and bypasses the $ENV allowlist, which is an opt-in exposure list and not a sandbox. Only this prefix is gated: public attributes of node-returned objects stay readable by design (MIGRATION.md #55).
```

!!! warning "已知边界：本限制只覆盖 `_` 前缀，不是整体沙箱"

    只保留 `_` 前缀名，**公有属性遍历不受限**。节点返回值若把模块挂在**公有**属性
    上，公有链照旧可读——例如节点返回 `SimpleNamespace(os=os)` 时，
    `$NODE.h.os.environ` 就是活的 `os.environ`（可继续取到具体变量值）。这不是缺陷，
    而是本限制声明的范围：它关闭的是"反射到 Python 内部属性"（`__class__` →
    `__globals__`）这条路，**不把节点返回值变成不可信数据**。

    因此：`exposeEnv` 是"显式暴露"清单而非沙箱；**节点插件不应返回把敏感模块 /
    对象挂在公有属性上的返回值**（`os` / `sys` / 连接串对象等）。这是作者侧的信任
    边界。详见[迁移指南](../reference/migration-guide.md) 的 #55 条目。

### 取数据原文：`$NODE.raw.<节点>…`

`$NODE.<节点>.<字段>` 对**看起来像表达式/模板**的字符串字段值会按历史语义重新求值
（引用了父容器缺失根键的数据文本除外，见 [迁移指南](../reference/migration-guide.md)）。
当字段其实是**数据**——LLM 输出、文档正文、prompt 回显——这种静默改写是不可接受的，
所以在前面加 `raw` 修饰符：

```json
{ "$NODE": { "llm": { "text": "{% $F.eq(1,1) %}" } } }
```

| 表达式 | 结果 |
|--------|------|
| `$NODE.llm.text` | `"True"`（历史语义：模板被求值） |
| `$NODE.raw.llm.text` | `"{% $F.eq(1,1) %}"`（**原文**，不求值） |
| `$NODE.raw.llm` | 整个节点结果原样返回（其中的字符串都不求值） |

规则：

* `raw` 贯通整条路径：每一段的字符串都不求值（中间段也不会）；非字符串值
  （int/float/bool/None/list/dict）与不加 `raw` 时完全一致。默认路径只在**走到的
  字符串段**上求值，不会深度遍历容器里的字符串——所以整节点是容器时两者都原样
  返回，差别出现在字符串叶子与中间字符串段上；`raw` 保证的是"整条路径都不求值"；
* 只有 `$NODE` 有 `raw` 修饰符——`$INPUT` / `$GLOBAL` / `$PARENT` / `$ENV` 没有，
  `$INPUT.raw.x` 就是输入对象里名为 `raw` 的键；
* **命名冲突**：`$NODE` 后第一段是修饰符，**仅当** `$NODE` 里没有同名节点、且其后
  紧跟一个字段段（节点 id 位）。流程里若有 id 恰为 `raw` 的节点，`$NODE.raw.<路径>`
  仍指那个节点（存量语义优先），并打出一次 WARNING 说明该冲突——此时请给节点改名
  才能使用本访问器；`$NODE.raw.0` / `$NODE.raw[0]` 也仍按节点 `raw` 解析。
* **运行期窗口**：判定看的是**当次**的 `$NODE`，所以"声明了 id 为 `raw` 的节点、但该
  节点还没产出"时，`$NODE.raw.<其他节点>…` 会先按**访问器**解析（取原文、无 WARNING），
  节点产出后同一表达式才切成节点语义（并多一条冲突 WARNING）。这是规则的必然结果，
  且不会改变既有流程的结果（旧版本两态都得到 `None`）。

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


## 编译缓存与性能

表达式不是每次调用都重新解析：每个表达式/模板字符串首次求值时被编译成
context 参数化的 thunk（闭包树），按 parser 实例做 LRU 缓存（8192 条），
后续调用只执行闭包——典型命中路径在微秒级（首解析的 1/50 以下）。

护栏语义（对使用者透明，列出仅供排查）：

* thunk 只捕获结构（根键/路径段/函数名），**不捕获任何上下文值**——同一
  字符串在不同 context 下复用同一编译产物是安全的；
* 函数解析发生在**调用时**：运行中 `registry.register()` 新增/覆盖立即生效，
  scoped registry 对未暴露函数返回 `"undefined"` 的契约不变；
* 超过 4096 字符的字符串（长模板正文带插值槽）进**独立小 LRU**（64 条），
  与主缓存分开计数——重复求值同样命中（微秒级），但条数上限保证大文本不会
  无界驻留内存；
* 编译失败（语法错误）不缓存，`parse_function` 的「失败原样返回」契约不变。

## 下一步

- [内置节点](../nodes/builtin.md) 看各节点如何使用 `output` / `condition`
- [API: plaita.core.expression](../api/expression.md) 查看 `ExpressionRegistry` 完整接口
