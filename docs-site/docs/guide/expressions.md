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
| `$PARENT.x` | 父流程上下文（子流程中可用；**子执行创建时**冻结的快照，见下） | `None` |
| `$ENV.key` | 环境变量（须在 `exposeEnv` 白名单内，见下文） | `None`（未声明时另有解析期 warning） |

缺省口径统一为"缺键返回 `None`"；例外有二：整个根不存在（如上下文尚未建立 `$NODE` 状态）时保留 `KeyError`——那通常意味着流程逻辑错误，静默反而危险；字段名以 `_` 开头时按下文「属性遍历与下划线名」抛 `ValueError`。

支持的命名空间由 `ExecutionContext` 维护：`INPUT` / `NODE` / `GLOBAL` / `PARENT` / `ENV`。命名空间名本身可通过 `express_input_name` 等参数自定义；子执行（`child` 内联子逻辑 / `parallel` 分支 / `map`·`filter`·`find`·`reduce`·`loop` 迭代体）**继承父执行的命名空间配置**（plaita#106）——父级自定义名后，子流程表达式用同一套名字寻址即可。**`$ENV` 白名单同样继承**（plaita#141）：子流程没有自己声明（非空）`exposeEnv` 时，父级的 `exposeEnv` 就是子执行的 allowlist；子流程自己声明了则以声明为准（替换，不合并）。父级从未声明 `expose_env` 的执行不受影响（子 `$ENV` 仍为空）。

`$PARENT` 是**子执行创建那一刻**的父状态快照（plaita#72）：`Parallel` 未列入 `joinBranches` 的后台分支不阻塞父流程，父在分支提交之后写入的 `$NODE` 条目不会出现在该分支的 `$PARENT` 里（未创建时按缺省口径求值为 `None`）。需要子流程看到新结果时，请用 `joinBranches`（父会等分支）或在分支内显式传参。

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

### 属性遍历：只允许数据（安全限制）

字段段读的是**数据**：`dict` / 映射按**键**取，其它对象按**属性**取。属性读取
有两道**硬报错**（不是返回 `None`）的闸门：

1. **下划线名**（plaita#55）：`_` 开头的段名一律拒绝；
2. **代码对象**（plaita#133）：属性遍历只允许数据。属性解析出的值是 module /
   function / builtin function / method / class / frame / traceback / generator /
   coroutine / async generator / code object 时拒绝——按 `type(obj).__mro__`
   真实类型判定，不读对象自己的 `__class__`。结果值是代码对象时**该字段本身**
   就抛错（`$NODE.obj.os` 即拒绝，不会先返回模块再等 `.environ`）。

| 表达式 | 结果 |
|--------|------|
| `$NODE.obj.text` | `"hello"`（公开**数据**属性） |
| `$NODE.obj.os`（`os` 是 module） | **`ValueError`**：代码对象，`plaita#133` |
| `$NODE.obj.os.environ` | **`ValueError`**：在 `.os` 就拒绝，环境变量不会被读取 |
| `$NODE.obj.__class__` | **`ValueError`**：`'__class__'` 不是允许的字段名 |
| `$NODE.obj._private` | **`ValueError`** |
| `$NODE.d._foo`（`d` 是 dict） | 键 `_foo` 的值——**键**不受限 |
| `$NODE._n2`（自动生成的节点 id） | 该节点结果——**键**不受限 |
| `$NODE.d.__class__`（`d` 是 dict） | **`ValueError`**（dunder **路径段**同样拒绝） |
| `$F.get($NODE.d, '__class__')` | 该键的**数据值**——显式映射辅助不受限（见下） |

规则：

* **属性读取**：任何 `_` 开头的段名一律拒绝；解析出代码对象（module / function /
  class / frame / generator / coroutine / code 等）也一律拒绝；
* **路径段取值**：单下划线键保持可读（`{"_foo": …}`、builder 自动生成的 `_n2` 节点 id）；
  `__` 开头的**路径段**（`$NODE.d.__class__`）拒绝——路径形态无法与属性遍历尝试区分，
  静默 `None` 只会掩盖越权尝试；
* **映射 / 索引路径保持原语义**：字段段在 dict / 映射上是**键**，不是属性；数组索引
  与映射值按原语义读取。本闸门只针对**属性遍历**；
* 拒绝是**硬错误**：抛 `ValueError`（消息点名路径、真实类型与 `plaita#133`），在
  `end` 节点的 `output` 等路径上表现为节点错误。不要指望"被挡住 → 得到 `None`"。

**刻意的不对称（映射辅助不受限）**：`$NODE.d.__class__` 抛错，但
`$F.get($NODE.d, '__class__')` / `$F.getDictValue($NODE.d, '__class__')` 仍返回该键的
**数据值**，`$F.keys($NODE.d)` 也仍列出它。映射辅助只是"按键取数据"——键不是属性、
不产生反射语义；路径形态的拒绝针对的是与属性遍历无法区分、且会静默 `None` 的写法。

原因：`$NODE.<节点>.__class__.__init__.__globals__.os.environ` 这类**反射链**，以及
`$NODE.h.os.environ` 这类**公有属性链**，在节点返回 dataclass / pydantic / 普通实例
或 `SimpleNamespace(os=os)` 时都能读出执行进程的环境变量，绕开 `$ENV` 的
`exposeEnv` allowlist（见下文）。#55 关闭反射链，#133 关闭公有属性链；公开**数据**
属性、`dict` 键、`{% … %}` 插值与 `$NODE.raw.…` 都不受影响。迁移说明见
[迁移指南](../reference/migration-guide.md)。

运行时错误文案（**逐字**，与代码、迁移指南一致）。

下划线闸（#55）：

```
expression field name '__class__' is not allowed in an expression path: names starting with '_' are reserved for Python internals (for example __class__ / __globals__ / __init__), so the attribute form reaches process state and bypasses the $ENV allowlist, which is an opt-in exposure list and not a sandbox. Public attribute traversal is additionally data-only (plaita#133): public attributes that resolve to modules, functions, classes, frames, generators, coroutines or other code objects are refused by name.
```

数据面闸（#133）：

```
expression path '$NODE.h.os' traverses a non-data object of type 'module': attribute traversal is data-only (plaita#133), so modules, functions, builtin functions, methods, classes, frames, tracebacks, generators, coroutines, async generators and code objects are refused by name. Read a data field instead; mapping keys and numeric indices are unchanged.
```

!!! warning "边界：属性遍历只允许数据，映射 / 索引与 `$F.*` 未改"

    #133 的收紧落在**属性遍历**：公有属性若解析出模块 / 函数 / 类 / frame /
    generator / coroutine / code 对象，具名拒绝；公有**数据**属性仍可读。
    `dict` / 映射的键与值、数组索引、`$F.*` 注册表、`$ENV` 的 `exposeEnv`
    allowlist 都保持原语义。

    **代价**：流程若把方法 / 类 / 模块**当值**读取（例如
    `$NODE.obj.some_method`），现在会抛 `ValueError`；请改为读取该字段背后的
    数据值。`exposeEnv` 仍是"显式暴露"清单而非沙箱；节点插件不应依赖公有属性
    传递敏感模块 / 对象。详见[迁移指南](../reference/migration-guide.md) 的
    #55 / #133 条目。

### 映射遍历：容器必须是数据（安全限制）

#133 关闭**属性**遍历后，映射面仍可能把 live 容器当数据读：`$NODE.h.env` /
`$NODE.d.env` 返回 live `os._Environ`，随后的 `.HOME` 走映射键；
`$F.get($NODE.h.env, 'HOME')` 也直接读它。plaita#203 把**映射容器**也收紧为
只允许数据：

| 表达式 | 结果 |
|--------|------|
| `$NODE.d.k`（`d` 是 dict） | 键 `k` 的值 |
| `$NODE.d._foo`（`d` 是 dict） | 键 `_foo` 的值——**键**不受限 |
| `$NODE.ds.k`（`ds` 是只加方法的 dict 子类） | 键 `k` 的真实存储值；覆写的 `get`/`__getitem__` 不执行 |
| `$NODE.h.env`（`env` 是 `os.environ`） | **`ValueError`**：非数据映射容器（`plaita#203`） |
| `$NODE.d.env` / `$NODE.d.env.HOME` | **`ValueError`**：在 `.env` 就拒绝，环境变量不会被读取 |
| `$NODE.lst[0]`（`lst[0]` 是 `os.environ`） | **`ValueError`**：索引结果同样是值位置 |
| `$F.get($NODE.h.env, 'HOME')` / `$F.getDictValue(...)` | **`ValueError`**：`$F.get` 旁路同样拒绝 |
| `$F.get($NODE.d, 'env')` | **`ValueError`**：返回的 live 容器同样拒绝 |
| `$F.get($NODE.d, 'k')` | 键 `k` 的值——数据 dict 不变 |
| `$INPUT.x`（dict / `CheckpointState`） | 值——引擎状态是数据 |
| `$ENV.HOME` | 仅当 `exposeEnv` 显式包含 `HOME` 时可读（语义未改） |

规则：

* **允许**：真实 `dict`（含子类）与引擎自有状态 mapping（`CheckpointState`
  以及 `$PARENT` 的内部惰性快照）。dict 子类走 `dict.get` / `dict.keys` 的
  C 槽，覆写的读写方法不执行；引擎状态走自己的 `get`。
* **拒绝**：`os.environ`、`collections.ChainMap`、`collections.UserDict`、
  `types.MappingProxyType`、自定义 `Mapping`、duck-typed `__getitem__` + `get`
  的 host 对象——在**入口（作基座）/ 结果值 / 索引结果 / 根值**四个位置都抛
  具名 `ValueError`，在读取任何键 / 值之前终止。
* **`$F.get` / `$F.getDictValue`**：第一个实参是 host mapping-like 时，在输入
  测量和 callee 之前拒绝；返回值是 host mapping-like 时也拒绝。
* **兼容性**：`UserDict` / `ChainMap` / `MappingProxyType` / 自定义 `Mapping`
  请先转成 plain dict（如 `dict(value)`），或改用 `$ENV` 显式暴露；只加方法的
  `dict` 子类保持可用。

运行时错误文案（**逐字**，与代码、迁移指南一致）：

```
expression path '$NODE.h.env' read a non-data mapping container of type '_Environ': only plain dict (including subclasses, read through dict's C slots) and the engine's CheckpointState may be traversed as a mapping (plaita#203). Host mapping-like objects such as os.environ / ChainMap / UserDict / custom Mapping are refused by name before any key or value is read; convert the data to a plain dict first.
```

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

!!! danger "五个持久化根不可就地改写（plaita#69，r6：fresh ≡ **本次求值**由引擎新建）"

    就地改写函数（`pop` `delListItem` `setListItem` `set` `delete` `clear`
    `setDictValue` `delDictValue` `clearDict`）的**目标位**（arg0）只有在
    **本次逻辑求值**由引擎新建该对象时才允许；否则抛 `ValueError` 并点名完整路径/根，
    说明 `provenance could not be proven fresh`。原因：活的持久化对象写进去会被序列化进
    checkpoint 并在恢复后保留（不是纯函数调用）。

    **"fresh" 的确切含义（r6）**：freshness 存在一个随**最外层求值入口**
    （`ExpressionParser.evaluate` / `ExecutionContext.evaluate`）建立、
    `finally` 里丢弃的 `ContextVar` 作用域里；嵌套求值（嵌套表达式串、`parse_function`、
    容器重建）共享同一作用域。所以 **A 次求值里造出的对象，在 B 次求值里一定不是 fresh**
    ——包括"引擎物化后写进 `$NODE`/checkpoint，下一次求值再就地写它"这种形状（r5 的实现
    是模块级、进程内的登记，曾经放行这条路径；verifier 的 C2 反例）。合法的就地改写因此
    必须把**物化与改写放进同一次求值**，例如
    `$F.set($F.json_loads('{}'),'k','v')` 或
    `$F.set($X,'k',$F.json_loads($F.json_dumps($INPUT.cfg)))`。

    **哪些类型能进数据模型（r7）**：值参去别名不再用 `copy.deepcopy`，而是引擎自有的
    结构拷贝（数据路径上**唯一**的拷贝入口）。**接受**：`dict`/`list`/`tuple`/`set`/
    `frozenset` 与 `plaita.core.types.native_types` 声明的标量 —— `str`/`bool`/`int`/
    `float`/`Decimal`/`datetime`/`date`/`None`（另加不可变的 `bytes`/`complex`）。
    **容器/标量子类会被归一化成普通基类值**：`OrderedDict`/`defaultdict`/`Counter` →
    `dict`，`namedtuple` → `tuple`，`IntEnum` → `int`，`str`/`int` 子类按值转换；拷贝走
    **基类协议**，所以子类覆写的 `__deepcopy__`/`__copy__`/`items`/`__iter__`/`__getitem__`
    **一次都不会被调用**。**仍然拒绝**（fail-closed，具名错误
    `ValueError: … not part of the persistable data model …` 并点名类型）：真正属于非数据
    模型的**可变**对象与容器 —— 自定义 `MutableMapping` 实现、`bytearray`、不透明的宿主
    类实例、dict view（`d.keys()`）等。这样"detached 副本"永远不可能偷偷就是活对象本身。
    典型地，`$INPUT.amount` 是 `Decimal` 时 `$F.set($F.json_loads('{}'),'k',$F.add($INPUT.amount,1))`
    **可用**（`number` 类型包含 `Decimal`；r6 曾误拒，r7 修复）。

    **r10 补记（逐字段保真 + "存储之外的自述"）**：① 标量子类的归一化按**全部可观察字段**
    判等，不是只看 `type`/`==`：`datetime` 子类连 PEP 495 的 `fold` 一起带过 —— DST 歧义
    墙钟时间（`America/New_York` 2020-11-01 01:30 出现两次）的 `fold=1` 必须得到正确的
    `timestamp()`/UTC 偏移（EST, 1604212200.0）；r9 曾静默丢成 `fold=0`（EDT,
    1604208600.0，`==`/`hash()` 都看不出来）。`Decimal` 的符号/系数/指数、`date` 的
    `year/month/day` 同理逐字段保留。② **数据不在真实 `dict` 存储里**的 `dict` 子类一律
    **具名拒绝**：MRO 里 `Mapping` 排在 `dict` 之前、或用 Python 覆写 `__iter__`/`__len__`
    （容器外延声明）、或实例带自己的 `__dict__` 状态（数据放在 `self._d` 这类）—— 这些形状
    r9 之前会**静默变成空 `dict`**（循环条件副本"成功"但丢数据）。只覆写
    `items`/`keys`/`values`/`__getitem__` **且没有实例状态**的子类仍按基类存储归一化
    （r7/r8 契约，宿主覆写零调用）；带实例属性的普通 `dict` 子类会被这条判据**过拒**。
    **不要用 `dict(...)` 当逃生口（r11 更正，实测）**：`dict(v)` 读的是**真实 dict
    storage**（即 `real dict storage`，基类 `dict.items`），对"数据在别处"的形状（`ItemsLiar`/`SlotsItemsLiar`/
    `HidingDict`）恰好返回 `{}` —— **静默给出错误答案**；`dict(v.items())` 反而**采信宿主
    自述**（真的调用宿主 `items()`），同样不可信。诚实的恢复方式是从**已知良好**的数据
    重建，或用 `ctx.evaluate` / producer（`register(..., fresh_producer=True)`）按值物化。
    回归：`TestIssue69R11DictEscapeHatchIsLossy::test_dict_of_a_refused_shape_is_empty`
    钉住 `dict(<refused shape>) == {}`，防止这条警告腐烂。

    **r11 补记（clause iii 按性质判定）**：数据在**真实 dict 存储之外**的形状一律具名
    拒绝，判据是**四条机械可判、零宿主钩子**的析取（`instance_state_signal`）：非空实例
    `__dict__` ∨ MRO 里 `dict` 之前任一层声明非空 `__slots__`（含字符串简写、继承/合并
    的槽）∨ 遮蔽的 `__dict__`（`@property def __dict__`）∨ 非 C 槽实现的
    `__getstate__`/`__setstate__`/`__reduce_ex__`。r10 只测 `__dict__` 这一个**代理**，
    于是 `class SlotsItemsLiar(dict): __slots__ = ("_d",)` 两条拷贝路径都静默产出 `{}`
    且 DSL 报成功 —— 而只多一个 `__iter__` 的 `SlotsIterLiar` 却被拒（两者只差**数据
    放在哪**）。判据只用类对象（MRO 与类命名空间经 C 描述符
    `type.__dict__["__mro__"]` / `["__dict__"]` 读），所以宿主**元类**覆写
    `__getattribute__` 也进不来（否则它能让判据漏裸异常，或"说谎"抹掉声明了
    `__slots__` 的基类、重新静默丢数据）。拒绝消息点名触发的信号
    （`instance state (__slots__)` / `instance state (__dict__)` /
    `shadowed __dict__` / `instance state hooks (...)`）。

    **r12 补记（第 5 条：C 布局存储 + 视图物化按真实类型）**：r11 的四条判据仍是**代理**
    的枚举 —— **C 级成员槽**这条通道看不见：

    ```python
    class N_DDLiar(defaultdict):
        def __init__(self, d):
            super().__init__(None)
            self.default_factory = dict(d)      # member_descriptor 的 C 槽
        def items(self): return self.default_factory.items()
    ```

    `defaultdict.default_factory` 是 `defaultdict.__dict__` 里的 `member_descriptor`
    （CPython 3.9.6 / 3.10.21 / 3.11.16 / 3.12.14 / 3.13.15 实测都是），数据全在 C 槽里时
    `dict.items(v) == {}`、实例 `__dict__` 也是空的 ⇒ r11 判 False ⇒ 两条拷贝路径静默产出
    `{}`、DSL 报成功。r12 的 clause (iii) 增加第 5 条：**MRO 里 `dict` 之前、且声明层不是
    `cls` 自己**的层声明的 C member/getset 描述符 ⇒ 具名拒绝，信号
    `instance state (C descriptor default_factory on defaultdict)`。**负控照旧**：精确
    `OrderedDict`/`defaultdict`/`Counter` 与纯 `dict` 子类仍归一化（精确 `defaultdict` 的
    `default_factory` 由**它自己**的 `tp_members` 声明，属它自己的存储口径）。**过拒面（明说）**：
    无数据在存储之外的良性 `defaultdict` **子类**（`class BenignDD(defaultdict): pass`）现在
    也被拒 —— 与"带实例属性/`__slots__` 的 `dict` 子类被过拒"同级代价，取 fail-closed；
    `OrderedDict`/`Counter` 的子类不受影响（它们的非 `cls` 层只声明被排除的
    `__dict__`/`__weakref__`）。同轮还收窄视图物化：只有**精确类型**是引擎自产
    `dict_keys`/`dict_values`/`dict_items`/`odict_*` 的视图才被 `list(...)` 物化，
    `KeysView.register(LieList)` 这类 **ABC 注册**（宿主可控）与
    `class V1(KeysView): __iter__ = dict.__iter__`（解析到别人的 C 描述符）一律具名拒绝 ——
    后者在 r11 会漏**裸** `TypeError`，前者能把 `KeysView.register(LieBytes)` 的
    `b'LIE'` 物化成 `[76, 73, 69]`（`_engine_copy` 的口径是 `b'LIE'`，值种类被静默改掉）。
    返回 `['LIE']` 的假视图不再可能被写成数据（**r13 更正**：这句**被反证** —— `$F.keys`
    从来不是引擎自产视图，它直接问宿主；`class LieKeysPlainList(dict): def keys(self): return
    ["LIE"]` 在 r12 下仍报成功、`$VAR == {'k': ['LIE']}` 且宿主 `keys()` 被调用。r13 把这条
    通道关掉，见下方 r13 补记）。
    **设计之外（明说）**：数据放在**类属性** / **闭包** / **模块级旁表** / **property 或数据
    描述符**后面时，零钩子静态判定不把这些当"实例状态"。**r13 理由更正**：旧措辞把这四族的
    原因统一写成"要看见就得调用宿主代码 / 可达性不可判定"，对**类属性**族**不准确** ——
    `_class_namespace(klass)` 本来就零钩子地扫过每层真实 `tp_dict`，类属性**看得见**；真实
    理由是"**这个属性是内容，还是常量/缓存/类级默认值？**"**无法仅从类对象判定**，而把任何
    非 dunder 类属性都当状态会拒绝语言本身（`__module__`/`__qualname__`/`__doc__`、装饰过的
    方法、类常量）。闭包 / 模块级旁表 / `property`-或-数据描述符三族才确实需要宿主代码或
    不可判定的可达性判定。这几族**不在**本判据的承诺范围内。

    **r13 补记（clause iii 第 5 条改成布局判据 + 映射辅助读真实存储）**：

    - **布局，不是名字**：r12 的豁免理由是"C 描述符只可能由拥有 C 布局的类型生成，Python
      类体写不出 `member_descriptor`" —— **假的**。Python 类体的 `__slots__` 就在该类
      **自己的** `tp_dict` 里生成 `member_descriptor`；r12 的 clause 2 按**名字** `__slots__`
      找槽、clause 5 又 `if klass is cls: continue`，于是"隐藏声明、保留槽"就能恢复静默
      `{}`/DSL 报成功：

      ```python
      class HiddenSlotsLiar(dict):
          __slots__ = ("_store",)                      # cls 自己的 member_descriptor
          def __init__(self, d): self._store = dict(d)
          def items(self): return self._store.items()
      del HiddenSlotsLiar.__slots__                    # 抹掉名字，槽还在
      ```

      r13 改成按**每个层命名空间里真实存在的** `member_descriptor`/`getset_descriptor`
      判定（忽略 `__dict__`/`__weakref__` 占位名）；豁免只有一条：声明层是 `cls` **自己**
      **且** `cls` 不是 **Python 定义的类**（判据 = `Py_TPFLAGS_HEAPTYPE` ∧
      ¬`Py_TPFLAGS_IMMUTABLETYPE`，即 `0x200` ∧ ¬`0x100`）。**注意不能只看 heap 位**：
      CPython 3.12/3.13 的 `collections.defaultdict` **自己就是堆类型**（裸解释器实测
      `0x20405740`，heap 位置位），只看 heap 会把精确 `defaultdict` 误拒；`IMMUTABLETYPE`
      位把它（以及 `dict`/`OrderedDict`）与 Python 类区分开（3.9 上该位未启用，而 3.9 的
      扩展类型都是静态类型 ⇒ 判据退化成"只看 heap"仍正确；引擎进程里该值观测为
      `0x20485740`，多出的是无关 version-tag 位）。
      于是 Python 类自己的槽**无论 `__slots__` 名字是否还在**都被看见（信号
      `instance state (C descriptor _store on HiddenSlotsLiar)`），子类继承到的扩展类型
      层槽照旧被看见；精确 `defaultdict`/`OrderedDict`/`Counter` 与纯 `dict` 子类照常归一化。
    - **`$F.keys`/`$F.values`/`$F.items`（含 `getDictKeys`/`getDictValues`）**：参数是 `dict`
      时改走**基类 C 描述符**（`dict.keys(v)` 等），宿主的 `keys()`/`values()`/`items()`
      **零调用**；`LieKeysPlainList` 这类"自述 `['LIE']`、数据在真实存储"的形状现在得到
      **真实存储**（`$VAR == {'k': ['a']}`），假视图到不了值参。普通 `dict` 的行为逐字不变。
      非 `dict` 的映射无法在不进入宿主代码的前提下读到真实存储 ⇒ **具名拒绝**
      （`ValueError`，含 `plaita#69`）。
    - **快照按引用共享的口径更正**：快照只重建 `dict`/`list`/`tuple`/`set`/`frozenset`，
      其它值按引用共享 —— 其中**包含容器** `types.MappingProxyType` / `collections.ChainMap`
      / `collections.UserDict` / `os.environ`（旧措辞"非容器宿主对象"是错的）。它们按引用
      共享是因为重建要猜构造协议；在只读快照路径上安全，且**不是数据通道**：DSL 路径的
      `_engine_copy` 对这四个**具名拒绝**（不在可持久化数据模型里）。

    **破坏性收紧（r2 起）**：不只是目标位，把活根当**值**传进去同样拒绝 ——
    `$F.set($VAR,'k',$INPUT)` 会把根对象存进 `$VAR`，之后 `$F.set($VAR.k,'x','y')`
    就能经别名写穿 `$INPUT`。base 上这些写法静默放行且会写穿，升级后必须显式派生副本。

    **浅 producer 的值参会自动去别名（r3）**：`$F.slice(...)` / `$F.reverse(...)` /
    `$F.values(...)` 等返回的是**新容器但元素仍指向根**。它们作**目标位**仍然可用
    （mutator 只改新容器自己的条目）；作**值参**且自身实参触达持久根时，值会先经引擎结构
    拷贝再存入，因此 `$F.set($VAR,'k',$F.slice($INPUT.items,0,1))` 不再让
    `$VAR.k[0] is $INPUT.items[0]` 成立，后续就地写也碰不到 `$INPUT`。**标量累加**
    （`$F.set($VAR,'total',$F.add($INPUT.n,1))`，`n` 为 `int`/`float`/`Decimal`）等合法
    用法不受影响；`Decimal`/`datetime`/`date` 载荷同样走这条路径（r7 起）。

    **callee 只在自身实参触达根时被拒（r3 + r5 运行时身份校验）**：
    `$F.get($VAR,'x')` / `$F.ifelse(...)` 这类调用如果实参里没有持久根，作目标或值参都
    放行**当且仅当运行时身份命中 fresh**（即结果确实来自引擎新建的容器）；只有
    `$F.get($INPUT,'cfg')` 这种实参触达根（或未声明/未分类的 callee）才 fail-closed 拒绝。

    **仍然允许（由构造证明 fresh）**：**同一次求值里**由引擎物化的非根键
    （`ctx.evaluate` / producer 的结果，且物化与改写属于同一次求值）的就地改写；循环条件
    里的改写（`Loop`/`While` 把"条件隔离副本的物化 + `Condition.match`"开成**一次**逻辑
    求值，改写落在副本上，`$LOOP-ITEM` 也被引擎结构拷贝）—— **前提是条件根的整棵子树都是
    数据模型值**（`dict`/`list`/`tuple`/`set`/`frozenset` + 可持久化标量）；以及对
    **新对象**的改写——DEEP producer
    （`$F.json_loads(...)` / `$F.split(...)` 及纯标量函数）、SHALLOW producer
    （`$F.slice(...)` / `$F.reverse(...)` / `$F.append(...)` / `$F.extend(...)` /
    `$F.sort(...)` / `$F.add(...)` / `$F.values(...)` …）以及自定义 registry 里
    `register(..., fresh_producer=True)` 声明的 producer。

    **循环条件根的过拒（r8 文档化，fail-closed）**：条件根里只要含**任何**非数据模型对象
    （`bytearray`、不透明宿主类实例、自定义 `Mapping`…），引擎就造不出隔离副本：该根
    **保留活引用且不登记** fresh，因此**该根内任何就地写都会被具名拒绝 —— 包括同根的兄弟
    纯数据子树**（例如载荷 `{"items": [{"n": 1}], "blob": bytearray(b"x")}` 下
    `$F.set($INPUT.cfg,'k','v')` 同样被拒，活对象 0 变化）。这是有意的 fail-closed：宁可
    按整根拒绝，也不"部分物化"出第二条拷贝实现、把活子对象留在引擎自有容器里。要就地改就
    **先物化要改的部分**：`ctx.evaluate` / producer /
    `$F.json_loads($F.json_dumps(<part>))`。

    **r9：这个 JSON 往返逃生口是**有损**的（明说，别当无痛 round-trip）**。
    `$F.json_dumps` 只认 JSON 原生类型，因此：

    * `Decimal` / `datetime` / `date` / `bytes` / `set`（以及 `frozenset`）**直接抛
      `TypeError`**：`Object of type Decimal is not JSON serializable` —— 逃生口在这
      些类型上根本不可用；
    * `tuple` **静默变成 `list`**（`(1, 2)` → `[1, 2]`）；`dict` 的**非 `str` 键**同样
      被 JSON 语义改写（键被字符串化）。

    按类型选**正确**的替代，不要用 JSON 往返硬扛：

    * `tuple` / `frozenset`：改用 `ctx.evaluate(<part>)` 或自定义 producer
      （`register(..., fresh_producer=True)`）—— 它们按值物化，不改形状；
    * `Decimal` / `datetime` / `date`：先**显式转换**成 JSON 可表达的类型
      （`str` / `int` / `float`）再往返，或整体改用 `ctx.evaluate` / producer；
    * `bytes` / `set`：`json_dumps` 无解 —— 用 `ctx.evaluate` / producer，或先显式
      转成 `str` / `list` 再改。

    回归（防止注记腐烂）：`tests/unit/test_issue69_r9_host_object_loopholes.py`
    的 `TestIssue69R9EscapeHatchIsLossy` 同时钉住 `tuple`→`list`、五类 `TypeError`
    与本节文字本身。

    **r6/r7 收紧：边界副本不再是 fresh**。子流程 `$INPUT` / 集合 / `Reduce.initial` /
    `Parallel` 分支输入的私有副本仍然是**深物化的私有快照**（活根无法按引用进入父流程
    之外的执行），但它们在**父求值**里造出、在子执行（另一次求值）里使用，因此子执行对
    副本的就地改写会**具名拒绝**（r5 是"改在副本上、不报错"）。r7 删除了这四处**作用域外**
    的 `note_fresh_closure` 死登记（r6 起就是 no-op），语义不变。父运行态与 checkpoint 的
    零写穿性质不变，只是由"拒绝"而不是"改副本"来保证。注意 `Parallel` 分支的拒绝**不是**
    "`ContextVar` 不跨线程"：`mode="coroutine"` 在 MainThread 里同样被拒，根因是边界调用点
    不在任何求值作用域内。

    **r5/r6 的破坏性收紧（owner 决策边界）**：**宿主直接绑定**的非持久化键不再按键名
    放行——`ctx.set_state("$V", {"a": 1}); parser.evaluate("$F.set($V,'k','v')", ctx.context)`
    会被具名拒绝（来源不可证 fresh）。合法写法是**在同一次求值里**把值交给引擎物化：
    `$F.set($F.json_loads('{}'), …)` / producer 结果（`$F.json_loads($F.json_dumps(...))`）。
    注意 `ctx.set_state("$V", ctx.evaluate({"a": 1}))` 已**不再**足够——`ctx.evaluate` 是
    一次独立求值，它的作用域在返回时就丢弃了（r5 曾放行，r6 拒绝）。同理，数据容器里挂
    **宿主自造副本**（`copy.deepcopy(...)`）也被拒；同一次求值里引擎物化的副本仍放行。
    verifier 的 T2-1 变体（`ctx.set_state("$V", ctx.get_state("$INPUT")["deep"])`，活子对象
    从未被当路径终点交出过）同样被拒且零写穿。

    **r14 / plaita#164 补记：显式 scratch 捐献 `ctx.set_scratch(key, value)`**。上面那条
    收紧让"宿主手里已有的对象"没有直白写法；`set_scratch` 是给它的**显式声明**入口，
    语义 = "这个对象只归本次执行所有、可以被就地改写"：

    ```python
    ctx.set_scratch("$V", {"a": 1})                        # 显式捐献（私有绑定）
    parser.evaluate("$F.set($V,'k','v')", ctx.context)     # 就地写，允许
    ctx.get_state("$V")                                    # {'a': 1, 'k': 'v'}
    "$V" in ctx.to_dict()                                  # False —— 不进 checkpoint
    ```

    它**不引入任何"证明对象是活的"谓词**：声明只把对象登记进**读取它的那一次逻辑求值**
    的 freshness 作用域（读取点登记），所以上面的 r6 作用域语义原样保留。四条边界都要记住：

    * **只登记被声明的那个对象**（对象身份），**不登记它的图**：`$V.cfg` 这类嵌套目标仍被
      拒。把 `$INPUT` 的子对象塞进 scratch 的图里也**不能**改写它。
    * **一次求值有效**：声明会被"读到 `$V` 的那次求值"消费；**第二次求值里同一个宿主对象
      仍被拒**。再用就重新 `set_scratch`。
    * **不进 checkpoint**：scratch 对表达式可见（`context["$V"]`），但不在 `__iter__` /
      `len()` / `keys()` / `__eq__` / `to_checkpoint_dict()` 的键视图里；`clean()` 与恢复
      会丢掉它。
    * **不得占用 checkpoint 键**（五根与其它 schema 键、以及已在 checkpoint 里的键），
      声明一个"就是活持久化对象"的对象也被具名 `ValueError` 拒绝（那会新开一条写穿通道）。

    **选 `set_scratch` 还是 `ctx.evaluate({...})`？** 想就地改**你自己手里那个对象**
    （保留对象身份）⇒ `set_scratch`；`ctx.evaluate(obj)` 会把它换成引擎副本，你手里那个
    不会被改。只想在**一个表达式里**派生临时容器 ⇒ 直接用 producer
    （`$F.json_loads(...)` / `$F.slice(...)`），不必绑定。需要"引擎物化一份副本"且物化与
    改写**属于同一次求值** ⇒ `with freshness_scope(): ctx.set_state("$V", ctx.evaluate({...}))`
    （跨求值再写它照旧被拒，见上面的 r6 口径）。

    **容器键名与深度不参与判决（r4 身份探测 + r5/r6 freshness）**：把活根挂在**任意键名**、
    **任意深度**下都无效——`InlineFlow(input={"$myroot": "$INPUT"})` 里的 `"$INPUT"`
    作为字典值求值就是活根对象；嵌套串 `$F.set($myroot.cfg,'k','v')` 在 r4 由身份探测
    具名拒绝，在 r5/r6 由 freshness 独立拒绝（r4 的 27 格容器轴在"零登记"下曾 9 格写穿，
    r5/r6 在零登记/有登记两套状态下判决一致：18 拒 / 9 放行 / 0 写穿）。

    完整分类矩阵（机械生成：378 目标位 + 294 值参位 + 112 浅值参去别名 + 42 callee 触达
    + 72 非根 callee + 511×2 producer × position 全表，0 格可制造别名；容器键名轴 27×2
    + 14 个多位置探针 + **90** 个作用域/物化调用点清单；性能守卫覆盖 mutator 路径并用
    checked-in 基线抓常量因子回归）：
    `docs/issue69-mutation-matrix.md`。
    重算：`PYTHONPATH=. python scripts/issue69_mutation_matrix/table.py` 与
    `probes.py` / `scope_inventory.py` / `bench_expr.py`。

!!! warning "未注册的函数名求值为 'undefined' 字符串"

    `$F` 是作用域注册表：调用未注册（或拼写错）的函数名**不会报错**，而是把整
    个调用求值为字符串 `'undefined'`（默认 registry 未注册该名字时也一样）。例如
    `$F.uppr($INPUT.name)`（拼错）静默得到 `'undefined'`，下游拿到的就是这串文本。
    排查时先核对函数注册名是否与[内置函数一览](#内置函数一览)或你的
    `registry.register(...)` 名字完全一致。

### 结果规模守卫（`$F.*`）

表达式引擎会拒绝「小输入 → 巨大输出」的放大：阈值**随输入规模走**，不是一刀切
的绝对上限。

```
allowed = min( max(floor, factor * input_size), hard )
```

| 旋钮 | 默认 | 环境变量 |
|------|------|----------|
| floor | `100_000`（字符 / 字节 / 元素） | `PLAITA_EXPR_MAX_RESULT_SIZE` |
| factor | `4` | `PLAITA_EXPR_GROWTH_FACTOR` |
| hard | `100_000_000` | `PLAITA_EXPR_MAX_RESULT_HARD` |

环境变量在导入时读取一次（改后需重启进程），非法值回退默认。超限抛
**`ValueError`**，不会静默截断。

判定是**调用点级**的：守卫在调用 Python 算子之前算出这次调用的结果上界；算不出来
时（类型不匹配 / 无法枚举）该调用注定抛**原生** `TypeError`/`KeyError`，而不是"跳过
守卫"。求值侧探针也不按函数白名单跳过，每次 `$F.*` 调用都按同一条规则复核，输入规模
用**内容量**（字符串长度、memoryview/array 的字节数、容器内容之和）而不是元素/参数
个数；容器逐项求和，绝不用可能低估的抽样近似。

**在分配之前拦截**的算子：

| 算子 | 何时拒绝 |
|------|----------|
| `mul` | 任意「支持 `__mul__` 且有 `__len__`」对象的重复超限：`seq * int` **与** `int * seq` 双向，覆盖 `str` / `bytes` / `bytearray` / `list` / `tuple` / `array.array`（数值乘法不受限） |
| `replace` | `len(s) + 命中次数 × (len(new) - len(old))` 超限（空模式按每字符间插入计）；`bytes`/`bytearray` 接受 bytes-like 的 pattern/replacement |
| `mod` | **str/bytes 的 `%` 是 printf 格式化**：映射键 `%(k)`、flags、宽度（数字或 `*`，负宽度取绝对值）、精度都会解析，并按"**分配量**"判定——`'%100000000d'`、`'%.900000000g'`（结果 1 字符但分配 ~1GB）、`'%(k)100000000s'`、`'%*d' % (-1e8, 1)` 都在此拦住；`s`/`r`/`a` 的精度是截断，不拒绝。转换符与值类型不匹配时先抛原生 `TypeError`。数值 `%` 不受限 |
| `join` | 分隔符被 `元素数-1` 次重复后的总长超限；被 join 的对象可以是任意可迭代——长度不可预知时由**调用点先物化一次**再按内容量判定（指针有 100MB 的独立字节预算），`__len__` 少报也不会截断结果 |
| `sort` | `sorted()` 同样会**消费**单次迭代器：调用点先物化一次再判定（与 `join` 同一套机制） |
| `add` `extend` `append` `concat` | 结果与输入同量级（线性），只有超过 hard 才会被拒 |

**量级示例**：`"x" * 10**6` 被拒（放大 10⁶ 倍）；`join(20 段 × 10000 字符)` = 200000
字符通过；`json_dumps([{"k": "v"*100}] * 2000)` = 222000 字符通过；
`mod("%s %s", (100KB, 100KB))` = 200001 字符通过；`join(['b'], $INPUT.long_sep)` 通过
（只有一个元素时分隔符根本不进结果）。反过来，`today('%c' * 100000)` = 2400000
字符（24 倍放大）会被拒绝——这是探针拦下的，与"非放大的大输入必须放行"不矛盾。

求值路径上的判定分两层：**预检层**（内建算子在调用 Python 算子之前判定）与
**探针层**（`$F.*` 返回后按同一条比值规则兜底）。探针用的输入规模是调用点**在调用
之前**算好、并作为**本帧局部量**传进来的快照——没有线程本地之类的共享槽位，也
**不在调用后回读实参**（否则被调用者改写自己的可变实参就能抬高额度）。
`{% ... %}` 插值同样过守卫。

**两个口径要分清**：
* **结果/输入口径**（探针与预检的 allowance）：字符串/字节按**长度**；容器/映射按
  **内容量之和**，并以**元素数**为下界（已知长度的内建容器用元素数豁免
  `['']*200000` 这种"内容量为 0"的退化情形，避免误拒）。
* **计费与枚举走祖先 C 槽**：`str.__len__` / `list.__iter__` / `dict.items` …，实例覆写的
  `__iter__`/`__len__`/`count`/`join` 只影响上界、影响不到原生算子与 `str()`/`repr()` 的 C 槽
  ⇒ 一律不采信（否则能按「空迭代器」的上界放行 1 亿字符的真实渲染）。这条对**所有算子**
  的前置检查与预留成立（第十二轮 M1：`mul`/`replace`/`add`/`extend`/`append`/`join`）；
  **没有对应 C 槽可读的值 ⇒ 拒绝**，不按实例协议猜。
* **先判后渲染**：渲染类路径（`%s/%r/%a`、`concat`、`{% ... %}`）一律先算**可证上界**，
  不可得或超额度 ⇒ **直接报错**（不调用 `str()`）；事后探针只是第二道网。
* **namedtuple 不做整体渲染**（`__code__` 相同 ≠ 行为相同：闭包/描述符都能换实现）：
  **索引下钻可用**（`$INPUT.v[1]` 实测返回元素），但 **`$INPUT.v.a` 这类字段访问在本
  表达式文法里不可用——静默返回 `None`**（不是本单引入的行为）；需要整体渲染请在
  上游先 `str(x)`（表达式文法里没有裸函数调用）。
* **渲染额度口径**（`%s/%r/%a`、bytes 模式 `%s`/`%b`、`concat`、`{% ... %}`）：只用
  **可证渲染上界**，不借用上面的元素数下界。第二十一轮起 `plaita/core/expression.py`
  的 **`_RENDER_DISPATCH` 是唯一判据表**（key = 转换符，value = `(kind, slots, bound)`；
  `_render_size`/`_spec_bound`/`_conversion_accepts`/`_definitely_rejected` 都只查它，
  归属判定由同一张表分两处驱动：数值族在 `_conversion_accepts` 按行 `kind`/`slots` 判定，
  文本/buffer 族在行的 `bound` 里判定）。放行：精确内建/标准库类型（str/bytes/bytearray/int/float/
  bool/None/容器/视图/迭代器/`Decimal`/`datetime`/`UUID`/`PurePath`/`Pattern`/`Enum`/
  `array`/`deque`/`SimpleNamespace`/默认 repr 的对象）与**干净子类**（表里该转换符派发的
  槽与安全祖先是**同一原始槽**）；拒绝：覆写 `__str__`/`__repr__` 的子类、bytes 模式下带
  **作者** `__bytes__` 的非 buffer 值、只有数值协议可取数的自定义对象——消息带
  "可用 `str(x)`/`int(x)`/`bytes(x)` 放宽"。**实测例外**：int 家族在 `d/i/u/o/x/X/c` 上
  原生 C 存储直读（覆写 `__int__`/`__index__` 被忽略 ⇒ 不设槽闸）；bytes/bytearray 子类的
  `%s`/`%b` 直读 C buffer（覆写 `__bytes__` 被忽略 ⇒ 按 C 字节数计）。
  **第二十二轮补：纯 Python 祖先的实例属性也要可证**——`PurePath.__str__`/`UUID.__str__`
  是 Python 代码、会读实例槽（`_str`/`_drv`/`_root`/`_parts`、`int`），子类放一个同名
  property 就能把"安全投影"换掉（实测上界 67 / 实际 10,000,000）；因此从子类到最近的已知
  stdlib 祖先之间**任何一层类命名空间不得新增名字**（`_stdlib_only_below`），上界改读**真实
  实例槽**（0 作者调用），遮蔽 property 的形状 fail-closed 拒绝；`SimpleNamespace` 改读
  `SimpleNamespace.__dict__` 的 member_descriptor（C 直读真实实例 dict，property 无效）；
  元类遮蔽 `type.__name__` 的子类一律拒绝。
  **第二十三轮补：精确类型也走同一道闸**——精确 `PurePath`/`UUID` 的实例槽同样可写
  （`p._str = "E"*1e7`、`object.__setattr__(u, "int", 10**400000)`），r22 的
  `value.parts` 投影 / 常量 64 会被击穿（投影 67/74 vs 实际 10,000,000/332,197、
  `%r` 峰值 22.5MB）；现在精确分支也读**真实实例槽**，巨槽 ⇒ 上界 ≥ 实际且分配前拒绝。
  **第二十四轮补：常量上界也要读真实成分**——类对象/enum 类/默认对象/函数/timezone/
  `repeat`/`count` 的 repr 内嵌 `__module__`/`__qualname__`/`__name__`/`name`/参数，这些
  **建成后仍可写** ⇒ 常量 256/64 是推理不是证明；现在一律读真实成分（不可变 getsets /
  C 槽 / `__reduce__`），读不到就拒绝。
  **第二十五轮补：`array.array` 按 typecode 实测最坏值**——`array('d')` 的 repr 每元素
  26 字符（`repr(-max)` 24 + `", "` 2），旧的 `32+len×24` 投影低于实际并能在 K≈4.17e6 时
  绕过 1e8 硬上限；现在 `d`/`f` 26、各整型 typecode 6–23、`u` 转义 10×/字符。
  **第二十六轮补：`%a`（ascii）的展开按 10× 计**——`%a` 会把非 ASCII 字符展开成
  `\xNN`/`\uNNNN`/`\UNNNNNNNN`（4–10×），所有 `as_repr` 上界里的作者可控名字
  （enum 类名/成员名、`PurePath`/`UUID` 子类名、`ZoneInfo.key`）都按 10× 计，且 `%a`
  的成长额度只按值的权威内容量计 ⇒ 1M 星平面名字在**分配前**被拒（r25 是分配后 20MB）。
  **第二十七轮补：str/bytes 的上界是真实逐字符转义和**——`\n` 是 2 不是 10、`é` 是 4、
  星号平面才是 10（+2 引号；bytes 前缀 +3/bytearray +14），所以 `"\n"*9999`、
  `"é"*9999`、`"\n"*100000`、bytes `"é"*12496` 都正常渲染；`%a` 的成长额度按值的
  权威内容量，`"é"*24997`/`"😀"*9998` 渲染、24,998/9,999 起拒（`"😀"*1e6` 仍在渲染前拒，
  消息里给阈值与逃生口：改用 `%r` 或提高 `PLAITA_EXPR_GROWTH_FACTOR`）。
  **第二十八轮补：bytearray 的引号规则与上界计算的内存预算**——`bytearray.__repr__`
  **无条件**转义 `'`（`bytes` 只在同时含 `"` 时转义），所以两者分开计；上界计算本身不得
  放大内存：bytes 用 `translate(None, delete)` 计数（1.00×），`str` 的 `re.subn` 链峰值
  2.1–4.2× 存储（文档与测试都写明预算）；容器的 str 元素同样走真实逐字符宽度。
  **计费与枚举一律走祖先 C 槽**（`str.__len__`/`list.__iter__`/`dict.items`…），实例覆写的
  `__iter__`/`__len__` 只影响上界、影响不到渲染的 C 槽，一律不采信。
  类型层事实经不可变 getsets 读取（`type.__dict__["__mro__"|"__dict__"|"__name__"].__get__`），
  元类数据描述符对 `__mro__`/`__dict__` 撒谎不再影响判定；判据只看真实类型对象，不看
  `__module__` 之类自述字段；枚举期间的用户异常一律转成"不可界定 ⇒ `ValueError`"；
  `join`/`sort` 的实参在调用点**物化一次**，守卫与执行共用同一次迭代。
* **渲染点清单守卫**（第十二轮）：`expression.py`/`expression_parser.py` 里所有
  `str()`/`repr()`/`format()`/`ascii()`、`%` 格式化、`.format()`/`.join()`/`.replace()`、
  `__str__`/`__repr__`/`__format__`/`__mod__` 属性取用、f-string 形参插值、`logger.*`
  插值、以及可静态识别的间接构造，都由 AST 测试**逐点登记**；标为 `guarded` 的点，其具名
  上界调用必须**支配**它（挪进 `if False:` 会红），并配每个渲染入口的行为反例。**它只是
  清单，不是上界证明**——上界真假由祖先槽反例等**行为**用例承重。
* **日志不插值作者数据**（第十二轮 M4 / 第十三轮 O4）：评估路径上的 `logger.*` 只记
  类型名/元数/形态；类型名截到 64 字符、异常消息只用 `BaseException.args` 的 C 槽取单
  str 参数并截到 200 字符（不调用覆写的 `__str__`）。仓库级日志卫生是 plaita#151，不在本单。
* **`%` 的上限判定不容许空转**（第十三轮 O1）：格式串宽度/精度本身就是分配下界；解析不出
  或无法证明原生必然抛错 ⇒ **拒绝**（旧实现 projected=0 时 27 字节能造出 477MB）。星号
  宽度只接受精确 `int` 并用 C 槽读（不跑 `__int__`）；映射取值**只读一次**并用同一份值渲染
  （消除 TOCTOU），`dict` 子类走 `dict.get`（不触发 `__missing__`）。
* **精确 `zoneinfo.ZoneInfo`** 计入上界（第十三轮 O6）；`ZoneInfo` 子类/未知 tzinfo 仍拒。
  精确 `collections.UserList`/`UserString` 走其 `.data` 的 C 槽；**其余自定义序列/自定义
  `replace`** 保持拒绝——逃生口在上游（产值的节点/插件传精确类型或先 `str(x)`）。
* **只收口 `Exception`**：`BaseException`/`KeyboardInterrupt`/`SystemExit` 照旧穿透。
* **enum / tzinfo 的作者数据计入上界**（第十二轮 M6/M7）：enum 成员 `value` 的 repr、
  `datetime`/`time` 的 `tzinfo` 的 repr 都跑同一套计费；`IntEnum`/`IntFlag`/`StrEnum`/`Flag`
  成员按各自 `__str__` 原始槽给上界（不再误拒）；覆写 `__str__` 的 enum 子类、repr 不可
  界定的 `tzinfo` 仍然拒绝。

**类型错误优先于尺寸错误**：`str + list`、`list * str`、`set * int`、`join` 遇到
非匹配元素、`%` 的参数个数/mapping 键缺失/非法转换符/`b'%s' % 'x'` 等情况在任何
规模（哪怕把三个旋钮压到 1/1/1）都抛原生 `TypeError`/`KeyError`/`ValueError`。

!!! warning "求值侧探针对未预检路径只是「检测到」，不是「阻止」"

    表达式求值时还会对任何 `$F.*` 的返回值做硬上限判定，并对**没有预检**的调用
    套同一条比值规则。但这类路径的值**已经被分配出来**了才被拒绝——最典型的是
    你自己 `registry.register(...)` 注册的函数：`$F.boom(1000000)` 会先造出
    1MB 字符串再抛 `ValueError`（恶意函数内部先做一次合法大操作再返回 99MB 字符串
    同样只能事后拒绝，峰值就是它自己分配的 94MB）。上表列出的内建放大算子才是
    分配前拦截（受保护路径的 tracemalloc 峰值不随放大规模增长）。大块计算请放在
    节点 I/O 里，不要指望这条守卫替你省内存。

    这类调用的**额度只由"调用之前"测得的实参快照决定**：判定不存在线程本地或
    跨函数通道，也不在调用后回读实参——`func(*args)` 期间跑的任何代码（含**改写
    自己的可变实参**）都无法放行外层结果；`{% ... %}` 插值也过同一条守卫。
    `join`/`sort` 需要的"先物化"由**调用点**按**算子名**完成，所以
    `functools.partial`/lambda/绑定方法包装后同样生效。

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
