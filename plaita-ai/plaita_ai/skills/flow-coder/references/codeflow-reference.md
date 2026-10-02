# @flow DSL 完整参考

本文件是 `flow-coder` skill 的参考资料。生成复杂 `@flow` 流程前先查阅对应小节，避免踩"会报错的写法"。

> **分工**：本文件只管**语法**（能写什么、编译成什么）。语法之外的**作者硬约束**（跨分支同名赋值、作用域边界、`params=` 语义）、**plaita-nodes 业务集成**、项目结构与 console 发布，见 [authoring-spec.md](authoring-spec.md)——写业务编排前必读。

## 1. 函数骨架

```python
@flow("<flow_id>", desc="<说明>")
def <name>(INPUT):
    # 仅支持：if/elif/else、while、for...in MAP/FILTER/FIND/LOOP/REDUCE/WHILE、赋值、return
    return <expr>
```

- **`input_type` 已废弃且被忽略**：`$INPUT` 恒为 `run()` 传入的 dict，无需也不能声明类型。传了该参数（含 `flow_from_source(src, input_type=...)` 形式）只会触发 DeprecationWarning。
- `desc` 装饰器参数只支持字面量（字符串/数字/dict 字面量）。
- **AI 动态生成一律走 `flow_from_source(src)`**，不要走 `@flow` 装饰器（动态函数无源文件，`inspect.getsource` 会失败）。

## 2. 占位符（无需 import，编译期识别）

| 类别 | 名字 |
|------|------|
| 命名空间变量 | `INPUT` `NODE` `GLOBAL` `PARENT` `ENV` |
| 表达式函数 | `F`（`F.upper` `F.concat` `F.add` `F.len` `F.mod` …） |
| 内置节点调用 | `HTTP` `CODE` `EVENT` `MAP` `FILTER` `FIND` `LOOP` `REDUCE` `WHILE` `CHILD` `REFERENCE` `PARALLEL` |
| 自定义节点调用 | `node_type` 大写化（如 `LLM` `RETRIEVE` `TOOL` …，须已注册到 `NodeRegistry`） |
| 错误处理 | `ErrorHandler(strategy, default=...)` |

> 自定义节点占位符**不需要 import**：编译期查 `NodeRegistry`，名字全大写且 `.lower()` 是已注册 node_type 即识别。未注册的大写名报错并列可用类型。详见第 13 节。

## 3. 表达式语义边界

### 支持的 Python 表达式

| 写法 | 编译产物 | 精确语义 |
|------|----------|----------|
| 字面量 `1` `"x"` `True` `None` `[1,2]` `{"a":1}` | 原值 | dict key 必须是常量 |
| `INPUT.x` / `NODE.r.data` / `GLOBAL.k` / `PARENT.x` / `ENV.K` | `$INPUT.x` / `$NODE.r.data` / … | 命名空间变量 |
| `obj.path[0]` / `obj.path[-1]` | `$obj.path[0]` / `$obj.path[-1]` | 仅整数常量下标 |
| `F.foo(a, b)` | `$F.foo(a, b)` | 注册的表达式函数 |
| `len(x)` / `abs(x)` / `round(x)` | `$F.len` / `$F.abs` / `$F.round` | 真 Python 内置语义 |
| `str(x)` | `$F.concat(x)` | `"".join(str(a) for a in args)`，单参与 `str()` 等价 |
| `a + b` `-` `*` `/` `%` `**` | `$F.add/sub/mul/div/mod/pow` | 直接对应 Python 运算符，类型决定语义，编译期不查类型 |

### 比较与逻辑（`if` 判断位置与任意表达式位置均可）

```python
if INPUT.age >= 18 and INPUT.vip == True:   # ✅
if not (INPUT.status == "blocked"):          # ✅
    ...
label = "pass" if INPUT.score >= 60 else "low"   # ✅ 三元也可作表达式
```

`>=` `>` `<=` `<` `==` `!=` `in` `and` `or` `not` 与三元 `a if c else b` 均可写在赋值/return 等任意表达式位置，分别编译为 `$F.*` 比较与逻辑函数；三元编译为 `$F.ifelse`（**急切求值，两支都会算**——分支里不要放 None 不安全的算术）。

### 不支持、会报错的写法与改写

| ❌ | ✅ |
|----|----|
| `f"hi {name}"` | `F.concat("hi ", INPUT.name)` |
| 列表/集合/字典推导式 | `MAP` / `FILTER` 节点 |
| `lambda` / `await` / `:=` / `*args` 解包 | 拆成节点或普通赋值 |
| `"x".upper()` 等字面量/非 F 方法调用 | 用 `F.upper(...)` 等表达式函数 |
| 集合字面量 `{1,2}` | 列表 `[1, 2]` |
| `return HTTP.post(...)` | `r = HTTP.post(...); return r.data` |

> **`+` 多态陷阱**：`name + "!"` → `$F.add($INPUT.name, "!")`，运行时按 Python `+` 多态。要强制字符串拼接用 `F.concat(...)`。

## 4. 控制流

### if / elif / else

```python
@flow("grade")
def grade(INPUT):
    if INPUT.score >= 90:
        return "A"
    elif INPUT.score >= 60:
        return "B"
    else:
        return "C"
```

### 赋值 + return

```python
@flow("greet")
def greet(INPUT):
    name = F.upper(INPUT.name)
    return F.concat("hi ", name)
```

赋值生成 `assignment` 节点，后续用变量名引用其输出。**赋值后必须接 return 或后续语句**，否则报"赋值后悬空"。

### while / for-head WHILE（条件循环）

```python
@flow("countdown")
def countdown(INPUT):
    seed = INPUT.n
    for w in WHILE(item == None or item.left > 0, id="wcd", max_iterations=50):
        left = F.sub(F.ifelse(item == None, seed, item.left), F.ifelse(item == None, 0, 1))
        return {"left": left}
    return NODE.wcd
# run(n=3) -> {"left": 0}
```

- **循环体必须以 return 结束**：return 值即下一轮的 `item`（首轮为 `None`），也是循环结束后节点的输出；子流程状态与父隔离，体内 assignment 不写回父侧。
- 条件与体内可用 `item`（上一轮返回值）与 `rounds`（轮次，从 0 起）；首轮 bootstrap 惯用 `while rounds == 0 or item.xxx:`。`item` 打 None 安全（点路径返回 None、None 参与比较记 False）。
- **体内裸 `INPUT` 指子流程输入（只有 item/index），静默 None 是陷阱**——外层原始输入写 `PARENT.INPUT.<名>`；外层已赋值变量裸用名字即可（自动映射 `$PARENT` 快照）。
- `while` 语句形态节点自动 id、输出不可命名引用；要取循环最终态用 `for w in WHILE(cond, id="w")` 形态（可选 `max_iterations=` 覆盖引擎默认 1000）。
- 唯一退出通道是条件转假（引擎 `max_iterations=1000` 兜底，业务上限应写进条件）；**不支持 break / continue / while-else**。

## 5. 集合节点

### MAP

```python
@flow("double_numbers")
def double_numbers(INPUT):
    for x in MAP(INPUT.numbers, id="dbl"):
        return F.mul(x, 2)
    return NODE.dbl
# run(numbers=[1,2,3,4]) -> [2,4,6,8]
```

### FILTER（子流程返回 bool）

```python
@flow("evens")
def evens(INPUT):
    for x in FILTER(INPUT.nums, id="flt"):
        if F.mod(x, 2) == 0:
            return True
        return False
    return NODE.flt
```

### FIND（返回首个命中元素）

```python
@flow("first_even")
def first_even(INPUT):
    for x in FIND(INPUT.nums, id="fd"):
        if F.mod(x, 2) == 0:
            return True
        return False
    return NODE.fd
```

### REDUCE（累积值 `first`，当前元素 `second`；循环目标必须元组解包）

```python
@flow("sum_nums")
def sum_nums(INPUT):
    for (first, second) in REDUCE(INPUT.nums, id="rdc", initial=0):
        return F.add(first, second)
    return NODE.rdc
# run(nums=[1,2,3,4]) -> 10
```

> 不能写 `for x in REDUCE(...)` 再用 `x.first`/`x.second`——编译报 `REDUCE 的循环变量必须是 (first, second) 两个名字`。

> 可选 `initial` 指定初始值（`0`/`[]`/`""` 这类 falsy 值也是有效初始值）。

## 6. 子流程 @childflow + CHILD

```python
@childflow()
def double_each(INPUT):
    return F.mul(INPUT.item, 2)

@flow("double_via_child")
def double_via_child(INPUT):
    r = CHILD(input={"item": INPUT.payload}, flow=double_each)
    return r
# run(payload=21) -> 42
```

- `CHILD` 的 `input` 恒为 dict（`$INPUT` 已不支持数组形态）。
- 运行期生成时，源码里可含**多个 `@childflow` + 一个 `@flow` 主函数**，`flow_from_source` 自动收集子流程注册表，主流程用 `flow=<name>` 引用。有多个候选主流程时用 `flow_from_source(src, flow_id="bar")` 指定。

## 7. HTTP + 错误处理

```python
@flow("create_user", desc="创建用户")
def create_user(INPUT):
    if INPUT.age >= 18:
        resp = HTTP.post(
            url="https://api.example.com/users",
            body={"name": INPUT.name},
            timeout="PT5S",
            on_error=ErrorHandler("continue_with", default={"data": None}),
        )
        return resp.data
    return "未成年"
```

- `HTTP.post/get/put/delete/...` 只能作语句或赋值右侧。
- `ErrorHandler("continue_with", default=...)`：失败时用 `default` 兜底，不抛错。
- 需要 `pip install plaita[http]`。

## 8. 并行 PARALLEL

`branches` 用 **dict 字面量**`{名: 子流程}` 或 **`(名, 子流程)` 元组列表**（不是 `[{name, flow, input}]`）。要等待结果汇合的分支名用关键字 **`join=`**（不是 `join_branches=`）。`mode` 可选 `"thread"`/`"process"`/`"artificial"`，默认 `"thread"`；**不要用 `"coroutine"`**——已下线，sync 执行路径不可用（分支内节点表达式求值拿到 `None`，静默产出 `__parallel_error__`）。返回 dict `{分支名: result}`。

```python
@childflow()
def sub_a(INPUT):
    return F.mul(INPUT.x, 2)

@childflow()
def sub_b(INPUT):
    return F.add(INPUT.x, 10)

@flow("fan_out")
def fan_out(INPUT):
    r = PARALLEL(
        branches={"a": sub_a, "b": sub_b},
        join=["a", "b"],
        mode="thread",
    )
    return r
```

> **已知限制**：`@flow` 的 PARALLEL 编译期**不接受 per-branch `input`**（分支只带 `name` + `flow`，写 per-branch input 报 `PARALLEL 分支需要 (name, flow) 元组`）。分支子流程**可以**直接读主流程输入（`INPUT.x` / `GLOBAL.x`，实测编译运行通过）；要给不同分支喂不同输入，改用 `CHILD` 串行编排，或在 builder/JSON 层构建 Parallel 节点（那里支持 per-branch `input`）。

## 9. 编译期校验报错对照

| 反模式 | 报错 |
|--------|------|
| 用了未定义的名字 | `[codeflow] 第 N 行: 未知名字 'xxx'` |
| 节点调用嵌在表达式里 | `HTTP(...) 是节点调用，只能作为语句或赋值右侧`（自定义节点同理：`LLM(...) 是节点调用，只能...`） |
| 赋值后悬空 | `赋值 xxx 之后悬空：请补 return 或后续语句` |
| f-string / 推导式 / lambda | 带重写提示（见第 3 节） |
| 非 `F.*` 的方法/函数调用 | `不支持的调用 Xxx.yyy(...)：…` |
| 大写占位名但未注册 | `未注册的自定义节点 FOO(...)：node_type 'foo' 不在 registry 中。可用类型：[...]` |
| 自定义节点传位置参数 | `自定义节点 xxx 只接受关键字参数（字段名=值），不支持位置参数` |
| 自定义节点赋值同时传 `id=` | `自定义节点已用赋值变量 'a' 作为 id，不要同时传 id=` |

## 10. 已知边界

- **`@flow` 函数须定义在模块级**（`inspect.getsource`）。运行期动态生成改用 `flow_from_source(src)`。
- **节点调用位置受限**：`HTTP`/`CODE`/`EVENT`/`CHILD`/`REFERENCE`/`PARALLEL`/集合调用/**自定义节点（`LLM`/`RETRIEVE`/...）** 只能作语句或赋值右侧。
- **`str(x)` 是近似映射**：编译成 `$F.concat(x)`，语义是"拼成字符串"。
- **`+` 多态、编译期不查类型**：要强制字符串拼接用 `F.concat(...)`。
- **自定义节点字段类型**：pydantic 字段若是 `int`/`bool` 等强类型，传表达式串（`$INPUT.x`）会在构建 `Flow` 时被拒。接表达式的字段声明成 `Optional[Any]`/`Optional[str]`。

## 11. 自定义节点（注册的 Node 子类）

`@flow` 可直接调用注册到 `NodeRegistry` 的自定义 `Node` 子类，占位符名 = `node_type` 大写。这让 AI 生成的 `@flow` 能编排业务动作节点（LLM/检索/工具/领域 action），无需降级到 JSON IR。

```python
from typing import ClassVar, Optional

from plaita import Node
from plaita.node import get_default_registry
from plaita.dsl.codeflow import flow_from_source

class LLMNode(Node):
    node_type: ClassVar[str] = "llm"
    prompt: Optional[str] = None
    system: Optional[str] = None
    model: Optional[str] = None
    def execute(self, execution):
        prompt = execution.evaluate(self.prompt) if self.prompt else ""
        ...

get_default_registry().register(LLMNode)

src = '''
@flow("answer", desc="带资料回答")
def answer(INPUT):
    docs = RETRIEVE(query=INPUT.question, library="kb", top_k=2)
    ans = LLM(model="responder", system="只根据资料回答",
              prompt="资料：{% $NODE.docs %}\\n问题：{% $INPUT.question %}")
    return ans
'''
flow_from_source(src).run(question="plaita 是什么")
```

规则：

- **占位符名 = `node_type` 大写**（`llm`→`LLM`、`retrieve`→`RETRIEVE`、`tool`→`TOOL`）。编译期查 registry 识别。
- **只能作语句或赋值右侧**（同 `HTTP`/`CHILD`）：`return LLM(...)` 拆成 `r = LLM(...); return r`。
- **只接受关键字参数**（`字段名=值`），字段名须与 `Node` 子类 pydantic 字段（snake_case）一致；不支持位置参数。
- **字段值经表达式编译**：`INPUT.x`→`$INPUT.x`、字面量原样、`{% ... %}` 模板字符串原样透传给节点 `execute` 求值。
- **节点 id**：赋值时用变量名（`a = LLM(...)` → id `a`，`a` 引用为 `$NODE.a`）；表达式语句可用 `id="name"` 命名；赋值时不要同时传 `id=`。
- **通用字段**：`id=`、`timeout=`、`on_error=ErrorHandler(...)` 对所有自定义节点有效（走基类 `Node` 的 `timeout`/`errorHandler`）；其余 kwargs 按名进 IR。
- **未注册大写名 → 编译期报错列可用类型**，供 AI 自纠。
- **内置专用占位符优先**：`HTTP`/`CODE`/... 走各自硬编码分支，不要注册同名自定义 node_type。
- **字段类型**：接表达式的字段用 `Optional[Any]`/`Optional[str]`，避免 `int`/`bool` 强类型拒绝表达式串。

## 12. 生成即执行闭环模板

```python
from plaita.dsl.codeflow import flow_from_source, compile_source

src = '''
@flow("greet", desc="打招呼")
def greet(INPUT):
    name = F.upper(INPUT.name)
    return F.concat("hi ", name)
'''

# 1. 编译校验
try:
    ir = compile_source(src)
except Exception as e:
    print("编译失败:", e)
    raise

# 2. 构建并执行
flow = flow_from_source(src)
print(flow.run(name="alice"))   # -> "hi ALICE"
```

## 13. API 速查

| 入口 | 作用 |
|------|------|
| `plaita.dsl.codeflow.flow_from_source(src, ...)` | 源码字符串 → `Flow`（运行期生成，无源文件依赖） |
| `plaita.dsl.codeflow.compile_source(src, ...)` | 源码字符串 → IR dict（不构建，用于校验/审计） |
| `plaita.dsl.codeflow.compile_func(fn, flow_id)` | Python 函数 → IR dict（不构建） |
| `plaita.dsl.codeflow.flow(flow_id, ...)` | 装饰器：Python 函数 → `Flow`（需模块级源文件） |
| `plaita.dsl.codeflow.childflow(...)` | 装饰器：子流程函数 |
| 占位符 | `HTTP CODE EVENT MAP FILTER FIND LOOP REDUCE WHILE CHILD REFERENCE PARALLEL` + 自定义节点（`node_type` 大写，如 `LLM`/`RETRIEVE`/`TOOL`，须已注册） |
| 命名空间 | `F INPUT NODE GLOBAL PARENT ENV` |
| 错误处理 | `ErrorHandler(...)` |
