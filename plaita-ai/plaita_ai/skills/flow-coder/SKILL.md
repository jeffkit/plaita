---
name: flow-coder
description: 用 plaita 的 @flow DSL 把自然语言需求编译成可执行流程并运行。当用户要求"用 @flow 实现"、"生成一个 flow"、"编排一个流程/Agent/工作流并执行"时使用本技能。技能指导 AI 产出 @flow 源码 → 编译期校验 → 运行期执行 → 错误回灌自纠，形成闭环。
---

# flow-coder：用 @flow 生成并执行流程

你是一个 plaita `@flow` 流程工程师。你的职责是把用户的自然语言需求翻译成 **`@flow` Python DSL 源码**，编译校验后执行，拿到结果。**不要**生成任意 Python 让用户自己跑——要用 plaita 提供的安全入口 `flow_from_source` / `compile_source`，它带 AST 静态校验与受限表达式层这两道护栏。

## 何时使用

- 用户说"用 @flow 实现 …"、"生成一个 flow 做 …"、"编排一个流程并跑一下"。
- 用户描述的需求可以被拆成**有向步骤**：条件分支、循环/集合处理、HTTP 调用、子流程、并行、工具调用等。
- 用户希望**生成即执行**，而不是只产出代码片段。

不要用于：纯一次性算术、单次字符串处理这类没有"流程"语义的任务（直接写 Python 更合适）。

## MCP 插件（推荐）

若环境已配置 **plaita-ai MCP**（`plaita-ai mcp`），优先通过 MCP 工具执行，不要手写 `import plaita`：

| MCP Tool | 作用 |
|----------|------|
| `flow_compile` | 编译校验 `@flow` 源码，返回 `{ok, errors:[{line,message}]}` |
| `flow_run` | 编译并执行（`inputs_json` / `globals_json` 为 JSON 字符串） |
| `flow_list_nodes` | 列出可用节点类型（自定义节点占位符大写名） |
| `flow_get_skill` | 拉取本 skill 全文（无本地 skill 时） |

闭环：**生成源码 → `flow_compile` → 失败则带行号错误重写 → `flow_run`**。

CLI 等价命令（与 MCP 共用内核）：`plaita-ai compile` / `plaita-ai run`。

## 核心 API（必读）

入口都在 `plaita.dsl.codeflow`：

| 入口 | 作用 |
|------|------|
| `flow_from_source(src, **kw)` | **运行期生成**：源码字符串 → `Flow`，可直接 `.run(**input)`。AI 生成场景就用它。 |
| `compile_source(src, **kw)` | 源码字符串 → IR dict，**不构建**。用于先校验/审计再执行。校验失败抛 `_CodeflowError`（带行号）。 |
| `flow / childflow` | 装饰器，用于写在源文件里的静态函数。**AI 动态生成不要用装饰器路径**——动态函数没有源文件，`inspect.getsource` 会失败。一律走 `flow_from_source`。 |

一个最小的"生成即执行"闭环：

```python
from plaita.dsl.codeflow import flow_from_source, compile_source

src = '''
@flow("greet", desc="打招呼")
def greet(INPUT):
    name = F.upper(INPUT.name)
    return F.concat("hi ", name)
'''

# 1. 先编译校验（失败会抛带行号的异常）
ir = compile_source(src)
# 2. 构建并执行
flow = flow_from_source(src)
print(flow.run(name="alice"))   # -> "hi ALICE"
```

## @flow DSL 速查（完整版见 references/codeflow-reference.md）

### 函数骨架

```python
@flow("<flow_id>", desc="<说明>")
def <name>(INPUT):
    # if / elif / else / for / return / 赋值
    return <expr>
```

- `INPUT` / `F` / `NODE` / `GLOBAL` / `PARENT` / `ENV` / `HTTP` / `CODE` / `EVENT` / `MAP` / `FILTER` / `FIND` / `LOOP` / `REDUCE` / `CHILD` / `REFERENCE` / `PARALLEL` 这些名字**不需要 import**，是 AST 编译期识别的占位符。
- **自定义节点占位符**：任何注册到 `NodeRegistry` 的 `Node` 子类，用 `node_type` 大写化作占位符即可在 `@flow` 里直接调用（如 `node_type="llm"` → `LLM(prompt=..., model=...)`、`"retrieve"` → `RETRIEVE(query=...)`、`"tool"` → `TOOL(action=...)`）。编译期查 registry，未注册的大写名会报错并列出可用类型，便于自纠。详见下方「自定义节点」。
- **不要写 `input_type`**：该参数已废弃且被忽略。`$INPUT` 恒为 `run()` 传入的 dict，字段用 `INPUT.x` 访问。
- **函数体从不被当 Python 执行**，只做静态 AST 分析。所以写法必须在支持子集内，否则编译期报错。

### 表达式映射（你写的 Python → 编译成的表达式）

| 你写的 | 编译成 | 备注 |
|--------|--------|------|
| `INPUT.age` | `$INPUT.age` | 命名空间变量 |
| `NODE.r.data` | `$NODE.r.data` | 引用上游节点输出 |
| `F.upper(x)` | `$F.upper(x)` | 调用注册的表达式函数 |
| `len(x)` / `abs(x)` / `round(x)` | `$F.len/abs/round` | 真 Python 内置语义 |
| `str(x)` | `$F.concat(x)` | 近似映射，单参与 `str()` 等价 |
| `a + b` `-` `*` `/` `%` `**` | `$F.add/sub/mul/div/mod/pow` | 类型决定语义，编译期不查类型 |
| `obj.path[0]` `[-1]` | `$obj.path[0]` | 仅整数常量下标 |

### 只能出现在 `if` 判断位置的比较/逻辑

`>=` `==` `!=` `in` `and` `or` `not` **只能写在 `if`/`elif` 的条件里**，不能写在赋值或 return 的表达式位置。

```python
if INPUT.age >= 18 and INPUT.vip == True:   # ✅
    return "adult-vip"
return "minor"
```

### 会报错的写法（必须改写）

| ❌ 写法 | ✅ 改写 |
|--------|--------|
| `f"hi {name}"` | `F.concat("hi ", INPUT.name)` |
| `a if c else b` | `if/else` 语句分支 |
| 列表/字典/集合推导式 | `MAP` / `FILTER` 节点 |
| `lambda` / `await` / `:=` / `*args` | 拆成节点或普通赋值 |
| `"x".upper()` 等字面量方法调用 | 用 `F.upper(...)` 等表达式函数 |
| `return HTTP.post(...)` 节点调用嵌在表达式 | `resp = HTTP.post(...); return resp.data` |
| 集合字面量 `{1,2}` | 用列表 `[1, 2]` |

> **`+` 的多态陷阱**：`name + "!"` 编译成 `$F.add($INPUT.name, "!")`，运行时按 Python `+` 多态。要强制字符串拼接且对非字符串报错，显式用 `F.concat(...)`。

### if / elif / else、赋值、return

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

赋值生成 `assignment` 节点，后续用变量名引用其输出：

```python
@flow("greet")
def greet(INPUT):
    name = F.upper(INPUT.name)
    return F.concat("hi ", name)
```

### 集合节点：MAP / FILTER / FIND / LOOP

用 `for x in MAP(...):` 语法，子流程写在函数体里：

```python
@flow("double_numbers")
def double_numbers(INPUT):
    for x in MAP(INPUT.numbers, id="dbl"):
        return F.mul(x, 2)
    return NODE.dbl

double_numbers.run(numbers=[1, 2, 3, 4])   # -> [2, 4, 6, 8]
```

`FILTER` / `FIND` 子流程要返回 bool，惯例 `if ... return True` / `return False`：

```python
@flow("first_even")
def first_even(INPUT):
    for x in FIND(INPUT.nums, id="fd"):
        if F.mod(x, 2) == 0:
            return True
        return False
    return NODE.fd
```

### 子流程：@childflow + CHILD

```python
@childflow()
def double_each(INPUT):
    return F.mul(INPUT.item, 2)

@flow("double_via_child")
def double_via_child(INPUT):
    r = CHILD(input={"item": INPUT.payload}, flow=double_each)
    return r
```

> `CHILD` 的 `input` 恒为 dict（`$INPUT` 已不支持数组形态）。
> **运行期生成时**，源码里可同时含多个 `@childflow` 函数 + 一个 `@flow` 主函数，`flow_from_source` 会自动收集子流程注册表，主流程用 `flow=<name>` 引用。

### HTTP 调用 + 错误处理

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

`HTTP.post/get/...` 只能作语句或赋值右侧，不能嵌在表达式里。`ErrorHandler(strategy, default=...)` 控制失败时是 `continue_with`（用 default 兜底）还是抛错。需要 `pip install plaita[http]`。

### 并行：PARALLEL

`branches` 用 **dict 字面量**`{名: 子流程}` 或 **`(名, 子流程)` 元组列表**；要等待结果汇合的分支名用 **`join=`**（不是 `join_branches=`）。返回 dict `{分支名: result}`。

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

`mode` 可选 `"thread"` / `"process"` / `"coroutine"` / `"artificial"`，默认 `"thread"`。

> **已知限制（重要）**：`@flow` 的 PARALLEL 编译期**不接受 per-branch `input`**——分支只能带 `name` + `flow`，子流程的输入由运行时分发。当前 plaita 运行时在 parallel 子流程里访问 `INPUT.x` / `PARENT.INPUT.x` / `GLOBAL.x` 会触发表达式求值 bug（`ExpressionParser._eval_variable missing 'tokens'`），**输入相关的并行分支在 `@flow` 源码模式下目前跑不通**。若任务必须按输入做并行扇出，建议改用 `CHILD` 串行编排或在 builder/JSON 层构建 Parallel 节点（那里支持 per-branch `input`），并在 prompt 里提示用户这一限制，不要在 `@flow` 里硬试 PARALLEL+输入。

### 自定义节点：LLM / 检索 / 工具 / 领域 action

业务自定义的 `Node` 子类（注册到 `NodeRegistry` 后）可在 `@flow` 源码里直接用，占位符名 = `node_type` 大写。这让生成即执行的闭环能直接编排业务动作节点，而不必降级到 JSON IR。

```python
# 业务侧先注册节点（须在 flow_from_source 之前）
from typing import ClassVar, Optional

from plaita import Node
from plaita.node import get_default_registry

class LLMNode(Node):
    node_type: ClassVar[str] = "llm"
    prompt: Optional[str] = None
    system: Optional[str] = None
    model: Optional[str] = None
    def execute(self, execution): ...

get_default_registry().register(LLMNode)
```

```python
src = '''
@flow("rag", desc="检索+回答")
def rag(INPUT):
    docs = RETRIEVE(query=INPUT.q, library="kb", top_k=2)
    ans = LLM(model="responder", system="只根据资料回答",
              prompt="资料：{% $NODE.docs %}\\n问题：{% $INPUT.q %}")
    return ans
'''
flow_from_source(src).run(q="plaita 是什么")
```

规则：
- 占位符名 = `node_type` 大写（`llm`→`LLM`、`retrieve`→`RETRIEVE`、`tool`→`TOOL`）。
- **只能作语句或赋值右侧**（同 `HTTP`/`CHILD`）：`return LLM(...)` 要拆成 `r = LLM(...); return r`。
- **只接受关键字参数**（`字段名=值`），字段名须与 `Node` 子类的 pydantic 字段（snake_case）一致。
- 字段值经表达式编译：`INPUT.x`→`$INPUT.x`、字面量原样、`{% ... %}` 模板字符串原样透传给节点 `execute`。
- 节点 id：赋值时用变量名（`a = LLM(...)` → id `a`，`a` 引用为 `$NODE.a`）；表达式语句可用 `id="name"`。赋值时不要同时传 `id=`。
- 通用字段：`id=`、`timeout=`、`on_error=ErrorHandler(...)` 对所有自定义节点有效。
- 接表达式的字段声明成 `Optional[Any]`/`Optional[str]`，避免 `int`/`bool` 强类型拒绝表达式串。
- 未注册的大写名 → 编译期报错列可用类型：`未注册的自定义节点 FOO(...)：node_type 'foo' 不在 registry 中。可用类型：[...]`——把错误回灌 LLM 自纠即可。

## 工作流（每次执行都按这五步走）

### 第 1 步：理解需求

把用户需求拆成：输入字段、输出形态、步骤（条件/循环/调用/并行）、是否需要子流程。若信息不足以确定行为，**先向用户提问**，不要猜。

### 第 2 步：生成 @flow 源码

写一段**自包含**的 `@flow` 源码字符串（含必要的 `@childflow`）。务必：
- 不写 `input_type`（已废弃）；字段全从 `INPUT.x` 取。
- 节点调用（`HTTP`/`CHILD`/`PARALLEL`/`MAP` 等）只作语句或赋值右侧。
- 字符串拼接用 `F.concat`，不要用 f-string 或 `+`（除非确认两端都是数字/列表）。
- 每条分支路径都有 `return`，避免"赋值后悬空"。

### 第 3 步：编译期校验

```python
from plaita.dsl.codeflow import compile_source
try:
    ir = compile_source(src)
except Exception as e:
    print("编译失败:", e)   # 带行号与重写提示
```

**校验失败不要放弃**——进入第 5 步自纠。

### 第 4 步：执行

```python
from plaita.dsl.codeflow import flow_from_source
flow = flow_from_source(src)
result = flow.run(**inputs)   # inputs 与 INPUT 字段对应
print(result)
```

执行时如果用到 HTTP，确保装了 `plaita[http]`。如果需要注入 LLM / 工具 / 检索库等自定义节点，把它们注册后或放进 `flow.global_context` 再 `.run`。

### 第 5 步：错误回灌自纠（核心）

编译失败或执行报错时，把**完整错误信息**（含行号、报错文本）拼回 prompt，重新生成一版源码再试。最多重试 3 轮。常见错误对照：

| 错误 | 修复 |
|------|------|
| `未知名字 'xxx'` | 该名字没在占位符/变量表里；检查拼写或改成 `INPUT.x` / `NODE.x` |
| `节点调用只能作为语句或赋值右侧` | 把 `return HTTP(...)` 拆成 `r = HTTP(...); return r.data` |
| `not 要用在条件位置` | `not` 只能放 `if` 判断里，别放表达式 |
| `赋值 xxx 之后悬空` | 赋值后补 `return` 或后续语句 |
| f-string / 三元 / 推导式报错 | 按上文"会报错的写法"表改写 |
| `不支持的调用 Xxx.yyy(...)` | 表达式里只能调 `F.xxx(...)` 或 `len/abs/round/str` |
| `未注册的自定义节点 XXX(...)：node_type 'xxx' 不在 registry 中` | 占位符名拼写错 / 节点未注册；按报错里列出的可用类型修正名字，或先 `get_default_registry().register(...)` |

## 输出规范

每次完成一个需求，向用户交付：

1. **最终 `@flow` 源码**（用 ```python 代码块完整给出）。
2. **编译/执行结果**：跑了哪些输入、各自输出。
3. **自纠记录**（如有）：第几轮发现什么错、怎么改的。

如果用户只要源码不要执行，明确说"只生成不跑"，可跳过第 4 步，但仍要走第 3 步校验。

## 参考资料索引

- `references/codeflow-reference.md` —— @flow 完整语法、表达式语义边界、节点清单、已知边界、可运行示例集。**生成复杂流程前先查它**。
- 项目内文档：`docs-site/docs/guide/code-dsl.md`（@flow DSL 完整指南）、`docs-site/docs/scenarios/agent-orchestration.md`（Agent 编排模式）。
- 示例代码：`examples/agent/nodes.py`（自定义 LLM/Tool/Retriever 节点）、`examples/agent/flows/*.json`（三个端到端 Agent 案例）。
