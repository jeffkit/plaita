---
name: plaita-flow-builder
description: 根据用户需求生成符合 plaita 规范的 Plaita 逻辑编排 flow。支持 JSON / YAML / Python DSL 三种产出形态，配置文件默认用 YAML。当用户提到「编排一个流程」「写一个 plaita flow」「生成流程 JSON/YAML」「用 plaita 实现某某逻辑」「把这个业务流程转成 flow」「plaita 流程怎么写」时使用此 Skill。即使用户没有明确说格式或「skill」，只要意图是用 Plaita/plaita 编排逻辑、生成流程定义，就应触发。
version: 0.2.0
---

# Plaita Flow Builder

把用户的自然语言需求翻译成 plaita 可直接执行的 **flow 定义**。

## 三种产出形态

plaita 的运行时只认 dict，JSON / YAML / Python DSL 只是同一份 flow 的不同序列化/抽象层，运行时行为完全一致。**按下面优先级选择产出形态：**

1. **配置文件 → 默认用 YAML**：可读性高、支持 `#` 注释、多行 `code` 字段不用 `\n` 转义。需要 `pip install logic-plaita[yaml]`。
2. **在 Python 代码里声明 / 写测试 / 生成模板 → 用 Python DSL**（`plaita.dsl`）：IDE 自动补全 + 构建期静态校验，把反模式在 build 时拦掉。
3. **与可视化编排工具互通 / 旧系统对接 → 用 JSON**：保持历史行为，无额外依赖。

> 除非用户明确要 JSON，或场景是工具互通，**否则优先产出 YAML**。

## 核心心智模型

Plaita flow 是一段静态定义：`flow_id` + `inputType` + `nodes[]`。节点之间用 `next` 串成线性流，分支节点用 `branches`/`else_next` 描述多路跳转。节点字段里的 `output`/`condition`/`input` 用 **`$` 前缀表达式** 引用上下文数据并调用内置函数。

理解这三件事就理解了 Plaita：
1. **节点**是逻辑单元（赋值、判断、循环、HTTP、代码……）
2. **`next`** 是控制流（含分支节点的多路跳转）
3. **表达式**是数据胶水（`$INPUT`/`$NODE`/`$GLOBAL`/`$F.func(...)`）

## 工作流程

### 1. 先把需求拆成「输入 → 处理 → 输出」

在动手写 JSON 之前，先用一两句话向自己（或用户）说清楚：
- **输入**：流程接收什么参数？是单个值、对象还是数组？→ 决定 `inputType.dataType`
- **处理**：要经过哪些步骤？有没有分支、循环、并行、外部调用？→ 决定节点列表与 `next` 拓扑
- **输出**：最终返回什么？正常结果还是业务错误？→ 决定 `end` 节点的 `resultType` 与 `output`

如果需求里有歧义（例如「判断年龄」没说阈值、循环没说集合来源），**先问用户**，不要瞎猜。宁可多问一句也不要产出一个看起来对、但语义错的流程。

### 2. 选择节点

完整节点字段与行为见 `references/nodes.md`。速查：

| 需求 | 节点 |
|------|------|
| 流程起点 / 终点 | `start` / `end` |
| 求值、拼接、转换 | `assignment` |
| 二分支（真/假） | `if`（`next` 真，`else_next` 假） |
| 多路条件跳转 | `switch`（`branches` + `priority` + `isDefault`） |
| 等值匹配 | `case`（`target` + `cases[]`） |
| 遍历集合 | `loop` / `map` / `filter` / `find` / `reduce` |
| 调用子流程 | `child`（共享父上下文）/ `reference`（独立） |
| 并行多分支 | `parallel` |
| 跑一段代码 | `code`（需 `code` extra） |
| 发 HTTP 请求 | `http`（需 `http` extra） |
| 等外部事件 | `event`（断点续执） |

需要某节点的**确切字段名**时，去读 `references/nodes.md` 对应小节，不要凭记忆写——字段名错了流程会跑不起来。

### 3. 写表达式

完整语法见 `references/expressions.md`。最关键的几条：

- 变量引用用 **`$`**：`$INPUT.name`、`$NODE.assign.field`、`$GLOBAL.key`、`$ENV.PATH`
- 函数调用用 **`$F.`**：`$F.upper($INPUT.name)`、`$F.concat(a, '-', b)`、`$F.len($NODE.items)`
- **纯表达式**直接写：`"output": "$INPUT.name"`
- **表达式是字符串的一部分**用 `{% %}` 插值：`"output": "你好，{% $INPUT.name %}"`
- ❌ **千万别写 `${INPUT.name}`**——那是错误语法，会被当普通字符串原样返回

### 4. 产出 flow 定义（优先 YAML）

固定结构（三种形态字段完全一致，只是序列化不同）：

**YAML（配置文件首选）**：

```yaml
flow_id: <唯一标识>
inputType: { dataType: object }
nodes:
  - { type: start, id: start, next: <第一个业务节点 id> }
  # ...业务节点...
  - { type: end, id: end, output: <表达式>, resultType: success }
```

**JSON（工具互通/旧系统）**：

```json
{
  "flow_id": "<唯一标识>",
  "inputType": { "dataType": "object" },
  "nodes": [
    { "type": "start", "id": "start", "next": "<第一个业务节点 id>" },
    { "type": "end", "id": "end", "output": "<表达式>", "resultType": "success" }
  ]
}
```

**Python DSL（代码内声明/测试/模板，带构建期校验）**：

```python
from plaita.dsl import build, start, end, if_, cond

flow = (
    build("<唯一标识>", input_type="object")
    .add(start(next="<第一个业务节点 id>"))
    # ...业务节点...
    .add(end(id="end", output="<表达式>"))
    .build()
)
```

线性流程可用 `plaita.dsl.linear` 省掉所有显式 `next` 和非分支节点的 `id`，节点按声明顺序自动串接，分支用 `then`/`else_` 标签跳转：

```python
from plaita.dsl import linear, cond

flow = (
    linear("adult_check", input_type="object")
    .start()
    .if_(condition=cond("$INPUT.age", ">=", 18), then="adult", else_="minor")
    .end("adult", output="成年")
    .end("minor", output="未成年")
    .build()
)
```

`flow.run(...)` 统一接受 **dict 或关键字参数**，`$INPUT` 始终是传入的 dict：

| 调用方式 | `$INPUT` |
|----------|----------|
| `flow.run(name="x")` | `{"name": "x"}` |
| `flow.run({"name": "x"})` | 该 dict |
| `flow.run()` | `{}` |

> 历史 scalar/array 位置传参（`flow.run("x")` / `flow.run(a, b, c)`）已移除；
> 标量/数组输入用 dict 包装并在表达式里引用对应字段（如 `$INPUT.value` / `$INPUT.items`）。

> 三种形态的加载入口：`Flow.from_string(s)`（自动识别 JSON/YAML）、`Flow.from_file(path)`（按后缀 `.json`/`.yaml`/`.yml`）、`plaita.dsl.build(...).build()`。完整对照见 `docs-site/docs/guide/yaml-and-dsl.md`。

## 写好 flow 的关键约束

这些是出错高发点，务必遵守：

1. **每个节点都要有唯一 `id`**，`next` 指向的必须是存在的节点 id。
2. **分支节点的跳转**：`if` 用 `next`+`else_next`；`switch` 的 `branches[].next` + 一个 `isDefault: true` 的默认分支；`case` 用 `cases[].next` + 可选 `default`。
3. **集合类节点**（`loop`/`map`/`filter`/`find`/`reduce`）的子流程放在 `childFlow` 里，子流程输入是 `item`+`index`（`reduce` 是 `first`+`second`）。
4. **`end` 节点**：`resultType: success` 求值 `output` 返回；`resultType: error` 抛 `FlowResultError`，用 `error: { code, message }`；`resultType: nop` 返回 `None`。
5. **超时**用 ISO 8601：`PT5S`/`PT1M`/`PT1H`，可加在流程级或节点级。
6. **错误处理**：节点可配 `errorHandler.strategy`：`abort`（默认中止）/ `continue`（忽略继续）/ `continue_with`（用 `defaultValue` 继续）。
7. **并发副作用**：`map.concurrent` 与 `parallel` 会并发，不要在里面用 `pop`/`set`/`clear` 等带副作用函数，也不要在共享上下文写竞争数据。
8. **`code`/`http` 节点需要 extra**：`pip install logic-plaita[code]` / `logic-plaita[http]`。生成这两个节点时，在交付说明里提醒用户装对应 extra。

## 交付规范

生成 flow 后，**不要只丢一段定义**。按这个结构交付：

1. **一句话意图复述**：确认你对需求的理解（输入/处理/输出）。
2. **flow 定义代码块**：默认用 **YAML**；用户明确要 JSON、或场景是工具互通时用 JSON；用户在 Python 代码里要用时给 DSL。代码块要带语言标注（```yaml / ```json / ```python）。
3. **节点拓扑说明**：用简短一行描述控制流，例如 `start → check(if) → [adult/minor] → end`。
4. **执行示例**：给出对应的 `flow.run(...)` 调用，让用户能直接验证。
5. **注意事项**：用到的 extra（`http`/`code`/`yaml`）、可能的边界情况、需要用户确认的假设。产出 YAML 时提醒 `pip install logic-plaita[yaml]`。

如果用户给了保存路径：YAML 用 `.yaml`/`.yml` 后缀，JSON 用 `.json` 后缀——`Flow.from_file` 按后缀选择解析器。否则在对话里展示。

## 常见反模式（避免）

- ❌ 用 `${INPUT.x}` 而不是 `$INPUT.x`
- ❌ `if` 节点忘了 `else_next`（若用户只需单分支，可以让 `else_next` 指向同一个 `end`）
- ❌ `switch` 没有 `isDefault` 分支，导致全部条件不命中时行为未定义
- ❌ 集合节点把逻辑写在 `output` 而不是 `childFlow` 里
- ❌ 节点 `id` 重名或 `next` 指向不存在的节点
- ❌ 为了「简洁」省略 `inputType`，导致传参方式不明

## 深入参考

需要查具体字段时读对应文件，不要把整个节点表背下来：

- `references/nodes.md` —— 全部内置节点的字段、行为与完整示例
- `references/expressions.md` —— 表达式语法、命名空间、60+ 内置函数分类
- `docs-site/docs/guide/yaml-and-dsl.md` —— YAML / Python DSL（含 `linear` 隐式 next）完整对照与节点工厂速查

当用户的需求涉及断点续执 / 分布式执行 / 事件节点等高级场景时，先读 `references/nodes.md` 的 `event` 小节，再决定 flow 结构。

> 用 Python DSL 交付时，`build()` 会自动做构建期校验：节点 id 重复、`next`/`else_next` 指向不存在的节点、`switch` 缺 `isDefault` 分支等反模式会在 build 时直接抛错——交付前务必先 `.build()` 跑一遍，避免把坏 flow 交给用户。
