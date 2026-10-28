# @flow 流程编写规范（权威单源）

> **定位**：本文件是 plaita `@flow` 流程编写的**单一权威规范**——收录语法参考不会告诉你的作者硬约束。约束按**拦截层级**两栏：**编译期强制**（编译器直接拒绝，报错带行号，把错误回灌即可自纠——见 §2.1/§2.2 与 §2.3 的 `**`/caps 门）与**运行期语义**（编译期不拦，跑起来才炸——如 §2.4/§2.5）。写之前分清约束落在哪一栏，别把注意力花在编译器已经拦的写法上。
> **语法细节**不在本文件，见 [codeflow-reference.md](codeflow-reference.md) 与 `docs-site/docs/guide/code-dsl.md`。
> **决议背景**：大仓 `docs/ADR-2026-08-27-orchestration-converge-on-plaita.md`（编排收敛 plaita、执行层统一 agentproc、mediaflow 全量迁移）。
> **同步约束**：改 `plaita/dsl/codeflow/**`、plaita-nodes 节点语义或大仓编排决议时，必须同步本文件（映射见 `plaita/docs/DOC_CODE_MAP.md`）。

---

## 0. 30 秒自检清单（每次交付前过一遍）

- [ ] 没有跨分支同名赋值（编译期即拒：变量名即节点 id，同一 id 只能出现一次）
- [ ] 没在 map/集合子流程的 end 里引用 if 块内赋值的节点（编译期报「未知名字」）；跨作用域聚合走了 MAP 输出（`NODE.<集合id>`）或 run 级报告通道
- [ ] 没把 `timeout=` 挪作他用（保留 kwargs，ISO 时长如 `PT5S`）
- [ ] 节点传参没用动态 `**` 展开（只认字符串键的 dict 字面量，逐键等价具名 kwargs；caps 字段直接写具名 kwargs）
- [ ] 接表达式的自定义节点字段声明为 `Optional[Any]` / `Optional[str]`，不是 `int`/`bool` 强类型
- [ ] 用 `Flow.run(**input)` 入口时 `$INPUT` 就是该 dict；若直接操作 `FlowExecution.run`，运行参数走 `params=`
- [ ] 大段 prompt 在业务节点预拼，flow 图里 `{% %}` 只引用预拼串
- [ ] 业务节点（agentrun/hitl/连接器）已注册：`import plaita_nodes` → `register_all()` → `get_default_registry()`，且在 `flow_from_source` **之前**
- [ ] dry-run 判断用 `$INPUT.dry_run`，节点内部语义用 `globalContext.dry_run`
- [ ] 发布 console 前确认 worker 侧能加载业务节点（`PLAITA_NODE_PATH` / `PLAITA_NODE_MODULES` / `PLAITA_PYTHON`）

---

## 1. 入口与运行语义

| 场景 | 入口 | 要点 |
|------|------|------|
| 仓内静态定义 | `@flow` 装饰器 | 函数必须定义在**模块级**（`inspect.getsource` 限制），局部函数编译不了 |
| 运行期生成 / AI 生成 | `flow_from_source(src)` | 无源文件依赖；源码内可含多个 `@childflow` + 一个主 `@flow` |
| 只校验不执行 | `compile_source(src)` / `compile_func(fn, id)` | 返回 IR dict，供审计/回放 |
| 编译落盘（产物 JSON） | `python -m plaita build <源码.py> -o <产物>.json` | canonical=console 正典形态（默认，字节稳定；`--format ir` 回退直出 IR）；`--register plaita_nodes` 显式注册业务节点、`--code-backend subprocess` 注册 CODE 节点（沙箱后端/语言白名单在 build 期即生效：`sandbox_backend` 不在 `PLAITA_SANDBOX_ALLOWED_BACKENDS`、`lang` 不在 `PLAITA_SANDBOX_ALLOWED_LANGUAGES` 内的源码会被**拒绝编译**，而不是落盘后在运行期才失败）、`--check` 供 CI 钉「产物落后源码」、`--embed-source` 把 @flow 源码写进 `metadata.source`（console 源码页签/跳转数据源，等效 mediaflow publish_console 注入）；产物路径防护（与源码同路径拒绝、退出码 2；`-o` 父目录按需创建；原子替换）。大仓子仓的 flow 编译脚本应调它而非自带样板（recursive `compile_v2.py` / issue-keeper `build_flows.py` 即薄壳范例，2026-10-09 收敛） |

- `input_type` / `output_type` **已废弃且被忽略**：`$INPUT` 恒为 dict，不要声明。
- **运行入口分两层，别混**：
  - `Flow.run(**input)`（含 `flow_from_source(src).run(**input)`）：kwargs 就是 `$INPUT`；
  - `FlowExecution().run(...)`（底层）：**kwargs 不进 `$INPUT`（确定的设计），运行参数走 `params=`**。
  - 业务仓统一走 `Flow.run`；文档/示例若出现 `FlowExecution.run`，参数必须显式 `params=`。

## 2. 作者硬约束（按拦截层级：编译期强制 / 运行期语义）

> **读法**：§2.1/§2.2 是**编译期强制**——写错立即报错（带行号），不用背，把报错回灌自纠即可；
> §2.4/§2.5 与 §4 的 dry-run 双通道是**运行期语义**——编译期不拦，跑起来才炸，这些才是要主动防的。

### 2.1 变量名即节点 id：跨分支同名赋值（编译期拒绝）

赋值变量名直接成为节点 id，**没有 phi 合并语义**。两个分支各自 `r = HTTP.get(...)` 会产生同名节点 id 的语义冲突，**编译器直接拒绝**（不再静默放行到运行期）：

```python expect-error
# 期望报错: 被多次赋值
@flow("bad")
def bad(INPUT):
    if INPUT.region == "eu":
        r = HTTP.get(url=INPUT.eu_url)
        return r.data
    r = HTTP.get(url=INPUT.us_url)   # ❌ 编译期报：变量 'r' 被多次赋值
    return r.data
```

**合流模式**：把两支结果写进不同变量，合流处用 `first_non_null` 类节点取第一个非空值（mediaflow 的做法：`items = FIRST_NON_NULL(nodes_="items_eu,items_us")`；该节点在 `plaita_flows/nodes.py`，业务侧自带）。更简单的写法：让每条分支直接 `return`（如上例改成两支各自 return，就没有合流问题）。

### 2.2 作用域边界：if 块 / 集合子流程

- **map 子流程的 end/body 不能引用 if 块内赋值的节点**——子流程编译边界看不到该 id，**编译期即报 `未知名字 'y'：可用 INPUT/NODE/GLOBAL/PARENT/ENV/F 或已赋值的节点变量`**。修法：把赋值提到集合节点之前（条件并进表达式，如 `y = A if cond else B`）。
- 集合子流程（`MAP`/`FILTER`/`FIND`/`LOOP`/`REDUCE`）体内：可读集合节点执行前已确定的父侧变量（`$PARENT.NODE.<名>` 快照）；**写不回父 context**；裸 `INPUT` 指子流程输入，读外层原始输入须 `PARENT.INPUT.<名>`。
- **跨作用域传数据的两种正规姿势**：
  1. 优先用集合节点自身输出：`MAP` 的输出即结果列表，经 `NODE.<集合id>` 在下游引用；
  2. 传不出来的（如 map 内每个 item 的富结果），走 **run 级报告通道**：子流程内 `report`/`writefile` 节点把每项结果追加写 jsonl（mediaflow 约定 `.flowcast/plaita-reports/<token>.jsonl`），主流程在集合节点之后聚合读回；map 的 body 只返回字面量。

### 2.3 `timeout=` 是保留 kwargs

所有节点通用字段 `id=` / `timeout=` / `on_error=ErrorHandler(...)` 走基类；`timeout` 收 ISO 时长（`PT5S`）。自定义节点别声明同名字段。

**`**` 展开与 caps 字段**：节点调用上的 `**` 展开只认字符串键的 dict **字面量**
（逐键等价具名 kwargs，合法值生效而非静默丢弃）；动态 dict（变量/调用）、非字符串
键、与具名同键编译期具名拒（自定义节点调用的 `**` 仍不支持）。caps 字段
`requestTimeout`/`maxRedirects`/`retryTimes` 过编译期字面量门：负数/表达式/显式
None 具名拒，`+5`/`-0` 这类单层 `±` 字面量折叠放行。动态传参改用具名 kwargs。

### 2.4 接表达式的节点字段一律 `Optional[Any]`

`@flow` 里 `LLM(prompt=INPUT.q)` 会把字段值编译成表达式串 `"$INPUT.q"` 传入 IR；pydantic 强类型字段（`int`/`bool`）会**构建时拒绝**。要接表达式的字段声明 `Optional[Any]` / `Optional[str]`；真字面量才用强类型。

> 层级说明：这一条落在**节点构造期**（`flow_from_source` / `compile_flow` 的 build 门），纯 `compile_source` 不含此步；且构造期报错**没有 source_line**，修复闭环弱于编译错误——所以字段名不要猜，用 `flow_describe_node("<node_type>")` 先查 schema（返回 pydantic schema + 必填字段最小示例 + 可接表达式的字段清单，plaita#351）。

### 2.5 字符串拼接用 `F.concat`

`+` 编译成 `$F.add`，运行时按 Python 多态：str 拼接、数字相加、list 相接——**类型决定语义，编译期不查**。强制拼接且对非字符串报错，显式 `F.concat(...)`。

### 2.6 节点命名与可读性（编译器自动语义命名）

无赋值名的节点不再产出 `_n1` 这类合成 id（2026-09-30 起）：

- **if/while 条件节点**：id = 条件语义 slug，如 `INPUT.score >= 90` → `score_ge_90`（字段名 + 运算符转写，ASCII 小写，超长截 24）；同 slug 冲突自动加 `_2.._5` 后缀；无 ASCII 语义（纯中文条件）回退 `_n{n}`。
- **return（end 节点）**：id = `ret_` + 返回表达式 slug（如 `ret_a`）；dict 字面量 return 无语义可取，保留 `_n{n}`，但 `name` 会兜底。
- **`end(...)` 终止语句**：同样按 `ret_` + output 表达式 slug 命名 id，`name`/`desc` 以 `end` 起头。`return` 恒定产出 `resultType="success"`，**error / nop 出口必须写 `end(result_type="error"|"nop", error={...})`**（见 §4）。
- 同时写入 `name`（短标签，如 `INPUT.score >= 90?`）与 `desc`（`if INPUT.score >= 90（第 4 行）`）——console 画布第二行显示、悬停可见；`source_line` 依旧逐节点回标，发布链路把 `metadata.source`（@flow 源码原文）一并带上后，画布可「节点 → 源码行」跳转（参照 mediaflow `publish_console.py`）。
- **含义**：运行期报错、dry-run 面板、执行记录里的节点名直接可读（`if_score_ge_90 出错了 (源码第 4 行)`）。作者侧唯一要做的：**让条件里的字段名可读**（`INPUT.score` 而非 `INPUT.v1`）。

### 2.7 `@childflow("<id>")`：声明处给内联子流程一个标识

`@childflow` 与 `@flow` 同形制：**位置参数是 `flow_id`**，`desc` 是纯关键字参数。

```python
@childflow("cf")                 # 位置 id
@childflow(desc="说明")           # 只有说明（与历史上唯一写法完全兼容）
@childflow("cf", desc="说明")     # 两者
@childflow()                     # 匿名
```

- **不参与节点寻址 / 查找**。`plaita/core/flow.py` 里 `flow_id` 的引用全在**日志与元数据路径**上：**5 处 `logger.warning` 调用**（`flow.py:178/250/281/311/343`，其消息里使用了 `flow_id`——结构性拓扑检查、`next`/`branches` 环检测、节点构建期校验、`$ENV` 未覆盖告警）、已废弃的 `Flow.id` 访问器（`:101-107`）与 IR 键归一化（`:118-122`）；**不要**把它当路由键、节点寻址键或跨流程引用名（引用子流程用的是源码里的函数名 `flow=<name>`）。
- **但它会被写进执行上下文，因此是可观测的——不是"零影响"**（#145 评审实测纠正）：
  - `plaita/core/context.py:314` → `$GLOBAL.flow_id`；`plaita/core/context.py:331`（写入 `plaita/core/state.py:632`）→ `$FLOW_ID`，别名 `$FLOW`。子流程内的表达式因此读得到**子流程自己**的 id。
  - **不给 id 时是 `None`，而 `_resolve_expression_string`（`plaita/core/context.py:137-148`）在子作用域求值为 `None` 时会回落到父上下文** ⇒ 同一个 `$FLOW_ID` 取到**父流程的 id**（父流程默认 id = 函数名）。
  - 净效果（实测，同一份源码，子 `end` 的 output 写 `$FLOW_ID`）：`@childflow("cf")` → `'cf'`；裸 `@childflow` → `'parent'`。**所以"给不给 id"会实打实改变 `$FLOW_ID` / `$GLOBAL.flow_id` 的取值——按它分支的表达式会走到不同分支。** 回归钉子：`tests/unit/test_codeflow_childflow_flow_id.py::TestFlowIdIsObservable`。
  - 代码里另有一处引用（`plaita/core/strategies.py:580` 把 `flow.flow_id` 放进事件订阅参数，`plaita/server/event_filter.py:175` 用它与 `state.flow_id` 比较），但**内联子流程走不到这条路径**：实测把同一个 event 节点放进带 id 的子流程里**根本不产生订阅**（子执行按 `lazy` 选择 Normal/Generator，不进入 `_subscribe_event`）⇒ **不构成额外的可观测面**，此处仅为记录。
- **不给 id 就是 `None`，不回落函数名**。内联子流程没有可用的函数名（被嵌入父流程 IR），回落会让两个同名子流程撞 id；宁可保持匿名（代价见上：`$FLOW_ID` 回落成父 id）。
- **原样写进 IR，往返保真**：`@childflow("cf")` 的 id 写进子流程 IR 的 `flow_id`，`emit_source` → `compile_source` 往返不丢（#145）——存量 JSON 产物转源码再转回来不会丢身份。
- **id 闸门只按类型把关**：任何 `str` 都收，包括 `""` 与纯空白（原样写入、原样往返）；**非 `str`**（如 `@childflow(123)`）编译期抛 `_CodeflowError` 并报行号。不做非空 / 去空白校验。
- **重名只告警不报错**：同一份文档里两个**不同**子流程声明了同一个 `flow_id` 时发 `UserWarning`；**同一子流程被父流程多处引用不告警**（那是共享同一份 IR 的合法复用）。`flow_id` 重复不影响执行（它只是标识），但排障时无法区分是哪一份。
- **源码字符串路径只认位置实参**：`flow_from_source` / `compile_source` 只读 `args[0]`，`@childflow(flow_id="cf")` 或 `@childflow("a", "b")` 里的关键字 / 第二个位置实参会被**静默忽略**（与既有 `@flow(flow_id=…)` 的约定一致）；真实装饰器 `childflow(...)` 才按 Python 语义生效。
- 存量 JSON 产物里的非 `str` `flow_id`（如 `5`）由发射器按 `str()` 归一（`@childflow('5')`），保证"发射器输出永远可重编译"（#76）。

## 3. prompt 工程约束

- **大段 prompt 不进 flow 图**（ADR 决议）：静态头部、指令模板在业务粘接节点（或 CODE 节点）里预拼成完整字符串；flow 图里 `{% ... %}` 只引用预拼串与少量表达式，保证图可读、可审计。
- 让模型「**全文直接输出、不要写文件**」这类输出纪律措辞，作为节点的固定指令模板统一维护（mediaflow 在 `nodes.py` 集中管理），不要每个 flow 各写一版。

## 4. 错误处理与 dry-run

- 错误策略统一 `on_error=ErrorHandler(strategy, default=...)`：可容忍步骤用 `continue_with` + `default` 兜底（如 `{"data": None}`），关键步骤保持默认 abort；运行期错误会回标源码行（`NodeExecutionError.source_line`）。
- **业务失败出口用 `end(result_type="error", error={"code": ..., "message": ...})`**：`return` 只能表达 success 出口，写 `return` 的失败分支会被下游当作正常结束（引擎抛 `FlowResultError` → 外层 `FlowExecutionException(error_type=ERROR_RESULT)`）；静默结束用 `end(result_type="nop")`。`result_type` 取值域是小写 `success`/`error`/`nop`（大小写敏感，域外值编译期硬错、不会静默降级）；`error` 是**纯字面数据**（引擎从不求值），写表达式（如 `{"message": INPUT.msg}`）会编译期报错，也**不参与 `$NODE`/`$F` 引用扫描**（载荷里写 `"$NODE.<id>"` 只是文本、不会判悬空）。语法见 `codeflow-reference.md` §4「end(...)：error / nop 结束出口」。
- **dry-run 双通道**：
  - 分支/流程级判断用 `$INPUT.dry_run`（业务入口把 flag 注入 input）；
  - 节点内部语义（跳过外呼、返回 fake）一律认 `globalContext.dry_run`——plaita-nodes 所有有副作用的节点都已尊重它：dry 下**不解析凭据、不连网**，返回带 `dry_run` 标记的 fake 结果（`writefile` 例外，照常写以便检查产物）。
  - 两个都设，别只设一个。

## 5. 业务集成：plaita-nodes（业务 flow 的节点主力）

内置占位符只有 `HTTP/CODE/EVENT/MAP/.../CHILD/PARALLEL/ASSIGN`。**Agent 调用、HITL、通知、凭据化连接器都在兄弟仓 plaita-nodes**（22 节点四族，清单见其 README）：

| 族 | 代表节点 | 用法要点 |
|----|----------|----------|
| Agent/LLM/决策 | `agentrun` `llm` `decision` | `agentrun` 经 agentproc `runner.run` 调 Agent CLI；`agent` 字段对应 agents.json 里的配置名 |
| 流程控制 | `gate` `rate_limit` `report` `hitl` `hitl_await` | `hitl` 同步阻塞等人；`hitl_await` 挂起 + poller 恢复（分布式/长等待用后者） |
| 出害口 | `github_comment` `git_publish` `notify` `writefile` `parse_json` | 通知加渠道只加 backend，不加新节点 |
| 凭据化连接器 | `api_request` `generic_webhook` `sql_query` `email_send` + IM webhook×4 | `credential` 字段引用编排台凭据页的凭据，运行时经 plaita.credentials 解密注入 |

### 5.1 安装与激活（少一步节点就是「未注册」）

```bash
# 大仓内三兄弟仓 editable（plaita / agentproc / plaita-nodes）
pip install -e ../plaita[http] -e ../agentproc/sdk/python -e ../plaita-nodes -e .
```

**激活序列（顺序敏感，必须在 `flow_from_source` 之前）**：

```python
import plaita_nodes                    # 顺带注册 agentproc recursive-direct executor
from plaita.node import get_default_registry, register_code_node
register_code_node(default_backend="subprocess")   # 用 CODE 节点才需要
from plaita_nodes.nodes import register_all        # 业务粘接节点（如有）
register_all()
get_default_registry()                 # ★ entry_points 懒发现靠它触发，漏了节点全"未注册"

from plaita.dsl.codeflow import flow_from_source
flow = flow_from_source(src)
```

### 5.2 agentrun 的配置（agents.json / providers.json）

- 搜索顺序：`~/.flowx → ~/.flowcast → <repo>/.flowcast → ~/.plaita → <repo>/.plaita`，**深合并、后者覆盖同名**——迁移语义：新配置写 `~/.plaita`，`~/.flowcast` 只兜存量。
- 与 flowcast 的两处差异（有意）：`env` 字段**按配置透传**（flowcast 白名单会静默丢弃）；`timeout_secs` 默认 1800。
- 配置里 `${VAR}` 插值缺失会 **fail-fast**；yaml 配置需 `pyyaml`。
- 字段 schema 以 `plaita-nodes/src/plaita_nodes/config.py` 为准。

### 5.3 CODE 节点：轻逻辑的正路

纯变换轻逻辑两条路：表达式 `F.*`（表达不了的）用 **CODE 节点**——`register_code_node(default_backend="subprocess")` 后，`CODE(code="...")` 的 code 须自带 `def run(input) -> dict`；多段轻逻辑集中到业务模块（如 `code_snippets.py`）单行 import 复用，不要内联大段代码。

> **逐节点 `sandbox_backend` 会被运营者白名单拦下**：worker / console 启动时按 `PLAITA_SANDBOX_ALLOWED_BACKENDS`（未配置 → `docker` ∪ 生效默认后端）施加白名单，流程 JSON 声明的后端（含 `CODE(..., sandbox_backend="unsafe")`）落在白名单外是**解析期报错**——不要在业务 flow 里声明后端档位，确需弱后端请让运营者显式放行（见运维 Runbook「code 沙箱后端白名单」）。

> **`language="js"` 默认被拒**（plaita#29）：语言白名单默认只放行 `python`，js 节点在**解析期**报错（`language='js' is not allowed by the operator`）。js 此前绕开整个档位体系（无隔离/无超时/无取消），故改为运营者经 `PLAITA_SANDBOX_ALLOWED_LANGUAGES=python,js` 显式放行；放行后 js 仍按 `sandbox_backend` 走档位（`restricted` 没有 js 实现）。业务 flow 的轻逻辑一律写 python，不要用 js。

> **`F.*` 扩展现状**：注册自定义表达式函数目前**没有公开 API**（mediaflow 用 `ExpressionParser._registry.register` 私有口，见其 `expressions.py`，脆弱）。新业务仓优先用 CODE 节点 / 业务节点替代；确需 `F.*` 时集中在一个 `expressions.py` 并注释私有 API 风险。

### 5.2 沙箱执行（agentrun + workspace，coding 场景）

`agentrun` 节点声明 `workspace` 字段（`.plaita/sandboxes.json` 里的名字，**infra
注册表，flow 只按名引用**）即把执行面关进沙箱：agent 的工具调用（bash/测试）在
隔离环境执行，控制面（checkpoint/EventBus）留宿主；数据进出只经 git
（clone 进 / push 出）。

```python
plan = AGENTRUN(agent=INPUT.agent, workspace="main",
                prompt=F.concat("阅读仓库并修复 issue：", INPUT.issue))
fix  = AGENTRUN(agent=INPUT.agent, workspace="main",       # 同 workspace 串行接力
                prompt=F.concat("按计划修复并跑通测试：", NODE.plan.text))
```

作者约束（编译期不拦，运行期才炸的坑）：

- `workspace` 与 `repo` **互斥**（workspace=沙箱 / repo=宿主直跑）；
- `workspace` 支持表达式（fan-out 写 `"task-{% $LOOP-INDEX %}"`），**求值为空
  即硬失败**——`$NODE` 缺键会静默 None，空名会让所有迭代共享同一沙箱；
- 未注册名 fail-closed；**spec 表达式必须确定性**（禁 `$F.now()`），否则按名
  重派生的断点续跑不成立；
- 同一 workspace 禁止出现在并行分支（workspace 级租约会在运行期快速失败）；
- 沙箱产物在**沙箱内 push**（出活约定 + 挂起时自动 wip 留档），宿主侧
  `git_publish` 指向遗留 checkout 会被运行期警告（绊线）；
- 大文件内容/diff 不进 prompt 上下文——留在 git 里，用路径/commit 引用
  （checkpoint 对全量上下文逐步重序列化，成本随步数二次方增长）。

driver 选型（注册表 `driver` 字段）：`docker`（现役）/ `krunvm`（本地 microVM
实验档）/ `ssh`（远端 VM 实验档，定义需 host/user/identity 等）——flow 写法
不感知 driver。全貌见 plaita-nodes `docs/sandbox-drivers-design.md`。

## 6. 业务 flow 项目结构模板

mediaflow 验证过的结构，新业务仓照抄：

```
<repo>/plaita_flows/
├── flows/*.flow        # @flow 源码 = 权威定义（可版本化、编译期行号校验）
├── nodes.py            # 业务粘接节点（register_all() 注册）
├── expressions.py      # F.* 纯函数注册（注意 5.3 的私有 API 警示）
├── code_snippets.py    # CODE 节点轻逻辑
├── run.py              # CLI 入口：按 5.1 激活序列 → flow_from_source → run
├── publish_console.py  # 发布 plaita-console（见 §7）
└── tests/              # flow 编译冒烟 + 节点单测
```

- **JSON IR 不入库**：JSON 仅为 console 对接的按需编译产物（ADR 决议），权威定义始终是 `.flow` 源码。
- **dry-run 是验收底线**：入口提供 `--dry-run`（双通道都设，见 §4），所有 flow 先 dry 跑通骨架再真实执行。
- 业务仓的 Python 依赖要落清单（pyproject/requirements），别只活在 README 一条命令里。

## 7. 发布 plaita-console

链路（参照 mediaflow `publish_console.py`）：`compile_source` 出 IR → console 建条目 → 存 IR draft → publish → API dry-run 验证。

**发布前必查**：console 拉起的 flow_worker **不会自动加载你业务 venv 里的节点**。worker 侧须注入：

- `PLAITA_NODE_PATH` / `PLAITA_NODE_MODULES` — 业务节点包路径/模块
- `PLAITA_PYTHON` — 业务 venv 解释器

漏了就是「发布成功、调度必败」（节点类型引擎不识别）。见 ADR-2026-08-27 worker 注入机制。

## 8. 已知运行期陷阱（历史案例）

- **cwd 敏感**：报告/产物路径若依赖 `--repo` 或 `os.getcwd()`，从错误目录启动会静默写错位置（mediaflow 曾因此批量 dry-run 误报，修复后又回滚）。路径一律显式传参，不赌 cwd。
- **`$NODE` 缺失键会抛错**：读上游节点输出做防御式兜底（mediaflow `_node_output` 助手），尤其是可选分支未执行时。
- **`$NODE` 字符串属性不再二次解析**：含 `[tag]`/引号/换行的普通字符串值按字面量处理（已修复的内核缺陷），节点输出带元字符不必再转义。

---

*维护：本文为 flow-coder skill 的 reference 之一，随 plaita-ai wheel 分发；`docs-site` 的 guide 页面以链接方式引用本文件，不复制内容。*
