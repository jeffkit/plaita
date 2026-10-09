# @flow 流程编写规范（权威单源）

> **定位**：本文件是 plaita `@flow` 流程编写的**单一权威规范**——收录语法参考不会告诉你的作者硬约束（多数编译期不拦、运行期才炸）、plaita-nodes 业务集成、业务 flow 项目结构与 console 发布链路。
> **语法细节**不在本文件，见 [codeflow-reference.md](codeflow-reference.md) 与 `docs-site/docs/guide/code-dsl.md`。
> **决议背景**：大仓 `docs/ADR-2026-08-27-orchestration-converge-on-plaita.md`（编排收敛 plaita、执行层统一 agentproc、mediaflow 全量迁移）。
> **同步约束**：改 `plaita/dsl/codeflow/**`、plaita-nodes 节点语义或大仓编排决议时，必须同步本文件（映射见 `plaita/docs/DOC_CODE_MAP.md`）。

---

## 0. 30 秒自检清单（每次交付前过一遍）

- [ ] 没有跨分支同名赋值（变量名即节点 id，无 phi 合并）
- [ ] 没在 map/集合子流程的 end 里引用 if 块内赋值的节点；跨作用域聚合走了 MAP 输出（`NODE.<集合id>`）或 run 级报告通道
- [ ] 没把 `timeout=` 挪作他用（保留 kwargs，ISO 时长如 `PT5S`）
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
| 编译落盘（产物 JSON） | `python -m plaita build <源码.py> -o <产物>.json` | canonical=console 正典形态（默认，字节稳定；`--format ir` 回退直出 IR）；`--register plaita_nodes` 显式注册业务节点、`--code-backend subprocess` 注册 CODE 节点、`--check` 供 CI 钉「产物落后源码」。大仓子仓的 flow 编译脚本应调它而非自带样板（recursive `compile_v2.py` / issue-keeper `build_flows.py` 即薄壳范例，2026-10-09 收敛） |

- `input_type` / `output_type` **已废弃且被忽略**：`$INPUT` 恒为 dict，不要声明。
- **运行入口分两层，别混**：
  - `Flow.run(**input)`（含 `flow_from_source(src).run(**input)`）：kwargs 就是 `$INPUT`；
  - `FlowExecution().run(...)`（底层）：**kwargs 不进 `$INPUT`（确定的设计），运行参数走 `params=`**。
  - 业务仓统一走 `Flow.run`；文档/示例若出现 `FlowExecution.run`，参数必须显式 `params=`。

## 2. 作者硬约束（编译期不拦，踩了才知）

### 2.1 变量名即节点 id：跨分支同名赋值禁止

赋值变量名直接成为节点 id，**没有 phi 合并语义**。两个分支各自 `r = HTTP.get(...)` 会产生同名节点 id 的语义冲突，编译器不会替你合并：

```python
@flow("bad")
def bad(INPUT):
    if INPUT.region == "eu":
        r = HTTP.get(url=INPUT.eu_url)
        return r.data
    r = HTTP.get(url=INPUT.us_url)   # ❌ 与上一分支同名
    return r.data
```

**合流模式**：把两支结果写进不同变量，合流处用 `first_non_null` 类节点取第一个非空值（mediaflow 的做法：`items = FIRST_NON_NULL(nodes_="items_eu,items_us")`；该节点在 `plaita_flows/nodes.py`，业务侧自带）。

### 2.2 作用域边界：if 块 / 集合子流程

- **map 子流程的 end 不能引用 if 块内赋值的节点**——子流程编译边界看不到该 id。
- 集合子流程（`MAP`/`FILTER`/`FIND`/`LOOP`/`REDUCE`）体内：可读集合节点执行前已确定的父侧变量（`$PARENT.NODE.<名>` 快照）；**写不回父 context**；裸 `INPUT` 指子流程输入，读外层原始输入须 `PARENT.INPUT.<名>`。
- **跨作用域传数据的两种正规姿势**：
  1. 优先用集合节点自身输出：`MAP` 的输出即结果列表，经 `NODE.<集合id>` 在下游引用；
  2. 传不出来的（如 map 内每个 item 的富结果），走 **run 级报告通道**：子流程内 `report`/`writefile` 节点把每项结果追加写 jsonl（mediaflow 约定 `.flowcast/plaita-reports/<token>.jsonl`），主流程在集合节点之后聚合读回；map 的 body 只返回字面量。

### 2.3 `timeout=` 是保留 kwargs

所有节点通用字段 `id=` / `timeout=` / `on_error=ErrorHandler(...)` 走基类；`timeout` 收 ISO 时长（`PT5S`）。自定义节点别声明同名字段。

### 2.4 接表达式的节点字段一律 `Optional[Any]`

`@flow` 里 `LLM(prompt=INPUT.q)` 会把字段值编译成表达式串 `"$INPUT.q"` 传入 IR；pydantic 强类型字段（`int`/`bool`）会**构建时拒绝**。要接表达式的字段声明 `Optional[Any]` / `Optional[str]`；真字面量才用强类型。

### 2.5 字符串拼接用 `F.concat`

`+` 编译成 `$F.add`，运行时按 Python 多态：str 拼接、数字相加、list 相接——**类型决定语义，编译期不查**。强制拼接且对非字符串报错，显式 `F.concat(...)`。

### 2.6 节点命名与可读性（编译器自动语义命名）

无赋值名的节点不再产出 `_n1` 这类合成 id（2026-09-30 起）：

- **if/while 条件节点**：id = 条件语义 slug，如 `INPUT.score >= 90` → `score_ge_90`（字段名 + 运算符转写，ASCII 小写，超长截 24）；同 slug 冲突自动加 `_2.._5` 后缀；无 ASCII 语义（纯中文条件）回退 `_n{n}`。
- **return（end 节点）**：id = `ret_` + 返回表达式 slug（如 `ret_a`）；dict 字面量 return 无语义可取，保留 `_n{n}`，但 `name` 会兜底。
- 同时写入 `name`（短标签，如 `INPUT.score >= 90?`）与 `desc`（`if INPUT.score >= 90（第 4 行）`）——console 画布第二行显示、悬停可见；`source_line` 依旧逐节点回标，发布链路把 `metadata.source`（@flow 源码原文）一并带上后，画布可「节点 → 源码行」跳转（参照 mediaflow `publish_console.py`）。
- **含义**：运行期报错、dry-run 面板、执行记录里的节点名直接可读（`if_score_ge_90 出错了 (源码第 4 行)`）。作者侧唯一要做的：**让条件里的字段名可读**（`INPUT.score` 而非 `INPUT.v1`）。

## 3. prompt 工程约束

- **大段 prompt 不进 flow 图**（ADR 决议）：静态头部、指令模板在业务粘接节点（或 CODE 节点）里预拼成完整字符串；flow 图里 `{% ... %}` 只引用预拼串与少量表达式，保证图可读、可审计。
- 让模型「**全文直接输出、不要写文件**」这类输出纪律措辞，作为节点的固定指令模板统一维护（mediaflow 在 `nodes.py` 集中管理），不要每个 flow 各写一版。

## 4. 错误处理与 dry-run

- 错误策略统一 `on_error=ErrorHandler(strategy, default=...)`：可容忍步骤用 `continue_with` + `default` 兜底（如 `{"data": None}`），关键步骤保持默认 abort；运行期错误会回标源码行（`NodeExecutionError.source_line`）。
- **dry-run 双通道**：
  - 分支/流程级判断用 `$INPUT.dry_run`（业务入口把 flag 注入 input）；
  - 节点内部语义（跳过外呼、返回 fake）一律认 `globalContext.dry_run`——plaita-nodes 所有有副作用的节点都已尊重它：dry 下**不解析凭据、不连网**，返回带 `dry_run` 标记的 fake 结果（`writefile` 例外，照常写以便检查产物）。
  - 两个都设，别只设一个。

## 5. 业务集成：plaita-nodes（业务 flow 的节点主力）

内置占位符只有 `HTTP/CODE/EVENT/MAP/.../CHILD/PARALLEL`。**Agent 调用、HITL、通知、凭据化连接器都在兄弟仓 plaita-nodes**（22 节点四族，清单见其 README）：

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
