# 节点执行能力契约（#330 §4 / #331 全文）

- **状态**：全文落地（2026-10-24；#330 落「前半」=词表+声明+注册三路同闸，
  #331 落「后半」=消费面：`validate_for_profile` / 递归闭包 / NodeRunner 兜底断言 /
  矩阵治理（§7）；`dry_run_safe` 推导化仍留后续单，关系裁决见 §5）
- **落点**：`plaita/node/capabilities.py`（词表再导出 + 校验 + 矩阵生成源）、
  `plaita/core/capabilities.py`（词表本体 + 闭包 + profile 闸 + runner 兜底）、
  `plaita/node/basic.py`（`Node.execution_requirements` 基类默认）、
  `plaita/node/__init__.py`（注册三路同闸 + `capability_matrix()`）、
  `plaita/core/_error_normalization.py`（根出口呈现，运行期兜底）、
  `plaita-console/backend/services/capability_gate.py`（publish/start 闸）
- **关联**：#330（本篇来源，含评论区 A2 实施前规格评审）；#311（嵌套挂起 NS-M3——
  运行期闸的原点，本契约不改变其任何运行期行为）；#331（词表消费方：入口校验/闭包/矩阵）；
  #317（跨引擎对拍锚——决策分类锚，不锚文案）；#190（dry_run_safe，关系见 §5）

## 1. 词表（5 token，封闭集）

token = 引擎完成 park/resume 或节点功能面所必需的**宿主基础设施**；节点业务集成的
外部单一服务可达性 = 运行期事实，**不进词表**（正交轴，防 `service:<node_type>`
每节点一符号的膨胀）。

| token | 定义 |
|---|---|
| `suspend:root` | 宿主支持根流程挂起/恢复（分布式运行时） |
| `suspend:nested` | 宿主支持 child/reference 嵌套链内挂起（NS-M3 帧路径；`suspend:root` 超集） |
| `event_bus` | 事件总线：订阅注册与恢复事件发布（挂起族的结构半边） |
| `queue` | **持久化队列 + 延迟排程基础设施**（delay_service：list→ZSET + 到期消费线程；定义句钉死「不只是任务队列」，防「queue 满足但 delay 永不唤醒」灰区） |
| `sandbox` | 代码沙箱（docker 等隔离后端；`sandbox_backend` 白名单体系的宿主侧） |
| `network` | 节点功能面所需的宿主出网能力（http/redis 等以出网为功能的节点） |

新增 token = 运行时能力面变更：改 `EXECUTION_CAPABILITY_TOKENS` + 本表，同 PR。

## 2. 声明

`Node.execution_requirements: ClassVar[tuple] = ()`（空 = 纯计算/控制流，任何宿主可跑）。
已声明面（2026-10-24 实况）：

| 节点 | 声明 |
|---|---|
| `event` | `("suspend:root", "event_bus")` |
| `approval` / `http_callback`（继承 event） | 同上 |
| `delay` | `("suspend:root", "event_bus", "queue")` |
| `redis_queue` / `kafka_queue` | `("suspend:root", "event_bus", "queue")` |
| `hitl_await`（plaita-nodes，继承**裸 Node**） | `("suspend:root", "event_bus")` |
| `http` / `redis` | `("network",)` |
| `code` | `("sandbox",)` |

## 3. 注册期不变量

`is_suspending=True ⇒ execution_requirements` 非空且含 `suspend:*` token；未知 token
一律拒绝（词表封闭）。三路注册同闸：`NodeRegistry.register`（含 console
`PLAITA_CONSOLE_NODE_MODULES` 动态节点经模块 `register_all()` 的路径）、
`_register_builtins`（内置声明回归在 import 期即红）、entry_points 插件发现
（违规记 `_load_errors` 失败现场 + WARNING，该节点不可用但发现过程与其余插件不受
牵连，#98 形制）。绕过面：`nodes[...] = ...` dict proxy 与裸 `_nodes` 直写不校验——
属未文档化的内部通道，勿用。

`unregistered`（运行宿主注册表缺 node_type）是**加载期事实**（`parse_node` 具名报错），
不经运行期错误呈现；`undeclared`（空声明）在不变量严格档下只应启动期红，运行期仅在
旋钮降级放行的过渡期出现。

## 4. 回滚旋钮（到期删除）

`PLAITA_SUSPEND_GUARD=warn` 作用于 **#330 新增两半边 + #331 两道静态闸**：①normal/
generator 根出口的呈现转换（保 NS-M3 原样裸 carrier + 结构化 WARNING）；②注册期
不变量（拒绝降 WARNING 放行）；③执行入口 `validate_for_profile` 拒绝、④NodeRunner
execute 前兜底断言（均降 WARNING 放行，交回运行期闸/呈现；`ExecutionProfileError`
不受旋钮影响）。**关不掉 NS-M3 运行期 carrier 闸**（#311 语义不变）。非法值 WARNING
回落严格档。**到期：2026-11-30 删除本节与 `SUSPEND_GUARD_ENV`**（防孤儿旋钮）。

## 5. 与 dry_run_safe 的关系（裁决）

`Node.dry_run_safe`（plaita#190，console 试跑安全三态）与 `execution_requirements`
是同一问题「节点需要什么才能跑」的两个投影（试跑安全 vs 宿主能力）。本单**不收编**：
两套声明并存、语义不同轴（dry_run_safe=False 的节点可无宿主要求，如 http 在无沙箱
宿主仍是合法部署）。后续单把 `dry_run_safe` 从 requirements 推导（声明 network/sandbox
→ 非 safe）或维持切割，归 #331 后续；在此之前改其一须同步评估另一侧（本节即同步
义务的落点）。

## 6. #317 对拍锚

「normal/debug + 挂起 → 两引擎具名拒绝、绝不成功」是 #317 消费矩阵的决策分类锚
（矩阵行 `29_suspend_normal_named_reject`）：Python=`SuspendNotExecutableError`，
Go=`Step{Error}`（ExecuteGenerator）/ runNodes suspend。**锚决策分类不锚文案**——
改具名错误面文案须同步对拍用例；`error_type="suspend_not_executable"` 稳定字符串
是两单互认的机器判据。#331 起该字符串也覆盖执行入口的静态拒绝（`stage="entry"`），
机器判据不变。

## 7. 消费面（#331 后半，2026-10-24 落地）

**三件事一个判定函数**（A2 r61 评审 §3：两层勿写两套判定）——全部住
`plaita/core/capabilities.py`（core 不反向 import node 层，节点属性鸭子读取）：

### 7.1 profile 判据 = ADR-001 runtime 组合

| profile | 组合 | 满足 token |
|---|---|---|
| `normal` / `generator` | Python reference 进程内 | `network`, `sandbox` |
| `distributed` | 分布式 worker 池（队列+总线+延迟排程） | 全集 |
| `local` | console 进程内后端（InMemoryEventBus+SQLite checkpoint，**无**持久队列/延迟排程） | 全集 − `queue` |

自定义 profile 以 token 集合直接给出（未知 token 具名拒）。

### 7.2 拒绝面：`validate_for_profile(flow, profile)`

**独立于 `model_validate`**（读面/解析/展示永不调用）。闭包 token ⊄ profile 即在
**任何节点执行前**具名拒绝：缺 park/resume 结构族（`suspend:*`/`event_bus`/`queue`）
→ `SuspendNotExecutableError`（`stage="entry"`，#317 机器判据不变；错误体新增稀疏键
`stage/profile/required/missing`）；只缺 `network`/`sandbox` →
`ExecutionProfileError`（`execution_profile_unsatisfied`，仅自定义 profile 可见）。
调用点（A2 r61 评审 §2 全量面）：执行入口（`run_compatible`/`arun_compatible` 的
`_prepare_strategy` 咽喉 + `run_distributed`）与 console `_publish_gates`（publish 与
import-version 共用）及本地档 start。distributed 入口**只判 token 不判帧类型**——
loop/parallel 帧内挂起由运行期作用域闸照常具名拒（传播面守恒，入口不预判）；
子执行（parent 非空）不闸。

### 7.3 递归闭包：`required_capabilities(flow, recursive=True)`

child / loop 族 / parallel 分支 + reference（经 `resolve_reference` 钩子；未注入记
`unresolved` 不当缺口）全部并入；站点带帧路径（根→叶，与 #330 根出口呈现同形）。
过渡期空声明挂起节点按 fail-closed 记 implicit `suspend:root`（`cause=undeclared`）。

### 7.4 兜底面：NodeRunner execute 前断言

每个 suspending 节点 `execute` 前断言宿主契约（宿主身份取执行链**根**，含子流程），
覆盖绕过 strategies 直驱 `_advance_one` 的旁路与静态闭包看不见的复合节点。正常流
不应触达，触达即入口漏了：具名错误（`stage="runner"`）+ WARNING，不静默；
`SuspendNotExecutableError` 在 runner 层与 carrier 同待遇原样上抛（不被重试/
errorHandler 吞掉改写）。

### 7.5 矩阵治理（差分断言 = 承重墙）

`NodeRegistry.capability_matrix()`（空声明节点同样入列）→ 治理互锁**三处联动**：

```
注册表（plaita/node/capabilities.py shipped_capability_matrix）
  ↔ 本仓账本 docs/governance/architecture.yaml gov.capability_matrix
  ↔ 大仓根 docs/DOC_ASSERTIONS.yml gov/capability-matrix expected
```

差分语义 `{added, removed, changed}`——**注册表新增 node_type 而矩阵未变 → `added`
非空 → 红**（A2 r61 评审 §1.3）。工具：`scripts/export_capability_matrix.py`
（`--check` 对账 / `--json` 导出 Python↔Go 对拍锚，Go 消费面=`pkg/ir/capability.go`，
Go=frozen 属维护线）；push 门=unit 差分断言常态拦截，深夜兜底=`ci-slow.yml`
nightly 独立步。维护动作：新增/改声明 → 跑脚本看差分 → 回写两处账本（只改一处即红）。
