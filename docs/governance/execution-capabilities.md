# 节点执行台阶契约（两语义模型；#330 §4 / #331 全文 → 台阶化 2026-10-25）

- **状态**：台阶化落地（2026-10-25，owner 裁决）。#330（词表+声明+注册三路同闸）
  与 #331（消费面：环境校验 / 递归闭包 / NodeRunner 兜底 / 矩阵治理）机制保持，
  **词表换芯**：5 token 退役为三值台阶（本篇 §1）。#330 时代「token 两处手写 +
  注册不变量缝补」的双写面同日退役（`is_suspending` 改为台阶派生值）
- **落点**：`plaita/node/capabilities.py`（归一化 + 注册闸 + 矩阵生成源）、
  `plaita/core/capabilities.py`（台阶本体 + 闭包 + 环境闸 + runner 兜底）、
  `plaita/node/basic.py`（`Node.execution_tier` + `__init_subclass__` 派生）、
  `plaita/node/__init__.py`（注册三路同闸 + `capability_matrix()`）、
  `plaita/core/_error_normalization.py`（根出口呈现，运行期兜底）、
  `plaita-console/backend/services/capability_gate.py`（publish/start 闸）
- **关联**：ADR-001（runtime 组合——两语义的治理出处）；#330/#331（本篇来源）；
  #311（嵌套挂起 NS-M3——运行期作用域闸的原点，台阶化不改变其任何运行期行为）；
  #317（跨引擎对拍锚——决策分类锚，不锚文案）；#190（dry_run_safe，关系见 §5）

## 1. 台阶（三值全序，封闭集）

执行语义只有两种（ADR-001）：**直跑**（inline，进程内当场跑完）与**托管**
（managed，能停能续的执行面）。节点侧唯一声明 = 执行台阶：

    inline < managed < managed:queue

| 台阶 | 定义 |
|---|---|
| `inline`（默认） | 任何宿主可跑：纯计算 / 控制流 / 出网 / 本机子进程 / 沙箱白名单内代码 |
| `managed` | 会挂起、靠**事件总线**唤醒（event、hitl_await 等）：宿主必须是托管面；进程内总线即可满足 |
| `managed:queue` | 挂起后靠**持久队列 + 配套唤醒服务**恢复（delay / approval / http_callback / redis_queue / kafka_queue）：宿主必须是服务齐配的托管面 |

判定 = flow 台阶（站点台阶 max）≤ 环境台阶，一次比较。**错误报缺不报档**：
说出缺的台阶、点名节点、给修法，不要求用户理解任何档位名词。

**退役词表**（5 token，仅迁移期兼容读取）：`suspend:root`/`suspend:nested` 合并的
教训——帧结构限制是**图的结构规则**（归 NS-M3 作用域闸
`SUSPEND_ALLOWED_FRAME_NODE_TYPES`，单一事实源不变），不是宿主能力；全面放开
嵌套挂起时只松作用域闸，词表零变化。`sandbox`/`network` 从不区分任何真实宿主
（沙箱白名单 `PLAITA_SANDBOX_ALLOWED_BACKENDS` 与目标服务可达性是环境配置闸 /
运行期事实），不设台阶。**第三方系统可达性永不进台阶**（#331 owner 裁决不变）。

新增台阶 = 执行形态面变更：须有真实部署形态背书，改 `EXECUTION_TIERS` + 本表 +
ADR，同 PR。

## 2. 声明（单一真相）

```python
class MyNode(Node):
    execution_tier = "managed"        # 唯一声明；不写 = inline
```

`is_suspending` 是**派生值**（`__init_subclass__` 据台阶推导，节点类不再手写）；
`Node.execution_requirements`（旧 token 元组）保留为**迁移兼容**读取——未声明
`execution_tier` 时经 `legacy_tokens_to_tier` 折算（第三方插件如 plaita-nodes
不破，折算时一次性 WARNING）。优先级：显式台阶 > 旧 token 折算 > is_suspending
兜底（fail-closed 记 `managed`）。

已声明面（2026-10-25 实况）：

| 节点 | 台阶 |
|---|---|
| `event` | `managed` |
| `hitl_await`（plaita-nodes，legacy 折算） | `managed` |
| `delay` / `approval` / `http_callback` / `redis_queue` / `kafka_queue` | `managed:queue` |
| `http` / `redis` / `code` / 全部控制流与计算节点 | `inline` |

**approval / http_callback 的台阶修正**（2026-10-25，睡死坑）：两者挂起后走
「队列 + 配套服务」（approval_service 登记待办/转发决策、http_callback_service
接线回调）——服务不在场则待办/回调永不发生。旧声明只有 `(suspend:root,
event_bus)` 会放过无服务环境；归 `managed:queue` 后，无服务环境在 publish/start
即具名拒。

## 3. 注册期契约

三路注册同闸（`NodeRegistry.register`〔含 console `PLAITA_CONSOLE_NODE_MODULES`
动态节点经模块 `register_all()` 的路径〕、`_register_builtins`、entry_points 插件
发现〔违规记 `_load_errors` 失败现场，#98 形制〕）。归一化幂等（注册时重算），
三条拒绝：

1. 台阶值非法（拼写错，`__init_subclass__` 类创建期即红）；
2. 「只写 `is_suspending=True` 不写台阶」具名拒（裸声明防线，#330 Stage 1 第 4 条
   旧口径；归一化**不静默改写**该声明）;
3. 旧 `execution_requirements` 含未知 token（词表封闭旧口径，迁移期）。

`unregistered`（运行宿主注册表缺 node_type）是**加载期事实**（`parse_node` 具名
报错），不经运行期错误呈现；`undeclared` 在旋钮降级放行的过渡期出现，运行期按
fail-closed 记 `managed`。

## 4. 回滚旋钮（到期删除）

`PLAITA_SUSPEND_GUARD=warn` 作用于 **#330/#331 全部静态闸**：①normal/generator
根出口的呈现转换（保 NS-M3 原样裸 carrier + 结构化 WARNING）；②注册期闸
（拒绝降 WARNING 放行）；③执行入口 `validate_for_profile` 拒绝、④NodeRunner
execute 前兜底断言（均降 WARNING 放行，交回运行期闸/呈现）。**关不掉 NS-M3
运行期 carrier 闸**（#311 语义不变）。非法值 WARNING 回落严格档。**到期：
2026-11-30 删除本节与 `SUSPEND_GUARD_ENV`**（防孤儿旋钮）。

## 5. 与 dry_run_safe 的关系（裁决）

`Node.dry_run_safe`（plaita#190，console 试跑安全三态）与执行台阶是同一问题
「节点需要什么才能跑」的两个投影（试跑安全 vs 宿主环境）。**不收编**：语义不同轴
（dry_run_safe=False 的节点可 inline，如 http）。后续单把 `dry_run_safe` 从台阶
推导（managed 族 → 非 safe）或维持切割，归 #331 后续；在此之前改其一须同步评估
另一侧（本节即同步义务的落点）。

## 6. #317 对拍锚

「normal/debug + 挂起 → 两引擎具名拒绝、绝不成功」是 #317 消费矩阵的决策分类锚
（矩阵行 `29_suspend_normal_named_reject`）：Python=`SuspendNotExecutableError`，
Go=`Step{Error}`（ExecuteGenerator）/ runNodes suspend。**锚决策分类不锚文案**——
改具名错误面文案须同步对拍用例；`error_type="suspend_not_executable"` 稳定字符串
是两单互认的机器判据。#331 起该字符串也覆盖执行入口的静态拒绝（`stage="entry"`），
台阶化不改判据。

## 7. 消费面（#331 后半；台阶化 2026-10-25 换内核）

**三件事一个判定函数**（A2 r61 评审 §3：两层勿写两套判定）——全部住
`plaita/core/capabilities.py`（core 不反向 import node 层，节点属性鸭子读取）：

### 7.1 环境台阶

环境不贴档位名，报自己有什么：

| 环境 | 台阶 | 事实源 |
|---|---|---|
| 直跑（`flow.run` / CLI / normal/generator 模式根） | `inline` | 模式推导 |
| 托管·单进程后端（console 本地档，`redis=None`） | `managed` | console 闸显式传 `local`（归一化兼容名） |
| 托管·服务齐配（集群 worker 池 / `run_distributed` 模式根） | `managed:queue` | 模式推导 |

旧 profile 名（normal/generator/distributed/local）与旧 token 集合形参照收
（归一化兼容）。managed:queue 环境内**队列服务是否真的在跑**是受理期/运维面
检查（外延服务健康 + 搁浅告警），不重复进台阶判定。

### 7.2 拒绝面：`validate_for_profile(flow, profile)`

**独立于 `model_validate`**（读面/解析/展示永不调用）。flow 台阶 > 环境台阶即在
**任何节点执行前**具名拒绝：`SuspendNotExecutableError`（`stage="entry"`，#317
机器判据不变；错误体稀疏键 `stage/profile/required/missing` 均为台阶口径；
`fix.action` 分流 `run_distributed` / `attach_queue_services` / `restructure`）。
调用点（A2 r61 评审 §2 全量面）：执行入口（`run_compatible`/`arun_compatible` 的
`_prepare_strategy` 咽喉 + `run_distributed`）与 console `_publish_gates`
（publish 与 import-version 共用）及本地档 start。托管入口**只判台阶不判帧
类型**——loop/parallel 帧内挂起由运行期作用域闸照常具名拒（传播面守恒，入口
不预判）；子执行（parent 非空）不闸。

### 7.3 递归闭包：`required_tier(flow, recursive=True)`

child / loop 族 / parallel 分支 + reference（经 `resolve_reference` 钩子；未注入
记 `unresolved` 不当缺口）全部并入；**台阶取站点 max**；站点带帧路径（根→叶，
与 #330 根出口呈现同形）与 `cause`（declared / legacy / undeclared）。过渡期裸
声明挂起节点按 fail-closed 记 implicit `managed`（`cause=undeclared`）。

### 7.4 兜底面：NodeRunner execute 前断言

每个托管（managed+）节点 `execute` 前断言宿主台阶（宿主身份取执行链**根**，含
子流程），覆盖绕过 strategies 直驱 `_advance_one` 的旁路与静态闭包看不见的复合
节点。正常流不应触达，触达即入口漏了：具名错误（`stage="runner"`）+ WARNING，
不静默；`SuspendNotExecutableError` 在 runner 层与 carrier 同待遇原样上抛（不被
重试/errorHandler 吞掉改写）。

### 7.5 矩阵治理（差分断言 = 承重墙）

`NodeRegistry.capability_matrix()`（`node_type → 台阶`，inline 节点同样入列）→
治理互锁**三处联动**：

```
注册表（plaita/node/capabilities.py shipped_capability_matrix）
  ↔ 本仓账本 docs/governance/architecture.yaml gov.capability_matrix
  ↔ 大仓根 docs/DOC_ASSERTIONS.yml gov/capability-matrix expected
```

差分语义 `{added, removed, changed}`——**注册表新增 node_type 而矩阵未变 →
`added` 非空 → 红**（A2 r61 评审 §1.3）。工具：`scripts/export_capability_matrix.py`
（`--check` 对账 / `--json` 导出 Python↔Go 对拍锚，Go 消费面=`pkg/ir/capability.go`，
Go=frozen 属维护线）；push 门=unit 差分断言常态拦截，深夜兜底=`ci-slow.yml`
nightly 独立步。维护动作：新增/改声明 → 跑脚本看差分 → 回写两处账本（只改一处即红）。

## 8. 平移口径（Go / Rust）

台阶词与满足关系是纯数据（`EXECUTION_TIERS` + 矩阵 JSON）：Rust（oracle）挂现有
conformance/hash 门对拍；Go（frozen）文档声明支持矩阵即可——机制无需跨语言重做。
`error_type="suspend_not_executable"` 与台阶字符串（`inline`/`managed`/
`managed:queue`）是跨引擎对拍锚。
