# ADR-001：Runtime 组合裁定——Python reference / Go frozen / Rust oracle / local backend

- **状态**：已接受（Accepted）——owner 拍板 2026-10-23（plaita#325，#329 tracking 的 D1）
- **落点**：`docs/governance/adr-001-runtime-composition.md`（与治理账本 `architecture.yaml` 同址）
- **证据基线**：plaita `45e7611` · plaita-go `df0a938` · plaita-rs `6c01ffc`（2026-10-23 各自 main）
- **关联**：#325（裁定源）；#329（A–F 架构改造 tracking，本篇为其 D1 锚）；#317（范围随本篇缩围）；plaita-go#24（冻结注记 owner 已留言）；plaita-rs#17（WK-M1）

**快照与活源约定**：§3 为 2026-10-23 的点时写实，不随后续代码演化改写；活的事实源是治理账本 `architecture.yaml`（大仓根 monarbor doctor 经 `docs/DOC_ASSERTIONS.yml` 的 `gov/*` 断言逐条校验）与各仓 AGENTS.md。裁定本身的变更须立新 ADR，或经 #325 评审轮修订本文件并留痕。

## 1. 裁定

| 维度 | 裁定 | 含义 |
|---|---|---|
| Python（plaita） | **reference，且唯一生产运行时** | 语义权威（expression / IR / 指纹 golden 的导出源）与唯一发布目标 |
| Go（plaita-go） | **frozen** | 仅安全修复；不再加节点与功能 |
| Rust（plaita-rs） | **oracle** | IR 结构规则（R1–R7）与语义差分的对照权威；不接生产发布 |
| console local | **engine backend** | 不是第四种执行语义；Python 引擎的进程内降级后端 |

三者合成的三角：Python 生产并定义语义；Rust 以独立再实现持续挑战 Python/Go 的 IR 与表达式语义（oracle 的价值恰在于它「不是生产的那个实现」）；Go 冻结为历史第二实现，只保安全。local 不是一种语义，是 Python 的执行后端形态。

配套执行裁定（均源自 #325，实施非本篇范围，见 §5 迁移注记与 §8）：

- **E1**：`VALID_RUNTIMES` 收敛 `("go","python","rust")` → `("python",)`（存量 go flow 的分支处理见 §5 注记 3）；
- **E2**：runtime 单真相 = `flow_versions.runtime` DB 列；`validate_definition` 增 `target_runtime` 形参；
- **E3**：#317 缩围为 IR/表达式 golden，删除 publish 矩阵消费。

## 2. 背景

plaita 生态现存三引擎：Python 本仓（reference）、plaita-go（Golang 移植）、plaita-rs（Rust 移植），后两仓均以 Python 为语义权威做对拍移植。#325 立单时的问题结构：

1. **用户产品形状对运行时有硬门槛**（#325 Summary）：flow 由用户/AI 编写、长时间运行、带审批/回调；`code` 节点主流是 Python。生产运行时必须能加载真实 flow 并提供可靠的恢复语义。
2. **恢复语义分叉已实证**（#317）：「同一份从断点继续」，Python 不重放已完成节点，Go 在无可用 mid-flow checkpoint 时从起点重入（重放前序）——对 http 回调 / 审批 / agentrun 这类副作用即重复执行。
3. **runtime 双真相**：IR 字段被 Python 装载强制 `"python"`（`core/flow.py:655`），console 路由用 DB 列（`runtime_pools.py` 门面 → `flow_versions.runtime` 列）。
4. **多引擎并行维护成本显性化**：每仓独立的对拍工具、审计波、能力清单；覆盖目标达成后边际收益递减。

## 3. 现状写实（2026-10-23 快照，基线见文首）

### 3.1 Python（plaita）——reference

- 装载：`plaita/core/flow.py:655-661`（`parse()`）只接受 `runtime=="python"`，其余具名拒（`Unsupported runtime: ...`）；`Flow.runtime: str = "python"`（`flow.py:72`）。
- 路由：三处全部读 DB 列、零处读 definition 字段——start 队列解析 `plaita-console/backend/api/executions.py:2348-2350`、调度行 `schedules.py:410`、cron 触发 `plaita/server/services/schedule_service.py:96-101`；worker 侧不按 definition runtime 分支。
- 取值域：`plaita/server/runtime_queues.py:35` `VALID_RUNTIMES = ("go", "python", "rust")`、`DEFAULT_RUNTIME = "python"`（`:38`）；非法值在保存/建图路径 422 `invalid_runtime`（`flows.py:196-205`、`graphs.py:71-75`），调度/发布重章路径具名 WARNING 落默认（`schedules.py:406`、`flows.py:619-625`）。
- 治理账本互锁：`docs/governance/architecture.yaml` `runtime_valid_set: [go, python, rust]`（r93 起）↔ 大仓根 `docs/DOC_ASSERTIONS.yml` 的 `gov/runtime-valid-set`（`yaml_path_equals`）↔ `runtime_queues.py` 实况。

### 3.2 Go（plaita-go）——拟 frozen

- 节点面：IR accept-list 47 型（`pkg/ir/ir.go:41`），plaita-nodes 分发 22/22 全覆盖（#24 复核）；`node-parity` 活体门对 Python 注册表双向对拍（AGENTS.md:186-189）。
- **`code` 节点后端（修正 #325 正文引文）**：admitted 后端 = javascript（goja，进程内）+ **python（subprocess sidecar）**（`pkg/workflow/nodes/code.go:52-56`）；go/yaegi 具名拒（A62-04：进程内执行无隔离无中断边界，`ErrInProcessGoUnsupported`）。非 admit 语言双层 fail-closed：装载期 `pkg/ir/normalize.go:947-948`（`code node language %q is not supported (this engine runs javascript or python)`）+ 构造期 `code.go:96-129`。#325 正文「`code` 只跑 JavaScript → 真实 flow 无法加载」系 plaita-go#43（python sidecar）合入前的旧状，以本节为准；生产语义差异仍在（sidecar 为子进程执行，重试分类按 #43 owner 裁决「Python 采纳完整集，Go 不放松」）。
- worker 恢复面（#317 分叉的现状形态）：continue/retry 载体无可用 mid-flow checkpoint 时降级「从 flow 起点重入（重放已完成前序）」——`pkg/worker/worker.go:914-924` 时间线具名事件 `continue_fallback_start`（§9.1 可观测化，#32）、`:938-944` 中途入口被拒同款回退、`:1325-1338` 无 checkpoint 的 `resume_checkpoint_missing` 终态（#325 所引 `worker.go:1328-1360` 一带）。中途入口本体（`ResumeFrom`）已落，回退仅剩 checkpoint 缺失/不可用/被拒臂。
- 维护波：审计批 15 张（#49–#66 系）已全清；open 仅 3——#24（track）、#87（测试修正）、#88（NS-M2 嵌套挂起 v1，裁定当日合并 `dea859f`，属冻结前收官）。

### 3.3 Rust（plaita-rs）——拟 oracle

- 形态：workspace 四 crate（expression / engine / cli / **worker**；worker bin `plaita-rs-worker`）。
- **「Rust 无 worker」已过时（本篇须调和的新事实）**：#325 立单同日，WK-M1（`89527bf`）与入位接线（r15 `47822e0`）先后并入 main（`6c01ffc`）——worker 消费 + registry 能力清单（五键 `engine / ir_contract_version / supported_node_types / unsupported_node_types / features`，SETEX TTL 30s / 心跳 10s，生命周期 starting→running→draining→stopped 墓碑）+ 消费循环读穿透（`RedisFlowStore` 实现引擎 `LocalFlowStore`：本地 miss → console 回源〔六 env 键与 plaita-go 逐字对齐，`PLAITA_CONSOLE_FLOW_SOURCE_URL` 未配置=特性关闭〕→ r11 hash 门 → `ir::load`）全链在 main。cargo 247P。
- 节点面：`SUPPORTED_NODE_TYPES` 25 型（`crates/plaita-engine/src/ir/normalize.rs:34-64`），未移植类型装载期具名拒（fail-closed）；CI `node-parity` 对 Python 活体对拍（例外表 28+1 条）。
- oracle 工具面：**R1–R7** = IR 结构校验规则全集（`crates/plaita-engine/src/ir/validate.rs:5-11`：唯一 start 且可达 / next 引用存在 / 嵌入流程达 end / joinBranches⊆分支名 / `$NODE` 引用可解 / `$F` 调用已注册 / output 字面量匹配 outputType）；`tools/diffcheck.py` 差分门 23/23（Go 参照 vs Rust 候选双跑，KNOWN_RED 空，本地门不在 CI）；指纹 golden 62 例（自 Python 真跑导出，`flow_hash_golden.json`）。
- console 选择性（修正 #325 正文引文）：「console 不可选」系 r12 前旧状——r12 起注册 `plaita-rs`→rust 池（`ENGINES` / `ENGINE_TO_RUNTIME`），`VALID_RUNTIMES` 含 rust，调度行 runtime=rust 可路由。

### 3.4 console local——拟 engine backend

- 机制：启动时 Redis 候选全不可达 → `local_mode=True`（`plaita-console/backend/main.py:217-246`，启动 WARNING 具名）；执行分派走进程内执行器（`executions.py:297-305` `get_local_executor` → `services/local_executor.py`：进程内 `FlowExecution` + InMemoryEventBus + SQLite checkpoint）；审计 `{"mode": "local"}`（`executions.py:2301`），`/health` 暴露 `local_mode`（`main.py:91-92`）。
- 定性：引擎语义仍是 Python reference（同一套 `Flow` 执行面），差别只在执行后端（进程内 vs worker 池）。与 #329-C1「local 降级为 backend」同口径；三处可观测（启动 WARNING / health / 审计）满足本仓 AGENTS.md「任何降级必须可观测」红线。

## 4. 关键调和：「全量入位」（协议面）×「oracle 不接发布」（策略面）

「Rust 已全量入位（owner 2026-10-23 裁决，`6c01ffc` 收官）」与「Rust=oracle 不接发布（#325）」不矛盾——两个说法说的是正交的两个面：

- **入位 = 协议/能力面的事实**：plaita-rs worker 能消费 `plaita:flow:queue:rust`、向 `{prefix}:registry:flow_worker:{instance_id}` 自报五键能力清单、经读穿透按 `flow_id+version` 取定义并过 r11 hash 门。这是技术接线完成度，owner 已裁决收官——**全链保留，并作为 oracle 对拍通道维持**。
- **oracle = 发布/流量面的策略**：哪些 runtime 出现在生产发布目标里。该面由 `VALID_RUNTIMES`（`runtime_queues.py`）及其治理互锁（`architecture.yaml runtime_valid_set` + 大仓根 `DOC_ASSERTIONS.yml` 的 `gov/runtime-valid-set` 断言）治理——即「gov/runtime-valid-set 断言面」。

正交性的关键：**oracle 通道不依赖 console 发布路由**。发布只发生在 python 池（唯一目标）；Rust worker 经读穿透按 `flow_id+version` 取同一份定义做差分执行——队列 / registry / 读穿透照常，唯生产流量永不路由至 rust。

因此 E1 的落地形态裁定如下：

1. `VALID_RUNTIMES` 收敛为 `("python",)`；`runtime=go/rust` 的保存/发布/调度取值 422 `invalid_runtime` 且原因具名（现有 `normalize_runtime` 错误文案「非法 runtime …；可选值: …」天然满足 #325「原因具名」验收）。
2. 账本三处联动：`runtime_queues.py` / `architecture.yaml runtime_valid_set` / 大仓根 `DOC_ASSERTIONS.yml` expected 同步改 `[python]`（`architecture.yaml` 头部维护约定的既有要求；只改一处即断言红）。
3. **不选 flag 门控方案**：flag 意味着生产路径永远存在一个「可被打开的 rust 池」——与「唯一生产运行时」的 fail-closed 表述冲突，且引入第二集合（选择域 vs 发布域）会重新制造 §5 要消灭的双真相形态。oracle 需求由协议面直连满足，发布面不留口；若未来 nightly 矩阵需要 console 驱动 rust run，立新 ADR 再议。

## 5. runtime 单真相裁定（双真相消解）

**裁定：`flow_versions.runtime`（DB 列）是唯一真相；definition/IR 内嵌的 `runtime` 字段是 reference 引擎装载口径的自描述标记，不是路由输入。**

依据（#329-A「状态行是真相」哲学在同一维度的推论）：

1. 路由三处全部读列不读定义（§3.1）；
2. IR 字段唯一读者是 `parse()` 的同值校验（`flow.py:655-661`）——E1 收敛后它恒为 `python`，不携带可路由信息；任何把它当路由真相的代码都是缺陷；
3. 列有完整治理链（取值域 422 / WARNING、账本断言、`DEFAULT_RUNTIME` 迁移语义、列注释自证路由职责 `models/flow.py:122-125`），字段没有；
4. 辨析：`runtime_pools.py` 是 `plaita.server.runtime_queues` 的 re-export 门面（其 docstring 明言「不要在这里加第二份映射逻辑」）；#325 正文以之指代 console 侧真相，实际载体是 `flow_versions.runtime` 列。

迁移注记（D1 执行轮的实施约束）：

1. **publish 回写 × definitionHash 耦合**：#325 执行项「publish 时把 `definition["runtime"]` 写成列值」会改变定义原始 JSON 键集，`definitionHash`（对原始 JSON 的 canonical 口径）随之改变——若回写只改内容不重算 hash，r11 hash 门（rs `flow_hash.rs:540-572` `verify_definition_hash`，go 同款）将以 `flow_hash_mismatch` 具名拒载，在途 resume 首当其冲。回写必须与 hash 重算同一写事务，或前移至 save（hash 计算之前）；三引擎指纹 golden 须补「带 runtime 键」形态用例钉死口径。
2. `validate_definition`（`plaita-console/backend/services/validation.py:42`）现无 `target_runtime` 形参——按 #325 增加，发布/保存按列值校验（含节点支持门）。
3. **存量检查分支**：`flow_versions` 无非 python 行 → 直接收敛 E1；有 → 先冻结 go/rust 新发布、存量迁移完成后收敛（对应 #325 验收「存量例外单列」）。
4. console rust 池识别面（`ENGINES` / `ENGINE_TO_RUNTIME` 的 rust 臂、capability_gate rust 池矩阵、issue300 测试族）随收敛退役或改指对拍链路；registry 五键闸本体保留（§4 协议面对拍仍消费）。

## 6. Go=frozen 后果清单

1. **安全修复 only**：依赖 CVE、沙箱逃逸、租户越权类修复照常受理；行为性变更（语义/节点/协议）不再接受。非行为的测试/构建修复允许（否则 CI 无法守绿），但不得借测试改动夹带语义变化（此口径细化如与 owner 意见不符，以 owner 为准）。
2. **不再加节点与功能**：accept-list 47 型冻结；`knownUnsupported` 7 型（calculate / ignore / sleep / trpc / mdb / readmdb / writemdb）永不补；plaita-nodes 22/22 维持现状。
3. **#24 Wave 2 覆盖清单冻结延后**：owner 已于 plaita-go#24 留言（2026-10-23）「待 Go 引擎解冻（若发生）再评」。
4. **审计批 15 张（#49–#66 系）为最终维护波**：已全清，不再开新的批量审计/变异波。
5. **worker fresh-run parity 回退不再追平**：§9.1 回退臂（`continue_fallback_start` 具名时间线 + `resume_checkpoint_missing` 终态）保持现状作具名可观测降级，不投入使其与 Python 零重放语义对齐——Go 池不在生产目标内，残余分叉只影响对拍归因（对拍工具已按每节点尝试数归一口径，#317 评审）。
6. **不删仓、不追性能；`node-parity` 活体门与既有 CI 照常运行**（防单侧漂移的最后防线）；#88（NS-M2，`dea859f`）为冻结前最后一批功能面，照常验收。

## 7. 裁定依据（逐条）

- **Python = reference 且唯一生产**：语义权威——rs/go 两仓的 expression / IR / 指纹 golden 均自 Python 真跑导出，两仓 AGENTS 明文指 plaita 为对拍权威；用户产品形状（§2.1）只有 Python 全部满足；三引擎中唯一无恢复语义分叉在案者（#317 的分叉主体是 go）。
- **Go = frozen**：owner 拍板（#325，2026-10-23）；覆盖目标已达成（plaita-nodes 22/22、accept-list 47 型）后边际收益递减；#317 实证的恢复语义分叉属引擎深层语义，追平成本高于非生产引擎的收益；审计波已收官。佐证：#317 owner 注记「D1 已裁定 Go runtime 冻结；本单 retry/suspend 部分与 publish 门消费矩阵随之取消」。
- **Rust = oracle**：R1–R7 规则集 + diffcheck 23/23 + 指纹 golden 62 例构成现成对拍权威面（§3.3）；入位接线完成使其能对真实生产定义做差分（§4 正交性）；owner 拍板「不接发布」。oracle 的持续义务：对拍结论失真类缺陷仍须修（如 A137-01 rs 侧重试分类偏离，#317 评审补注②，归宿建议 rs 仓独立小单）。
- **local = engine backend**：机制即 Python 引擎进程内化（§3.4），非独立语义；#329-C1 同口径；可观测性已达标（AGENTS.md「任何降级必须可观测」）。

## 8. 执行影响面与后续（均非本篇范围）

| 事项 | 仓/落点 | 说明 |
|---|---|---|
| E1/E2 实施（§4 + §5 注记 1–4） | plaita | 验收 = #325 四条：ADR 落地（本篇）；账本 `VALID_RUNTIMES` 断言全绿；`runtime=go` 发布被拒且原因具名；Go flow 的 IR 与 DB 列一致（存量例外单列） |
| E3 #317 缩围 | plaita-go / plaita-rs | 保留 IR/表达式 golden 主体（先对账 rs `tools/diffcheck.py` 现 23 案勿重建语料，#317 评审补注①）；删 publish 矩阵消费；A137-01 另立 rs 小单 |
| 各仓定位文档同步 | plaita-go / plaita-rs | go AGENTS.md:68「残余偏差：尚无中途入口」已过时（`ResumeFrom` 已落）；rs README「不做 Flow Worker」「diffcheck 15/15」已过时（worker 已落 / 23/23）——各仓自行小修 |
| #329 其余子单 | plaita | A1/A2/B1/C1/C2/E1/E2/F1 按 #329 建议序推进（D1 = 本篇为首） |
| 已合并未关单收尾 | plaita-rs / plaita-go | rs#17（WK-M1）、go#88（NS-M2）待关单流程 |

## 9. 边界与修订

- 不删 Go/Rust 仓；本篇不要求 plaita-rs 任何代码变更（#325 边界「不动 Rust」）。
- 本篇只做裁定与写实，零产品代码；实施按 §8 单独立项。
- §3 为点时快照，事实漂移不改写本篇；治理账本与各仓 AGENTS 是活源。
- 裁定变更须立新 ADR，或经 #325 评审轮（A1/A2 规格评审）修订本篇并留痕。
