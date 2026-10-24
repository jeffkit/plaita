# r96 —— C1 阶段 2：local 降级 engine backend + conformance 双后端套件（plaita#323）

- **席次**：B2 第九十六轮 · worktree `plaita-wt2` · 分支 `fix/c1-stage2-local-backend`（f60e990a + bcc263bb，两阶段提交）
- **规格源**：#323（P1）正文 + ADR-001 §1（`docs/governance/adr-001-runtime-composition.md`，owner 已裁「console local = engine backend，不是第四种执行语义」）+ r94 勘明（FlowWorker 基类存储抽象 + storage/{memory,sqlalchemy} 在位；test_local_* 钉死进程内语义：取消 Event / 强删墓碑 / 线程日志处理器）
- **边界门**：基线 HEAD=origin/main（9eda18b8，含 0652c1bb=阶段 1）

## 交付

1. `f60e990a` **阶段①**：`services/engine_backend.py`（新，~470 行）+ `tests/console/test_engine_backend_conformance.py`（新，14 用例）+ 边界门 `_DOMAINS` 登记 `services.engine_backend=gateway`（一行，门的既定扩点）。
2. `bcc263bb` **阶段②**：`local_scheduler.fire` 执行起跑切 `engine_backend.start_execution`（模块内惰性导入换名 + docstring 注记；API 面与测试调用面零变化）。

## 修法（engine backend 形态）

复用集群同一个 `FlowWorker` 状态机（`start_flow` 内含步界驱动循环 / 取消检查点 / 租约面 / usage·timeline 采集），只换 storage：

- `LocalExecutionStorage`：`local_executions` 行适配。error↔failed 词汇一次性收口；两条 CAS 规则消覆写战争（与旧环同一 `update_local_execution_status_if` 原子原语）：**非终态写不复活终态行**（镜像跳过返回 True，不喂 `StatePersistError`）+ **首个终态获胜**（同终态补写只丰富字段=取消点 context 快照，`_finalize_cancelled` 同语义）；空 context 写 `'null'` 保持行形状（context=None 非 {}）。
- `ConsoleFlowStorage`：定义**只读**（#323 边界「不动 SQL 为权威的定义模型」）；租户随执行线程引擎上下文（与 Redis namespace 路由同构）；version 缺省走 `_latest_published` 同源解析。
- memory 腿：`MemoryExecutionStorage` + `DictFlowStorage` + `MemoryLogSink`（参照形态，无 console SQLite 依赖）。
- **注册表共用**：线程/取消 Event/强删墓碑与 `local_executor` 共用模块级表——`cancel_local_execution` 置位同一 Event、`delete_local_execution` join 同一线程、日志抑制同一窗口 ⇒ **API 面对 engine 起跑的执行零感知**。
- `EngineBackendWorker._cancel_requested` 改读进程内 Event 表；回滚开关 `PLAITA_DISABLE_CANCEL_CHECKPOINT` 同源生效；`invoker_for_trigger` 的 worker 行不改写本地行 invoker（insert 时落"local"，零变化）。

## conformance 双后端套件（等价性证明基建）

同一组行为用例 `local` / `memory` 各跑一遍（7 用例 × 2 = 14，`docs/proofs/data/r96-conformance.txt` rc=0）：取消 Event（T8 步界收口 + 取消点 checkpoint + 迟到 output 不写）/ 挂起 checkpoint 落行 / 墓碑（强删日志抑制 + 行清空 + preminted 拒绝）/ 日志处理器（线程过滤 + 随删清理）/ 租约面（NullExecutionLease 全链）。

**负控与方法论注记（三条，后续席次可复用）**：

1. **`_spawn` 同 id 清墓碑是旧环既定语义**——preminted 拒绝用例不能经 `launch` 起跑（`_spawn` 先 discard 墓碑再跑，start_flow 必放行）；改为直驱 `worker.start_flow` 取证（`deleted/discarded` + stub 类级 call_log 空 = 引擎从未进入）。
2. **墓碑窗口内同 eid 日志本就被抑制**——不能靠日志线在该窗口取证（第一版即栽在此：memory 腿过、local 腿 "status=deleted" 日志永远 absent）。取证通道换 call_log。
3. **pytest 下 root logger 缺省 WARNING**——探针 logger 与 engine backend 模块 logger 须在夹具里钉 INFO（布局无关：钉 `engine_backend.logger` 对象而非字符串名，双布局 logger 名不同）。

## 守恒证明（refactor 零行为变化）

- **既有家族零改动**：本轮未改 `local_executor.py` / `api/*` / `models/*` / `flow_store.py` 一行（`git diff 0652c1bb..bcc263bb --stat` 仅 4 文件：2 新 + 2 上述）。
- 受影响 12 家族 121P 全绿（含 fire 实跑的 test_local_scheduler、直调 fire 的 a48 生命周期、r94 边界门）：`docs/proofs/data/r96-families.txt` rc=0。
- **console 全量 1592P / 13 skipped**：`docs/proofs/data/r96-console-full.txt` rc=0（r100 基线 1577P，增量 = 14 conformance + main 后续并入用例）。
- ruff 改动 4 文件全过：`docs/proofs/data/r96-ruff.txt` rc=0。
- **阶段独立回绿**：临时 worktree 挂 `f60e990a`（阶段①树，未含 fire 切换）跑 conformance+边界门+local 双家族 = 43 passed rc=0。

## 已声明分叉（不属四域，随套件 docstring 与模块 docstring 双落）

- worker 协议字段（flow_hash/generation/pending/next_step/timeline/node_timings 的 ExecutionState 侧）本地行无列不落——本地档 resume 走既有 API 旧环，指纹守卫属集群重投语义；
- 本地墓碑 = 进程内强删窗口（线程收口即清，A43-01 原语义），非集群持久墓碑；
- API 的手动 start/resume 仍走 local_executor 旧环（**阶段 3 收口**：合并后删第二套 executor；engine-started 执行的 resume 经旧环读行内 context 续跑，行形状已对齐）。
- 行为微差如实记：start 建档后 worker 首落 running 不再写空 context（旧行 context=None 保持）；集群 `invoker=worker` 语义不进本地行。

## 残面

- `_dispatch_suspend_pending`（挂起服务补派）在无 Redis 本地档按基类降级（pending 不落行 ⇒ 无补派凭据）——本地档服务型挂起的补偿派发本就不在旧环能力内，无回退。
- `MemoryLogSink.forget` 与 `fs.delete_local_logs` 对齐（随删清日志）；集群档无此面。
