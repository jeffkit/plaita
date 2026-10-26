# ADR-002：Go 解冻——evolving companion（跟随 Python 进化）

- **状态**：已接受（Accepted）——owner 拍板 2026-10-25
- **裁定源**：owner 指示「go 不应该再是冻结，可以跟随 python 一起进化了」（2026-10-25，执行台阶两级终版并树当日）
- **关联**：ADR-001（本篇修订其 Go= frozen 裁定，其余裁定——Python reference / Rust oracle / local engine backend——**全部维持**）；plaita-go#24（Wave 2 覆盖清单，owner 留言「待 Go 引擎解冻（若发生）再评」——**本篇即解冻事件**）

## 1. 裁定

| 维度 | ADR-001（2026-10-23） | ADR-002（2026-10-25） |
|---|---|---|
| Go（plaita-go） | frozen：仅安全修复，不加节点与功能 | **evolving companion**：语义面跟随 Python 进化；行为性变更恢复受理 |
| Python | reference 且唯一生产运行时 | **不变** |
| Rust | oracle | **不变** |
| E1/E2（`VALID_RUNTIMES` 收敛 python-only、runtime 单真相=DB 列） | 已裁定未实施 | **维持**——解冻 ≠ Go 重回生产发布路由；Go 是否恢复为生产可选 runtime 是独立决策，留 owner（见 §5 open questions） |

一句话：**Go 从「历史第二实现，只保安全」变为「第二实现，跟随进化」**——Python 定义语义的权威地位不变；Go 的义务从「不漂移」（node-parity 活体门）升级为「跟随时区」（Python 的节点契约/能力词汇变更在同一发布窗口内跟随落地）。

## 2. 跟随机制（sync protocol）

1. **Python-first，同窗口跟随**：节点契约面的变更（新增节点类型、执行台阶词汇、`wake_service` 接线、IR 契约版本 bump、能力清单键形）在 plaita 先落；PR 正文须标注「**Go 跟随项**」，plaita-go 以独立小单在同一发布窗口内承接（不阻塞 Python 并树，但超窗未跟随时 node-parity 活体门会红——红灯即催办）。
2. **对拍锚即同步面**：治理账本 `gov.capability_matrix`（两值台阶矩阵）↔ Go `pkg/ir` 对拍测试（`TestExecutionTierParityWithPythonLedger` 等，2026-10-25 起为**首个真实对拍**——勘定此前并无 Go 测试消费导出矩阵）↔ Python `scripts/export_capability_matrix.py --check` 三点一致。契约变更改锚须两侧同 PR。
3. **新节点加入走 #80 形制**：工厂注册 + IR golden + manifest accept-list 自动生成纪律（#300 硬性要求不变）+ `ContractVersion` bump 规则不变（消费者可感知才 bump）。
4. **Rust 不跟随本裁定**：oracle 的价值恰在「独立再实现」；其同步义务仍限于对拍结论失真类缺陷（ADR-001 §7）。

## 3. 解冻恢复的义务面（从 ADR-001 §6 的「永不」降级为「候选」）

| ADR-001 §6 冻结后果 | 解冻后状态 |
|---|---|
| 1. 安全修复 only | 照旧受理；**行为性变更恢复受理** |
| 2. 不再加节点与功能（accept-list 47 型冻结） | **解除**——新节点走 §2.3 形制；`knownUnsupported` 7 型仍逐型评估（calculate 等的拒载提示本身是对拍锚，解除须两侧同步） |
| 3. #24 Wave 2 覆盖清单冻结延后 | **解冻恢复评审**（owner 留言的触发条件已成立；列跟进单，非本篇实施） |
| 4. 审计批为最终维护波 | **解除**——恢复常规审计/变异波节奏，随跟随时区自然发生 |
| 5. worker fresh-run parity 回退不再追平 | **降级为候选跟进项**：`continue_fallback_start` 回退臂保持具名可观测降级现状；是否投入对齐 Python 零重放语义由 owner 另拍（不因解冻自动背上此债） |
| 6. 不删仓、不追性能；活体门照常 | **照旧**——node-parity 活体门与既有 CI 仍是防单侧漂移的最后防线 |

## 4. 边界（解冻不改变的事）

- Python 仍是语义权威：expression / IR / 指纹 golden 的导出源不变；Go 对齐以 Python 真跑产物为对拍基准，不接受 Go 侧私自定义语义。
- console/keeper 控制面留在 Python 侧（mona.yaml 定位不变）；Go 定位仍是「引擎 + worker 运行时」。
- 本篇不构成对 Go 的任何排期承诺：跟随义务的触发器是 Python 侧契约变更本身，不是时间表。

## 5. Open questions（留 owner）

1. Go 是否恢复为**生产可选 runtime**（E1 收敛反向修正，`VALID_RUNTIMES` 恢复 go）——解冻是前置条件而非充分条件；涉及 §4 正交性与存量 go flow 迁移面，另拍。
2. fresh-run parity 回退臂追平（§3.5）是否立项——候选，另拍。
3. Rust 是否在 oracle 之外获得任何跟随义务——本篇明确**否**，除非 owner 另裁。
