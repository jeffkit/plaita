# 跨引擎行为一致性消费矩阵（#317 §2/§3）

- **状态**：首次落盘（2026-10-24，#317 实施；go 列证据=plaita-go `fix/issue317-samples` 28/28 实跑）
- **落点**：`docs/governance/conformance-matrix.md`（与治理账本 `architecture.yaml`、ADR-001 同址）
- **证据基线**：plaita `a56125ab` · plaita-go `fix/issue317-samples`（基于 main `c6c323b`）· plaita-rs `6c01ffc`（A1 r137 静态引证，未实跑）
- **关联**：#317（本篇来源）；ADR-001（#325，Runtime 组合裁定，本篇的治理框架）；#300（结构面清单，关系见 §4）；#316（运行期对账器，关系见 §5）；#315（Stage 0 不新增阻断门禁，门消费姿态见 §6）；plaita-go#43（确定性错误四类白名单裁决，E1-E6 的口径源）

## 1. 定位

本矩阵是**发布期静态面**的行为语义事实源：同一 case 在三个引擎池上与权威口径（Python 逐行）的一致性。三列对应三个现实部署池——`VALID_RUNTIMES = ("go", "python", "rust")`（`plaita/server/runtime_queues.py:35` 今日实况）。

**ADR-001（#325）框架下的读法**：

- Python=reference 且唯一生产；Go=frozen（仅安全修复）；Rust=oracle（不接生产发布）。矩阵因此有双重身份：
  1. **oracle 通道的对照记录**——ADR-001 §4「入位=协议能力面作 oracle 通道」的对拍面保留，本矩阵是其行为语义输出；
  2. **Go frozen 已知分叉的收敛进度表**——red-declared 格的每夜红=分叉仍存的证据，收敛单落地翻绿=自动验收（A1 §3）。
- ADR-001 配套裁定 E3 将 #317 缩围为「IR/表达式 golden、删 publish 矩阵消费」；**矩阵门消费本篇不接线**（见 §6）。缩围后本矩阵作为对拍通道记录与收敛进度表仍然成立——这正是它现在以文档形态落在 governance 而非 console 门里的原因。
- 若 E1 落地（`VALID_RUNTIMES` 收敛 `("python",)`），go/rust 两列转为历史对照记录，矩阵不删。

**行=case（不是节点类型）**：33+ 节点类型 × 语义维度在 28 例封顶前提下不可展开；case 是钉子单位。「能不能跑某类节点」的结构面归 #300 清单（§4）。加载一致性**不进矩阵值域**——结构面归 #300 清单（交集判定），go/rs 装载面差异归 diffcheck 契约案（plaita-go `tools/diffcheck.py`，r129-r131 的 14 分歧归裁 a=2/b=4/c=7/d=1 与 r129-130 表达式 golden 在此引用不重复）。

## 2. 值域

| 值 | 含义 |
|---|---|
| `green` | 行为与权威口径一致 |
| `red` | 行为分叉**未声明**（缺陷候选，须提单） |
| `red-declared` | 已声明分叉：方向注记 + 收敛指针（见 §3 类 e）；不判缺陷 |
| `n/a` | 目标池 manifest `supported_node_types` 不含案例涉及节点类型——**从 #300 清单派生，不手写第二份**（单源；此类格在门判定时结构性不可达）。本篇未认领任何 n/a 格 |
| `pending` | 尚未对齐（本篇对 A1 四值域的增补）：rs 列对接前的事实态；`pending` 不得长期存留，对接后翻 green/red |

证据标注约定：`(实跑)` = 本轮 shadow harness 28 例实测（py↔go）；`(引证)` = A1 r137 评审的静态代码引证（给 file:line），未实跑复核。

## 3. 分叉分类：类 e「已声明行为分叉」（首例 R2）

r131 装载面 14 分歧的 a/b/c/d 四类归裁口径不变（引用 #317 评审区，不重复）。本篇按 A1 §3 裁决建议**增设类 e**：

> **类 e（declared divergence）**：方向明确（权威侧 vs 偏差侧）、后果明确（外部世界可观测）、有收敛计划（引擎面单），但不满足 (c)「语义等价的执法层差」的语义等价前提，也不宜按 (a) 立即修口。落矩阵=`red-declared`；落两侧 AGENTS.md=声明。

> 注：轮次指令把重放分叉转述为「c 类（可接受偏离）」，A1 r137 §3 的裁决实为**不入 (c)、立新类 (e)**——(c) 要求语义等价，而副作用节点重复执行是外部可观测的状态变化，不满足。本篇按 A1 原议落 (e)。

**R2 即首例**：Python 自 checkpoint/last_node 步进**不重放**已完成前序节点（`ResumeType.CONTINUE`，失败节点在 checkpoint 无条目、从 last_node_id 后继步进恰好重跑失败节点——G1）；Go 的 continue 载体从 flow 起点重入、**重放**已完成前序节点（plaita-go AGENTS.md §worker 二期自述）。纯计算 flow 输出等价（重放不可见），但对 http 回调/审批/agentrun 等副作用节点即重复执行。方向：Python=权威；Go=偏差。收敛指针：Go 引擎中途入口单（Stage 1）。矩阵格=go 列 R2 行 `red-declared`；两侧 AGENTS.md 已声明（plaita-go §worker 二期、本仓「深入阅读」节）。

## 4. 与 #300 manifest supported 清单的关系

- **清单=结构面**（能不能加载、执行某类节点；`capability_gate.py` 交集判定，无清单时 fail-open 兜底保持不动）；**矩阵=行为面**（行为是否与权威一致）。
- 门消费顺序（若未来接线，§6）：先清单门后矩阵门。矩阵不复制清单内容；`n/a` 单源派生自清单 `supported_node_types`，保证两边永不矛盾。
- 顺带（A1 r137）：n/a 判定勿读 manifest `features` 字段——rs 清单 `features: []` 与引擎实况漂移（A137-02），features 现阶段不可作判据。

## 5. 与 #316 对账器的关系

- **矩阵=发布期静态面**：发布时点的行为语义事实（「这个 flow 发布到这个池会表现如何」的依据面）。
- **#316 `scripts/carrier_audit.py`=运行期动态面**：在途载体的六类失联信号（stream/PEL/订阅/timer/DLQ/隔离流），独立只读。
- 互补不重叠：矩阵回答「语义对不对」，对账器回答「载体丢没丢」。

## 6. 门消费姿态（本篇不接线）

- ADR-001 E3 已将 #317 缩围删 publish 矩阵消费；且 #315 Stage 0 约定不新增阻断门禁。原建议 4 的 422 fail-closed 门**不实施**。
- 若 owner 未来重新启用行为级门，按 A1 §2 的细化语义：`red`→422（错误体带 case_id/runtime/reason）；`red-declared`→**不 422**，结构化 WARNING + 响应体带 matrix 引用（换道/降级必须可观测）；缺格/文件缺失→422 `reason=no_data`/`matrix_missing`；进阶硬门（副作用节点 ∧ red-declared→422）属行为级门，留 owner-call。
- 本篇的矩阵文档缺失/漂移由评审流程（而非门禁）守护：改对拍语料不更新本矩阵=评审驳回项。

## 7. 矩阵

权威口径=Python 逐行；`python` 列全绿是构造性的（reference 即权威），不逐格标注证据。

### 纯计算（01-16，M2 存量，driver=fresh）

| case_id | 节点类型 | python | go | rust | 注 |
|---|---|---|---|---|---|
| 01_assignment_concat | assignment/end | green | green (实跑) | pending | $F.concat/add 与模板插值的算子面 |
| 02_assignment_math_chain | assignment/end | green | green (实跑) | pending | $F.mul/sub/add/div 链 |
| 03_assignment_template | assignment/end | green | green (实跑) | pending | {% %} 模板方言 |
| 04_switch_priority | switch | green | green (实跑) | pending | priority 降序命中 |
| 05_switch_default | switch | green | green (实跑) | pending | default 分支 |
| 06_switch_contains_and | switch | green | green (实跑) | pending | 条件组 and+contains |
| 07_loop_double | loop/child | green | green (实跑) | pending | 逐元素 inline 子流程 |
| 08_loop_with_index | loop/child | green | green (实跑) | pending | $INPUT.item/index |
| 09_loop_empty | loop | green | green (实跑) | pending | 空集合 → None |
| 10_parallel_join | parallel | green | green (实跑) | pending | 分支继承父 $INPUT |
| 11_parallel_mixed_input | parallel | green | green (实跑) | pending | 显式/继承混合 |
| 12_child_inline | child | green | green (实跑) | pending | 输入表达式注入 |
| 13_child_with_loop | child/loop | green | green (实跑) | pending | 嵌套组合 |
| 14_parse_json_success | parse_json | green | green (实跑) | pending | 成功路径 |
| 15_parse_json_fallback | parse_json | green | green (实跑) | pending | 兜底合并 |
| 16_parallel_switch_nested | parallel/switch | green | green (实跑) | pending | 嵌套组合 |

### 错误分类与重试归属（17-22，driver=fresh；比较信号=每节点尝试数+终态错误类）

| case_id | 节点类型 | python | go | rust | 注 |
|---|---|---|---|---|---|
| 17_error_e1_valueerror_nonretryable | code | green | green (实跑) | green (引证 `nodes/code.rs` Deterministic) | 确定性四类白名单不烧预算（plaita-go#43 口径）；retryTimes=5 两侧均 1 次尝试 |
| 18_error_e2_keyerror_retryable | code | green | green (实跑) | **red** (引证 `decode_python_runner_output` 全 ok=false 判 deterministic) | 集外类型维持可重试缺省（py 4 次尝试）；rs 1 次=A137-01，修口=四类白名单镜像 plaita-go#43 |
| 19_error_e3_exit_noenvelope_retryable | code | green | green (实跑) | **red** (引证 `code.rs:402-414` 无输出/坏 JSON 同判 deterministic) | 基础设施失败可重试（py 4 次）；rs 1 次=A137-01 同族，与 E2 分打信封解码/退出状态两路 |
| 20_error_e4_end_errorresult_nonretryable | end | green | green (实跑) | green (引证 `nodes/mod.rs:511-515`) | resultType=error 是 flow 的决定不是瞬态（#43 后半）；三侧一致钉死防回归 |
| 21_error_e5_http_unsupported_scheme | http | green | green (实跑) | pending | 实测修正 A1 表格引文：两侧均落 do-request 期 1002 瞬态（非 1001 生成期），retryTimes=3 下 4 次尝试；rs 待测（A1 引文同随此修正存疑） |
| 22_error_e6_http_loopback_refused | http | green | green (实跑) | green (引证 http.rs Transient) | loopback 拒连=传输瞬态 1002，retryTimes=2 下 3 次尝试；hermetic 显式 127.0.0.1 避代理 env 面 |

### 节点结果形状（23-26，driver=fresh）

| case_id | 节点类型 | python | go | rust | 注 |
|---|---|---|---|---|---|
| 23_shape_s1_end_nop | assignment/end | green | **red-declared** `load-refusal:go` (实跑) | pending（A1 引证 execute_end 有 "nop" 臂，结果形状待实测） | end resultType=nop（静默结束）：py result=null；go 装载期具名拒（fail-closed，从严方向）。声明=go 拒载钉 py 行为；若 rs 实测与 py 同则 green |
| 24_shape_s2_end_error_numeric_code | end | green | green (实跑) | pending（**疑似 red**：A1 引证 `EndError.code` 是 String，数字 code serde 拒或折叠；待实测） | 数字错误码类型保真是消费方 switch 契约；三向分叉高发点 |
| 25_shape_s3_end_output_missing_var | end | green | green (实跑) | pending | end output 引用缺失变量：模板方言经典分叉点（静默空串 vs 具名/None），实测两侧一致 |
| 26_shape_s4_code_null_penetration | code/end | green | green (实跑) | pending | None 穿透求值（`$NODE.c.a.b`）：静默 nil 族的求值层版本（#63 修的是文法层），实测两侧一致 |

### 挂起恢复（27-28，driver=event-resume / continue-after-error）

| case_id | 节点类型 | python | go | rust | 注 |
|---|---|---|---|---|---|
| 27_suspendresume_r1_event_resume | event/assignment/end | green | green (实跑) | pending（WK-M1 worker resume 链待测——正是矩阵该回答的格） | 比较口径=恢复后节点状态的确定性公共核（status 翻转 completed + event_data 保真）；event_id 时间戳/subscription_id 运行时指派/挂起态形状 extras 两侧结构性不同，不进比对面。驱动不对称已文档化：py 分布式 resume 只处理被恢复节点本身+逐拍步进；go ResumeSuspension 一步到位 |
| 28_suspendresume_r2_continue_replay | assignment/code/end | green | **red-declared** `replay-from-start:go` (实跑) | pending（A1：待测） | §3 类 e 首例：go 重放已完成前序（原始轨迹 A 双现 6 事件留证），py 不重放（A 单现）；副作用重复执行风险；收敛=Stage 1 中途入口单。实测补充：py 分布式首拍止于失败边界（start+a 返回），continue 载体自 checkpoint 步进恰好重跑失败节点 |

## 8. 负控记录（#317 验收条款）

对拍器判定必须有牙——两处临时变异（均已还原，不入库）：

1. **表达式语义**：`pkg/expression/plaitaexpr/math_functions.go` `add` 整数分支 `return s` → `return s + 1`：
   `TestSamplesAgainstPython/01_assignment_concat` 红：`diff $.count [value_mismatch]: numeric 4 != 5 (python=4 go=5)`（终值+轨迹三层同红）。
2. **重试归属**：`pkg/workflow/nodes/code_python.go` `pyDeterministicEnvelopeTypes` 白名单判定短路（确定性错误误判可重试）：
   `TestSamplesAgainstPython/17_error_e1_valueerror_nonretryable` 红：`diff trace[...][1].attempts [node_attempts_mismatch]: node attempt count 1 vs 6 (retry attribution) (python=1 go=6)`——重试预算全额烧尽被尝试数信号当场抓获，即 plaita-go#43 的活体防回归。

## 9. 复现

```bash
# py↔go 对拍（无 Python 仓时自动 skip；http 用例 hermetic 零出网）
cd plaita-go && go test ./pkg/shadow/ -count=1

# 报告重生成（docs/SHADOW_REPORT.md）
cd plaita-go && go run ./cmd/shadow-compare \
  -python-repo ../plaita -md docs/SHADOW_REPORT.md -json /tmp/shadow-report.json
```

rs 列对接路径：`tools/diffcheck.py`（go↔rust 双跑）扩执行语义案 + rs CLI 补 `attempts` 字段（A1 §1 harness 前置），对接后本矩阵 rs 列逐格翻牌。
