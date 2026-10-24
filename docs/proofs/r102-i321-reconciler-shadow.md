# r102（B3 修复席）· #321 A2 载体对账器 shadow——单分类器双视图，零控制动作

- 单：plaita#321（P1，depends-on #320/#316 均已落；#329-A2 线）；规格修正源=A1 r138 评审（okguitar，已按任务书升格为要求）
- 分支：`fix/a2-reconciler-shadow`（独立 worktree `/private/tmp/plaita-b3-r102`）
- 基线：任务书派单时 main → 开工实测 `9eda18b8`（B1 r89 收官 + #330 合入），门禁前后 `origin/main` 零漂移（开工对账 fetch 复核两次）
- 分支命名注记：任务书 worktree 处写 `feat/…`、推送处写 `fix/…` 不一致；按仓内 A2 线惯例（`fix/a2r50…58`）与推送目标统一取 `fix/a2-reconciler-shadow`。

## 规格 → 落地（A1 r138 四条修正全数接住）

| 评审修正 | 落地 |
|---|---|
| 「无载体」规范性引用 #316 四存放处分类器，勿自造第二分类器；shadow 直接消费 #316 JSON 快照同源 | shadow 候选集与 actual 侧**只**来自 `carrier_audit.scan_round`+`render_snapshot` 产出（无第二台扫描器）；测试侧快照亦由真实 audit 产出，不手拼。为使快照可承载 per-execution 视角，对 `scripts/carrier_audit.py` 做**加法扩展**：快照 schema 1→2 新增 `executions` 段（非终态全集 + 带载体终态行，逐行 `carriers/messages/verdict`，verdict 复用 `_verdict`）；gauge 族与既有键全部不动，消费方零破坏（console 后端无快照消费点，grep 实证） |
| 边界矛盾修正：阶段 1 shadow 纯只读；控制类动作留阶段 3（worker fenced CAS 通道） | `scripts/reconciler_shadow.py` 零写路径：Redis 面复用 `carrier_audit.ReadOnlyRedis` 白名单只 GET 状态行；产物只落文件（tmp+rename 复用 `carrier_audit._atomic_write`）；无游标/守卫键。fence+cancel / pending_lost / 终局化仅以**意图记录**（`action_class=control`）呈现，明确标注阶段 3 |
| 扫描面改 progress_at 之外的载体存在性判据（progress_at 阈值扫不到新鲜 queued 行） | 候选判据=快照 per-execution 段（非终态全集，含刚建立即丢载体的行）；`progress_at` 全程不读；mode 5（租约新鲜无步界推进）出圈声明同 #316（检测面仍归 reap_zombie，fence+cancel 属控制类留阶段 3） |
| expected 侧（本单新增「按执行重建」视角） | `expected_carriers(status, pending)`：queued→{message}；running→在途消息假设（pending None）或按 kind；suspended→kind 决定，pending None=老行 orphan 不猜 expected；terminal/枚举外→{}。pending 读面复用 #320 `ExecutionState.effective_pending()`（老行 `dispatch_pending`→service、`$NODE.<id>.subscription_id`→event 惰性推导，同口径不编造 deadline） |
| custody 义务（DLQ/隔离流在押=有载体不补） | 「message」期望由 pel **或** dlq 满足；custody 行产 `custody_replay`（observe）信息意图、绝不产 lost_message；终态行+DLQ 先判 gc（重放已完成执行=双执行，只能清） |
| 到期分支「重建或 error(pending_lost)」 | `kind=event` 过 deadline / `kind=timer` 过 due_at 且对应载体缺席 → `finalize_pending_lost`（control）；未过期/不可解析（migrated 不编造）→ rebuild（enqueue）。已到期但载体仍在场=仍 armed，不是差异 |
| 双向互证（同源不同视） | `mutual_verify`：per-execution 段逐格重放快照 `cells`/`violations.missing`/`orphan_terminal`，每轮固定校验写进报告（`mutual_verification` 段）；负控含「撕裂快照两视」格（篡改 cells 计数 → 失配具名可观测） |

## 意图×动作类枚举（gauge `plaita_reconciler_shadow_intents{intent,action_class}`，全格显式 0/N）

enqueue（阶段 2，不写状态行）：`start`（queued×丢消息）/ `continue`（running×丢消息）/ `redispatch_service` / `rebuild_subscription` / `rebuild_timer`；
control（阶段 3，fenced CAS）：`finalize_pending_lost` / `gc`；
observe：`custody_replay` / `inspect`。
差异 gauge `plaita_reconciler_shadow_diffs{kind}`：lost_message / lost_subscription / lost_timer / extra_carrier（任务书四格）。
自指标四件（meta-alert 语义同 #316）：`_last_success_timestamp` / `_duration_seconds` / `_check_errors` / `_sources_failed`。

## 先红后绿

- 红相：`git stash push scripts/carrier_audit.py`（回 schema 1 基线）跑最终测试文件 → **19 failed / 4 passed**（`data/r102-tests-shadow-prefix-red.txt`）。红全部落在行为面（schema2 段缺失→shadow 整轮 degraded→差异/意图/互证/降级语义全部失格），非 import 错误；4 绿=shadow 脚本自身的纯函数/配置格（expected 推导表、配置 fail-visible、白名单拒写、load_snapshot），与快照段无关，基线上本就该绿。
- 绿相：pop 后 23P（`data/r102-tests-postfix-green.txt`，与 carrier_audit 50 + issue320 19 合跑 92P）；补 e2e 接线格后终版 24P（`data/r102-tests-shadow-final.txt`）。

## 负控格清单（24 格）

四差异格：丢消息×3 形态（queued→start / running→continue / suspended×service→redispatch，各带修复态归零）+ DLQ custody 不补格；丢订阅×3（armed→rebuild / 过期→finalize / **老行 effective_pending 推导**）；丢定时器×2（armed / 过期）；多余载体×2（终态→gc 带互证 / 双视互补格——#316 判 pel 非 missing 而 shadow 仍报 lost_subscription+extra_carrier）。
零差异正控：四类健康执行 diffs 空、intents 空、互证三项全真。
同源互证：混合世界 7 执行（含损坏行/老行 orphan/终态 orphan/第二租户）cells+missing+orphan 全重放相等，并钉「#316 missing=3 vs shadow 可重建差异=2」（老行 orphan 不猜 expected）+ 撕裂快照失配格。
降级×3：低版快照（schema1 → sources_failed 具名）/ 过期快照（loss 不进 gauge、意图 verifiable=false、放宽阈值旋钮即恢复）/ 快照文件缺失（轮级上抛 rc=1、不覆写上一轮产物）。
只读×2：shadow 轮前后 Redis 键集 `DUMP` 逐字节不变；ReadOnlyRedis 拒写。入口接线×1：`main --once` 真走 audit `flush` 快照文件→shadow 落报告。配置×2 + expected 推导表×1。

## 门禁（无管道直录 rc，`data/`）

| 门 | 结果 | 文件 |
|---|---|---|
| unit | **9063 passed / 13 skipped，rc=0**（注：收集发生在第 24 格〔e2e 接线格〕加入前；该文件单跑终版 24P 见 shadow-final） | `r102-gate-unit.txt` |
| console | **1577 passed / 14 skipped，rc=0**（与 r101 终树同数，守恒） | `r102-gate-console.txt` |
| integration | 213 passed / 1 skipped，rc=0 | `r102-gate-integration.txt` |
| e2e | 74 passed / 3 skipped，rc=0 | `r102-gate-e2e.txt` |
| ruff | CI 硬门 `ruff check plaita tests --select E722` rc=0；改动三文件默认规则 rc=0；BLE001（CI `--exit-zero` 咨询）零报 | `r102-gate-ruff.txt` |
| mkdocs | `--clean --strict` rc=0（既有 INFO 锚点提示为基线噪音） | `r102-gate-mkdocs.txt` |

## 边界与后续（写给阶段 2/3）

- 本轮纯 shadow：无任何 enqueue/状态行写/Redis 键写；「补的动作可在 dispatch ledger 回放」「20 次 kill -9 0 失联」两条 #321 验收属阶段 2 接管面，本轮不宣称。
- 幂等键三选一（A1 §四）未在本轮锁死：shadow 无入队面，不产生第二套去重真相；阶段 2 开工前须在 (a) start-dedup 族 / (b) 对账守卫键（须入只读豁免清单）/ (c) #322 连接器幂等键 中拍板。
- 节奏：shadow 缺省 10s（#321 正文），快照陈旧度门限缺省 120s=2 审计周期（A1 §三建议口径；env `PLAITA_RECONCILER_SHADOW_SNAPSHOT_MAX_AGE_SECONDS` 旋钮）。阶段 2 若要「≤60s 被补」原验收，须改独立扫描并承担双读成本。
- #316 gauge 语义升级建议（page 人 → 量 reconciler，violation 率应被接管压到 ≈0）留阶段 2 验收条款，本轮未动 #316 告警面。
