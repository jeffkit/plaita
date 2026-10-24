# r97 · plaita#324 — C2 flow publish outbox 按行 lease（解除单副本约束）

- 席位/轮次：B2 修复席 第九十七轮（2026-10-24）
- 分支：`fix/c2-outbox-row-lease`（基线 fe64f773；门禁期间 origin/main 前移 2 提交 a502face/c707f747，`git diff --name-only` 核实为 ADR 治理文档，与本单改动面零交集）
- 规格：jeffkit/plaita#324（正文即规格）：按行 lease `{ns}:outbox:lease:{id}`、谁抢到谁投、TTL 短于重试间隔；读回校验保留、重复写天然幂等；验收 2 副本无重复交付、失败行按退避重试；**边界：不做全局 leader、不改 outbox 表结构**
- 改动面：`plaita-console/backend/services/engine_outbox.py`（租约面 + 投递/对账接线 + 文档串）、`tests/console/test_issue324_outbox_row_lease.py`（新，21 格）、`tests/console/test_engine_outbox.py`（1 处 stats 精确断言扩形，#303 同款先例）、`docs-site/docs/distributed/ops-runbook.md`（env 登记 + 投递链节）

## 缺陷（b117-03 单副本约定）

`engine_outbox.py` 投递/对账线程无 claim/leader 机制：多副本并发会重复投递（同内容覆盖写、幂等无害）但 `attempts` 计数互相覆盖（退避/DLQ 记账失真），部署上禁止多副本。

## 修法

1. **租约原语**：`acquire_row_lease(target, tenant, row_id)` 对目标 Redis `SET {ns}:outbox:lease:{行id} <token> NX PX <ttl>`，三态返回——
   - `_RowLease`（抢到）：谁抢到谁投；`release()` 走 Lua 比对 token 才删（TTL 过期后迟到释放不误删他副本租约）；
   - `None`（他副本持有中）：调用方本轮跳过该行、**不动行态**——attempts/退避/DLQ 记账只由持有者写（计数互覆盖就此失效）；
   - `LEASE_UNAVAILABLE` 哨兵（租约面不可达，fail-open）：照常行动（投递自身对死目标具名失败），**#299「不可达 → attempts 记账 → 退避 → DLQ」语义逐格守恒**——首版 fail-closed（跳过不记账）被既有守恒格当场拦下（见「先红后绿②」），fail-open 下最坏情形 = 两副本并发同内容覆盖写，规格明言幂等兜底。
2. **TTL 域**：`PLAITA_CONSOLE_ENGINE_OUTBOX_LEASE_TTL_SECONDS`（默认 1.5s），合法域 `0 < ttl < 2s`（**短于最小重试退避** = 规格「TTL 短于重试间隔」：持有者崩溃后租约至多滞留一个 TTL，绝不阻塞到期重试）；域外/非法/inf/nan 一律 WARNING + 回退默认（b117-04 同款卫生）。
3. **接线**：`run_delivery_cycle` / `run_reconcile_cycle` 逐行动作前抢租约（**在进程内 `OUTBOX_DELETE_LOCK` 之外**——跨副本互斥是 Redis 往返，不持进程锁做网络 I/O），`finally` 释放；stats 增 `lease_skipped`/`lease_open`。`_iter_delivered_ids` 改带 `(id, tenant_id)`（租约键需在重读前知道租户）。
4. **边界守恒**：#303 删除协议（`OUTBOX_DELETE_LOCK`/deleting tombstone）零改动——仍是进程内互斥；多副本「同时删除」与他副本投递间的残留小窗在模块文档串「多副本约定」如实注记（纳入跨副本互斥超出本单边界）。

## 先红后绿

**① 格级（`tests/console/test_issue324_outbox_row_lease.py`，21 格）**

| 树 | 结果 | 证据 |
|---|---|---|
| 基线（stash 服务文件） | **21 failed**（租约面新符号/新统计键缺失红） | `data/r97-cells-pre.txt` |
| 修复树（终码） | **21 passed, rc=0** | `data/r97-cells-post.txt` |

格覆盖：TTL 域（7 参非法值回退+告警）/键形租户命名空间/互斥（同行斥、异行容）/PX 落盘/过期后迟到释放不误删/租约面不可达哨兵+no-op 释放/持有行跳过零接触（无 key、attempts=0）/fail-open 重试记账守恒/崩溃租约 TTL 过期不阻塞重试/失败行租约即放+退避表与单副本一致（attempts=1、next≈+2s、到期重投读回校验 delivered）/对账同租约（持有跳过、重投后即放）。

**② 行为级（跨进程 2 副本，`data/r97-lease-replica-driver.py`）**——多副本重复投递在进程内**不可复现**（两线程同被 `OUTBOX_DELETE_LOCK` 串行化），必须两真进程；fakeredis `TcpFakeServer` 提供真 TCP 跨进程共享键空间 + 共享 SQLite 文件，2 子进程 × 6 行（含 `flaky` 混沌行：前 0.8s 目标侧拒绝）× ~104 轮/副本：

| 树 | 结果 | 证据 |
|---|---|---|
| 基线（origin/main archive 干净树 /tmp/r97-base） | **RED：f1–f5 每行被两副本各投一次（totals 全 2）**；且两子进程各自记 `flaky attempts=1`（计数互覆盖同框复现） | `data/r97-lease-pre.txt/.json` |
| 修复树（终码） | **GREEN ×3：每行合计恰 1 次投递，duplicates={}**；`lease_skipped=4/3` 实时分流可见；flaky 行失败→退避→由另一副本补投恰一次 | `data/r97-lease-post.txt/.json`（终码 fail-open 形；另两次绿相为 fail-closed 中间形与终码复跑） |

**1 小时墙钟压缩口径（任务书裁量，如实声明）**：生产 2 副本 × 1h × 2s poll = 1800 轮/副本；驱动为 2 副本 × ~104 轮 × poll 0.05s（约 6s 墙钟），覆盖 抢租约/释放/持有跳过/退避重试/恢复补投 全路径组合；重复投递的判据（同轮扫描重叠窗）在首轮 10ms 内即达稳态，轮数等价性成立而非时长等价。验收条款「失败行按退避重试」由混沌行 + 格级退避表断言共同承载。

## 守恒（#301/#303/#314 面零回归）

| 面 | 证据 |
|---|---|
| #299 投递/退避/DLQ 语义 | console 全量 `test_engine_outbox.py` 全绿（含 `test_delivery_failure_retries_then_dlq` 不可达→attempts 记账→DLQ→恢复补投全链） |
| #301/#302 删除投递/对账 | `test_engine_outbox.py` 删除/漂移/source-missing 族全绿 |
| #303 删除协议串行化 | `test_issue303_delete_vs_outbox_race.py` 全绿；`OUTBOX_DELETE_LOCK`/tombstone 代码零改动 |
| A2R50-02 人工出口 | `test_a2r50_outbox_manual_exit_lock.py` 全绿 |
| #314 保存侧锚点写 | 本单零触 flows 写面；console 全量门覆盖其套件 |
| 唯一既有测试改动 | `test_reconcile_healthy_rows_untouched` 的 stats **精确等值断言**扩形 +`lease_skipped`/`lease_open`（#303 加 `skipped` 时同款先例，注释已注记） |

## 门禁（无管道直录 rc）

| 门 | 结果 | 证据 |
|---|---|---|
| console 全量（backend `pytest tests/console -q`） | **1613 passed / 13 skipped, rc=0**（=开工基线 1592 + 新 21 格精确对账） | `data/r97-gate-console.txt` |
| ruff（4 个改动 py） | All checks passed, rc=0 | `data/r97-ruff.txt` |
| mkdocs `--strict` | rc=0 | `data/r97-mkdocs.txt` |
| 根仓 unit/integ/e2e | 未跑——本单零触 `plaita/` 引擎面（B2 席口径：unit 全量仅触引擎面时跑） | — |

## 残面与取舍注记

- **删除协议仍进程内**：多副本同时删除同一 flow/version 与他副本投递间存在小窗（投递锁内重读早于删除标记提交的陈旧读）——模块文档串注记，部署面提示「避免多副本并发删除同一流程/版本」；纳入跨副本互斥超边界（不做全局 leader）。
- **fail-open 瞬断窗**：租约面瞬断 + 目标存活时，两副本可并发同内容覆盖写（幂等兜底）；`lease_open` 计数与 WARNING 保证降级可观测（红线）。
- **持有者 TTL 内崩溃 + 目标写入已生效**：至少一次语义下的既定残余（读回校验 + 覆盖写兜底），模块文档串注记。
- 租约键随行生命周期：行删后孤儿键至多滞留一个 TTL 自然过期，无清理任务、无表结构改动。

## 交付形态

- 提交 1：`fix(console): #324 outbox 按行 lease`（服务 + 21 格 + 既有断言扩形 + runbook）
- 提交 2：`docs(proofs): r97 证据入库 + 台账第九十七轮`
- 回评：#324 一行（纯技术）
