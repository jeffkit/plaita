# B108-01/02 证明：park 周期清扫 + dedup discarded 指标/审计

- 单号：duo-a 第一百零八轮 B108-01 / B108-02（均 b/P2；#289 park 的收尾项）
- 基线：`origin/main` = `c24ce3e`
- 边界：只补 park 的周期清收与 discarded 观测面；不改 #289 park/恢复语义、
  #286/#287 迁移、#288 reservation。

## B108-01：park 无独立 sweeper

park 保留原 id 依赖同 key/同 due 重试或 worker 终态自然收敛；无人重试时
queued 行/map/token/配额没有最终清收。

修法：新增 `plaita/server/start_dedup_sweeper.py`：

- park 时写 `{ns}:start-dedup-park:{key}`（`{id, parked_at}`，TTL 1h）；
  `mark_enqueued` 成功/恢复时清除标记。
- `sweep_parks` 只扫该标记，超时默认 60s（与 claiming TTL 同源）；预算
  100 标记/tick；单条失败 WARNING+计数不中断。
- 超时后：
  - 行 status running/终态（或映射已换主）⇒ 执行已接管，只删标记；
  - 行 queued/缺失 ⇒ `RedisExecutionStorage.delete_execution_state`（写墓碑+
    删行）→ `start_dedup.release`（墓碑分支清 map/owner/token）→
    `release_tenant_start` 回配额 → 删标记 → WARNING 审计 + 计数器。
- `ParkSweeper` 线程在 console cluster lifespan 启动/停止；env
  `PLAITA_START_DEDUP_PARK_SWEEP_INTERVAL` / `..._BUDGET`；
  `PLAITA_DISABLE_START_DEDUP_PARK_SWEEP=1` 启动 WARNING。

## B108-02：discarded 无独立审计

worker 终态命中放弃载体只有 WARNING，指标面与普通成功不可区分。

修法：

- `RedisFlowWorker._dedup_discarded_total` 计数器；`collect_worker_metrics`
  新增 `worker_dedup_discarded_total` counter（`/metrics`）。
- 放弃时写 `plaita:execution:dedup-discard:{execution_id}`（7d：
  execution/carrier/dedup_key/status/reason/discarded_at）。
- console `GET /api/executions/{id}` 读取并返回 `dedup_discarded` 字段；
  ops-runbook/flow-worker 注记。

## 测试

- `tests/unit/test_start_dedup_park_sweeper.py` **4P**：超时 queued 清收+墓碑+
  配额回笼+指标；fresh 不动；running/completed 只删标记；row missing 清认领+
  配额。
- console `test_issue289_mark_race.py` **3P**：原 park/恢复/终态三例；终态例
  追加 `_dedup_discarded_total==1`、metrics 文本、audit key、详情
  `dedup_discarded` 断言。
- 既有 #288/#289/r59/schedule/console dedup 全绿。

## 负控（先红后绿）

隔离副本 `/tmp/r62-nc`：

| NC | 变异 | 结果 |
|---|---|---|
| A | sweeper 永不清理 | 2F（expired queued/row missing 未清） |
| B | worker 放弃不递增 counter | 1F（=0） |
| C | worker 放弃不写审计键 | 1F（详情无 dedup_discarded） |
| D | sweeper 忽略超时 | 1F（fresh park 被清） |

完整输出：`docs/proofs/data/r62-park-sweeper-discard-audit-negative-control.txt`。

## 验收

- 全量 unit（`-m "not slow"`）：**8795P/12S/14 deselected/3501 subtests**（rebase 到 `eee7b37` 后复跑，rc=0；较上轮 8776P 增本单 4 条 sweeper + 他席 15 条）
- e2e + integration：**287P/4S**（rc=0；layering/wheel/size 定向 46P 绿）
- console：**1121P/13S**（rebase 后复跑，rc=0；新增断言并入既有 3P）
- landing gate：**rc=0 / 允许落地**（main 最近完成 run `eee7b37` CI success；Console E2E 绿）
