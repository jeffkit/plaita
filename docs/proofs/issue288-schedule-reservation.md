# #288 证明：schedule 触发接入 reservation 状态机（+ A106-02 409 窗口）

- 单号：plaita#288（P1；A1 r106 对 r59 不变量的对抗发现）
- 基线：`origin/main` = `41085cc`（r59 token 状态机）
- 边界：schedule 触发的 reservation/清理/孤儿重试；前端 409 重试窗口与
  superseded 集合。不改 worker 消费侧、CANCEL、DLQ replay、console POST 语义。

## 根因

r59 不变量 6 是「XADD 前必有 reservation」，console 做了，但 `fire_schedule`
是 claim → 配额 → queued 占位行 → **直接 `enqueue_task`** → mark。且
`_MARK_ENQUEUED_LUA` 允许 `claiming` 直接进 `enqueued`，所以事后补 reservation
也不会被状态机挡住。

claim token/owner 在慢占位行窗口过期后：

- XADD 仍执行，mark 失败，只 XDEL carrier；
- queued 占位行 + map 残留（7 天 TTL）；
- 同 due 第二次触发走孤儿分支只打日志返回 None，不 adopt 不 release，
  该 due 永久无 carrier。

A106-02：前端 409 重试只有 3 次（≈3s），小于 claiming TTL 60s；superseded
不在重试集合；超窗后 UI 抛错，用户再提交会生成新 dedup_key → 第二次执行。

## 修法

- `fire_schedule` 写占位行后、XADD 前调用 `_dedup.reserve_enqueue(token)`；
  若失败（慢写跨 TTL）先 `guard` 用同一 token 续期再 reserve；仍失败则放弃
  XADD 并逐级回滚。
- `_MARK_ENQUEUED_LUA` 收紧为只允许 `enqueueing → enqueued`：claiming 直入
  被拒，强制所有 XADD 先 reservation（不变量 6 由状态机保证，不再靠调用方）。
- 失败路径（reservation/入队/mark）统一：quota release + `_discard_queued_placeholder`
  （删除行并写墓碑）+ `_dedup.release(no token)`（墓碑分支清 map/owner/token）
  + mark 失败 XDEL carrier；fire_record/enqueue=failed，advance 正常。
- 同 due 孤儿：若 map 指向的行还在，discard（墓碑）+ 条件 release，映射清掉后
  用新 id **同调用重试**本 due；清理失败则具名 WARNING 返回 None，不留 7 天
  静默孤儿。live owner 分支保持 r71「同 due 收敛既有 id、enqueue=dedup」。
- 前端 A106-02：新增 `isExecutionStartRetryable`（in_progress + superseded）、
  `START_RETRY_WINDOW_MS=65_000`（≥ claiming TTL 60s）、
  `startRetryWindowError`（超窗具名引导「刷新执行列表，不要换 key 重复提交」）；
  `startExecution` 按 deadline 重试同一 body，不再 `attempt >= 3`。

## 测试

- 新增 `tests/unit/test_issue288_schedule_reservation.py` **5P**：
  ① mark 必须 enqueueing（不变量 6）；② reservation 失败不 XADD 且回滚干净；
  ③ A1 r106 慢占位行+TTL 过期：guard 续期→reserve→入队成功、单行/单配额；
  ④ 同 due 孤儿清理后重试新 id；⑤ mark 失败 XDEL + discard 行/墓碑/映射。
- 既有 schedule 用例（#198/#23/#28/schedule_trigger_visibility）+ r59 状态机
  **69P**；console dedup 三文件 **15P**。
- 前端 `node scripts/check-api-transport.mjs` **30/30**、`tsc --noEmit` 干净。

## 负控（先红后绿）

隔离副本 `/tmp/r60-nc`：

| NC | 变异 | 结果 |
|---|---|---|
| A | schedule 去 reserve + mark 失败不清理 | 4F（orphan row/map、msg None） |
| B | mark 重新允许 claiming→enqueued | 1F（不变量 6） |
| C | 孤儿分支直接 return None | 1F（同 due 不重试） |
| D | 前端退回 3 次/superseded 不入集合 | node 1/30 断言失败 |
| E | mark 失败不 discard 行/墓碑/映射 | 1F（queued 行残留） |

A1 r106 最小复现探针：pre `first fire None`、queue 空、map+queued 行残留、
同 due 再触发仍 None；post first fire 成功、单 carrier/单行/配额 1、第二次
dedup 一致。完整输出：
`docs/proofs/data/r60-schedule-reservation-negative-control.txt`。

## 验收

- 全量 unit（`-m "not slow"`）：**8774P/12S/14 deselected/3501 subtests**（rebase 到 `4985022` 后复跑，rc=0；较上轮 8769P 增本单 5 条）
- e2e + integration：**287P/4S**（rc=0；layering/wheel/size 定向 46P 绿）
- console：**1115P/13S**（rebase 后复跑，rc=0）
- landing gate：**rc=0 / 允许落地**（main 最近完成 run `4985022` CI success；Console E2E 绿）
