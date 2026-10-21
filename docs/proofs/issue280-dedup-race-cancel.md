# #280 证明：dedup 预认领/占位行竞窗 + CANCEL 三处原子性

- 单号：plaita#280（P1；顺带 A1 r101 a/P2×3，同一 seam）
- 基线：`origin/main` = `4379a76`（含 #273/#274/r66）
- 边界：只动 console POST 预认领/queued 占位行、queued CANCEL 与 worker
  step-persist 取消复核；不改 worker 7d dedup TTL/同键同 id 契约。

## 根因

### ① #280 并发同 key 双 POST

A `SET NX` 映射成功但尚未写 queued 占位行；B 命中映射后读行不存在，把 A
的在途认领当孤儿删除并用 B 的 id 重认领。A/B 各自落行、入队：同一 dedup_key
返回两个 id、两条 start 载体；落败 id 永久 queued（无载体/无终态 TTL）且
占一个配额。

### ② CANCEL 的 CAS 与配额释放非原子

`_CANCEL_QUEUED_ROW_LUA` 置 cancelled 后，Python 再 `release_tenant_start`。
CAS 后进程崩溃 → cancelled 行 + 配额永久占用；再次 CANCEL 命中终态短路，
不补释放。

### ③ CANCEL 落在取消检查之后、running 落盘之前

worker 步界顶部检查取消标志通过后执行节点；若 CANCEL 在这之后到达，step
persist 仍写 `status=running`，覆盖 console 已 CAS 的 cancelled（配额已释放）。

### ④ 占位行已落、入队前崩溃

POST 写了 queued 占位行但尚未入队时崩溃；同 key 重试命中映射+行（queued），
直接返回 mapped，永不补载体 → 永久 queued。

## 修法

1. **原子认领 + owner 标记**（#280）：`_CLAIM_START_DEDUP_LUA` 在 `SET NX`
   同一脚本里写 `owner=claiming:<id>`（60s）；B 命中 live claiming owner 时
   绝不删除/重认领，短暂轮询占位行（10×50ms）后仍返回同一 mapped id。
   force DELETE 的 execution tombstone 存在时仍按孤儿回收（保持「7 天内同 key
   换新 id」）；owner 过期（崩溃）才允许孤儿释放/重认领。
2. **queued 行无 owner 恢复**：A 在落行→入队之间崩溃时 owner 过期，后续同 key
   POST 见到 queued 行+无 owner，用**同一 id** 补一条 start 载体（worker 自命中/
   去重吸收重复），不再永久 queued；A 成功入队后 owner 置 `enqueued:<id>`（7d）。
3. **认领失败路径清理**：配额 429 / 占位行写失败 / 入队失败均释放映射+owner
   （与 r66 入队失败清行对齐），不让重试撞到无行的在途映射。
4. **CANCEL 原子释放**：`_CANCEL_QUEUED_ROW_LUA` 增加 KEYS[2]=配额 ZSET，
   置 cancelled 同脚本 `ZREM`；再 CANCEL 命中 cancelled 终态短路时补一次幂等
   `release_tenant_start`（覆盖旧版遗留 leak）。
5. **worker 落盘前复核取消**：step-persist 写 running 之前再次
   `_cancel_requested`；命中即写 cancelled 并 break，绝不让 running 覆盖。

## 测试

- `plaita-console/backend/tests/console/test_issue280_dedup_race_cancel.py`
  **5 passed**：并发同 key（屏障卡 A 的占位行写入）→ 两响应同 id、恰一条
  start、配额 1；owner 过期 → 孤儿换新 id；queued 行无 owner → 同 id 补入队；
  queued CANCEL → 配额同脚本释放；re-CANCEL cancelled → 补释放旧 leak。
- `tests/unit/test_issue280_cancel_step_persist.py` **1 passed**：run_distributed
  内设置取消标志，断言 persist 序列不含 `step_persist`、含
  `cancelled_before_step_persist`，最终 cancelled。

## 负控（先红后绿）

- NC A（去掉 owner 标记 + Lua ZREM + post-CAS release）：并发 race **两个不同
  id**（`548c…` / `5baf…`）；CANCEL 后配额 **1**；re-CANCEL 后配额 **1** →
  **3F/2P**。
- NC B（queued 行不补入队）：崩溃窗口重试后 start 载体 `[]`（永久 queued）→
  **1F**。
- NC C（删除 step-persist 取消复核）：persist 序列
  `['step_persist', 'cancelled']` → **1F**。

还原修复后 console **5 passed**、unit **1 passed**。完整日志
`docs/proofs/data/issue280-negative-control.txt`。

## 验收

- 全量 unit `-m "not slow"` **8745P/12S/14 deselected/3498 subtests**；
  e2e+integration **287P/4S**；console **1073P/13S**；plaita-ai
  **241P/14S/12 subtests**；layering/wheel/size 全绿；
- `landing_gate.py ci` **rc=0**（main `4379a76` CI success + Console E2E 绿）
  后单 commit 推送（closes #280）。
