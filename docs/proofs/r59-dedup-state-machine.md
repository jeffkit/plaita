# r59 证明：start dedup 受理 seam 设计级收口（#286/#287）

- 单号：plaita#286、#287（均 P1；#280/#285 的第三轮修后对抗打穿）
- 基线：`origin/main` = `ca39bc2`
- 边界：start dedup 受理 seam（claim/占位行/入队/owner/release、schedule 预
  认领、409 客户端语义、force DELETE 迟到载体）；不改 worker 消费侧 start
  去重、CANCEL、DLQ replay。

## 根因

1. **#286 stale release TOCTOU**：`_release_start_dedup_if_owner` 先 GET
   map==id，再 DEL。B 在「决定释放」与 DEL 之间可停顿；A 期间续期 owner、
   写行、入队、CAS 成功并 200。B 按旧决定删除 A 的 map/owner，认领新 id，
   同 dedup_key 双 200/双载体/双配额。
2. **#287 owner 键迁移缺失**：新代码只读新前缀，滚动升级时旧实例的
   `<map>:owner=claiming:A` 被当无 owner，B 换新 id；旧 `enqueued:A` 被当
   崩溃窗口重复补 carrier；`foo:owner` 旧 owner 键与新 map 键共键。
3. schedule 只把 dedup_key 交给 worker，入口不预认领：同 due 重触发各铸新
   id、各占配额，cron 重试可耗尽配额。
4. 409 in-progress 无 Retry-After，前端不重试；后端 handler 用
   `time.sleep` 阻塞事件循环。force DELETE 落在 guard 后、XADD 前时，A 的
   迟到载体留在 Stream。

## 修法（状态机表见 flow-worker.md「受理状态机」）

- 新增 `plaita/server/start_dedup.py`：console 与 schedule 共用的 token CAS
  原语。owner 值仍 `state:id`（迁移可读），token 存
  `{ns}:start-dedup-token:{key}`。每次转换 Lua 校验 map==id、owner 状态、
  token、墓碑；`release` 在同一 Lua 内复核 row，不再 GET→DEL。
- 转换：claim → guard（慢写跨 TTL 且 owner/token 缺失可同 token 续期）→
  写占位行 → reserve_enqueue（`enqueueing`）→ XADD → mark_enqueued
  （`enqueued`）。失败清理用 token release；孤儿 release 无 token，要求
  row 缺失/无 token/owner 缺失或 claiming（墓碑例外）。
- 旧格式：`read_owner` 双读新键/旧 `<map>:owner`；旧 claiming 视为 live 并
  `adopt_legacy` 迁到新键+token，旧 enqueued 视为已入队。`foo:owner` 反向
  碰撞先用 Lua 把 foo 的 owner 迁到新键、释放共键，再认领；无法安全迁移
  ⇒ 409 `execution_start_legacy_collision`。写路径只写新键，runbook 要求
  console **先排空旧实例再升级**。
- schedule：`fire_schedule` 在配额 admission 前 claim 同一 dedup_key；同
  due 命中映射直接收敛既有 execution_id（不入队/不占配额）；配额/入队/
  mark 失败均 release claim + 回滚配额（+ XDEL 载体）。
- ④：409 in-progress/superseded 带 `Retry-After: 1`；handler 等待改
  `asyncio.sleep`；前端 `startExecution` 生成/复用 `dedup_key`，对 in-progress
  按 Retry-After/指数退避重试同一 body（纯函数有 node 断言）。
- ⑤：reservation 后 force DELETE ⇒ mark_enqueued 失败路径
  `_safe_xdel_carrier` 清收刚写入的 target message；tombstone 仍兜底。

## 测试

- 新增 `tests/unit/test_r59_start_dedup_state_machine.py` **10P**：token
  转换/guard 错误 token 拒绝、stale release、旧 claiming/enqueued 迁移、
  反向碰撞、owner 不被旧 id 覆盖、schedule 同 due 单 id/单配额、配额满与
  入队失败回滚。
- 新增 console `test_issue286_287_owner_state_machine.py` **5P**：stale
  release 收敛 A、旧 claiming 409 + Retry-After、旧 enqueued 不重复 carrier、
  `foo:owner` 反向碰撞、force DELETE 后 XDEL 迟到载体。
- 既有 #280/#285 console 用例保持 **10P**；#198/#23/#28 schedule 用例 **52P**；
  前端 `node scripts/check-api-transport.mjs` **29/29**。

## 负控（先红后绿）

隔离副本 `/tmp/r59-nc` 逐项变异（断言未放宽）：

| NC | 变异 | 结果 |
|---|---|---|
| A | release 回退 GET→DEL | stale release 2F（#286） |
| B | 去掉旧 owner 双读 | 迁移 4F（#287） |
| C | schedule 不预认领 | 同 due 2 条消息 1F |
| D | 去掉 Retry-After | header 断言 1F |
| E | 去掉 XDEL | A 迟到载体残留 1F |
| F | guard 不校验 token | 错误 token 通过 1F |
| G | mark_enqueued 不校验 token | 错误 token 通过 1F |

A1 r105 探针：stale release pre 双 id/双载体/quota 2；owner migration pre
双 id/双载体、post B 409 且不换 id（旧 A 晚期写仍可能入队 → drain 硬约束）；
force-after-guard pre 队列残留 A 载体、post 单载体。完整输出：
`docs/proofs/data/r59-dedup-state-machine-negative-control.txt`。

## 不变量（r106 对抗基线）

1. 同一 dedup_key 任意交错最多一个 execution_id/一条可执行 carrier/一个配额。
2. 任何写转换要求 map==id、owner 状态、token 匹配、无墓碑。
3. release 原子复核 token/owner/row/墓碑；无 token 只在 row 缺失且无
   token/owner 缺失或 claiming 时生效（墓碑例外）。
4. owner 不能被另一个 id 覆盖。
5. 旧格式只读迁移；新写只写新键；console 滚动升级必须先排空旧实例。
6. schedule/console 共用同一 claim 原语；enqueue 前必有 reservation。
7. 失去认领的载体必须 XDEL；tombstone 是 worker 侧最终防线。

## 验收

- 全量 unit（`-m "not slow"`）：**8769P/12S/14 deselected/3493 subtests**
  （rebase 到 `1f01658` 后复跑，rc=0）
- e2e + integration：**287P/4S**（rc=0；layering/wheel/size 定向 46P 绿）
- console：**1115P/13S**（rebase 后复跑，rc=0）
- 前端：`node scripts/check-api-transport.mjs` **29/29**、`tsc --noEmit` 干净
- landing gate：**rc=0 / 允许落地**（main 最近完成 run `1f01658` CI
  success；Console E2E 绿）
