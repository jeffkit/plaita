# #289 证明：mark 失败不再 discard——park-retry + worker 终态防线

- 单号：plaita#289（P1；A1 r107 对 r60 的对抗：carrier 已被消费后 mark 失败）
- 基线：`origin/main` = `39c875a`
- 边界：console/schedule 的 mark 失败处置、孤儿恢复、worker start 终态命中
  防线；不改 #286/#287 迁移、#288 reservation、CANCEL/DLQ replay。

## 根因

r59/r60 把 `mark_enqueued` 失败当作「没有入队」：discard 占位行 + 写墓碑 +
XDEL carrier + 释放 map + 回滚配额，返回 409。但 XADD 已发生，carrier 可能
已被 worker 消费并执行：

- XDEL 不能撤回已投递/已消费的消息；
- 墓碑写在执行之后，不取消运行中的 A，也不阻止重试；
- map 被释放后同 dedup_key/同 due 重试铸 B → A（已完成）+ B 双执行。

schedule 孤儿分支也只清理/返回 None，无法自愈。

## 修法

1. **mark 失败原地恢复**（`start_dedup.recover_after_mark_failure`）：仅在
   map 仍指向本 id 且无墓碑时，guard（owner/token 缺失可同 token 续期）→
   reserve_enqueue → mark_enqueued。成功则保留载体/占位行/映射，按原 id 返回。
2. **park-retry**：恢复失败但 map 仍属本 id、无墓碑 ⇒ **不清收**：保留
   carrier/queued 行/map/owner/token/配额，console 返回 409 in-progress
   （Retry-After），schedule 返回 None；同 key/同 due 重试走 hit/孤儿恢复，
   收敛原 id。只有映射已换主或墓碑（A 载体不会被执行）才 XDEL+discard+
   release+回滚配额。
3. **schedule 孤儿同 id 恢复**：map 指向的 row 存在且无墓碑时
   `adopt_for_recovery` → 补 carrier → mark，返回原 id；row 缺失/墓碑才清理
   映射后用新 id。
4. **worker 消费点第二道防线**：`_handle_start_dedup_hit` 命中已终态
   （completed/error/cancelled）时具名 WARNING + `discarded=True`，ack 不执行；
   即使上游仍产生重复载体，也不会二次跑。
5. 正常路径、#286/#287 stale/migration、#288 reservation 均不回退。

## 测试

- console `test_issue289_mark_race.py` **3P**：
  ① A 被消费完成后 mark 失败：原地恢复，A 返回 200 completed，row 保留、
  无墓碑、同 key 重试仍 A、队列无 B；
  ② 恢复不可用：park 409 in-progress，row/map/配额保留；TTL 过期后重试同 id
  恢复（只补 A 载体）；
  ③ worker 命中已完成 A 的载体 B：`discarded=True` 不执行。
- `tests/unit/test_issue289_schedule_mark_race.py` **2P**：schedule 同场景
  原 id 收敛；park 后同 due 孤儿恢复同 id。
- 既有 #288 schedule **5P**（孤儿/ park 语义已更新到 #289）、r59 状态机 11P、
  #280/#285/#286 15P、schedule 49P 全绿。

## 负控（先红后绿）

隔离副本 `/tmp/r61-nc`：

| NC | 变异 | 结果 |
|---|---|---|
| A | console recover 关闭 + park 分支走旧 discard | mark race 1F（A 已 completed 仍 409→重试 B） |
| B | schedule recover 关闭 + park 分支走旧 discard | schedule mark race 1F（first fire None→同 due B） |
| C | worker 终态返回去 `discarded` | 终态防线 1F |

A1 r107 探针：console pre A 409 + row 删除 + 墓碑 + same-key B；post A 200
completed、row 保留、无墓碑、retry 同 A。schedule pre first None + discard +
同 due B；post first 返回 A msg、map/owner A、同 due None、队列仅 A。完整输出
`docs/proofs/data/r61-mark-park-negative-control.txt`。

## 验收

- 全量 unit（`-m "not slow"`）：**8776P/12S/14 deselected/3501 subtests**（rc=0；较上轮 8774P 增本单 2 条 unit）
- e2e + integration：**287P/4S**（rc=0；layering/wheel/size 定向 46P 绿）
- console：**1118P/13S**（rc=0；较上轮 1115P 增本单 3 条）
- landing gate：**rc=0 / 允许落地**（main 最近完成 run `39c875a` CI success；Console E2E 绿）
