# 第 47 轮：存储面两格（A2R13 fence 世代 ABA 第三格 + A51-03 删除后计数键残留）

- 台账：`duo-b3-ledger.md` 第四十七轮（`duo-a2-ledger` A2R13 / `duo-a-ledger` A51-03）
- 分支：`wt-b3`（基于 `origin/main` `5ccaf6a`）
- 生产文件：`plaita/server/execution_lease.py`、`plaita/storage/redis.py`、
  `plaita/storage/fenced.py`、`plaita-console/backend/api/executions.py`
- 回归：`tests/unit/test_r47_fence_generation_and_counter_residue.py`、
  `tests/unit/test_console_mechanism_keys.py`

## A2R13：`delete_execution_state` 后 fence 世代 ABA

### 根因

`try_acquire_fenced` 原先直接对 **fence 键**做 `INCR`：

```
acquire: SET NX lease; INCR fence; SET lease={holder}:{gen}
delete : DEL state/index/fence/lease
```

fence 键被删除后，同一 `execution_id` 重新 acquire 时 `INCR` 从 0 回到 1。
首次执行的旧 worker 世代也是 1，于是：

1. 旧 worker 的迟到 `save_execution_state` 命中 fence CAS，把已删除的执行
   状态与索引成员写回（或覆写新执行状态）；
2. 旧 worker 的 `delete_execution_state` 同样命中 CAS，可直接删掉新执行。

### 修法选择：世代计数器与 fence 键分离（INCR 永续）

新增 `{ns}:execution:fencegen:{id}`（`execution_fence_generation_key`）：

- `try_acquire_fenced` 的 Lua 改为对**计数器** `INCR`，再把当前世代写入
  fence 键（fence 键只承载 CAS 比较值）。
- `delete_execution_state`（控制面 pipeline 与持世代的 Lua 两条路径）只删
  fence 键，**刻意保留** `fencegen`。
- 升级兼容：存量 fence 键有值而 `fencegen` 缺失时，Lua 以 fence 值作为
  floor 把计数器抬到其后（fence=7 → 新世代 8），世代绝不回退。

**TTL 语义**：

| 键 | TTL | 删除行为 | 作用 |
|---|---|---|---|
| `execution:fence:{id}` | 7 天（acquire 时 SET EX） | 删除 | 当前世代 CAS 比较值；删除即吊销 |
| `execution:fencegen:{id}` | **无 TTL（永久）** | **不删除** | 单调世代锚；INCR 永续 |
| `execution:lease:{id}` | 租约 TTL（默认 120s，renew 续期） | 删除 | 互斥租约 |

选择「永续计数器」而非「删除时写 floor 墓碑」：墓碑 TTL 到期后 ABA 会
重新出现，且控制面删除还要原子读-写 floor；计数器每执行 id 只多一个裸
整数键，删除后残留无害，严格保证世代全局单调。

### 负控（production stash，保留新测试）

`docs/proofs/data/47-nc-a2r13-a51-03.txt`：

- `test_force_delete_does_not_reset_generation`：`assert 1 > 1`（新 worker
  世代回到 1）；
- `test_stale_worker_save_refused_after_reacquire`：
  `Failed: DID NOT RAISE ExecutionLeaseError`（旧 gen save 命中 CAS）；
- `test_stale_worker_delete_refused_after_reacquire`：
  `Failed: DID NOT RAISE ExecutionLeaseError`（旧 gen delete 删除新执行）；
- `test_legacy_fence_value_seeds_generation_counter`：`fencegen` 键缺失。

修复后四条全绿，且断言 `fencegen` TTL = -1（永久）。

## A51-03：删除后计数键残留

### 根因

以下计数键按 `execution_id` 存 7 天：

- `{ns}:execution:nofail:{id}`（确定性失败，上限 12）
- `{ns}:execution:g1wakeups:{id}`（G1 唤醒，上限 2）
- `{ns}:execution:noderefetch:{id}`（兜底总重试，上限 100）
- `{ns}:execution:noderetry:{id}` 与 `{ns}:execution:noderetry:{id}:{node}`
  （节点重试预算）
- `{ns}:execution:affinityhandover:{id}`（不亲和交接，上限 3）

删除执行原先只清 state/index/fence/lease。同一 id 新建执行会继承旧预算：
例如 `nofail=12` 时新执行第一次确定性失败就被判「不可救」；G1 唤醒
`=2` 后拒绝人工唤醒；`noderetry=5` 时 cap=1 的新预算全拒。

### 修法

- `RedisExecutionStorage.delete_execution_state`：`counter_keys_for_execution`
  汇总五类固定键，并用 `SCAN MATCH {noderetry base}:*` 收集节点维度子键
  （不用阻塞式 `KEYS`；execution_id 按字面量转义）；所有键与控制面删除
  同 pipeline 提交。
- `FencedExecutionStorage._FENCED_DELETE_LUA`：新增 `KEYS[5..]`，CAS 匹配后
  逐个 `DEL`，覆盖持世代的删除路径。
- 扫描失败只记 WARNING 并退回固定键（主删除语义不受影响，残留由 7 天
  TTL 兜底）。
- `fencegen` 不在计数清理范围（A2R13 的单调锚）。
- console `_is_mechanism_key` 登记 `:execution:fencegen:`（裸整数，若不排除
  会让 `GET /api/executions` 500）。

### 负控

- `test_delete_purges_all_counter_families`：删除后
  `plaita:execution:nofail:exec-r47` 仍存在（`1 == 0`）；
- `test_new_execution_budget_starts_at_zero`：删除后
  `_read_deterministic_failure_count == 12`（预期 0）；
- `test_fenced_delete_with_generation_purges_counters`：持世代删除路径同样
  残留 `nofail`；
- `test_mechanism_keys_are_excluded`：`fencegen` 未被排除。

修复后全部转绿，并验证他执行（含节点子键）预算不被误删。

## 回归结果

- 定向（第 47 轮新测试 + fencing/lease/storage/机制键）：
  **133 passed**（新增 7 条）。
- Redis 存储集成回归：`tests/integration/test_storage_redis.py` +
  `tests/integration/test_storage_commons.py` = **11 passed**。
- console 删除持久性 + A50 串行化：
  `test_execution_delete_persistence.py` +
  `test_issue_a50_tenant_delete_races.py` = **16 passed**（零变化）。
- console 执行列表投影（含 `fencegen` 裸整数键）：
  `test_clean_c4_executions_api.py` = **23 passed**。
- `tests/unit` 全量：**8262 passed / 15 skipped / 3498 subtests passed**
  （729.89s，rc=0）。
- `scripts/landing_gate.py ci`：**rc=1**，main 最近完成 run
  `a2fbb19`（r49/r50）conclusion=`cancelled`。按任务约定「红则压分支」，
  本单只推 `wt-b3`，不合并 main。

## 负控命令

```bash
git stash push -m r47-nc-revert-production -- \
  plaita/storage/redis.py plaita/server/execution_lease.py \
  plaita/storage/fenced.py plaita-console/backend/api/executions.py
.venv/bin/python -m pytest \
  tests/unit/test_r47_fence_generation_and_counter_residue.py \
  tests/unit/test_console_mechanism_keys.py -q
# 8 failed, 2 passed  → docs/proofs/data/47-nc-a2r13-a51-03.txt
git stash pop
```
