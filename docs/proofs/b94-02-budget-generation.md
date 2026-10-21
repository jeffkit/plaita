# b94-02 证明：budget_generation 哈希对齐实际生效预算

- 来源：`duo-a-ledger.md` 第九十四轮 A1 b94-02（P2，可复现契约偏差）
- 基线：`origin/main` = `1032874`
- 边界：`load_runtime_config` 的预算四键解析、默认 lease TTL 常量、
  `RedisFlowWorker` 的实际 TTL 钳制；不改队列/租约实现、不改代际数据结构。

## 根因

`RedisFlowWorker.__init__` 把构造/CLI 实际生效的预算四键作为 overrides 传入
`load_runtime_config`（注释自称「与队列构造实际采用的钳制值同源，避免代际
哈希与实际预算分叉」），但 `load_runtime_config` 只消费 identity 类 overrides；
`max_deliveries` / `lease_ttl_seconds` / `task_max_age_seconds` / `dlq_max_len`
仍只读 env。默认部署：

- worker 实际租约 = `execution_lease.DEFAULT_LEASE_TTL_SECONDS` = **120**
  （`self.lease_ttl_seconds` 驱动 acquire/renew；ops-runbook 亦载 120）；
- `runtime_config.DEFAULT_LEASE_TTL_SECONDS` = **300**，哈希恒按 300 计算；
- `event=config.effective` 自报 300，与实际 120 相悖。

后果：按文档把 env 对齐到 120（行为零变化）即哈希翻转 →
`config.budget_stale` 假告警；CLI `--max-deliveries/--lease-ttl-seconds`
实际生效但哈希不变/不匹配；console/schedule/event_filter 产消息的哈希
与 worker 实际预算也不一致。

## 修法

1. `runtime_config.DEFAULT_LEASE_TTL_SECONDS` 300 → **120**，与
   `execution_lease.DEFAULT_LEASE_TTL_SECONDS` 及 ops-runbook 单源对齐
   （runtime_config 不得 import server，故以常量+注释约束同步）。
2. 新增 `_budget_int(overrides, key, env, env_name, default, ...)`：
   显式 override 优先（`None` = 未提供，回退 env），与 identity 键同法；
   非法值按原 semantic class 收集 error/WARNING。四个预算键全部改用它。
3. `RedisFlowWorker.__init__` 在 `super()` 前把实际 `lease_ttl_seconds`
   按与 overrides 相同口径钳制（`max(1, int(...))`，<=0 WARNING），保证
   「实际生效值 == 哈希输入 == config.effective 自报值」，不再出现
   实际 0 / 哈希 1 的边角分叉。

## 测试（6 条，`tests/unit/test_b94_02_budget_generation.py`）

对应任务要求四条负控：

1. **默认部署**：env 不设、init 传 120 ⇒ `config.lease_ttl_seconds == 120`，
   `budget_generation == bg1(120)`，且 != bg1(300)；
2. **env 对齐 120 零变化**：无 overrides + `PLAITA_LEASE_TTL_SECONDS=120`
   与默认 override 120 的 budget/config generation 完全一致（不翻转）；
3. **CLI 覆盖**：`lease_ttl_seconds=600` / `max_deliveries=3` ⇒ 配置值生效
   且 budget hash 改变；
4. **config.effective 自报 == 实际**：真实 `RedisFlowWorker` 启动日志
   `lease_ttl=120/max_deliveries=5`（及 CLI 600/3 变体）与
   `worker.lease_ttl_seconds`、`_runtime_config`、`budget_generation` 逐项一致。

外加：`task_max_age_seconds=3600`、`dlq_max_len=7` 的 override 也进入哈希。

相邻回归：`test_a2r31_config_generation.py`、`test_a78_event_index_residuals.py`、
`test_a69_03_index_cleanup.py` **58 passed**；console
`test_a2r31_config_unified.py` + `test_a69_02_event_canonical.py`
**21 passed**。

## 负控（先红后绿）

恢复 pre-fix：`DEFAULT_LEASE_TTL_SECONDS=300` + 四个键只读 env（忽略
overrides）：

```
6 failed
E       AssertionError: assert 300 == 120        （默认部署 lease 不自报/不哈希 120）
E       AssertionError: assert 300 == 120        （env 120 与默认 override 不一致）
E       AssertionError: assert 300 == 600        （CLI 覆盖未进入配置/哈希）
E       AssertionError: assert 2592000 == 3600   （task_max_age override 丢弃）
E       AssertionError: assert 'lease_ttl=120' in '... lease_ttl=300 ...'  （config.effective 自报 300）
E       AssertionError: assert 'lease_ttl=600' in '... lease_ttl=300 ...'  （CLI 变体）
```

还原修复后 **6 passed**。完整日志
`docs/proofs/data/b94-02-negative-control.txt`。

## 哈希翻转迁移面（已写入 commit message 与 ops-runbook）

默认部署的预算哈希由 `bg1(300)` 变为 `bg1(120)`，含预算字段的
`config_generation` 同步翻转一次；在途消息携带旧哈希，滚动升级窗口会触发
`queue_config_budget_stale_total` / `config.budget_stale`。这是迁移面而非
数据损坏：drain 旧消息或接受一次性告警，全部 worker/console 重启到新构建后
收敛，新消息按新哈希写入。ops-runbook「配置代际」小节已加注记。

## 验收（2026-10-21，基线 1032874）

- 新增 6 passed；相邻 config/event 回归 **58P + 21P**；
- 全量 unit（`-m "not slow"`）**8600 passed / 12 skipped / 14 deselected /
  3498 subtests**；e2e+integration **287P/4S**；plaita-ai
  **241P/14S/12 subtests**；console **1039P/13S**（本地 stale `frontend/dist`
  已用当前源码重建后全绿；重建产物不入库）；
- layering / wheel / size 三相位全绿；`landing_gate.py ci` 绿
  （main `1032874` success）后单 commit 推送。
