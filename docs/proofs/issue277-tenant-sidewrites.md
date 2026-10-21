# #277 证明：删除租户后 DLQ/订阅迟到写必须按世代拒绝

- 单号：plaita#277（P1 security；跨世代数据泄露）
- 基线：`origin/main` = `3497a0e`（实现前；落地时并发 B3 r66 已合入，已 rebase）
- 边界：只加 DLQ/订阅写面的租户 active+世代闸；不改 `delete_tenant` 的
  flow 拒绝门/DB-凭据事务顺序，不改 execution fencing 主体。

## 根因

`delete_tenant` 会清空租户 namespace、队列/PEL、DLQ、订阅并写
`plaita:tenant_status[tenant]=disabled`；但已经取走任务的在途 worker 只在
`_dispatch_task` 入口查一次租户状态。删除后它仍可能走失败/挂起路径：

- `RedisStreamTaskQueue.dead_letter()` 无状态/世代校验，直接 XADD 平台级 DLQ，
  payload 含旧租户 `params/data`（探针 OLD-SECRET）；
- `RedisEventSubscriptionStorage.store_subscription()` / `mark_event_processed()`
  对 `plaita:subscription:` 无状态/世代校验，旧执行仍可重注册/更新订阅。

同名重建的租户（新世代）通过 console DLQ/订阅读面读到前代数据；默认
execution fencing 只保护状态写，DLQ/订阅不在其内。

## 修法

1. **世代发布**：`tenant_context.py` 定义平台键常量；
   `server/tenant_status.py` 增加 `publish_tenant_generation` /
   `read_tenant_generation`（Redis meta HASH）。`create_tenant` 提交后发布
   新 `created_at` 世代；`publish_all_tenant_status` 启动对账补齐各租户
   世代；`delete_tenant` 的 disabled 墓碑由状态闸拒绝。
2. **DLQ 写闸**：`StreamTask` 增加 `tenant_generation`，队列 `read`/`invalid`
   构造时从 Redis meta 读取当时世代。`dead_letter()` 写 XADD 前调用
   `tenant_write_allowed(redis, tenant_id, task.tenant_generation)`：
   - disabled / 世代不同 / 世代缺失（meta 已存在）→ 抛具名
     `TenantWriteRejected`，**不写 DLQ、不 ack**；
   - `dead_letter_if_allowed` 捕获后返回 False（消息留 pending + WARNING +
     `tenant_write_rejected` 计数）；
   - default 租户保持 #237 兼容。
3. **订阅写闸**：`EventSubscription` 增加 `tenant_generation`；worker
   `_consume_loop` 派发时把 task 世代写入 ContextVar；`store_subscription`
   在无显式世代时取上下文/当前世代并比较，`mark_event_processed` 用订阅已存
   世代复核。disabled/旧世代 → `TenantWriteRejected`，不写平台订阅键。
4. 分层：状态/世代常量放在 foundation `tenant_context.py`，event 层只引用
   常量并做异步读，不向上 import server（layering 门绿）。
5. `PLAITA_DISABLE_FENCING=1`：DLQ/订阅闸独立于 fencing 仍生效；execution
   状态写在关闭 fencing 时的既有回滚风险按 ops-runbook 注记，不在本单重做。

## 测试

`tests/unit/test_issue277_tenant_sidewrites.py`（7 条）：

- 删除后 DLQ 迟到写拒绝、DLQ 长度不变、`dead_letter` 抛
  `TenantWriteRejected(reason=tenant_disabled)`；
- 同名重建新世代：旧 task 拒绝、新 task 正常死信（DLQ 无 OLD-SECRET）；
- meta 存在但 task 无世代 → fail-closed；
- default 租户兼容；
- 队列 `read` 捕获当前世代；
- 订阅：删除/旧世代 store 拒绝；新世代 store 成功；世代再次前移后
  `mark_event_processed` 拒绝。

`plaita-console/backend/tests/console/test_issue277_tenant_sidewrites.py`
（1 条，真实 `create_tenant`/`delete_tenant` + fakeredis）：删除后旧世代
DLQ 写返回 False、DLQ 空；重建后旧世代仍拒、新世代可正常死信。

相邻回归：queue/DLQ 族 **133P**；event/subscription 族 **185P**；
console 租户族 **22P**。

## 负控（先红后绿）

关闭 `dead_letter` 的 `tenant_write_allowed` 与订阅 `_tenant_write_allowed`
（直接放行）：

```
unit: 5 failed, 2 passed
E       assert True is False                      （删除后 DLQ 写被放行）
E       Failed: DID NOT RAISE TenantWriteRejected  （旧世代订阅写被放行）
console: 1 failed
E       assert True is False
ERROR ... Dead-lettered task 1-0 -> test:r54-console-dlq ...（OLD-SECRET 复活）
```

还原修复后 unit **7 passed**、console **1 passed**。完整日志
`docs/proofs/data/issue277-negative-control.txt`。

## 验收（2026-10-21，rebase 到 280be04 后）

- 新增 unit 7P + console 1P；相邻 queue/DLQ 133P、event/subscription 185P、
  console 租户 22P；B3 r66 新增队列用例 13P；
- 全量 unit **8714P/12S/14 deselected/3498 subtests**；e2e+integration
  **287P/4S**；console **1068P/13S**；plaita-ai **241P/14S/12 subtests**；
  layering/wheel/size 全绿；`landing_gate.py ci` 绿后单 commit 推送
  （closes #277）。
