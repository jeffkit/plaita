# #243 证明：#241 租约内 suspended 复核不得自撞租约

- 单号：plaita#243（P1 回归，#241 引入）
- 基线：`origin/main` = `366e44e`
- 边界：只动 resume 租约内复核的 suspended 补偿路径；#241 的
  「取租约后重读并复核」主体语义、#233/#195 的挂起派发与补偿语义、
  #33「suspended + continue 不取租约」契约均不回退。

## 根因

#241 在 `resume_flow` 取到租约后再次调用 `_load_and_guard_resume_state`
复核盘上状态。若窗口内上一持有者已把执行写成 `suspended` 并留下
`dispatch_pending`，复核分支调用 `_compensate_suspend_dispatch`；后者内部
**再次 `_acquire_lease` 同一 execution 的租约**——外层 resume 已持有 ⇒
内层必然失败 ⇒ 补派跳过（日志「执行租约被另一 worker 持有」）。复核随后
返回 `already_suspended` + `rejected_after_lease`，`run()` 对正常返回 ack，
消息不再重投；delay/approval 任务永远不会被投递，执行永久 suspended。

触发窗口是 #241 新增的：旧实现的 suspended 短路在取租约**之前**，补派
可以自己成功取租约；新实现把补偿放到了外层租约**之内**。

## 修法

- `_load_and_guard_resume_state(..., lease_held: bool = False)`：租约内
  复核调用传 `lease_held=True`；租约前调用保持默认 False。
- `_compensate_suspend_dispatch(..., lease_held=False)`：`lease_held=True`
  时**不再 acquire**，直接复用外层租约所有权，调用拆出的
  `_dispatch_suspend_pending`。
- `_dispatch_suspend_pending`：重读盘上状态 → 校验仍 `suspended` 且有
  `dispatch_pending` → 投递一次 → 清凭据。投递失败上抛（消息不 ack 可重投）；
  清凭据失败同样上抛（服务侧幂等吸收重复投递）。
- 独立载体（start dedup、租约前短路）保持原「短期租约 + 持锁重读」路径，
  `lease_held` 默认 False，行为不变。

## 测试（2 条，`tests/unit/test_issue243_resume_suspended_compensation.py`）

交错复现（提报席 fakeredis 复现转正）：

1. 盘上 running；A 先持租约（gen=1）；
2. 包装 `_acquire_lease`：B 的 load→acquire 窗口内 A 写
   `suspended + dispatch_pending={pending:True, service_type:delay,...}` 并
   释放租约；
3. B 取得 gen=2 租约，租约内复核看到 suspended，用**外层租约**补派。

断言：

- 引擎 `FlowExecution` 未构造（不重跑挂起节点，#33）；
- 盘上 `dispatch_pending is None`（负控主判据；自撞时为 `pending=True`）；
- `plaita:delay:queue` **恰 1 条**任务，任务 `type=delay`、
  `execution_id` 正确；
- 返回 `status=suspended` / `already_suspended` / `rejected_after_lease` /
  `dispatch_compensated` 全为真；
- 第二次 `continue`：凭据已清 ⇒ 不再补派，队列仍 1 条（只投一次）；
- 外层租约在 finally 释放。

第 2 条：`_compensate_suspend_dispatch(lease_held=True)` 在无凭据时是纯
no-op——不取租约、不投递（#33 零租约契约的单元级钉子）。

相邻回归实跑：`#241`（TOCTOU completed/suspended 两例）、`#233`（正常挂起
派发/补偿）、`#33`（suspended+continue 零租约）、`#195`（事件 correlation）、
`#242`（DLQ）、`#57`、`#123` 共 **71 passed**。

## 负控

把租约内复核调用改回 `lease_held=False`（恢复自撞 acquire）：

```
1 failed, 1 passed
E       AssertionError: 补派后凭据必须清空，实际 {'pending': True, ...}
E       assert {...} is None
WARNING ... 执行 exec-243 挂起补派跳过（source=resume:continue）：
        执行租约被另一 worker 持有，由其推进/重新派发
```

还原修复后 **2 passed**。完整日志 `docs/proofs/data/243-negative-control.txt`。

## 验收

- 新增 2 passed；相邻回归 71 passed；
- 全量 unit（`-m "not slow"`）**8122 passed / 12 skipped / 14 deselected /
  3492 subtests**；e2e+integration **287P/4S**；console **549P/13S**；
  plaita-ai **233P/14S**；layering / wheel / size 三相位全绿；
- 基线 `366e44e` CI **success**，`landing_gate.py ci` 绿后单 commit 推送
  （`closes #243`）。
