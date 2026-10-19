# worker P2 批证明：跨 flow execution_id 守卫 + suspended 早退取消检查

- 台账来源：`~/.issue-keeper/duty/duo-a-ledger.md`
  - ① cross-flow `execution_id` 覆写（**第三十一轮 V1**，任务书引作 r33 V1）；
  - ② suspended 早退路径漏取消检查（**第三十四轮 ④**）。
- 基线：`origin/main` = `786de97`
- 无 GitHub issue 单号（台账项，按 core-F5 先例落地）。
- 边界：只动 `flow_worker.py` 的 start 载体查盘判定、start 异常分类与
  `_load_and_guard_resume_state` 的 suspended 短路分支；首投、同 flow
  start→resume 载体转换、无取消意图的 #33 契约、终态幂等与 flow_hash
  校验面全部不变。

## ① cross-flow execution_id 覆写

### 根因

`_existing_start_checkpoint` 在盘上 `flow_id` 与本次 start 不一致时返回
`None`（按首投处理），随后 `start_flow` 新建 `context={}` 的 running 行
**无条件覆写**同一 execution_id：

```
盘上: execution_id=X, flow_id=fA, status=suspended, checkpoint={$LAST_NODE: wait,...}
start_flow(flow_id=fB, execution_id=X)
  → _existing_start_checkpoint: stored=fA != fB → None
  → ExecutionState(flow_id=fB, context={}) 落盘   # A 的 checkpoint 永久丢失
  → 执行 fB 到 completed，盘上变成 fB/completed
```

旧探针（修复前）：

```
[probe1] before: fA suspended {'$LAST_NODE': 'wait', '$NODE': {'x': 1}}
[probe1] start returned: {'execution_id': 'exec-r37', ..., 'result': 'B-done'}
[probe1] after: fB completed {'$LAST_NODE': 'end'}
[probe1] A checkpoint preserved? False
```

影响：A 的断点/状态被永久抹掉，此后任何正确的 event/continue resume 都
失去续跑现场；触发面是畸形消息、误重投或 execution_id 复用（正常 console
每次新铸 UUID，但 ID 冲突一旦发生就是不可逆的数据损失）。

### 修法

- 新增 `ExecutionIdConflictError(ValueError)`：查盘发现 flow_id 不一致即
  `logger.error` + 抛出，**不落任何行**、不动 A 的状态。
- `start_flow` 单独捕获该异常：若 dedup 映射是**本次**认领的，先释放
  （避免 7 天内同 dedup_key 指向另一条 flow 的 execution_id），再原样上抛。
- 继承 `ValueError` 是刻意选择：`run()` 对 ValueError 走 poison ack——同一
  execution_id 换 flow 重投永远不会自愈，重投只烧 delivery；状态保留 +
  ERROR 日志是这条路径的可观测面。
- 同 flow 的 start→resume 载体转换（#234）原样保留。

## ② suspended 早退路径漏取消检查

### 根因

`_load_and_guard_resume_state` 对 `suspended + continue/retry` 在**租约之前**
幂等短路（#33），而取消标志检查在 `resume_flow` 取到租约之后。若控制面在
running 阶段写下取消标志，执行随后挂起，这条 continue 消息会在取消检查之前
早退：消息被 ack、盘上保持 suspended、flag 仍为 1。独立的 cancel resume
载体一旦丢失/未被消费，执行就带着取消意图长期 suspended。

旧探针（修复前）：

```
[probe2] resume returned: {'status': 'suspended', 'already_suspended': True}
[probe2] after: suspended end_time= None flag= 1
```

### 修法

suspended 幂等短路前先查 `_cancel_requested(execution_id)`：

- 未命中 → 原有短路/补派路径逐字不变（**不取租约**，#33 契约保持）；
- 命中 → 打 WARNING 并返回 `(state, None)`，交给调用方走正常路径：
  取租约 → 租约内复核 → 复用 resume 入口既有的取消检查点终态化
  `cancelled`（保留取消点前 checkpoint，写 `end_time`，finally 释放租约）。
  只有确有取消意图时才多取一次租约——这次取租约是终态化写盘的互斥前提。

新探针（修复后）：

```
[probe2] resume returned: {'status': 'cancelled', 'cancelled_at_resume': True}
[probe2] after: cancelled end_time= 2026-10-19T08:40:22... flag= 1
[probe2] FE called? 0
```

flag 保留到 7 天 TTL 自清理（与运行中取消终态化后的既有语义一致）。

## 测试

新增 `tests/unit/test_worker_p2_cross_flow_and_suspended_cancel.py`
（8 个函数 / 9 个用例），并更新
`tests/unit/test_issue234_start_carrier_resume.py` 中原本钉住旧行为的
`test_flow_id_mismatch_falls_back_to_fresh_start`。

① cross-flow（4 条）：

- 跨 flow start 抛 `ExecutionIdConflictError`，引擎不启动，A 的
  `flow_id/status/context` 原样保留（负控主判据）；
- 异常是 `ValueError` 子类（run() poison ack 语义）；
- 本次认领的 dedup 映射被释放；
- 同 flow_id 的 start 载体仍正常转 resume（不过度收紧）。

② suspended+cancel（5 个用例）：

- `continue` 与 `retry` 命中取消标志 → `cancelled` + `end_time` +
  checkpoint 保留 + 租约释放 + 引擎不启动；
- 无取消标志 → 仍 `already_suspended`、不取租约（#33 回归）；
- start 重投载体转 resume 后同样吃取消意图。

## 负控（还原必红）

命令与输出见 `docs/proofs/data/worker-p2-negative-control.txt`。

- 还原①（mismatch `return None`）→ `TestCrossFlowExecutionIdGuard`
  **2 failed / 2 passed**，主用例 `DID NOT RAISE ExecutionIdConflictError`；
- 还原②（删除早退前取消检查）→ `TestSuspendedCancelEarlyReturn`
  **3 failed / 1 passed**，`assert 'suspended' == 'cancelled'`；
- 还原修复后：新文件 + #234 回归 **20 passed**。

## 验收

- 定向回归（23 个相关文件）：**224 passed**；
- `python3 scripts/landing_gate.py ci`：绿（main CI success）；
- 文档同步：`docs-site/docs/distributed/flow-worker.md`（start 载体
  flow_id 冲突、suspended 取消优先）、本证明与负控日志。
