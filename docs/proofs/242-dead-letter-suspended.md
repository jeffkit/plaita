# #242 证明：死信守卫只收尸 running；noderefetch 成功推进清零

- 单号：plaita#242（P1）
- 基线：`origin/main` = `b9d0703`（含 #241 的 resume 取租约后复核落地；
  本单在其上 rebase，未改 #241 代码）
- 边界：只动死信守卫的终态化判据与 `noderefetch` 清零收口；
  #241（load/acquire TOCTOU）、#240（error→running 预检）不混入。

## 根因（两条独立缺陷叠加）

### ① 死信守卫不区分 suspended / running

`plaita/server/flow_worker.py::_dead_letter_guard` 对「非终态 + 租约空 +
重试计数达预算（单节点 `noderetry` 或兜底 `noderefetch`）」一律写
`status=error` + `end_time`。`suspended` 在等外部事件/审批/delay/超时，
重试计数达限并不代表执行搁浅；被终态化后，后续 `resume(event/cancel/
timeout)` 全部命中 `already_terminal`，执行被永久打封，只能人工重建。

### ② noderefetch 终身累计

`{ns}:execution:noderefetch:{id}` 只在失败时 `INCR`，成功推进只清
`noderetry` / `nofail`，不清总计数。长寿命执行累计 100 次可重试失败后
（`NODE_RETRY_TOTAL_MAX=100`），即使当前已经 `suspended`，任意一条超过
`max_deliveries` 的重投消息都会触发①，把可恢复执行终态化。

## 修法

1. **状态门**：终态化分支增加 `getattr(state, "status", "") == "running"`
   前置条件。只有真正卡住的推进态可收尸；`suspended` 及其它非终态保持
   原状，走既有的「重入队一份同体消息后放行原死信」恢复路径（消息不丢，
   DLQ/ack 语义不变——只改执行状态是否 terminalize）。
2. **成功清零**：新增 `_reset_node_retry_total`（`DEL noderefetch`），与
   `_reset_node_retry_counter` / `_reset_deterministic_failure_counter`
   放在同一收口——`_process_execution_result` 中任一节点成功推进后调用。
   计数语义变为「连续失败」，长寿命执行跨节点的偶发失败不再终身累计。
3. **G1 唤醒不清**：与确定性失败计数同款理由——唤醒后若仍连续撞墙，兜底
   上限必须还能收敛；唤醒即清会形成「唤醒→清零→再撞→再唤醒」的无界循环。
4. **#123 不回退**：单节点预算键与「只清刚成功节点」的语义完全不动；
   running + 任一路径达限仍终态化。

## 测试（7 条，`tests/unit/test_issue242_dead_letter_suspended.py`）

- `suspended` + 总计数达限 / 单节点预算达限（参数化 2 条）：
  `status == "suspended"`、`error is None`、`end_time is None`、
  重入队 1 条同体消息；
- `suspended` 记录上已有 error/end_time：逐字段保持原样（不被覆盖）；
- `suspended` + 租约仍在：仍 `return False`、不终态化、不重入队；
- `running` + 总计数/单节点预算达限（参数化 2 条）：仍终态化 error、
  写 `end_time`/`error_type=node_retry_budget_exhausted`、不重入队；
- 成功推进（`is_end=True` 的步进返回）后 `noderefetch` 键被删除，
  执行正常 completed。

相邻回归实跑：`#241`（租约后复核）、`#123`（节点预算/执行 id 钉住）、
`#57`（租户命名空间）、`#47`（失租守卫）、`#233`（suspended 补派）、
`#73`（确定性失败/G1 唤醒/终态 ack）、`test_worker_node_retry.py`
共 **110 passed**。

## 负控

### NC1：删掉状态门（恢复「suspended 也被终态化」）

```python
# 修复态
if (getattr(state, "status", "") == "running"
        and (retry_count >= self._node_retry_budget() or total_exhausted)):
# 变异态
if retry_count >= self._node_retry_budget() or total_exhausted:
```

结果 **3 failed, 4 passed**，红文案：

```
E       AssertionError: expected suspended, got error
E       assert 'error' == 'suspended'
```

### NC2：删掉成功推进处的 `_reset_node_retry_total`

结果 **1 failed, 6 passed**，红文案：

```
E       AssertionError: 成功推进必须清零兜底总重试计数——否则长寿命执行必然累计到 100 后误杀 suspended（plaita#242）
E       assert '100' is None
E        +  where '100' = get('plaita:execution:noderefetch:exec-242')
```

两条负控分别精确对应 issue 确认帖的修复方向 ①/②；还原后均为 7 passed。
完整日志见 `docs/proofs/data/242-negative-control.txt`。

## 验收

- 新增用例 7 passed；相邻回归 110 passed；
- 全量 unit（`-m "not slow"`）**8112 passed / 12 skipped / 14 deselected /
  3492 subtests**；e2e+integration **287P/4S**；console **549P/13S**；
  plaita-ai **233P/14S**（本机补齐 `mcp` 依赖后，与 CI 安装面一致）；
- `scripts/landing_gate.py ci` 绿后单 commit 推送（`closes #242`）。
