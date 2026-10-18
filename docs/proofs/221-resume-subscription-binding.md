# #221 证明：resume 绑定挂起订阅，陈旧决议不再吞下游等待

- 单号：plaita#221（P1，core 正确性）
- 交付：okguitar（W46，gist `16b7d2dc`）；落地席 B1 按 **#221-only** 抽取
  （同一 gist 的 #222/#223 面不在本单范围）
- 基线：`origin/main` = `afe4bba`（交付补丁 base `9c8820a8`，在 `237f1f12`
  上 `git apply --check` 亦干净；本单只取 #221 hunk）
- 边界：`_handle_resume` 准入 + 手工 resume 载体绑定；不改 event_filter
  去重键、不改 cancel 语义、不改节点 resume 分发表

## 缺陷

同一挂起点的 event 与 timeout 用两把**不同**的去重键
（`plaita/server/event_filter.py:222` 的 `dedup:<event_id>:<sub>` 与 `:333`
的 `timeout:<sub>`），同一订阅可同时入队两条 resume。旧准入只看
「`is_suspending` + `status=pending`」（`strategies.py:385-392`），从不比对
`subscription_id`。先到的一条解决 A 节点、流程推进并在下游 B 重新挂起后，
后到的陈旧 resume 会把 B 标成 completed/timeout 并写入 A 的事件载荷——
无报错、无告警，执行是 suspended（非终态），终态短路也挡不住。

## 实现

- `plaita/core/strategies.py`
  - `_SUBSCRIPTION_BOUND_RESUME_TYPES = (EVENT, TIMEOUT)`：cancel 是执行级
    意图（console `data=None`），刻意不绑定。
  - `RESUME_BINDING_KEY = "__plaita_subscription_id"`：手工 resume 的私有
    载体键，准入通过后、分发到节点前剥除，不污染 `event_data` / 流程输出。
  - `_resume_binding_violation(prev_state, resume_type, resume_data)`：
    - 消息绑定（公开 `subscription_id` 或私有键）必须等于
      `prev_state["subscription_id"]`；
    - event 再比对消息 `event_type` 与挂起 checkpoint 的解析后 `event_type`；
    - 不匹配 / 缺绑定（而 checkpoint 有订阅）⇒ 返回可读原因；
    - `prev_state` 无 `subscription_id`（历史 checkpoint / 简化 bus 替身）⇒
      无绑定可比，放行（修复前行为不变）。
  - `_handle_resume` 在 `current_node.resume(...)` 之前调用上述判据：违规时
    `logger.warning` + `ResumeGuardError`。worker 既有
    `_chain_has_resume_guard_error` 豁免把执行**保持 suspended**、ack 消息、
    不终态化。
- `plaita-console/backend/api/executions.py`：`suspended_subscription_id`
  从 `context.$NODE[$LAST_NODE]` 取当前挂起订阅；手工 event/timeout resume
  附加私有键（`data=None` 也附，与本地档同形）。
- `plaita-console/backend/services/local_executor.py`：
  `_with_resume_binding` 同语义的本地档实现。
- `plaita-console/backend/api/cluster.py`：quick-test 事件载荷补
  `subscription_id`（手工构造，生产由 event_filter 填）。

## 回归用例

`tests/unit/test_issue221_resume_subscription_binding.py`（6 条）：

1. `test_stale_event_resume_at_downstream_node_is_rejected`：陈旧 event
   resume 落在下游 pending 节点 ⇒ 异常链含 `ResumeGuardError`，`$NODE.waitB`
   与基线逐键相等（仍 pending、无 `event_data`）。
2. `test_event_and_timeout_same_subscription_only_one_applies`：同一订阅的
   陈旧 timeout 在 B 挂起时被拒，B 不被写。
3. `test_resume_without_subscription_id_is_rejected_when_state_has_one`：
   缺绑定不是「不匹配才拒」；同节点同订阅的有效 resume 仍能推进（正控）。
4. `test_resume_with_wrong_event_type_is_rejected`：订阅 id 对、
   `event_type` 不符仍拒。
5. `test_cancel_without_subscription_id_still_applies`：cancel 不被误杀。
6. `test_real_filter_enqueues_event_and_timeout_then_stale_timeout_rejected`：
   接上产生侧——真实 `EventFilter.handle_event` +
   `_on_subscription_timeout` 对同一订阅确实双双入队（两把去重键），
   先消费 event、推进到 B 后，后到 timeout 被绑定校验拒绝。

既有契约（手工 resume 现在必须带绑定）同步更新 5 个测试文件：
`test_checkpoint_resume.py` / `test_issue33_suspended_continue_guard.py` /
`test_wave4_subscription_timeout.py` / `test_obs_langfuse.py` /
`test_user_review_fixes.py`——按生产消息形状补 `subscription_id`。

## 负控（落地席独立复跑）

方法：备份修复后的 `plaita/core/strategies.py`，`git checkout HEAD --` 还原到
基线（即删掉 `_resume_binding_violation` / `RESUME_BINDING_KEY` 与
`_handle_resume` 的准入块），清 `__pycache__` 且 `PYTHONDONTWRITEBYTECODE=1`，
跑新用例与 repro；跑完从备份还原并 `cmp` 逐字节复核。

```
$ .venv/bin/python -m pytest tests/unit/test_issue221_resume_subscription_binding.py -q
FAILED ...::test_stale_event_resume_at_downstream_node_is_rejected
FAILED ...::test_event_and_timeout_same_subscription_only_one_applies
FAILED ...::test_resume_without_subscription_id_is_rejected_when_state_has_one
FAILED ...::test_resume_with_wrong_event_type_is_rejected
FAILED ...::test_real_filter_enqueues_event_and_timeout_then_stale_timeout_rejected
5 failed, 1 passed

$ PYTHONPATH=. .venv/bin/python docs/proofs/data/221-repro.py ; echo $?
step4 STALE resume(A) while suspended at waitB -> node waitB is_suspend= False is_end= False
  waitB status      = completed
  waitB event_data  = {'event_id': 'evt-a', 'event_type': 'a.done', ..., 'subscription_id': 'sub-1'}
RESULT: RED —— 陈旧 resume 静默解决了 waitB，流程带 A 的载荷继续
1
```

修复后同两条命令全绿：

```
$ .venv/bin/python -m pytest tests/unit/test_issue221_resume_subscription_binding.py -q
6 passed
$ PYTHONPATH=. .venv/bin/python docs/proofs/data/221-repro.py
step4 STALE resume(A) while suspended at waitB -> ResumeGuardError:
  subscription_id 不匹配（消息 'sub-1' ≠ 挂起订阅 'sub-2'）; execution stays suspended
RESULT: GREEN —— 陈旧 resume 被订阅绑定校验拒绝
0
```

还原后 `cmp` 复核：`plaita/core/strategies.py` 与备份逐字节一致。

原始负控记录另见 `docs/proofs/data/221-negative-control.txt`（交付方 W46）；
落地席独立复跑见 `docs/proofs/data/221-landing-negative-control.txt`。

## 契约变更

手工 event/timeout resume 现在必须携带挂起订阅绑定；已在 `MIGRATION.md`
的 Unreleased 段写明。生产 `event_filter` 消息本就带 `subscription_id`；
控制台与本地档自动从 checkpoint 附加私有键；cancel 豁免。

## 验收记录

- 新增用例：**6 passed**（含真实 EventFilter 双入队组合）。
- 受影响的既有用例：`test_checkpoint_resume.py` +
  `test_issue33_suspended_continue_guard.py` + `test_obs_langfuse.py` +
  `test_user_review_fixes.py` + `test_wave4_subscription_timeout.py`
  = **138 passed**。
- console 全量：`plaita-console/backend/tests/console` = **537 passed /
  13 skipped**（本地档审批 resume 端到端含在内）。
- 全量 `bash scripts/ci-gate.sh pytest
  "--ignore=tests/unit/test_sandbox_release_on_terminal.py"`：
  **8199 passed / 15 skipped / 3434 subtests passed**，覆盖率
  **93.76%**（门槛 80%），分层 / wheel / SC-003 全绿。
- **基线环境破损（与本单无关）**：`tests/unit/test_sandbox_release_on_terminal.py`
  在本机与**干净 HEAD worktree** 上同现 3 failed（`ModuleNotFoundError:
  plaita_nodes.sandbox`——两侧 `plaita-nodes` 均无该模块）。首跑全量在该文件
  处 `-x` 停止（7077 passed）；其后 46 个文件单独跑 **1122 passed**，合起来
  即排除该文件的完整套件。
- `python scripts/landing_gate.py ci`：main CI 最新完成 run `5d828f0` =
  success，允许落地。
