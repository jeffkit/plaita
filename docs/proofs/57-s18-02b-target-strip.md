# RED-PROOF — S18-02B worker 侧目标剥离回退 + A2R19-03 组合

日期：2026-10-20 · 执行者：修复落地席 B3（第五十七轮）
仓：`plaita`（基线 `origin/main 3cbf8f4`，worktree `plaita-wt3`）
规格：`duo-a2-ledger.md` §S18-02（第 8 条 / A46-02）、§A2R19-03

## §0 结论速览

| 项 | 结论 |
|---|---|
| ① S18-02B | A46-02 worker 侧回退已在 r50（`1ede53c`）随 S18-02A 落地：`_handover_non_affine` 在 enqueue→ack 前复核目标注册键，缺失/非 running 剥 `target_instance_id` 广播；读失败 fail-open。本轮按 A2R18 探针形态复跑：修复后**一次交接即终止**（state=completed、counter=1）；还原修复前形态**4 次交接 → AffinityHandoverExhausted**。补一条终态回归：广播副本对任意 worker 都不再触发交接。 |
| ③ A2R19-03 × S18-02A | r49 的启动注册重试与 r50 的 register 状态机组合已验证：Redis 恢复后重试成功时保留启动期 `draining`（不复活为 running）、心跳不覆写、派发白名单继续拒绝。新增组合回归 `test_retry_success_preserves_startup_drain_status`。 |
| ② plaita-nodes 余量 | 见 `plaita-nodes/docs/proofs/57-p3-residuals.md`（watchdog 早关路径回归 + `write_run_lock` None 收尸 + jevlike 置信度校验）。 |

## §1 代码面（①，r50 已落地，本轮复核）

`plaita/server/flow_worker.py`：

1. `_unavailable_dispatch_target(body)`：仅当「本机不是目标」时复核；注册键
   不存在 → `missing`；非 JSON → `invalid`；status 非 running → 返回该
   status；读 Redis 失败 → `None`（fail-open 保留 target）。
2. `_handover_non_affine(task, queue, exc)`：在 `_record_affinity_handover`
   之前 `pop(TARGET_INSTANCE_FIELD)`，WARNING
   `dispatch-target-unavailable:broadcast target=<id> status=<status>`；随后
   仍走 enqueue→ack（S18-03），失败留 pending。
3. `_consume_loop` 的 `TaskNotForThisWorker` 分支回写 `_handover_non_affine`
   的 ack 结果。

新增回归（`tests/unit/test_issue42_dispatch_target.py`）：
`test_stripped_copy_is_claimable_by_any_worker` —— 剥 target 后的副本对
`w-low` / `w-third` 都不再抛 `TaskNotForThisWorker`，风暴在一次交接内终止。

## §2 探针（真实 Redis，复刻 `r46_dispatch_target_ghost.py`）

脚本：`docs/proofs/data/r57-target-ghost-probe.py`（Redis db14）。
时序：A 注册 running 且被容量路由选中 → 派发 target=A → **派发后 A 置
stopping**（目标不可用窗口）→ B 消费。

```text
### POST-FIX (A46-02 strip on)
WARNING plaita: dispatch-target-unavailable:broadcast target=MacBookPro-9d0f0d6e status=stopping (执行 r57-target-exec 不再定向，按广播重投)
[route-while-draining] {'a_draining_in_memory': True, 'a_registry_status': 'running', 'target_is_a': True, 'rejected': [('MacBookPro-422891a1', 'status: starting')]}
[dispatch-one] {'enqueued': True, 'decision': 'targeted', 'target_is_a': True, 'message_target': 'MacBookPro-9d0f0d6e'}
[after] {'state_status': 'completed', 'state_error_type': None, 'handover_counter': '1', 'pending': 0, 'stream_len': 0, 'dlq_len': 0}

### NEGATIVE CONTROL (R57_KEEP_TARGET=1, pre-fix shape)
ERROR plaita: 执行 r57-target-exec 交接达上限（4/3）——判定「无宿主可跑」，已终态化 error，不再交接（终止自旋）。最后一次不亲和原因: target_instance_id=MacBookPro-aa682ae4 非本机（MacBookPro-d7846789）：显式派发给别的 worker（交接让出）
[negative-control] keep-target (pre-fix shape)
[after] {'state_status': 'error', 'state_error_type': 'AffinityHandoverExhausted', 'handover_counter': '4', 'pending': 1, 'stream_len': 1, 'dlq_len': 0}
```

原始输出：`docs/proofs/data/r57-target-ghost-nc.txt`。

## §3 A2R19-03 × S18-02A 组合（③）

`tests/unit/test_issue_a2r19_registry_retry.py`：
`test_retry_success_preserves_startup_drain_status` —— 启动注册失败（写键
瞬断）→ 停机请求已在内存置 `draining` → Redis 恢复后重试线程注册成功：

- 注册键 `status=draining`（register 保留生命周期状态，不写 running）；
- `DispatchCandidate.dispatchable is False`（S18-02A 白名单）；
- 心跳线程存活且 0.5s 后状态仍为 draining（不覆写）；
- `registry_registered is True`（就绪面在重试成功后转 ready）。

既有 `test_stop_cancels_pending_registration_retry` 继续覆盖「重试线程不得
在 stop 后写回注册键」。

## §4 验证

```text
$ pytest -q tests/unit/test_issue42_dispatch_target.py
35 passed

$ pytest -q tests/unit/test_issue_a2r19_registry_retry.py
5 passed

$ pytest -q tests/unit/test_r50_registry_state_machine.py tests/unit/test_a2r18_drain_handover.py \
    tests/unit/test_worker_drain.py tests/unit/test_worker_affinity.py
（见本轮台账全量/定向记录）
```
