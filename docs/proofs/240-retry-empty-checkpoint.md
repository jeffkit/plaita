# #240 证明：error + retry 的空 checkpoint 不得先翻 running

- 单号：plaita#240（P1+P2）
- 基线：`origin/main` = `635c7f7`
- 边界：只动 error→running 的 retry 前置能力校验；不改 suspended 路由、
  租约/terminal 写屏蔽、#53 running+retry 语义、#33 suspended+continue。

## 根因（顺序错误）

`plaita/server/flow_worker.py::resume_flow` 的 G1 唤醒分支在**验证 retry 能力
之前**就把 error 翻成 running 并落盘：

```python
if retry_wakeup:
    state.status = "running"
    state.error = None
    state.end_time = None
    self._persist_state_or_raise(execution_id, state, "retry_wakeup")
```

retry 要求盘上有 checkpoint；策略层在更晚的 `run_distributed` 才抛
`ResumeGuardError`。协议错误分支虽「保持原状」，但原状已被改成 running——
一次非法 `error → running` 迁移，同时抹掉 error payload 与 end_time（失败
证据丢失），错误消息还自证 `status=running`。

本单按**三个状态不变量**分别钉住（issue 正文一处非法迁移的三个被破坏字段）：

1. `status` 必须保持 `error`；
2. 原 `error` payload 原样保留；
3. 原 `end_time` 原样保留。

## 修法

在 `retry_wakeup` 翻转**之前**预检：

```python
if retry_wakeup and not state.context:
    raise ResumeProtocolError(
        "... remains error (status=error, error/end_time preserved)",
        execution_id=execution_id,
    )
```

- 不落盘、不创建 `FlowExecution`、不清重试计数、不触发节点；
- 新增 `except ResumeProtocolError: raise`（在通用 `except Exception` 之前）：
  预检异常必须原样上抛给 `run()`（ack、不终态化），不能被通用失败分支二次
  终态化成 `NodeFailureTerminalizedError`；
- G1 预算/确定性失败/flow_hash 的现有短路顺序不变（都在翻转前）；
- 合法 `error + 非空 checkpoint + retry` 仍先翻 running 落盘再步进。

## 测试（9 条，`tests/unit/test_issue240_retry_empty_checkpoint.py`）

三个不变量各参数化 `context={}` / `context=None`（共 6 条）：

- 抛 `ResumeProtocolError` 且消息含 `status=error`；
- `status == "error"`（不是 running）；
- `error == 原始 payload`（`message/code/error_type/node_id`）；
- `end_time == 原始值`；
- `FlowExecution` **未被构造**（无节点执行）。

回归 3 条：

- `error + 非空 checkpoint + retry` → 引擎看到 running、saved_context 原样、
  推进到 completed；
- `error + continue` → `already_terminal` 且仍 error（终态短路不受影响）；
- `running + retry` → 仍断点续跑（#53 语义不变，新预检只作用于
  `retry_wakeup`）。

## 负控

把预检条件改为 `if False and retry_wakeup and not state.context:`：

```
6 failed, 3 passed
FAILED ...keeps_status_error[empty-dict]/[none]
FAILED ...preserves_error_payload[empty-dict]/[none]
FAILED ...preserves_end_time[empty-dict]/[none]
```

红文案（全文 `docs/proofs/data/240-negative-control.txt`）：
`assert 'status=error' in "resume 协议错误（执行保持原状 status=running）..."`、
`assert None == {...}`（error payload）、`assert None == '2026-10-02T00:00:00'`
（end_time）。3 条回归在变异下保持绿（合法 retry/continue/running+retry 不受
预检影响）。还原后 **9 passed**。

## 回归

相邻 worker/resume 套件：`test_g1_resume_retry` / `test_worker_g1_retry_combo` /
`test_issue53_unmatched_branch_resume` / `test_issue73_deterministic_bounded` /
`test_checkpoint_resume` / `test_issue234_start_carrier_resume` /
`test_issue221_resume_subscription_binding` = **78 passed**。

## 验收记录

- #240 定向：**9 passed**；负控 **6F/3P**；还原后 9P；
- 相邻套件：**78 passed**；
- console 全套：**548 passed / 13 skipped**；
- plaita-ai 全套：**233 passed / 14 skipped**；
- e2e + integration：**287 passed / 4 skipped**；
- 全量 unit：**8113 passed / 1 failed / 12 skipped / 3492 subtests**。唯一失败为
  既知 `test_issue159_policy_url_exception_sinks.py`（yarl 文案环境差异，clean
  基线同样失败）；
- landing gate：`7f85393` conclusion=**success**（CI / slow lane / Console E2E /
  Deploy docs）；
- 产品代码：`plaita/server/flow_worker.py`（预检 + 异常分流）+ 测试/证明。

## 交付记录

- commit：本单唯一 commit（`git log -1`），提交信息含 `closes #240`。
- 验收面：unit 8113P/1F（既知 yarl 环境面）；console 548P/13S；
  plaita-ai 233P/14S；e2e+integration 287P/4S。
- landing gate：绿（base `7f85393` CI / slow lane / Console E2E / Deploy docs）。
- 并发注记：落地期间 base 先为 `806ec14`（#239②），其 CI 因 #151 log hygiene
  失败（`storage/redis.py:138` 裸异常，与本单无关）；并发修复 `7f85393` 落地后
  base 转绿。本单在独立 worktree 基于 `7f85393` 仅携带 #240 改动提交；共享工作
  树中他席 #239 phase-2 WIP 未纳入本 commit。
