# #237 证明：租户 ContextVar 跨边界传播契约收口

- 单号：plaita#237（P1）
- 基线：`origin/main` = `4090ca1`（本轮开工后并发推进；#237 正文基线 `fd08740`，
  本单改动与 #227 等并发提交无冲突）
- 边界：只动 contextvars 传播面（+ tenant_status fail-open 的 WARNING 计数）；
  未改 tenant_status fail-open 语义、未引入新租户模型。
- 契约文档：`docs/DESIGN-tenant-context-propagation.md`。

## 1. 同族边界核对（已修三处确认）

| 边界 | 证据/测试 | 结果 |
|---|---|---|
| #58 同步节点线程 `_run_sync_node` | `test_sync_node_timeout` / `test_context` | 绿 |
| #199 `run_async_from_sync` | `test_issue199_async_bridge_context` | 绿 |
| #212 `async_gen_to_sync` | `test_issue212_async_gen_context` | 绿 |
| #213 console `executor.submit` | `test_issue213_quick_test_context` | 绿 |

四者合计 **49 + 2 passed**（含 context/sync-timeout）；相邻 executor/Parallel/
loop 套件 **100 passed / 6 subtests**。

## 2. 本单补的三处（探针实证）

修复前实测：

```
direct (sync node)        -> tenant-b   # #58 已修
parallel-thread 分支      -> default    # 洞：Parallel.arun run_in_executor
                                           + ThreadParallelExecutor 未复制
process 分支              -> default    # 洞：ContextVar 不随 pickle
```

修复：

1. **`ThreadParallelExecutor.submit/map`**（`plaita/core/parallel_executor.py`）：
   提交侧逐任务 `contextvars.copy_context()`，worker `ctx.run(fn, ...)`；
   `map` 每个 item 单独复制（Context 不可并发进入）。覆盖 Parallel join/
   background 分支与 `Map.concurrent` 的 `execute` 路径。
2. **`Parallel.arun` thread/process 外层**（`plaita/node/concurrent.py`）：
   `loop.run_in_executor(None, pool_execute, ...)` → `asyncio.to_thread(...)`
   （`to_thread` 复制 contextvars；裸 executor 不复制）。这是真正的第一道丢失点。
3. **进程模式**（`plaita/core/context.py`）：`ExecutionContext` 在构造期把租户
   钉在 `_tenant_id`（子上下文继承父执行身份）；`__getstate__` 把它写入
   `_pickled_tenant`，子进程 `__setstate__` 恢复 `_tenant_id` 并
   `set_current_tenant`。关键点：不能只在 `__getstate__` 读
   `current_tenant()`——ProcessPool 的 pickler 跑在队列线程，那里是 default
   （实测 `TRACE SETSTATE default`）。

新增测试 `tests/unit/test_issue237_context_boundaries.py`（6 条）：

- `ThreadParallelExecutor.submit` / `map` 的 tenant + 第二 ContextVar (fence)；
- 真实 Parallel(thread) 分支返回 tenant-b，且不污染调用方；
- `ExecutionContext` pickle 往返携带租户；
- 真实 Parallel(process) 分支返回 tenant-b（pytest 子进程，含 `slow` 标记）。

## 3. tenant_status fail-open WARNING 计数（owner 裁决）

`plaita/server/tenant_status.py`：

- fail-open 语义不变（缺失记录 = active）；
- 无客户端 / 读失败 / 无记录三条路径统一 `_note_fail_open`：WARNING +
  进程内 `tenant_fail_open_total()` 计数；
- 新增 `tests/unit/test_issue237_tenant_status_warning.py`（5 条）：缺失记录
  计数+WARNING、无客户端计数+WARNING、disabled 拒绝不计数、active 不计数、
  空 tenant_id 不计数。

## 4. 负控（逐边界还原必红）

| 变异 | 结果 | 红文案（全文 `docs/proofs/data/237-*`） |
|---|---|---|
| A · `ThreadParallelExecutor.submit/map` 回退 base | **3 failed / 3 passed** | submit/map/parallel-thread 的 `tenant-b` 断言失败（实得 default） |
| B · `Parallel.arun` 回退 `run_in_executor` | **1 failed / 5 passed** | `parallel_thread_branch_sees_tenant` 实得 default |
| C · `__getstate__` 不携带租户 | **2 failed / 4 passed** | pickle 往返与 process 分支实得 default |
| D · `_note_fail_open` 静默 no-op | **2 failed / 3 passed** | `tenant_fail_open_total()` 未增长 / 无 WARNING |

还原后：6 boundary + 5 status 全绿；A/B/C 互不干扰（定向只杀对应面）。
证据：`docs/proofs/data/237-neg-a-thread-executor.txt`、
`237-neg-b-to-thread.txt`、`237-neg-c-process-pickle.txt`、
`237-neg-d-tenant-status.txt`。

## 5. 同族扫描（grep 站点 → 归类）

命令：

```
grep -rn "threading.Thread\|ThreadPoolExecutor\|ProcessPoolExecutor\|run_in_executor\|executor.submit\|asyncio.run\|ensure_future\|create_task\|to_thread" \
  --include='*.py' plaita/ plaita-console/backend/ | grep -v '/tests/\|/build/'
```

归类：

- **已携带**：`runner._run_sync_node`（#58）、`async_utils` 两桥（#199/#212）、
  console cluster submit（#213）、flow_worker 消费线程（每条消息 set）、
  event_filter（订阅 tenant_id set/reset）、`Parallel.arun`/`ThreadParallelExecutor`
  /process ExecutionContext（#237）。
- **同 loop 自动继承**：`Parallel.coroutine` 的 gather/ensure_future、
  `Map.arun` 的 gather、event/redis 的 `create_task(..., context=...)`。
- **显式载荷、无 ContextVar 读**：delay/approval/http_callback/schedule 服务
  （`task_config["tenant_id"]`）、flow_worker 的 watchdog/cancel 线程
  （`_cancel_watch` 显式存 tenant）。
- **基础设施线程**：obs/alerts/metrics/log_handler/registry/control——不读租户
  做存储/凭据路由；日志在调用侧取 tenant。
- **子进程**：code sandbox / agentrun 用显式 env/参数，不读 ContextVar。
- **未在本轮改**（owner 写边界策略）：存储/凭据写路径的 hard fail-closed 需要
  `set_current_tenant` 的 explicit-set 标记与调用点审计，属独立语义变更；
  已在契约 §4 记为后续执行项，不在「只动传播面」范围。

## 6. 验收记录

- #237 定向：6 boundary + 5 status **11 passed**；
- 已修三处 + #58：**49 + 2 passed**；相邻 executor/Parallel/loop：**100 passed**；
- console 全套：**546 passed / 13 skipped**；
- plaita-ai 全套：**233 passed / 14 skipped**；
- e2e + integration：**287 passed / 4 skipped**；
- 全量 unit：**8013 passed / 1 failed / 12 skipped / 3434 subtests**。唯一失败为
  既知 `test_issue159_policy_url_exception_sinks.py`（yarl 文案环境差异，clean
  基线同样失败）；无新增失败；
- landing gate：`4090ca1` conclusion=**success**（CI / slow lane / Console E2E /
  Deploy docs）；
- 产品代码：`plaita/core/{context,parallel_executor}.py`、
  `plaita/node/concurrent.py`、`plaita/server/tenant_status.py` + 测试/文档。

## 7. 交付记录

- commit：本单唯一 commit（`git log -1`），提交信息含 `closes #237`；
  #228 已在先前提交关闭并作为同专题判例并入。
- 验收面：unit 8013P/1F（既知 yarl 环境面）；console 546P/13S；
  plaita-ai 233P/14S；e2e+integration 287P/4S。
- landing gate：绿（base `4090ca1` CI / slow lane / Console E2E / Deploy docs）。
- **未改语义（owner 写边界策略）**：存储/凭据写路径的 hard fail-closed 需要
  `set_current_tenant` explicit-set 标记与写调用点审计，属独立语义变更；
  本轮只落传播 + tenant_status WARNING 计数，契约 §4 已记为后续执行项。
