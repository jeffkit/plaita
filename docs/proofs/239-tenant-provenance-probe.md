# #239 先行证明：租户来源三态标记 + 写调用点审计探针（只观测）

> **phase 2 已更新（owner 2026-10-18 裁决）**：写路径已切 fail-closed——来源
> `default` 抛 `TenantContextMissingError`（见
> `docs/proofs/239-fail-closed-writes.md`）。本文件记录先行观测阶段，其中
> "default 写仍成功"的断言已被 phase 2 测试迁移取代；三态标记与跨边界传播
> 机制部分仍然有效。

- 单号：plaita#239（P2；**先行非行为部分**——fail-closed 决策待 owner 对后半裁决）
- 基线：`origin/main` = `8740485`（#237 契约落地后）
- 边界：不改任何写行为；探针只计数 + DEBUG。提交信息用 `refs #239`，不关闭
  issue。

## 1. 三态来源标记

`plaita/tenant_context.py`：

- `_tenant_source_ctx`（ContextVar）三态：
  - `explicit`：当前上下文显式 `set_current_tenant(...)`；
  - `derived`：经执行边界从上一上下文派生；
  - `default`：从未设置（`current_tenant()` 回退 default）。
- `set_current_tenant(tenant_id, *, source="explicit")` 返回 **`TenantToken`**
  （tenant/source 两个 `Token`），`reset_current_tenant` 同时复位两者且兼容
  历史裸 `Token`。
- `mark_tenant_derived()`：边界 worker 内把 explicit→derived；derived 幂等；
  **default 保持 default**（否则默认执行会被误记为派生）。
- `current_tenant_source()` 供探针读取。

### 边界接线（随 #237 的 copy_context/ctx.run 链）

| 边界 | 标记点 |
|---|---|
| 同步节点线程（#58） | `runner._run_sync_node` 的无超时/超时两条线程路径 |
| `run_async_from_sync`（#199） | worker 内 `_run_derived()` |
| `async_gen_to_sync`（#212） | `_drive()` 首行 |
| `ThreadParallelExecutor.submit/map`（#237） | `_run_derived(fn, ...)` |
| `Parallel.arun` to_thread（#237） | `_pool_execute_derived()` |
| console quick-test submit（#213） | `_run_publish_derived()` |
| 进程模式（#237） | `ExecutionContext._tenant_source` → `__getstate__` 携带，子进程 `__setstate__` 置 derived（default 仍 default） |

`copy_context`/`ctx.run` 会把来源一起带过去；边界 worker 内的
`mark_tenant_derived` 只修改**副本**，调用方来源保持 explicit（有测试钉住）。

## 2. 写调用点审计探针（只观测）

`tenant_context.record_write_probe(site)`：

- 按 `(site, source)` 计数（`write_probe_snapshot()` 可读，测试用
  `reset_write_probe()`）；
- `default` 来源额外 `logger.debug("tenant write probe: ... source=default")`；
- **不抛错、不改变返回值、不拦截**。

接线：

- `plaita/server/tenant_context.py`：`storage.save_execution_state` /
  `storage.delete_execution_state` / `storage.save_flow`；
- `plaita-console/backend/services/credentials_svc.py`：`credentials.save`
  （校验通过后）/ `credentials.delete`。

## 3. 测试（13 条）

`tests/unit/test_issue239_tenant_provenance.py`（11）：

- 三态：未设置 → `("default","default")`；显式 set/reset → explicit；
  `mark_tenant_derived()` 对 default 无操作；
- 跨边界：`ThreadParallelExecutor.submit` / `.map`、running-loop
  `run_async_from_sync`、`ExecutionContext` pickle → 全部 derived；
  边界后调用方仍 explicit；
- 探针：default 计数 + DEBUG 文案；explicit 计数；storage save_flow 计数且
  返回 True（行为不变）。

`plaita-console/backend/tests/console/test_issue239_write_probe.py`（2）：

- default 来源 `save_credential` / `delete_credential` 成功且各计数 1（**行为
  零变更**，fail-closed 未实施）；
- 显式 tenant-b 下同样计数到 explicit。

## 4. 负控（只观测面）

| 变异 | 结果 | 红文案/证据 |
|---|---|---|
| A · 去掉线程池 worker 的 `mark_tenant_derived()` | **2 failed / 9 passed** | submit/map 的 source 断言实得 explicit（`239-neg-a-derived.txt`） |
| B · 去掉 credentials.save 的 probe | **2 failed** | 两例 `credentials.save` 计数缺失（`239-neg-b-cred-probe.txt`） |
| C · 去掉 storage.save_flow 的 probe | **1 failed** | `KeyError` 取 `("storage.save_flow","default")`（`239-neg-c-storage-probe.txt`） |

还原后 13 passed。

## 5. 语义边界

- **写行为零变更**：default 来源的凭据/存储写照常成功；探针不参与控制流。
- **fail-closed 未实施**：owner 对 #239 后半的裁决未到；本 commit 只落
  explicit-set 标记与真实分布观测（契约 §4 的先行项）。
- `#237` 的传播/契约不受影响；新增标记随同一 copy_context/ctx.run 链传播。

## 6. 验收记录

- #239 定向：**13 passed**（11 unit + 2 console）；负控 A/B/C 如上；
- 既有 tenant/boundary 套件：**73 + 7 passed**
  （tenant_context / runner_tenant / #199 / #212 / #237 / subscription /
  #27 / credentials routing / #213 quick-test / #226 credential isolation /
  credentials API）；
- console 全套：**548 passed / 13 skipped**；
- plaita-ai 全套：**233 passed / 14 skipped**；
- e2e + integration：**287 passed / 4 skipped**；
- 全量 unit：**8024 passed / 1 failed / 12 skipped / 3434 subtests**。唯一失败为
  既知 `test_issue159_policy_url_exception_sinks.py`（yarl 文案环境差异，clean
  基线同样失败）；
- landing gate：`8740485` conclusion=**success**（CI / slow lane / Console E2E /
  Deploy docs）；
- 产品代码：`plaita/tenant_context.py`、`core/{context,runner,async_utils,
  parallel_executor}.py`、`node/concurrent.py`、`server/tenant_context.py`、
  console `cluster.py` / `credentials_svc.py`（均为标记/探针接线）。

## 7. 交付记录

- commit：本单唯一 commit（`git log -1`），提交信息 **`refs #239`**
  （#239 保持 open，待 owner 后半裁决）。
- 验收面：unit 8024P/1F（既知 yarl 环境面）；console 548P/13S；
  plaita-ai 233P/14S；e2e+integration 287P/4S。
- landing gate：绿（base `8740485` CI / slow lane / Console E2E / Deploy docs）。
- 行为边界：写行为零变更（default 来源写仍成功）；fail-closed 未实施。
