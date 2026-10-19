# 租户上下文跨边界传播契约（plaita#237）

> 状态：P1 契约收口。适用范围：所有把执行搬出当前线程/进程的边界
> （线程、线程池、asyncio 同步桥、进程族、`executor.submit`、事件总线消费者）。

## 1. 背景：同根五次复发

租户身份由 `plaita.tenant_context` 的 ContextVar 承载；ContextVar **不随**
新线程/新进程/裸 executor 传播。历史上逐点补过五次：

| # | 边界 | 修复 |
|---|---|---|
| #58 | 同步节点线程（`NodeRunner._run_sync_node`） | `contextvars.copy_context()` + `node_ctx.run` |
| #199 | `run_async_from_sync`（running loop 时 offload 线程） | 调用线程 `copy_context()`，worker `ctx.run(asyncio.run, ...)` |
| #212 | `async_gen_to_sync`（lazy worker 线程） | 同 #199，整个 `_drive` 在 `ctx.run` 下 |
| #213 | console `cluster.py` 的 `executor.submit(asyncio.run, ...)` | `submit(ctx.run, asyncio.run, ...)` |
| #237 | `ThreadParallelExecutor.submit/map`；`Parallel.arun` 的 `run_in_executor`；进程模式分支 | 本文件 + 代码 |
| #256 | code 节点 Python `subprocess` 沙箱（fresh interpreter） | 首版经 env 传播 `PLAITA_TENANT_ID` + 凭据路径；A42-06 确认子进程不是凭据隔离边界（同用户 + 共享密钥 + 宿主 FS），改为**非 default 租户 spawn 前 fail-closed** |

每次缺一个显式契约 → 新执行单元再次复发。本契约把「哪些边界必须携带租户、
怎么携带、缺失时如何处置」写成可 review 的规则。

## 2. 传播义务（必须）

任何跨边界执行单元都必须在**提交侧**显式携带租户上下文：

- **线程 / 线程池**：`contextvars.copy_context()` + worker `ctx.run(fn, ...)`。
  一个 `Context` 只能进入一次；并发提交（如 `map`）必须**逐任务**复制。
- **asyncio 同步桥**：在调用线程复制，worker 内 `ctx.run(asyncio.run, coro)`。
- **asyncio 任务 / gather / ensure_future**：同一事件循环内由 Task 自动继承
  上下文，无需额外复制；跨到线程用 `asyncio.to_thread`（**不要**用
  `loop.run_in_executor` + 裸函数——它不复制 contextvars）。
- **进程族**：`Context` 不可 pickle。租户必须作为**执行身份**写入序列化体
  （`ExecutionContext._tenant_id` → `__getstate__` → 子进程 `__setstate__`
  恢复），不能依赖提交进程的 ContextVar。
- **进程族子进程 runner（code 沙箱）**：`subprocess` 后端与 worker 同用户、
  文件系统不受限，且所有租户共用同一把解密密钥——它不是凭据隔离边界
  （A42-06 探针可直接读 default 文件或 `set_current_tenant("default")`）。
  该后端对**非 default 租户在 spawn 前 fail-closed**
  （`TenantContextPropagationError`，用户代码零执行机会），租户级 code 节点
  走 `docker`（不挂载宿主凭据）；default 租户既有语义不变（#256）。
- **`executor.submit` / 第三方 executor**：按线程池规则；无法复制时，必须把
  `tenant_id` 作为显式参数/消息字段传递。
- **事件总线消费者**：从订阅/消息的 `tenant_id` 字段 `set_current_tenant`，
  handler 结束 `reset`（`event_filter.py` 先例）。
- **服务线程池**：服务任务以 `task_config["tenant_id"]` 显式携带租户（现状
  delay/approval/http_callback/schedule 均如此）；若将来服务 handler 要读
  ContextVar，必须先按线程池规则复制。

## 3. 边界矩阵（当前实现）

| 边界 | 机制 | 回归钉 |
|---|---|---|
| 同步节点线程 `_run_sync_node` | `copy_context` + `node_ctx.run`（#58） | `test_sync_node_timeout` / `test_context` |
| `run_async_from_sync` | `copy_context` + `ctx.run(asyncio.run)`（#199） | `test_issue199_async_bridge_context` |
| `async_gen_to_sync` | 同上（#212） | `test_issue212_async_gen_context` |
| console quick-test submit | `submit(ctx.run, asyncio.run, ...)`（#213） | `test_issue213_quick_test_context` |
| `ThreadParallelExecutor.submit/map` | 提交侧逐任务 `copy_context`（#237） | `test_issue237_context_boundaries` |
| `Parallel.arun` thread/process 外层 | `asyncio.to_thread`（复制 contextvars；#237） | 同上 |
| 进程模式分支 | `ExecutionContext` 携带 `_tenant_id` 跨 pickle（#237） | 同上 |
| `Map.arun` concurrent | `asyncio.gather`（同 loop 自动继承） | `test_loop` + #237 executor.map |
| `Map.execute` concurrent | `ThreadParallelExecutor.map`（#237） | 同上 |
| flow_worker 消费线程 | 每条消息 `set_current_tenant(body["tenant_id"])` | `test_runner_tenant_context` |
| event_filter 消费者 | 订阅/事件 tenant_id 显式 set/reset | `test_subscription_tenant_timeout` |
| delay/approval/http_callback/schedule 服务 | `task_config["tenant_id"]` 显式载荷 | 各服务测试 |
| obs/alerts/metrics/registry/control/log 后台线程 | 基础设施线程，无租户敏感读；日志在调用侧取 tenant | — |
| code 节点 Python subprocess 沙箱 | 非 default 租户 spawn 前 fail-closed（子进程不是凭据隔离边界；同用户 + 共享密钥 + 宿主 FS）；default 语义不变（#256 / A42-06） | `test_issue256_subprocess_tenant_credentials` |
| agentrun 子进程 | 显式 env/参数，不读 ContextVar | — |

## 4. Owner 裁决（写边界 / 状态闸）

- **存储/凭据写边界 fail-closed**：租户上下文缺失或与凭据路由不一致时应拒绝
  写并 WARNING（宁可单跑失败，不可跨租户串写）。
  - 现状：console 写 API 用显式 `tenant_scope(required=True)`；引擎存储按
    ContextVar 路由。ContextVar 的「上下文丢失」与「有意 default」在当前模型
    里不可区分——真正的硬拒绝需要一个 **explicit-set 标记**
    （`set_current_tenant` 置位）+ 写路径审计。
  - **phase 2 已落（plaita#239，owner 2026-10-18 裁决 fail-closed）**：
    `set_current_tenant` 携带 explicit/derived/default 三态来源（`TenantToken`
    同步复位），执行边界 worker 调 `mark_tenant_derived()`；
    `guard_tenant_write(site)` 在 storage（`save_execution_state` /
    `delete_execution_state` / `save_flow`，含 Redis 直写站点）与 credentials
    （`save` / `delete`）对来源 `default` 抛具名
    `TenantContextMissingError`，**不触达写**；`explicit` / `derived` 照常。
    探针计数保留（含被拒尝试）。
  - **单租户部署迁移行**：flow_worker / console 启动时显式
    `set_current_tenant(DEFAULT_TENANT_ID, source="explicit")`（见
    `flow_worker.run`、console `create_app`/`lifespan`）；测试 conftest 同样
    显式设 default，不再依赖静默降级。
- **租户状态闸 fail-open 维持**：缺失记录 = active（default 单租部署兼容）；
  但每次命中（无客户端 / 读失败 / 无记录）必须 `WARNING` 并累加进程内计数
  `tenant_fail_open_total()`（`plaita/server/tenant_status.py`，本轮实现）。
- **Redis-only 恢复（A2R25-06）**：console 启动对账以 DB 为权威，把 Redis
  短视图里 DB 已不存在的非 default 租户回落成 `disabled` 墓碑——恢复旧快照
  把删除时的 disabled 回滚成 active 时，重启 console 即纠正。`tenant_is_active`
  对「整条记录缺失」仍 fail-open（#237）：无法与升级前未发布状态区分；
  default 行缺失同样保持（#237/A48-04），只告警不改写。

## 5. Review 检查项（新增执行单元时）

1. 这个边界会不会把代码跑出当前线程/进程/事件循环？
2. 是线程池？→ 提交侧 `copy_context`（逐任务）；是 asyncio 跨线程？→
   `asyncio.to_thread`；是进程？→ 租户写入序列化体并在子进程恢复；是
   子进程 runner？→ env/参数显式携带，子进程入口恢复，无法恢复则 fail-closed。
3. 添加「租户不退 default」性质测试 + 变异负控（删复制代码必须红）。
4. 若写存储/凭据，检查租户与路由一致；需要 fail-closed 时先确认 explicit-set
   标记与调用点（见 §4 后续项）。

## 6. 相关代码

`plaita/core/async_utils.py`、`plaita/core/runner.py`、`plaita/core/context.py`、
`plaita/core/parallel_executor.py`、`plaita/node/concurrent.py`、
`plaita/node/code.py`（code 沙箱子进程，#256）、`plaita/credentials.py`
（`subprocess_credentials_env`）、`plaita-console/backend/api/cluster.py`、
`plaita/server/tenant_status.py`、`plaita/server/{flow_worker,event_filter}.py`、
`plaita/server/services/*`。
证明与负控：`docs/proofs/237-tenant-context-contract.md`、`docs/proofs/data/237-*`；
#256：`tests/unit/test_issue256_subprocess_tenant_credentials.py`。
