# 升级指南 (Migration Guide)

本文件列出 plaita 历次版本中**破坏向后兼容**的改动，并给出迁移步骤。如果你升级后遇到行为变化，先来这里查。

本文件只关心**怎么改代码**。

---

## Unreleased（0.5.x）

### Storage：`db` 执行/流程存储从公开路径下架

**变更前**：`--execution-storage-type db` / `--flow-storage-type db` 以及
`create_storage_component("db", "execution"|"flow")` 会实例化
`SqlalchemyExecutionStorage` / `SqlalchemyFlowStorage`。
**问题**：这些实现的方法是 `async def`，而 `ExecutionStorage`/`FlowStorage` ABC
与 `FlowWorker`/`EventFilter` 均为**同步**调用——状态不会真正落盘（coroutine 被当返回值）。

**变更后**：

- factory 对 `db` + `execution`/`flow` 直接 `ValueError`（说明原因与替代方案）
- FlowWorker / EventFilter CLI：`--execution-storage-type` / `--flow-storage-type`
  仅接受 `memory` | `redis`
- `db` **subscription** 与 **event-bus** 仍可用（调用方为 async）
- `plaita.storage.sqlalchemy` 类保留供实验，**不得**再经公开 factory/CLI 用于执行状态

```diff
-python -m plaita.server.flow_worker --execution-storage-type db --database-url ...
+python -m plaita.server.flow_worker --execution-storage-type redis --redis-url ...
```

```diff
-create_storage_component("db", "execution", database_url=...)
+create_storage_component("redis", "execution", redis_url=...)
+# 或 memory（单测 / 本地）
```

### Event：`HAS_SQLALCHEMY` 与 `__all__` 条件修复

**变更前**：SQLAlchemy 符号是否进入 `plaita.event.__all__` 错误地绑定 `HAS_REDIS`。
**变更后**：独立 `HAS_SQLALCHEMY` 标志；`__all__` 按该标志扩展。

### 任务队列：Redis List → Stream（at-least-once）

**变更前**：`EventFilter` / 测试脚本 `RPUSH`，`RedisFlowWorker` `BLPOP` — **at-most-once**。
**变更后**：

- 同一 `--queue-name` 键现为 **Redis Stream**（需 Redis 5+）
- 入队：`XADD`（`plaita.server.task_queue.enqueue_task`）
- 消费：consumer group（默认 `plaita-workers`）+ `XREADGROUP`；成功 `XACK`；超时 `XCLAIM` 回收 pending
- 处理失败**不** ack → 至少一次重投；畸形消息 ack 丢弃
- CLI：`--consumer-group`、`--consumer-name`、`--claim-min-idle-ms`

**迁移**：

```diff
-redis-cli RPUSH plaita:flow:queue '{"type":"start",...}'
+python -c "from plaita.server.task_queue import enqueue_task; import redis; r=redis.from_url('redis://...'); enqueue_task(r,'plaita:flow:queue',{...})"
```

旧 List 键上的积压消息**不会**自动迁移；升级前 drain 或换新 stream 键名。

重复 `start`/`resume` 在崩溃回收后可能再执行一次——节点与副作用应幂等。

### 中间态落盘：每步持久化

**变更前**：`PERSIST_EVERY_N_STEPS = 5`，连续推进最多丢 4 步。
**变更后**：默认 **1**（每步 `save_execution_state`）。挂起/结束/出错仍为立即落盘。

---

**变更前**：部分文档写「至少一次」「容错长时工作流」「生产级」；订阅 `event_type` 被写成 fnmatch。
**变更后（文档与注释）**：

- FlowWorker = suspend/resume 编排器；任务队列已改为 Stream **at-least-once**（见上节）
- 中间态落盘间隔公开为 `FlowWorker.PERSIST_EVERY_N_STEPS`（默认 1）
- 订阅匹配为 event_type **全等**；fnmatch 仅 handler 路径
- 用户文档入口：`docs-site/docs/distributed/flow-worker.md`「可靠性边界」

无调用方代码迁移；若运维按旧文档按「至少一次」做容量规划，需按新边界重估。

---

## 0.5.0

0.5.0 是一次"激进 break"主版本，按架构师/开发者/使用者三视角批评集中整改。下面按"踩坑概率从高到低"排序。

### 0. 分布式：订阅失败不再挂起 + FlowWorker 默认启用 EventBus

**变更前**：`register_subscription` 失败仍返回 `is_suspend=True`；FlowWorker 需 `--use-event-bus` 才注入总线，默认与 EventFilter 订阅存储分裂。
**变更后**：

- 订阅失败抛 `FlowExecutionException`，**拒绝挂起**
- FlowWorker **默认**创建 EventBus；仅 `--no-event-bus` 可关闭（`--use-event-bus` 保留为无操作兼容）
- EventFilter CLI 优先使用 `event_bus.subscription_storage`

```diff
-python -m plaita.server.flow_worker --use-event-bus ...
+python -m plaita.server.flow_worker ...
+# 仅在确认无挂起节点时:
+python -m plaita.server.flow_worker --no-event-bus ...
```

### 0b. DSL：共享 ``validate_flow_ir`` + AI 编译门闭合

**变更前**：拓扑校验在 builder / sexpr 各写一份；`flow_from_source` / `compile_flow` 只做 AST→IR，不查拓扑；`_default_known_node_types` 吞掉 registry 异常返回空集。
**变更后**：

- 唯一入口：`plaita.dsl.ir_validate.validate_flow_ir` / `build_flow`（默认递归 `childFlow` / parallel branch flow）
- `FlowBuilder.build`、`parse_sexpr`、`flow_from_source`、`plaita_ai.compile_flow` 全部走同一门
- registry 不可用时**显式抛错**，不再静默空集

```python
from plaita.dsl import validate_flow_ir, build_flow, FlowIRValidationError
from plaita.dsl.codeflow import flow_from_source

flow = flow_from_source(src)  # 含拓扑校验
```

### 0c. EventBus：handler 成功后再去重；factory db 传 engine；sexpr experimental

**变更前**：三后端在 handler 执行**前** `mark_event_processed`，首次失败即永久丢事件；`create_event_bus("db")` 传 `database_url` 但构造函数要 `engine`；sexpr 与 @flow 并列宣传。
**变更后**：

- 去重改为 `is_event_processed` 跳过已成功项，**成功后再** `mark_event_processed`
- factory db / subscription storage：`database_url → create_async_engine → engine=`
- `plaita.dsl.sexpr` 标为 **experimental**（非一等作者路径）
- 新增 `plaita.core.node_context.NodeExecutionContext` Protocol；`Node.execute` 类型注解切过去

### 0d. codeflow 包拆分 + Console Admin/Contract 分面（非破坏）

**变更前**：`plaita/dsl/codeflow.py` 单文件巨石；Console Swagger/README 未区分管理面与契约面。
**变更后**：

- `plaita.dsl.codeflow` 改为包：`_common` / `_expr` / `_nodes` / `_stmt` / `_source`；公开导入路径不变（`from plaita.dsl.codeflow import flow, ...`）
- Console：`main.py` 用 `_mount_admin` / `_mount_contract` + OpenAPI tag `admin`/`contract`；README 增加「API 分面」说明（URL 不变）

### 1. `plaita.flow` shim 删除（import 路径 break）

**变更前**：`from plaita.flow import Flow, FlowExecution, ...` 可用（带 `DeprecationWarning`，原计划 0.6.0 删）。
**变更后**：`plaita/flow.py` 模块**已删除**，`from plaita.flow import ...` 直接 `ImportError`。

```diff
-from plaita.flow import Flow, FlowExecution, ExecutionMode
+from plaita import Flow, FlowExecution, ExecutionMode
# 或显式走 core:
+from plaita.core.flow import Flow
+from plaita.core.executor import FlowExecution, ExecutionMode
```

**迁移**：全局搜索 `from plaita.flow import` / `import plaita.flow`，改成 `from plaita import ...`（推荐）或 `from plaita.core.* import ...`。`parse` / `parse_and_run` / `FlowCallback` / `FlowEvent` / `CallbackManager` / `LoggerCallback` / `FlowExecutionException` 同样从 `plaita` 顶层导出。

### 2. CodeNode 默认 `sandbox_backend` 改 `docker`（需装 Docker daemon）

**变更前**：`register_code_node()` 默认 `sandbox_backend="restricted"`（RestrictedPython AST 沙箱，文档自承有 AST 绕过向量）。
**变更后**：默认 `sandbox_backend="docker"`（容器级隔离：`--network none` / `--read-only` / 资源上限）。Docker daemon 不可用时 **`register_code_node()` 拒绝注册** 并报错，列出三种降级路径，**不允许静默降级到 `restricted`**。

```python
from plaita.node import register_code_node

register_code_node()                  # 0.5.0 默认 docker, 需本机 Docker daemon
register_code_node(default_backend="subprocess")  # 半信任: 进程级, 不隔离 FS/网络
register_code_node(default_backend="unsafe")      # 完全信任代码作者
register_code_node(default_backend="restricted")  # 显式声明半信任 (AST 沙箱, 有已知绕过向量)
```

**迁移**：
- 生产多租户：装 Docker daemon，开箱即用。
- 无 Docker 环境 / 半信任：显式 `register_code_node(default_backend="subprocess")`。
- 内部开发完全信任：`register_code_node(default_backend="unsafe")`。
- daemon 不可用且未显式降级时，注册期报错：`CodeNode default sandbox backend is "docker" but Docker daemon is unavailable. Install Docker, or pass default_backend="subprocess"|"unsafe"|"restricted".`

### 3. HMAC nonce 改 Redis 后端（多 worker 真重放保护）

**变更前**：`plaita-console` 的 HMAC nonce 缓存是进程内单例，gunicorn 多 worker 下每个 worker 独立缓存，3 秒窗口内**跨 worker 重放可行**。`replay_protected=True` 给"已保护"的错觉。
**变更后**：抽象 `NonceStore` 接口，新增 `RedisNonceStore`（`SET nonce NX EX ttl` 原子操作，跨 worker 共享）。`verify_authorization` / `PlaitaClient` 启动时按配置注入 store。

```python
from plaita_console.backend.services.signature import (
    configure_nonce_store, enable_replay_protection,
)

# 单进程 / 测试 (默认)
enable_replay_protection()                       # InMemoryNonceStore

# 生产多 worker (推荐)
configure_nonce_store(redis_url="redis://...")   # 自动切 RedisNonceStore
enable_replay_protection()
```

**break**：`replay_protected=True` 在多 worker 部署下若未配 Redis store，启动时 `logger.warning` 明确告知"仅单 worker 保护"。

**迁移**：多 worker 部署（gunicorn/uvicorn workers>1）配 `configure_nonce_store(redis_url=...)`；单进程可继续用默认 `InMemoryNonceStore`。

### 4. `Flow.parse_flow` 解耦全局 registry（解析/执行分离）

**变更前**：`Flow` 在 Pydantic `model_validator(mode="before")` 里调 `get_default_registry()`，Flow 解析隐式耦合模块级单例 registry 状态——import 期注册了坏节点，全进程解析任何 flow 都坏。
**变更后**：`Flow` 只解析结构，`nodes` 字段保留为原始 dict；节点解析延迟到执行期 `Flow.resolve_nodes(registry)`。`Flow.from_string` / `from_file` / `Flow.model_validate` 默认仍调一次 `resolve_nodes(get_default_registry())`，**99% 用户用法不变**。

**迁移**：一般无需改动。仅当**自定义 registry** 或**在 import 期注册节点**时：
- 显式传 registry：`FlowExecution(flow, registry=my_registry)` 或 `flow.resolve_nodes(my_registry)`。
- 不再依赖"import 期注册的节点立刻对 `Flow.from_string` 生效"——确保注册发生在 `Flow` 解析之前（`init_default_registry()` 是推荐入口，见下条）。

### 5. `init_default_registry()` 显式初始化入口

**变更前**：默认 `NodeRegistry` 是模块级 `_default_registry = NodeRegistry()`，import 期静默创建，插件发现首次 `get_default_registry()` 时隐式触发——隐式可变单例 + import 期副作用。
**变更后**：新增 `init_default_registry(*extra_nodes, auto_discover=True)` 显式入口，建议在启动脚本调一次。`get_default_registry()` 仍可用，但首次隐式调用会 `logger.debug` 提示。

```python
from plaita.node import init_default_registry, register_code_node

init_default_registry()                       # 显式装载内置 + 插件节点
register_code_node(default_backend="docker")  # 按需 opt-in CodeNode
```

**迁移**：建议在应用启动脚本顶部把"隐式 `get_default_registry()`"换成显式 `init_default_registry()`。不调仍可用（向后兼容），仅多一条 debug 日志。

### 6. `execution.mode` 内部类型 str → `ExecutionMode` enum

**变更前**：`execution.mode` 是裸字符串（`"normal"`/`"generator"`/`"distributed"`），全库散落 `mode == "generator"` 字符串比较——拼写错误静默成 `False`。
**变更后**：内部类型 `Optional[ExecutionMode]`，比较一律走 enum。**公共入口仍接受字符串**：`Flow.run(mode="generator")` / `FlowExecution(mode="generator")` / `execution.mode = "generator"` 在边界处经 `_coerce_mode` 统一一次。

```python
from plaita.core.executor import ExecutionMode

# 这两种写法等价 (公共入口接受字符串):
flow.run(mode="generator")
flow.run(mode=ExecutionMode.GENERATOR)

# 节点插件内部比较改 enum (若你直接读 execution.mode):
if execution.mode == ExecutionMode.GENERATOR:   # 0.5.0
    ...
# 而非:
# if execution.mode == "generator":             # 0.4.x (0.5.0 起不再推荐)
```

**迁移**：99% 代码无需改动（公共入口兼容字符串）。仅当你**直接读 `execution.mode` 并和字符串比较**（第三方节点插件），建议改成 `ExecutionMode.GENERATOR` / `ExecutionMode.DISTRIBUTED` / `ExecutionMode.NORMAL`。`execution.mode` getter 现在返回 `ExecutionMode` enum（不再是 str），`assertEqual(execution.mode, "normal")` 这类断言需改成 `ExecutionMode.NORMAL`。

### 7. `Parallel` background branches 失败不再静默

**变更前**：`Parallel` 节点 fire-and-forget 后台分支失败时无 future 引用、无错误回调、无超时——失败完全沉默。
**变更后**：持有 future 引用，`add_done_callback` 记录失败分支异常到模块级 `_BG_STATE`（按 `execution_id` 索引，避免 `FlowExecution` 在多进程下不可 pickle）。新增调试接口：

```python
from plaita.node.concurrent import wait_background_branches, get_background_errors

wait_background_branches(execution, timeout=5)   # 可选: 等后台分支收尾
errors = get_background_errors(execution)         # 取后台分支失败列表
```

**迁移**：无需改动（fire-and-forget 语义不变，只是失败不再沉默）。调试时可调 `wait_background_branches` / `get_background_errors`。

### 8. `$ENV` 升级静默失败告警

**变更前**（0.4.0）：`$ENV` 改 allowlist，旧 flow 升级后 `$ENV.HOME` 静默变空字符串，下游继续跑不报错——"沉默地坏掉"。
**变更后**：`Flow.model_validate` / `model_validate_json` 扫描所有节点表达式，若发现 `$ENV.` 引用但 `expose_env` 为空，`logger.warning` 一次列出 key 名 + 修复指引。**不报错**（保持兼容），让沉默变可见。

**迁移**：无需改动。升级后留意日志 warning，按提示把 key 加到 `expose_env`。

### 附：异常治理 / executor 拆分 / mutmut 范围

- **bare `except:` 清零**：`plaita/` 内 5 处 bare `except:` 全部改 `except Exception:` 并加 `logger.warning`/`debug`；CI 对 `E722`（bare except）硬失败，对 `BLE001`（blind-except）仅 warn（`--exit-zero`），存量位点随整改逐步清理。
- **`executor.py` 拆分**：757 行拆成 `plaita/core/strategies.py`（`RunOptions`/`ExecutionMode`/三策略/helper）+ `plaita/core/executor.py`（`FlowExecution` facade）。公共导出不变（`from plaita import FlowExecution, ExecutionMode` 仍可用）。
- **mutmut 覆盖范围诚实声明**：变异测试**仅覆盖纯同步路径**（callback/expression/parallel_executor/calculate/decide）；async/distributed/timeout 不在 mutmut 自动覆盖范围，见 `docs/mutation-testing.md` 顶部声明。引用变异测试结果时请标注"仅同步路径"。

---

## 0.4.0

0.4.0 一次性引入了 5 处破坏性变更，集中在**安全模型**与**接口收紧**。下面按"踩坑概率从高到低"排序。

### 1. `$ENV` 改为 allowlist 模型（默认空）

**变更前**：流程表达式 `$ENV.XXX` 可读取任意环境变量，靠一份"敏感前缀黑名单"过滤。
**变更后**：`$ENV` 默认空；Flow 必须显式声明 `expose_env` 才能读到环境变量。

```python
# 变更前
flow = Flow.from_string('{"flow_id": "demo", ...}')
# 流程里 $ENV.HOME 仍能读到

# 变更后
flow = Flow.from_string(
    '{"flow_id": "demo", "exposeEnv": ["HOME", "API_BASE"], ...}',
)
# 或 Python 构造：
flow = Flow(flow_id="demo", expose_env=["HOME", "API_BASE"], ...)
```

**迁移**：搜索代码库所有 `$ENV.` 引用，把 key 名收集起来，加到对应 Flow 的 `expose_env`（JSON 写 `exposeEnv`，Python 写 `expose_env`）。

**注意（黑名单已移除）**：早期 0.4.0 曾保留 `_SENSITIVE_ENV_PREFIXES` 黑名单作为 allowlist 之上的「第二层防御」，但它的 `startswith` 匹配挡不住 vendor 前缀密钥名（`OPENAI_API_KEY` / `STRIPE_KEY` / `PG_CONN` 等），已移除。现在 allowlist 命中即暴露，仅打 `logger.warning` 做审计——**请自行确认 `expose_env` 里没有不该暴露的密钥**，不要再依赖任何「敏感词拦截」。

### 2. `CodeNode` 移出默认注册表（opt-in）

**变更前**：`{"type": "code", ...}` 节点开箱即用。
**变更后**：CodeNode 不在默认 `_BUILTIN_NODES` 里，必须显式注册。

```python
from plaita.node import register_code_node

register_code_node()  # 默认注册到全局 registry
# 或自定义 registry：
# register_code_node(my_registry)
```

**迁移**：在 worker / server 启动入口（`main()` 或 FastAPI lifespan）调一次 `register_code_node()`。

未注册时解析流程会得到错误：`unRecognized node type: code. CodeNode was moved out of the default registry in 0.4.0; call register_code_node() at startup to opt in.`

### 3. CodeNode 默认沙箱化（RestrictedPython）

**变更前**：Python 后端走 raw `exec()`，可访问任意内置模块。
**变更后**：默认 `sandbox_backend="restricted"`（RestrictedPython AST 沙箱），允许 import 的模块白名单见 `SANDBOX_SAFE_MODULES`（math/json/re/datetime 等纯计算模块）。

可选后端：

| backend | 隔离强度 | 适用场景 |
|---|---|---|
| `restricted`（默认） | AST 级 | 多租户、纯计算 |
| `subprocess` | 进程级 | 需要 math/datetime/json 以外的模块，半信任 |
| `docker` | 容器级（无网络/只读 FS） | 完全不信任代码，生产多租户 |
| `unsafe` | 无 | 完全信任代码作者（内部开发） |

```json
{
  "type": "code",
  "language": "python",
  "sandbox_backend": "docker",
  "code": "def run(input): ..."
}
```

**迁移**：如果你的 CodeNode 用了 `os`/`sys`/`open`/网络/文件，要么改代码、要么显式设 `sandbox_backend="unsafe"`（仅当你完全信任代码作者）。**JS 后端（PyExecJS）仍未沙箱化，且 PyExecJS 自 2020 年起未维护——不要在生产多租户场景使用。**

### 4. `parallel mode="coroutine"` 下线

**变更前**：`mode="coroutine"` 可用，但实际在任何 running event loop（FastAPI/Starlette）中 100% 崩 `RuntimeError`。
**变更后**：直接 `raise ValueError`，错误消息指引改用 `mode="thread"` 或 `mode="process"`。

**迁移**：把 `"mode": "coroutine"` 改成 `"mode": "thread"`。

```diff
 {
   "type": "parallel",
-  "mode": "coroutine",
+  "mode": "thread",
   "branches": [...]
 }
```

### 5. `Flow.start_node` 删入度 0 启发式

**变更前**：Flow 没有显式 `type=start` 节点时，会扫描节点数组找"未被任何 next/branch 引用的节点"当入口——多个孤儿节点时结果依赖数组顺序。
**变更后**：没有显式 Start 节点直接抛 `FlowStartMissingError`。

**迁移**：

1. 手写 JSON/YAML 的：检查所有 flow 定义，确保每个 flow 都有一个 `{"type": "start", ...}` 节点。
2. 用 `@flow` / `FlowBuilder` 的：框架会自动注入，无需改动。
3. 用可视化编排工具导出的：检查导出结果是否包含 Start 节点。

### 6. `execution.context` 类型由 `dict` 改为 `CheckpointState`

**变更前**：`ExecutionContext.context` / `FlowExecution.context` 返回 `Dict[str, Any]`（内部 `_context` dict 的活引用）。
**变更后**：底层存储改为 `plaita.core.state.CheckpointState`（Pydantic `BaseModel`），`context` 返回该 model 实例。它实现完整 dict-like 协议（`__getitem__`/`__setitem__`/`__contains__`/`get`/`keys`/`items`/`__iter__`/`__eq__`），对 `$INPUT`/`$NODE` 等 prefixed key 的访问与旧 dict 完全一致；`__eq__` 与 plain dict 比较也保持兼容（`assertEqual(ctx.context, {...})` 仍通过）。

```python
# 变更前
assert isinstance(execution.context, dict)

# 变更后
from plaita.core.state import CheckpointState
assert isinstance(execution.context, CheckpointState)
# dict-like 行为不变：
execution.context["$INPUT"]
"$LAST_NODE" in execution.context
execution.context.get("$NODE", {})
```

**迁移**：

- 直接 `isinstance(ctx, dict)` 的断言会失败——改用 `isinstance(ctx, CheckpointState)` 或只断言 dict-like 行为（`in` / `[]` / `.get`）。
- `to_dict()` / `from_dict()` 的存储格式**逐键兼容**，Redis/SQL 里旧的 checkpoint 无需迁移即可加载（round-trip property test 钉死）。
- 把 `execution.context` 当 dict 传给第三方库（`json.dumps` / `dict(...)` / `deepcopy`）仍可用（dict-like 协议）；但需要"纯 dict"的场景请显式 `dict(execution.context)` 或 `execution.to_dict()`。

### 7. `execution.flow_id` / `last_node_id` / `last_branch` → `execution.state.xxx`

**变更前**：`FlowExecution` facade 以裸属性透传 typed 系统状态——`execution.flow_id`、`execution.last_node_id`、`execution.last_branch`（13 个 `@property` 透传的一部分）。
**变更后**：裸属性删除，统一走 `execution.state` 视图（`plaita.core.executor._StateView`，`None` 归一化）：

```python
# 变更前
flow_id = execution.flow_id or "unknown"
upstream = execution.last_node_id

# 变更后
flow_id = execution.state.flow_id or "unknown"
upstream = execution.state.last_node_id
branch = execution.state.last_branch
```

**迁移**：

- `plaita/server/nodes/{redis_queue,kafka_queue,http_callback,delay,approval}_node.py` 与 `plaita/node/assignment.py` 已一并改到新 API；第三方节点插件读这几处需同步改。
- `execution.context["$FLOW_ID"]` / `execution.get_state("$FLOW_ID")` 字典式访问**不变**，仍可用。
- `execution.mode` / `execution.timeout` 不变（内部改存 `RunOptions`，facade property 保持兼容）。
- `ExecutionContext`（内部对象）的 `last_node_id` / `last_branch` / `flow_id` typed property 保留——它们是 `execution.state` 视图的底层实现，runner / strategy 仍用。

### 附：`ErrorStrategy` 字段类型 str → enum

**变更前**：`ErrorHandler.strategy` 字段类型 `Optional[str]`，比较时用 `_strategy_eq(value, member)` 容忍字符串/enum 混用。
**变更后**：字段类型 `ErrorStrategy`（enum），validator 接受 enum/str/`continue_with` 下划线别名，比较用 `==`。

**迁移**：99% 的代码无需改动（validator 兼容字符串）。仅当你**手动构造** `ErrorHandler(strategy="continue_with")` 而不是从 JSON 解析时，建议改成 `ErrorHandler(strategy=ErrorStrategy.CONTINUE_WITH)`。`_strategy_eq` 已删除，外部 import 需要清理。

---

## 升级检查清单

升级到 0.5.0 前过一遍：

- [ ] 全局搜索 `from plaita.flow import` —— 改成 `from plaita import ...`
- [ ] 全局搜索 `register_code_node` —— 确认 Docker daemon 可用，或显式 `default_backend="subprocess"|"unsafe"|"restricted"`
- [ ] 多 worker 部署：配 `configure_nonce_store(redis_url=...)` + `enable_replay_protection()`
- [ ] 节点插件直接读 `execution.mode` 的：字符串比较改 `ExecutionMode` enum
- [ ] 启动脚本：用 `init_default_registry()` 显式初始化默认 registry（可选，向后兼容）
- [ ] 跑一遍单元测试套件：`pytest tests/ -q --ignore=tests/integration --ignore=tests/e2e -m "not integration"`
- [ ] 跑一遍集成测试套件：`pytest tests/integration/`

升级到 0.4.0 前过一遍：

- [ ] 全局搜索 `$ENV.` —— 列出 key 加到对应 Flow 的 `expose_env`
- [ ] 全局搜索 `type.*code` —— 在启动入口加 `register_code_node()`
- [ ] 全局搜索 `mode.*coroutine` —— 改成 `thread`
- [ ] 全局搜索 flow 定义 —— 确认每个都有 `type=start` 节点
- [ ] 跑一遍单元测试套件：`pytest tests/ -q --ignore=tests/integration --ignore=tests/e2e -m "not integration"`
- [ ] 跑一遍集成测试套件：`pytest tests/integration/`

升级遇到本文件没覆盖的行为变化？开 issue 贴复现步骤，会补到这里。
