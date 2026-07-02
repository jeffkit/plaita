# 更新日志

本页记录 **v0.4.0 及以后**的正式变更历史，按版本倒序排列。  
0.4.0 之前的历史可通过 `git log --oneline` 查阅仓库完整提交记录。

## 未发布 — 架构止血三件套

### 包结构

- **演示脚本与伴生文档移出 wheel**：`plaita/event/demo_eventbus.py`、`plaita/event/timeout_example.py`、`plaita/server/extended_nodes_demo.py`、`plaita/server/test_event.json` 及各 `.md` 迁至 `examples/` 与 `docs/`；`pyproject.toml` 新增 `[tool.setuptools.exclude-package-data]` 兜底，防止 `.md`/`.json`/demo 类文件再混入发布包。导入改为绝对路径，`demo_eventbus` 不再玩 `sys.path` 黑魔法。

### Facade

- **移除 `FlowExecution` 的 `__getattr__` / `__setattr__` 魔法**：context 字段（`context` / `execution_id` / `event_bus` / `cancel_event` / `express_*`）改为具名 property，state 访问改为显式 delegate 方法。未声明的属性就是普通实例属性，拼写错误不再静默落进 context state。`strict_attrs` 开关与 `trigger_*` → `on_*` 映射一并移除（后者无调用方）；要手动触发回调请直接用 `execution.callback_manager.on_xxx(...)`。

### 错误模型

- **`FlowExecutionException` 子类体系**：新增 `NodeNotFoundError` / `FlowStartMissingError` / `NodeExecutionError` / `NodeTimeoutError` / `FlowTimeoutError` / `FlowErrorException` / `ErrorResultException` / `ResumeError`，各自携带默认 `code` / `error_type`。调用方只需提供 `message` 与 `node`，不必再往调用点塞 `-500` / `-520` / `-1` 等魔法数字。基类 `(code, message, error_type, node)` 位置签名保留以兼容历史调用方；`run_distributed` 边界仍按历史契约归一化为 `FLOW_ERROR / -500`。

### 测试

- `tests/unit/test_facade_attr_proxy.py` 改为验证显式委托行为（无 magic、未知属性不污染 context、`execution_id` 只读）。
- 顺手把 `ExecutionContext` 拆出 `_safe_environment` / `_coerce_input_value` 两个模块级纯函数，类体回到 196 LOC，满足 SC-003 `< 200` 预算。

---

## 未发布 — 第二轮清理（毒点 triage A/B 档）

### A 档：低风险清理

- **表达式函数未注册不再静默**：`plaita/io.py` 的 `$F.xxx(...)` 在函数未注册时改为 `logger.warning`（保留返回 `"undefined"` 以兼容 scoped registry 语义），拼写错误 / 沙箱漏注册可被日志捕捉。
- **`Flow.from_string` 不再吞 JSON 异常**：JSON 解析失败时保留原始异常作为 cause，避免被 YAML fallback 的次级报错淹没。
- **`Node` 基类收紧**：`execute(execution)` 改为必选参数（所有内置子类本就必选，仅 `http.py` 残留 `=None` 一并清理）；删掉从未被调用的 `_validate_input` 死占位；`validate` / `_validate_output` 作为真实扩展钩子保留并补 docstring。
- **logger 反模式修正**：`plaita/core/runner.py` / `callback.py` 的 `f"... {x}"` + `exc_info=True` 改为 `%` 延迟格式化；随后把 `plaita/server/**` 与 `plaita/event/memory.py` 下同类 `logger.error(f"... {e}", exc_info=True)` 一并清理（共 46 处），全仓不再有 f-string + exc_info 反模式。

### B 档：结构与并发

- **三 Strategy 共享单步推进**：抽 `_advance_one(flow, runner, callback_manager, node)` 原语，统一「run → 判 End → 解析 next」序列；`NormalStrategy` / `GeneratorStrategy` 复用之，`DistributedStrategy` 因 `EventNode` 挂起语义保留独立单步路径。新增 `Flow.is_end_node(node)`，`executor.py` 中散落的 `from plaita.node import End` 函数内导入全部消除（circular-import 带状消除到 `flow.py` 一处）。
- **同步节点线程池化**：`NodeRunner._run_sync_node` 从「每节点一个 daemon 线程」改为提交到模块级有界 `ThreadPoolExecutor`（`PLAITA_SYNC_NODE_POOL_SIZE` 可调，默认 32），`asyncio.wait_for` + `cancel_event` 协作取消语义不变。
- **`$NODE` 并发写**：经核查，并行分支经 `get_child_execution()` 拿到独立子 context（各自 `_context` dict），不共享父级 `$NODE`，故 `update_node_result` 无需加锁；一度尝试加 `threading.Lock` 反而破坏 process 模式 pickle（`test_concurrent`），已回退并注释说明。
- **分布式 resume enum 化**：新增 `ResumeType` 枚举（`CONTINUE/CANCEL/TIMEOUT/EVENT`）+ `ResumeType.coerce()`（接受 enum 或字符串，兼容旧调用方），取代 `executor.py` 中的裸字符串 magic string；`_handle_resume` 统一在入口 coerce，覆盖 `execute` 与历史 `_handle_resume_operation` 两条入口。结果 dict 的类型化（`is_end`/`is_suspend` 字段）属序列化边界，涉及 server/consumers/tests，本轮不动。
- **`Flow.start_node` 不再瞎猜入口**：移除「无显式 Start 且全员有入度时回退 `nodes[0]`」的猜测，改为抛 `FlowStartMissingError` 并提示如何修复（加 Start 节点或打破环）。

### Shim 退役计划

- `plaita/flow.py` / `plaita/errors.py` / `plaita/types.py` 三个兼容 shim 的 `DeprecationWarning` 加上明确移除版本 **0.6.0**。
- 仓库内部用法全部迁离 shim：`plaita/client.py`、`plaita/node/concurrent.py` 改从 `plaita.core.*` 导入；18 个非集成测试文件改从 `plaita.core.flow` / `executor` / `callback` / `errors` 导入。shim 现仅服务外部旧调用方，`tests/integration/test_backward_compat.py` 仍专门守护其可用性。

---

## 0.4.0 — 品牌统一 & API 清理

### 品牌

- **环境变量前缀统一**：控制台所有环境变量从 `LOKI_CONSOLE_*` 重命名为 `PLAITA_CONSOLE_*`；服务端环境变量从 `LOKI_*` 统一为 `PLAITA_*`（`REDIS_URL` / `QUEUE_NAME` 等通用名仍保留作为回退）
- **版本号对齐**：`plaita/__init__.py` 中 `__version__` 与 `pyproject.toml` 保持一致

### API

- **`run_distributed` 明确为首选分布式入口**：移除高层 `run(mode='distributed')` 与实例方法之间的行为差异文档警告；`run(mode='distributed')` 已统一路由到实例的 `run_distributed`，跨步骤保留回调**仍需复用同一实例**（见[执行模式文档](../guide/execution-modes.md)）

### 文档

- **DSL 文档重组**：`@flow` 升为首选 Python API，JSON/YAML 合为「配置文件格式」，S-expr 降为高级用法，Builder 明确定位为「动态流程修改 API」
- **reduce 节点 bug 修复**：子流程输入改为关键字参数 `first`/`second`（原为位置参数导致归约结果错误）；`initial` 判断改为 `is None` 以支持 `0`/`[]` 等 falsy 初始值
- **定位澄清**：plaita 定位为**通用逻辑编排运行时**，AI Agent 编排为典型应用场景之一

---

## 0.3.x（历史参考）

- 分层架构重构：`plaita.core` 执行核心层、依赖反转、三策略分离
- 引入 Checkpoint / 断点续执、事件系统、FlowWorker 与扩展节点
- 引入 `CodeNode` / `HTTP` / `Parallel` 等节点

> 完整历史：`git log --oneline v0.3.16`

---

## 历史详情（0.3.16）

### 架构

- **分层核心**：引入 `plaita.core` 执行核心层，`event` / `storage` / `server` 依赖 `core`，反向依赖被消除
- **依赖反转**：`core → event` 反向依赖通过 `async_utils` 下沉 + 默认 event bus provider 注入解除
- **facade 重构**：`FlowExecution` 精简为薄 facade，拆出 `ExecutionContext` / `NodeRunner` / `CallbackManager` + `Normal`/`Generator`/`Distributed` 三策略
- **分层约束测试**：新增 `test_layering` 强制 `foundation→event→storage→server` 依赖方向

### 节点与注册表

- **`NodeRegistry`**：全局 node dict 替换为作用域化 `NodeRegistry`，支持 `parent` 与 `entry_points` 插件发现
- **惰性节点 id 索引**：`Flow` 维护 `_node_index`，`find_node_by_id` 摊销 O(1)
- **惰性插件发现**：`get_default_registry()` 首次调用才扫描 entry_points，`import plaita.node` 不再拖入可选依赖

### 兼容与 API

- **顶层懒 re-export**：`from plaita import Flow, Node, FlowExecution, ...` 经 `__getattr__` 懒加载
- **统一 shim**：`plaita.errors` / `plaita.types` 弃用改为 lazy `__getattr__`，触发 `DeprecationWarning`
- **可操作 ImportError**：缺失 extra 时明确提示应安装哪个 extra

### 执行与可靠性

- **内核全异步**：同步 API 经 `async_utils` 桥接；同步节点跑 daemon 线程 + Future 桥接，超时 set `cancel_event` 协作取消
- **超时取严**：调用方/节点/流程 timeout 取更严者，不再静默丢弃
- **`strict_attrs`**：`FlowExecution` 未知属性写入可 fail-fast，避免拼写错误静默持久化
- **回调贯穿分布式**：`FlowWorker` 跨步骤保留用户回调
- **订阅异步化**：`_subscribe_event` await `register_subscription`，取代 fire-and-forget
- **事件表惰性创建**：fire-and-forget 建表改为 lazy awaited ensure

### 构建

- 迁移到 `pyproject.toml`，optional dependency groups + entry_points
- 注册 pytest marks，新增 `lint` extra，覆盖率配置排除需真实基础设施的后端

## 0.3.x 之前

- 引入 Checkpoint / 断点续执、事件系统、FlowWorker 与扩展节点（delay/queue/http_callback/approval）
- 引入 `CodeNode` / `HTTP` / `Parallel` 等节点

> 如需更早历史，运行 `git log --oneline` 查看。
