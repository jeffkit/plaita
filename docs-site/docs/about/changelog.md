# 更新日志

本页记录 **v0.4.0 及以后**的正式变更历史，按版本倒序排列。  
0.4.0 之前的历史可通过 `git log --oneline` 查阅仓库完整提交记录。

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
