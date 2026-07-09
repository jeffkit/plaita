# 更新日志

本页记录 **v0.4.0 及以后**的正式变更历史，按版本倒序排列。  
0.4.0 之前的历史可通过 `git log --oneline` 查阅仓库完整提交记录。

## 未发布 — Runtime & Harness Review 修复

### 安全模型

- **移除不完备的 `$ENV` 敏感前缀黑名单**：`_SENSITIVE_ENV_PREFIXES` 用 `startswith` 匹配，挡不住 vendor 前缀的真实密钥名（`OPENAI_API_KEY` / `STRIPE_KEY` / `PG_CONN` 等），是「挡不住却给人安全感」的纸糊防御。改为 allowlist 命中即暴露、打 `logger.warning` 做审计可见性，**allowlist 即用户责任**。`tests/unit/test_context.py` 的 env 契约测试同步改写。
- **`_safe_environment` docstring 同步**：删除「second defense layer」表述，写明 warn 仅做审计。

### 运行时正确性

- **`_get_attr` 根因修复**（`plaita/core/expression_parser.py`）：dict-like 对象（含 live `CheckpointState`）优先走 `.get(path)`，而非 `getattr`。`$INPUT` 是 storage key 不是 Python 属性，旧行为导致 live `CheckpointState` 无法解析 `$PARENT.$INPUT.<field>`，靠 `setup_flow` 把 `$PARENT` 拍成 plain dict 绕过。根因修复后两路都通；plain-dict 快照保留用于 checkpoint 序列化，注释更新为真实职责。新增直接单测。
- **`cancel_event` 进程内传播**：子 `ExecutionContext` 共享父的 `Event`（`__init__` / `child()`），`clean()` 时根上下文重建、子上下文重新同步到父当前 `Event`，使父 flow 取消能传到进程内子 flow / 并行分支（thread 模式）。跨进程不传播的语义不变。新增 `TestExecutionContextCancelPropagation`。
- **`_coerce_input_value` 删死参数 `in_format`**：签名声明但函数体未用，移除并更新调用点。
- **`_resolve_default_event_bus` 收窄 except**：`ImportError` 单独 warning，其它异常仍兜底并保留 traceback。
- **`List` import 补齐**：`context.py` 顶部 `typing` import 缺 `List`（靠 `from __future__ import annotations` 没运行时炸，但静态检查会报）。

### LLM harness 工具化（`plaita-ai/tests/llm/`）

- **删 `.zshrc` 读取**：`_resolve_deepseek_key` → `_resolve_api_key`，只认 env（`PLAITA_LLM_API_KEY` 优先，`DEEPSEEK_API_KEY` 回退）；`conftest.py` 同步，无 key 则 skip `@pytest.mark.llm`。
- **去 `sys.path` hack**：新增 `_bench_tasks.py` 用 `importlib` 显式加载 `agent-benchmark/tasks.py`（目录名含连字符，无法作为包导入），不再 `sys.path.insert` 相对路径。
- **ReAct source 提取去脆弱**：优先 `plaita_run_flow`、回退 `plaita_compile_flow`、再回退从 `AIMessage.content` 解析 fenced `@flow` 块；提取失败打 warning 而非静默 `""`。
- **`input_fields_hint` 用全部用例 keys 并集**，避免异构用例误导 planner。
- **instruction 外置 + 版本化**：`prompts.py` 定义 `FOT_INSTRUCTION` / `REACT_INSTRUCTION` 与 `PROMPT_VERSION`，结果带 `prompt_version`。
- **`select_tasks` 支持 `task_ids` 过滤**；`run_react_task` 类型注解对齐 `ToolLike`。
- **新增 `runner.py` benchmark 入口**：`run_benchmark(agent, task_ids, ..., seed=0)` 落盘 `<out_dir>/<timestamp>_<agent>_<model>.json` + `summary.json`，控制台打印汇总表，失败任务摘要进 stderr。结果含 `seed` / `model` / `provider` / `base_url` / `prompt_version` 保证可复现可比。`plaita-ai llm-benchmark` CLI 子命令转调之（dev tool，需从 checkout 运行）。
- **`plaita-ai/pyproject.toml`** 新增 `anthropic` optional extra（`langchain-anthropic>=0.3`），并入 `all`。

---

## 未发布 — 架构止血三件套

### 包结构

- **演示脚本与伴生文档移出 wheel**：`plaita/event/demo_eventbus.py`、`plaita/event/timeout_example.py`、`plaita/server/extended_nodes_demo.py`、`plaita/server/test_event.json` 及各 `.md` 迁至 `examples/` 与 `docs/`；`pyproject.toml` 新增 `[tool.setuptools.exclude-package-data]` 兜底，防止 `.md`/`.json`/demo 类文件再混入发布包。导入改为绝对路径，`demo_eventbus` 不再玩 `sys.path` 黑魔法。

### Facade

- **移除 `FlowExecution` 的 `__getattr__` / `__setattr__` 魔法**：context 字段（`context` / `execution_id` / `event_bus` / `cancel_event` / `express_*`）改为具名 property，state 访问改为显式 delegate 方法。未声明的属性就是普通实例属性，拼写错误不再静默落进 context state。`strict_attrs` 开关与 `trigger_*` → `on_*` 映射一并移除（后者无调用方）；要手动触发回调请直接用 `execution.callback_manager.on_xxx(...)`。

### 错误模型

- **`FlowExecutionException` 子类体系**：新增 `NodeNotFoundError` / `FlowStartMissingError` / `NodeExecutionError` / `NodeTimeoutError` / `FlowTimeoutError` / `FlowErrorException` / `ErrorResultException` / `ResumeError`，各自携带默认 `code` / `error_type`。调用方只需提供 `message` 与 `node`，不必再往调用点塞 `-500` / `-520` / `-1` 等魔法数字。基类 `(code, message, error_type, node)` 位置签名保留以兼容历史调用方；`run_distributed` 边界仍按历史契约归一化为 `FLOW_ERROR / -500`。

### 测试

- `tests/unit/test_facade_attr_proxy.py` 改为验证显式委托行为（无 magic、未知属性不污染 context、`execution_id` 只读）。
- 顺手把 `ExecutionContext` 拆出 `_safe_environment` / `_coerce_input_value` 两个模块级纯函数。
- **SC-003 改为软/硬预算**：软 200（告警）/ 硬 400（失败）。`FlowExecution` 显式委托允许超过软预算；禁止为过软门再拆无意义 mixin。`ci-gate` 与 e2e 同步。

---

## 未发布 — 第二轮清理（毒点 triage A/B 档）

### A 档：低风险清理

- **表达式函数未注册不再静默**：`plaita/io.py` 的 `$F.xxx(...)` 在函数未注册时改为 `logger.warning`（保留返回 `"undefined"` 以兼容 scoped registry 语义），拼写错误 / 沙箱漏注册可被日志捕捉。
- **`Flow.from_string` 不再吞 JSON 异常**：JSON 解析失败时保留原始异常作为 cause，避免被 YAML fallback 的次级报错淹没。
- **`Node` 基类收紧**：`execute(execution)` 改为必选参数（所有内置子类本就必选，仅 `http.py` 残留 `=None` 一并清理）；删掉从未被调用的 `_validate_input` 死占位；`validate` / `_validate_output` 作为真实扩展钩子保留并补 docstring。
- **logger 反模式修正**：`plaita/core/runner.py` / `callback.py` 的 `f"... {x}"` + `exc_info=True` 改为 `%` 延迟格式化；随后把 `plaita/server/**` 与 `plaita/event/memory.py` 下同类 `logger.error(f"... {e}", exc_info=True)` 一并清理（共 46 处），全仓不再有 f-string + exc_info 反模式。

### C2：`@flow` 表达式语义边界

- **编译错误可读化**：`plaita/dsl/codeflow.py` 最后一处 `ast.dump(node)` 报错改为可读调用名（`_describe_call`）；`_compile_expr` 的通用 fallback 不再抛裸类型名，改为带「@flow 只支持…详见 code-dsl 文档」的提示。
- **常见 Python 写法给重写提示**：新增 `_FOOTGUN_HINTS` 表，f-string / 三元 / 列表-集合-字典推导式 / `lambda` / `await` / 海象 / `*args` 解包 / 集合字面量等不支持写法各自给出可操作重写建议（如 f-string → `F.concat(...)`）。这些写法本就会编译失败，加提示是纯 DX 提升，不改任何成功编译的语义。
- **语义边界文档化**：`docs-site/docs/guide/code-dsl.md` 新增「表达式语义边界」小节，列全支持写法与精确语义、`str → $F.concat` 的近似映射、`+` 多态陷阱（编译期不查类型），以及不支持写法清单；编译期校验表与「已知边界」同步更新。
- **顺手补漏**：迁掉 `plaita/node/child.py` 残留的 `from ..flow import Flow`（相对导入 shim），仓库内部不再有 shim 用法。
- **未动**：`_BUILTIN_TO_F` / `_BINOP_TO_F` 的类型推断（让 `str→concat`、`+` 多态在编译期就按类型拒绝/允许）会改变现有可用流程的语义，属产品决策，本轮不动，已在文档显式标注为已知边界。

### C3：循环依赖带状消除

- **core → node.event_node 反向依赖切断**（真正的分层违规）：给 `Node` 基类加 `is_suspending: ClassVar[bool] = False` 标志与多态 `resume(execution, resume_type, resume_data)` 方法；`EventNode` 置 `is_suspending = True` 并实现 `resume()`（内部按 `ResumeType` 分发 `on_cancel/on_timeout/on_event`）。`DistributedStrategy._execute_current_node` 改问 `current_node.is_suspending`，`_handle_resume` 改调 `current_node.resume(...)`——`plaita/core/executor.py` 中 3 处 `from plaita.node.event_node import EventNode` 函数内导入（含 1 处 B1 后遗留的死导入）全部消除，内核不再 `isinstance` 具体节点类型。
- **flow ↔ executor 伪 band-aid 提到顶部**：核查发现 `executor.py` 顶部并不反引 `flow`（`Flow` 仅作运行时鸭子类型参数），`runner.py` 运行时只引 `core.errors`，`callback/context/expression` 均不反引 flow——**这条根本不是真环**，`flow.py` 里 4 处 `from plaita.core.executor import FlowExecution` 函数内导入是历史遗留的伪 band-aid。全部提到模块顶部坦诚声明，`Flow.run/arun/debug` 与 `parse_and_run` 不再藏 import。
- **保留**：`flow.py` 顶部 `from plaita.node import End, Node, Start` 是 core→node 的真实依赖（node→core.errors 是 DAG 无环），且 `is_end_node` 已把 End 判断收口到 `flow.py` 一处，故保留。

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

## 未发布 — C1：表达式引擎统一为单套缓存文法

### 架构

- **表达式引擎双轨制消除**：移除 `plaita/io.py` 求值路径上的「正则粗筛 + pyparsing 精解」双轨制。新增 `plaita/core/expression_parser.py` 的 `ExpressionParser`，一套按 prefix 构建并缓存的 pyparsing 文法覆盖字面量 / 变量路径 / `$F.func(...)` 函数调用 / `{% ... %}` 插值；`evaluate` / `parse_function` 退化为薄包装。旧引擎每次调用都 `pp.Forward()` + `setParseAction` 重建 `function_call` 规则的问题随之消除，`io.py` 净减约 140 行。
- **求值语义锁定**：新增 `tests/unit/test_expression_golden.py`（45 条）逐项锁行为——根变量缺失 key 仍抛 `KeyError`、中段字段以父对象为 context 递归 `evaluate`（含 `$PARENT.$INPUT.name` 这种段名带 `$` 的旧语法）、`[n]`/`.n` 段直接索引不递归、未知函数回退 `"undefined"`、`{% ... %}` 仅当内部以 prefix 开头才触发。全量套件 835 passed 无回归。
- **保留 A 档行为**：函数未注册的 `logger.warning`（A 档引入）随 `_eval_function_call` 搬入 `expression_parser.py`，拼写错误 / 沙箱漏注册仍可被日志捕捉。

### 顺带修复的边角

- 非根位置的负索引（如 `$INPUT.names[-1]`）现在能正确取值（旧 `get_attr` 正则只认非负索引，此场景返回 `None`）。
- `$F.now()` 等零参函数调用现在可解析（旧 `delimitedList` 要求至少一个参数）。

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
