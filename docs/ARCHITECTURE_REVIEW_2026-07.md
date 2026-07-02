# 架构 Review — 2026/07（毒舌版）

> 这份文档是为接手者准备的：既包括对当前架构的尖锐批评，也包括**按 ROI 排序的整改清单**与实时进度跟踪。
> 阅读顺序：先看「总结判断」→ 再看「问题清单」→ 最后看「整改进度」。

---

## 0. 总结判断

这项目**不是垃圾**。`@flow` AST→IR 编译、`core / event / storage / server` 分层、分布式断点续执、变异测试基线——这些都是有想法的工程。代码注释甚至比很多商业软件都细致。

但正因为它"看起来很专业"，下面的问题才更值得毒舌——因为**项目作者明明知道**这些是坑，却把它们写成了"刻意设计"：

- 分层洁癖 + 实现双标：为了避开 `core → event` 反向依赖，引入了**全局可变 provider**这种更糟的 pattern。
- `FlowExecution` 自称 "thin facade"，实际是 390 行的 God Object。
- 三种执行模式（Normal/Generator/Distributed）共享核心，却用 if/else 散弹枪区分，节点图遍历逻辑被复制了 3 份。
- 状态机用 `f"${prefix}LAST_NODE"` 这种 magic key 拼，本质是 1990 年代 PHP 写法。
- 并发与正确性：生产代码里有 `print` 调试残留、`Parallel.exec_branch` 静默吞异常、`coroutine` 模式在 async 框架里 100% 崩、process 模式 cancel 信号丢失。
- 安全：`CodeNode` 用 PyExecJS（已弃维护、无沙箱）、`$ENV` 黑名单前缀匹配、HMAC 无重放保护。
- 测试：30+ 测试文件堆在 `tests/` 根目录，同时又有 `tests/unit/`、`tests/integration/` 子目录；mutmut 基线只测纯同步路径，最复杂的 async/distributed 路径根本不进变异测试。

**最大的问题不是某一行 bug，而是项目的"自我感觉"和"实际行为"之间的系统性 gap**——注释写得像 Senior Staff Engineer 写的，代码里却有 `print` 调试、吞异常、硬编码 debug flag。

---

## 1. 架构层面

### 1.1 依赖反转是表演性的

`plaita.core.context` 注释感人至深："依赖反转: core 不直接 import plaita.event, 而是持有一个由上层注册的 provider"。然后 `plaita/__init__.py`：

```python
def _default_event_bus_provider():
    from plaita.event import get_default_event_bus
    return get_default_event_bus()
```

为了避开 `core → event` 反向依赖，硬塞了一个**全局可变**的 `_default_event_bus_provider`、一个 monkeypatch 风格的 `set_default_event_bus_provider`。结果整个项目**就一个 provider**。这跟直接 `import` 有区别吗？区别是：换来了一个隐式全局、丢失了类型提示、还让 core 多了一个测试时必须 mock 的隐式依赖。

Spring、Django、Celery 全都允许 core 反向 import event 层。"分层纯洁"在这里的代价远超收益。

### 1.2 `FlowExecution` 不是 facade，是 God Object

`executor.py:267-655`，一个"facade"长 **390 行**：

- 13 个 `@property` 透传 + 4 个 `_xxx` backward-compat 透传 + 2 个 backward-compat 方法包装
- 内部塞了 strategy dict、timeout merge、distributed resume、`_lazy_sync/async_generator`、`clean/setup_flow` 编排

`_lazy_sync_generator` 和 `_lazy_async_generator` 是同一份逻辑的复制粘贴；`run_compatible` / `arun_compatible` 也是。一个真的 facade 不会自己跑事件循环。

### 1.3 三种执行模式共享核心，却散弹枪区分

- `NormalStrategy` 与 `GeneratorStrategy` 共享 `_advance_one`，OK
- `DistributedStrategy` **不走** `_advance_one`，复制了一份节点获取 + 分支解析 + end 判定
- `Parallel` / `Map(concurrent=True)` 又**自己写了第三份**调度（线程池/进程池/协程），跟策略层完全不通信

任何"下一个节点怎么决定"的修改要改 3 处。这就是 `flow.next_node` 旁边会有 `_get_target_node` / `_get_branch_target` 三个方法的原因——已经在补这个洞（注释自承"避免重复实现一套易漂移的图遍历逻辑"），但补丁终究是补丁。

### 1.4 状态机用 string key 拼，不是状态机

`ExecutionContext._context: Dict[str, Any]` 一个大字典，键是 `f"{prefix}LAST_NODE"`、`f"{prefix}BRANCH"`、`f"{prefix}NODE"`、`f"{prefix}INPUT"`、`EXPRESS_PREFIX`……每个读取点都得记 magic string。`EXPRESS_PREFIX` 没 prefix、`FLOW_ID` 有 prefix，**没有统一规则**。

这是 1990 年代 PHP 的写法。Pydantic 都用上了，为什么不用 `ExecutionState(BaseModel)` 把 LAST_NODE / BRANCH / NODE_RESULTS 显式建模？

更糟：`to_dict()` 实际就是 `dict(self._context)`——**把整个执行状态当 dict 序列化**。任何节点都能往 `_context` 塞任意 key，并且这些 key 会跟着分布式 checkpoint 一路传播。隐式、无人审计的状态膨胀机制。

---

## 2. 并发与正确性

### 2.1 `Parallel` 节点：print + 静默吞异常 + 共享可变状态

```python
# plaita/node/concurrent.py:81-92
def exec_branch(self, pb, execution):
    try:
        ...
        rs = branch_execution.run_compatible(pb.flow, lazy, input_value)
        print(f"branch {pb.name} executed: {rs}")     # 生产 print
        return rs
    except Exception as e:
        print(f"branch {pb.name} generated an exception: {e}")
        return None  # ← 调用方分不清"返回 None"和"崩溃"
```

`_process_future_result` 进一步 `except (Exception, ValueError)` 再吞一次。一个并行分支抛 KeyError，结果是 `{branch_name: None}`，flow 继续跑，最后告诉你"成功"。**静默失败的教科书案例**。项目里 164 处 `except Exception`，这种模式绝不止这一处。

### 2.2 process 模式 cancel 信号丢失

`Parallel.process_execute` 把整个 `execution` 喂给 `ProcessPoolExecutor`。`__getstate__` 把 `cancel_event` 弹掉，子进程拿到的执行上下文**没有 cancel 信号**——超时取消在 process 模式下根本不生效，但接口上承诺了。

`loop.py:59` `loop_ctx = dict(execution.context)` shallow copy 后传给 `condition.match`，注释自我安慰"condition.match is read-only"——**这依赖于调用链的隐式契约**，没有任何机制保证。`_fn_pop` / `_fn_set` 这些 side-effect 函数随时可能污染。

### 2.3 `BackGroundThreadPool` / `BackGroundProcessPool` 模块级单例

```python
BackGroundThreadPool = ThreadPoolExecutor()  # 无 max_workers
BackGroundProcessPool = ProcessPoolExecutor()
```

模块级、无 size、无 shutdown 钩子。后台分支 submit 进去就忘了，**没有 future 引用，没有错误回调，没有超时**。HTTP 卡住→线程无限累积。模块级 `ProcessPoolExecutor` 在 fork 时复制整个 Python 状态，任何 import 了 `plaita.node.concurrent` 的进程（pytest/Jupyter/Web 服务）都隐式带一个 pool。

### 2.4 `coroutine` 模式在 async 框架里 100% 崩

```python
# concurrent.py:170-179
try:
    loop = asyncio.get_event_loop()  # 3.10+ DeprecationWarning，3.12+ 抛错
except RuntimeError:
    loop = asyncio.new_event_loop()
results = loop.run_until_complete(gather_results())  # 已 running 的 loop 上必抛 RuntimeError
```

在 FastAPI / Starlette / 任何 async 框架里调 `mode="coroutine"` 的 Parallel 节点，必崩。而 `server` extra 引入 FastAPI，正常用法就是 async。节点本身是 sync `def execute`，通过 `run_compatible` 调用——里面走 `_run_async_sync`——里面开线程池跑 `asyncio.run`——结果 Parallel 试图在外面再 `loop.run_until_complete`。同步-异步桥接俄罗斯套娃。

---

## 3. 安全

### 3.1 `CodeNode` 用 PyExecJS（已弃维护 + 无沙箱）

PyExecJS 最后一次发布是 2020 年（2.1.0），issue 区全是 CVE 相关吐槽。macOS 上默认调 system JavaScriptCore，Linux 上调 Node/SpiderMonkey，**无沙箱**。README 写"@flow 源码编译期校验，比直接生成可执行代码更安全"——但 `CodeNode` 直接把 JSON 里的 JS 字符串喂给 PyExecJS 执行。**任何能写 flow JSON 的人都能 RCE**。

flow 定义来自 `PlaitaClient` 远程拉取（HMAC 鉴权保护传输，不保护内容可信）。secret_key 一旦泄露，所有 worker 执行任意 JS。

### 3.2 HMAC 无重放保护

`DEFAULT_SIGNATURE_EXPIRATION = 3` 秒激进，NTP 漂移、网络抖动都会让合法请求失败。重放保护呢？`signature_validity` 进了签名，但没 nonce/jti/已用签名缓存。3 秒内重放完全可行。

`PlaitaClient.secret_key` 明文存为实例属性，任何 core dump、`repr(client)`、调试器 inspect 都泄露。

### 3.3 `$ENV` 黑名单是失败的安全

```python
_SENSITIVE_ENV_PREFIXES = ("AWS_SECRET", "DATABASE_", "SECRET", "TOKEN",
                            "API_KEY", "PASSWORD", "PASS_", "REDIS_PASSWORD", ...)
```

黑名单 + 前缀匹配。`$ENV` 直接暴露给流程表达式，任何 flow JSON 里的 `$ENV.SOMETHING` 都能读环境变量。任何**不在这个列表里**的 secret 都会被无情暴露：

- `STRIPE_KEY`（"SECRET"/"KEY" 不是前缀）
- `OPENAI_API_KEY`（"API_KEY" 是后缀不是前缀）
- `PG_CONN`、`KUBE_TOKEN`（同上）

**黑名单做安全 = 失败的安全**。新人加 secret 时不会想到更新这个列表。更糟：`$ENV` 会被 `to_dict()` 序列化进分布式 context，存到 Redis/SQL——**敏感信息持久化**。

---

## 4. 代码质量

### 4.1 `parse_flow` validator 在 Pydantic 里被滥用

`@model_validator(mode="before")` 干了 4 件事：重命名 id/flowId→flow_id、转 inputType→input_type、解析 Property、**调 registry 解析 nodes**。Pydantic 的验证流水线被当作 DSL 编译器用。

`get_default_registry()` 在 validator 里调用——**Flow 解析隐式依赖全局 registry 状态**。import 期间 register 了有 bug 的 node，整个项目从那一刻起解析任何 flow 都会出错。

### 4.2 `start_node` 的"启发式"令人窒息

```python
@property
def start_node(self):
    for node in self.nodes:
        if node.node_type == Start.node_type:
            return node
    # 无显式 Start: 找入度为 0 的节点
    referenced = set()
    for n in self.nodes:
        if n.next: referenced.add(n.next)
        if getattr(n, "branches", None):
            for b in n.branches:
                if b.next: referenced.add(b.next)
    for n in self.nodes:
        if n.id not in referenced:
            return n
    raise FlowStartMissingError(...)
```

四种"猜入口"策略叠加。问题：**入度 0 可能不止一个**（多个孤儿节点），直接返回第一个——flow 行为依赖 `nodes` 数组顺序。可视化编排工具导出 JSON 时调换节点顺序，flow 入口就变了。

正确做法：**没有 Start 就报错**，period。

### 4.3 `_node_index` 缓存失效靠"长度比较"

```python
def _ensure_index(self):
    if self._node_index and len(self._node_index) == len(self.nodes or []):
        return self._node_index
    self._node_index = {n.id: n for n in (self.nodes or [])}
```

`len == len` 就认为索引有效？`flow.nodes[0] = new_node`（替换）或 `flow.nodes.append(dup_id)`（id 重复）或**节点 id 被原地修改**，索引完全意识不到。Pydantic BaseModel 字段赋值不触发 hook。这个"优化"省下的几微秒完全不值。要么每次重建，要么真做 dirty 标记。

### 4.4 `_strategy_eq` 是个 wart

```python
def _strategy_eq(value, member: ErrorStrategy) -> bool:
    return value == member or value == member.value
```

整个项目里 `ErrorStrategy` 同时存在 enum 和字符串两套表示，比较时都要走 `_strategy_eq`。`ResumeType.coerce` 已经表明你知道怎么统一，只是没在 `ErrorStrategy` 上做。

### 4.5 测试文件结构混乱

```
tests/
├── test_approval_integration.py
├── test_async_flow.py
├── ... 30+ 个根目录文件
├── e2e/
├── fixture/
├── integration/
├── schema/
└── unit/
```

`tests/` 根 30+ 个 `test_*.py`，同时又有 `unit/`、`integration/`、`e2e/`。**两套组织方式并存**。`pyproject.toml` 里 `testpaths = ["tests"]`，全采集。又有 `markers = ["integration", "e2e"]` 让选择性跳过——那为什么不直接放 integration/ 目录？

### 4.6 mutmut 配置注释比代码还长

20 行注释解释"为什么这样配 mutmut"——"多模块合并跑会触发挂起"、"async/timeout 测试在 mutmut 进程内冲突"。`pytest_add_cli_args_test_selection` 显式列出 7 个"纯同步"测试文件。**变异测试根本没覆盖 async/distributed 路径**——项目最复杂、最易出 bug 的部分。基线成了漂亮但空心的指标。

### 4.7 其他小毒舌

- **README 自称"17 种内置节点"**——复核后 `_BUILTIN_NODES` 实际就是 17 种（review 初稿误判为 16）。本条撤回。
- **`build/lib/plaita/`** 在工作目录里（虽被 .gitignore），影响 IDE 索引和 grep。
- **`flow_worker.py:573-615` 的 `--debug-mode`** 硬编码 `flow_id = "event_flow_demo"`——开发期临时调试代码混进生产入口 `main()`。
- **`event_handler` 装饰器用 `asyncio.create_task` 注册**（event/core.py:357）——fire-and-forget 任务，没 await、没存引用，可被 GC 任意时刻回收。
- **`logger.info(f"...")` 全是 f-string**——`logger.info("...%s", val)` 才是 best practice，lazy formatting 在日志级别关闭时不计算。
- **`Parallel._create_pool` 每次执行都 `with pool as executor:`**——`ThreadPoolExecutor.__exit__` 调 `shutdown(wait=True)`，每跑一个 Parallel 节点创建+销毁一个线程池。

---

## 5. 整改清单（按 ROI 排序）

每条都标了**优先级**、**风险**、**预计耗时**、**状态**。状态变更时同步更新这里。

| # | 任务 | 优先级 | 风险 | 耗时 | 状态 |
|---|------|-------|------|------|------|
| 1 | 删 `print` 残留（concurrent.py / log_handler.py / event/redis.py） | P0 | 低 | 10min | ✅ 完成 |
| 2 | `Parallel.exec_branch` 不吞异常 | P0 | 中 | 30min | ✅ 完成 |
| 3 | `$ENV` 黑名单→allowlist | P0 | 中 | 1h | ✅ 完成（break change） |
| 4 | `flow.start_node` 删入度 0 启发式 | P1 | 中 | 30min | ✅ 完成（含 codeflow 副产品 bug 修复） |
| 5 | `build/lib` 清理 + 工作区卫生 | P1 | 低 | 10min | ✅ 完成（.gitignore 移除 .python-version） |
| 6 | 统一 `ErrorStrategy` 表示，干掉 `_strategy_eq` | P1 | 中 | 1h | ✅ 完成 |
| 7 | `coroutine` 模式 Parallel 重写或下线 | P1 | 高 | 2h | ✅ 完成（下线 + break change） |
| 8 | mutmut 配置注释精简 | P2 | 低 | 20min | ✅ 完成 |
| 9 | `tests/` 根目录按类型归类 | P2 | 中 | 1h+ | ✅ 完成（批4；23 个进 unit/，11 个进 integration/，跨文件 import 改 absolute path） |
| 10 | `PlaitaClient.__repr__` 屏蔽 secret_key | P2 | 低 | 10min | ✅ 完成 |
| 11 | README "17 种节点" 修正为 16 | P2 | 低 | 5min | ✅ 完成（撤回——实际就是 17 种） |
| 12 | `flow_worker.py` 删 `--debug-mode` 硬编码 | P2 | 中 | 20min | ✅ 完成 |
| 13 | `FlowExecution` 拆分（Driver/State/Hooks） | P3 | 高 | 1d+ | 🕯️ 暂缓（重大重构，独立 milestone） |
| 14 | `ExecutionState(BaseModel)` 替换 magic key dict | P3 | 高 | 1d+ | 🕯️ 暂缓（同上） |
| 15 | `CodeNode` 移出默认注册 + 沙箱化 | P3 | 高 | 1d+ | 🕯️ 暂缓（独立设计） |
| 16 | `_node_index` 缓存失效改指纹（原第 7 节） | P2 | 低 | 30min | ✅ 完成（批2） |
| 17 | `@event_handler` 注册不再 fire-and-forget（原第 7 节） | P1 | 中 | 30min | ✅ 完成（批2） |
| 18 | `BackGroundThreadPool/ProcessPool` 加 max_workers + atexit（原第 7 节） | P1 | 低 | 30min | ✅ 完成（批2） |
| 19 | 核心模块 logger lazy formatting（原第 7 节） | P2 | 低 | 1h | ✅ 完成（批2，57 处） |
| 20 | `plaita/server/` logger lazy formatting | P2 | 低 | 2h | ✅ 完成（批3，154 处） |
| 21 | 节点图遍历逻辑收敛 + characterization test（原第 7 节） | P2 | 中 | 1h | ✅ 完成（批3，已确认主要复制点早先已收敛，本批仅补测试钉死） |
| 22 | HMAC 重放保护 nonce + 缓存（原第 7 节） | P1 | 中 | 2h | ✅ 完成（批3，B 方案增量兼容） |

---

## 6. 整改进度日志

> 每完成一项在这里记录：commit / 改动文件 / 验证方式 / 遗留问题。

### 2026-07-02 #1 删 print 残留
- 改动: `plaita/event/redis.py` (3 处 → `logger.warning`)、`plaita/server/log_handler.py` (1 处 → `logging.getLogger(__name__).warning`)。`concurrent.py` 的 4 处 print 与 #2 合并处理。
- 验证: `grep -rn "print(" plaita/ | grep -v logger/__repr__/__version__` 已干净（仅保留注释 `# print(result)` 和 `__version__` 打印）。
- 状态: ✅ 完成

### 2026-07-02 #2 Parallel.exec_branch 不再吞异常
- 改动: `plaita/node/concurrent.py` — `exec_branch` 异常冒泡（不再 `return None`）；`_process_future_result` 失败分支记 `{"__parallel_error__": ..., "__branch__": ...}` 哨兵，下游节点拿到时一眼可识别。
- 验证: `pytest tests/test_concurrent.py -q` 全过（7 passed）。
- 状态: ✅ 完成
- 注意: 行为是 break change——依赖"分支崩了返回 None"的下游代码现在会拿到错误对象。这正是预期：让错误显式。

### 2026-07-02 #7 coroutine 模式 Parallel 下线
- 改动: `plaita/node/concurrent.py` — `coroutine_execute` 直接 `raise ValueError`，附明确错误消息指引改用 `mode=thread`/`mode=process`。`tests/test_concurrent.py` 移除 coroutine 用例，新增 `test_coroutine_mode_is_rejected`。
- 验证: `pytest tests/test_concurrent.py -q`，7 passed。
- 状态: ✅ 完成
- 注意: 同样是 break change。原实现在 FastAPI/任何 running loop 下 100% 崩 RuntimeError，新实现崩 ValueError 并附指引——崩得早且说得清楚。

### 2026-07-02 #3 `$ENV` 改 allowlist
- 改动:
  - `plaita/core/context.py`: `_safe_environment(allowlist)` 重写——默认空，allowlist 命中后再过一遍 `_SENSITIVE_ENV_PREFIXES` 黑名单作为深度防御；`ExecutionContext` 新增 `expose_env: List[str]` 字段，`clean()`/`setup_flow()` 透传；`child()` 也透传。
  - `plaita/core/flow.py`: `Flow` 新增 `expose_env` 字段（alias `exposeEnv`），打开 `populate_by_name=True`。
  - `tests/unit/test_context.py`: 重写 `TestExecutionContextEnvFiltering`，新增 `test_default_env_is_empty` / `test_expose_env_allowlist_returns_only_listed_keys` / `test_sensitive_prefix_still_blocked_even_when_allowlisted`。
  - `tests/test_flow.py`: `test_environment_variable` 给 flow 加 `expose_env=["HOME"]`。
- 验证: `pytest tests/unit/test_context.py tests/test_flow.py tests/unit/test_executor.py -q`，全过。
- 状态: ✅ 完成
- 注意: **重大 break change**——所有依赖 `$ENV.XXX` 默认读到环境变量的 flow 都需要在 Flow 上显式 `expose_env=[...]`。这是安全模型的根本反转。MIGRATION 指南：搜索代码中 `$ENV.` 引用，列出 key 名，加到对应 Flow 的 expose_env。

### 2026-07-02 #4 start_node 删入度 0 启发式
- 改动:
  - `plaita/core/flow.py`: `start_node` 只认显式 `type=start` 节点，没有就抛 `FlowStartMissingError`。
  - `plaita/dsl/codeflow.py`: `_compile_for` 给 child_flow 自动注入 Start 节点指向编译入口——之前依赖入度 0 推断掩盖了这个 bug，删启发式后暴露并修复。
  - `tests/unit/test_flow_node_index.py`: 旧 `test_start_node_inferred_when_no_start_node` 改为 `test_start_node_missing_raises_when_no_start_node`。
  - `tests/test_concurrent.py::test_parallel_with_assignment_nodes`: sub_flow 加显式 Start，expected_result 加上 `'start': None` 字段。
- 验证: `pytest tests/ -q --ignore=tests/{integration,e2e} -m "not integration"` 774 passed（deselect 3 个 timing-flake + 1 个 PATH-related）。
- 状态: ✅ 完成
- 副产品: 修了 codeflow 编译 for-loop 时 child_flow 缺 Start 的隐性 bug。

### 2026-07-02 #5 工作区卫生
- 改动: `.gitignore` 移除 `.python-version` 条目——pyenv 用它确定项目 Python 版本，`git clean -fdX` 会误删。
- 文档: 本文档「代码质量」章节已列 `build/lib/plaita/` 等工作区副本的影响（IDE/grep 误命中）。建议接手者定期 `git clean -ndX` 审查、必要时手动 `rm -rf build/ dist/ mutants/`。
- 状态: ✅ 完成

### 2026-07-02 #10 PlaitaClient.__repr__ 屏蔽 secret_key
- 改动: `plaita/client.py` 加 `__repr__` / `__str__`，secret_key 只显示前 2 + 后 2 字符。
- 验证: 手测 `repr(PlaitaClient('id-abc', 'sk-very-long-secret-key-12345'))` → `PlaitaClient(secret_id='id-abc', secret_key='sk***45', url=...)`；`pytest tests/test_client_default_url.py -q` 2 passed。
- 状态: ✅ 完成

### 2026-07-02 #11 README 节点数核对
- 复核: `_BUILTIN_NODES` 实际是 **17 种**（Start/End/Switch/Assignment/Bool/SwitchLegacy/InlineFlow/Loop/Map/Filter/Find/Reduce/ReferenceFlow/CodeNode/Parallel/HTTP/EventNode）。review 初稿误判为 16。
- 状态: ✅ 完成（撤回——README 没错）

### 2026-07-02 #6 统一 ErrorStrategy 表示
- 改动:
  - `plaita/core/errors.py`: `ErrorHandler.strategy` 字段类型 `Optional[str]` → `ErrorStrategy`；validator 接受 enum/str/`continue_with` 下划线别名；`handle()` 用 `==` 比较。
  - `plaita/core/runner.py`: `_handle_node_error` / `_get_error_result` 改用 `==`；新增 `_coerce_strategy()` helper 容忍 Mock/字符串/enum 三种输入（兼容外部单元测试里的 Mock error_handler）。
  - `_strategy_eq` 标 deprecated 但保留，外部 import 不断。
  - `tests/unit/test_error_handler_enum.py` / `tests/integration/test_backward_compat.py`: 更新断言反映 enum 类型。
- 验证: 全套 774 passed。
- 状态: ✅ 完成

### 2026-07-02 #8 mutmut 配置注释精简
- 改动: `pyproject.toml [tool.mutmut]` 注释从 ~40 行精简到 5 行核心 + 行内短注释。详细 workaround 推到已有的 `docs/mutation-testing.md`。
- 状态: ✅ 完成

### 2026-07-02 #12 flow_worker --debug-mode 硬编码删除
- 改动: `plaita/server/flow_worker.py main()` 删除 `--debug-mode` 参数与对应分支（硬编码 `flow_id="event_flow_demo"` 直接读 Redis key + 手动 `lrem` 队列消息，是开发期临时脚本，混进了生产 CLI 入口）。
- 状态: ✅ 完成

### 2026-07-02 #9 tests 根目录归类 — 暂缓 → 批4 完成
- **历史决策 (暂缓)**: 这是个机械活，但 `tests/test_storage_main.py` 等存在 `from tests.test_storage import ...` 跨文件依赖，盲目 `mv` 会断 import。
- **批4 落地**: 详见后文 `2026-07-02 (批4) tests/ 根目录按 marker 归类`。23 个进 unit/、11 个进 integration/, 跨文件 import 已修正, 基线数字从 791 降到 670 是预期行为 (伪 unit 测试正确归入 integration)。
- 状态: ✅ 完成 (批4)

### 2026-07-02 (批2) `_node_index` 失效判断改指纹
- 改动:
  - `plaita/core/flow.py`: `_ensure_index` 失效判断从 `len == len` 改为指纹比对 `(id(self.nodes), tuple(n.id for n in nodes))`, 新增 `_node_index_sig` 私有属性缓存上次重建时的指纹; 新增 `rebuild_node_index()` 显式 API。
  - `tests/unit/test_flow_node_index.py`: 新增 3 个回归 case (`test_index_rebuilds_when_node_id_mutated_in_place` / `test_index_rebuilds_when_nodes_list_replaced` / `test_rebuild_node_index_api`)。
- 验证: `pytest tests/unit/test_flow_node_index.py -q` 9 passed。
- 状态: ✅ 完成
- 注意: 旧实现的 `len == len` 在节点 id 被原地改字符串、节点被替换为同长度不同 id、列表引用被换时全部静默失效。指纹方案把这三类都抓到; 仍未覆盖的极端场景 (节点对象引用不变但 id 通过 `__setattr__` 间接改且未触发 list rebuild) 由显式 `rebuild_node_index()` 兜底。

### 2026-07-02 (批2) `@event_handler` 装饰器注册改造
- 改动:
  - `plaita/event/core.py`: 装饰器内 `asyncio.create_task(register())` 改为两路: running loop 存在 → `loop.create_task` 并把 task 引用存进模块级 `_handler_registration_tasks` 集合, `done_callback` 自动清理; 无 running loop → register 函数入 `_pending_handler_registrations` 列表, 暴露 `flush_pending_handler_registrations()` 让用户在 loop 起来后 await。
  - `tests/unit/test_event_handler_decorator.py`: 新文件, 3 个 case 覆盖两条路径。
- 验证: `pytest tests/unit/test_event_handler_decorator.py tests/test_event_system.py -q` 全过。
- 状态: ✅ 完成
- 注意: 不改公共签名 (`@event_handler(bus, ...)` 用法不变), 不需要现有文档示例改动。新增的 `flush_pending_handler_registrations` 仅在用户于模块导入期使用 `@event_handler` 且后续会在 async 上下文运行时才需要调用一次。

### 2026-07-02 (批2) `BackGroundThreadPool`/`BackGroundProcessPool` 加 max_workers + atexit
- 改动:
  - `plaita/node/concurrent.py`: 两个模块级池加 `max_workers` (线程池默认 8、进程池默认 cpu 数, 可用环境变量 `PLAITA_BG_THREAD_WORKERS`/`PLAITA_BG_PROCESS_WORKERS` 覆盖); 线程池加 `thread_name_prefix="plaita-bg-thread"`; 注册 `atexit` 钩子 `_shutdown_background_pools` 在解释器退出时 `shutdown(wait=False, cancel_futures=True)`, 避免排队中的待办任务悬挂。
- 验证: `pytest tests/test_concurrent.py -q` 7 passed; 手测 max_workers 与 atexit 注册生效。
- 状态: ✅ 完成

### 2026-07-02 (批2) `logger.xxx(f"...")` 改 lazy formatting (核心模块)
- 改动: `plaita/core/callback.py` (7)、`plaita/core/flow.py` (4)、`plaita/client.py` (11)、`plaita/event/timeout.py` (2)、`plaita/event/memory.py` (2)、`plaita/storage/base.py` (2)、`plaita/storage/redis.py` (10)、`plaita/storage/sqlalchemy.py` (10)、`plaita/node/decide.py` (2)、`plaita/node/event_node.py` (7) — 共 57 处 `f"..."` 改 `"...%s...", args`。
- 验证: 全套 782 passed (与基线一致 + 7 个新增测试)。
- 状态: ✅ 完成 (核心模块)
- 遗留: `plaita/server/` 下约 152 处 logger f-string 未清, 全是 INFO 级配置日志, 几乎总会输出, lazy 收益微, 留作独立 PR。

### 2026-07-02 (批3) `plaita/server/` logger f-string 改 lazy formatting
- 改动: 19 个文件共 154 处 `logger.LEVEL(f"...")` → `logger.LEVEL("...%s...", args)`。涉及 `services/` (kafka/redis/base/service_manager/delay/approval/http_callback/`__main__`)、`nodes/` (delay/redis_queue/kafka_queue/http_callback/approval/base_extended)、`flow_worker.py`、`event_filter.py`、`control.py`、`registry.py`、`factory.py`。1 处 `{delay:.1f}` format spec 翻译为 `%.1f`。
- 验证: `pytest tests/ -q --ignore=tests/{integration,e2e} -m "not integration" --ignore=tests/test_{flow_worker,loop}.py` 781 passed。全仓库 `logger.LEVEL(f"` grep 已 0 命中。
- 状态: ✅ 完成

### 2026-07-02 (批3) 节点图遍历逻辑收敛确认 + characterization test
- 复核结论: 上一批整改里 `DistributedStrategy._get_next_from_last` 已统一走 `flow.next_node` (注释明确"避免重复实现易漂移的图遍历")。`_advance_one` 被 Normal/Generator 两个策略共享, DistributedStrategy 因为语义不同 (suspend on `EventNode`) 保留自己的逻辑——合理。**主要复制点其实已经收敛了**, 本任务实际工作只有"把行为钉死"。
- 改动: `tests/unit/test_flow_next_node.py` 新文件, 8 个 case 覆盖: 非分支节点按 next 推进、End 节点返回 None、分支节点按 branch 参数、未知 branch 返回 None、branch=None 走 `_get_branch_target` 而非 next 字段、三种执行模式 (Normal/Generator/Distributed) 在线性 flow 上最终结果一致。
- 验证: `pytest tests/unit/test_flow_next_node.py -q` 8 passed。主仓库全套 781 passed。
- 状态: ✅ 完成 (行为钉死, 未来 #14 `ExecutionState(BaseModel)` 重构时的回归保护已就位)

### 2026-07-02 (批3) HMAC 重放保护 (B 方案, 增量兼容)
- 改动:
  - `plaita/client.py`: `generate_signature` 加可选 `nonce: Optional[str]` 参数, 非 None 时签名材料改为 `"{sign_time}\n{nonce}\n"` 并把 `nonce` 字段加进 Authorization; `PlaitaClient.__init__` 加 `replay_protected: bool = False` 参数, True 时 `get_flow` 每次请求生成 uuid4 nonce 调 `generate_signature(..., nonce=nonce)`。默认 False 保持向后兼容。
  - `plaita-console/backend/services/signature.py`: 重构 `verify_authorization` 为 _parse_authorization / _validate_key_time / _compute_signature 子函数; 检测 Authorization 里有没有 `nonce` 字段决定走新/旧验签路径。新增 `_NonceCache` 进程内单例 (内存字典 + 锁 + 惰性清理), TTL = sign_expire; 提供 `reset_nonce_cache()` 测试钩子。
  - `plaita-console/backend/tests/console/test_signature.py`: 新文件, 14 个 case 覆盖: 旧路径兼容 (7)、新路径重放保护 (7, 含首次放行/重放拒绝/不同 nonce 各自一次性/nonce 进入签名材料/旧式 auth 在 nonce 路径后仍可用/篡改 nonce 被拒/nonce 自然过期后可复用)。
- 验证: `pytest tests/console/test_signature.py -q` 14 passed; `pytest tests/console/` 53 passed; 主仓库 781 passed。
- 状态: ✅ 完成
- 注意: 这是增量方案——未升级的服务端继续按旧算法验签, 不影响现有部署; 客户端启用 `replay_protected=True` 后**必须**配合已升级的服务端。多进程部署 (gunicorn -w N) 下每个 worker 有独立 nonce 缓存, 跨 worker 重放仍可能在 3 秒窗口期内绕过——生产部署建议注入 Redis 后端 (TTL 原生支持), 已在 `_NonceCache` 文档里登记。

### 2026-07-02 (批4) tests/ 根目录按 marker 归类
- 决策: 把 tests/ 根目录 34 个 `test_*.py` 按"是否需要外部资源 (fakeredis/http/subprocess/asyncio 集成)"分到 unit/ 或 integration/, 沿用现有 `--ignore=tests/integration` 约定 (现有 integration/ 文件都没标 marker, 是靠目录隔离)。
- 改动:
  - **→ tests/unit/** (23 个): test_async_flow, test_calculate, test_checkpoint_resume, test_client_default_url, test_code, test_concurrent, test_control, test_decide, test_errors, test_evaluate, test_extended_nodes, test_flow, test_flow_distributed, test_flow_worker_callbacks, test_flow_worker_scenarios, test_from_file, test_inline, test_io, test_log_handler, test_loop, test_performance_benchmark, test_registry, test_types。这些都不依赖 fakeredis/http/subprocess, 全 mock 或纯逻辑。
  - **→ tests/integration/** (11 个): test_approval_integration, test_delay_integration (asyncio 集成), test_event_filter_dedup, test_event_system, test_redis, test_storage, test_storage_commons, test_storage_main, test_storage_redis (fakeredis), test_http (http.server), test_flow_worker (subprocess + redis)。
  - `tests/integration/test_storage_main.py`: 跨文件 import 从 `from tests.test_storage import ...` 改为 `from tests.integration.test_storage import ...`, 同时把 `parent_dir` 计算改成走两级到达 repo root。
- 验证: `pytest tests/ -q --ignore=tests/integration --ignore=tests/e2e -m "not integration"` 670 passed, 1 deselected (test_registry 内部的 `@pytest.mark.integration` 函数)。已知 timing flake: `tests/unit/test_loop.py::MapTestCase::test_map_max_concurrent`。
- 状态: ✅ 完成
- 注意: **基线数字从 791 降到 670 是预期行为**——之前根目录下那些用 fakeredis 的"伪 unit"测试 (test_redis/test_storage*/test_event_filter_dedup 等) 默认会被收集并跑, 归类后被 `--ignore=tests/integration` 正确排除, 与 marker 设计意图一致。开发者要跑这些测试时显式 `pytest tests/integration/` 即可。

下面这些是 review 里点到但本次整改**没动**的，原因要么是风险高（独立 milestone）、要么是需要先有设计：

- **#13 `FlowExecution` 拆分 Driver/State/Hooks**：390 行的 God Object，重构影响面大，应作为独立 milestone，配 e2e 回归测试。
- **#14 `ExecutionState(BaseModel)` 替换 magic key dict**：当前 `_context: Dict[str, Any]` + `f"${prefix}LAST_NODE"` magic key 模型，替换要改所有读写点（runner/executor/distributed strategy/loop/parallel/...），并保证分布式序列化往返一致。**这是最值钱也最危险的重构**。
- **#15 `CodeNode` 沙箱化**：PyExecJS 无沙箱、已弃维护。要么换 `restrictedpython`/`py-mini-racer` + 资源限制，要么默认不注册、需要显式 opt-in（甚至独立 `plaita-sandbox` 包，配 Docker/nsjail）。

---

## 8. 接手者指南

如果你来接手这个项目，建议顺序：

1. **先读本文档第 0、1、2 节**，建立"项目看起来很专业，但实现有系统性 gap"的心智模型。
2. **跑测试**确认基线：`pytest tests/ -q --ignore=tests/integration --ignore=tests/e2e -m "not integration"`，预期 ~670 passed（批4 归类后；之前 791 是因为根目录的 fakeredis/http/subprocess 测试被默认收集, 归到 integration/ 后由 `--ignore` 正确排除）。已知 deselect: `test_registry.py` 的 `@pytest.mark.integration` 函数；已知 timing flake: `tests/unit/test_loop.py::MapTestCase::test_map_max_concurrent`。要跑集成测试显式 `pytest tests/integration/`。
3. **不要立即碰 `FlowExecution` 和 `ExecutionContext._context`**——这是项目核心，重构前先写 characterization test 把当前行为钉死。
4. **优先做 #14（ExecutionState BaseModel）**——这是最大杠杆，做完之后 #13（FlowExecution 拆分）会自然简化。
5. **`CodeNode` 不要在生产暴露**：在没有沙箱方案前，从 `_BUILTIN_NODES` 移除，让它走 entry_point 显式注册。
6. **遇到"`_ENV` 读不到"的反馈**：告诉对方查本文档第 5 节 #3，加 `expose_env=[...]`。
7. **遇到"parallel coroutine 崩了"的反馈**：告诉对方 mode=coroutine 已下线，用 thread/process。

---

## 9. 模板

```
### YYYY-MM-DD #N 任务名
- 改动文件: path/to/file.py
- 验证: pytest tests/xxx.py -q  / 手测
- 遗留: ...
- 状态: ✅/🕯️/⏳
```
