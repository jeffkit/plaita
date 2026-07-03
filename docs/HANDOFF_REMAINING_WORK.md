# Handoff — 剩余架构整改清单

> 给下一个接手会话的文档。本文件只列**还没做完的事**；已完成的 9 项见
> `docs/ARCHITECTURE_REVIEW_2026-07.md`（毒舌版总账）与最近一次 commit 的 diff。
>
> 写这份文档的原因：上一轮 review 我做了 6/9 真做完、2/9 只完成形式、1/9 误判、
> 1/9 没动。剩下的 3 件都是 1+ 天独立 PR 量级，不适合塞在一次 review 里赶完——
> 硬塞会重蹈"内部 review 自称拆完了实际只是化妆"的覆辙。

---

## 0. 当前状态（接手前必读）

### 0.1 上次会话做了什么

9 个 task，按完成度分三档：

| 档位 | 任务 |
|---|---|
| ✅ 真做完 | MIGRATION.md、README CodeNode 段、错误消息加指引、InMemoryEventBus 标注、删 `_create_pool` 改单例池、删 `_default_event_bus_provider` 全局 singleton |
| 🟡 中间步 | ExecutionState 显式 schema（`plaita/core/state.py` + drift warning，**没**做完整 BaseModel 重写）、FlowExecution generator finally 去重（**没**拆 Driver/State/Hooks） |
| 🔴 误判/未动 | `branch.next` 兜底（误判，回滚后加 docstring）、三种执行模式散弹枪（未动） |

### 0.2 当前测试基线

```
pytest tests/ -q --ignore=tests/integration --ignore=tests/e2e -m "not integration"
→ 690 passed, 5 skipped, 1 deselected
```

基线 684 + 6 个新 `tests/unit/test_state.py`，0 回归。

集成测试：`pytest tests/integration/` → 198 passed，加新 layering 断言后 12 passed。

### 0.3 已知非回归 fail（基线就 fail，不是我引入的）

1. **`tests/e2e/test_success_criteria.py::TestSC003LargestClassUnder200LOC::test_execution_context_under_200_loc`**
   - `ExecutionContext` 当前 263 LOC（限制 200）。基线 261 LOC。
   - 这是项目自定的 SC-003 设计目标，e2e 默认不进基线命令。
   - **修这条要么真拆 ExecutionState（见下面任务 #1），要么把 ExecutionContext 里的 typed property 抽到 mixin**。

2. **`tests/unit/test_loop.py::MapTestCase::test_map_max_concurrent`**
   - timing flake，文档自评（`ARCHITECTURE_REVIEW_2026-07.md`）已记录。

### 0.4 工作区状态

未提交改动（9 文件改 + 4 文件新增）：

```
M  README.MD
M  plaita/__init__.py
M  plaita/core/context.py
M  plaita/core/executor.py
M  plaita/core/flow.py
M  plaita/node/__init__.py
M  plaita/node/concurrent.py
M  plaita/node/decide.py
M  tests/integration/test_layering.py
?? MIGRATION.md
?? plaita/core/state.py
?? tests/unit/test_state.py
?? .python-version  # pyenv 用，不应 commit
```

**接手第一步**：先 `git diff` 复核，决定哪些 squash 进第一个 commit、哪些拆开。建议至少拆 3 个 commit：(1) 文档类、(2) `_default_event_bus_provider` 删除 + layering 测试改造、(3) `state.py` + ExecutionState 中间步。

---

## 1. 真正的大活：3 件

### 任务 #1 — ExecutionState 完整 BaseModel 重写

**优先级**：P1（架构债最重，且修完顺带解决 SC-003 LOC fail）

**为什么没做完**：

`plaita/core/context.py` 现在的 `ExecutionContext._context: Dict[str, Any]` 是个大字典，键是 `f"{pfx}LAST_NODE"`、`f"{pfx}BRANCH"` 等 magic string。上一轮我加了 `CheckpointSchema` (`plaita/core/state.py`) 和 drift warning，但**底层存储仍是 dict**——只是让新加 magic key 的人会被 warn，没真建模。

完整 BaseModel 重写需要解决 4 个互相耦合的问题，**任何一个搞错都会破坏分布式 checkpoint 二进制兼容**：

#### 1.1 序列化格式必须 byte-for-byte 兼容

Redis/SQL 持久化的 checkpoint 是 `dict(self._context)` 的 JSON。改成 BaseModel 后 `model_dump()` 默认行为可能改变 key 顺序/嵌套结构。需要：

```python
class ExecutionState(BaseModel):
    model_config = ConfigDict(extra="allow")  # 允许节点 plugin 塞非 schema 字段

    last_node_id: Optional[str] = None
    last_branch: Optional[str] = None
    flow_id: Optional[str] = None
    execution_id: str = ""
    input_value: Any = None
    node_results: Dict[str, Any] = Field(default_factory=dict)
    global_context: Dict[str, Any] = Field(default_factory=dict)
    parent_context: Dict[str, Any] = Field(default_factory=dict)
    env: Dict[str, str] = Field(default_factory=dict)
    express_prefix: str = "$"  # 顶层 unprefixed key 也要保留

    def to_checkpoint_dict(self) -> Dict[str, Any]:
        """输出与旧 dict 格式逐字节一致的 JSON-able dict。"""
        # key 名仍要带 prefix ($LAST_NODE 等)，保留历史 checkpoint 兼容
        ...
```

**验证方式**：写一个 "round-trip property test"——随机生成 N 个旧 dict checkpoint，用 `from_dict` 转 ExecutionState 再 `to_dict`，断言与原 dict 完全相等。已有 `tests/unit/test_context.py::test_to_dict_from_dict` 是雏形，扩展为 property test。

#### 1.2 表达式引擎依赖 `Dict[str, Any]` 视图

`ExpressionEvaluator.evaluate(value, context_dict, prefix)` (`plaita/core/expression.py`) 直接吃 dict。两个选择：

- **(a) ExecutionState 暴露 dict-like 视图**：实现 `__getitem__`/`__contains__`/`keys()`/`__iter__`，让 ExpressionEvaluator 无感知切换。**推荐**——改动面小。
- **(b) ExpressionEvaluator 改吃 ExecutionState**：彻底但改动面大，所有 evaluator 的测试都要改。

走 (a) 时注意：Pydantic v2 BaseModel 不是 dict 子类，`isinstance(state, dict)` 会变 False——grep 一遍这种断言。

#### 1.3 节点 plugin 写非 schema 字段

`set_state("my_node_local_key", ...)` 是 supported escape hatch。BaseModel 用 `extra="allow"` 接住，但 `model_dump()` 默认会 dump extra 字段，需要验证它们能 round-trip。

#### 1.4 child/parent 链

`ExecutionContext.parent` 是另一个 ExecutionContext 实例（**不进 checkpoint**，只在内存里）。BaseModel 化时 parent 必须排除：

```python
model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)
parent: Optional["ExecutionContext"] = Field(default=None, exclude=True)
```

#### 1.5 顺手解决 SC-003 LOC fail

 ExecutionContext 拆完后，类 LOC 应该从 263 降到 ~150（state 部分挪到 ExecutionState）。`tests/e2e/test_success_criteria.py::test_execution_context_under_200_loc` 自然变 green。

**预计耗时**：1.5–2 天。**风险**：高（分布式恢复回归）。**验证**：必须有 Redis/SQL checkpoint 的端到端 resume 测试覆盖。

---

### 任务 #2 — FlowExecution 真拆 Driver / State / Hooks

**优先级**：P2（架构清晰度问题，但当前 750 行能跑，不阻塞功能）

**为什么没做完**：

上一轮我只抽了 `_emit_flow_end_on_close` 共享 helper 消除 generator finally 复制。`executor.py` 仍是 750 行，`FlowExecution` 仍是 God Object：13 个 `@property` 透传 + `_lazy_sync_generator`/`_lazy_async_generator` 两套生成器 + `run_compatible`/`arun_compatible` + `_finish`/`run_distributed` 两套归一化（**故意不同**，注释写了）。

拆分目标：

```
FlowExecution (Driver)
├── 持有 ExecutionContext (State)        ← 任务 #1 之后会变成 ExecutionState
├── 持有 NodeRunner
├── 持有 CallbackManager (Hooks)
├── 持有 strategies dict
└── 公开 API: run / arun / debug / run_distributed
    （内部桥接全部下沉到 async_utils）
```

#### 2.1 sync/async 桥接下沉

`run_compatible` / `arun_compatible` 非 lazy 分支的差异是 `_run_async_sync(coro)` vs `await coro`——这是 async_utils 该处理的事。建议 `async_utils.py` 加：

```python
def drive_strategy(coro_or_agen, *, lazy: bool, sync: bool):
    """统一 sync/async × lazy/eager 四种组合的桥接。"""
    ...
```

`FlowExecution.run_compatible` 退化为 1 行 `return drive_strategy(self._prepare_strategy(...), lazy=True, sync=True)`。

#### 2.2 `_finish` 与 `run_distributed` 的归一化策略

当前两套（normal 透传 `FlowErrorException`，distributed 归一化为 `-500`）。**不要强行合并**——这是有意的对外契约差异。但可以把两个 helper 写在同一个 `_error_normalization.py` 模块，让差异显式可见。

#### 2.3 Strategy 状态 vs Context 状态 vs Callback 编排

`FlowExecution` 现在自己持 `mode`/`timeout`（Strategy 状态），又透传 `_ctx`（Context 状态），又调 `callback_manager.on_flow_start/end`（Callback 编排）。拆 Driver 后这三者应该是 Driver 的**协作者**，不是 Driver 的属性。改：

- `mode`/`timeout` → 移到 `RunOptions` dataclass，传给 strategy
- `callback_manager` → 保持协作者地位，Driver 调用而不拥有生命周期
- `_ctx` → Driver 持有但不透传（删 13 个 property，节点统一通过 `execution.context.xxx` 访问，或 `execution.state.xxx`）

**13 个 property 透传的删除策略**：节点的 `execution.set_state(...)` / `execution.get_state(...)` / `execution.evaluate(...)` 是公开 API，保留；`execution.express_prefix` / `execution.execution_id` 这种 pure read 保留；中间的 typed property (`last_node_id` 等) 改为 `execution.state.last_node_id`，**这是 break change，需要 MIGRATION.md 加一条**。

**预计耗时**：1.5 天。**风险**：中（节点 plugin API 变更）。**验证**：跑全套 + examples/agent/。

---

### 任务 #3 — 三种执行模式的调度路径统一

**优先级**：P3（架构洁癖问题，bug 风险低但改起来烦）

**为什么没动**：

当前 `next_node` 决策有 3 个实现：

1. `flow.next_node(current, branch)` (`plaita/core/flow.py:202`) — 主路径
2. `Map.concurrent=True` (`plaita/node/loop.py:99-109`) — 自起 `ThreadPoolExecutor`，不走 strategy 层
3. `Parallel.pool_execute` (`plaita/node/concurrent.py:128`) — 走自己的 `wait()` 调度

任何"分支调度策略"的修改要改 3 处。`Map.concurrent` 和 `Parallel` 本质都是"把 child_flow 投到并行执行器"，但 API 完全不通信。

#### 3.1 抽 `ParallelExecutor` 协议

```python
class ParallelExecutor(Protocol):
    def map(self, fn: Callable, items: List[Any]) -> List[Any]: ...

class ThreadParallelExecutor(ParallelExecutor):
    def __init__(self, max_workers: Optional[int] = None):
        self._pool = BackGroundThreadPool  # 复用单例
    ...

class ProcessParallelExecutor(ParallelExecutor): ...
```

#### 3.2 Map/Parallel/Filter/Find/Reduce 全部走 ParallelExecutor

`Map.concurrent=True` 不再自起池，构造时拿一个 executor（默认 thread）。

#### 3.3 cancel/timeout 协议

进程模式 cancel_event 跨进程不传播（pickle 弹掉）的限制要文档化到 ParallelExecutor 接口，不要让每个调用点自己 try。

**预计耗时**：1 天。**风险**：中（并发行为回归）。**验证**：`tests/unit/test_concurrent.py` + 加并发回归测试。

---

## 2. 没做完但优先级低的小活

### 任务 #4 — `examples/agent/` 补真示例或 README 删宣传

**优先级**：P3

README 表格里宣传 "AI Agent 多步工具编排" 是首要场景，但 `examples/agent/` 只有 `demo.py` (120 行) + `nodes.py` (277 行)，没有 RAG / tool-use / router 示例。

两个选项：
- **(a) 补真示例**：写 `examples/agent/rag.py`、`examples/agent/tool_use.py`、`examples/agent/router.py` 各 100-200 行可运行 demo。
- **(b) 删宣传**：README 的"它适合做什么"表格删掉 AI Agent 那两行。

走 (a) 更值——plaita 卖点就是 agent 编排，没示例等于自杀。

### 任务 #5 — mutmut 覆盖 async/distributed 路径

**优先级**：P3

`pyproject.toml [tool.mutmut] only_mutate` 只列了 4 个纯同步模块（callback/expression/calculate/decide）。`executor.py`（750 行 async 桥接）、`runner.py`（超时+重试+cancel）、`concurrent.py`（线程/进程）、`code.py`（沙箱）全部不进变异测试。

文档自评里说"async/distributed/timeout 路径在 mutmut 进程内冲突"——这是已知 workaround。**真正修法**：要么把 async 测试改成 `pytest-asyncio` mode 让 mutmut 能跑，要么给 mutmut 配 separate-process worker。

### 任务 #6 — `_get_branch_target` 兜底契约再审视

**优先级**：P4（我上次误判，标完文档后留观察）

`plaita/core/flow.py:217` 的 `target = b.next or b.name` 是 `Switch`/`Logic` 类节点的设计语义（branch.name 自身就是目标节点 id）。我加了 `Branch` docstring 文档化。

**值得再审视的场景**：如果未来加新的 branching 节点类型，作者需要知道这个隐式契约。考虑：

- **(a)** 加 `Branch.require_explicit_next: ClassVar[bool]`，Switch 设 False，其他默认 True，违反时解析期就报错。
- **(b)** 把 Switch/Logic 的 branch.name-as-id 模式做成显式字段 `branch.target_is_name: bool = False`，Switch 内部自动设 True。

---

## 3. 接手 checklist

1. **先跑基线**：`pytest tests/ -q --ignore=tests/integration --ignore=tests/e2e -m "not integration"` → 期望 690 passed。
2. **跑集成测试**：`pytest tests/integration/` → 期望 198+ passed（layering 新断言 12 passed）。
3. **跑 e2e 看 SC-003 fail**（基线就 fail，不是回归）。
4. **决定先做哪个任务**：建议顺序 #1 → #2 → #3。#1 解决 SC-003 LOC fail + 分布式 checkpoint 显式 schema；#2 顺带消化 #1 的 break change；#3 是收尾。
5. **每个任务一个 PR**，别 squash——这三个改动各自需要独立 review。
6. **每完成一个任务**：更新本文档对应章节状态为 ✅，commit message 引用本文件章节号。

---

## 4. 不要做的事

- **不要为了过 SC-003 LOC 把 ExecutionContext 硬拆成 mixin**——mixin 是退路不是正路。真做 ExecutionState(BaseModel) 才是。
- **不要把 `_finish` 和 `run_distributed` 的归一化强行合并**——注释里写了 normal vs distributed 的对外契约**故意不同**。
- **不要删 `branch.next or branch.name` 兜底**——Switch/Logic 设计语义，删了破坏现有 flow。
- **不要在没有 Redis/SQL 端到端 resume 测试覆盖的情况下动 `to_dict/from_dict` 格式**——分布式 checkpoint 二进制兼容是硬约束。
- **不要 commit `.python-version`**（pyenv 用，每个开发者环境不同）。
