# 执行模式

plaita 支持三种执行模式以适应不同场景。三种模式共享同一套 `Flow` 定义与节点体系，差异在于**控制权交给方式**与**是否支持跨进程暂停/恢复**。

| 特性 | Normal | Generator | Distributed |
|------|--------|-----------|-------------|
| 阻塞 | 是 | 否（yield） | 否（可挂起） |
| 跨进程 | 否 | 否 | 是 |
| 状态检查 | 否 | 是 | 是 |
| 最适用于 | 快速流程 | 调试 | 长时工作流 |
| 复杂度 | 低 | 中 | 高 |

## Normal 模式（默认）

同步阻塞执行，跑到 `End` 节点后一次性返回结果。适合快速、请求-响应式的流程。

```python
from plaita import Flow

flow = Flow.from_string(open("echo.json").read())
result = flow.run(name="kongjie")  # 阻塞直到完成
```

异步版本：

```python
result = await flow.arun(name="kongjie")
```

## Generator 模式 { #generator }

异步生成器，**每执行完一个节点就 yield 一次输出**，调用方控制节奏、可在步骤间检查或修改上下文。适合调试器、可视化单步、交互式检查。

```python
from plaita import Flow

flow = Flow.from_string(open("flow.json").read())

for step in flow.debug(name="test"):
    print(f"[{step['type']}] {step['id']} -> {step['result']}")
    print(f"context: {step['context']}")
    if step["is_end"]:
        print("流程完成")
        break
    # 可选：在此检查/修改 step["context"]，或暂停
```

每次 yield 的字典结构：

| 字段 | 含义 |
|------|------|
| `id` / `type` / `name` | 节点 id / 类型 / 展示名 |
| `result` | 该节点输出 |
| `branch` | 命中的分支（分支节点） |
| `context` | 当前执行上下文快照 |
| `is_end` | 是否为到达 End 节点的那一步（是则 `True`，流程随即结束） |
| `is_suspend` | 是否在事件节点挂起（Distributed） |
| `execution_id` | 本次执行 ID |

!!! note "Generator 模式的 on_flow_end 时机"

    `on_flow_end` 回调会推迟到生成器**真正被消费完毕或关闭**时才触发，而非 `flow.debug()` 调用返回时。这样保证生命周期回调与实际执行进度一致。

!!! warning "Generator 模式同样执行流程级 timeout"

    流程的 `timeout` 字段（ISO 8601 时长，如 `"PT1S"`）在 Generator 模式下**同样生效**——你在步骤间检查/暂停消耗的时间计入预算，超时后抛 `FlowTimeoutError`（或节点级 `NodeTimeoutError`）。调试器场景请给流程留足 `timeout`，或依赖节点级超时。

## Distributed 模式

为**可跨进程挂起/恢复**设计：每次调用**只推进一个节点**，执行上下文可序列化持久化，流程在挂起节点处暂停，外部事件到达后从断点恢复。

!!! warning "命名与能力边界"

    `ExecutionMode.DISTRIBUTED` / `run_distributed` 表示「单步 + suspend/resume」，**不**表示：

    - 任务至少一次投递（`RedisFlowWorker` 使用 Redis Stream + `XACK` / `XCLAIM`）
    - 「恰好一次」副作用（队列仍是至少一次；resume 有 lease 防并发，但崩溃重投后节点仍可能再跑）

    中间态落盘间隔由 `PERSIST_EVERY_N_STEPS` 控制，默认 **1**（每步落盘）。部署前请读 [FlowWorker · 可靠性边界](../distributed/flow-worker.md#可靠性边界必读)。

```python
from plaita import Flow, FlowExecution

flow = Flow.from_string(open("approval_flow.json").read())
execution = FlowExecution()

# 第一次推进：执行到事件节点并挂起
step = execution.run_distributed(flow, {"applicant": "alice"})
# step: {"is_suspend": True, "context": {...}, ...}

# 把 step["context"] 持久化（如存入 ExecutionStorage）
save(step["execution_id"], step["context"])

# ... 一段时间后，外部事件到达，在另一个进程恢复 ...
saved_context = load(execution_id)
step = execution.run_distributed(
    flow,
    None,
    saved_context=saved_context,
    resume_type="event",
    resume_data={"approved": True},
)
```

### 恢复类型（resume_type）

挂起在 `EventNode` 后，恢复时通过 `resume_type` 决定如何唤醒：

| resume_type | 行为 |
|-------------|------|
| `event` | 事件到达，调用 `node.on_event(execution, resume_data)` |
| `timeout` | 等待超时，调用 `node.on_timeout(execution)` |
| `cancel` | 取消等待，调用 `node.on_cancel(execution)` |
| `continue` | 不走恢复分支，从上次 `LAST_NODE` 之后继续推进下一节点 |

!!! warning "两种调用方式的关键区别"

    **正确模式（推荐）**：实例化 `FlowExecution` 并复用它：

    ```python
    execution = FlowExecution(callback_handlers=[my_callback])
    step1 = execution.run_distributed(flow, params)           # 第一步
    step2 = execution.run_distributed(flow, saved_context=...) # 恢复步骤，回调仍在
    ```

    **注意陷阱**：`FlowExecution.run(flow, mode='distributed')` 是类方法，**每次调用都会创建新实例**。如果你注册了回调，这些回调在后续调用中会丢失：

    ```python
    # ⚠️ 这样做，回调只在第一次调用时生效
    step1 = FlowExecution.run(flow, params, mode='distributed', callback_handlers=[my_callback])
    step2 = FlowExecution.run(flow, None, mode='distributed', context=step1["context"])  # 回调丢失！
    ```

    如果只有单次 fire-and-forget 调用（不需要在多步骤间保留回调），则类方法也是可以的。

## 如何选择

- **短时、要立即拿结果** → Normal
- **要单步、要检查中间状态、做调试器** → Generator
- **要等外部事件、要跨进程、要长时运行** → Distributed（配合 [断点续执](../distributed/index.md)）

## 时序图

三种模式的交互时序见 [架构 - 时序图](../architecture/sequence-diagrams.md)。

## 下一步

- [调试](debugging.md) —— Generator 模式的实际用法
- [断点续执](../distributed/index.md) —— Distributed 模式深度
- [API: plaita.core.executor](../api/executor.md)
