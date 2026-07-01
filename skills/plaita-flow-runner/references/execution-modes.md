# 执行模式参考

plaita 三种执行模式共享同一套 `Flow` 定义与节点体系，差异在于**控制权交给方式**与**是否支持跨进程暂停/恢复**。

| 特性 | Normal | Generator | Distributed |
|------|--------|-----------|-------------|
| 阻塞 | 是 | 否（yield） | 否（可挂起） |
| 跨进程 | 否 | 否 | 是 |
| 状态检查 | 否 | 是 | 是 |
| 最适用于 | 快速流程 | 调试 | 长时工作流 |
| 复杂度 | 低 | 中 | 高 |

## Normal 模式（默认）

同步阻塞执行，跑到 `End` 节点后一次性返回结果。

```python
from plaita import Flow

flow = Flow.from_string(open("echo.json").read())
result = flow.run(name="kongjie")  # 阻塞直到完成
```

异步版本：

```python
result = await flow.arun(name="kongjie")
```

一步到位：

```python
from plaita import parse_and_run
print(parse_and_run(open("echo.json").read(), name="kongjie"))
```

## Generator 模式

异步生成器，**每执行完一个节点就 yield 一次**，调用方控制节奏、可在步骤间检查或修改上下文。适合调试器、可视化单步、交互式检查。

```python
from plaita import Flow

flow = Flow.from_string(open("flow.json").read())
for step in flow.debug(name="test"):
    print(f"[{step['type']}] {step['id']} -> {step['result']}")
    print(f"context: {step['context']}")
    if step["is_end"]:
        print("流程完成")
        break
```

每次 yield 的字典结构：

| 字段 | 含义 |
|------|------|
| `id` / `type` / `name` | 节点 id / 类型 / 展示名 |
| `result` | 该节点输出 |
| `branch` | 命中的分支（分支节点） |
| `context` | 当前执行上下文快照 |
| `is_end` | 是否到达 End 节点 |
| `is_suspend` | 是否在事件节点挂起（Distributed） |
| `execution_id` | 本次执行 ID |

> `on_flow_end` 回调会推迟到生成器**真正被消费完毕或关闭**时才触发，而非 `flow.debug()` 调用返回时。

## Distributed 模式

为跨进程、长时运行的工作流设计。每次调用**只推进一个节点**，执行上下文可序列化持久化，流程在事件节点处挂起，外部事件到达后从断点恢复。

```python
from plaita import Flow, FlowExecution

flow = Flow.from_string(open("approval_flow.json").read())
execution = FlowExecution()

# 第一次推进：执行到事件节点并挂起
step = execution.run_distributed(flow, {"applicant": "alice"})
# step: {"is_suspend": True, "context": {...}, ...}

# 把 step["context"] 持久化（如存入 ExecutionStorage）
save(execution._ctx.execution_id, step["context"])

# ... 一段时间后，外部事件到达，在另一个进程恢复 ...
saved_context = load(execution_id)
step = execution.run_distributed(
    flow, None,
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

> `FlowExecution.run(..., mode='distributed')` 会路由到 `run_distributed`，但**每次都会新建一个 execution 实例**，导致用户回调无法跨步骤保留。需要跨步骤保留回调时，**复用同一个 `FlowExecution` 实例**并直接调用 `run_distributed`。

## 解析入口

```python
from plaita import Flow, parse

flow = Flow.from_string(json_str)
flow = Flow.model_validate_json(json_str)
flow = Flow.model_validate(dict_data)
flow = parse(json_str)        # str 或 dict 均可，会校验 runtime == "python"
```

## 如何选择

- 短时、要立即拿结果 → Normal
- 要单步、要检查中间状态、做调试器 → Generator
- 要等外部事件、要跨进程、要长时运行 → Distributed
