# 生成器调试器

用 `flow.debug()` 的 Generator 模式 + `FlowCallback` 构建一个简单的可视化调试器：逐步展示节点执行、结果与上下文。

## 目标

- 单步执行流程，每步打印节点 id / 类型 / 结果
- 命中分支时显示走向
- 到达 End 时输出最终结果

## 流程示例

用一个带分支的流程做演示：

```json
{
    "flow_id": "debug_demo",
    "inputType": { "dataType": "object" },
    "nodes": [
        { "type": "start", "id": "start", "next": "check" },
        {
            "type": "if",
            "id": "check",
            "condition": { "field": "$INPUT.age", "operator": "gte", "value": 18 },
            "next": "adult",
            "else_next": "minor"
        },
        { "type": "end", "id": "adult", "output": "成年", "resultType": "success" },
        { "type": "end", "id": "minor", "output": "未成年", "resultType": "success" }
    ]
}
```

## 单步调试器

```python
from plaita import Flow

flow = Flow.from_string(open("debug_demo.json").read())

final = None
for step in flow.debug(age=20):
    branch = f" -> branch='{step['branch']}'" if step["branch"] else ""
    print(f"[{step['type']}] {step['id']}: result={step['result']!r}{branch}")
    if step["is_end"]:
        final = step["result"]
        break

print("最终结果:", final)
```

`age=20` 时输出：

```
[start] start: result=None
[if] check: result='adult' -> branch='adult'
[end] adult: result='成年'
最终结果: 成年
```

`age=12` 时会走 `minor` 分支。

## 叠加回调做埋点

Generator 模式与回调可叠加。给执行加一个 `FlowCallback` 收集节点耗时：

```python
import time
from plaita import Flow, FlowExecution, FlowCallback

class TimingCallback(FlowCallback):
    def __init__(self):
        self._t = {}
    def on_node_start(self, flow, node, **kwargs):
        self._t[node.id] = time.time()
    def on_node_end(self, flow, node, result=None, **kwargs):
        dt = (time.time() - self._t.get(node.id, time.time())) * 1000
        print(f"  (耗时 {dt:.1f}ms)")

cb = TimingCallback()
execution = FlowExecution(callback_handlers=[cb])
for step in execution.run_compatible(flow, True, age=20):  # lazy=True 返回生成器
    print(f"[{step['type']}] {step['id']}: {step['result']!r}")
    if step["is_end"]:
        break
```

## 交互式单步

```python
for step in flow.debug(age=20):
    print(f"当前节点: {step['id']}, 结果: {step['result']!r}")
    print(f"上下文: {step['context']}")
    if step["is_end"]:
        print("完成:", step["result"])
        break
    input("按 Enter 继续下一步...")
```

适合做成命令行调试器，或在前端把每个 `step` 渲染成一帧。

## 检查与修改上下文

`step["context"]` 是当前上下文快照，可在两步之间观察 `$NODE` / `$INPUT`，定位表达式求值或分支走向问题。注意：修改 `step["context"]` 不会回写到执行引擎——若需注入状态，用 `execution.set_state(...)` 在自定义节点里改。

## 异步版本

```python
async def adebug():
    async for step in flow.arun_compatible_lazy(...):  # 异步生成器
        ...

# 或直接 await flow.arun(...) 配合 LoggerCallback 观察日志
```

## 要点回顾

- `flow.debug()` = Generator 模式，每节点 yield 一次
- 每次 yield 含 `id` / `type` / `result` / `branch` / `context` / `is_end`
- 可与 `FlowCallback` 叠加做耗时/埋点
- `on_flow_end` 在生成器消费完毕后才触发，生命周期与进度一致

## 下一步

- [调试](../guide/debugging.md)
- [回调机制](../guide/callbacks.md)
- [执行模式](../guide/execution-modes.md)
