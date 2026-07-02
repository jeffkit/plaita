---
name: plaita-flow-runner
description: 运行/执行/调试 plaita 的 flow（本地执行，支持 JSON/YAML）。当用户提到「运行这个 plaita flow」「跑一下这个流程」「调试 flow」「单步执行」「plaita 流程报错了帮我看看」「验证生成的 flow 能不能跑」「执行 flow 并看结果」时使用此 Skill。即使用户没说「skill」，只要意图是在本地把一段 Plaita flow 跑起来、调试或验证，就应触发。注意：本 Skill 仅覆盖本地执行（Flow.run / debug / distributed），不处理远程 PlaitaClient。
version: 0.2.0
---

# Plaita Flow Runner

把一段 Plaita flow 在本地跑起来——执行、调试、验证、排错。flow 可以是 JSON 或 YAML。

## 前置：确认 plaita 已安装

执行前先确认运行时可用。在 shell 里跑：

```bash
python3 -c "import plaita; print(plaita.__version__)"
```

若报 `ModuleNotFoundError: No module named 'plaita'`，提示用户安装：

```bash
pip install logic-plaita
```

若 flow 里用到 `code` / `http` 节点，还需对应 extra：`pip install logic-plaita[code]` / `logic-plaita[http]`。若 flow 是 YAML 格式，需 `pip install logic-plaita[yaml]`。

> 如果用户在 plaita 仓库内工作（仓库根目录有 `plaita/` 包），直接 `import plaita` 会用仓库源码，无需 pip 安装。

## 工作流程

### 1. 拿到 flow 定义

flow 来源可能是：
- 用户直接贴的 JSON / YAML 字符串
- 一个文件路径（如 `echo.json`、`flow.yaml`、`flow.yml`）
- 之前由 `plaita-flow-builder` skill 刚生成的 flow

**按来源选加载入口**，不要假设一定是 JSON：
- 字符串内容 → `Flow.from_string(s)`：自动识别 JSON/YAML
- 文件路径 → `Flow.from_file(path)`：按后缀 `.json`/`.yaml`/`.yml` 选择解析器
- 想一步到位 → `parse_and_run(content, ...)`：同样自动识别

> YAML 需要 `logic-plaita[yaml]`。若用户贴的是 YAML 且解析报「需要 PyYAML」，提示安装 extra。加载入口都已统一，无需手动 `json.loads`——但仍要捕获解析异常，区分「格式错」与「执行错」。

### 2. 选执行模式

根据用户目的选模式。三种模式共享同一套 flow 定义，差异在控制权与是否可跨进程暂停。

| 目的 | 模式 | 入口 |
|------|------|------|
| 快速拿结果 | Normal（默认） | `flow.run(...)` / `await flow.arun(...)` |
| 单步调试、看中间状态 | Generator | `for step in flow.debug(...)` |
| 长时工作流、等外部事件、跨进程 | Distributed | `FlowExecution().run_distributed(...)` |

绝大多数「跑一下看结果」的需求用 **Normal** 就够了。只有用户明确说要调试、看每一步、或断点续执时才用另外两种。

完整模式细节见 `references/execution-modes.md`。

### 3. 推断输入参数

`flow.run(...)` 统一接受 **dict 或关键字参数**，`$INPUT` 始终是传入的 dict：

| 调用方式 | `$INPUT` |
|----------|----------|
| `flow.run(name="kongjie", age=18)` | `{"name": "kongjie", "age": 18}` |
| `flow.run({"name": "kongjie"})` | 该 dict |
| `flow.run()` | `{}` |

> 历史 scalar/array 位置传参（`flow.run("x")` / `flow.run(a, b, c)`）已移除。
> 标量/数组输入用 dict 包装，例如 `flow.run({"value": "kongjie"})` 并在表达式里引用 `$INPUT.value`。

**不要凭空编参数**。从用户那里确认输入值；若用户只给了一部分，问清楚缺的；若用户说「随便给个示例」，就给一个最小可行示例并说明你用了什么值。

### 4. 执行并捕获结果

#### Normal 模式

```python
from plaita import Flow

flow = Flow.from_file("echo.json")   # 或 echo.yaml / echo.yml

result = flow.run(name="kongjie")
print(result)
```

异步上下文里用 `await flow.arun(name="kongjie")`。

一步到位的写法：

```python
from plaita import parse_and_run
print(parse_and_run(open("echo.yaml").read(), name="kongjie"))
```

#### Generator 模式（调试）

```python
from plaita import Flow

flow = Flow.from_file("flow.json")
for step in flow.debug(name="test"):
    print(f"[{step['type']}] {step['id']} -> {step['result']}")
    if step["is_end"]:
        print("流程完成")
        break
```

每次 yield 的字典含：`id` / `type` / `name` / `result` / `branch` / `context` / `is_end` / `is_suspend` / `execution_id`。

#### Distributed 模式（断点续执）

```python
from plaita import Flow, FlowExecution

flow = Flow.from_file("approval_flow.json")
execution = FlowExecution()

# 第一次推进：执行到事件节点并挂起
step = execution.run_distributed(flow, {"applicant": "alice"})

# 把 step["context"] 持久化……之后恢复：
step = execution.run_distributed(
    flow, None,
    saved_context=saved_context,
    resume_type="event",
    resume_data={"approved": True},
)
```

`resume_type`：`event`（事件到达）/ `timeout`（等待超时）/ `cancel`（取消）/ `continue`（从 LAST_NODE 之后继续下一节点）。

> 复用同一个 `FlowExecution` 实例调用 `run_distributed`，否则回调无法跨步骤保留。

### 5. 解读输出与错误

执行后向用户报告：
- **成功**：展示返回值，并对照 `end` 节点的 `output` 表达式说明结果是怎么来的。
- **业务错误**（`resultType: error`）：抛 `FlowResultError(code, message)`，报告 code 与 message。
- **执行异常**：常见原因见下表。

| 现象 | 可能原因 | 排查 |
|------|---------|------|
| `ModuleNotFoundError: execjs/requests` | 用了 `code`/`http` 节点但没装 extra | `pip install logic-plaita[code]` / `[http]` |
| 表达式被原样返回字符串 | 写成了 `${INPUT.x}` | 改成 `$INPUT.x` |
| `KeyError` / 字段为 None | 表达式引用了不存在的字段 | 用 Generator 模式逐节点看 `context` |
| 节点 `id` 找不到 / `next` 断链 | JSON 拓扑错误 | 检查所有 `next` 指向的 id 存在 |
| 传参对不上 | `dataType` 与调用方式不匹配 | 按 `inputType.dataType` 调整传参 |

遇到执行异常时，**优先用 Generator 模式单步跑**，定位到出错的节点，再回头看该节点的字段配置。

## 交付规范

执行后按这个结构反馈：

1. **执行方式**：用了哪种模式、什么参数。
2. **结果**：返回值（或错误码/错误信息）。
3. **解读**：结果与 flow 定义的对应关系——哪个 `end` 的 `output` 产生了这个值，分支走到了哪条路。
4. **下一步建议**（若出错）：具体改哪个节点的哪个字段。

## 边界与约束

- 本 Skill **只管本地执行**。若用户要远程执行（`PlaitaClient.run_flow`），明确告知这不在本 Skill 范围，引导其参考 `plaita.client.PlaitaClient`。
- 不要修改用户的 flow 定义来「让它跑通」——除非用户明确要求。发现 flow 问题时，**指出问题并建议修改**，由用户决定。
- 执行用户提供的 flow 前，若 flow 来自不可信来源，提醒用户 `code` 节点会执行任意代码，注意安全。
- 长时 / Distributed 流程不要在交互式调试里无限推进，推进到挂起点就停，把 `context` 交给用户持久化。

## 深入参考

- `references/execution-modes.md` —— 三种执行模式的完整字段、yield 结构、resume_type 细节
