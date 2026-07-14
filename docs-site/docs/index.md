---
title: plaita —— Plaita 逻辑编排系统 Python 运行时
description: 在 Python 中解析并执行 JSON 定义的逻辑流程，支持同步、生成器与分布式三种执行模式，及断点续执、事件驱动恢复。
---

# plaita

**Plaita 逻辑编排系统的官方 Python 运行时。**

plaita 将 JSON 格式的逻辑流程定义解析为可执行的 `Flow`，并以插件化的 `Node` 体系驱动执行。它把**流程定义**与**执行逻辑**分离，支持同步阻塞、生成器单步、以及可跨进程挂起/恢复（Distributed）三种执行模式——从「一次请求即返回」到「等外部事件再继续」；Distributed **不是**默认具备至少一次投递的容错引擎。

## 核心特性

- :material-graph-outline: **JSON 流程定义** —— 用节点 + `next`/分支描述控制流，平台可视化编排或手写皆可
- :material-cube-outline: **插件化节点** —— 默认 16 种内置节点（`CodeNode` 需显式注册），自定义节点只需实现 `execute`，支持 entry_points 自动发现
- :material-robot-outline: **AI Agent 编排（典型场景）** —— LLM 规划 + plaita 执行是最常见的使用场景之一；`@flow` / JSON 有构建期校验，`flow_from_source` 让 AI 生成的流程编译期就拦错
- :material-sync: **三种执行模式** —— Normal 同步、Generator 单步调试、Distributed 跨进程断点续执
- :material-clock-outline: **超时与错误策略** —— 节点级/流程级 ISO 8601 超时、`abort`/`continue`/`continue_with` 错误策略与重试
- :material-bell-ring-outline: **事件系统** —— EventBus 抽象；生产推荐 memory（测）/ redis（跑），sqlalchemy 为 experimental
- :material-language-python: **Python 同构** —— 基于 Pydantic，全异步内核，3.10+

## 快速上手

```bash
pip install plaita
```

```json
{
    "flow_id": "echo",
    "inputType": { "dataType": "object" },
    "nodes": [
        { "type": "start", "id": "start", "next": "end" },
        { "type": "end", "id": "end", "output": "$INPUT.name", "resultType": "success" }
    ]
}
```

```python
from plaita import Flow

flow = Flow.from_string(open("echo.json").read())
print(flow.run(name="kongjie"))  # => "kongjie"
```

>>> 想要更完整的入门？前往 [快速开始](guide/quickstart.md)。

## 它适合做什么

| 场景 | 推荐模式 | 说明 |
|------|----------|------|
| AI Agent 多步工具编排 | Normal / Distributed | LLM 规划 + plaita 执行；`parallel`/`child` 多工具与子 Agent，`approval` 人工介入 |
| 运行期执行 AI 生成的流程 | Normal + `flow_from_source` | `@flow` 源码编译期校验，再交给运行时，比直接生成可执行代码更安全 |
| 请求-响应的即时逻辑 | Normal | 同步阻塞，一次返回结果 |
| 流程可视化调试器 | Generator | 每节点 yield，可单步、检查上下文；天然即 Agent trace |
| 人工审批 / 外部回调 / 定时延迟 | Distributed | 挂起-持久化-事件恢复（见 [可靠性边界](distributed/flow-worker.md#可靠性边界必读)） |
| 跨进程挂起/恢复的长时流程 | Distributed | 上下文序列化后 resume；任务队列 at-least-once（须幂等） |

详见 [应用场景](scenarios/index.md)。

## 我想…

=== "快速上手"

    5 分钟跑通第一个流程：[快速开始 →](guide/quickstart.md)

    选择适合你的流程编写方式（@flow / JSON / YAML / Builder）：[流程编写方式 →](guide/flow-authoring.md)

=== "构建 AI Agent"

    让 LLM 规划 + plaita 执行，用 `@flow` / `flow_from_source` 安全地运行 AI 生成的流程：

    [Agent 编排场景 →](scenarios/agent-orchestration.md) &nbsp;·&nbsp; [@flow DSL →](guide/code-dsl.md)

=== "做长时工作流"

    人工审批、外部回调、定时延迟、消息队列触发：

    [断点续执 →](distributed/index.md) &nbsp;·&nbsp; [审批流示例 →](scenarios/approval-flow.md)

=== "调试 & 可视化"

    单步执行、检查中间状态、构建调试器：

    [Generator 模式 →](guide/execution-modes.md#generator) &nbsp;·&nbsp; [生成器调试器示例 →](scenarios/debug-with-generator.md)

---

## 文档导览

| 章节 | 内容 |
|------|------|
| [指南](guide/index.md) | 安装、快速开始、流程编写、表达式、执行模式、错误处理、回调、调试 |
| [架构](architecture/index.md) | 分层设计、执行引擎、状态管理、时序图 |
| [节点系统](nodes/index.md) | 内置节点手册、自定义节点、注册表与插件 |
| [AI 集成](ai/index.md) | MCP、工具节点与数据源、ReAct/FoT Agent、Skill |
| [断点续执](distributed/index.md) | Checkpoint、事件系统、FlowWorker、扩展节点 |
| [应用场景](scenarios/index.md) | Agent 编排、HTTP 集成、审批流等端到端示例 |
| [API 参考](api/index.md) | 由源码 docstring 自动生成 |
