---
title: plaita —— Plaita 逻辑编排系统 Python 运行时
description: 在 Python 中解析并执行 JSON 定义的逻辑流程，支持同步、生成器与分布式三种执行模式，及断点续执、事件驱动恢复。
---

# plaita

**Plaita 逻辑编排系统的官方 Python 运行时。**

plaita 将 JSON 格式的逻辑流程定义解析为可执行的 `Flow`，并以插件化的 `Node` 体系驱动执行。它把**流程定义**与**执行逻辑**分离，支持同步阻塞、生成器单步、分布式断点续执三种执行模式，能覆盖从"一次请求即返回"到"跨进程长时运行工作流"的全频谱场景。

## 核心特性

- :material-graph-outline: **JSON 流程定义** —— 用节点 + `next`/分支描述控制流，平台可视化编排或手写皆可
- :material-cube-outline: **插件化节点** —— 内置 17 种节点，自定义节点只需实现 `execute`，支持 entry_points 自动发现
- :material-robot-outline: **Agent 编排运行时** —— LLM 规划 + 流程执行；`@flow` / S-expr / JSON 三种前端均有构建期校验，`flow_from_source` 让 AI 生成的流程编译期就拦错
- :material-sync: **三种执行模式** —— Normal 同步、Generator 单步调试、Distributed 跨进程断点续执
- :material-clock-outline: **超时与错误策略** —— 节点级/流程级 ISO 8601 超时、`abort`/`continue`/`continue_with` 错误策略与重试
- :material-bell-ring-outline: **事件系统** —— EventBus 抽象 + memory/redis/sqlalchemy 后端，支撑长时工作流挂起与恢复
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
| 人工审批 / 外部回调 / 定时延迟 | Distributed | 挂起-持久化-事件恢复 |
| 跨服务编排、容错长时工作流 | Distributed | 上下文序列化跨进程恢复 |

详见 [应用场景](scenarios/index.md)。

## 文档导览

- [指南](guide/index.md) —— 从安装到表达式、执行模式、错误处理、回调、调试
- [架构](architecture/index.md) —— 分层设计、执行引擎、状态管理、时序图
- [节点系统](nodes/index.md) —— 内置节点手册、自定义节点、注册表与插件
- [断点续执](distributed/index.md) —— Checkpoint、事件系统、FlowWorker、扩展节点
- [应用场景](scenarios/index.md) —— Agent 编排、HTTP 集成、审批流等端到端示例
- [API 参考](api/index.md) —— 由源码 docstring 自动生成
