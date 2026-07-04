# 使用指南

本章节带你从零开始使用 plaita。

## 选择你的路径

=== "我是初学者"

    **目标：** 跑通一个流程，理解基本概念。

    1. [安装](installation.md) —— pip 安装，验证版本
    2. [快速开始](quickstart.md) —— 5 分钟跑通 echo 流程
    3. [流程编写方式](flow-authoring.md) —— 了解 JSON / YAML / DSL 的区别，选一种适合自己的
    4. [表达式](expressions.md) —— 学会用 `$INPUT` / `$NODE` / `$F.func()`
    5. [执行模式](execution-modes.md) —— 理解 Normal / Generator / Distributed 三种模式

=== "我在构建 AI Agent"

    **目标：** 让 LLM 规划 + plaita 执行，或让 AI 生成可运行的流程。

    1. [快速开始](quickstart.md) —— 先跑通基础示例
    2. [流程编写方式](flow-authoring.md) —— 重点看 **@flow + `flow_from_source`** 那一列
    3. [S-expr 与 @flow DSL](code-dsl.md) —— AI 生成友好的两种格式详解
    4. [应用场景 - Agent 编排](../scenarios/agent-orchestration.md) —— 端到端的 LLM + plaita 架构
    5. [AI 集成（plaita-ai）](../ai/index.md) —— MCP 服务、内置 ReAct/FoT Agent、flow-coder skill
    6. [执行模式](execution-modes.md) —— 了解 Distributed 模式，支持 HITL（人工介入）

=== "我要做长时工作流"

    **目标：** 实现人工审批、外部回调、定时延迟、消息队列触发等场景。

    1. [快速开始](quickstart.md) —— 先跑通基础示例
    2. [执行模式](execution-modes.md) —— 理解 Distributed 模式
    3. [断点续执](../distributed/index.md) —— Checkpoint、EventBus、FlowWorker
    4. [应用场景 - 审批流](../scenarios/approval-flow.md) —— 带完整代码的审批流示例
    5. [应用场景 - 队列触发](../scenarios/queue-trigger.md) —— Redis/Kafka 消息触发

=== "我要集成 HTTP / 外部服务"

    **目标：** 在流程中调用外部 API，处理超时、错误与重试。

    1. [快速开始](quickstart.md) —— 先跑通基础示例
    2. [流程编写方式](flow-authoring.md) —— 选择适合的格式（JSON 或 Python DSL）
    3. [错误处理与超时](error-handling.md) —— `timeout`、`abort`/`continue`/`continue_with`
    4. [应用场景 - HTTP 集成](../scenarios/http-integration.md) —— 完整示例

---

## 本章节包含

| 页面 | 内容 |
|------|------|
| [安装](installation.md) | pip 安装，extras，开发依赖 |
| [快速开始](quickstart.md) | echo 示例，本地/异步/远程执行 |
| [流程编写方式](flow-authoring.md) | JSON / YAML / Builder / S-expr / @flow 五种方式对比 |
| [JSON 流程定义](flow-definition.md) | Flow 字段、控制流、条件运算符参考 |
| [YAML 与 Python Builder DSL](yaml-and-dsl.md) | YAML 语法，Builder 链式调用，`linear` 简写 |
| [S-expr 与 @flow DSL](code-dsl.md) | Lisp 风格 S-expr，@flow AST 编译，`flow_from_source` |
| [表达式](expressions.md) | `$INPUT`/`$NODE`/`$F.func()`，60+ 内置函数 |
| [执行模式](execution-modes.md) | Normal / Generator / Distributed |
| [错误处理与超时](error-handling.md) | ISO 8601 超时，错误策略，重试 |
| [回调机制](callbacks.md) | `FlowCallback` 生命周期钩子 |
| [调试](debugging.md) | Generator 模式单步，日志，常见问题 |
