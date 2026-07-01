# 应用场景

本章节用端到端可运行示例把概念串起来，覆盖 plaita 最典型的几类用法。每个场景都给出完整流程 JSON 与驱动代码。

## 场景一览

| 场景 | 模式 | 关键能力 | 适用动机 |
|------|------|---------|---------|
| [Agent 编排](agent-orchestration.md) | Normal / Distributed | LLM 规划 + 流程执行、`@flow` 运行期生成、`parallel`/`child`/HITL | AI Agent 多步工具编排 |
| [回声流程](echo.md) | Normal | 最小流程、本地/远程执行 | 入门、即时逻辑 |
| [HTTP 集成](http-integration.md) | Normal | `http` 节点、超时、错误策略 | 调外部 API |
| [审批流](approval-flow.md) | Distributed | `approval` 节点、挂起/恢复、FlowWorker | 人工审批长时工作流 |
| [队列触发](queue-trigger.md) | Distributed | `redis_queue`/`kafka_queue`、外延服务 | 等消息到达再继续 |
| [生成器调试器](debug-with-generator.md) | Generator | `flow.debug()` + `FlowCallback` | 可视化单步调试 |

## 阅读建议

- 刚入门：从 [回声流程](echo.md) 开始
- 做 AI Agent：看 [Agent 编排](agent-orchestration.md) —— LLM 规划 + plaita 执行
- 要调外部服务：看 [HTTP 集成](http-integration.md)
- 要做长时工作流：看 [审批流](approval-flow.md) 与 [队列触发](queue-trigger.md)
- 要做调试器/观测：看 [生成器调试器](debug-with-generator.md)
