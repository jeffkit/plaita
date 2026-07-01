# plaita 文档

欢迎阅读 `plaita` 技术文档，这是 Plaita 逻辑编排系统的 Python 运行时。

## 目录

-   **[系统架构](architecture.md)**：深入了解系统设计、核心组件和执行流程，包含详细的架构图和时序图。
-   **[断点续执架构](checkpoint-architecture.md)**：详细介绍断点续执（Checkpoint）功能的架构设计，支持长时间运行工作流的挂起与恢复。
-   **[使用指南](usage.md)**：介绍如何安装 `plaita`、编写流程定义，以及在应用中执行流程。
-   **[扩展指南](extension.md)**：学习如何通过创建和注册自定义节点来扩展系统。

## 快速开始

### 安装

```bash
pip install plaita
```

### 基本使用

```python
from plaita import Flow

# 加载流程定义
with open('echo.json', 'r') as f:
    flow = Flow.model_validate_json(f.read())

# 执行流程
result = flow.run(name="世界")
print(result)  # 输出: 世界
```

更多详情请查看 [使用指南](usage.md)。

## 核心概念

| 概念 | 说明 |
|------|------|
| **Flow（流程）** | 由多个节点组成的工作流定义 |
| **Node（节点）** | 流程中的最小执行单元 |
| **Context（上下文）** | 流程执行时的状态存储 |
| **Expression（表达式）** | 用于引用上下文中的数据，如 `${INPUT.name}` |

## 运行模式

`plaita` 支持三种运行模式：

- **Normal（普通模式）**：同步阻塞执行，适用于快速流程
- **Generator（生成器模式）**：支持单步调试和状态检查
- **Distributed（分布式模式）**：支持跨进程执行长时间工作流

详情请参阅 [系统架构 - 执行模式](architecture.md#执行模式)。

