# plaita Documentation / plaita 文档

Welcome to the technical documentation for `plaita`, the Python runtime for the Plaita logic orchestration system.

欢迎阅读 `plaita` 技术文档，这是 Plaita 逻辑编排系统的 Python 运行时。

---

## Language / 语言选择

### English

-   **[Architecture](en/architecture.md)** - System design, core components, and execution flow
-   **[Checkpoint Architecture](en/checkpoint-architecture.md)** - Long-running workflow suspend and resume mechanism
-   **[Usage Guide](en/usage.md)** - Installation, flow definitions, and execution
-   **[Extension Guide](en/extension.md)** - Creating and registering custom nodes

### 中文

-   **[系统架构](zh/architecture.md)** - 系统设计、核心组件和执行流程
-   **[断点续执架构](zh/checkpoint-architecture.md)** - 长时间工作流的挂起与恢复机制
-   **[使用指南](zh/usage.md)** - 安装、流程定义和执行
-   **[扩展指南](zh/extension.md)** - 创建和注册自定义节点

---

## Quick Start / 快速开始

```bash
pip install plaita
```

```python
from plaita import Flow

with open('flow.json', 'r') as f:
    flow = Flow.model_validate_json(f.read())
    result = flow.run(name="World")
    print(result)
```

---

## Project Links / 项目链接

- **Source Code / 源代码**: [jeffkit/plaita](https://github.com/jeffkit/plaita)
- **Documentation / 文档**: [jeffkit.github.io/plaita](https://jeffkit.github.io/plaita/)
