# 命令行

plaita 提供少量命令行入口；AI 集成相关命令由 **`plaita-ai`** 包提供。

## plaita 核心

### 查看版本

```bash
python -m plaita
# 输出版本号（当前 0.5.0）
```

!!! note "演示脚本不在 wheel 内"

    `examples/` 目录**不随 wheel 分发**，`python -m examples.*` 需 clone 仓库后在仓库根目录运行：

    ```bash
    git clone https://github.com/jeffkit/plaita.git
    cd plaita
    python -m examples.event_demo.demo_eventbus --backend memory

    pip install plaita[server]
    python -m examples.server_demo.extended_nodes_demo
    ```

### 外延服务入口

```bash
python -m plaita.server.services --help
```

!!! note "`plaita dist-node` 已移除"

    `plaita dist-node` 命令行工具已从本运行时仓库移除，不再内置节点分发能力。

## plaita-ai CLI

安装：`pip install plaita-ai`

```bash
plaita-ai compile flow.py                 # 编译 @flow → IR JSON
plaita-ai run flow.py --input '{"x":1}'   # 编译并执行
plaita-ai list-nodes                      # 已注册节点类型
plaita-ai skill                           # 打印 flow-coder skill
plaita-ai mcp                             # 启动 MCP stdio 服务
```

### 工具清单

```bash
plaita-ai tools validate tools.yaml [--resources resources.yaml]
plaita-ai tools list tools.yaml [--resources resources.yaml]
```

MCP 启动时可挂载工具清单：

```bash
plaita-ai mcp --tools tools.yaml --resources resources.yaml
# 或环境变量 PLAITA_TOOLS / PLAITA_RESOURCES
```

详见 [工具节点与数据源](../ai/tools.md)、[MCP 服务](../ai/mcp.md)。

### LLM benchmark（开发用）

需从源码 checkout 运行：

```bash
plaita-ai llm-benchmark --agent both --out-dir runs
```

## 下一步

- [安装](../guide/installation.md)
- [AI 集成](../ai/index.md)
- [断点续执 - 外延服务](../distributed/services.md)
