# 命令行

plaita 提供少量命令行入口，主要用于版本检查与演示。

## 查看版本

```bash
python -m plaita
```

输出版本号（当前 `0.3.16`），可用于验证安装。

## 事件系统演示

内置一个事件总线演示脚本（内存后端），便于理解发布/订阅：

```bash
python -m examples.event_demo.demo_eventbus --backend memory
```

> 该脚本在覆盖率配置中被 omit，仅供演示，不属于稳定 CLI。

## 扩展节点演示

`server` extra 提供一个扩展节点演示：

```bash
pip install plaita[server]
python -m examples.server_demo.extended_nodes_demo
```

## 外延服务入口

`plaita.server.services` 提供启动 `ServiceManager` 的入口（需 `server` extra）：

```bash
python -m plaita.server.services --help
```

## 已移除的 CLI

!!! note "`plaita dist-node` 已移除"

    `plaita dist-node` 命令行工具已从本运行时仓库移除，不再内置节点分发能力。

## 下一步

- [安装](../guide/installation.md)
- [断点续执 - 外延服务](../distributed/services.md)
