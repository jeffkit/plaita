# 编排台（Console）

编排台是 plaita 的可视化控制台：拖拽画布编排流程、schema 驱动的节点配置表单、
Dry-Run 试运行、执行历史与集群管理。后端为 FastAPI，前端随 pip 包分发，**安装即用**。

## 安装与启动

```bash
pip install plaita-console            # 含后端 API 与打包好的前端页面
pip install "plaita-console[nodes]"   # 连同通用业务节点插件一起安装
python -m plaita_console              # 或使用 plaita-console 命令
```

启动后打开 <http://localhost:8080>。前置条件只有一个：**Redis 可达**
（默认 `redis://localhost:6379/0`，经 `PLAITA_CONSOLE_REDIS_URL` 覆盖）。

从源码开发则克隆仓库，后端 `python run.py`、前端 `pnpm dev`，见仓库内
`plaita-console/README.md`。

## 配置

全部经 `PLAITA_CONSOLE_*` 环境变量配置，常用项：

| 变量 | 说明 |
|------|------|
| `PLAITA_CONSOLE_HOST` / `PLAITA_CONSOLE_PORT` | 监听地址，默认 `0.0.0.0:8080` |
| `PLAITA_CONSOLE_REDIS_URL` | Redis 连接 |
| `PLAITA_CONSOLE_DB_URL` | 流程定义持久化（默认本地 SQLite） |
| `PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN=true` | 本地开发免密；局域网/生产请改设 `PLAITA_CONSOLE_ADMIN_API_KEY` |
| `PLAITA_CONSOLE_SECRET_ID` / `SECRET_KEY` | 对外契约接口的 HMAC 密钥 |

## 主要能力

- **流程编排**：拖拽画布 + 节点面板；节点配置面板由节点 schema 自动生成表单
  （表达式字段带 `$INPUT` / `$NODE` 变量插入），复杂结构回落 JSON 编辑
- **子流程**：map / loop / while / parallel 分支等进入子画布编辑，面包屑返回
- **Dry-Run**：保存前在控制台进程内试运行并查看逐步输出
- **执行与集群**：执行历史、集群页一键拉起 flow_worker / 调度 / 事件服务等实例
  （`cluster_config.yaml` 定义服务模板与实例数）
- **Copilot**：AI 辅助生成流程（`PLAITA_CONSOLE_COPILOT_BRAIN` 可选大脑）

## 节点插件

节点能力三来源：

1. **内置 22 种**（start / if / switch / loop / map / http / event / parallel 等）——随引擎自带
2. **插件包** `pip install plaita-nodes`——agentrun / hitl / gate 等，经
   `plaita.nodes` entry-points 自动注册
3. **自定义**——参照[自定义节点](../nodes/custom.md)编写后，以同样机制打包分发

控制台「节点」页实时展示当前已注册的全部类型及其 schema。
