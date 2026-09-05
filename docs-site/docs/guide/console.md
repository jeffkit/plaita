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

## 本地单机模式（零依赖上手）

**不装 Redis 也能用**：console 启动时若 Redis 不可达，自动进入本地单机模式——
流程在 console 进程内执行、执行历史与节点级 trace 存 SQLite，首次启动还会
写入 3 个示例流程（快速开始 / 循环与映射 / HTTP 调用）。

本地模式的边界：集群管理、任务队列、事件挂起/恢复（审批等）不可用，对应
页面会明确提示；恢复方式就是启动 Redis 并重启 console。

## 调试：数据固定与单节点重放

试跑面板支持 n8n 式调试：

- **节点级 IO 检视**：点击时间线节点展开完整输入/输出 JSON（不再截断）
- **数据固定（pin）**：把某次试跑的节点输出固定下来，后续试跑跳过该节点
  真实执行（HTTP 调用等重副作用节点尤其实用），面板以 mock 高亮
- **仅运行此节点**：上游取固定值、下游 mock 化无副作用，只真实执行目标节点

固定值只在试跑内存里，不会写进流程定义。

## 凭据

外部服务机密（webhook 地址、数据库密码等）集中在「凭据」页管理：Fernet
加密落库，流程节点按名引用（`credential: "feishu-bot"`），流程定义里不落
明文。保存后自动导出加密文件供引擎节点运行时解密；console 拉起的
flow_worker 自动带上密钥环境。

密钥来源：`PLAITA_CREDENTIALS_KEY` 环境变量，或自动生成于 DB 同目录的
`.plaita-credentials.key`。多机部署请统一注入同一密钥。

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
