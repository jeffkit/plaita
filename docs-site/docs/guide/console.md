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

## 企业能力：RBAC / 审计 / 环境晋升

**RBAC**：`PLAITA_CONSOLE_ADMIN_PASSWORD` 未设时，首次启动自动生成 admin
账号（随机密码打印到日志，请立即修改）。角色三级：

- **admin**：全部权限（用户管理 / 凭据 / 集群 / 审计 / 生产环境删除）
- **editor**：编排与执行（增删改流程、发布、试跑、启动执行）
- **viewer**：只读

登录后走会话 token（7 天）；`X-Admin-API-Key` 服务账号兼容（等价 admin）。
角色/密码变更即时撤销旧会话。「用户」页管理账号与角色。

**审计**：「审计」页留痕管理面敏感操作（发布、保存、删除、执行、凭据变更、
用户管理），含操作人 / IP / 时间 / 元数据——机密内容永不入审计。

**环境晋升**：console 实例经 `PLAITA_CONSOLE_ENV` 标识环境（dev/test/prod）：

- 每次发布写部署记录（环境、操作人、定义 SHA-256 指纹）
- 「导出晋升包」→ 目标环境 console「导入」：指纹校验防篡改，导入为草稿
  再发布，形成 dev → prod 的受控晋升
- `PLAITA_CONSOLE_ENV=prod` 时删除操作需要 admin

## 本地单机模式（零依赖上手）

**不装 Redis 也能用**：console 启动时若 Redis 不可达，自动进入本地单机模式——
流程在 console 进程内执行、执行历史与节点级 trace 存 SQLite，首次启动还会
写入 3 个示例流程（快速开始 / 循环与映射 / HTTP 调用）。

本地模式的边界：集群管理、任务队列、事件管理不可用（对应页面明确提示）；
审批/事件的挂起-恢复支持单进程语义（checkpoint 存 SQLite，重启后仍可显式
恢复）。完整对比见下文与 [部署模式](deployment.md)。

四档部署形态（本地单机 / 单机标准 / 分布式 / SDK 嵌入）的完整对比与升级路径
见 [部署模式](deployment.md)。

## 调试：数据固定与单节点重放

试跑面板支持 n8n 式调试：

- **节点级 IO 检视**：点击时间线节点展开完整输入/输出 JSON（不再截断）
- **数据固定（pin）**：把某次试跑的节点输出固定下来，后续试跑跳过该节点
  真实执行（HTTP 调用等重副作用节点尤其实用），面板以 mock 高亮
- **仅运行此节点**：上游取固定值、下游 mock 化无副作用，只真实执行目标节点

固定值只在试跑内存里，不会写进流程定义。

## 连接器

`pip install plaita-nodes` 自带的连接器节点（凭据按名引用，机密不落流程定义）：

| 节点 | 用途 | 凭据数据 |
|------|------|----------|
| `feishu_webhook` / `wecom_webhook` / `slack_webhook` / `dingtalk_webhook` | IM 群通知 | `{"url": ...}`；钉钉可加 `"secret"` 自动加签 |
| `generic_webhook` | 任意 Webhook POST | `{"url": ...}` |
| `api_request` | 通用 REST（静态 Header 鉴权） | `{"base_url": ..., "headers": {...}}` |
| `sql_query` | SQL 查询/写入（SQLAlchemy） | `{"url": "postgresql://..."}` 或 host/port/user/password/database |
| `email_send` | SMTP 邮件 | `{"host", "port", "username", "password", "use_tls"/"use_ssl"}` |

新增连接器即一个新的节点类 + entry-point 注册，无需改动编排台。

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
