# Plaita Console

Plaita 流程引擎可视化管理控制台

## 功能特性

- **服务拓扑视图** - 实时展示集群服务结构与关联关系
- **服务生命周期管理** - 启动/停止各类服务
- **执行实例管理** - 流程执行的查看、启动、停止
- **实时监控** - 执行状态、日志流、任务队列
- **可视化流程查看器** - 图形化展示流程执行进度
- **可视化 Flow 编排** - 浏览器内拖拽编排 Plaita 流程（节点面板 + 画布 + 配置抽屉 + 试跑），保存为带 semver 版本号的流程定义，支持草稿/发布版本管理
- **节点管理** - 查看内置节点 schema、注册/删除自定义节点描述
- **多租户** - 用户可属多个租户（成员制 + 会话内切换），流程/版本/执行/调度/凭据/自定义节点/审计按租户隔离；平台管理员跨租户管理与全量视角。本地单机档与集群档均支持（集群档经消息租户 + 键 namespace 隔离，见下方"多租户边界"）
- **对外契约接口** - 暴露 `/api/flowVersion/semver/detail`（HMAC 鉴权），供 `plaita.PlaitaClient` 拉取已发布流程定义；支持按租户签发契约密钥（租户只能拉本租户流程），全局密钥仍兼容平台级拉取

## 多租户说明

- **模型**：`tenants` + `tenant_members`（用户↔租户多对多，租户内 admin/editor/viewer 角色）；`users.platform_admin` 为平台管理员（管理租户/成员，可带 `X-Tenant-ID` 头跨租户操作）。
- **会话**：登录响应含 `memberships` 与 `active_tenant`；`POST /api/auth/switch-tenant` 切换活跃租户；前端侧边栏提供租户切换器。成员变更即吊销该用户全部会话。
- **无活跃租户 fail-closed（#254）**：普通用户会话若没有任何有效租户成员关系（典型：被移出租户后重新登录），管理 API 一律 **403「当前会话无活跃租户」**——不回退 `users.role` 遗留全局角色，不把 `None` 当作平台全量视角，写路径也不回退 `default`。只有可证明的平台级身份（`PLAITA_CONSOLE_ADMIN_API_KEY` / `users.platform_admin` / insecure 开发开关）享有平台全量视角与写路径 default 回退。
- **租户角色 ≠ 平台身份（#260 / #261 / #263）**：`/api/users` 的全局账号变更（创建 / 改角色 / 改密 / 删除）要求 `users.platform_admin`（或平台级 API-Key）；租户 admin 只能经 `/api/tenants/{tid}/members/*` 管理**本租户**成员，不能操作 `platform_admin=True` 的账号，`GET /api/users` 也只返回本租户成员。全局服务停止（`POST /api/services/{id}/stop`）同样只允许平台身份。**`/api/cluster/*` 与 `/api/clusters/*` 的全部端点（含 GET 读面）一律要求平台身份**——基础设施连接串 / 完整集群配置没有租户内语义；响应再经序列化层脱敏（URL 口令段与 `PASSWORD`/`SECRET`/`TOKEN` 等 env 键值换 `***`）作为纵深防御。`/api/dispatch/ledger` 对非平台身份按本租户过滤（显式传他租户 403 + 审计），平台身份默认全量。任何已认证角色都可以修改自己的密码；被拒绝的尝试进审计（`*.denied`）。
- **隔离**：流程/版本/执行/调度/凭据/自定义节点/属性类型/Copilot 会话/审计/部署记录全部携带 `tenant_id`，查询按会话租户过滤，跨租户访问即 not-found。存量库首次多租户启动时自动迁移：建 `default` 租户、回填数据、legacy admin 提升为平台管理员。
- **凭据明文归属（#254）**：`GET /api/credentials/{name}` 返回明文前校验租户归属——租户上下文必须命中本租户记录，跨租户同名凭据返回 404（不泄露存在性与明文）。
- **租户内唯一**：`flow_id`、凭据名、自定义节点 type、属性类型名的唯一性均为租户内唯一（同名流程可共存于不同租户）。
- **停用租户对运行面生效（#27）**：`POST /api/tenants/{id}/status` 置 `disabled` 后，该租户已签发会话的 API 调用按请求实时返回 **403**（无需等会话过期；平台管理员不受限，否则无法重新启用），其调度被置 `enabled=false`、`schedule_service` 不再入队。集群档还会把状态写入 Redis `plaita:tenant_status`，供 worker 丢弃该租户的 start/resume（含挂起执行的恢复）——跨 Redis 部署该闸门不生效（见 `docs-site/docs/distributed/ops-runbook.md`）。
- **删除租户清理（#245/#255/#257/A54-01/03/04/05/06/09）**：`DELETE /api/tenants/{id}` 先校验租户内无流程（有 flow 拒绝，拒绝门先于一切清理）。随后同一事务删除 `credentials`/成员/租户行、本地档四表（`local_schedules`/`local_schedule_fires`/`local_executions`/`local_logs`）与租户级 DB 资产（自定义节点描述/属性类型/Copilot 会话元数据/部署记录）；凭据旁文件 `credentials_file(tenant_id)` 在 commit **之前**删除（失败即回滚：租户行还在、可重试，绝不出现「租户已删、旧密钥仍可读」；旧版本留下的 404 残留由 404 路径补清扫）。集群档清理 `{ns}:*` 租户命名空间（执行状态/取消/fence/租约/重试计数/start-dedup、flow 发布定义）、平台键里归属该租户的调度/事件记录/事件订阅，**并按条目 `tenant_id` 清理平台级日志 Stream `plaita:logs:*`**；任务队列按**键集合**排空（console 队列 + `PLAITA_QUEUE_NAME`/`QUEUE_NAME` + `PLAITA_CONSOLE_TENANT_PURGE_QUEUES` allowlist + Redis 中已存在的同族版本队列，如 `plaita:flow:queue:v2`），每条队列只删能明确归属的条目。删除后写 Redis `plaita:tenant_status=disabled` 墓碑——在途 start/resume 与旧 enabled 调度不再执行；同名重建回填 `active`。`save_credential` 拒绝已删/不存在的租户（堵迟到写复活 store）。清理失败显式报错；重建同名租户读不到旧凭据/执行/日志/调度/节点/属性类型/Copilot/部署。审计 `audit_logs` **不物理删除**（合规证据保留），但租户 admin 的 `/api/audit` 只返回**当前世代**（`tenants.created_at` 之后）的记录，平台身份可全量审计。404 补清扫遇到残留 flow（租户行已不在）时返回 **409 具名拒绝**，要求显式归档/清除，绝不静默留给同名重建；`create_tenant` 对任何前代遗留数据（flow/节点/属性类型/Copilot/部署）同样拒绝同名继承。
- **服务读面脱敏（#265）**：`GET /api/services` 的合成基础设施/组件条目在响应序列化层对连接串做脱敏（URL userinfo 口令、凭据键值换 `***`，保留 scheme/host/port/path/db）——租户 viewer 仍可看实例状态与非敏感字段，但读不到口令；`/api/services/{id}` 与 `/api/services/topology` 走同一脱敏层。
- **派发台账租户/世代隔离（A54-02）**：`GET /api/dispatch/ledger` 对平台身份返回全量台账与全局队列对账；租户身份只返回**当前世代**的本租户条目（条目含 `tenant_generation`，缺失世代的旧行 fail-closed 不可见），不返回全局队列计数（`queue_length=null`、`error` 说明对账仅平台身份可用）。
- **请求级租户世代（A51-02）**：会话解析用**一条 JOIN** 同时取成员资格/角色与 `tenant.created_at + contract_secret_id`（认证快照）。请求级依赖把该世代绑定到 ContextVar；`save_credential`/`add_member` 的首次捕获与所有租户作用域写路径（成员角色/移除、flow/版本/节点描述/属性类型、本地执行/调度、凭据删除）在 **flush 后同一事务内**复核「请求世代 == 当前行世代」，delete+同名 create 跨 ABA 的旧请求写入重建租户会以 `TenantGenerationMismatch`/`UserError` 失败。平台身份带 `X-Tenant-ID` 时认证处读一次租户行（缺失 403）；未挂依赖的 CLI/单测回退「首次读捕获」存量行为。
- **执行删除持久性（A43-01）**：`DELETE /api/executions/{id}` 只对终态开放；running/suspended 默认 **409**（指引先 cancel），显式 `force=true` 才「先取消再删」。集群档经 `delete_execution_state` 在同一次删除里清掉状态键、列表索引成员、取消标志、事件频道，并**吊销 `{ns}:execution:fence:{id}` 与 `{ns}:execution:lease:{id}`**——在途 worker 的下一次 fenced 落盘因世代失配抛 `ExecutionLeaseError`，不会把已删执行写回（删除后复活）。本地档 `force` 删除置位取消事件并有界等待执行线程收口；超时未退出时登记墓碑丢弃其后续日志写入，随后删除该执行的 `local_logs` 与 `local_executions` 行，删除后不可查。`FencedExecutionStorage.delete_execution_state` 在持世代号时同样做 fence CAS，旧世代不能删除新世代的状态（A43-07）。A51-01/A52-01 起删除还会写**平台根执行墓碑** `plaita:exec_tombstone:{id}`（与四删同 pipeline/Lua），worker 在派发/直达启动/重投/死信守卫克隆前按同租户复核——删除后仍留在队列或 PEL 里的旧 `start` 载体不再重建执行；新 `execution_id` 正常放行，同 id 重放被丢弃。
- **契约密钥**：每个租户一组 `contract_secret_id/contract_secret_key`（租户管理页创建/轮换，明文仅显示一次），只能拉取本租户已发布流程；全局 `PLAITA_CONSOLE_SECRET_ID/SECRET_KEY` 仍可用（平台上下文，可拉任意租户），存量集成不破坏。
- **多租户边界（如实说明）**：集群档的租户数据（流程定义、执行状态、resume 租约）按键 namespace 隔离（`plaita:{tenant}:*`，default 租户保持历史前缀 `plaita:*` 以兼容存量数据），任务消息携带 `tenant_id`（缺省视为 default），引擎日志/调度值/订阅内嵌租户字段供 console 过滤。仍保持平台级共享的：任务队列 Stream（机制通道，租户在消息内）、事件总线存储与 pubsub（跨租户事件总线，订阅按租户过滤；删除租户时按记录 `tenant_id` 清理该租户条目）、服务注册/控制通道。本地档凭据按租户导出到旁文件（`.plaita-credentials.<tenant>.json`），default 租户沿用历史文件路径。
- **滚动升级兼容**：消息缺 `tenant_id` 按 default 处理；default 租户流量走历史键前缀——旧 worker + 新 console（或反向）混跑时 default 租户不受影响，非 default 租户需双侧升级。

## 快速开始

### 使用 Docker Compose

```bash
cd plaita-console/docker
docker-compose up -d
```

访问 http://localhost:5173 打开控制台。

### 本地单机档（无需 Redis）

不想装 Redis 时，后端可直接以本地单机模式跑起来（启动时 ping 不通 Redis 会自动降级，日志里会有明确提示）：

```bash
cd plaita-console/backend
pip install -r requirements.txt
python run.py
```

流程定义、版本、执行历史、节点 trace 全部存 SQLite（默认 `./plaita_console.db`，可用 `PLAITA_CONSOLE_DB_URL` 换库）；流程在 console 进程内的后台线程执行。想强制本地档（即使本机有 Redis 也不连），把 Redis URL 指向一个不可达地址即可：

```bash
PLAITA_CONSOLE_REDIS_URL=redis://127.0.0.1:1/0 python run.py
```

本地档能力边界（与集群档的差异，如实说明）：

- **可用**：流程编排/发布、本地执行与节点级 trace、挂起-恢复（审批/事件节点，checkpoint 存 SQLite）、日志/执行页、内置调度循环（替代集群档的 schedule_service 进程）、HMAC 契约接口 `/api/flowVersion/semver/detail`。
- **UI 入口**：本地档只起后端 API；干净 clone 下 `backend/webDist/` 不存在（gitignore），访问 `/` 不会有前端页面。要看控制台界面，另起前端开发服务（`cd frontend && pnpm install && pnpm dev`，访问 http://localhost:5173），或运行 `scripts/build_package.sh` 生成 webDist。
- **用 PlaitaClient 拉流程**：需要 `requests`（`pip install "plaita[http]"` 或 `pip install requests`），缺依赖时 plaita 会给出可操作的安装指引。
- **不可用**：集群/服务拓扑/服务启停、事件总线、任务队列视图——对应管理 API 返回 503。
- **限制**：执行与恢复必须在同一个 console 进程内；进程重启后进程内事件订阅丢失，挂起的执行需显式 resume 继续；cancel 为尽力而为（只标记状态，不中断正在运行的线程）；HMAC 重放保护退化为进程内 nonce 判定，仅单进程部署有效。

### 启动第三方依赖服务

Plaita Console 依赖 Redis 来存储服务注册信息。可以使用以下方式启动：

```bash
# 方式 1: 使用本地 Redis
redis-server

# 方式 2: 使用 Docker 仅启动依赖服务
cd plaita-console/docker
docker-compose -f docker-compose.deps.yml up -d
```

### 演示模式

如果你只是想测试 Plaita Console 的功能，可以运行演示脚本注册模拟服务：

```bash
# 确保 Redis 已启动，然后运行演示脚本
cd plaita-console
python scripts/demo_services.py
```

这会注册几个模拟服务（FlowWorker、DelayService 等），让你可以在控制台中看到服务拓扑和状态。

### 本地开发

#### 后端

```bash
cd plaita-console/backend

# 安装依赖
pip install -r requirements.txt

# 方式 1: 使用启动脚本（推荐）
python run.py

# 方式 2: 使用 uvicorn
python -m uvicorn main:app --reload --port 8080

# 方式 3: 直接运行
python main.py
```

#### 前端

```bash
cd plaita-console/frontend

# 安装依赖
pnpm install

# 启动开发服务器
pnpm dev
```

访问 http://localhost:5173 打开控制台。

## 项目结构

```
plaita-console/
├── frontend/               # 前端项目 (React + TypeScript + Vite)
│   ├── src/
│   │   ├── components/     # 可复用组件
│   │   ├── pages/          # 页面组件
│   │   ├── hooks/          # 自定义 Hooks
│   │   ├── services/       # API 调用
│   │   └── stores/         # 状态管理
│   ├── package.json
│   └── vite.config.ts
├── backend/                # 后端项目 (FastAPI)
│   ├── api/                # API 路由
│   ├── models/             # 数据模型
│   ├── services/           # 业务逻辑
│   └── main.py
├── docker/                 # Docker 配置
│   ├── docker-compose.yml
│   ├── Dockerfile.frontend
│   └── Dockerfile.backend
└── README.md
```

## API 文档

启动后端后，访问 http://localhost:8080/docs 查看 Swagger API 文档。
Swagger 按 tag 分面：`admin`（管理面）/ `contract`（契约面）/ `health`。

### API 分面

Console 对外暴露两套鉴权面，**路径前缀同为 `/api`，鉴权方式不同**：

| 分面 | 调用方 | 鉴权 | 未配置密钥 |
|------|--------|------|------------|
| **管理面 (admin)** | Console 前端 / 运维 | `X-Admin-API-Key` 或 `Authorization: Bearer` | **503**（除非 `ALLOW_INSECURE_ADMIN=true`） |
| **契约面 (contract)** | 外部 `PlaitaClient` | HMAC（`Authorization` 头，与 `plaita/client.py` 对称） | **503** |
| **健康检查** | 探针 | 无 | — |

路由挂载见 `backend/main.py`：`_mount_admin` / `_mount_contract`。契约面**不加** `require_admin_auth`。

### 管理面 API

| 端点 | 方法 | 描述 |
|------|------|------|
| `/api/services` | GET | 获取所有服务列表 |
| `/api/services/topology` | GET | 获取服务拓扑 |
| `/api/services/{id}/stop` | POST | 停止服务 |
| `/api/executions` | GET | 获取执行列表 |
| `/api/executions` | POST | 启动新执行 |
| `/api/executions/{id}` | DELETE | 删除执行（仅终态；running/suspended 返回 409，需先取消或显式 `force=true`）。集群档同时删除状态/索引/取消标志/事件频道并**吊销 fence/lease**；本地档先取消并等待线程收口，再删除执行行与日志 |
| `/api/executions/{id}/resume` | POST | 恢复执行（`resume_type`=continue/retry/cancel/timeout/event，可选 `data`）；可选 `params`（#157）**只在 `resume_type=continue` 且盘上 checkpoint 为空时**被引擎绑定为 `$INPUT`（手动 continue 抢在 pending start 被 reclaim 之前的窗口）；不传/空/`null` 不落 `params` 键（与 #122 载体形状一致）。**空 checkpoint 下的 `retry`/`cancel`/`timeout`/`event` 与本字段无关**：它们被策略层守卫以 `ResumeGuardError` 拒绝——执行保持 running、消息被 ack 吞掉、`$INPUT` 永不绑定（预存语义，非本单引入）；**checkpoint 非空时 `params` 惰性**，绑定值一律以盘上 checkpoint 为准 |
| `/api/queues` | GET | 获取队列状态（含死信 DLQ 键） |
| `/api/queues/dlq` | GET | 死信总览：各 DLQ Stream 堆积量与最近条目（`?count=`） |
| `/api/metrics` | GET | 集群 Prometheus 文本：队列积压 / DLQ 堆积 / 存活 worker 数 |
| `/api/logs` | GET | 获取日志 |
| `/api/logs/stream` | GET (SSE) | 实时日志流 |
| `/api/flows` | GET / POST | 流程列表 / 新建流程 |
| `/api/flows/{id}` | GET / DELETE | 流程详情（含版本列表）/ 删除流程 |
| `/api/flows/{id}/versions/{ver}` | GET / PUT / DELETE | 取版本 / 保存草稿（Flow 强校验）/ 删除版本 |
| `/api/flows/{id}/versions/{ver}/source` | GET | codeflow 源码视图：metadata.source（权威）优先，缺失时 emit_source 反编译兜底（source_kind 标注 authoritative/decompiled/unavailable） |
| `/api/flows/{id}/publish` | POST | 发布指定版本（draft → published） |
| `/api/flows/dry-run` | POST | 同步试跑 Flow JSON，返回节点级结果 |
| `/api/nodes` | GET / POST | 节点描述列表 / 注册自定义节点 |
| `/api/nodes/{type}` | DELETE | 删除自定义节点（内置不可删） |
| `/api/cluster/*` · `/api/clusters/*` | — | 集群配置 / 启停 / 基础设施（command 白名单）。**全部端点（含读面）要求平台身份**；响应脱敏 URL 口令与 env 密钥（#263） |
| `/api/dispatch/ledger` | GET | 派发台账 + 队列对账；非平台身份只看本租户（#263） |
| `/api/events/*` | — | 事件订阅与查询 |

### 契约面 API

| 端点 | 方法 | 描述 |
|------|------|------|
| `/api/flowVersion/semver/detail` | POST | HMAC 鉴权；`application/x-www-form-urlencoded`（`flowId`/`version`）；返回已发布流程定义供 `PlaitaClient` 拉取 |

## 技术栈

### 前端
- React 18
- TypeScript
- Vite
- TailwindCSS
- @xyflow/react 12 (流程编排画布与拓扑图)
- TanStack Query (数据获取)
- Zustand (状态管理)

### 后端
- FastAPI
- Pydantic
- Redis
- SQLAlchemy (流程定义/版本/节点描述持久化)
- SSE (Server-Sent Events)

## 环境变量

### 后端

| 变量名 | 默认值 | 描述 |
|--------|--------|------|
| `PLAITA_CONSOLE_REDIS_URL` | `redis://localhost:6379/0` | Redis 连接 URL |
| `PLAITA_CONSOLE_HOST` | `127.0.0.1` | 监听地址（**默认仅本机**；对外部署显式设 `0.0.0.0`，启动日志会打 warning，见 #208） |
| `PLAITA_CONSOLE_PORT` | `8080` | 监听端口 |
| `PLAITA_CONSOLE_DEBUG` | `false` | 调试模式 |
| `PLAITA_CONSOLE_DB_URL` | `sqlite:///./plaita_console.db` | 流程定义/版本/节点描述持久化（SQLAlchemy） |
| `PLAITA_CONSOLE_ADMIN_API_KEY` | _空_ | 管理面 API Key（`X-Admin-API-Key` / Bearer）。**空且未放开 insecure 时管理 API 返回 503** |
| `PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN` | `false` | 显式允许无管理密钥启动（**仅本地开发**） |
| `PLAITA_CONSOLE_SECRET_ID` | _空_ | 对外契约 HMAC secret-id（**空则 `/api/flowVersion` 返回 503**，不再接受空串签名） |
| `PLAITA_CONSOLE_SECRET_KEY` | _空_ | 对外契约 HMAC secret-key |
| `PLAITA_CONSOLE_LANGFUSE` | `auto` | Langfuse 观测（[plaita.obs](../README.MD#可观测性plaitaobs)）：`auto`=配了 `LANGFUSE_PUBLIC_KEY` 即开；`true` 强制开；`false` 强制关。trace id = 执行实例 ID；缺 `plaita[langfuse]` 依赖自动降级为不观测（集群档对应 worker 的 `--langfuse` / `PLAITA_WORKER_LANGFUSE=1`）。另需 `LANGFUSE_PROJECT_ID`（如 proj-plaita-local）——执行详情页的「Langfuse」深链依赖它计算 trace URL |

`PLAITA_CONSOLE_ADMIN_API_KEY` 只供服务端/CLI 等非浏览器调用方使用（`X-Admin-API-Key` 或 Bearer）。**浏览器端一律使用会话登录后的 Bearer token**——不要通过 `VITE_*` 把管理密钥注入前端 bundle（Vite 会在构建期内联，公开静态资源即可下载，见 #266）。

`POST /api/flows/dry-run` 有两层安全闸门：(1) **拒绝**含 `code` / `python` / `javascript` 等危险节点的流程；(2) 试跑前统一注入 `globalContext.dry_run=True`，并在执行器读点 `get_global_variable("dry_run")` 上钉死（试跑专用 `_DryRunExecution`，随子执行下发——流程声明 `dry_run=false` 也改不回去）——经 `PLAITA_CONSOLE_NODE_MODULES` 注册的业务节点（gate / capture / agentrun / llm / email / webhooks / database / git_publish 等）据此跳过真实副作用（子进程 / 出网 / agent CLI），名黑名单枚举不到它们。二者共同防止 console 进程 RCE。

## 开发指南

参见 [AI_DEVELOPMENT_GUIDE.md](../requirements/AI_DEVELOPMENT_GUIDE.md)

### UI 旅程 e2e（语义化节点 id 可读性）

两个层次，覆盖同一旅程（画布语义名/desc、抽屉源码跳转、拖拽 + 保存 round-trip）：

1. **argusai browser suite**（CI 主路径，随 `scripts/e2e-run.sh` 全量跑）：
   `tests/e2e/ui-semantic-ids.yaml`——声明式 browser 步骤，单跑：
   `scripts/e2e-run.sh ui-semantic-ids`。数据播种经浏览器 evaluate + fetch
   （Bearer 取自 localStorage；ui-backend 容器不设 ALLOW_INSECURE_ADMIN）。
2. **本地等价执行器**（无需 Docker/mcp2cli/argusai-mcp）：
   `BASE_URL=... ADMIN_PASSWORD=... python scripts/e2e-ui-semantic-ids-local.py`
   ——按同一份 YAML 逐 case 执行与断言，用于本机快速回归。

注意：console 前端 vite `base: './'`（dsht 子路径部署取舍），**双层深链**直刷会
白屏（如 `/flows/<id>/edit`）——两个 e2e 全程走客户端路由导航，不使用深链接刷新。

## 许可证

MIT


## 租户上下文（plaita#239 phase 2）

console 进程启动时显式声明 default 租户
（`set_current_tenant(DEFAULT_TENANT_ID, source="explicit")`，已在
`create_app()` / `lifespan` 内置）。凭据/存储写路径对来源 `default` 已
fail-closed（`TenantContextMissingError`）；自定义 WSGI/ASGI 启动器若绕过
`create_app()`，必须自行执行同一行声明，否则写操作会被拒绝。
