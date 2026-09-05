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
- **对外契约接口** - 暴露 `/api/flowVersion/semver/detail`（HMAC 鉴权），供 `plaita.PlaitaClient` 拉取已发布流程定义

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
| `/api/executions/{id}` | DELETE | 取消执行 |
| `/api/queues` | GET | 获取队列状态 |
| `/api/logs` | GET | 获取日志 |
| `/api/logs/stream` | GET (SSE) | 实时日志流 |
| `/api/flows` | GET / POST | 流程列表 / 新建流程 |
| `/api/flows/{id}` | GET / DELETE | 流程详情（含版本列表）/ 删除流程 |
| `/api/flows/{id}/versions/{ver}` | GET / PUT / DELETE | 取版本 / 保存草稿（Flow 强校验）/ 删除版本 |
| `/api/flows/{id}/publish` | POST | 发布指定版本（draft → published） |
| `/api/flows/dry-run` | POST | 同步试跑 Flow JSON，返回节点级结果 |
| `/api/nodes` | GET / POST | 节点描述列表 / 注册自定义节点 |
| `/api/nodes/{type}` | DELETE | 删除自定义节点（内置不可删） |
| `/api/cluster/*` | — | 集群配置 / 启停（command 白名单） |
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
| `PLAITA_CONSOLE_HOST` | `0.0.0.0` | 监听地址 |
| `PLAITA_CONSOLE_PORT` | `8080` | 监听端口 |
| `PLAITA_CONSOLE_DEBUG` | `false` | 调试模式 |
| `PLAITA_CONSOLE_DB_URL` | `sqlite:///./plaita_console.db` | 流程定义/版本/节点描述持久化（SQLAlchemy） |
| `PLAITA_CONSOLE_ADMIN_API_KEY` | _空_ | 管理面 API Key（`X-Admin-API-Key` / Bearer）。**空且未放开 insecure 时管理 API 返回 503** |
| `PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN` | `false` | 显式允许无管理密钥启动（**仅本地开发**） |
| `PLAITA_CONSOLE_SECRET_ID` | _空_ | 对外契约 HMAC secret-id（**空则 `/api/flowVersion` 返回 503**，不再接受空串签名） |
| `PLAITA_CONSOLE_SECRET_KEY` | _空_ | 对外契约 HMAC secret-key |

前端管理面请求会自动带 `X-Admin-API-Key`：优先读 `localStorage.plaita_admin_api_key`，其次 `VITE_PLAITA_ADMIN_API_KEY`。

`POST /api/flows/dry-run` **拒绝**含 `code` / `python` / `javascript` 等危险节点的流程，防止 console 进程 RCE。

## 开发指南

参见 [AI_DEVELOPMENT_GUIDE.md](../requirements/AI_DEVELOPMENT_GUIDE.md)

## 许可证

MIT

