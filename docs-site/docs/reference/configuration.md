# 配置与可选依赖

plaita 的权威构建配置在 [`pyproject.toml`](https://github.com/jeffkit/plaita/blob/main/pyproject.toml)。本页列出依赖分组、entry_points 与测试/覆盖率配置。

## 运行时依赖

核心极轻：

| 依赖 | 用途 |
|------|------|
| `pydantic>=2.0` | 数据模型（`Flow` / `Node` / `Event` 等） |
| `pyparsing>=3.0` | 表达式解析 |
| `isodate` | ISO 8601 超时时长解析 |
| `croniter>=2.0` | cron 定时表达式解析（定时触发器） |

## 可选依赖（extras）

| extra | 依赖 | 提供能力 |
|-------|------|---------|
| `redis` | `redis>=4.0` | Redis 存储 / Redis EventBus / Redis 节点 |
| `server` | `fastapi`、`uvicorn`、`python-multipart`、`python-jose[cryptography]`、`passlib[bcrypt]`、`SQLAlchemy>=1.4`、`cachetools>=5.0` | 服务端、SQLAlchemy 后端、FlowWorker、扩展节点 |
| `code` | `PyExecJS`、`RestrictedPython>=7.0` | `CodeNode`（Python AST 沙箱 + JS 执行；0.5.0 起默认 docker 沙箱，需 Docker daemon） |
| `http` | `requests`、`aiohttp>=3.8` | `HTTP` 节点；`PlaitaClient`（requests） |
| `yaml` | `PyYAML>=6.0` | YAML 格式流程定义 |
| `sqlalchemy` | `SQLAlchemy>=1.4`、`aiosqlite>=0.19` | SQLAlchemy EventBus / Storage 路径（实验性） |
| `credentials` | `cryptography>=42.0` | 凭据加密（`plaita.credentials`） |
| `dev` | `pytest`、`pytest-asyncio`、`fakeredis`、`pytest-cov` 等 | 开发/测试 |
| `lint` | `mypy`、`flake8`、`black`、`ruff>=0.6` | 静态检查/格式化（可选） |
| `all` | 上述 redis/server/code/http/yaml/sqlalchemy 全部 | 一键装齐 |

```bash
pip install plaita[server,http]
pip install plaita[all]
```

> `http` extra 同时探测 `requests` 与 `aiohttp`：使用 `HTTP` 节点时缺少任一个都会抛出 ImportError 并列出缺失的库名（`PlaitaClient` 仅依赖 `requests`）。

## entry_points：节点插件

`pyproject.toml` 声明了 `plaita.nodes` entry_points，把 `server` 扩展节点自动注册：

```toml
[project.entry-points."plaita.nodes"]
delay = "plaita.server.nodes.delay_node:DelayNode"
redis_queue = "plaita.server.nodes.redis_queue_node:RedisQueueNode"
kafka_queue = "plaita.server.nodes.kafka_queue_node:KafkaQueueNode"
http_callback = "plaita.server.nodes.http_callback_node:HttpCallbackNode"
approval = "plaita.server.nodes.approval_node:ApprovalNode"
```

第三方节点库可同样声明 entry_points 实现自动发现（见 [节点注册表与插件](../nodes/registry.md)）。

## Python 版本

`requires-python = ">=3.10"`。推荐 3.10+。

## 测试与覆盖率

`[tool.pytest.ini_options]`：

- `testpaths = ["tests"]`
- markers：`integration`（多组件/可选后端）、`e2e`（端到端）

`[tool.coverage.run]` 排除需要真实基础设施的后端（redis/kafka/sqlalchemy/http services）、demo 脚本与远程客户端——这些由 integration/E2E 套件覆盖，不计入单元 gate：

```toml
omit = [
  "plaita/storage/redis.py",
  "plaita/storage/sqlalchemy.py",
  "plaita/server/**",
  "plaita/node/redis.py",
  "plaita/node/http.py",
  "plaita/client.py",
  "plaita/event/demo_eventbus.py",
  "plaita/event/redis.py",
  "plaita/event/sqlalchemy.py",
  "plaita/event/timeout.py",
  "plaita/event/utils.py",
]
```

## 文档站依赖

文档站（本站）依赖独立于主项目，见 [`docs-site/requirements.txt`](https://github.com/jeffkit/plaita/blob/main/docs-site/requirements.txt)：`mkdocs`、`mkdocs-material`、`mkdocstrings[python]`、`mkdocs-mermaid2-plugin`。

## 下一步

- [命令行](cli.md)
- [安装](../guide/installation.md)
