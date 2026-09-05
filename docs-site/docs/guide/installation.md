# 安装

plaita 要求 Python **3.10+**（推荐 3.10）。

## 从 PyPI 安装

```bash
pip install plaita
```

## 从源码安装

```bash
git clone <repository_url>
cd plaita
pip install .
```

## 验证安装

```bash
python3 -m plaita
```

若输出版本号（当前 **0.5.0**）即安装成功。

## 核心依赖

plaita 核心只依赖 `pydantic`、`pyparsing`、`isodate`、`croniter`，其余能力按需安装 extra。

## 可选依赖（extras）

| extra | 安装命令 | 提供能力 |
|-------|---------|---------|
| `redis` | `pip install plaita[redis]` | Redis 存储 / Redis EventBus / Redis 节点 |
| `server` | `pip install plaita[server]` | FastAPI 服务端、SQLAlchemy 存储、FlowWorker、扩展节点 |
| `code` | `pip install plaita[code]` | `CodeNode`（RestrictedPython AST 沙箱 + PyExecJS JS 后端）。0.5.0 起 `register_code_node()` **默认 `docker` 容器级沙箱**（需本机 Docker daemon），可选 `subprocess` / `restricted` / `unsafe` |
| `http` | `pip install plaita[http]` | `HTTP` 节点（requests + aiohttp）；`PlaitaClient`（requests） |
| `yaml` | `pip install plaita[yaml]` | YAML 格式流程定义 |
| `sqlalchemy` | `pip install plaita[sqlalchemy]` | SQLAlchemy EventBus / Storage（实验性） |
| `credentials` | `pip install plaita[credentials]` | 凭据加密（`plaita.credentials`） |
| `all` | `pip install plaita[all]` | 上述全部 |

当你尝试使用未安装 extra 的功能时，plaita 会抛出**可操作的 `ImportError`**，明确提示应安装哪个 extra，例如：

```
ImportError: The 'http' extra is required for this feature but is not installed.
Install it with: pip install plaita[http]
```

> 注意：`http` extra 包含 `requests` 与 `aiohttp` 两个库。`HTTP` 节点在入口会**同时探测两者**，缺少任一个都会在报错中列出缺失的库名——按提示 `pip install plaita[http]` 一次装齐即可。

## 开发依赖

贡献代码或本地构建文档时安装：

```bash
pip install plaita[dev]    # pytest / pytest-asyncio / fakeredis / pytest-cov
pip install plaita[lint]   # mypy / flake8 / black（可选）
```

## 部署模式总览

plaita 有四档形态：本地单机（SQLite）/ 单机标准（Redis）/ 分布式 / SDK 嵌入，
逐档平滑升级。完整对比见 [部署模式](deployment.md)。

## 编排台（可视化控制台）

不想写代码编排流程？安装编排台即可获得 Web 界面（流程画布 / 节点配置 / 试运行 / 集群管理）：

```bash
pip install plaita-console
python -m plaita_console
```

打开 <http://localhost:8080> 即可使用。后端会直接托管打包好的前端页面（无需 Node 环境）；
前端静态资源随 wheel 分发，也支持 `plaita-console` 命令行入口。

常用环境变量：

| 变量 | 说明 |
|------|------|
| `PLAITA_CONSOLE_PORT` / `PLAITA_CONSOLE_HOST` | 监听地址（默认 `0.0.0.0:8080`） |
| `PLAITA_CONSOLE_REDIS_URL` | Redis 连接（默认 `redis://localhost:6379/0`） |
| `PLAITA_CONSOLE_ADMIN_API_KEY` | 管理 API 密钥；本地开发可设 `PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN=true` 免密 |

详见 [编排台使用](console.md)。

## 节点插件包

默认注册表 23 种内置节点之外，通用业务节点以插件包形式分发，**pip 安装即自动注册**
（经 `plaita.nodes` entry-points，无需手动登记）：

```bash
pip install plaita-nodes
```

可用节点：`agentrun`（经 agentproc 驱动 AI Agent）/ `hitl`（人工介入）/ `gate` /
`capture` / `report` / `notify` / `writefile` / `llm` / `rate_limit` / `hitl_await`。

自研节点同样走 entry-points 分发，参见 [节点注册表与插件](../nodes/registry.md)。

详见 [配置与可选依赖](../reference/configuration.md)。
