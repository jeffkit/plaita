# event_demo — 事件系统演示

演示 plaita 事件总线的处理器、订阅、过滤、重试、超时与多后端（memory / redis / sqlalchemy）。

## 怎么跑

在 plaita 仓库根目录执行（`examples` 需作为包导入）：

```bash
# 内存后端（零依赖，推荐先跑这个）
python -m examples.event_demo.demo_eventbus --backend memory

# 订阅超时检查器（SubscriptionTimeoutChecker）
python examples/event_demo/timeout_example.py
```

可选后端（按需安装依赖）：

```bash
# SQLAlchemy 后端：需要 pip install "sqlalchemy[asyncio]" aiosqlite greenlet
python -m examples.event_demo.demo_eventbus --backend db

# Redis 后端：需要 pip install redis，且本地有 Redis（默认 redis://localhost:6379/0）
python -m examples.event_demo.demo_eventbus --backend redis

# 混合后端：总线/存储/订阅/跟踪器可分别选 memory / redis / db
python -m examples.event_demo.demo_eventbus --backend mixed
```

## 文件副作用说明

- `demo_eventbus.py` 的日志固定写到**本目录**下的 `plaita.log`；
  SQLAlchemy 后端的 SQLite 数据库固定写到**本目录**下的 `event_demo.db`
  （可用 `--db-url` 覆盖）。不会污染你运行命令时所在的目录。
- `timeout_example.py` 全程内存操作，无文件副作用。

## 依赖

- 必需：plaita 本身（仓库根目录 `pip install -e .`）
- `--backend db`：`sqlalchemy[asyncio]`、`aiosqlite`、`greenlet`
- `--backend redis` / `mixed`（含 redis 组件）：`redis` + 运行中的 Redis
