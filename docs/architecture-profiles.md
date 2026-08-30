# plaita 架构配套说明（存储 / 队列 / 事件总线的真实视图）

> 面向使用者：回答「跑这套系统到底需要什么、数据放在哪、什么情况下会丢」。
> 设计细节见 `docs/scheduler-design.md` 与大仓 ADR-2026-08-27。

## 先说结论：真实后端只有两种

无论 `cluster_config.yaml` 顶层怎么写，**当前 console 拉起的服务一律是 Redis 后端**：

| 数据 | 位置 | 说明 |
|---|---|---|
| 任务队列 | Redis Stream `plaita:flow:queue` | 消费组（at-least-once + DLQ） |
| 执行状态 / 节点输出 | Redis `plaita:execution:*` | console 执行页的数据源 |
| 流程定义（运行时副本） | Redis `plaita:flow:{id}:{ver}` | 发布时由 console 同步写入 |
| 日志 | Redis `plaita:logs:*` | worker 的 RedisStreamHandler 写入 |
| 事件 / 订阅 | Redis（pub/sub + `plaita:subscription:*`） | 挂起恢复依赖 EventFilter |
| 服务注册表 / 拓扑 | Redis `plaita:registry:*`（心跳 TTL 30s） | 拓扑页数据源 |
| 调度定义 / 触发历史 | Redis `plaita:schedules` 等 | 调度服务与 console 共享 |

**唯一的 SQL**：console 自己的流程定义库（`flow_store`，SQLAlchemy，默认
`sqlite:///plaita_console.db`——它是「权威定义库」，发布时才同步到 Redis）。

⚠️ `cluster_config.yaml` 顶层的 `eventbus / queue / storage` 三个键在
**console 拉起服务的路径里不生效**（服务进程环境只由 `services.*.env` 组装，
worker 自身默认 redis 后端）。它们出现在配置编辑器里属于历史遗留，别按它们
做部署决策。

## Flow 运行需不需要「外挂持久化」？

分两条运行路径说清楚：

1. **console + worker 路径**（本文档主场景）：即使流程是纯同步、无挂起节点，
   worker 也会把执行状态（开始/结束/节点输出）写入 Redis——这是执行实例页、
   日志页能看见它的原因。这份状态很轻（每个执行一份 JSON），随时可清理，
   **不算重型持久化**。只有 delay/approval 等挂起节点才产生「跨进程等待」，
   依赖 Redis 里的订阅与事件通道。
2. **内嵌运行路径**（`plaita_flows.run` / 进程内 `Flow(...).run()`）：不经过
   worker，纯同步流程可以真正全内存跑完，零外挂依赖——适合单测与脚本。

## 三档标准配套

### 1. 快速上手（单机 · 轻量）

- **前提只有一个：本地有 Redis**（`redis://localhost:6379/0`，无需持久化配置）
- console（SQLite 流程库）+ 一键启动基础服务（执行器/延迟/事件恢复/调度）
- 执行历史、日志就是 Redis 里的一些键，重不重都在内存页面上看得见，随手可清
- 适合：体验、演示、验证一个想法。执行/日志键不需要备份。

### 2. 开发级别（单机 · 数据要留）

- 同上，但 Redis 开 AOF 持久化（执行历史、调度定义重启不丢）
- flow_worker 可多实例（消费组天然分摊）；调度服务保持 1 实例
- 定期清理 `plaita:execution:*` / `plaita:logs:*`（量大了拖慢 keys 扫描）
- 适合：日常真实使用（如 mediaflow 每日内容生产）

### 3. 生产级别（多机 · 可靠）

- **Redis**：独立实例 + AOF（`appendonly yes`）+ 内存上限与淘汰策略评估；
  执行状态与调度定义是关键数据，考虑定期 RDB 备份
- **流程定义库**：flow_store 是 SQLAlchemy，可将连接指向 PostgreSQL
  （建表自动迁移），SQLite 仅建议单机
- **worker**：多实例 + 消费组回收（`claim_min_idle_ms`）+ DLQ 监控
  （`plaita:flow:queue:dlq`）
- **观测**：日志键定期归档；EventFilter / 调度服务常驻且纳入进程守护
  （systemd / supervisor / 容器）
- **安全**：console 必须设置 `PLAITA_CONSOLE_ADMIN_API_KEY`

## 与配置的对应关系

| 你想改的 | 改哪里 |
|---|---|
| Redis 地址 | `cluster_config.yaml` 顶层 `redis.url` + 各 `services.*.env.PLAITA_REDIS_URL`（或 console 启动 env） |
| 队列名 | flow_worker / schedule_service 的 `PLAITA_QUEUE_NAME` |
| 服务实例数 | `services.*.max_instances` / 集群管理页「启动实例」 |
| 流程库换 PG | console 的 flow_store 连接串（SQLAlchemy URL） |
| 业务节点包 | flow_worker 的 `PLAITA_NODE_PATH` + `PLAITA_NODE_MODULES` |
