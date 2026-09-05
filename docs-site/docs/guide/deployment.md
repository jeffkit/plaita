# 部署模式

plaita 的部署形态分四档，由低到高：**本地单机（SQLite）→ 单机标准（Redis）→ 分布式（Redis 多实例）**，外加 **SDK 嵌入**（不作为独立部署，而是把引擎当库用）。四档共用同一套流程定义与编排台，逐档平滑升级、不必推倒重来。

## 总览

| | ① 本地单机 | ② 单机标准 | ③ 分布式 | SDK 嵌入 |
|---|---|---|---|---|
| 外部依赖 | **无** | Redis 5+ | Redis 5+（可多机） | 无 |
| 流程执行 | console 进程内 | flow_worker（console 拉起） | 多实例 worker 水平扩容 | 你的进程内 |
| 执行历史 | SQLite | Redis | Redis | 内存/自选存储 |
| 画布试跑 + pin 调试 | ✅ | ✅ | ✅ | — |
| RBAC / 审计 / 凭据 | ✅ | ✅ | ✅ | — |
| 集群管理 / 队列 / 事件 | ❌ | ✅ | ✅ | — |
| 挂起-恢复（审批/事件） | ❌* | ✅ | ✅ | ✅（配 EventBus） |
| 适用场景 | 个人试用 / 演示 / 轻量自动化 | 团队共享编排台 | 生产规模 / 高可用 | 把编排嵌进你的应用 |

\* 本地单机模式下挂起型节点（event/approval 等）不会真正挂起等待，pending 结果会当作普通输出继续执行——需要审批流的请升到 Redis 档。

## ① 本地单机（SQLite）—— 零依赖起步

**一条命令可用，不需要 Redis、不需要 worker：**

```bash
pip install plaita-console
python -m plaita_console
```

console 启动时探测 Redis 不可达，即自动进入本档（`local_mode`）：

- 流程在 console 进程内的后台线程执行，执行记录与**节点级 trace** 存 SQLite（`local_executions` 表）
- 首次启动自动进入「创建管理员」向导；RBAC、审计、凭据、试跑调试全部可用
- 发布/删除只写 SQLite，跳过引擎同步

**边界**（对应页面会明确提示 503）：

- 集群管理 / 任务队列 / 事件管理不可用
- 挂起-恢复语义不生效（见上）
- 单进程执行，无水平扩展；`cancel` 为尽力而为（标记状态，不中断线程）
- `local_mode` 在**启动时**判定：运行中 Redis 掉线不会自动切换档位，需重启 console

**升级到 ②**：装好 Redis → 重启 console → 完成。已有流程定义在 SQLite 里继续有效，发布开始同步到引擎。

## ② 单机标准（Redis）—— 团队共享编排台

在 ① 的基础上加 Redis，即可获得完整能力：

```bash
# Redis 就绪后重启 console，即退出本地单机模式
PLAITA_CONSOLE_ADMIN_API_KEY=<服务账号密钥> PLAITA_CONSOLE_ENV=prod \
  plaita-console
```

- 发布的流程同步到引擎存储（`plaita:flow:{id}:{version}`），「集群」页可一键拉起
  flow_worker / 调度 / 事件服务等实例（模板见 `cluster_config.yaml`）
- 挂起-恢复、审批、延迟任务全部生效；执行历史与实时流（SSE）来自 Redis
- 拉起的 worker 自动带上凭据文件与密钥环境

**基础设施**可用 console「集群」页的容器化生命周期一键拉起（Redis/PostgreSQL），
或自备。生产建议再设置 `PLAITA_CONSOLE_ADMIN_API_KEY`（服务账号）并关闭
`ALLOW_INSECURE_ADMIN`。

## ③ 分布式（Redis 多实例）—— 生产规模

在 ② 之上把 worker 与外延服务扩到多实例/多机：

- worker 命令：`python -m plaita.server.flow_worker`（集群页或进程管理器拉起）
- 多实例消费同一 Stream（consumer group），resume 租约防并发恢复，超限进 DLQ
- 事件恢复（event_filter）、延迟（delay_service）、回调（http_callback_service）
  按需独立扩容

运维细节（Stream 迁移、租约、DLQ、故障处理）见
[分布式运维 Runbook](../distributed/ops-runbook.md)。

## SDK 嵌入 —— 不部署编排台

只要引擎不要界面：

```bash
pip install plaita
```

```python
from plaita.core.flow import Flow
from plaita.core.executor import FlowExecution

flow = Flow.model_validate(definition)
result = FlowExecution().run_compatible(flow, False, **params)
```

默认 memory 存储 + InMemoryEventBus；需要跨进程时换 Redis 实现
（见 [断点续执总览](../distributed/index.md)）。编排台（①②③）只是引擎之上
的可选控制面。

## 各档共同的账号体系

四档中只要跑编排台，账号与审计行为一致：users / sessions / audit_logs /
credentials 都在 console 的 SQLite 中（与 Redis 无关）；`X-Admin-API-Key`
服务账号在 ②③ 档同等可用。

## 选型速查

- 「我想看看这是什么」→ ①，两分钟出画面
- 「团队要共用一个编排台」→ ②
- 「执行量大 / 要高可用 / 挂起流程多」→ ③
- 「我只想在自己的 Python 服务里编排」→ SDK 嵌入
