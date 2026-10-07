# 分布式运维 Runbook

面向 `RedisFlowWorker` + `EventFilter` 的部署与故障处理。控制面**硬依赖 Redis 5+**（Stream）。

## 推荐拓扑

| 组件 | 后端 | 说明 |
|------|------|------|
| ExecutionStorage / FlowStorage | **redis** | 同步契约；勿用 db |
| EventBus / subscription storage | **redis** | 与 Worker / EventFilter 同 Redis |
| 任务队列 | Redis **Stream**（`--queue-name`） | consumer group 默认 `plaita-workers` |
| resume lease | Redis `SET NX EX` | 键 `plaita:execution:lease:{execution_id}` |

memory 仅单测 / 本地 demo。SQLAlchemy `db` 为 **experimental**，需 `PLAITA_ALLOW_EXPERIMENTAL_DB=1`，且 **永不**用于 execution/flow。

## 多租户键空间（2026-09 起）

租户数据键按租户 namespace 隔离，规则由 `plaita/server/tenant_context.py` 单点定义：

| 键族 | default / 空租户 | 其他租户 |
|------|------------------|----------|
| 流程定义 / 注册表 | `plaita:flow:*`、`plaita:flow_list`、`plaita:flow_versions:*` | `plaita:{tenant}:flow:*` 等 |
| 执行状态 | `plaita:execution:{id}` | `plaita:{tenant}:execution:{id}` |
| 执行列表索引 | `plaita:execution:index`（ZSET，`:ready` 为回填标记） | `plaita:{tenant}:execution:index`（+`:ready`） |
| resume lease | `plaita:execution:lease:{id}` | `plaita:{tenant}:execution:lease:{id}` |
| 任务队列 | `plaita:flow:queue`（平台共享，消息内带 `tenant_id`） | 同左 |

兼容规则：消息缺 `tenant_id` 视为 default；default 租户沿用历史键前缀，新旧版本混跑时 default 流量不受影响，非 default 租户需 console 与 worker 双侧升级。

### 租户停用闸（plaita#27）

租户状态权威源在 console 的关系库（`tenants.status`）。运行面（调度服务 /
FlowWorker）是独立进程、只连 Redis，故 console 改状态时把状态写入
`plaita:tenant_status`（HASH：tenant_id → `active`/`disabled`），server 侧在
**入队 / 派发**前读取：

- `fire_schedule`（cron 循环 + console 立即触发）：停用租户不入队；
- `FlowWorker._dispatch_task`：停用租户的 `start` / `resume` 跳过（消息 ack 丢弃）。

读取缺失 / Redis 抖动一律视为**未停用**（fail-open）——停用闸是加严措施，不应
把全部租户的运行面拦死。停用即时生效（无需等会话过期）；console 侧会话
（`resolve_session`/auth 403）与本地调度直接查库，不依赖本条。重新启用后
同一会话立即恢复，无需重新登录。

## 环境变量速查

| 变量 | 默认 | 含义 |
|------|------|------|
| `PLAITA_REDIS_URL` / `REDIS_URL` | `redis://localhost:6379/0` | Redis |
| `PLAITA_QUEUE_NAME` | `plaita:flow:queue` | Stream 键 |
| `PLAITA_CONSUMER_GROUP` | `plaita-workers` | consumer group |
| `PLAITA_CONSUMER_NAME` | instance_id / `worker-<pid>` | 本实例 consumer |
| `PLAITA_CLAIM_MIN_IDLE_MS` | `60000` | pending 可被回收的最短空闲 |
| `PLAITA_LEASE_TTL_SECONDS` | `120` | resume 租约 TTL |
| `PLAITA_MAX_DELIVERIES` | `5` | 超过后进 DLQ |
| `PLAITA_DLQ_KEY` | `<queue>:dlq` | 死信 Stream |
| `PLAITA_ALLOW_EXPERIMENTAL_DB` | unset | 允许 factory 创建 db EventBus/subscription |
| `PLAITA_NODES_WORKSPACE_ROOT` | 由 worker 推导（见下） | `writefile` 节点的写入根（jail） |
| `PLAITA_ALLOW_UNRESTRICTED_WRITES` | unset | `=1` 关闭 writefile jail（仅单机信任部署） |

## writefile 写入 jail（2026-10 起默认开启） {#writefile-写入-jail}

`writefile` 节点（`plaita-nodes`）以 `PLAITA_NODES_WORKSPACE_ROOT` 为写入根：
设置后绝对路径与 `../` 穿越都必须落在该根内，否则报 `escapes workspace_root`
拒绝写入；**未设置时保持历史行为——任意路径可写**。能提交流程 JSON 的人因此
一度可写 `/etc/cron.d/...`、worker 自身代码或配置（持久化 RCE 原语）。

worker / console 启动时注入该变量（`plaita/writefile_jail.py`），次序：

1. 显式 `PLAITA_NODES_WORKSPACE_ROOT` → 原样使用（运营者配置优先）；
2. `PLAITA_ALLOW_UNRESTRICTED_WRITES=1` → 显式放行任意路径（**仅单机信任部署**）；
3. 都没有 → fail-closed 推导默认根：`PLAITA_PROJECT_ROOT` → worker 工作目录 →
   家目录（`/` 不构成边界，逐级后退）。

生效值启动日志可见（`writefile 写入 jail: …`）。**多租户 / 不受信流程部署请显式
配置第 1 条**，把根收敛到业务仓或沙箱目录；`PLAITA_PROJECT_ROOT` 已设的部署
（console 拉起的 worker、Docker 镜像）默认即落在部署根内，无需额外配置。

jail 是**进程级**的：worker 一次启动一个根，不能按执行/租户分别设根（`--concurrency`
下多任务共享同一根）。跨租户写不同目录的部署请给每租户独立 worker。

## List → Stream 迁移（升级必做）

旧版本用 Redis **List**（`RPUSH`/`BLPOP`）。新版本同一键名是 **Stream**，格式不兼容。

1. **停** EventFilter / 外延服务入队，再停 Worker（或先扩容只读）。
2. **Drain** 旧 List（若仍有积压）：

```bash
python scripts/drain_list_queue_to_stream.py \
  --redis-url "$PLAITA_REDIS_URL" \
  --list-key plaita:flow:queue \
  --stream-key plaita:flow:queue:v2
```

3. 新部署使用新 stream 键（推荐改名 `…:v2`），或确认旧 List 已空后 `DEL` 再让 Stream `XADD` 创建同名键。
4. 启动 Worker（会 `XGROUP CREATE … MKSTREAM`），再启 EventFilter。
5. 入队一律用 `plaita.server.task_queue.enqueue_task`，**禁止** `RPUSH`。

## 日常观测

远程 status（若开 registry）返回的 `queue` 字段含：

- `stream_length` / `pending` / `dlq_length`
- 计数：`acked` / `reclaimed` / `dead_lettered` / `lease_conflicts` / `failed` / `residue_swept`

也可以 Redis：

```bash
redis-cli XLEN plaita:flow:queue
redis-cli XPENDING plaita:flow:queue plaita-workers
redis-cli XLEN plaita:flow:queue:dlq
redis-cli KEYS 'plaita:execution:lease:*'
# 外延服务：delay 的排程/重试计数/死信（触发失败不再静默出排程，见 services.md）
redis-cli ZCARD plaita:delay:queue:scheduled
redis-cli HGETALL plaita:delay:queue:scheduled:dlq
```

**读 `XLEN` 时注意残留条目**：`XLEN` 计的是 Stream 里的条目数，含「已 `XACK` 未 `XDEL`」的残留（XACK 与 XDEL 之间进程被杀）——它们的执行早已终态，不代表有活干。判积压先看 `XPENDING`（`pending>0` 才是待处理）与消费组 `lag`；若 `XLEN>0` 而 `pending=0`/`lag=0`，按残留处理：worker 启动时与每 300s 会兜底回收（每轮 ≤256 条，大量残留按轮次收敛，`residue_swept` 计数可见），也可 `XRANGE` 看条目 payload 里的 `execution_id`，在 console 实查执行确为终态后确认无积压（2026-10-07 曾据 `XLEN=3` 误判「新系统未投产」，实为演练残留）。

## 僵尸执行巡检 {#僵尸执行巡检}

worker 崩溃后 pending 里的 start 任务重投会**另起全新执行**重跑，旧行永久停在
`running`。例行巡检用 `scripts/reap_zombie_executions.py` 把超期未更新的
running 行标记为 error(`orphaned`)，供监控/人工复核：

```bash
python scripts/reap_zombie_executions.py \
    --redis-url "$PLAITA_REDIS_URL" --idle-minutes 60 [--dry-run]
```

多租户按命名空间隔离，非 default 租户需对每个 `plaita:{tenant}` 各跑一次
（`--namespace plaita:{tenant}`）。

**判据不能只看 `last_update_time`**：执行状态在单节点执行期间无心跳（只有
`PERSIST_EVERY_N_STEPS` 步界才写回），一个 3 小时的长节点其时间戳可以陈旧 3
小时而执行完全健康（租约正被看门狗每 40s 续）。故有两道闸（2026-10 修复）：

| 闸 | 作用 |
|----|------|
| 租约键 `{ns}:execution:lease:{id}` 存在 → 跳过 | 活 worker 正推进长步骤，绝不标记 |
| 落盘走条件写（状态键与巡检读到的原始串一致才写） | 活 worker 的步界写插在巡检读之后时放弃落盘——否则它的 running/completed 会被 error 覆写（监控先见 error 又翻回 completed），按 error 驱动补偿的 keeper 还会触发真·双跑 |

先对齐 `--dry-run` 输出与 console 执行详情再实跑；`--idle-minutes` 须大于业务
最长单节点耗时。

## 故障手册

| 现象 | 可能原因 | 动作 |
|------|----------|------|
| 任务不消费 | group/stream 键不一致；Worker 未起 | 核对 `--queue-name` / group；看 Worker 日志 |
| `XLEN>0` 但 `XPENDING`/`lag` 为 0 | 已 `XACK` 未 `XDEL` 的残留条目（XACK 与 XDEL 之间进程被杀），**不是**真积压 | worker 启动时与每 300s 兜底回收（`residue_swept`）；`XRANGE` 看 payload 的 `execution_id`，console 实查为终态即可确认无活干 |
| pending 堆积 | 处理失败反复 reclaim；lease 冲突 | 查日志；调大 lease TTL；看 DLQ |
| DLQ 增长 | `max_deliveries` 触顶；毒丸/业务错 | `XRANGE` DLQ 查 `reason`；修业务后可人工 `enqueue_task` 回灌（活 worker 持租约的执行会被死信守卫跳过，见 [FlowWorker · 长步骤与消息回收](flow-worker.md#长步骤与消息回收)） |
| 反复重投，日志刷「保存执行状态失败 (…)」 | Redis 写路径瞬断/序列化失败——落盘失败已不再静默 ack（2026-10 评审修复） | 查 Redis `INFO`/延迟日志；恢复后 pending 自动重投收敛，勿人工 ack |
| 反复重投，日志刷「挂起任务投递失败」 | 挂起服务队列 `plaita:{subtype}:queue` rpush 失败；suspended 已保留等重派 | 查对应外延服务（DelayService 等）与其队列长度；恢复后重投自动重派 |
| 日志刷「延迟任务触发失败，…ms 后重试」/ 排程 ZSET 不降 | 到期瞬间 `publish` 失败（Redis 抖动）；任务按退避重试，**不**出排程 | 查 Redis 健康；恢复后自动重试成功。超限条数看 `HGETALL plaita:delay:queue:scheduled:dlq`（含失败原因/次数），修因后人工回灌 `plaita:delay:queue` |
| 双 resume | 旧版本无 lease | 升级到含 lease 的版本；查 lease key |
| 挂起永不恢复 | EventBus 与 subscription 不同 Redis；`--no-event-bus` | Worker/Filter 同总线；去掉 no-event-bus |

## 与可靠性文档的关系

语义边界见 [FlowWorker · 可靠性边界](flow-worker.md#可靠性边界必读)。业务幂等见 [幂等 Resume](idempotent-resume.md)。
