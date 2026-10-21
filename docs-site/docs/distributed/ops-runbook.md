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
| 执行索引回填游标 | `plaita:execution:index:backfill:cursor`（SCAN 游标，24h TTL；回填完成即删） | `plaita:{tenant}:execution:index:backfill:cursor` |
| resume lease | `plaita:execution:lease:{id}` | `plaita:{tenant}:execution:lease:{id}` |
| fence 当前世代（CAS 比较值） | `plaita:execution:fence:{id}`（裸整数，7d TTL，删除执行时清） | `plaita:{tenant}:execution:fence:{id}` |
| fence 世代计数器（单调锚） | `plaita:execution:fencegen:{id}`（裸整数，**无 TTL，删除执行不清**） | `plaita:{tenant}:execution:fencegen:{id}` |
| 节点重试计数（可重试预算） | `plaita:execution:noderetry:{id}`（裸整数，7d TTL；另有 `:{node}` 子键） | `plaita:{tenant}:execution:noderetry:{id}` |
| 兜底总重试计数 | `plaita:execution:noderefetch:{id}`（裸整数，7d TTL） | `plaita:{tenant}:execution:noderefetch:{id}` |
| G1 唤醒计数（`retry` 次数上限） | `plaita:execution:g1wakeups:{id}`（裸整数，7d TTL） | `plaita:{tenant}:execution:g1wakeups:{id}` |
| 确定性失败计数（连续失败上限） | `plaita:execution:nofail:{id}`（裸整数，7d TTL） | `plaita:{tenant}:execution:nofail:{id}` |
| 不亲和交接计数 | `plaita:execution:affinityhandover:{id}`（裸整数，7d TTL） | `plaita:{tenant}:execution:affinityhandover:{id}` |
| 任务队列 | `plaita:flow:queue`（平台共享，消息内带 `tenant_id`） | 同左 |
| 租户状态闸（#27） | `plaita:tenant_status`（HASH：`tenant_id -> active\|disabled`，平台机制键） | 同左 |

> 这些**机制键**（lease / fence / fencegen / cancel / noderetry / noderefetch /
> g1wakeups / nofail / affinityhandover）都是裸整数或非执行状态载荷，console
> 的列表/详情路径按前缀逐个排除（`_is_mechanism_key`）——新增同类键必须同步
> 登记，否则 `GET /api/executions` 对 int 调 `.get()` 直接 500（历史已发生多次）。
>
> 删除单个执行时，`delete_execution_state` 清掉上表的 lease / fence 与五类计数键
> （含 `noderetry:{id}:{node}` 子键）；**`fencegen` 保留且无 TTL**，用于防止
> 同一 `execution_id` 重建后世代回退（旧 worker 的迟到 save/delete 必须被 CAS
> 拒绝）。租户整删走 `{ns}:*` 扫描，会把 `fencegen` 一并清掉。
>
> **恢复现场（A2R25-05/07）**：fenced save/delete 要求 fence 与 fencegen 同时
> 等于本世代——只恢复旧 `state+fence`（fencegen 留新值）会让陈旧 worker 的
> 写/删被 `ExecutionLeaseError` 拒绝。只恢复状态键时 `load_execution_state`
> 会补回索引成员；全量兜底用
> `python plaita-console/scripts/reindex_executions.py --tenant <slug>`
> （`--all-tenants` 需 console DB），避免执行在列表里不可见。

兼容规则：消息缺 `tenant_id` 视为 default；default 租户沿用历史键前缀，新旧版本混跑时 default 流量不受影响，非 default 租户需 console 与 worker 双侧升级。

### 停用租户的跨进程生效（#27） {#停用租户的跨进程生效27}

console 在租户启停时把状态写入 `plaita:tenant_status`（并启动时以权威库回填），
worker 派发 start/resume 前、`schedule_service` 入队前据此拦截停用租户：

- **缺失记录 = active**（fail-open）：升级前未发布状态的既有部署不会全租户停摆；
  也容忍「console 与 worker 不共享 Redis」的运维缺口——此时闸门不生效，需在
  worker 侧 Redis 上自行 `HSET plaita:tenant_status <tenant> disabled`
  （与 `sync_published_to_engine.py` 同款边界）。
- 手动排查：`redis-cli HGETALL plaita:tenant_status`；恢复漏发布可重启 console
  （启动回填）或手工 `HSET`。
- **Redis-only 恢复的墓碑回滚（A2R25-06）**：启动对账以 DB 为权威，把 Redis
  里 DB 已不存在的非 default 租户回落成 `disabled` 墓碑（`GET /health` 前后的
  `租户状态对账` 日志汇总 published/disabled_stale/failed）。RDB/键恢复把删除
  时的 `disabled` 回滚成删除前 `active` 时，**重启 console** 即纠正；字段整条
  缺失仍按 #237 fail-open（无法与升级前未发布状态区分），恢复后请用
  `HGETALL` 核对并用上面的 `HSET` 补齐。
- console 侧另有实时闸：停用后该租户已签发会话的 API 调用返回 **403**，且其调度
  被置为 `enabled=false`。

## 环境变量参考 {#环境变量参考}

本页是 worker / console 运行时 **ops** 环境变量的完整参考。只列会影响生产行为的
ops 开关；AI/copilot、plaita-nodes 业务节点、内部 marker（`PLAITA_TENANT_ID`、
`PLAITA_SCRIPT` 等）不在此表。默认值与失败形态以当前代码为准。

### 读取时机

| 标记 | 含义 |
|---|---|
| `import` | 模块导入时求值。改值必须重启进程；运行中改 env 无效。 |
| `ctor` | 构造对象（队列/回调/服务）时读取。改值只影响之后新建的对象。 |
| `call` | 每次调用/每个任务读取。同进程改 env 即时生效（测试/热修可用）。 |
| `startup` | 进程启动或节点注册时读取。需重启。 |
| `request` | console 每次请求读取（`get_settings()` 不是单例）。改 env 对下一请求生效。 |
| `launch` | console 每次拉起服务进程时从 `process_env` 读取。 |

> **先记住三条**：① **`0` 不是统一的“关闭”**（见文末矩阵）；② 只有少数开关是
> “精确 `0`/`1`”，其余会 `strip` 或 `lower`，不要凭直觉写 `false`；③ `import` 组
> 改了 env 不重启等于没改。

### 基础连接 / 队列 / 保留 / 观测

| 变量 | 默认 | 读取时机 | 作用与 0/非法值 | 陷阱 |
|---|---|---|---|---|
| `PLAITA_REDIS_URL` / `REDIS_URL` | `redis://localhost:6379/0` | `startup` | Redis 连接；`PLAITA_REDIS_URL` 优先，`REDIS_URL` 兼容 fallback。worker / services / event_filter / schedule / console 全部走同一 resolver（B2R31-02） | 空串 = identity 配置错误，启动预检拒绝；URL 非法/不可达在连接期失败；console 不可达会进本地单档且**不会自动恢复** |
| `PLAITA_QUEUE_NAME` / `QUEUE_NAME` | `plaita:flow:queue` | `startup` | worker 消费 / 事件过滤 / 调度派发的回退队列 | 空串启动预检拒绝（不再写空 Stream）；多部署共用同一 Redis 时务必隔离；与 `PLAITA_CONSOLE_TASK_QUEUE` 不同源会打 WARNING |
| `PLAITA_CONSUMER_GROUP` | `plaita-workers` | `startup` | consumer group | 改名等于换消费组，旧 pending 不再被本组回收；空串拒绝 |
| `PLAITA_CONSUMER_GROUP_START_ID` | `0` | `startup`（建组） | 新消费组起点；group 改名应设 `$`（不重放历史） | 新组在非空 Stream 上从 `0` 建立会 WARNING 列出将重放的条数 |
| `PLAITA_CONSUMER_NAME` | instance_id / `worker-<pid>` | `startup` | 本实例 consumer 名 | 多实例不要手工设成同值 |
| `PLAITA_CLAIM_MIN_IDLE_MS` | `60000` | `startup` | pending 可被回收的最短空闲（毫秒） | 过小会抢走兄弟 worker 处理中的任务；非整数启动 `ValueError` |
| `PLAITA_LEASE_TTL_SECONDS` | `120` | `startup` | resume 租约 TTL（秒） | 非整数启动 `ValueError` |
| `PLAITA_MAX_DELIVERIES` | `5` | `startup` | 超过后进 DLQ | 同时约束正常重投、DLQ 重复副本、定义重试三条路径；调大可能挤掉真实死信。消息信封带 `budget_generation` 后，改预算不会作用到旧代际的 PEL（留 pending + WARNING，见配置代际小节） |
| `PLAITA_DEPLOYMENT_EPOCH` | `legacy` | `startup` | 部署纪元（`plaita:tenant_status:epoch` 的权威锚点） | 锚点在部署配置而非 Redis 快照，Redis-only 恢复不会把它一起回滚；空串启动预检拒绝 |
| `PLAITA_TENANT_STATUS_RECONCILE_SECONDS` | `60` | `startup`（console lifespan） | 运行中租户状态周期对账；`0` = 关闭（启动 WARNING） | 关闭后 Redis-only 恢复的墓碑回滚只能靠 `POST /api/cluster/tenant-status/reconcile` 手动纠正 |
| `PLAITA_TENANT_STATUS_EPOCH_STRICT` | unset（部署纪元存在时启用） | `startup` | 显式 `0` = 关闭 worker 的租户状态纪元闸（回滚到 #237 fail-open，启动 WARNING） | 关闭后旧快照里的 `active` 会再次放行已删租户 |
| `PLAITA_EXECUTION_DEADLINE_SECONDS` | `1800` | `call`（每次执行） | 未配置任何 `flow.timeout` / `execution.timeout` / 调用期 timeout 时的执行级总预算（秒，支持小数）；到期抛 `ExecutionDeadlineError`，worker 不重试并终态化 error（#267） | 非数字/非有限/`<=0` 回退默认并 WARNING；env 写 `0` 不会变成“零预算”或“无限” |
| `PLAITA_EXECUTION_TOMBSTONE_TTL_SECONDS` | `max(2592000, STATE_TTL)` | `call`（每次删除执行） | 执行删除墓碑 TTL（`plaita:exec_tombstone:{id}`） | `<=0` 非法：回退默认 + WARNING，不再表示永久；结果 clamp 到 `PLAITA_TASK_MAX_AGE_SECONDS`（墓碑不得先于年龄闸到期） |
| `PLAITA_TASK_MAX_AGE_SECONDS` | `2592000`（30 天） | `call`（每次派发） | start/resume 载体最大年龄，超龄在派发时 ack + WARNING 丢弃 | 年龄取 payload `timestamp` 与 Redis Stream ID 时间的**最大值**；两者都不可判 → **fail-closed** 丢弃；`0` 关闭（启动 WARNING：墓碑到期后无兜底） |
| `PLAITA_DLQ_KEY` | `<queue>:dlq` | `startup` | 死信 Stream | 空串启动预检拒绝；worker 的 effective 键写入 registry，删除租户时从 registry 收集（A2R31-08）；改键后旧 DLQ 不再被读 |
| `PLAITA_EVENT_TTL_SECONDS` | `604800`（7 天） | `call`（每次写事件） | 事件记录/类型索引统一 TTL（console 发布、引擎 `store_event`、服务 resume 落盘同源） | 非法/`<=0` 回退默认并 WARNING；**不要**单独调短而不调整订阅寿命，否则手动唤醒事件会在挂起执行仍等待时先失效 |
| `PLAITA_EVENT_INDEX_SWEEP_INTERVAL` | `60` | `ctor`（janitor 构造） | 事件/订阅索引 janitor 周期（秒） | 非法/`<=0` 回退默认并 WARNING；已有 janitor 不更新 |
| `PLAITA_EVENT_INDEX_SWEEP_BUDGET` | `500` | `ctor`（janitor 构造） | 每 tick **每个存储**最多检查的索引成员数 | `<=0` 按 1 执行并 WARNING；预算语义是「检查数」而非「删除数」 |
| `PLAITA_DISABLE_EVENT_INDEX_SWEEP` | unset | `startup` | `=1` 关闭 janitor（启动打 WARNING） | 冷桶（`flow:`/`node:`）的过期成员不再被主动清理，`zcard/scard` 可持续高于 live 数 |
| `PLAITA_DISABLE_EVENT_INDEX_PRUNE_ON_WRITE` | unset | `call`（每次写后） | `=1` 关闭写侧节流兜底清理（每 type 60s、至多 8 个成员的 `EXISTS`+`ZREM`） | 仅影响 console-only/无 worker 部署；读时惰性清理不受影响 |
| `PLAITA_METRICS_PORT` | unset / `0` | `startup` | worker 的 Prometheus 抓取端端口 | 非整数 → 0 + WARNING；负数静默钳 0（均关闭端点） |
| `PLAITA_METRICS_HOST` | `0.0.0.0` | `startup` | worker `/metrics` 监听地址 | 空白 → `0.0.0.0`；默认对全网卡，注意网络策略 |
| `PLAITA_ALERT_WEBHOOK` | unset | `startup` | 死信/僵尸巡检事件的 JSON POST 出口 | 空白 → None；URL 可能含凭据，日志已掩码 |
| `PLAITA_ALLOW_EXPERIMENTAL_DB` | unset | `call`（factory 调用时） | 允许创建 `db`/SQLAlchemy EventBus/Storage | 仅**精确 `1`**；否则 `ValueError`；生产控制面禁用 |
| `PLAITA_NODES_WORKSPACE_ROOT` | 由 worker 推导（见 writefile jail 小节） | `startup` 注入（节点侧每次执行读取） | `writefile` 写入根（jail） | 显式值原样使用；`/` 不构成边界 |
| `PLAITA_ALLOW_UNRESTRICTED_WRITES` | unset | `startup` | 仅**精确 `1`** 关闭 writefile jail（打 WARNING） | 单机信任部署才可用；多租户禁用 |

### Worker / 队列 / 容量 / 保留

| 变量 | 默认 | 读取时机 | 作用与 0/非法值 | 陷阱 |
|---|---|---|---|---|
| `PLAITA_TENANT_MAX_RUNNING` | 未设 / `0` = 不限（WARNING）；负/非法 = 不限 + WARNING；启动预检拒绝负/非法 | `call`（每次 admit） | 每租户 start 并发上限；`plaita:tenant_quota` HASH 可按租户覆盖；console 超限 429，worker 对无名额外部消息延迟重投 | 0 不是“全拒”；HASH 值 0/负 = 该租户不限（WARNING），**不会**回退 env |
| `PLAITA_TENANT_RUNNING_TTL_SECONDS` | `86400`；≤0/非法 → 86400 | `call`（每次 admit） | 名额条目 TTL，崩溃自愈 | 长于 TTL 的合法运行会提前让出名额 |
| `PLAITA_TENANT_QUOTA_DEFER_MAX` | `64`；<1/非法 → 64 | `call`（每次 defer） | worker 超配额重投预算；达限带 `quota_bypass` 放行 | 放行是存活性优先，不是配额失效 |
| `PLAITA_DLQ_MAX_LEN` | `1000`；非法 → 1000 + WARNING；**0/负 → 1 + WARNING**；启动预检拒绝 ≤0 | `ctor`（队列构造，worker 启动） | 每次死信后 `XTRIM` 保留最近 N 条 | **0 不是关闭，只保留 1 条**；钳制必留 WARNING；改值需重启；已有队列保留旧值 |
| `PLAITA_EXECUTION_STATE_TTL_DAYS` | `30`（2592000s）；≤0 → 0（关闭）；非法 → 30 | `call`（每次 save） | 仅终态（completed/error/cancelled）设 TTL；running/suspended 不设，保恢复 | 0/负是唯一明确的“关闭 TTL” |
| `PLAITA_WORKER_CONCURRENCY` | `1`；非整数 → 启动 `ValueError`；`0`/负 → 构造时钳 1 | `startup`（argparse/构造） | N 个消费线程 | 非整数是原始 traceback 退出，不是友好告警；0/负不是“停用” |
| `PLAITA_CAPACITY_STALE_SECONDS` | `90`；≤0/NaN/inf/非法 → 90 + WARNING | `call`（每次判定） | 容量快照新鲜期 | 0 回默认，不是关闭判定 |
| `PLAITA_CONSOLE_DISPATCH_SHADOW` | `true`；`0/false/no/off`（大小写不敏感）→ false | `call`（每次 plan） | 只影响影子决策标签；真正入队是单独的 `dispatch_one` | 其他任何值（含拼写错误）都是 true |
| `PLAITA_CONSOLE_TASK_QUEUE` | `plaita:flow:queue` | `call` | console 生产侧队列：手动启动 / 立即触发 / 容量派发 / 调度派发同源（A56-02/B2R31-02） | **空串 = 配置错误**（启动预检与四个读取点统一拒绝，不再得到空 Stream 键）；新旧并行时两处 env 必须一起改 |
| `PLAITA_DISPATCH_LEDGER_KEY` | `plaita:dispatch:ledger` | `call` | 派发台账键 | 空白 → 默认；改键后旧台账不再被读 |
| `PLAITA_DISPATCHER_ID` | 主机名 | `call` | 台账 actor 标识 | 空白 → 主机名；多实例同主机名会混账 |
| `PLAITA_WORKER_LANGFUSE` | 未设；仅**精确 `1`** 开启 | `startup` | 初始化 LangfuseCallback | `true` 不生效；缺 extra/凭据只告警 |
| `PLAITA_SANDBOX_LIFECYCLE` | `1`；仅**字节等于 `0`** 关闭 | `call`（每次装配 handler） | 挂载 plaita-nodes 沙箱生命周期回调 | `" 0 "`、`"false"`、`"00"` 都仍开启 |
| `PLAITA_EXECUTION_EVENTS_PUBLISH` | `1`；仅**精确 `0`** 关闭 | `call`（每次发布） | SSE 实时事件发布 | `"false"`/`" 0 "`/`"00"` 仍发布 |
| `PLAITA_EXECUTION_TIMELINE_MAX` | `500`；0/负 → 1；非法 → 500 + WARNING | `ctor`（callback 构造） | 每执行时间线事件环；超限丢最旧并计 `dropped` | 已有 callback 不更新；0 只留 1 条 |
| `PLAITA_EVENT_RECONCILE_INTERVAL` | `300` | `ctor`（reconciler 构造） | 事件回扫周期（秒） | 非数字/0/负 → WARNING + 默认（启动预检拒绝）；不再接受紧环 |
| `PLAITA_EVENT_RECONCILE_BACKFILL` | `3600` | `ctor` | 启动/游标丢失回扫窗口（秒） | 非数字 → WARNING + 默认；0 = 空窗口，不是“全量回扫” |
| `PLAITA_WORKER_DENY_REPOS` | 未设；逗号分隔仓名 | `call`（每次 claim） | 命中仓的 start 不领/让出 | 逗号项会 trim；空项忽略 |
| `PLAITA_WORKER_MIN_FREE_DISK_GIB` | 未设/`0` = 关闭；负 → 0；非法 → 0 + WARNING | `call`（每次 claim） | 低于守线不领取、空转重探、盘回线自动恢复 | 0 是关闭；非法值按关闭（有 WARNING）；支持小数 |
| `PLAITA_WORKER_DISK_GUARD_PATH` | `.`（worker cwd） | `call`（每次 claim） | `os.statvfs` 探测点 | 须落在实际写入卷；空白 → `.` |
| `PLAITA_WORKER_DRAIN_TIMEOUT` | `30`；负 → 0；非法 → 30 + WARNING | `call`（每次 drain） | 停止领取/停止等待的 **advisory** 上界（秒，支持小数）；超时后尽力 `cancel()` 在途执行 | **不是“节点中止”的上界**：步在进程退出前完成仍可能 ack；进程先退则消息留 pending，由其他 worker 从步界检查点续跑（那一步会重放） |
| `PLAITA_NODE_PATH` | 未设；`os.pathsep` 分隔 | `startup` | 逐项 `sys.path.insert(0)` | 不校验目录存在 |
| `PLAITA_NODE_MODULES` | 未设；逗号分隔模块 | `startup` | 逐个 import 并调 `register_all()`/`register()` | 失败 **ERROR + SystemExit**（worker 退出），不是跳过 |
| `PLAITA_LOG_LEVEL` | `INFO` | `startup` | 根 logger 级别；`--quiet` 等价 WARNING | 非法级别 → 启动预检具名拒绝（不再裸 `ValueError` traceback） |
| `PLAITA_INSTANCE_ID` | `<service_type>-<pid>` | `startup`（服务 CLI/worker） | 注册表实例 id；worker 读取并与注册键同值（B2R31-02） | **空串拒绝**；未设才生成 |
| `PLAITA_CHECK_INTERVAL` | `1`（秒） | `ctor`（ScheduleService） | 调度扫描周期 | 非数字/0/负 → WARNING + 默认（启动预检拒绝）；不再接受紧环 |
| `PLAITA_DELAY_QUEUE` | `plaita:delay:queue` | `ctor`（DelayService） | 延迟队列键族前缀（`:scheduled`/`:attempts`/`:dead` 派生） | env 优先于 `service_config`；**空串拒绝**（不再得到空键），不 trim |

### 回滚开关（`strip()` 后等于 `1` 才生效）

| 变量 | 回滚后行为 | 读取点/时机 |
|---|---|---|
| `PLAITA_DISABLE_TENANT_QUOTA` | worker 不做每租户准入（console 429 独立） | 每次 admit |
| `PLAITA_DISABLE_NODE_RETRY` | 节点失败直接终态 `error` | 每次失败 |
| `PLAITA_DISABLE_FENCING` | 裸 `RedisExecutionStorage` + `SET NX` 租约，fence 世代失效 | 每次存储操作/worker 启动 |
| `PLAITA_DISABLE_AFFINITY` | 跳过 repo 路径检查（“领到就跑”） | 每次 claim |
| `PLAITA_DISABLE_DISPATCH_TARGET` | 忽略消息体 `target_instance_id`，回到抢单 | 每次 claim |
| `PLAITA_DISABLE_HOLDER_LIVENESS` | 租约冲突分支无条件 ack（含决议载体收紧回滚） | 每次租约冲突 |
| `PLAITA_DISABLE_LEASE_WATCHDOG` | 不启动租约续租线程 | worker 启动 |
| `PLAITA_DISABLE_EVENT_RECONCILE` | EventFilter 不挂载回扫器（Pub/Sub 丢通知兜底消失） | `EventFilter.start()` |
| `PLAITA_DISABLE_CANCEL_CHECKPOINT` | 不查取消标志键（只在步界生效） | 每次步界 |
| `PLAITA_DISABLE_CANCEL_INTERRUPT` | 不启动步内取消监听（取消只在步界聚合） | 每次执行/步界 |
| `PLAITA_DISABLE_CODE_NODE` | 不注册 CodeNode；含 code 节点的整单被丢弃 | worker 启动 |

> 这些开关接受 `" 1 "`（会 strip），但 **`"true"` 不生效**；只有 `1` 的十进制文本。
> `PLAITA_DISABLE_CODE_NODE` 的读取函数返回的是“启用”布尔，env 语义与其它
> `DISABLE_*` 相同。

### code 沙箱

| 变量 | 默认 | 读取时机 | 作用与 0/非法值 | 陷阱 |
|---|---|---|---|---|
| `PLAITA_SANDBOX_TIMEOUT` | `10` 秒 | `import` | subprocess python/js 墙钟超时 | 0/负/小数/非整数 → import `ValueError`；0 不是无限 |
| `PLAITA_SANDBOX_MEMORY_MB` | `0` = 不限 | `import` | subprocess `RLIMIT_AS`（Linux 才生效） | 负值/非整数 → import `ConfigError`（B2R31-03 起不再接受负值）；0 仍是不限 |
| `PLAITA_SANDBOX_DOCKER_IMAGE` | `python:3.12-slim` | `import` | python docker 镜像 | 拉取/启动失败在节点执行期 |
| `PLAITA_SANDBOX_DOCKER_TIMEOUT` | `30` 秒 | `import` | docker 墙钟超时 | 0/负/小数/非整数 → import `ValueError` |
| `PLAITA_SANDBOX_DOCKER_MEMORY_MB` | `128` | `import` | `--memory` 参数 | 0/负原样传给 docker，执行期才暴露接受/拒绝；非整数 → import `ValueError` |
| `PLAITA_SANDBOX_DOCKER_CPUS` | `"0.5"` | `import` | `--cpus` 字符串 | 仅接受有限正数；`0`/`abc`/`nan`/`inf` → import `ConfigError`（B2R31-03 起不再延迟到执行期） |
| `PLAITA_SANDBOX_DOCKER_USER` | `""`（镜像默认用户，python 镜像为 root） | `import` | `--user` | 设非 root 后写非 `/tmp` 路径会失败（预期约束） |
| `PLAITA_SANDBOX_NODE_BIN` | `node` | `import` | js subprocess 的宿主可执行文件 | 缺失时执行期报错并提示 |
| `PLAITA_SANDBOX_DOCKER_NODE_IMAGE` | `node:20-alpine` | `import` | js docker 镜像 | 内网部署需预拉/改镜像 |
| `PLAITA_SANDBOX_ALLOWED_LANGUAGES` | `python` | `startup`（注册时） | 语言白名单（逗号/分号分隔）；`js` 必须显式放行；`python` 始终并回 | 非法语言 → `ValueError`；放行 js 打 WARNING |
| `PLAITA_SANDBOX_ALLOWED_BACKENDS` | `docker` ∪ 生效后端 | `startup`（注册时） | 后端白名单；显式值会校验 | 非法后端 → `ValueError`；`unsafe` 打 CRITICAL；显式值会与 worker 生效后端求并集 |
| `PLAITA_CODE_BACKEND` | worker `subprocess`；库级 `docker` | `startup`（注册时） | worker 选默认后端 | 非法值 → 启动 `ValueError`；docker 不可用时 **fail-closed**（拒绝注册并停止启动），只有 `PLAITA_ALLOW_SUBPROCESS_FALLBACK=1` 且白名单含 `subprocess` 才降级；直接 `register_code_node(default_backend=...)` 不传白名单时**不会**校验（B58-10） |
| `PLAITA_ALLOW_SUBPROCESS_FALLBACK` | unset（关闭） | `startup`（注册时） | `=1` 且白名单含 `subprocess` 时，允许配置后端不可用后降级 `subprocess`（A56-01 显式逃生开关） | 只开它而白名单不含 `subprocess` 仍拒绝注册；降级打 WARNING（`event=code_node.subprocess_fallback`） |

### 表达式 / HTTP / 并发

| 变量 | 默认 | 读取时机 | 作用与 0/非法值 | 陷阱 |
|---|---|---|---|---|
| `PLAITA_EXPR_MAX_RESULT_SIZE` | `100000` | `import` | 表达式结果基础上限 | 非法/≤0 → 默认 + WARNING |
| `PLAITA_EXPR_GROWTH_FACTOR` | `4` | `import` | 单次放大倍数 | 同上 |
| `PLAITA_EXPR_MAX_RESULT_HARD` | `100000000` | `import` | 单次结果硬上限 | 同上 |
| `PLAITA_HTTP_DNS_TTL` | `30` 秒 | `call`（每次策略校验） | SSRF 判定 DNS 缓存 TTL | **0 = 关闭缓存**（唯一明确语义）；负/`nan`/`inf`/非法 → 30 + WARNING（不再出现“负值当 0”“inf 永不过期”） |
| `PLAITA_HTTP_CONNECTOR_LIMIT` | `100` | `call`（每次建会话） | 共享 connector 并发上限 | 0/负/非法 → 100 + WARNING（不是“钳 1”，也不是静默回退） |
| `PLAITA_HTTP_COOKIES` | 未设 = 关 | `call`（每次建会话） | flow cookie jar | strict bool：`1/true/yes/on`（`strip().lower()`，`TRUE` 生效）；非法 → false + WARNING |
| `PLAITA_HTTP_MAX_RESPONSE_BYTES` | `10485760`（10MiB） | `call`（每次请求） | 响应体字节上限 | 0/负/非法 → 10MiB + WARNING，不是关闭/无限 |
| `PLAITA_HTTP_BLOCK_PRIVATE` | unset | `call`（每次策略校验） | 强制所有 `http` 节点屏蔽回环/内网/链路本地 | strict bool（`1/true/yes/on`，`strip().lower()`）；非法值 fail-closed 为 true + WARNING，启动预检拒绝；`0` 不关闭已配置的其他策略 |
| `PLAITA_BG_THREAD_WORKERS` | `8` | `import` | 后台线程池大小 | 非整数 → import `ValueError`；0/负 → import 通过，首次建池运行期 `ValueError` |
| `PLAITA_BG_PROCESS_WORKERS` | `cpu_count()` | `import` | 后台进程池大小 | 同上 |
| `PLAITA_BG_STATE_MAX` | `256` | `import` | 并发节点状态 LRU 容量 | 非整数 → import `ValueError`；0/负 → 状态立即淘汰，后台错误不可见 |
| `PLAITA_LOOP` | 未设 | `import`（包入口） | 仅 `uvloop`（strip + lower）安装 uvloop policy | 未装 uvloop 只 debug 留痕；其他值忽略 |

### console 运维 / 安全

| 变量 | 默认 | 读取时机 | 作用与 0/非法值 | 陷阱 |
|---|---|---|---|---|
| `PLAITA_CONSOLE_HOST` | `127.0.0.1` | `startup`（`run_server`） | 监听地址 | 非回环打 WARNING（#208）；**官方容器 CMD 硬编码 `--host 0.0.0.0`，此 env 不生效且无告警**（A58-01） |
| `PLAITA_CONSOLE_PORT` | `8080` | `startup`（`run_server`/容器 CMD） | 监听端口 | 非整数 → `get_settings()` `ValueError`（import app 时） |
| `PLAITA_CONSOLE_REDIS_URL` | `redis://localhost:6379/0` | `startup`（lifespan） | Redis 连接；不可达 → 本地单机模式 | 降级是**永久**的；运行中恢复 Redis 需重启 |
| `PLAITA_CONSOLE_DB_URL` | `sqlite:///./plaita_console.db` | `startup`（`init_engine` 一次） | 流程/版本/执行历史 SQLAlchemy URL | 凭据文件/密钥目录由它推导；**相对 URL 按各进程 cwd 解析**，PG/未设置/内存库回退 cwd 并打 WARNING（A67-04）；官方 compose 默认无持久卷 |
| `PLAITA_CONSOLE_ENV` | `dev` | `request` / 每次发布 | 环境标识；仅精确 `prod` 时删除需 admin | 其他值（test/staging）无额外语义 |
| `PLAITA_CONSOLE_ADMIN_API_KEY` | `""` | `request`（`get_settings`） | 管理面 API Key | 空且未 `ALLOW_INSECURE_ADMIN` → 管理 API 503 |
| `PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN` | `false` | `request` | 显式允许无管理 Key（仅本地开发） | strict bool：`1/true/yes/on`（`strip().lower()`，`" true "`/`TRUE` 均生效）；非法 → false + WARNING，启动预检拒绝 |
| `PLAITA_CONSOLE_ADMIN_PASSWORD` | 未设 | `startup`（bootstrap） | 设置且 users 表为空 → 自动建 `admin`（平台管理员） | 未设**不会**自动建号/打印随机密码，走前端创建向导 |
| `PLAITA_CONSOLE_RECONCILE_ORPHANS` | `suspend` | `startup`（lifespan） | 本地模式启动对账：`suspend` / `fail` / `off`（strip + 大小写不敏感） | 非法值 WARNING + 回退 `suspend`；只处理启动时 `running` 的执行 |
| `PLAITA_CONSOLE_SECRET_ID` / `_SECRET_KEY` | `""` | `request` | 全局契约 HMAC 密钥；空 → 全局契约路径 503 | 租户契约密钥仍可用 |
| `PLAITA_CONSOLE_NODE_MODULES` | 未设；逗号分隔 | `startup`（import） | 加载业务节点并注册 CodeNode | 失败 WARNING（console 不退出），流程运行期才报未知节点 |
| `PLAITA_CONSOLE_NODE_PATH` | 未设；`os.pathsep` | `startup`（import） | 逐项插 `sys.path` | 不校验目录存在 |
| `PLAITA_CONSOLE_LANGFUSE` | `auto` | `call`（每次执行） | `false` 强制关；`true` 强制开；`auto` 配了 `LANGFUSE_PUBLIC_KEY` 才开 | strict enum：未知值（如 `bogus`）→ **false + WARNING**（B2R31-03 起不再 fail-open）；启动预检拒绝非法值 |
| `PLAITA_CREDENTIALS_FILE` | SQLite：DB 同目录；非 SQLite/无 DB env：cwd（WARNING） | `call`（每次读/写） | 引擎/console 凭据文件；console 按租户写旁文件 | 未显式设置且无法推断共享目录 → 回退 cwd + WARNING；**显式相对路径**同样按各进程 cwd 解析 + WARNING（A67-04）；**多副本必须绝对路径**；空文件 → `CredentialStoreCorruptError`，不是空 store |
| `PLAITA_CREDENTIALS_KEY` | 未设 | `call`（每次加解密） | 内联 Fernet key，优先于文件；**同时是 DB 字段加密主密钥**（`local_executions.input_json/context_json`、`tenants.contract_secret_key` 经 HKDF 派生用途子密钥） | 非法 key → `CredentialError`/`FieldCryptoError`；不要传给子进程；**丢失/轮换会让历史 checkpoint 与既有租户契约密钥不可恢复** |
| `PLAITA_CREDENTIALS_KEY_FILE` | console：DB 同目录；引擎：cwd（WARNING） | `call`（每次加解密） | 密钥文件路径（凭据与 DB 字段加密共用） | console 首次用 `O_EXCL` 直接以 0600 创建；**显式相对路径**按各进程 cwd 解析 + WARNING（A67-04）；存量 0644 在启动对账/读取时收紧、O_EXCL 落败者复用前也收紧 + WARNING（A67-05）；引擎缺文件 fail-closed；**DB 备份须与密钥文件同等级保护且分开放置** |
| `PLAITA_PROJECT_ROOT` | 未设 | `startup`（writefile jail 应用） | jail 默认根的第一候选（`PLAITA_NODES_WORKSPACE_ROOT` 优先） | 落在引擎 checkout 内会上溯到父目录；`/` 不构成边界 |
| `PLAITA_PYTHON` | 未设 | `launch`（每次拉起服务进程） | 覆盖 `service_manager` 的 `cmd[0]`（业务 venv 解释器） | 从 `process_env` 读（全局 env 或 `services.*.env`）；不校验存在性 |

**凭据导出对账边界（当前定案）**：对账只在 console 启动时执行一次，不是周期
任务；没有对应租户的旁文件/凭据行只记 WARNING、不自动删除或隔离（B67-01，
自动收敛留给 owner 裁决）；SIGKILL 落在「导出成功→提交 DB」之间留下的鬼凭据
最长到下一次重启、同租户写或显式 `DELETE` 才收敛（B67-02）；对账在 SQLite
写锁内逐租户做文件 I/O，慢盘/万级租户 + 滚动重启的第二个实例可能撞 5s
busy timeout 而拒绝启动（B67-03），编排层应串行重启或放宽超时。详见
[console 配置文档](../guide/console.md) 的「凭据」一节。

### `0` 值语义速查（不要用一个“0=关闭”概括）

| 变量 | `0`/负值实测 | 语义 |
|---|---|---|
| `PLAITA_DLQ_MAX_LEN` | 0 → 1 + WARNING，-5 → 1 + WARNING（启动预检拒绝） | **不是关闭**；只保留 1 条死信，钳制必须留痕 |
| `PLAITA_CAPACITY_STALE_SECONDS` | 0/负 → 90 | 回默认，不是关闭判定 |
| `PLAITA_EXECUTION_TIMELINE_MAX` | 0/负 → 1 | 不是关闭，只留 1 条事件 |
| `PLAITA_HTTP_CONNECTOR_LIMIT` | 0/负/非法 → 100 + WARNING | 回默认，不是无限 |
| `PLAITA_HTTP_MAX_RESPONSE_BYTES` | 0/负/非法 → 10MiB + WARNING | 不是无限，回默认 |
| `PLAITA_HTTP_DNS_TTL` | 0 → 关闭缓存；负/非法/nan/inf → 30 + WARNING | 0 是唯一明确“关闭缓存”的 |
| `PLAITA_TENANT_MAX_RUNNING` | 0 → 不限 + WARNING；负/非法 → 不限 + WARNING（启动预检拒绝） | 0 不是全拒 |
| `PLAITA_TENANT_STATUS_RECONCILE_SECONDS` | 0 → 关闭周期对账 + 启动 WARNING | 不是紧环；关闭后需手动 reconcile |
| `PLAITA_TENANT_RUNNING_TTL_SECONDS` | 0/负 → 86400 | 回默认 |
| `PLAITA_TENANT_QUOTA_DEFER_MAX` | 0/负 → 64 | 回默认 |
| `PLAITA_EXECUTION_STATE_TTL_DAYS` | 0/负 → 0 | 明确关闭 TTL |
| `PLAITA_EXECUTION_DEADLINE_SECONDS` | 0/负 → 1800 + WARNING | 回默认，不是零预算/无限 |
| `PLAITA_EXECUTION_TOMBSTONE_TTL_SECONDS` | 0/负 → 默认 + WARNING | 不是“永久墓碑” |
| `PLAITA_TASK_MAX_AGE_SECONDS` | 0 → 关闭年龄闸 | 负 → 0（关闭） |
| `PLAITA_EVENT_TTL_SECONDS` | 0/负 → 7 天 + WARNING | 回默认，不是永久 |
| `PLAITA_EVENT_INDEX_SWEEP_INTERVAL` | 0/负 → 60 + WARNING | 回默认，不是停扫 |
| `PLAITA_EVENT_INDEX_SWEEP_BUDGET` | 0/负 → 1 + WARNING | 不是无界，也不是停摆 |
| `PLAITA_WORKER_CONCURRENCY` | 0/负 → 1 | 钳最小，不是关闭 |
| `PLAITA_EXPR_*` | 0/负 → 默认 | 回默认 + WARNING |
| `PLAITA_SANDBOX_TIMEOUT` / `_DOCKER_TIMEOUT` | 0/负 → import `ValueError` | 拒绝，不是无限 |
| `PLAITA_SANDBOX_MEMORY_MB` | 0 = 不限；负值/非数字 import `ConfigError` | 负值不再被接受（B2R31-03） |
| `PLAITA_SANDBOX_DOCKER_CPUS` | 非有限正数 import `ConfigError` | 不再延迟到执行期失败 |
| `PLAITA_WORKER_MIN_FREE_DISK_GIB` | 0 = 关闭；负 → 0 | 明确关闭 |
| `PLAITA_WORKER_DRAIN_TIMEOUT` | 0 = 不等待；负 → 0 | 立即进入 force-stop；在途步若在进程退出前完成仍可 ack |
| `PLAITA_EVENT_RECONCILE_INTERVAL` | 0/负/非法 → 300 + WARNING（启动预检拒绝） | 不再接受紧环 |
| `PLAITA_EVENT_RECONCILE_BACKFILL` | 0 → 0（空窗口）；负/非法 → 3600 + WARNING | 0 不是“全量回扫”，是启动时不回填 |
| `PLAITA_CHECK_INTERVAL` | 0/负/非法 → 1 + WARNING（启动预检拒绝） | 不再接受紧环 |
| `PLAITA_BG_THREAD_WORKERS` / `_PROCESS_WORKERS` | 0/负 import 通过 | 首次建池运行期 `ValueError`，不是无限 |
| `PLAITA_BG_STATE_MAX` | 0/负 import 通过 | 状态 LRU 立即淘汰，后台错误不可见 |

### 非法值处理矩阵 {#非法值处理矩阵}

> A2R31 起，worker / console / services / event_filter 的进程入口先跑
> `preflight_security_and_limits()` + `load_runtime_config(role, strict=True)`；
> 下表是**统一口径**。库内/嵌入式调用用非 strict 模式：安全门 fail-closed，
> 其余 WARNING + 安全默认。

| 行为 | 变量 |
|---|---|
| **启动/导入期 fail-fast**（聚合后一次退出） | `PLAITA_LOG_LEVEL`、`PLAITA_HTTP_BLOCK_PRIVATE`（未知布尔）、`PLAITA_SANDBOX_MEMORY_MB`（负/非数字）、`PLAITA_SANDBOX_DOCKER_CPUS`（非有限正数）、`PLAITA_CONSOLE_LANGFUSE`（非 auto/true/false）、`PLAITA_TENANT_MAX_RUNNING`（负/非数字）、`PLAITA_CONSOLE_ALLOW_INSECURE_ADMIN`（未知布尔）、`PLAITA_EVENT_RECONCILE_INTERVAL` / `PLAITA_CHECK_INTERVAL`（0/负/非数字）、`PLAITA_MAX_DELIVERIES` / `PLAITA_LEASE_TTL_SECONDS` / `PLAITA_DLQ_MAX_LEN`（≤0/非数字）、`PLAITA_SANDBOX_TIMEOUT`、`PLAITA_SANDBOX_DOCKER_TIMEOUT`、`PLAITA_BG_*`、`PLAITA_CONSOLE_PORT`、`PLAITA_SANDBOX_ALLOWED_LANGUAGES/_BACKENDS`、`PLAITA_CODE_BACKEND`（worker 路径未知值） |
| **空串 identity/routing fail-fast** | `PLAITA_QUEUE_NAME`、`QUEUE_NAME`、`PLAITA_CONSOLE_TASK_QUEUE`、`PLAITA_CONSUMER_GROUP`、`PLAITA_DLQ_KEY`、`PLAITA_REDIS_URL`、`REDIS_URL`、`PLAITA_CONSOLE_REDIS_URL`、`PLAITA_INSTANCE_ID`、`PLAITA_DELAY_QUEUE`、`PLAITA_DEPLOYMENT_EPOCH` |
| **WARNING + 回退默认（绝不静默）** | `PLAITA_HTTP_MAX_RESPONSE_BYTES`、`PLAITA_HTTP_DNS_TTL`（拒绝 nan/inf）、`PLAITA_HTTP_CONNECTOR_LIMIT`、`PLAITA_HTTP_COOKIES`、`PLAITA_CONSOLE_DEBUG`、`PLAITA_DLQ_MAX_LEN`（≤0 钳 1）、`PLAITA_EXECUTION_STATE_TTL_DAYS`、`PLAITA_CAPACITY_STALE_SECONDS`、`PLAITA_EXECUTION_TIMELINE_MAX`、`PLAITA_WORKER_DRAIN_TIMEOUT`、`PLAITA_WORKER_MIN_FREE_DISK_GIB`、`PLAITA_EXPR_*`、`PLAITA_EXECUTION_DEADLINE_SECONDS`、`PLAITA_EXECUTION_TOMBSTONE_TTL_SECONDS`、`PLAITA_TASK_MAX_AGE_SECONDS`、`PLAITA_EVENT_TTL_SECONDS`、`PLAITA_EVENT_INDEX_SWEEP_INTERVAL/_BUDGET`、`PLAITA_CONSOLE_RECONCILE_ORPHANS`、`PLAITA_TENANT_MAX_RUNNING`（运行期负/非法按不限）、`PLAITA_TENANT_RUNNING_TTL_SECONDS`、`PLAITA_TENANT_QUOTA_DEFER_MAX` |
| **运行期/执行期才失败** | `PLAITA_SANDBOX_DOCKER_MEMORY_MB`（0/负传给 docker）、`PLAITA_SANDBOX_NODE_BIN`（缺失）、`PLAITA_BG_*_WORKERS=0/负`（首次建池）、`PLAITA_CODE_BACKEND`（直接 `register_code_node` 不传白名单） |
| **接受任意值/仅特定值特殊** | `PLAITA_CONSOLE_ENV`、`PLAITA_SANDBOX_LIFECYCLE`/`PLAITA_EXECUTION_EVENTS_PUBLISH`/`PLAITA_WORKER_LANGFUSE`（legacy 精确 0/1）、`PLAITA_DISABLE_*`（strip 后等于 1） |

### 配置代际（A2R31 / B2R31-01）

worker 启动时计算并打印 `event=config.effective`，同时写入 registry metadata 与
容量块：

| 字段 | 生成 | 用途 |
|---|---|---|
| `config_family` | `cf1-` + sha256(queue/group/dlq/schema/namespace)[:12] | **硬兼容键**：两端都非 legacy 且不同 → 首投不执行、reclaim 不 XCLAIM，留 pending + WARNING |
| `config_generation` | `cfg1-` + sha256(全部影响语义字段)[:12] | 机队一致性/控制面混跑视图；不含 `LOG_LEVEL` 等纯观测项 |
| `budget_generation` | `bg1-` + sha256(max_deliveries/lease_ttl/task_max_age/dlq_max_len)[:12] | 预算变更不误判为消息不兼容；旧 PEL 留 pending 等 drain/匹配代际 |

- 消息信封新增 `config_family` / `config_generation` / `budget_generation` /
  `config_effective_at`；旧消费者忽略未知字段。重投/交接/克隆/DLQ replay
  **继承源值**，缺失按 `legacy`（不 gate，不伪造当前值）。
- 控制面：`GET /api/cluster/config` 返回 console 的派生哈希；`GET
  /api/cluster/instances` 显示每个外部实例的三维代际；`/api/services` 的
  worker metadata 含同样字段。
- **指标**：`queue_config_family_mismatch_total`、`queue_config_legacy_total`、
  `queue_config_budget_stale_total`、`tenant_status_stale_epoch_total`。
- **迁移**：queue/group/DLQ 改名仍需 drain；新消费组对已存在 Stream 用
  `PLAITA_CONSUMER_GROUP_START_ID=$` 避免重放。family 不匹配的消息会留 pending，
  需要匹配代际的 worker 或迁移 runbook 处理，不会无限烧 delivery。
- **b94-02 预算哈希迁移**：默认 `PLAITA_LEASE_TTL_SECONDS` 由 300 对齐为
  **120**（与 `execution_lease` 实际默认及本文档一致），且
  `load_runtime_config` 现在消费 worker 构造/CLI 的预算四键（不再是
  env-only）。默认部署的 `budget_generation` 及含预算字段的
  `config_generation` 会**一次性翻转**；在途消息携带旧哈希，滚动升级窗口会
  触发 `queue_config_budget_stale_total` / `config.budget_stale` 告警。这是
  迁移面而非数据损坏：drain 旧消息或接受一次性告警，待全部 worker/console
  重启到新构建后收敛，新消息按新哈希写入。
- **预算四键只走 env（#272）**：`PLAITA_LEASE_TTL_SECONDS` /
  `PLAITA_MAX_DELIVERIES` / `PLAITA_TASK_MAX_AGE_SECONDS` /
  `PLAITA_DLQ_MAX_LEN` 是生产者信封（console/schedule/event_filter）与
  worker 共用的唯一预算来源。worker CLI `--lease-ttl-seconds` /
  `--max-deliveries` 必须与 env 一致；不一致时启动即 `ConfigError`（给两边
  值与 env 键名），否则新任务带旧 `budget_generation`，失败载体在 PEL 中
  永久 `config_budget_stale`（直到 30 天年龄闸），reclaim 无法接管。
- **CLI identity 与 env 同源（#275）**：`--consumer-group` / `--dlq-key` /
  `--queue-name` 在启动时先 strip 并写回实际使用的参数；与 env/default 不一致
  即 `ConfigError`/exit 2（给出两边值）。尾随空格不会再产生独立 consumer
  group/DLQ 键或不同 Stream；`--queue-name` 不同会直接拒绝启动，不再「看似
  健康但无人消费」。
- **租户世代写闸（#277）**：`delete_tenant` 写 disabled 墓碑；`create_tenant`
  把新世代写入 `plaita:tenant_status:meta`。DLQ 与 `plaita:subscription:` 写面
  在写入前复核租户 active + 世代（在途 worker 读取/派发时捕获）；已删租户或
  旧世代的迟到写具名拒绝（DLQ 消息不 ack、留 pending；订阅写拒绝），同名重建
  的新租户读不到前代 payload。`PLAITA_DISABLE_FENCING=1` 只关闭 execution 状态
  写的 fencing，本闸独立于该开关生效（状态写回滚路径的既有风险按原样注记）。
- **世代随消息走（#283）**：任务信封新增 `tenant_generation`；重投/交接/
  归一化副本继承源值，worker dispatch 入口与 DLQ/订阅写闸比对**源世代**而非
  读取时 meta。无该字段的存量载体按 legacy 口径：dispatch 宽限、写闸在当前
  世代已发布时 fail-closed（消息留 pending）；DLQ replay 继承 DLQ 信封源世代。
  订阅无世代上下文且当前世代已发布 → 拒绝写入（不再自动 adopt 当前世代）。
- **create_tenant 世代先于 active（#284）**：flush 后、commit 前发布新世代，
  发布失败 ⇒ 建租户失败并回滚（不进入 active）；启动对账同样按
  generation → status 顺序，世代失败不发布 active。
- **租户状态纪元**：console 每次状态写入/对账都刷新
  `plaita:tenant_status:epoch`（值来自 `PLAITA_DEPLOYMENT_EPOCH`）。worker
  发现 Redis epoch 缺失/更旧 → 非 default 租户拒绝、default 保持 #237
  fail-open；Redis epoch 更新 → 本 worker 拒绝。运行中恢复后可用
  `POST /api/cluster/tenant-status/reconcile` 立即纠正，或等周期对账。

### 五个最容易踩的坑

1. **`0` 不是关闭**：先查上面的 0 值矩阵；尤其 `DLQ_MAX_LEN=0` 只保留 1 条、
   `EXECUTION_TIMELINE_MAX=0` 只留 1 条、`BG_*_WORKERS=0` 运行期炸。
2. **“精确 `0`/`1`”与 `strip` 不是一回事**：`EXECUTION_EVENTS_PUBLISH`/
   `SANDBOX_LIFECYCLE`/`WORKER_LANGFUSE` 是精确匹配；`DISABLE_*` 会 `strip`；
   `ALLOW_INSECURE_ADMIN`/`HTTP_BLOCK_PRIVATE`/`HTTP_COOKIES` 自 A2R31 起走
   strict bool（`1/true/yes/on` 与 `0/false/no/off`，`strip().lower()`）。
3. **空串 identity 值 = 配置错误**：`PLAITA_CONSOLE_TASK_QUEUE=`、
   `PLAITA_DELAY_QUEUE=`、`PLAITA_INSTANCE_ID=`、`PLAITA_QUEUE_NAME=`、
   `PLAITA_DLQ_KEY=` 等自 A2R31 起启动预检拒绝，不再得到空键/空 id；确认不需要
   该变量时请 `unset`，不要留空值。
4. **`import`/`ctor` 组改 env 不重启等于没改**：sandbox、expr、BG、`LOOP` 是 import
   期；DLQ、timeline、reconciler、schedule/delay 服务是构造期。只有 `call` 组能
   进程内热改。
5. **console 的 host 在官方容器里是“假开关”**：`Dockerfile.backend` 硬编码
   `--host 0.0.0.0`，设 `PLAITA_CONSOLE_HOST=127.0.0.1` 不会收窄监听，也不会触发
   #208 告警；容器部署请用端口映射/网络策略控制暴露面。

## 按宿主资源路由（低盘机不抢单，#49） {#按宿主资源路由}

**症状**：多 worker 池下，低盘机器反复「抢单白跑」——先领到任务，flow 的
preflight 节点（跑在领取宿主上）才判 `os.statvfs(repo)` 不足 → `retry-later`；
富盘机器同时刻空闲（`XLEN=0`）。2026-10-08 一夜实测 11+ 次（`runs.jsonl` 全是
`stage=preflight`、`disk 15.6–16.3GiB < min`），且 keeper 的 retry-later 退避
升档把瞬时低盘放大成 ≈6h 停机（`retry_later_streak` 6–7）。

**处置**：给每个 worker 配 claim 前的本机盘预检，阈值与 flow 侧 preflight 对齐：

```bash
export PLAITA_WORKER_MIN_FREE_DISK_GIB=20      # recursive 侧 RECURSIVE_MIN_FREE_DISK_GIB 默认 20
export PLAITA_WORKER_DISK_GUARD_PATH=/home/ubuntu/repo   # 流程真正写入的卷
```

- 低盘 worker 期间**零 claim**（不 XREADGROUP、不 XCLAIM、不烧 delivery、不写
  retry-later 回评），任务由池内富盘 worker 领走；
- 盘回线**自动恢复领取**，无需重启 worker；
- 阈值 0/unset = 关闭（存量行为）；探测失败放行（预检故障不停摆整池）；
- 与 `PLAITA_WORKER_DENY_REPOS`（按**仓**拒跑）互补：一个按宿主盘量，一个
  按仓体量，两者都走「不领/让给别人」路径，性质都是**本地准入闸**，不是
  调度器——真正的按资源路由（按 worker 心跳上报盘/负载分发）仍是缺口
  （见 [已知缺口](#已知缺口)）。

运维侧还应核对退避口径：`retry-later` 对「宿主资源类」原因（disk/负载）不
应升档，或盘回线时重置这类挂账的退避，否则白跑修好了、挂账仍要等 6h。

## writefile 写入 jail（2026-10 起默认开启） {#writefile-写入-jail}

`writefile` 节点（`plaita-nodes`）以 `PLAITA_NODES_WORKSPACE_ROOT` 为写入根：
设置后绝对路径与 `../` 穿越都必须落在该根内，否则报 `escapes workspace_root`
拒绝写入；**未设置时保持历史行为——任意路径可写**。能提交流程 JSON 的人因此
一度可写 `/etc/cron.d/...`、worker 自身代码或配置（持久化 RCE 原语）。

worker / console 启动时注入该变量（`plaita/writefile_jail.py`），次序：

1. 显式 `PLAITA_NODES_WORKSPACE_ROOT` → 原样使用（运营者配置优先）；
2. `PLAITA_ALLOW_UNRESTRICTED_WRITES=1` → 显式放行任意路径（**仅单机信任部署**）；
3. 都没有 → fail-closed 推导默认根：`PLAITA_PROJECT_ROOT` → worker 工作目录 →
   家目录（`/` 不构成边界，逐级后退）。候选目录若落在**引擎自身 checkout** 内
   （含其子目录）→ 先上溯到该 checkout 的父目录（部署根），见下。

生效值启动日志可见（`writefile 写入 jail: …`）。**多租户 / 不受信流程部署请显式
配置第 1 条**，把根收敛到业务仓或沙箱目录；`PLAITA_PROJECT_ROOT` 已设的部署
（console 拉起的 worker、Docker 镜像）默认即落在部署根内，无需额外配置。

jail 是**进程级**的：worker 一次启动一个根，不能按执行/租户分别设根（`--concurrency`
下多任务共享同一根）。跨租户写不同目录的部署请给每租户独立 worker。

### 默认根不取引擎自身 checkout（plaita#51） {#jail-默认根不取引擎自身-checkout}

worker / console 常常就从自己的 plaita clone 启动（Mac 开发机：`cwd` =
`.../infra4agent/plaita`）。此时第 3 条的「工作目录」候选就是引擎源码目录，拿它当
写入根会让**跨仓** run 的产物写不出去：目标仓是它的兄弟目录（`.../infra4agent/
agentproc/.flowcast/runs/.../land-failure.log`），WRITEFILE 一律报
`escapes workspace_root` → 节点确定性失败（同仓 run 的 run_dir 在 clone 内，故只在
跨仓失败路径暴露）。

现在落到 checkout 内的候选会先上溯到**父目录**——正是运营者语义里的部署根（docker
档 `PLAITA_PROJECT_ROOT=/app`、运维档手工配的 `.../infra4agent` 都是这一层），也是
VM worker 的实际效果（`cwd=/home/ubuntu` 覆盖 `/home/ubuntu/projects/...`）。上溯
只走一层，且 `/` 仍不构成边界；显式 `PLAITA_NODES_WORKSPACE_ROOT` 原样使用、不做
归一，要更窄的根（如只放业务仓）就显式配它。worker 的 `cwd` 不在引擎 clone 内、
又想覆盖多个兄弟仓时，另有 `PLAITA_PROJECT_ROOT=<部署根>`（推导第一候选）可指。

## code 沙箱后端白名单（plaita#22 起默认开启） {#code-沙箱后端白名单}

`code` 节点（`CodeNode`）的隔离强度由流程 JSON 里的 `sandbox_backend` 逐节点声明：
`docker`（容器级）/ `restricted`（AST 级，有已知绕过向量）/ `subprocess`（进程级，
**不隔离网络与文件系统**）/ `unsafe`（进程内 raw `exec`，任意模块、宿主凭据可读）。

流程 JSON 由流程作者提供，因而 `sandbox_backend` 是**不可信输入**：没有白名单，
作者写一行 `"sandbox_backend": "unsafe"` 就能在 worker/console 进程内执行任意代码。

worker / console 启动时经 `register_code_node(allowed_backends=...)` 施加白名单
（`plaita/node/__init__.py::resolve_sandbox_allowed_backends`），解析期即拒绝白名单
外的后端——**不是执行期才生效**。生效集合：

1. 显式 `PLAITA_SANDBOX_ALLOWED_BACKENDS`（逗号 / 冒号 / 空白分隔的后端名）；
2. 未设 → 默认 `docker`；
3. 再并入**生效的默认后端**（worker 看 `PLAITA_CODE_BACKEND`，默认 `subprocess`；
   console 默认 `docker`）——运营者选定的默认后端必须自身可用，否则不带
   `sandbox_backend` 的流程会被自己的白名单拦下。

启动日志可见生效集合；白名单含 `unsafe` 时打 **CRITICAL**（宿主任意代码执行），
未显式配置时打 WARNING。多租户 / 不受信流程部署请显式配置第 1 条，例如：

```bash
PLAITA_SANDBOX_ALLOWED_BACKENDS=docker PLAITA_CODE_BACKEND=docker \
  python -m plaita.server.flow_worker --redis-url redis://localhost:6379/0
```

未显式配置时的默认档已经挡住 `unsafe` 与 `restricted`；`subprocess` 只在它是生效
默认后端时可用（worker 默认档即如此）。

**配置后端不可用时 fail-closed（A56-01）**：worker 启动注册 `code` 节点时会探测
`PLAITA_CODE_BACKEND` 指定的后端；`docker` 但 daemon 不可用时**具名拒绝注册并停止
启动**（`CodeNodeRegistrationError`，日志 `event=code_node.registration_refused`），
**不会**自动降级 `subprocess`，也**不会**把显式白名单扩集。若部署确实需要可用性
降级，必须**同时**显式给出两个开关：

```bash
PLAITA_SANDBOX_ALLOWED_BACKENDS=docker,subprocess \
PLAITA_CODE_BACKEND=docker \
PLAITA_ALLOW_SUBPROCESS_FALLBACK=1 \
  python -m plaita.server.flow_worker --redis-url redis://localhost:6379/0
```

只开 `PLAITA_ALLOW_SUBPROCESS_FALLBACK=1` 而白名单不含 `subprocess` 仍拒绝注册——
降级目标必须已在运营者白名单内（否则一次 daemon 抖动就会让流程作者拿到新后端）。
降级成功打 WARNING（`event=code_node.subprocess_fallback`）。默认 subprocess 档
（不设 `PLAITA_CODE_BACKEND`）语义不变。

`subprocess` 后端**不是**租户凭据隔离边界（plaita#256 / A42-06）：子进程与 worker
同用户、文件系统不受限，所有租户共用同一把解密密钥；非 default 租户会在 spawn 前
fail-closed（`TenantContextPropagationError`，用户代码不执行），default 语义不变。
多租户 / 不受信流程必须用 `docker`（容器内不挂载宿主凭据文件）。

## code 语言白名单（plaita#29 起默认只放行 python） {#code-语言白名单}

`language: "js"` 曾**完全绕开**上面那套档位：js 一律走 PyExecJS（subprocess 拉起外部
JS 引擎，CommonJS `require` 可用），既不看 `sandbox_backend`，也没有超时/取消——
后端白名单只拦 Python。运营者把后端收敛到 `docker` 也拦不住一行 `language: "js"`。

现在语言与后端是两个独立维度，且语言默认 **fail-closed**：

1. **语言白名单**：worker / console 经 `PLAITA_SANDBOX_ALLOWED_LANGUAGES`
   （逗号 / 冒号 / 空白分隔的语言名）配置，未配置 → 只放行 `python`；`python`
   （`CodeNode` 的默认语言）始终保留。白名单外的语言在**解析期**被拒
   （`language='js' is not allowed by the operator`）。放行 `js` 时启动日志打 WARNING。
2. **语言 × 后端档位**：放行 js 后它同样受 `PLAITA_SANDBOX_ALLOWED_BACKENDS` 约束，
   且 `restricted` 没有 js 实现（RestrictedPython 是 Python 专用 AST 沙箱），
   声明的后端无该语言实现时解析期报错，**不静默降级**。

js 各档位对应：`docker` 跑 `PLAITA_SANDBOX_DOCKER_NODE_IMAGE`（默认 `node:20-alpine`，
`--network none` / 只读 FS / 资源与 pids 上限，与 Python 容器同一套加固参数）；
`subprocess` 跑 `node -e`（`PLAITA_SANDBOX_NODE_BIN`，默认 `node`，受
`PLAITA_SANDBOX_TIMEOUT` 墙钟秒数约束，0/负值启动期拒绝；超时/取消整组击杀 +
env 白名单）；`unsafe` 是历史
PyExecJS 裸跑（无隔离、无超时）。

```bash
# 需要 js 节点的部署：显式放行 + 预拉 node 镜像（离线环境把镜像指到内网）
PLAITA_SANDBOX_ALLOWED_LANGUAGES=python,js \
PLAITA_SANDBOX_DOCKER_NODE_IMAGE=registry.internal/node:20-alpine \
  python -m plaita.server.flow_worker --redis-url redis://localhost:6379/0
```

不需要 js 的部署保持默认即可（js 节点在解析期被拒，是预期行为）。库调用方自己接线时
用 `register_code_node(allowed_languages=(...))`，或经
`plaita.node.resolve_sandbox_allowed_languages()` 取同一口径。

## HTTP 节点 SSRF 防护（plaita#31） {#http-ssrf-hardening}

`http` 节点支持节点级 `allowedHosts` / `deniedHosts` / `blockPrivateNetworks`，
但 `blockPrivateNetworks` 默认 `False`，节点级策略又由流程作者（半信任主体）声明。
`PLAITA_HTTP_BLOCK_PRIVATE=1` 提供**运营者级**强制：置位后所有 `http` 节点生效
`blockPrivateNetworks`，节点声明只能更严、不能更松（effective = 节点声明 OR 运营者
开关）。该 env 由节点在请求期直接读取，deploy 入口无需额外注入；默认不设，保持
历史行为——**多租户 / 不受信流程部署请显式置位**：

```bash
PLAITA_HTTP_BLOCK_PRIVATE=1 \
  python -m plaita.server.flow_worker --redis-url redis://localhost:6379/0
```

策略激活（任一策略存在或运营者开关置位）时：

- **建连解析即校验 + 只连校验过的 IP**：校验与连接用同一份解析与判定，连接只指向
  通过校验的地址（aiohttp 自定义 resolver / requests 自定义 connector），堵住 DNS
  rebinding（预检答公网、连接时答内网）的 TOCTOU 绕过。Host 头与 TLS SNI 仍是
  原域名，证书校验不受影响。
- **忽略代理环境变量**：策略激活的同步请求与 aiohttp 侧一样钉死 `trust_env=False`。
  经 `HTTP_PROXY`/`HTTPS_PROXY` 的连接，目标解析与建连都发生在代理侧，节点侧
  pinning 无从生效——egress 依赖代理的部署请改用直连或在代理侧做同等校验。
- **不再复用 flow 作用域共享 `ClientSession`**：改用带 pinning resolver 的专用
  连接（安全换性能）；未激活策略的请求照常复用共享会话。

**响应体上限**：`response.text` 已改为流式读 + 字节上限，默认 **10MiB**
（`PLAITA_HTTP_MAX_RESPONSE_BYTES` 可调）——超限抛错，避免内网高速端点 30s 内拖回
GB 级响应撑爆 worker（还会进 `$NODE.<id>.RESPONSE` 状态与 checkpoint）。重定向不再
复用自动跟随（自动跟随会无条件读尽每个 3xx 的 body，绕过上限）：sync/async 都是
节点逐跳手动跟随、中间跳 body 直接丢弃不读。重定向上限统一为
节点 `maxRedirects`（默认 5，同步/异步同口径；此前默认路径用 requests 的 30 /
aiohttp 的 10，见 [MIGRATION](https://github.com/jeffkit/plaita/blob/main/MIGRATION.md)）。
确需更大 body 的部署显式调大该值。

## 无损升级（rolling upgrade）

分层的准确结论，先记住边界：

| 层 | 能否无损 | 关键约束 |
|---|---|---|
| 前端静态产物 | ✅ | 资源按内容哈希发布并保留上一版；API 只增不删 |
| Console API（无状态） | ✅ | 滚动重启即可；SSE 掉线有票据 + 轮询回落，不丢数据 |
| Worker 池 | 🟡 状态/消息无损、不双跑；**节点副作用可能重放** | 单步可能跑几十分钟，drain 有界 → 超时那一步会被重放，业务节点须幂等 |
| 执行状态 / 消息格式 | 🟡 只增字段可以；**改语义必须 bump `schema_version`** | 混跑窗口内旧消费者对新语义必须拒收 |
| Console DB | 🟡 expand → backfill → 切读 → contract | 尚无迁移框架，见「已知缺口」 |

### 发布顺序（重要）

1. **先升消费者，再升生产者**：`schema_version` 只在新语义上线时 bump；旧 worker 拒收「比自己新」的消息（进 DLQ + 告警），所以 producer（console BFF / 调度服务 / `event_filter`）必须后升。**例外**：worker 自身的重投/交接/归一化副本继承源消息的版本（不会把旧载体写成新版本），worker 灰度因此不要求整池原子升级；但 `event_filter` / 调度 / BFF 在同一批次产生的是**新载体**，仍按生产者处理。**回滚/生产者先行的处置**（A49-01）：旧 worker 对超版本载体只死信一次，**不再克隆恢复副本**（副本仍是同一版本，克隆会形成 DLQ 线性增长的热环）；载体在 DLQ 里留证，升级后从 DLQ 重放即可，`suspended` 执行也可由后续 event/cancel/timeout 唤醒。
2. **Console API 先升**（只增不删），确保新旧前端都能用；前端静态资源后发。
3. **Worker 灰度**：先起新版本实例与旧实例**同 consumer group 混跑**，观察 `lease_conflicts` / `reclaimed` / `dead_lettered` 无异常后，再逐个 drain 旧实例。
4. 每台 worker 的停机动作是 `draining`，不是 kill（见下）。

### Worker 停机：draining 语义

信号（SIGTERM/SIGINT）与控制通道 stop 命令都只**请求** drain，绝不立即退出：

1. 注册表状态置 `draining`（`GET /api/services` 可见 `flow_worker` 的 status），编排/就绪探针据此摘流量；该状态**立即落盘**，不再等 10s 心跳（S18-02A）；
2. 消费循环在**任务边界**退出：在途任务完整跑完并 ack；drain 前已发出、drain 后才返回的 `read`/reclaim 结果**不 dispatch、不 ack**（post-read gate），消息留 pending；
3. 等待有界（advisory）：超过 `PLAITA_WORKER_DRAIN_TIMEOUT`（默认 30s）仍有在途任务 → 停止等待、**尽力** `execution.cancel()` 并退出；**不承诺节点在 N 秒内中止**。若步在进程退出前完成，仍允许 ack；若进程先退出，消息留 pending，由其他 worker 经 XCLAIM 从**步界检查点**续跑（那一步会重放）。可重试失败的退避 sleep 在超时后立即中断，不再 enqueue/ack；
4. 退出前把注册表状态置 `stopping`（同样立即落盘）再注销；注销后 closed 闸封住心跳/容量/迟到计数写入，注册键不会复活。

> 语义边界：`PLAITA_WORKER_DRAIN_TIMEOUT` 是「停止领取/停止等待」的上界，不是「节点中止」的上界。硬中断（把 stop token 传进节点/沙箱）是 B18-01，**尚未实现**；在它落地前不要对外承诺「超时放弃当前步」。

K8s/systemd 侧对齐（否则 drain 会被 SIGKILL 提前打断）：

```yaml
terminationGracePeriodSeconds: 90   # > PLAITA_WORKER_DRAIN_TIMEOUT + 收尾开销（注销/日志/连接关闭）
# 就绪探针：注册表 status != draining（或进程内 status 命令）
# preStop：调控制通道 stop（graceful=true），或直接 SIGTERM（handler 已是 drain 语义）
# 部署策略：maxUnavailable: 0（worker 池禁止并发减容）
```

> 单步可能长达数十分钟（如 HITL gate 3300s）。**不要把 grace 调到超过最长步**——drain 不保证中止节点：单步超过 grace 时仍会被 SIGKILL，随后消息重放。正确做法是接受「drain 超时 → 该步重放」，并让业务节点幂等（at-least-once 契约）。

### flow_hash 兼容门（引擎升级最容易误伤的一处）

`ExecutionState.flow_hash` = 实际加载的 Flow 的规范化 JSON 指纹，resume 时比对，防止「运行中改定义 → 续跑走错分支」。但**引擎升级可能改变指纹算法口径**，于是历史挂起执行会被判成「定义变更」。判定表：

| 存的算法标记 | 指纹是否相同 | 行为 |
|---|---|---|
| 相同 | 相同 | 正常续跑 |
| 相同 | 不同 | **拒绝**（定义确实变了）——同算法下 `allow_flow_hash_change` 也不放行 |
| 不同 | **相同** | 直接续跑并刷新标记（定义没变，只是升级换了算法标签） |
| 不同 | 不同 | 报 `flow_hash_mismatch`（`upgrade_suspected: true`），需**显式放行** |
| 缺失（老状态） | 任意 | 按同算法保守处理 |

显式放行（仅用于确认「流程定义没变 / 可接受」）：

```bash
curl -X POST "$CONSOLE/api/executions/$EXEC_ID/resume" \
  -H 'Content-Type: application/json' \
  -d '{"resume_type":"continue","data":{"allow_flow_hash_change":true}}'
```

放行会打 WARNING 并把新指纹写回状态，留审计痕迹。**同算法下哈希不同时该开关无效**——那种情况必须改回定义或新建执行。

### 兼容纪律（写进 review checklist）

- `ExecutionState` **只增字段**，新字段必须有缺省值；读取方必须按「None = 没有这个能力」处理（见 `tests/unit/test_upgrade_compat.py`）。
- 任务消息只增字段；**改语义必须 bump `TASK_SCHEMA_VERSION`**（`plaita/server/task_queue.py`）。入队一律用 `enqueue_task()`，别直接 `XADD`（否则丢信封）。**重投/交接/归一化副本必须传 `schema_version=源版本`**——副本承载源载体的语义，盖当前版本会让混跑池里的旧消费者拒收唯一恢复载体。
- Console API 响应只增不删；前端资源内容哈希 + 保留上一版。
- DB 变更走 expand → backfill → 切读 → contract，禁止一步到位改列语义。

### 升级验收清单

1. 消息不丢：终态落盘或进 DLQ（`dlq_length` 增量可解释）；
2. 同一执行不双写：无 `lease_conflicts` 引发的双终态；
3. 状态可跨版本读：新旧 worker 混跑期间无「状态解析失败」；
4. 升级窗口 Console API 无 5xx；
5. 明确承认并接受：drain 超时的那一步会重放（业务节点幂等或带去重键）。

### Console DB 迁移（alembic）

flow store（flows / flow_versions / local_executions / users…）的 schema 现在由
alembic 版本化，引导收敛在 `services/schema_migrations.py`，启动时幂等执行：

| 库的状态 | 引导动作 |
|---|---|
| 空库 | `create_all` 建全量 schema → 认领基线 `0001_baseline` |
| 存量库（有业务表、无 `alembic_version`） | 先按历史语义对齐（补缺失表/旧列），再**认领基线**（不重放 DDL） |
| 已有版本记录 | `upgrade head` |

要点：

- **存量库无损**：认领基线不动业务数据（已在真实旧库上验证数据行数不变）。
- 迁移目标 = **应用真正在用的引擎**（`services/schema_migrations` 通过
  `config.attributes["engine"]` 传入）。这点很关键：早期实现只读
  `get_settings().db_url`，结果是「stamp 表面成功、真实库永远没有版本记录」。
- 新增变更写独立 revision（`migrations/versions/`），遵循 expand → backfill →
  切读 → contract，禁止一步改列语义；`downgrade` 必须写并在测试库里跑过。
- alembic 缺失时退回旧的 `create_all` 路径并打 WARNING（最小安装仍可启动，
  但 schema 无版本记录）。

### 本地单机模式的启动对账（无队列时的「僵尸执行」）

本地模式没有 Redis 队列：执行由进程内线程推进。**console 重启后，重启前
`running` 的执行会失去唯一执行线程而永远卡住**（集群模式靠 pending 重投，
本地模式没有等价物）。现在启动时对账，口径由 env 决定：

| `PLAITA_CONSOLE_RECONCILE_ORPHANS` | 行为 |
|---|---|
| `suspend`（默认） | 置 `suspended` 并写入原因——执行在每个步界都落过 checkpoint，可人工恢复（不自动续跑：本地模式没有 at-least-once 保障，自动续跑等于无声重放副作用） |
| `fail` | 置 `failed` + 结束时间（宁可显式失败也不留可恢复态） |
| `off` | 不对账（仅排查期使用） |

只处理 `running`，条件更新（CAS）避免与执行线程并发完成打架；结果写入
`GET /health` 的 `reconcile` 字段，启动日志同时告警。

集群档另有 `GET /health` 的 `engine_sync` 字段：启动回填已发布流程到引擎
Redis 的结果（`status=ready/degraded`、`written/skipped/errors/versions/
batches/truncated/truncated_reason/elapsed_ms`）。回填按每批 200 版本一个
pipeline，并受墙钟预算 `PLAITA_CONSOLE_ENGINE_SYNC_DEADLINE`（默认 5s）与
`PLAITA_CONSOLE_ENGINE_SYNC_MAX_VERSIONS`（默认 5 万）约束；触界只降级、
不阻断启动，日志有 WARNING。Redis 半开时回填在声明的读超时内失败并降级，
不会让 readiness 永久不达。

非 SQLite 的 `PLAITA_CONSOLE_DB_URL` 显式设置 connect/pool 超时（默认 5s）
与 PG `lock_timeout=5s`；DB 黑洞/半开时启动在约 5s 内具名失败（不回退
`create_all` 再阻塞一次），启动日志中的目标 URL 已脱敏。

### 已知缺口（本 runbook 尚未覆盖） {#已知缺口}

- **回滚演练**：alembic 的 `downgrade` 路径尚无测试覆盖（当前只有基线版本）。
- ~~Console flow store 没有迁移框架~~ → 已引入 alembic（见下「Console DB 迁移」），
  但**回滚（downgrade）尚未验证**，且当前只有基线版本，真实变更仍需按 expand/contract 写。
- `engine_version` 目前只做观测（跨 minor resume 打 WARNING），未做硬门。
- **按资源的任务路由**：worker 侧只有本地准入闸（`PLAITA_WORKER_MIN_FREE_DISK_GIB`
  盘守线、`PLAITA_WORKER_DENY_REPOS` 按仓拒跑），没有「dispatcher 按 worker
  上报的盘/负载选投递对象」——后者需要心跳携带资源画像，尚未实现（#49 可选修法）。

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
```

**读 `XLEN` 时注意残留条目**：`XLEN` 计的是 Stream 里的条目数，含「已 `XACK` 未 `XDEL`」的残留（XACK 与 XDEL 之间进程被杀）——它们的执行早已终态，不代表有活干。判积压先看 `XPENDING`（`pending>0` 才是待处理）与消费组 `lag`；若 `XLEN>0` 而 `pending=0`/`lag=0`，按残留处理：worker 启动时与每 300s 会兜底回收（每轮 ≤256 条，大量残留按轮次收敛，`residue_swept` 计数可见），也可 `XRANGE` 看条目 payload 里的 `execution_id`，在 console 实查执行确为终态后确认无积压（2026-10-07 曾据 `XLEN=3` 误判「新系统未投产」，实为演练残留）。

### console 审计口径（A55-S2/S3/S4）

- **拒绝审计按窗口合并**：`*.denied` / `auth.denied` / `auth.login_failed` 以
  `(action, actor, ip, tenant, reason)` 为键，60s 内只落一行；被合并的次数累计到
  该键下一行的 `detail.suppressed`。平台身份闸（`/api/cluster`、DLQ replay、租户
  生命周期）与租户成员闸同口径——低权流量不能按请求数撑大审计表。失败登录的
  `reason` 入键：`bad_credentials` 与锁定期 `locked` 各自成行，锁定尝试不会被
  前一条口令错误行吞掉。
- **租户成员操作归属**：`tenant.add_member` / `set_member_role` / `remove_member`
  的 `tenant_id` 是**目标租户**（不是平台调用者的 `""`），`detail.target_tenant`
  同值回显——否则该租户 admin 在自己的 `/api/audit` 里看不到「谁改了本租户成员」。
- **AI 生成留痕**：`flow.ai_generate`（`POST /api/flows/ai-generate/stream`，
  平台身份专属）在成功启动生成流时写审计，只记 `agent` 与工作区租户，prompt
  与生成物不入库；拒绝走 `flows.ai_generate.stream.denied`。
- **DLQ replay 归属**：`POST /api/queues/dlq/{dlq_id}/replay` 的成功、具名拒绝
  以及 404/422 尝试都写 `dlq.replay`；行内 `tenant_id` 是**死信条目所属租户**
  （不是调用者的 `X-Tenant-ID`），`detail.target_tenant` 同值回显。`actor` /
  `auth_source` / `key_fingerprint`（API-key 的 sha256 前 16 位）/ `request_id`
  随行落库，便于回答「哪把 key 在何时回灌了哪个租户的死信」。
- **读面边界**：`GET /api/audit` 的 `limit` 必须 ≥1（负值 422），上限 1000，
  默认 200；`before_id` 是稳定游标。服务层（`services/audit.list_audit`）再钳
  一次，绕过 HTTP 的内部调用也不能物化全表；每次读取另写 `audit.read`，记录
  过滤条件、生效上限与返回行数。

## start dedup 受理状态机迁移（r59） {#start-dedup-迁移}

`dedup_key` 的 claim/占位行/入队/owner/release 现在统一走
`plaita/server/start_dedup.py` 的 token CAS Lua（完整状态表见
[FlowWorker · 受理状态机](flow-worker.md#start-dedup-受理状态机)）：

- **滚动升级硬约束**：新代码只写新 owner/token 键
  （`{ns}:start-dedup-owner:{key}` / `{ns}:start-dedup-token:{key}`），旧
  console 读不到新键。升级 console 必须**先排空旧实例再启动新实例**，禁止
  新旧 console 混跑；worker 池升级纪律不变（先升消费者）。
- **旧键兼容**：旧 `<map>:owner=claiming:A` 在新代码下按 live claim 处理并
  迁移到新键+token（不换 id、不重复补 carrier）；旧 `enqueued:A` 直接返回
  A。旧键 7 天自然过期，**不要**脚本盲删：`start-dedup:foo:owner` 同时可能
  是 `foo:owner` 的 map 键；新代码遇到该反向碰撞会先把 foo 的 owner 迁到新
  键再释放共键，无法安全迁移时返回 409 `execution_start_legacy_collision`。
- **schedule/cron**：同一 due 的重复触发在 schedule 入口预认领处收敛（不再
  铸新 id/入队/占配额）；写占位行后、XADD 前必须 reservation（慢写跨 claiming
  TTL 先同 token 续期），配额/reservation/入队失败逐级回滚。**mark 失败先原地
  恢复；map 仍属本 id 且无墓碑时 park（保留载体/行/映射、不回滚配额），同 due
  重试收敛原 id**；孤儿 row 在时同 id 恢复，只有 row 缺失/墓碑才清理换新 id。
  人工 `manual` 触发仍走独立唯一键。
- **客户端语义**：`execution_start_in_progress` /
  `execution_start_superseded` 409 带 `Retry-After: 1`；console 前端对两者
  自动用同一 `dedup_key` 退避重试，窗口 65s ≥ claiming TTL 60s；超窗提示刷新
  执行列表，**不要换 key 重复提交**（会形成第二次执行）。其他调用方应同样重试
  同一 dedup_key。
- **mark 失败/carrier 已消费（#289）**：mark 失败不再 discard/墓碑/XDEL，而是
  先 `recover_after_mark_failure`（guard→reserve→mark）原地恢复；map 仍属本 id
  且无墓碑时 park，同 key/同 due 重试收敛原 id（409 in-progress 引导）。worker
  消费点另有终态防线：载体命中的执行已 completed/error/cancelled 时具名放弃并
  ack，不再执行。
- **force DELETE**：guard/reservation 后、`mark_enqueued` 前被删除的 start
  载体由失败路径 XDEL 清收（避免队列残留 tombstone 载体）；执行墓碑仍是
  worker 侧最终防线。
- **不变量**（A1 r106 对抗用）：① 同一 dedup_key 任意交错最多一个
  execution_id/一条可执行 carrier/一个配额；② 任何写转换都要求
  map==id、owner/token 匹配、无墓碑；③ release 只在 token 匹配（失败清理）
  或 row 缺失且无 token（孤儿）时生效，墓碑例外；④ owner 不能被另一个 id
  覆盖；⑤ 旧格式只读迁移，绝不静默把源 owner 写进不同 key 空间。

## 指标与告警（plaita#26） {#指标与告警}

在此之前死信产生、队列积压、worker 全灭、观测丢弃**全是静默的**——没有指标
端点、没有告警出口，值守只能靠人刷日志。现在两条出口：

### `/metrics`（Prometheus 文本）

| 端点 | 覆盖 | 鉴权 |
|------|------|------|
| worker `http://<host>:<PLAITA_METRICS_PORT>/metrics` | 本进程队列 `stream_length`/`pending`/`dlq_length` + 全部进程内计数器 + worker 存活/活跃任务数/draining/心跳时间戳 | 无（运维网内网面，勿暴露公网） |
| console `/api/metrics` | **集群级**：各队列积压 + DLQ 堆积 + 注册表中存活的 flow_worker 数；`plaita_metrics_scrape_degraded` / `plaita_metrics_failed_commands_total{command=…}` 标注本次抓取的部分读失败 | 任意**已认证会话（viewer+）**或 `X-Admin-API-Key`；全局观测面是否收窄到平台身份待 owner 裁决（见 `docs/proofs/s6-global-observability-policy.md`） |

`plaita_workers_registered_total` 归零 = worker 全灭。**但该 0 值可能是「读不到」**：
console 抓取是逐命令 best-effort 的，单个 `XLEN`/`XPENDING`/注册表扫描失败时
仍返回 200 与 0 值以保证页面不整体 500。此时 `plaita_metrics_scrape_degraded`
为 1、失败命令计入 `plaita_metrics_failed_commands_total`，并打 WARNING（60s
限流）——**告警规则应先看 degraded=0 再判 0 值**。全断（SCAN 本身失败）仍
500 fail-closed。`/api/events`、`/api/logs` 等列表端点同理：部分键读取失败时
响应带 `degraded: true` 与 `skipped: n`（空列表 ≠ 没有数据），并打 WARNING。

启动：

```bash
PLAITA_METRICS_PORT=9100 python -m plaita.server.flow_worker \
    --redis-url "$PLAITA_REDIS_URL"          # 或 --metrics-port 9100
```

观测队列丢弃（Langfuse `background=True`）经 `LangfuseCallback.observer_stats()`
读 `dropped`，宿主可自行转成指标/告警。

### 告警 webhook

设置 `PLAITA_ALERT_WEBHOOK` 后，死信（`dead_letter`）与僵尸巡检处置
（`zombie_reaped`）事件以 JSON POST 发出：

```json
{"event": "dead_letter", "queue": "plaita:flow:queue:v2",
 "dlq_key": "plaita:flow:queue:v2:dlq", "dlq_id": "1700-0",
 "message_id": "1699-0", "reason": "max_deliveries=5",
 "delivery_count": 5, "schema_version": 1, "dlq_length": 7,
 "dlq_max_len": 1000, "trimmed_count": 0, "ts": 1699999999.5}
```

发送是**有界队列 + 后台线程**的 best-effort 旁路：webhook 端慢/挂不会反压队列
消费，队列满或投递失败只计数（worker `/metrics` 的
`plaita_alert_webhook_{sent,failed,dropped}_total` 可见）。

## 僵尸执行巡检 {#僵尸执行巡检}

worker 崩溃后 pending 里的 start 任务重投会**另起全新执行**重跑，旧行永久停在
`running`。例行巡检用 `scripts/reap_zombie_executions.py` 把超期未更新的
running 行标记为 error(`orphaned`)，供监控/人工复核：

```bash
python scripts/reap_zombie_executions.py \
    --redis-url "$PLAITA_REDIS_URL" --idle-minutes 60 [--dry-run] \
    [--alert-webhook "$PLAITA_ALERT_WEBHOOK"]
```

`--alert-webhook`（缺省跟随 `PLAITA_ALERT_WEBHOOK`）在**真处置成功**后逐条
POST `{"event": "zombie_reaped", ...}`；dry-run 不告警，告警通道故障不影响
巡检推进（事件已落盘）。

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

## 唤醒预算达限（plaita#73） {#唤醒预算达限plaita73}

**症状**：error 态执行点 console 详情页的「从断点重试」没反应（旧版 BFF 返回
「已受理」而 worker 静默拒绝）；worker 日志里反复出现

```
执行 <id> G1 唤醒次数达上限（2/2），拒绝再次唤醒——error 终态保留，避免「唤醒重置预算」无限循环
执行 <id> 确定性失败已判不可救（12/12），拒绝唤醒重跑——error 终态保留
```

**为什么有上限**：`resume_type=retry` 会清零节点重试预算（设计意图：人工唤醒后拿
全新预算），只清不限次数就成自毁循环——`唤醒 → 清零预算 → 跑满预算 → 又终态化 →
再唤醒`（2026-10-10 实测某执行一天被唤醒 5 轮、每轮烧 5 次沙箱；这条链的调用方
是 keeper/inflight-watch 的**自动** `retry`）。两道闸都以 `FlowWorker` 类常量为准：

| 计数键（default 租户；其他租户加 `plaita:{tenant}:` 前缀） | 阈值 | 语义 |
|------|------|------|
| `plaita:execution:g1wakeups:{id}` | `G1_MAX_WAKEUPS` = 2 | 该执行被 `retry` 唤醒的次数（**人工与自动共用额度**） |
| `plaita:execution:nofail:{id}` | `DETERMINISTIC_FAILURE_MAX` = 12 | **连续**确定性失败次数（`exited 1`/`sync_in`/超时…）；任一节点成功推进即清零，G1 唤醒**不**清零 |

**处置**（人工解封 = 重置预算，只在确认失败原因已修复后做）：

```bash
# 1) 先看计数与执行状态
redis-cli GET plaita:execution:g1wakeups:<execution_id>
redis-cli GET plaita:execution:nofail:<execution_id>
redis-cli GET plaita:execution:<execution_id>     # status / error 内容

# 2) 修复根因（配额、沙箱、定义…）后删计数键，再点「从断点重试」/ 发 retry 消息
redis-cli DEL plaita:execution:g1wakeups:<execution_id>
redis-cli DEL plaita:execution:nofail:<execution_id>
```

- BFF 侧的 resume 端点对 `error + retry` 会先读这两个键，达限即返 **409**
  （detail 里带 `reason` / `counter_key` / 解封提示），不再静默入队——操作台把
  该 message 直接显示在「从断点重试」按钮下方；
- 计数键带 7 天 TTL，不会永久占位；`nofail` 的清零**只**发生在节点成功推进时，
  因此「同一位置反复撞墙」最多烧 12 次全款即停，别把 12 当固定值——它是
  `DETERMINISTIC_FAILURE_MAX`，调它请同步 console 的判据（同源常量）；
- 上限是**保护**：达限即停 ≠ 执行不可救，人工确认修复后删键即可继续。
- **两道拒绝只对 `error` 态生效**（plaita#53）：`running` 执行的 `retry` 是
  「从断点续派」（与 `continue` 同路），即使计数键达限也照常派发——计数键寿命
  长于状态行，按「已终态」拒绝会得到「返回已受理、一行不推进」的假受理。

## 故障手册

| 现象 | 可能原因 | 动作 |
|------|----------|------|
| 任务不消费 | group/stream 键不一致；Worker 未起 | 核对 `--queue-name` / group；看 Worker 日志 |
| `XLEN>0` 但 `XPENDING`/`lag` 为 0 | 已 `XACK` 未 `XDEL` 的残留条目（XACK 与 XDEL 之间进程被杀），**不是**真积压 | worker 启动时与每 300s 兜底回收（`residue_swept`）；`XRANGE` 看 payload 的 `execution_id`，console 实查为终态即可确认无活干 |
| pending 堆积 | 处理失败反复 reclaim；lease 冲突 | 查日志；调大 lease TTL；看 DLQ |
| DLQ 增长 | `max_deliveries` 触顶；毒丸/业务错 | `XRANGE` DLQ 查 `reason`；修业务后**必须**经 replay helper / `POST /api/queues/dlq/{dlq_id}/replay` 回灌（一次性标记 + 审计 + 源 `schema_version` 继承；无 `execution_id` 的 start 会铸造确定性 id）。裸 `enqueue_task` 绕过一次性标记，且在回滚窗口可能把超版本载体按错误语义重跑。活 worker 持租约的执行会被死信守卫跳过，见 [FlowWorker · 长步骤与消息回收](flow-worker.md#长步骤与消息回收)。replay 审计流 `plaita:dlq:replay:audit` 有界（`PLAITA_REPLAY_AUDIT_MAX_LEN`，默认 1000，超限裁最旧并 WARNING）——需长期合规留存的把该流外送归档 |
| 反复重投，日志刷「保存执行状态失败 (…)」 | Redis 写路径瞬断/序列化失败——落盘失败已不再静默 ack（2026-10 评审修复） | 查 Redis `INFO`/延迟日志；恢复后 pending 自动重投收敛，勿人工 ack |
| 反复重投，日志刷「挂起任务投递失败」 | 挂起服务队列 `plaita:{subtype}:queue` rpush 失败；suspended 已保留等重派 | 查对应外延服务（DelayService 等）与其队列长度；恢复后重投自动重派 |
| 双 resume | 旧版本无 lease | 升级到含 lease 的版本；查 lease key |
| `error` 执行点「从断点重试」无反应 / API 返 409 | `retry` 唤醒预算达限（`g1wakeups` 或 `nofail`） | 见 [唤醒预算达限](#唤醒预算达限plaita73)：修根因 → `DEL` 计数键 → 重试 |
| `running` 执行点「从断点重试」返回 200 却毫无推进（`node_timings`/`last_update_time` 不动） | ① 推进租约被**在册** worker 持有 → `continue`/`retry` resume 消息按既有语义 ack 丢弃（事件/超时/取消决议载体不 ack，留 pending 重投；worker 日志有「**本次 resume 未生效**」）；② 条件节点/switch 分支未命中且线上引擎**不含** `strategies.py` 的未命中防御 → 不派发、不落终态，执行永久 `running` | ① 确认持有者已卡死则人工 cancel 后按需重开，反复 retry 无效；② grep worker 日志的 `matched no branch` / `has no 'next'` 确认引擎版本——升级到含该防御的版本后同图会落 `error` 终态（plaita#53，见 [FlowWorker · running 执行的 retry](flow-worker.md#running-执行的-retry)） |
| 挂起永不恢复 | EventBus 与 subscription 不同 Redis；`--no-event-bus` | Worker/Filter 同总线；去掉 no-event-bus |

## 与可靠性文档的关系

语义边界见 [FlowWorker · 可靠性边界](flow-worker.md#可靠性边界必读)。业务幂等见 [幂等 Resume](idempotent-resume.md)。
