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
| 节点重试计数（可重试预算） | `plaita:execution:noderetry:{id}`（裸整数，7d TTL） | `plaita:{tenant}:execution:noderetry:{id}` |
| G1 唤醒计数（`retry` 次数上限） | `plaita:execution:g1wakeups:{id}`（裸整数，7d TTL） | `plaita:{tenant}:execution:g1wakeups:{id}` |
| 确定性失败计数（连续失败上限） | `plaita:execution:nofail:{id}`（裸整数，7d TTL） | `plaita:{tenant}:execution:nofail:{id}` |
| 任务队列 | `plaita:flow:queue`（平台共享，消息内带 `tenant_id`） | 同左 |

> 这些**机制键**（lease / fence / cancel / noderetry / g1wakeups / nofail）都是裸
> 整数或非执行状态载荷，console 的列表/详情路径按前缀逐个排除
> （`_is_mechanism_key`）——新增同类键必须同步登记，否则 `GET /api/executions`
> 对 int 调 `.get()` 直接 500（历史已发生四次）。

兼容规则：消息缺 `tenant_id` 视为 default；default 租户沿用历史键前缀，新旧版本混跑时 default 流量不受影响，非 default 租户需 console 与 worker 双侧升级。

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
| `PLAITA_METRICS_PORT` | unset | worker 的 Prometheus 抓取端端口（unset/`0` = 不启动） |
| `PLAITA_METRICS_HOST` | `0.0.0.0` | worker `/metrics` 监听地址 |
| `PLAITA_ALERT_WEBHOOK` | unset | 死信/僵尸巡检事件的 JSON POST 出口 |
| `PLAITA_ALLOW_EXPERIMENTAL_DB` | unset | 允许 factory 创建 db EventBus/subscription |
| `PLAITA_NODES_WORKSPACE_ROOT` | 由 worker 推导（见下） | `writefile` 节点的写入根（jail） |
| `PLAITA_ALLOW_UNRESTRICTED_WRITES` | unset | `=1` 关闭 writefile jail（仅单机信任部署） |
| `PLAITA_CODE_BACKEND` | `subprocess` | worker 注册 `code` 节点时生效的沙箱后端 |
| `PLAITA_SANDBOX_ALLOWED_BACKENDS` | `docker` ∪ 生效后端 | `code` 节点后端白名单（见下） |
| `PLAITA_WORKER_DRAIN_TIMEOUT` | `30` | 优雅停机等待在途任务的上限（秒）；超时放弃当前步并退出，消息留 pending 待 XCLAIM 接管 |
| `PLAITA_WORKER_MIN_FREE_DISK_GIB` | unset（= 关闭） | claim 前的本机盘守线（GiB，#49）：低于它就不领任务、空转重探、盘回线自动恢复。应与 flow 内 preflight 节点的阈值对齐（见下） |
| `PLAITA_WORKER_DISK_GUARD_PATH` | `.`（worker 工作目录） | 盘预检路径（`os.statvfs` 的探测点）；须落在「跑流程会写满」的那个卷上 |
| `PLAITA_CONSOLE_RECONCILE_ORPHANS` | `suspend` | 本地模式启动对账口径：`suspend` / `fail` / `off` |

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
   家目录（`/` 不构成边界，逐级后退）。

生效值启动日志可见（`writefile 写入 jail: …`）。**多租户 / 不受信流程部署请显式
配置第 1 条**，把根收敛到业务仓或沙箱目录；`PLAITA_PROJECT_ROOT` 已设的部署
（console 拉起的 worker、Docker 镜像）默认即落在部署根内，无需额外配置。

jail 是**进程级**的：worker 一次启动一个根，不能按执行/租户分别设根（`--concurrency`
下多任务共享同一根）。跨租户写不同目录的部署请给每租户独立 worker。

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
`PLAITA_SANDBOX_TIMEOUT` 约束，超时/取消整组击杀 + env 白名单）；`unsafe` 是历史
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

1. **先升消费者，再升生产者**：`schema_version` 只在新语义上线时 bump；旧 worker 拒收「比自己新」的消息（进 DLQ + 告警），所以 producer（console BFF / 调度服务）必须后升。
2. **Console API 先升**（只增不删），确保新旧前端都能用；前端静态资源后发。
3. **Worker 灰度**：先起新版本实例与旧实例**同 consumer group 混跑**，观察 `lease_conflicts` / `reclaimed` / `dead_lettered` 无异常后，再逐个 drain 旧实例。
4. 每台 worker 的停机动作是 `draining`，不是 kill（见下）。

### Worker 停机：draining 语义

信号（SIGTERM/SIGINT）与控制通道 stop 命令都只**请求** drain，绝不立即退出：

1. 注册表状态置 `draining`（`GET /api/services` 可见 `flow_worker` 的 status），编排/就绪探针据此摘流量；
2. 消费循环在**任务边界**退出：在途任务完整跑完并 ack；
3. 等待有界：超过 `PLAITA_WORKER_DRAIN_TIMEOUT`（默认 30s）仍有在途任务 → 放弃当前步并退出。**该消息不 ack**，留在 pending，由其他 worker 经 XCLAIM 从**步界检查点**续跑（那一步会重放）；
4. 退出前把注册表状态置 `stopping` 再注销。

K8s/systemd 侧对齐（否则 drain 会被 SIGKILL 提前打断）：

```yaml
terminationGracePeriodSeconds: 90   # > PLAITA_WORKER_DRAIN_TIMEOUT，留出收尾余量
# 就绪探针：注册表 status != draining（或进程内 status 命令）
# preStop：调控制通道 stop（graceful=true），或直接 SIGTERM（handler 已是 drain 语义）
# 部署策略：maxUnavailable: 0（worker 池禁止并发减容）
```

> 单步可能长达数十分钟（如 HITL gate 3300s）。**不要把 grace 调到超过最长步**——正确做法是接受「drain 超时 → 该步重放」，并让业务节点幂等（at-least-once 契约）。

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
- 任务消息只增字段；**改语义必须 bump `TASK_SCHEMA_VERSION`**（`plaita/server/task_queue.py`）。入队一律用 `enqueue_task()`，别直接 `XADD`（否则丢信封）。
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
- 计数：`acked` / `reclaimed` / `dead_lettered` / `lease_conflicts` / `failed` / `residue_swept` /
  `dlq_guard_skipped` / `claim_guard_skipped`（后两者 = 守卫/回收闸门拦下、消息留 pending 的次数，
  见 [FlowWorker · 长步骤与消息回收](flow-worker.md#长步骤与消息回收)）

也可以 Redis：

```bash
redis-cli XLEN plaita:flow:queue
redis-cli XPENDING plaita:flow:queue plaita-workers
redis-cli XLEN plaita:flow:queue:dlq
redis-cli KEYS 'plaita:execution:lease:*'
```

**读 `XLEN` 时注意残留条目**：`XLEN` 计的是 Stream 里的条目数，含「已 `XACK` 未 `XDEL`」的残留（XACK 与 XDEL 之间进程被杀）——它们的执行早已终态，不代表有活干。判积压先看 `XPENDING`（`pending>0` 才是待处理）与消费组 `lag`；若 `XLEN>0` 而 `pending=0`/`lag=0`，按残留处理：worker 启动时与每 300s 会兜底回收（每轮 ≤256 条，大量残留按轮次收敛，`residue_swept` 计数可见），也可 `XRANGE` 看条目 payload 里的 `execution_id`，在 console 实查执行确为终态后确认无积压（2026-10-07 曾据 `XLEN=3` 误判「新系统未投产」，实为演练残留）。

## 指标与告警（plaita#26） {#指标与告警}

在此之前死信产生、队列积压、worker 全灭、观测丢弃**全是静默的**——没有指标
端点、没有告警出口，值守只能靠人刷日志。现在两条出口：

### `/metrics`（Prometheus 文本）

| 端点 | 覆盖 | 鉴权 |
|------|------|------|
| worker `http://<host>:<PLAITA_METRICS_PORT>/metrics` | 本进程队列 `stream_length`/`pending`/`dlq_length` + 全部进程内计数器 + worker 存活/活跃任务数/draining/心跳时间戳 | 无（运维网内网面，勿暴露公网） |
| console `/api/metrics` | **集群级**：各队列积压 + DLQ 堆积 + 注册表中存活的 flow_worker 数 | 管理面 `X-Admin-API-Key` |

`plaita_workers_registered_total` 归零 = worker 全灭。启动：

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
 "dlq_key": "plaita:flow:queue:v2:dlq", "message_id": "1699-0",
 "reason": "max_deliveries=5", "delivery_count": 5, "ts": 1699999999.5}
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

## 故障手册

| 现象 | 可能原因 | 动作 |
|------|----------|------|
| 任务不消费 | group/stream 键不一致；Worker 未起 | 核对 `--queue-name` / group；看 Worker 日志 |
| `XLEN>0` 但 `XPENDING`/`lag` 为 0 | 已 `XACK` 未 `XDEL` 的残留条目（XACK 与 XDEL 之间进程被杀），**不是**真积压 | worker 启动时与每 300s 兜底回收（`residue_swept`）；`XRANGE` 看 payload 的 `execution_id`，console 实查为终态即可确认无活干 |
| pending 堆积 | 处理失败反复 reclaim；lease 冲突；队列饱和下消息排队等槽 | 查日志；调大 lease TTL；看 DLQ。**注意**：租约活着的执行消息不再被 XCLAIM（回收闸门 #47），`pending` 里久留但 `times_delivered` 不涨 = 正常排队，不是故障 |
| DLQ 增长 | `max_deliveries` 触顶；毒丸/业务错 | `XRANGE` DLQ 查 `reason`；修业务后可人工 `enqueue_task` 回灌（活 worker 持租约的执行会被死信守卫跳过，见 [FlowWorker · 长步骤与消息回收](flow-worker.md#长步骤与消息回收)）。**同一执行最多每 30min 产生一份「重入队恢复副本」型死信**（`DLQ_REQUEUE_COOLDOWN_SECONDS`，#47）——超出此速率说明有别的来源，别当成冷却没生效 |
| 反复重投，日志刷「保存执行状态失败 (…)」 | Redis 写路径瞬断/序列化失败——落盘失败已不再静默 ack（2026-10 评审修复） | 查 Redis `INFO`/延迟日志；恢复后 pending 自动重投收敛，勿人工 ack |
| 反复重投，日志刷「挂起任务投递失败」 | 挂起服务队列 `plaita:{subtype}:queue` rpush 失败；suspended 已保留等重派 | 查对应外延服务（DelayService 等）与其队列长度；恢复后重投自动重派 |
| 双 resume | 旧版本无 lease | 升级到含 lease 的版本；查 lease key |
| `error` 执行点「从断点重试」无反应 / API 返 409 | `retry` 唤醒预算达限（`g1wakeups` 或 `nofail`） | 见 [唤醒预算达限](#唤醒预算达限plaita73)：修根因 → `DEL` 计数键 → 重试 |
| 挂起永不恢复 | EventBus 与 subscription 不同 Redis；`--no-event-bus` | Worker/Filter 同总线；去掉 no-event-bus |

## 与可靠性文档的关系

语义边界见 [FlowWorker · 可靠性边界](flow-worker.md#可靠性边界必读)。业务幂等见 [幂等 Resume](idempotent-resume.md)。
