# WORKER_PROTOCOL — 执行状态行协议（plaita#320 A1 / #322 B1 所有权 v1）

> **定位**：协议完备性**存档**——把执行状态行新字段的 schema、写序不变量与
> 迁移语义钉成文档，供跨语言/跨实现对账时引用。**不构成任何 Go 实现承诺**
> （ADR-001：Go 已 frozen，Python 为 reference、Rust 为 oracle）；此前
> 「写进 WORKER_PROTOCOL.md 供以后 Go」的措辞按 A1 r138 评审 §二.4 修正为
> 存档定位。

## 1. 状态行新字段（`ExecutionState`，`plaita/storage/base.py`）

| 字段 | 类型 | 写者 | 语义 |
|---|---|---|---|
| `pending` | `Optional[Dict]` | worker 挂起落盘（与 suspended 同一次 CAS） | 执行意图引用——「为什么停下、在等什么」。引用不复制：订阅/timer 内容仍以 context（`$NODE.<id>.subscription_id`）与外部存储为准 |
| `generation` | `int`（默认 0） | fenced CAS 落盘盖章（`storage/fenced.py`） | 复用 fence 世代——最后一次 fenced 写的世代。0 = 老行/未过 fenced 路径。#321 reconciler 幂等键 `execution_id + generation` 的取值源 |
| `progress_at` | `Optional[str]`（ISO） | 行创建（首次 running 落盘）置 now；仅 `step_persist` 刷新 | 「步骤在前进」证据。与 `last_update_time` 严格正交：后者被看门狗每 TTL/3 刷新（活性），两者构成 mode 5（长节点卡死）双信号 |
| `next_step` | `Optional[Dict]` | start 首落 / `retry_wakeup` / `step_persist` | 预分配的下一步引用 `{node_id, step_id, parent_step_id}`。**#322 起 `step_id` 是铸造的分配决定**（`core/ownership.new_step_id()`；声明即所有权，不可再生），`parent_step_id` = 刚消费的 token（声明链锚）——schema 形状不变，语义从 aid 升级为决定（见 §6） |
| `last_report_at` | `Optional[str]`（ISO） | `report_progress()` 的 worker 单点写（节流） | 「步内有上报的推进」时刻（#322）。与 `progress_at` / `last_update_time` 三信号正交；终态行**不清**（观测残留非意图声明）。None = 无上报/老行 |

### 1.1 `pending` schema

```
{
  "kind": "event" | "timer" | "service" | "start" | "cancel",
  "ref_id":      str | null,   # event: subscription_id；service/timer 视生产者
  "event_type":  str,          # event 必填（resolved 后的动态 $expr 结果）
  "due_at":      iso,          # timer 必填
  "deadline":    iso,          # event 必填（见 §3）；终局时限
  "node_id":     str | null,   # 最内层挂起节点（NS 帧栈对齐，见 §4）
  "payload":     dict,         # 仅 service：{service_type, service_config}
  "recorded_at": iso,
  "migrated":    true          # 仅读面惰性推导产物（§5），手写禁止
}
```

本轮（#320）写路径只产出 **`event` / `service`**；`timer` / `start` / `cancel`
为 schema 预留（生产者随各自里程碑启用：#321 reconciler / 调度占位行 / 取消
意图落行）。写侧经 `validate_pending_ref` fail-closed（kind 白名单、各 kind
必填项）；加载路径**不做**硬校验（畸形 pending 不得放大成毒丸行，读者按
缺省处理）。

## 2. 写序不变量（防 #303 形状经对账器复发，A1 r138 §三）

1. **pending 的任何增/改/清只发生在带 generation CAS 的状态行写内；终态迁移
   与 pending 清除必须同一次 CAS。**
   机械化执行点：`_persist_state_or_raise` 咽喉——`status ∈ {completed,
   error, cancelled}` 的写自动清 `pending`/`dispatch_pending`/`next_step`，
   终态行不可能带 armed 引用离开本函数（否则对账器读到「终态执行带活载体」
   = #316 违例类 orphan_terminal 的对偶生成面）。
2. **物化载体（enqueue/rpush/register）是投递线程同型物化**：补发/重投侧必须
   enqueue 前重读 status（非终态）+ generation 未变。义务落 **#321**；
   既有先例=挂起补偿派发的「持租约 + 持锁重读」（#233/#243）。

## 2.1 B1 所有权 v1（plaita#322）——step token / 幂等键 / 进度租约

> **义务声明（A1 r138 评审注记，落地存档）**：#320 窗①（ack 后崩溃）与
> 窗②（租约冲突 ack）的重建语义是「重驱节点」——**#320 重建的副作用安全
> 依赖 #322**：只有声明了 step token 的步骤经连接器幂等键获得「同逻辑步骤
> 重试 = 同键 = 外部去重」的保证。#322 落地前，#320 的重建读法只回答
> 「该做什么」，不回答「重做是否安全」。

**step token 生命周期**（write-ahead：先声明、再执行）：

- **铸造**（三个写点，均为状态行 CAS 内）：start 首落（声明 start 节点）、
  `retry_wakeup`（声明 re-run 节点 = `$LAST_NODE`）、`step_persist`（声明
  图遍历后继，`parent_step_id` = 刚消费的 token）。token = `uuid4().hex`
  （`core/ownership.new_step_id()`）——跨重放的稳定性锚在「重投读同一行」，
  不在 token 形状（#336：**不可推导**是防伪造设计，按世代/计数推导 token
  的方案不采）；挂起行 `next_step=None`（等待意图由 pending 承载）、
  终态行随咽喉同 CAS 清除（终态不得声明未完成的工作）。
  **error→retry 不漂移（plaita#336，owner 裁决案 b）**：error 终态在清
  声明**前**把行上声明转存 `state.error["failed_step"]`（形态同
  `next_step`：`{node_id, step_id, parent_step_id}`，随失败证据落盘）；
  `retry_wakeup` 先读该键——重跑节点与转存声明同节点 ⇒ 复用同 token
  （重试 N 次 = 同键 = 外部去重生效），无证据/节点漂移 ⇒ 照铸新。案 a
  （error 行豁免清声明）不采：#320 不变量不开口子。
- **消费**：worker 在每次 `run_distributed` 前把行声明的 token 绑到执行对象
  （`bind_step`）。**键锚在行上而非节点上**：声明与实际推进偶有错位
  （如 start 首步在步循环前的预消费）不影响重放稳定性——同状态行 ⇒ 同
  token。挂起/老行恢复的首步 tokenless（无声明即无所有权主张，诚实优于
  编造；该窗口的重建保护属 #321 侧义务）。

**幂等键形**：

- 执行面：`execution.idempotency_key = f"{execution_id}:{step_id}"`
  （attempt/投递计数**不进键**，只做观测；执行段恒取**根**执行 id——子执行
  `$EXECUTION_ID` 是构造期随机 uuid）。
- 复合容器：`get_child_execution()`（签名零变更）+ `bind_child_step`
  为循环迭代/并行分支派生 `f"{step_id}:{sub_key}"`（10 次迭代 = 10 个键；
  同迭代重放同键；替身子执行缺 bind_step 静默跳过）；`derive_key(i)` 为
  同名组成面。
- 连接器外发：`f"{idempotency_key}:{node_id}"`——同一声明内多个外呼调用
  互不串键。内置 http 连接器对**非安全方法**（POST/PUT/PATCH/DELETE）
  自动带 `Idempotency-Key`；GET/HEAD/OPTIONS/TRACE（语义安全）不带；
  用户显式给的头不覆盖；tokenless/替身执行对象一律不带头（收据面绝不
  打断节点）。副作用收据**只给内置连接器**，不给 code 节点（#322 边界）；
  agentrun 等 plaita-nodes 侧连接器经同一 `execution.idempotency_key`
  面接入（独立仓，另行落地；不改 Go/Rust）。

**进度租约**（三信号正交）：

| 信号 | 写者 | 语义 |
|---|---|---|
| `last_update_time` | 看门狗心跳（每 TTL/3） | 活性 |
| `progress_at` | 仅 `step_persist`（#320 钉死） | 步骤前进 |
| `last_report_at` | `report_progress()` worker 单点写（节流，`PLAITA_PROGRESS_REPORT_MIN_INTERVAL_SECONDS`，默认 10s） | 步内上报的推进 |

- 过期谓词：`now - max(progress_at, last_report_at) > step_timeout`
  （`core/ownership.step_lease_expired` / `ExecutionState.step_lease_expired`）。
- `step_timeout`：节点 `timeout` 优先（毫秒数/ISO8601），未声明用
  `PLAITA_STEP_LEASE_TIMEOUT_SECONDS`（默认 24h，**默认宽**——阈值误杀的
  代价远大于漏报）。
- **本单过期只是可观测信号**（#321「running 有租约但停滞 → fence+cancel」
  的判定输入、#316 gauge 的升级数据源）；看门狗续租行为**不变**（#27/#31/
  #55/#32/#62 五单误杀家族的教训：活性判断与节点存亡解耦）。

## 3. 窗④补强选型：deadline 必填（二选一裁量）

「事件先于订阅」窗的补强，规格给两案：事件位标（与 `event_reconcile` 复合游标
`"<ts>|<event_id>"` 同形、按订阅回放）或 deadline 必填。**本实现选 (b)
deadline 必填**：

- 位标方案的安全实现隐含两条跨层契约——事件 zset 保留期 ≥ 最长回放窗
  （否则位标指向已被修剪的区间=静默有损回放，需要新的耐久性承诺），以及
  `scan_once` 匹配层的按订阅回放扩展。超出「匹配层小扩展」的安全实现面。
- deadline 必填完全落在状态模型内：event kind pending 恒带终局时限
  （节点 `subscription_timeout` 优先，未配置回落
  `PLAITA_PENDING_EVENT_DEADLINE_SECONDS`（正数，非法忽略 WARNING）/缺省
  30 天「默认宽」），④窗验收口径按 A1 重述收窄为**「不失联（可检可终局）」
  ——事件不保证不丢，但等待必可检、超期必可终局**。超期处置消费方=#321
  reconciler（timeout resume / cancel 终局化）。
- 位标不与 (b) 互斥：作为未来扩展叠加时 schema 不破坏（pending 增可选键）。

## 4. NS 帧栈对齐

`pending.node_id` = **最内层挂起节点**（A1 r138 §二.4）：嵌套挂起（checkpoint
带 `$SUSPEND_FRAMES`，最内帧在前）取首帧 node_id，且该节点的
`subscription_id`/`event_type`/`subscription_timeout` 活在**该帧的 context
快照**里（逐帧查，不在根 `$NODE`）；根流程挂起取 `$LAST_NODE` + 根 `$NODE`。
配套：`_subscribe_event` 把 resolved `event_type` 与 `subscription_timeout`
落进 node_state（worker 挂起落盘拿不到节点对象）。**pending 重建（未来 #321
消费）不得走普通 resume**——嵌套链 resume 对 continue/retry 具名拒
（ResumeGuardError），恢复须经 `resume_type=event/timeout/cancel` 决议。

## 5. 迁移与退役

- **惰性推导发生在读面、不回写**（A1 §四.3——回写须走同款 CAS，读面推导
  免迁移写风暴）。`ExecutionState.effective_pending()` 优先级：
  显式 `pending` → 存量 `dispatch_pending` 推导 `kind=service`（`migrated:
  true`）→ suspended + 根上下文订阅存在推导 `kind=event`（`deadline: null`
  ——老行推不出节点超时配置，不编造）→ None（orphan）。
- **`dispatch_pending` 显式退役**：停写（新挂起只写 `pending`，双字段并存=
  服务派发意图双真相）；降级为只读迁移别名（推导见上）；补偿派发后的清除写
  **两字段同一次 CAS 双清**（清新留旧会让推导复活已派发凭据）。字段保留至
  存量行自然 TTL 耗尽（90 天观察窗）。
- **generation 迁移**：老行 0 → 首次 fenced 写 0→N，天然与存量互斥（CAS），
  零人工干预、零写风暴。
- **回滚真语义**（A1 §四.4 按实写）：关开关退回消息驱动时，「新字段被旧
  worker 忽略」实为旧 worker 全对象 save **物理清除**新字段——回滚后行的
  新字段不存在，不残留半态。

## 6. 消费方接口注记

- **#321 载体对账器（下游）**：按状态推动作的全部读法已备——`queued 无载体
  → start`；`running 无租约 → continue`（读 `next_step` 核对行声明的工作）；
  `running 有租约但 progress_at 停滞 → fence+cancel`（阈值可配+默认宽）；
  `suspended pending disarmed → 重建或 error(pending_lost)`；`terminal 有
  载体 → gc`。幂等键 `execution_id + generation`。
- **#316 Stage 0（只读，本轮不动）**：`scripts/carrier_audit.py` 对新字段
  无感（负控格钉：带新字段的行扫描结果与老行全等）。新字段是其未来数据源
  升级（gauge 判据从「载体四存放处」升级为「载体×pending 交叉」），接口
  注记留此，不预设实现轮。
- **#322（B1 所有权 v1，已落地）**：`next_step.step_id`/`parent_step_id` 键位
  已从预留升级为**分配决定**（铸造/消费/幂等键形见 §2.1）；`next_step` 自此
  是声明（不可再生），不再是纯 aid 字段。重建（#321）与重放的副作用安全
  依赖该 token——见 §2.1 义务声明。
- **#312（认识论分流）**：正交且互惠——词汇表管消息侧处置，本单管状态行
  意图记录；唯一交叉「无载体判定须含第四存放处（DLQ/隔离流在押=有载体）」
  是 #321 的读法义务（A125 收容不 touch 状态行 ⇒ pending 仍 armed，恢复走
  重放通道不走重建，两通道不得并发——义务同交 #321）。

## 7. 后端覆盖面

- **Redis（生产路径）**：全字段生效；fenced CAS 盖章 generation。
- **Memory（测试/单机）**：全字段生效（generation 恒 0——无租约语义）。
- **SQLAlchemy（experimental，已拒产路径）**：列集冻结在存量字段（本就无
  `dispatch_pending` 列），新字段不入列集=静默丢弃；读面推导可用（legacy
  分支不依赖新列）。本后端禁产约定见其 docstring。
