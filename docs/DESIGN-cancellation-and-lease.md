# 取消语义端到端 + 执行租约 fencing 设计

> 状态：设计稿 v1（2026-10-02，基于 feat/codeflow-loop-parent-scope @ 207d5cb 核实）
> 范围：`plaita/server/`（flow_worker / execution_lease / task_queue / event_filter）+ 引擎 `plaita/core/`（runner / context / strategies）+ console 后端（api/executions / services/local_executor）
> 先例：风格与粒度对齐 `docs/scheduler-design.md`

## 1. 定位与目标

让「取消一个执行」在集群档与本地档都成为**有保证的操作**，让「同一执行双 worker 并发推进」在 Redis 层被结构性排除。设计原则：**BFF/控制面只表达意图，worker 是执行状态的唯一写者**；挂起（suspended）执行的取消路径已在产线验证（E2E 钉住），本设计只补运行中执行的取消与租约互斥漏洞，不改动已验证语义。

## 2. 现状地图（逐处核实，2026-10-02 @ 207d5cb）

### 2.1 取消链路逐环

```
console 取消按钮
  │ POST /executions/{id}/cancel
  ▼
BFF plaita-console/backend/api/executions.py:351-418
  ├─ 终态幂等：completed/error/cancelled 直接返回 …… :386-392   ✅ 可达
  ├─ 入队 resume_type=cancel 消息 …………………………………… :395-405   ⚠️ 对运行中执行不可达（见下）
  └─ 直接写 status=cancelled ……………………………………… :407-410   ❌ 对运行中执行是覆写战争起点
  ▼
Redis 队列 plaita:flow:queue（Stream）
  └─ worker 单任务串行消费（flow_worker.py:645-732）：
     cancel 消息排在当前 start/resume 任务之后，
     而 _process_execution_result 的推进循环不到 end/suspend 不退出
     （flow_worker.py:403-458）→ cancel 消息永远等不到被消费 ❌ 结构性不可达
  ▼
worker 推进循环 flow_worker.py:403-458
  ├─ 每步把内存 state.status 覆写回 running ……… :421,441-443   ❌ 覆写掉 BFF 写的 cancelled
  ├─ is_end 写 completed ……………………………………… :407-413        ❌ 取消后被翻回 completed
  └─ 无任何取消检查点                                          ❌ 断点根因
  ▼
引擎 plaita/core
  ├─ cancel_event 父子链共享同一 Event ……………………… context.py:205
  ├─ 每节点入口无条件 clear ……………………………………… runner.py:264-268   ❌ 并发分支互相清信号
  ├─ 全仓唯一 set 点 = sync 节点超时 ……………………… runner.py:370-371
  ├─ code 沙箱等待循环消费 cancel_event → killpg …… code.py:410-417（killpg :367-384）
  └─ 进程模式 Parallel 拿到的是全新 Event（pickle 剥离）… context.py:261-270；
     仅在分支入口检查一次 ………………………………… concurrent.py:165-172
  ▼
本地档 local_executor.py
  ├─ cancel 只写 SQLite cancelled …………………………… :453-461        ✅ 写入成功
  └─ 执行线程每步覆写回 running/completed …………… :366-370,:345-355 ❌ 同集群档同病
```

**已修好、本设计不动的部分**：

- 挂起执行取消：BFF 先写 cancelled 再投 cancel 消息，worker `resume_flow` 终态短路原样返回（flow_worker.py:268-279），EventNode 的 on_cancel 不会续跑到 end。✅ 验证可用，**必须保持**。
- EventFilter 终态 GC 已含 cancelled（event_filter.py:32，:131-146），取消后残留订阅被回收。
- SIGTERM 优雅停机：只置位、任务边界退出（flow_worker.py:776-788，run 循环 read 已切 ≤1s 分片 :671-675）。

**结构性新发现（断点清单之外）**：

- **start 路径无租约**：`start_flow`（flow_worker.py:176-235）全程不 `try_acquire`，start 消息被 XCLAIM 重投后可能双开执行（execution_id 不同，不互踩但副作用双份）。
- **租约丢失不写脏状态（好消息）**：`except ExecutionLeaseError: raise` 在两个异常链都排在兜底之前（flow_worker.py:325-326、:447-448），失租路径不会把 state 改写成 error——fencing 只需守住 persist 写，不用改异常结构。
- **`update_local_execution_status_if`（条件 UPDATE）已存在**（local_executor.py:223 使用），本地档取消可直接复用，不必新增 SQLite 原语。
- **订阅超时基础设施已存在但是死代码**：`EventSubscription.timeout` 字段（event/core.py:44）、`register_subscription(timeout=)` 形参（event/core.py:285-291）、`SubscriptionTimeoutChecker` 全套实现（event/timeout.py:15-84）——全仓无任何实例化点（仅 event/__init__.py:17 导出）。EventNode 自动超时只差「透传 + 找个进程把 checker 跑起来」。

### 2.2 租约生命周期（现状时序）

```
worker A                          Redis                         worker B
  │ resume_flow
  │ SET NX EX lease={holder} ……… execution_lease.py:60-63（TTL=120s :10）
  │ ▸ 步1 run_distributed
  │   （单步内无续租）
  │ 步界 renew（Lua CAS）……… execution_lease.py:70-73，flow_worker.py:423
  │ ▸ 步2 run_distributed（LLM，>120s）
  │                                lease EXPIRE ✗
  │                                ◀── pending 消息 idle>60s，XCLAIM 抢走
  │                                    （task_queue.py:297-306，不校验 A 存活）
  │                                                             │ resume_flow
  │                                                             │ 终态短路不拦 running
  │                                                             │ try_acquire 成功 ✗✗
  │                                                             │ ▸ 并发推进（副作用双份）
  │ 步2 结束 → 步界 renew 失败                                    │ state 双写 last-writer-wins
  │ → ExecutionLeaseError 自爆 … flow_worker.py:342-348           │ （storage/redis.py:88-97 无条件 SET）
```

重叠窗口 = **整步时长**（A 要到下一步界才发现失租）；执行状态写是无条件 `SET`（storage/redis.py:88-97），谁后写谁赢。`execution_lease.py` 本体（SET NX EX + Lua compare-and-del/renew，:14-29）是干净的，问题全在**使用侧只在步间续租、且写侧无 fencing**。

## 3. 取消语义设计

### 3.1 worker 步间取消检查：选型

| 方案 | 做法 | 结论 |
|---|---|---|
| A. worker 每步重读 state | 循环头 `load_execution_state` 查 `status=="cancelled"` | 不引入新键；但「cancelled」语义被控制面直接写状态污染，BFF 仍须区分运行中/挂起两套写法，且 crash+XCLAIM 恢复路径无从知晓「曾被要求取消」 |
| **B. 独立取消标志键（选它）** | BFF 写 `SET {ns}:execution:cancel:{id} = ts EX 604800`；worker 循环头 `EXISTS` 检查 | 意图与状态分离；带 TTL 自清理由（复用 event_filter 去重键的同类模式 event_filter.py:153）；BFF 对 running/suspended 统一只写标志，行为一致 |

落地（全部在现有结构上）：

- **BFF** `executions.py` cancel 端点：终态幂等保持（:386-392）；**挂起执行保持现状**（直接写 cancelled + 入队，已验证路径）；**运行中执行改为**：写标志键（租户路由键，复用 `_exec_key` 的 namespace 规则 executions.py:144-146，可抽 `_cancel_key`）+ 入队 cancel 消息（保留，作 worker 全灭后的死人开关），**不再直接写 status=cancelled**。前端看到的「已受理」语义不变。
- **worker** `_process_execution_result` 循环（flow_worker.py:403）else 分支头（:420 后）加一个检查点：`_cancel_requested(execution_id)` 为真 → `state.status="cancelled"` + `end_time` + save + `_finalize_observers()` + break。redis 客户端用与 `_dispatch_service_task` 相同的 `getattr(self, "redis_client", None)` 容错模式（:484），无 Redis（单测内存派生类）时视为未取消。租户上下文已由 `_dispatch_task` 在消息处理前 set（flow_worker.py:624），键路由天然正确。
- **XCLAIM 恢复路径同享**：`resume_flow` 取得租约后（:290-294 附近）加同一检查——crash 后消息被重投，若取消标志在，直接终态化而非继续推进。
- **本地档**：模块级 `_cancel_events: Dict[str, threading.Event]`（与 `_threads` 同锁管理，:42-43）；`cancel_local_execution` 置位 Event（:453-461）；`_run_flow` 循环头检查 Event，且每步状态落库改用 `update_local_execution_status_if(id, "running", "running")` 式条件更新（原语已在 :223 使用）——条件不满足即说明已被置 cancelled，立即收口。执行线程是状态唯一写者，覆写战争消失。

### 3.2 引擎层取消传播：双 Event 拆分 + 公共 cancel()

现状根因：一个 Event 身兼两职——「上节点超时的节点级信号」（需每节点 clear，runner.py:261-268 的注释说明了为什么）与「整个执行的取消意图」（需粘滞）。「每节点入口 clear」对前者正确、对后者致命。

最小改动（不改 clear 语义、不动超时路径）：

1. `ExecutionContext` 增加 `cancel_requested: threading.Event`，与 `cancel_event` 同样的父子共享规则（context.py:205 同型一行）、同样的 pickle 剥离/重建（:261-270 同型处理）。
2. `runner._execute_with_retry` 节点入口：照旧只 clear `cancel_event`；**永不碰 `cancel_requested`**；入口处 `cancel_requested.is_set()` 则直接抛 `FlowCancelledException`（新异常，继承 FlowExecutionException），跳过本节点。
3. `FlowExecution.cancel()` 公共入口（executor.py 新增，约 :159 旁）：同时 set 两个 Event——set `cancel_event` 让 code 沙箱等待循环（code.py:410）当场 killpg，set `cancel_requested` 让引擎在节点边界拒绝继续。
4. 子执行链免费获得传播（共享同一 Event 实例）；进程模式 Parallel 维持现状：入口检查（concurrent.py:165-172）改为同时查 `cancel_requested`，跨进程传播仍是已声明的限制（concurrent.py:366-372），本设计不解决。

改动面：`context.py`（~6 行）、`runner.py`（~8 行）、`executor.py`（~10 行）、`errors.py`（1 个异常类）、`concurrent.py`（2 行检查）。normal/generator/distributed 三策略无需改——拦截点在 NodeRunner 公共入口。

### 3.3 取消时在途节点：默认语义

**默认：步界取消 + 沙箱类节点即时击杀，LLM/HTTP 类节点不中断、跑完当前节点。**

- 理由：队列是 at-least-once（flow_worker.py:508-513 类注释），硬杀在途 LLM 调用后，XCLAIM 重投的 resume 会从 checkpoint 重放该节点——非幂等副作用（发邮件、扣款）双份；让节点自然完成则副作用恰好一份，取消只损失一个节点的时延。code 沙箱是例外：进程树 killpg 已实现且语义干净（code.py:394-427 的注释明确「调用方拿到异常的时刻进程已不存在」），即时杀无重放问题。
- checkpoint 语义：取消发生在步界，上一节点的结果已随 `PERSIST_EVERY_N_STEPS=1`（flow_worker.py:55,436-444）落盘；cancelled 终态的 `state.context` 就是**取消点前一步的完整 checkpoint**。cancelled 是终态（flow_worker.py:269、executions.py:386），**不允许从取消点 resume**；要重跑 = 新执行。若产品将来要「取消点续跑」，checkpoint 数据已具备，只差放开终态短路——列入开放问题。
- watchdog 失租路径复用同一机制：失租 → 调 `execution.cancel()` → 当前步（若是 code 节点）当场死，否则步界死 → worker 以 `ExecutionLeaseError` 退出且**不写状态**（见 §2.2 新发现第二、三条）。

### 3.4 EventNode 自动超时：透传设计

基础设施已备（§2.1 新发现第四条），只补三段：

1. **透传**：`EventNode` 增加可选字段 `subscription_timeout`（秒，None=无限等待=现状）；`_subscribe_event` 的 `subscription_params`（strategies.py:547-553）加 `"timeout": node.subscription_timeout`——`register_subscription` 已接受该形参（event/core.py:285-291），下游存储（含 sqlalchemy 列 event/sqlalchemy.py:55）就绪。
2. **宿主**：`SubscriptionTimeoutChecker` 跑在 **event_filter 进程**（它已持有 subscription_storage 与队列入口，超时回调直接复用 event_filter.py:157-171 的 resume 任务形状，`resume_type="timeout"`）+ 去重键 `plaita:event_filter:timeout:{sub_id}` SET NX（同 :153 模式）防多实例/重发。
3. **消费侧已就绪**：`ResumeType.TIMEOUT` 在 `_handle_resume` 白名单内（strategies.py:362），`EventNode.on_timeout` 存在（event_node.py:153-170），resume 后订阅注销已有（strategies.py:388-410）。无此字段的存量挂起执行行为零变化。

### 3.5 兼容性红线

- 挂起执行取消路径**一行不改**：BFF 挂起分支、worker 终态短路（flow_worker.py:268-279）、E2E cancel 回归用例全部保持绿灯。
- runner 的每节点 clear、sync 超时置位（runner.py:370-371）语义不变——只加不改。
- 无 Redis 的内存 worker / 单测路径：取消检查与 watchdog 全部容错降级为 no-op。

## 4. 租约 fencing 设计

### 4.1 看门狗续租线程

- `RedisFlowWorker` 增加单条后台看门狗线程（run() 启动、stop() 停止）：登记当前活跃 `(execution_id, holder)`，每 `TTL/3`（默认 40s，lease_ttl_seconds=120，execution_lease.py:10）renew 一次。
- renew 返回 False（Lua compare 失败 = 已被他人持有/过期，execution_lease.py:70-73）→ 标记该执行 `lease_lost` + 调 `execution.cancel()`（§3.3）中止当前步 → 步界 `_renew_lease_if_held`（flow_worker.py:342-348）或 persist 前的 `lease_lost` 检查抛 `ExecutionLeaseError` → 消息不 ack（:688-695 现有路径）。
- 看门狗让「存活 worker 的租约永不空窗」，XCLAIM 实际只会捡到真死 worker 的消息——§2.2 的抢占双跑被**根除**，fencing 是其下的第二道保险。

### 4.2 fencing token（世代号 CAS 写）

- **世代号产生**：acquire 改走 Lua：`INCR {ns}:execution:fence:{id}` → `SET lease {holder}:{gen} EX ttl` → 返回 gen。renew/release 的 compare 对整个 value 串做，不用改（execution_lease.py:14-29 原样）。
- **写侧守门**：新增 `FencedExecutionStorage` 包装器（套在 `RedisExecutionStorage` 外，挂进 `TenantRoutingExecutionStorage._storage_cls` 同一注入点，tenant_context.py:76-99）：`save_execution_state` 用单段 Lua `if GET fence_key == ARGV[gen] then SET state_key ...` ——世代不符（已被新世代接管）即拒绝写并让上层抛 `ExecutionLeaseError`。
- **Redis 原语选型：Lua，不用 WATCH/Multi。** 三条理由：acquire 的 INCR+SET 与写侧的 compare+SET 都是「读-判-写」原子序，WATCH/Multi 需要客户端重试循环、两次 RTT；仓内已有同构 Lua 先例（execution_lease.py:14-29）风格统一；多键原子性 WATCH 也给不了更多保证。
- **兼容旧数据**：fence 键首次 acquire 时创建（INCR 天然建键）；由**旧 worker** 推进、**新 worker** 只读的混合期，旧 worker 写侧无 CAS 仍可能覆盖——接受为灰度期已知风险（租约本身仍在挡第一道），混合期结束（全 worker 滚动完成）后 fencing 才视为完全生效，见 §6 波次②的验证门。纯内存 storage（单测）走 NullLease 同款 no-op。

## 5. 测试计划

| # | 场景 | 模拟方法 |
|---|---|---|
| T1 | 双 worker 竞争同一执行 | fake Redis 上两个 `RedisFlowWorker`；A 持租约不 renew，B `resume_flow` 断言 `ExecutionLeaseError` 且消息留 pending |
| T2 | 单步超 TTL 被 XCLAIM 抢占 | 步注入 sleep(>ttl)；断言：无看门狗时 B 接管、A 步界自爆不写状态（state.status 非 error）；有看门狗时 A renew 成功、B try_acquire 失败 |
| T3 | 步间取消（集群档） | BFF 语义等价：循环中途回写标志键；断言 worker 下一步界终态化为 cancelled、context 为上一步 checkpoint、不再翻回 running |
| T4 | 取消后 resume 被拒 | 对 cancelled 执行投 resume 消息，断言终态短路返回 `already_terminal`（现有 E2E 语义延伸） |
| T5 | 看门狗失效恢复 | 看门狗线程注入 renew 连续失败，断言执行 cancel() 被调、当前 code 步进程组被杀、worker 以 ExecutionLeaseError 退出且不写 state |
| T6 | 引擎级取消传播 | 父子 flow + 并行分支：A 分支长跑中 `execution.cancel()`，断言 B 分支下一节点入口抛 FlowCancelledException、`cancel_event` 的节点级 clear 不影响 `cancel_requested` |
| T7 | 并发分支超时互不误伤（回归） | 复用现有 sync 超时用例形态（tests/unit/test_sync_node_timeout.py）：A 分支节点超时置位后，B 分支下一节点正常执行（钉住 clear 语义不被本次改动破坏） |
| T8 | 本地档取消 | 线程长跑中 `cancel_local_execution`，断言执行线程在步界退出、SQLite 终态 cancelled 不被覆写 |
| T9 | EventNode 订阅超时 | fake bus + `subscription_timeout=1`，断言 checker 触发 `resume_type=timeout` 入队、on_timeout 落状态、订阅注销、去重键挡二次 |
| T10 | fencing 世代号 CAS | fake Redis：旧 gen 写被拒、新 gen 写成功；fence 键缺失（旧数据）首 acquire 后可写 |

模拟基建：现有 fake Redis 模式（tests/integration/test_flow_worker.py、test_storage_redis.py 的口径）+ `PLAITA_LEASE_TTL_SECONDS`/`claim_min_idle_ms` 参数化缩短窗口。

## 6. 灰度与回滚（四个可独立合入的波次）

| 波次 | 内容 | 验证门 | 回滚 |
|---|---|---|---|
| ① 取消可达 | 标志键 + worker 步间/resume 检查 + BFF running 分支改造 + 本地档 Event/条件更新 | T3/T4/T8 + 既有 E2E cancel 全绿（挂起路径未动） | 关闭检查点（一个布尔开关），BFF 恢复直写（回到现状行为） |
| ② 看门狗 + fencing | watchdog 线程 + 世代号 acquire + FencedExecutionStorage | T1/T2/T5/T10；灰度期监控 `lease_conflicts`（task_queue.py:84 已有指标）归零 | 摘除包装器 + watchdog 不启动，acquire 退回 SET NX（fence 键残留无害，带 TTL 清理） |
| ③ 引擎传播 | 双 Event + cancel() + FlowCancelledException | T6/T7 全量单测回归 | 属性存在但无人 set 即退化为现状；无数据迁移 |
| ④ EventNode 超时 | 透传 + checker 宿主 | T9；存量无该字段的订阅零变化 | 字段默认 None；checker 不启动即回现状 |

①②③ 互相独立可并行开发；④ 完全正交。每波独立 PR、独立回滚，不要求同车。

## 7. 开放问题（✅ 已拍板，2026-10-02，jeffkit 授权按建议执行）

1. **运行中 LLM 调用取消是硬杀还是软中断**：本设计默认软（跑完节点，§3.3 理由：at-least-once 重放副作用双份）。若要硬杀，需给 LLM/HTTP 节点补请求级取消（httpx/cancel token），工作量另计。
   **拍板：维持软中断**（code 沙箱 killpg 例外不变）。硬杀待真实诉求出现再立项。
2. **cancelled 是否允许「从取消点 resume」**：checkpoint 数据天然在（§3.3），放开只需把 cancelled 移出终态短路并定义新 resume 语义——但会改变「终态幂等」契约，涉及 console 前端与审计口径。
   **拍板：维持终态不可 resume**。产品出现真实诉求再立项。
3. **fencing 与旧 worker 混跑期**：接受旧 worker 可覆盖（§4.2）还是要求 fencing 上线时 worker 全量同窗滚动？
   **拍板：随 v0.6.0 合 main + daemon 重启一次性全量滚**，混跑期自然归零；灰度期以 `lease_conflicts` 归零 + 「已在步界响应取消」日志为 fencing 生效判据。
4. **XCLAIM → XAUTOCLAIM**：看门狗（②）落地后抢占双跑已被根除，XAUTOCLAIM 只是 RECLAIM 扫描的效率/语义改良（Redis≥6.2），建议降级为顺手项而非必做。
   **拍板：不做**。
5. **start 路径无租约**（§2.1 新发现一）：是否给 `start_flow` 也加租约？影响面是「双开不同 id 的重复执行」，与互斥是两个问题，建议单独立项。
   **拍板：单独立项**，不塞进本批。

## 8. 生态关联

同族语义对齐：agentproc Python SDK 的超时终止已实现「进程组 SIGTERM → kill_grace_secs 宽限 → killpg SIGKILL」的分级击杀（../agentproc/sdk/python/src/agentproc/runner.py:27 契约、:1132-1138 实现），本设计的「软中断默认 + code 沙箱 killpg 例外」（§3.3）与之同构，跨仓取消语义保持一致口径。
