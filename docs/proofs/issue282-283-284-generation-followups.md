# #282/#283/#284 证明：世代写闸的三条收尾（消费循环 / 载体世代 / 建租户顺序）

- 单号：plaita#282、#283、#284（均 P1；#277 世代写闸的修后跟进）
- 基线：`origin/main` = `3276c20`
- 边界：只动 `TenantWriteRejected` 写路径与租户世代 seam；不混 #280/#241
  等相邻单。生产消息格式新增信封字段（向后兼容：缺失=legacy）。

## 根因

### #282 超投递分支绕过单一死信入口

`_reclaim_one` 的 `max_deliveries` 分支先过守卫、再直调 `dead_letter`。
`dead_letter` 的租户写闸对已删/停用租户抛 `TenantWriteRejected`；该异常
从 `read()` 穿出（在消费循环内层 try 之外），直接杀死 concurrency=1 的
消费线程 → 进程重启 → 同一条 pending 被反复 reclaim，形成崩溃/重启循环。

### #283 世代在读取时采样，副本会被「洗成」新世代

`StreamTask.tenant_generation` 由 `_tenant_generation_for(body)` 在
`_task_from_fields` 里读**当前** meta 得到。旧 worker 迟到的 requeue/
handover/replay 副本在租户同名重建后被读成新世代，于是两处闸门同时失效：
死信写入把旧 payload 写进新租户 DLQ；dispatch 按新世代正常执行旧副本。
`store_subscription` 对无世代上下文的订阅直接 adopt 当前世代，同样把旧
执行订阅写进新租户。

### #284 建租户先发布 active，世代发布是 best-effort

`create_tenant` 先 `_publish_status(active)`，再 `_publish_generation`
且失败只 WARNING。世代写失败时租户已 active、Redis meta 仍是旧世代（或
缺失），旧世代 DLQ/订阅写在新租户窗口内被放行。启动对账同样先 status
后 generation。

## 修法

1. **单一死信入口 + 循环兜底**（#282）
   - `_reclaim_one` 超投递分支改走 `dead_letter_if_allowed`；拒绝时返回
     False、消息留 pending（不 ack、不入 DLQ），不再有异常穿出 `read()`。
   - 消费循环内层新增 `except TenantWriteRejected`：`note_tenant_write_rejected()`
     计数 + ERROR，不 ack、不 poison，`finally` 清上下文后继续下一条。
   - 循环最外层保留 `TenantWriteRejected` 兜底（覆盖 read/ack 等循环体调用），
     短退避后继续，绝不退出消费线程。
2. **世代随载体走**（#283）
   - 信封新增 `tenant_generation`；`enqueue_task`/`RedisStreamTaskQueue.enqueue`
     接受并写入；`_task_from_fields` 只从信封解析，不再读当前 meta。
   - `_requeue_envelope_kwargs` 继承源世代（覆盖 retry/handover/dead-letter
     守卫克隆等全部副本）；DLQ 信封保留源世代，replay 继承；console/
     schedule/event_filter/dispatch_ledger 的生产消息在入队时打当前世代。
   - worker dispatch 入口比对**源世代**（消费循环已按载体设置
     `current_tenant_generation()` ContextVar）与当前 meta：不一致或当前
     世代缺失即丢弃（不执行、不写新租户面）。无世代存量载体按 legacy 宽限，
     写闸仍 fail-closed。
   - `store_subscription`：无世代上下文且当前 meta 已发布世代 → 具名拒绝
     （`subscription_generation_missing`）；当前无世代（旧部署）才按 legacy
     放行，不再自动 adopt。
3. **建租户 generation → active，失败回滚**（#284）
   - `create_tenant` 在 `flush()`（拿到 `created_at`）后、`commit()` 前发布
     新世代；发布失败（或世代不可确定）抛 `TenantError`，事务回滚，不进入
     active。成功后才 commit + 发布 active。
   - `publish_tenant_generation` 返回 bool（失败 WARNING + False）；启动对账
     按 generation → status 顺序，世代失败计 `failed` 并跳过 active。

## 测试

- `tests/unit/test_issue282_283_generation.py` **5 passed**：
  ① 已删/停用租户超投递 reclaim 不抛、DLQ 空、消息 pending、拒绝计数；
  ② requeue 副本保留源世代（重建到 gen2 后读回仍是 gen1）；
  ③ dispatch 拒绝旧世代副本（start 不得执行）；
  ④ 无世代订阅在当前世代已发布时被具名拒绝，显式匹配世代可写；
  ⑤ consume-loop 捕获拒写：不杀循环、计数、坏消息留 pending、后续正常消息
  照常处理。
- `plaita-console/backend/tests/console/test_issue284_create_generation_order.py`
  **2 passed**：世代发布失败 ⇒ `TenantError` + 无租户行 + 非 active；
  正常创建顺序 generation → active，旧世代写被 `generation_changed` 拒绝。
- 既有 `test_issue277_tenant_sidewrites.py` 的队列读取用例改为显式在信封
  打 `tenant_generation`（读取端不再采样 meta）。
- `scripts/mutation_replay/mutations/87-*.json` 两条锚点随 `event_filter.py`
  入队调用形状更新（old/new 同步加 `tenant_generation`，变异语义不变）。

## 负控（先红后绿）

隔离副本 `/tmp/r56-nc`（共享 clone 有并发席位，负控只改副本）。组合回退
7 处实现：#282 直调 `dead_letter`、副本不继承世代、读取端采样 meta、
删除 dispatch 门禁、订阅 auto-adopt、#284 旧顺序 + 吞错。

- 修复前：同一命令 **6 failed**（NC 覆盖的 4 条 unit + 2 条 console），关键报错：
  `TenantWriteRejected ... tenant_disabled` 穿出 `read()`、
  `KeyError: 'tenant_generation'`、`old-generation carrier must not start`、
  `DID NOT RAISE TenantWriteRejected`、`DID NOT RAISE TenantError`、
  顺序 `['active', 'generation']`。
- 还原后：同一命令 **6 passed**。完整日志与变异清单见
  `docs/proofs/data/r56-generation-negative-control.txt`。

## 验收

- 全量 unit（`-m "not slow"`，push 门配置）：**8741P/12S/14 deselected/
  3493 subtests**（rebase 到 `8c663df` 后复跑，rc=0，386.21s）
- e2e + integration：**287P/4S**（rc=0）
- console：**1086P/13S**（rebase 后复跑，rc=0；含本单 2 条）
- plaita-ai：**293P/13S/12 subtests**（`uv run --isolated --extra all`；
  仓库 `.venv` 不装 mcp/langchain extras，缺 extras 时 MCP stdio 冒烟会因
  子进程 ImportError 变红，属环境面）
- 分层/wheel/类尺寸：**46P**（import layering / wheel contents /
  layering / success criteria 定向）
- `landing_gate.py ci`（含 Console E2E 次级门）：**rc=0 / 允许落地**
  （main 最近完成 run `8c663df` CI success；Console E2E 绿）
