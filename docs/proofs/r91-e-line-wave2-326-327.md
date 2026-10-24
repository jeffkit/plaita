# r91 / E 线 wave 2（B1 席）：#326 $ENV 注入机制+存量清理 + #327 配额观测面（扫描对账纠偏+告警线）

- 基线：`origin/main` 开工时 `b49ec1a1`（wave 1 `664d1523` 后裔，已核祖先）
- 分支：`fix/e-line-wave2`（主 clone）
- 规格：#326/#327 正文 + A2 r60 评审 + **owner 裁决落单评论（2026-10-24，
  两单五点/四点全定）**。wave 1 未做面（$ENV 五澄清点、配额三选一、观测面）
  本轮全部收口。

## 单① #326：checkpoint 不落 $ENV + 恢复重注 + 存量清理 + 告警回收

- **①a 不落快照（值+键名）**：strip 机械化于持久化咽喉点
  `_persist_state_or_raise`（#320 写序同款：不靠写点自觉），重绑**持久化
  副本**——引擎在飞 context 字典不动（在飞节点仍按进程内 $ENV 求值）。
  覆盖 start/步进/挂起/终态/错误态全部 phase。
- **①b 恢复重注**：`resume_flow` 在 `run_distributed` 前调
  `_reinject_env_for_resume`——引擎恢复是 `context.context = saved_context`
  整体替换（`DistributedStrategy.execute`），不重注则 `$ENV.XXX` 静默为空；
  按当前 worker 进程 env + flow `expose_env` allowlist 重注，具名 WARNING
  （`event=tenant_env.reinject`，值漂移声明——owner 裁决①接受非确定性）。
- **①c 求值来源** = 重注后的进程 env：`saved_context` 断言新值，不吃旧
  快照、不静默为空。
- **①d 存量清理**：`scripts/purge_env_from_checkpoints.py`（对账器家族
  顺带面，单发不新增进程）——SCAN `{ns}:execution:*` 成员判别同 storage
  （`_index_member_id` 同款：后缀无冒号非 index），字节预滤 `"$ENV"`
  不命中即跳过，**CAS Lua 改写**（字节比对不中不写——worker 并发落盘
  绝不被旧快照回滚），TTL 按读取值重设。降级验收：脚本可跑 + 新执行
  secret env grep 不到（本文件负控格即证）。
- **①e 告警回收**：`_process_execution_result` 每执行一次的 $ENV 告警删除
  （不留死代码）；`_safe_environment` 告警文案同步改真（「会进入 checkpoint
  序列化」已不成立 → 「值仅进程内存、恢复路径重注」）。

## 单② #327：配额观测面（计数分工的观测半边）

- **②d 扫描对账纠偏**：`tenant_dlq_quota.reconcile_tenant_dlq_counts`
  ——平台 DLQ XRANGE 解析信封 payload `tenant_id`（与 #316
  `carrier_audit._scan_dlq` 同源形制）+ 隔离流 SCAN+XLEN（按键归属，免
  解析），与写路径 HASH 差异处 HSET/HDEL 纠偏。**纠偏是满格 pending「等
  水位回落」的收敛半边**：计数只朝虚高漂（xtrim 不通知），虚高不纠则
  满格载体永无回头日（负控 d-1 实证纠偏后重投过闸落 DLQ）。worker 周期
  hook `_reconcile_dlq_quota_if_due`（与 #43 残留回收同形制：消费循环同
  线程、到期才扫、非阻塞锁、观测异常降级不打断消费），间隔 env
  `PLAITA_TENANT_DLQ_RECONCILE_INTERVAL_SECONDS` 缺省 600s（≤0 关，非法
  loud 回默认）。竞态语义：纠偏 HSET 与并发 HINCRBY 互踩回退 ≤1，下一轮
  自愈（docstring 明示）。
- **②b 分流名告警**：任一租户两流合计 >50% 上限 → 具名 WARNING（文案带
  `platform_dlq:N+quarantine:M` 双落点名）。
- **②e trim 互挤**：明示可接受（owner 裁决④；模块 docstring 规格化——
  A125 修后 DLQ=证据 quarantine，被挤条目隔离流可查；租户感知 trim 留 T0
  后续）+ **总量 20% 告警线**：平台 DLQ 总长 > maxlen×
  `DLQ_TOTAL_ALERT_RATIO`(0.2) → `tenant_dlq.total_watermark` 早警。
- **②a/c/f 守恒+引道面**：满格具名告警文案锁断言；满格拒收**绝不引去
  隔离流**（改道=配额绕行暗面复活）；env 旋钮纪律守恒。

## 负控（先红后绿，红相=临时 worktree @ b49ec1a1）

新负控 16 格：`tests/unit/test_issue326_env_checkpoint_reinjection.py`（a
两格：strip 副本语义+终态 legacy 行；b 两格：重注+具名 WARNING+无
allowlist 静默；c 一格：值漂移胜旧快照；d 三格：干跑/实清保 TTL/CAS
防回滚；e 一格：旧告警回收）+ `tests/unit/test_issue327_dlq_quota_reconcile.py`
（a 一格：满格告警+不引道；b 一格：分流名告警；d 三格：幻影纠偏解闭+
hook 接线+虚低上调；e 一格：总量 20% 线；f 一格：间隔旋钮）。

- **红相**（HEAD 基线树实跑）：14 failed / 2 passed——2 绿恰为 wave 1
  守恒面格（满格拒收留 pending、不引道），14 红全为新行为格。
- **绿相**：16/16 passed。
- **守恒**：wave 1 两文件 + 租户/DLQ/env 家族 11 文件 104P 全绿零改动。

## 门禁

- unit 独跑（`pytest tests/unit -q`）：**9103P/13S rc=0**（= r90 基线
  9087 + 本轮 16 新格；首跑 9095P/8F——8F 全为消费循环鸭子型替身缺新
  hook 桩（7F，r90 同族课：循环内新增调用改 getattr+callable 防御）与
  A2R20 from_url 超时卫生门抓脚本（1F，对齐对账家族超时口径），修复后
  复跑全绿）。
- console 后端门（engine/worker 改动必跑，r90 纪律）：**1613P/13S rc=0**
  （与 r97 历史 1613P 零漂移）。
- ruff：改动面 0 增量（基线 13 条 BLE001 既有，行号平移）。
- mkdocs（docs-site --strict）：rc=0。
