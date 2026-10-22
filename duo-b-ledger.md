# B1 席位交付台账

> 一行摘要（第六十八轮，2026-10-22）：**#296 MAX_TRACKED 饱和收窄（基线 `cc7abfe`）**——规格是「仅新键 60s」，实现却把全局 `_saturated_until` 放进所有键的 `retry_after`，任一新键触发后既有未锁键（含 admin）也 429，且每 60s 新键可续期全局阻断。修法：改为按新键记录 `_saturated_keys{(canonical_ip,user): until}`；只有该键 `retry_after` 返回剩余秒数，既有键/活动锁不受影响；prune/reset 清理过期饱和。ops-runbook 明确「仅新键；既有键 retry_after 恒 0；不全局施加」。测试 `test_296_*` **3P**（direct/HTTP/重复新键），既有 B112-02 保持；负控 pre/post：既有 admin retry 60→0、HTTP admin 正确 429→200/Retry-After 60→null、新键仍 60；全量 unit **8795P/12S/14 deselected/3501 subtests**、e2e+integration **287P/4S**、console **1201P/13S**；`landing_gate.py ci` rc=0（`cc7abfe` CI + Console E2E）。#296 关单回执另附。

> 一行摘要（第六十七轮，2026-10-22）：**login_guard 设计级收口 #293/#294/#295 + B112-01/02（基线 `b2906c2`）**——① IP canonical 化（IPv4-mapped→IPv4/压缩展开）+ IPv6 /64 聚合；account 桶 IP 集合按 300s 窗口滑动（进程 dict / Redis ZSET），同 /64 多地址只算 1、旧 IP 锁过期后不续锁；② break-glass 不再是用户名列表即绕过：用户名白名单 + 来源 IP 白名单 + 一次性 HMAC token（`bg1.<exp>.<nonce>.<hmac>`，短 TTL，Redis SET NX 跨副本），失败照常计数，每次尝试独立 `auth.login_break_glass` 审计（break_glass=true）；③ Redis throttle 全部用 `redis.call('TIME')`/`redis.time()`，禁止进程 monotonic 入共享键；④ `0.0.0.0/0`/`::/0` 全 trust 配置具名拒绝、IPv4-mapped 参与 CIDR 匹配；⑤ MAX_TRACKED 满表新键 60s 具名饱和（不静默放行、不驱逐活动锁）。桶/窗口语义表入 ops-runbook（文档即规格）。测试 console `test_issue293_294_295_login_guard_v2.py` **14P** + A112 **12P** + #208 **8P**；负控 pre/post（A1 r113 探针 + post 探针）；全量 unit **8795P/12S/14 deselected/3501 subtests**、e2e+integration **287P/4S**、console **1198P/13S**；`landing_gate.py ci` rc=0（`b2906c2` CI + Console E2E 绿）。#293/#294/#295 关单回执另附。

> 一行摘要（第六十六轮，2026-10-22）：**login_guard 硬化 A112-01/02 + B112-01/02（基线 `3c9d1a0`）**——① 三桶限速：per-key `(ip,user)` 5 次/300s、账号桶（失败来自 ≥3 个不同 IP）、IP 桶（单 IP fan-out ≥10，锁 5m 减少 NAT 影响），锁内正确/错误都 429、再次触发翻倍封顶 24h；② 反枚举：missing 跑固定 dummy PBKDF2、disabled 跑真实 hash（计时比 16×→1.0×）；③ 可信代理：`PLAITA_TRUSTED_PROXIES` allowlist，peer 可信时 XFF 从右向左取首个 untrusted，否则忽略 XFF；④ 多 worker：`RedisLoginThrottle`（Lua 原子共享 key/IP/account），无 Redis 本地档退回进程内并文档化；⑤ break-glass env 绕过限速（仍校验口令）；⑥ MAX_TRACKED 不再驱逐活动锁。测试 console `test_issue_a112_login_hardening.py` **12P** + #208 **8P**；NC-L1~L11 pre/post（端点探针 + timing/eviction 探针）；全量 unit **8795P/12S/14 deselected/3501 subtests**、e2e+integration **287P/4S**、console **1184P/13S**；`landing_gate.py ci` rc=0（`3c9d1a0` CI + Console E2E 绿）。

> 一行摘要（第六十五轮，2026-10-22）：**B109-01/02 + B2 r67 注记轻量硬化（基线 `a75b4ab`）**——① `safeExternalUrl` 外部链接 scheme 白闸（仅 http/https；javascript:/data:/相对路径返回 null），`ExecutionDetail` Langfuse 链接非白即降级纯文本；② `buildLogsHref` 用 URLSearchParams 逐参数编码，Cluster 日志深链的 serviceType/instanceId 特殊字符不再改写 query；③ `test_issue69_mutation_matrix.py` 的恒真断言 `assertTrue(x is False or x)` 改为 `assertFalse(refused)`（对照实现 Reduce.initial refused=False，写穿由 live_changed 全表断言兜住）。前端 `node check-api-transport` **33/33** + tsc 干净；unit **8795P/12S/14 deselected/3501 subtests**、e2e+integration **287P/4S**、console **1121P/13S**；负控（去 scheme 白闸/退回字符串拼接/改错期望值）三红→恢复绿；`landing_gate.py ci` rc=0（`eee7b37` CI + Console E2E `a75b4ab` 绿）。

> 一行摘要（第六十四轮，2026-10-22；任务书标称第六十三轮——台账第六十三轮已被 #289 park-retry 轮占用，按序列顺延）：**B108-01/02 收尾（基线 `c24ce3e`）**——① **park 周期清扫**：新增 `plaita/server/start_dedup_sweeper.py`，park 时写 `{ns}:start-dedup-park:{key}`（id+parked_at），console cluster lifespan 启动 `ParkSweeper`（默认 30s/超时 60s=claiming TTL/预算 100，env 可调/可关）；超时后 queued/缺失行 ⇒ 墓碑+删行+清 map/token+回配额+WARNING，running/终态只删标记；`mark_enqueued` 成功清除标记。② **discarded 独立审计**：worker 终态放弃载体递增 `_dedup_discarded_total`，`/metrics` 新 counter `worker_dedup_discarded_total`，写 `plaita:execution:dedup-discard:{id}`（7d），console 执行详情返回 `dedup_discarded`。测试 unit **4P** sweeper + console 既有 3P 追加指标/审计/详情断言；负控 A–D（永不清理/忽略超时/无 counter/无审计）实红；全量 unit **8795P/12S/14 deselected/3501 subtests**、e2e+integration **287P/4S**、console **1121P/13S**；`landing_gate.py ci` rc=0（`eee7b37` CI + Console E2E 绿）。

> 一行摘要（第六十三轮，2026-10-22；任务书标称第六十二轮——台账第六十二轮已被 #288 reservation 轮占用，按序列顺延）：**#289 mark 失败 park-retry（基线 `39c875a`）**——`mark_enqueued` 失败不再 discard/墓碑/XDEL（carrier 可能已被 worker 消费执行）：新增 `recover_after_mark_failure`（guard→reserve→mark 原地恢复）；map 仍属本 id 且无墓碑时 **park**（保留载体/占位行/映射/配额），同 key/同 due 重试收敛原 id（console 409 in-progress + Retry-After）；只有映射换主/墓碑才清收。schedule 孤儿 row 在且无墓碑时用 `adopt_for_recovery` 同 id 恢复，不再清理换新 id。worker 消费点终态防线：载体命中的执行已 completed/error/cancelled 时具名 WARNING + `discarded=True`，ack 不执行。测试 unit **2P** + console **3P** + 既有 #288/schedule/状态机/console dedup 全绿；负控 A–C（console/schedule 回退 discard、worker 去 discarded）实红；全量 unit **8776P/12S/14 deselected/3501 subtests**、e2e+integration **287P/4S**、console **1118P/13S**；`landing_gate.py ci` rc=0（`39c875a` CI + Console E2E 绿）。#289 关单回执另附。

> 一行摘要（第六十二轮，2026-10-22；任务书标称第六十一轮——台账第六十一轮已被 #286/#287 状态机轮占用，按序列顺延）：**#288 schedule 接入 reservation 状态机（基线 `41085cc`）**——`fire_schedule` 在占位行后、XADD 前调用 `reserve_enqueue`（慢写跨 claiming TTL 先 guard 同 token 续期）；`_MARK_ENQUEUED_LUA` 收紧为只允许 `enqueueing→enqueued`（不变量 6 由状态机强制）；reservation/入队/mark 失败统一 discard 占位行（墓碑）+释放 claim+回滚配额（mark 失败另 XDEL）；同 due 孤儿清理后同调用重试新 id；A106-02 前端重试窗口 65s ≥ claiming TTL、superseded 入集合、超窗具名引导不换 key；后端 409 仍带 Retry-After。测试 unit **5P** + 既有 schedule/状态机 **69P** + console dedup **15P** + 前端 node **30/30**；负控 A–E 五类实红；全量 unit **8774P/12S/14 deselected/3501 subtests**、e2e+integration **287P/4S**、console **1115P/13S**；`landing_gate.py ci` rc=0（`4985022` CI + Console E2E 绿）。#288 关单回执另附。

> 一行摘要（第六十一轮，2026-10-22；任务书标称第六十轮——台账第六十轮已被 DLQ replay family 轮占用，按序列顺延）：**start dedup 受理 seam 设计级收口（#286/#287，基线 `ca39bc2`）**——新增 `plaita/server/start_dedup.py` token CAS 状态机（claim/guard/reserve_enqueue/mark_enqueued/release 全 Lua，每步校验 map/owner/token/墓碑；release 原子复核 row，修 #286 stale GET→DEL）；owner 双读旧 `<map>:owner` 并 adopt 迁移、`foo:owner` 反向碰撞先迁 owner 再释放共键（#287，滚动升级硬要求 console 先排空旧实例）；schedule/cron 与 console 共用预认领，同 due 不再铸新 id/入队/占配额，配额/入队失败回滚 claim；409 带 Retry-After + 前端同 dedup_key 退避重试 + 后端 `asyncio.sleep`；force DELETE 迟到载体 XDEL。测试 unit **11P** + console **5P** + 前端 node **29/29**；负控 A–G 七项全红；全量 unit **8769P/12S/14 deselected/3493 subtests**、e2e+integration **287P/4S**、console **1115P/13S**；`landing_gate.py ci` rc=0（`1f01658` CI + Console E2E 绿）。#286/#287 关单回执另附。

> 一行摘要（第六十轮，2026-10-22；任务书标称第五十九轮——台账第五十九轮已被 #285 占用，按序列顺延）：**DLQ replay 目标 family 校验（r58 a/P2，基线 `dc31efa`）**——replay 在源 `config_envelope` 后、marker/入队前解析目标 stream 的生效 family（默认按目标 stream + 生效 group/DLQ；自定义 group/DLQ 显式 `target_config_family`），源/目标不一致（含源 legacy）⇒ `target_family_mismatch`：不写 marker、目标 XLEN=0、源 DLQ 保留、审计记 rejection；管理员 `allow_family_mismatch=true` 才可跨 family（WARNING+审计），显式 family 非法/无法解析 ⇒ `target_family_unknown`。测试 unit **5P** + 既有 replay 三文件 **41P** + console replay 两文件 **19P**；负控 pre-fix worktree **3F/2P** + r104 规格探针「replayed ok 但目标 pending」实红→拒绝；全量 unit **8746P/12S/14 deselected/3493 subtests**、e2e+integration **287P/4S**、console **1091P/13S**；`landing_gate.py ci` rc=0（`dc31efa` CI + Console E2E 绿）。无 issue 号（duo-a 第九十六轮 a/P2）。

> 一行摘要（第五十九轮，2026-10-22；任务书标称第五十七轮——台账第五十七/五十八轮已被 A69 复核轮与 #282-284 轮占用，按台账序列顺延）：**#285 P1 owner CAS 收口（基线 `c08e118`）**——A 写占位行/入队/写 owner 前用 Lua 复核 map==我、无墓碑、owner==我（claiming TTL 过期但映射仍属我时原地续期），owner 写入改 CAS 绝不覆盖他 id；owner 键改独立前缀 `{ns}:start-dedup-owner:{key}` 修 `foo`/`foo:owner` 碰撞；败者不再 ghost 200（载体未落返回 409 in-progress，重试收敛），A 失去认领收敛当前映射或 409 superseded；占位行失败 503+Retry-After 并释放映射/写墓碑。测试 console **5P**（#280 并发用例按新契约调整，同 id/单载体断言保持）；负控 pre-fix worktree **5F→5P** + A1 探针 owner TTL=1s/force DELETE/后缀碰撞/3+ ghost 全部实红→单 id 单载体；全量 console **1091P/13S**、unit **8741P/12S/14 deselected/3493 subtests**、e2e+integration **287P/4S**、plaita-ai **293P/13S/12 subtests**、layering/wheel/size **46P**；`landing_gate.py ci` rc=0（`c08e118` CI + Console E2E 绿）。

> 一行摘要（第五十八轮，2026-10-22；任务书标称第五十六轮——台账第五十六/五十七轮已被 A69 复核轮占用，按台账序列顺延）：**#282/#283/#284 三单 P1 收口（基线 `3276c20`）**——① #282 超投递改走 `dead_letter_if_allowed`（拒写不 ack、消息留 pending）+ 消费循环捕获 `TenantWriteRejected`（计数/不退出），删除租户后不再崩溃重启；② #283 世代随信封走（requeue/handover/DLQ replay 继承源世代），dispatch 按 ContextVar 源世代比对当前 meta 拒旧副本，订阅无上下文且当前世代已发布即具名拒绝（不再 adopt）；③ #284 create_tenant 世代先于 active、发布失败回滚，启动对账同序。测试 unit **5P** + console **2P**；负控隔离副本组合回退 7 处 **6F→6P**；全量 unit **8741P/12S/14 deselected/3493 subtests**、e2e+integration **287P/4S**、console **1086P/13S**、plaita-ai **293P/13S/12 subtests**、layering/wheel/size **46P**；`landing_gate.py ci` rc=0（`8c663df` CI + Console E2E 绿）。

> 一行摘要（第五十七轮，2026-10-21；重复派单核验轮——本任务书（标称第五十轮：A69-02/03 修后复核 + A2R24-10 全端点扫尾核验）与第五十六轮 `b99cfa2` 同内容，属重复派单；按台账序列顺延，本轮不重做全量验收，做独立抽验 + 勘误）：**独立抽验全绿，第五十六轮计数勘误（基线 `origin/main` `b99cfa2`；零代码改动，新增残面 = 0）**——① **A69 族定向实数 44P 全绿**（cleanup 12 + prune_metrics 7 + write_prune 4 + backfill 4 + a78 残面 7 + r52 tombstone 10）；**勘误**：第五十六轮条目「50P／tombstone 16」系计数笔误——`test_r52_execution_tombstone.py` 自创建 `fc03041` 即 10 条（从未改动，全台账唯一一处「16」），已就地修正，实质结论（全绿）不变。② **janitor 预算/游标语义实现复核**（与第五十六轮记录一致）：`prune_indexes` 预算=实际检查成员数（allowance 耗尽即停）、`{cursor,pending}` 游标仅存进程内 `_index_prune_state` 不写 Redis 键、A78-05 EXISTS 判死+ZREM 单条 Lua、只按 EXISTS 不按 score、全库无 `KEYS` 命令（仅 Lua 脚本 KEYS 形参）；真实 Redis 探针不重跑（同一代码树，第五十六轮 21/21 在案）。③ **A2R24-10**：`test_issue_a2r24_request_strict.py` **14P**——cancel 专测 + `test_every_json_request_body_forbids_unknown_fields` OpenAPI 扫尾覆盖 enable/disable、tenants、cluster 等全部写端点；live 探针不重跑（同上）。④ **验收**：`tests/unit` 全量与 console 全量不重跑——`b99cfa2` 相对第五十六轮全量绿树 `4970e05` 仅差 3 行台账、代码零改动（对齐第五十轮 docs-only 不跑全量的先例）；`landing_gate.py ci` 独立 **rc=0**（main `b99cfa2` success；首跑因本机代理 `127.0.0.1:7897` 切断 api.github.com TLS 按 fail-closed rc=1，直连复核可用后摘代理重跑取 rc）；仓库源码/测试零改动（仅本台账勘误 + 追加；他席 `duo-b3-ledger.md` 与未跟踪 `duo-a-ledger.md`/`duo-a2-ledger.md` 未触碰）。

> 一行摘要（第五十六轮，2026-10-21；任务书标称第五十轮——台账第五十轮已由 ops-runbook 环境变量实测定版轮占用，按台账序列顺延）：**A69-02/03 修后复核 + A2R24-10 全端点扫尾核验（两个已落批修复的验证轮；基线 `origin/main` `4970e05`；零代码改动，新增残面 = 0）**——① **A69-02/03**（B2 r51 `e374aa1` + 残面 `88607b0`/`3fec442` 均已落）：原 9+10 条规格负控**逐条单跑**全绿——console `test_a69_02_event_canonical.py` **11/11**（§2.6 九条 + TTL env 覆盖 + legacy 频道保留）、unit `test_a69_03_index_cleanup.py` **12/12**（§3.5 十条 + EventFilter 挂载 + 无 KEYS）；A69 族定向 **44P**（cleanup 12 + prune_metrics 7 + write_prune 4 + backfill 4 + a78 残面 7 + tombstone 10；原记 50P/16 为计数笔误，第五十七轮勘误）。**janitor 预算/游标语义**：实现复核（`prune_indexes` 预算=实际检查成员数、进程内 `{cursor,pending}` 游标不写 Redis 键、B78-01 按实际进入桶数轮转、A78-05 单条 Lua 判死删除、只按 EXISTS 不按 score）+ 真实 Redis 8.0.2 db9 探针 **21/21**：budget=1×7 桶×(1 live+1 dead) **7 tick 收敛**、rotation 逐 tick +1 无饿死、单 tick examined≤1、live 7/7 保留、死成员 7/7 清除、收敛后 pending 有界（≤1/桶）且续扫 7 tick 排干；订阅 4 桶×2 orphan **8 tick 收敛**且游标排干；budget=2 全 live examined=2/pruned=0、两 tick 累计覆盖 3 成员（小集合单页全量下「pending 优先+页内补足」）、零误删；全程零 KEYS（commandstats 0→0）、跑后 db9 dbsize=0。② **A2R24-10**（B1 r49 `3d9a741` + r55 `de5a592` 均已落）：抽 **5 类端点**独立探针 **21/21**，未知字段一律 422 `extra_forbidden`——执行控制 `POST /executions/{id}/cancel`（`{"reason"}`）、调度生命周期 `POST /schedules/{id}/enable|disable`（`{"force"}`）、租户生命周期 `DELETE /tenants/{id}`（`{"reason"}`）+ `POST /tenants`（strict create）、集群面 `POST /cluster/refresh` + `POST /cluster/tenant-status/reconcile`、流程 strict body `PUT /flows/{id}/versions/{v}`（拼错乐观锁字段）；其中 enable/disable、tenants、cluster 三族是 r55 落地测试未逐一覆盖的端点。兼容向不误伤：cancel `{}` 200、enable `{}` 200、`DELETE /tenants/ghost` `{}` 404（到达 handler）、正确 `base_updated_at` 200；拒绝无副作用（cancel 意图键未写、调度零入队且 enabled 未翻转、租户列表不变）。console 定向 **25P**（canonical 11 + request_strict 14，含 OpenAPI 扫尾 checked≥66）。③ **验收**：`tests/unit` 全量 **8599P/12S/3498 subtests（rc=0，790.97s）**；`landing_gate.py ci` 独立 **rc=0**（main `4970e05` success）；仓库源码/测试零改动（仅追加本台账；共享 clone 他席 `duo-b3-ledger.md` 与未跟踪 `duo-a-ledger.md`/`duo-a2-ledger.md` 未触碰）。

> 一行摘要（第五十五轮，2026-10-21；任务书标称第四十九轮——台账第四十九轮已由 B74-02/B75-01 占用，按台账序列顺延）：**A2R24-10 未知请求字段全端点扫尾**（基线 `origin/main` `8eac9fd`，提交 `de5a592` 已推 main）——r49 `3d9a741` + B2 r53 已把 38 个请求模型改 `extra="forbid"`（40 个 JSON body operation），本轮复核确认无漏网模型；**残面是 27 个没有 body 参数的写端点**（12 POST + 15 DELETE：cancel/logout/trigger/enable/disable/rotate-secret/cluster refresh·reconcile·infra check/start/stop/stream ticket；flows/versions/nodes/property-types/credentials/events subscriptions/executions/schedules/tenants/members/users/cluster infra·instances·clusters DELETE），Starlette 把请求体读走后丢弃——传 `{"reason":...}`/`{"params":...}`/`{"force":true}` 仍 200。新增 `EmptyRequestModel`（零字段 + forbid）逐路由挂载：空体/`{}`/`null` 保持 200，任何字段 422 `extra_forbidden`。OpenAPI 扫尾升级为「所有写端点必须有请求体」：69 个写操作中 67 个有 body（66 个严格模型 + dispatch/one #262 ignore），仅 copilot（AG-UI 透传）与 flowVersion/semver/detail（form 契约）豁免。负控：还原 10 个 api 模块 → 6 类端点全部 pre-fix 200（cancel/logout/trigger/flow·node·credential DELETE），扫尾 checked=39 红；修复后本文件 14P。定向 **218P**（89+129）、console 全量 **1038P/13S/1F**（唯一失败为本地 10-18 陈旧 `frontend/dist` 环境项 `test_issue266`）、ruff 改动行无新增、landing_gate **rc=0**（main `8eac9fd` success）；MIGRATION/README 已记录空体兼容与 `DELETE /executions/{id}?force=true` 迁移。
> 一行摘要（第五十四轮，2026-10-21；任务书标称第四十七轮——台账第四十七轮已由 A2R31 占用，按台账序列顺延）：**A69-02/03 修后复核 + A2R24 契约族余项 + A55-S1/S5-S7 落地面**（基线 `origin/main` `f1777ed`）——① **A69-02**：pre-fix `3fd516f` + 当前 `test_a69_02_event_canonical.py` **9F/2P**（2 绿=legacy 只读/legacy 频道），当前事件索引族（A69/A78/A83）**42P**；真实 Redis 8.0.2 db10 探针：janitor budget=1 在 3 桶×（2 dead+2 live）**6 tick 收敛**（每 tick examined=1、live 6/6 保留），40d 老时间戳活事件保留、90d ghost 删除，订阅四索引（type/correlation/flow/node）dead 成员被 SREM 且 live 保留，写订阅把桶 TTL 3s→604800、prune 不改 TTL，全程 KEYS 增量 0。② **A2R24-02/05/06/07/08/09** 现树 **41P**（queue `start` 索引 + `start_id` 游标、logs/events total/truncated/next_cursor、`flow_name`、三投影、trigger 统一、SSE media type）。③ **残面 `9c017d3`**：`/schedules/{id}/history` 原两档 `total=本页条数`、limit 以上不可达，且本地记录 `{triggered_at,status,error}` 与集群/前端 `{fired_at,flow_id,version,enqueue,msg_id}` 不一致（本地对话框时间恒空、状态恒「入队失败」）→ typed `ScheduleHistoryResponse`（total 真总数、offset/next_offset、limit/offset 越界 422）+ 本地字段映射。④ **A55-S**：S1（含 r57 `flow.ai_generate`）/S5/S7 已落 HEAD，S6 仍为规格待 owner。负控 4F、定向 63P、unit 全量 8599P/12S/3498 subtests（rc=0）、console 1030P/13S/1F（本地 10-18 陈旧 dist 环境项）、tsc/ruff/mkdocs/landing_gate 全绿。
> 一行摘要（第五十三轮，2026-10-21；任务书标称第四十七轮——台账第四十七轮已由 A2R31 配置代际族占用，本轮按台账序列顺延）：**A69-02/03 规格余项复验 + A1 r83 三条 a 类残面收口**（基线 `origin/main` `f2d0589`；B2 r51 `e374aa1`/B1 r48 `e417316` 已落，本轮零重复实现）——①**负控**：pre-fix `3fd516f` + 当前用例 **19F/4P**（A69-02 9F/2P、A69-03 10F/2P；4 绿=legacy 只读/legacy 频道兼容/存活成员/老时间戳保护）；②**A83-03** 判死+删除的 Lua 改 500 成员/批 EVAL（事件+订阅同源），janitor 大预算不再 `too many results to unpack`；**A83-05** 订阅四索引改「TTL 只延长不缩短」，键形抽 `subscription_index_keys` 与 delete/reindex 同源；**A83-02** 新增事件/订阅两个 ensure helper + 回填 `--reindex`（正典事件 `types:` + 订阅 data 四索引），`skipped_existing` 分支也补索引（含 A1 r83 未列的订阅侧修复缺口）；**B83-01** `-1` 语义注释改实义（边界保留待 owner 裁决）。真实 Redis 8.0.2 db13：pre-fix 8200 成员单 EVAL 精确 `ResponseError user_script:9 too many results to unpack` 且零删除，post-fix budget=9000 **8200/8200/errors=0/0.11s**；订阅桶 **604800** 不被短写砍短、2.2s 后仍在；事件/订阅 reindex 均恢复 `list_*` 可见且幂等。新测 **8P**/pre-fix **8F**、定向 **417P**、unit 全量 **8597P/12S/3498 subtests（rc=0）**、console **1021P/13S**（1F=本地 stale `frontend/dist`）、ruff 无新增、`mkdocs --strict` rc=0、landing_gate **rc=0**；`88607b0` 已推 origin/main，main CI run `39517368826` 四 job **success**。

> 一行摘要（第五十二轮，2026-10-21；任务书标称第四十八轮——台账第四十八至五十一位已被既有交付占用，按台账序列顺延）：**A69-02/03（r70 PR2/PR3）规格余项独立复验 + 回填幂等残面**（基线 `origin/main` `2a39cfc`；B2 r51 `e374aa1`/A1 r78 已落，本轮零重复实现）——①**负控逐条**：pre-fix `3fd516f` 归档 + 当前 console 用例 **9F/2P**（九条规格负控全红；2 绿=legacy 只读/legacy 频道兼容）；当前树 26 个单点回退 **26/26 适用项精确红**——A69-02 12（canonical 写/可见/回扫/旧读面回滚、TTL 7d、单 timestamp、两形去重 canonical 优先、legacy 只读、受信租户剥离、TTL env、legacy 频道、回填 TTL=0/dry-run）、A69-03 14（事件/订阅读清、冷类型桶、flow+node 冷桶、EventFilter 挂载、预算有界、live 保留、老时间戳不按 score、console node、delete-after-expiry、坏桶 WARNING、无 KEYS）。②**真实 Redis 8.0.2 db12 探针 22/22**：canonical TTL **604800**/score==timestamp/legacy 键 0、引擎 get/list 可见；回填剩余 TTL/不删旧键/二次 `migrated=0,skipped_existing=1`；janitor budget=1/存储跨 7 桶 **16 tick 收敛**（单 tick examined≤2 合计、live 全保留、40d 老时间戳保留）、无 KEYS（commandstats 92→92）、热桶读后 zcard 6→2。③**残面（单 commit `3fec442`）**：`migrate_console_events.py` 对记录体缺/空 `event_id` 原会由 `Event` 每次生成新 uuid，`--apply` 重跑不断复制 canonical（幂等承诺被破坏）→ 按 legacy 键后缀派生，新增回归 + event-system 文档。④**验收**：新测 post **4P**/pre-fix **1F**；定向 **64P** + console A69-02 **11P**；`tests/unit` 全量 **8589P/12S/3498 subtests（rc=0，728.04s）**；ruff 全绿；`mkdocs --strict` **rc=0**；`landing_gate.py ci` 独立 **rc=0**（main `2a39cfc` success）；`3fec442` 已推 origin/main，main CI run `39510451425`（head `3fec442`）四 job **success**。

> 一行摘要（第五十一轮，2026-10-21；任务书标称第四十七轮——台账第四十七轮已由 A2R31 配置代际族占用，按台账序列顺延）：**A69-02/03 规格余项独立核验 + 残面补遗**（基线 `origin/main` `15cc33d`；B2 r51 `e374aa1`/r55 已落，本轮零重复实现）——①**负控逐条**：pre-fix archive `3fd516f` + 当前用例复现 **19F/3P**（A69-02 9F/1P、A69-03 10F/2P；3 绿=legacy 只读/存活成员/老时间戳保护）；当前树 26 个单点回退 **26/26 精确红**——A69-02 9（canonical 写/可见/回扫/TTL 7d/单 timestamp/两形去重 canonical 优先/legacy 只读/旧读面回滚/受信租户剥离）、A69-03 12（事件读清 ZREM/订阅读清 SREM/冷类型桶 janitor/flow+node 冷桶/EventFilter 挂载/预算有界/仅 EXISTS 判活/不按 score/node 索引补清/delete-after-expiry False 后 janitor 清/坏桶 WARNING 继续/无 KEYS）、回填 4（dry-run/TTL=0/畸形/剩余 TTL）、legacy 频道保留 1。②**真实 Redis 8.0.2 db9 探针**：budget=1×3 桶×(2 live+2 ghost) 12 tick 收敛 **2/2/2**、单 tick examined≤1、live **6/6**；40d 老时间戳活事件保留；console publish 单写 canonical（legacy 记录/类型键均 0、记录 TTL **604800**、score==timestamp、GET 同时可见 canonical+legacy）；回填 dry-run 不写/apply 2 条/**剩余 TTL 5s 复制**/旧键保留/二次幂等；租户 purge 清两形记录与两形索引；`PLAITA_DISABLE_EVENT_INDEX_SWEEP=1` WARNING 且不挂载；订阅读清、冷 `flow:` 桶清理、data 过期后 delete 返回 False。③**残面补遗（单 commit `66c2fe9`）**：`event-system.md` 补 legacy Pub/Sub 频道 `plaita:event:channel:{type}` **deprecated（保留一个 release）**、写侧兜底清理 env/失败语义、janitor 预算「每 tick 每存储」口径；删除 `_prune_event_index_on_write` 重复 `if not members`；新增 legacy 频道保留回归（摘除发布侧负控 **1F**）。④**验收**：定向 **118P**；`tests/unit` 全量 **8588P/12S/3498 subtests（rc=0，717.89s）**；console A69-02 **11P**；ruff 无新增（13 条既有 BLE001）；`mkdocs --strict` **rc=0**；`landing_gate.py ci` 独立 **rc=0**（main `15cc33d` success）；`66c2fe9` + 台账已推 origin/main。

> 一行摘要（第五十轮，2026-10-21；任务书标称第四十九轮——台账第四十九轮已由 B74-02/B75-01 占用，按台账序列顺延；本任务是已交付的 B57-09/B58-11 重新派发，原章节见台账第四十四轮 `60e6773`，本轮为 A2R31 配置代际落地后的独立实测复核 + 漂移修正）：**对 `docs-site/docs/distributed/ops-runbook.md`「环境变量参考」抽样实测定版（基线 `origin/main` `b5dbab9`；文档 `bd7f87d` 已推 origin/main）**——①**实测**：51 个变量 / **166 条观测**（worker/retention、import 期沙箱/BG/expr 子进程矩阵、HTTP、console settings、runtime resolver；覆盖默认值、读取时机、0/负/非法值失败形态），r58 草案 98 个 `PLAITA_*` 名零丢失（现章节 113 名，含 A2R31 新增 15 名）。②**修正 11 行漂移**（均为 A2R31 语义收紧或既有文档写反）：`SANDBOX_MEMORY_MB` 负值改为 import `ConfigError`（原“接受且等同不限”）；`SANDBOX_DOCKER_CPUS` 改为 import 期有限正数校验（原“执行期才报错”）；`HTTP_DNS_TTL` 负/`nan`/`inf`/非法一律 30 + WARNING、仅 `0` 关缓存（原“负值当 0 / inf 永不过期 / 无 WARNING”）；`HTTP_CONNECTOR_LIMIT` 0/负/非法 → 100 + WARNING（原“钳 1 / 无 WARNING”）；`HTTP_COOKIES` 改 strict bool（`TRUE` 生效）；`HTTP_BLOCK_PRIVATE` 补 `on` 与非法 fail-closed；`HTTP_MAX_RESPONSE_BYTES` 补 +WARNING；`CONSOLE_ALLOW_INSECURE_ADMIN` 改 strict bool（`1/yes/on`、`" true "` 均生效，原“均不生效”）；`CONSOLE_LANGFUSE` 未知值改 **false + WARNING**（原“也当开”，B58-09 已随 A2R31 修复）；`EVENT_RECONCILE_BACKFILL` 的 `0` 明确为空窗口、仅负/非法回退 3600 + WARNING；`METRICS_PORT` 负数静默钳 0。③**`.env.example`（B58-11）**：修正“留空即 fail-closed”的错误表述——compose 的 `${VAR:-change-me-*}` 在空值时会用公开模板值，必须填写；裸机留空才是 503。④**验证**：`mkdocs build -f docs-site/mkdocs.yml --strict` **rc=0**；`landing_gate.py ci` 独立 **rc=0**（main `b5dbab9` success）；`b5dbab9..bd7f87d` 已推 origin/main。⑤**附带观察（未改代码/未开单）**：`runtime_config.DEFAULT_LEASE_TTL_SECONDS=300` 与 `execution_lease.DEFAULT_LEASE_TTL_SECONDS=120` 不一致，且 `load_runtime_config` 不消费 `overrides["lease_ttl_seconds"]`；console/worker 的 `budget_generation` 目前都按 300 计算（不分裂），但哈希不反映 worker 实际 120 的租约，供 B2 复核。本轮仅改文档与样例，运行时代码零改动。

> 一行摘要（第四十九轮，2026-10-21；任务书标称第四十八轮——台账第四十八轮已由 A69 复核 + A2R24 契约族占用，按台账序列顺延）：**B74-02 / B75-01「unknown ≠ default」队列三态**（基线 `origin/main` `40f7615`；代码 `1443e75` 已推 origin/main）——①**三态解析**：`candidate_queue_name` 仅「非空且非纯空白字符串」返回 known，缺失/空白/纯空白/非字符串统一 unknown=`None`，**不再回落默认队列**；`queue_name` 改 `Optional[str]`，unknown 在默认策略下永不进入 `allowed`/`target`，拒绝原因稳定前缀 `queue: unknown (worker metadata lacks queue_name)`（known 不匹配仍是 `queue: mismatch`）。②**只拒定向不拒广播**：`decide_route` 对 unknown 只做定向过滤；无目标时 `dispatch_one` 仍按调用方队列广播、消息不带 `target_instance_id`（legacy default worker 可继续消费），容量定向退化但不丢消息。③**显式兼容开关**：`PLAITA_DISPATCH_UNKNOWN_QUEUE=skip`（默认）/`assume-default`；后者仅在请求默认队列 `plaita:flow:queue` 时把 unknown 当默认，plan/dispatch 响应与台账新增 `queue_source ∈ {declared, assumed-default}`，并留 WARNING（含 instance_id 列表）；非法值 WARNING 后按 skip。`dispatch_ledger.build_ledger_entry` 新增 `queue_source` 字段；console `/metrics` 导出进程内 `plaita_dispatch_unknown_queue_total{queue}`（只统计带队列过滤且候选未声明队列的调用）；新 worker 注册 metadata 增加 `queue_contract_version=2`。④**测试与负控**：新增 `tests/unit/test_b75_01_unknown_queue.py` 16 条、`plaita-console/backend/tests/console/test_b74_02_unknown_queue.py` 13 条，并按新语义改写 r54 固化的「缺 metadata=default」用例；临时把 `candidate_queue_name` 还原为 `return DEFAULT_DISPATCH_QUEUE` 做语义负控：unit **13F/3P**、console **12F/1P**，覆盖 5 组（缺失/空白/非字符串、assume-default 三面、skip 广播与 legacy 可消费、known mismatch 回归、plan/dispatch 同判据）。⑤**回归**：定向 unit 135P、console 57P；`tests/unit` 全量 **8588P/12S/3498 subtests（rc=0，729.83s）**；console 全量 **1020P/13S/1F**（唯一失败 `test_issue266_built_bundle_has_no_admin_key_material` 为本地 2026-10-18 构建的 gitignored `frontend/dist` 陈旧产物，非本轮改动）；`mkdocs --strict` rc=0；ruff 改动文件无新增（`flow_worker.py` 既有 BLE001 同基线）；`landing_gate.py ci` 独立 **rc=0**（main `40f7615` success）；推送后 main CI run `39486582420`（head `1443e75`）四 job **success**。

> 一行摘要（第四十八轮，2026-10-21；任务书标称第四十七轮——台账第四十七轮已由 A2R31 配置代际族占用，按台账序列顺延）：**A69-02/03 修后复核（A1 r78 残面 A78-01/04/05 + B78-01）+ A2R24 契约族余项（02/05/06/07/08/09）**（基线 `origin/main` `b22cff7`；代码 `e417316` + `0f11be2`）——①**A69 复核**：原 9+10 条负控与 A69-04 套件合计 **85P**、console canonical **10P** 全绿；修四残面：**A78-01（P2）**类型桶 TTL 改 Lua `max(当前剩余,目标)` **只延长不缩短**，回填脚本显式 `index_ttl=默认 7d`（负控：2×7d canonical + 5s legacy 回填，pre-fix 桶 `ttl=5`、post `>7d−60` 且 `list_events` 3 条；`PLAITA_EVENT_TTL_SECONDS` 调小同根因覆盖）；**A78-05（P3）**读侧/janitor/写侧清理由 `EXISTS→ZREM/SREM` 两段往返改**单条 Lua 原子判死删除**（代理在 eval 前重写同 id：pre-fix 活成员被删 `zcard=0`、post 保留；event/subscription 各一例）；**A78-04（P3）**janitor interval `nan/inf` 拒绝回退 60s + WARNING（默认周期 monkeypatch 0.02s：pre-fix 仅 1 次 sweep、post ≥3）；**B78-01（P3）**桶轮转按本 tick **实际进入桶数**前进（20 桶×(1 live+1 missing)、budget=4：pre-fix 12 tick 未收敛、post ≤10）。②**A2R24**：02 queue stream `start` 改**索引**语义（5 条 `start=2` → n=2,3/index=2,3）、`start>10000` 422、`next_start_id`/`start_id` 排他游标、`truncated`；05 `/logs`、`/logs/instance`、`/events` 的 `total`=匹配总数（`truncated` 时为下界）+`returned`+`next_cursor`（键集 `(ts,entry_id)`；events `ts:event_id`），`limit ge=1` 与 `order` Literal 非法值 422，本地 SQL 键集游标与双向排序；06 `flow_name` 复核通过（B2 `ba1501d` 的 list/detail 用例在基线）；07 tenant 详情 `member_count`、单版本 `tenant_id`、晋升包取**实体租户**、平台同名 flow 跨租户详情/单版本/导出 409 `ambiguous_flow_id`+`available_tenants`；08 trigger 两模式统一 `{success,mode,execution_id,msg_id}`；09 两个 SSE 路由 OpenAPI 200 **仅** `text/event-stream`，ticket 端点声明 `StreamTicketResponse`。新增 console 16 条，pre-fix **15F/1P**（唯一绿为显式 `X-Tenant-ID` 路径），post **16P**；定向 console 98P、审计面 61P。③**审计 S 残面**：S1 成功审计（schedules/nodes/property_types/events/flows/cluster/services/capacity/auth/copilot/dryrun/supervisor/credentials）落地面在，S5 metrics 有界 SCAN + event_type 字符集校验在，S7 runbook 审计/保留/replay/鉴权口径在；S6 全局观测面仍待 owner 裁决（`docs/proofs/s6-global-observability-policy.md`）。**回归**：`tests/unit` 全量 **8559P/12S/3498 subtests（rc=0，718.93s）**；console 全量 **1000P/13S/1F**（唯一失败 `test_issue266_built_bundle_has_no_admin_key_material` 为本地陈旧 `frontend/dist` 产物，环境项）；layering **3P**；`mkdocs --strict` **rc=0**；前端 `tsc --noEmit` **rc=0**；ruff 改动文件无新增（32 条 BLE001 与基线同数）；`landing_gate.py ci` 独立 **rc=0**（main `b22cff7` success）；代码 `e417316` + `0f11be2` + 台账 `5c8852e` 已推 origin/main，main CI run `39478700236`（head `5c8852e`）四 job **success**。

> 一行摘要（第四十七轮，2026-10-21；任务书标称第四十六轮——台账第四十六轮已由 A67-04/05 凭据残面 `c8b0390` 占用，按台账序列顺延）：**A2R31 配置代际族施工（r31 B2R31-01/02/03；基线 `origin/main` `6a6f8df`，合并 `9fa0afd`；代码 `ca60069` + merge `98d2d87` + 台账 `60fa05c` 已推 origin/main）**——①**B2R31-01 三维代际**：新增 `plaita/runtime_config.py`，`cf1-`/`cfg1-`/`bg1-` canonical 哈希（family=queue/group/dlq/schema/namespace；generation=family+deployment/tenant epoch/budget/安全开关；budget=max_deliveries/lease/age/dlq_len，纯观测项不参与）；消息信封新增四字段，console/schedule/event_filter 新生产消息写当前代际，worker 五条重投/交接/克隆与 DLQ replay 继承源值（缺失=legacy，绝不盖当前值）；首投与 reclaim 同判：family 不匹配不执行、reclaim 用 `XRANGE` 预读不 `XCLAIM`（不烧 delivery），budget 不同不把新预算作用旧 PEL；超 `task_max_age` 的拒收载体进 DLQ 并带 `config_rejected` 防克隆热环；group 改名 `PLAITA_CONSUMER_GROUP_START_ID=$` 不重放、queue 改名同前缀旧 Stream pending 启动 WARNING；registry metadata/容量块/`/api/cluster/{config,instances}`/dispatch family 过滤/`queue_config_*` 指标全链可观测；租户状态纪元 `plaita:tenant_status:epoch` 锚定 `PLAITA_DEPLOYMENT_EPOCH`，worker 比较（非 default 缺/旧 epoch 拒绝，default 保持 #237 fail-open；Redis epoch 更新拒绝本 worker），console 周期对账（默认 60s）+ `POST /api/cluster/tenant-status/reconcile`，`PLAITA_TENANT_STATUS_EPOCH_STRICT=0` 为回滚阀。②**B2R31-02 单一 resolver**：queue（console/worker/schedule 同源）、redis 别名（event_filter 补 `REDIS_URL`）、DLQ、instance id 全部收敛；空串 identity 启动预检拒绝（manual/capacity/schedule 不再分裂）；console 启动 drift 预检（queue/redis/dlq）+ registry DLQ 键进租户删除清理集合。③**B2R31-03 非法值口径**：identity/security/resource 类在 worker/console/services/event_filter 入口聚合 fail-fast，optional 类 WARNING+安全默认；strict bool 含 `on`、`HTTP_COOKIES=TRUE`、`ALLOW_INSECURE_ADMIN=1` 生效；`SANDBOX_MEMORY_MB` 负值/`SANDBOX_DOCKER_CPUS` 非法 import fail-fast；`CONSOLE_LANGFUSE` 未知=false；0/负紧环与空键拒绝或 WARNING；`DISABLE_*` 保留精确 `1` 方言 + WARNING（`true` 不改变安全语义）。**负控**（clean archive `6a6f8df` → `/tmp/plaita-r46-prefix`）：old PEL 4 次投递 + 新 max 3 pre-fix `dlq_len=1/pending=0` → fix（源 `budget_generation`）`dlq_len=0/pending=1` + `event=config.budget_stale`；空 `PLAITA_CONSOLE_TASK_QUEUE` pre-fix manual=`''`/capacity=sched=默认 → fix 三路径 `ConfigError`；仅 `REDIS_URL` pre-fix event_filter=localhost → fix 全入口 real-host；租户恢复 pre-fix active/`fail_open=0`/无 epoch → fix epoch 闸拒绝 + `stale_epoch_total=1`；六形态 pre-fix `on→False`/`bogus→True`/`-1` 接受/`inf` 接受/紧环/空键 → fix True/False/import ConfigError/30s+WARNING/default+WARNING/拒绝。**回归**：新增 engine 39 + console 10；定向 21 文件 **401P/6S**、合并后抽核 **109P**、console 定向 **71P**；合并后 `tests/unit` 全量 **8552P/12S/3498 subtests（rc=0，717.08s）**；console 全量 **984P/13S/1F**（唯一失败 `test_issue266_built_bundle_has_no_admin_key_material` 为本地 gitignored `frontend/dist` 旧产物，环境项）；layering **19P**；`mkdocs --strict` **rc=0**；ruff 新文件全绿、改动文件无新增（既有 BLE001）；`landing_gate.py ci` **rc=0**（main `9fa0afd` success）；**推送后 main CI `60fa05c` success（四 job）**。

> 一行摘要（第四十六轮，2026-10-20；任务书标称第四十五轮——台账第四十五轮已由 A69 TTL 族核验 `9027ebb` 占用，本轮按台账序列顺延为第四十六轮）：**A67-04/05 凭据残面小修 + B67-01/02/03 文档边界（规格 A1 r67；基线 `origin/main` `9027ebb`；代码/文档已推 `be97cfb`）**——①**A67-04 静默回退可观测**：引擎 `_default_credentials_dir` 对未设置 `PLAITA_CONSOLE_DB_URL`、非 `sqlite:///`（PG/MySQL）、内存 SQLite、相对 SQLite URL 打一次性 WARNING（按 `(原因,组件,解析路径)` 去重；相对路径明确「按各进程 cwd 解析」；非 SQLite 只记 scheme，避免 DB URL userinfo 口令入日志）；console `_db_dir` 委托引擎单源、`credentials_key_file` 空 env 按未设置，显式 `PLAITA_CREDENTIALS_FILE` 不触发解析告警；console 启动对账即触发。②**A67-05 key 权限**：console 首次生成改 `os.open(O_CREAT|O_EXCL|O_WRONLY, 0o600)` + write/fsync（并发落败者等待并复用胜者 key），删除「先 0644 后 chmod」窗口；引擎/console 读取与 console 启动对账对存量 0644 key 收紧 0600 + WARNING（chmod 失败只告警一次/路径、不阻断解密），摘要新增 `key_permission_tightened`；空 key 文件最多等 1s 后 fail-loud。③**B67 文档**：`console.md`/`ops-runbook`/`DOC_CODE_MAP` 标注孤儿侧车/孤儿行只 WARNING 不自动收敛（B67-01，手工共享密钥取舍留 owner）、对账仅启动时/鬼凭据最长到下次重启/同租户写/DELETE（B67-02）、慢盘+滚动重启可能撞 5s busy timeout 拒绝启动（B67-03），并加「旧版本 → 当前版本」迁移 runbook。**负控**（pre-fix clean archive `/tmp/plaita-b1-r46-prefix` vs fix）：A67-04 无 env 旧 cwd 文件仍解出 `OLD-SECRET` 且 `warnings=[]`（fix 两条回退 WARNING，值不变=非阻断）；A67-05 放大 chmod 窗口 pre-fix 观测 `0o644`/44B、对账后仍 `0o644` 且摘要无字段（fix 创建期无 chmod 观测、final 0600，对账收紧 0600 + `key_permission_tightened=true` + WARNING）。**回归**：新增引擎 8 + console 6 条；定向 **82P**；首跑 `tests/unit` **8504P/1F** 暴露 #151 日志卫生（chmod 失败 WARNING 传裸异常）→ 改 `_scrub_exception_text` 后卫生 **14P**；终跑 `tests/unit` **8506P/12S/3498 subtests（rc=0，739.21s）**；ruff 改动行无新增；layering **19P**；`mkdocs --strict` **rc=0**；`landing_gate.py ci` **rc=0**（main `9027ebb` success）；**推送后 main CI `c8b0390` success**。代码 commit `dfb3df7` + 文档 `be97cfb` 已推 origin/main。

> 一行摘要（第四十五轮，2026-10-20）：**A69 TTL 族规格核验（A1 r70；A69-02/03 由 B2 r51 `e374aa1` 落、A69-04 由本席 r42 `8f579a2` 落；基线 `origin/main` `60e6773`，轮末未移动）**——①**逐条复验**：A69-02 §2.6 九条负控（canonical 记录/list_events/reconciler/TTL/单 timestamp/两形去重 canonical 优先/legacy 只读/旧读面回滚/受信租户）+ `PLAITA_EVENT_TTL_SECONDS` 覆盖 1 条 = **10P**；A69-03 §3.5 十条负控（读时 ZREM/SREM、冷桶 janitor、flow/node、预算有界、存活保护、老时间戳保护、console node、delete False+janitor、坏桶 WARNING、无 KEYS）+ EventFilter 挂载 + 写侧 60s/8 成员 4 条 = **16P**；A69-04 §4.5 十二条负控 + 不短于 state/新鲜载体 2 对照 + fenced 二次删除 = **16P**，r52 墓碑/tenant purge **10P**。逐项核对键形/TTL/单一时间戳/共享 writer/legacy 兼容/回填脚本/janitor 预算与进程内游标/只按 EXISTS/console `node:`/墓碑独立 30d/`<=0` 非法回退+WARNING/clamp/`SET NX`+只延长/载体双源 max/fail-closed/`>=`/DLQ 不刷新/启动 WARNING 均与规格一致；`[policy]` 提交 `8f579a2` 带 `Policy-ack: jeffkit 2026-10-19`。②**残面修复（单 commit `a0b91a7`）**：回填脚本 `_replay_ttl` 对 `TTL=0`（键剩余不足 1 秒）原会以 `SET ... EX 0` 触发 `invalid expire time` 中断整批 → 与 `-2` 同按已过期跳过；重投/交接/克隆的年龄证据原「payload 非空字符串即原样保留」，畸形/超前值会把源 Stream ID 老年龄洗白 → 改为取两源中**年龄更大（更早）**的可解析值。新增 5 条（回填 3 + A69-04 畸形/超前 2 参数化），pre-fix **3F/2P**，post **5P**。③**回归**：A69 定向 **55P**；`tests/unit` 全量 **8498P/12S/3498 subtests（rc=0，743.35s）**；console 全量 **961P/13S/1F**（唯一失败 `test_issue266_built_bundle_has_no_admin_key_material` 为本地陈旧 `frontend/dist` 产物，环境项；A69-02 console 10P 全绿）；`mkdocs build -f docs-site/mkdocs.yml --strict` **rc=0**；ruff 改动行无新增（既有 11 条 BLE001）；`landing_gate.py ci` **rc=0**（main `60e6773` CI success）。代码 commit `a0b91a7` 已推 origin/main。

> 一行摘要（第四十四轮，2026-10-20）：**B57-09 ops 开关总表落成 docs-site「环境变量参考」+ B58-11 `.env.example`（基线 `origin/main` `bd8f896`；素材 A1 r58 77 开关逐项表）**——①**实测复核**：clean archive（`git archive bd8f896` → `/tmp/plaita-r44b1`）重跑 r58 五组探针共 **357 条观测**（worker 154 + import 63 + misc 66 + console 74），另直测 `WORKER_CONCURRENCY`（0/-3 parser rc=0、abc rc=1）、`LOG_LEVEL=BOGUS`（`ValueError: Unknown level`）、`INSTANCE_ID=`（空串不回退）；77 项中 **69 项动态、8 项静态**（`ADMIN_PASSWORD`/`CONSOLE_NODE_MODULES/PATH`/`DISABLE_EVENT_RECONCILE`/`LOOP`/`NODE_MODULES`/`NODE_PATH`/`WORKER_LANGFUSE` 均为启动态或完整 app 依赖）。②**章节**：`ops-runbook.md` 的 `## 环境变量速查` 替换为 `## 环境变量参考 {#环境变量参考}`，按读取时机（import/ctor/call/startup/request/launch）+ 基础连接/Worker/回滚/沙箱/表达式 HTTP 并发/console 六组列「默认/时机/作用与 0 非法值/陷阱」，并保留/并入 r51–r54 新增的 deadline/墓碑/载体年龄/事件 TTL/janitor/`ALLOW_SUBPROCESS_FALLBACK` 等 **9 项新变量**（共 107 个 `PLAITA_*` 名，r58 草案 98 名零丢失）。③**对齐现实现**（r58 草案后已落地）：`WORKER_DRAIN_TIMEOUT` 改“advisory 上界 + 努力 cancel + 步完成仍可 ack”（A45-01 修复）；`CONSOLE_TASK_QUEUE` 调度派发已读 env（A56-02 修复），并记空串在手动/触发路径得到空 Stream 键、容量派发把空白当默认；`CODE_BACKEND` docker 不可用已 fail-closed，只有 `ALLOW_SUBPROCESS_FALLBACK=1` 且白名单含 subprocess 才降级（A56-01）。④**B58-11**：新增根 `.env.example`（3 个 compose 必填项留空 + 常用项），并在 deployment/console/installation/console README 链接；填值后 `docker compose config` **rc=0 且 0 处 `change-me-*`**。⑤**顺手对齐文档口径**：console.md/installation.md 的监听默认改 `127.0.0.1`（容器 CMD 仍 0.0.0.0）、ADMIN_PASSWORD 改“设置才建号，未设走向导”（B58-06/B56-07）。验证：`mkdocs build -f docs-site/mkdocs.yml --strict` **rc=0**；`landing_gate.py ci` **rc=0**（main `bd8f896` CI success）。交付单 commit（docs-only；未改运行时代码）。

> 一行摘要（第四十三轮，2026-10-20）：**A69-02/03（台账 r70 PR2/PR3）在本席开工前已由 B2 r51（`e374aa1`）完整落地并合入 main（merge `1c09480`，19 文件 +2034/−134）；本轮零重复实现，改为独立复核 + 补唯一未覆盖规格点**——①**复核（22P）**：spec §2.6 九条与 §3.5 十条负控对应测试 10+12 条全在且全绿；逐项核对正典键形 `plaita:event:events:{id}`/`types:{type}` 7d、记录体与 zset score 共用单一 `Event.timestamp`、legacy 24h 只读兼容 + console GET 按 event_id 去重 canonical 优先、base service 复用 `store_event_sync`、回填脚本默认 dry-run/不删旧键/幂等、`list_events` 按桶 ZREM、`list_subscriptions` MGET+SREM、`IndexJanitor` 随 EventFilter 挂载（SCAN+ZSCAN/SSCAN 进程内游标、预算 500/tick、只按 EXISTS 判活、仅 SCAN 族无 KEYS）、console delete 补 `node:`、event-system/ops-runbook/DOC_CODE_MAP 与五个 env 均与规格一致。②**补测**：规格 §3.3-3「写侧每 type 60s、至多 8 成员轮转兜底」此前只有实现无测试——新增 `tests/unit/test_a69_03_index_write_prune.py` 4 条（孤儿 ZREM + 存活/新写成员保留、8 成员窗口与 offset 轮转、`PLAITA_DISABLE_EVENT_INDEX_PRUNE_ON_WRITE=1` 对照、`zcard` 异常只 WARNING 不影响落盘）；clean archive 临时移除 `_prune_event_index_on_write` 调用负控 **3F/1P**（disable 对照绿），恢复后 **4P**，ruff 绿。③**回归**：spec 验收套件（issue109/110 parity r3/r5、issue79、resume persistence、console issue191）**80P**；A69-04 墓碑/tenant purge **27P**；`tests/unit` 全量 **8484P/12S/3498 subtests（rc=0）**；console 全量 **946P/13S/1F**（唯一失败 `test_issue266_built_bundle_has_no_admin_key_material` 为本地 gitignored `frontend/dist` 旧产物，CI fresh build 覆盖，与本轮无关）；`landing_gate.py ci` **rc=0**（main `1c09480` CI success）。④**零耦合**：与 A69-04 文件集零交集（B2 改 `event/*`、console events、base_service、docs、脚本；r42 改 `core/retention`、`storage/*`、`task_queue`、`flow_worker`、`dlq_replay`），墓碑回归不触达。测试提交 `45ed3e5`；**未改** B2 已合入的键形/writer/janitor 实现（避免重复实现，`fix(console/events)` 原提交即 `e374aa1`）。

> 一行摘要（热修，2026-10-20）：**修 `3d71e6d` main 红——console payload 断言对齐 A69-04 载体时间戳注入**——`_enqueue` 经 `enqueue_task` 对缺失载体时间注入 `timestamp`（A69-04 预期行为），`test_payload_shape_preserved` 的逐字相等断言未跟上。改法：**显式登记唯一新增键** `timestamp`（`set(body) == set(payload) | {"timestamp"}`）并校验 `datetime.fromisoformat` 合法，其余字段仍逐字精确比对；未删测试、未放宽任何既有字段。**负控**：还原旧断言 → **1 failed**（红形态与 CI 一致：`Left contains 1 more item: {'timestamp': ...}`）；修复后定向 **6 passed**；console 全量 **936P/13S**，唯一失败 `test_s17_frontend_contract::test_issue266_built_bundle_has_no_admin_key_material` 为本地陈旧 `frontend/dist` 产物扫描（CI 前端 job 从源码构建，与本改动无关）。`landing_gate.py ci` 修复前独立 **rc=1**（main `3d71e6d` failure），CI 绿后复跑 **rc=0**（`aff6be5` success）。代码 commit `aff6be5`（与本轮工作区改动逐字一致，共享 clone 并发提交）已推 origin/main。

> 一行摘要（第四十二轮，2026-10-20）：**A69-04 墓碑 TTL 独立化 + 载体年龄 fail-closed（PR1；台账 r70；基线 `origin/main` `59dcff7`，推送前 rebase 至 `3fd516f`）**——①**墓碑 horizon**：`execution_tombstone_ttl_seconds` 改独立默认 30d，`max(30d, STATE_TTL_DAYS)` 只作下限（`STATE_TTL=0` 不再产生永久墓碑）；`PLAITA_EXECUTION_TOMBSTONE_TTL_SECONDS<=0` 视为非法回退默认 + WARNING（**policy 变更**，commit 带 `[policy]` 与 `Policy-ack: jeffkit 2026-10-19`），结果 clamp 到 `PLAITA_TASK_MAX_AGE_SECONDS`；新增 `plaita/core/retention.py` 下沉 `task_max_age_seconds`（storage 不得反向 import server），`FlowWorker.__init__` 与 console lifespan 调 `validate_tombstone_retention`（每进程一次 WARNING）。②**证据/幂等**：普通删除 pipeline 与 fenced Lua 都改「键不存在才创建 + 已存在只延长 TTL」，二次 delete 保留首次 `deleted_at`/`tenant`/`reason`；Lua 用 TTL 比较兼容 Redis <7，存量永久墓碑不缩短。③**载体年龄**：年龄改 `max(payload ISO, Stream ID 时间)`，缺失/畸形 **fail-closed**（WARNING + `/metrics carrier_age_unknown_total`），超龄边界 `>=`；`enqueue_task` 仅对缺失值注入（新 `default_timestamp`，校验 ISO），重投/交接/克隆经 `_copy_carrier_body` 保留源年龄（源缺 timestamp 时由源 Stream ID 派生，绝不补 now），start→resume/dedup/协议归一化按新载体写 now；DLQ replay 用 `dead_lettered_at`/DLQ Stream ID，两源皆无则具名 `carrier_timestamp_unknown` 拒绝。④**A51-01 对齐**：平台根键/同 pipeline/同一段 Lua/tenant purge/五点拦截逐项不变，r52 墓碑回归 10P 零变化；残留「删后压队熬过墓碑 TTL」按 B52-02 horizon 记入 docs/runbook。**负控**：新增 `tests/unit/test_a69_04_tombstone_age.py` 14 条（含 fenced 二次删除），pre-fix 基线 **13F/1P**（对照组「墓碑不短于 state TTL」绿），post **24P**（含 r52 10P）；定向 19 文件 **289P**（rebase 前 `59dcff7`）；`tests/unit` 全量 **11939P/12S**（同基线），唯一红 `test_issue_a2r19_registry_retry::test_ensure_registration_self_heals_after_redis_recovers` 为时序 flake（单文件 5P 复跑绿，与本轮链路无交集）；rebase 至 `3fd516f` 后定向 8 文件 **142P** 复跑绿；`mkdocs build --strict` **rc=0**；ruff 改动行无新增（既有 BLE001 除外）；`landing_gate.py ci` **rc=0**（main `3fd516f` CI success），`landing_gate.py trailers` 1 个 `[policy]` 提交带 Policy-ack。代码 commit `8f579a2` 已推 origin/main。PR2/PR3（A69-02/03 事件索引）按台账第七十轮施工顺序转 B2 下轮。

> 一行摘要（第四十一轮，2026-10-20）：**A2R24 契约族 console 侧对齐（按价值序 01/03/04；基线 `origin/main` `3cbf8f4`）**——①**错误响应统一（A2R24-01）**：新增 `backend/error_contract.py`，`detail` 统一为 `{code, message, ...历史业务键}`；`create_app` 挂 HTTPException / RequestValidationError / 未捕获 Exception 三个处理器（保留 Retry-After 等 headers；错误体带顶层 `request_id`，与 X-Request-ID 同值）；历史三种 dict（message+conflict、message+error/reason、error+配额字段）在源站与 handler 双向收敛且保留业务键；**47 处** `str(e)`/f-string 异常原文出网改 `error_detail`/`scrubbed_detail`（响应只留固定文案，原文进 WARNING 日志），`cluster._connect_redis_or_502` 的 Redis URL 仅保留 `scheme://user@host:port`、口令段 `***`；未捕获异常由 Starlette 默认 `text/plain` 改 JSON 500 `internal_error`，异常原文不出网。前端 `messageFromDetail` 已兼容对象 `message`，本次无需改前端。②**services 本地档（A2R24-04）**：`get_redis` 统一走 `get_redis_or_503`；`types/list`、`{id}`、`{id}/status`（及 stop）与同模块 list/topology 对齐为 503 JSON，三个 GET 不再 `NoneType` 500 `text/plain`。③**logs/stats 采样标注（A2R24-03）**：平台视角 `LogStatsResponse` 新增 `estimated`/`sample_size`（样本未覆盖整条 Stream 时 `estimated=true`、`sample_size=实际采样条数`）并记 WARNING；tenant 分支保持逐条精确（`estimated=false`、`sample_size=null`）；`truncated` 仍只表示预算/键扫描触界，不再被误读为「精确」。**负控**（临时回退三处实现）：错误形状 **4F**（string 404、手工/自动 422、未捕获 500、create_app 挂载）、services 模块全部 GET **5F**（目标三端点 + 共享 helper 回退后的 list/topology）、stats 平台标注 **1F**；恢复后新测 **12P**。定向新测 12P；console 全量 **926P/13S**（唯一失败 `test_issue266_built_bundle_has_no_admin_key_material` 为本地 gitignored `frontend/dist` 旧产物，环境项，与本次 backend 改动无关；CI 由 fresh build 专用检查覆盖）；`landing_gate.py ci` **rc=0**（main `3cbf8f4` CI success）。代码 commit `1142288` 已推 origin/main；推送后 CI run `39348725204` 四 job **success**（Console backend regression success、Regression gate success、Console frontend contracts success、plaita-ai success）。A2R24-02/05/06/07/08/09/10 未在本轮展开，按价值序续。

> 一行摘要（第四十轮，2026-10-20）：**性能上界 + 分裂队列两格（台账 A2R8-04/05 + A56-02；基线 plaita-nodes `663dbd0` / plaita `d64ff67`）**——①**A2R8-04 SQL 先物化整批**：`sql_query` 固定 `fetchmany(256)`，300 行 × 128 KiB、`max_bytes=1024` 时首行即超限但 256 行已进内存（tracemalloc **32.335 MiB = 33,111× 预算**）。改首行单读校准行宽，此后每批 `min(256, 剩余字节预算/已观测最大行宽, 剩余行数)`，至少 1 行用于判定“恰好等于上限后是否还有更多”；fail-closed/`observed_*` 语义不变，峰值 **0.433 MiB**（单行+驱动开销），不再随行数增长。②**A2R8-05 protocol_lines 无界**：直跑与沙箱两路径直接 append（20k 条 ~1 KiB 新字符串驻留 **21.42 MiB**）；新增 `_ProtocolLineBuffer`（10,000 行 / 4 MiB 双闸），超限继续消费但不驻留，末尾非 JSON 标记行 + WARNING，`parse_stream_details`/usage 扫描自动跳过；峰值 **4.19 MiB**。③**A56-02 查漏**：console `api/schedules.py` 已在 main 走 `_task_queue_name()`（B3 r54），残留是服务端 `ScheduleService.__init__` 固化 `PLAITA_QUEUE_NAME` 且忽略 console env——cron 与“立即触发”分裂。新增 `resolve_dispatch_queue_name()` **每次派发时**解析（显式 `service_config.queue_name` > `PLAITA_CONSOLE_TASK_QUEUE` > `PLAITA_QUEUE_NAME` > `QUEUE_NAME` > 默认），注册元数据同步刷新；两侧 env 同时存在且不同时打一次 WARNING（保留键名；按 #151 判定表 A 类在 `_EXEMPT_ENV_NAMES` 登记）。负控：A2R8-04 回退生产文件 **2F/1P**（峰值 33.9 MB，上限内对照绿）；A2R8-05/A56-02 回退 → 收集期 ImportError，行为面 pre/post 探针精确（21.42→4.19 MiB、`plaita:flow:queue`→`:v2`）。新增 9+8 条测试；nodes 定向 73P、全量 **383P/2S**；plaita 定向 33P、`tests/unit` **8426P/12S/3498 subtests（rc=0）**；`mkdocs --strict` rc=0；ruff 改动行无新增；`landing_gate.py ci` **rc=0**（main `d64ff67` CI success）。代码 commit：plaita-nodes `5d2bf7f`、plaita `19201c6`。证据 `docs/proofs/duo40-a2r8-a56-02-bounds.md` + `data/duo40-*`。

> 一行摘要（第三十九轮，2026-10-20）：**A2R20 延迟族余量三格（r55 A2R20-05/06/07；基线 `f121d42`）**——①**A2R20-05 Redis 超时声明**：新增 `plaita/redis_client.py` 统一构造（显式 connect/read：console 交互 2s/5s、worker/扩展服务/运维脚本 5s/30s、健康检查 3s/3s；`PLAITA_REDIS_CONNECT_TIMEOUT`/`PLAITA_REDIS_SOCKET_TIMEOUT` 只覆盖未显式传参的调用点，URL query 优先），覆盖 console 数据面/nonce/SSE/cluster/services 健康、worker 队列与租约、`server/factory` 存储侧默认、services 启动器与 5 个运维脚本；AST 静态检查禁止运行时裸 `from_url`。黑洞探针（accept 不响应）redis-py **5.0.8 下 console/worker/nonce 由 >8s 阻塞 → 均 5.0s**，8.1 同为 5.0s（裸 `from_url` 对照仍随版本漂移）。②**A2R20-06 启动回填有界**：`sync_all_published_to_engine` 每 200 版本一个 pipeline（SET+SADD×2），SQL 改 `FlowStore.iter_published_versions` 键集分页（1000/页），加墙钟 5s/版本数 5 万双预算；触界只降级不阻断启动，`/health.engine_sync` 暴露 `ready/degraded` 与 written/skipped/errors/batches/truncated_reason/elapsed_ms。真实 SQLite 探针 **2,000 版本 1.46s/6,000 往返 → 0.05s/10 往返（29×/600×）**；合成 10k→50 往返 0.20s、30k→150 往返 0.61s（修复前外推 90k 往返/23s）。③**A2R20-07 PG 启动超时**：非 SQLite `create_engine` 注入 `connect_timeout=5`/`pool_timeout=5`/`pool_pre_ping=True` 与 PG `lock_timeout=5s`（env 可覆盖）；连接类异常沿 cause 链识别后 **fail-fast，不再回退 create_all 二次阻塞**；启动日志目标 URL 脱敏。PG 黑洞探针 **>8s 阻塞 → 5.15s 具名 OperationalError**；SQLite 参数零变化。负控：裸 `from_url`/无 pipeline/无 deadline/max_versions/PG 无超时均精确红。新增 21 条测试（`test_a2r20_redis_timeouts.py` 8、`test_issue_a2r20_startup_budgets.py` 12、nonce 超时断言 1）+ 2 处既有断言同步；定向 6 文件 **56 passed**；console 全量 **888 passed/13 skipped**（唯一失败 `test_issue266_built_bundle_has_no_admin_key_material` 为本地 gitignored `frontend/dist` 旧产物，环境项；CI 用 fresh build 专用检查）；`tests/unit` **8413 passed/12 skipped/3498 subtests（rc=0）**；`mkdocs build --strict` rc=0；ruff 新增/改动文件全绿（既有 BLE001 除外）；`landing_gate.py ci` **rc=0**（main `f121d42` CI success）。代码 commit `0fb3e13`。

> 一行摘要（第三十八轮，2026-10-19）：**A55-S2/S3/S4 余量（DLQ/queue 审计面）**——①**S2 拒绝限频**：`auth.ensure_platform_admin`（DLQ replay、cluster 生命周期、服务停止）与 `tenants._audit_denied`（租户成员闸）此前用 `audit.record`，每次 403 写一行；改为 `record_denied`（`(action, actor, ip, tenant)` 60s 窗口合并，下一行带 `detail.suppressed`，WARNING 每次保留）。实测 tenant admin 5 次 replay 403 由 **5 行→1 行**；editor（平台闸）/viewer（角色闸）/401/失败登录四条路径均按窗口合并。console conftest 增 autouse `reset_denied_throttle()`：进程级窗口会跨用例串味且每例独立 DB，避免「有审计行」假红。②**S3 归属与身份**：`POST /api/queues/dlq/{dlq_id}/replay` 此前审计行 `tenant_id` 取 `request.state`（API-key 无 header/平台管理员为 `""`），与死信条目租户断链；现显式传**条目生效租户**并回显 `detail.target_tenant`，跨租户 `X-Tenant-ID` 以条目为准；API-key 行带 `auth_source=api-key`、`key_fingerprint`（sha256 前 16 位）、`request_id`、`role_snapshot`。另补 404/422 尝试留痕（`dlq_not_found`/`target_not_allowed`），补齐 A53-S2「每次尝试」契约。③**S4 读面纵深**：`services/audit.list_audit` 增 `AUDIT_LIMIT_DEFAULT=200`/`AUDIT_LIMIT_MAX=1000` 并在服务层 `max(1, min(...))` 钳制（SQLite `LIMIT -1` 不再物化全表）；API 的 `Query(ge=1)` 保持 422，读审计 `audit.read` 记录过滤条件/生效上限/行数；租户 admin 只读当前世代本租户行。负控 5 组均精确红：平台闸回退 → 3 条用例红（5/3 行）；租户成员闸回退 → 1 条红（3 行）；replay 归属回退 → 2 条红（tenant=""）；4xx 不留痕 → 1 条红；服务层去钳 → 1 条红（1205 行）；API 去 ge → 1 条红（-1 变 200）。新增 `test_r55_audit_queue_surface.py` 10 条；定向 11 文件 **127 passed**；console 全量 **842 passed/13 skipped**（唯一失败 `test_issue266_built_bundle_has_no_admin_key_material` 为本地 `frontend/dist` 旧产物，环境项；CI Console frontend contracts success）；`tests/unit` **8389 passed/12 skipped/3498 subtests（rc=0）**；`mkdocs --strict` rc=0；ruff 全绿。同步 `ops-runbook.md`（console 审计口径）与 `s6` proof。代码 commit `a6992bb` 已推 origin/main，CI run `39264934264` 四 job **success**，`landing_gate.py ci` **rc=0**。

> 一行摘要（第三十七轮，2026-10-19）：S18-01/03 drain 语义与交接顺序（r55 A2R18；基线 `2b3b595`，含 B3 的 S18-02A registry 状态机）——①**S18-01**：`_consume_loop` 增 **post-read gate**（drain 前发出、drain 后返回的 read/reclaim 结果不 dispatch/ack/bump，消息留 PEL；`drain-read-gate` WARNING）；`_force_stop_after_drain` **无论 active 是否为 0** 都置 `_running=False`（关闭「定时器先到、任务恰好收尾」窗口）；新增 D2 标记 `_force_stop_event` 与 `_cancel_inflight_after_drain`（快照取消登记表、按登记租户在锁外调 `execution.cancel()`，best-effort、失败 WARNING `drain-force-stop-cancel`）；节点重试退避从整体 `_sleep(delay)` 改 **0.2s 分片**，D2 时 `_requeue_retryable_failure` 直接 `return False`（不 enqueue/不 ack，`retry-backoff-aborted-by-drain`），仅 D1 不打断；`stop(wait=True)` / `run(wait_for_inflight=True)` 在停看门狗/注销前按 drain 剩余预算有界等待在途归零。硬中断按 B18-01 留 owner 裁决，未实现。②**S18-03**：`_handover_non_affine` 由 ack→enqueue 改 **enqueue→ack** 并返回 bool（`_consume_loop` 回写 `acked`，日志不再把「已交接/留 pending」混为一谈）；enqueue 失败原条留 PEL/Stream、ack 失败保留新副本与原 pending；与 A46-02 目标不可用剥 target 的 `copy_body` 复用同一安全序。③**S18-04**：新增 `tests/unit/test_a2r18_drain_handover.py` 22 条，含 event/timeout/cancel × concurrency=1/2 × `PLAITA_DISABLE_HOLDER_LIVENESS=1` 参数化（gate-on pending=1 且租约释放后 XAUTOCLAIM 可推进；回滚开关复现 ack）及 post-read gate、force-stop cancel、可中断退避、交接 enqueue 失败/ack 失败三态。负控（rebase 后复跑）：A45-01 去超时 cancel、A45-02 删 post-read gate、A45-02 恢复 `active<=0` 早退、A46-06 恢复整体 `_sleep`、A45-05 恢复 ack→enqueue 各 **1F 精确红**；S18-04 删决议载体分支 **6F/6P**（gate-on 6 红、rollback 6 绿作对照）。定向 8 文件 **206 passed**；rebase 后 `tests/unit` 全量 **8389 passed/12 skipped/3498 subtests（rc=0）**；`mkdocs build --strict` rc=0；ruff 改动行无新增 BLE001（11 条均为既有）。代码 commit `c34251e` 已推 origin/main（CI run `39248405831` 四 job **success**），`landing_gate.py ci` **rc=0**。

> 一行摘要（第三十六轮）：DLQ 运维面收口（r53 A53-01/A53-S2/A53-S3 + A53-11 查漏）——①**#264 P1**：`delete_tenant` 只排空任务 Stream，平台级 DLQ 保留前代 payload；同名重建后新租户 viewer 仍按 `payload.tenant_id` 读到，回灌后旧执行以新租户身份跑。修法：`purge_tenant_dlq` 按**信封内** `payload.tenant_id` 收窄 XDEL（畸形/不可归属跳过 + WARNING，失败 loud）；`_tenant_dlq_names` 覆盖 `PLAITA_DLQ_KEY`/各队列同族/`*:dlq`，非 Stream 键跳过；`_purge_runtime_after_commit` 与 queue 并列、三步各自尝试后统一上抛。读面纵深：租户视角要求**显式** `tenant_id` 相等 **且** `dead_lettered_at >= 租户 created_at`（当前世代；优先 `request.state.tenant_generation`，DB 回退，取不到 fail-closed）；缺字段/畸形不再回退 default，只对平台可见。②**replay 幂等（A53-S2）**：新增 `plaita/server/dlq_replay.py` + `POST /api/queues/dlq/{dlq_id}/replay`（平台管理员）：`(dlq_key, dlq_id)` `SET NX EX 7d` 标记，二次回灌 `already_replayed`；无 `execution_id` 的 start 铸造确定性 `uuid5` 并补 `dedup_key`；`schema_version` 继承源信封（缺失=LEGACY 1，超限入队前 `schema_too_new`）；执行墓碑→`execution_deleted`；payload 缺租户须显式 `tenant_id`，冲突→`tenant_mismatch`；每次尝试写 `plaita:dlq:replay:audit`（控制台另记 DB 审计），DLQ 原条目保留。③**trim 可观测（A53-S3）**：捕获 XTRIM 返回值→`dlq_trimmed` + 限频 WARNING；失败→`dlq_trim_failures` + WARNING（基线仅 DEBUG）；`dead_lettered`/error/alert 移到 XADD 后、ack 前，ack 失败计 `dlq_ack_failures` 且 WARNING 含 `source_ack=failed`；alert 增加 `dlq_id/dlq_length/dlq_max_len/trimmed_count/schema_version`；stats/metrics 增 `dlq_max_len`/`dlq_saturated`。④**A53-11 查漏**：DLQ 解析对 msg_id/字段键/last_id 用 `errors="replace"`、整体 try/except 单条降级；`get_queue` 的合法 JSON 非 object 不再 `**` 展开 500；console Redis 客户端 `encoding_errors="replace"`。**顺修 main 红**：A48 夹具未跟上 A54-06 orphan-flow 拒绝门（CI `f6b1b2b` failure），改为先清 flow 再走 404 补清扫，未走 override。负控：#264 pre-fix **4F/2P**（同名重建 viewer 读到 2 条前代条目）、post **6P**；replay 还原标记+确定性 id → **4F/15P**，baseline 裸 `enqueue_task` 双投探针 **2 条 completed 执行**、post **19P**；trim baseline **5F**、post **5P**；容错 baseline **6F/1P**、post **7P**。`tests/unit` 全量 **8322 passed/12 skipped/3498 subtests（rc=0）**；console 全量 **790 passed/13 skipped**（唯一失败为本地 `frontend/dist` 旧产物扫描，CI 前端 job success）；`mkdocs --strict` rc=0；layering 3P；ruff 改动行无新增 BLE001。代码 commit `89dabe0` 已推 origin/main，CI run `39236316390` 四 job **success**，`landing_gate.py ci` **绿**。

> 一行摘要（第三十五轮）：世代绑定批（r52 A51-01/A51-02）——①**A51-01 执行删除墓碑**：删除只清 state/index/fence/lease+计数键，删除前入队/崩溃回收/节点失败重投/死信守卫克隆的 `start` 载体仍会走 `start_flow` 的「无状态=全新执行」分支，用旧 `$INPUT` 重建 completed。修法：删除时把**平台根墓碑** `plaita:exec_tombstone:{id}` 与删除同一 pipeline（fenced 档同一 Lua，排在 A51-03 计数键之后）原子落盘；`_dispatch_task`（配额闸之前）、`start_flow` 直达调用、`_handle_start_dedup_hit`、`_requeue_body_for_retry`、`_dead_letter_guard` 五点按同租户复核——命中 ack 丢弃、不克隆、不终态化；同 id 重放释放 dedup 映射，新 id 正常放行。墓碑 TTL 跟随状态 TTL（`PLAITA_EXECUTION_TOMBSTONE_TTL_SECONDS` 可覆盖，`<=0` 永久），并补 `PLAITA_TASK_MAX_AGE_SECONDS`（默认 30 天）兜住 TTL 到期后的历史载体；平台根键不被租户清理擦除。②**A51-02 请求级世代绑定**：r38 令牌在服务首次读才捕获，旧租户请求跨 delete+create 后捕获新世代并写入重建租户。修法：`resolve_session` 单条 outer JOIN 同时取成员资格/角色与 `Tenant.created_at/contract_secret_id`；`require_auth` 写 `request.state.tenant_generation`（平台 `X-Tenant-ID` 读一次租户行，缺失 403）；async 依赖 `bind_tenant_generation` 绑定 ContextVar；`assert_tenant_generation` 在 flush 后同一事务复核，覆盖凭据删除、成员角色/移除、flow/版本/节点描述/属性类型、本地执行/调度（已护栏 `save_credential`/`add_member` 改请求 token 优先，0 新增读）。施工顺序 **①→②**：墓碑先收窄「删除后重建执行」面，世代绑定再关闭「请求跨 ABA 写入重建租户」面。负控：A51-01 新用例 pre-fix **8F/2P**（四路径均重建 completed），post **10P**；A51-02 baseline 探针 `add_member ok`/`save ok` → 绑定旧世代后 `UserError`/`ValueError`、重建租户零成员/零凭据；新 console 用例 **7P**（含 HTTP sync handler 的 ContextVar 传播）。定向 17P；rebase 后 `tests/unit` **8285 passed/12 skipped/3498 subtests**、console **716 passed/13 skipped**；`ci-gate.sh` **PASSED**（8565 passed/16 skipped、coverage 93.77%、layering/wheel/size 绿）；`mkdocs --strict` rc=0；ruff 改动行无新增 BLE001。阶段 2（队列信封 `tenant_generation` / 租户级世代）按 r52 规格保留，未在本批展开。代码 commit `fc03041` 已推 origin/main，CI run `39220890511`（head `fc03041`）**success**，`landing_gate.py ci` **绿**。

> 一行摘要（第三十四轮）：版本边界收尾三格（r49 A49-01/A49-02 + r50 A50-03）——①**A49-01 克隆-死信热环**：超版本拒收撞上死信守卫「非终态+租约空→重入队恢复」，副本继承同一 v2 再拒收，流内恒 1 条、DLQ 每轮 +1（回滚/生产者先行）。修法：拒收时 `StreamTask.schema_rejected=True`，守卫对该标记**不克隆、不终态化**，放行死信一次（活租约仍留 pending）；生产者先行语义=载体进 DLQ 留证、升级后重放，suspended 仍可被 event/cancel/timeout 唤醒。②**A49-02 DLQ 毒丸 500**：`_dead_letter_invalid` 写字符串 payload，定型 `DlqEntry.payload` 在租户过滤前抛 ValidationError。修法：非 dict payload 统一包 `{"raw": ...}`（整信封不可解析同款），数值字段容错，补 `schema_version`/`raw_fields`/`truncated`。③**A50-03 重试口径**：worker 见 `NodeExecutionError` 即判可重试，F2 的 1001/非校验 ValueError 分类在 abort 包装层丢失。修法：core 公开 `is_retryable_error`，worker 沿 `__cause__` 继续复核（#219 输出校验例外保留）。负控还原 **2F**（DLQ 6≠1、克隆仍在流内）、**1F**（ValidationError/500）、**3F**（两条确定性链+深层超时）；定向 63+29、全量 unit 8265 passed/12 skipped/3498 subtests、console 686 passed/13 skipped；layering/wheel/size 与 mkdocs --strict 绿；代码 commit `a2fbb19` 已推 origin/main，CI run `39209673516`（head `5ccaf6a`，含本 commit）success。
>
> 一行摘要（第三十三轮）：A46-04 / A47-F2（决议载体 × 租约冲突）——suspend 提交窗口内到达的 `event`/`timeout`/`cancel` 被 `_lease_conflict_ack_safe` 按「持有者在册」当重复载体 ack，持有者落盘 suspended 并释放租约后零载体、执行永久 suspended（默认 event 节点自动恢复网为 0）。修法：决议载体**不看存活信号、一律不 ack**（信号缺失也不 fail-open），留 pending 等 reclaim / #242 守卫副本推进；`continue`/`retry`/`start` 原 ack 语义不变，#258 状态感知窗口不受影响。负控 7F 精确红、对照 3 绿；定向+相关 652 passed，`tests/` 全量 8541 passed/rc=0。顺带修复 main 红（`9411e2c` 的 disk_guard 测试桩缺 `schema_version`），新 CI run success。
>
> 一行摘要（第三十二轮）：A42-05 + A44-01 + A44-02（版本边界绕过守卫族）——超版本信封的**首投与回收同判**并统一走 `dead_letter_if_allowed`（守卫拒绝时留 pending 等新 build）；`_reclaim_one` 补信封版本校验（崩溃窗口后不再把新 schema 消息按旧语义真执行）；worker 的重投/交接/归一化副本**继承源 `schema_version`**（选「继承」而非「超版本副本进死信」）。DLQ 信封补 `schema_version`，invalid-payload 第二旁路同步接守卫。负控 12F 精确红，既有兼容语义零变化。
>
> 一行摘要（第三十一轮）：#258 P1——event/timeout resume 在流内推进并落过 checkpoint 后重投，`_handle_resume` 判协议错误、`run()` 旧分支 ack 唯一载体致 running 永久悬挂。修法：ack 前状态感知，`running` 且 `$LAST_NODE` 已不是 pending 挂起点时把载体归一化为 `continue`（入口直接续跑；`run()` 兜底先入队新副本再 ack，读盘/入队失败不 ack 留 pending）。真 #33 错配零变化；负控 5F/3P 精确红。

| 轮次 | Issue | 交付内容 | 验收 |
|---|---|---|---|
| 第五十六轮 | 台账 A69-02/03 修后复核 + A2R24-10 全端点扫尾核验（无 GitHub issue；任务书标称第五十轮，台账 50 已由 ops-runbook 实测定版轮占用，按序列顺延） | **两个已落批修复的验证轮（零代码改动）**：① **A69-02/03**（B2 r51 `e374aa1` + 残面 `88607b0`/`3fec442`）——原 9+10 条规格负控逐条单跑（console canonical 11/11 + unit index_cleanup 12/12）+ A69 族定向 50P；**janitor 预算/游标语义**：实现逐项核对（预算=检查成员数、进程内 `{cursor,pending}` 不写 Redis 键、B78-01 按实际进入桶数轮转、A78-05 单条 Lua 判死删除、只按 EXISTS 不按 score）+ 真实 Redis 8.0.2 db9 探针 21/21——budget=1×7 桶 7 tick 收敛/rotation 逐 tick +1 无饿死/examined≤1 每 tick/live 7/7/死成员 7/7/pending 有界且续扫排干，订阅 4 桶×2 orphan 8 tick 收敛，budget=2 全 live 检查有界零误删，全程零 KEYS。② **A2R24-10**（B1 r49 `3d9a741` + r55 `de5a592`）——抽 5 类端点独立探针 21/21：未知字段一律 422 `extra_forbidden`（executions cancel、schedules enable/disable、tenants DELETE+create、cluster refresh+reconcile、flows 版本乐观锁字段；enable/disable、tenants、cluster 三族为 r55 落地测试未逐一覆盖端点），兼容向空体/`{}` 不误伤、拒绝无副作用。**新增残面 = 0** | 逐条复验全绿：A69-02 11P + A69-03 12P + 族定向 50P、console 定向 25P（canonical 11 + request_strict 14）、两探针 **42 断言 42 绿**（`/tmp/r50b_probe_janitor.py`、`/tmp/r50b_probe_a2r24_10.py`；真实 Redis db9 跑后 dbsize=0）；`tests/unit` 全量 **8599P/12S/3498 subtests（rc=0，790.97s）**；`landing_gate.py ci` 独立 **rc=0**（main `4970e05` success）；仓库仅追加本台账并推送 |
| 第五十五轮 | 台账 A2R24-10 全端点余量（无 GitHub issue；任务书标称第四十九轮，台账 49 已由 B74-02/B75-01 占用，按序列顺延） | **无 body 写端点请求体严格化**：① 复核 r49 `3d9a741`/B2 r53：38 个请求模型全部 `StrictRequestModel`，40 个 JSON body operation 无漏网；② 27 个无 body 参数的写端点（POST cancel/logout/trigger/enable/disable/rotate-secret/cluster refresh·reconcile·infra check/start/stop/stream ticket；DELETE flows/versions/nodes/property-types/credentials/events subscriptions/executions/schedules/tenants/members/users/cluster infra·instances·clusters）此前静默丢弃任意请求体，新增 `EmptyRequestModel`（零字段、`extra="forbid"`）挂到每个 handler；空体/`{}`/`null` 兼容，未知字段 422；③ OpenAPI 扫尾由「有 body 的模型必须 forbid」升级为「所有写端点必须有 body」，69 个写操作 67 个声明 requestBody，豁免仅 copilot（AG-UI 透传）与 flowVersion/semver/detail（form 契约）；④ MIGRATION/README 记录兼容面与 `DELETE /executions/{id}?force=true` 迁移 | 负控：还原 10 个 api 模块（保留新测试）→ **7F/7P**：cancel `{"reason"}`、logout `{"all_sessions"}`、trigger `{"params"}`、flow DELETE `{"force"}`、node DELETE `{"force"}`、credential DELETE `{"force"}` 全部 **pre-fix 200**（响应体证据在 `/tmp/a2r24-r49-nc/nc.txt`），OpenAPI 扫尾 checked=39 红；恢复后本文件 **14P**（含空体/`{}`/`null` 兼容用例）。定向 10 文件 **89P** + 8 文件 **129P**；console 全量 **1038P/13S/1F**（`test_issue266_built_bundle_has_no_admin_key_material`，本地 10-18 陈旧 `frontend/dist`，gitignored，非本轮）；ruff 仅既有 BLE001（改动行无新增）；`landing_gate.py ci` **rc=0**（main `8eac9fd` conclusion=success）；commit `de5a592` 已推 origin/main |
| 第五十四轮 | 台账 A69-02/03 修后复核 + A2R24 契约族余项 + A55-S1/S5-S7 落地面（无 GitHub issue；任务书标称第四十七轮，台账 47 已被 A2R31 占用，按序列顺延） | **A69 复核**：①负控——pre-fix `3fd516f` + 当前 `test_a69_02_event_canonical.py` **9F/2P**（2 绿=legacy 只读/legacy 频道；其余 9 条规格负控精确红）；A69-03 的 12 条规格用例在 pre-fix 因 `plaita.event.index_janitor` 不存在结构性红（ModuleNotFoundError），当前树全绿。②当前树 A69/A78/A83 事件索引 **42P**。③真实 Redis 8.0.2 db10 探针：budget=1、3 桶×（2 dead+2 live）**6 tick 收敛**、每 tick examined=1、live 6/6 保留；40d 老时间戳活事件保留/90d ghost 删除；订阅四索引各 1 dead → scard=2、live 保留；写订阅把桶 TTL 3s→604800、prune 不改 TTL；全程 KEYS 增量 0。**A2R24 复核**：02/05/06/07/08/09 现树 **41P**（queue `start` 索引 + `start_id` 排他游标 + `truncated/next_start_id`；logs/events `total`=真匹配数 + `returned/truncated/next_cursor`，`limit ge=1`、`order` Literal；`ExecutionInfo.flow_name`；flow/credential/tenant/queue 三投影 + versions typed + 平台同名 flow 409 + export entity tenant；trigger `{success,mode,execution_id,msg_id}`；SSE `text/event-stream`）。**残面修复 `9c017d3`**：`/schedules/{id}/history` 原两档 `total=len(records)`、limit 以上历史不可达，本地档记录 `{triggered_at,status,error,execution_id}` 与集群档/前端 `{fired_at,flow_id,version,enqueue,msg_id}` 不一致（本地对话框时间恒空、状态恒显「入队失败」）→ typed `ScheduleHistoryResponse`（`total`=匹配总数、`returned`、`truncated`、`offset` 0..10000、`next_offset`；`limit` 1..50 越界 422）+ `ScheduleFireRecord` 统一两档字段（本地保留 `execution_id/status/error`，集群为 null）；前端 `api.ts` 类型与 `docs/scheduler-design.md` 同步。**A55-S 落地面**：S1 26 个零审计端点 + cluster 19 成功路径（含 r57 `flow.ai_generate`）、S5 metrics 队列族有界 SCAN/0 KEYS + event_type 字符集、S7 runbook 审计/metrics 口径均在 HEAD；S6 仍为规格（`docs/proofs/s6-global-observability-policy.md` + runbook「待 owner 裁决」），未入码。 | 新测 `test_issue_a2r24_schedule_history_contract.py` **4P**；回退 `api/schedules.py`+`services/local_scheduler.py` **4F** 精确红（total=本页、本地无 `fired_at`、`limit=0` 未 422）；定向 9 文件 **63P**；`tests/unit` 全量 **8599P/12S/3498 subtests（rc=0，901.11s）**；console 全量 **1030P/13S/1F**（唯一失败 `test_issue266` 命中本地 2026-10-18 gitignored `frontend/dist` 陈旧产物，非本轮）；`tsc --noEmit` rc=0；ruff 新改文件全绿；`mkdocs --strict` rc=0；`landing_gate.py ci` **rc=0**（main `f1777ed` success）；代码 `9c017d3` 已推 origin/main |
| 第五十三轮 | 台账 A69-02/03（r70 PR2/PR3）规格余项核验 + A1 r83 三条 a 类残面 A83-02/03/05（无 GitHub issue；任务书标称第四十七轮，台账 47 已被 A2R31 占用，按序列顺延） | **规格复验**：pre-fix `3fd516f` 归档 + 当前用例 **19F/4P**（A69-02 9F/2P、A69-03 10F/2P；4 绿=legacy 只读/legacy 频道兼容/存活成员/老时间戳保护），当前树全绿。**残面修复（单 commit `88607b0`）**：①**A83-03（P3）**——判死+删除的 Lua 按 500 成员/批拆 `EVAL`（事件与订阅共用 `_prune_index_members_atomic`/`_sync`），janitor 大预算不再撞 Lua 5.1 `unpack` 上限；②**A83-05（P3）**——订阅四索引 `SADD` 后改 `_INDEX_TTL_EXTEND_LUA`「只延长不缩短」，键形抽 `subscription_index_keys` 让 store/delete/reindex 同源（防 console 漏 `node:` 式漂移）；③**A83-02（P2）**——新增 `ensure_event_index_member_sync`/`ensure_subscription_index_members_sync`；回填 `--apply` 的 `skipped_existing` 顺带补索引（`index_repaired`），新增 `--reindex [--apply]` 扫正典事件 `events:*` 与订阅 `data:*` 修复已过期/被砍短的桶（dry-run/幂等/TTL 只延长、不改记录/data TTL、不删键；**含 A1 r83 未列的订阅索引修复缺口**）；④**B83-01**——`-1` 语义注释改为实义（新桶/外部持久桶按目标 TTL；行为边界保留待 owner 裁决）；`event-system.md` 同步四节。 | 新测 **8P** / pre-fix HEAD **8F** 精确红（A83-03 `errors=1`、订阅桶 `ttl=2`、回填 `zcard=0`、`reindex` AttributeError、CLI `SystemExit=2`）；真实 Redis 8.0.2 db13：pre-fix 单 EVAL 8200 成员 → `ResponseError user_script:9: too many results to unpack` 且 zcard 保持 8200，post-fix budget=9000 **examined=8200/pruned=8200/errors=0/0.11s**；订阅短 TTL 写后桶 TTL 仍 **604800**、2.2s 后桶在；事件 reindex 修复后 zcard=1/桶 604800/记录 TTL 不变/`list_events` 可见、二次 `already_indexed=1`；`migrate` skipped_existing `index_repaired=1`；订阅 reindex 四索引恢复、桶 604800、data TTL 不变、`list_subscriptions` 可见。定向 **417P/3S/49 subtests**；`tests/unit` 全量 **8597P/12S/3498 subtests（rc=0，729.46s）**；console 全量 **1021P/13S/1F**（`test_issue266` 命中本地 10-18 gitignored `frontend/dist` 旧产物，clean archive 该测 skip，本轮未触前端）；ruff 无新增（redis.py 11 BLE001 既有）；`mkdocs --strict` rc=0；`landing_gate.py ci` **rc=0**（main `f2d0589` success）；`88607b0` 已推 origin/main，main CI run `39517368826` 四 job **success** |
| 第五十二轮 | 台账 A69-02/03（r70 PR2/PR3）规格余项核验 + 回填幂等残面（无 GitHub issue；任务书标称第四十八轮，台账 48–51 已被既有交付占用，按序列顺延） | **A69-02/03 独立复验**：①pre-fix `3fd516f` + 当前 console 用例 **9F/2P**（九条规格负控全红）；当前树 26 个单点回退 **26/26 适用项精确红**（A69-02 12 + A69-03 14，覆盖 canonical/回扫/TTL/单 timestamp/去重/legacy/租户/回填、读清/janitor 冷桶/预算/EXISTS/score/node/坏桶/KEYS）。②real Redis 8.0.2 db12 探针 22/22：canonical TTL 604800、score==timestamp、legacy 键 0、引擎 get/list 可见；回填剩余 TTL/不删旧键/幂等；janitor budget=1/存储跨 7 桶 16 tick 收敛、examined≤2/tick 合计、live/40d 老时间戳保留、无 KEYS、热桶读后 zcard 6→2。③**残面 `3fec442`**：回填脚本对缺/空 `event_id` 原由 `Event` 每次生成新 uuid，`--apply` 重跑不断复制 canonical → 按 legacy 键后缀派生；新增回归（pre-fix 1F）+ event-system 文档。 | 新测 post **4P** / pre-fix **1F**；定向 **64P** + console A69-02 **11P**；`tests/unit` 全量 **8589P/12S/3498 subtests（rc=0，728.04s）**；ruff 全绿；`mkdocs --strict` **rc=0**；`landing_gate.py ci` 独立 **rc=0**（main `2a39cfc` success）；`3fec442` 已推 origin/main，main CI run `39510451425`（head `3fec442`）四 job **success** |
| 第五十一轮 | 台账 A69-02/03（r70 PR2/PR3）规格余项核验（无 GitHub issue；任务书标称第四十七轮，按台账序列顺延） | **A69-02/03 独立核验 + 残面**：① pre-fix `3fd516f` + 当前用例 **19F/3P**（A69-02 9F/1P、A69-03 10F/2P）；当前树 26 个单点回退 **26/26 红**（9 canonical/TTL/去重/legacy/租户 + 12 读清/janitor/预算/EXISTS/score/node/坏桶/KEYS + 4 回填 + legacy 频道）。② real Redis 8.0.2 db9：budget=1 12 tick 收敛 2/2/2、examined≤1/tick、live 6/6；老时间戳活事件保留；canonical TTL 604800、score==timestamp、legacy 键 0/旧读面可见；回填 dry-run/剩余 TTL/幂等/不删旧键；purge 两形；janitor 关闭 WARNING；订阅冷桶/delete-after-expiry。③ 残面：legacy 频道 deprecated 文档 + 写侧 env/预算口径 + 重复守卫 + 回归。 | 定向 **118P**；unit 全量 **8588P/12S/3498 subtests（rc=0，717.89s）**；console A69-02 11P；ruff 无新增；mkdocs rc=0；landing_gate rc=0（main `15cc33d` success）；`66c2fe9` 已推 origin/main |
| 第四十九轮 | 台账 B74-02 / B75-01（P3，无 GitHub issue；任务书标称第四十八轮，按台账序列顺延） | **unknown ≠ default 队列三态**：①`candidate_queue_name` 仅非空非纯空白字符串为 known，缺失/空白/纯空白/非字符串统一 `None`，不再回落 `DEFAULT_DISPATCH_QUEUE`；`DispatchCandidate.queue_name: Optional[str]`，unknown 默认策略下不进 `allowed`/`target`，拒绝前缀 `queue: unknown (worker metadata lacks queue_name)`，known 不匹配仍是 `queue: mismatch`。②unknown 只拒定向不拒广播：无目标时 `dispatch_one` 按调用方队列广播、不带 `target_instance_id`，legacy default worker 仍可消费。③`PLAITA_DISPATCH_UNKNOWN_QUEUE=skip`（默认）/`assume-default`（仅默认队列生效，WARNING 含 instance_id 列表，非法值 WARNING 后回 skip）；plan/dispatch 响应与台账 `build_ledger_entry` 新增 `queue_source ∈ {declared, assumed-default}`；console `/metrics` 导出 `plaita_dispatch_unknown_queue_total{queue}`；worker 注册 metadata 增 `queue_contract_version=2`。④r54 固化的「缺 metadata=default」用例按新语义改写 | 语义负控（临时还原 `candidate_queue_name` 返回默认）：unit **13F/3P**、console **12F/1P**，覆盖 5 组（缺失/空白/非字符串、assume-default 三面、skip 广播+legacy 可消费、known mismatch 回归、plan/dispatch 同判据）；新增 unit 16 条 + console 13 条；定向 unit 135P、console 57P；`tests/unit` 全量 **8588P/12S/3498 subtests（rc=0，729.83s）**；console **1020P/13S/1F**（唯一失败为本地 2026-10-18 构建的 gitignored `frontend/dist` 陈旧产物，非本轮）；`mkdocs --strict` rc=0；ruff 无新增；`landing_gate.py ci` **rc=0**（main `40f7615` success）；代码 `1443e75` 已推 origin/main，main CI run `39486582420`（head `1443e75`）四 job **success** |
| 第四十八轮 | 台账 A69-02/03 复核（A78-01/04/05、B78-01）+ A2R24-02/05/06/07/08/09（无 GitHub issue；任务书标称第四十七轮，按台账序列顺延） | **A69 修后复核四残面**：①A78-01——类型桶 TTL 改 Lua `max(当前,目标)` 只延长不缩短，`store_event_sync` 增 `index_ttl`（回填传默认 7d），`store_event`/`batch_store_events` 同源；②A78-05——读侧/janitor/写侧索引清理改单条 Lua（EXISTS+ZREM/SREM 原子），event/subscription 双面；③A78-04——janitor interval `nan/inf` 拒绝回退 60s + WARNING；④B78-01——桶轮转按实际进入桶数前进。**A2R24 契约族**：②02 queue stream `start` 索引语义 + `start_id` 游标 + `truncated`；⑤05 logs/events `total` 真匹配数 + `returned`/`next_cursor` 键集游标 + `limit ge=1`/`order` Literal；⑥06 `flow_name` 复核；⑦07 tenant `member_count`、单版本/晋升包实体 `tenant_id`、平台同名 flow 409；⑧08 trigger `{success,mode,execution_id,msg_id}`；⑨09 SSE OpenAPI 仅 `text/event-stream` + ticket schema | A78 新测 **7P**，clean archive pre-fix **7F**；A69 相关套件 85P + console canonical 10P；console 新增 16 条 post **16P** / pre-fix **15F/1P**；定向 console 98P、审计面 61P；`tests/unit` **8559P/12S/3498 subtests（rc=0，718.93s）**；console **1000P/13S/1F**（唯一失败为本地陈旧 `frontend/dist`，环境项）；layering 3P；`mkdocs --strict` rc=0；`tsc --noEmit` rc=0；ruff 无新增；`landing_gate.py ci` **rc=0**（main `b22cff7` success）；代码 `e417316` + `0f11be2` + 台账 `5c8852e` 已推 origin/main，main CI run `39478700236`（head `5c8852e`）四 job **success** |
| 第四十七轮 | 台账 A2R31 B2R31-01/02/03（P2×3；无 GitHub issue；任务书标称第四十六轮，按台账序列顺延） | **配置代际族施工**：①B2R31-01——新增 `plaita/runtime_config.py` 三维 canonical 哈希（`cf1-` family=queue/group/dlq/schema/namespace、`cfg1-` generation=+epoch/预算/安全开关、`bg1-` budget=max_deliveries/lease/age/dlq_len）；信封四字段，console/schedule/event_filter 写当前代际，worker 五条重投/交接/克隆与 DLQ replay 继承源值（缺失 legacy）；首投/reclaim 同判（family 不匹配不执行、`XRANGE` 预读不 `XCLAIM`；budget 不同不作用旧 PEL），超 `task_max_age` 的拒收进 DLQ + `config_rejected` 防克隆热环；group 改名 `$`/queue 改名孤儿预检；registry metadata/容量块/`/api/cluster/{config,instances}`/dispatch family 过滤/`queue_config_*` 指标；租户状态纪元锚定 `PLAITA_DEPLOYMENT_EPOCH`（非 default 缺/旧 epoch 拒绝、default #237 fail-open），console 周期对账 + reconcile 端点，`EPOCH_STRICT=0` 回滚阀。②B2R31-02——queue/redis/dlq/instance id 单一 resolver；空串 identity 启动预检拒绝；console drift 预检 + registry DLQ 进删除清理。③B2R31-03——identity/security/resource 聚合 fail-fast，optional WARNING+安全默认；strict bool 含 `on`、`COOKIES=TRUE`、`ALLOW_INSECURE_ADMIN=1`；sandbox 负值/cpus 非法 import fail-fast；Langfuse 未知=false；0/负紧环与空键拒绝或 WARNING；`DISABLE_*` 保留精确 `1` + WARNING。 | 负控（clean archive `6a6f8df`）：old PEL 4 投 + 新 max 3 pre-fix `dlq_len=1/pending=0` → fix `dlq_len=0/pending=1` + `config.budget_stale`；空 queue env 三路径分裂 → 统一 `ConfigError`；仅 `REDIS_URL` event_filter localhost → 全入口 real-host；Redis 恢复 active/`fail_open=0`/无 epoch → epoch 闸拒绝 + `stale=1`；六形态 `on→False`/`bogus→True`/`-1` 接受/`inf` 接受/紧环/空键 → True/False/import ConfigError/30s+WARN/default+WARN/拒绝。新增 engine 39 + console 10；定向 **401P/6S**、合并后抽核 **109P**、console 定向 **71P**；`tests/unit` 全量 **8552P/12S/3498 subtests（rc=0，717.08s）**；console **984P/13S/1F**（唯一失败为本地陈旧 `frontend/dist` 的 issue266 扫描，环境项）；layering 19P；`mkdocs --strict` rc=0；ruff 新文件全绿、改动无新增；`landing_gate.py ci` **rc=0**（main `9fa0afd` success）；代码 `ca60069` + merge `98d2d87` + 台账 `60fa05c` 已推 origin/main，main CI `60fa05c` **success** |
| 第四十五轮 | 台账 r70 A69-02/03/04（核验轮；无 GitHub issue） | **A69 TTL 族逐条核验 + 两残面修复**：①A69-02（B2 r51）9 负控 + env=10P——正典 `events:{id}`/`types:{type}` 7d、单一 `Event.timestamp`、base service 复用 `store_event_sync`、GET 两形去重 canonical 优先、legacy 只读自然过期、回填脚本默认 dry-run/按剩余 TTL/不删旧键/已存在跳过/幂等逐项核对；②A69-03（B2 r51）10 负控 + no-KEYS/EventFilter 挂载 + 写侧 4 条=16P——`list_events` 按桶 ZREM、`list_subscriptions` MGET+SREM、janitor SCAN+ZSCAN/SSCAN 进程内游标/预算 500/60s/只按 EXISTS 不按 score/坏桶 WARNING 不终止/关闭启动 WARNING、console delete 补 `node:`；③A69-04（本席 r42）12 负控 + 2 对照 + fenced=16P，r52 10P——独立 30d、`max(30d, STATE_TTL)` 下限、`<=0` 非法回退 + WARNING、clamp、`SET NX`+只延长、载体双源 max/缺失畸形 fail-closed/`>=`、`enqueue_task` 仅缺失注入、DLQ replay 用 `dead_lettered_at`/DLQ Stream ID、FlowWorker+console lifespan 启动校验、`[policy]` 带 Policy-ack。**残面**：回填脚本 `TTL=0` 走 `EX 0` 会中断整批（改按已过期跳过）；重投副本对畸形/超前 payload timestamp 原样保留会洗白源 Stream ID 年龄（改取两源更早者）。 | 新增 5 条（`test_a69_02_event_backfill.py` 3 + A69-04 畸形/超前参数化 2），pre-fix **3F/2P**，post **5P**；A69 定向 **55P**；`tests/unit` 全量 **8498P/12S/3498 subtests（rc=0，743.35s）**；console **961P/13S/1F**（唯一失败为本地陈旧 `frontend/dist` 的 issue266 产物扫描，环境项）；`mkdocs --strict` rc=0；ruff 改动行无新增（既有 11 条 BLE001）；`landing_gate.py ci` **rc=0**（main `60e6773` CI success）；代码 commit `a0b91a7` |
| 第四十三轮 | 台账 r70 A69-02/03（P2；无 GitHub issue；实现已由 B2 r51 落地） | **复核 + 写侧兜底补测**：PR2/PR3 已于开工前合入 main（`e374aa1`/merge `1c09480`），本轮不重复实现。spec 9+10 条负控对应 22 条测试全绿；逐项核对正典键形/TTL/单一 timestamp/legacy 只读+GET 去重/base_service 复用/回填脚本/`list_events` ZREM/`list_subscriptions` MGET+SREM/`IndexJanitor` 游标预算与 SCAN 族/console delete `node:`/文档 env。补 §3.3-3 写侧「每 type 60s、至多 8 成员、offset 轮转」4 条负控（此前仅有实现无测试）。 | 新测 pre-fix **3F/1P**（clean archive 移除调用；disable 对照绿），post **4P**；规格负控 22P；验收套件 80P；墓碑/tenant purge 27P；`tests/unit` **8484P/12S/3498 subtests**（rc=0）；console **946P/13S/1F**（本地 `frontend/dist` 旧产物，环境项）；`landing_gate.py ci` **rc=0**；ruff 绿；测试提交 `45ed3e5` |
| 第四十一轮 | 台账 A2R24-01 / A2R24-03 / A2R24-04（r24；无 issue 号） | **契约族 console 侧对齐（按价值序）**：①A2R24-01——新增 `plaita-console/backend/error_contract.py`：`detail` 统一 `{code, message, ...历史业务键}`，`install_error_handlers` 注册 HTTPException（保留 Retry-After 等 headers）/ RequestValidationError（自动 422 → `validation_error` + `errors`）/ 未捕获 Exception（JSON 500 `internal_error`，原文只进 `logger.error`）；错误体在 request_id 中间件存在时带顶层 `request_id` 与 `X-Request-ID`。三种历史 dict（`message+conflict+latest_updated_at`、`message+error/reason`、`error+tenant_id+max_running`）在源站改 `error_detail` 并在 handler 幂等归一，业务键保留；47 处异常文本出网（5 文件 40 处 `str(e)` + 7 处 f-string）改结构化/`scrubbed_detail`，cluster Redis URL 经 `redact_url_userinfo` 只留 `scheme://user@host:port`、口令 `***`。②A2R24-04——`api/services.py` 抽 `get_redis_or_503`，`get_redis` 依赖统一过闸；`types/list`、`{id}`、`{id}/status` 与 list/topology 同为 503 `application/json`（此前 NoneType 500 text/plain），stop 一并覆盖。③A2R24-03——`LogStatsResponse` 增 `estimated`/`sample_size`：平台视角在样本未覆盖整条 Stream 时标 `estimated=true` 并记 WARNING，tenant 视角 `estimated=false`/`sample_size=null` 保持精确；`truncated` 仍只表示预算/键扫描触界。同步更新受影响的既有断言（detail 统一对象后改用 `detail["message"]`/保留业务键） | 新增 `tests/console/test_issue_a2r24_error_contract.py` 5 条、`test_issue_a2r24_services_stats.py` 7 条（3 类端点统一形状、本地档 5 个 GET 全 503 JSON、平台/租户 stats 标注对比）；**负控**（临时回退 handler 挂载 / `get_redis_or_503` / `estimated` 回写）：错误形状 **4F**（string 404、手工/自动 422、未捕获 500、create_app 挂载）、services 模块全部 GET **5F**、stats 平台标注 **1F**，恢复后 **12P**。定向 12P；console 全量 **926 passed / 13 skipped**（唯一失败 `test_issue266_built_bundle_has_no_admin_key_material` 为本地 gitignored `frontend/dist` 旧产物，环境项；CI fresh build 检查）；`landing_gate.py ci` **rc=0**（main `3cbf8f4` CI success）；ruff 改动文件无新增（仓库门禁只查 E722/BLE001）；代码 commit `1142288` 已推 origin/main，推送后 CI run `39348725204` 四 job **success** |
| 第四十轮 | 台账 A2R8-04 / A2R8-05 / A56-02（r8/r56；无 issue 号） | **性能上界 + 分裂队列两格**：①A2R8-04（plaita-nodes `database.py`）——固定 `fetchmany(256)` 在判限前先物化整批；300×128 KiB、`max_bytes=1024` 峰值 32.335 MiB（33,111× 预算）。改首行单读校准、批大小按剩余字节预算/已观测最大行宽收缩（至少 1 行用于“恰好等于上限后还有更多”判定），上限语义与 `observed_*` 不变；峰值 0.433 MiB。②A2R8-05（`agent_run.py`）——直跑/沙箱两路径 `protocol_lines.append` 无界（20k×1 KiB 常驻 21.42 MiB）；新增 `_ProtocolLineBuffer`（10,000 行 / 4 MiB 双闸，超限停止驻留 + 非 JSON 标记 + WARNING），峰值 4.19 MiB。③A56-02 查漏——console `api/schedules.py` 已在 main 走 `_task_queue_name()`（B3 r54）；残留为服务端 `ScheduleService.__init__` 固化 `PLAITA_QUEUE_NAME`、忽略 `PLAITA_CONSOLE_TASK_QUEUE`。新增 `resolve_dispatch_queue_name()` 每次派发解析（显式 config > console env > worker env > `QUEUE_NAME` > 默认），元数据刷新；两侧不同源 WARNING，并按 #151 在 `_EXEMPT_ENV_NAMES` 登记键名豁免。同步 `ops-runbook.md` 双 env 口径 | 负控：A2R8-04 回退生产文件 → **2F/1P**（峰值 33.9 MB，上限内对照绿）；A2R8-05/A56-02 回退 → 收集期 ImportError，行为面 pre/post 探针精确（21.42→4.19 MiB；`plaita:flow:queue`→`:v2`）。新增 9+8 条测试；nodes 定向 6 文件 **73P**、全量 **383P/2S**；plaita 定向 3 文件 **33P**、`tests/unit` 全量 **8426P/12S/3498 subtests（rc=0，736.40s）**；`mkdocs build --strict` **rc=0**；ruff 改动行无新增；`landing_gate.py ci` **rc=0**（main `d64ff67` CI success）。证据 `docs/proofs/duo40-a2r8-a56-02-bounds.md` + `data/duo40-*`；代码 commit：plaita-nodes `5d2bf7f`、plaita `19201c6` |
| 第三十九轮 | 台账 A2R20-05 / A2R20-06 / A2R20-07（r55；无 issue 号） | **延迟族上界三格**：①A2R20-05——`plaita/redis_client.py` 成为唯一构造入口：connect/read 显式（console 2s/5s、worker 5s/30s、健康 3s/3s；`PLAITA_REDIS_CONNECT_TIMEOUT`/`PLAITA_REDIS_SOCKET_TIMEOUT` 覆盖，URL query 优先），覆盖 console `connect_redis`/nonce/SSE/cluster/服务健康、worker 队列+租约、`server/factory` 存储侧默认、services 启动器、5 个运维脚本；AST 测试禁裸 `from_url`。②A2R20-06——回填每 200 版本一个 pipeline（SET+SADD×2），SQL 侧 `FlowStore.iter_published_versions` 键集分页 1000/页；墙钟 5s + 版本上限 5 万，触界/错误 WARNING 且不阻断启动；`/health.engine_sync` 暴露 `ready/degraded` 与计数。③A2R20-07——非 SQLite `create_engine` 注入 `connect_timeout=5`/`pool_timeout=5`/`pool_pre_ping=True` + PG `lock_timeout=5s`；连接类异常 fail-fast（不回退 create_all），启动日志 URL 脱敏；SQLite 零变化。同步 README/console guide/ops-runbook | 新增 `tests/unit/test_a2r20_redis_timeouts.py` 8 条、`tests/console/test_issue_a2r20_startup_budgets.py` 12 条、nonce 超时断言 1 条；既有 reap/nonce 断言同步。**负控**：裸 `from_url` 静态红；回填回退逐版本写 → 往返 10→6,000 红；去 deadline/max_versions → 无法触界红；PG 去 connect timeout → 黑洞 >8s 红。量化：Redis 黑洞 5.0.8 下 console/worker/nonce **>8s→5.0s**（8.1 同 5.0s）；回填 2,000 版本 **1.46s/6,000 往返→0.05s/10 往返**，30k **0.61s/150 往返**（旧外推 90k/23s）；PG 黑洞 **>8s→5.15s OperationalError**。定向 6 文件 **56 passed**；console 全量 **888 passed/13 skipped**（唯一失败 `test_issue266_built_bundle_has_no_admin_key_material` 为本地 gitignored `frontend/dist` 旧产物，环境项）；`tests/unit` **8413 passed/12 skipped/3498 subtests（rc=0）**；`mkdocs --strict` rc=0；ruff 改动文件全绿（既有 BLE001 除外）；`landing_gate.py ci` **rc=0**；代码 commit `0fb3e13` |
| 第三十八轮 | 台账 A55-S2 / A55-S3 / A55-S4（r55；无 issue 号） | **A55 审计余量（DLQ/queue 面）**：①S2——`auth.ensure_platform_admin`（DLQ replay、cluster 生命周期、服务停止）与 `tenants._audit_denied`（租户成员闸）由 `audit.record` 改 `record_denied`：`(action, actor, ip, tenant)` 60s 窗口合并，下一行带 `detail.suppressed`，WARNING 每次保留；未认证/低权流量不再按请求数撑大审计表。console conftest 增 autouse `reset_denied_throttle()`，避免进程级窗口跨用例串味（每例独立 DB 时会造成「有审计行」假红）。②S3——`POST /api/queues/dlq/{dlq_id}/replay` 审计行改为显式传**死信条目生效租户**（不再取 `request.state`，API-key 无 header 时 `""`），`detail.target_tenant` 同值回显；跨租户 `X-Tenant-ID` 以条目为准；API-key 行带 `auth_source`/`key_fingerprint`（sha256 前 16 位）/`request_id`/`role_snapshot`。补 404/422 尝试审计（`dlq_not_found`/`target_not_allowed`），补齐 A53-S2「每次尝试」契约。③S4——`services/audit.list_audit` 增 `AUDIT_LIMIT_DEFAULT=200`/`AUDIT_LIMIT_MAX=1000` 并在服务层 `max(1, min(...))` 钳制（SQLite `LIMIT -1` 不再物化全表）；API `Query(ge=1)` 仍 422；`audit.read` 记录过滤条件/生效上限/行数，租户 admin 只读当前世代本租户行。同步 `ops-runbook.md`（console 审计口径）与 `docs/proofs/s6-global-observability-policy.md` | 新增 `tests/console/test_r55_audit_queue_surface.py` 10 条（S2 平台/租户成员闸限频、401/失败登录、S3 归属/指纹/4xx、S4 服务层钳制/API 下限/读审计/世代隔离）；**负控 5 组精确红**：`ensure_platform_admin`+`tenants._audit_denied` 回退 `record` → 3 用例红（5/3 行）；replay 去显式 tenant/target_tenant → 2 用例红（`tenant=""`）；4xx 去审计 → 1 用例红；服务层去钳 → 1 用例红（1205 行）；API 去 `ge=1` → 1 用例红（-1 变 200）；恢复后 10 passed。定向 11 文件 **127 passed**；console 全量 **842 passed/13 skipped**（唯一失败 `test_issue266_built_bundle_has_no_admin_key_material` 为本地 `frontend/dist` 旧产物，环境项；CI Console frontend contracts success）；`tests/unit` 全量 **8389 passed/12 skipped/3498 subtests（rc=0）**；`mkdocs build --strict` rc=0；ruff 改动文件全绿。代码 commit `a6992bb` 已推 origin/main，CI run `39264934264` 四 job **success**，`landing_gate.py ci` **rc=0** |
| 第三十七轮 | 台账 S18-01 / S18-03 / S18-04（r55 A2R18；无 issue 号） | **drain 语义与交接顺序**：①S18-01——`_consume_loop` 在 `queue.read()` 返回后、`_bump_active(+1)` 前检查 `_drain_event`，命中即 WARNING `drain-read-gate` 并退出（不 dispatch/不 ack/不 bump，消息留 PEL 由 XCLAIM 回收；同时覆盖 `_reclaim_one` 孤儿）。`_force_stop_after_drain` 删除 `active<=0` 早退，无论在途数都置 `_running=False`；先置 `_force_stop_event`（D2），再对 `_cancel_watch` 登记表快照、按登记租户在锁外 `execution.cancel()`（失败 WARNING，不阻断退出）。`_requeue_retryable_failure` 的退避改 `_interruptible_backoff`（0.2s 分片、片间检查 D2；attempt 梯度不变），D2 中断时返回 False、不 enqueue/ack；`stop(wait=True)`/`run(wait_for_inflight=True)` 在停看门狗/注销前有界等待。②S18-03——`_handover_non_affine` 返回 bool，先 `queue.enqueue(copy_body, **信封)` 再 `queue.ack(mid)`；enqueue 失败不 ack（原条留 PEL/Stream），ack 失败返回 False（新副本 + 原 pending；租约/终态去重）；A46-02 剥 target 回退共用同一路径；`_consume_loop` 用返回值回写 `acked`。③S18-04——`test_a2r18_drain_handover.py` 22 条：决议载体 × concurrency 1/2 × 回滚开关参数化（gate-on 留 pending 且释放租约后可 XAUTOCLAIM；rollback 复现 ack），以及 gate/强停 cancel/退避中断/交接三态。同步 `flow-worker.md`（新增 drain 状态机与 ack 策略）、`ops-runbook.md`（advisory 超时、post-read gate、grace 公式、停止等待/未完成不 ack 的边界）、`DOC_CODE_MAP`。**与 S18-02A 兼容**：保留 registry 状态机/closed 闸/即时落盘与 A46-02 目标不可用回退；交接统一 enqueue→ack | 新增 `tests/unit/test_a2r18_drain_handover.py` 22 条；既有 `test_worker_affinity/test_worker_drain/test_worker_node_retry/test_schema_version_boundary` 断言同步（enqueue→ack 顺序、active=0 也停、分片退避）。**负控**：A45-01 去超时 cancel / A45-02 删 post-read gate / A45-02 恢复 `active<=0` 早退 / A46-06 恢复整体 `_sleep` / A45-05 恢复 ack→enqueue 各 **1F 精确红**；S18-04 删 `_DECISION_RESUME_TYPES` 分支 **6F/6P**（gate-on × event/timeout/cancel × c1/c2 全红，rollback 对照组全绿）。定向 8 文件 **206 passed**；rebase 后 `tests/unit` 全量 **8389 passed/12 skipped/3498 subtests（rc=0）**；`mkdocs build --strict` rc=0；ruff 仅既有 11 条 BLE001（改动行无新增）；`landing_gate.py ci` **rc=0**（main `2b3b595` CI success 后放行；代码 commit `c34251e` 的 CI run `39248405831` 四 job **success**） |
| 第三十六轮 | [#264](https://github.com/jeffkit/plaita/issues/264)（P1）；台账 A53-01 / A53-S2 / A53-S3 / A53-11 | **DLQ 运维面收口**：①#264——删除清理矩阵只覆盖租户命名空间/平台键/任务 Stream，DLQ（`<queue>:dlq`，平台级）跨世代存活；同名重建后新租户 viewer 按 `payload.tenant_id` 重新可见并回灌旧 payload。修法：`purge_tenant_dlq` 读**信封内** `envelope.payload.tenant_id` 收窄 `XDEL`（DLQ 顶层无 `tenant_id`，旧 `_task_tenant_id` 恒 None），畸形/缺字段跳过并 WARNING；`_tenant_dlq_names` 汇总 `PLAITA_DLQ_KEY`、各任务队列 `:dlq` 与 `*:dlq`，非 Stream 键跳过；`_purge_runtime_after_commit` 增 `dlq` 步（三步各自尝试、失败 loud 上抛，404 补清扫同路径）。读面纵深：`_dlq_entry_visible` 要求显式 `tenant_id == scope` **且** `dead_lettered_at >= tenant.created_at`（当前世代；`request.state.tenant_generation` 优先、DB 回退、取不到 fail-closed）；缺字段/畸形不再回退 default，只对平台可见；`DlqEntry` 透出 `tenant_id`。②A53-S2——新增 `plaita/server/dlq_replay.py`：`(dlq_key, dlq_id)` `SET NX EX 7d` 一次性标记（二次 `already_replayed`），`schema_version` 继承源信封（缺失=LEGACY 1；超限入队前 `schema_too_new`），无 `execution_id` 的 start 铸造确定性 `uuid5` + `dedup_key`，执行墓碑→`execution_deleted`，缺/冲突租户→具名拒绝，每次尝试 `XADD plaita:dlq:replay:audit`、DLQ 条目保留；console 增 `POST /api/queues/dlq/{dlq_id}/replay`（`ensure_platform_admin` + DB 审计 + `DlqReplayRequest`）；`api/queues.py` 回显 `schema_version` 保持基线。③A53-S3——`dead_letter` 捕获 XTRIM 返回值：removed>0 → `dlq_trimmed` + 限频 WARNING；异常 → `dlq_trim_failures` + WARNING（基线 DEBUG）；`dead_lettered`/`logger.error`/alert 移至 XADD 后、ack 前；ack 失败计 `dlq_ack_failures` 并 WARNING `source_ack=failed`；alert 增 `dlq_id/dlq_length/dlq_max_len/trimmed_count/schema_version`；stats/metrics 增 `dlq_max_len`/`dlq_saturated`/`queue_dlq_trimmed_total`/`queue_dlq_trim_failures_total`/`queue_dlq_ack_failures_total`。④A53-11 查漏——DLQ 解析对 msg_id/字段键/last_id `errors="replace"`、整体 try/except 降级为 `parse_error` 单条、`raw_fields` 键规范化；`get_queue` Stream/List 对合法 JSON 非 object 不再 `**` 展开 500；binary Redis 客户端的 `type()` 判定统一 `_text`；`connect_redis` 增 `encoding_errors="replace"`。**顺修 main 红**：A48 夹具在 A54-06 orphan-flow 拒绝门后仍只删租户行（CI `f6b1b2b` failure），改为先清 `FlowRecord` 再走 404 补清扫，未走 override。同步 `flow-worker.md`/`ops-runbook.md`/`255` proof/`DOC_CODE_MAP` | 新文件 `tests/unit/test_r53_dlq_replay.py` 19 条、`test_r53_dlq_trim.py` 5 条、`test_r53_dlq_purge.py` 5 条、console `test_issue264_dlq_tenant_generation.py` 6 条、`test_r53_dlq_replay_api.py` 9 条、`test_r53_dlq_view_tolerance.py` 7 条；**负控**：#264 回退 queues/tenants_svc/task_queue → **4F/2P**（同名重建 viewer 读到 2 条前代条目；delete 不清 DLQ；loud 路径 AttributeError），post 6P；replay 回退标记 NX+确定性 id（uuid4）→ **4F/15P**，baseline 裸 `enqueue_task` 双投探针 **2 条 completed 执行**（两个随机 id），post 19P；trim 回退 task_queue/metrics/alerts → **5F**，post 5P；容错回退 queues/main → **6F/1P**，post 7P；a48 修复后 9P。定向 29+42；`tests/unit` 全量 **8322 passed/12 skipped/3498 subtests（rc=0）**；console 全量 **790 passed/13 skipped**（唯一失败 `test_issue266_built_bundle_has_no_admin_key_material` 为本地 `frontend/dist` 旧产物，环境项；CI Console frontend contracts success）；`mkdocs build --strict` rc=0；layering 3P；ruff 改动行无新增 BLE001（其余为既有）。main CI run `39236316390`（head `89dabe0`）四 job **success**，`landing_gate.py ci` **绿**；代码 commit `89dabe0` 已推送 origin/main |
| 第三十五轮 | 台账 A51-01 / A51-02（r52；无 issue 号） | **世代绑定批（①→②）**：①A51-01——删除持久性只覆盖 state/index/fence/lease（本轮 rebase 后含 A51-03 计数键），start 载体在删除后由 `start_flow`「盘上无状态=全新执行」重建；四路径（队列 start / 崩溃未 ack 的 XCLAIM 回收 / 节点失败 `_requeue_body_for_retry` / 死信守卫克隆）全部实测旧 `$INPUT` 重执行。修法：`RedisExecutionStorage.delete_execution_state` 在同一 MULTI/EXEC 写 `plaita:exec_tombstone:{id}`（值含 tenant/deleted_at/reason，TTL 默认跟随 `PLAITA_EXECUTION_STATE_TTL_DAYS`）；`FencedExecutionStorage` 同段 Lua 在 CAS 成功后删计数键并写墓碑（墓碑键排最后，`fencegen` 仍不删）。worker 五点复核：`_dispatch_task` 在租户状态闸后、配额闸前丢弃并释放同 id dedup；`start_flow` 预铸 id 命中即返回 discarded；`_handle_start_dedup_hit` 区分同 id 重放（拒绝+释放映射）与新 id 复用 dedup_key（放行新启动）；`_requeue_body_for_retry` 返回 None 哨兵、`_requeue_retryable_failure` ack 原条不入队；`_dead_letter_guard` 在租约探测后只放行 DLQ、不克隆/不终态化。新增 `PLAITA_TASK_MAX_AGE_SECONDS`（默认 30 天、0 关闭）兜住墓碑 TTL 到期后的历史载体；墓碑平台根键不被 `_purge_tenant_redis` 擦除。②A51-02——r38 世代令牌在服务首次读才捕获，旧租户请求跨 delete+create 后把重建租户的世代当自己的。修法：`resolve_session` 用一条 outer JOIN 同时取成员资格/角色/`Tenant.created_at+contract_secret_id`；`require_auth` 写 `request.state.tenant_generation` 并对平台 `X-Tenant-ID` 读一次租户行（缺失 403；未 bootstrap 的内嵌/测试挂载显式 WARNING 跳过）；`tenant_generation_context` async 依赖绑定 ContextVar；`tenant_write_guard.assert_tenant_generation` 在写/flush 后同一事务复核，覆盖 `delete_credential`、`set_member_role`/`remove_member`、`ensure_flow`/`create_flow`/`save_flow_definition`/`delete_flow`/`publish_version`/`delete_version`、节点描述/属性类型 upsert/delete、`insert_local_execution`、本地调度 create/update/delete；`save_credential`/`add_member` 首次捕获改「请求 token 优先」（已护栏路径 0 新增读）。阶段 2（队列信封租户世代）按规格保留 | 新文件 `tests/unit/test_r52_execution_tombstone.py` 10 条、console `test_r52_tenant_generation_binding.py` 7 条；**负控 A51-01 pre-fix 8F/2P**（四条路径均 `completed`+旧 `$INPUT`，仅新 id 对照与一条无关断言绿），post **10P**；**负控 A51-02**：baseline archive 探针 `in-flight add_member outcome: ok` / `save_credential ok`（bob 成重建租户 admin、凭据植入）→ 绑定旧世代后 `UserError: 租户不存在` / `ValueError ... 拒绝写入凭据`、成员 `[]`、凭据空、bob `role=None`；HTTP 用例证明 async 依赖的 ContextVar 传到 sync handler（在途 flow save 409、零行）。定向 17P；rebase 后 `tests/unit` **8285 passed/12 skipped/3498 subtests（rc=0）**、console **716 passed/13 skipped（rc=0）**；`ci-gate.sh` **PASSED**（`tests/` 8565 passed/16 skipped、coverage **93.77%**、layering 3P/wheel/size 绿）；`mkdocs build --strict` rc=0；ruff 仅既有 BLE001（新增 except 均已 `# noqa: BLE001`）；`landing_gate.py ci` **绿**（main `fc03041` CI success，run `39220890511`）；代码 commit `fc03041` 已推送 origin/main |
| 第三十四轮 | 台账 A49-01 / A49-02 / A50-03（r49/r50；无 issue 号） | **版本边界收尾三格**：①A49-01——`_read_once`/`_reclaim_one` 的超版本拒收走 `dead_letter_if_allowed` 后会进入 FlowWorker 守卫的「非终态 + 租约空 → 重入队恢复副本」分支；副本经 A44-02 继承同一 v2，本 build 下轮再拒收再克隆，形成流内恒 1 条、DLQ 每轮 +1 的自持热环（回滚/生产者先行/伪造信封）。修法：`StreamTask` 增 `schema_rejected` 标记，`_reject_unsupported_schema` 用 `dataclasses.replace` 打标；`_dead_letter_guard` 在租约检查之后、节点重试兜底之前对该标记直接放行死信（不克隆、不终态化、不消耗重试预算）。语义选择「DLQ 留证一次」：生产者先行时载体不再自动留在 pending，而是进 DLQ 等升级后重放；suspended 执行状态原样保留，仍可被 event/cancel/timeout 唤醒；活租约仍拒绝死信留 pending。②A49-02——console `DlqEntry.payload` 定型 `Dict`，而 `_dead_letter_invalid` 对截断原始串写入字符串 payload；`_parse_dlq_entry` 在 `_owned_by_tenant` 之前构造模型抛 ValidationError，一条毒丸让全部 DLQ 流/全部租户 500。修法：非 dict payload 统一包 `{"raw": ...}`（整信封不可解析/JSON 非对象同款），`delivery_count`/`dead_lettered_at`/`schema_version` 容错转换，`raw_fields`/`truncated` 仅接受合法形状；新增三字段透出。③A50-03——core 的 node-F2 把 `NodeException(1001)` 与非校验 `ValueError` 判不重试，但 distributed 把异常包成 `NodeExecutionError`；worker 的 `_is_retryable_node_failure` 命中包装层即返回 True，同一确定性错误仍执行 5 次。修法：`core.runner.is_retryable_error` 公开（保留 `_is_retryable_error` 别名），worker 在 `NodeExecutionError` 之后继续沿 `__cause__` 复核确定性分类；#219 输出校验标记仍可重试；深层超时/取消仍优先。同步 `flow-worker.md` / `ops-runbook.md` | 新文件 `test_r49_schema_reject_recovery_loop.py` 5 条、`test_r50_f2_worker_retry_parity.py` 6 条、console `test_r49_dlq_parse_tolerance.py` 5 条；**负控**：去掉 `schema_rejected` 标记 → **2F/3P**（suspended/running 两格：`xlen(dlq)=6≠1`、`xlen(stream)=1` 克隆仍在）；还原 payload 直传 → **1F**（`ValidationError: payload Input should be a valid dictionary`）；还原 worker 包装层早退 → **3F/3P**（1001 链、ValueError 链、深层超时不重试三格红，1002/#219/无 cause 三格绿）。修复后定向 13 文件 **169 passed**；rebase 后核心 5 文件 **63 passed**；console 5 文件 **55 passed**；`tests/unit` 全量 **8265 passed / 12 skipped / 3498 subtests passed（rc=0）**；`plaita-console/backend/tests` 全量 **686 passed / 13 skipped（rc=0）**；`ci-gate.sh` layering **3 passed** / wheel / size 绿；`mkdocs build --strict` **rc=0**；ruff 仅既有 BLE001（改动行无新增）；`landing_gate.py ci` **绿**（main `5ccaf6a` CI success，run `39209673516`；本 commit 的 run 被并发推送取消，但 `5ccaf6a` 含本 commit 且回归门 success）；代码 commit `a2fbb19` 已推送 origin/main |
| 第三十三轮 | 台账 A46-04 / A47-F2（r46/r47；无 issue 号） | **决议载体租约冲突不再 ack**：suspend 提交窗口 `[订阅注册, 租约释放)` 内到达的 `event`/`timeout`/`cancel` 不是重复载体，而是 suspended 的唯一决议来源（#33）。旧 `_lease_conflict_ack_safe` 只查持有者注册表在册/非 draining，持有者活着就 ack 掉它们；持有者随后落盘 suspended 并释放租约 ⇒ 零载体、执行永久 suspended（默认 event 节点自动恢复网为 0，A47-02）。修法（A47 F2 配方）：`type=resume` 且 `resume_type∈{event,timeout,cancel}` **一律返回 False**（信号缺失分支——旧格式租约 / `--no-registry` / Redis 瞬断——也不退化 ack），消息留 pending；租约释放后由 reclaim 推进，租约长期持有时 #242 死信守卫拒绝死信并在释放后重入队副本（F2 探针 `pending=1/dlq=0` → 释放后 `delivery=1` → `completed`）。`continue`/`retry`/`start` 保持原存活判据 ack（#33/#50/#23 不变）；#258 的 `ResumeProtocolError` 状态感知分支是协议错配窗口，不受影响；回滚开关 `PLAITA_DISABLE_HOLDER_LIVENESS=1` 仍整体旁路（含本收紧）。同步 `flow-worker.md`（回收判据 / #33 / #53 丢弃可见性）与 `ops-runbook.md`。**另修 main 红**：`9411e2c` 引入 `_dispatch_task(schema_version=...)` 后 `test_worker_disk_guard` 假桩未跟上（`TypeError` 致任务重投烧到 DLQ，2 个消费循环用例红）——本 commit 补形参 | 新用例 10 条（`test_worker_lease_conflict_liveness.py` 31 passed）；**负控 pre-fix 7 failed / 24 passed**（event/timeout/cancel × 活持有者 3、× 无信号 3、pending→释放后回收 1 精确红），`continue`/`retry`/`start` 对照 3 条 pre-fix 即绿；修复后 31 passed。定向+相关 32 文件 **652 passed**；`tests/` 全量 **8541 passed / 16 skipped（rc=0）**；`ci-gate.sh` layering/wheel/size 绿；`mkdocs --strict` rc=0；ruff 改动行无新增（仅既有 BLE001）。**落地**：代码 commit `3090b47` 已推送 origin/main；推送前 `landing_gate.py ci` 为红（基线 `c0bffe0` CI failure = 上述 disk_guard 测试桩，本地复现 2F），本 commit 一并修绿，未走 `--override`；新 run `39201157306` **success**，落地后 `landing_gate.py ci` **绿**（main `3090b47`） |
| 第三十二轮 | 台账 A42-05 / A44-01 / A44-02（r42/r44；无 issue 号） | **版本边界三格同族修复**：①A42-05——`_read_once` 超 `max_schema_version` 的死信改经 `_reject_unsupported_schema` → `dead_letter_if_allowed`（守卫拒绝/异常时留 pending、计 `dlq_guard_skipped`），与超投递/兜底异常同一入口；②A44-01——`_reclaim_one` 在 XCLAIM 后、返回消费循环前做与首投相同的信封版本校验，超版本走同一守卫死信路径（不回给旧语义执行），检查置于超投递判据之前；③A44-02——`enqueue_task`/`RedisStreamTaskQueue.enqueue` 新增显式 `schema_version`，worker 的 6 条重投/交接/归一化路径（start 幂等命中 resume、节点可重试失败、亲和交接、死信守卫恢复、超配额延迟、#258 归一化）经 `_requeue_envelope_kwargs` 继承源信封版本；`_dispatch_task`/`start_flow`/`_handle_start_dedup_hit` 透传。**语义选择**：重投副本承载源载体语义，故采用「继承源版本」而非「超版本副本进死信」——混跑池旧实例仍能接管唯一恢复载体；运维边界=worker 自身灰度不再要求整池原子升级，但 BFF/调度/event_filter 新载体仍须「先升消费者、再升生产者」。另：DLQ 信封固定带 `schema_version`（畸形消息另带 `raw_fields`/`truncated`），`_dead_letter_invalid` 同经守卫入口；历史缺字段消息固定按 `LEGACY_TASK_SCHEMA_VERSION=1` 解析/继承，不随写版本 bump。新增 `tests/unit/test_schema_version_boundary.py` 17 条；同步 `flow-worker.md` / `ops-runbook.md` | 新文件 **17 passed**；定向+相关 24 文件 **312 passed**（#186/#252/#258/affinity/quota/#28/#53/#91 重试/start dedup/event 回归）；rebase 后复跑核心 19 文件 **268 passed**；`tests/unit` 全量 **rc=0**；**负控**：A42-05 还原 → **2F**（`guard_calls=0`；deny 时 DLQ 1≠0、pending 0≠1）；A44-01 还原 → **2F/2P**（reclaim 返回 `StreamTask(schema_version=2)`，旧 build 真执行形态；同版本/缺版本两条保持绿）；A44-02 还原（helper 与 start-dedup 均不传）→ **6F**（副本落当前版本）；invalid-payload 还原 → **2F**。r42 探针 `probe_252.py` 由旧断言翻转（`guard_calls=1, dlq=0, pending=1`）；r44 矩阵 `crash-reclaim` 由 `served` → `dead_letter`（`schema_rejected=1`），DLQ 信封含 `schema_version`；混跑池探针：新 build 重投副本信封仍为 v1，旧 build **served** 而非死信。`mkdocs build --strict` **rc=0**；ruff 仅既有 BLE001（改动行无新增）；`landing_gate.py ci` **绿**（main `e11aabf` CI success）；代码 commit `9411e2c` 已推送 origin/main |
| 第三十一轮 | [#258](https://github.com/jeffkit/plaita/issues/258)（P1；与 #50 同族） | **#258 P1**：`run()` 的 `ResumeProtocolError` 分支改为 `_handle_resume_protocol_error`——ack 前在租户上下文内读盘：仅当 `status=running` 且 `$LAST_NODE` 已不是 pending 挂起点（非挂起节点，或 suspending 但已决议）时，构建 `resume_type=continue` 副本，**先入队、后 ack** 原条；状态/定义读取瞬断或入队失败则不 ack，消息留 pending 等回收。`resume_flow` 在租约内复核后按同一判据把入口意图直接归一化为 continue（直接调用方也能恢复；已完成节点不重放）。真 #33 错配（suspended+continue、running+仍 pending 挂起点、陈旧 event 打在新挂起点）保持 `already_suspended` / ack+poison / #221 绑定守卫原语义。新增共用判据 `_resume_carrier_stale_reason`；`ResumeProtocolError` docstring 与 #50 互引；新增 `tests/unit/test_issue258_stranded_resume_carrier.py` 8 条（event/timeout 崩溃窗口恢复、真 #33 短路、陈旧绑定拒绝、run 分支状态感知与读盘瞬断）；同步 `flow-worker.md` / `idempotent-resume.md` | 新文件 **8 passed**；定向+相关 15 文件 **204 passed**（#33/#221/#233/#240/#242/#252/affinity/event 回归），复跑 **27 passed**；**负控** 还原 `flow_worker.py` → **5 failed / 3 passed**（入口 event/timeout 重投与 run 分支状态感知精确红；真 #33/陈旧绑定 3 条保持绿）；r41 探针 VERDICT 由 `STRANDED-RUNNING` → `NOT-REPRODUCED`（重投不再被判协议错误，而是继续撞注入的瞬态落盘失败）；全量 `bash scripts/ci-gate.sh` 通过（pytest+coverage≥80%、分层、wheel、SC-003）；`mkdocs build -f docs-site/mkdocs.yml --strict` **rc=0**；ruff 仅既有 BLE001（改动行无新增）；`landing_gate.py ci` **绿**（main `373395e` CI success）；代码 commit `3631755`（closes #258）已随 main `373395e` 推送 |

## 第五十一轮 · b94-02 + b94-01（2026-10-21，B1 修复席）

- 基线：`origin/main` `1032874`（CI success，`landing_gate.py ci` rc=0）。
- **b94-02（P2）budget_generation 哈希与实际预算脱钩**：`load_runtime_config`
  只消费 identity 类 overrides，worker 传入的 `max_deliveries` /
  `lease_ttl_seconds` / `task_max_age_seconds` / `dlq_max_len` 被静默丢弃；
  默认 worker 实际租约 120、哈希恒按 300，`config.effective` 自报 300。
  修法：`_budget_int` 让显式 overrides 优先（env 回退），
  `runtime_config.DEFAULT_LEASE_TTL_SECONDS` 与 `execution_lease`/runbook
  对齐为 120，worker 实际 TTL 按同一口径钳制。负控 6F（默认 300 vs 120、
  env 对齐不翻转、CLI 600/3 不生效、task_max_age/dlq override 丢弃、
  config.effective 自报 300），还原 6P。迁移面：默认
  `budget_generation`/`config_generation` 一次性翻转（300→120），在途消息
  触发 `config.budget_stale`，drain 或接受一次性告警；commit message 与
  `docs-site/docs/distributed/ops-runbook.md` 已注记。
- **b94-01（P3）`PLAITA_EVENT_TTL_SECONDS` 无上界**：新增
  `MAX_EVENT_TTL_SECONDS = 10 年`；`2^63`/`2^63-1`/上限+1 均 WARNING 具名
  回退 7d，不再进入 `SET EX`；合法值（含上限）不变，惰性 ZREM 自愈保留。
  负控 3F（3 个超界参数化全红，返回 315360001 等原值），还原 9P。
- 验收：全量 unit `-m "not slow"` **8600P/12S/14 deselected/3498
  subtests**；e2e+integration **287P/4S**；console **1045P/14S**（本地
  stale `frontend/dist` 用当前源码重建后全绿，产物不入库）；plaita-ai
  **241P/14S/12 subtests**；layering/wheel/size 全绿；`landing_gate.py ci`
  rc=0（main `1032874` success）。
- 本单为单 commit（见本轮 commit message）；未触碰他席未跟踪的
  `duo-a-ledger.md` / `duo-a2-ledger.md`。

## 第五十二轮 · #269/#270/#271（2026-10-21，B1 修复席）

- 基线：实现基于 `02b67b8`；落地前并发 B2 r61 `58364b1` 已并入 origin，
  本单 rebase 到 `58364b1` 后复跑（unit 8687P/12S、console 1045P/14S）。
- **#269（P1）CLI group/DLQ 覆盖与生产者 family 分叉**：`main()` 先解析
  CLI，再跑 `_cli_startup_problems`——`--consumer-group` / `--dlq-key` 与
  env-resolved 生产者值不一致即 `ConfigError` + exit 2（给两边值，要求
  全部署用 env）；匹配值照常。`--queue-name` 路由错位按 issue 边界不处理。
  CLI 值（strip 后）作为 overrides 进入 `load_runtime_config`，family/
  config.effective 与实际 worker 同源。
- **#270（P1）CLI 0/负值绕过预检**：`--lease-ttl-seconds` /
  `--max-deliveries` `<1` 具名拒绝、exit 2，worker 不构造；不再静默钳 1。
  库内构造的钳制保留为嵌入式兜底；lease=1 合法但极端（`_watchdog_interval`
  1.0s，测试钉住相对关系）。env-only preflight 仍先于 argparse（parser
  default 自身读 env，非法环境不能炸 traceback）。
- **#271（P1）TASK_MAX_AGE=0 语义分叉**：`core.retention.task_max_age_seconds`
  增加可选 environ；`envelope_for_stream` 改用它——0/负数→0（关闭年龄闸，
  与 worker 一致），非法→30d+WARNING。生产者/worker budget 一致；age=0
  的失败载体可被 reclaim（不再永久 `config_budget_stale`/PEL），负控实测
  reclaim 返回 None + budget_stale 告警。
- 负控： #269/#270 把 `_cli_startup_problems` 直接 `return []` →
  **9F/7P**（group/dlq 覆盖、lease/max 0/-1、main 三项；group main 覆盖
  `DID NOT RAISE SystemExit`；4 条 family 对齐测试仍绿），还原 **16P**；#271 改回 `_safe_int(minimum=1)`
  → **3F/1P**（age=0 budget 分叉、负数语义、reclaim None + budget_stale），
  还原 **4P**。
- 验收：全量 unit `-m "not slow"` **8687P/12S/14 deselected/3498
  subtests**；e2e+integration **287P/4S**；console **1045P/14S**；
  plaita-ai **241P/14S/12 subtests**；layering/wheel/size 全绿；
  `landing_gate.py ci` rc=0（main `4eec873` success）。
- 单 commit 关闭 #269/#270/#271；未触碰他席 `duo-b3-ledger.md` 与未跟踪
  `duo-a-ledger.md` / `duo-a2-ledger.md`。

## 第五十三轮 · #272（2026-10-21，B1 修复席）

- 基线：`origin/main` `0c5dea5`（CI success，gate rc=0）。
- **#272（P1）生产者 envelope 仍只按 env 算预算**：worker 合法 CLI
  `--lease-ttl-seconds 600 --max-deliveries 3` 时自身哈希 600/3，生产者
  信封仍 120/5；首投可执行，失败/崩溃留 PEL 后被 worker 判
  `config_budget_stale`，reclaim 永久跳过（默认 30 天年龄闸）。
- 修法（沿 r52 #269 先例，fail-fast）：新增
  `runtime_config.envelope_budget_values()` 作为生产者预算四键单源，
  `envelope_for_stream` 改用它；`_cli_startup_problems` 将 worker 实际
  四键（CLI lease/max + `task_max_age_seconds()` + `_dlq_max_len_from_env()`）
  与生产者元组逐键比较，任一不一致 → 具名 ConfigError（给两边值与 env
  键名）；`main()` exit 2、worker 不构造。CLI 与 env 一致或仅 env 配置照常，
  两端 `budget_generation` 必同哈希。四键全部覆盖（lease/max 为 CLI 面，
  task_max_age/dlq_max_len 比对 worker 实际解析 vs 生产者解析）。
- 文档：`docs-site/docs/distributed/ops-runbook.md`「配置代际」新增
  **预算四键只走 env** 口径（CLI 必须与 env 一致，否则启动拒绝；worker-only
  覆盖会让新任务带旧 budget、PEL 永久 stale）。
- 负控：budget_labels 置空 → **6F/4P**（CLI 600/3 无 problem、四键
  mismatch 各红、main `DID NOT RAISE SystemExit`；匹配/env-only、单源 helper、
  reclaim、分叉探针 4 条仍绿），还原 **10P**。分叉探针记录 pre-fix 形态
  （producer bg1-6a07… vs worker bg1-ed55…，reclaim None/stale）。
- 验收：全量 unit `-m "not slow"` **8697P/12S/14 deselected/3498
  subtests**；e2e+integration **287P/4S**；console **1054P/13S**；
  plaita-ai **241P/14S/12 subtests**；layering/wheel/size 全绿；
  `landing_gate.py ci` rc=0（main `0c5dea5` success）。
- 单 commit 关闭 #272；r51/r52 既有负控（b94-02 6 条、#269-271 20 条）
  零回归；未触碰他席 `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第五十四轮 · #275/#277（2026-10-21，B1 修复席）

- 基线：实现基于 `origin/main` `3497a0e`；落地前并发 B3 r66 `280be04` 已合入，
  本单 rebase 后复跑（unit 8714P/12S、console 1068P/13S）。
- **#275（P1）CLI identity 只 strip 比较、未写回**：`_cli_startup_problems`
  现在判定前 strip 并**写回实际使用的 args**（queue/group/DLQ）；尾随空格的
  group/dlq/queue 规范化后与 env 一致，不再产生独立 consumer group（同消息
  双投）/带空格 DLQ 键/错 Stream。新增 `--queue-name` 校验：空值或与
  `resolve_queue_name("worker")` 不同 → 具名 ConfigError（给两个 Stream +
  `PLAITA_QUEUE_NAME`）；真实 group/dlq 差异仍拒绝。ops-runbook 记 CLI
  identity 与 env 同源。
- **#277（P1 security）删除租户后在途 DLQ/订阅迟到写**：
  - foundation `tenant_context` 定义平台键常量；`server/tenant_status` 增
    `publish/read_tenant_generation` 与 `tenant_write_allowed`；
  - `create_tenant` 发布新世代 meta，`publish_all_tenant_status` 启动对账
    补齐；delete 的 disabled 墓碑由状态闸拒绝；
  - `StreamTask.tenant_generation` 在 read 时从 meta 捕获；`dead_letter`
    写前复核 active+世代，旧世代/已删/缺失（meta 存在）→ `TenantWriteRejected`，
    `dead_letter_if_allowed` 返回 False（不 ack、不写 DLQ、WARNING +
    `tenant_write_rejected` 计数）；
  - `EventSubscription.tenant_generation` + worker 派发 ContextVar；
    `store_subscription`/`mark_event_processed` 同款世代闸；event 层只引用
    foundation 常量，layering 门绿；
  - `PLAITA_DISABLE_FENCING=1` 下 DLQ/订阅闸仍生效；execution 状态写的
    既有回退风险按 ops-runbook 注记。
- 负控： #275 删除 strip 写回 + queue 校验 → **5F/3P**（尾空格三组 args 未
  写回、queue 不同/空值无 `--queue-name`），还原 **8P**；#277 关闭 DLQ/订阅
  闸 → unit **5F/2P**（删除/重建/缺世代 DLQ 放行、订阅未拒）、console
  **1F**（死信复活 OLD-SECRET），还原 unit **7P** + console **1P**。
- 验收：全量 unit `-m "not slow"` **8714P/12S/14 deselected/3498
  subtests**；e2e+integration **287P/4S**；console **1068P/13S**；
  plaita-ai **241P/14S/12 subtests**；layering/wheel/size 全绿；
  `landing_gate.py ci` rc=0 后单 commit 推送。
- 单 commit 关闭 #275/#277；未触碰他席未跟踪 `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第五十五轮 · #280（2026-10-21，B1 修复席；含 A1 r101 a/P2×3）

- 基线：`origin/main` `4379a76`（含 #273/#274/r66）；gate rc=0（CI + Console
  E2E 绿）。
- **#280（P1）dedup 预认领与占位行非原子**：`_claim_start_dedup_at_post` 改
  为 Lua 同时 `SET NX` + `owner=claiming:<id>`（60s）。并发 B 命中 live
  claiming owner 时不再删除/重认领，短暂轮询占位行后返回同一 mapped id；
  owner 过期/force-delete 墓碑才允许孤儿释放重认领。queued 行无 owner
  （崩溃在落行→入队之间）→ 用同一 id 补一条 start 载体；成功入队后 owner=
  `enqueued:<id>`（7d）。429/占位行失败/入队失败均释放映射+owner。
- **P2×3（CANCEL 原子性族）**：
  1. `_CANCEL_QUEUED_ROW_LUA` 增加 KEYS[2]=配额 ZSET，置 cancelled 同脚本
     ZREM；再 CANCEL 命中 cancelled 终态时补一次幂等 release（旧 leak 补偿）。
  2. worker step-persist 写 running 前复核 `_cancel_requested`，命中即写
     cancelled 并 break，防 running 覆盖已 CAS 的 cancelled。
  3. queued 占位行写后/入队前崩溃 → 同 key 重试补入队同 id（见上），不再
     永久 queued；认领失败路径清映射+owner。
- 测试：console `test_issue280_dedup_race_cancel.py` **5P**（并发屏障同 key
  → 同 id/一条载体/配额 1；owner 过期孤儿换新 id；queued 无 owner 补入队；
  CANCEL 配额同脚本释放；re-CANCEL 补释放）；unit
  `test_issue280_cancel_step_persist.py` **1P**（persist 序列无 step_persist）。
- 负控：NC A（去 owner/ZREM/post-CAS release）→ **3F/2P**（两个不同 id、
  CANCEL 后配额 1、re-CANCEL 配额 1）；NC B（不补入队）→ **1F**（载体 []）；
  NC C（删取消复核）→ **1F**（`['step_persist','cancelled']`）；还原 console
  5P + unit 1P。日志 `docs/proofs/data/issue280-negative-control.txt`。
- 验收：全量 unit `-m "not slow"` **8745P/12S/14 deselected/3498
  subtests**；e2e+integration **287P/4S**；console **1073P/13S**；
  plaita-ai **241P/14S/12 subtests**；layering/wheel/size 全绿；
  `landing_gate.py ci` rc=0（4379a76 CI success + Console E2E 绿）。
- 单 commit 关闭 #280；未触碰他席未跟踪 `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第五十八轮 · #282/#283/#284（2026-10-22，B1 修复席；任务书标称第五十六轮——台账第五十六/五十七轮已被 A69 复核轮占用，按序列顺延）

- 基线：`origin/main` `3276c20`；落地门 main 最近完成 run `8c663df`
  CI success + Console E2E 绿。
- **#282（P1）超投递绕过单一死信入口**：`_reclaim_one` 的
  `max_deliveries` 分支改走 `dead_letter_if_allowed`（守卫/租户写闸拒绝
  → False：不 ack、不写 DLQ、消息留 pending）；消费循环内层新增
  `except TenantWriteRejected`（计数 + ERROR，不 ack、不 poison），外层
  保留同类兜底（覆盖 read/ack 等循环体调用，短退避继续）；新增
  `note_tenant_write_rejected` 计数。concurrency=1 不再崩溃/重启循环。
- **#283（P1）世代读取时采样**：信封新增 `tenant_generation`
  （`enqueue_task`/`RedisStreamTaskQueue.enqueue` 写入）；`_task_from_fields`
  只解析信封，不再按当前 meta 采样；`_requeue_envelope_kwargs` 继承源世代
  （retry/handover/死信守卫克隆全族）；DLQ 信封保留源世代、replay 继承；
  console/schedule/event_filter/dispatch_ledger 生产消息入队时打当前世代。
  dispatch 入口用消费循环已设置的 `current_tenant_generation()` ContextVar
  比对当前 meta：不一致/当前缺失即丢弃（不执行）；**未改 `_dispatch_task`
  签名**，与 #277 订阅写闸复用同源上下文，存量测试替身零感知。无世代存量
  载体按 legacy 宽限（写闸仍 fail-closed）；`store_subscription` 无世代
  上下文且当前世代已发布 → `TenantWriteRejected(subscription_generation_
  missing)`，不再 auto-adopt。两条 87 变异锚点随 event_filter 入队形状同步
  （old/new 均加 `tenant_generation`，变异语义不变）。
- **#284（P1）建租户 generation/active 顺序**：`create_tenant` 在
  `flush()`（取 `created_at`）后、`commit()` 前发布新世代；世代不可确定或
  发布失败 → `TenantError`，事务回滚，不进入 active。成功后才 commit +
  发布 active。`publish_tenant_generation` 改为返回 bool（失败 WARNING +
  False）；启动对账按 generation → status，世代失败计 `failed` 并跳过
  active。
- 测试：unit `test_issue282_283_generation.py` **5P**（超投递 reclaim 不抛/
  pending；副本保源世代；dispatch 拒旧副本；订阅无世代拒绝；consume-loop
  捕获后继续并计数）；console `test_issue284_create_generation_order.py`
  **2P**（发布失败回滚；generation→active 顺序 + 旧世代拒写）；既有 #277
  队列读取用例改为显式信封世代。
- 负控：隔离副本 `/tmp/r56-nc`（共享 clone 有并发席位）组合回退 7 处实现
  → **6F**（NC 覆盖的 4 unit + 2 console：`TenantWriteRejected` 穿出
  `read()`、`KeyError: 'tenant_generation'`、旧副本仍 start、
  `DID NOT RAISE TenantWriteRejected`、`DID NOT RAISE TenantError`、顺序
  `['active','generation']`）；还原 **6P**。日志
  `docs/proofs/data/r56-generation-negative-control.txt`。
- 验收：全量 unit `-m "not slow"` **8741P/12S/14 deselected/3493
  subtests**（rebase 到 `8c663df` 后复跑）；e2e+integration **287P/4S**；
  console **1086P/13S**（rebase 后复跑）；
  plaita-ai **293P/13S/12 subtests**（`uv run --isolated --extra all`；
  仓库 venv 缺 mcp/langchain extras，缺 extras 时 MCP stdio 冒烟因子进程
  ImportError 变红，属环境面）；layering/wheel/size **46P**；
  `landing_gate.py ci` rc=0 后单 commit 推送。
- 单 commit 关闭 #282/#283/#284；未触碰他席 `duo-b3-ledger.md` 与未跟踪
  `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第五十九轮 · #285（2026-10-22，B1 修复席；任务书标称第五十七轮——台账第五十七/五十八轮已被 A69 复核轮与 #282-284 轮占用，按序列顺延）

- 基线：`origin/main` `c08e118`；落地门 main 最近完成 run `c08e118`
  CI success + Console E2E 绿。
- **#285（P1）owner 过期/墓碑后原认领者 A 的迟到写**：
  - `_start_claim_guard`（Lua）：A 在写 queued 占位行、入队前复核
    map==我、无我的墓碑、owner==claiming:我；通过则续期。claiming TTL
    过期但映射仍属我且无人接管 ⇒ 原地续期（不双 id）；被接管/墓碑 ⇒
    放弃写/入队。Redis 故障 fail-closed。
  - `_set_start_dedup_owner` 改 CAS（Lua）：只允许 map==我、无墓碑、owner
    缺省或同 id（claiming→enqueued）；绝不覆盖他 id 的 owner。入队后 CAS
    失败 ⇒ 清行/释放配额，收敛当前映射或 409。
  - owner 键改独立前缀 `{ns}:start-dedup-owner:{key}`（旧 `<map>:owner`
    在 `dedup_key="foo:owner"` 时与 `"foo"` 的 map 键碰撞）。
  - 败者端：行不存在时有界等 A 落行，等不到且认领有效 ⇒ 409
    `execution_start_in_progress`（同一 dedup_key 重试），映射释放/换主/
    墓碑 ⇒ 重新认领；行存在但 owner=claiming 时也等 enqueued，不在载体
    落盘前发 200（ghost 200）。
  - 失败路径：占位行失败 503 + Retry-After，释放映射/配额并写执行墓碑
    （经 `_discard_queued_placeholder`）；等待中的败者重查后换新 id。
- 测试：console `test_issue285_owner_guards.py` **5P**（owner TTL=1s 慢
  占位行、force DELETE+live claiming、foo/foo:owner 四键独立、3+ 败者
  无 ghost 200、owner CAS 不覆盖）；#280 并发用例按新契约调整（B 先拿
  409 in-progress，A 完成后同 key 重试收敛；同 id/单载体/配额断言不变）。
  worker 侧零改动（#280 worker 消费去重未动）。
- 负控：pre-fix `git worktree`（`c08e118`，本单未提交）+ 新用例 →
  **5F**（A 返回自己 id、键碰撞 3==4、B 200 而非 409、`allow_from_state`
  缺失）；主工作树同命令 **5P**。A1 r103 探针（ROOT 参数化）：owner TTL=1s
  双 200/双载体/owner 覆盖；3+ 败者 ghost 200（queue 空）；force DELETE
  A 200 墓碑 id+双载体；后缀碰撞重试队列增长——post-fix 均单 id/单载体/
  409 或收敛。日志 `docs/proofs/data/r57-owner-cas-negative-control.txt`。
- 验收：全量 console **1091P/13S**；unit `-m "not slow"` **8741P/12S/14
  deselected/3493 subtests**；e2e+integration **287P/4S**；plaita-ai
  **293P/13S/12 subtests**（`uv run --isolated --extra all`）；layering/
  wheel/size **46P**；`landing_gate.py ci` rc=0 后单 commit 推送。
- 单 commit 关闭 #285；未触碰他席 `duo-b3-ledger.md` 与未跟踪
  `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第六十轮 · DLQ replay 目标 family（2026-10-22，B1 修复席；r58 a/P2，任务书标称第五十九轮——已被 #285 占用，按序列顺延）

- 基线：`origin/main` `dc31efa`；落地门 main 最近完成 run `dc31efa`
  CI success + Console E2E 绿。
- **缺陷（r58 a/P2）**：`replay_dead_letter` 只继承源 family，不校验目标
  stream family。源 F1 死信灌进 F2 stream 仍返回 `replayed=True/ok` + 写
  marker + 目标 Stream；目标 worker family gate 拒收留 pending
  （`config_family_mismatch=1`），无执行——「replayed ok」是假成功，且
  marker 已烧导致修好后不能直接重放。
- 修法：
  - 新增 `_resolve_target_family`：默认
    `envelope_for_stream(target_stream, role="worker")`（目标 stream + 生效
    group/DLQ，与目标 worker/生产者同源）；自定义 group/DLQ 部署由调用方
    显式传 `target_config_family`；显式值必须合法 `cf1-`，解析异常/非法 ⇒
    `target_family_unknown`（fail-closed）。
  - 在源 `config_envelope` 后、schema 检查/marker/入队前比较源 family
    （缺失/畸形已归一为 `legacy`）与目标 family：不一致 ⇒
    `target_family_mismatch`（detail 含双 family），不写 marker、目标 XLEN
    不变、源 DLQ 保留，`_reject` 审计记 rejection。
  - `allow_family_mismatch=True` 才允许显式跨 family（WARNING + 审计）；
    console `DlqReplayRequest` additive 暴露 `target_config_family` /
    `allow_family_mismatch`。
- 测试：新增 `tests/unit/test_r58_dlq_replay_target_family.py` **5P**
  （跨 family 拒绝 + 同 family 正控、同 family 成功、legacy 默认拒绝 +
  显式 override、非法显式 family 不消耗 marker、自定义 group/DLQ 显式声明）；
  既有 replay 用例源信封补 `config_family` 正控：unit 三文件 **41P**、
  console 两文件 **19P**。
- 负控：pre-fix worktree（`dc31efa` + 本单文件，删除 family 检查）→
  **3F/2P**（跨 family / legacy / 非法显式均报 `ok`）；主树同命令 **5P**。
  规格探针 r104（ROOT 参数化）：修复前 `replayed=True/ok`、目标 XLEN=1、
  marker=True、family_mismatch=1、pending=1；修复后
  `False/target_family_mismatch`、XLEN=0、marker=False、源 DLQ=1、无 pending。
  日志 `docs/proofs/data/r58-dlq-replay-family-negative-control.txt`。
- 验收：全量 unit `-m "not slow"` **8746P/12S/14 deselected/3493
  subtests**；e2e+integration **287P/4S**；console **1091P/13S**；
  `landing_gate.py ci` rc=0 后单 commit 推送。
- 无 issue 号（duo-a 第九十六轮 a/P2）；未触碰 #286/#287 dedup seam、他席
  `duo-b3-ledger.md` 与未跟踪 `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第六十一轮 · #286/#287（dedup 受理 seam 设计级收口，2026-10-22，B1 修复席；任务书标称第六十轮——已被 DLQ replay family 轮占用，按序列顺延）

- 基线：`origin/main` `ca39bc2`；落地门 main 最近完成 run `ca39bc2`
  CI success + Console E2E 绿。
- **#286（P1）stale orphan release TOCTOU**：`_release_start_dedup_if_owner`
  旧实现 GET map==id 后 DEL，B 可在决定与 DEL 之间停顿；A 续期/落行/入队/CAS
  成功后 B 仍删 A 映射 → 双 200/双载体/双配额。现在 release 是 Lua 原子
  复核 map/token/owner/row/墓碑：token 在位/row 存在/enqueued ⇒ 拒绝；
  墓碑例外允许清映射。B 的 stale 决定失败后收敛到 A（或 409）。
- **#287（P1）owner 键迁移**：`read_owner` 新键优先、缺失时只读旧
  `<map>:owner`；旧 `claiming:A` 视为 live 并 `adopt_legacy` 迁到新键+token，
  旧 `enqueued:A` 视为已入队（不重复补 carrier）。`foo:owner` 反向碰撞由
  Lua 先把 foo 的 owner 迁到新键再释放共键；无法安全迁移 ⇒ 409
  `execution_start_legacy_collision`。写路径只写新键；**console 滚动升级
  必须先排空旧实例**（runbook 硬约束）。
- **状态机**：`plaita/server/start_dedup.py`（console+schedule 共用）：
  claim SET NX+owner claiming+token → guard（map/token/owner/墓碑，慢写跨
  TTL 且 owner/token 缺失可同 token 续期）→ 写占位行 → reserve_enqueue
  （enqueueing）→ XADD → mark_enqueued（enqueued，TTL 7d）。失败清理
  token release；崩在落行→入队之间的行由 `adopt_for_recovery` 原子建立
  reservation 后同 id 补 carrier。
- **③ schedule/cron**：`fire_schedule` 在配额 admission 前 claim 同一
  dedup_key；同 due 重触发收敛既有 execution_id（不入队/不占配额）；配额
  满/入队失败/mark 失败均 release claim+回滚配额（+ XDEL 载体）。
- **④ 409/transport**：in-progress/superseded 409 带 `Retry-After: 1`；
  handler 等待改 `asyncio.sleep`（不再 time.sleep 阻塞事件循环）；前端
  `startExecution` 生成/复用 `dedup_key`，对 in-progress 按 Retry-After/
  指数退避重试同一 body，收敛既有/终态 id。
- **⑤ force DELETE 迟到载体**：reservation 后 mark 失败路径
  `_safe_xdel_carrier` 清收刚 XADD 的 target message；tombstone 仍是 worker
  侧最终防线。
- 测试：unit `test_r59_start_dedup_state_machine.py` **11P**（含 guard/mark
  错误 token、stale release、旧 claiming/enqueued、反向碰撞、schedule 同 due/
  配额/入队失败）；console `test_issue286_287_owner_state_machine.py` **5P**
  （stale release 收敛 A、旧 claiming 409+Retry-After、旧 enqueued 不重复
  carrier、反向碰撞、force DELETE 后 XDEL）；既有 #280/#285 **10P**、
  schedule #198/#23/#28 **52P**、前端 node **29/29**。
- 负控：隔离副本逐项变异 A–G（release 回退 GET→DEL / 去旧 owner 双读 /
  schedule 不预认领 / 去 Retry-After / 去 XDEL / guard 去 token /
  mark_enqueued 去 token）全部实红后还原；A1 r105 探针 pre 双 id/双载体
  实红、post 收敛/409/单载体。证据
  `docs/proofs/data/r59-dedup-state-machine-negative-control.txt`。
- 验收：全量 unit `-m "not slow"` **8769P/12S/14 deselected/3493
  subtests**（rebase 到 `1f01658` 后复跑）；e2e+integration **287P/4S**；
  console **1115P/13S**（rebase 后复跑）；前端 node **29/29** + tsc 干净；
  `landing_gate.py ci` rc=0 后单 commit 推送。
- **新状态机不变量（A1 r106 对抗清单）**：
  1. 同一 dedup_key 任意交错最多一个 execution_id/一条可执行 carrier/一个配额；
  2. 任何写转换要求 map==id、owner 状态、token 匹配、无墓碑；
  3. release 原子复核 token/owner/row/墓碑；无 token 只在 row 缺失且
     token 缺失、owner 缺失/claiming 时生效（墓碑例外）；
  4. owner 不能被另一个 id 覆盖；enqueued 终态不可被失败清理删除；
  5. 旧格式只读迁移；新写只写新键；console 升级必须先排空旧实例；
  6. schedule/console 共用 claim 原语；XADD 前必有 reservation；
  7. 失去认领的载体必须 XDEL，tombstone 是 worker 侧最终防线。
- 单 commit 关闭 #286/#287；未触碰他席 `duo-b3-ledger.md` 与未跟踪
  `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第六十二轮 · #288（schedule 接入 reservation 状态机，2026-10-22，B1 修复席；任务书标称第六十一轮——已被 #286/#287 状态机轮占用，按序列顺延）

- 基线：`origin/main` `41085cc`（A1 r106 对抗确认 console 全不变量守恒，唯一
  破口是 schedule 未接 reservation）；落地门 `4985022` CI + Console E2E 绿。
- **#288（P1）**：`fire_schedule` 原 claim→配额→占位行→直接 `enqueue_task`
  →mark；claim 在慢占位行窗口过期后仍 XADD，mark 失败只 XDEL carrier，留下
  queued 行+map 7 天；同 due 再触发落孤儿分支只返回 None，不 adopt 不 release。
  修法：
  - 占位行后、XADD 前 `reserve_enqueue(token)`；reservation 失败先 `guard`
    同 token 续期（慢写跨 claiming TTL）再 reserve，仍失败放弃 XADD。
  - `_MARK_ENQUEUED_LUA` 收紧为只允许 `enqueueing→enqueued`：claiming 直入
    被拒，不变量 6 由状态机强制（不再靠调用方自觉）。
  - reservation/入队/mark 失败统一：quota release + `_discard_queued_placeholder`
    （discard 行+写墓碑）+ `_dedup.release(no token)`（墓碑分支清 map/owner/
    token）；mark 失败另 XDEL carrier；fire_record/enqueue=failed、advance
    正常。
  - 同 due 孤儿：row 残留则 discard（墓碑）+条件 release，清理成功后同调用
    重试新 id；清理失败具名 WARNING 返回 None，不再静默留 7 天。
  - live owner 分支保持 r71：同 due 收敛既有 id、fire_record enqueue=dedup。
- **A106-02 a/P2（前端）**：新增 `isExecutionStartRetryable`
  （in_progress+superseded）、`START_RETRY_WINDOW_MS=65_000`（≥ claiming TTL
  60s）、`startRetryWindowError`（超窗具名引导「刷新执行列表，不要换 key
  重复提交」）；`startExecution` 按 deadline 重试同一 body，删除 `attempt>=3`。
- 测试：新增 `tests/unit/test_issue288_schedule_reservation.py` **5P**（mark
  必须 enqueueing、reservation 失败不 XADD/回滚干净、慢占位行跨 TTL 续期后
  成功、孤儿清理后重试新 id、mark 失败 XDEL+discard+release）；既有 schedule/
  状态机 **69P**、console dedup **15P**；前端 node **30/30** + tsc 干净。
- 负控：隔离副本 A–E（去 reserve+不清理 / mark 允许 claiming / 孤儿直接
  return None / 前端退回 3 次窗口 / mark 失败不 discard）全部实红；A1 r106
  探针 pre `first fire None`+queue 空+map/queued 行残留、post 单 carrier/
  单行/quota 1、第二次 dedup 一致。证据
  `docs/proofs/data/r60-schedule-reservation-negative-control.txt`。
- 验收：全量 unit `-m "not slow"` **8774P/12S/14 deselected/3501
  subtests**（rebase 到 `4985022` 后复跑）；e2e+integration **287P/4S**；
  console **1115P/13S**（rebase 后复跑）；前端 node 30/30 + tsc 干净；
  `landing_gate.py ci` rc=0 后单 commit 推送。
- 不变量清单更新：不变量 6 现在由 `_MARK_ENQUEUED_LUA` 强制（只有
  enqueueing 可 mark）；schedule 孤儿自愈（清理后同调用重试）；失败路径不得
  留无 carrier 的 queued 行/map。
- 单 commit 关闭 #288；未触碰他席 `duo-b3-ledger.md` 与未跟踪
  `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第六十三轮 · #289（mark 失败 park-retry + worker 终态防线，2026-10-22，B1 修复席；任务书标称第六十二轮——已被 #288 reservation 轮占用，按序列顺延）

- 基线：`origin/main` `39c875a`；落地门 `39c875a` CI + Console E2E 绿。
- **#289（P1）**：mark_enqueued 失败时 carrier 可能已被 worker 消费并执行；
  r59/r60 的 discard/墓碑/XDEL/释放 map 撤不回运行，同 key/同 due 重试铸 B
  → A（已完成）+ B 双执行。修法：
  - 新增 `start_dedup.recover_after_mark_failure`：map 仍属本 id、无墓碑时
    guard（owner/token 缺失可同 token 续期）→ reserve → mark 原地恢复。
  - 恢复失败但 map 仍属本 id、无墓碑 ⇒ **park**：不清收、不回滚配额；console
    返回 409 in-progress（Retry-After），schedule 返回 None；同 key/同 due
    重试走 hit/孤儿恢复，收敛原 id。只有映射换主/墓碑（载体不会执行）才
    XDEL+discard+release+配额回滚。
  - schedule 孤儿：row 存在且无墓碑 ⇒ `adopt_for_recovery` 补 carrier 同 id
    恢复；row 缺失/墓碑才清理映射后新 id。
  - worker 消费点终态防线：`_handle_start_dedup_hit` 命中 completed/error/
    cancelled 时具名 WARNING + `discarded=True`，ack 不执行。
- 测试：console `test_issue289_mark_race.py` **3P**（A 被消费完成后 mark 失败
  → 200 completed/原 id/row 保留/同 key retry A；恢复不可用 → park 409/
  row+map+配额保留/TTL 后同 id 恢复；worker 终态命中 discarded）；
  `test_issue289_schedule_mark_race.py` **2P**（schedule 同场景原 id 收敛；
  park 后孤儿同 id 恢复）。既有 #288 schedule 5P（孤儿/park 语义更新）、
  r59 状态机 11P、#280/#285/#286 15P、schedule 49P 全绿。
- 负控：隔离副本 A–C（console recover 关闭+旧 discard、schedule 同、
  worker 去 discarded）全部实红；A1 r107 双探针 pre console A 409+墓碑+
  同 key B、schedule first None+同 due B；post console A 200 completed/retry A、
  schedule first A msg/同 due None/队列仅 A。证据
  `docs/proofs/data/r61-mark-park-negative-control.txt`。
- 验收：全量 unit `-m "not slow"` **8776P/12S/14 deselected/3501
  subtests**；e2e+integration **287P/4S**；console **1118P/13S**；
  `landing_gate.py ci` rc=0 后单 commit 推送。
- 不变量更新：mark 失败 ≠ 未执行；carrier 已消费时只能恢复/park，不能
  discard/换 id；worker 终态命中必须 ack 不执行。正常/迁移/reservation
  路径不回退。
- 单 commit 关闭 #289；未触碰他席 `duo-b3-ledger.md` 与未跟踪
  `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第六十四轮 · B108-01/02（park 周期清扫 + discarded 独立审计，2026-10-22，B1 修复席；任务书标称第六十三轮——已被 #289 轮占用，按序列顺延）

- 基线：`origin/main` `c24ce3e`；落地门 `eee7b37` CI + Console E2E 绿。
- **B108-01 park 无独立 sweeper**：park 的 queued 行/map/token/配额依赖同
  key/同 due 重试或 worker 终态自然收敛，无人重试时无最终清收。
  - 新增 `plaita/server/start_dedup_sweeper.py`：park 写
    `{ns}:start-dedup-park:{key}`（id+parked_at，TTL 1h）；`mark_enqueued`
    成功/恢复清除标记。
  - `sweep_parks` 只扫标记，超时默认 60s（claiming TTL），预算 100/tick；
    running/终态/映射换主只删标记；queued/缺失 ⇒ 墓碑+删行+清
    map/owner/token+`release_tenant_start` 回配额+WARNING 审计+指标。
  - console cluster lifespan 启动 `ParkSweeper` 线程（默认 30s），关闭时先停
    线程；env `PLAITA_START_DEDUP_PARK_SWEEP_INTERVAL` / `..._BUDGET`；
    `PLAITA_DISABLE_START_DEDUP_PARK_SWEEP=1` 启动 WARNING。
- **B108-02 discarded 无独立审计**：worker 终态放弃只有 WARNING。
  - `RedisFlowWorker._dedup_discarded_total` + `collect_worker_metrics` 新
    counter `worker_dedup_discarded_total`（/metrics）。
  - 写 `plaita:execution:dedup-discard:{id}`（execution/carrier/dedup_key/
    status/reason/discarded_at，7d）；console 详情返回 `dedup_discarded`；
    ops-runbook/flow-worker 注记。
- 测试：`test_start_dedup_park_sweeper.py` **4P**（超时 queued 清收/墓碑/配额/
  指标；fresh 不动；running/completed 只删标记；row missing 清认领+配额）；
  console `test_issue289_mark_race.py` 既有 3P 追加 counter/metrics/audit key/
  GET 详情 `dedup_discarded` 断言。既有 #288/#289/r59/schedule/console 全绿。
- 负控：隔离副本 A–D（sweeper 永不清理 / 忽略超时 / worker 无 counter /
  无审计键）全部实红；修复后 4P/3P 全绿。证据
  `docs/proofs/data/r62-park-sweeper-discard-audit-negative-control.txt`。
- 验收：全量 unit `-m "not slow"` **8795P/12S/14 deselected/3501
  subtests**（rebase 到 `eee7b37` 后复跑）；e2e+integration **287P/4S**；
  console **1121P/13S**；`landing_gate.py ci` rc=0 后单 commit 推送。
- 单 commit；未触碰他席 `duo-b3-ledger.md` 与未跟踪
  `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第六十五轮 · B109-01/02 + B2 r67 注记（轻量硬化，2026-10-22，B1 修复席）

- 基线：`origin/main` `a75b4ab`；落地门 `eee7b37` CI success + Console E2E
  `a75b4ab` 绿。
- **B109-01 ExecutionDetail Langfuse href scheme**：外部链接直接进 `<a
  href>`；新增 `safeExternalUrl`（`new URL` + 仅 http/https，其余/相对/
  畸形返回 null），页面在 null 时降级 `<span>` 纯文本（title 说明）。
- **B109-02 Cluster 日志深链拼接**：`service_type`/`instance_id` 直接拼
  query；新增 `buildLogsHref` 用 URLSearchParams 逐参数编码，特殊字符
  （& = #）不改查询结构；Cluster 改用它。
- **B2 r67 注记恒真断言**：`assertTrue(x is False or x)` → `assertFalse(x)`；
  对照实现 Reduce.initial 的 live 子对象由引擎物化/隔离，refused=False，
  写穿由 live_changed 全表断言兜住。
- 测试：前端 node `check-api-transport` **33/33**（新增 scheme/编码行为断言
  + 两处接线静态断言）、`tsc --noEmit` 干净；mutation matrix
  **40 passed + 1265 subtests**。
- 负控：① safeExternalUrl 去 scheme 检查 → B109-01 断言 1F；② buildLogsHref
  退回字符串拼接 → B109-02 断言 1F；③ 恒真断言改成 assertTrue → mutation
  matrix 1F。证据 `docs/proofs/data/r63-hardening-negative-control.txt`。
- 验收：全量 unit `-m "not slow"` **8795P/12S/14 deselected/3501
  subtests**；e2e+integration **287P/4S**；console **1121P/13S**；
  `landing_gate.py ci` rc=0 后单 commit 推送。
- 单 commit；未触碰他席 `duo-b3-ledger.md` 与未跟踪
  `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第六十六轮 · login_guard 硬化（A112-01/02 + B112-01/02，2026-10-22，B1 修复席）

- 基线：`origin/main` `3c9d1a0`；落地门 `3c9d1a0` CI + Console E2E 绿。
- **A112-01 分布式爆破**：新增账号桶（同一账号失败来自 ≥3 个不同 IP →
  锁账号）；保留 per-key `(ip,user)` 5 次/300s；新增 IP 桶（单 IP fan-out
  ≥10 → 锁 IP 5m，低于账号锁，NAT/共享出口影响小）。单 IP 重复失败不触发
  账号桶（L5 另一 IP 仍可登录）。
- **A112-02 计时枚举**：`users_svc.login` 对 missing 跑固定 dummy PBKDF2、
  disabled 跑真实 hash；A1 probe ratio 16.0× → 1.0×（阈值 ≤2）。
- **B112-01 可信代理**：`PLAITA_TRUSTED_PROXIES`（IP/CIDR allowlist）；
  仅 peer 可信时解析 XFF，从右向左取首个 untrusted；peer 不可信一律忽略
  XFF（L2 伪 XFF 仍锁）。
- **B112-02 多 worker**：`RedisLoginThrottle`（key/IP/account 三维 Lua 原子
  共享计数，前缀 `plaita:login-guard:`）；endpoint 从 `app.state.redis` 注入；
  无 Redis 本地档退回进程内并 runbook 注明单 worker 语义。
- **L6 break-glass**：`PLAITA_LOGIN_BREAK_GLASS_USERS` 绕过限速（仍校验
  口令，WARNING）。
- **L10**：`_prune` 不驱逐活动锁；容量满时新键不跟踪，已有锁保持。
- 测试：`test_issue_a112_login_hardening.py` **12P**（L1~L11 逐条，含 Redis
  两实例共享计数与两个 app 端点共享计数）；既有 #208 **8P**；`test_r55_
  audit_observability` 的 retry_after monkeypatch 适配新签名。
- 负控：pre `/tmp/plaita-r112-tree` vs post 主树（端点探针 + A1 timing/
  eviction）：L1 429→200、L3 helper 无→有、L4 200→429、L5 未试账号 200→429、
  L6 429→200、L7/L8 无实现→有、L9 16×→1.0×、L10 evicted True→False、
  L11 200/401→429/429；L2 429→429（保留性）。证据
  `docs/proofs/data/r64-login-hardening-negative-control.txt`。
- 验收：全量 unit `-m "not slow"` **8795P/12S/14 deselected/3501
  subtests**；e2e+integration **287P/4S**；console **1184P/13S**（rebase 到
  `3c9d1a0` 后复跑）；`landing_gate.py ci` rc=0 后单 commit 推送。
- 单 commit；未触碰他席 `duo-b3-ledger.md` 与未跟踪
  `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第六十七轮 · login_guard 设计级收口（#293/#294/#295 + B112-01/02，2026-10-22，B1 修复席）

- 基线：`origin/main` `b2906c2`；落地门 `b2906c2` CI + Console E2E 绿。
- **桶/窗口语义表**已入 ops-runbook「登录限速与反枚举」：per-key canonical IP
  5/300s；IP 桶 IPv6 /64 聚合 10/300s→5m；account 桶窗口内 ≥3 个不同 IP 桶
  →15m；翻倍≤24h。
- **#293**：`canonical_ip`（IPv4-mapped→IPv4、压缩展开）+ `bucket_ip`（IPv6
  /64）；account 集合进程内每次 record/retry 按窗口清理、Redis 改 ZSET
  （ZADD/ZREMRANGEBYSCORE/ZCARD）；同 /64 多地址只算 1，旧 IP 锁过期后单次
  失败不续锁。
- **#294**：break-glass 必须用户名白名单 + 来源 IP 白名单 + 一次性 HMAC
  token（`bg1.<exp>.<nonce>.<hmac>`，secret≥32，短 TTL，Redis SET NX 跨副本）；
  失败照常计数；每次尝试独立 `auth.login_break_glass` 审计；仅用户名列表
  回落常规限速。
- **#295**：Redis Lua 用 `redis.call('TIME')`，retry 用 `redis.time()`；进程
  time/monotonic 不入共享键；NTP 回拨 until 只前进。
- **B112-01**：`0.0.0.0/0`/`::/0` 具名拒绝；peer/XFF canonical 化后 CIDR
  匹配（IPv4-mapped 可命中 IPv4 网段）。
- **B112-02**：MAX_TRACKED 满表且无过期项时新键 60s 具名饱和（不静默 0、
  不驱逐活动锁）。
- 测试：`test_issue293_294_295_login_guard_v2.py` **14P**；A112 **12P**
  （L6 改 token 语义）；#208 **8P**；r55 审计适配。负控 pre/post：同 /64
  waits [0,0,900]→[0,0,0]、旧 IP 续锁 1800→0、break-glass 20×401/no marker
  →{401,429}/marker、Redis clock 0/999900→900、/0 全 trust→拒绝、MAX_TRACKED
  0→60。证据 `docs/proofs/data/r65-login-guard-v2-negative-control.txt`。
- 验收：全量 unit `-m "not slow"` **8795P/12S/14 deselected/3501
  subtests**；e2e+integration **287P/4S**；console **1198P/13S**；
  `landing_gate.py ci` rc=0 后单 commit 推送。
- 单 commit 关闭 #293/#294/#295；未触碰他席 `duo-b3-ledger.md` 与未跟踪
  `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第六十八轮 · #296（MAX_TRACKED 饱和仅新键，2026-10-22，B1 修复席）

- 基线：`origin/main` `cc7abfe`；落地门 `cc7abfe` CI + Console E2E 绿。
- **#296（P1）**：r65 的 `retry_after` 对任何键都追加全局
  `_saturated_until`；任一新键触发饱和后既有未锁键（含 admin）也 60s 429，
  且每 60s 新键可续期形成全局登录面阻断，与规格「仅新键」不符。
  修法：饱和按新键记录 `_saturated_keys{(canonical_ip,user): until}`；只有
  触发饱和的键 `retry_after` 返回剩余秒数，既有键/活动锁不受影响；prune/reset
  清理过期饱和；ops-runbook 明确「仅新键；既有键 retry_after 恒 0；不全局
  施加」。
- 测试：`test_296_saturation_binds_only_the_new_key`、
  `test_296_existing_key_not_blocked_over_http`、
  `test_296_repeated_new_keys_cannot_block_existing_key` **3P**；既有
  `test_b112_02_max_tracked_saturation_not_silent` 保持。
- 负控：pre `/tmp/plaita-r114-tree` vs post（A1 probe）：既有 admin
  retry 60→0；HTTP admin 正确 429→200、Retry-After 60→null；新键仍 60；
  重复新键不阻断既有键。证据
  `docs/proofs/data/r66-saturation-negative-control.txt`。
- 验收：全量 unit `-m "not slow"` **8795P/12S/14 deselected/3501
  subtests**；e2e+integration **287P/4S**；console **1201P/13S**；
  `landing_gate.py ci` rc=0 后单 commit 推送。
- 单 commit 关闭 #296；未触碰他席 `duo-b3-ledger.md` 与未跟踪
  `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第六十九轮 · #297/#298（runner 模块级信封 + 孤儿守卫窗口，2026-10-22，B1 修复席）

> 编号碰撞注记：派单写「第六十八轮」，但 68 已由上一轮 #296 占用（同文件
> 第六十八轮条目），沿用既有「占用即递增」先例记第六十九轮。

- 基线：`origin/main` `eb08fd0`（#43/r75、B2 r70 已并入）；落地门
  `landing_gate.py ci` rc=0（`eb08fd0` 主 CI + Console E2E 双绿）。
- **#297（P2）**：`_RUNNER_TEMPLATE` 的 `try` 只包 `run()` 函数体，模块级
  顶层确定性错误（`raise`/表达式 TypeError/未定义名）在信封外 → runner
  非零退出无 stdout → 父进程按基础设施失败判可重试，retryTimes=3 实测
  真实执行 4 次、顶层副作用重复 4 遍；unsafe 档 1 次具名终态；顶层
  `print` 后 raise 更退化为「not valid JSON」、traceback 丢失。
  修法：①runner 安装 `sys.excepthook` 把顶层未捕获异常写成与 run() 同形
  的 `ok=false` 信封（`type` 字段供 `origin_type` 穿透；正常走完用户代码
  立即还原默认 hook，runner 自身尾巴失败维持基础设施级缺省）；②
  `_decode_runner_output` 整段 JSON 失败时回退 `_extract_trailing_envelope`
  （只认 stdout **末尾**的合法信封；信封前 print 不再降级，无合法信封的
  脏输出仍基础设施可重试、不文本嗅探）。两档终态一致：顶层 TypeError
  4→1 次、具名 `SandboxEnvelopeError: TypeError: ...`。
  边界不变：SyntaxError 保存期拦、重命名子类按名字面集、ImportError 可重、
  顶层 `sys.exit()`/`os._exit()` 不经 excepthook（与修前同形）；JS 模板未改。
- **#298（P2）**：`evaluate_proposal` 的 A2R46-01 孤儿守卫 `try` 从
  `evaluate()` 才开始，`save_version` 与响应校验在守卫外；「服务端已落库 +
  响应缺 `version`/读取响应断连」→ `ConsoleClientError` 穿出 → `run_loop`
  兜底 outcome 无 `candidate_version` → 无主孤儿版本（W5a/W5b 实测
  versions_in_lib 含 1.0.1、outcome candidate_version=null）。
  修法：守卫起点前移到 `save_version` 调用本身；响应异常时
  `_recover_persisted_version` 回查版本库，仅认领「save 前快照不存在 +
  definition 逐字一致」的新版本（取最高 semver），降级 failed_iteration +
  candidate_version + WARNING `version kept as lineage`；未落库/恢复失败/
  仅有并发写入者不同定义版本时原异常穿出，不猜号、不虚构谱系。
- 负控（先红后绿，证据 `docs/proofs/data/r69-297-298-negative-control.txt`）：
  #297 顶层三类型+print 后 raise+顶层表达式/未定义名 pre 全 4 次执行/裸
  RuntimeError/not valid JSON → post 全 1 次 + 具名；定向 8F/2P/1S →
  10P/1S。#298 W5a/W5b pre outcome candidate_version=null（`KeyError:
  'candidate_version'`）→ post 均为 `1.0.1` + WARNING；定向 2F/2P →
  4P；「未落库不虚构」「并发不同定义不认领」两守恒 pre/post 均绿。
- 新增测试：`tests/unit/test_issue297_runner_module_scope.py`（10P/1S，
  docker 无镜像跳过；subprocess 实弹+unsafe 对照+解码边界）；
  `plaita-ai/tests/test_supervisor.py` +4（两变体 + 两守恒）。
- 验收：全量 unit `-m "not slow"` **8836P/13S/14 deselected/3501
  subtests**；e2e+integration **287P/4S**；console **1215P/13S**；
  plaita-ai（`uv run --isolated --extra all`）**329P/13S/12 subtests**；
  定向 41P/1S + supervisor 25P；`ci-gate.sh layering|wheel|size` PASS；
  ruff 新增代码零 BLE001/E722；`mkdocs --strict` rc=0；`landing_gate.py ci`
  rc=0 后单 commit 推送。
- 文档：ops-runbook 确定性错误节（信封覆盖模块级、print 污染、#297 前
  行为）、error-handling 沙箱信封段、MIGRATION「信封覆盖范围」、
  `evaluate_proposal` docstring；proof
  `docs/proofs/issue297-298-module-scope-orphan-window.md`。
- 单 commit 关闭 #297/#298；未触碰他席 `duo-b3-ledger.md` 与未跟踪
  `duo-a-ledger.md`/`duo-a2-ledger.md`。

## 第七十一轮 · #299（flow publish outbox 跨 Redis 投递链，2026-10-22，B1 修复席；任务书标称第六十八轮——68/69/70 已被 #296、#297/#298、B2 r70 占用，按序列顺延）

> 编号碰撞注记：派单写「第六十八轮」，68/69/70 均已被占用（本文件第六十八/
> 六十九轮 + eb08fd0 合并的 B2 r70），沿用「占用即递增」先例记第七十一轮。

- 基线：`origin/main` `daadc00`；落地门 `landing_gate.py ci` rc=0（main CI +
  Console E2E 双绿，落地前复核 origin/main 未漂移）。
- **#299（高优 feat，接入方接入）console 侧 outbox 全链**——跨 Redis 部署下
  `engine_sync` 只写 console 所连 Redis，「发布成功、worker 找不到 flow」，
  人工跑 `sync_published_to_engine.py` 是批处理兜底非实时。worker 读穿透
  （issue §2，plaita-go FlowSource 回源）归 B3 后续轮；B2 #300 的 publish
  校验逻辑未触碰（`api/flows.py` 零改动）。
- 修法四件：①`flow_publish_outbox` 表（`models/flow.py` + alembic
  `0003`，字段按 issue 建议 + 实现列 `next_attempt_at`；幂等键唯一约束
  `(tenant_id, flow_id, version, definition_hash)`），**publish 事务内**与
  flow_versions 同事务入队（SAVEPOINT 吸收幂等竞态；回滚零残留），删除
  流程/版本同事务 purge（防对账复活已删版本）；②投递 worker（main 后台
  线程，poll 默认 2s）：至少一次写 `{ns}:flow:{fid}:{ver}`（复用
  `sync_flow_to_engine` 写入面）+ **读回校验**存储原文
  `flow-definition-json-sortkeys-v2` 指纹才记 delivered；失败指数退避
  （2s→120s 封顶）→ 8 次转 `failed`（DLQ 档，ERROR 告警）→ 慢速重排
  （300s 翻倍封顶 30min）自动补投；③对账（启动首遍 + 每 300s，0=关）：
  delivered 缺 key/hash 不符 → 打回 pending 重投（预算重置）；④配置
  `PLAITA_CONSOLE_ENGINE_OUTBOX_TARGET_REDIS_URL`（空=整体关闭，同 Redis
  零负担零线程）+ poll/reconcile/max_attempts/batch/failed_requeue 五节律
  env（非法 WARNING+回退默认）；`/health` 新增 `engine_outbox`
  （status/pending/failed/last_delivered_at/last_error/当轮计数）。
- **开发期实锤并修正**：指纹必须按「投递后工件」（定义 + 注入
  flow_id/version）算——worker 校验的是 Redis 存储原文，按原始定义算读回
  校验永远不等（首跑 verify_hash_mismatch 实证）；`engine_payload` 收口 +
  与 `compute_flow_definition_hash` 等价性单测钉住。
- 负控（先红后绿，证据 `docs/proofs/data/r71-299-negative-control-{pre,post}.txt`
  + 脚本同目录）：pre=双 Redis 实例发布后目标缺 key（RED）；post=投递
  delivered=1、目标 hash 与工件一致（GREEN）。场景矩阵见
  `docs/proofs/issue299-outbox-cross-redis.md`（回滚零残留/幂等/DLQ→恢复
  补投/对账重投/守恒/已删版本不复活/租户命名空间/同 Redis 零负担）。
- 新增测试：`tests/console/test_engine_outbox.py` 18P（含 API 层发布端点
  → outbox → 投递全链 + 指纹等价 + hash 损坏拒投）；`test_schema_
  migrations.py` +4P（fresh create_all+stamp / 存量 stamp-baseline /
  0002→0003 upgrade 三路径都长出表 + 幂等）。
- 验收：console 全量 **1236P/13S**（基线 1215P/13S，零回归）；unit
  `-m "not slow"` **8836P/13S/3501 subtests**（与 r69 逐位一致）；e2e+
  integration **287P/4S**；定向 66P；ruff 新增代码零告警；
  `mkdocs --strict` rc=0。开发期自抓自修 1 个真 bug（成功分支 continue
  跳过行级 commit，delivered 状态未落库——被对账 checked=0 断言抓住）。
- 文档：ops-runbook 新节「跨 Redis 部署的发布投递链」+ console env 表 6 行
  + 非法值矩阵行；DOC_CODE_MAP 行 26 补 engine_outbox/flow_store/main.py；
  proof `docs/proofs/issue299-outbox-cross-redis.md`。
- 单 commit 关闭 #299；未触碰他席 duo-a-ledger/duo-a2-ledger/duo-b4-ledger。

## 第七十三轮 · #301/#302（outbox 删除投递语义 + 源表漂移对账，2026-10-22，B1 修复席；顺带收口 c117-06/b117-04/b117-03）

- 基线：`origin/main` `41984f1`（A1 r117 对抗轮基线，含我 r71 的 #299）；
  落地门 `landing_gate.py ci` rc=0（`a905dca` 主 CI + Console E2E 双绿）。
  本轮两单是 A1 r117 对抗产出同 seam 收口（#299 投递语义全家桶的补全）。
- **#301（P2）删除链路跨 Redis 半边清**：DELETE flow/version 的
  `remove_flow_*_from_engine` 全挂 console 侧 `get_redis_dep`，outbox
  `purge_for` 只删 DB 行——目标 Redis 的 `{ns}:flow:{fid}:{ver}` +
  `flow_list` 成员 + `flow_versions:{fid}` 注册集永驻（A1 实弹
  still_there=1）。修法：`engine_outbox.purge_target_keys()` 与投递**同源
  配置**（TARGET_REDIS_URL，复用 lifespan 缓存连接），复用引擎存储
  `RedisFlowStorage.delete_flow` 清三件（不另立清理逻辑）；挂钩
  `api/flows.py` 两个 DELETE 端点且**先清目标后删行**——删除事务失败
  回滚时已 delivered 行被对账重投自愈（反向排序会留无行覆盖幽灵）；
  尽力而为：目标不可达具名 ERROR 不阻断删除，行已 purge 后对账不再
  覆盖，runbook 写明恢复后人工清理。未启用（同 Redis）no-op。
- **#302（P2）源表 definition 被改三路全闭**：outbox 行内副本自洽投旧
  定义/对账用行内 hash 恒绿/幂等 publish 不补行（A1 实弹 target
  desc="outbox"≠console "OPS-EDITED" 永久漂移）。**口径拍板：console
  权威=源表现值，重投不锁行**（锁行会冻结分叉、违背最终一致承诺）。
  修法：对账每行**先对源表**——重读 `flow_versions.definition` 现值重算
  指纹与行内 hash 比对：分歧→按源表现值更新行内副本+指纹、打回 pending
  重投（`drift_count` 逐行累计 + `/health.drift_total` 聚合，迁移
  `0004_outbox_drift_count` 补列幂等；SAVEPOINT 吸收同键同 hash 竞态）；
  源表不可解析→保留旧指纹照常重投，投递期自检 `definition_unparsable`
  具名拒投进 DLQ（目标不被坏数据污染）；**源表行已删**（漏 purge 残留，
  含 A1 A5b 交错构造）→ 清目标三件+删行，`delete_flow` 吞异常只回 False
  不能作成功判据、以「目标 key 已消失」为收尾口径，清理失败保留行下轮
  重试。后对目标（#299 原口径）不动；对账统计新增 drifted/source_missing。
- **c117-06（c）DLQ 人工出口**：`POST /api/flows/publish-outbox/{id}/
  resolve`（置 `resolved` 终态留痕，追加 manually resolved by <actor>）/
  `.../discard`（删行仅审计）。resolved 不进投递扫描不进对账；delivered
  行双 409（resolve 无意义/discard 失对账覆盖）；跨租户/不存在 404；
  `/health.engine_outbox.resolved` 计数。
- **b117-04（P3）**：`_env_float` 加 `math.isfinite`——inf/nan 的
  `value < minimum` 恒 False 修前静默放行（poll=inf 投递链停摆），现与
  DNS_TTL 先例同口径 WARNING 具名+回退默认。
- **b117-03（P3）**：文档钉单副本约定（无 claim/leader；多副本重复投递
  幂等无害但 attempts 互覆失真）入 ops-runbook outbox 节。
- 负控（先红后绿，A1 探针同源复刻；证据
  `docs/proofs/data/r73-301-302-negative-control-{pre,post}.txt` + 脚本
  同目录；pre 对 A1 的 clean archive `/tmp/plaita-r117-tree`=基线树）：
  **pre 0/5 GREEN**（S1 still_there=1/S2 requeued=0 恒绿无信号/S3 无
  出口/S4 inf 静默放行）→ **post 5/5 GREEN**（三件全清/drifted=1+
  drift_total=1 按 OPS-EDITED 收敛/resolve 出队 scanned=0/inf→2.0/S5
  健康行零误报守恒）。proof `docs/proofs/issue301-302-outbox-delete-drift.md`。
- 新增测试：`test_engine_outbox.py` **+15P**（#301 三件全清/单版本只清
  己/API 层清/未配 no-op/不可达具名不抛；#302 漂移收敛/坏 JSON DLQ 可见/
  源缺失收尾/不可达保留行/健康零误报；c117-06 四态；b117-04 inf/nan/
  -inf）+ `test_schema_migrations.py` **+2P**（0004 fresh/0003→0004 升级
  幂等）；守恒测试断言更新到新统计形状。
- 验收：console 全量 **1253P/13S**（基线 1236P/13S，+17P 零回归）；unit
  `-m "not slow"` **8836P/13S/3501 subtests**（与 r69/r71 逐位一致）；
  e2e+integration **287P/4S**；ruff 改动文件零告警；`mkdocs --strict`
  rc=0；`landing_gate.py ci` rc=0。
- 文档：ops-runbook「跨 Redis 部署的发布投递链」节扩四块（删除投递
  语义/对账扩口径/人工出口/单副本约定 + drift_total/resolved 字段）+
  env 表 2 行（inf/nan 拦截）+ 非法值矩阵行；DOC_CODE_MAP 行 26。
- 单 commit 关闭 #301/#302（c117-06/b117-04/b117-03 顺带收）；未触碰
  他席 duo-a-ledger/duo-a2-ledger/duo-b3-ledger/duo-b4-ledger。

## 第七十四轮 · CI 红修复（迁移图双 head 线性化，2026-10-22，B1 修复席；任务书标称第六十八轮续作——74 为本文件下一个空闲编号）

> 一行摘要：**main CI 三连红根因=迁移图双 head（真 bug，非测试竞态）**——#299
> 的 `0003_flow_publish_outbox` 与 #300 的 `0003_flow_version_runtime` 并发落地
> 各自以 `0002_audit_identity` 为父，head 集={0003_flow_version_runtime,
> 0004_outbox_drift_count}；二次启动 `run_migrations` 走 `upgrade` 分支
> `command.upgrade(cfg, "head")` 抛 `MultipleHeads`，被 `init_engine`
> 兜底（非连接类错误）降级 `create_all-fallback`（schema 停止补列/推进，
> `flow_store.py:1535-1537`）。CI 39739418521 chunk B 三 attempts 同断言
> `assert 'create_all-fallback' == 'upgrade'`，与本机基线 8d715f7 临时
> worktree 复现逐字吻合（4F/7P，四失败同根因：second_boot/idempotent_
> many/stamped_db_upgrades_outbox/stamped_0003_upgrades_drift）。
> **修法=线性化**：`0003_flow_version_runtime` 改挂 `0003_flow_publish_outbox`、
> `0004` 改挂 `0003_flow_version_runtime`（0001→0002→0003p→0003v→0004 单链）；
> 两迁移均幂等 ADD COLUMN（列/表存在性守卫 + server_default），对已 stamp
> 任意版本的库（含双 head 期间停在 0001/0003p/0004 的生产库）重挂安全收敛。
> 新增回归 `test_migration_graph_has_single_head`：分叉在图层面拦下，
> 不等启动路径出 fallback 谜语。**负控**：基线（双 head）+ 新测试 = 5F/7P
> 精确红（新测试报双 head，原 CI 红面全数复现）；修复树 12P。证据
> `docs/proofs/data/r74-ci-red-negative-control-{pre,post}.txt`。
> 验收：console 全量 **1272P/13S**；unit **8847P/13S/14 deselected/3501
> subtests**（pytest_rc=0 无管道判定）；integration+e2e **287P/4S**（CI
> 同参 --timeout=150）；ruff 改动文件零告警。**环境注记（既有、非本轮）**：
> 本机 .venv 未装真实 mcp 时 `pytest plaita-ai/tests` 全目录跑出 1F
> （test_mcp_stdio_smoke 握手）——`test_mcp_integration` 的 `_inject_stub_mcp`
> 向 sys.modules 注 stub 污染 smoke 文件的 `_REAL_MCP` 探测（子进程真导入
> 必炸）；基线 8d715f7 同样 1F，CI（装真包）success——测试隔离缺陷候选单，
> 未提报待拍板。**教训复踩警示**：`pytest | tail; echo $?` 取的是 tail 的
> 退出码，第一遍 unit 判定无效、以输出落文件读 `pytest_rc` 重跑纠正。
> **终态**：推送 `c83229d` 后 main CI + CI slow lane + Console E2E 三线
> **success**；`landing_gate.py ci` **rc=0**（✅ main CI 绿 ✅ Console E2E
> 绿 ✅ 允许落地）——三连红熔断解除，B2/B4 在途交付恢复落地。

## 第七十五轮 · #303（outbox 删除协议对偶破口——deleting tombstone + 互斥串行化，2026-10-22，B1 修复席）

- 基线：`origin/main` `1dd62a3`（开工 pull 自任务书基线 `bfd0b62`，B2 r74 两单在途已合入）。
- 机制（A1 r119 实证）：#301「先清目标后删行」防住了「清理失败留幽灵」，防不住对偶方向——DELETE 与投递/对账线程无同步点：delivered 行被对账打回 pending → DELETE 清 key → 投递 poll 重投复活 key → 删行 purge → **key 在/行无/对账永不覆盖/零日志**；对称窗=投递线程已在 `_iter_due_rows` 读出行、DELETE 全程完成后写出 key（同构后果）。
- 修法（**口径一：删除完成后目标侧不复活**，二选一拍板已写文档）：行级 tombstone + 进程内互斥双保险——`engine_outbox` 新增 `STATUS_DELETING` + `OUTBOX_DELETE_LOCK`（RLock；「单副本约定」b117-03 延伸）；`deleting_guard()`＝锁内标记 deleting（独立事务提交）→ 清目标三件 → 删除事务（purge 行）→ 异常按 (id, 原状态) 快照恢复原状再上抛（404 语义不变）；api/flows.py 两个 DELETE 端点全程入守卫；投递/对账逐行动作前持锁 `_reload_row` 重读行现态（deleting/已删行跳过，对账改 id 扫描+逐行提交，stats 增 `skipped`）；resolve/discard 拒 deleting 行（409 语义）；`stats_snapshot` 增 `deleting` 计数；main.py lifespan 启动恢复 `recover_deleting_rows`（崩溃死在「已标记、未删行」→ 打回 pending 交还常规自愈）。**#301 自愈方向保留**：回滚 → delivered 恢复 → 对账重投收敛（有专测钉住）。
- 负控先红后绿：新回归 `tests/console/test_issue303_delete_vs_outbox_race.py` **10 用例**——pre **10F**（A1b API 层 seam 注入重放：`{'delivered'} == {'deleting'}` 精确红 + 删行前对账重投日志实拍「outbox 对账重投：race@1.0.0 目标 缺 key」；对称陈旧读窗；真实线程在途投递持锁时删除先行不等待）→ post **10P**；A1 探针 post（`/tmp/r119_outbox_probe_postfix.py`，唯一差异=保真补守卫步、A1b 重放交错不变）：`status_between=deleting / target_key_after_all=0 / row_left=0` 零复活。证据 `docs/proofs/data/r75-outbox-delete-race-negative-control-{pre,post}.txt`。
- 守恒：既有 `test_engine_outbox` 33P（stats 契约增 `skipped` 键，更新一处全等断言）；console 全量 **1296P/13S**（前值 1285+本轮 10 新增+1 修改，零回归）；e2e+integration **287P/4S**（CI 同参 `--timeout=150`）；unit **8847P/13S/14 deselected/3501 subtests**（与前值全等）；ruff 改动文件零告警；mkdocs `--strict` rc=0。
- 文档：engine_outbox 模块 docstring「删除协议串行化（#303，口径一）」节 + FlowPublishOutbox 状态机补 deleting + ops-runbook 删除投递语义节改写（#301/#303）+ `/health.engine_outbox.deleting` 字段说明。
- 教训：负控测试自身的准备顺序 bug（投递轮在 resolve 之前跑导致 1.0.1 已 delivered）被 post 首跑抓住——fixture 顺序也是被测语义的一部分。
- **终态**：推送 `14529b6` 后 main CI + CI slow lane + Console E2E 三线 **success**；`landing_gate.py ci` **rc=0**（✅ main CI 绿 ✅ Console E2E 绿 ✅ 允许落地）。#303 关单回执另附。

## 第七十六轮 · #304（published 版本删除零守卫连锁——删除前置守卫 + 定义缺失具名终态 + start 具名拒绝，2026-10-22，B1 修复席；任务书标称第七十一轮——71～75 已被占用，按序列顺延）

- 基线：`origin/main` `b710ed0`（开工 fetch 时任务书基线 `bfd0b62` 已过时；同 clone 他席 B3 r80 在途，其 ledger WIP 未触碰）。
- 机制（A2R49-01 实弹）：`delete_version`/`delete_flow` 对 published 版本零状态/在途检查，删行同步清引擎定义键 → worker `resume_flow`（钉死版本、租约/try 之前）取定义抛裸 ValueError → run() poison-ack → 挂起执行永久挂起；start 侧 `_resolve_start_version` None 照常入队 → 幽灵 queued 行 + start 准入名额不释放；删最新 published 静默改靶旧版。对照面（published 覆盖 409 / 非终态执行删除 409 A43-01）有守卫，唯独版本删除裸奔。
- 修法（**口径：状态守卫可 force，引用守卫不可越**，二选一拍板落档 `api/flows.py` 守卫节 + ops-runbook #304 专节），三面并用：
  ①**删除前置守卫**（`_enforce_version_delete_guard`，delete_flow/delete_version 共用，全程在 #301/#303 删除协议与引擎键清理**之前**）：published 默认 409 `version_published`（与不可覆盖同口径；`?force=true` 显式逃生）；非终态执行引用（走租户执行索引 `RedisExecutionStorage.list_executions`，default/"" 同命名空间无别名漏扫）→ 409 `version_in_use` 附清单（≤20 条+total），**force 不可越**——级联取消刻意不采用（取消是异步意图，删行清键后取消路径自身还要定义键收口，竞窗造新孤儿；「先取消→待终态→再删」唯一无窗口序）；扫描不可达 503 `version_in_use_scan_failed`（fail-closed）；本地档（无 Redis）引用判据退化为状态守卫。
  ②**worker 具名终态**：`FlowDefinitionMissingError(ValueError)`（继承保 poison-ack 语义）+ `_terminalize_definition_missing`（status=error + `error_type=flow_definition_missing` 机器可判 + `_persist_state_or_raise` 单一收口释放配额名额；幂等不覆写终态；G1 error 可 retry 唤醒保留）；start/resume 三处接线（start_flow 首行 / resume_flow 租约前 / 租约内 #247 复核），租约内路径补专属 `except FlowDefinitionMissingError: raise` 防通用 except 二次终态化。租约前终态写安全性：定义缺失时任何 worker 都无法推进，此写要么生效、要么被仍持缓存定义的活 worker 推进覆写（后者证明执行未孤儿化，无害）。
  ③**start 具名拒绝**：`StartVersionNotStartable`——显式 version 不存在/非 published → 404 `version_not_found`（detail `exists` 区分）；存储读失败 fail-open 本体保留（`_runtime_queue_for_flow` 全捕获回落默认队列语义不变）；latest 无 published 响应带 `warnings`（不阻断）；正常解析响应新增 `version` 回显（删新版改靶可直接读出）。
- 负控先红后绿：新回归 console `test_issue304_version_delete_guard.py` **11 用例** + unit `test_issue304_definition_missing_terminalize.py` **5 用例**。pre（**基线 worktree `7030e78`，不用 stash**）：console **7F/4P**（4P=零回归钉子）+ unit **4F/1P** → post **16P 全绿**；A2 探针实跑对照（ROOT 指工作树）：P1a/P1d/P1e 族 FAIL=修复面翻转（409/404 具名拒绝）、P1d-3b 对照面（published 覆盖 409）保持 PASS。证据 `docs/proofs/data/r76-issue304-negative-control-{pre,post}.txt` + `r76-issue304-a2-probe-postlive.txt`。
- 既有测试语义升级一处（#304 刻意变更面）：`test_issue300` unknown-flow 钉子拆二——显式版本 404 `version_not_found`（原 200；原注释自证「本单不扩 scope」，本单即扩此 scope）+ latest fail-open 200 带警告；#301/#303 两个 outbox 测试 DELETE 加 `?force=true`（本测无在途执行，引用判据放行）。
- 守恒：console 全量 **1312P/13S**（rebase 后含 B3 r80 新增 4 用例；前值 1296 + 本轮 11 + #300 拆分 +1 + B3 +4，零回归）；unit **8927P/17S/226 deselected/3501 subtests rc=0**（计数与 r75 前值不可直接比——r75 后 main 又进他席用例；判定以 rc 为准）；e2e+integration **287P/4S**（按目录口径，与 r75 全等；CI 同参 `--timeout=150`）；ruff 改动文件零新增（pre/post 均 26 条既有 BLE001）；mkdocs `--strict` rc=0（本机 pyenv 缺 3.13 须 `PYENV_VERSION=system PYTHON=python3` 绕过）。
- **落地熔断插曲（B1 顺带修 main 红）**：首轮 landing_gate 红——`b710ed0`（B3 r80）CI failure。根因：`TestRealGoWorkerRegistration` 的 `sql_query` 节点由 **plaita-nodes 子仓**经 `plaita.nodes` entry-point 注册，不在 core、不在 console pip 依赖面——B3 本机（editable 装 plaita-nodes）全绿，CI 三连红（save 422 → publish 404 的静默链）。修法：该测试类按 `importlib.util.find_spec("plaita_nodes")` skipif（`365b7da`，测试本体零改动）；本地 chunk B CI 同形复跑 448P 全绿 + 干净 worktree 复现核实依赖链后落。教训：**entry-point 插件注册的节点 = 环境依赖面**，console 测试用例选节点前先问「CI 装不装」。
- 教训（复踩防范）：后台长跑 `pytest | tail` 会把 pytest 退出码换成 tail 的、统计行被截——判定要无管道或 rc 落盘（本轮 unit 首跑因此重跑）。多席同 clone：负控 pre 用基线 worktree + 只拷测试文件，不用 stash；他席 ledger WIP 全程不碰。
- **终态**：推送 `e41b0c3`（#304 修复主体）+ `365b7da`（CI 熔断修复）+ `bd2f6d6`（tenants e2e force）。`365b7da` 的 CI 跑被 B3 r81 推送顶掉（并发组取消非红）；**全绿终态实现在 `c0c8074`**——main CI + Console E2E + CI slow lane 三线 success，`landing_gate.py ci` **rc=0**（✅ main CI 绿 ✅ Console E2E 绿 ✅ 允许落地）。E2E 场景面对齐（executions 自备已发布 flow + 显式版本 404 钉子 / tenants·flows-versions force / contract teardown force）由并行 B1 席立 **#306** 接续收口（`c6ea308` executions 为本席、`bd2f6d6` tenants 为本席、其余 `c0c8074` 为并行席）。#304 关单回执另附（同 clone 双席接力实录：我方工作树未提交的 flows-versions 修复由并行席拾起入库，台账 r76 条目亦由其补提交——两席零冲突，条目唯一）。

## 第七十七轮 · #306（#299/#304 落地后 E2E 四红场景跟进——产品代码零改动，2026-10-22，B1 修复席；任务书标称第七十二轮——72～76 已被占用，按序列顺延）

- 基线：`origin/main` `2010604`（开工时）；推送时 main 已被同任务书并行会话推进至 `bd2f6d6`（见下）。
- **双会话碰撞与打捞（本轮特殊态）**：同一 #306 任务书有另一会话并行施工——19:47 executions 提交 `c6ea308` → 19:58 tenants 提交 `bd2f6d6` 并推 main → 20:01 flows-versions 改动落树后中断（无进程、无台账、单未关）。本席接续：审两提交（与 A1 r121 逐项方案一致）+ 接管其在树未提交的 flows-versions 改动 + 补齐缺口（contract teardown、台账、关单）。
- 四红根因（CI `365b7da` **293P/4F/12S**，与 #306 诊断一致）：①executions start 打全仓无 seed 的 e2e-test-flow@1.0——修前靠 #304 收掉的「None 幽灵入队」拿 200，修后具名 404；②tenants/flows-versions 清理 DELETE 撞 #304 新守卫 409 `version_published`；③contract 503 用例挂是②的级联（tenants 清理断链→e2e-qa 契约租户残留→ui-backend `has_tenant_secret=True`→503 分支失效变 401）；sequential 级联 skip 12 例。
- 修法（**场景面，产品代码零改动**）：executions 自备已发布 flow（`c6ea308`：create/save/publish e2e-exec-flow@1.0.0 + version 回显钉子 + 显式不存在版本 404 `version_not_found` 负向钉子；**seed 必须落 1.0.0 不能「1.0」**——PUT semver 校验 422「1.0」且 start 精确匹配无归一化）；tenants 两处清理 DELETE 补 force（`bd2f6d6`）；flows-versions「删除整个流程」拆二——无 force 默认 409 `version_published` 守卫钉子 + `?force=true` 正删（在树改动接管，`c0c8074`）；contract teardown DELETE 补 force（ignoreError 下 409 已使清理静默失效；teardown 不进 totals，纯恢复清理语义）。**#304 口径核对：状态守卫可 force、引用守卫不可越——三 flow 清理时点均无非终态执行，force 合法**（探针实弹：published+执行 completed 后无 force 删除 409，force 200）。
- 可选项③（contract 503 自洽加固）评估后**不做**：修②即自愈（mid/post contract 10P）；真隔离须 contract 套件管理跨 suite 租户态，拓扑级改动超 #306 场景跟进边界。
- 负控链：CI `365b7da` 四红锚 → 本地 mid 态（executions/tenants 已修）**313 例 flows-versions 409 存活复现** → post 全量 **314P/0F/0S rc=0**（+4 executions +1 flows-versions 拆分；原 12 级联 skip 全部真跑：tenants 清理链+租户删除、flows-versions 引擎侧 redis-cli 联动断言；其余 14 suite 计数与 CI 全等零回归）。证据 `/tmp/r77-e2e-{mid,post}.log`。
- 诊断插曲：mid 态 tenants 清理「意外绿」与 CI 红相反——ui-backend 容器日志实证请求带 `?force=true`（`DELETE /api/flows/e2e-tenant-flow?force=true 200 OK`），根因即并行会话的在树改动已被 mid 态跑到；「读数与预期相反先查树是否在移动」。
- 插曲（环境）：本机 Docker daemon 拉镜像 EOF——Docker Desktop 代理指死端口 7890（活端口 7897/7891），`settings-store.json` OverrideProxy 改 7897+重启恢复（备份 `/tmp/settings-store.json.bak-r77`）；E2E 镜像曾被清盘，mid 态带 build 全量重建（**场景 yaml 由宿主 argusai-mcp 运行时读取、不进镜像——一次 build 服务修复前后两态**）。
- 观察遗留（不在本单动）：engine 系/auth 套件多条 `ignoreError` 清理 DELETE 在 #304 后对 published 流程静默 409、清理失效（无跨 suite 残留——各 id 套件私有、每轮 env 全新）；若日后要求「清理必须真清」，按同法补 force。
- **终态**：推送 `3bd662c`（r76 台账补记，前会话完成态原样入账）+ `c0c8074`（本单收口）；Console E2E 于 `c0c8074` **success**（bd2f6d6 红→绿），main CI + slow lane 全绿，`landing_gate.py ci` **rc=0**。#306 关单回执另附。

## 第七十八轮 · #305 Phase 1（POST /api/graphs/validate——编辑器 GraphDTO 结构化校验 + publish 门共享实现，2026-10-22，B1 修复席；任务书标称第七十三轮——73～77 已被占用，按序列顺延）

- 基线：`origin/main` `4909e3b`；推送时远端已被并行席位推进至 `4c8c56e`（B2 r74-76：executions start/准入双语义合成 + #300 触发面池路由 + 前端 nodePool），与本轮文件零相交，rebase 干净后落 `f327ee2`。
- 契约（GraphDTO）：输入=前端 `components/flow` @xyflow 编辑器序列化形状，权威实现 `flowConverter.ts`；服务端镜像 `services/graph_validation.py::graph_to_definition`（折叠规则逐条对齐：if true/false→next/else_next、switch|case handle→branches[i].next、线性首条出边→next、`parentId`/`isContainerChild` 容器子节点跳过〔已双写回 owner childFlow〕、assignment 缺 outputType 注入引擎默认、`meta` 平铺 flow_id/version/desc/author/inputType/outputType/globalContext/metadata/timeout）。契约文档=模块 docstring + `plaita-console/docs/graph-dto.md`；前端未改（Phase 1 无前端类型入树，tsc 不适用）。
- 共享实现（硬要求）：图内容校验**直调**保存/发布面两个权威函数，不写第二份——字段 schema=`services/validation.validate_definition`（保存/导入同函数，内核 Flow.model_validate），能力门=`capability_gate.gate_flow_nodes` 本体（#300 publish/start 同函数）。一致性契约双向矩阵钉住：V⇒P（validate 过→用其转出 definition save+publish 必 200）；P⇒V（publish 422 ⇔ validate capability 错误带 `runtime/unsupported/supported` 与 #300 detail 同键；save 422 ⇔ validate `invalid_definition` 同因文本）。validate 额外做编辑器图结构/连线校验——比 publish 严是设计内不对称（不对称行同时钉住 save/publish 仍 200：「发布得了但运行期才炸」的图在保存前拦）。
- 校验面：结构（显式 start 单入口/多 start 死节点、从 start 可达终端必须 end〔引擎 strategies 缺 next 硬失败〕、环=引擎 `_resolve_cycle` 回边判据〔#105 汇聚不误报；end 视为绝对终点不沿出边〕无 timeout=error/有 timeout=warning、孤儿逐节点、子图 childFlow/parallel 分支同判据、if 缺真/假分支、switch 缺 isDefault、容器类缺 childFlow——与 ir_validate 硬编码集同判据）+ 连线语义（悬空 target、if handle∈{true,false}、switch/case handle∈branches[].name、线性多出边、end 出边=死边 warning）+ 字段（catch-all invalid_definition + 逐节点 parse_node/node_validate + ir 规则〔$NODE/$F/joinBranches/childflow 收尾/outputType 预检〕warning）+ 能力门（池无清单 fail-open 同口径 + 响应内 `pool_manifest_missing` warning，本地档 redis=None 跳过）+ reference 引用按请求租户解析存在性（不跨租户借用、文案不泄露他租户）。
- 错误结构：`{type: node|field|edge|capability, code, node_id?, field?, message, severity}` 全量累积（非首错即停）；error 级→422 `invalid_graph`（detail 带全量 errors+definition），warning 不阻断 200。鉴权=既有会话/API key（`_mount_admin(graphs.router)`），租户照 `tenant_scope(required=True)`；embed token 留 Phase 3。
- 负控先红后绿：新回归 `tests/console/test_issue305_graph_validate.py` **39 用例**。pre（基线 worktree `4909e3b` 只拷测试文件）：pre-A 基线原样收集期 ImportError rc=2（模块不存在）；pre-B worktree 内放空 router 壳让断言执行→**39F/0P**（全部对着 404 面翻红，断言非空转）→ post **39P/0F** rc=0。四类各≥2 具名（node：缺/多 start、孤儿、重复 id、非 end 终端、环±timeout、菱形汇聚不误报环、子图缺 start；field：未知类型、assignment 缺 output、end.error 非对象、flow 级 inputType 非法；edge：悬空、handle 不在分支、线性双出边、if 未知 handle、if 双真分支、if 缺假分支、end 出边 warning；capability：map/while vs go 池、池无清单 fail-open、本地档跳过）+ 双向矩阵 7 行 + 多租户 3 例 + 鉴权 1 例。证据 `docs/proofs/data/r78-issue305-negative-control-{pre,post}.txt`。
- 判据修正（实现期自证）：①end 的出边曾造假环（a→end→a）——运行期 `next_node(end)` 恒 None，环/可达性邻接把 end 当绝对终点，死边由 `edge.end_outgoing` warning 提示；②`end.resultType` 非法值被引擎静默归一 success（warning 非拒绝面），pydantic 级拒绝面换 `end.error` 非对象；③if 的 condition 是结构化 Condition（field/operator/value）不是表达式串（json.loads）。
- 守恒：console 全量 **1360P/13S rc=0**（rebase 后含 B2 新增；本轮 +39）；unit **8927P/17S/226 deselected/3501 subtests rc=0**（与 r76 前值逐项相等）；integration+e2e **287P/4S**（CI 同参 `--timeout=150 -m "not slow" --timeout-method=thread`，与 r75/76 全等）；ruff 改动文件零告警。
- **终态**：推送 `f327ee2` 后 main CI + CI slow lane + Console E2E 三线 **success**；`landing_gate.py ci` **rc=0**（✅ main CI 绿 ✅ Console E2E 绿 ✅ 允许落地）。#305 **不关单**（四期 Plan 之 Phase 1；Phase 2 generate API 排下一轮），进度回评已附。

## 第七十九轮 · #307（#304 删除守卫引用判据两处失效——翻页穷尽 + 扫描 strict fail-closed，2026-10-22，B1 修复席）

- 基线：`origin/main` `f06755e`（与任务书一致；开工 git status 仅他席未跟踪台账三件，无在途代码）。
- 机制（A2R50-01 实弹，探针归档 `~/.issue-keeper/duty/a2-r50-scripts/`）：r76 落地的 `_enforce_version_delete_guard` 引用判据两处失效——①`find_flow_version_active_executions` 调 `list_executions(query=...)` 未传 limit，存储层默认 100 且 start_time **升序**，忙 flow（30 天 TTL 内 cron 即积 >100 条终态）尾部挂起执行不可见 → `delete_version/delete_flow?force=true` 200 放行（对照 50+1 正确 409，边界恰 100）；②`list_executions` 整体 except→[]，ping 通过后的批量读故障被判「无引用」放行，违反 #304 自身 503 fail-closed 契约。
- 修法（判据一个函数收口 + 存储层 strict 模式）：①判据**翻页穷尽**（显式 limit+offset 循环至短页/空页、收满 limit 提前停；列表端点分页语义不动——#307 边界）；②`RedisExecutionStorage` 读路径增 `strict` 参数，守卫路径异常一律转 `VersionInUseScanError` → 503。**strict 须穿透三层吞点**：`list_executions` 外层、`_ensure_execution_index`（整批失败→零更新降级 + 单键/坏行→部分索引降级——**首版只加外层实测 3F 不翻绿**，异常实际死在回填层 catch-all，证据 `r79-issue307-intermediate-outer-strict-only.txt`）、`_load_indexed_states` 坏行跳过。同族调用点核对：fenced/tenant_context 显式透传、reap_zombie 自行翻页（空页停）、ops 走 HTTP 分页——「默认参数当全量」仅守卫一处。
- 负控先红后绿：新回归 `tests/console/test_issue307_delete_guard_scan_limits.py` **9 用例**——pre（基线树直接跑）**7F/2P**（120+1 双端点 200 放行、边界 101 窗外一格、异常直读+端到端 fail-open、strict 缺失 TypeError；对照格=窗口内 100 条 409 绿 + 无引用 force 200 绿）→ post **9P** rc=0；#304 既有守卫 11 + 终态 5 用例合跑 **25P 零回归**。证据 `docs/proofs/data/r79-issue307-negative-control-{pre,post}.txt` + `-issue304-regression.txt`。
- 守恒：console 全量 **1369P/13S rc=0**（基线 collect-only 1373 = 1360P+13S 与 r78 台账终值吻合，+9 全为本轮；差额 9 曾疑记账漂移，stash 往返 collect-only 定案）；unit **8861P/13S/14d/3501 subtests rc=0**（**树真实测**——r76/r78 台账 8927P/226d 为旧树 carried 值，现树 slow 标记仅 2 文件 14 条，subtests 3501 全等可互证）；integration+e2e **287P/4S**（CI 同参，与 r75/76/78 全等）；ruff 改动文件零新增（两文件 30→25 既有 BLE001，新文件 0）；mkdocs --strict rc=0（配置在 docs-site/ 子目录）。runbook #304 节扫描语义同步修准（翻页穷尽 + 503 覆盖探活通过后的扫描故障）。
- 教训：**「在建议处加开关」不等于「修到位」**——异常注入负控必须实跑翻绿才算数，首版 strict 只盖外层时 3F 红的就是更深的吞点；多层降级结构里 fail-closed 要按「读面全链」审计吞点，不是按调用点。
- **终态**：推送 `cc5d1fc`（修复）+ `20eaeac`（台账）后 main CI + CI slow lane + Console E2E 三线 **success**；`landing_gate.py ci` **rc=0**（✅ main CI 绿〔20eaeac〕✅ Console E2E 绿〔cc5d1fc〕✅ 允许落地）。#307 关单回执另附。**运维注**：gate 首跑两连 rc=2「无已完成 run」为 run 完成瞬间 API 列表索引瞬态（gh 同款命令 shell 直跑有数据、子进程偶发 `[]`，env 三组对照全正常排除 PATH/token 因素，稍候重跑即绿）——gate 红前先重跑一次再归因。

## 第八十轮 · #309（graph validate 缺形节点静默丢失 + 展开容器子边误报 + A2R51-01 reference 一致性——#305 Phase 2 接线前两面加固，2026-10-22，B1 修复席）

- 基线：`origin/main` `d3d3c12`（与任务书一致；开工 git status 仅他席未跟踪台账三件，无在途代码）。issue 补记并单的 A2R51-01 按任务书一并消化；探针 `/tmp/r124_probe_graph_validate.py` + `~/.issue-keeper/duty/a2-r51-scripts/` 全数复用。
- 三面机制：
  - ①A124-01（缺形节点零报错剔除）：`_top_level_nodes` 四类按「缺失」折叠（非 dict / data 非 dict / parentId 非空 / isContainerChild）——r124 实证 valid=true 且折叠定义缺节点=照此保存无声丢失。新增 `shape_errors`：凡非「合法容器子节点」（parentId+isContainerChild 双标记+宿主在图中）的剔除一律 `graph.malformed_node`（type=node，severity=error→422 保存被拦），另覆盖「双标记齐全但宿主不在图」陈旧 ownerId 面；标记宽容读 node/data 两层（Phase 1 `test_container_children_skipped` 放节点层、编辑器 flowConverter 产 data 层，两者并存不破守恒）；合法展开子节点保持静默跳过（②前置）。
  - ②A124-02（展开容器子边误报 unknown_source）：前端 jsonToGraph 子边进同一 edges 数组 × 服务端端点全集=顶层 id → 展开态容器图 validate 必 422。`edge_errors` 先取 `_container_child_ids`：子↔子内部连线跳过（已双写进 owner.childFlow，折叠只匹配顶层 source——与折叠规则对称）；子节点×其他端点混连保持拒绝（child 出边=折叠丢边、指入 child=悬空 next，后者病因文案改准「容器子节点：折叠后悬空 next」）。
  - ③A2R51-01（P2 并入，**选型台账方向①validate 层形状真相**）：(a) reference 既无 flow_id 也无 childFlow → `reference_target_unspecified` error——「交字段 schema 校验报」的兜底层不存在实锤（flow_id Optional + ir_validate 对 reference 豁免 childFlow 检查，本机复核 child.py 坐实）；(b) `normalize_runtime(None)→'python'` 实测 × python 池 reference 零可执行形状（named-ref 全仓无 flow_id 注入器 + `ReferenceFlow.setup_child_flow` 覆写父类丢 childFlow 解析，child.py:40/:106/:127 三点核实）→ `reference_unexecutable_on_runtime` warning（可观测不阻断，go 池不发）。**选型理由**：验收格即按方向①写；不动 manifest（会翻转 Phase 1 三个正当测着陷阱本身的 reference 用例+波及 publish 门全线）、不动 schema（无 runtime 视角、误伤 go 池合法 named-ref）、方向③引擎注入器工程量最大——方向②③为遗留，`_REFERENCE_DEAD_RUNTIMES` 常量注释钉证据（引擎补齐后摘）。save/publish 面不对称留证（无目标 reference 定义 PUT/publish 仍 200=R51-A save 格口径）。
- 负控先红后绿：新文件 `tests/console/test_issue309_graph_validate_hardening.py` **16 用例** + #305 双向矩阵 `TestConsistencyMatrixIssue309` **4 行**（缺形 422 同因=折叠丢失实证 / 展开容器 save+publish 200 V⇒P / reference 无目标 422⇄save+publish 200 不对称 / named-ref@python valid=true+warning 面钉）。pre=**d3d3c12 基线 worktree 装终版夹具实跑 14F/6P**（6P=4 对照格〔orphan/collapsed/child→top/save 不对称〕+2 断言不存在物的范围控〔inline 非unspecified/go 无 warning〕）→ post **59P/0F**（309 文件 16 + 305 文件 43=39 既有+4 追加）；r124 探针复跑 RED×3 具名翻红（node_id 指名、不再静默消失）、RED-4 valid=true、GREEN×2 守恒。证据 `docs/proofs/data/r80-issue309-{pre-red,post-green}.txt`。
- 教训：负控夹具三处形状笔误（isContainerChild 误放 fields 层〔应在 data 层〕/`_errors_by_code` 只认 422 detail/矩阵底图 id 错位）——**首版 pre 红证据混入夹具伪影（16F/4P），终版夹具在基线 worktree 重取 14F/6P 才是规范负控**；「pre 红得有理」必须核对夹具形状与终版一致。
- 守恒：console 全量 **1386P/14S**（+20 全为本轮；**2F=a2r19 metrics 降级标记面，干净树 stash 往返复现=基线既有环境敏感红**，与本轮无关）；unit **8932P/17S/226d/3501 subtests**（两跑 3F/4F 集不同=环境敏感；**4F=test_r53_dlq_trim×3+test_issue159 policy sink，干净树 stash 复现=基线既有**；与 r79 台账 8861P/14d 的差额为记账 scope 差异非本树回归——本树两跑 8933/8932P 稳定）；integration+e2e **287P/4S**（CI 同参 `--timeout=150 -m "not slow" --timeout-method=thread`，与 r75/76/78/79 全等）；ruff 改动文件 0 告警；mkdocs --strict rc=0（graph-dto.md 契约文档同步三面新错误码与子边免报语义）。
- 政策：新增 `_REFERENCE_DEAD_RUNTIMES` 行为常量 + 两具名 error 语义命中 policy-touching 定义，提交带 `[policy]` + `Policy-ack: jeffkit 2026-10-22`（#309 即 owner 令），`landing_gate.py trailers` 自检 rc=0。
- **终态**：推送 `b5fa291`（修复）+ `2420c9b`（台账）后 main CI + CI slow lane（8m3s）+ Console E2E（7m7s）三线 **success**；`landing_gate.py ci` 终态复核 **rc=0**（✅ main CI 绿〔2420c9b〕✅ Console E2E 绿 ✅ 允许落地）。#309 关单回执已附（三面分别引证据+方向①选型理由）。#305 Phase 2 generate API 排下一轮。

## 第八十一轮 · #305 Phase 2（POST /api/graphs/generate——意图→GraphDTO 草稿，generate→validate→回喂闭环 + A128-01 内容级自检，2026-10-23，B1 修复席）

- 基线：`origin/main` `1920f06`（与任务书一致；开工 git status 仅他席未跟踪台账三件，无在途代码）。
- 契约（owner 拍板四期 Plan 第二期，Phase 1 `f327ee2` 的生成侧镜像）：入参=prompt（自然语言）+可选 hints（`preferred_node_types` 节点类型偏好 / `reference_flows` 租户内可引用子流程清单，注入生成 prompt）+runtime（缺省 python，能力门判定池）；出参=GraphDTO 草稿+`generation` 元数据（model/duration_ms/attempts/usage 跨轮累积）。**纯函数式不落库**——保存仍走宿主既有 save（负控有「生成后 list_flows 为空」钉子）。
- 生成闭环（`services/graph_generation.py`）：LLM 单发补全 → 必过 Phase 1 `validate_graph`（同一入口）→ 不过则 errors **原样**回喂自纠（有界 3 轮，ai_flow 先例）→ 用尽仍不合法 200 `valid=false` + errors[] 原样带回（含 capability 错误的 runtime/unsupported/supported 同键保真）；`generation.unparseable_output`/`generation.provider_error` 属可回喂生成面错误。模型接入=pydantic-ai Anthropic 端点，凭证走 flowcast providers.json（与 copilot flow_agent 同源、后端不落密钥，`GRAPH_GEN_PROVIDER/_MODEL` 可覆盖）；**窄域无工具单发补全，非 #292「spawn 宿主大脑」级**，鉴权照 Phase 1 validate 同口径（editor+ 写闸 + `tenant_scope(required=True)`）；测试/宿主经注入式 `complete` 打桩零出网（fixture 把 `default_model` 打成 assert 炸弹防误触达）。
- **A128-01 内容级自检**（A1 r128 预判消化；generate=首个非编辑器 GraphDTO 产源）：`graph_validation` 新增 `childflow_double_write_errors`——折叠豁免子节点（parentId+isContainerChild）不核内容已双写 owner.childFlow 的信任缝，validate 面保持限级（Phase 1 59P 不动）；generate 出草稿前逐投影抽查：宿主非容器类（无 childFlow 义务，validate 恰好不报=静默缝）/双写副本缺 id（**含编辑器 `owner::id` 命名空间后缀匹配**——jsonToFlow 递归展开给子节点 id 加 owner 前缀，投影 id≠双写 id 是编辑器常态而非伪造）/type/共享字段值/branches（剥 next）不一致，任一命中具名 500 `generation.childflow_not_double_written`（路由转具名 500），**不出草稿、不进回喂**——生成管线形状不变量被击穿要响亮失败。容器类宿主无 childFlow 载体的形态不在自检域（validate 具名 `node.child_flow_missing`，非静默缝，走带回面）。**自检面放在 graph_validation.py**（GraphDTO 契约的家，与 shape_errors 对称），generate 是第一个消费方。
- 负控先红后绿：`tests/console/test_issue305_graph_generate.py` **23 用例**（契约 6：200 形状/空 prompt/非法 runtime/未知字段/不落库/注入缝；回喂闭环 6：二轮收敛〔第二轮 prompt 含 graph.no_start〕/耗尽原样带回/capability 错误保真/围栏解析收敛/恒不可解析/死边 warning 不阻断；A128-01 六格：ghost 投影 500 无草稿/childFlow 缺 twin 500/字段分叉 500/非容器宿主 500/**忠实展开态正控通过**〔自检不误报，折叠定义 childFlow 三节点全在〕/无载体走带回面；租户鉴权 5：401/viewer 403 editor 闸/reference 租户内解析+跨租户不借不泄露/hints 注入 prompt 实证/构造失败具名 503）。pre-A 基线树 rc=2 ImportError；pre-B 基线 worktree+空桩模块终版夹具 **23F/0P**（404 断言红，46 处 404 非空转）→ post **23P**。证据 `docs/proofs/data/r81-issue305-generate-{pre-red,post-green}.txt`。
- 判据两则（负控夹具期自证）：①FastAPI `Field(min_length=1)` 会把空串变成 pydantic 校验错误列表（非具名 detail）——具名错误契约要求 handler 面收口，Field 不设 min_length；②end 出边 warning 负控的终点必须用既有节点（新 end 节点会变孤儿 error），沿用 Phase 1 `end→a` 构造。
- 守恒：#305(43)+#309(16)=**59P** 零回归；console 全量 **1412P/13S rc=0**（CI 同参；基线同参 collect-only=1425，现树 1412P+13S=1425 **全等**——+23 全为本轮，对 r80 记账 1386P/14S 的差是 r80 落库后并行席位的基线漂移非回归）；unit **8861P/13S/14d/3501 subtests rc=0**（=r79 逐项全等）；integration+e2e **287P/4S**（CI 同参，=r75~80 全等）；ruff 改动 4 文件 0 告警；mkdocs --strict rc=0（`docs/graph-dto.md` 同步 Phase 2 节：契约/闭环/四类状态码/A128-01 自检语义/模型接入）。
- **终态**：推送 `cd61b26`（实现）+ 本台账提交；landing_gate 见终态补记。Phase 3（embed token + iframe `/embed/graph-editor` + postMessage 协议）排下一轮——**embed token 是安全面，实现前安全设计要点已列给主控转 owner 过目**（短期 token 签发/校验面、与 admin key 隔离、iframe origin 收窄、postMessage targetOrigin 钉死、readonly 态授权降级、token 撤销与 TTL）。
- **终态补记**：推送 `07afcb8`（实现）+ `cbe170c`（台账；rebase 到 origin/main `c924221`——incoming B2 r82 #308/B3 r85 #307 追补与本轮零相交，rebase 后树上本轮+守恒+incoming 面合跑 105P 零冲突零回归）后 main CI + CI slow lane + Console E2E 三线 **success**；`landing_gate.py ci` **rc=0**（✅ main CI 绿〔cbe170c〕✅ Console E2E 绿 ✅ 允许落地）。#305 Phase 2 进度回评已附（issues/305#issuecomment-6347943277，含 Phase 3 embed token 安全设计五点待 owner 过目：token 形态与 TTL/权限面收窄 v1 零直写/frame-ancestors 白名单+postMessage 禁 \*/租户钉死/审计+可撤销）。

## 第八十二轮 · B123-01（resume 定义缺失终态化的租约前写覆写窗——A1 r123/r124 已给单点修法，2026-10-23，B1 修复席）

- **机制**（A1 r123 发现 + r124 C124-② 升级）：resume 入口租约前撞 `FlowDefinitionMissingError` 时把**租约前快照**（suspended+旧 context）传 `_terminalize_definition_missing` 强行终态化——未持租约 ⇒ fenced 存储纯透传普通写，load→取定义窗口内持缓存定义的活 worker 推进至 completed 后，陈旧 error 写无条件覆写终态。三点放大：整行回写倒灌旧断点（retry 重放已成功节点）/ `_release_sandboxes` 按快照 context 跨进程 pause 活跑沙箱（error→keep_data=True）/ 假终态 `run_end(final=true)` 先行双发。旧注释「被覆写=无害」论证只看了 error→error 方向，反向成立即有害。
- **修法**（A1 单点，一处收三点，`05e8623`）：`:3889` 改传 `state=None`（与 start `:3275` 同款）——助手现读盘：已终态一律不覆写（`:2408` 防线）、非终态才终态化且落在**现读行**（context 不倒灌）；#304 可观测终态+配额单一收口语义保持；in-lease 复核点 `:3966` 持租约 fenced 不受影响。注释同步重写。
- **负控先红后绿**：`tests/unit/test_issue304_resume_prelease_race_b12301.py` 6 用例（竞窗用 `get_flow_definition` 桩序列化「快照已读→活 worker 写终态→取定义失败」最坏序）——主格 completed 不被覆写 + 三波及面各 1 格 + state=None 现读直格 2。pre-red 基线 **4F/2P**（4 竞窗格全红；2 直格基线即绿=GREEN 锁定）→ post **6P**；同族对照 #304 全套/#247/#241/沙箱终态释放 **17P**。
- **守恒**：console 全量 **1435P/13S rc=0**（collect-only 1448 全等零回归；+23=兄弟席位 #308/#307 追补先合入非本树新增）；unit **8867P/13S/14d/3501 subtests**（=r81 基线 8861+本轮 6 格精确对账）；integration+e2e **287P/4S**（=r75~81 全等）；ruff 改动零新增（flow_worker.py 11 条 BLE001 基线存量，stash 对照同数）；mkdocs --strict rc=0。证据 `docs/proofs/data/r82-b12301-{pre-red,post-green}.txt`。
- **终态**：实现 `05e8623`（rebase 到 origin/main `1be3594`——incoming B2 r83/r84 + B3 r86 零相交，rebase 后定向 17P 复验零冲突）推送；landing_gate **rc=0**（main CI 绿 + Console E2E 绿）。Phase 3（embed 面）待 owner 过安全设计，听主控派单。
