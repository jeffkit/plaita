# r73 证明：outbox 删除投递语义 + 源表漂移对账 + DLQ 人工出口 + inf/nan 具名拒

- 单号：plaita#301（P2 删除链路不清目标 Redis）、plaita#302（P2 源表
  definition 被改漂移不可自愈）；顺带收口 c117-06（DLQ 人工出口）、
  b117-04（`_env_float` inf/nan 静默放行）、b117-03（多副本 attempts
  丢失更新 → 文档钉单副本约定）
- 基线：`origin/main` = `41984f1`（A1 r117 对抗轮基线；`2f1be94` 为 #299）
- 负控证据：`docs/proofs/data/r73-301-302-negative-control-{pre,post}.txt`
  （脚本 `docs/proofs/data/r73-301-302-negative-control.py`；pre 对 A1 的
  clean archive `/tmp/plaita-r117-tree`（= 基线 41984f1）跑同一套判据）

## 负控先红后绿（A1 r117 探针同源复刻）

pre（基线树）0/5 GREEN，逐项复现 A117 实测：

```
VERDICT RED S1_301_target_three_cleaned: ghost still_there=1 flow_list=1 versions_set=1 purge_target_keys=MISSING
VERDICT RED S2_302_source_drift_named_and_converged: reconcile={'checked': 1, 'requeued': 0} target_desc='outbox' console_desc='OPS-EDITED' drift_total=MISSING row_status=delivered
VERDICT RED S3_c117_06_dlq_manual_exit: engine_outbox.resolve_row MISSING（无人工出口）
VERDICT RED S4_b117_04_inf_named_rejected: poll_seconds(inf)=inf default=2.0
SUMMARY 0/5 GREEN
```

post（修复后工作树）5/5 GREEN：三件全清（still_there=0）/ 漂移具名
（drifted=1、drift_total=1）且按源表现值收敛（target desc=OPS-EDITED）/
resolve 出队（scanned=0、health_resolved=1）/ inf → 2.0 / 健康行零误报
（S5 守恒，pre/post 语义见下）。

## #301：删除链路清目标 Redis 三件

**修法**：`engine_outbox.purge_target_keys(flow_id, tenant_id=, version=,
client=)`——与投递**同源配置**（`TARGET_REDIS_URL`）建连（复用 lifespan
缓存的 `app.state.engine_outbox_target`），复用引擎存储
`RedisFlowStorage.delete_flow` 清三件（版本 key + `flow_versions:{fid}`
注册集 + `flow_list` 成员），与投递落盘 `sync_flow_to_engine` 同一存储
抽象、不另立清理逻辑。

- 挂钩点：`api/flows.py` 两个 DELETE 端点，**先清目标后删行**——若删除
  事务失败回滚，已 delivered 行仍在，对账把 key 重投自愈；反之（先删后
  清）清理失败会留下无行覆盖的幽灵 key。
- 尽力而为：目标不可达 → 具名 ERROR（降级必须可观测）+ 不阻断删除；
  此时行已 purge、对账不再覆盖，恢复后需人工清理幽灵 key（runbook 写明）。
- 边界：outbox 未启用（同 Redis 部署）→ no-op；单版本删除只清该版本，
  兄弟版本与 `flow_list` 保留。
- 对账侧顺带收口：源表行已删而 outbox 行仍在的残留（删除竞态/A1 A5b
  交错构造）由对账按删除语义收尾——清目标三件 + 删行（见 #302）。

## #302：对账先对源表，漂移具名 + 按源表现值重投

**口径（拍板）**：console 是权威库，`flow_versions.definition` 现值是
唯一权威；outbox 行内副本只是投递缓存。对账每行**先对源表**：重读源表
现值重算 `engine_payload` 指纹与行内 `definition_hash` 比对——

- **分歧 = DB 级修改漂移**（API 面禁止覆盖 published，绕开 API 的写入是
  唯一来源）：按源表现值更新行内副本与指纹、打回 pending 重投（重投被
  投递期自检/读回校验兜住），`drift_count` 逐行累计、
  `/health.engine_outbox.drift_total` 聚合（迁移 `0004_outbox_drift_count`
  补列，幂等）；`last_error` 具名「源表定义与行内副本不一致」。
  不选「锁行等人工」：那会永久冻结分叉、违背 outbox「最终一致」承诺。
- **源表不可解析**：保留旧指纹照常入队重投，投递期自检以
  `definition_unparsable` 具名拒投进 DLQ——目标不被坏数据污染、漂移可见。
- **源表行已删除**（漏 purge 残留）：清目标三件 + 删行；清理失败保留行
  （仍 delivered）下轮重试，`delete_flow` 吞异常只回 False 不能作成功
  判据，以「目标版本 key 已消失」为收尾口径。
- **后对目标**（#299 原口径不动）：缺 key / hash 不符 → 打回重投。
  对账统计新增 `drifted` / `source_missing`。

## c117-06：DLQ 人工出口

`POST /api/flows/publish-outbox/{id}/resolve`（置 `resolved` 终态、留痕，
追加 `manually resolved by <actor>`）/ `.../discard`(删行，仅审计日志)。
`resolved` 不进投递扫描（`_iter_due_rows` 只取 pending/failed）、不进对账
（只扫 delivered）；delivered 行两者都 409（resolve 无意义 / discard 会
失去对账覆盖）；跨租户/不存在 → 404。`/health.engine_outbox.resolved`
计数。

## b117-04：inf/nan 具名拒

`_env_float` 增 `math.isfinite` 检查——`float('inf')/float('nan')` 的
`value < minimum` 恒 False，修前静默放行（`POLL=inf` →
`Event.wait(inf)` 投递链停摆、`RECONCILE=inf/nan` → 周期对账静默失效）；
现与 `PLAITA_HTTP_DNS_TTL`「nan/inf → 拦截」先例同口径：WARNING 具名 +
回退默认。

## b117-03：单副本约定（文档钉死）

投递/对账线程无 claim/leader、无 FOR UPDATE；多副本并发会重复投递（同
内容覆盖写、幂等无害）但 `attempts` 互覆（退避/DLQ 计数失真）。ops-runbook
「单副本约定」钉死：不要起多个 console 副本共享同一数据库 + 目标 Redis
跑 outbox。

## 测试与验收

- 新增 `test_engine_outbox.py` 15P（#301 三件全清/单版本只清己/API 层
  清理/未配 no-op/不可达具名不抛；#302 漂移重投收敛/坏 JSON 进 DLQ 可见/
  源缺失收尾/目标不可达保留行/健康零误报；c117-06 resolve 留痕出队/
  discard 删行/delivered 409/跨租户 404；b117-04 inf/nan/-inf 具名拒）+
  `test_schema_migrations.py` 2P（0004 fresh 补列 / 0003→0004 升级幂等）。
- console 全量 **1253P/13S**（基线 1236P/13S，+17P 零回归）；unit
  `-m "not slow"` **8836P/13S/3501 subtests**（与 r69/r71 逐位一致）；
  e2e+integration **287P/4S**；ruff 改动文件零告警；`mkdocs --strict`
  rc=0；负控 pre 0/5 → post 5/5。
- 文档：ops-runbook「跨 Redis 部署的发布投递链」节（删除投递语义/对账
  扩口径/人工出口/单副本约定/drift_total）/ env 表 2 行 / 非法值矩阵行；
  DOC_CODE_MAP 行 26。
