# r71 证明：flow publish outbox（跨 Redis 部署的 durable 发布投递链）

- 单号：plaita#299（feat(console): durable flow publish outbox +
  worker-side pull-through for cross-Redis deployments；本单落地 **console
  侧 outbox 全链**——worker 读穿透（issue §2）由 plaita-go 后续轮实施）
- 基线：`origin/main` = `eb08fd0`（daadc00 已并入）
- 负控证据：`docs/proofs/data/r71-299-negative-control-{pre,post}.txt`
  （脚本 `docs/proofs/data/r71-299-negative-control.py`）

## 根因（修复前缺口）

console 发布链路的引擎同步（`engine_sync`）只写 **console 进程所连的
Redis**（模块边界注释自认）；跨机部署唯一手段是运维在目标机手工跑
`sync_published_to_engine.py`——批处理脚本，不是实时同步。负控 pre
（同一台机、两个独立 Redis 实例，配了目标地址）：

```
publish rc=200: published
console_redis key plaita:flow:demo:1.0.0 exists: True
target_redis  key plaita:flow:demo:1.0.0 exists: False
RESULT: RED —— 跨 Redis 部署下目标环境拿不到定义（worker 侧 ErrFlowNotFound）
```

即「console 发布成功、worker 找不到 flow」。

## 修法（四件）

1. **outbox 表**（`flow_publish_outbox`，模型 `models/flow.py` +
   alembic `0003_flow_publish_outbox`）：字段按 issue 建议（id/tenant_id/
   flow_id/version/definition_hash/definition/status(pending|delivered|
   failed)/attempts/last_error/created_at/delivered_at）+ 实现列
   `next_attempt_at`（退避/慢速重排调度）。**publish 事务内**与
   `flow_versions` 同事务写入（`flow_store.publish_version` →
   `engine_outbox.enqueue_publish`，SAVEPOINT 吸收幂等键竞态）；
   删除流程/版本同事务 `purge_for` 清行（对账不复活已删版本）。
2. **投递 worker**（`services/engine_outbox.py` + main.py 后台线程）：
   - 至少一次：写 `{ns}:flow:{flow_id}:{version}`（复用
     `engine_sync.sync_flow_to_engine` 同款写入面）后**读回校验**存储原文
     的 `flow-definition-json-sortkeys-v2` 指纹一致才记 delivered；
   - **指纹按投递后工件**算（定义 + 注入 flow_id/version，见
     `engine_payload`）——worker 侧 `compute_flow_definition_hash` 校验的是
     Redis 存储原文，按原始定义算会永远不等（开发期实测踩中并修正）；
     与引擎函数的等价性有单测钉住（`test_hash_algo_parity_with_engine_worker`）；
   - 幂等键 `(tenant_id, flow_id, version, definition_hash)` 唯一约束：
     同版本重复发布不重复入队/投递；覆盖写同内容，重复投递无脏数据；
   - 失败指数退避（2s 起封顶 120s）→ 连续 8 次（可配）转 `failed`
     （DLQ 档，ERROR 告警 + /health 暴露）→ 慢速重排（300s 起翻倍封顶
     30min）自动补投——目标长时间不可用恢复后自动补齐，定义不丢。
3. **对账**：启动首遍 + 每 300s（可配，0=关）扫 delivered 行——目标缺
   key / hash 不符打回 pending 重投（重投预算重置，last_error 留因）。
4. **配置/文档**：目标 Redis 地址
   `PLAITA_CONSOLE_ENGINE_OUTBOX_TARGET_REDIS_URL`（空=整体关闭，同 Redis
   部署零负担零线程）；5 个节律/预算 env 全部「非法 WARNING + 回退默认」；
   ops-runbook 新节「跨 Redis 部署的发布投递链」+ env 表 6 行 + 非法值
   矩阵行更新；DOC_CODE_MAP 同步。`/health` 新增 `engine_outbox` 字段
   （status/pending/failed/last_delivered_at/last_error/当轮计数）。

## 负控（先红后绿）

| 场景 | pre | post |
|---|---|---|
| 跨 Redis 发布（脚本双实例） | 目标缺 key（RED） | 投递 delivered=1、目标 hash 与工件一致（GREEN） |
| publish 回滚 | （无 outbox 概念） | `test_publish_rollback_leaves_no_outbox`：版本退回 draft + 零残留 |
| 同版本重复发布 | — | `test_republish_idempotent_no_duplicate`：仍 1 行 |
| 目标不可达 | — | 退避重试 → DLQ（`test_delivery_failure_retries_then_dlq`）→ 恢复自动补投 |
| delivered 被删/改 | — | `test_reconcile_missing_key_requeues` / `test_reconcile_hash_mismatch_requeues` 重投；守恒 `test_reconcile_healthy_rows_untouched` |
| 已删版本 | — | `test_version_delete_purges_outbox_no_resurrect`：对账 checked=0 |
| 租户命名空间 | — | `test_delivery_tenant_namespace`：`plaita:t1:flow:...`；default 走历史前缀 `plaita` |
| 同 Redis 部署 | 行为不变 | `test_publish_disabled_no_outbox`：零 outbox 行 |

## 验收数字

- 新增测试：`tests/console/test_engine_outbox.py` 18P（含 API 层发布端点
  → outbox → 投递全链 + 指纹等价性）；
  `tests/console/test_schema_migrations.py` +4P（fresh/存量/upgrade 三路径
  都长出 outbox 表 + 幂等）。
- console 全量：**1236P/13S**（基线 1215P/13S + 21P，零回归）；unit/e2e/
  integration 与 landing_gate 见台账本轮条目。

## 边界与不变量

- 未配目标 Redis 时全部为 no-op：不建连、不起线程、publish 零额外写入
  （issue 验收条款「同 Redis 时不产生额外队列/写入负担」）。
- 不改 `flow-definition-json-sortkeys-v2` 算法（issue 非目标）；指纹算法
  标记 `OUTBOX_HASH_ALGO` 只读对齐 worker 侧 `FLOW_HASH_ALGO`。
- 不改 publish 校验逻辑（B2 并行单 #300 的地盘）；`api/flows.py` 零改动。
- worker 读穿透（issue §2，plaita-go FlowSource 回源 console API）不在本单，
  B3 后续轮。
