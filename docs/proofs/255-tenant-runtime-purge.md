# #255 证明：删除租户清理运行数据 + 运行面墓碑

- 单号：plaita#255（P1 security，console/tenants）
- 基线：`origin/main` = `7c6bf3d`（+ 本席位 #255 commit `125fa79`）
- 边界：只动 `tenants_svc.delete_tenant` 的清理面、`create_tenant` 的状态
  回填（同名重建覆盖墓碑）；不新增平台键、不改 `tenant_is_active` 的
  fail-open 语义（#27/#237 保持）。

## 根因（issue 证据）

`delete_tenant` 只删凭据/成员/租户行，运行数据全部留存：

- 本地档 SQLite：`local_schedules` / `local_schedule_fires` /
  `local_executions` / `local_logs`；
- 集群档 Redis：`{ns}:execution:*`（状态/取消/fence/租约/重试计数）、
  `{ns}:start-dedup:*`、`{ns}:flow*`（发布定义）；平台键
  `plaita:schedules` / `plaita:event:*` / `plaita:subscription:*` 里归属
  该租户的条目。

叠加 `tenant_is_active`「记录缺失 = active」的 fail-open：删除后仍在队列
里的 start/resume 继续执行并把残留写回 Redis；旧 enabled 调度继续按 cron
入队；同名重建后新成员直接读到旧执行/日志/调度。

## 修法

1. **本地四表**（与租户行同一事务，失败整体回滚）：
   - `local_schedule_fires` 按本租户 `schedule_id` 收窄（先取 id 再删定义）；
   - `local_schedules` / `local_executions` / `local_logs` 按 `tenant_id`；
   - default 租户同时匹配历史空串（bootstrap 前的存量行）。
2. **Redis 租户命名空间**：
   - 非 default：`{ns}:*` 一次清理（`tenant_namespace` 与引擎同一映射）；
   - default 与平台键共用 `plaita:` 前缀：只清
     `plaita:execution:*`、`plaita:start-dedup:*`、`plaita:flow_list`、
     `plaita:flow_versions:*`，flow 定义逐键判 `type == string`
     ——队列/DLQ 是 Stream，绝不能被误删。
3. **平台键按记录归属收窄**：
   - `plaita:schedules` 里 `tenant_id` 匹配的条目 + 各自 fires/lock；
   - `plaita:event:*` 两种记录形状（console / 引擎）按受信 `tenant_id`，
     同步摘除 `type:`/`types:` zset 成员；
   - `plaita:subscription:data:*` 及 type/correlation/flow/node 索引、
     processed 去重记录、event_filter timeout/dedup 标记；
   - `plaita:tenant_running:*`、`plaita:tenant_quota` 字段。
   - **共享任务队列**（A50-05/A54-09）：按消息体 `payload.tenant_id` 收窄
     `XACK`+`XDEL`，覆盖 console 队列、worker env 队列与已存在的同族 Stream；
   - **DLQ（#264/A53-01）**：DLQ 是平台级键，条目归属在**信封内**
     `envelope.payload.tenant_id`（顶层没有 `tenant_id`）；删除时按该字段收窄
     `XDEL`，畸形/不可归属条目跳过并 WARNING。读面另做纵深：租户视角要求
     `payload.tenant_id == 当前租户` 且 `dead_lettered_at >= 租户 created_at`
     （当前世代），前代/不可归属条目只对平台可见。
4. **墓碑**：删除时写 `plaita:tenant_status[tenant_id] = disabled`（不再
   clear）。`tenant_is_active` 对缺失记录仍 fail-open（兼容语义不变），但
   「显式删除」不再是「缺失」——在途消息/调度被闸门拦下；同名重建
   `create_tenant` 回填 `active`。
5. **顺序**：拒绝门（有 flow）→ 本地事务删除 → flush → 旁文件（#257）→
   Redis 清理 → 墓碑 → commit。任一步失败都保留租户行，可重试；
   404 路径对非 default 租户补清扫本地/Redis 历史残留。
6. 清理失败 loud：Redis 错误原样上抛、墓碑写失败 `logger.exception` 后
   上抛、本地 DB 错误原样上抛。

## 测试（`test_issue255_tenant_runtime_purge.py`，3 条）

- 删 acme：四表清空、`plaita:acme:*` 清空、调度/事件/订阅/配额按租户收窄，
  beta/default 数据与 `plaita:flow:queue` Stream 不受影响；
  墓碑 `disabled`；重建后 `active` 且旧执行/日志/调度/凭据均不可读；
  本地调度循环扫描 0 次；集群 stale 调度被墓碑拦下（`fire_schedule → None`，
  队列长度 0）。
- Redis 清理失败注入：loud 抛错 + 事务回滚（租户/凭据/本地数据仍在，
  旁文件已按 #257 先清），恢复后重试成功。
- 删 default：平台队列 Stream/DLQ 与他租户键保留，default 的 execution/
  start-dedup/flow 定义/调度条目清空。

## 负控（必红）

```
A) 去掉本地四表清理：
   1 failed
E  assert [{'schedule_id': 'sched-acme', ...}] == []   （旧调度残留）

B) 去掉 Redis 清理 + 墓碑：
   1 failed
E  assert 'active' == 'disabled'                       （删除后仍 fail-open）

C) 只去掉墓碑（保留 Redis 清理）：
   1 failed（同上 tombstone 断言）；同一状态下探针：
   tenant_is_active=True / fire_schedule returned True / queue_len=1
   （旧 enabled 调度仍入队——issue 证据本体）
```

还原修复后 #255+#257 **8 passed**。完整日志
`docs/proofs/data/255-257-negative-control.txt`。

## 验收

- 新增 3 passed；相关 tenants/cluster/scheduler/events **58 passed**；
  全量 console **616P/13S**；
- `landing_gate.py ci` 绿（main `881383b` CI success）；
- 单 commit `closes #255`。

## 边界说明

集群档日志 Stream（`plaita:logs:*`）按条目内 `tenant_id` 读面收窄，属平台键，
不在本单主控确认帖枚举的清理面（本地档日志已随 `local_logs` 清理）；
其单键 `max_len` 有界且 TTL/运维可回收。事件记录/订阅是确认帖明确枚举的
平台键对象，已按记录归属逐条清理。
