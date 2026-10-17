# RED-PROOF — #198 fire 回写 CAS + console PUT 乐观锁

日期：2026-10-18 · 执行者：修复落地席（jeffkit）· 基线 `87e628e` · 单 commit
工作区：`/Users/kong/projects/infra4agent/plaita`

## §0 结论速览

| 验收项 | 结果 |
|---|---|
| 交错：读旧快照 → PUT 停用 → fire 回写 | 修复前 `enabled: False → True`；修复后 **保持 False** |
| fire 回写字段 | 只更新 `last_fired_at` / `last_enqueue_ok` / `next_run_at`；`enabled` / `cron` / `params` / `version` 以在库值为准 |
| 并发改 cron | fire 的 `next_run_at` 按**当前** cron 重算（不覆盖并发编辑） |
| console PUT 冲突 | `expected_updated_at` 不一致 → 结构化 **409**（`conflict=schedule_stale` + `latest_updated_at`）；无 expected 的旧客户端也由服务端读取基准做 CAS |
| #118 | dedup_key / cron 到期语义不变；相关 83 用例绿 |
| 负控 | 恢复旧快照整份回写 ⇒ unit **5 failed / 1 passed**；console 交错用例 **1 failed / 2 passed** |
| 验收面 | unit 快车道 **7752 passed / 3429 subtests**；e2e **74 passed / 3 skipped**；integration **213 passed / 1 skipped**；console API **3 passed**；`ci-gate.sh` **PASSED**（8052 passed / 覆盖率 93.83%）；`landing_gate.py ci` **success** |

## §1 缺陷与规格

- `fire_schedule` 用调用方传入的**扫描旧快照** `_merge(schedule, updates)`
  后整份 HSET（`plaita/server/services/schedule_service.py:237-246`）；
- console PUT 读-改-写整份 JSON（`plaita-console/backend/api/schedules.py`），
  无版本/CAS。

时序：调度器读到 `enabled=true` → 用户 PUT 停用（`enabled=false`）→
`fire_schedule` 用旧快照回写 → `enabled` 被翻回 `true`；反向的并发编辑
（cron/params/version）也被静默回滚。

提报席规格（#198 comment）：触发回写只更新
`last_fired_at`/`last_enqueue_ok`/`next_run_at`（逐字段/CAS）；console PUT
加乐观锁，冲突 409（参考 flows.py C5-1 结构化 409）；负控恢复 `_merge`
整份回写必红；#118 dedup/cron 语义保持。

## §2 实现

### §2.1 `schedule_service.cas_update_schedule`（WATCH/MULTI）

```python
with redis_client.pipeline() as pipe:
    pipe.watch(SCHEDULES_KEY)
    current = json.loads(pipe.hget(...))
    if check_updated_at and current["updated_at"] != expected:
        raise ScheduleConflictError(...)
    mutate(current)          # 只改触发字段
    pipe.multi(); pipe.hset(...); pipe.execute()
```

- `WatchError`（其他写者抢先）⇒ 重新 WATCH 读-合并-写，不丢更新；
- 记录不存在/JSON 损坏 ⇒ 返回 None（调用方跳过/404）；
- `fire_schedule` 的 mutate 只碰三个触发字段；`next_run_at` 用**当前**
  cron 重算（并发改 cron 时不会写回旧 cron 的计算结果）；
- `_fire_if_due` 的 `next_run_at` 自愈路径同改，不再整份回写旧快照；
- `enabled`/`cron`/`params`/`version` 不被旧快照覆盖。

### §2.2 console PUT 乐观锁

- `ScheduleUpdateRequest` 新增 `expected_updated_at`；
- 读取时记录 `base_updated_at`；请求显式带 expected 且不等 ⇒ 立即结构化 409；
- 写回走 `cas_update_schedule(..., expected_updated_at=base_updated_at,
  check_updated_at=True)`：即使旧客户端不带 expected，服务端也用读取基准做
  CAS，并发编辑不会被静默覆盖；
- 冲突响应：
  ```json
  {"detail": {"message": "调度已被其他编辑更新，请加载最新后重试",
              "conflict": "schedule_stale",
              "latest_updated_at": "..."}}
  ```
- 本地单机档：读后写前复核 `updated_at`，冲突同样 409；SQLite 更新保持原语义。
- 不改 cron 解析、`next_run_after`、dedup_key（#118）、创建/删除端点。

## §3 修复前 / 修复后

探针：`docs/proofs/data/198-repro.py`。修复前在干净 worktree `87e628e` 跑
（`PYTHONPATH=/tmp/wt198-before`），修复后在本树跑；机读结果
`docs/proofs/data/198-before-after.json`。

| 观测 | 修复前 | 修复后 |
|---|---|---|
| PUT 停用后 `enabled` | `False` | `False` |
| fire 回写后 `enabled` | **`True`（被翻回）** | **`False`** |
| `last_fired_at` / `last_enqueue_ok` | 写入 | 写入（不变） |
| `cron` | `* * * * *`（旧快照） | 在库值（并发编辑保留） |
| CAS 冲突（expected=t1，current=t2） | 助手不存在 | `ScheduleConflictError(latest=t2)`，不写入 |
| console PUT stale expected | 无 409（静默覆盖） | **409 `schedule_stale`** |

## §4 验收

### §4.1 定向

```
pytest tests/unit/test_issue198_schedule_cas.py -q  ⇒ 6 passed
cd plaita-console/backend && pytest tests/console/test_schedule_cas_api.py -q ⇒ 3 passed
pytest tests/unit/test_issue23_start_lease_window.py \
       tests/unit/test_issue28_tenant_quota.py \
       tests/unit/test_issue46_services_logging.py \
       tests/unit/test_issue151_log_hygiene.py \
       tests/unit/test_worker_start_dedup.py \
       tests/unit/test_issue198_schedule_cas.py -q ⇒ 83 passed
```

覆盖：交错停用、cron 并发、version/params 保留、manual 不改
next_run_at/enabled、CAS 冲突不写、删除后不重建；console 409、交错、
fire 字段保留；#118 相关回归全绿。

### §4.2 unit 快车道

```
pytest tests/unit -m "not slow" -q ⇒ 7752 passed, 11 skipped, 13 deselected,
3429 subtests passed
```

### §4.3 e2e / integration

```
pytest tests/e2e -q          ⇒ 74 passed, 3 skipped
pytest tests/integration -q  ⇒ 213 passed, 1 skipped
```

### §4.4 本地门禁与 landing gate

- `ci-gate.sh`：**PASSED**。`pytest tests/` ⇒ **8052 passed / 15 skipped /
  3429 subtests**（120 warnings），覆盖率 **93.83%**（阈值 80%）；import
  layering 3 passed；wheel 无 `test_*.py`；SC-003 类尺寸硬顶满足。
- `landing_gate.py ci`：**success**——`origin/main` 最近完成 run `87e628e`
  conclusion=success，放行。

## §5 负控（变异纪律）

在 `6e78941` 的临时 worktree 中把 `fire_schedule` 回写恢复为旧快照整份
HSET：

```diff
-        cas_update_schedule(redis_client, schedule_id, _apply_fire)
+        stale = dict(schedule); stale.update(updates)
+        redis_client.hset(..., json.dumps(stale, ...))
```

结果：

```
tests/unit/test_issue198_schedule_cas.py ⇒ 5 failed, 1 passed
plaita-console/backend/tests/console/test_schedule_cas_api.py ⇒ 1 failed, 2 passed
```

- **红**：交错停用（`enabled` 被翻回 true）、cron 并发（旧 cron 覆盖）、
  version/params 覆盖、manual 翻回 enabled、删除后重建；console 交错用例；
- **绿**：CAS 冲突助手本身（未被变异）、console 的两个 409 用例（独立于
  fire 路径）。

变异 diff 与红名单：`docs/proofs/data/198-negative-control.diff` /
`198-negative-control.txt`；SHA256 见 `198-before-after.json`。

## §6 关系与边界

- **#118**：dedup_key / `next_run_at` / cron 到期语义未改；相关用例全绿。
- **#59/#101**：supervisor PUT / 草稿保存是**另一面**的同族「读旧快照整份
  写回」问题；本单只覆盖 schedule 定义面。关闭 #198 时注明 #59/#101 仍需
  各自规格落地。
- 不改 console 的调度创建/删除端点；不引入分布式锁（单键 WATCH/MULTI 足够）。
- 本地单机档保持 SQLite 更新语义；读后写前 `updated_at` 复核，冲突 409。

## §7 复跑

```bash
# 修复前/后探针
PYTHONPATH=/tmp/wt198-before .venv/bin/python docs/proofs/data/198-repro.py
PYTHONPATH=.                .venv/bin/python docs/proofs/data/198-repro.py

# 定向
.venv/bin/python -m pytest tests/unit/test_issue198_schedule_cas.py -q
(cd plaita-console/backend && ../../.venv/bin/python -m pytest \
    tests/console/test_schedule_cas_api.py -q)
.venv/bin/python -m pytest tests/unit/test_issue23_start_lease_window.py \
    tests/unit/test_issue28_tenant_quota.py tests/unit/test_issue198_schedule_cas.py -q

# 验收面
.venv/bin/python -m pytest tests/unit -m "not slow" -q
.venv/bin/python -m pytest tests/e2e -q
.venv/bin/python -m pytest tests/integration -q
bash scripts/ci-gate.sh
python3 scripts/landing_gate.py ci
```

## §8 变更文件

- `plaita/server/services/schedule_service.py`：`ScheduleConflictError`、
  `cas_update_schedule`（WATCH/MULTI）；fire/自愈只合并触发字段；
- `plaita-console/backend/api/schedules.py`：`expected_updated_at`、结构化
  409、CAS 写回（cluster）/读后复核（local）；
- `tests/unit/test_issue198_schedule_cas.py`：6 条触发器 CAS 测试；
- `plaita-console/backend/tests/console/test_schedule_cas_api.py`：3 条
  console PUT 409/交错/字段保留测试；
- `docs/proofs/data/198-repro.py` / `198-before-after.json` /
  `198-negative-control.{diff,txt}`：机读旁证。
