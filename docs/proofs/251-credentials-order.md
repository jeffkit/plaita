# #251 证明：凭据 DB 与导出旁文件的顺序一致性

- 单号：plaita#251（P1，console/credentials）
- 基线：`origin/main` = `a8b5363`
- 边界：只动 `save_credential` / `delete_credential` 的 DB/导出顺序与
  `_export_store` 的 session 复用面；#223 的原子写 helper、#245 的租户级
  清理、#226 的租户隔离均不改。

## 根因

`plaita-console/backend/services/credentials_svc.py`：

```python
# delete_credential（issue 正文）
session.delete(row)
session.commit()          # ← DB 行先消失
_export_store(affected)   # ← 再导出；这里失败时 DB 已删、旁文件仍旧
```

导出失败（磁盘满/权限/IO）后：

- `get_credential_record(name)` → `None`（DB 行已删）；
- 引擎 `plaita.credentials.get_credential(name)` 仍返回旧密文（旁文件是
  引擎真实读取源，不是缓存）；
- 再次 `delete_credential` 因 DB 无行直接返回 `False`，旁文件永远清不掉。

`save_credential` 是同一模式的另一面：先 `commit` 再导出，导出失败时 DB
已有新行、旁文件仍旧（本单任务明确要求的「DB 不留新行」判据）。

## 修法（同一事务内先原子导出，再 commit）

- `_export_store(tenant_id, *, session=None)`：新增 session 复用；调用方
  `session.flush()` 后，同一事务的 SELECT 能看到未提交的 INSERT/UPDATE/
  DELETE，导出的行集与即将提交的 DB 状态一致。
- `_export_then_commit(session, tenant_id)`：
  1. `_export_store(..., session=session)`（内部仍是 #223 的
     `_atomic_write_text`：同目录 tmp + fsync + `os.replace`）；
  2. 导出成功才 `session.commit()`；
  3. 导出失败 ⇒ `rollback()`：DB 不留新行/删除，旁文件由原子写保证保持旧值；
  4. 导出成功但 commit 失败 ⇒ rollback 后按回滚后的 DB 状态补导一次
     （best-effort，失败只记日志），避免旁文件超前于 DB。
- `save_credential` / `delete_credential` 删除末尾的「commit 后 export」，
  改为 `flush()` + `_export_then_commit()`。

结果：

- 导出失败：save/delete 的 DB 与旁文件都保持旧状态（可安全重试）；
- 删除成功：DB 行与旁文件同时清除；
- 不会出现「DB=None + 引擎可读旧值」。

## 测试（5 条，`test_issue251_credentials_order.py`）

- save 新增：导出失败 ⇒ `get_credential_record("new") is None`、引擎读不到
  new、旁文件不含 new（任务判据「DB 不留新行」）；
- save 更新：导出失败 ⇒ DB 回滚为旧密文、旁文件旧值；
- delete：导出失败 ⇒ DB 行仍在、引擎仍读到旧值（两者一致，可重试）；
- delete 重试：恢复导出后再次删除成功，DB 与旁文件都清除；
- 成功路径：save 后 DB/旁文件一致，delete 后两处都清除。

相邻回归实跑：#223（原子写三注入点）、#226（凭据隔离）、#245（租户级
清理）、#239（写探针）**19 passed**；全量 console **583P/13S**。

## 负控

把 `_export_then_commit` 改回「先 commit 再 `_export_store`」（DB-first）：

```
4 failed, 1 passed
E       AssertionError: assert {'name': 'new', ...} is None          （save 留新行）
E       AssertionError: assert {'token': 'NEW'} == {'token': 'SECRET'}（更新未回滚）
E       AssertionError: db row None but engine read {'token': 'SECRET'}
E       AssertionError: assert False is True                          （重试不再清理）
```

第三条与 issue 要求的精确红文案一致；还原修复后 **5 passed**。完整日志
`docs/proofs/data/251-negative-control.txt`。

## 验收

- 新增 5 passed；相邻 19 passed；全量 console **583P/13S**；
- 全量 unit（`-m "not slow"`）**8172 passed / 12 skipped / 14 deselected /
  3492 subtests**；e2e+integration **287P/4S**；plaita-ai
  **241P/14S/12 subtests**；layering / wheel / size 三相位全绿；
- 基线 `a8b5363` CI **success**，`landing_gate.py ci` 绿后单 commit 推送
  （`closes #251`）。
