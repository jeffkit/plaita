# #257 证明：删除租户的凭据旁文件先清、成功才 commit DB

- 单号：plaita#257（P1 security，console/tenants）
- 基线：`origin/main` = `7c6bf3d`（+ 本席位 #257 commit `016f065`）
- 边界：只动 `tenants_svc.delete_tenant` 的凭据清理顺序/404 补清扫与
  `credentials_svc.save_credential` 的归属校验；#245 的凭据同事务删除、
  #251 的凭据单条顺序契约、#226 的租户隔离均不改。

## 根因

`plaita-console/backend/services/tenants_svc.py` 旧顺序：

```python
session.commit()                              # ← 租户/凭据行先消失
_remove_tenant_credentials_export(tenant_id)  # ← 再删引擎可读旁文件
```

unlink 失败（权限/磁盘）后：

- 租户行/凭据行已提交删除，重试 `delete_tenant` 只拿到 `False`（API 404）
  ——「404 死路」；
- 旁文件仍在，引擎按租户上下文仍能解出旧明文；
- 同名租户重建即继承旧密文。

与 #251 修掉的 `delete_credential` 顺序倒置同构。

## 修法

1. **先清旁文件、后 commit**：删除行 `session.flush()` 后调用
   `_remove_tenant_credentials_export`，成功才 `commit`。失败 ⇒ 上下文管理器
   回滚，租户/凭据行仍在，重试不会撞 404。
   失败方向 fail-safe：宁可「租户在而凭据不可达」，不可「租户删而凭据残留」。
2. **404 路径补清扫**：旧版本留下的「行已删、旁文件在」中间态，重试删除时
   即使租户行不存在也执行旁文件清理（仍失败则 loud 上抛）。
3. **拒绝复活写**：`save_credential` 在同一事务内先确认 `Tenant` 行存在，
   已删/从未存在的租户抛 `ValueError`（API 400），不落库、不重导旁文件。
4. 「仍有 flow 先清空」拒绝门保持在所有清理之前（#255 判据 2）。

## 测试（`test_issue257_tenant_credentials_order.py`，5 条）

- unlink 注入 → loud 失败且租户/凭据行/旁文件都在；恢复后重试成功，
  引擎 `get_credential` 读不到；
- 删除后 `save_credential` 拒绝写回，列表/详情/旁文件均无复活；
- 从未存在的租户同样拒绝写回；
- 旧版本 404 残留（直接删 DB 行、保留旁文件）重试可补清扫；
- 有 flow 时删除被拒且旁文件不被触碰。

相邻回归：#245/#251/#226/#223/#239/`test_credentials_api` **26 passed**；
全量 console **616 passed / 13 skipped**。

## 负控（必红）

变异：① 把 commit 移到旁文件清理之前（恢复 DB-first）；② 去掉
`save_credential` 的租户存在性检查。

```
3 failed, 2 passed
E  assert None is not None            # 失败后租户行已不存在（fail-safe 反向）
E  Failed: DID NOT RAISE ValueError   # 已删租户仍可写回
E  Failed: DID NOT RAISE ValueError   # 未知租户仍可写回
```

还原修复后 **5 passed**。完整日志 `docs/proofs/data/255-257-negative-control.txt`。

## 验收

- 新增 5 passed；定向 + 相邻 26 passed；全量 console **616P/13S**；
- `landing_gate.py ci` 绿（main `881383b` CI success）；
- 单 commit `closes #257`。
