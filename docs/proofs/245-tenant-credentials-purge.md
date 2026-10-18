# #245 证明：删除租户同步清理凭据（DB 行 + 导出旁文件）

- 单号：plaita#245（P1 security）
- 基线：`origin/main` = `a7a6fdd`。负控运行时的本地 HEAD 为 `b9d0703`
  （rebase 前），其 `tenants_svc.py` 与 `a7a6fdd` 逐字节相同——该文件在
  `a7a6fdd..b9d0703` 区间无改动。
- 边界：只动 tenants 删除清理面；不改凭据加密格式、worker 读取路径与
  `delete_tenant` 的「有流程拒绝删除」语义；#238 conformance 不混入。

## 缺陷

`tenants_svc.delete_tenant` 只删 `Tenant` / `TenantMember` 行；该租户的
`Credential` 行与引擎导出旁文件 `credentials_file(tenant_id)` 全部保留。
平台管理员重建同名租户后：

- `GET /api/credentials` / `GET /api/credentials/{name}` 直接读回旧租户明文；
- worker 继续把旧旁文件当新租户的凭据源加载；
- 「删除租户」无法吊销旧密钥。

## 修法

`delete_tenant`：

1. 同一事务内删除 `Credential.tenant_id == tenant_id` 的全部行（与
   TenantMember/Tenant 行一起提交）；
2. 提交后调用 `_remove_tenant_credentials_export(tenant_id)`，路径取自
   `credentials_svc.credentials_file(tenant_id)`（与 console 导出、worker
   读取共用同一命名，对齐 #215 的旁文件 `<stem>.<tenant><suffix>` 规则）；
3. 旁文件删除失败（`FileNotFoundError` 除外）先 `logger.exception` 再重抛
   ——显式报错，不静默留下可被 worker 读取的旧密文。

测试把旁文件路径按 #215 约定**字面**拼出（不借 `credentials_file` 计算
期望值），避免命名回归时导出与断言两端一起错、用例仍绿。

## 负控（pre-fix 必红）

证据：`docs/proofs/data/245-negative-control.txt`

在 `a7a6fdd` 基线状态（`delete_tenant` 无 Credential 删除、无旁文件清理；
负控运行时本地 HEAD `b9d0703`，该文件与之逐字节相同）上跑新增用例：

```
FFF
FAILED test_delete_tenant_purges_credentials_and_sidecar
  AssertionError: 删除租户后凭据旁文件残留: .../creds.acme.json
FAILED test_delete_tenant_only_purges_its_own_credentials
  AssertionError: .../creds.acme.json 仍存在
FAILED test_sidecar_removal_failure_is_loud
  Failed: DID NOT RAISE PermissionError
3 failed
```

修复后同文件：`3 passed`。

## 测试

新增 `plaita-console/backend/tests/console/test_issue245_tenant_credentials_purge.py`：

- `test_delete_tenant_purges_credentials_and_sidecar`：API 端到端——创建租户
  → 保存凭据（明文不可出现在导出文件中）→ 删除租户 → **字面旁文件不存在**
  / DB 行清空 / 重建同名租户 `GET /api/credentials` 为空、详情 404、
  `plaita.credentials.get_credential` 按租户上下文解析不到旧值；
- `test_delete_tenant_only_purges_its_own_credentials`：删除 acme 不得误删
  default / beta 的凭据文件与行（清理面按租户收窄）；
- `test_sidecar_removal_failure_is_loud`：旁文件 unlink 失败必须抛
  `PermissionError`，文件保留（失败可观测，不伪造清理成功）。

`test_tenants.py` 的 fixture 把 `PLAITA_CREDENTIALS_FILE` 钉到 `tmp_path`：
`delete_tenant` 现在会清理 default 租户的凭据文件，避免既有「允许删
default」用例碰到 cwd 下真实文件。

## 验收

```
# 新增用例
pytest plaita-console/backend/tests/console/test_issue245_tenant_credentials_purge.py
→ 3 passed

# console 全套（backend/tests/console，含既有 #245 回归面）
pytest plaita-console/backend/tests/console -q
→ 552 passed, 13 skipped

# unit 全套
pytest tests/unit -q
→ exit 0（8119 collected）

# 落地熔断
python scripts/landing_gate.py ci
→ ✅ main 最近完成 run 366e44e conclusion=success（2026-10-18T15:34:37Z），rc=0
```
