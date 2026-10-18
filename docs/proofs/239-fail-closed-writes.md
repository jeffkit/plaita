# #239 phase 2 证明：写路径租户来源 fail-closed

- 单号：plaita#239（P2，owner 2026-10-18 裁决）
- 基线：`origin/main` = `7f85393`（#239② 观测 + #151 日志卫生修复之后）
- 边界：只动租户上下文解析 + credentials/storage 写入口；#237 的传播链接线
  不动；不改 tenant_status fail-open。

## 0. Owner 裁决（#239 评论 2026-10-18）

1. 写路径（credentials/storage 的 save/delete）来源 `default`（从未设置）⇒
   具名 `TenantContextMissingError`，不再静默写 default；
2. `derived`（跨边界正确传播）与 `explicit` 照常；
3. 单租户部署迁移：服务入口（flow_worker/console 启动）显式
   `set_current_tenant(DEFAULT_TENANT, source=explicit)`；
4. 测试 conftest 显式设 default；依赖静默降级的测试逐个过；
5. 负控：还原静默降级必红。

## 1. 实现

### 具名错误与守卫

`plaita/tenant_context.py`：

- `TenantContextMissingError(RuntimeError)`：带 `site` / `tenant_id`，消息
  指明写站点与 `source='default'`，并给出修复指引（服务入口显式声明 / 跨边界
  传播）；
- `guard_tenant_write(site)`：先 `record_write_probe(site)` 记录分布（含被拒
  尝试），来源 `default` 时抛错；`explicit`/`derived` 放行。

### 写入口接线

| 入口 | 站点 |
|---|---|
| `plaita/server/tenant_context.py`（租户路由包装器） | `storage.save_execution_state` / `storage.delete_execution_state` / `storage.save_flow` |
| `plaita/storage/redis.py`（Redis 直写，#239② 四站点） | `_tenant_write_observability` → `guard_tenant_write`（原 warning/闩/吞异常全部移除） |
| `plaita-console/backend/services/credentials_svc.py` | `credentials.save` / `credentials.delete` |

- 底层写不触达：默认来源在进入存储前抛错；
- 探针计数保留，`write_probe_snapshot()` 可观测 `(site, "default")` 的被拒量；
- #239② 的「探针异常不阻断」语义被 owner 裁决取代；该函数不再吞异常。

### 服务入口显式声明（单租户迁移）

- `plaita/server/flow_worker.py::run()` 首行
  `set_current_tenant(DEFAULT_TENANT_ID, source=TENANT_SOURCE_EXPLICIT)`；
- `plaita-console/backend/main.py::create_app()` 与 `lifespan` 各一行；
- 部署文档：`docs-site/docs/distributed/flow-worker.md`「租户上下文显式声明」
  与 `plaita-console/README.md`（自定义启动器必须保留该行）。

### 测试迁移

- `tests/conftest.py` / `plaita-console/backend/tests/console/conftest.py`：
  autouse fixture 显式 `set_current_tenant(DEFAULT, source='explicit')`，
  测试不再依赖静默 default；需要「缺失上下文」形状的用例在 fresh
  `contextvars.Context()` 中运行；
- 原 #239 先行断言（default 写成功）迁移为 fail-closed 断言；
- #239② 的 `test_issue239_migration_warning.py` 从「首告 WARNING + 站点闩 +
  异常不阻断」重写为「default 抛具名错误 + explicit/derived 成功」；
- #151 日志卫生：`_tenant_write_observability` 不再把异常对象交给 logger
  （7f85393 的 scrub 修复 + 本单移除吞异常路径），scanner 通过。

## 2. 测试

`tests/unit/test_issue239_tenant_provenance.py`（13）：

- 三态/跨边界 derived（submit/map、async 桥、ExecutionContext pickle）不变；
- **storage 写**：default → `TenantContextMissingError` 且 fake 底层写 0 次、
  计数 `(storage.save_flow, default)=1`；explicit → 成功；derived（经
  ThreadParallelExecutor）→ 成功。

`plaita-console/backend/tests/console/test_issue239_write_probe.py`（3）：

- credentials save/delete：default → 具名错误、DB 未落库、计数 1/1；
  explicit → 成功；derived（线程池）→ 成功。

`tests/unit/test_issue239_migration_warning.py`（4）：

- Redis 四站点写：default 抛错并计数 3 次；错误消息含 site/source；
  explicit 两次成功计数 2；derived 一次成功计数 1。

回归：tenant/boundary 套件 **55 passed**；storage/lease 套件 **195 passed /
1 skipped**；console credentials/API **7 passed**。

## 3. 负控

把 `guard_tenant_write` 还原为「只 record、不抛」：

```
2 failed, 14 passed
FAILED test_storage_write_default_source_fail_closed
FAILED test_credentials_writes_default_source_fail_closed
```

红文案为 `DID NOT RAISE <class 'plaita.tenant_context.TenantContextMissingError'>`
（default 写重新静默成功）。还原后 **16 passed**。证据
`docs/proofs/data/239p2-negative-control.txt`。

## 4. 语义边界

- 默认来源写**行为变更**：从静默写 default 变为具名拒绝（owner 裁决）；
- explicit/derived 行为不变；#237 传播链接线未改；
- tenant_status fail-open 未改；
- 不引入新租户模型；服务入口显式 default 是单租户部署的唯一迁移行。

## 5. 验收记录

- #239 phase 2 定向：**16 passed**；#239② 迁移测试 **4 passed**；
- 负控 **2F/14P**；还原 16P；
- 回归：tenant/boundary **55 passed**；storage/lease **195 passed / 1 skipped**；
  console credentials/API **7 passed**；
- console 全套：**549 passed / 13 skipped**；
- plaita-ai 全套：**233 passed / 14 skipped**；
- e2e + integration：**287 passed / 4 skipped**；
- 全量 unit：**8106 passed / 1 failed / 12 skipped / 3492 subtests**。唯一失败为
  既知 `test_issue159_policy_url_exception_sinks.py`（yarl 文案环境差异，clean
  基线同样失败）；#151 日志卫生扫描通过（#239② 探针的裸异常路径已移除）；
- landing gate：`7f85393` conclusion=**success**；
- 产品代码：`plaita/tenant_context.py`、`server/tenant_context.py`、
  `storage/redis.py`、console `main.py` / `credentials_svc.py` + conftest/测试/
  文档；#237 的传播链接线未改。

## 6. 交付记录

- commit：本单唯一 commit（`git log -1`），提交信息含 `closes #239`。
- 验收面：unit 8106P/1F（既知 yarl 环境面）；console 549P/13S；
  plaita-ai 233P/14S；e2e+integration 287P/4S。
- landing gate：绿（base `7f85393` CI / slow lane / Console E2E / Deploy docs）。
- 行为变更：default 来源写从静默成功 → `TenantContextMissingError`（owner
  裁决）；explicit/derived 不变；服务入口显式 default 迁移行已内置并写入部署
  文档。
