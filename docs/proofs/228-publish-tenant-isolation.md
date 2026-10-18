# #228 证明：flows publish 的租户过滤（同名 flow + 同版本）

- 单号：plaita#228（P1，security/test）
- 基线：`origin/main` = `19585f6`（issue 基线 `9c8820a`；提报席载体 base
  `afe4bba1`，树 `c49ba447`，本单只取其中的 #228 hunk）
- 边界：**纯测试**——只改 `tests/console/test_tenants.py` 与 `docs/proofs/`；
  产品代码零改动（变异仅负控期间临时注入，跑完逐字还原）。

## 缺口

`plaita-console/backend/api/flows.py:365` 的
`store.publish_version(flow_id, req.version, tenant_id=tenant)` 是 publish 的
租户闸。既有 `test_tenants.py` 的 `_publish_flow` 使用 `acme-flow` /
`default-flow` **不同 flow_id**，`tenant_id=None` 变异仍能命中正确行，因此
「两租户同 flow_id + 同 version」的跨租户写/读面没有用例。

## 新增测试（2 条，`test_tenants.py`）

1. `test_publish_same_flow_id_does_not_cross_tenants`：
   - root 建 `tenant-a` / `tenant-b`；两租户各建同名 flow `shared-publish`；
   - B 保存 `1.0.0` 草稿（definition 含唯一 marker `B-SECRET`）；A **没有**该版本；
   - A 以 `X-Tenant-ID: tenant-a` 发布 `1.0.0` → **404**，响应不含 `B-SECRET`；
     B 的版本仍 `draft`、定义仍含 `B-SECRET`；
   - A 保存自己的 `1.0.0`（`A-ONLY`）并发布 → 200，响应只含 A 定义；B 仍 `draft`。
2. `test_publish_check_404_even_before_version_lookup`：A 无同名 flow 时发布 →
   404（钉住 `get_flow_record` 前置租户过滤不回归）。

平台视角（API key）与既有 15 条 tenant 用例保持原样。

## 负控

把 `flows.py:365` 的 `tenant_id=tenant` 改为 `tenant_id=None`：

- `test_tenants.py`：**1 failed / 15 passed**——唯一红是
  `test_publish_same_flow_id_does_not_cross_tenants`；
- **完整 console 套件**：**1 failed / 545 passed / 13 skipped**——只有新增用例
  红，既有套件零新增失败（issue 的方法论：旧套件发现不了该变异）；
- 精确红文案（全文 `docs/proofs/data/228-negative-control-local.txt`）：

```
AssertionError: {"flow_id":"shared-publish","version":"1.0.0","status":"published",
"definition":"...\"output\": \"B-SECRET\"...","created_by":"b",...}
assert 200 == 404
```

即变异态下 A 发布成功并读回 B 的完整定义（`B-SECRET`）；B 的版本被置
`published`。还原后：`test_tenants.py` **16 passed**，console 全套
**546 passed / 13 skipped**。

提报席原始负控/探针证据随件在 `docs/proofs/data/228-negative-control.txt`
与 `228-repro.py`。

## 验收记录

- 新增用例：**2 passed**；负控：`test_tenants` **1F/15P**、完整 console
  **1F/545P/13S**（仅新用例红）；还原后 console **546P/13S**；
- e2e + integration：**287 passed / 4 skipped**；
- 全量 unit：**7955 passed / 1 failed / 12 skipped / 3434 subtests**。唯一失败为
  既知的 `test_issue159_policy_url_exception_sinks.py`（yarl 错误文案环境差异，
  clean 基线上同样失败）；本单只改 console 测试，不导入该模块，无新增失败；
  `19585f6` 起 sandbox_release 三条环境红已按能力探测转 skip（12 skipped）；
- landing gate：`19585f6` conclusion=**success**；
- 产品代码：零改动（`git diff` 仅测试与 `docs/proofs/`）。

## 交付记录

- commit：本单唯一 commit（`git log -1`），提交信息含 `closes #228`。
- 验收面：console 546P/13S；e2e+integration 287P/4S；unit 7955P/1F
  （1F 为既知 yarl 环境面）。
- landing gate：绿（base `19585f6` CI success / slow lane success）。
