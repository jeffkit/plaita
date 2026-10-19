# #263 同族证明：`/api/dispatch/ledger` 租户隔离

- 单号：plaita#263（P1 security 同族读面）
- 边界：只动 `api/capacity.py` 的读面与 `plaita/server/dispatch_ledger.py`
  的租户计数/过滤；写面（`dispatch/one` 的 #262 权威字段）不变。

## 根因

`GET /api/dispatch/ledger` 对任何已认证身份返回全量 `XRANGE`：租户 viewer
可读他租户的 `tenant_id` / `actor` / `issue_id` / `execution_id`、路由原因
与队列积压对账。

## 修法

- 非平台身份：`tenant_scope(required=True)` 收窄 rows、`ledger` 计数、
  `queue_length` 计数与 `parity`（缺 `tenant_id` 的历史条目不可见，
  fail-closed）；显式传他租户 `tenant_id` ⇒ 403 + `dispatch.ledger.denied`
  审计（不静默忽略）。
- 平台管理员 / API-Key：默认全量（`tenant_id=null`），可用 `tenant_id`
  query 显式收窄。
- 租户计数按消息体/k 台账条目的显式 `tenant_id` 扫描；与删除侧
  `task_queue._task_tenant_id` 同一归属判据（payload 损坏/缺字段不归属）。

## 负控（见 `data/r40-negative-controls.txt`）

把端点两处 `tenant_id=scope_tenant` 改回 `None`：
`test_viewer_only_sees_own_tenant` 红——viewer 读到
`{a-1, a-2, b-1, b-2, b-3, legacy}`。恢复后新文件 **5 passed**；
#42/#262 定向 **26 passed**。
