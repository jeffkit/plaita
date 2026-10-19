# #263 证明：cluster/clusters 读面平台闸 + 响应脱敏

- 单号：plaita#263（P1 security，console/cluster）
- 基线：`origin/main` = `37434a0`（本席位 r39 提交 `3506ac3` 之上）
- 边界：只动 `api/cluster.py` 的鉴权与响应序列化；不改变 ServiceManager /
  ClusterRegistry 的落盘与生命周期语义。

## 根因

#261 只给写面挂了 `ensure_platform_admin`。读面原样回显平台级数据面配置：

- `GET /api/cluster/infrastructure[\\/{name}]` → `url`（含
  `redis://:password@…`）与健康详情；
- `GET /api/clusters[/active|/{id}]` → `redis_url`；
- `GET /api/clusters/{id}/config` → 完整 YAML（`redis.url` +
  `services.*.env` + `infrastructure.*.docker.env`，样例含
  `POSTGRES_PASSWORD`）。

租户 admin 读到连接串即可直连共享 Redis，读写所有租户的执行状态/队列，
绕过 `tenant_scope`；env 里的数据库口令同理。同族未闸端点还包括
`DELETE /cluster/instances*`、`POST /cluster/refresh` 与
`POST /cluster/quick-test`（后者在 console 宿主真实执行流程，simple 实测
输出 42）。

## 修法

1. **全部 30 个路由**统一挂 `ensure_platform_admin`（14 个既有写面 + 16 个
   本次补挂；GET 读面不设例外），拒绝写 `*.denied` 审计 + WARNING；
2. **序列化层脱敏兜底**：所有响应模型继承 `_RedactedResponseModel`
   （`model_serializer(mode="wrap")`），加上裸 dict 返回点显式调用
   `redact_cluster_secrets`：
   - URL 的 userinfo 口令段 → `***`（保留 scheme/host/port/path/db）；
   - query 中凭据键的值、`password=…` 型凭据运行 → `***`；
   - dict 键命中 `plaita.core.url_redaction._is_credential_key`
     （`PASSWORD`/`SECRET`/`TOKEN`/`KEY`…）⇒ 整棵值 → `***`；
   - 任何脱敏异常 fail-closed 为 `***`，绝不回明文。
   即便未来新增端点漏挂平台闸，配置类响应也不会原样出网。

## 负控（逐条还原，见 `data/r40-negative-controls.txt`）

| 还原 | 结果 |
|------|------|
| 仅去掉 16 个新闸（保留脱敏） | `test_tenant_admin_denied_and_secret_never_echoed` 16 条全红（回到 200） |
| 恢复基线 `api/cluster.py`（闸+脱敏均无） | 24 failed / 6 passed；读面 200 且回显 `redis://:s3cret-pass@…`、`DB_PASSWORD: "s3cret-pass"`；quick-test 200 + 输出 42 |
| 仅去掉模型 serializer（保留闸） | 8 failed / 22 passed；`test_redaction_masks_url_password_and_env_secrets` 与平台正控看到明文 |

恢复后定向 6 文件 **69 passed**；console 全套 **727 passed / 13 skipped**。
