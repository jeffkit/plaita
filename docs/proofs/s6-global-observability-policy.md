# A55-S6 全局可观测面政策（规格，待 owner 裁决，未入码）

- 单号：duo-a-ledger 第五十五轮 B55-02 / A55-S6（无 GitHub issue）
- 状态：**2026-10-19 只写规格，不实现**。本文件是裁决输入，不是已生效策略。
- 关联：#265（P1，`/api/services` host 回显口令）、#263（cluster 读面凭据）、
  A2R13-04/05（dispatch ledger / cluster 读面）、B2R10-04（`/api/metrics` viewer
  可读全局指标）、A55-S5（本轮已把 metrics 的 `KEYS *:dlq` 改为队列族有界 SCAN，
  但**没有**改变读面权限）。

## 1. 现状（r55 探针 + 本轮复核）

以下端点对 viewer（任意已认证会话）开放，返回**跨租户**的全局规模/拓扑/心跳：

| 端点 | 返回 | 可被推断的信息 |
|---|---|---|
| `GET /api/metrics` | 全局队列/DLQ 长度、worker 实例 ID/数量 | 全局负载、worker 存活 |
| `GET /api/dispatch/capacity` | host、active_tasks、free_disk_gib、concurrency、dispatchable | worker 容量/磁盘/并发 |
| `GET /api/services` | host、metadata（`worker_capacity`）、last_heartbeat、active_tasks | 全局拓扑/心跳/容量（#265 还含凭据 URL） |
| `GET /api/services/{id}/status` | status、active_tasks、last_heartbeat | 同上 |
| `GET /api/queues` | 全局 queue length | 各队列规模 |
| `GET /api/queues/{name}` | `length`=XLEN 全局；`tasks` 按租户过滤 | **他租户条数 = length − 本租户可见数** |
| `GET /api/queues/dlq` | 每 Stream `length` 全局；entries 按租户 | 他租户死信规模 |
| `GET /api/dispatch/ledger` | 全量 ledger（无过滤） | 他租户 actor/issue/execution（A2R13-04） |
| `GET /api/cluster/instances` | 全局实例；每次调用先 `refresh_status()` | 全局拓扑 + 副作用（A2R13-05） |

关键点：**不需要 payload**。租户 viewer 知道自己的任务数后，用全局 `length`
做减法即可得到其他租户在队数量；`active_tasks` 随时间的变化可观察其他租户的
并发/失败节律。

## 2. 待裁决的两个方向

### 方向 A（推荐）：全局拓扑/容量/心跳默认平台专属

- `/api/metrics`、`/api/dispatch/capacity`、`/api/services*`、`/api/cluster/instances`
  加平台身份闸（复用 `ensure_platform_admin`；`metrics` 当前未取 identity，
  需补 `Depends(require_auth)` 后判 `platform_admin`）。
- `/api/queues` 的 `length` 对租户身份改为**本租户计数**，或只对平台返回全局值；
  租户视图显式给出“全局计数不可用”的原因（不伪造成 0，也不做减法暴露）。
- 租户 viewer 403 + `*.denied` 审计（复用 A55-S2 的拒绝留痕）。
- #265（host 含口令）作为第一步先修（脱敏或平台闸）。

### 方向 B：租户聚合

- 入队时给队列/指标加 `tenant` 维度（内存/Redis 计数器），租户面只返回自身聚合；
  不得返回 host/instance/worker 维度。
- 需要新增按租户计数的服务端聚合，**不能让前端从全局值做减法**。
- 成本更高：队列 Stream 没有租户段，需改写入路径与容量统计口径。

### 方向 C：维持现状并明确文档

- 若产品明确“租户 viewer 可见全局规模”，在 runbook/API 文档写清这一契约，
  并把 #265 单独修掉（凭据层仍是 P1）。
- 本文件不建议此方向：当前“全局 length 减本租户 tasks”已是可复现的跨租户
  规模推断（r55 探针 3−1=2）。

## 3. 实施清单（裁决后）

1. `api/metrics.py`：补 `Depends(require_auth)` + `identity` 参数；平台闸或
   租户聚合二选一。保留 A55-S5 的有界 SCAN 与 `plaita_metrics_truncated`。
2. `api/capacity.py`：`GET /dispatch/capacity` 加平台闸；`/dispatch/ledger` 已有
   租户过滤（A54-02），保持。
3. `api/services.py`：列表/详情/状态按裁决收窄；`host` 脱敏先于权限（#265）。
4. `api/queues.py`：`length` 的租户视图语义（全局/本租户/不可用）必须显式；
   DLQ 的 `length` 同口径。
5. `api/cluster.py`：读面已有 `ensure_platform_admin`（#263），保持；避免 GET
   触发 `refresh_status()` 的副作用（A2R13-05）。
6. 审计：拒绝写 `*.denied`；平台身份读取全局面建议写 `observability.read`（或
   至少指标），以便回答“谁在何时看了全局拓扑”。
7. 文档：`docs-site/docs/distributed/ops-runbook.md` 的 `/api/metrics` 鉴权行
   已改为“任意已认证会话（viewer+），待裁决”；裁决后同步本文件与 runbook。

## 4. 验收（裁决后）

- 方向 A：viewer 调全局端点 → 403 + denied 审计；平台身份 200；#265 探针变
  403/脱敏；`/api/queues` 租户视图不再可用 `length − tasks` 推断。
- 方向 B：viewer 只看到本租户聚合值；不同租户的返回值互不影响；无 host/
  instance/worker 维度。
- 方向 C：文档与实现一致，且跨租户推断被产品明确接受（记录裁决）。

## 5. 本轮已做 / 未做

- 已做（A55-S5）：`/api/metrics` 不再 `KEYS *:dlq`，只扫描 `KNOWN_QUEUES`
  队列族；`event_type` 拒绝 `:dlq` 后缀与非法字符；队列/DLQ 视图不再把事件
  键当队列。
- 未做（本文件）：任何读面权限/租户聚合改动。**等 owner 裁决后再入码。**
