# #214 + #215 证明：两处 P1 测试缺口（纯测试，不改产品代码）

- 单号：plaita#214（viewer 写保护无回归）、plaita#215（tenant root 凭据路由无用例）
- 基线：`origin/main` = `0736c16`
- 边界：只新增/修改测试与证明文档；`git diff` 不含 `auth.py` / `credentials.py`
  等产品代码（负控期间临时注入，验证后已逐字还原）。

## #214 viewer 非 admin 写路由矩阵

### 缺口

`plaita-console/backend/auth.py::_authorize` 有两道闸：

1. admin 前缀闸（`/api/users`、`/api/audit`、`/api/credentials`、`/api/cluster`、
   `/api/tenants`）→ 403「该操作需要 admin 角色」；
2. 写方法闸（非 GET/HEAD/OPTIONS）→ viewer 403「该操作需要 editor 及以上角色」。

既有 `test_viewer_blocked_from_mutations` 只打 `POST /api/credentials`，先命中
第 1 道闸；第 2 道闸的 `raise` 改成 `return` 时，旧测试集仍全绿（issue 实测
19 passed）——RBAC 绕过无回归防护。

### 新增测试

`plaita-console/backend/tests/console/test_issue214_viewer_write_matrix.py`：

- 6 条参数化 viewer 写用例，全部**非 admin 前缀**，断言 403 且 detail 为
  **editor 闸**（不是 admin 闸）：
  `PUT /api/flows/{id}/versions/{ver}`、`POST /api/flows/{id}/publish`、
  `DELETE /api/flows/{id}`、`POST /api/schedules`、
  `POST /api/flows/dry-run`、`POST /api/flows/{id}/supervisor/iterate`；
- 正控：viewer GET `/api/flows` → 200；editor `PUT .../versions/...` → 200；
- 对照：viewer `POST /api/credentials` → 403 且 detail 仍是 admin 闸
  （证明本矩阵走的是另一道闸）。

### 负控

把 `auth.py` 的 viewer 写分支 `raise` 改成 `return`（其余不动）：

- 新文件：**6 failed / 3 passed**（3 个正控保持绿）；
- 既有 `test_admin_auth.py + test_enterprise.py`：**19 passed**（历史盲区复现）。

精确红文案（节选，全文 `docs/proofs/data/214-negative-control.txt`）：

```
AssertionError: PUT /api/flows/hello-plaita/versions/1.1.0 未被 viewer 写闸拒绝: 200
  {"flow_id":"hello-plaita","version":"1.1.0","status":"draft",...}
assert 200 == 403

AssertionError: DELETE /api/flows/hello-plaita 未被 viewer 写闸拒绝: 200
  {"success":true,"flow_id":"hello-plaita"}

AssertionError: POST /api/schedules 未被 viewer 写闸拒绝: 200
  {"schedule_id":"sched-...","tenant_id":"default",...}

AssertionError: POST /api/flows/dry-run 未被 viewer 写闸拒绝: 200
  {"result":"ok","nodes":[...]}
```

即变异后 viewer 实际完成了保存版本、删除 flow、创建调度、执行 dry-run——
正是 issue 描述的 RBAC 绕过面。既有套件在变异下全绿（19 passed），
证明新用例是唯一回归信号。

还原后：新文件 **9 passed**。

## #215 tenant root 凭据路由

### 缺口

`plaita/credentials.py::tenant_credentials_file` 对 default/空租户返回基础文件，
其余租户返回旁文件 `<stem>.<tid><suffix>`。既有测试覆盖 default/空/acme/t-1234，
**从未使用 `root`**：把实现改成 `if tid in (DEFAULT_TENANT_ID, "root")` 时，
旧 12 条仍全绿，而名为 root 的租户会读平台默认凭据文件（跨租户串用）。

### 新增测试

`tests/unit/test_credentials_tenant_routing.py::TestReservedTenantNames`：

1. `root` / `admin` 路由到 `creds.root.json` / `creds.admin.json`，`default`
   仍是基础文件；
2. 在 `set_current_tenant("root")` 下用 `get_credential` 读**只写入旁文件**的
   同名凭据与 root-only 凭据，断言取到 root 的值、default 文件条目对 root
   不可见；
3. `root` 与 `admin` 各自旁文件互不共享。

旁文件路径按文档规则**字面**拼出（`creds.root.json`），不借
`tenant_credentials_file()` 计算写入路径——否则函数回归时写读两端一起错，
用例仍绿，负控会漏。该教训在首轮负控中实际出现，已修正后重跑。

### 负控

把 `if tid == DEFAULT_TENANT_ID:` 改成
`if tid in (DEFAULT_TENANT_ID, "root"):`：

- **3 failed / 12 passed**（既有 12 条全绿，新增 3 条全红）；
- 精确红文案（全文 `docs/proofs/data/215-negative-control.txt`）：

```
AssertionError: assert PosixPath('/data/creds.json') == PosixPath('/data/creds.root.json')
AssertionError: assert PosixPath('.../creds.json') == PosixPath('.../creds.root.json')
plaita.credentials.CredentialError: 凭据 'root-only' 不存在（当前租户可用: （无））
```

第三条直接证明 root 读了空的默认文件，而不是自己的旁文件。

还原后：**15 passed**（12 既有 + 3 新增）。

## 验收记录

- 新增 console 矩阵：**9 passed**；
- 凭据路由文件：**15 passed**（12 既有 + 3 新增）；
- console 全套：**509 passed / 13 skipped**（含本单 9 条）；
- 既有鉴权套件（正控）：`test_admin_auth.py + test_enterprise.py` **19 passed**；
- 全量 unit：**7840 passed / 4 failed / 11 skipped / 3429 subtests**。
  4 条失败在 clean `0736c16` 上**逐条相同**（本机环境面，与本次纯测试 diff 无关）：
  - `test_sandbox_release_on_terminal.py` ×3 —— 本机安装的 `plaita-nodes`
    不含 `plaita_nodes.sandbox`（`ModuleNotFoundError`），lifecycle drain 断言
    随之落空；
  - `test_issue159_policy_url_exception_sinks.py` ×1 —— yarl/aiohttp 错误文案
    环境差异（期望 `invalid character`，本机返回 `Failed to parse`）。
  本 diff 只新增/修改测试文件，产品代码 `git diff` 为空；clean 基线与工作树
  失败集合逐条相同，故无新增失败；
- e2e + integration：**287 passed / 4 skipped**；
- landing gate：`0736c16` conclusion=**success**；
- 产品代码：无改动（`git diff` 仅测试与 `docs/proofs/`）。

## 交付记录

- commit：本单唯一 commit（`git log -1`），提交信息
  `test(console,credentials): #214/#215 ... closes #214 closes #215`。
- 全量验收面：unit 7840P/4F（4F 为 clean 基线同款环境面，见上）；console
  509P/13S；e2e+integration 287P/4S。
- landing gate：绿（base `0736c16` CI success）。
