# RED-PROOF — #210 Supervisor 三处裸 fetch 改走 API_BASE + authHeaders

日期：2026-10-18 · 执行者：修复落地席（jeffkit）· 基线 `ef90eb3` · 单 commit
工作区：`/Users/kong/projects/infra4agent/plaita`

## §0 结论速览

| 验收项 | 结果 |
|---|---|
| 配置探测 | `fetch(`${API_BASE}/supervisor/config`, { headers: authHeaders() })` |
| 数据集列表 | `${API_BASE}/flows/${flowId}/supervisor/datasets` + `authHeaders()` |
| 迭代 POST | `${API_BASE}/flows/${flowId}/supervisor/iterate` + `Content-Type` 合并 `authHeaders()` |
| 错误分支 | `readErrorText` / `errorDetailText` 展示 `status` + `detail`，不再静默 `catch` |
| 源码扫描 | 修复前 3/3 裸 `/api`、无 API_BASE/鉴权头；修复后 3/3 有 API_BASE + authHeaders |
| 前端构建 | `npm run build`（tsc + vite）**EXIT 0**（14.18s） |
| 负控 | 恢复裸 `/api` fetch ⇒ 源码测试 **4 failed / 1 passed** |
| 主验收面 | 源码测试 **5 passed**；前端构建 **EXIT 0**；`ci-gate.sh` **PASSED**（8057 passed / 覆盖率 93.83%）；e2e **74 passed / 3 skipped**；integration **213 passed / 1 skipped**；`landing_gate.py ci` **success** |

## §1 缺陷与规格

Supervisor 页三处请求绕过统一封装：

- `fetch('/api/supervisor/config')`
- `fetch(`/api/flows/${flowId}/supervisor/datasets`)`
- `fetch(`/api/flows/${flowId}/supervisor/iterate`, { headers: {'Content-Type': ...} })`

既不带 `authHeaders()`（Authorization / X-Admin-API-Key / X-Tenant-ID），也不带
`API_BASE`（`SUB_PATH` 前缀）。鉴权部署下三处 401；子路径部署下三处 404；
`catch` 静默吞成 `null`/`[]`，界面只显示空白或泛化错误。同页 `api.getFlows()`
是正确的封装对照。

规格（#210 comment）：三处改 `API_BASE` + `authHeaders()`（或 `api.*` 封装）；
错误分支展示 `status`/`detail`；负控恢复裸 fetch 必红；`#171` 深链族另述。

## §2 实现

`plaita-console/frontend/src/pages/Supervisor.tsx`：

- import `API_BASE, authHeaders`；
- 新增 `errorDetailText(body, resp)` / `readErrorText(resp)`：状态码 + detail
  （字符串或结构化对象）统一展示；
- 配置探测：`fetch(`${API_BASE}/supervisor/config`, { headers: authHeaders() })`，
  非 2xx 抛错并 `setError`；
- 数据集列表：`fetch(`${API_BASE}/flows/${flowId}/supervisor/datasets`,
  { headers: authHeaders() })`，非 2xx 抛错并 `setError`；
- 迭代：URL 加 `API_BASE`，headers 合并
  `{ 'Content-Type': 'application/json', ...authHeaders() }`；
  非 2xx `setError(errorDetailText(body, resp))`；
- 不改 `authHeaders()` / `request()` 实现、不改 supervisor 后端端点/鉴权模型。

## §3 修复前 / 修复后

源码探针：`docs/proofs/data/210-repro.py`（与提报席 r10 同口径）。修复前在
`/tmp/wt210-mut`（三处裸 fetch 的变异树）扫描，修复后在本树扫描；机读结果
`docs/proofs/data/210-before-after.json`。

| 调用 | 修复前 | 修复后 |
|---|---|---|
| config | 裸 `/api`，无 API_BASE/authHeaders/headers | `${API_BASE}` + `authHeaders()` |
| datasets | 裸 `/api`，无 API_BASE/authHeaders/headers | `${API_BASE}` + `authHeaders()` |
| iterate | 裸 `/api`，有 Content-Type 但无 API_BASE/authHeaders | `${API_BASE}` + `...authHeaders()` |
| 错误展示 | `catch` 静默 / 仅 detail | `status` + detail |

前端构建：`npm run build`（tsc + vite）**EXIT 0**，`✓ built in 14.18s`
（证据 `docs/proofs/data/210-build.txt`）。

**测试基础设施说明**：本仓前端 `package.json` 没有 Vitest/MSW/Playwright
测试脚本，添加测试框架会超出“只动 Supervisor.tsx 三处调用”的边界；本单用
**源码级回归测试 + 前端构建**做可重跑验证，与提报席的 source-level 复现同口径。
浏览器级「鉴权 + SUB_PATH」运行时用例留待前端测试框架接入后补（不作为本单
的已完成项）。

## §4 验收

### §4.1 源码测试

```
pytest tests/unit/test_issue210_supervisor_fetch.py -q  ⇒ 5 passed
```

覆盖：三处 URL 均含 `${API_BASE}`、`authHeaders()` 注入、无裸 `fetch('/api...')`、
错误分支 `readErrorText`/`errorDetailText`。

### §4.2 主套件 / 门禁

- 主门禁 `pytest tests/`（unit + e2e + integration）⇒ **8057 passed / 15
  skipped / 3429 subtests**（120 warnings）；分项 e2e **74 passed / 3 skipped**、
  integration **213 passed / 1 skipped**；
- `ci-gate.sh`：**PASSED**，覆盖率 **93.83%**（阈值 80%）；import layering
  3 passed；wheel 无 `test_*.py`；SC-003 类尺寸硬顶满足；
- `landing_gate.py ci`：**success**——`origin/main` 最近完成 run `ef90eb3`
  conclusion=success，放行。

## §5 负控（变异纪律）

在 `ff9e23c` 的临时 worktree 恢复三处裸 fetch：

```diff
-    fetch(`${API_BASE}/supervisor/config`, { headers: authHeaders() })
+    fetch('/api/supervisor/config')
...
-    fetch(`${API_BASE}/flows/${flowId}/supervisor/datasets`, { headers: authHeaders() })
+    fetch(`/api/flows/${flowId}/supervisor/datasets`)
...
-      const resp = await fetch(`${API_BASE}/flows/${flowId}/supervisor/iterate`, {
+      const resp = await fetch(`/api/flows/${flowId}/supervisor/iterate`, {
-        headers: { 'Content-Type': 'application/json', ...authHeaders() },
+        headers: { 'Content-Type': 'application/json' },
```

结果：

```
tests/unit/test_issue210_supervisor_fetch.py ⇒ 4 failed, 1 passed
```

- **红**：三处 URL/头断言 + `test_no_bare_api_fetch_remains`；
- **绿**：`test_error_branches_surface_status_and_detail`（错误展示代码未被
  变异，证明杀的是三处调用面，不是测试自身）。

变异 diff 与红名单：`docs/proofs/data/210-negative-control.diff` /
`210-negative-control.txt`；SHA256 见 `210-before-after.json`。

## §6 边界与 #171

- 只动 `Supervisor.tsx` 的三处调用；不改 `authHeaders()`/`request()`、不改
  supervisor 后端端点与鉴权模型。
- **#171（Cluster.tsx 深链 `<a href>`）**：同属 SUB_PATH 族，但入口是页面
  跳转而非 API 调用，且该单已 **CLOSED**；本单未触碰，也未把 #171 的修复
  算入本 commit。若需全仓 `href="/"` 审计，按 #171 的验收条款单独执行。

## §7 复跑

```bash
# 源码前后扫描
python3 docs/proofs/data/210-repro.py /tmp/wt210-mut   # before (mutant)
python3 docs/proofs/data/210-repro.py .                # after

# 源码测试
.venv/bin/python -m pytest tests/unit/test_issue210_supervisor_fetch.py -q

# 前端构建
cd plaita-console/frontend && npm run build

# 主验收面
.venv/bin/python -m pytest tests/unit -m "not slow" -q
.venv/bin/python -m pytest tests/e2e -q
.venv/bin/python -m pytest tests/integration -q
bash scripts/ci-gate.sh
python3 scripts/landing_gate.py ci
```

## §8 变更文件

- `plaita-console/frontend/src/pages/Supervisor.tsx`：三处 URL/头 + 错误展示；
- `tests/unit/test_issue210_supervisor_fetch.py`：5 条源码级回归/负控锚；
- `docs/proofs/data/210-repro.py` / `210-before-after.json` /
  `210-negative-control.{diff,txt}` / `210-build.txt`：机读旁证。
