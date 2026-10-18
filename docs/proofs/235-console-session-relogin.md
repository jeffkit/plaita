# #235 证明：ConsoleClient 会话失效后自愈（重登一次 + 重放一次）

- 单号：plaita#235（P2，bug）
- 基线：`origin/main` = `f04de20`（issue 基线 `d6687a3f`）
- 边界：只动 `plaita-ai/plaita_ai/console_client.py` 的认证/重试面 +
  回归测试/证明产物。

## 缺陷

会话模式（用户名/密码、无 admin key）只在 `_session_token` 为空时登录一次；
收到 401 后既不清 token 也不重登，进程内所有 console 调用永久 401：

- 旧 `_auth_headers`（`console_client.py:89-91`）：`if not self._session_token:
  self.login()`；
- 旧 `_request`（`console_client.py:129-139`）：401 直接抛，无清 token / 重登。

console 侧会话 7 天 TTL（`users_svc.SESSION_TTL_DAYS`）、用户被禁用、登出、改密
都会让已签发 token 失效；`ops_mcp._get_client()` 又把 client 进程级缓存，
长跑 MCP server 只能重启恢复。

## 修复

`_request` 在收到 401 且**同时满足**以下条件时自愈：

1. 非 `_skip_auth`（登录端点自身不递归）；
2. 尚未为本次调用重登过（`relogged` 布尔，保证最多一次）；
3. `_session_auth_configured()`：无 `admin_api_key` 且用户名/密码齐全
   （API-Key 优先；key 模式的 401 是配置错误，不重登）。

动作：清 `_session_token` → `login()` → 重放本次请求（同一 method/path/
json/params）。重放仍 401 时走原错误路径抛出（status/detail 不变）。每次
调用最多一次登录失败记录，与 console `login_guard`（#208，5 次失败/300s 锁定）
兼容；成功重登会清零失败计数。

## 回归用例（`plaita-ai/tests/test_issue235_console_session_relogin.py`）

| 用例 | 断言 |
|---|---|
| `test_expired_session_relogins_once_and_retry_succeeds` | 会话失效后第二次调用：恰好一次重登并成功；新 token 持久化，第三次调用不再重登 |
| `test_repeated_401_relogins_at_most_once_per_call` | 服务端持续 401：该次调用只重登一次（login=2、auth 尝试=3），随后按 401 抛出，无循环 |
| `test_api_key_401_never_logs_in` | API-Key 模式（即使同时配了用户名/密码）401：login 调用数 = 0、无重放 |
| `test_failed_login_is_not_retried` | `/auth/login` 自身 401（密码错）不递归：login 调用数 = 1 |

## 负控（还原「永不重登」必红）

把 `console_client.py` 还原到 `f04de20` 基线实现（无 401 重登分支），跑同一
用例文件：**2 failed / 2 passed**——红的正是自愈与有界重试两条；API-Key 与
login-401 两条守卫保持绿（它们约束的是「不许多做」）。完整输出见
`docs/proofs/data/235-negative-control.txt`。还原修复后 **4 passed**。

## 验收记录

- 新增用例：**4 passed**；
- 既有 console client 用例：**12 passed**（`test_console_client.py`）；
- plaita-ai 全量：**233 passed / 14 skipped**（`mcp` 按 CI 安装；基线同环境
  为 229 passed / 14 skipped + 新增 4 条）；
- `scripts/ci-gate.sh`：pytest/layering/wheel/size 全绿；
- landing gate：main `f04de20` conclusion=success，放行。
