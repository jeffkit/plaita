# A112-01/02 + B112-01/02 证明：login_guard 硬化

- 单号：duo-a 第一百一十二轮 A112-01/02、B112-01/02（P1/P2）
- 基线：`origin/main` = `3c9d1a0`
- 边界：登录限速/代理 IP/共享计数/反枚举；不改鉴权语义与密码哈希参数。

## 根因与修法

1. **A112-01 分布式爆破无账号桶**：旧键只有 `(client_ip, username)`，N 个 IP
   各错几次永不触发同一账号锁。新增**账号桶**：同一账号失败来自 ≥3 个不同
   IP → 锁账号。保留单 IP fan-out 的 **IP 桶**（≥10 次 → 锁 IP 5m），单 IP
   重复失败只触发 per-key/IP 桶，不误锁账号（NAT/共享出口另一 IP 可登录）。
2. **A112-02 计时枚举**：`users_svc.login` 在用户不存在/禁用时短路，不跑
   PBKDF2（210k）。现在 missing 跑固定 `_DUMMY_PASSWORD_HASH`、disabled 跑真实
   hash，响应时间比 ≤2×（实测 16×→1.0×）。
3. **B112-01 可信代理**：`_client_ip` 默认只用 ASGI peer；仅当 peer 命中
   `PLAITA_TRUSTED_PROXIES` 才解析 XFF，并从右向左取首个 untrusted。
4. **B112-02 多 worker 计数分裂**：新增 `RedisLoginThrottle`（Lua 原子，key/IP/
   account 三维共享）；endpoint 从 `app.state.redis` 注入，无 Redis 的本地档
   退回进程内 `LoginThrottle`（单 worker 语义，runbook 注明）。
5. **L6 break-glass**：`PLAITA_LOGIN_BREAK_GLASS_USERS` 绕过限速（仍校验口令，
   日志 WARNING）。
6. **L10 驱逐**：`_prune` 不再驱逐活动锁；容量满时新键不跟踪，已有锁保持。
7. **L11**：锁定期内正确/错误口令都 429（账号/IP/per-key 三维检查在口令校验前）。

## 测试（NC-L1~L11 逐条）

`plaita-console/backend/tests/console/test_issue_a112_login_hardening.py` **12P**：

- L1 共享代理受害者真实 IP 正确登录 200；
- L2 不可信 peer 的伪 XFF 仍锁；
- L3 代理链右向左首个 untrusted（函数级三断言）；
- L4 3 个不同 IP 打同一账号 → 账号桶；
- L5 单 IP fan-out → IP 桶（未尝试账号也 429），另一 IP 正常 200；
- L6 break-glass 锁中正确登录 200、错误仍 401；
- L7 两个 throttle 实例共享 Redis 合计 5 次即锁；两个 app 实例共享 Redis
  第 6 次 429；
- L8 账号锁过期后再次触发 900→1800；
- L9 missing/disabled 计时比值 ≤2（实测 ~1.0）；
- L10 容量收紧后新键写入不驱逐 victim 活动锁；
- L11 账号锁内正确/错误都 429。

既有 `test_issue208_login_rate_limit.py` **8P** 保持（per-key 5 次锁、锁内正确
拒绝、成功清零、同 IP 其它账号不牵连、退避、窗口过期）。

## 负控（pre/post）

pre = `/tmp/plaita-r112-tree`（3c9d1a0），post = 主工作树；端点探针
`/tmp/r64_nc_probe.py`、A1 timing/eviction 原件：

| NC | pre | post |
|---|---|---|
| L1 共享代理受害者 | 429 | 200 |
| L2 伪 XFF | 429（已安全） | 429（保留） |
| L3 代理链 helper | 不存在 | 右向左 first untrusted |
| L4 分布式账号桶 | 200 | 429 |
| L5 单 IP fan-out 未尝试账号 | 200 | 429；另一 IP 200 |
| L6 break-glass | 429 | 200 |
| L7 Redis 共享 | 无实现 | 12P 中 2 条共享计数断言 |
| L8 账号退避 | 无账号桶 | 900→1800 |
| L9 计时比 | 16.0× | 1.0× |
| L10 活动锁驱逐 | evicted=True | evicted=False |
| L11 锁内正确/错误 | 200/401 | 429/429 |

完整输出：`docs/proofs/data/r64-login-hardening-negative-control.txt`。

## 验收

- 全量 unit（`-m "not slow"`）：**8795P/12S/14 deselected/3501 subtests**（rc=0）
- e2e + integration：**287P/4S**（rc=0；layering/wheel/size 定向 46P 绿）
- console：**1184P/13S**（rebase 到 `3c9d1a0` 后复跑，rc=0；含本单 12 条与 他席 51 条）
- landing gate：**rc=0 / 允许落地**（main 最近完成 run `3c9d1a0` CI success；Console E2E 绿）
