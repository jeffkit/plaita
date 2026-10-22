# #293/#294/#295 + B112-01/02 证明：login_guard 设计级收口

- 单号：plaita#293/P1、#294/P1、#295/P1 + B112-01/02（b/P2）
- 基线：`origin/main` = `b2906c2`（r64 硬化）
- 边界：login_guard 桶语义/时间源/break-glass/代理信任/容量策略；不改密码
  哈希参数、登录响应形状、鉴权与会话语义。

## 桶语义（文档即规格）

| 桶 | key（归一） | 窗口/阈值 | 锁定 | 关键规则 |
|---|---|---|---|---|
| per-key | canonical IP + username | 300s/5 | 15m 翻倍≤24h | IPv4-mapped→IPv4；IPv6 canonical |
| IP | IPv6 /64 聚合（IPv4 精确） | 300s/10 | 5m 翻倍≤24h | 单地址多地址同桶 |
| account | 账号 + 窗口内不同 IP 桶 | 300s/≥3 桶 | 15m 翻倍≤24h | ZSET/dict 按窗口滑动；旧 IP 不续锁 |

## 修法

### #293 IP 归一 + 账号窗口
- `canonical_ip`（IPv4-mapped→IPv4、IPv6 压缩展开）用于 per-key/可信代理解析；
  `bucket_ip` 对 IPv6 取 /64（`IPV6_BUCKET_PREFIX`）用于 IP/account 桶。
- 进程内 account 集合每次 record/retry 按 `WINDOW_SECONDS` 清理；锁过期后旧
  IP 失败不续锁。Redis account 改用 ZSET（`ZADD` + `ZREMRANGEBYSCORE` + `ZCARD`），
  TTL 仅兜底，不再用长 TTL set 当窗口。
- 同 /64 三个地址只算一个桶；3 个不同 /64 或 IPv4 才锁账号。

### #294 break-glass 收口
- 不再「用户名列表即绕过」。必须同时满足：用户名白名单、
  `PLAITA_LOGIN_BREAK_GLASS_ALLOWED_IPS` 来源（默认回环）、
  `X-Break-Glass-Token` 一次性 HMAC token：
  `bg1.<expires>.<nonce>.<hmac>`，secret `PLAITA_LOGIN_BREAK_GLASS_SECRET`
  ≥32 字符，短 TTL，单次消费（Redis SET NX，跨副本；无 Redis 进程内）。
- break-glass 失败照常 `record_failure`（不允许无限爆破）；每次尝试写独立
  `auth.login_break_glass` 审计（`break_glass=true`, allowed/reason/outcome）。
- 只配用户名列表/无 token/来源不符：回落常规 401/429。

### #295 时间源
- Redis Lua 内部 `redis.call('TIME')` 取 UTC 秒+微秒；`retry_after` 用
  `redis.time()`。进程 monotonic/time.time 不再进入共享键；NTP 回拨时
  `until` 只前进不后退。多副本/重启后锁与 Retry-After 同量级。

### B112-01 可信代理
- `PLAITA_TRUSTED_PROXIES` 条目 canonical 化后匹配；`0.0.0.0/0`/`::/0`
  前缀 0 条目具名 WARNING 拒绝，不开启裸 XFF。
- IPv4-mapped peer 归一后匹配 IPv4 CIDR；XFF 候选归一。

### B112-02 MAX_TRACKED
- 满表且无过期条目时新键进入 60s 具名饱和（返回 60 / Retry-After），
  不再静默 `return 0`；活动锁不驱逐。

## 测试

- 新增 `test_issue293_294_295_login_guard_v2.py` **14P**：同 /64 不锁、不同 /64
  锁、canonical/mapped 同键、窗口外旧 IP 不续锁、Redis ZSET 窗口、Redis TIME
  跨进程时钟、break-glass 无 token 20 次照常锁/一次性 token+审计/失败计数/
  来源与 token fail-closed、/0 代理拒绝、mapped peer CIDR、MAX_TRACKED 饱和。
- 既有 `test_issue_a112_login_hardening.py` 12P（L1~L11，L6 改为 token 语义）、
  `test_issue208_login_rate_limit.py` 8P、`test_r55_audit_observability` 保持。

## 负控（pre/post）

pre = `/tmp/plaita-r113-tree`（b2906c2），A1 探针原件；post = 主工作树：

- #293：同 /64 waits [0,0,900]→[0,0,0]；旧 IP 续锁 1800→0；mapped/展开变体
  3 键→2 桶；Redis 旧 IP 续锁 1800→0。
- #294：20 次错误 401×20/no 429/no marker → {401,429}/marker true；valid token
  200；无 token 不绕过。
- #295：高 monotonic 副本 0 绕过 / 低副本 999900s → 两实例一致 900s。
- B112-01：/0 全 trust XFF 裸信 → networks=[]、direct 203.0.113.9；mapped peer
  `::ffff:10.0.0.1` → 203.0.113.50。
- B112-02：满表新键 [0×6]/retry 0 → 60/60 具名饱和。

完整输出：`docs/proofs/data/r65-login-guard-v2-negative-control.txt`。

## 验收

- 全量 unit（`-m "not slow"`）：**8795P/12S/14 deselected/3501 subtests**（rc=0）
- e2e + integration：**287P/4S**（rc=0；layering/wheel/size 定向 46P 绿）
- console：**1198P/13S**（rc=0；较上轮 1184P 增本单 14 条）
- landing gate：**rc=0 / 允许落地**（main 最近完成 run `b2906c2` CI success；Console E2E 绿）
