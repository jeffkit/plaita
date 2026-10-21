# b94-01 证明：PLAITA_EVENT_TTL_SECONDS 显式上界

- 来源：`duo-a-ledger.md` 第九十四轮 A1 b94-01（P3，设计边界/资源入口）
- 基线：`origin/main` = `1032874`
- 边界：只动 `event_retention_ttl_seconds` 的解析上界；保留既有
  `<=0`/非数字 WARNING 回退、合法值语义、惰性 ZREM 自愈与 janitor 三路径。

## 根因

`plaita/event/redis.py::event_retention_ttl_seconds` 只拦非整数与 `<=0`，
无上界。`PLAITA_EVENT_TTL_SECONDS=99999999999999999999`（>2^63）零 WARNING
原样放行；写入 `SET ... EX` 时 Redis 报 `value is not an integer or out of
range`，连 `2^63-1` 也因秒→毫秒溢出报 `invalid expire time`。管道是 MULTI，
Redis 不回滚：SET 失败而 ZADD/EVAL 继续执行 ⇒ 类型索引 ghost（读侧 ERROR +
惰性 ZREM 自愈，故非 P1）。

## 修法

`MAX_EVENT_TTL_SECONDS = 10 * 365 * 24 * 60 * 60`（10 年业务上限）。
解析在 `ttl <= 0` 之后增加：

```python
if ttl > MAX_EVENT_TTL_SECONDS:
    logger.warning(
        "%s=%d 超过上限 %d 秒（10 年）——事件保留期回退默认 %d 秒；"
        "超大值会在 Redis SET EX 秒转毫秒时溢出并留下半提交 ghost",
        EVENT_TTL_ENV, ttl, MAX_EVENT_TTL_SECONDS, DEFAULT_EVENT_TTL_SECONDS,
    )
    return DEFAULT_EVENT_TTL_SECONDS
```

- 超界值在入口处具名 WARNING + 回退默认（7d），不再进入 SET EX；
- 合法值（含恰好等于上限）原样返回；
- 既有 `<=0`/非数字回退与惰性 ZREM 自愈保留为兜底。

## 测试（9 条，`tests/unit/test_b94_01_event_ttl_bound.py`）

- 超界参数化：`2^63`、`2^63-1`、`上限+1` ⇒ 返回默认 7d，WARNING 含
  `上限` 与具体 `MAX_EVENT_TTL_SECONDS`（具名拒绝，非静默）；
- 合法值：`1`、`120`、`上限` ⇒ 原样返回；
- 既有行为：`0`、`-1`、`not-a-number` ⇒ 仍回退默认。

相邻回归：A69/A78 事件索引与 TTL 族（见 b94-02 证明的 58P + 21P 联合跑）。

## 负控（先红后绿）

删除上界分支：

```
3 failed, 6 passed
E       assert 315360001 == 604800   (cap+1 被原样返回)
FAILED ...test_over_bound_falls_back_with_named_warning[2^63]
FAILED ...test_over_bound_falls_back_with_named_warning[2^63-1]
FAILED ...test_over_bound_falls_back_with_named_warning[cap+1]
```

两条 2^63 边界值与 cap+1 均红（pre-fix 返回超大值，不返回默认）；
合法值/非正数/非数字 6 条保持绿。还原修复后 **9 passed**。完整日志
`docs/proofs/data/b94-01-negative-control.txt`。

## 验收（与 b94-02 同树同轮）

全量 unit **8600P/12S/14 deselected/3498 subtests**；e2e+integration
**287P/4S**；console **1039P/13S**；plaita-ai **241P/14S/12 subtests**；
layering/wheel/size 全绿；`landing_gate.py ci` 绿后单 commit 推送。
