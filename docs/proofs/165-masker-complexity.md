# plaita#165 证明：掩码器复杂度上界 + 入口上限的「机制 / 政策」分离

* **仓库**：`jeffkit/plaita`
* **基线**：`origin/main` = `43757bd`（2026-10-17；`#159` 探针前提重指后的 main）
* **工作区**：`/tmp/w27-a/wt`（`git worktree add -b fix/165-policy-window origin/main`）
* **机制提交**：`fe65c65 fix(core): #165 机制——入口上限触发必须响亮 …`
* **政策提交**：本文件所在提交（标题含 `[policy]`，trailer `Policy-ack: jeff 2026-10-17`）
* **未 push、未开 PR**（按任务纪律）

---

## 0. 结论速览

| 判据 | 修前（基线 2048） | 修后（政策 64 KiB） |
|---|---|---|
| 长度 3000 的**正常** webhook URL | `<unprintable-url>`（整段抹掉） | `https://hooks.slack.com/***/***` |
| 长度 3000 的 Redis URL | `<unprintable-url>` | `redis://user:***@host:6379/0` |
| 超限触发 | `logger.debug`（静默）、无计数 | `logger.warning`（length/limit/位点）+ `_cap_trip_count += 1` |
| 负控：门槛临时改回 2048 | —— | 同一条 URL 又变成 `<unprintable-url>`（计数 +1，WARNING 出现） |
| DoS 上界（`"x://"*n`，n=1600/4000/20000） | 4–319 µs | 7–683 µs（仍为亚毫秒级，见 §5） |
| 机制有界性 | 形状闸门 + 闭包字符预算 | **不变**（政策只改门槛值，不碰预算） |

---

## 1. 问题与裁决

`538d763`（W14-A）用 `_MAX_CLEAN_INPUT = 2048` 把 80 KB 输入从「>30 s 不返回」降到
4.7 µs，代价是：**正常但较长的 URL 会被静默整段掩成 `UNPRINTABLE_URL`**，而触发点
只有 `logger.debug`，既不可见也不可计数。

用户裁决（选项 B）：**政策与机制分离**——

1. **机制**（`fe65c65`）：字符预算 + 形状闸门保证有界；每次超限必须 WARNING + 可计数；
2. **政策**（本提交）：门槛 2048 → **64 KiB**（`64 * 1024`），正常流量绝无可能触及；
3. 触发**不许** debug 级静默。

---

## 2. 红先证：修前 3000 字符正常 URL 被整段掩掉

脚本：`redproof.py`（`/tmp/w27-a/redproof.py`，原始输出 `logs/RED-before.log`）。

基线（`43757bd`，`_MAX_CLEAN_INPUT = 2048`）：

```text
threshold=2048
[webhook-3000] input_len=3000
  result='<unprintable-url>'
  warnings=0
  cap_trip_count=None -> None
[redis-3000] input_len=3000
  result='<unprintable-url>'
  warnings=0
  cap_trip_count=None -> None
[scrub-3000] input_len=3009
  result='boom <unprintable-url> end'
  warnings=0
  cap_trip_count=None -> None
```

⇒ 红：**正常 URL 被整段掩掉，且没有任何 WARNING / 计数**（当时连
`_cap_trip_count` 这个可观测名字都不存在）。

---

## 3. 修后：同一 URL 正常掩码（保留安全前缀）

政策提交后（`_MAX_CLEAN_INPUT = 64 * 1024`，原始输出 `logs/GREEN-after.log`）：

```text
threshold=65536
[webhook-3000] input_len=3000
  result='https://hooks.slack.com/***/***'
  warnings=0
  cap_trip_count=0 -> 0
[redis-3000] input_len=3000
  result='redis://user:***@host:6379/0'
  warnings=0
  cap_trip_count=0 -> 0
[scrub-3000] input_len=3009
  result='boom https://hooks.slack.com/***/*** end'
  warnings=0
  cap_trip_count=0 -> 0
```

保留的是**端点 + path 形状**（`hooks.slack.com` / `host:6379`），凭据本体（path 的
每个 segment、userinfo 口令、query）一律 `***`——与 `#159` 的渲染契约逐字一致。

---

## 4. 负控：门槛临时改回 2048 ⇒ 同一条 URL 又被整段掩掉

脚本：`negative_control.py`（原始输出 `logs/negative-control.log`）。运行时用
`mock.patch` 把 `_MAX_CLEAN_INPUT` 改回 2048，**同一条** 3000 字符 URL：

```text
=== 正控：政策门槛 64 KiB（修后） ===
[webhook-3000] threshold=65536 input_len=3000
  result='https://hooks.slack.com/***/***'
  cap_trip_count=0 -> 0
[redis-3000] threshold=65536 input_len=3000
  result='redis://user:***@host:6379/0'
  cap_trip_count=0 -> 0
=== 负控：临时改回 2048（同一条 URL 必须又被整段掩掉） ===
[webhook-3000] threshold=2048 input_len=3000
  result='<unprintable-url>'
  cap_trip_count=0 -> 1
  warning_text='URL 清洗输入超出 #165 上限（safe_render）：length=3000 > limit=2048，已 fail-closed'
[redis-3000] threshold=2048 input_len=3000
  result='<unprintable-url>'
  cap_trip_count=1 -> 2
  warning_text='URL 清洗输入超出 #165 上限（safe_render）：length=3000 > limit=2048，已 fail-closed'
```

⇒ 修后能正常掩码**确实由政策值决定**：只回退一个常量，行为立即回到「整段抹掉」，
而且触发是响亮的（WARNING + 计数）。该负控同时以测试形式入库：
`test_negative_control_reverting_to_2048_caps_the_same_url`。

---

## 5. DoS 上界：冷缓存曲线（修前 vs 修后）

### 5.1 测量纪律

`_secret_forms_cached` / `_delimiter_pieces` / `_quoted_forms` / `_encoding_forms`
都是 `lru_cache`。每个 `(入口, n)` 都在**新解释器**里、import 完成后调一次
（`bench.py`），所以表里是**冷缓存**代价；同一进程重复调用会命中缓存（见 §5.3）。

### 5.2 曲线表：`"x://" * n`（原始日志 `logs/base-curve.log` / `logs/post-curve.log`）

| n | 长度 | 入口 | 修前 ms（2048） | 修后 ms（64 KiB） | 修后输出 |
|---:|---:|---|---:|---:|---|
| 1600 | 6400 | `factory._mask_url` | 0.004 | 0.008 | `***` |
| 4000 | 16000 | `factory._mask_url` | 0.008 | 0.012 | `***` |
| 20000 | 80000 | `factory._mask_url` | 0.033 | 0.030 | `***` |
| 1600 | 6400 | `_mask_url_for_log` | 0.005 | 0.007 | `***` |
| 4000 | 16000 | `_mask_url_for_log` | 0.005 | 0.009 | `***` |
| 20000 | 80000 | `_mask_url_for_log` | 0.004 | 0.061 | `<unprintable-url>`（cap WARNING） |
| 1600 | 6400 | `_mask_webhook_url_for_log` | 0.006 | 0.021 | `<unprintable-url>` |
| 4000 | 16000 | `_mask_webhook_url_for_log` | 0.007 | 0.030 | `<unprintable-url>` |
| 20000 | 80000 | `_mask_webhook_url_for_log` | 0.006 | 0.067 | `<unprintable-url>`（cap WARNING） |
| 1600 | 6400 | `_scrub_url_text` | 0.090 | 0.050 | `<unprintable-url>` |
| 4000 | 16000 | `_scrub_url_text` | 0.065 | 0.087 | `<unprintable-url>` |
| 20000 | 80000 | `_scrub_url_text` | 0.319 | 0.683 | `<unprintable-url>`（scrub_budget WARNING） |

要点：

* **政策加宽没有让 DoS 回退**：n=1600/4000（6400/16000 字符）在修后**真的进了**
  掩码器（不再被入口门槛提前挡掉），但掩码器的 O(1) 形状闸门
  （`len(text) * text.count(":") > 2^21`）与闭包字符预算仍把它们压到**亚毫秒**；
  n=20000（80 KB）在入口门槛处 O(1) fail-closed（顺带 WARNING + 计数）。
* 80 KB 的 `_scrub_url_text` 是表里最慢的一档（0.683 ms），来自正则扫描 80 KB
  文本本身，仍远低于本单原始判据（>30 s）与 250 ms 测试硬上限。
* **语义差异（刻意）**：2049–64 KiB 的分隔符密集敌对 token 现在由掩码器返回
  `***`（`factory._REDACTED`），而不是入口常量 `<unprintable-url>`；两者都是
  fail-closed 常量、都不含原文，既有 `test_issue165_masker_complexity_bound.py`
  的断言（不回原文 / 有界 / n=20000 为 `UNPRINTABLE_URL`）全部继续成立。

### 5.3 `lru_cache` 热态陷阱：285× 证据

**为什么必须冷缓存**：对“3 次取最小值”的朴素写法，第 2、3 次命中
`_secret_forms_cached`，量到的是**热**代价，会系统性低估真实成本。

* W14-A 的实测记录（`/tmp/w14-a-out/RED-PROOF.md` §5.0，2026-10-17）：
  `"x://"*40` **冷 187.8 ms vs 热 0.66 ms，差 285×**。
* 本机复跑同一形状（pre-#165 树 `87b8ad0`，清空四个缓存后 cold，紧接着 hot；
  `hot_trap.py`，原始日志 `logs/hot-trap-prefix-5x.log`）：

  ```text
  n=40 len=160 cold_ms=177.754 hot_ms=1.016 ratio=174.9x
  n=40 len=160 cold_ms=188.260 hot_ms=0.921 ratio=204.3x
  n=40 len=160 cold_ms=163.135 hot_ms=0.943 ratio=172.9x
  n=40 len=160 cold_ms=173.330 hot_ms=0.950 ratio=182.4x
  n=40 len=160 cold_ms=181.699 hot_ms=0.902 ratio=201.5x
  ```

  ⇒ 本机 173–204×，与 W14-A 的 285× 同一量级；**热态数字会把超二次代价藏掉**。
  因此本文件与测试的所有耗时数字都是冷缓存（新进程 + 单次）口径。

### 5.4 边界声明：其它分隔符的额外抽查（不属于本单判据）

为确认“政策加宽是否把别的敌对形状重新放进来”，额外抽查了 `":" / "@" / "?" /
"#" / "%" / "x@" / "x?"` 七个族（`adv_curve.py`，原始日志 `logs/adv-curve.log`）。
结论：

* 7 个族在 1600 / 4000 / 20000 规模上全部**有界**，最慢是 `"x@"*20000`（40 KB）
  经 `_mask_url_for_log` 的 **44.1 ms**（同规模 `"x://"*20000` 是 0.058 ms）；
* 拉满政策边界（64 KiB）时最慢仍是 `"x@"*32768` 的 **90.6 ms**（`":"*65536` 是
  0.059 ms；其余族 2.1–12.7 ms）。它来自 `_bare_at_token_spans` / `_delimiter_stop`
  的“每个 `@` 各自前后找分隔符”路径——**已知的 Θ(n²) 项**，而 `factory` 的形状
  闸门只计 `":"`（W14-A 的判据：超二次的两个来源都由冒号驱动）⇒ 该族不受闸门保护，
  `@` 密度落在既有闭包字符预算（`_MAX_CLOSURE_CHARS = 1M`）里，仍是**有界**成本
  （≈100 ms 上限），但**不是** µs 级。
* 这**不影响本单验收判据**：原始 #165 的超二次族是 `"x://"*n`（§5.2 的 µs 级曲线
  未回退）。若要把 `@` 密集族也压到 µs，需要另单扩展 `factory` 形状闸门（把 `@`
  计入 `_MAX_SPAN_SCAN_WORK` 的乘积），超出本单「机制保留、只改政策」的边界；
  本单不夹带该改动，只在此登记。

---

## 6. 机制：门槛触发必须响亮

`fe65c65` 的机制（与门槛数值无关）：

```python
_cap_trip_count = 0

def _record_cap_trip(where, length, limit):
    global _cap_trip_count
    _cap_trip_count += 1
    logger.warning(
        "URL 清洗输入超出 #165 上限（%s）：length=%d > limit=%d，已 fail-closed",
        where, length, limit,
    )

def _enforce_clean_budget(url, where):
    length = _input_length(url)
    if length is not None and length > _MAX_CLEAN_INPUT:
        _record_cap_trip(where, length, _MAX_CLEAN_INPUT)
        return False
    return True
```

* 三个入口上限调用点统一走 `_enforce_clean_budget`：
  `_safe_render`（`safe_render`）、`_render_absolute_url_ref`
  （`absolute_url_ref`）、`_mask_relative_url_ref`（`relative_url_ref`）；
* `_scrub_url_text` 的**累计** URL 预算用尽也走同一记录（`scrub_budget`）；
* 原来的两处 `logger.debug("…超出 #165…")` 已删除——**不许 debug 级静默**；
* WARNING 只含 `length` / `limit` / 位点，**不含输入内容**；
* `_cap_trip_count` 是模块级名字，监控或测试可直接读。

### 6.1 机制守卫（`tests/unit/test_issue165_cap_trip_signal.py`，9 passed）

| 用例 | 断言 |
|---|---|
| `test_over_cap_warns_and_counts` | threshold+1 ⇒ `UNPRINTABLE_URL` + 恰好 1 条 WARNING + 计数 1；消息含 length/limit/safe_render，不含输入本体 |
| `test_at_cap_does_not_count` | 恰好 threshold ⇒ 正常掩码、计数 0、无 WARNING |
| `test_under_cap_does_not_count` | 普通短 URL ⇒ 计数 0（负控：守卫不空转） |
| `test_absolute_url_ref_trip_is_loud` | 绝对 URL 超限 ⇒ `absolute_url_ref` WARNING + 计数 |
| `test_relative_url_ref_trip_is_loud` | scheme-relative 引用超限 ⇒ `relative_url_ref` WARNING + 计数 |
| `test_cumulative_scrub_budget_trip_is_loud` | 累计预算用尽 ⇒ `scrub_budget` WARNING + 计数（无单条超限） |
| `test_counter_is_monotonic_across_trips` | 连续三次触发 ⇒ 1/2/3 单调递增 |
| `test_count_is_exported_for_observability` | `_cap_trip_count` 是模块级 int |
| `test_bytes_over_cap_trips_too` | `bytes` 按字节数计，同样触发 |

---

## 7. 政策：为什么是 64 KiB

```python
# **门槛值是政策，不是机制**（``[policy]`` 提交单独落，可单独回滚）……
_MAX_CLEAN_INPUT = 64 * 1024
```

依据：

1. **正常流量上界**：四家 webhook 与常见策略 URL 通常 < 1 KiB；第三方 HTTP 异常
   文本里的请求 URL 通常 < 8 KiB。64 KiB 比正常上界高约 **8×**，现实流量
   “绝无可能”触及；3 KB / 8 KB / 16 KB 三档正常长 URL 在政策测试里全部正常掩码。
2. **机制的界不靠这个门槛**：真正的成本界是掩码器的形状闸门
   （`len(text) * text.count(":") > _MAX_SPAN_SCAN_WORK = 2^21`，O(1)）与闭包字符
   预算（`_MAX_CLOSURE_CHARS` / `_MAX_REDACTION_CHARS`）。§5.2 的曲线证明：政策
   加宽后 n=1600/4000 **进入**掩码器仍然亚毫秒。门槛只负责“极端超宽输入根本不
   接触掩码器”这一层 defense-in-depth。
3. **可单独回滚**：政策是一个常量行；回滚它不会关掉 WARNING/计数机制（§4 负控）。
4. **与累计预算的关系**：`_MAX_SCRUB_URL_CHARS` 仍是 64 KiB。直接日志路径
   （`_mask_url_for_log` / `_mask_webhook_url_for_log`）单条 URL 可到 64 KiB；
   任意文本路径（`_scrub_url_text`）是**累计** 64 KiB，超过即 `scrub_budget`
   fail-closed + WARNING。两档门槛同宽，语义清晰：单条 URL 的宽窗 == 一次清洗的
   总预算，不会出现“单条允许、总量反而不允许”的隐性矛盾。

政策守卫（`tests/unit/test_issue165_policy_window.py`，8 passed + 3 subtests）：

| 用例 | 断言 |
|---|---|
| `test_threshold_is_the_wide_policy_value` | `_MAX_CLEAN_INPUT == 65536` |
| `test_threshold_is_documented_at_the_constant` | 源码唯一 `64 * 1024` + 注释含「政策，不是机制」「64 KiB」 |
| `test_3000_char_webhook_url_masks_normally` | 3000 字符 ⇒ `https://hooks.slack.com/***/***`，无 WARNING、计数 0 |
| `test_3000_char_redis_url_masks_normally` | 3000 字符 ⇒ `redis://user:***@host:6379/0` |
| `test_3000_char_url_inside_exception_text_masks_normally` | `_scrub_url_text` 同样正常掩码 |
| `test_wide_window_still_masks_several_real_sizes` | 3 KB / 8 KB / 16 KB 全部正常掩码 |
| `test_exactly_at_wide_threshold_still_masks` | 64 KiB 边界（`<=`）正常；+1 字符 fail-closed + WARNING |
| `test_negative_control_reverting_to_2048_caps_the_same_url` | 回退门槛 ⇒ 同 URL 又整段掩掉（WARNING + 计数） |

---

## 8. 横切守卫与测试记录

| 测试面 | 结果 | 原始日志 |
|---|---|---|
| `tests/unit/test_issue165_cap_trip_signal.py`（新） | **9 passed** | —— |
| `tests/unit/test_issue165_policy_window.py`（新） | **8 passed + 3 subtests** | —— |
| `tests/unit/test_issue165_masker_complexity_bound.py`（既有） | **15 passed** | —— |
| 上述三个文件一次跑 | **32 passed + 3 subtests**，0.88 s | —— |
| `tests/unit/test_no_silent_swallow.py` + `tests/integration/test_layering.py` + `#158/#159×2/#160/#161` 五个 sink 文件 | **918 passed**，4.05 s | `logs/test_guards_and_sinks.log` |
| `tests/unit/test_redis_url_parse.py`（掩码器主面 + 50 锚） | **101 passed + 104 subtests**，229.87 s | `logs/test_redis_url_parse.log` |
| `test_hot_path_logs.py` + `test_log_handler.py` + `test_issue84_log_queue_bounds.py` + `test_issue46_services_logging.py` | **20 passed + 8 subtests**，5.80 s | `logs/test_logging_surface.log` |

### 8.1 关于 `tests/unit/test_issue151_log_hygiene.py`

该文件**不在当前 `origin/main`**（`git cat-file -e origin/main:…` 失败），它属于
尚未并入的 W19-A 分支（`da2d819`）。按纪律从该分支取出运行做对照：

* 基线（未改，`43757bd`）：**11 failed, 3 passed**；
* 本修复树：**11 failed, 3 passed**（与基线逐条相同）。

失败原因全部是 W19-A 自身未合并：`plaita.core.url_redaction._redact_env_value`
不存在（3 条 ImportError），以及 W19-A 的 `_REMAINING_EXC_INFO_FILES` 终态空表与
main 上 62 处 `exc_info` 未处理站点对账不上（8 条）。**没有一条**指向本 diff；
本 diff 也没有新增 `exc_info` / env 值插值 / 裸异常日志。原始日志：
`logs/test_issue151_baseline.log`（基线）与 `logs/test_issue151.log`（本树）。
其余横切守卫（`test_no_silent_swallow` / `test_layering` / sink 五件套）全绿。

---

## 9. 产物 / 复现

```text
w27-a.patch        # 两个提交（相对 origin/main 43757bd）
.HEAD / .tree      # 政策提交的 HEAD / tree
RED-PROOF.md       # 本文件
redproof.py        # §2/§3 红证与修后对照
negative_control.py# §4 负控（mock.patch 回 2048）
bench.py           # §5 冷缓存曲线（每点一个新解释器）
hot_trap.py        # §5.3 cold vs hot 285× 陷阱
adv_curve.py       # §5.4 其它分隔符的额外抽查
logs/              # 全部原始输出（曲线 / 红证 / 负控 / 各守卫测试）
```

复现（工作树 `/tmp/w27-a/wt`）：

```bash
# 红证 / 修后 / 负控
.venv/bin/python redproof.py
.venv/bin/python negative_control.py

# 冷缓存曲线（每点独立进程）
for entry in masker log webhook scrub; do
  for n in 1600 4000 20000; do .venv/bin/python bench.py "$entry" "$n" 3; done
done

# 定向测试
.venv/bin/python -m pytest \
  tests/unit/test_issue165_cap_trip_signal.py \
  tests/unit/test_issue165_policy_window.py \
  tests/unit/test_issue165_masker_complexity_bound.py -q
```

**边界声明**：未跑 plaita 根包全量 `tests/unit`（#169 已知挂起点）；未 push、未开 PR。
