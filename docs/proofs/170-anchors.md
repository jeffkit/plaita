# RED-PROOF — #170 路线 C：两个最小产品修复 + 61 条变异重锚

日期：2026-10-17 · 基线与执行者见下 · 工作区 `/tmp/w25-a`（分支 `fix/w25-a`，未 push、未开 PR）

## §0 结论速览

| 验收项 | 结果 |
|---|---|
| #79 `RedisEventStorage.list_events` 返回完整性 | **修复**：生产建连 `decode_responses=True` + `_as_str` 兜底；去掉 `type_keys[:10]`；全局最早 `limit` + 全局时间序；载荷缺失 ERROR 响亮。**bytes 半边在 current main 已被 #109/#110 的 `_zset_member_ids` 顺带治好**（实测 1 绿），本次补的是机制侧 + 截断/有序/配额。 |
| #85 回扫游标无条件推到窗口上界 | **修复**：只有确认窗口排空才推到 `window_end`；未排空推进到「本轮已消费的最后一条 `(timestamp, event_id)`」。5 事件 / `batch_size=2`：修复前 `2/5` 且 `ev2..ev4` 永久丢，修复后 4 轮 `5/5`。 |
| 两个 bug 的红证 / 负控 | **红证**：把三份产品源码换回 `origin/main` 后，新回归面 **12 failed / 3 passed**（3 绿逐条说明见 §2.3/§3.3）。**负控（变异）**：对应变异 m01/m02/m03/m09/m12/m47/m56 逐条重放 **KILLED**（改回即红）。 |
| 61 条变异定义 | **11 条**按新 main 诚实重锚并逐条重放 **KILLED**；**50 条归档**到 `mutations/parked/`（目标代码/测试属于未落地的 `fix/79-85` 加固面，拒绝伪造锚点）。 |
| 附带重锚（不改语义） | 我方 redis 重写顺带打破的 2 条（m109/m110）+ recent merge 后存量的 5 条（159/88-*）× 全部 **KILLED**。合计 **18/18 KILLED**。 |
| `test_every_mutation_anchor_appears_exactly_once` | **回绿**（`TestIssue69R12MutationJsonContract` 2 passed）。 |
| 锚点负控（抽 3 条毒化 `old`） | **3/3 报 stale**（`kind=anchors`，rc=1，不进 pytest）。 |
| `--check-anchors` | **JSON store（本次对象）**：305 条定义 / 问题 0，rc=0。**full store 仍红**：`mutations.py` 的 #133/#33 家族在 current main 上本就整体陈旧（49 条目标文件不存在 + 55 条 `old` 命中 0），与 #170/#79/#85 无关——见 §4.5。 |
| 横切守卫 | `test_no_silent_swallow` / `test_expression_size_guard` / `tests/integration/test_layering` **全绿**（682 passed / 979 subtests 的定向子集内含）。`test_issue151_log_hygiene.py` 在本基线**不存在**（W2 批次未落地），见 §5。 |

## §1 环境与基线

- 基线（`git fetch origin main` 后）：`43757bd4235213a6b2afba53d24d9f217fdbdc0f`
- 工作区：`git worktree add --detach /tmp/w25-a origin/main && git checkout -b fix/w25-a`
- 解释器：`/Users/jeff.jie/.venvs/plaita/bin/python`（3.11.16）
- **venv 的 editable `plaita` 指向另一个（落后的）检出**——本文件所有实测都在命令前加
  `PYTHONPATH=/tmp/w25-a` 并用 `plaita.__file__` 核过；重放器自己也有隔离探针
  （`plaita.__file__ ⊂ 导出树`），见 §4.2 日志。
- 未 push、未开 PR；gist 只作传输。

## §2 #79 —— `list_events` 返回完整性

### §2.1 在 current main 上的真实存活面（先复现）

复现脚本：`docs/proofs/data/repro79.py`（fakeredis，`decode_responses=False`，
与 `initialize()` 的 `aioredis.from_url` 生产默认同参）。修复前原文
`docs/proofs/data/raw/79-before-origin-main.txt`：

```
client decode_responses = False
typed   list_events('t00', limit=200) = 1 (expect 1)          # bytes 半边：#109/#110 已顺带修好
all     list_events(limit=200)        = 10 (expect 12)        # type_keys[:10] 截断存活
all     types = ['t00', ... 't09']                            # t10/t11 整类消失
all     globally time-ordered = True                          # 本样本恰好有序（见下）
limit=5 list_events(limit=5)          = 5 (expect 5)
11 types limit=11 -> 10 (expect 11)                           # 配额分母含未遍历类型
VERDICT: DEFECT-ALIVE
```

**与 issue #79 原文的偏差（如实记录）**：issue 说「生产客户端下恒返回空」；在
current main（43757bd）上该后果已被 **#109/#110** 的 `_zset_member_ids`
（`zrangebyscore` member 显式解码）顺带治好——`typed`/`all` 都非空，bytes 回归测试
在修复前即为绿。仍存活的是：**`decode_responses` 未显式开**（机制断言）、
**`type_keys[:10]` 整类截断**、**每类型 `max(1, limit // len(type_keys))` 配额 +
按类型扫描顺序拼接**（早事件在后面的类型时取不到、跨类型乱序）。乱序样本见
`tests/unit/test_issue79_event_storage_list_events.py::test_no_type_listing_is_globally_time_ordered`
（`a@1,a@3,b@2,b@4` ⇒ 旧实现 `[1,3,2,4]`，修复前红）。

### §2.2 修复

`plaita/event/redis.py`
- `initialize()` 自建客户端：`aioredis.from_url(self.redis_url, decode_responses=True)`
  （治本；外部注入的 bytes 客户端由 `_as_str` 兜底，两者都要）。
- 新增 `_as_str`（bytes/str → str）；`_zset_member_ids` 改为复用它。
- `list_events` 重写：
  - `_scan_type_keys()` 列出**全部** `types:*`（去重、显式解码）；
  - `_zrange_entries()` 对每个类型各取前 `limit` 条 `(score, event_id)`
    （**不再 `[:10]`**；每类型取 `limit` 条足以复原全局最早 `limit` 条）；
  - 全局按 `(score, event_id)` 排序后取 `limit`，再 mget 载荷，最后按
    `(timestamp, event_id)` 落序；
  - 索引成员载荷缺失：`logger.error`（带 event_id/key），不再静默跳过。

`plaita/event/memory.py`
- `list_events` 与 Redis 同契约：**先过滤 → 全局排序 → 再截断**（旧实现按插入顺序
  截断，`limit` 取到的是「最早插入的 limit 条」）；过滤改 `is not None`（`0.0` 是
  合法时间戳）。

修复后原文 `docs/proofs/data/raw/79-after-fix.txt`：`all = 12`、`11 types limit=11 -> 11`、
`limit=5` 取全局最早 `ev00..ev04`、`VERDICT: OK`。

### §2.3 红证（改回即红）

把 `plaita/event/redis.py`、`plaita/event/memory.py`、`plaita/server/event_reconcile.py`
换回 `origin/main` 版本（`docs/proofs/data/raw/red-base-junit.xml`）：

```
12 failed, 3 passed
```

红（12）：#79 的 `test_initialize_builds_client_with_decode_responses`、
`test_more_than_ten_event_types_all_returned`、`test_eleven_types_with_limit_eleven_all_returned`、
`test_no_type_listing_is_globally_time_ordered`、`test_limit_selects_globally_earliest_events`；
#85 的 `TestS2CompositeCursor`、`TestS4BatchSizeGuard`、`TestS6MemoryStorageOrdering`（2 条）、
`TestS7LoudMissingPayload`、`TestS8CursorNoCas`、`TestAH3CursorWriteLoud`。

3 绿（如实说明，**不是**"红证不成立"）：
1. `test_bytes_client_returns_stored_event`：bytes 后果已被 #109/#110 治好（§2.1）；
2. `TestS3ReplayFailureVisible`：#153 已在 main 上修好「失败可见」；
3. `TestAD5WindowEndClamp`：main 的旧游标根本不按事件推进（每轮直接推 `window_end`），
   所以「未来事件不越过窗口上界」这条在旧实现上**空洞地**成立；它守的是新游标逻辑。

### §2.4 负控（变异）

m01/m02/m03 分别把 `_as_str`/类型遍历/`decode_responses` 改回缺陷形态，
`--one` 全部 **KILLED**（记录见 `docs/proofs/data/170-replay-records.json`）：

| 变异 | 红用例 |
|---|---|
| m01-as_str-bytes-passthrough | `test_bytes_client_returns_stored_event` |
| m02-list-events-first-ten-types | `test_more_than_ten_event_types_all_returned` |
| m03-decode-responses-off | `test_initialize_builds_client_with_decode_responses` |

m109/m110（既有 #109/#110 定义）也因本次 redis 重写重新取锚并 **KILLED**。

## §3 #85 —— 回扫只在确认排空时推进游标

### §3.1 复现（current main）

`docs/proofs/data/repro85.py`：5 条事件落在回填窗口、`batch_size=2`、真
`MemoryEventStorage` + 字典桩游标。修复前原文
`docs/proofs/data/raw/85-before-origin-main.txt`：

```
scan#1 replayed: 2  seen=['ev0', 'ev1']  cursor=...207406
scan#2 replayed: 0  seen=['ev0', 'ev1']  cursor=...2074199
scan#3 replayed: 0  seen=['ev0', 'ev1']  cursor=...207428
scan#4 replayed: 0  seen=['ev0', 'ev1']  cursor=...207433
events never replayed: ['ev2', 'ev3', 'ev4']
VERDICT: DEFECT-ALIVE
```

游标每轮都被推到 `window_end`，`ev2/ev3/ev4` 不再落在任何后续窗口内。

### §3.2 修复（`plaita/server/event_reconcile.py`）

- **唯一推 `window_end` 的条件**：本页**未被存储截断**（`len(page) < limit`）且
  **没有**因 `batch_size` 留下的剩余；
- 未排空 ⇒ 游标推进到本轮已消费的最后一条 **`(timestamp, event_id)` 复合位点**，
  下一轮从该位点续扫（边界事件重扫由 `handle_event` 的 SET NX 去重键幂等吸收）；
- event_id 打破「同一时间戳并列」的活锁；若整页都是已消费前缀（超长并列），
  取数有界翻倍（≤ `CURSOR_SKIP_MAX_FACTOR=10`），仍越不过就不推进 + WARNING
  （不漏扫）；
- 守卫/响亮：`batch_size <= 0` 不取事件、不推进游标；游标**单调**（拒绝回退，
  WARNING）；消费到的事件时间戳越过 `window_end`（发布方时钟超前）时游标夹回
  `window_end`；游标写失败 **ERROR**（下一轮重扫、去重键不重复 resume）；
- 游标格式向后兼容：`"<ts>"`（旧）与 `"<ts>|<event_id>"`（新），bytes 客户端可读；
- **#153 失败语义不变**：任一事件回放失败即整轮作废、不推进游标、失败事件不计入
  `replayed`/`last_replayed`、计入 `last_rejected` 后上抛。

修复后原文 `docs/proofs/data/raw/85-after-fix.txt`：

```
scan#1 replayed: 2  cursor=...|ev1
scan#2 replayed: 1  cursor=...|ev2
scan#3 replayed: 1  cursor=...|ev3
scan#4 replayed: 1  cursor=...|ev4
events never replayed: []
VERDICT: OK
```

### §3.3 红证与负控

- 红证：§2.3 的同一份红树（`TestS2`/`TestS4`/`TestS8` 等 7 条红）；
  另有直接探针的 `VERDICT: DEFECT-ALIVE`（§3.1）。
- 负控（变异）：m09（batch 守卫）、m12（游标单调）、m47（窗口上界夹取）、
  m56（游标写失败响亮）逐条 **KILLED**：

| 变异 | 红用例 |
|---|---|
| m09-batch-size-guard-off | `TestS4BatchSizeGuard::test_batch_size_zero_no_crash_no_scan` |
| m12-cursor-rollback-guard-off | `TestS8CursorNoCas::test_save_cursor_refuses_rollback` |
| m47-window-end-clamp-off | `TestAD5WindowEndClamp::test_forged_skipped_cannot_push_cursor_beyond_window` |
| m56-cursor-failure-not-loud | `TestAH3CursorWriteLoud::test_cursor_write_failure_is_loud_with_ids` |

`m05-strict-after-becomes-inclusive` 的破坏语义（严格-after → 包含）从存储页 API
移到回扫器的 `(timestamp, event_id) > cursor` 边界，**file 随语义位点改变**并
**KILLED**（`TestS2CompositeCursor::test_batch_size_one_progresses_over_distinct_timestamps`）。

## §4 61 条变异定义（route C）

### §4.1 判定规则

- 只有 `old` 能在 **current main 的真实源码**里**诚实**取到（同一语义位点）才重锚，
  `new` 的**破坏语义**与 `expect_fail` 不变（`new` 的标识符/缩进随新源码适配，
  `rationale` 逐条比对）；
- 目标代码/测试在当前 main **不存在**的，**不伪造锚点**：逐字归档到
  `scripts/mutation_replay/mutations/parked/`，`PARKED.json` 记录所需未落地特性；
- `mutations/*.json`（激活集）与 `mutations/parked/`（归档）分开；
  `replay.py --check-anchors` 与 r12 聚合判决都只 glob 前者（不递归）。

### §4.2 11 条重锚 + 重放 KILLED（#79/#85 家族）

| 变异 | concern | 新锚位点 | 重放 |
|---|---|---|---|
| m01-as_str-bytes-passthrough | #79 | `_as_str` 的 bytes 解码 | KILLED |
| m02-list-events-first-ten-types | #79 | `_zrange_entries` 的全类型遍历 | KILLED |
| m03-decode-responses-off | #79 | `initialize()` 的 `decode_responses=True` | KILLED |
| m05-strict-after-becomes-inclusive | S2 | 回扫器 `(ts,id) > cursor` 严格边界 | KILLED |
| m08-handle-event-swallows-again | S3 | `event_filter.handle_event` 最外层 `raise` | KILLED |
| m09-batch-size-guard-off | S4 | `scan_once` 的 `batch_size <= 0` 守卫 | KILLED |
| m10-memory-list-events-unsorted | S6 | memory `list_events` 的 `sort` | KILLED |
| m11-missing-payload-silent | S7 | 载荷缺失的 `logger.error` | KILLED |
| m12-cursor-rollback-guard-off | S8 | `_save_cursor_raw` 的单调守卫 | KILLED |
| m47-window-end-clamp-off | AD5 | 消费位点越过 `window_end` 的夹取 | KILLED |
| m56-cursor-failure-not-loud | AH3 | 游标写失败的 ERROR + 后果说明 | KILLED |

这 11 条的 `expect_fail` 全部落在本次**随修复入库**的两个回归文件
（`tests/unit/test_issue79_event_storage_list_events.py` 6 测 +
`tests/unit/test_issue85_reconcile_hardening.py` 9 测；后者是 **route C 最小实现的
子集**，文件头已明说不是 `fix/79-85` 的完整加固套件，类名沿用分支命名只为让已重锚
定义能原样重放）。

### §4.3 50 条归档（`mutations/parked/`）

它们锚定的机器在当前 main **整体不存在**（`event_reconcile.py` 216 行 vs 分支
2083 行），无处可取锚；逐条所需特性见 `parked/PARKED.json` 的
`unlanded_feature`/`reason`。按族：

- 页契约/排空：`m07 m17 m19 m21 m22 m24 m25`（`EventPage`/`list_events_page`/
  `exhausted`/`stopped_early`/`drain confirmation`）
- 隔离清单（quarantine）：`m13 m23 m27 m28 m30 m31 m32 m33 m34 m35 m44`
- 去重认领/landed 探针：`m15 m16 m20`
- 调用侧封顶/条件写：`m29`（`advance_boundary`）
- 读失败/绝对 due/两键自愈/skipped 对账：`m36 m37 m38 m39 m40 m41`
- 自愈游标/钉住/dead-letter/时钟：`m42 m43 m45 m46 m48 m49`
- 停滞/left_pending/按轮计数：`m50 m52 m53 m54`
- 饥饿看门狗/小件/扫描可达：`m57 m58 m62 m63 m65 m66 m67`
- KV marker/无推进上界/重试溢出：`m59 m60 m61 m64`
- 积压趋势/停滞计数（AS/AY/BB）：`m70`

**这不是"删掉 50 条来让门禁变绿"**：它们今天在 main 上就是死重（`old` count=0、
`expect_fail` 测试 0/61 在场），归档把「不可重放」变成**显式、可审计**的状态，
并保留了逐字节原文与所需特性；`fix/79-85` 落地后原样移回即可重放/重切。

### §4.4 收尾证据

1. `test_every_mutation_anchor_appears_exactly_once`：
   `TestIssue69R12MutationJsonContract` **2 passed**（`docs/proofs/data/170-check-anchors-json-store.txt`）。
2. JSON store 锚点检查：`305 条定义，问题 0 条`，rc=0。
3. 18 条重锚逐条 `--one` 重放：**18/18 KILLED**（`170-replay-records.json` +
   `170-replay-summary.txt`；双机读信道一致）。
4. 负控（抽 3 条毒化 `old`：m01 / m12 / m47）⇒ **3/3 stale**
   （`170-negative-controls.txt`：`kind=anchors`，`old 命中 0 次（应为 1）`，
   rc=1，锚点检查先于 pytest、无测试运行）。

### §4.5 full-store `--check-anchors` 的存量红（与本次无关，如实披露）

`--check-anchors` 同时加载 `mutations.py`。该文件是 #133/#33 家族，在 current main 上
**本就整体陈旧**（目标文件 `plaita/core/_author_capability.py` 不存在；
`expression.py`/`expression_parser.py` 的 `old` 大量命中 0）。full-store 输出
（`docs/proofs/data/170-check-anchors-full.txt`）409 条定义里 **104 条报问题**
（49 条含「目标文件不存在」、55 条含「old 命中 0 次」，两组有交叠），全部是
`M*` 前缀（`M9`…`M33`），**0 条**是本次 61 条 / JSON store。它们不属于 #170 的处置面，也没有被本次改动触碰；要让 full-store 全绿需
#133/#33 修复落地或另单重锚，本文不越界修改。

## §5 横切守卫（新规补跑）

改了事件/存储/日志面 ⇒ 补跑，结果全绿：

```
tests/unit/test_no_silent_swallow.py
tests/unit/test_expression_size_guard.py
tests/integration/test_layering.py
+ 事件/回扫定向子集
=> 682 passed, 979 subtests passed
```

- `tests/unit/test_issue151_log_hygiene.py`：**本基线不存在**（W2 批次 #168/#151 未落地），
  无法补跑；本改动不新增 f-string 日志（全部 lazy `%` 参数），并显式降噪：
  载荷缺失由静默 → ERROR、游标写失败由 WARNING → ERROR。
- 没有为变绿放宽任何白名单/判据；没有 `--no-verify`；未跑 plaita 根包全量
  `tests/unit`（#169 已知挂起点），只用定向子集 + 分层守卫。

## §6 边界与残余（诚实）

- #79 的 **bytes 后果**在 current main 已被 #109/#110 治好；本次仍补了
  `decode_responses=True`（机制侧）与 `_as_str`（注入侧），并新增 bytes 回归，
  但**不能**声称"本次修好了生产恒返回空"——那是 #109/#110 的功劳。
- #85 是**最小产品修复**，不是 `fix/79-85` 的完整加固：无页契约
  （`EventPage`/`list_events_page`）、无隔离清单/自愈/看门狗/dead-letter；
  同时间戳超长并列只保证**不丢**（有界翻倍），不保证单轮内推进到尾部。
- 50 条归档定义在加固面落地前**不可重放**；这是 issue #170 的原始形态
  （"fix 落地前死重"），本次只把状态显式化，没有把它们的验收口径改松。
- 未 push、未开 PR。

## §7 复现命令

```bash
cd /tmp/w25-a
export PYTHONPATH=/tmp/w25-a
PY=/Users/jeff.jie/.venvs/plaita/bin/python

# 红/绿（§2、§3）
$PY -c 'import plaita; print(plaita.__file__)'          # 必须是 /tmp/w25-a/plaita/...
$PY docs/proofs/data/repro79.py
$PY docs/proofs/data/repro85.py

# 回归面
$PY -m pytest tests/unit/test_issue79_event_storage_list_events.py \
  tests/unit/test_issue85_reconcile_hardening.py \
  tests/unit/test_event_reconcile.py tests/unit/test_event_memory_unit.py \
  tests/unit/test_issue109_110_r3_parity.py \
  tests/integration/test_event_filter_dedup.py -q

# r12 聚合判决
$PY -m pytest \
  'tests/unit/test_issue69_r12_c_storage_and_view_layout.py::TestIssue69R12MutationJsonContract' -q

# JSON store 锚点（只挂 mutations/，不加载 #133/#33 的 mutations.py 存量红）
bash docs/proofs/data/check-anchors-json-store.sh

# 逐条重放（18 条）
$PY scripts/mutation_replay/replay.py --one m01-as_str-bytes-passthrough --source worktree

# 横切守卫
$PY -m pytest tests/unit/test_no_silent_swallow.py \
  tests/unit/test_expression_size_guard.py tests/integration/test_layering.py -q
```

## 附：产物清单（`docs/proofs/data/`）

| 文件 | 内容 |
|---|---|
| `170-anchor-census.json` | 61 条逐条状态（11 重锚 + 50 归档）+ 18 条重放判定 + 存量红统计 |
| `170-replay-records.json` | 18 条重锚的机读重放记录（killed/red） |
| `170-replay-summary.txt` | 18 条重放的终端汇总（含 rc） |
| `170-check-anchors-json-store.txt` | JSON store 锚点检查（305/0，rc=0） |
| `170-check-anchors-full.txt` | full store 锚点检查（含 `mutations.py` 存量红） |
| `170-negative-controls.txt` | 3 条毒化 `old` 的 stale 负控原始输出 |
| `170-red-base-junit.xml` | 换回 `origin/main` 源码后的 junit（12 failed / 3 passed） |
| `raw/79-before-origin-main.txt` / `raw/79-after-fix.txt` | #79 修复前后原始输出 |
| `raw/85-before-origin-main.txt` / `raw/85-after-fix.txt` | #85 修复前后原始输出 |
| `repro79.py` / `repro85.py` | 两个复现脚本 |
