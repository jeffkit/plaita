# RED-PROOF — #196 reclaim 扫描窗口按游标轮转

日期：2026-10-18 · 执行者：修复落地席（jeffkit）· 基线 `357f2cd` · 单 commit
工作区：`/Users/kong/projects/infra4agent/plaita`

## §0 结论速览

| 验收项 | 结果 |
|---|---|
| 9 条 repro（前 8 活跃、e8 孤儿） | 修复前 3 轮 `[]`；修复后第 2 轮 reclaim `e8` |
| 20 pending / 15 活跃 / 5 孤儿 | 修复前 10 轮 `[]`；修复后 **7 轮** 全部 reclaim `e15..e19` |
| 活跃条目 | `times_delivered` 全程 **1**（#47 闸门语义不变，不被 XCLAIM） |
| 窗口可配置 | `reclaim_window` 参数（默认 8）；窗口=2 的轮转用例通过 |
| 游标自身 | 显式跳过（防 idle 归零后自我 XCLAIM 刷 delivery_count） |
| 负控 | 固定 `count=8`/无游标 ⇒ 新文件 **3 failed / 2 passed**（2 条 #47 守卫仍绿） |
| 验收面 | unit 快车道 **7740 passed / 3429 subtests**；e2e **74 passed / 3 skipped**；integration **213 passed / 1 skipped**；`ci-gate.sh` **PASSED**（8040 passed / 覆盖率 93.83%）；`landing_gate.py ci` **success** |

## §1 缺陷与规格

`_reclaim_one` 每轮固定 `xpending_range(min="-", count=8)`（`plaita/server/
task_queue.py:607-613`）：前 8 条租约活跃时，租约闸门 `continue` 跳过它们，
for 循环结束直接 `return None`——第 9 条起的真孤儿永远不进入扫描，XCLAIM
恢复路径被固定窗口饿死（#47 残面）。

提报席规格（#196 comment `6237466319`）：
1. 扫描窗口**有轮转/游标**，从上次 id 之后继续、到尾部回绕；
2. 20 pending（15 活跃 + 5 孤儿）在有限轮内全部 reclaim，测试给出轮次上界；
3. 活跃条目不 XCLAIM、`delivery_count` 不增长（#47 语义不变）；
4. 窗口大小可配置（默认 8），但必须有轮转；
5. 负控：固定 `count=8`/无游标 ⇒ 新用例红；#47 用例全绿。

**载体**：分支 `remote/p1-batch-186-189-194-196` 不存在（`couldn't find remote
ref`；树 `33f4a36f6109` 非本仓对象）。gist `46a9fd119af5449fdd65eaab8db6a29f`
的 `w32.patch` 是 **#186/#189/#194/#196 批**；本 commit **只提取 #196 的
`task_queue.py` 游标 hunk + `test_issue196_reclaim_rotation.py`**，未应用
#186/#189/#194（边界：只动 reclaim 面）。

## §2 修复

`plaita/server/task_queue.py`：

- 构造函数新增实例级 `self._reclaim_cursor: Optional[str] = None`，以及
  `reclaim_window`（默认 8，`max(1, int(...))`）；不新增 Redis 键。
- `_reclaim_one`：
  - `window = self.reclaim_window`；
  - `xpending_range(min=self._reclaim_cursor or "-", max="+", count=window)`；
  - 空页 ⇒ 游标归 `None`（下轮回到头部）；
  - 遍历时记录 `last_scanned`；条目等于 `scanned_from`（游标自身）则跳过
    （防 XCLAIM 归零 idle 后自我抢占）；
  - 成功 XCLAIM / 就地死信处置后把游标推进到 `claimed_id`；
  - 整窗扫完：短窗（到尾部）⇒ 游标 `None`；整窗 ⇒ 游标 `last_scanned`
    （窗口尾下轮作为游标跳过，实际推进 7 条新条目）。
- #47 租约闸门极性（True=活跃→跳过）、XCLAIM 条件、单条返回契约、DLQ
  `max_deliveries` 口径均未改。

## §3 修复前 / 修复后

探针：`docs/proofs/data/196-repro.py`。修复前在干净 worktree `357f2cd` 跑
（`PYTHONPATH=/tmp/wt196-before`），修复后在本树跑；机读结果
`docs/proofs/data/196-before-after.json`。

| 场景 | 修复前 | 修复后 |
|---|---|---|
| 9 pending：前 8 活跃，e8 孤儿；3 轮 | `reclaimed=[]` | `reclaimed=['e8']`（第 2 轮） |
| 20 pending：前 15 活跃，e15..e19 孤儿；10 轮上限 | `reclaimed=[]`（10 轮） | `reclaimed=['e15'..'e19']`（**7 轮**） |
| 活跃条目 `times_delivered` | 全 1 | 全 1（未被 XCLAIM） |
| 窗口=2（`reclaim_window`） | 参数不存在 | 6 pending / 3 活跃 / 3 孤儿，3 轮内回收 |

## §4 验收

### §4.1 定向

```
pytest tests/unit/test_issue196_reclaim_rotation.py -q  ⇒ 5 passed
pytest tests/unit/test_issue196_reclaim_rotation.py \
       tests/unit/test_issue47_reclaim_lease_guard.py \
       tests/unit/test_clean_c4_task_queue.py \
       tests/unit/test_task_queue.py -q  ⇒ 33 passed
```

覆盖：9 条 repro、20/15/5、活跃不 XCLAIM、单条活跃回绕、窗口可配置；
#47 租约闸门与既有 task_queue 用例全绿。

### §4.2 unit 快车道

```
pytest tests/unit -m "not slow" -q ⇒ 7740 passed, 11 skipped, 13 deselected,
3429 subtests passed
```

### §4.3 e2e / integration

```
pytest tests/e2e -q          ⇒ 74 passed, 3 skipped
pytest tests/integration -q  ⇒ 213 passed, 1 skipped
```

### §4.4 本地门禁与 landing gate

- `ci-gate.sh`：**PASSED**。`pytest tests/` ⇒ **8040 passed / 15 skipped /
  3429 subtests**（120 warnings），覆盖率 **93.83%**（阈值 80%）；import
  layering 3 passed；wheel 无 `test_*.py`；SC-003 类尺寸硬顶满足。
- `landing_gate.py ci`：**success**——`origin/main` 最近完成 run `357f2cd`
  conclusion=success，放行。

## §5 负控（变异纪律）

在 `e308a4c` 的临时 worktree 中恢复固定头 8 条、无游标：

```diff
-                min=self._reclaim_cursor or "-",
+                min="-",
-                count=window,
+                count=8,
-        scanned_from = self._reclaim_cursor
+        scanned_from = None
...
-                self._reclaim_cursor = claimed_id
+                self._reclaim_cursor = None
...
-        self._reclaim_cursor = None if len(pending) < window else last_scanned
+        self._reclaim_cursor = None
```

结果：

```
tests/unit/test_issue196_reclaim_rotation.py ⇒ 3 failed, 2 passed
```

- **红**：`test_issue_repro_ninth_orphan_is_reclaimed`（3 轮仍 `[]`）、
  `test_twenty_pending_fifteen_active_five_orphans_all_reclaimed`（10 轮仍
  `[]`）、`test_reclaim_window_is_configurable`（无游标时已 claim 的 e3 被
  反复抢到，收不齐 e3/e4/e5）；
- **绿**：`test_active_entries_are_never_xclaimed`、
  `test_single_active_pending_stays_untouched_across_rounds`（#47 守卫语义，
  不依赖轮转）。

变异 diff 与红名单：`docs/proofs/data/196-negative-control.diff` /
`196-negative-control.txt`；SHA256 见 `196-before-after.json`。

## §6 关系与边界

- **#47**：活跃租约闸门与 `times_delivered` 语义不变；#47 用例全绿。
- **#186/#189/#194**：与 #196 同在一个 gist 批次（`w32.patch`），但本 commit
  只提取 #196 的 task_queue 游标 hunk；`InvalidTaskPayload` / `get_flow` /
  节点超时等文件未触碰（边界）。
- **#195/#198**：有独立规格单，未混入。

## §7 复跑

```bash
# 修复前/后探针
PYTHONPATH=/tmp/wt196-before .venv/bin/python docs/proofs/data/196-repro.py
PYTHONPATH=.                .venv/bin/python docs/proofs/data/196-repro.py

# 定向
.venv/bin/python -m pytest tests/unit/test_issue196_reclaim_rotation.py -q
.venv/bin/python -m pytest tests/unit/test_issue196_reclaim_rotation.py \
    tests/unit/test_issue47_reclaim_lease_guard.py \
    tests/unit/test_clean_c4_task_queue.py tests/unit/test_task_queue.py -q

# 验收面
.venv/bin/python -m pytest tests/unit -m "not slow" -q
.venv/bin/python -m pytest tests/e2e -q
.venv/bin/python -m pytest tests/integration -q
bash scripts/ci-gate.sh
python3 scripts/landing_gate.py ci
```

## §8 变更文件

- `plaita/server/task_queue.py`：`_reclaim_cursor` 轮转/回绕、游标自跳过、
  `reclaim_window` 可配置；
- `tests/unit/test_issue196_reclaim_rotation.py`：5 条性质/守卫/窗口测试；
- `docs/proofs/data/196-repro.py` / `196-before-after.json` /
  `196-negative-control.{diff,txt}`：机读旁证。
