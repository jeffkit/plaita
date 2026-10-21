# r52 证明（#271）：TASK_MAX_AGE=0 在生产者/worker 双侧同义

- 单据：plaita#271（P1，producer/worker budget 分叉 + age=0 永久 PEL）
- 基线：`origin/main` = `02b67b8`（含 r51 `_budget_int`/默认 120）
- 边界：只修 `envelope_for_stream` 对 `PLAITA_TASK_MAX_AGE_SECONDS` 的解析；
  不改变 `0=关闭年龄闸` 语义，不处理其他预算/CLI 覆盖（#269/#270 另单）。

## 根因

worker 用 `plaita.core.retention.task_max_age_seconds()`（`<=0` → 0，非法 →
30d + WARNING）；`envelope_for_stream()` 用
`_safe_int(..., minimum=1)`，把 0 当非法回退 30d。同一配置下：

- worker budget = `bg1(... age=0 ...)`；
- 生产者信封 budget = `bg1(... age=30d ...)`；
- 首次投递不查 budget（能执行）；失败/崩溃留在 PEL 后，reclaim 一律判
  `config_budget_stale` 跳过；而 age=0 又让 `_config_age_expired` 恒 False
  ⇒ 没有年龄闸/DLX 逃生，消息永久 pending。

## 修法

1. `plaita/core/retention.py::task_max_age_seconds(environ=None)` 增加可选
   映射参数（生产默认 `os.environ`），保持单一解析口径。
2. `envelope_for_stream` 的 `task_max_age` 改为
   `_task_max_age_seconds(env)`：
   - `0` / 负数 → 0（关闭年龄闸，与 worker 一致）；
   - 非法字符串 → 30d + WARNING（core 既有口径）；
   - 其余正数原样。
3. 其余三个预算键仍用 `_safe_int(minimum=1)`（0/非法 → 默认），不变。

## 测试（4 条，`tests/unit/test_r52_age_zero_alignment.py`）

- age=0：生产信封 budget == `load_runtime_config("worker")` 的 budget，
  且等于 `bg1(age=0)`；
- age=-1：core 的 `<=0=关闭` 语义在生产者侧同样成立，budget 同为 `bg1(0)`；
- 非法字符串：core 回退 30d 且 WARNING，信封 budget 为 `bg1(30d)`；
- reclaim：age=0 + 生产者/worker 同 budget，首次投递进 PEL 后由另一
  consumer XCLAIM，任务被重投且 `config_budget_stale == 0`（负控下
  reclaim 返回 None 并打 budget_stale）。

## 负控（先红后绿）

把 `envelope_for_stream` 改回
`_safe_int(env.get("PLAITA_TASK_MAX_AGE_SECONDS"), DEFAULT_TASK_MAX_AGE_SECONDS)`：

```
3 failed, 1 passed
E       assert 'bg1-6a07ef4af763' == 'bg1-3aa045b1fc18'   （age=0 budget 分叉）
FAILED ...test_age_zero_budget_matches_worker
FAILED ...test_age_negative_keeps_same_zero_semantics
E       AssertionError: age=0 carrier must be reclaimable; pre-fix producer
        used 30d while worker used 0 -> budget mismatch -> permanent pending
E       assert None is not None
FAILED ...test_age_zero_reclaim_not_budget_stale
WARNING ... event=config.budget_stale ... source_budget_generation=bg1-6a07...
        local_budget_generation=bg1-3aa045b1fc18 ... 消息留 pending
```

还原修复后 **4 passed**。完整日志
`docs/proofs/data/r52-age-zero-negative-control.txt`。

## 验收（2026-10-21，基线 02b67b8）

- 新增 4 passed；相邻 A2R31/task_queue/worker 面 **94 passed**；
- 全量 unit **8687P/12S/14 deselected/3498 subtests**；e2e+integration
  **287P/4S**；console **1045P/14S**；plaita-ai **241P/14S/12 subtests**；
  layering/wheel/size 全绿；`landing_gate.py ci` 绿后单 commit 推送
  （closes #271）。
