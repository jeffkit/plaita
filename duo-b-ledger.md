# B1 席位交付台账

> 一行摘要（第三十轮）：#252 P1——`run()` 兜底死信接入 `dead_letter_if_allowed`，活租约/瞬断时守卫拒绝即不 ack、消息留 PEL；捆绑修 `max_deliveries=1` 首投零尝试死信（首投必得一次处理尝试）。负控 4F/2P 与 4F/1P 精确红。

| 轮次 | Issue | 交付内容 | 验收 |
|---|---|---|---|
| 第三十轮 | [#252](https://github.com/jeffkit/plaita/issues/252) + A1 r37 a/P2（捆绑，#252 同域） | **#252 P1**：新增 `RedisStreamTaskQueue.dead_letter_if_allowed(task, reason)`——守卫 `None` 恒放行；拒绝/抛异常返回 `False` 并推进 `dlq_guard_skipped`，不 ack、不 xadd。`run()` 兜底异常分支改用它；拒绝时 WARNING 带 execution_id/delivery_count，消息留 pending。**捆绑单**：`_read_once` 删除首投 `>= max_deliveries` 预判死信（`xreadgroup(">")` 恒为首投、此前零处理尝试）；`max_deliveries=1` 现在允许恰好一次处理尝试，失败/未 ack 后由 worker 兜底或 reclaim 死信。选择「修语义」而非「钉死契约」：`max_deliveries` 在 `__init__`/reclaim 注释与 `flow-worker.md` 的既定口径是「N 次处理尝试后死信」，首投零尝试不能套用预算耗尽判据。`flow-worker.md` 同步钉死计数口径。新增 `test_issue252_dlq_guard_fallback.py` 9 条；D1 桩补 `dead_letter_if_allowed` 接口 | 新文件 **9 passed**；定向+相关 11 文件 **133 passed**；**负控①** 兜底还原直调 `dead_letter` → **4 failed / 2 passed**（活租约/瞬断精确红在 `dlq_length == 0`；预算耗尽/孤儿重入队也红，证明守卫未被调用）；**负控②** `_read_once` 还原首投死信 → **4 failed / 1 passed**（3 条 max=1 用例 + single-attempt 参数化）；`mkdocs build -f docs-site/mkdocs.yml --strict` **rc=0**；ruff 仅既有 BLE001（改动行无新增）；`landing_gate.py ci` **绿**（main `d635165` CI success）；代码 commit `a219f41`（closes #252），推送 main |
