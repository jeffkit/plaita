# r98 / A135-01（P2）：租户隔离流进 console 列表/replay 面 + 回灌成功摘条清债

- 基线：`origin/main` `9072b9a`（A1 r135 判定基线 `14c9691` 的后裔，已核祖先关系；
  五处执法点行号 528/559/585/598/688 与 A1 台账逐一吻合）
- 分支：`fix/quarantine-replay`（独立 worktree `/private/tmp/plaita-b3-r98`）
- 规格：duo-a-ledger.md 第一百三十五轮 §1 A135-01 + 任务书清债面（重放成功摘条）

## 缺陷（A1 r135 §1）

A125-01/02 收容终局写 `{dlq}:quarantine:{tenant}`（task_queue.py:1302），承诺
「租户恢复后按信封内源世代人工重放可救」，但该键不以 `:dlq` 结尾、也不是登记
队列键——console 五处执法点全拒：

1. `_is_stream_key`（queues.py:528）只认登记 stream 与 `*:dlq`；
2. `list_dlq`（:559）只收 known family `*:dlq`——隔离流列表不可见；
3. `_resolve_dlq_key` 显式 key（:585）要求 `endswith(":dlq")` 且登记——404；
4. `_resolve_dlq_key` 无 key 扫描（:598）只扫 `*:dlq`；
5. replay 目标侧（:688）同样 `:dlq` 后缀执法——且隔离流键尾截断是空操作，
   默认目标错构。

## 修法

- 键形态识别：`_split_quarantine_key`（按**第一**段 `:quarantine:` 切分——租户
  id 自身含该段时其后全归租户；基键须为已登记 `*:dlq` + 非空租户段）+
  `_is_quarantine_key`/`_quarantine_tenant`/`_quarantine_keys`（基键锚定有界
  SCAN，#202 同形，逐键复核形态）。
- 五处执法点泛化：`_is_stream_key` 收隔离流；`list_dlq` 纳入隔离流（键名自带
  租户段——**租户视角只收本租户的隔离流**，别租户连键名存在性都不泄露＝越权格；
  平台视角全量；条目仍过租户/世代过滤）；`_resolve_dlq_key` 显式 key 与无 key
  扫描双路径放行；replay 默认目标按基 DLQ 反推（`{dlq}:quarantine:{t}` → 源任务
  队列），目标校验加「隔离流键不可作目标」。
- 清债面：回灌成功（`reason=ok`）后隔离条目 XDEL 摘除；`already_replayed`
  （幂等标记在案=此前已成功）也补摘，覆盖首次摘条失败残留；失败路径
  （schema_too_new 等）条目保留。隔离流场景响应多带 `quarantine`/`entry_removed`
  两键；**平台 DLQ 响应体逐字节不变**（`return result.as_dict()` 原路径未动，
  `target_tenant` 仍只在审计 detail 回显）。
- 重放本体零改动：`replay_dead_letter`（dlq_replay.py）对 key 本就通用、信封
  五世代键全兼容（A1 r135 §1「反差」面证实）——console 放行即达。

## 负控（tests/console/test_r135_quarantine_replay.py，15 项）

先红后绿：`git stash push -- queues.py` 跑基线 → **10 红 5 绿**（红=全部隔离流
行为面：列表可见性/越权格/A125-01 停用租户重放/A125-02 legacy 世代继承/失败
保留/无 key 定位/already_replayed 补摘/审计/键形态识别；绿=恰为守恒与不放水钉：
无隔离流时列表零新增、隔离流键作目标 422、畸形键 404、DLQ 响应形状、DLQ 零值
行）→ pop 后 **15/15 绿**。

- A125-01 场景：收容信封（全世代键）→ replay ok → 目标条目 schema/config/
  budget/tenant_generation/config_effective_at 逐键继承 + 隔离条目摘除；
  摘条后同 id 再打=具名 404。
- A125-02 半边：无 `tenant_generation` 的 legacy 世代载体 → replay ok → 目标
  条目不写该字段（缺失=legacy 继承，不按当前 meta 猜）。
- 越权格：acme/beta 双隔离流，acme viewer 视角只见本租户流，平台视角双见。
- already_replayed 补摘：预置幂等标记+条目 → replay 返回 already_replayed 且
  摘除生效。
- DLQ 守恒：原条目保留、响应键集 = 原 7 键（无 quarantine/entry_removed/
  target_tenant）、KNOWN_DLQ_KEYS 零值行照旧。

## 门禁（rc 直录，命令无管道）

| 门 | 命令（cwd） | rc | 结果 |
|---|---|---|---|
| console 全量 | `.venv/bin/python -m pytest tests/console -q`（plaita-console/backend） | 0 | 1541 passed, 14 skipped |
| unit 全量 | `.venv/bin/python -m pytest tests/unit -q`（仓根） | 0 | 8938 passed, 13 skipped, 3501 subtests |
| ruff | `ruff check queues.py test_r135_quarantine_replay.py` | 0 | All checks passed |
| mkdocs | `mkdocs build -f docs-site/mkdocs.yml --clean --strict`（3.10 venv） | 0 | built in 6.86s |

日志归档：`docs/proofs/data/r98-quarantine-replay/`（pre-red/post-green/console/
unit/ruff/mkdocs 六件）。

## 文档

- `docs-site/docs/distributed/flow-worker.md`：收容终局段补 console 重放面
  （列表可见性/回灌路径/摘条清债语义/失败保留）。
- `docs-site/docs/distributed/ops-runbook.md`：#277 租户世代写闸条目补「隔离流
  的观察与重放」操作形制。

## 施工中抓到的两个自伤（已修，非基线缺陷）

1. 首版 `_is_quarantine_key` 用 `rpartition`（最后一段切分）——租户 id 自身含
   `:quarantine:` 时基键解析错位；改 `partition`（第一段）+ 切分结果作单一
   事实源（`_split_quarantine_key`），端点默认目标同源。
2. 端点原代码 `return result.as_dict()`（二次构造新字典）——扩展键落在仅进
   审计的 `payload` 变量上响应不可见；守恒要求使然不改原返回路径，隔离流场景
   显式返回扩展后的 dict。
