# r87 · A2R50-02 — outbox 人工出口（resolve/discard）入 `OUTBOX_DELETE_LOCK`（#303 锁族收口）

- 席位：B3 第八十七轮（worktree `/private/tmp/plaita-b3-r87`，分支 `fix/a2r50-outbox-lock`）
- 基线：origin/main `2e59e4d`（任务书基线 `5a3c911`=r86 之后、B2 r83 已合；两文件面与 r83 的 api/schedules 面零交集）
- 规格：A2 台账「第五十轮」A2R50-02 + r56 核销表（核销注记：#303 的锁只进了
  `deleting_guard` 与投递/对账循环，`resolve_row`/`discard_row` 两个人工出口仍在锁外）
- 剧本：A2 r56 归档探针换根复跑（`~/.issue-keeper/duty/a2-r56-scripts/test_a2r50_axis23_outbox_schedule-r56.py`）

## 缺陷（A2 r50 已复现，本轮回放确认）

`resolve_row`/`discard_row` 的 deleting 检查是**锁外 check-then-act**：行读在
`mark_deleting` 提交之前、状态写在之后 → tombstone 被覆盖（resolve）/行被抢删
（discard）。删除事务随后失败时 `restore_deleting` 只恢复「仍为 deleting」的行 →
被出口动过的行不恢复：published 版本永久失去投递/对账载体（投递扫不到
pending/failed、对账扫不到 delivered、重复 publish 幂等早退不重建行）→ 跨 Redis
部署幽灵不可自愈。

确定性重放手法（A2 剧本）：monkeypatch `_get_scoped_row` 在返回前插入
`mark_deleting`（独立事务已提交）——= 真实竞窗中「读后、写前」被删除协议插队，
行对象保持陈旧读。

## 修法（`plaita-console/backend/services/engine_outbox.py`）

`resolve_row`/`discard_row` 检查与写**全程入 `OUTBOX_DELETE_LOCK`**（与 #303
`deleting_guard`/投递/对账同锁），锁内 `_reload_row` 重读现态——竞窗内落下的
tombstone/删除在此显形：deleting → `ValueError`（API 既有映射 409
`outbox_row_not_eligible`）、已删 → `None`（404），陈旧读不放行。语义守恒：
delivered → 409、租户不匹配/不存在 → 404、resolve 幂等留痕、discard 删行，全部
不变。锁粒度=单行动作（与投递/对账一致，毫秒级），等待方最多让出一个行动位，
不饿死。模块文档与 `OUTBOX_DELETE_LOCK` 注释同步收口；runbook 人工出口段落补
锁语义（docs-site `ops-runbook.md`）。

## 负控（先红后绿）

修前剧本探针（`data/r87-probe-a2r50-manual-exit-prefix.py`，A2 r56 剧本换根到本
worktree）**3 红格**（`r87-probe-prefix-output.txt`）：discard 竞窗放行（返回
failed）/ resolve 竞窗放行（返回 resolved）/ 幽灵链（restore 恢复 0 行、行已删、
published 版本无任何自愈载体）。

修后同探针（`r87-probe-prefix-on-fixed-tree.txt`）：3 格全部以具名拒翻红——
`ValueError: 该行处于删除协议中（deleting）`×3，无一格仍放行。

上游化负控 `tests/console/test_a2r50_outbox_manual_exit_lock.py` **6 用例**：
基线（stash 修法）**5 failed / 1 passed**（`r87-tests-baseline-red.txt`；红因
3×DID NOT RAISE + 等锁格「出口已在在途投递期间完成」+ API 层 200≠409）→ 修复树
**6 passed**：

| 格 | 断言 |
|---|---|
| 竞窗具名拒 ×2（resolve/discard 参数化） | 竞窗内落下的 tombstone 必被锁内重读看见 → ValueError(deleting)，tombstone 原样 |
| 幽灵链全程 | 竞窗出口被拒 → restore 恢复 1 行 → failed 行回投递扫描（自愈可达，无幽灵） |
| 等锁对照面 | 在途投递（失败投递构形）持锁期间出口不得落状态；放锁后按现态落 resolved——等待有限不饿死 |
| API 层 | 竞窗内 resolve → 409 `outbox_row_not_eligible`（具名码），行保持 deleting |
| 守恒锚 | 顺序语义零漂移：resolve 置终态留痕且幂等、discard 删行、404/租户面不变（基线亦绿） |

## 守恒（全部无管道直录 rc）

| 族 | 结果 | rc |
|---|---|---|
| #303 既有 10 用例 + outbox 族 33P + 新增 6P 定向 | 49 passed | 0 |
| console 全量 | 1463 passed / 14 skipped | 0 |
| 根仓 unit 全量 | 8875 passed / 13 skipped（+3501 subtests） | 0 |
| integration | 213 passed / 1 skipped | 0 |
| e2e | 74 passed / 3 skipped | 0 |
| ruff check（改动两文件） | All checks passed | 0 |
| mkdocs strict（等价 CI 命令，3.10 venv） | built in 6.14s | 0 |

证据文件：`docs/proofs/data/r87-{family-outbox,console,unit-root,integ,e2e,mkdocs}.txt`。

备注两条：①`ruff format` 对 `engine_outbox.py` 基线本就有既存漂移（origin/main
直检即 would reformat，与 r86 备注同性质）；仓门只跑 `ruff check`（pyproject：
「CI 门禁只跑 ruff」），新增文件风格与族内既有测试一致。②unit/console 计数与
r86（8936/1399）的差异来自基线前移（2e59e4d 含 r83 等他人合并），非本单增删。

## 对照面（#303 语义不变）

`OUTBOX_DELETE_LOCK` 语义仍为「行动粒度串行」：`deleting_guard` 全程持锁、投递/
对账逐行动作前锁内重读、人工出口同样在行动粒度上持锁并重读——四面共用同一把
行级锁，删除窗口内既不可能有投递写出 key（#303 原口径），也不可能有人工出口
盖掉 tombstone（本单新收口）。既有 `test_issue303_delete_vs_outbox_race.py` 10
用例零改动全绿，含在途投递持锁时删除等待格（`test_delete_waits_for_inflight_
delivery_then_purges`）与 tombstone 顺序拒格（`test_manual_exit_rejects_deleting_row`）。
