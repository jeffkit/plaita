# r88 · A2R53-01 — create_tenant 状态发布与世代发布同标 fail-closed（墓碑拦行窗收口）

- 席位：B3 第八十八轮（worktree `/private/tmp/plaita-b3-r88`，分支 `fix/a2r53-tenant-status`）
- 基线：origin/main `1ed9723`（=r87 已合，与任务书一致；开工期间 origin/main 前进
  `df894c7`/`092e8a0`=B2 r85 调度重章面，`git diff --name-only` 核与本次触面
  零交集，已 rebase 至 `092e8a0`，提交=`6515d4a`）
- 规格：A2 台账「第五十三轮」A2R53-01 + 「第五十六轮」核销表（红·实弹）；
  探针手法复刻自 `~/.issue-keeper/duty/a2-r56-scripts/probe_a2r53_tenant_create_tombstone-r56.py`

## 缺陷（A2 r53 实弹复现，r56 复核仍红）

`create_tenant` 同函数双标：世代发布 `_publish_generation`（事务内）失败 →
TenantError 回滚建租户（#284 fail-closed）；状态发布 `_publish_status`
（commit 后）经 `publish_tenant_status` 的 `except Exception: warning` 吞掉
（tenant_status.py:124-129）。删除路径写下的 disabled 墓碑（#255）在同名重建
时若这一次 hset 恰好故障（抖动/READONLY/池耗尽，与毫秒前的世代发布成功只隔
毫秒级）→ create 200 + 契约密钥签发 + DB 行 active，Redis 短视图仍是
disabled，worker 的 `tenant_is_active` 拦该租户一切 start/resume——
「console active / 运行面 disabled」拦行窗，自愈只剩下次 console 重启的
`publish_all_tenant_status` 对账，唯一线索是 console 日志一条 WARNING。

## 修法

- `plaita/server/tenant_status.py`：`publish_tenant_status` 返回 `bool`
  （成功 True / Redis 失败 WARNING 后 False；无客户端/无租户 id 空操作
  True）——与 `publish_tenant_generation`「返回是否成功，由调用方决定语义」
  同形制。默认吞异常告警的既有行为不变，其余调用方（set_tenant_status、
  启动对账）不判返回值、语义零变化。
- `plaita-console/backend/services/tenants_svc.py`：`_publish_status` 透传
  bool（无客户端/ImportError 视为成功，与 `_publish_generation` 同口径）；
  create_tenant 把 active 发布（#255 墓碑覆盖点）从 commit 后挪进 commit 前
  事务窗口、判 False raise TenantError「创建失败：状态发布失败（拒绝留下
  运行面拦行窗）」→ 整个建租户事务回滚，不留半租户。世代→active 顺序
  （#284）保持。世代已发布而 commit 失败的极端残留（active 覆盖墓碑+无 DB
  行）被世代闸兜住：`tenant_write_allowed` 对无世代迟到写 fail-closed、带
  世代不匹配拒（与 #284 世代键残留同构，非本单新增面）。
- 选型对照：任务书两案中取「与 #284 同标 fail-closed」。补偿删除案
  （commit 后失败再删行）自身可失败 → 半租户残留风险更高，弃。

## 负控（先红后绿，A2 r53 剧本复刻）

上游化 `plaita-console/backend/tests/console/test_a2r53_tenant_status_publish.py`
（3 用例，故障注入=只对 `plaita:tenant_status` 的 hset 抛 ConnectionError、
世代 meta 键健康——毫秒级窗口故障模型）：

1. `test_status_publish_failure_aborts_create_no_half_tenant`（红格翻转）：
   注入故障重建 → 修前 create 200+墓碑留存+`tenant_is_active=False`；修后
   具名 TenantError + DB 无行（回滚）+ 短视图不被误写 + 健康 redis 重试同名
   create 即成功且运行面同态（失败不烧掉重建资格）。
2. `test_healthy_recreate_overrides_tombstone`（对照绿守恒）：hset 健康 →
   墓碑覆盖 active、`tenant_is_active=True`（#255 语义不变）。
3. `test_generation_publish_failure_still_aborts_before_status`（佐证格守恒）：
   #284 fail-closed 不变——世代失败仍 TenantError 回滚，且 spy 证明 active
   状态发布未执行。

stash 服务文件跑基线：**1 failed / 2 passed（rc=1）**——红格正是注入故障用例，
两对照格修前即绿；pop 后 **3 passed（rc=0）**。

## 守恒与门禁（rc 直录，无管道）

| 门 | 结果 |
|---|---|
| 负控 pre（stash 红） | `red-rc=1`（1 failed / 2 passed） |
| 负控 post（绿） | `green-rc=0`（3 passed） |
| console 全量 | `console-rc=0`（1479 passed, 14 skipped；含 #284 顺序守恒族） |
| unit | `unit-rc=0`（8881 passed, 13 skipped, 3501 subtests） |
| integration | `integ-rc=0`（213 passed, 1 skipped） |
| e2e | `e2e-rc=0`（74 passed, 3 skipped） |
| ruff（3 改动文件） | `ruff-rc=0` |
| mkdocs --strict | `mkdocs-rc=0` |

## 产物

- `data/r88-gate-console.txt` / `r88-gate-unit.txt` / `r88-gate-integ.txt` /
  `r88-gate-e2e.txt` / `r88-gate-ruff.txt` / `r88-gate-mkdocs.txt` /
  `r88-pre-red.txt` / `r88-post-green.txt`
