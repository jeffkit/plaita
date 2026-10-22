# A134-02 证明：delete-vs-publish TOCTOU——删除守卫「先查后删」钉上守卫快照

- 单号：A1 台账第一百三十四轮 A134-02（P2）
- 基线：`origin/main` = `d34701e`（r87 合并点，开工与收工 fetch 均此值）
- 分支：`fix/a134-02-delete-toctou`（worktree `plaita-wt2`，主 clone 只读）
- 边界：`services/flow_store.py`（`delete_version`/`delete_flow` 谓词化 +
  `_delete_versions_pinned` 新助手）、`api/flows.py`（两端点传守卫快照 +
  `VersionConflictError`→409 `version_stale`；delete_version `prior=None`
  直接 404）、`docs-site/docs/distributed/ops-runbook.md`（守卫节补一条）；
  不改 `_enforce_version_delete_guard` 判据本身、#303 删除协议、force 级联
  暂停语义、前端（delete 路径无专门错误码处理，零触）。

## 缺陷（A1 r134 §2 读码判定，本轮负控实弹复刻）

`_enforce_version_delete_guard` 的 `version_published` 409 判据读**独立事务
快照**（delete_version 端点 `get_version` / delete_flow 端点
`list_versions`）；守卫内两次 Redis 扫描（在途执行 #307 + 钉版调度
A2R52-01）+ `_outbox_purge_target` 把「判据读→删行」窗口拉到百 ms 级。
store 侧 `delete_version`（`session.delete(row)` 按 PK）与 `delete_flow`
（批量 DELETE）**均无 status 谓词**。触发序：守卫读到 draft → 并发
`publish_version` 提交翻转（draft→published + outbox 行 + 引擎同步）→ 删除
事务无条件删行 + `_outbox_purge` 清掉刚写出的 outbox 行。后果：刚发布版本
被**静默删除**（无 force 语义参与）；跨 Redis 部署 outbox 被 purge ⇒ 定义
永不到达目标引擎（#304 FlowDefinitionMissingError 族）。A2R54-02 谓词药方
只套了 save/publish 面；r133 标「设计内」的窗内新 start 是 worker 兜底可接
的面，本条「窗内 publish 翻转」worker 接不住删除本身，是新面。

## 修法：DELETE 钉在守卫观察到的 `{version, status}` 快照上

- **store `delete_version(expected_status=)`**：条件 DELETE（谓词含
  `status = :expected`）+ rowcount≠1 → `VersionConflictError`（409 语义，
  带 `latest_updated_at` 与「当前 published；force 可越」指引）；`None`
  保留按 PK 无条件删除（存量直调方：test_flow_store / test_engine_outbox /
  #303 族零改动全绿）。
- **store `delete_flow(expected_versions=)`**：抽 `_delete_versions_pinned`
  ——逐行条件 DELETE（rowcount≠1 → 冲突）+ **幸存者复核**（删后仍有行 =
  守卫没见过的新版本，如窗内 import 直发 → 冲突）；两路失败事务整体回滚、
  零删除、outbox 不 purge。`None` 保留无条件级联。不变量：**被删的每一行
  守卫都见过**。
- **API 两端点**：守卫返回的 `versions`/`prior` 快照直传 store；
  `VersionConflictError` → 409 `version_stale`（`conflict` 键 +
  `latest_updated_at`，与 A2R54-02 具名面同形——前端 save/publish 的
  「冲突→重试」语义对 delete 同样成立）。delete_version `prior=None` 直接
  404 `version_not_found`：不存在的版本没有可钉的快照，也不再「先清目标侧
  再由 store 抛 404」半截清理。
- **force 同窗核**：force 只越 published **状态守卫**、不越快照漂移——
  force+观察 draft+窗内 publish 同样 409；重试 force 即过（重试时守卫看到
  published、快照一致、force 显式放行）。副作用对齐：级联暂停与审计
  `prior_status` 用的都是同一份观察快照，漂移窗内不再可能「审计记 draft
  实删 published」。

## 负控（先红后绿）

窗口注入 seam=`flows_api._outbox_purge_target`（守卫两扫之后、删行之前的
最后一步）返回后插并发 publish（store 翻转 + outbox + 引擎同步），A1 剧本
「判据过→publish 插窗→DELETE」确定性复刻；独立探针
`~/.issue-keeper/duty/b2-r88-scripts/a134_02_probe.py`（含 conftest 同款
`set_current_tenant(explicit)` 入口行，否则 #239 写闸拒 `redis.save_flow`、
engkey 列失真）：

| 场景 | 修复前（本树 stash） | 修复树 |
|---|---|---|
| delete_version / draft→publish 插窗 | **200 / 行删 / 键清** | **409 version_stale / published / 键在** |
| delete_version / outbox 启用 | **200 / 行删 / outbox 被 purge** | **409 / published / pending 行保** |
| delete_version / force+draft 观察 | **200（force 掩护静默删 published）** | **409；重试 force → 200（force 语义守恒）** |
| delete_flow / draft→publish 插窗 | **200 / 行+记录删** | **409 / 行+记录在** |
| delete_flow / 窗内新建+发布 1.0.1 | **200（守卫没见过的 published 被静默级联删）** | **409（幸存者复核）/ 两版本行都在** |

pytest 版同款 6 负控 + 7 守恒 + 4 store 层直证：
`tests/console/test_a134_02_delete_publish_toctou.py` 修前 **9 failed
（6 负控 `200 == 409` + 3 store 新 kwarg TypeError）/ 7 passed** → 修后
**16 passed**。证据：`docs/proofs/data/r88-a134-02-pre-fix.txt` /
`r88-a134-02-post-fix.txt` / `r88-a134-02-probe-pre-fix.txt` /
`r88-a134-02-probe-post-fix.txt`。

## 守恒（无管道直录 rc）

| 族 | 结果 | rc |
|---|---|---|
| console 全量 | 1496 passed / 13 skipped（r87 基线 1480P + 本轮新 16） | 0 |
| 既有删除族（#304/#307×2/#303/#308/A2R52×2/A2R54/A50/A48/engine_outbox） | 132 passed | 0 |
| unit 全量（根仓） | 8896 passed / 13 skipped / 3501 subtests | 0 |
| integration 全量（根仓） | 213 passed / 1 skipped | 0 |
| e2e 全量（根仓） | 74 passed / 3 skipped | 0 |
| ruff check（改动两 py + 新探针） | All checks passed | 0 |
| mkdocs build --strict | built | 0 |

证据：`docs/proofs/data/r88-a134-02-gate-{console,unit,integration,e2e,ruff,mkdocs}.txt`、
`r88-a134-02-family-conservation.txt`。#303 删除协议（tombstone/恢复/
自愈）、#307 strict 扫描、#308 级联、A2R52-01 调度门零改动全绿；409 四码
分工不变（`version_published`/`version_in_use`/`version_stale`/`version_conflict`）。

## 残面与移交注记

- 窗内新 start（r133 标「设计内」）不在本修范围：非终态执行引用守卫仍按
  守卫时快照判，窗内新 start 删除后由 worker 具名终态兜底——本轮不改动。
- 多后端 READ COMMITTED 下，store 内 SELECT 与条件 DELETE 之间仍有亚 ms 级
  窗（rowcount=0 兜底为具名冲突，面朝安全侧）；与 A134-03 同族（门验对象
  与钉扎谓词两快照），SQLite 默认部署不可达，未另加宽。
- API 层 `get_version`/`list_versions` 与守卫 Redis 扫描之间同样有窗：本修
  把「删除事务」钉到守卫**输出**快照上，判据窗口从「判据读→删行」收窄到
  「判据读→守卫返回」（两读间无写，SQLite 同事务快照一致；多后端下窗内
  publish 仍由条件 DELETE 拒）。
- store 层 `expected_*` 为 keyword-only 新参，存量位置调用零改动；前端
  delete 路径无 version_stale 专门处理（Flows.tsx 裸 await），409 message
  直出，行为面零触。
