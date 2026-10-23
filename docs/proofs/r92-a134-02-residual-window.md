# r92 · plaita#314 —— A134-02 幸存者复核 commit 前残窗（孤儿版本行）修复

- **单**：plaita#314（P2，A1 r136 实弹，证伪 r88 树「复核兜住窗内新增版本」格）
- **基线**：origin/main `9072b9a`（开工后 origin 进 r98 两 commit `2c83e9f`/`e5456ca`，
  `git diff HEAD origin/main -- services/flow_store.py` 空＝与修面零交互，未 rebase）
- **分支**：`fix/a134-02-residual-window`
- **改动**：`plaita-console/backend/services/flow_store.py`（约 +90 行）、
  `tests/console/test_issue314_save_delete_residual_window.py`（新增 5 格）

## 缺陷

`delete_flow(expected_versions=)` 的幸存者复核只兜「复核时点已提交」：复核
SELECT 与事务 commit 之间存在残窗。FK 关闭引擎（裸 engine/存量 SQLite——
`init_engine` 之外的一切构造路径，含测试/脚本/探针）上，删除事务持写锁令窗内
`save_flow_definition` 的新版本 INSERT **排队到删除提交之后**才落地，而保存侧
一切更早的存在性检查（`ensure_flow`、`existing` SELECT）都发生在排队前，孤儿
版本行（flows 已删、flow_versions 残留）直接落库，「零删除或全删除」契约破。
后续同 id 重建时旧行复活进新生命周期（草稿态、旧定义），污染版本列表、
`list_version_tenants`/导出面跨生命周期串数据。

机制注记：pysqlite 方言 SELECT 不开事务（自动提交、锁即取即放），排队中的
INSERT 不持任何可被删除侧观察的锁；因此**删除侧 commit 前的任何二次闸都
看不见这个写**——修法必须在保存侧 commit 路径上。

## 修法（三件，裁量落地为「父行锚点 + 具名翻译 + 二次闸」组合）

1. **保存侧父行锚点写**（载荷件，`_save_flow_once` INSERT 分支 +
   `_assert_flow_row_alive`）：新版本 `flush()` 后、`commit` 前，同事务内执行
   零变异 `UPDATE flows SET author = author WHERE tenant_id=:tid AND
   flow_id=:fid`，rowcount≠1 → `VersionConflictError`（409）→ 事务整体回滚、
   版本行随之消失。机制：锚点跑在 INSERT 已取得的写锁内——FK 关闭引擎上若
   删除已提交，INSERT 落地时父行已亡，rowcount=0 即拒；若父行仍在，写锁持有
   到 commit，并发删除必排队到本事务之后、撞删除侧幸存者复核 409（零删除）。
   选自赋值而非前进 `updated_at`：flows.updated_at 是列表排序键，版本保存不应
   挪它（守恒）；rowcount 口径 SQLAlchemy 各方言一致按匹配行计（MySQL 走
   CLIENT_FOUND_ROWS）。任务书选项（b）「串行化删除与 import 的锁面」即由此
   以 DB 写锁实现，进程内/跨进程同一把锁；选项（a）的「删除侧二次闸」对
   SQLite 排队写无效（见机制注记），降为下条第 3 件的 MVCC 缝封堵。
2. **FK IntegrityError 具名翻译**（FK 强制引擎——console `init_engine` 每次
   checkout 拨 PRAGMA、PG/MySQL 原生）：这些引擎上孤儿本就被 FK 拦住，但
   INSERT 具名失败为 IntegrityError，直穿 `save_flow_definition` 重试环会被
   误报成 `ValueError("版本 … 已存在")`（误导 + allocate_version 白重试 3 次）。
   现捕获 → rollback → 跨会话复核父行：消失 → `VersionConflictError("…
   被并发删除…")`；仍在 → 原样上抛（真约束冲突语义不变）。
3. **删除侧二次闸**：幸存者复核自 `_delete_versions_pinned` 抽出为
   `_assert_no_version_survivors`，在 `_outbox_purge` 之后、flush/commit 之前
   重跑一次——封 MVCC 后端「首轮复核 → commit」缝可落进的已提交新版本
   （SQLite 单写锁下该缝由第 1 件兜住，此处近零成本一条索引 SELECT）。

两件收敛后任何时序终态均满足验收条件：**import 具名失败（409）或删除具名
409（零删除），不存在 flows 已删而版本行残留**。`publish_version` 无需改动
（其条件 UPDATE 谓词自带同型保护：删除先行 → rowcount=0 → 409；发布先行 →
删除撞 status 失配 → 409）；单版本 `delete_version` 按单边界不动；CAS 更新
既有草稿路径自带条件 UPDATE 门（行亡即 409），不加锚。

## 负控（先红后绿）

| 证据 | 修前 | 修后 |
|------|------|------|
| A1 探针换根复跑（`b2-r92-scripts/a136_probe3_survivor_toctou_wt2.py`，注入缝=`_outbox_purge` 停 1.2s） | `import -> saved v2`；孤儿版本 **True**；契约破坏 **True**（`data/r92-probe-prefix-red.txt`） | `import -> VersionConflictError: …被并发删除…`；孤儿 **False**；契约破坏 **False**（`data/r92-probe-postfix-green.txt`） |
| 新增回归 5 格（`test_issue314_save_delete_residual_window.py`） | **5 failed**（`data/r92-tests-prefix-red.txt`；stash 仅 flow_store.py，无混装面） | **5 passed** |

5 格覆盖：FK 引擎竞态终态（翻译路径）、裸引擎竞态终态（锚点路径）、FK
误报翻译直证（不再「已存在」）、锚点门 raise→回滚原子成对（父行不误滚、
版本行必滚）、锚点正控（通过且 author/updated_at 零变异）。时序确定性：
Event 在删除事务内 purge 返回后置位 + 0.1s 后发起 import，早于 1.2s 后的
commit——无竞态赌运气。

## 守恒

| 门 | 结果 |
|----|------|
| A134-02 16 格 + #303/#304/#307（含 followups/saturation）/#308 家族 + 新 5 格 | **80 passed**，rc=0（`data/r92-gate-family.txt`） |
| console 全量 | **1532 passed / 13 skipped / 0 failed**，rc=0，3:07（`data/r92-gate-console-full.txt`；r91 时点基线 2 红已被 r98 清除，本树零红） |
| ruff（flow_store.py + 新测试文件） | All checks passed，rc=0 |
| mkdocs | 见下（纯 docs 增量，构建过门） |

## 残面注记（不属本单）

- `create_flow` × `delete_flow` 同 PK 复生竞态（PG MVCC 下删除可吞掉刚重建
  的同名 flow）：#314 单边界明确只指 delete_flow×import 版本行孤儿面，未动；
  建议另格。
- 「窗内 import 发起但 `ensure_flow` 在删除提交**之后**才跑」→ 父行被重建、
  v2 落进新生命周期：save 路径既有的 ensure 语义（save 即创建），无孤儿，
  非本单缺陷面。
- alembic 基线与 `models/flow.py` 复合 FK 一致性未复核（#308 已管）。
