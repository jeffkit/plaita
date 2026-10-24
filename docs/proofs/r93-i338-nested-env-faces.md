# r93 / E 线 wave 2 盲区两连（B1 席）：#338 嵌套挂起帧面三支柱同源扩展 + #339 local 手动面闭合（随 #335 收敛）

- 基线：`origin/main` 开工时 `44043122`（已核祖先，r103 后裔）
- 分支：主 clone 直落 main（主控指定主 clone 席；逐路径 add，邻席 #335 WIP 不暂存）
- 规格：#338/#339 正文（A1 r140 实弹，探针 `/tmp/a1r140/`）+ #326 owner 裁决
  ①④ 沿用；#311 NS-M3 帧 schema（`_carrier_frame_dicts`）为遍历面事实源。

## 单① #338：strip/重注/purge 三支柱从顶层键扩到嵌套挂起帧

新共享模块 `plaita/core/env_checkpoint.py`（三处同源，P1 命名 `strip_env_snapshot`
/ `reinject_env_snapshots`）：

- **strip（持久化面）**：根面 + `$SUSPEND_FRAMES[*].context` 及帧内 `$PARENT`
  链全部拔除 `$ENV`（值与键名，owner 裁决④）；**copy-on-write**——未命中的
  面原样共享引用，引擎在飞对象不动（r91「重绑副本」语义的嵌套延伸）。
  接线点不变：`_persist_state_or_raise` 咽喉（`state.context =
  strip_env_snapshot(state.context)`）。
- **重注（恢复面）**：根面 + 每帧按**该帧生效 allowlist**——沿根 flow 的
  child/reference 节点（`node.child_flow`）逐层下行解析，内层 flow 自声明
  `expose_env` 优先、否则继承父级生效值（`setup_flow` 同判据；与引擎嵌套恢复
  `_handle_nested_resume` 同一条下行路径）；帧内 `$PARENT` 链第 j 层按第
  i−j 层 flow 的 allowlist 重注。生效 allowlist 为空的面**不造空 `$ENV`**
  （键名不凭空显形）；帧路径漂移降级重注 + 具名 WARNING（恢复路径会具名拒，
  不静默）。根 flow 无 allowlist 但内层 flow 自声明时帧面照注（原实现的
  `if not allowlist: return` 短路已拆除）。具名 WARNING
  `event=tenant_env.reinject` 保留并扩帧面计数；值漂移 owner 裁决①声明不变。
- **purge（存量清理面）**：`scripts/purge_env_from_checkpoints.py` 的
  `kept` 短路改嵌套感知——解析后递归同走 `strip_env_snapshot`（identity
  判定：未命中原对象返回即 kept）；字节预滤不变（帧内 `$ENV` 同样带
  `"$ENV"` 字节）；CAS/TTL 语义不变。import 形制对齐对账家族
  （carrier_audit 同款 sys.path bootstrap）。
- **MIGRATION.md** 凭据条目精确化（嵌套帧面含入 + purge 嵌套清下面）——
  issue 影响面「过度声明」随修转真。

## 单② #339：console local 手动面 strip/重注

- **时序实况**：本席开工按任务书先给旧环补丁（负控 5 格在旧环形上先红，
  实证 [E]/[F] 两面）；**进行中收敛会话（#335 C1 阶段 3）当轮删除旧环**
  （`start/resume_local_execution`/`_run_flow` 已从工作树移除，手动
  start/resume 切 `engine_backend` 同一 `FlowWorker` 状态机）——issue 原文
  「根治=C1 阶段 3 删旧环后自然收敛」当场兑现，窗口期补丁对象消失。
- **收敛后形态**：strip 由 worker 咽喉机械化继承（storage 适配层
  `LocalExecutionStorage` 收到的 context 已在咽喉 strip，#338 扩展含嵌套帧）；
  重注由 `resume_flow` 驱动链的 `_reinject_env_for_resume` 机械继承（含
  #338 帧面扩展）。本席交付转为**新路径验收格**：
  `plaita-console/backend/tests/console/test_issue339_local_ring_env.py`
  （5 格：挂起/完成/取消三落库点无 `$ENV` + resume 重注 saved_context 断言 +
  真引擎全链挂起→strip 行→手动 event resume→completed 且终点求值=当前进程
  env），由收敛会话迁移注入点（`fw.FlowExecution` patch + `engine_backend`
  入口）后与本席负控逐格同构，5/5 绿。该文件随 #335 收敛落地（其依赖
  `engine_backend.resume_execution` 为 #335 新面）。

## 负控（先红后绿）

- **#338 六格**（`tests/unit/test_issue326_env_checkpoint_reinjection.py`
  追加，家族文件就地扩展）：嵌套 strip（帧面+父链拔除/在飞不动/净行零字节
  变化）×2、嵌套重注（内层自声明优先+根未声明不造空面 / 继承根 allowlist+
  旧快照覆盖）×2、purge 嵌套（干跑不写+实清三面+幂等 kept）×1、**真引擎
  四面格**×1（真引擎 NS-M3 嵌套挂起→真咽喉落盘→行内三面无 `$ENV`（值与
  键名）→purge 对净行 no-op→轮换 env 重注达三面→嵌套恢复内层求值=新值）。
  红相 6 failed / 9 passed（9 绿恰为 wave 1/2 既有面守恒）→ 修后 15/15 绿。
- **A1 探针复跑**（`docs/proofs/data/r93-probe-nested-env-postfix.txt`）：
  [A] 顶层 strip ✓；[C] purge 对咽喉净行 `kept` + 帧内无残留 ✓（嵌套 legacy
  行的 purge 格由 TestPurgeScriptNested 手搭覆盖）；[D] 轮换 env 后嵌套恢复
  内层求值 = 新值 ✓；[B] 探针读引擎**在飞** context（重绑副本契约下按设计
  保留 `$ENV`——在飞求值源），落盘行三面已净由单测四面格断言（真引擎
  `_persist_state_or_raise` 落盘行 grep 无 `"$ENV"`）。
- **#339 五格红相**（旧环形，#335 收敛前实跑捕获）：5 failed——挂起/完成/
  取消行 `$ENV` 明文进 SQLite + resume 无重注；崩溃形状由 A1 探针
  `probe_local_ring_env.py` 引擎级复现（`FlowErrorException: KeyError:
  "'$ENV' not found in expression context"`，与本轮留证
  `/tmp/b1r93-probe-local-ring-prework.log` 同形）。
- **守恒**：wave 1/2 家族 9 格零改动全绿；integ+e2e 287P/4S rc=0。

## 门禁（无管道直录）

- unit 独跑（`pytest tests/unit -q`）：**9147P/13S rc=0**（首跑 9146P/1F——
  **#151 卫生门抓新模块裸传异常对象**（`env_checkpoint.py` 帧路径下探
  WARNING 直传 `exc`，r90 第三例同族课），改 `_scrub_exception_text` 后
  复跑全绿；9147 = 开工基线 9141 + 本轮 6 新格）。
- console 后端门（engine/worker 改动必跑）：**1637P/14S rc=0**（独跑复门；
  首跑 1636P/1F 为 issue37 usage 格负载敏感假红——与 mkdocs 并跑窗口叠
  加，定向复跑 0.95s 绿 + 独跑全绿，r88 判据；数字含共享树上 #335 未提交
  面的 1630P 基线+收敛新增）。
- integration + e2e：287P/4S rc=0。
- ruff：改动面 0 增量（flow_worker BLE001 11 条=HEAD 既有同数；新文件/
  脚本/测试零报错）。
- mkdocs（docs-site --strict，`PYTHON=/opt/homebrew/bin/python3` 绕 pyenv
  shim）：rc=0。

## 证据

- `docs/proofs/data/r93-probe-nested-env-postfix.txt`（A1 探针①修复后全输出）
- 台账：`duo-b-ledger.md` 第九十三轮
- #339 收敛说明：修复语义 = #335 删旧环（收敛会话）+ #338 咽喉/重注扩展
  （本轮）的机械继承；验收格 5/5 绿随 #335 落地。
