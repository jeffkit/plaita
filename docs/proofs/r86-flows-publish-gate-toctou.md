# A2R54-01/02 证明：flows 版本发布面双修——门快照 TOCTOU + CAS 状态盲写

- 单号：A2 台账 A2R54-02（P2，核销表头名主害）、A2R54-01（P2，一单双修）；
  涉 plaita#300（能力门）残面收口
- 基线：`origin/main` = `c13bde3`（B3 r85 已合）
- 分支：`fix/a2r54-publish-gate`（独立 worktree `/private/tmp/plaita-b3-r86`，
  主 clone 归 B1 [#305 Phase 2]，零交集）
- 边界：只动 `flow_store.py`（publish_version / _cas_update_draft）与
  `api/flows.py`（publish / import 两处接线）；不改发布 API 形状、#300 门
  判据本身、outbox/引擎同步语义。

## 缺陷（A2 r54 提报，r56 核销双红实弹）

1. **A2R54-01 门快照 TOCTOU**：`publish_flow` 门判据取 `get_version` 快照 →
   `gate_flow_nodes`（数据源=Redis 注册清单，窗口含网络往返）→
   `publish_version` 新会话重 SELECT 无门、无定义钉扎——竞窗内并发 CAS 草稿
   保存（PUT 本就不设门）落进即把**未过门定义**送进发布+引擎同步+outbox
   三件。import 直发链同构。
2. **A2R54-02 CAS 状态盲写**：`_save_flow_once` 既有行分支「SELECT 状态检查
   （draft 可写）→ CAS UPDATE」两语句之间夹进 publish 提交：CAS 谓词
   `WHERE updated_at=:base` 不含 status，且 `publish_version` 状态翻转不前进
   updated_at ⇒ 已发布行被打回 draft + 定义覆盖 + published_at 残留
   （契约端转 code 3、引擎留旧定义库/引擎分叉、自愈重发遇夹塞节点 422 卡死）。

## 修法

### 修① A2R54-02（flow_store.py）

- `publish_version` 状态翻转由 ORM 属性赋值改**单条条件 UPDATE**：
  `SET status='published', published_at=:ts, updated_at=:ts WHERE … AND
  status='draft' [AND updated_at=:snap]`——发布同时**前进 updated_at**（主害
  单点收口：并发 CAS 保存基准自动失配 → 409）；`:snap`=翻转前行快照（钉扎）。
- `_cas_update_draft` 谓词补 `AND status='draft'`（含 legacy 降级档——该谓词
  不依赖 updated_at 列，旧库同样挡住打回）。
- legacy（列缺失）翻转 UPDATE 无 updated_at 推进，但保留 `status='draft'`
  谓词；翻转失败（rowcount=0）抛 `VersionConflictError`（409 语义，
  `latest_updated_at` 供前端加载最新）。

### 修② A2R54-01（flow_store.py + api/flows.py）

- `publish_version` 新增 `gate_fn: Callable[[FlowVersion], None]`：发布事务内
  对**重 SELECT 出的 fresh 行**复核能力门（校验对象=落盘对象，同事务同快照）；
  门抛出异常原样上抛、事务回滚零写入。`None` 保留旧调用面（测试/兼容）。
- 关键时序：**快照先于门读**——门校验期间（含 Redis 往返）落进的并发保存使
  钉扎谓词失配；若先门后快照，谓词取到竞后值就钉不住了。
- API `publish_flow` 与 `import_version`（import 直发链）两处注入
  `gate_fn=lambda fresh: _gate_runtime_capability(redis, fresh, where=…)`，
  复用既有 422 结构化映射；两处均新增 `VersionConflictError → 409
  version_stale` 映射（与 PUT 同款结构化错误体）。pre-gate 保留（快速失败、
  零写入语义不变）。

## 负控（先红后绿）

探针源=A2 r56 归档 `probe_a2r54_publish_save_race-r56.py` 换根复跑
（`~/.issue-keeper/duty/a2-r56-scripts/`，零真实 Redis/网络）：

| 格 | 基线 c13bde3 | 修复树 |
|---|---|---|
| A2R54-01 门快照TOCTOU（未过门 D2 被发布） | **RED-HIT** | RED-HIT* |
| 对照绿1 无竞窗 | GREEN | GREEN |
| A2R54-02 CAS 盲写（已发布行打回 draft） | **RED-HIT** | **RED-MISS（修复）** |
| 对照绿2 published 拦截 | GREEN | GREEN |
| 佐证 legacy 降级打回 | 复现 | **RED-MISS（修复）** |

\* 修复树上原探针格 1 维持红属**预期**：该格驱动 `publish_version`
**无 gate_fn**（无门调用面，门由 API 层注入是修②形态）；有门面由修后探针
G1/G5 证全拒。证据：`r86-probe-prefix-output.txt`（2红/3绿）、
`r86-probe-prefix-on-fixed-tree.txt`（红格2+佐证格翻绿）。

修后负控（`r86-probe-a2r54-postfix.py`，8 格全绿
`r86-probe-postfix-output.txt`）：G1 re-gate 拒未过门 fresh 行 / G2 钉扎
（竞窗落 gate_fn 回调内 → 409）/ G3 三明治（publish 前进 updated_at + CAS
失配）/ G4 legacy 档 status 谓词挡打回 / G5 import 直发链同构 / 对照绿A-C
（正常发布、幂等重发布、published 拦截——语义守恒）。

上游化测试 `plaita-console/backend/tests/console/
test_a2r54_publish_race_toctou.py` **10P**（真门=fakeredis 注册清单 +
`gate_flow_nodes`，非桩）：基线 **8 failed / 2 passed**（红，
`r86-tests-baseline-red.txt`；2 passed=守恒格）→ 修复树 **10 passed**。
含 API 层两格：pre-gate 与重 SELECT 之间落竞窗 → 422 unsupported_node_types
+ 草稿保持；re-gate 期间落竞窗 → 409 version_stale（latest_updated_at）+
草稿保持。

## 守恒（全部无管道直录 rc）

| 族 | 结果 | rc |
|---|---|---|
| console 全量 | 1399 passed / 14 skipped | 0 |
| 根仓 unit 全量 | 8936 passed / 17 skipped | 0 |
| integration | 214 passed / 2 skipped | 0 |
| e2e | 12 passed / 2 skipped | 0 |
| ruff check（改动两文件） | All checks passed | 0 |
| mkdocs strict（等价 CI 脚本，3.10 venv） | built in 6.44s | 0 |

证据文件：`docs/proofs/data/r86-{console,unit-root,integ,e2e,mkdocs}.txt`。

备注两条：①基线两文件 `ruff format` 本就不净（既存漂移），新增代码除
`version_stale` 块（与既有 PUT 同款仓内风格）外均不引入新 format 差异；
②首轮 console/unit 跑法误带管道（`pytest|tail` 吃真实退出码），已按仓规
无管道重跑并以直录 rc 为准——表中数字全部来自无管道复跑。

## 残面与移交注记

- `gate_fn=None` 调用面（store 直调）不发门——生产两处调用点均已注入；
  后续新增发布调用点必须带 gate_fn（docstring 已写明）。
- A2R54-03（semver 无列宽上界，P3）不在本轮范围，台账维持原状态。
- 修复树上原探针格 1 的红（无门调用面）与「#300 门只在发布时点」的 PUT 面
  组合语义不变：PUT 保存不设门是 #300 原设计，本修不扩门到保存路径。
