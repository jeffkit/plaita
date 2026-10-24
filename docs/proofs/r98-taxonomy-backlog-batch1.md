# r98：#332 log-site-taxonomy 重算门背账裁定（全量 154 条一轮清零）

- 分支：`fix/taxonomy-backlog-b1`（B2 第九十八轮）
- 单：plaita#332（P2，判定表自上次全绿后漂移、重算门长期中止）
- 结论：**未判定背账 154 → 0，重算门 rc=0（判定表与代码逐字节一致，1143 行）**；跳过 0 条（154 条全部为 `plaita/` 包内引擎/console 站点，无第三方面）；新增判据族 11（B_RULES 8 + A_RULES 3）+ OVERRIDES 30 条，A/B/C/D 四类语义不变。

## 1. 背账的再枚举（不盲信 r88b 清单）

r88b 清单（`docs/proofs/data/r88b-taxonomy-backlog-154.txt`）枚举于 r88b 时点；此后
r89–r91 及 #326/#327 等已合入，站点行号与成员均已漂移（当前中止点
`plaita/core/ownership.py:99 :: sub_key`，非 r88b 的 `runner.py:711`）。

探针（收集全部再退出，#332 建议 2 的临时形制，不入库）对当前树全量重枚举：
**仍是 154 条**（巧合），但成员已更新——新增 `tenant_dlq_quota.py` 12 条、
`ownership.py`/`start_dedup_sweeper.py` 等，部分 r88b 条目已被既有规则自然覆盖。
基线：`docs/proofs/data/r98-taxonomy-unmatched-after-c1.txt` 的前置即原始枚举
（与 c1 文件同探针，判前 154 条）。

## 2. 判定方法（逐条实读，禁看情况）

1. **逐条读代码上下文**：每条背账读调用点 ±10 行 + 所在函数；6 处追到构造面
   （`note`=report_progress 文案〔NodeContext 明文不给 code 节点，作者数据不可达〕、
   `where`=掩码器字面量站点标签、`violation`=`_resume_binding_violation` 引擎
   可读原因、`effective`=配置解析标量、`detail`=`_scrub_url_text(e)` 中转、
   `warning`=preflight 引造文案）。
2. **裸名走 OVERRIDES**（site 精确、rationale 具名；沿既有先例：`task`×4 全部
   沿 redis.py:2869「asyncio Task 句柄」同 rationale）；`detail`×2 判 **C**——
   语义等同 C 规则 2「清洗 helper 的产物」（赋值点 `_scrub_url_text(e)` 中转），
   保持构造面已掩约束，这是本轮裁定挑出的真「应掩保持」站点。
3. **族式语义走规则表，且只追加在 B_RULES/A_RULES 列表末尾**（新 tuple，不前插、
   不改既有 tuple）⇒ 既有行首匹配 tuple 不变，零扰动可静态论证 + 实证对账（§4）。
   新判据族：异常投影标量（`exc_name`/`etype`/`code_suffix`，#151 卫生注释祝福
   模式）/fencing 代数（`generation`）/计数族（`removed·trimmed·examined·pruned·
   errors·cleaned·skipped·dropped_*·unattributed·orphans·defers·rounds·
   full_current·full_cap·quota_current·desired·platform_n·quarantine_n·
   platform_total·quarantine_streams·batch_size·length`）/具名原因与枚举族
   （`_reason·violation·target_status·key_type·surface·language·policy·
   message_type·allowed·env`）/步骤旗标（`_ok`）/时间戳游标族（`age·due_at·
   default_timestamp·deleted_at·parked_at·cursor`）/上限（`cap`）/凭据文件路径
   与权限位（`key_file·mode`）/租户标识族（`_tenant` 后缀 + 裸 `tenant`）/
   dedup 映射执行 id（`mapped`）/存储索引键族（`type_key·index_key·key_prefix·
   state_key·quarantine_key·key_text·sub_key·bucket`）。

## 3. 先红后绿（rc 无管道直录）

| 步 | 状态 | 门 rc | 未判定数（探针） | 证据 |
|----|------|-------|-----------------|------|
| origin/main 基线 | 中止于 ownership.py:99 | 1 | 154 | #332 正文 + 枚举 |
| c1 `11a45d56` OVERRIDES +30 | 基线红（机制=all-or-nothing） | 1 | 124 | `r98-taxonomy-unmatched-after-c1.txt` |
| c2 `876bb525` B_RULES +8 | 基线红 | 1 | 41 | `r98-taxonomy-unmatched-after-c2.txt` |
| c3 `5ad0717a` A_RULES +3 | **未判定清零** | — | 0 | `r98-taxonomy-unmatched-after-c3.txt` |
| c4 本批 JSON 发射 | **门绿** | **0** | 0 | `python3 scripts/log_site_taxonomy.py` → `✓ 判定表与代码重算一致（1143 行）` |

中间 commit 门维持 #332 基线红：`classify_expr` 首个未匹配即 `SystemExit`、
JSON 只能全量发射，「每批独立回绿」机械上不可分批实现；以探针剩余数作每批
进度证据（154→124→41→0），各批 #151 卫生测试 14 passed + ruff 绿。

## 4. 零扰动对账（红点不扩的实证口径）

`docs/proofs/data/r98-taxonomy-zero-perturbation.txt`：对全仓插值表达式分别用
**origin/main 规则**与本轮规则判类——main 可判定的 **1750 条类别逐一相同
（MISMATCH=0）**，新增覆盖 167（154 背账 + 同表达式多方法站点的重复计数）。
即：**没有任何既有已判定行因本轮新判据翻类/翻红**。

## 5. 发射 delta 审计（旧表 ≠ 健康基线）

`docs/proofs/data/r98-taxonomy-emitted-delta.txt`：旧提交 JSON 821 行 vs 新发射
1143 行（+1114/−792/同键内容变化 11）。旧表发射基点远早于 r89——`--check` 在
`build_rows` 阶段即 SystemExit、**永远走不到缺行 diff 输出**（main() 的
`n_sites - c_sites` 段是死代码），手工补行者看不到缺行清单，漂移远大于中止点
所示。11 个同键变化行全部为**行号漂移碰撞**（同 file:line 同 method、底层调用
已不同，如 `event_filter.py:436` 旧=`_subscription_id` 新=`interval_seconds`），
新类均由既有规则判定，逐行核代码确认无误。

红点（C 类）320→540：+220 全部来自**表外站点首判**（异常文本经 `_scrub_*/
_mask_*` 构造面清洗的站点在漂移期大量新增而表中无行），非既有行翻红（§4）。
其中 `detail`×2 为本轮 OVERRIDES 如实判 C（构造面已掩保持）。

## 6. 验收条件对照（#332）

- ✅ 重算 rc=0：`python3 scripts/log_site_taxonomy.py` → rc=0（1143 行逐字节一致）
- ✅ 未匹配集合为空：探针 rc=0、未匹配=0
- ✅ 边界：未改任何 logger 调用点；未重开分类规则设计（四类语义不变）

## 7. 残面注记

- **中止点机制未改**：`classify_expr` 仍首错即停（#332 建议 2「收集全部再退出/
  CI 显式信号」属门脚本行为变更，超出本单裁定边界，留 owner 拍板）。本次裁定
  轮的枚举探针配方：`~/.issue-keeper/duty/b2-r98-scripts/enum_unmatched.py`
  （monkeypatch 收集全部未匹配再退出），可复用于后续轮。
- **手工补行时代应终结**：后续合并轮新增 logger 站点须跑 `--emit`/`--check`
  （门绿态下 `--check` 的缺行 diff 输出恢复有效）。
- r88b 清单文件保留作历史证据；其 154 条成员与当前 154 条不同（漂移实证）。

## 8. 门禁 rc 直录

- `python3 scripts/log_site_taxonomy.py` → rc=0（✓ 1143 行）
- `pytest tests/unit/test_issue151_log_hygiene.py` → 14 passed（每批复跑 rc=0）
- `ruff check scripts/log_site_taxonomy.py` → All checks passed
- mkdocs strict → rc=0（JSON/proofs 不入 nav，见提交）
