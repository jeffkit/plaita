# A2R57-01/02 证明：次生破口双修——import 直发漏重章 + 幽灵调度双门复活

- 单号：A2 台账第五十七轮 A2R57-01（P2 主）、A2R57-02（P2 主）；
  顺带裁量 A2R57-03（P3，与 02 同点一单双修）、A2R57-04（P3，前端读面）
- 基线：`origin/main` = `41f7eda`（71ef50d 之后）
- 分支：`fix/a2r57-secondorder`（worktree `plaita-wt2`，主 clone 只读）
- 边界：`api/flows.py`（import publish 块接 restamp+审计字段）、
  `api/schedules.py`（复启门 helper + enable/update 接门 + 标注回清）、
  `frontend/src/pages/Schedules.tsx`（编辑保存 version 透传 + SELECT 占位）；
  不改普通 publish 面、级联暂停语义、create/update 422 门本身。

## 缺陷（A2 r57 实弹 4 红）

1. **A2R57-01 import 直发漏重章**：`_restamp_schedules_for_publish` 唯一调用点
   在普通 publish（flows.py:1005）；import-version `publish=true` 是 runtime
   变更第二入口（晋升包 runtime 随包落版本行），publish 成功后只
   `sync_flow_to_engine` 零调度触点——version=None 调度行留旧章，cron 按旧章
   错池、与 manual 按版本行现值分裂（A2R50-03 原害从 import 门原样重开，
   且无 WARNING/审计痕迹）。
2. **A2R57-02 幽灵调度双门复活**：force 级联暂停行（`pinned_version_missing`
   标注在案）POST enable 与 PUT `{enabled:true}` 零门 200——幽灵 cron 每 tick
   重新产出 `flow_definition_missing` 垃圾执行（A2R52-01「幽灵触发面随删除
   同步止住」被一键绕过）。
3. **A2R57-03 标注僵尸化**：标注写后全仓零清理点，重钉合法版本+复启后行仍
   `pinned_version_missing: true`（健康行永久带病征，「降级可观测」反向失效）。
4. **A2R57-04 编辑保存静默钉版**：前端 `version: version || defaultVersion`
   兜底——跟随 latest 行打开编辑（SELECT 借位显示 defaultVersion）不动版本
   直接保存 → PUT 带 defaultVersion 静默重钉，「跟随 latest」语义丢失
   （75a66f7 修活保存链后可达）。

## 修法

- **修①**（flows.py import_version）：publish 块 `sync_flow_to_engine` 后同调
  `_restamp_schedules_for_publish(redis, tenant, flow_id, out.version,
  getattr(out, "runtime", None))`；`schedules_restamped` 计入 `flow.import`
  审计 payload（非 publish 恒 0）——与普通 publish 同字段留痕。
- **修②**（schedules.py）：新增 `_raise_if_pin_missing(schedule)`——consult
  行上标注 → 409 `schedule_pinned_version_missing`（含 `conflict` 键与
  「先重钉有效版本（PUT version）或解钉（version 置空）后再启用」指引）。
  接门三处：enable 本地/集群两分支（fetch 后、mutate 前）；update 共享校验段
  `if req.enabled and not req.version: _raise_if_pin_missing(schedule)`——
  **同请求重钉有效版本（version 非空，422 门已先验）放行**=修复动作单步完成；
  暂停/改名/解钉不入门（A2R52-01 豁免语义不变）。具名码按任务书
  `schedule_pinned_version_missing`（台账修法稿作 `schedule_pin_missing`，
  取任务书）。
- **修③**（schedules.py update `_apply`）：`req.version is not None` 分支
  `current.pop("pinned_version_missing", None)`——重钉与解钉都重新裁定了钉版
  面，标注随写回清；本地档行无该键，pop 为 no-op。enable 门已挡标注行，
  enable 成功路径无需 pop。
- **修④**（Schedules.tsx）：编辑态 payload `version` 原样透传（空串=显式
  解钉，后端 update 语义自然承接），不再 `|| defaultVersion` 兜底；新建仍兜底
  （创建面既定语义）。SELECT 编辑态对 version=null 行显示「跟随最新（不钉
  版本）」占位，不再借位显示 defaultVersion。

## 负控（先红后绿）

探针源=A2 r57 归档换根复跑（`~/.issue-keeper/duty/a2-r57-scripts/
test_a2r57_fix_surface_regression-r57.py` → `b2-r87-scripts/test_a2r57_b2_
worktree.py`，ARCHIVE 指向本 worktree，零真实 Redis/网络）：

| 格 | 修复前（本树） | 修复树 |
|---|---|---|
| 03a import 直发漏重章 | **RED（4F 之一）** | **GREEN** |
| 02c enable 零门复活幽灵 | **RED** | **GREEN**（409 拒，行不武装） |
| 02d PUT{enabled:true} 同洞 | **RED** | **GREEN**（409 拒，行不武装） |
| 02b 重钉不清标注 | **RED** | **GREEN** |
| 02a/02e/02f/03b 对照绿 | GREEN | **GREEN（守恒）** |

证据：`docs/proofs/data/r87-pre-red.txt`（4 failed / 4 passed，
与 A2 归档 probe-output-r57.txt 逐格一致）、`r87-post-green.txt`
（8 passed）。

## A2R52-01 验收格对表（任务书指定）

`a2-r52-scripts/test_a2r52_axis1_schedule_lifecycle.py` 换根复跑，基线
（stash 后）与修复树**失败集逐条相同**（3F/4P，`diff` 空）——3 失败均为
挖矿探针的「缺陷行为断言」在修后树按设计红（2e59e4d 落地起即然），非本轮
增量。01a 的正确行为断言（PUT 钉不存在版本 → 422）两树均过，门不误伤。
证据：`r87-r52-baseline.txt` / `r87-r52-fixed.txt`。

## 守恒（无管道直录 rc）

| 族 | 结果 | rc |
|---|---|---|
| console 全量 | 1480 passed / 13 skipped | 0 |
| schedules/flows 直触家族（pin_guard+300pool+cas+trigger_visibility+history+lifecycle+local_scheduler） | 52 passed | 0 |
| flows 家族（flows_api/flow_version/flow_source/304guard/a2r54race/a2r53） | 45 passed | 0 |
| enterprise/300gate/flow_store×2/fk/r52deploy | 96 passed / 2 skipped | 0 |
| ruff check（改动两 py 文件） | All checks passed | 0 |
| tsc --noEmit（前端） | 净 | 0 |
| eslint src/pages/Schedules.tsx | 净 | 0 |

证据文件：`docs/proofs/data/r87-gate-console.txt`。根仓 unit 全量未跑：
本轮零触引擎面（diff 仅 console api×2 + 前端 tsx），console 全量已覆盖
engine 导入面。根仓 unit 上次全绿记录见 r86 证明。

## 残面与移交注记

- 复启门是标注 consult 不是现查：标注丢失窗（如未来新写入路径漏标注）门不
  感知——现写入点唯一（flows.py 级联暂停），如后续新增写入点须同点落标注。
- gate 前读→CAS 写之间与 force 删版级联的理论竞窗（标注写入落在 fetch 后）
  先于本修即存在（enable/update 可与级联并发），本轮未加宽；后果限=带病征
  或标注被版本改写覆盖的观察面，武装门仍由 enable 二次 fetch 现读把守。
- A2R57-03 只覆盖集群档（标注只落集群行）；本地档无自由列，行为不变。
- 前端编辑态「跟随最新」选项 = 显式解钉（PUT version=""），对钉版缺失行是
  修复动作之一；对话框无 enabled 字段，复启门不会从对话框触发。
