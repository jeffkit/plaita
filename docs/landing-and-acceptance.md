# 落地与验收流程约定（2026-10-17 提案，待 owner 拍板）

> 背景：2026-10-15 18:31 ~ 10-16 11:42，origin/main 上 `ci.yml` 连续 10+ 次 failure，
> 期间各波次落地照常合并（#169 门禁盲窗）；政策性取舍以修复速度进入 main
> （如 #165 的 2048 字符入口上限）。本文给三类风险各配一条**机器可查证**的规则，
> 落地面收敛到一个脚本：`scripts/landing_gate.py`。
> **生效状态见文末**——拍板前为提案，脚本可试用不强制。

## 1. 落地熔断：main CI 红灯 = 停并

- 合并进 main 之前必跑：

  ```bash
  python scripts/landing_gate.py ci
  ```

- 判据：origin/main 上 `ci.yml` 最近一次**已完成** run 的结论。`failure` /
  `timed_out` / `startup_failure` → 退出码 1，禁止合并；全部 `in_progress` →
  退出码 2（无结论，等待或人工判断）。
- 豁免：安全类修复凭**双 ack**，用 `--override "<原因> / <ack 人>"`；
  override 内容原样打进 stdout 留痕，不静默放行。
- 首红优先处置：第一红立即定位（此刻最便宜）；后续红默认同因，除非证伪。
  连续多红不当作多个独立问题。
- 自动化接线（定检冒烟 + 当日 merge 自动二分 + 自动提单）另单跟踪，见
  仓内 issue「落地熔断自动化」。

## 2. policy-ack trailer：政策类改动显式确认

- **policy-touching 定义**（任一命中即算）：
  1. diff 新增/修改模块级**行为常量**（输入上限、阈值、默认开关，如
     `url_redaction._MAX_CLEAN_INPUT`）；
  2. 改变**公开错误语义**（新异常类型、错误消息口径、fail-closed/fail-open 取向）；
  3. 收窄**文档化契约**（此前文档承诺的行为变为拒绝/降级）。
- 约定：此类提交**必须**带 trailer，且标题或正文含 `[policy]` 标记：

  ```
  Policy-ack: jeffkit 2026-10-17
  ```

- 核验（落地前对拟并区间跑）：

  ```bash
  python scripts/landing_gate.py trailers <base>..<head>
  ```

  含 `[policy]` 标记而缺 `Policy-ack` trailer → 退出码 1。
- 原则：**机制与政策分离落地**——机制先落（有界但宽 + 触发响亮，WARNING 级
  而非 debug），真正的阈值/政策作为独立一行提交在 ack 后落，可单独回滚。
  政策触发点（cap-trip 等）必须可观测（计数或 WARNING 日志），不许 debug 级静默。

## 3. 证明产物入库

- 修复声称「变异全杀死 / 定向全绿」的，**证明文件必须随修复提交进仓**：
  `docs/proofs/<issue>-<slug>.md`（机读 JSON 旁证放 `docs/proofs/data/`）。
- gist 只是传输通道，不是台账；`git grep -i red-proof` 应能命中实证文件本身。
- 无证明不合并（对 mutation-backed 修复）；存量已引用未入库的证明
  （如 `test_issue165_masker_complexity_bound.py` docstring 引用的 RED-PROOF.md）
  按「证明产物入库」单回收。

## 4. 验收锚点 = main

- 验收判据（含「全量绿」类断言）一律对**合并后的 origin/main**执行，
  不对交付树——门禁盲窗期间「全量绿」均为交付树口径，main 集成态失真。
- landed-but-open（落地未关单）是流程正常态，但年龄有期望：**≤48h**。
  台账 = 落地队列 issue（#156）；超龄在台账标注原因（等裁决 / 等窗口 / 验收排队）。

## 5. 分级验收

| 级 | 范围 | 验收深度 |
| --- | --- | --- |
| A | 纯测试 / 文档 | 抽验 ~20%（看 diff 是否名实相符） |
| B | 机制修复 + 变异判死证明 | **验证明**：`replay.py --check-anchors` + kill 记录核对，不重新推导语义 |
| C | security / 并发与事件循环 / policy-touching / 契约变更 | 交付方之外**另一 agent 对抗审**（专问「这个修法会弄坏什么」）+ owner 对 policy 项 ack |

按族批验收：同族单（如 security config 族）一次裁决产一份族级清单，
族内逐单成本递减。

## 生效状态

**提案**（2026-10-17，起因与证据：#169 / #156 / `gh run list --workflow ci.yml`）。
owner 拍板后：把本节改为「生效」，并在落地清单（#156）、值守约定与
`scripts/ci-gate.sh` 侧接线；未接线前本文件只是约定草案，不构成本仓硬门槛。
