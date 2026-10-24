# r94 / B1 席：#336 失败重试幂等键漂移——error 终态咽喉转存 token 随失败证据落盘 × retry_wakeup 复用（owner 裁决案 b）

- 基线：`origin/main` `3271ba89`（r92 CI 终态补记后裔，已核祖先）
- 交付：主 clone 直落 main（主 clone 席位口径；逐路径 add，A1 台账未跟踪文件不暂存）
- 规格：#336 正文（A1 r139 实弹探针，修复三案已列单）+ owner 裁决 =
  **案 b：token 存 error 扩展键随失败证据落盘**（A1 倾向案）。落地面 =
  #322 所有权 v1（`d61a038a`）× #320 状态模型（`a3a1409f`/`45f13fe7`）。

## 缺陷与裁决

#322 的幂等键稳定性锚是「重投读同一行 ⇒ 同 token」，但 error 终态化在
`_persist_state_or_raise` 咽喉无条件清 `next_step`（#320 写序不变量 1 的
机械化点），retry_wakeup 铸造点 `declared_step_id` 判空恒真 ⇒ 必铸新
token——error→retry 主路径（G1 预算耗尽终态化后的人工/自动 retry、
`deterministic_failure_exhausted` 后的 retry）两次执行携带不同
`Idempotency-Key`，外部去重失效；失效点恰在上一次尝试已外发收据之后
（如 POST 成功、响应解析失败）。进程内 redelivery（未终态化）同键保证
不受影响。

owner 三案裁决对表（留档）：

| 案 | 内容 | 裁决 | 理由 |
|---|---|---|---|
| a | error 行豁免 next_step 清除 | **不采** | 违 #320 写序不变量 1 字面（终态行不得声明未完成的工作），对账器/#321 重建读面全要给 error 开口子 |
| b | 终态化把 token 存 error 扩展键，retry_wakeup 复用 | **采纳（owner 拍板）** | 不动 #320 不变量；token 随失败证据同一次 CAS 落盘，语义=「失败证据的一部分」 |
| c | retry_wakeup 按 noderetry 计数键世代推导 token | **不采** | 与 `new_step_id` 随机防伪造设计冲突（`core/ownership.py`：稳定性锚在「读同一行」非 token 形状，**不可推导**是特性） |

## 修法

- **转存面**（`plaita/core/ownership.py` 新增纯函数）：
  `FAILED_STEP_EVIDENCE_KEY = "failed_step"`；
  `preserve_failed_step(next_step)` 校验并摘出 `{node_id, step_id,
  parent_step_id}`（形态与 #320 `next_step` 字段设计一致；声明缺失/畸形
  返回 None，不编造）；`failed_step_evidence(error)` 读面（缺失/畸形一律
  None，与 `declared_step_id` 同款缺省纪律）。
- **咽喉点**（`flow_worker._persist_state_or_raise` 终态分支）：error 行在
  `next_step = None` **前**转存进 `state.error[FAILED_STEP_EVIDENCE_KEY]`
  （同一次落盘=CAS 原子；error 为 None 时兜底造 dict——token 证据不因
  失败详情载体缺失而丢）；completed/cancelled 不转存（retry 只唤醒
  error 行，终态收据残留无消费方——`last_report_at` 观测残留不同律
  先例）；`next_step` 照清（#320 不变量不开口子）；再终态化幂等（声明
  已清 ⇒ 无转存 ⇒ 既有证据不覆写）；与既有 error 扩展键
  （`failed_node`/`node_retries`/`error_type` 等）共存只加键。
- **复用面**（`retry_wakeup` 铸造点）：error 清空前先读证据——重跑节点
  （`$LAST_NODE`）与转存声明**同节点** ⇒ 复用同 token（重试 = 同逻辑
  步骤）；无证据/节点漂移 ⇒ 照铸新（首投/其它路径行为不变；漂移格 =
  陈旧声明从未消费过，复用反而错锚）。行上声明仍存活时（
  `declared_step_id` 真值）消费优先级不变（#322「读同一行」锚优先）。
- **文档**：`docs/WORKER_PROTOCOL.md` §2.1 铸造 bullet 扩 #336 语义
  （转存键形 + 复用判据 + 案 a/案 c 不采理由留档）。

## 覆盖互补（与 #337 的组合，B2 在修）

- #337（SNE 终态不重试）：封的是「确定性失败被双层重试预算烧穿」——
  节点内 retryTimes + worker 重投（at-least-once 消息层），修后 SNE 类
  直达终态不烧。**不经 retry_wakeup**（重投≠error 终态后唤醒）。
- #336（本单）：封的是「合法 retry（G1/人工）唤醒 error 行时幂等键
  漂移」。两单覆盖互补不冲突：#337 落地后 SNE 类确定性失败 = 自动重投
  面已封（至多 1 次副作用）+ 人工 retry 面键稳定（本单咽喉转存对 SNE
  终态化行同样生效——`_error_fields` 身份修正〔#337 建议 3〕与本单
  扩展键互不相扰，共存格已钉）。

## 负控（先红后绿，rc 直录）

新 `tests/unit/test_issue336_retry_token_stability.py` 13 格：

- **基线红 6F/7P**（`docs/proofs/data/r94-tests336-red.txt`，rc=1）：
  3 格咽喉未转存（证据键缺失）+ 3 格漂移实锤——含 A1 r139 探针真组件
  复刻（真 `FlowExecution`/`declared_step_id`/`new_step_id`/
  `connector_idempotency_key`/真咽喉/真 resume_flow：首跑 T1 失败 →
  retry 绑 T2，连接器键 `exec-issue336:4601…:a ≠ exec-issue336:83fd…:a`
  逐字节实锤）。
- **修后 13/13 绿**（`docs/proofs/data/r94-tests336-green.txt`，rc=0）：
  - A 咽喉转存 8 格：转存同一次落盘/无声明不造键/畸形不转存/error=None
    兜底造 dict/completed·cancelled 不转存×2/再终态化幂等/既有扩展键共存。
  - B retry 复用 5 格：漂移探针全链（修后绑回 T1）/连续两次 retry 同
    token（G1 预算=2 内，确定性失败 `ValueError` 经真步循环终态化、
    `NodeFailureTerminalizedError` 面）/无证据照铸新（守恒）/节点漂移
    不复用（防御）/连接器键跨失败-重试逐字节相同（真键模型端到端）。
- 测试形制注记：确定性失败 staging 走 `ValueError`（`RuntimeError` 判
  可重试会走重投不终态化）；终态化后 `resume_flow` 以
  `NodeFailureTerminalizedError` 上抛（设计行为），绑定读点在
  `run_distributed` 前不受影响。

## 守恒

- #320/#322 家族直系：**82P rc=0**（#320 43 + #322 39，与 r90 台账数
  吻合；`docs/proofs/data/r94-family-320-322.txt`）——咽喉 E 组
  （completed 三向格）/retry_wakeup C 组（无证据铸造格）原样绿。
- #151 卫生门定向 14P rc=0（改动面必跑；`r94-hygiene-151.txt`）。
- unit 全量独跑/console 后端门/integ+e2e 见台账门禁行。

## 残面与边界声明

- 覆盖面=「行上带 next_step 声明的 error 终态」；挂起清声明后的 error
  （next_step 已 None）无证据可转存，retry 照铸新 = 既有 tokenless 窗
  语义（#322 v1 范围：挂起恢复首步 tokenless，重建保护属 #321 义务），
  非本单放大。
- `state.error` 载体兜底造 dict 仅在「error 行 + 有声明 + error=None」
  组合（现网 worker 各 error 终态路径均落 dict，该组合为防御性）。
- 证据键随行生命周期：retry 唤醒清 `error=None` 时一并消费（不复存
  活）；未 retry 的 error 行证据持久保留（观测面可读「失败尝试的
  token」）。
