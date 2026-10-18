# B2 席位交付台账

> 一行摘要（第十八轮）：#230 触发器对话框切换流程清空已选版本——保存回落新流程默认版本，不再把旧流程 version 带给新流程。
> 一行摘要（第十五轮）：#235 ConsoleClient 会话模式 401 自愈——清死 token、重登一次、重放原请求；API-Key 401 不重登，常驻进程不再永久 401。

| 轮次 | Issue | 交付内容 | 验收 |
|---|---|---|---|
| 第十八轮 | [#230](https://github.com/jeffkit/plaita/issues/230) | `ScheduleDialog` 流程切换 `onChange` 补 `setVersion('')`，保存不再把旧流程 version 写给新流程；版本候选由 `['flow', flowId]` queryKey 驱动、随流程联动（无 placeholderData），清空后回落新流程 defaultVersion；r23 断言转绿，负控（删 `setVersion('')`）精确红 | 前端 `pnpm build`（tsc+vite）绿；`check-source-view` 36 / `check-dry-run-scope` 15 全绿；unit 快车道 **7968 passed / 15 skipped / 3434 subtests**；`landing_gate.py ci` 绿（main `e813036` CI success） |
| 第十五轮 | [#235](https://github.com/jeffkit/plaita/issues/235) | `_request` 会话模式 401：清 `_session_token` → `login()` 一次 → 重放本次请求一次（`relogged` 有界，重放仍 401 按原样抛）；`_session_auth_configured()` 钉住 API-Key 优先（key 模式 401 不重登）、`_skip_auth` 不递归；回归 4 条 + 负控 2F/2P | plaita-ai **233 passed / 14 skipped**；`ci-gate.sh` pytest/覆盖/layering/wheel/size 全绿；`landing_gate.py ci` 绿（main `f04de20` CI success） |
