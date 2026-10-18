# B2 席位交付台账

> 一行摘要（第十八轮）：#230 触发器对话框切换流程清空已选版本——保存回落新流程默认版本，不再把旧流程 version 带给新流程。
> 一行摘要（第十七轮）：#211 并发双起修复已随 merge `0736c161`（w40 席位）落地于 HEAD——本席位复核 7 条原子启动回归 + repro 全绿、负控 3 红可复现，确认无残留、未重复施工；重启孤儿对账按 issue 规格属另单（C6 另一半）。
> 一行摘要（第十五轮）：#235 ConsoleClient 会话模式 401 自愈——清死 token、重登一次、重放原请求；API-Key 401 不重登，常驻进程不再永久 401。

| 轮次 | Issue | 交付内容 | 验收 |
|---|---|---|---|
| 第十七轮 | [#211](https://github.com/jeffkit/plaita/issues/211) | **复核，无新增代码**：`start_service` 的「计数 → 预留 → 启动 → 回滚」已由 w40 席位实现（每 `service_type` 一把 `asyncio.Lock`；`starting` 预留计入额度且不污染 `instances`；失败释放预留）。HEAD `47476bd` 已含 merge `0736c161`（父 `8799f275`），issue 已被提报席 PASS/关闭。`instances` 持久化/重启对账是 issue 明示的「C6 另一半，另单」，未在本单混入 | 目标用例 **7 passed** + reset **7 passed**（HEAD `47476bd` 复跑）；repro **GREEN**（A 1/1、B 2/2、C 3/20、D 失败回滚后重试成功）；负控还原无锁实现 **3 failed / 4 passed**，还原后 **7 passed**；console 全套 **546 passed / 13 skipped**；unit 快车道 **7968 passed / 15 skipped / 3434 subtests**（slow 子集另跑 13 passed / 2 skipped）；`landing_gate.py ci` **绿**（main `e813036` success）；e2e quick **环境红**：本机 Docker 走 `127.0.0.1:7890` 代理拉基础镜像被拒（非代码） |
| 第十八轮 | [#230](https://github.com/jeffkit/plaita/issues/230) | `ScheduleDialog` 流程切换 `onChange` 补 `setVersion('')`，保存不再把旧流程 version 写给新流程；版本候选由 `['flow', flowId]` queryKey 驱动、随流程联动（无 placeholderData），清空后回落新流程 defaultVersion；r23 断言转绿，负控（删 `setVersion('')`）精确红 | 前端 `pnpm build`（tsc+vite）绿；`check-source-view` 36 / `check-dry-run-scope` 15 全绿；unit 快车道 **7968 passed / 15 skipped / 3434 subtests**；`landing_gate.py ci` 绿（main `e813036` CI success） |
| 第十五轮 | [#235](https://github.com/jeffkit/plaita/issues/235) | `_request` 会话模式 401：清 `_session_token` → `login()` 一次 → 重放本次请求一次（`relogged` 有界，重放仍 401 按原样抛）；`_session_auth_configured()` 钉住 API-Key 优先（key 模式 401 不重登）、`_skip_auth` 不递归；回归 4 条 + 负控 2F/2P | plaita-ai **233 passed / 14 skipped**；`ci-gate.sh` pytest/覆盖/layering/wheel/size 全绿；`landing_gate.py ci` 绿（main `f04de20` CI success） |
