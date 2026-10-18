# B2 席位交付台账

> 一行摘要（第十五轮）：#235 ConsoleClient 会话模式 401 自愈——清死 token、重登一次、重放原请求；API-Key 401 不重登，常驻进程不再永久 401。

| 轮次 | Issue | 交付内容 | 验收 |
|---|---|---|---|
| 第十五轮 | [#235](https://github.com/jeffkit/plaita/issues/235) | `_request` 会话模式 401：清 `_session_token` → `login()` 一次 → 重放本次请求一次（`relogged` 有界，重放仍 401 按原样抛）；`_session_auth_configured()` 钉住 API-Key 优先（key 模式 401 不重登）、`_skip_auth` 不递归；回归 4 条 + 负控 2F/2P | plaita-ai **233 passed / 14 skipped**；`ci-gate.sh` pytest/覆盖/layering/wheel/size 全绿；`landing_gate.py ci` 绿（main `f04de20` CI success） |
