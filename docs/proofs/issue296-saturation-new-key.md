# #296 证明：MAX_TRACKED 饱和仅作用于新键

- 单号：plaita#296（P1；r65 饱和实现与规格不符）
- 基线：`origin/main` = `cc7abfe`
- 边界：只收窄 `LoginThrottle` 饱和作用域；不改 per-key/IP/account 桶、
  break-glass、Redis TIME。

## 根因

规格（ops-runbook / r67 台账）：表满且无过期条目时，**新键**进入 60s 具名
饱和；既有键不受影响。实现把全局 `_saturated_until` 放进 `retry_after` 的
所有键等待值，任一新键触发饱和后所有既有未锁键也 429；攻击者可每 60s 用
一个新键续期，形成全局登录面阻断（含 admin UI 登录）。

## 修法

- 饱和从全局标量改为**按新键记录**：`_saturated_keys: {(canonical_ip,user): until}`。
- 仅当 `_entries` 满且该键不在表中时设置该键的 60s 饱和；同一新键后续请求
  返回剩余秒数。
- `retry_after` 只读取**当前请求键**的饱和时间；既有键/活动锁逻辑不变。
- `_prune` 清理过期饱和键；`reset`/`reset_all` 一并清除。
- ops-runbook 明确「仅新键；既有键 retry_after 恒 0；不全局施加」。

## 测试

- `test_296_saturation_binds_only_the_new_key`：既有 admin retry 0、新键 60。
- `test_296_existing_key_not_blocked_over_http`：新键触发饱和后 admin 正确
  登录 200 且无 Retry-After；新键自身 429。
- `test_296_repeated_new_keys_cannot_block_existing_key`：连续 5 个新键续期
  饱和，既有键 retry 恒 0。
- 既有 `test_b112_02_max_tracked_saturation_not_silent` 保持（新键 60）。

## 负控（pre/post）

pre `/tmp/plaita-r114-tree` / post 主树，A1 probe：
- direct 既有 admin retry 60→0；HTTP admin 正确 429→200、Retry-After 60→null；
- 新键自身仍 60→60 具名饱和；重复新键不阻断既有键。

完整输出：`docs/proofs/data/r66-saturation-negative-control.txt`。

## 验收

- 全量 unit（`-m "not slow"`）：**8795P/12S/14 deselected/3501 subtests**（rc=0）
- e2e + integration：**287P/4S**（rc=0；layering/wheel/size 定向 46P 绿）
- console：**1201P/13S**（rc=0；较上轮 1198P 增本单 3 条）
- landing gate：**rc=0 / 允许落地**（main 最近完成 run `cc7abfe` CI success；Console E2E 绿）
