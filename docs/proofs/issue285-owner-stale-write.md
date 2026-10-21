# #285 证明：owner 过期/墓碑后原认领者的迟到写

- 单号：plaita#285（P1；#280 owner 机制的修后对抗）
- 基线：`origin/main` = `c08e118`（含 #280 `3276c20`、#282/#283/#284）
- 边界：只动 console start 预认领/占位行/入队/owner 标记与败者收敛路径；
  不改 worker 消费侧 start 去重、配额原语、CANCEL 语义。

## 根因

#280 的 owner 标记只约束**后来者 B**：B 读 owner/墓碑决定是否重认领。原
认领者 A 的三处写入完全没有所有权复核：

1. `_write_queued_placeholder` 前不复核：慢存储跨过 claiming TTL 后 A 仍写行；
2. `_enqueue` 前不复核：B 已换新 id / force DELETE 已写墓碑，A 仍入队旧载体；
3. `_set_start_dedup_owner` 是无条件 `SET`：A 恢复后把 owner 覆盖成
   `enqueued:A`，映射却是 B。

后果：同 dedup_key 两个 200、两个 id、两条载体；A 的 200 可能对应永久
queued 行（慢 case）或墓碑 id（force case）。另有两处同族问题：

- **owner 键后缀碰撞**：owner 键取 `<map>:owner`，`dedup_key="foo:owner"`
  的 map 键恰好与 `"foo"` 的 owner 键同名，两合法 key 互覆；
- **ghost 200**：A 落占位行失败返回 503 时，已在等待的 B/C/D 仍拿 A 的 id
  返回 200，但 id 既无行也无载体。

## 修法

1. **写前所有权复核**（`_start_claim_guard`，Lua）：A 在写占位行、入队前
   复核「map==我、无我的墓碑、owner==claiming:我」；通过则顺带续期。
   claiming TTL 过期但映射仍属我且无人接管 ⇒ 原地续期继续（不制造双 id）；
   被接管/墓碑 ⇒ 放弃写/入队。
2. **owner CAS**（`_set_start_dedup_owner`，Lua）：只允许 map==我、无墓碑、
   owner 缺省或同 id（可从 `claiming:` 迁到 `enqueued:`）；绝不覆盖另一个
   id 的 owner。入队后 CAS 失败 ⇒ 清自己的占位行/释放配额，收敛到当前映射
   或 409，不再返回自己的 200。
3. **owner 键独立命名空间**：`{ns}:start-dedup-owner:{key}`，与
   `{ns}:start-dedup:{key}` 分离；`foo` 与 `foo:owner` 不再互覆。
4. **败者端不返回 ghost 200**：行不存在时，有界等 A 落行；等不到且认领
   仍有效 ⇒ **409 `execution_start_in_progress`**（客户端以同一 dedup_key
   重试）；映射已释放/换主/墓碑 ⇒ 重新认领，按胜者 id 收敛。行已存在但
   owner 仍是 claiming 时同样短暂等待 enqueued，避免在载体落盘前发 200。
5. **失败路径**：占位行落盘失败 ⇒ 503 + `Retry-After`，释放映射/配额并写
   执行墓碑（经 `_discard_queued_placeholder`），等待中的败者重查后会换新
   id；A 已发出的旧 id 不再被后续重试复用。

## 测试

- `plaita-console/backend/tests/console/test_issue285_owner_guards.py`
  **5 passed**：
  ① claiming TTL=1s + 慢占位行：A 恢复后不落行/不入队，响应收敛 B，单载体；
  ② force DELETE + live claiming：A 不返回自己的墓碑 id，B 唯一有效；
  ③ `foo` / `foo:owner` 的 map/owner 四键互不相同，重试各自稳定；
  ④ A 落行失败：B/C/D 全部 409 in-progress（无 ghost 200），重试收敛新 id；
  ⑤ owner CAS 不覆盖他 id 的 owner，旧 id 释放不清他人映射/owner。
- 既有 #280 并发用例按新契约调整：B 在 A 尚未落行时拿到 409 in-progress，
  A 完成后用同一 key 重试收敛到 A；同 id/单载体/配额断言保持不变。

## 负控（先红后绿）

- 隔离 pre-fix：`git worktree add --detach /tmp/r57-nc c08e118`（本单未提交）
  + 拷入新用例 → **5 failed**（A 返回自己 id、键碰撞 3==4、B 200 而非 409、
  `allow_from_state` 不存在等）；主工作树同命令 **5 passed**。
- A1 r103 探针（仅把 ROOT 改为 argv[1]）：
  - owner TTL=1s：pre-fix A/B 双 200 双载体、owner 被 A 覆盖；post-fix
    同一 id 单载体。
  - 3+ 败者 + A 失败：pre-fix A 503 / B/C/D 200 同一 ghost id、queue 空；
    post-fix A 503 / B/C/D 409、queue 空。
  - force DELETE：pre-fix A 200 墓碑 id + B 200 新 id、两条载体、owner 覆盖；
    post-fix A 收敛 B、单载体。
  - 后缀碰撞：pre-fix owner 占用 `foo:owner` 的 map 位、重试队列增长；
    post-fix 四键独立、重试稳定。
- 日志与完整输出：`docs/proofs/data/r57-owner-cas-negative-control.txt`。

## 验收

- 全量 console：**1091P/13S**（rc=0；较上轮 1086P 增本单 5 条）
- 全量 unit（`-m "not slow"`）：**8741P/12S/14 deselected/3493
  subtests**（rc=0）
- e2e + integration：**287P/4S**（rc=0）
- plaita-ai：**293P/13S/12 subtests**（`uv run --isolated --extra all`；仓库
  venv 缺 mcp/langchain extras）
- landing gate（CI + Console E2E）：**rc=0 / 允许落地**（main 最近完成 run
  `c08e118` CI success；Console E2E 绿）
