# #275 证明：CLI identity 必须规范化后写回实际参数

- 单号：plaita#275（P1；#269 守卫的 strip 旁路）
- 基线：`origin/main` = `3497a0e`（实现前；落地时并发 B3 r66 已合入，已 rebase）
- 边界：只动 worker CLI 启动预检/参数规范化；不改 #269 已覆盖的正常差异拒绝。

## 根因

`_cli_startup_problems` 用 `str(...).strip()` **只做比较副本**，`main()` 仍把
原始 `args.consumer_group` / `args.dlq_key` / `args.queue_name` 交给 worker：

- `--consumer-group "plaita-workers "` 与 env 同值（strip 后）→ guard 放行，
  worker 实际加入尾随空格的独立 consumer group。Redis 视其为两个 group，
  同一 Stream 消息被两组各投一次；而 `canonical_json` 会 strip，family 哈希
  相同，family gate 不拦（更隐蔽）。
- `--dlq-key "...:dlq "` 同理：实际 DLQ 键带空格，运维/其他进程按无空格键
  读不到。
- `--queue-name` 完全未参与 guard：CLI 指定另一个 Stream 时 worker 正常启动，
  但 console/schedule/event_filter 仍写 env Stream，任务无人消费。

## 修法

1. `_cli_startup_problems` **判定前**规范化并写回**实际使用的 args**：
   ```python
   args.queue_name = str(args.queue_name or "").strip()
   args.consumer_group = str(args.consumer_group or "").strip()
   args.dlq_key = str(args.dlq_key or "").strip()
   ```
   后续 `load_runtime_config` overrides 与 worker 构造都用写回后的值；不再是
   「比较用 strip、实际用原值」。
2. 新增 `--queue-name` 校验：空值具名拒绝；与 `resolve_queue_name("worker")`
   不一致 → 具名 problem（给出 CLI 与 env 两个 Stream + `PLAITA_QUEUE_NAME`）。
   通过后 DLQ 比较不再需要跳过 queue 覆盖（`--queue-name` 已被要求与 env 同源）。
3. 真实差异仍然拒绝：group/dlq 与 env 不一致的用例保持原判定；尾随空白
   规范化后与 env 一致则放行，且实际 group/键/Stream 无空格。

## 测试（8 条，`tests/unit/test_issue275_cli_identity_normalization.py`）

- group/dlq/queue 尾随空格 → `_cli_startup_problems` 返回 []，且 `args.*` 被
  写回为 strip 后的值；
- group/dlq 真实差异 → 仍具名拒绝（含两边值与 env 键名）；
- queue 不同 → 具名拒绝（含两个 Stream + `PLAITA_QUEUE_NAME`）；空 queue →
  具名拒绝；
- evidence：直接绕过 guard 构造 `g` / `g ` 两个 consumer group，同一 Stream
  消息两组均可读到（pre-fix 双投机制；实际入口已被规范化/queue 校验拦截）。

## 负控（先红后绿）

删除规范化写回三行 + queue-name 校验块（恢复 pre-fix）：

```
5 failed, 3 passed
E       assert 'plaita-workers ' == 'plaita-workers'
E       assert '...:dlq ' == '...:dlq'
E       assert ' plaita:flow:queue ' == 'plaita:flow:queue'
E       assert '--queue-name' in ''
5 failed
```

还原修复后 **8 passed**。完整日志
`docs/proofs/data/issue275-negative-control.txt`。

## 验收（2026-10-21，rebase 到 280be04 后）

- 新增 8 passed；r52 #269-271 / r53 #272 / r51 b94-02 既有负控零回归；
- 全量 unit（`-m "not slow"`）**8714 passed / 12 skipped / 14 deselected /
  3498 subtests**；e2e+integration **287P/4S**；console **1068P/13S**；
  plaita-ai **241P/14S/12 subtests**；layering / wheel / size 全绿；
- `landing_gate.py ci` 绿后单 commit 推送（closes #275）。
