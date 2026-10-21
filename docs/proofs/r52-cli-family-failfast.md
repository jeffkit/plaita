# r52 证明（#269/#270）：CLI 覆盖必须在启动期 fail-fast

- 单据：plaita#269（P1，family 分叉）、#270（P1，CLI 0 绕过预检）
- 基线：`origin/main` = `02b67b8`（含 r51 `4eec873` 的 `_budget_int`/默认 120）
- 边界：只动 worker CLI 启动面的校验与配置视图；不改队列 gate/租约实现，
  不处理 `--queue-name` 路由错位（#269 明示边界）。

## 根因

`main()` 在 `argparse` **之前**执行
`preflight_security_and_limits()` + `load_runtime_config("worker", strict=True)`，
预检只看 env；随后才解析并直接使用 `args.lease_ttl_seconds` /
`args.max_deliveries`，并把 CLI 的 group/DLQ 传入 worker 构造。

- **#270**：`--lease-ttl-seconds 0` / `--max-deliveries 0` 完全绕过 fail-fast；
  worker 把 `max_deliveries` 钳到 1；r51 后 lease TTL 也钳到 1。0 不再炸
  `SET EX`，但 1 秒租约无续租余量（watchdog 1s 续租/1s 到期），仍非
  fail-fast，且 `config.effective` 与 CLI 实际值可能不一致。
- **#269**：worker 的 `config_family` 由构造时实际 group/DLQ 计算，而
  console/schedule/event_filter 的 `envelope_for_stream()` 只读 env。CLI
  `--consumer-group` / `--dlq-key` 覆盖使两端 family 不同；首投 family gate
  拒绝且不 ack，消息永久 pending（直到 30 天年龄闸）。

## 修法

1. `main()` 先 `parser.parse_args()`，再做预检。
2. 新增 `_cli_startup_problems(args)`：
   - `--lease-ttl-seconds` / `--max-deliveries` `< 1` → 具名 problem（字段名 +
     最小值 1）；不再用钳制掩盖非法 CLI；库内构造的钳制仅作嵌入式兜底。
   - `--consumer-group` 与 `resolve_consumer_group()` 不一致 →
     problem（给出两边值 + 要求全部署用 `PLAITA_CONSUMER_GROUP`）。
   - `--dlq-key` 与 `resolve_dlq_key(queue)` 不一致 → problem（同款；
     `--queue-name` 覆盖时跳过，属另单路由面）。
   - 空 group/DLQ → 具名 identity 错误。
3. 预检通过后把 CLI 值（strip 后）作为 overrides 传给
   `load_runtime_config("worker", strict=True)`，使 `config.effective` /
   family / 预算哈希与实际 worker 一致；任一 ConfigError → stderr +
   `SystemExit(2)`，worker 不构造。
4. CLI 值匹配 env 时照常启动（只允许「全部署同一值」的覆盖），因此
   producer/consumer family 一致。

## 测试（16 条，`tests/unit/test_r52_cli_startup_guards.py`）

- #269：group 覆盖不匹配 → problem 含两边值；group 匹配 env → 接受；
  dlq 覆盖不匹配 → problem；dlq 匹配 env → 接受；`main()` 收到不匹配
  `--consumer-group` → exit 2、worker 未构造、stderr 含两边值。
- #270：lease 0/-1、max_deliveries 0/-1 → 具名 problem（含字段名与
  `minimum 1`）；`main()` 收到 lease/max 0 → exit 2、worker 未构造；
  lease=1 合法但极端（`_watchdog_interval()==1.0`，断言相对关系）。
- family 对齐：accepted（env 匹配）worker 与 console/schedule/event_filter
  三种 producer 的 `config_family` 相等；worker-only group 覆盖构造则
  精确展示 pre-fix 分叉（该构造被 main 预检拒绝）。

## 负控（先红后绿）

`_cli_startup_problems` 直接 `return []`（恢复 pre-fix 无 CLI 预检）：

```
9 failed, 7 passed
FAILED ...test_group_override_rejected_with_both_values
FAILED ...test_dlq_override_rejected_with_both_values
FAILED ...test_lease_below_one_rejected[0]/[-1]
FAILED ...test_max_deliveries_below_one_rejected[0]/[-1]
FAILED ...test_main_rejects_lease_zero_before_worker
FAILED ...test_main_rejects_max_deliveries_zero_before_worker
FAILED ...test_main_rejects_group_override_before_worker
（main group 覆盖：Failed: DID NOT RAISE SystemExit）
```

还原修复后 **16 passed**。完整日志
`docs/proofs/data/r52-cli-negative-control.txt`。

## 验收（2026-10-21，基线 02b67b8）

- 新增 12 passed；相邻 A2R31/task_queue/worker 面 **94 passed**；
- 全量 unit（`-m "not slow"`）**8687 passed / 12 skipped / 14 deselected /
  3498 subtests**；e2e+integration **287P/4S**；console **1045P/14S**；
  plaita-ai **241P/14S/12 subtests**；layering / wheel / size 全绿；
- `landing_gate.py ci` 绿后单 commit 推送（closes #269/#270）。
