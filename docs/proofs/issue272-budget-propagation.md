# #272 证明：生产者与 worker 的预算四键必须同源

- 单号：plaita#272（P1；接 r51 b94-02 / r52 #269-271 的 CLI↔env seam）
- 基线：`origin/main` = `0c5dea5`
- 边界：只动 worker CLI 预算覆盖预检与生产者预算单源 helper；
  b94-02 的 worker 自身哈希修复、family/#271 的解析修复不重做。

## 根因

生产者 `envelope_for_stream()` 只用 env/default 算四个预算值
（lease_ttl / max_deliveries / task_max_age / dlq_max_len）。worker 用合法
CLI `--lease-ttl-seconds 600 --max-deliveries 3` 启动时，worker 自身哈希是
600/3（b94-02 已修），但 console/schedule/event_filter 新任务信封仍是
120/5：

- 首投不检查 budget → 任务正常执行；
- 失败/崩溃留下 PEL 后，worker 按 600/3 判 `config_budget_stale`，拒绝
  reclaim；`config_budget_stale` 持续增长，消息滞留到 30 天年龄闸。

A1 r96 探针（4eec873）：
`producer bg1-6a07ef4af763 != worker bg1-ed55554c3872; reclaims [None,None,None];
pending 1; config_budget_stale 3`。

## 修法（默认 fail-fast，沿 r52 #269 先例）

1. `runtime_config.envelope_budget_values(environ=None)`：生产者预算四键的
   唯一解析口（`_safe_int` 三键 + core.retention 的 age 解析）；
   `envelope_for_stream` 改用它，保证生产者信封与 CLI 预检看到同一元组。
2. `_cli_startup_problems` 增加预算比对：
   - worker 实际值 = `args.lease_ttl_seconds` / `args.max_deliveries` /
     `task_max_age_seconds()` / `_dlq_max_len_from_env()`；
   - 与 `envelope_budget_values()` 逐键比较；任一不一致 → 具名 problem，
     给出 worker 实际值、env/default 值、env 键名（lease/max 同时给 CLI
     旗标）；`main()` 聚合为 `ConfigError`、exit 2、worker 不构造。
   - CLI 与 env 一致，或只用 env（CLI 默认即 env 值）→ 照常启动；两端
     `budget_generation` 必同哈希。
   - 四键全部覆盖：lease/max 为 CLI 面；task_max_age/dlq_max_len 无 CLI
     旗标，比对的是 worker 实际解析 vs 生产者解析（防止 parser 分叉）。
3. `docs-site/docs/distributed/ops-runbook.md`「配置代际」加
   **预算四键只走 env** 口径：CLI 必须与 env 一致，否则启动拒绝；worker-only
   覆盖会造成新任务带旧 budget、PEL 永久 stale。

## 测试（10 条，`tests/unit/test_issue272_budget_propagation.py`）

- CLI 600/3 vs env 默认 → 两条 problem，含两边值 + `PLAITA_LEASE_TTL_SECONDS`
  / `PLAITA_MAX_DELIVERIES`；
- env 同步 600/3 + CLI 一致 → 无 problem；
- 四键逐个 mismatch：lease / max_deliveries / task_max_age（monkeypatch
  worker 解析为 1）/ dlq_max_len（monkeypatch worker 解析为 7）→ problem
  含 label、两边值、env 键；
- `main()` 带 600/3（env 默认）→ exit 2、worker 未构造、stderr 含两边值；
- `envelope_budget_values` 是生产者信封的实际输入（budget ==
  `budget_generation(**values)`）；
- 匹配 600/3：生产者/worker 同 budget，PEL 首投后 reclaim 成功、
  `config_budget_stale == 0`；
- 分叉探针（直接构造 worker 队列绕过 CLI 门）：producer 120/5 vs worker
  600/3 → reclaim None、`config_budget_stale > 0`（记录 pre-fix 失败形态，
  正是 CLI 预检要阻止的路径）。

## 负控（先红后绿）

把 `budget_labels = (...)` 改为空元组（恢复 pre-fix 不做预算比对）：

```
6 failed, 4 passed
E       AssertionError: assert '--lease-ttl-seconds' in ''
（CLI 600/3 无 problem）
FAILED ...test_cli_budget_override_rejected_with_both_values
FAILED ...test_each_budget_key_mismatch_rejected[lease_ttl_seconds...]
FAILED ...test_each_budget_key_mismatch_rejected[max_deliveries...]
FAILED ...test_each_budget_key_mismatch_rejected[task_max_age_seconds...]
FAILED ...test_each_budget_key_mismatch_rejected[dlq_max_len...]
E       Failed: DID NOT RAISE SystemExit
FAILED ...test_main_rejects_cli_budget_mismatch
```

匹配/env-only、单源 helper、reclaim、分叉探针 4 条保持绿（预检门不参与
它们）；还原修复后 **10 passed**。完整日志
`docs/proofs/data/issue272-negative-control.txt`。

## 验收（2026-10-21，基线 0c5dea5）

- 新增 10 passed；r52 既有 #269-271 20 条 + r51/b94-02 6 条零回归；
- 全量 unit（`-m "not slow"`）**8697 passed / 12 skipped / 14 deselected /
  3498 subtests**；e2e+integration **287P/4S**；console **1054P/13S**；
  plaita-ai **241P/14S/12 subtests**；layering / wheel / size 全绿；
- `landing_gate.py ci` **rc=0**（main `0c5dea5` CI success）后单 commit 推送
  （closes #272）。
