# r58 证明：DLQ replay 目标 queue family 校验

- 单号：duo-a 第九十六轮 a/P2（无独立 issue 号；`#286/#287` 属 dedup seam，
  本单不涉）
- 基线：`origin/main` = `dc31efa`
- 边界：只动 `plaita/server/dlq_replay.py` 的目标 family 校验与 console
  replay 请求参数；不改 marker/幂等/schema/tombstone/审计语义，不改 worker
  family gate。

## 根因

`replay_dead_letter` 构建 `config_envelope` 时只继承源 family，从未校验目标
stream 的 family。源 F1 的死信回灌到 F2 的 stream 时：

- replay 返回 `replayed=True, reason="ok"`，写一次性 marker 与目标 Stream；
- 目标 worker 读到时 `_family_mismatch` 判真，计数
  `config_family_mismatch=1`、消息留 pending；
- 不产生执行，但运维界面显示「replayed ok」——假成功，且 marker 已烧掉，
  修好后无法直接重放同一条死信（需 force/清 marker）。

## 修法

1. 在源 `config_envelope` 构建后、schema 检查/marker/入队之前解析目标
   stream 的**生效 family**（`_resolve_target_family`）：
   - 默认 `envelope_for_stream(target_stream, role="worker")`（目标 stream +
     生效 group/DLQ，与目标 worker/生产者同源）；
   - 自定义 group/DLQ 部署由调用方显式传 `target_config_family`（请求体同名
     字段）；显式值必须是合法 `cf1-` 代际，否则 `target_family_unknown`；
   - 解析异常/无法确定 ⇒ `target_family_unknown`（fail-closed）。
2. 源 family（缺失/畸形已 `normalize_generation` 为 `legacy`）与目标不一致
   ⇒ `ReplayResult(replayed=False, reason="target_family_mismatch",
   detail=both families)`：不写 marker、不写目标 Stream、源 DLQ 保留、审计
   行 `result=target_family_mismatch`（`_reject` 统一落审计）。
3. 管理员显式 `allow_family_mismatch=True` 才可跨 family（WARNING + 审计）；
   绝不静默把源 family 继承进不同 stream。console 请求模型同步暴露
   `target_config_family` / `allow_family_mismatch` 两个 additive 字段。
4. 检查在 marker 之前，因此拒绝不消耗幂等标记，修正参数后可立即重放；
   同 family 正控与既有 replay 行为不变。

## 测试

- 新增 `tests/unit/test_r58_dlq_replay_target_family.py` **5 passed**：
  ① 跨 family 拒绝（replayed=False/reason、目标 XLEN=0、marker 不存在、
  源 DLQ 保留、审计 rejection、目标无 pending/family_mismatch；随后同 family
  正控成功）；② 同 family 直接成功；③ legacy 源默认拒绝、显式 override
  成功；④ 非法显式 family ⇒ `target_family_unknown` 且不消耗 marker；
  ⑤ 自定义 group/DLQ 场景显式 `target_config_family` 按声明校验。
- 既有 replay 用例的源信封补 `config_family`（与目标同源，正控语义不变）：
  `tests/unit/test_r53_dlq_replay.py`（+1 行 fixture）、
  `test_r65_dlq_replay_audit_cap.py`、`test_a69_04_tombstone_age.py`、
  console `test_r53_dlq_replay_api.py`、`test_r55_audit_queue_surface.py`。
- 既有 replay 三文件 **41P**、console 两文件 **19P** 全绿。

## 负控（先红后绿）

- 隔离 pre-fix：worktree `dc31efa` + 本单文件，删除 target-family 检查块 →
  **3 failed / 2 passed**（跨 family、legacy、非法显式均报 `ok`）；主工作树
  同命令 **5 passed**。
- 规格探针 `/tmp/r104_dlq_replay_family.py`（ROOT 参数化）：
  - 修复前：`replayed=True/ok`、目标 XLEN=1、marker=True、目标
    `family_mismatch=1`、pending=1；
  - 修复后：`replayed=False/target_family_mismatch`、目标 XLEN=0、marker=False、
    源 DLQ=1、目标无 pending/mismatch。
- 完整输出：`docs/proofs/data/r58-dlq-replay-family-negative-control.txt`。

## 验收

- 全量 unit（`-m "not slow"`）：**8746P/12S/14 deselected/3493
  subtests**（rc=0；较上轮 8741P 增本单 5 条）
- e2e + integration：**287P/4S**（rc=0）
- console：**1091P/13S**（rc=0；replay 相关既有用例 19P 全绿）
- landing gate（CI + Console E2E）：**rc=0 / 允许落地**（main 最近完成 run
  `dc31efa` CI success；Console E2E 绿）
