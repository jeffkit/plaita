# r101（B3 修复席）· #328 F1 版本维度收敛——family 启动校验 + budget 快照，删除永久 pending

- 单：plaita#328（P1，无依赖；#329-F1 tracking 行「两个版本维度 / family 启动校验 / budget 快照 / 删除永久 pending」）
- 分支：`fix/f1-version-dimensions`（独立 worktree `/private/tmp/plaita-b3-r101`）
- 基线：任务书/正文基线 `658a912f` → 开工实测 `664d1523`（r100 合并）→ 门禁前 rebase 到 `origin/main a56125ab`（B1 #320 执行状态模型三笔；`flow_worker.py` 交集但 hunk 不相交、零冲突、语义正交——#320 在执行状态持久化区，本单在信封/队列/启动校验区）
- 续做说明：本轮工位是上会话被中断的半成品（三文件未提交 + 测试初稿 + 初稿红相 `r101-i328-pre-red.txt`），本会话续做而非重做；初稿实现遗留 **4 处运行期断链**（`flow_worker` 构造期读已删 `RuntimeConfig.generation`、`event_filter`/`schedule_service`/console `executions.py` 函数内 import 已删 `envelope_for_stream`）与 console/capacity/metrics/dlq_replay 残面，均在本轮补齐。

## 规格 → 落地

| 正文建议 | 落地 |
|---|---|
| ① 只保留两个版本维度：wire schema + IR | 信封 = `schema_version` + `tenant_generation` + `max_deliveries`；`config_family/config_generation/budget_generation/config_effective_at` 四哈希从 `runtime_config`（`ConfigGeneration`/`build_generation`/`envelope_for_stream`/`normalize_generation`…）、`task_queue`、`flow_worker`（注册 metadata / 队列构造探测 / 容量块）、`capacity`（容量块、`reject_reason` family 闸、`REASON_CONFIG_FAMILY_MISMATCH`、三派发函数的 `config_family` 参数）、`metrics`（三计数器）、console `cluster.py`（两个响应模型 + `config_hash`）全部删除。账本 `docs/governance/architecture.yaml` 新增 `gov.version_dimensions: [ir, wire_schema]`（新增维度须 ADR + 删除计划） |
| ② `config_family` 移到 worker 启动校验 | `_cli_startup_problems`（B1 r52 #269/#270 既有）即是启动校验：`--queue-name/--consumer-group/--dlq-key` 与 env 不一致 ⇒ 拒启动 exit 2；namespace 空 env 由 `load_runtime_config(strict=True)` 在 argparse 前拒绝。docstring/文案由「family gate 会拒收」改写为「启动校验就是 family 闸」。**#272 预算一致性块删除**（预算随信封走后 worker-only 预算覆盖不再搁浅 PEL；`<1` 硬校验保留） |
| ③ `budget_generation` → 消息上的预算快照 | `enqueue_task(max_deliveries=_SNAPSHOT_FROM_ENV)` 缺省盖章 `PLAITA_MAX_DELIVERIES`；`StreamTask.max_deliveries`；reclaim 死信判据 `task.max_deliveries if not None else self.max_deliveries`；畸形快照 WARNING + 按缺失；requeue/DLQ 信封/DLQ replay 三继承面带快照（replay 缺失/畸形显式 `None`＝legacy 信封，不落到「取当前 env」缺省） |
| ④ `tenant_generation` 保留为身份字段 | 不动（#277/#283 语义照旧，三继承面继续带） |
| ⑤ 除 schema-too-new→DLQ 外无永久 pending | `task_queue` 的 family 预读闸（原 :795-801）与 budget 代际 reclaim 跳过（原 :1727-1741）已删；DLQ replay 的 `target_config_family/allow_family_mismatch/target_family_mismatch/target_family_unknown` 闸删除（console `queues.py` 两请求字段同删，前端零消费） |

生产者侧（console `executions.py` / `schedule_service` 两处 / `event_filter` 两处）去掉 `**envelope_for_stream(...)` 拆包，快照由 `enqueue_task` 缺省盖章。

## 先红后绿

- 初稿红相（上会话，测试初稿 × `664d1523`）：`data/r101-i328-pre-red.txt`。
- **最终红相**（最终测试文件 × `origin/main a56125ab` 干净 archive 树，`import plaita` 解析到该树）：`data/r101-i328-pre-red-final.txt` — **13 failed / 7 passed**。13 红逐条落在目标缺陷：信封形状×3（写四哈希/无 `max_deliveries` 参数）、外族 family 戳 read/reclaim 滞留×2、旧 budget 戳 reclaim 跳过×1、快照预算不生效×1、requeue/DLQ/replay 继承面×4、metrics/stats 三计数器×2。7 绿=兼容格正控（legacy 载体按本地预算×2、快照未耗尽照常×1、legacy requeue 不变×1）+ 启动校验三格（B1 r52 既有实现，本单只改文案），旧实现上本就该绿。
  - 负控锋利度修正：`_queue` 助手最初无条件吞掉旧 kwargs 使三条滞留负控在旧代码上假绿（旧队列缺省 family=legacy 被旧闸放行）；改为**按构造器签名探测**（旧世界传入、新世界丢弃）后三条精确红。
- 绿相：`data/r101-i328-post-green.txt` — 20 passed。

## 存量测试处置（守恒说明）

- 整档退役 4：`test_b94_02_budget_generation`（预算哈希）、`test_r58_dlq_replay_target_family`（replay family 闸）、`test_issue272_budget_propagation`（#272 预算一致性 + budget_generation reclaim）、`test_r52_age_zero_alignment`（age=0 的 budget_generation 对齐）——全部测被删机制本身。
- `test_a2r31_config_generation` 拆分：删 B2R31-01 六类（canonical/family gate/budget stale/requeue 继承/dispatch family/DISABLE 方言中的 generation 断言），保留 B2R31-02/03（resolver/非法值/租户纪元/group 改名与孤儿流）23 测；DISABLE 方言剥出无 generation 版本。
- 适配：`test_r52_cli_startup_guards`（`TestFamilyAlignment`→`TestTopologyAlignment`：同一部署判据=queue/group/dlq 三元组逐项相等）；`test_r53/r65/a69_04` 与 console `test_r53_api/r55/r135` 去 `envelope_for_stream` 种行，r135 `with_generations`→`with_snapshot`（断言 `max_deliveries` 继承）。
- **预算机制簇 legacy 种子**（`test_task_queue` DeadLetterGuard / `issue252` / `issue312` / `issue326` / `issue327` / `a125` / `issue282_283`）：这些用例用队列本地 `max_deliveries`（1/3/1e5）裁判死信，而 `enqueue_task` 缺省现在盖章 env 快照 5 ⇒ 快照盖过本地值。机制用例改种 **legacy 载体**（`max_deliveries=None`），本地预算继续裁判；快照路径由 #328 新文件专门覆盖。
- 变异锚点：`scripts/mutation_replay/mutations/87-event-enqueue-failure-keeps-dedup-key.json` / `87-timeout-…json` 的 `old` **与 `new`** 同步到新 try 体（只改 old 会让变异体 ImportError 而非「静默吞异常」，失去本义）。

## 门禁（无管道直录 rc，`data/`）

| 门 | 结果 | 文件 |
|---|---|---|
| unit | **9017 passed / 13 skipped，rc=0** | `r101-gate-unit.txt`（跑于最终树减去 `_queue` 助手签名探测 4 行；该文件单跑复验 20P 见 post-green） |
| console | **1577 passed / 14 skipped，rc=0**（与 r100 终树同数） | `r101-gate-console.txt` |
| integration | 213 passed / 1 skipped，rc=0 | `r101-gate-integration.txt` |
| e2e | 74 passed / 3 skipped，rc=0 | `r101-gate-e2e.txt` |
| ruff | CI 硬门 `ruff check plaita tests --select E722` rc=0；BLE001（CI `--exit-zero` 咨询）按文件计数与 origin/main 逐一相等（cluster 9 / event_filter 3 / flow_worker 11，皆非本单触碰行）；其余 12 个改动文件零报 | `r101-ruff-e722.txt` |
| mkdocs | `--clean --strict` rc=0 | `r101-mkdocs.txt` |

改动面：40 files，+965 / −2248。

## 给主控/owner 的备注

- 账本断言对侧：大仓根 `docs/DOC_ASSERTIONS.yml` 可加 `gov/version-dimensions`（`yaml_path_equals` · `path: gov.version_dimensions` · `expected: [ir, wire_schema]`），与 `gov/runtime-valid-set` 同形；本单不跨仓提交。
- `GET /api/cluster/config`/`instances` 响应减去四哈希与 `config_hash`、DLQ replay API 减去两请求字段：前端 `src/` grep 零引用，无需前端改动。
- env 零增零删（正文「每季净减 1」口径）：本单删的是信封字段、API 字段、三个指标与一个启动检查块，不含 env；`PLAITA_MAX_DELIVERIES` 语义从「本地预算」扩为「生产者快照源 + legacy 本地预算」。
