# r94 — plaita#323 C1 控制面阶段 1：逻辑边界 + AST import 门（B2 第九十四轮）

- issue：jeffkit/plaita#323（#329 架构改造 C1 子单）· 基线 HEAD=`658a912f`（开工实测 origin/main=`45e7611`，任务书基线为其祖先，差 27 commits 全部为无文件交集的兄弟席提交，以实测基线开工）
- 分支：`fix/c1-control-plane`（origin/main `45e7611` 起）
- 开工留言：结构清单已按开工令先上单（评审未到、动关键结构前列明将实施的结构），评论区可见

## 交付（三笔分阶段提交，每笔独立回绿）

| commit | 内容 |
|---|---|
| `b06ce60` | 阶段 1-a：调度行读写（CRUD/_view/fire_history*/_record_fire）自 `local_scheduler` 下沉新模块 `services/schedule_store.py`（registry 域）；`local_scheduler` 保留运行面（`_claim_due`/`fire`/`trigger_now`/扫描循环）并**再导出**行读写符号——`api/flows`、`api/schedules` 与既有测试的 `local_scheduler.X` 调用面零变化（含 `test_issue_a48` 对 `local_scheduler._record_fire` 的 monkeypatch：`fire()` 经本模块全局名解析，再导出恰好保住该语义）；`tenants_svc._disable_tenant_schedules` 改依赖 `schedule_store`，消掉 registry→gateway 反向边 |
| `2c99f2a` | 阶段 1-b：`KNOWN_QUEUES`/`KNOWN_DLQ_KEYS` 自 `api/queues.py` 下沉 `api/redis_scan.py`（无域键登记，与「键枚举单源」同归属）；`queues` 再导出保 `queues_api.KNOWN_QUEUES` 调用面（`test_issue26` 断言不变）；`api/metrics.py` 改从 `redis_scan` 取，消掉 observability→gateway 边 |
| `2da9d0e` | 阶段 1-c：`tests/console/test_c1_control_plane_layering.py` —— 三域+shared AST import 边界门 |

## 边界门设计（plaita#323 建议项①）

- **四域登记表 `_DOMAINS`**：registry（SQL 权威定义模型：models/migrations 前缀、flow_store 族、deployments、schedule_store、graph/validation 族）/ observability（api.{audit,logs,events,metrics,sse_redis}+services.{audit,obs_link}）/ gateway（main/run/__main__/auth+其余 api/*+运行面 services）/ shared（config/error_contract/request_models/secure_fields/streaming、api.redis_scan、services.{atomic_io,read_redaction,runtime_pools}）。**未登记模块=门红**，新文件必须显式归域。
- **禁表**：shared↛registry/observability/gateway；registry↛gateway/observability；observability↛gateway；gateway 顶层不限。
- **解析器**（沿 `tests/integration/test_layering.py` 手法 + console 双布局适配）：
  - 相对（`from ..services import x`）与平铺（`from services import x`）归约到同一逻辑名；`plaita_console.` 前缀防御性剥除；
  - **名字候选优先于裸包**：`from services import audit` 记 `services.audit`，不记裸 `services`（包是载体不是目标；裸包=gateway，记了就成假阳性——首跑 [registry]/[observability] 两格假红即此，修后真树零违规）；
  - 相对导入越界判据 = 上溯步数**超过**包深度（`from .. import x` 自一深包合法落在根层，首版 `>=` 误杀）；
  - 函数体惰性导入**算数**（console 的惰性导入恰是真实依赖边），`TYPE_CHECKING` 豁免；
  - PEP 328 上溯 + 名字非模块时向上归约（`services.tenant_write_guard.assert_tenant_generation` → `services.tenant_write_guard`）。
- **唯一具名豁免**：`api/{audit,logs,events} → auth`（`require_auth`/`tenant_scope` HTTP 鉴权框架横切）；`test_exempt_edges_still_referenced` 强制豁免随真实导入共存，防豁免表腐烂。
- **负控六臂**（合成树）：registry 平铺导入 gateway / registry 相对导入 gateway / shared 导入 registry / 豁免对不报 / 函数体惰性导入必报 / TYPE_CHECKING 豁免——修前手法全过即门不假绿。

## 门禁（无管道 rc 直录，数据在 `docs/proofs/data/r94-*`）

| 门 | 结果 | rc |
|---|---|---|
| console 全量（`plaita-console/backend pytest tests/console -q`） | **1576 passed, 13 skipped**（3m29s） | 0 |
| 根 unit（`pytest tests/unit -q`） | **8946 passed, 13 skipped, 3501 subtests**（13m37s） | 0 |
| 根 integration | **213 passed, 1 skipped** | 0 |
| 根 e2e | **74 passed, 3 skipped** | 0 |
| ruff（改动 7 文件） | All checks passed | 0 |
| mkdocs（3.10 直连解释器绕 worktree `.python-version` 钉子，`--clean --strict`） | built in 12.62s | 0 |

守恒证明：console 基线 1555P（r99 时点）→ 本轮 1576P，增量全部来自**新增**门测试（20 格）与 main 期间兄弟席新增；**既有测试文件零改动**（`git diff 45e7611..HEAD --stat` 仅 2 个重构模块 + 1 新模块 + 1 新测试 + 3 个常量/导入面文件）。行为零变化 = 全家族零改动通过。

## 阶段 2（local 降级为 engine backend）——本轮未做，结构与前置已就位

- 落点：`local_executor._run_flow` 自驱动循环 → 引擎基类 `FlowWorker`（存储抽象已在：`ExecutionStorage/FlowStorage/EventBus` 注入、`redis_client` 全 getattr 兜底、`plaita/storage/{memory,sqlalchemy}` 两后端在位）+ SQLite storage 适配；`local_scheduler` 触发循环 → `ScheduleService` 同实现换 SQL 后端；`reconcile.py` 僵尸对账 → 与租约/PEL 同一恢复语义。
- 验收口径：conformance 套件同一组场景跑 local/cluster 两后端（#323 建议项③）。
- 本轮已就位的结构前提：三域门（后续重构的每一刀都过禁表）+ 调度行读写已归 registry（SQL 后端复用的落点）。
- 风险与量级注记：阶段 2 触及 `test_local_*` 五文件所钉的进程内语义（取消 Event、强删墓碑、`_ThreadLogHandler`→local_logs、checkpoint 落 `local_executions.context_json`），「测试零改动」口径下需先立 conformance 再换实现——建议独立成轮，勿与边界门混提。

## 残面 / 注记

- `services/examples.py` 归 gateway（非倒置 `gate_fn`）：默认 `gate_fn` 惰性导入 `api.flows` 被 `test_local_mode`/`test_a134` 等多测试直依赖，倒置即破坏「测试零改动」；gateway 归属语义成立（播种=发布闸消费方工作流）。
- `services/__init__.py` 再导出 `service_manager`（gateway）→ 裸 `services` 包登记为 gateway；`from . import X` 形式不受影响（解析器记 `services.X` 具体名）。
- 大仓文档（ARCHITECTURE/mona.yaml）无需改动：本轮全部在 plaita 子仓内。
