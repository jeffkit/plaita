# 文档 ↔ 代码映射表

> 最后更新：2026-07-09  
> 用途：改代码后按「代码路径模式」匹配，判断是否需要同步文档。

| 文档路径 | 代码路径模式 | 同步触发条件 |
|----------|--------------|--------------|
| `docs/mutation-testing.md` | `plaita/core/**`, `plaita/node/**`, `plaita/event/memory.py`, `plaita/event/core.py`, `plaita/io.py`, `plaita/dsl/builder.py`, `plaita/storage/{base,memory}.py`, `tests/unit/*_mutations.py`, `scripts/*mutation*`, `pyproject.toml` `[tool.mutmut]` | 变异基线分数变化、only_mutate 扩缩、sweep/recheck 流程、§7 持续推进规范 |
| `AGENTS.md` / `CLAUDE.md` | — | 仓库导航、质量门禁、变异测试入口约束变更 |
| `docs-site/docs/architecture/layering.md` | `plaita/core/**`, `plaita/event/**`, `plaita/storage/**`, `plaita/server/**`, `tests/integration/test_layering.py` | 分层规则、默认 EventBus 解析方式、允许/禁止的 import 方向变更 |
| `docs-site/docs/architecture/execution-engine.md` | `plaita/core/executor.py`, `plaita/core/strategies.py`, `plaita/core/runner.py` | 执行模式、facade/strategy 拆分、公共运行入口变更；**订阅失败禁止挂起**；SC-003 软/硬预算 |
| `docs-site/docs/architecture/state-management.md` | `plaita/core/context.py`, `plaita/core/state.py` | Checkpoint / EventBus 解析 / `$ENV` / 取消语义变更 |
| `docs-site/docs/distributed/event-system.md` | `plaita/event/**`, `plaita/node/event_node.py`, `plaita/server/event_filter.py`, `plaita/server/flow_worker.py` | EventBus API、后端、默认总线；EventFilter 与 worker 共享 subscription storage；FlowWorker 默认启用 bus |
| `MIGRATION.md` | `plaita/**`, `plaita-ai/**` | 破坏性 API / 安全默认值变更 |
| `docs/archive/CODE_REVIEW-pre-0.5.md` | — | **已归档**，勿再更新；新审查另开文档 |
| `README.MD` | `plaita/__init__.py`, `pyproject.toml` | 版本、extras、快速上手示例变更 |
| `.github/workflows/ci.yml` | `scripts/ci-gate.sh`, `pyproject.toml` `[tool.coverage.*]` | CI 门禁步骤或覆盖率阈值变更 |
| `plaita-console/backend/auth.py` / `config.py` / `main.py` | `plaita-console/backend/**`, `plaita-console/frontend/src/services/api.ts` | 管理面 Admin API Key、契约 HMAC fail-closed、dry-run 危险节点闸门；Admin/Contract OpenAPI 分面与 README |
| `plaita-console/README.md` | `plaita-console/backend/main.py`, `plaita-console/backend/api/**` | API 分面说明、鉴权表、Swagger tag |
| （DSL IR）`plaita/dsl/ir_validate.py` | `plaita/dsl/**`, `plaita-ai/plaita_ai/flow_runner.py` | 共享拓扑校验、`flow_from_source` / `compile_flow` 编译门、registry 静默失败策略 |
| （codeflow 包）`plaita/dsl/codeflow/` | `plaita/dsl/codeflow/{_common,_expr,_nodes,_stmt,_source,_compiler}.py` | `@flow` 编译器拆分、公开 API re-export、私有符号兼容导出 |
| （Event 去重 / factory）`plaita/event/{memory,redis,sqlalchemy}.py`, `plaita/server/factory.py` | `plaita/event/**`, `plaita/server/factory.py` | handler 成功后再 mark；db 后端 `database_url→engine` |
| （节点窄接口）`plaita/core/node_context.py` | `plaita/node/basic.py`, `plaita/core/executor.py` | `NodeExecutionContext` Protocol |
