# AGENTS.md — plaita

> Plaita 逻辑编排系统的官方 Python 运行时（JSON/`@flow` → 可执行 Flow）。
> 本文件是 AI / 开发者进入仓库的**唯一导航契约**；细则链到 `docs/`。
> （`CLAUDE.md` 为本文件的软链，Claude Code 入口。）

## 项目概述

plaita 把流程定义与执行逻辑分离，支持 Normal / Generator / Distributed 三种模式，
插件化 Node + EventBus。Distributed = 可跨进程挂起/恢复（非默认至少一次投递）；
可靠性边界见 `docs-site/docs/distributed/flow-worker.md`。

**技术栈：** Python 3.10+ · Pydantic · pytest · mutmut · ruff  
**主仓库：** `https://github.com/jeffkit/plaita`

## 架构地图

依赖方向：`foundation/core` ← `event/storage/node` ← `server`（禁止反向）。
详细分层见 `docs-site/docs/architecture/layering.md`。

关键目录：
- `plaita/core/` — Flow / Executor / Context / Expression
- `plaita/node/` — 内置节点与 registry
- `plaita/event/` · `plaita/storage/` — 事件总线与执行状态
- `plaita/dsl/` — `@flow` / builder / sexpr
- `plaita/server/` — FlowWorker 与外延服务（optional）
- `plaita-console/` — FastAPI + React 管理台（backend API 面与集群档链路经 argusai E2E 覆盖）
- `tests/unit/` — 单测与 `*_mutations.py`
- `scripts/` — `ci-gate.sh`、变异测试 sweep/recheck
- `docs/` — 工程文档（变异测试、DOC_CODE_MAP）

## 开发约定

**分支：** 特性在独立分支 / worktree；勿直接破坏性改 main 而不过 CI。  
**提交：** `type: 简述`；**禁止** 提交信息加 `Co-authored-by`，**禁止** `--no-verify` 跳过 hooks。

**禁止事项：**
- 把全量 mutmut 塞进 PR CI（太慢且初筛不可信）
- 多模块合并跑 `mutmut run`（会挂 / cache 污染）
- 直接引用初筛 `survived`/`timeout` 当真实分数（必须独立进程 recheck）
- `core` 顶层 import `server` / 可选后端
- 跳过 hooks（`--no-verify`）

**文档同步：** 改代码后查 [`docs/DOC_CODE_MAP.md`](docs/DOC_CODE_MAP.md)，按映射同步相关文档。

## 常用命令

```bash
pip install -e ".[dev,lint,all]"          # 与 CI 对齐的 extras
bash scripts/ci-gate.sh                   # 回归门禁（测+覆盖+分层+SC-003）
bash plaita-console/scripts/e2e-run.sh    # console 全系统 E2E（argusai；需 Docker + mcp2cli + npm i -g argusai-mcp）
bash plaita-console/scripts/e2e-gate.sh   # 同上，门禁形态：前置硬检查 + 残留自清理 + 退出码红绿（--quick 冒烟子集）
make coverage                             # 单元覆盖率 gate
make mutation && make mutation-recheck    # 单模块变异（先收窄 only_mutate）
mkdocs build -f docs-site/mkdocs.yml --strict   # 改文档后
```

## 变异测试（持续推进）

完整契约：[`docs/mutation-testing.md` §7](docs/mutation-testing.md)。摘要：
- 日常只跑相关单测；改核心模块后 **单模块** mutation + recheck
- 禁止把全量 mutmut 加进 PR CI；只做**单模块**
- 初筛 `survived`/`timeout` 必须 `recheck_mutants.sh` 独立进程复核
- **跑前 `rm -rf mutants .mutmut-cache`**（防跨模块 cache 污染，见 §2.18/§2.19）
- 当前优先：可选扩面 `codeflow/_source.py`（`_stmt` 95.2%、`_nodes` 89.8%、`_expr` 99.2%）
- 本轮已建基线：sexpr 100%、async_utils 89.3%、codeflow/_common 95.2%、_expr 99.2%、_nodes 89.8%、_stmt 95.2%
- recheck 须在 `mutants/` 内用 `tests/...` 路径（勿用 `../tests`，见 §2.20）

## 深入阅读

| 文档 | 说明 |
|------|------|
| `docs/mutation-testing.md` | 变异基线、坑点、§7 持续推进 |
| `docs/DOC_CODE_MAP.md` | 改代码后同步哪些文档 |
| `MIGRATION.md` | 破坏性变更 |
| `docs-site/` | 用户文档（mkdocs） |
