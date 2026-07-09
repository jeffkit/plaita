# AGENTS.md — plaita

> Plaita 逻辑编排系统的官方 Python 运行时（JSON/`@flow` → 可执行 Flow）。
> 本文件是 AI / 开发者进入仓库的导航入口（≤100 行）；细则链到 `docs/`。

## 项目概述

plaita 把流程定义与执行逻辑分离，支持 Normal / Generator / Distributed 三种模式，
插件化 Node + EventBus，覆盖即时请求到跨进程长时工作流。

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
- `tests/unit/` — 单测与 `*_mutations.py`
- `scripts/` — `ci-gate.sh`、变异测试 sweep/recheck
- `docs/` — 工程文档（变异测试、DOC_CODE_MAP）

## 开发约定

**分支：** 特性在独立分支 / worktree；勿直接破坏性改 main 而不过 CI。  
**提交：** `type: 简述`；**禁止** 提交信息加 `Co-authored-by`。

**禁止事项：**
- 把全量 mutmut 塞进 PR CI（太慢且初筛不可信）
- 多模块合并跑 `mutmut run`（会挂 / cache 污染）
- 直接引用初筛 `survived`/`timeout` 当真实分数（必须独立进程 recheck）
- `core` 顶层 import `server` / 可选后端
- 跳过 hooks（`--no-verify`）

## 常用命令

```bash
pip install -e ".[dev,lint,all]"          # 与 CI 对齐的 extras
bash scripts/ci-gate.sh                   # 回归门禁（测+覆盖+分层+SC-003）
make coverage                             # 单元覆盖率 gate
make mutation && make mutation-recheck    # 单模块变异（先收窄 only_mutate）
mkdocs build -f docs-site/mkdocs.yml --strict   # 改文档后
```

## 变异测试（持续推进）

完整契约：[`docs/mutation-testing.md` §7](docs/mutation-testing.md)。摘要：
- 日常只跑相关单测；改核心模块后 **单模块** mutation + recheck
- 当前优先：strategies 等价收尾（不硬杀）→ 可选扩面 sexpr/codeflow/async_utils
- 假低分教训：初筛不可信；recheck 须在 `mutants/` 内用 `tests/...` 路径（§2.20）

## 深入阅读

| 文档 | 说明 |
|------|------|
| `docs/mutation-testing.md` | 变异基线、坑点、§7 持续推进 |
| `docs/DOC_CODE_MAP.md` | 改代码后同步哪些文档 |
| `MIGRATION.md` | 破坏性变更 |
| `docs-site/` | 用户文档（mkdocs） |
| `CLAUDE.md` | 同内容的 Claude Code 入口 |
