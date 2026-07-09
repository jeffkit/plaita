# CLAUDE.md — plaita

> Claude Code / Cursor Agent 入口。完整导航见 [`AGENTS.md`](AGENTS.md)；本文件只强调易踩坑约束。

## 必读约束

1. **质量门禁**：改动后按场景跑 `scripts/ci-gate.sh` 或相关单测；改 docs 跑 `mkdocs build --strict`。
2. **变异测试**：细则 [`docs/mutation-testing.md` §7](docs/mutation-testing.md)。
   - 禁止把全量 mutmut 加进 PR CI。
   - 只做**单模块**；初筛 `survived`/`timeout` 必须 `recheck_mutants.sh` 独立进程复核。
   - 跑前 `rm -rf mutants .mutmut-cache`（防跨模块 cache 污染，见 §2.18/§2.19）。
   - 当前优先队列：strategies 等价收尾（不硬杀）；可选扩面 sexpr/codeflow/async_utils。
   - recheck 须在 `mutants/` 目录内用 `tests/...` 路径（勿用 `../tests`，见 §2.20）。
3. **分层**：`core` 不得顶层依赖 `server` / redis/sqlalchemy 后端。
4. **提交**：不要添加 `Co-authored-by`；不要 `--no-verify`。
5. **文档同步**：改代码后查 [`docs/DOC_CODE_MAP.md`](docs/DOC_CODE_MAP.md)。

## 常用命令

```bash
pip install -e ".[dev,lint,all]"
bash scripts/ci-gate.sh
make mutation && make mutation-recheck    # 先收窄 only_mutate
```

## 深入

| 文档 | 说明 |
|------|------|
| `AGENTS.md` | 仓库导航地图 |
| `docs/mutation-testing.md` | 变异基线与持续推进 |
| `MIGRATION.md` | 破坏性变更 |
