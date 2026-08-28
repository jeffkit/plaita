# PGE-plaita —— Planner-Generator-Evaluator（flowcast 旗舰场景的 plaita 重构）

> 对标 `flowcast/examples/pge.flow.js`（1082 行）。本目录是同一场景在
> plaita 上的重构：**原子节点 + CODE 轻逻辑 + @flow 编排**。

## 流程结构

- **Phase 0 preflight**：baseline sha 快照 → worktree 隔离（复用已有）→ baseline gate 健康检查（不绿即中止，交还人修 main）
- **Phase 1 Planner**：需求 → spec（sprints 数组，clamp 到 max_sprints）
- **Phase 2 sprints LOOP**（condition：verdict==pass 继续，fail 即 break=中止）：每 sprint = Generator 编码 → gate → 修复一轮 → 再 gate
- **Phase 3**：cross-provider review（换 reviewer profile 审完整 diff；NEEDS_FIX → 修复一轮 → 重审）→ land commit

## 与 flowcast 版的差异

| 关注点 | flowcast 版 | plaita 版 |
|---|---|---|
| 断点续跑 | Checkpoint（step 缓存） | Distributed 挂起/恢复 + context 落盘 |
| Agent 调用 | runProfile（flowcast executor） | `agentrun` 原子（agentproc） |
| 质量门 | runGates + resumeFix 内建 | `capture`（跑命令）+ CODE 判定 + 修复轮展开 |
| worktree/git | flowcast git 原语 | `capture` 跑 git 命令 + `pge_lib.py` 薄壳 |
| baseline 检查 | 内建探测/降级逻辑 | CODE 判定 + switch 分支（显式可见） |

## 运行

```bash
# dry-run / 真跑均从本目录（或任意 cwd）：
.runtime/venv/bin/python -m plaita_flows.run --flow pge   # 若放入 flows/ 目录
# 或直接：
python -c "
from plaita import Flow
import json, plaita_nodes
from plaita.node import get_default_registry, register_code_node
register_code_node(default_backend='subprocess')
get_default_registry()
flow = Flow.from_file('pge.flow.json')  # IR 形态可直接运行
out = flow.run(repo='<目标仓>', goal='<需求>', gate='pytest -q',
               worktree_dir='<wt>', lib_dir='.',
               planner_agent='glm-52', generator='glm-52', reviewer_agent='glm-5-turbo')
"
```

输入：`repo`（目标项目）、`goal`（需求）、`gate`（验证命令）、
`planner_agent`/`generator`/`reviewer_agent`（agents.json profile 名）、
`worktree_dir`、`max_sprints`、`allow_dirty_gates`。

## 设计说明

- sprints LOOP 用 `condition`（verdict==pass 继续；fail 即 break=中止）表达
  "sprint 失败不进下一个 sprint" 的 PGE 核心纪律
- cross-review 的 NEEDS_FIX 修复轮定长展开（1 轮），与 flowcast 版语义一致
- 业务函数集中在 `pge_lib.py`（git 序列/plan 解析校验/preserve 描述），
  CODE 节点单行 `import` 引用——轻逻辑不写成自定义节点类
