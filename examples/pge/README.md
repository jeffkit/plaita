# PGE-plaita —— Planner-Generator-Evaluator（flowcast 旗舰场景的 plaita 重构）

> 对标 `flowcast/examples/pge.flow.js`（1082 行）。本目录是同一场景在
> plaita 上的重构：**原子节点 + CODE 轻逻辑 + flow JSON 编排**。

## 流程结构

- **Phase 0 preflight**：baseline sha 快照 → worktree 隔离（复用已有）→ baseline gate 健康检查（不绿即中止，交还人修 main）
- **Phase 1 Planner**：需求 → spec（sprints 数组，clamp 到 max_sprints）
- **Phase 2 sprints LOOP**（condition：verdict==pass 继续，fail 即中止）：每 sprint = Generator 编码 → gate → 修复一轮 → 再 gate → Evaluator 判定
- **Phase 3**：cross-provider review（换 reviewer profile 审完整 diff；NEEDS_FIX → 修复一轮 → 重审）→ land commit

## 前置依赖

```bash
# 1) plaita 本体（plaita 仓根目录）
pip install -e .

# 2) 节点插件包（agentrun / capture / gate / notify / writefile 等都在这里）
pip install -e ../plaita-nodes      # 或 pip install plaita-nodes

# 3) 真跑（非 --dry-run）还需要：
#    - agentproc（agentrun 节点经它驱动 Agent CLI）
#    - agents.json / providers.json 里配置的 profile（默认 glm-52 等）及凭证
```

CODE 节点由 `run.py` 里的 `register_code_node(default_backend='subprocess')`
显式注册：`subprocess` 后端无需 Docker、无需 `plaita[code]` 依赖；
如需 AST 沙箱（`restricted`）才要 `pip install plaita[code]`。

## 运行

入口是本目录的 `run.py`（加载同目录的 `pge.flow`——IR 形态，可直接运行）：

```bash
# 先用 dry-run 空转一遍：capture / gate / agentrun 全部返回假结果，不碰外部系统
python examples/pge/run.py --repo /path/to/目标仓 --goal "给 calc 新增 percentage 函数并配测试" --dry-run

# 真跑（会创建 worktree、跑 gate、调用 Agent CLI）
python examples/pge/run.py --repo /path/to/目标仓 --goal "需求描述" \
    [--gate "python -m pytest -q"] [--sprints 1] \
    [--planner glm-52] [--generator glm-52] [--reviewer glm-5-turbo]
```

输入参数：`--repo`（目标 git 仓）、`--goal`（需求）、`--gate`（验证命令，
默认 `python -m pytest -q`）、`--planner/--generator/--reviewer`（agents.json
profile 名）、`--sprints`（max_sprints 上限）。

也可以在 Python 里直接跑 IR 文件（注意文件名是 `pge.flow`，没有 `.json` 后缀）：

```python
import plaita_nodes
from plaita.node import get_default_registry, register_code_node
register_code_node(default_backend='subprocess')
plaita_nodes.register_all()

from plaita import Flow
flow = Flow.from_file('examples/pge/pge.flow')
out = flow.run(repo='<目标仓>', goal='<需求>', gate='pytest -q',
               worktree_dir='<wt>', lib_dir='examples/pge',
               planner_agent='glm-52', generator='glm-52', reviewer_agent='glm-5-turbo',
               max_sprints=1, dry_run=True)
```

## 设计说明

- **门禁纪律以 sprints LOOP 的中止语义实现**：`loop` 节点的
  `condition`（`$LOOP-RESULT.verdict == "pass"` 继续，否则整条 LOOP 中止、
  流程走 abort 分支）表达"sprint 失败不进下一个 sprint"的 PGE 核心纪律。
  `capture` 节点没有 `strict` 之类的门禁开关——不要往 `pge.flow` 里加
  未定义字段，它们会被静默忽略。
- cross-review 的 NEEDS_FIX 修复轮定长展开（1 轮），与 flowcast 版语义一致
- 业务函数集中在 `pge_lib.py`（git 序列/plan 解析校验/preserve 描述），
  CODE 节点单行 `import` 引用——轻逻辑不写成自定义节点类
