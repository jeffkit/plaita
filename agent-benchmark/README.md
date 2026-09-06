# agent-benchmark

评估 **`flow-coder` skill** 有效性的基准测试集：把一组"用 plaita `@flow` 实现需求"的任务交给一个 AI Agent（claude code CLI + `deepseek-v4-flash`），让它生成 `@flow` 源码、编译校验、执行，再对照预期输出自动打分。

> skill 装在 `~/.claude/skills/flow-coder/`，本目录的 harness 通过把 skill 内容拼进 agent prompt 来加载它（`--arm skill`）。跑 `--arm both` 即可对比"有 skill / 无 skill"，量化 skill 的增益。

## 目录

```
agent-benchmark/
├── README.md            # 本文件
├── tasks.py             # 任务集定义（需求 + 测试用例 + 验收方式）
├── run_benchmark.py     # 执行器：调 claude CLI → 评分 → 出报告
├── runs/                # 每个 agent 运行的产物（solution.py / results.json / 日志）
└── results/             # 汇总报告（json + markdown）
```

## 前置条件

1. **claude code CLI** 已安装并在 PATH（`which claude`）。
2. **DEEPSEEK_API_KEY** 已在 `~/.zshrc` 里 `export`（harness 会回退解析它）。
3. 当前在 plaita 仓库根目录，`plaita` 可 `import`（`pip install -e .`）。
4. HTTP 类任务需 `pip install plaita[http]`；不想跑外网任务用 `--skip-http`。

harness 把请求路由到 DeepSeek 的 Anthropic 兼容端点：

```
ANTHROPIC_BASE_URL=https://api.deepseek.com/anthropic
ANTHROPIC_AUTH_TOKEN=$DEEPSEEK_API_KEY
--model deepseek-v4-flash
```

## 用法

```bash
# 列出所有任务
python agent-benchmark/run_benchmark.py --list

# 跑全部任务，带 skill（隔离模式，默认）
python agent-benchmark/run_benchmark.py

# 评估 skill 有效性：skill 臂 vs 无 skill 臂对比（隔离模式下才有意义）
python agent-benchmark/run_benchmark.py --arm both --skip-http

# 只跑指定任务
python agent-benchmark/run_benchmark.py --tasks cond-grade,map-double,nested-childflow

# 按难度 / 类别过滤
python agent-benchmark/run_benchmark.py --difficulty easy
python agent-benchmark/run_benchmark.py --category map

# 跳过依赖外网的 http 任务
python agent-benchmark/run_benchmark.py --skip-http

# 包含 known_broken 任务（REDUCE/PARALLEL，plaita 运行时 bug，默认跳过）
python agent-benchmark/run_benchmark.py --include-broken

# 关闭隔离模式：在 plaita 仓库内运行（agent 可读源码/文档，不推荐用于评估 skill）
python agent-benchmark/run_benchmark.py --no-isolated
```

### 隔离模式（默认，评估 skill 的关键）

默认在**仓库外的隔离目录**（`~/.claude/skills/flow-coder-workspace/runs/`）运行 agent：
- agent 的工作目录与 `-d` 都指向隔离目录，**不会被指向 plaita 仓库**；
- prompt 显式禁止 agent 去文件系统搜索 plaita 源码/文档；
- `plaita` 只通过 `PYTHONPATH=<plaita_root>` 让 `solution.py` 能 `import`，agent 无法浏览其源码。

这样 `without_skill` 臂只能凭 prompt 里给的极简 API 提示完成任务，**才能真正测出 skill 的边际价值**。
非隔离模式（`--no-isolated`）下 agent 能读 `docs-site/docs/guide/code-dsl.md` 和 `examples/`，baseline 会被仓库文档污染（实测 easy/medium 任务两臂都会 pass，看不出 skill 增益）。

### 环境变量覆盖

| 变量 | 默认 | 说明 |
|------|------|------|
| `BENCHMARK_MODEL` | `deepseek-v4-flash` | 传给 `claude --model` 的模型名 |
| `DEEPSEEK_BASE_URL` | `https://api.deepseek.com/anthropic` | `ANTHROPIC_BASE_URL` |
| `DEEPSEEK_API_KEY` | 从 env / `~/.zshrc` | `ANTHROPIC_AUTH_TOKEN` |
| `BENCHMARK_TIMEOUT` | `360` | 单任务超时秒数 |
| `BENCHMARK_CLAUDE` | `which claude` | claude 可执行文件路径 |

## 工作原理（单任务）

1. harness 在 `runs/<arm>/<task_id>/` 写 `inputs.json`——**只含测试输入，不含 expected**，防止 agent "背答案"。
2. 把 `flow-coder` skill 内容（`--arm skill` 时）+ 需求 + 输出规范拼成 prompt，调用：
   ```
   claude -p --model deepseek-v4-flash --dangerously-skip-permissions -d <plaita_root> <prompt>
   ```
3. agent 在该目录产出 `solution.py`（含 `@flow` 源码的运行器）与 `results.json`：
   ```json
   [{"input": {...}, "actual": <运行结果>}, ...]
   ```
4. harness 读 `results.json`，按 task 的 `validator`（`exact` / `contains` / `keys`）对照 expected 打分。
5. 全部任务跑完，汇总成 `results/report-<ts>.{json,md}` 与 `results/detail-<ts>.json`。

每个 `runs/<arm>/<task_id>/` 还会保留 `agent_stdout.txt` / `agent_stderr.txt`，便于事后排查 agent 的生成与自纠过程。

## 任务集（24 个）

| id | 难度 | 类别 | 考查点 |
|----|------|------|--------|
| `cond-grade` | easy | conditional | if/elif/else |
| `str-greet` | easy | string_concat | F.concat + F.upper（避免 f-string） |
| `arith-calc` | easy | arithmetic | F.add / F.mul |
| `in-op-small` | easy | conditional_in | `in` 操作符在 if 判断位置 |
| `len-count` | easy | builtin | F.len |
| `arith-precedence` | easy | arithmetic | 中间赋值实现 (a+b)*c（无中缀优先级） |
| `map-double` | medium | map | MAP 集合节点 |
| `filter-evens` | medium | filter | FILTER + bool 子流程 |
| `find-first-even` | medium | find | FIND 集合节点 |
| `reduce-sum` | medium | reduce | REDUCE（`for first, second in ...`）⚠️ broken |
| `childflow-double` | medium | childflow | @childflow + CHILD |
| `router-intent` | medium | conditional_nested | 多分支条件 |
| `string-template` | medium | string_concat | 多字段 F.concat 模板（避免 f-string） |
| `map-dict-price` | medium | map | MAP over list[dict]，字段访问 |
| `filter-count` | medium | filter | FILTER + F.len 统计 |
| `nested-childflow` | medium | childflow | CHILD 调 CHILD 嵌套子流程 |
| `guard-validate` | medium | conditional_nested | 边界校验 + 多分支 |
| `assignment-chain` | medium | multi_step | 多赋值串联引用上游节点 |
| `map-filter-chain` | hard | composite | MAP → FILTER 串联 |
| `map-dict-filter-chain` | hard | composite | MAP over dict → FILTER |
| `parallel-fanout` | hard | parallel | PARALLEL + join ⚠️ broken |
| `discount-price` | hard | multi_step | 条件 + 算术 + 保底价 |
| `tiered-discount` | hard | multi_step | 阶梯折扣（多区间条件） |
| `http-continue-with` | hard | http | HTTP + ErrorHandler 兜底（依赖外网） |

> ⚠️ `reduce-sum` 与 `parallel-fanout` 标了 `known_broken`：`@flow` 源码模式下 REDUCE 运行时报 `KeyError`、PARALLEL 子流程表达式求值报 `ExpressionParser._eval_variable missing 'tokens'`，均为 **plaita 运行时 bug**，非 skill 问题。默认跳过（`--include-broken` 可强制包含，会消耗超时）。

## 如何解读报告

- **平均通过率**：所有任务测试用例的通过比例，衡量生成质量。
- **全通过任务数**：完整通过的任务数，衡量"一次生成可正确执行"的覆盖率。
- **`--arm both` 对比**：skill 臂相对 noskill 臂的提升，即 skill 的有效性。**必须在隔离模式（默认）下跑**，否则 noskill 臂能读仓库文档，两臂都会 pass easy/medium，看不出增益。

## 评分公平性

- agent 拿不到 expected，只能拿 inputs，无法"背答案"。
- 流程逻辑必须由 `@flow` 源码表达（prompt 明确禁止用普通 Python 绕过 DSL）。
- **隔离模式**下 noskill 臂只拿到极简 API 提示（`flow_from_source`/`compile_source` 入口 + "@flow 是 DSL"），不能读仓库文档/源码；skill 臂拿到完整 skill。两者的差异即 skill 的真实边际价值。
