# 变异测试下一阶段 Handoff（给新会话）

> **⚠️ 已过时（2026-07-09）**：下文基线数字（expression 20% 等）早已过期。
> **请以 [`docs/mutation-testing.md`](mutation-testing.md)（含 §7 持续推进）为准**；
> 仓库入口见根目录 `AGENTS.md` / `CLAUDE.md`。本文仅作历史交接存档。

> 上一会话已完成：覆盖率评估 + 变异测试基础设施搭建 + `callback.py` 测试强化到 100%。
> 本文档把下一阶段的工作交接清楚，目标：**把 `expression` / `calculate` / `decide`
> 三个模块的 mutation score 从 ~20% / ~20% / ~57% 拉到 80%+**，参照 `callback.py`
> 的做法。

## 0. 一分钟上下文

- 仓库：`/Users/kongjie/projects/loki/pyloki`（Python 3.10，pyenv 环境 `loki`）。
- 工具：**mutmut 3.x**（已入 `pyproject.toml` 的 `[tool.mutmut]`，已加入 `dev` 依赖）。
- 变异测试原理与全部已知坑点见 `docs/mutation-testing.md`，**先读它**。
- 覆盖率门禁：`[tool.coverage.report] fail_under = 79`，当前整体 ≈ 79.8%。

## 1. 当前基线（已测，勿重测除非要验证）

| 模块 | 变异点 | killed | survived | Mutation score |
|---|---|---|---|---|
| `plaita/core/callback.py` | 118 | 118 | 0 | **100%**（已强化） |
| `plaita/core/expression.py` | 575 | 115 | 460 | **20.0%** ← 下一阶段重点 |
| `plaita/node/calculate.py` | 79 | 16 | 63 | **20.3%** |
| `plaita/node/decide.py` | 104 | 59 | 45 | **56.7%** |

`callback.py` 的强化提交在 `tests/unit/test_callback.py`，可当模板：它用「精确
断言参数/事件名/日志文案/exc_info」杀掉了所有存活变异。

## 2. 下一阶段目标与优先级

1. **`plaita/core/expression.py`**（最高价值，575 变异点 / 460 存活）。
2. `plaita/node/decide.py`（57%，补到 80%+ 相对容易）。
3. `plaita/node/calculate.py`（20%，但变异点少，工作量可控）。

## 3. 工作流（每个模块重复一遍）

### 3.1 把 `only_mutate` 改成**单个**目标模块

编辑 `pyproject.toml`：

```toml
only_mutate = [
  "plaita/core/expression.py",
]
```

> ⚠️ **不要**一次放 4 个模块合并跑——876 个变异点会触发 mutmut worker 长时复用
> 挂起，结果不稳定。**逐模块跑**是已验证的可靠方式。

确认 `pytest_add_cli_args_test_selection` 已含该模块的纯同步测试文件（4 个模块
的测试文件上一会话已配好；扩到新模块时再加）。**不要加含 async/超时/`mode="process"`
的测试文件**（见 `docs/mutation-testing.md` §3）。

### 3.2 跑初筛 + 复核

```bash
cd /Users/kongjie/projects/loki/pyloki
make mutation            # rm -rf mutants .mutmut-cache && mutmut run，~1–2min
mutmut results           # 列 survived / timeout / not checked（不列 killed）
make mutation-recheck    # 复核 timeout 的真实 killed/survived → mutation-recheck.txt
```

> `mutmut results` 里的 **`timeout` 全是假阳性**（mutmut Pool worker 复用 + 进程内
> pytest.main() 开销），必须 `make mutation-recheck` 才能拿到真实状态。`survived`
> 是可信的。`killed` = 总数 − survived − timeout（从 `mutants/**/*.meta` 的
> `exit_code_by_key` 数：`1`=killed, `0`=survived, `-24`=timeout）。

### 3.3 找存活变异点、看改了什么

```bash
mutmut results | grep survived        # 列存活变异点
mutmut show <mutant-id>               # 看具体 diff，例如：
# mutmut show plaita.core.expression.xǁExpressionEvaluatorǁevaluate__mutmut_8
```

### 3.4 补断言杀死存活变异

针对存活变异点的语义，给对应测试文件补「精确断言」。`callback.py` 的套路：
- 改参数/丢参数 → `assert_called_once_with(精确参数)`；
- 改字符串常量 → 断言日志/返回值的精确文案；
- 改默认值/布尔操作 → 断言该路径的具体行为；
- 改日志 exc_info → `assertTrue(record.exc_info)`（不是 `assertIsNotNone`，
  `exc_info=False` 会让 `record.exc_info=False` 而非 `None`）。

补完后先 `python -m pytest tests/unit/test_expression.py -q` 确认原码通过，再回到
3.2 重跑变异测试验证分数。

### 3.5 验证达标后

- 把该模块从 `only_mutate` 移除（或保留单文件配置继续下一个）。
- 更新 `docs/mutation-testing.md` §2.3 的基线表数字。
- 跑一次 `python -m pytest tests/ -m "not integration and not e2e" -q` 确认无回归。

## 4. expression.py 起步提示

- 测试文件：`tests/unit/test_expression.py`、`tests/unit/test_expression_golden.py`。
- 575 变异点 / 460 存活——大概率是「测试只断言能跑通、不断言运算结果」。
- 建议先 `mutmut show` 抽样 20 个 survived，按变异类型归类（改操作符？改常量？
  改分支？），再批量补断言；`ExpressionEvaluator` 的每个操作符/函数都应有
  「输入→精确输出」用例，覆盖边界（0、负数、空串、除零、类型不匹配）。
- 该模块行覆盖 100%，所以**不需要补用例提覆盖**，只需把现有用例的断言做精确。

## 5. 关键文件清单

- `pyproject.toml`：`[tool.mutmut]` 配置、`[tool.coverage.report]` 门禁。
- `Makefile`：`coverage` / `mutation` / `mutation-recheck` / `clean-mutants`。
- `scripts/recheck_mutants.sh`：timeout/notchecked/all 复核（手动 pytest，~1s/个）。
- `scripts/run_mutation_baseline.sh`：逐变异点独立进程跑（备用，慢）。
- `tests/unit/test_callback.py`：强化的模板用例。
- `docs/mutation-testing.md`：完整背景、坑点、跑法。
- 产物（已 .gitignore）：`mutants/`、`mutation-recheck.txt`、`.mutmut-cache/`。

## 6. 勿踩的坑（上一会话血泪）

1. **shell CWD 串目录**：mutmut 会在 `mutants/` 子目录里跑 pytest。若你在 shell
   里 `cd mutants` 后再 `mutmut run`，它会读到 `mutants/pyproject.toml`（旧配置）
   而非根目录。跑 mutmut 前务必 `cd /Users/kongjie/projects/loki/pyloki`。
2. **改 `pyproject.toml` 的 mutmut 配置会让 mutmut 重生成变异点并清空已跑结果**：
   改完配置就要重跑 `make mutation`，别指望复用旧结果。
3. **`timeout` 类不可信**：见 §3.2，必须复核。
4. **合并跑会挂**：见 §3.1，逐模块跑。
5. **提交时不要加 Co-authored-by**（用户规则）。
6. **代码-文档同步**：项目无 `docs/DOC_CODE_MAP.md`，该规则不触发；但变异基线
   数字变更要同步更新 `docs/mutation-testing.md` §2.3。

## 7. 验收标准

- 目标模块 mutation score ≥ 80%（killed/(killed+survived)，timeout 已复核归并）。
- `python -m pytest tests/ -m "not integration and not e2e" -q` 全绿、无回归。
- `docs/mutation-testing.md` §2.3 基线表已更新。
- 新增/修改的测试有清晰断言，不靠 `call_count` 这类弱断言蒙混。
