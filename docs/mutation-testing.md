# 变异测试（Mutation Testing）基线与流程

> 状态：基线已建立（`plaita/core/callback.py`），流程可复用。
> 工具：[mutmut](https://github.com/boxed/mutmut) 3.x。配置见 `pyproject.toml` 的 `[tool.mutmut]`。

## 1. 为什么做变异测试

行覆盖率只能证明「代码被跑过」，不能证明「测试能抓住缺陷」。变异测试会
系统性地改写源码（把 `==` 改成 `!=`、删掉参数、改字符串常量、翻转布尔……），
然后跑测试：如果测试仍然通过，说明这个变异点「存活（survived）」——测试
对该逻辑缺乏断言。**Mutation score = killed / (killed + survived)** 是比
行覆盖率更真实的质量信号。

## 2. 当前基线（`plaita/core/callback.py`）

基线模块选取原则：行覆盖率高（100%）、纯逻辑、无外部依赖，用来「在满覆盖下
暴露测试断言强度」。

### 2.1 第一轮基线（暴露问题）

| 指标 | 数值 |
|---|---|
| 变异点总数 | 118 |
| 真实 killed | 60 |
| 真实 survived | 58 |
| **Mutation score** | **≈ 50.8%** |

> 行覆盖 100% 但 mutation score 仅 ~51%，说明大量变异点存活。存活集中在：
> suspend/resume 回调未被调用、`LoggerCallback` 未断言、`_call_handlers` 日志
> 参数未校验、`on_flow_end`/`on_node_end` 只断言 `call_count` 不断言事件名/参数。

### 2.2 强化测试后（`tests/unit/test_callback.py`）

针对上述存活点补了 3 组断言：
- `TestCallbackManagerDispatchArgs`：断言 8 个生命周期方法把「正确事件名 + 参数
  （含 error/exception/extra kwargs）」原样分发；
- `TestCallbackManagerInit`：断言 `parent` 被记录、`inherit_handlers` 默认 False
  时不继承父 handler；
- `TestCallHandlersErrorLogging`：断言 handler 抛错时 warning 文案与 `exc_info`
  traceback 都到位；
- `TestLoggerCallbackMessages`：逐条断言 8 条日志文案。

| 指标 | 数值 |
|---|---|
| 变异点总数 | 118 |
| 真实 killed | **118** |
| 真实 survived | **0** |
| **Mutation score** | **100%** |

> 复核结果见 `mutation-recheck.txt`；初筛结果可用 `mutmut results` 查看。

### 2.3 扩展基线（expression / calculate / decide）

把 `only_mutate` 扩到三个高覆盖模块后，逐模块跑出的基线：

| 模块 | 行覆盖 | 变异点 | killed | survived | **Mutation score** |
|---|---|---|---|---|---|
| `plaita/core/callback.py` | 100% | 118 | 118 | 0 | **100%** |
| `plaita/core/expression.py` | 100% | 575 | 575 | 0 | **100%** |
| `plaita/node/calculate.py` | 96% | 79 | 78 | 1 | **98.7%** |
| `plaita/node/decide.py` | 92% | 107 | 107 | 0 | **100%** |

> 关键发现：**expression 与 calculate 的初筛 mutation score 仅 ~20%**——尽管行覆盖
> 96–100%，测试对逻辑变更的捕获力很弱。`expression.py` 575 个变异点里大量存活，
> 说明 `ExpressionEvaluator` 的测试多以「能跑通」为主，缺少对运算结果 / 边界条件 /
> 操作符语义的精确断言。`decide.py`（~57%）稍好但仍有大量存活。
>
> ⚠️ **初筛的 `survived` 不可全信**：mutmut 并行 Pool 复用 worker 不仅制造假阳性
> `timeout`，也会把真正 killed 的变异点误标为 `survived`（实测 expression 初筛 458
> survived，逐个复核后真实 survived 仅 234）。**必须用
> `scripts/recheck_mutants.sh survived`（或 `all`）对 survived 逐个复核**，才能拿到
> 真实分数。本表数字均为「初筛 + 全量复核」后的真实值。

#### 2.3.1 强化后（本轮工作）

参照 `callback.py` 的做法，给三个模块的测试补「精确断言」，结果：

| 模块 | 强化前 | 强化后 | 主要补断言 |
|---|---|---|---|
| `plaita/core/expression.py` | ~59% | **100%** | 逐函数精确 `description`/`category`/`has_side_effects`；`register` 错误文案与默认 `description=""`；`override` 语义；`unregister` no-op；默认 registry 单例缓存；`evaluate` 的 prefix 透传；`_fn_or` 回退 `False`（非 None）；`_fn_remove` 用 4 元素列表拉开 `+1`/`+2` 差异 |
| `plaita/node/decide.py` | ~87% | **100%** | `Condition.match` 的默认/自定义 prefix 透传、None 处理的 `or`/`==`/`is` 分支；`ConditionGroup` prefix 透传与空条件；`_parse_condition_content` 的 `and`/`or` 分支用「缺键」用例；`_create_condition_group` 缺省值；`Switch.execute` 命中/默认分支的精确日志文案 |
| `plaita/node/calculate.py` | ~71% | **98.7%** | 逐函数 `label`/`description`/`param_type`/`return_type` 的 `data_type`；`Call.from_json` 三条 assert 的精确文案 + 格式化符 `%`（`/` 变异会抛 TypeError）；缺 `params` 键默认 `{}` |

> `calculate.py` 剩余 1 个存活变异点 `Call.from_json__mutmut_16`
> （`FUNCTIONS.get(name, None)` → `FUNCTIONS.get(name,)`）是**等价变异**——
> `dict.get(k)` 与 `dict.get(k, None)` 对缺失键都返回 `None`，任何测试都无法区分，
> 属于 mutmut 不可避免的噪声，不计为缺陷。
>
> 强化提交：`tests/unit/test_expression.py`、`tests/test_decide.py`、
> `tests/test_calculate.py`。全量回归 `pytest tests/ -m "not integration and not e2e"`
> 909 passed，覆盖率 80.49%（gate 79% 通过）。

## 3. 已知坑点（mutmut + 本仓库）

在跑通基线的过程中踩到三个 mutmut 与本仓库交互的问题，均已规避，扩基线时
请留意：

1. **`mode="process"` 并发测试崩溃**：`tests/test_concurrent.py` 的
   `mode="process"` 子测试会与 mutmut 强制设置的 multiprocessing `fork`
   启动方式冲突而崩。已在 `pytest_add_cli_args` 用 `--ignore` 排除该文件。
2. **async / 超时相关测试在进程内 pytest.main() 下不兼容**：
   `test_approval_integration`（pytest-asyncio 事件循环）、`test_call_timeout`
   （asyncio CancelledError）等在 mutmut 进程内执行会报错。故
   `pytest_add_cli_args_test_selection` 只选了 callback 的**纯同步**测试子集。
   扩基线到其他模块时，把该模块对应的「纯同步」测试文件加进去。
3. **假阳性 timeout / 假阳性 survived**：mutmut 的 multiprocessing Pool 复用
   worker 时，旧变异点的超时 deadline 不会被取消，会在 ~timeout 秒后误杀正在跑
   新变异点的同一 worker；加上单变异点进程内 pytest.main() 执行开销本身就有
   ~25s，低于 mutmut 的 per-mutant 超时。两者都会把真正 killed/survived 的变异点
   误标为 `timeout`。**更隐蔽的是：worker 复用还会把真正 killed 的变异点误标为
   `survived`**（实测 expression 初筛 458 个 survived，逐个复核后真实 survived 仅
   234，即 ~225 个其实是 killed）。**因此 `timeout` 与 `survived` 都不可全信，
   必须用 `scripts/recheck_mutants.sh survived`（或 `all`）逐个复核**，才能拿到
   真实 mutation score。`killed` 类相对可信（测试确实失败了）。

## 4. 如何跑

### 4.1 安装

```bash
pip install mutmut      # 已加入开发依赖；或 pip install -e ".[dev]"
```

### 4.2 跑基线（并行初筛，~1.5min）

```bash
make mutation               # 等价于：rm -rf mutants .mutmut-cache && mutmut run
mutmut results              # 查看 survived / timeout / not checked
mutmut show <mutant-id>     # 查看某个变异点的具体改动
```

### 4.3 复核假阳性 timeout（必须）

```bash
make mutation-recheck       # 等价于：bash scripts/recheck_mutants.sh timeout
```

该脚本在 `mutants/` 副本里用 `MUTANT_UNDER_TEST` 激活每个 timeout 变异点、
命令行跑 pytest（单个 ~1s），给出真实 killed/survived，写入
`mutation-recheck.txt`。合并初筛的 killed/survived 即得最终 mutation score。

### 4.4 固化基线 / 扩基线

`only_mutate` 当前包含 5 个模块，`pytest_add_cli_args_test_selection` 已含它们
对应的纯同步测试文件。**但多模块合并跑会触发 mutmut worker 长时复用挂起，结果
不稳定**。复跑时建议**逐模块**：临时把 `only_mutate` 改成单个文件，再：

```bash
make mutation && make mutation-recheck
```

逐模块基线已测（见 §2.3）：callback 100%、expression 100%、calculate 98.7%、
decide 100%、**parallel_executor 100%**（均为「初筛 + 全量复核」后的真实值）。
本轮已参照 callback 的做法，给 expression / calculate / decide / parallel_executor
的测试补「精确断言」（运算结果、操作符语义、分支条件、错误文案、元数据、
max_workers 透传 / pool 单例绑定 / 异常文案），全部拉到 80%+ 目标之上。

`plaita/core/parallel_executor.py` 是 2026-07 任务 #3 抽出的纯同步执行器协议
（``ParallelExecutor`` / ``ThreadParallelExecutor`` / ``ProcessParallelExecutor``
/ ``SequentialExecutor``），无 async、无外部依赖，是天然的变异测试目标——纳入
基线即把"三种执行模式调度路径统一"的新抽象钉死。

扩到新模块时：把目标文件加入 `only_mutate`，把其对应的**纯同步**测试文件加入
`pytest_add_cli_args_test_selection`（含 async/超时/进程的测试文件不要加，见
§3），然后逐模块跑，并用 `scripts/recheck_mutants.sh survived`（或 `all`）
复核初筛结果。

## 5. 覆盖率门禁

`pyproject.toml` 已加 `[tool.coverage.report] fail_under = 79`。跑：

```bash
make coverage    # pytest -m "not integration and not e2e" --cov=plaita --cov-report=term --cov-fail-under=79
```

当前 gate 范围（`[tool.coverage.run] omit` 排除可选后端 redis/kafka/sqlalchemy、
server、client、demo 等）整体行覆盖 **≈ 80.5%**，909 个单元测试通过。低于 79%
会失败。目标：拉到 85%+ 后把 `fail_under` 上调。

### 5.1 覆盖率薄弱点（gate 范围内，按覆盖升序）

| 文件 | 覆盖 | 缺失/总 |
|---|---|---|
| `plaita/core/async_utils.py` | 46% | 38/70 |
| `plaita/__init__.py` | 54% | 27/59 |
| `plaita/dsl/sexpr.py` | 64% | 245/682 |
| `plaita/dsl/codeflow.py` | 68% | 210/653 |
| `plaita/event/core.py` | 69% | 51/162 |
| `plaita/storage/memory.py` | 69% | 22/71 |
| `plaita/node/event_node.py` | 74% | 27/103 |
| `plaita/dsl/builder.py` | 78% | 89/397 |

`async_utils.py`（sync/async 桥接）几乎无直接单测，仅被间接覆盖——
是性价比最高的补测目标。

## 6. async / distributed / timeout 路径的变异测试

`plaita/core/executor.py`（async 桥接 + 三策略）、`plaita/core/runner.py`
（超时 + 重试 + cancel）、`plaita/node/concurrent.py`（线程/进程）、
`plaita/node/code.py`（沙箱）是项目最复杂、最易出 bug 的部分，却长期不进变异
测试。**根本原因**：mutmut 并行模式在 worker 进程内跑 `pytest.main()`，与
`pytest-asyncio` 的事件循环 / asyncio `CancelledError` / `multiprocessing fork`
冲突（见 §3 第 1、2 条），一跑就挂或假阳性。

### 6.1 真正的修法：逐变异点独立进程

`scripts/run_mutation_baseline.sh` 已经是这条路的实现：它对 `only_mutate` 范围内
每个变异点单独跑 `mutmut run <mutant>`，每个变异点拿到一个**全新进程**=全新事件
循环，从根本上避开 mutmut Pool worker 复用导致的：

- 事件循环跨变异点残留；
- 过期 timeout deadline 误杀（§3 第 3 条）；
- `multiprocessing fork` 启动方式与 `mode="process"` 测试冲突（§3 第 1 条）。

代价是慢（每变异点一次 `mutmut run` 冷启动，~25–60s/个），但**结果可信**——
没有假阳性 timeout / survived 的污染，不需要再 `make mutation-recheck`。

### 6.2 操作步骤（以 `plaita/core/runner.py` 为例）

1. 临时把 `only_mutate` 收窄到目标模块，并把 `pytest_add_cli_args` 里针对该模块
   的 `--ignore` / 测试选择配好。async 模块**不要**走 `pytest_add_cli_args_test_selection`
   的纯同步子集——让 mutmut 跑该模块的全量测试（含 async）：

   ```toml
   only_mutate = ["plaita/core/runner.py"]
   pytest_add_cli_args_test_selection = [
       # 留空或只放该模块的测试；async 测试在独立进程里能跑
   ]
   ```
   （`pytest_add_cli_args` 里的 `--ignore=tests/test_concurrent.py` 仍保留，因为
   `mode="process"` 与 mutmut 的 fork 冲突是进程级问题，独立进程也躲不开。）

2. 先 `mutmut run` 生成变异点（并行初筛，可接受挂起/假阳性，只为生成 `mutants/`）：

   ```bash
   make mutation
   ```

3. 再用逐变异点独立进程跑真实结果：

   ```bash
   scripts/run_mutation_baseline.sh          # 写入 mutation-baseline.txt
   ```

   或只复核初筛标 `not checked` / `timeout` 的：

   ```bash
   scripts/run_mutation_baseline.sh --recheck
   ```

4. 看 `mutation-baseline.txt` 末尾的 `Mutation score`。对 survived 的逐个
   `mutmut show <id>` 补精确断言（async 路径尤其要断言：超时确实触发 `FlowTimeoutError`、
   cancel 传播到子节点、重试次数边界、异常类型而非仅"抛了"）。

### 6.3 当前进度与边界

- ✅ 同步基线已扩到 `parallel_executor.py`（任务 #3 副产物，100% 变异得分）。
- 🟡 async/distributed 模块（executor/runner/concurrent/code）的**逐变异点基线
  尚未跑全**：每模块数百变异点 × ~30s/个 ≈ 数小时，且 runner 的超时用例需逐点
  复核是否被 `timeout_constant` 误杀。这是独立的 1+ 天工作量，留作 follow-up，
  不阻塞当前同步基线。
- ❌ 不要把 async 模块塞进 `mutmut run` 并行模式——会挂/假阳性，白跑。
- ❌ 不要为了过变异测试把 async 测试改成同步——丢失 async 语义本身比变异得分
  倒退更严重。
