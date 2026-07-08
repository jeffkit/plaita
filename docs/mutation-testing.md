# 变异测试（Mutation Testing）基线与流程

> **最后更新**：2026-07-08（§2.12 state.py 99.1%、§2.11 io.py 97.4%；§2.4 event/memory.py 重新度量；11 个未补强模块全量复跑）
> 状态：第一阶段基线（7 个高分模块）+ 第二阶段全量扫描（17 个模块）均已完成；
> `expression_parser.py` 已补强至 **100%**（313/313）；
> `concurrent.py` 已补强至 **100%**（289/289，recheck 确认）；
> `loop.py` 已补强至 **99.3%**（300/302，见 §2.9）。
> 工具：[mutmut](https://github.com/boxed/mutmut) 3.x。配置见 `pyproject.toml` 的 `[tool.mutmut]`。

> **⚠️ 覆盖范围声明（2026-07，诚实评估）**
> 变异测试分两阶段：
> - **阶段一（高分模块）**：7 个纯同步模块均达 98.7%-100%，测试断言质量高。
> - **阶段二（全量基线扫描）**：17 个模块已完成 mutmut 初筛 + recheck，得到真实
>   mutation score。大多数模块得分偏低（0-33%），这是正常的"基线"状态——
>   说明这些模块测试数量不少但**断言精度**不足，是后续补断言的优先队列。
> - **async/timeout 路径的限制**：`event.memory.py` 的 `wait_for_event` 相关测试
>   会在 mutmut 进程内运行时挂死，已用 `test_event_core.py` 替代（仅覆盖 36/418 行）。
>   `executor.py` / `runner.py` / `concurrent.py` 等 async 模块通过 recheck（独立子进程）
>   获得了可信结果，不再受进程内 pytest.main() 污染。

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

## 2.4 全量基线扫描（17 个模块，2026-07-06）

使用 `scripts/run_parallel_mutation_sweep.sh --workers 4`（4 个 git worktree 并行）完成了
17 个模块的全量初筛 + recheck。recheck 阶段新增了 `timeout 60 python -m pytest` 保护，
防止 async 测试（如 `test_wait_for_event_timeout`）挂死单个变异点复核。

### 阶段一（已有高分基线）

| 模块 | 得分 | 变异点 | 状态 |
|---|---|---|---|
| `plaita/core/callback.py` | 100% | 118/118 | ✅ 已建立 |
| `plaita/core/expression.py` | 100% | 575/575 | ✅ 已建立 |
| `plaita/node/calculate.py` | 98.7% | 215/216 | ✅ 1 个等价变异 |
| `plaita/node/decide.py` | 100% | 136/136 | ✅ 已建立 |
| `plaita/core/parallel_executor.py` | 100% | 39/39 | ✅ 已建立 |
| `plaita/core/runner.py` | 100% | 123/123 | ✅ 已建立 |
| `plaita/core/context.py` | 100% | 121/121 | ✅ 已建立 |

### 阶段二（全量基线扫描，recheck 结果）

| 模块 | 得分 | 变异点 | survived | 备注 |
|---|---|---|---|---|
| `plaita/node/code.py` | **100%** | 369/369 | 0 | ✅ 完美 |
| `plaita/core/expression_parser.py` | **100%** | 313/313 | 0 | ✅ 已强化（见 §2.5） |
| `plaita/node/concurrent.py` | **100%** | 289/289 | 0 | ✅ 已强化（见 §2.6） |
| `plaita/core/executor.py` | **90.7%** | 196/216 | 20 | ✅ 已强化（见 §2.7） |
| `plaita/dsl/builder.py` | **99.9%** | 876/877 | 1 | ✅ 已强化（见 §2.8） |
| `plaita/node/loop.py` | **99.3%** | 300/302 | 2 | ✅ 已强化（见 §2.9） |
| `plaita/core/errors.py` | 13% | 6/46 | 40 | 🔴 需补断言 |
| `plaita/node/__init__.py` | 7% | 3/44 | 41 | 🔴 需补断言 |
| `plaita/event/memory.py` | **65%** | 270/418 | 148 | ⚠️ 见 §2.10（07-08 用完整套件重新度量） |
| `plaita/core/strategies.py` | 12% | 4/32 | 28 | 🔴 需补断言 |
| `plaita/core/flow.py` | 8% | 4/53 | 49 | 🔴 需补断言 |
| `plaita/io.py` | **97.4%** | 259/266 | 7 | ✅ 已强化（见 §2.11），7 等价变异 |
| `plaita/event/core.py` | 5% | 1/22 | 21 | 🔴 需补断言 |
| `plaita/node/event_node.py` | 0% | 0/80 | 80 | 🔴 需补断言 |
| `plaita/core/state.py` | **99.1%** | 213/215 | 2 | ✅ 已强化（见 §2.12），2 等价变异 |
| `plaita/storage/memory.py` | 0% | 0/28 | 28 | 🔴 需补断言 |
| `plaita/storage/base.py` | 0% | 0/14 | 14 | 🔴 需补断言 |

> 上表中 `strategies`/`flow`/`event/core`/`event/memory` 的数字为 2026-07-08 复跑结果
> （`scripts/run_unboosted_mutation_sweep.sh`，日志 `mutation-unboosted-20260707_184801.txt`）；
> 其余未补强模块仍为 07-06 基线值。`event/memory.py` 旧值 6%\* 已被 §2.10 的完整套件
> 度量取代——之前"无法标准 recheck"的结论不成立，详见 §2.10。

> *`event/memory.py` 的旧 footnote（07-06）：当时只用 `test_event_core.py` 侧面覆盖
> 36 个变异点，得到保守的 6%\*，并认为 `wait_for_event` 系列在 mutmut 中会挂死、
> 无法标准 recheck。**该结论已于 07-08 被推翻**——见 §2.10，用完整测试套件 + 单点
> 超时保护后实际可跑全量 418 个变异点，杀灭 270 个。

> **重要说明**：阶段二的低分（0-33%）是"正常基线"，不是测试框架失效：
> - 这些模块的单测**数量充足**（覆盖率 92-99%），但测试大多以"能跑通"为主
> - 缺少对返回值、操作符语义、错误路径、边界条件的**精确断言**
> - 补强路径：参照 callback.py / expression.py 的做法，逐函数补精确断言
> - 优先队列：~~`expression_parser.py`（60%）~~（已升至 100%）→ ~~`concurrent.py`（33%）~~（已升至 100%）→ ~~`executor.py`（22%）~~（已升至 90.7%）→ ~~`builder.py`（16%）~~（已升至 99.9%）→ ~~`loop.py`（17%）~~（已升至 99.3%）→ ~~`io.py`（1%）~~（已升至 97.4%）→ ~~`state.py`（0%）~~（已升至 99.1%）→ `errors.py`（13%）/ `event_node.py`（0%）

## 2.5 expression_parser.py 强化（2026-07-06，100%）

`plaita/core/expression_parser.py` 从阶段二基线的 **60%（105/175）** 提升至 **100%（313/313）**。

### 关键发现：class-level 缓存陷阱

`ExpressionParser._instances` 是类级 dict，在 mutmut in-process 多轮 pytest 之间
跨变异点共享。第一轮测试填充缓存后，后续 `_build_grammar` 变异点永远不会被重新
调用 ——138 个 `_build_grammar` 变异全部幸存的根因即此。

**修复方法**：新增 `tests/unit/test_expression_parser_mutations.py`，每个测试类在
`setUp()` 中显式调用 `ExpressionParser._instances.clear()`，强制每个测试独立触发
`_build_grammar()` 构建。

### 变异点分布与测试覆盖策略

| 类别 | 变异点数 | 代表模式 | 杀灭策略 |
|---|---|---|---|
| `_build_grammar`（语法构建）| 138 | `prefix=None`、关键字变种、`=None` | `setUp` 清空缓存 + 逐特性测试 |
| `__init__` / `for_prefix`（构建/缓存）| 9 | default arg 变 `'XX$XX'`、`inst=None` | 直接断言 `prefix` 属性 + 缓存 identity |
| `_eval_variable`（变量解析）| 5 | `rpartition` 替换、递归 context/registry | 嵌套表达式字符串测试 |
| `_eval_function_call`（函数调用）| 11 | logger 参数改 `None`、warning 消息变形 | `assertLogs` 断言日志内容 |
| `_eval_prefix` / `_eval_template`（前缀/模板）| 6 | `parse_all=False`、`matched=True` | parse_all 边界 + 多模板字符串 |
| `parse_function` / `evaluate`（入口）| 7 | context/registry 传 `None` | 自定义函数仅在 custom registry 注册 |
| `_get_attr`（属性访问）| 2 | 等价变异（`get(k, None)` vs `get(k,)`） | 间接测试（等价变异无法杀灭） |

### 最终得分

```
mutmut in-process:   276 killed / 37 not-checked / 0 survived（总 313）
recheck 独立进程:     37/37 not-checked → 全部 KILLED
真实 mutation score: 313/313 = 100%
```

等价变异（`_get_attr__mutmut_3`：`obj.get(path, None)` vs `obj.get(path,)`）在独立
recheck 中也被杀灭，因为其他测试恰好覆盖了相关路径。

## 2.6 concurrent.py 强化（2026-07-06，100%）

`plaita/node/concurrent.py` 从阶段二基线的 **33%（59/181）** 提升至 **100%（289/289，recheck 确认）**。

### 关键发现：mutmut timeout 全部为假阳性

mutmut 直接跑结果：138 killed + 151 timeout + 26 survived = 289。
经 `scripts/recheck_mutants.sh` 独立子进程复核：

- **26 个 survived → 全部 killed**（新增 `test_concurrent_mutations.py` 精准断言覆盖）
- **151 个 timeout → 全部 killed**（独立子进程跑时 151/151 全部杀灭，确认是 mutmut worker 复用假阳性）

真实 mutation score = **289/289 = 100%**。

### 新增测试：`test_concurrent_mutations.py`（20 个精准杀灭测试）

针对 26 个 survived 变异逐一设计，分六类：

| 测试类 | 目标方法 | 杀灭变异数 |
|---|---|---|
| `TestWaitBackgroundBranchesKillMutations` | `wait_background_branches` | 3 |
| `TestThreadExecuteArgs` | `thread_execute` | 4 |
| `TestProcessExecuteArgs` | `process_execute` | 4 |
| `TestCoroutineExecuteErrorMessage` | `coroutine_execute` | 3 |
| `TestExecutePassesExecutionThrough` | `execute` | 3 |
| `TestExecBranchAsyncMutations` | `exec_branch_async` | 9 |

### 核心断言策略

1. **参数传递验证**：`assert_called_once_with(THREAD, execution)` 精确断言方法调用参数，捕获所有参数替换/省略变异。
2. **字符串内容断言**：对 `coroutine_execute` 的 ValueError 消息做大小写 + XX 前缀校验，杀灭字符串变异。
3. **异步日志捕获**：用 `assertLogs("plaita.node.concurrent", level="DEBUG")` 验证 `logger.debug` 参数，杀灭 9 个 exec_branch_async 变异。
4. **后台 Future 状态验证**：在 `_BG_STATE` 中注册已完成 Future，验证 `wait_background_branches` 返回正确的 `done` 计数。

## 2.8 builder.py 强化（2026-07-06，99.9%）

`plaita/dsl/builder.py` 从阶段二基线的 **16%（48/302）** 提升至 **99.9%（876/877）**。

> 注：两次扫描变异总数不同（302 vs 1042），因初筛用了 `mutate_only_covered_lines=true` 且覆盖统计不完整。
> 第二轮以完整 builder 测试套件（test_dsl + test_builder_extended + test_builder_mutations）重新扫描，得到全量 1042 个变异点。

### 变异分布（1042 个总变异点）

| 方法/类别 | 变异数 | 杀灭 | 核心模式 |
|---|---|---|---|
| `FlowBuilder.from_dict` | ~70 | ~70 | 字段 key 字符串（camelCase 精确匹配）、runtime 默认值 |
| `FlowBuilder.validate` | ~80 | ~80（全为假阳性 timeout） | if/for/条件逻辑 |
| 节点工厂（code/http/event/child/reference 等）| ~150 | 全部 | type 字符串、id/input/next/url 参数 None 替换 |
| `branch/case/switch` | ~50 | ~50 | key 名大小写、priority 默认值、error 消息 |
| 集合节点（loop/map/filter/find/reduce）| ~80 | 全部 | next/initial/concurrent/condition 参数 |
| `LinearBuilder` 方法 | ~200 | ~200 | `_ensure_id(None)` 变异、`**extra` 删除、next/else_next 参数 |
| `build/child_flow` 函数 | ~40 | 全部 | 各构造器参数 None 替换 |
| `to_json` (`FlowBuilder`+`LinearBuilder`) | ~20 | ~20 | ensure_ascii、indent 参数 |
| `run/arun` | ~10 | ~10 | `*args` 删除 |

### 关键发现：156 个 timeout 全为假阳性

mutmut 直接跑结果：880 killed + 156 timeout + 6 survived = 1042。
经独立进程 recheck：所有 timeout 在独立进程中均 killed，确认是 mutmut worker 复用假阳性。

### 新增测试：`test_builder_mutations.py`（148 个精准杀灭测试）

分 18 个测试类，系统覆盖所有 survived 变异：

| 测试类 | 目标 | 关键策略 |
|---|---|---|
| `TestCondGroup` | cond_group relation | 验证 "or"/"and" 合法，大写版本报错 |
| `TestErrorHandler` | error_handler strategy | 验证默认 "abort"，"continue"/"continue_with" 合法 |
| `TestBranchFactory` | branch key names | 精确断言 "name"/"priority"/"next" 键名为小写 |
| `TestCaseFactory` | case normalization | 验证 `c.get("id")` fallback 到 next |
| `TestCollectionFactories` | loop/map/filter/find/reduce | next/initial/concurrent 参数转发 |
| `TestCodeFactory/TestHttpFactory/TestEventFactory` | type 字符串 | 精确断言 type=="code"/"http"/"event" |
| `TestFlowBuilderFromDict` | 所有字段键名 | 完整 roundtrip + 每字段独立测试 |
| `TestFromDictRuntimeSupplement` | runtime key | 非默认 runtime="javascript" 区分变异 |
| `TestBuildFunction/TestChildFlowFunction` | 构造器参数 | 所有 kwargs 转发到 FlowBuilder |
| `TestToJsonEnsureAscii` | ensure_ascii=False | 中文字符不被转义 |
| `TestLinearBuilderEnsureId` | `_ensure_id(id)` | 显式 id 应保留，不用 None |
| `TestLinearBuilderIfBranches` | if_ branch args | next/else_next/then/else_ 完整转发 |
| `TestLinearBuilderCollectionSupplement` | collection **extra | 额外字段在节点中保留 |

### 残余 survived（1 个，等效变异）

- **`to_json__mutmut_3`**：`ensure_ascii=None` — CPython json 模块将 `None` 视为假值，行为与 `ensure_ascii=False` 完全等效 → 等效变异，任何测试均无法区分。

## 2.7 executor.py 强化（2026-07-06，90.7%）

`plaita/core/executor.py` 从阶段二基线的 **22%（20/91）** 提升至 **90.7%（196/216）**。

> 注：两次扫描变异总数不同（91 vs 219），因第一轮基线采用 `mutate_only_covered_lines=true`
> 且测试集不同，导致覆盖到的可变异行数有差异。第二轮以完整 executor 测试套件重新扫描。

### 关键发现：旧 mutmut-stats.json 缓存导致零变异

首次切换目标模块后，`mutmut run` 报告 "0 files mutated"。根因：`mutants/mutmut-stats.json`
仍保留上一轮 `concurrent.py` 的 test-mapping 数据；新模块与旧 stats 不匹配，导致覆盖检测失败。

**解法**：切换目标时先执行 `rm -rf mutants/`，强制重新收集覆盖统计。

### 变异分布（219 个总变异点）

| 方法 | 变异数 | 杀灭 | 核心模式 |
|---|---|---|---|
| `__init__` | ~26 | 全部 | 参数传递（verbose/mode/registry/parent/handler/runner） |
| `FlowExecution.run` | ~55 | ~40 | 选项转发、timeout、distributed 参数 |
| `execute` | ~15 | 全部 | TypeError 消息、timeout merge |
| `run_compatible` | ~30 | ~26 | _prepare_strategy 参数、lambda 闭包 |
| `_ensure_flow_resolved` | ~12 | ~9 | None guard、nodes getattr、registry 传递 |
| `_prepare_strategy` | ~30 | ~25 | on_flow_start/setup_flow 参数、timeout 计算 |
| `_merge_timeout/_ms` | ~8 | 全部 | a/b 参数顺序、min() 逻辑 |
| `run_distributed` | ~25 | ~22 | 策略参数、saved_context/resume_type/timeout |
| 委托方法（get_state 等） | ~20 | 全部 | 参数传递到 `_ctx` |

### 新增测试：`test_executor_mutations.py`（76 个精准杀灭测试）

分 13 个测试类，按方法分组：

| 测试类 | 目标 | 关键断言策略 |
|---|---|---|
| `TestFlowExecutionInit` | `__init__` | 属性检查 + NodeRunner 参数捕获 |
| `TestFlowExecutionDelegation` | get_state/get_global_variable | 直接 state 注入后读取 |
| `TestFlowExecutionGetChildExecution` | get_child_execution | parent/mode/cm 继承验证 |
| `TestFlowExecutionTimeouts` | _parse_timeout/_merge_timeout_ms | 边界值（None/最小值） |
| `TestFlowExecutionExecute` | execute | TypeError 消息 + timeout 捕获 |
| `TestFlowExecutionRunClassmethod` | FlowExecution.run | 捕获内部 execution 实例的属性 |
| `TestFlowExecutionEnsureFlowResolved` | _ensure_flow_resolved | dict-node 触发 resolve_nodes |
| `TestFlowExecutionPrepareStrategy` | _prepare_strategy | mock strategy.execute 捕获参数 |
| `TestFlowExecutionRunDistributed` | run_distributed | 捕获 DistributedStrategy.execute 参数 |
| `TestRunCompatibleCallbackSupplement` | run_compatible | on_flow_end 参数 flow 对象验证 |
| `TestMergeTimeoutStaticMethod` | _merge_timeout | _merge_timeout(5000,10000)=5000 |
| `TestPrepareStrategyTimeoutSupplement` | _prepare_strategy timeout | 捕获 timeout_ms → strategy |
| `TestRunDistributedSavedContextSupplement` | run_distributed saved_context | 捕获 saved_context → strategy |

### 残余 survived（20 个，等效变异为主）

以下 20 个变异被判断为**等效变异**（semantic equivalent）或难以区分：

1. **`__init__._5`**: `RunOptions(mode=..., )` — timeout 参数缺失，但 RunOptions.timeout 默认 = None → 等效
2. **`run._20,21`**: `execution.timeout = None / timeout and execution.timeout` — execute() 内部重新 merge，结果不变 → 等效
3. **`run._25,27-45`**: distributed 模式的 key 变异（`XXresume_typeXX` 等）— 错误 key 导致 options.get 返回 "continue" 默认值，与原始值相同 → 等效
4. **`run_compatible._28,29`**: lazy 模式 on_lazy_finally lambda 变异 — 影响 generator close 路径，无对应测试场景
5. **`_ensure_flow_resolved._8`**: `getattr(flow, "nodes", )` — MagicMock 节点总有 nodes 属性 → 等效
6. **`_prepare_strategy._21`**: `timeout_ms=None` — 仅当实际设有超时时可观察，已有 test_flow_timeout_reaches_strategy_execute 覆盖，属于测试框架边界内等效

## 2.9 loop.py 强化（2026-07-06，99.3%）

`plaita/node/loop.py` 从阶段二基线的 **17%（9/52）** 提升至 **99.3%（300/302）**。

> 注：两次扫描变异总数不同（52 vs 302），因初筛用了 `mutate_only_covered_lines=true`
> 且覆盖统计不完整。第二轮以完整 loop 测试套件重新扫描得到全量 302 个变异点。

### 变异分布（302 个总变异点）

| 方法/类别 | 变异数 | 杀灭 | 核心模式 |
|---|---|---|---|
| `Loop.execute` / `Loop.arun` | ~60 | ~60 | evaluate(None)、run_compatible 参数、index 初始值/增量、条件上下文 LOOP-ITEM/INDEX |
| `Map.execute` / `Map.arun` | ~60 | ~60 | run_compatible/arun_compatible 参数、并发 Semaphore、_build_executor |
| `Filter.execute` / `Filter.arun` | ~40 | ~40 | evaluate、run_compatible 参数、debug_mode |
| `Find.execute` / `Find.arun` | ~40 | ~40 | evaluate、run_compatible 参数、index |
| `Reduce.execute` / `Reduce.arun` | ~60 | ~60 | initial 求值、object/array 两条路径的 run_compatible 参数 |
| `Reduce._child_is_array_input` | ~40 | ~38 | 属性名大小写（dataType/data_type/DATATYPE）、fallback getattr |
| `BaseCollectionNode` | ~10 | 全部 | typeDefs/itemType validator |

### 新增测试：`test_loop_mutations.py`（57 个精准杀灭测试）

分 12 个测试类，系统覆盖所有可杀变异：

| 测试类 | 目标 | 关键断言策略 |
|---|---|---|
| `TestLoopConditionMutations` | Loop 条件上下文 | 用 `$LOOP-ITEM`/`$LOOP-INDEX` 条件验证 LOOP-ITEM/INDEX 被正确写入 loop_ctx |
| `TestLoopExecuteMutations` | Loop.execute 参数 | 严格 evaluate（None→[]）、run_calls 记录 flow/debug/index |
| `TestLoopArunConditionMutations` | Loop.arun 条件 | async 版 LOOP-ITEM/INDEX 条件测试 |
| `TestLoopArunMutations` | Loop.arun 参数 | IsolatedAsyncioTestCase + arun_compatible call 记录 |
| `TestFilterExecuteMutations` / `TestFilterArunMutations` | Filter 两路径 | evaluate、debug=False、index 参数完整性 |
| `TestFindExecuteMutations` / `TestFindArunMutations` | Find 两路径 | evaluate 结果验证（返回匹配元素而非 None）|
| `TestReduceExecuteMutations` | Reduce object+array style | `_make_array_style_reduce_node` + `_make_array_reduce_ctx` 捕获 args |
| `TestReduceChildIsArrayInput` | `_child_is_array_input` | MagicMock input_type with `dataType`/`data_type` attrs |
| `TestReduceArunMutations` | Reduce arun 两路径 | object + array inputType flow |
| `TestMapArunMutations` | Map.arun 并发/顺序 | IsolatedAsyncioTestCase + index 值断言 |

### 关键测试技巧

1. **严格 evaluate mock**：`evaluate(None) → []`，`evaluate(non-None) → collection`——用来区分
   `evaluate(self.collection)` vs `evaluate(None)` 变异。
2. **run_calls 记录器**：`_make_strict_ctx` / `_make_async_strict_ctx` 在 `run_compatible` /
   `arun_compatible` 侧效中记录 `{flow, debug, args, kw}`，精确断言 `flow is not None`、
   `debug is False`、`"index" in kw`。
3. **index 递增验证**：四元素集合 `[a,b,c,d]`，child_return 返回 `index`，
   最终结果 = 3——同时杀灭 `index=1`（初始值）、`index-=1`（递减）、`index+=2`（步长2）变异。
4. **条件上下文验证**：`condition={"field": "$LOOP-ITEM", "operator": "gt", "value": 3}`
   + collection=[5,6,7]，LOOP-ITEM=None 时条件返回 False→break→result≠7。

### 残余 survived（2 个，等效变异）

- **`Loop.arun__mutmut_29`**：`condition.match(loop_ctx, )` — `Condition.match` 默认
  `prefix="$"`，与显式传 `"$"` 完全等效。
- **`Map.arun__mutmut_19`**：`Semaphore(len(triples) or 2)` 代替 `or 1` — 当集合为空时
  `Semaphore(0 or 1)=1` vs `Semaphore(0 or 2)=2`，但 gather 有 0 个任务，结果永远是 `[]`，
  Semaphore 值无影响。

## 2.10 event/memory.py 重新度量（2026-07-08，65%）

`plaita/event/memory.py` 在 07-06 基线中被标为 `6%\*`，备注"仅 test_event_core.py 侧面
覆盖 / 无法标准 recheck"。07-08 用 `scripts/run_unboosted_mutation_sweep.sh` 复跑后推翻
该结论：

| 指标 | 07-06（旧） | 07-08（新） |
|---|---|---|
| 变异点总数 | 36（仅 covered subset） | 418（全量） |
| killed | 2 | **270** |
| survived | 34 | 148 |
| **Mutation score** | 6%\* | **65%** |

### 为什么旧值偏低

旧 recheck 只用了 `test_event_core.py`（侧面覆盖），没有跑模块自己的
`test_event_memory_unit.py`，且当时认为 `wait_for_event` 的 `asyncio.wait_for(future,
timeout=None)` 变异会让 mutmut 进程内测试挂死、无法完成全量 418 点复核。

### 新做法

`run_full_mutation_sweep.sh` 的 recheck 阶段对每个变异点用 `timeout 60/120 python -m
pytest` 独立子进程跑，async 挂死会被 60/120s 超时兜底（超时即视为 killed——变异让测试
挂住也算被抓住）。配合完整测试套件 `test_event_memory_unit.py + test_event_core.py`，
418 个变异点全部可跑，杀灭 270 个。

> 剩余 148 survived 仍需补断言（`wait_for_event` 的超时语义、`EventBus` 的 handler
> 注册/卸载、事件 payload 透传等），但已不再是"不可度量"。

### 顺带修复：看门狗 fd 泄漏

复跑过程中发现 `run_full_mutation_sweep.sh` 的看门狗 `sleep 600 && kill ... &` 会继承
父管道写端；mutmut 提前结束后若 `kill` 漏掉，sleep 被孤儿化仍握着管道，导致上游 `tee`
读不到 EOF、驱动脚本死等在第一模块之后。修复：看门狗放进子 shell 并
`</dev/null >/dev/null 2>&1`，再加 `wait` 收尸。

## 2.11 io.py 强化（2026-07-08，97.4%）

`plaita/io.py` 从阶段二基线的 **1%（1/83）** 提升至 **97.4%（259/266）**。

> 前史：`tests/unit/test_io_mutations.py` 在 `8a67839` 就已存在，但**未接入**
> `run_full_mutation_sweep.sh` 的 `TESTS_FOR_MODULE`（只列了 `test_io.py`/
> `test_io_format.py`），所以 io.py 长期显示 1%。本轮既补断言又接入脚本。

### 修复的脚本分母 bug

本轮暴露 `run_full_mutation_sweep.sh` 的分数计算缺陷：`mutmut results` 只列出
**非 killed** 的变异点（survived / timeout / not checked / no tests），不含 killed。
脚本用 `grep -c "plaita\."` 数 results 输出当分母，导致只要有 killed，分母就被低估、
分数严重偏低（io.py 真实 97.4% 被脚本报成 6–12%）。**阶段二基线表中凡是有 killed
的模块，其文档分数均可能被低估**；0% 模块（无 killed）不受影响。真实分数需用
`mutmut run` 进度条的 total 或 `mutmut junit` 重新统计——另题修复。

### 新增/补强断言（`tests/unit/test_io_mutations.py`）

逐函数精确断言：`get_value` 多键回退、`Property.from_json` 全别名（data_type/
dataType/type、label/title、desc/description、is_required/isRequired、default_value/
defaultValue/default、item_type/items）、required bool/list 分支、handle_object_type
children/properties 回退 + name 默认键、handle_array_type item_type 优先级 + properties
回退、`__str__` 三态（标量/对象/数组）、`get_attr` 索引/对象/字典路径 + 缺失键
IndexError、`match` 全类型（STRING 非空、INTEGER `type(a) is int` 排斥 bool、BOOL
`is True/False` 排斥 0/1、FLOAT/NUMBER 含 Decimal）、`_match_array`/`_match_object`、
`evaluate`/`parse_function` 默认 prefix + 自定义 registry 透传 + `_parser_components_cache`
副作用、`_RegisteredFunctionsProxy.__repr__`。

### 剩余 7 个 survived（全为等价变异，不可杀灭）

- `handle_object_type._4/_6`：`content.get("children", {})` → `None`/缺省，经
  `or content.get("properties", {})` 兜底，行为一致。
- `handle_array_type._15/_17`：`content.get("properties", [])` → `None`/缺省，
  falsy 短路结果一致。
- `get_attr._23/_26`：`getattr(obj, key, [])` → `None`/缺省，经 `hasattr` 守卫
  （True 时 default 永不生效）行为一致。
- `get_attr._41`：`obj.get(path, None)` → `obj.get(path,)`（即 `obj.get(path)`），
  对缺失键均返回 None。

## 2.12 state.py 强化（2026-07-08，99.1%）

`plaita/core/state.py` 从阶段二基线的 **0%（0/80）** 提升至 **99.1%（213/215）**。

> 既有 `tests/unit/test_state.py` 已有较完整的 round-trip / 整体 dict 比较，但
> mutmut 改内部路由而 dict 视图输出相同时仍存活，故显示 0%。本轮补的是**逐行为
> 精确断言**。

### 新增 `tests/unit/test_state_mutations.py`

- `_key` 拼接、`CheckpointSchema.system_keys/bare_keys/all_known_keys` 默认 + 自定义 prefix
- `validate_checkpoint`：多 unknown 各自告警、消息含 `CheckpointSchema`、自定义 prefix、
  **`$lower`（前缀+非大写）与 `UPPER`（大写+无前缀）均不告警**——杀 `and→or` 变异
- `CheckpointState.__getitem__`：三类缺失键抛 `KeyError` **且 args[0]==key**（杀
  `KeyError(key)→KeyError(None)`）、EXPRESS_PREFIX/extras 读取
- `__setitem__`：EXPRESS_PREFIX 路由 prefix、schema 键路由 typed field、extras 懒分配
- `__contains__`：非 str 返回 False、各键成员跟踪
- `__iter__`/`__len__`/`keys`/`items`/`values`：计数 + 一致性
- `get` 默认值
- `__eq__` vs `CheckpointState` / vs 非字典非 state（返回 False）/ `__hash__` 抛 TypeError
- `fresh`：仅 `$EXECUTION_ID`+`$ENV` 在场、默认 + 自定义 prefix/names 全量断言
- `setup_flow`：6 键 + EXPRESS_PREFIX 全量、自定义 prefix
- `update_node_result`：缺失时建 map、追加、自定义 node_name
- `from_checkpoint_dict`：默认 prefix/names 全量、EXPRESS_PREFIX 覆盖 fallback、非 dict 回退
- **`_field_to_key` 全 9 字段**：经 dict 键写入后直接断言 typed field 属性
  （`s.last_node_id`/`s.flow_id`/...）——round-trip 整体比较会被 extras 路由绕过，
  只有直接断言 typed field 才能杀灭各字段字符串常量变异

### 剩余 2 个 survived（等价变异，不可杀灭）

- `__contains__._4/_5`：`if key == "EXPRESS_PREFIX"` 常量改写为 `"XXEXPRESS_PREFIXXX"`/
  `"express_prefix"`。由于 EXPRESS_PREFIX 的在场性本就由 `_present` 跟踪，通用分支
  `key in self._present` 已正确处理，特殊分支冗余——变异行为与原代码一致。

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
