# #220-L2 parallel 分支 input/condition 双链往返：残余面收口 + 独立回归

- 单号：plaita#220（第八轮清单 L2，高优先）
- 分支：`wt-b3`（worktree 先 `fetch` + rebase `origin/main`）
- 基线：`origin/main` = `8740485`（#237 落地后）
- 测试：`tests/unit/test_issue220_l2_parallel_branch_fields.py`（16 例 / 57 subtests）

## 背景：L2 的基础面已在 main，残余面仍有两类可观测丢失

第八轮把 L2 记为「@flow 只发 `branches={name: child}`，分支 `input`/`condition`
全丢」。基础表达面已由 `9c8820a` 补齐（分支值 dict 形态 + 发射器对称），
`tests/unit/test_issue220_dsl_roundtrip_loss.py` 已钉住 `truthy` 条件与
`$INPUT.x` 输入。本轮对同一对字段做**全形态**重跑，仍有两类残余：

1. **@flow 链：`contains` / `notContains` 分支条件不可表达**。
   `_invert_cond` 只认中缀比较算子，遇到这两个算子直接
   `EmitError: 不支持的条件算子 'contains'`——整条 flow 发不出，而不是静默丢。
2. **sexpr 链：分支 `input` 与条件 `value` 的类型漂移**。两个槽位走表达式
   发射器（裸原子约定），字面字符串 `"true"` / `"42"` / `"nil"` 读回成
   `True` / `42` / `None`；嵌套 dict/list 内的同类字符串同样漂移。运行时
   `Parallel._resolve_branch_input` 用 `evaluate(pb.input)`、`Condition.match`
   用 `evaluate(self.value)`，非 `$` 串按字面量处理——漂移会改变分支输入与
   条件比较的实际值。

## 修复（代码臂）

| 链 | 位置 | 修法 |
|---|---|---|
| @flow | `_emit._invert_cond` | `contains` → `F.contains(field, value)`；`notContains` → `not F.contains(field, value)`（参数序即 `value in field`） |
| @flow | `_expr._compile_condition` | 对称识别 `F.contains(a, b)` 调用形态为 `contains` 算子 |
| @flow | `_common._NEGATE_OP` | 补 `contains ↔ notContains`，`not` 可翻转该算子 |
| sexpr | `sexpr._node_to_src`（parallel 臂） | 分支 `:input` 改走 `_data_value_to_src`（`$` 开头仍按表达式） |
| sexpr | `sexpr._cond_to_src` | 条件 `value` 改走 `_data_value_to_src` |

> 代码臂与同一工作树的 `#238` DSL conformance 轮次同根：其 proof 明确把
> 「parallel 分支 `input` 的数据漂移」列入本轮收口。本 commit 的价值是把
> **L2 这一面**独立钉死（全 12 算子 × 双链 × 字面量类型 × 复合条件），
> 以后 #238 矩阵或逐点补丁回退时，本文件会直接红。

## 负控（修复前，基线 8740485）

```
$ .venv/bin/python -m pytest tests/unit/test_issue220_l2_parallel_branch_fields.py -q
...
E   EmitError: 不支持的条件算子 'contains'
E   EmitError: 不支持的条件算子 'notContains'
...
E   AssertionError: {'field': '$INPUT.x', 'operator': 'eq', 'value': -7}
E                 != {'field': '$INPUT.x', 'operator': 'eq', 'value': '-7'}
...
E   AssertionError: {'field': '$INPUT.x', 'operator': 'eq', 'value': None}
E                 != {'field': '$INPUT.x', 'operator': 'eq', 'value': 'nil'}
...
21 failed, 9 passed, 43 subtests passed in 0.65s
```

对应发射文本（修复前）：

```
(parallel ... (pbranch a :flow (...) :input true :when (cond $INPUT.x eq true)))
```

修复后：

```
(parallel ... (pbranch a :flow (...) :input "true" :when (cond $INPUT.x eq "true")))
```

## 验收

- 本文件：`16 passed, 57 subtests passed`
- 相关既有回归：`test_issue220_dsl_roundtrip_loss.py` / `test_codeflow_coverage4.py` /
  `test_sexpr*.py` 共 `373 passed, 268 subtests passed`
- DSL/sexpr 全族：`pytest tests/unit -k "codeflow or sexpr"` → `1109 passed, 2 skipped`
- 全量 unit（`-m "not slow"`）：`8014 passed, 15 skipped, 14 deselected,
  3491 subtests passed in 349.17s`（exit 0）
- `ci-gate.sh`：layering / wheel / size 三相绿；pytest 相位在本机 82% 处被既有的
  Redis/线程类测试阻塞（与本次改动无关，单跑该文件 14 passed），故 pytest 全量
  以 `tests/unit` 的 8014 条为准
- `mkdocs build --strict`：exit 0
- `landing_gate.py ci`：`main` CI `success`（`0e74dd0`），允许落地

## 边界

- 不改运行时节点语义（`Parallel.execute` / `Condition.match` 一字未动）。
- `ParallelBranch` 的 `priority` / `label` / `desc` / `is_default` / `next` 仍
  无表达面：`Parallel` 执行不消费这些字段，属 #238 矩阵登记的不可表达项。
- 分支 `input` 的 `$` 前缀字面量（如 `"$100 fee"`）在 @flow 链仍 fail-closed：
  该槽位按表达式求值，非法表达式应在编译期拒绝，而不是静默当字面量。
