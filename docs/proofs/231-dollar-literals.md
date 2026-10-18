# #231 证明：@flow 以 `$` 开头的字符串字面量保持字面量

- 单号：plaita#231（P1）
- 基线：`origin/main` = `f04de20`
- 边界：只动 `@flow` 字符串字面量的求值判定面（`_expr.py` + 受语义变更影响的
  codeflow 测试）；不改表达式引擎 `evaluate("$INPUT")` / `parse_function` 的
  变量语义；#232 未触碰。

## 根因

`plaita/dsl/codeflow/_expr.py`：

- `_compile_expr(ast.Constant)` 返回用户字面量（plain str）；
- `_render_arg` 对**所有** `str` 且 `startswith("$")` 的值原样返回——编译器
  自己产出的 `$INPUT.x` / `$F.foo(...)` 与用户字面量无法区分。

结果 `F.concat("$5", " USD")` → `$F.concat($5, " USD")`（运行期 KeyError），
`F.concat("$INPUT", "x")` 在 `$INPUT="VALUE"` 下静默变成 `"VALUEx"`；纯字面量
`return "$5"` / dict/list 内的 `$` 字符串同样被求值器吃掉。

## 修法

`_expr.py` 引入两个 str 子类，**不改运行期求值器**：

- `_CompiledExpr(str)`：编译器产出的表达式串。`_compile_expr` 的
  Name/Attribute/Call/BinOp/UnaryOp/BoolOp/Compare/IfExp/Subscript 返回（含
  折叠中间值）全部用该标记；`_render_arg` 只对标记值裸渲染。
- `_LiteralIR(str)`：以 `$` 开头的字面量的 IR 安全形态。字符串值渲染为
  `$F.concat("<literal>")`（运行期还原字面量；`_render_arg` 用 `.literal`
  原值加引号，不会嵌套）。`ast.Constant` 的 `$` 字符串返回该标记；dict/list
  内的值随容器自动生效（JSON/Pydantic 序列化即安全串）。
- `_render_arg`：`_LiteralIR` → 原字面量引号；`_CompiledExpr` → 裸渲染；
  其它 plain str（含非 `$` 前缀）→ 引号 + 既有转义（`\ " \n \r \t`）。
- 动态表达式、直接 `evaluate("$INPUT", ctx)`、条件/赋值/节点字段全部沿用原
  语义；纯字面量/dict/list 的 IR 形态变为 `$F.concat("...")`（语义等价）。
- Round-trip：`_emit.py` 无需修改——`$F.concat("$5")` 经既有
  `_invert_expr`/`_invert_f_arg` 发射为 `F.concat("$5")`，重编译得到同一 IR。

## 语义变更与测试契约

既有 `test_codeflow_childflow_flow_id.py` 有 5 条用例把 `"$FLOW_ID"` /
`"$GLOBAL.flow_id"` 写成**带引号的字面量**却期待被当表达式（旧缺陷的产物）。
按 #231 契约，引号内以 `$` 开头是字面量；动态读取改用**不带引号**的
`GLOBAL.flow_id`。已更新：

- `"$FLOW_ID"` 用例改为断言返回字面量 `"$FLOW_ID"`（钉住新契约）；
- 可观测性/分支/往返用例改用 `GLOBAL.flow_id`（动态表达式）；
- `tests/unit/test_reviewfix_a_strings.py`：ASCII 穷举逆变换把 `$` 纳入
  （不再有特例）；旧的 `test_dollar_passthrough_known_edge`（断言 `$` 透传）
  改为 `test_dollar_prefix_literal_is_quoted_not_passthrough`，断言
  `_render_arg("$") == '"$"'`、`_render_arg("$$INPUT") == '"$$INPUT"'` 且
  `$F.concat("$")` / `$F.concat("$$INPUT")` 可运行；
- 其余 codeflow/表达式套件无行为变化（1391 passed / 966 subtests）。

## 新增测试（16 条，`tests/unit/test_issue231_dollar_literals.py`）

- F 参数：`"$5"` 引号 + 运行 `"$5 USD"`；`"$INPUT"` → `"$INPUTx"`；
  `"$HOME"` 不再 KeyError；`"a$b"` 非前缀不变；多行 `$` 字面量转义保真；
  混合 `"$5", INPUT.x, " tail"` 只给字面量加引号；
- 对照：`F.concat(INPUT.x,"y")` 仍 `$F.concat($INPUT.x, "y")`，运行 `ABCy`；
- 纯字面量 `"$5"`、嵌套 dict/list 的字面量与动态值、dict 键 `"$key"` 不求值；
- round-trip：concat / 纯字面量 / dict / list 发射→重编译 IR 等价。

## 负控

把 `_render_arg` 恢复为「plain str 以 `$` 开头即原样返回」并去掉
`ast.Constant` 的 `_LiteralIR` 包装（即修复前的实现）：

```
13 failed, 3 passed
```

红文案（节选，全文 `docs/proofs/data/231-negative-control.txt`）：

```
AssertionError: $F.concat($5, " USD")          # 期望 $F.concat("$5", " USD")
AssertionError: $F.concat($INPUT, "x")
KeyError: "'$HOME' not found in expression context..."
AssertionError: $F.concat($5, $INPUT.x, " tail")
```

3 条保持绿的是动态表达式、非 `$` 前缀字面量与其中一条不区分语义的
round-trip 用例（符合预期：负控只杀字面量面）。还原后 **16 passed**。

## 验收记录

- 新增 #231：**16 passed**；负控 **13F/3P**；还原后 16P；
- codeflow/表达式相关套件：**1391 passed / 966 subtests**；
- 更新后的契约测试：`test_codeflow_childflow_flow_id.py` +
  `test_reviewfix_a_strings.py` + 新文件 = **82 passed**；
- console 全套：**546 passed / 13 skipped**；
- plaita-ai 全套（rebase 到 `fd08740` 后，含 #235）：**233 passed / 14 skipped**；
- e2e + integration：**287 passed / 4 skipped**；
- rebase 后定向套件：**1375 passed / 961 subtests**；
- 全量 unit：**7971 passed / 1 failed / 12 skipped / 3434 subtests**。唯一失败为
  既知 `test_issue159_policy_url_exception_sinks.py`（yarl 文案环境差异，clean
  基线同样失败）；无新增失败；
- landing gate：`fd08740` conclusion=**success**（CI / CI slow lane）；
- 产品代码：仅 `plaita/dsl/codeflow/_expr.py`（+ 受语义变更影响的 codeflow
  测试契约）；#232 未触碰。

## 交付记录

- commit：本单唯一 commit（`git log -1`），提交信息含 `closes #231`；落地时
  rebase 到 `fd08740`（并发并入的 #235），本单改动无冲突。
- 验收面：unit 7971P/1F（既知 yarl 环境面）；console 546P/13S；
  plaita-ai 233P/14S；e2e+integration 287P/4S。
- landing gate：绿（base `fd08740` CI / CI slow lane success）。
