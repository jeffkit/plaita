# #238 DSL 往返 conformance：全节点类型 × 全字段 × 双链

- 单号：plaita#238（P1）
- 分支：`wt-b3`（worktree 先 `fetch` + rebase `origin/main`；main-first 仲裁）
- 基线：`origin/main` = `8740485`（#237 租户上下文契约落地后）
- 边界：只动 DSL 往返面（`plaita/dsl/codeflow/**`、`plaita/dsl/sexpr.py` 及其
  conformance 测试/文档）。#227 已关，不混入；#220-L2 分支输入的同类数据
  漂移因落在同一往返面，按 conformance 要求一并收口。

## 问题

三表示（JSON IR / `@flow` / sexpr）互转长期靠逐 issue 补 round-trip 测试，
每轮都会再挖出新的静默丢字段面：

`#75/#76/#88/#89/#127/#136-#150/#200/#207/#220`（一次 7 处）→ `#227`
（`loop.condition`）——逐点修不够。安全/语义字段静默丢失最危险（#200 的
SSRF 白名单、#227 的循环条件都会让流程「成功」但语义裸奔）。

## 方案：registry 驱动的 conformance 矩阵

新增 `tests/unit/test_issue238_dsl_conformance.py`：

1. **registry 是权威清单**：从 `NodeRegistry()` 取全部内置节点类型，反射每个
   节点类的 `model_fields`。registry 新增节点/字段而矩阵未登记
   → `test_every_registry_field_is_classified` 直接红。
2. **全字段 fixture**：19 个节点类型（registry 18 + registry 外的 `code`）各
   一份把模型字段填满的 IR，值刻意选「类型敏感」拼写（`"true"` / `"1"` /
   `$…`），任何裸原子/丢引号路径都会漂移并被抓住。
3. **双链逐格断言**：`JSON → @flow → JSON` 与 `JSON → sexpr → JSON`。
   - `OK`：逐字段相等；
   - `LOST`：链无法表达，fixture 给非默认值并**反向断言仍然丢失**——将来
     补上支持而不更新矩阵也会红；
   - `INERT`：模型有字段但该节点类型无运行期/作者面；
   - `DERIVED`：validator 合成或编译期派生注解；
   - `TYPE_LOST`：整类型不可表达，链调用必须 fail-closed 抛错。
4. **整树深等价**：归一化后整树相等；归一化只做「剥派生注解、snake/camel
   别名、登记丢失字段、非语义 id 按图序规范化」——`$NODE.` 引用到的语义 id
   保持原名，故下游引用仍被钉死。
5. **丢失面显式登记**：每个非 OK 单元都有原因说明
   （`test_every_non_ok_cell_has_a_reason`），矩阵含过期字段也会红
   （`test_matrix_has_no_stale_fields`）。
6. **负控**：`TestNegativeControls` 逐条拆掉本轮修复并断言对应字段重新丢失；
   `test_harness_detects_unregistered_new_field` 动态注册带新字段的节点类，
   断言覆盖检查报红。

完整矩阵见 [`238-conformance-matrix.txt`](data/238-conformance-matrix.txt)：
19 节点类型 / 490 个（字段×链）单元；状态计数
`ok=300, lost=52, derived=111, type-lost=25, inert=2`。

## 本轮修复

### `@flow` 链

| 面 | 修前 | 修法 |
|---|---|---|
| 条件算子 `contains` / `notContains` | `_invert_cond` 只认中缀比较 → `EmitError`，整条 flow 发不出 | 发射为 `F.contains(field, value)` / `not F.contains(...)`；`_compile_condition` 对称识别该调用形态；`_NEGATE_OP` 补 contains 互转 |
| `while` 基类字段 | `_emit_while` 不调 `_common_kwargs`；编译侧 `_compile_while_for` 也不调 `_apply_base_kwargs` | 两处补齐 → `timeout` / `timeoutHandler` / `errorHandler` 往返 |
| `itemType`（集合节点） | 两条链都没有表达面 | `_emit_collection` 发 `item_type=`；`_compile_for` 双读；sexpr `:item-type` |
| HTTP `content_type` | 两条链都没有表达面 → 静默回落 `application/json` | `_call_http` / `_compile_node_call` / sexpr `:content-type` 三处对称 |
| `end` 基类字段 | `return` / `end(...)` 装不下 `timeout` / handler | `end(...)` 接受 `timeout` / `on_error` / `on_timeout`；`_emit_return` 有任一基类字段时改走 `end(...)` |
| 普通 assignment 基类字段 | `n = expr` 语句形态装不下 timeout/handler | 有基类字段时改发 `ASSIGN(...)`（与 `outputType`/`upstreamOutput` 同路径） |
| 自定义节点 `on_timeout` | `_call_custom` 发原始 dict + `on_timeout=ErrorHandler(...)`，编译器把后者当普通表达式编译并失败 | 发射侧只走 `on_timeout=`；编译侧在通用 kwarg 循环前消费 `on_timeout` |
| `case` / `switch` | `case` 不在 `_UNEMITTABLE_TYPES`，会发出编译不回去的 `CASE(...)` | 与 `switch` 同口径 fail-closed（`EmitError`） |

### `sexpr` 链

| 面 | 修前 | 修法 |
|---|---|---|
| `timeoutHandler` | 读写两侧都没有 | 读 `:on-timeout`（`(on-timeout …)` 与 `(on-error …)` 共用字段集）；写对称发射 |
| `while` | 节点表完全没有该类型 → 未知节点 | 新增 `_c_while` + 写侧 `while` 臂（`:when` / `:max-iterations` / `:child` / `-> next`） |
| `case` | `next` 不发；`value` 字符串按裸原子漂移；`outputType` 不发 | 补 `-> next`、`_data_value_to_src`、`:output-type` |
| `switch` | 分支 `label`/`desc`、节点 `outputType` 不发；`branch.next` 缺席时发成 `-> None` | 补齐三字段；`next` 仅在非 None 时发射 |
| `itemType` / `content_type` | 无表达面 | `:item-type` / `:content-type` 读+写 |
| 数据槽位字符串 | `mock.value`、`eventFilter`、`end.error`、`code`、条件 `value`、`case.target/value`、parallel 分支 `input`、handler 的 default/message 走表达式裸原子 → `"true"/"1"/"nil"` 变 bool/int/None | 全部改走 `_data_value_to_src` / `_string_to_src`（`$…` 仍按表达式） |
| 完整 Property | `_type_to_src` 只发 `dataType`，children/itemType/约束往返即丢 | 简单 `dataType` 保持裸符号字节；完整 dict 发 `(dict …)`，读侧 `_type_spec` 走 `_value()` |
| `globalContext` / `metadata` | 读侧不走 `_value()` → `(dict …)` 留成 list | `_compile_flow` 两处走 `_value()`；写侧按数据发射 |
| `errorHandler` snake 拼写 | 写侧只读 camel | `_dual(n, "errorHandler", "error_handler")` |

## 与落地席 harvest 扫掠 harness 的关系

main 在 `0f5ad6f` 已先行落地另一套 #238 实现
（`tests/unit/_dsl_roundtrip_samples.py` + `test_dsl_roundtrip_conformance.py`，
940+901 行）：harvest 现网 IR 片段 + pydantic 补齐 + sexpr/model/e2e 三表示
出口，46 类型全集扫掠、丢失路径白名单与钉值。两者互补，本提交按 main-first
保留并修正它：

- **import 修正**：main 的 `f7b5d3e` 已把 helper 路径改为
  `sys.path.insert(0, Path(__file__).parent)` 的自定位形式，本提交保留该
  方案（main-first；不再依赖 PYTHONPATH 注入）。
- **钉值环境自适应**：BASELINE 含 plaita-nodes 外部类型；未安装时它们没有
  样本。钉值只统计 `S.sample_types()` 实际 active 的类型，否则同一提交在
  「本仓 CI / 装了业务节点的本地 / 落地席环境」红绿不一致。
- **测试顺序自适应**：默认 registry 是进程级单例，同一次 pytest 会话里别的
  测试会临时注册 `codeflow_test_*` / `issue*_probe` / `code`。samples 快照
  增加「产品面」过滤（排除 `tests.*` 定义的临时类与默认不注册的 `code`），
  覆盖检查的 `want` 也在 live registry 上按同一谓词过滤——否则分块 CI /
  全量单进程的收集顺序一变就假红。
- **环境差按需构造 + 产品面过滤**：保留落地席 `1150526/a9b0186` 的
  「`want - have` 现场 `S.sample_ir(t)`」口径，以及 `46f79e4` 的
  best-effort（构造失败降 warning 跳过）；同时 `want` 先按产品面谓词过滤。
  main 的 CI 红之一即 `code: KeyError`（A-F 分块的测试模块先把 `code`
  临时注册进默认 registry，它默认是 opt-in 类型、samples 不构造）——
  过滤后该假红消失。
- **已知系统性模式**：保留落地席 `76bd3e7` 的
  `_is_known_systematic`（model 链 errorHandler/timeoutHandler 子对象丢失
  按模式归档 + ≤250 硬上限）；本提交的 BASELINE（294，active）继续按精确
  路径钉值，两者互补。
- main `76bd3e7` 的 CI 红为 `test_no_loss_outside_whitelist` 的「白名单外
  新增丢失面」——来源是测试态类型/环境差落到 `_current_losses`。本提交的
  产品面过滤把这些类型排除出扫掠，且生产修复把真实丢失收窄到 BASELINE
  内，故本分支通过。
- **同 commit 收减基线**：本轮的修复把 active 丢失路径 **441 → 294**
  （sexpr/`@flow` 的 while、contains、itemType、content_type、
  timeoutHandler、end/assignment 基类字段等）。按该测试自己的规则
  （「修复减员须同 commit 收白名单」）已更新 BASELINE；timeoutHandler 从
  「整字段丢」变成 e2e/model 链只丢 `errorCode`/`errorMessage`（模型
  `model_dump` 别名面，属它已登记的系统性模式），按新路径登记。
- 两套 harness 同时进 ci-gate `conformance` 相位与 ci.yml 具名步骤。

## 已知不可表达项（白名单，均有原因说明）

- **@flow**：`switch` / `case` 整类型（无语句形态）；`start` / `if` 的
  `timeout` / handler（语句位不存在）；命名引用
  `reference.flow_id` / `flow_version`（调度器注入形状）；根流程 / 子流程
  `outputType`（`@flow`/`@childflow` 显式废弃并忽略）；集合/while 内联子流程的
  `flow_id` / `desc`（无装饰器位）。
- **两链**：parallel 分支的 `priority/label/desc/is_default/next`（Parallel
  执行不消费）；非输出承载节点的 `output`、集合节点的 `input`（execute 不读）；
  `end.next`（终态边）；`name/desc/source_line`（派生注解）；
  `if`/`case.branches`（validator 合成）。

## 负控

见 [`data/238-negative-control.txt`](data/238-negative-control.txt)：5 条负控
全绿，分别证明 `while` 基类字段、HTTP `content_type`、sexpr
`:on-timeout`、contains 算子映射、registry 新字段检测都有区分力（拆掉修复
即红）。

## 验收

| 面 | 命令 | 结果 |
|---|---|---|
| conformance 本体（两套） | `pytest tests/unit/test_issue238_dsl_conformance.py tests/unit/test_dsl_roundtrip_conformance.py -q` | 60 passed（56 + 落地席 harvest 4） |
| DSL 相关全量（含两套 conformance） | `pytest tests/unit/test_codeflow*.py tests/unit/test_sexpr*.py tests/unit/test_dsl.py tests/unit/test_builder*.py tests/unit/test_issue*.py tests/unit/test_issue238_dsl_conformance.py tests/unit/test_dsl_roundtrip_conformance.py` | 4023 passed / 4 skipped / 2386 subtests |
| integration + e2e（CI 口径 `-m "not slow"`） | `pytest tests/integration tests/e2e -m "not slow" -q` | 287 passed / 4 skipped |
| ci-gate 相位 | `ci-gate.sh conformance` / `layering` / `wheel` / `size` | 四相位全绿（conformance 56 passed；wheel 无 test 文件；SC-003 hard ceiling 满足） |
| 全量（CI 口径，分块） | `pytest tests/unit -m "not slow"` + `pytest tests/integration tests/e2e -m "not slow"` | unit **8074 passed / 15 skipped / 14 deselected**（3492 subtests）；integration+e2e **287 passed / 4 skipped**；合计 **8361 passed / 19 skipped / 14 deselected**，两段 rc=0 |
| 文档 | `mkdocs build -f docs-site/mkdocs.yml --strict` | rc=0 |
| 落地门禁 | `python scripts/landing_gate.py ci` | `8740485` 时绿；最新 `a9b0186` **红**，失败点正是本提交修复的 `test_dsl_roundtrip_conformance.py::test_all_registry_types_have_samples`（`code: KeyError`）——本提交落地后 main 才会转绿（fail-closed：不绕过红灯） |

> 说明：本地 `ci-gate.sh pytest` 无 `-m "not slow"`，会把 slow 标记的
> integration/e2e 也拉进来（本机 macOS 下 >20min 未收敛）；CI 的实际口径是
> 分块 `-m "not slow"`。上表用同一口径跑全量，另单跑 integration+e2e
> 交叉确认。

