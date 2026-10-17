# RED-PROOF — #133 属性遍历只允许数据（代码对象按真实类型具名拒绝）

日期：2026-10-18 · 执行者：修复落地席（jeffkit）· 基线 `f603970` · 单 commit（本文件随修复入库）
工作区：`/Users/kong/projects/infra4agent/plaita`

## §0 结论速览

| 验收项 | 结果 |
|---|---|
| 载体 | `patch-133-data-only-traversal.patch` **缺失**：gist `501029c5` 两版、#133 交付帖（comment `6235415328`）、#156 都只有文件名/期望树引用，无可下载文件；`089689af…` 不是有效 git 对象。按 issue 规格由修复席实现（见 §1） |
| 主链 `$NODE.h.os.environ.HOME` | 修复前返回活的 `os.environ['HOME']`；修复后**具名 `ValueError`**，在 `.os` 就早停，环境变量未被读取 |
| 代码对象 | module / function / builtin function / method / class / frame / traceback / generator / coroutine / async generator / code 全部在**值位置**与**基座位置**被拒（12 类 × 2 位置） |
| 数据正控 | dataclass / 普通类 / Pydantic / SimpleNamespace / dict / list 索引、`$F.*`、`$ENV` allowlist 全部不变 |
| 负控（变异） | 同时关掉入口闸与结果闸 ⇒ 新文件 **42 failed / 34 passed**、#55 契约文件 **1 failed / 83 passed**；数据正控在变异下仍绿（杀的是安全断言，不是测试本身） |
| 验收面 | e2e **74 passed / 3 skipped**；integration **213 passed / 1 skipped**；`ci-gate.sh` **PASSED**（`pytest tests/` **8005 passed / 15 skipped / 3429 subtests**，覆盖率 **93.86%**）；`landing_gate.py ci` **success** |
| 反向证明 | 修复前/后探针逐表达式对照见 §3；机读旁证 `docs/proofs/data/133-before-after.json` |

## §1 载体与实现依据

- 首轮已核实：gist `501029c5` 的 `EXPECTED.tree.branch` / `README.md` / `consolidate-cat1.patch`
  是全部文件；okguitar 其他 gist（`4339c5` / `13af8f92` / `6b33bd69`）只有 #55 或
  store 链，没有 245 行修复本体；本机、VM、残留 worktree 均无
  `_REJECTED_TRAVERSAL_TYPES` 实现。
- 依据 #133 正文的「期望修法」与 comment `6235415328` 的最终共识（对象种类白名单，
  不是属性名黑名单），本实现对齐以下口径：
  - 判据在唯一收口点 `_get_attr`；`$ENV` allowlist、`$F.*` 注册表、`{% %}` 语义、
    映射/索引路径不改；
  - 判据用**真实类型** `type(obj).__mro__`，成员判定用 `is`（#163/#170 F1：
    元类 `__eq__` 可以让 `in` 说谎）；
  - 错误是具名 `ValueError`，含完整路径、真实类型与 `plaita#133`，不回显值；
  - 三处契约同步改写（公有链测试、文案测试、文档逐字引用），不删断言。

## §2 实现

`plaita/core/expression_parser.py`

```python
_REJECTED_TRAVERSAL_TYPES = (
    types.ModuleType, types.FunctionType, types.BuiltinFunctionType,
    types.MethodType, types.CodeType, types.FrameType, types.TracebackType,
    types.GeneratorType, types.CoroutineType, types.AsyncGeneratorType,
    type,  # class objects
)
```

- `_is_code_object(obj)`：`type(obj).__mro__` 与上表做**身份**比较；
- `_get_attr(obj, path, *, path_hint=None)`：
  - **入口闸**：`_is_code_object(obj)` ⇒ 拒绝（基座是模块/函数/类/… 时任何属性名都拒绝）；
  - **结果闸**：属性路径 `getattr` 得到的值若 `_is_code_object(value)` ⇒ 拒绝
    （`$NODE.h.os` 本身即抛错，不先返回模块）；
  - `path_hint=(root_key, walked, upto)` 只在拒绝时 `_render_path`，热路径不拼字符串；
  - 映射/索引路径保持原语义（`os.environ` 作为**容器**按键读取仍是数据面）。

`_TRAVERSAL_REJECTION_TEMPLATE` 的运行时文案（逐字，见
`MIGRATION.md` 与 `docs-site/docs/guide/expressions.md`）：

```
expression path '$NODE.h.os' traverses a non-data object of type 'module': attribute traversal is data-only (plaita#133), so modules, functions, builtin functions, methods, classes, frames, tracebacks, generators, coroutines, async generators and code objects are refused by name. Read a data field instead; mapping keys and numeric indices are unchanged.
```

#55 的下划线文案同步改为不再声称「Only this prefix is gated / 公有属性按设计可读」，
因为 #133 之后这句话不再成立（见 `test_rejection_message_makes_no_sandbox_promise`）。

## §3 修复前 / 修复后（同一探针，逐表达式）

探针：`docs/proofs/data/133-repro.py`。修复前在干净 worktree `f603970` 上跑
（`PYTHONPATH=/tmp/wt133-before`），修复后在本树跑；两边都用
`plaita.__file__` 核过（机读结果 `docs/proofs/data/133-before-after.json`）。

| 表达式 | 修复前 | 修复后 |
|---|---|---|
| `$NODE.h.text` | `"hello"` | `"hello"` |
| `$NODE.d.k` | `"v"` | `"v"` |
| `$NODE.payload.text` | `"hello"` | `"hello"` |
| `$INPUT.payload.text` | `"hello"` | `"hello"` |
| `$NODE.h.os` | `module` | **`ValueError` #133** |
| `$NODE.h.os.environ` | `_Environ` | **`ValueError` #133**（`.os` 早停） |
| `$NODE.h.os.environ.HOME` | `str`，`same_as_home=true`（活的进程环境值） | **`ValueError` #133**（环境变量未被读取） |
| `$NODE.h.mod.environ` | `_Environ` | **`ValueError` #133**（基座是 module） |
| `$NODE.h.fn.x` | `None`（静默） | **`ValueError` #133**（基座是 function） |
| `$NODE.h.cls.some_attr` | `TypeError`（裸异常） | **`ValueError` #133**（基座是 class） |
| `$NODE.h.gen.gi_frame` / `coro.cr_frame` / `agen.ag_frame` | `None`（静默） | **`ValueError` #133** |
| `$NODE.h.frame.f_locals` / `tb.tb_next` / `code.co_consts` | `None`（静默） | **`ValueError` #133** |
| `$F.get($NODE.d, 'k')` | `"v"` | `"v"`（`$F.*` 未改） |
| `$NODE.h.env` / `$NODE.h.env.HOME` / `$NODE.d.env.HOME` | `_Environ` / `str` | **仍可读**（相邻同族残面，见 §6；不在本 commit） |

修复后的错误示例：

```
ValueError: expression path '$NODE.h.os' traverses a non-data object of type 'module':
attribute traversal is data-only (plaita#133), so modules, functions, builtin functions,
methods, classes, frames, tracebacks, generators, coroutines, async generators and code
objects are refused by name. Read a data field instead; mapping keys and numeric indices
are unchanged.
```

## §4 验收

### §4.1 定向契约测试

```
pytest tests/unit/test_issue133_data_only_traversal.py \
       tests/unit/test_issue55_attr_traversal.py \
       tests/unit/test_expression_size_guard.py::TestTwelfthRoundH3RenderSiteInventory -q
⇒ 169 passed, 13 subtests passed
```

覆盖：12 类代码对象的 `_is_code_object` / 基座位置 / 值位置 / 表达式基座位置；
四根对称（`$NODE` / `$INPUT` / `$GLOBAL` / `$PARENT.$INPUT`）；数据正控
（dataclass / 普通类 / Pydantic / SimpleNamespace / dict / list / `$F.*`）；
完整路径/真实类型/不回显；`type(obj).__mro__` 不读 `__class__`；
元类 `__eq__` 不影响判定；端到端 Flow（`NodeExecutionError`，`__cause__` 为
`ValueError`，秘密值不出现在消息中）。

### §4.2 e2e

```
pytest tests/e2e -q  ⇒ 74 passed, 3 skipped
```

### §4.3 integration

```
pytest tests/integration -q  ⇒ 213 passed, 1 skipped
```

### §4.4 unit 与本地门禁

- 全量 unit（含 slow）：**7715 passed / 2 failed**——两条失败都是
  `TestTwelfthRoundH3RenderSiteInventory`：新增的
  `_TRAVERSAL_REJECTION_TEMPLATE.format(...)` 渲染点未登记。补登记后该 class
  **9 passed / 13 subtests**；不是产品行为失败。
- `landing_gate.py ci`：`origin/main` 最近完成 run `f603970` **success**，放行。
- `ci-gate.sh`：**PASSED**。`pytest tests/` ⇒ **8005 passed / 15 skipped /
  3429 subtests**（120 warnings），覆盖率 **93.86%**（阈值 80%）；import
  layering 3 passed；wheel 无 `test_*.py`；SC-003 类尺寸硬顶满足。

## §5 负控（变异纪律）

在 `b3216c6` 的临时 worktree 中同时关掉两处闸：

```diff
-    if _is_code_object(obj):
+    if False:  # MUTANT: entry gate disabled
         _reject_non_data_traversal(path, obj, path_hint)
...
-        if _is_code_object(value):
+        if False:  # MUTANT: result gate disabled
             _reject_non_data_traversal(path, value, path_hint)
```

结果：

```
tests/unit/test_issue133_data_only_traversal.py  ⇒ 42 failed, 34 passed
tests/unit/test_issue55_attr_traversal.py        ⇒  1 failed, 83 passed
```

- 42 条红覆盖每一种代码对象的值位置/基座位置、四根、完整路径、文案、端到端；
  1 条红是改写后的 `test_public_attribute_traversal_is_data_only`。
- **数据正控在变异下仍绿**（34 passed 含 dataclass/Pydantic/dict/list/`$F.*`），
  说明杀的是安全断言，不是测试自身。
- 变异 diff 与完整红名单：`docs/proofs/data/133-negative-control.diff`、
  `docs/proofs/data/133-negative-control.txt`；SHA256 见
  `docs/proofs/data/133-before-after.json`。

## §6 相邻同族残面（不在本 commit）

以下通路在 #133 的**属性遍历**闸门之外，仍可读环境变量：

```
$NODE.h.env.HOME      # h = SimpleNamespace(env=os.environ) ⇒ '/Users/...'
$NODE.d.env.HOME      # d = {"env": os.environ}              ⇒ '/Users/...'
```

原因：`os.environ`（`_Environ`）是 mapping-like 容器，`_get_attr` 走映射路径
（键不是属性），#133 的代码对象闸不覆盖它；issue 正文明确「映射/索引路径
保持不变」。按边界另开新单（映射值/容器的数据类型判定），不混入本 commit。
本 commit 的保证是：**属性遍历**不再能把 module/function/class/... 当值或当基座，
因此 `$NODE.h.os.environ.HOME` 在 `.os` 就具名拒绝。

## §7 复跑

```bash
# 修复前/后探针
PYTHONPATH=/tmp/wt133-before .venv/bin/python docs/proofs/data/133-repro.py  # 基线 worktree
PYTHONPATH=.                .venv/bin/python docs/proofs/data/133-repro.py  # 修复后

# 定向
.venv/bin/python -m pytest tests/unit/test_issue133_data_only_traversal.py \
    tests/unit/test_issue55_attr_traversal.py -q

# 验收面
.venv/bin/python -m pytest tests/e2e -q
.venv/bin/python -m pytest tests/integration -q
bash scripts/ci-gate.sh
python3 scripts/landing_gate.py ci
```

## §8 变更文件

- `plaita/core/expression_parser.py`：对象种类闸（入口 + 结果）+ 文案；
- `tests/unit/test_issue133_data_only_traversal.py`：新增性质/正控/端到端测试；
- `tests/unit/test_issue55_attr_traversal.py`：三处契约改写（公有链、文案、文档）；
- `tests/unit/test_expression_size_guard.py`：登记新渲染点；
- `MIGRATION.md` / `docs-site/docs/guide/expressions.md`：规则、代价、逐字文案；
- `docs/proofs/data/133-repro.py` / `133-before-after.json` /
  `133-negative-control.diff` / `133-negative-control.txt`：本证明的机读旁证。
