# RED-PROOF — #203 映射容器只允许数据（映射值面 + `$F.get` 旁路）

日期：2026-10-18 · 执行者：修复落地席（jeffkit）· 基线 `d66db5d` · 单 commit
工作区：`/Users/kong/projects/infra4agent/plaita`

## §0 结论速览

| 验收项 | 结果 |
|---|---|
| 容器作值 / 作基座 | `$NODE.h.env` / `$NODE.d.env` 不再返回 live `_Environ`；`.HOME` 在读取前具名 `ValueError`（plaita#203） |
| `$F.get` 旁路 | `$F.get($NODE.h.env, 'HOME')` / `$F.getDictValue(...)` 拒绝；host 函数返回 mapping-like 时在输入测量/callee 之前拒绝 |
| host mapping-like 覆盖 | `os._Environ` / `ChainMap` / `UserDict` / `MappingProxyType` / 自定义 `Mapping` / duck-typed `__getitem__`+`get` 全部覆盖 |
| 零调用 | poisoned `Mapping` 的 `__getitem__`/`get` 计数在 `$NODE.pm.KEY` / `$NODE.d.pm` / `$F.get($NODE.d, 'pm')` 上 **1/1/2 → 0/0/0** |
| 数据正控 | plain dict、只加方法的 dict 子类、`CheckpointState`/`_LazyRootSnapshot`、dataclass/Pydantic/SimpleNamespace、`$ENV` allowlist 全不变 |
| 负控（A：关 #203 闸） | #203 文件 **13 failed / 6 passed**（6 个数据/文档正控仍绿）；#133 76 passed；#55 84 passed |
| 负控（B：关 #133 闸） | #133 文件 **41 failed / 35 passed**（与历史 42/34 的差异仅 `test_error_does_not_echo_values` 被 #203 结果闸独立兜住）；#203 19 passed |
| 验收面 | unit 快车道 **7724 passed / 3429 subtests**；e2e **74 passed / 3 skipped**；integration **213 passed / 1 skipped**；`ci-gate.sh` **PASSED**（8024 passed / 覆盖率 93.82%）；`landing_gate.py ci` **success** |

## §1 问题与规格

A 席对抗复核（#203 comment `6237466319`）确认两条入口：

1. **映射值面**：`$NODE.h.env` / `$NODE.d.env` 返回 live `os._Environ`，
   `.HOME` 走映射键读取；
2. **`$F.get` 旁路**：`$F.get($NODE.h.env, 'HOME')` 直接调 `_Environ.get`；
   实测 `ChainMap` / `UserDict` / 自定义 `Mapping` 同样可达。

#133 只关闭**属性**遍历（`$NODE.h.os.environ`），不是漏测：`_get_attr` 的映射
分支（`dict` 或 `__getitem__` + `get`）与 `$F.get` 都没有对**容器本身**做类型
判定。本单按 A 席验收 1–7 实现，不扩大 #133 的属性面。

## §2 实现

### §2.1 数据映射谓词（`plaita/core/_data_mapping.py`）

- **允许**：真实 `dict`（含子类，`type(value).__mro__` 身份比较）与引擎自有
  mapping（`CheckpointState` / `_LazyRootSnapshot`，精确类型、身份比较）；
- **拒绝**：其余 mapping-like——`os._Environ`、`ChainMap`、`UserDict`、
  `MappingProxyType`、自定义 `Mapping`、duck-typed `__getitem__` + `get` 的
  host 对象；
- 判据不读 `__class__`（#163 纪律）；`is_mapping_like` 只做
  `isinstance(Mapping)` / `hasattr(__getitem__)` / `hasattr(get)` 探测，
  **从不调用** `__getitem__`/`get`；探测失败 fail-closed。

### §2.2 四处闸 + `$F.get`

- `_get_attr` **入口**（基座）、**映射/属性结果值**、**根值**、**索引结果**；
- `$F.get` / `$F.getDictValue`：
  - 解析器在 **#68 输入测量与 callee 之前**检查第一个实参（host mapping-like
    ⇒ 具名拒绝）；
  - 数据 dict / 引擎状态先做一次**安全预览**（`dict.get` C 槽 / 引擎 `get`），
    返回值是 host mapping-like 时在测量之前拒绝；
  - `_safe_mapping_get` 内部再判一次（直调也安全），返回位置同样判；
  - 真实 dict 一律走 `dict.get` C 槽：子类覆写的 `get`/`__getitem__`/
    `__getattribute__` **零调用**（与 #69 r13 的 `$F.keys`/`values`/`items`
    同一纪律）。
- `$F.keys` / `values` / `items` / `getDictKeys` / `getDictValues` 对非 dict
  容器沿用 #69 的具名拒绝（同一结果，文案不同）。

运行时文案（**逐字**，见 `MIGRATION.md` 与表达式指南）：

```
expression path '$NODE.h.env' read a non-data mapping container of type '_Environ': only plain dict (including subclasses, read through dict's C slots) and the engine's CheckpointState may be traversed as a mapping (plaita#203). Host mapping-like objects such as os.environ / ChainMap / UserDict / custom Mapping are refused by name before any key or value is read; convert the data to a plain dict first.
```

## §3 修复前 / 修复后（同一探针）

探针：`docs/proofs/data/203-repro.py`。修复前在干净 worktree `d66db5d` 跑
（`PYTHONPATH=/tmp/wt203-before`），修复后在本树跑；两边都用 `plaita.__file__`
核过。机读结果：`docs/proofs/data/203-before-after.json`。

| 表达式 | 修复前 | 修复后 |
|---|---|---|
| `$NODE.d.k` / `$INPUT.x` / `$F.get($NODE.d,'k')` | 数据值 | 数据值（不变） |
| `$NODE.h.env` / `$NODE.d.env` | `_Environ` | **`ValueError` #203** |
| `$NODE.h.env.HOME` / `$NODE.d.env.HOME` | `str`，`same_as_home=true` | **`ValueError` #203**（未读环境变量） |
| `$NODE.cm.SECRET` / `$NODE.ud.SECRET` / `$NODE.mproxy.SECRET` | `s1` / `s2` / `s3` | **`ValueError` #203** |
| `$NODE.lst[0]` / `$NODE.lst[0].HOME` | `_Environ` / `same_as_home` | **`ValueError` #203** |
| `$F.get($NODE.h.env,'HOME')` / `$F.getDictValue(...)` | `same_as_home` | **`ValueError` #203** |
| `$F.get($NODE.cm,'SECRET')` | `s1` | **`ValueError` #203** |
| `$F.get($NODE.d,'env')`（数据 dict 里的 live 容器） | `_Environ` | **`ValueError` #203** |
| `$F.get($F.probe_env(),'HOME')`（host 函数返回容器） | `same_as_home` | **`ValueError` #203**（`$F.get(...)` 预检） |
| poisoned `$NODE.pm.SECRET` / `$NODE.d.pm` / `$F.get($NODE.d,'pm')` | 读 1/1/2 次 | **拒绝，读 0/0/0 次** |
| `$INPUT.x` / `$F.get($INPUT,'x')`（`CheckpointState`） | `input-x` | `input-x`（不变） |
| `$ENV.HOME`（空 / 显式） | `None` / `exposed-home` | `None` / `exposed-home`（不变） |
| 覆写 `get` 的 dict 子类 `$NODE.od.k` / `$F.get($NODE.od,'k')` | `OVERRIDE`（读 1/2 次） | **`real`**（覆写 0 次；C 槽真实存储） |

修复后的错误示例：

```
ValueError: expression path '$NODE.h.env' read a non-data mapping container of type
'_Environ': only plain dict (including subclasses, read through dict's C slots) and the
engine's CheckpointState may be traversed as a mapping (plaita#203). Host mapping-like
objects such as os.environ / ChainMap / UserDict / custom Mapping are refused by name
before any key or value is read; convert the data to a plain dict first.
```

## §4 验收

### §4.1 定向

```
pytest tests/unit/test_issue203_data_mapping_traversal.py \
       tests/unit/test_issue133_data_only_traversal.py \
       tests/unit/test_issue55_attr_traversal.py \
       tests/unit/test_expression_size_guard.py::TestTwelfthRoundH3RenderSiteInventory -q
⇒ 188 passed, 13 subtests passed
```

覆盖：容器作值/基座/根值/索引；`$F.get` 两处（首参 + 返回值）；host function
返回容器；poisoned mapping 零调用；`ChainMap`/`UserDict`/`MappingProxyType`/
自定义 `Mapping`/duck-type；数据正控（dataclass/Pydantic/SimpleNamespace/
plain dict/只加方法的 dict 子类/`CheckpointState`）；`$ENV`；覆写 `get` 的
dict 子类零调用；端到端 Flow（`NodeExecutionError`，秘密值不出现在消息）。

### §4.2 unit 快车道

```
pytest tests/unit -m "not slow" -q ⇒ 7724 passed, 11 skipped, 13 deselected,
3429 subtests passed
```

### §4.3 e2e / integration

```
pytest tests/e2e -q          ⇒ 74 passed, 3 skipped
pytest tests/integration -q  ⇒ 213 passed, 1 skipped
```

### §4.4 本地门禁与 landing gate

- `ci-gate.sh`：**PASSED**。`pytest tests/` ⇒ **8024 passed / 15 skipped /
  3429 subtests**（120 warnings），覆盖率 **93.82%**（阈值 80%）；import
  layering 3 passed；wheel 无 `test_*.py`；SC-003 类尺寸硬顶满足。
- `landing_gate.py ci`：**success**——`origin/main` 最近完成 run `d66db5d`
  conclusion=success，放行。

## §5 负控（变异纪律）

### §5.1 A — 关掉 #203 容器闸（`is_mapping_like` 恒 `False`）

```
tests/unit/test_issue203_data_mapping_traversal.py ⇒ 13 failed, 6 passed
tests/unit/test_issue133_data_only_traversal.py    ⇒ 76 passed
tests/unit/test_issue55_attr_traversal.py          ⇒ 84 passed
```

- 13 红是全部安全断言（容器/ roots/索引/`$F.get`/poisoned/文案/端到端）；
- 6 绿是**数据/文档正控**：`test_data_mappings_stay_readable`、
  `test_checkpoint_state_backed_input_stays_readable`、
  `test_dict_subclass_overrides_are_not_executed`、
  `test_env_allowlist_semantics_unchanged`、
  `test_flow_data_attribute_still_works`、
  `test_docs_quote_issue203_message_verbatim` ⇒ 杀的是安全断言，不是测试自身；
- #133/#55 全绿 ⇒ #203 闸关掉不影响属性面契约。

### §5.2 B — 关掉 #133 代码对象闸（入口 + 结果）

```
tests/unit/test_issue133_data_only_traversal.py ⇒ 41 failed, 35 passed
tests/unit/test_issue203_data_mapping_traversal.py ⇒ 19 passed
```

- #133 红名单与历史 42/34 **逐条相同**，唯一差异是
  `test_error_does_not_echo_values` 现在绿：即使 #133 闸关掉，`$NODE.h.os.environ`
  的结果值 `_Environ` 也被 #203 的结果闸独立具名拒绝——这是纵深防御，不是
  判据放宽；
- #203 全绿 ⇒ #203 闸不依赖 #133 的代码对象闸，两闸独立承重。

变异 diff 与完整红名单：`docs/proofs/data/203-negative-control-container.diff`、
`203-negative-control-code-object.diff`、`203-negative-control.txt`；
SHA256 见 `docs/proofs/data/203-before-after.json`。

## §6 兼容性裁决

- **真实 dict（含子类）**：保持可读；读**真实存储**（`dict.get` / `dict.keys`
  C 槽）。子类覆写的 `get`/`__getitem__`/`__getattribute__` 不再执行——与
  #69 r13 对 `$F.keys`/`values`/`items` 的纪律一致；只加方法的子类不受影响。
- **`CheckpointState` / `_LazyRootSnapshot`**：引擎自有，精确类型允许。
  `_LazyRootSnapshot` 是 `$PARENT` 的内部惰性快照（嵌套 loop / inline flow），
  不是用户容器。
- **`UserDict` / `ChainMap` / `MappingProxyType` / 自定义 `Mapping`**：不再能
  作为表达式映射容器读取；请先转 plain dict（`dict(value)`），或改用 `$ENV`
  显式暴露。`UserDict` 等若作为**普通值**返回（不做映射键读取）仍可被节点
  使用；被表达式读取时具名拒绝。
- **`$F.get` 的非 mapping 对象**：只实现 `get`、没有 `__getitem__` 的普通对象
  保持历史 `a.get` 行为；一旦同时有 `__getitem__`，就按 mapping-like 处理。
- **#163/#69 路径面**：host mapping-like 形状现在更早被 #203 具名拒绝；直调
  `_probe_isinstance` / `_engine_mapping_view` 的 #69 契约测试保持不变，路径面
  测试接受更早的 #203 具名拒绝（两者都是 `ValueError`，都不回显宿主文本）。

## §7 复跑

```bash
# 修复前/后探针
PYTHONPATH=/tmp/wt203-before .venv/bin/python docs/proofs/data/203-repro.py
PYTHONPATH=.                .venv/bin/python docs/proofs/data/203-repro.py

# 定向
.venv/bin/python -m pytest tests/unit/test_issue203_data_mapping_traversal.py \
    tests/unit/test_issue133_data_only_traversal.py \
    tests/unit/test_issue55_attr_traversal.py -q

# 验收面
.venv/bin/python -m pytest tests/unit -m "not slow" -q
.venv/bin/python -m pytest tests/e2e -q
.venv/bin/python -m pytest tests/integration -q
bash scripts/ci-gate.sh
python3 scripts/landing_gate.py ci
```

## §8 变更文件

- `plaita/core/_data_mapping.py`：数据映射谓词 + 具名文案；
- `plaita/core/expression_parser.py`：入口/结果/根/索引闸、`$F.get` 预检、
  `_mapping_get` 的 dict C 槽读取；
- `plaita/core/expression.py`：`_safe_mapping_get`（`$F.get`/`$F.getDictValue`
  首参 + 返回值闸）；
- `tests/unit/test_issue203_data_mapping_traversal.py`：新增性质/零调用/兼容性/
  端到端测试；
- `tests/unit/test_issue55_attr_traversal.py` / `test_expression_parser_mutations.py` /
  `test_issue163_*` / `test_issue69_r13_*` / `test_no_silent_swallow.py` /
  `test_expression_size_guard.py`：契约与登记同步；
- `MIGRATION.md` / `docs-site/docs/guide/expressions.md`：规则、兼容性、逐字文案；
- `docs/proofs/data/203-repro.py` / `203-before-after.json` /
  `203-negative-control*.{diff,txt}`：机读旁证。
