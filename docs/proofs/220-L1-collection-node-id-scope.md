# #220-L1 独立回归：@flow 集合节点 id 作用域

- 单号：plaita#220（tracking，7 面中的高 3 面之 L1）
- 发现基线：`origin/main` = `f83a268`（2026-10-18；5/5 集合节点 + 命名 WHILE 均复现）
- 上游落地：施工期间 W43 六面包以 `9c8820a`（`#219 + #220 剩余六面`）进入 main，
  其中已含与本席独立实现**逐字同义**的 L1 修复
- 本提交范围：独立回归测试（13 用例）+ 本证明 + `docs-site` 文档注记；
  **不改实现**——实现以 `9c8820a` 为准，本席不重复造第二条修复
- 边界：不改 `_emit.py` 发射形态、不改 sexpr 链、不改集合/While 运行时语义

## 缺陷（基线 `f83a268`）

`emit_source` 把下游 `$NODE.<id>` 发射成**裸变量名**（`_invert_value` 把
`$NODE.` 前缀逆映射为空）：

```python
@flow('l1probe')
def l1probe(INPUT):
    for item in MAP(INPUT.items, id='agg'):
        return item
    return agg
```

集合节点在源码里是 `for item in MAP(..., id='agg')`，没有赋值语句可承接 id；
`_compile_for` 当时未登记 `ctx.names`，重编译最后的 `return agg` 落到
`_resolve_name` 的未知名字分支。5 类集合节点全红，命名 WHILE 的 for-head
（`for item in WHILE(cond, id="w")`）同根因：

```
map/filter/find/loop/reduce  _CodeflowError: [codeflow] 第 5 行: 未知名字 'm'
while                        _CodeflowError: [codeflow] 第 5 行: 未知名字 'w'
```

不引用该 id 时能编译——因此这是「往返即丢」而非集合节点不可用；sexpr 链
一直正常（IR 直接携带 `$NODE.<id>`，不走名字作用域）。

## 上游修复（`9c8820a`）

`_stmt.py` 两处，均把节点 id 登记为 `$NODE.<id>`：

- `_compile_for`（集合节点）与 `_compile_while_for`（命名 WHILE）在子流程
  编译**之后**、后续语句 `_compile_block(rest, ...)` **之前**登记。

登记位置是语义要求：循环体是子流程，体内读不到本节点结果（父侧尚未写回）；
放在 child 作用域构造之后，id 不漏进体的 `$PARENT` 快照；放在 `rest` 之前，
下游裸名才能解析。发射器/sexpr 无需改动。

本席在 `9c8820a` 树上验证：

- 本文件的 13 用例 **13 passed**；
- `docs/proofs/data/220-repro.py`（`PYTHONPATH` 指向 main 树）**all_ok = true**
  （L1/L2/L4/L5/L6 全 true）；
- 上游已有 `tests/unit/test_issue220_dsl_roundtrip_loss.py::TestL1CollectionNodeId`
  覆盖 5 类 + WHILE，与本席独立用例互为交叉验证。

## 本提交补的独立回归价值

上游用例覆盖「往返后 `$NODE.<id>` 相等」；本席另钉住：

- 发射器实际发裸名（`return agg`）——修复点必须在编译侧；
- 手写源码直编译（不经发射器）的裸名同样解析为 `$NODE.agg`；
- 作用域边界：循环体引用自身 id 仍按 `未知名字` 拒绝（集合 + WHILE）；
- 护栏：未登记裸名（`missing`）仍拒绝，修复不放宽未知名字解析；
- 正控：显式 `NODE.agg` 与 sexpr 链基线即绿；
- 运行时：重编译后的 MAP 下游真实拿到 `[1, 2, 3]`，不只 IR 文本相等。

结果：**13 passed**。

## 负控（还原上游 `9c8820a` 的两处登记）

在 `9c8820a` 树上仅删去 `_compile_for` / `_compile_while_for` 的
`ctx.names[node_id] = f"$NODE.{node_id}"` 两行（其余不动）：

- **8 failed / 5 passed**；绿的 5 条正是作用域边界 ×2、未登记裸名护栏、
  显式 `NODE.` 正控、sexpr 正控——都不依赖 id 登记，符合预期。
- 精确红文案（全文 `docs/proofs/data/220-L1-negative-control.txt`）：

```
E  _CodeflowError: [codeflow] 第 5 行: 未知名字 'agg'：可用 INPUT/NODE/GLOBAL/PARENT/ENV/F 或已赋值的节点变量
```

## 回归面（最终提交树 = `9c8820a` + 测试/文档）

| 套件 | 结果 |
|------|------|
| 本文件（在 main 树上） | **13 passed** |
| unit 快车道（`tests/unit -m "not slow"`） | **7887 passed / 16 skipped / 3434 subtests** |
| `tests/e2e` | **74 passed / 3 skipped** |
| `tests/integration` | **213 passed / 1 skipped** |
| codeflow 相关子集（emit/loop-scope/while/canonical/命名/子作用域） | **147 passed** |
| 既有往返面（#136/#200/#207 + sexpr 扩展/覆盖 + #220-L3 + W43 六面） | **198 passed / 5 subtests** |
| `mkdocs build -f docs-site/mkdocs.yml --strict` | 通过 |
| `landing_gate.py ci` | **success**（main `9c8820a`） |

## 文档同步

`docs-site/docs/guide/code-dsl.md` 的「集合节点 · 作用域」补一句：循环之后
集合节点 `id=` 进入名字作用域，`return NODE.dbl` 可直接写 `return dbl`；
`emit_source` 发射下游 `$NODE.<id>` 用的正是裸名形态，往返依赖它解析回同一
引用（命名 WHILE 同理）。
