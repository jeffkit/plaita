# #220-L3 证明：sexpr 往返丢失 `CodeNode.sandbox_backend`

- 单号：plaita#220（tracking，7 面中的高 3 面之 L3）
- 基线：`origin/main` = `574e5e6`（2026-10-18；发现时点）
- 施工范围：`plaita/dsl/sexpr.py`（读侧 `_c_code` + 写侧 `_node_to_src` 的 code 分支）、
  新增测试、本证明、`docs-site/docs/guide/code-dsl.md` 同步
- 边界：不改 `plaita/node/code.py` 的运行时语义；不触碰 L1/L2/L4-L6 与
  B1/B2 在途文件（console `cluster.py`、event/runner）

## 缺陷

`CodeNode.sandbox_backend` 是逐节点的沙箱档位（`plaita/node/code.py:959`）。
`CodeNode.execute` 直接读该属性选择隔离实现；字段缺席时回退进程级默认
（0.5.0 起 `docker`）。sexpr 的读写两侧此前都不认它：

- 写侧 `_node_to_src` 的 `code` 分支只发 `:lang` / `:code` / `:input` / `-> next`；
- 读侧 `_c_code` 只读 `:lang` / `:code` / `:input`。

因此 `flow_to_sexpr` → `compile_sexpr` 往返把 `subprocess` / `restricted` /
`unsafe` **静默洗回默认 `docker`**——隔离档位被无声降级（安全字段丢失）。
`@flow` 侧原本就保留（`_emit.py::_call_code` 透传所有字段）。

复现（基线 `574e5e6`）：

```
(flow f-l3 (start :id s -> n) (code :id n :lang python :code "print(1)" :input $INPUT -> e) (end :id e))
NODE: {'type': 'code', ..., 'input': '$INPUT'}
lost: None   # 原 IR 为 sandbox_backend="subprocess"
```

## 修复

- 写侧：读 `sandboxBackend` / `sandbox_backend`（camel/snake 双读，与 #200/#207
  同口径）；字段携带时恒以**引号字符串**发射 `:sandbox-backend "..."`。
  `_string_to_src` 保证 `1` / `true` / `nil` 这类值读回仍是字符串，不退化成
  int/bool/None。字段缺席时不发明字节（保留「不设 = 用进程默认」语义）。
- 读侧：接受 `:sandbox-backend` / `:sandbox_backend` / `:sandboxBackend`；
  值必须是**非空字符串字面量**（`CodeNode.execute` 不对该字段做表达式求值）。
- fail-closed：两侧对非字符串 / 空串 / 容器值 **响亮拒绝**（写侧
  `SexprEmitError`、读侧 `SyntaxError`），绝不静默回退默认档位。

## 验收

新增 `tests/unit/test_issue220_sexpr_code_sandbox.py`（24 用例）：

- 4 个合法档位（`docker`/`subprocess`/`restricted`/`unsafe`）全字段往返逐项相等，
  并断言 `CodeNode(**node).sandbox_backend` 真实生效（不只留在 IR dict）；
- 4 个字符串样值（`1`/`true`/`42`/`nil`）保持 `str` 类型；
- camel IR（`sandboxBackend`）发射；读侧 3 个关键字别名；
- 缺席不发明字节；`@flow` → sexpr 跨前端链路保真；
- 写侧 5 个、读侧 5 个不可表达值 fail-closed。

结果：**24 passed**。

## 负控（恢复现实现）

把 `plaita/dsl/sexpr.py` 的两个 hunk 逐字还原（其余不动）：

- 新文件：**23 failed / 1 passed**；唯一绿的是「字段缺席不发明字节」——
  它断言的正是无字段行为，符合预期。
- 精确红文案（全文 `docs/proofs/data/220-L3-negative-control.txt`）：

```
E  assert ':sandbox-backend "docker"' in '(flow f-l3 ... (code :id n :lang python :code "def run(x): return x" :input $INPUT.x -> e) ...)'
E  KeyError: 'sandbox_backend'
```

## 回归面（修复后）

| 套件 | 结果 |
|------|------|
| `tests/unit` | 7864 passed / 16 skipped / 3429 subtests |
| `tests/e2e` + `tests/integration` | 287 passed / 4 skipped |
| sexpr/codeflow/code 相关子集 | 1140 passed / 3 skipped |
| `mkdocs build --strict` | 通过 |

既有 #136/#142/#143/#147/#150/#200/#207 往返用例全绿；`plaita/dsl/sexpr.py`
之外的代码零改动。

## 文档同步

`docs-site/docs/guide/code-dsl.md` 的「Node-type coverage: writer ⊇ reader」
全字段表补 `code | sandbox_backend` 行，并说明引号发射、缺席不发、
非字符串两侧 fail-closed 的口径（按 `docs/DOC_CODE_MAP.md` 映射）。
