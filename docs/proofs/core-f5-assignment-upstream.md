# core-F5 证明：assignment 多 upstream 无命中不得静默产出 `[]`

- 审计来源：`local/plaita-audit-20261017.md` P2「执行正确性」core-F5
- 基线：`origin/main` = `a7a6fdd`
- 边界：只动 `Assignment.execute` 的多 upstream 选择分支；单 upstream、
  命中路径、数据/映射/索引/`$F.*` 求值面、`output_type` 校验与无来源
  `return None` 兼容面均不变。

## 根因

`plaita/node/assignment.py::Assignment.execute`（修复前）：

```python
elif len(self.upstream_output) > 1:
    upstream = execution.state.last_node_id
    value = [out for out in self.upstream_output if out["upstream"] == upstream]
    if value:
        value = value[0]["value"]
```

无命中时 `value` 停留在过滤结果 `[]`，随后统一进入
`execution.evaluate(value)`。空列表若通过 `output_type`（`array`/`any`
等），就会被当作合法结果下发——下游无法区分「合法的空结果」与
「当前 upstream 根本没有在声明列表里」。这是静默的数据错误，且多
upstream 的选择本应是确定性的：`last_node_id` 必须在声明列表中。

## 修法

```python
elif len(self.upstream_output) > 1:
    upstream = execution.state.last_node_id
    matched = [out for out in self.upstream_output if out["upstream"] == upstream]
    if not matched:
        declared = [out["upstream"] for out in self.upstream_output]
        raise AssignmentError(
            f"assignment node {self.id!r}: 多 upstream 无命中——"
            f"当前上游 {upstream!r} 不在 upstream_output 声明的 "
            f"{declared!r} 中；拒绝静默下发空列表 []"
        )
    value = matched[0]["value"]
```

- `AssignmentError(ValueError)` 定义在 `plaita/node/assignment.py`，并从
  `plaita.node` 导出；保持 `except ValueError` 处理面兼容。
- 无命中时**不调用** `evaluate`、不产出任何值；异常按节点异常走既有
  重试/errorHandler 路径。
- 命中时 `matched[0]` 与旧 `value[0]` 选择顺序一致；单 upstream 分支原样。

## 测试（8 条，`tests/unit/test_core_f5_assignment_upstream.py`）

无命中（3 条）：

- 双 upstream + `last_node_id=u3` → `AssignmentError`，消息含节点 id /
  `u3` / 声明的 `u1,u2` / `[]`，且 `evaluate` 未被调用（旧实现正是在
  这里把 `[]` 交给求值）；
- `AssignmentError` 是 `ValueError` 子类；
- `last_node_id=None` 同样具名拒绝。

合法面回归（5 条）：

- 单 upstream：`evaluate` 收到声明的 value，返回值原样；
- 双 upstream 命中 `u2`：执行 `$NODE.u2`，返回值原样；
- 命中路径的 value 分别取映射、`$F.index(...)`、数据列表（参数化 3 条）：
  `evaluate` 收到的原始表达式/数据与返回值均不变。

相邻回归实跑：`test_decide.py`、`test_coverage_sweep.py`、
`test_issue238_dsl_conformance.py`、`test_issue220_dsl_roundtrip_loss.py`、
`test_sexpr_coverage3.py` 共 **213 passed / 11 subtests**。

## 负控

把多 upstream 分支还原为修复前的「无命中保持过滤结果 `[]`」：

```
2 failed, 6 passed
E       Failed: DID NOT RAISE AssignmentError
```

主用例精确红在 `with pytest.raises(AssignmentError)`；还原修复后
**8 passed**。完整日志见 `docs/proofs/data/core-f5-negative-control.txt`。

## 验收

- 新增 8 passed；相邻回归 213 passed / 11 subtests；
- 全量 unit（`-m "not slow"`）**8120 passed / 12 skipped / 14 deselected /
  3492 subtests**；e2e+integration **287P/4S**；console **549P/13S**；
  plaita-ai **233P/14S**；layering / wheel / size 三相位全绿；
- 基线 `a7a6fdd` CI **success**，`landing_gate.py ci` 绿后单 commit 推送。
