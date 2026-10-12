"""plaita#107 回归：状态表自引用容器 / 快照出口的环与深度 / 节点结果按值入库。

issue 形态：``output: "$NODE"``（把整张 ``$NODE`` 状态表作为 assignment 输出）
让状态表写回自身成为**自引用容器**。历史 ``_snapshot_value`` 是无 memo、无环
检测、无深度上限的朴素递归 ⇒

* ``generator`` 每步 emit ``context.to_dict()`` ⇒ 裸 ``RecursionError``；
* ``distributed`` 每步同样出口，但异常被
  ``_error_normalization.raise_distributed_error`` 归一化成
  ``FlowErrorException("maximum recursion depth exceeded")``——节点 id/路径全丢；
* ``normal`` 反而"成功"（更糟：静默产出自引用/别名结构，``$NODE['a'] is $NODE``）；
* 无环的深嵌套值（实测 2500 层）在 checkpoint 出口撞同一处。

修复（#107）：
1. ``_snapshot_value`` 改显式栈迭代物化——数据深度与 Python 递归深度解耦；
   环按 ``<recursive: path>`` 占位符显式截断；超 ``_SNAPSHOT_MAX_DEPTH`` 抛
   本仓 ``StateSnapshotError``（消息含深度与路径）。
2. ``CheckpointState.update_node_result`` 按值入库（整表先浅拷、结果经
   ``_snapshot_value`` 深物化），``$NODE.<node>`` 成为私有快照，不再被后续
   步骤原地改写。
"""

import unittest

from plaita.core.context import ExecutionContext
from plaita.core.errors import FlowErrorException, FlowExecutionException, StateSnapshotError
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.core.state import (
    _SNAPSHOT_CYCLE_MARK,
    _SNAPSHOT_MAX_DEPTH,
    CheckpointState,
    _snapshot_value,
)


def _selfref_flow_def(alias: bool = False) -> dict:
    """``s → a(output="$NODE") → e(output="$NODE.a")``（alias 版插一个 b 步）。"""
    nodes = [
        {"id": "s", "type": "start", "name": "start", "next": "a"},
        {"id": "a", "type": "assignment", "name": "a",
         "output": "$NODE", "outputType": {"data_type": "object"},
         "next": "b" if alias else "e"},
    ]
    if alias:
        nodes.append({"id": "b", "type": "assignment", "name": "b",
                      "output": "$NODE.a", "outputType": {"data_type": "object"},
                      "next": "e"})
    nodes.append({"id": "e", "type": "end", "name": "end",
                  "resultType": "success", "output": "$NODE.a"})
    return {"flow_id": "issue107-selfref-alias" if alias else "issue107-selfref",
            "runtime": "python", "inputType": {"dataType": "object"}, "nodes": nodes}


def _run_normal_to_end(alias: bool = False):
    execution = FlowExecution()
    execution.mode = "normal"
    result = execution.execute(Flow.model_validate(_selfref_flow_def(alias)), {})
    return result, execution.get_state("$NODE")


# ---------------------------------------------------------------------------
# 验收 1：三模式都不再 RecursionError；distributed 不再归一化出递归文案
# ---------------------------------------------------------------------------


class TestSelfRefStateAcrossModes(unittest.TestCase):
    def test_normal_mode_completes_and_table_has_no_self_reference(self):
        result, table = _run_normal_to_end()
        # End 节点 output="$NODE.a" ⇒ 结果就是 a 步写入的值快照（写回时 $NODE 只有 s）
        self.assertEqual(result, {"s": None})
        self.assertEqual(sorted(table), ["a", "e", "s"])
        self.assertIsNot(table["a"], table)
        self.assertNotIn("e", table["a"])

    def test_generator_mode_does_not_raise_recursion_error(self):
        flow = Flow.model_validate(_selfref_flow_def())
        steps = list(FlowExecution.run(flow, {}, mode="generator"))
        self.assertEqual(len(steps), 3)  # s / a / e
        self.assertTrue(steps[-1]["is_end"])
        # 每步 emit 的 context 都在快照出口物化成功，且 $NODE 不自引用
        for step in steps:
            node_table = step["context"]["$NODE"]
            if "a" in node_table:
                self.assertIsNot(node_table["a"], node_table)

    def test_distributed_mode_does_not_normalize_recursion_error(self):
        flow = Flow.model_validate(_selfref_flow_def())
        execution = FlowExecution()
        execution.mode = "distributed"
        try:
            result = execution.run_distributed(flow, {})
            context = result.get("context")
            while not result.get("is_end"):
                result = execution.run_distributed(flow, saved_context=context)
                context = result.get("context", context)
        except FlowErrorException as exc:
            # 历史形态：FlowErrorException("maximum recursion depth exceeded ...")
            self.fail(f"distributed 归一化出 FlowErrorException: {exc}")
        except FlowExecutionException as exc:
            self.fail(f"distributed 步进失败: {type(exc).__name__}: {exc}")
        self.assertTrue(result.get("is_end"))
        self.assertEqual(sorted(context["$NODE"]), ["a", "e", "s"])
        self.assertIsNot(context["$NODE"]["a"], context["$NODE"])


# ---------------------------------------------------------------------------
# 验收 2：深嵌套不再 RecursionError（或抛本仓自定义异常且含深度/路径）
# ---------------------------------------------------------------------------


class TestSnapshotDepthGuard(unittest.TestCase):
    def test_deep_dict_2500_via_checkpoint_export(self):
        value = "leaf"
        for _ in range(2500):
            value = {"n": value}
        ctx = ExecutionContext()
        ctx.set_state("$DEEP", value)
        data = ctx.to_dict()
        depth, node = 0, data["$DEEP"]
        while isinstance(node, dict):
            depth += 1
            node = node["n"]
        self.assertEqual(depth, 2500)
        self.assertEqual(node, "leaf")

    def test_deep_dict_2500_via_snapshot_value(self):
        value = "leaf"
        for _ in range(2500):
            value = {"n": value}
        snap = _snapshot_value(value)
        depth, node = 0, snap
        while isinstance(node, dict):
            depth += 1
            node = node["n"]
        self.assertEqual(depth, 2500)
        self.assertEqual(node, "leaf")

    def test_deep_list_2500(self):
        value = "leaf"
        for _ in range(2500):
            value = [value]
        snap = _snapshot_value(value)
        depth, node = 0, snap
        while isinstance(node, list):
            depth += 1
            node = node[0]
        self.assertEqual(depth, 2500)
        self.assertEqual(node, "leaf")

    def test_deep_tuple_1200_fixups_applied_in_order(self):
        value = "leaf"
        for _ in range(1200):
            value = (value,)
        snap = _snapshot_value(value)
        self.assertIsInstance(snap, tuple)
        depth, node = 0, snap
        while isinstance(node, tuple):
            depth += 1
            node = node[0]
        self.assertEqual(depth, 1200)
        self.assertEqual(node, "leaf")

    def test_depth_over_limit_raises_repo_error_with_depth_and_path(self):
        value = "leaf"
        for _ in range(_SNAPSHOT_MAX_DEPTH + 5):
            value = {"n": value}
        with self.assertRaises(StateSnapshotError) as cm:
            _snapshot_value(value)
        message = str(cm.exception)
        self.assertIn(f"depth {_SNAPSHOT_MAX_DEPTH + 1}", message)
        self.assertIn(str(_SNAPSHOT_MAX_DEPTH), message)
        # 路径按头 4 / 尾 2 段渲染，超长中间以 '...' 标记——精确比对整条路径
        # （assertIn 会被「尾段从下标 2 起」这类实现差异骗过：多出的 'n' 段
        # 里也含目标子串）
        self.assertEqual(
            message.split("at path ", 1)[1],
            "$['n']['n']['n']['n']['...']['n']['n']",
        )

    def test_shallow_path_is_not_abbreviated(self):
        value = {"a": {"b": {"c": "leaf"}}}
        snap = _snapshot_value(value)
        self.assertEqual(snap["a"]["b"]["c"], "leaf")

    @staticmethod
    def _chain_cycle(levels: int):
        """``k1 → k2 → … → kn → self`` 回指根；占位符路径恰有 ``levels + 1`` 段。"""
        root = {}
        node = root
        for i in range(levels):
            node[f"k{i + 1}"] = {}
            node = node[f"k{i + 1}"]
        node["self"] = root
        return root

    def test_cycle_path_at_threshold_is_not_abbreviated(self):
        """7 段路径（阈值内）必须完整回显——杀掉 >/>= 与 +/-1 的阈值变异。"""
        root = self._chain_cycle(6)
        snap = _snapshot_value(root)
        node = snap
        for i in range(6):
            node = node[f"k{i + 1}"]
        self.assertEqual(
            node["self"],
            "<recursive: $['k1']['k2']['k3']['k4']['k5']['k6']['self']>",
        )

    def test_cycle_path_beyond_threshold_is_abbreviated_head4_tail2(self):
        """8 段路径必须截断为头 4 + '...' + 尾 2——钉死尾段的精确切片。"""
        root = self._chain_cycle(7)
        snap = _snapshot_value(root)
        node = snap
        for i in range(7):
            node = node[f"k{i + 1}"]
        self.assertEqual(
            node["self"],
            "<recursive: $['k1']['k2']['k3']['k4']['...']['k7']['self']>",
        )

    def test_depth_error_is_flow_execution_exception_subclass(self):
        """normal 模式原样透传、distributed 归一化后仍保留深度/路径文案。"""
        self.assertTrue(issubclass(StateSnapshotError, FlowExecutionException))
        self.assertEqual(StateSnapshotError.code, -500)


# ---------------------------------------------------------------------------
# 环：显式截断（可见占位符），不是静默产出损坏结构、更不是递归炸栈
# ---------------------------------------------------------------------------


class TestSnapshotCycleDetection(unittest.TestCase):
    def test_self_referential_dict_truncated_with_placeholder(self):
        value = {"a": 1}
        value["self"] = value
        snap = _snapshot_value(value)
        self.assertEqual(snap["a"], 1)
        self.assertEqual(snap["self"], _SNAPSHOT_CYCLE_MARK.format(path="$['self']"))
        self.assertIsInstance(snap["self"], str)

    def test_mutual_cycle_truncated_with_full_path(self):
        left = {}
        right = {"left": left}
        left["right"] = right
        snap = _snapshot_value(left)
        self.assertEqual(
            snap["right"]["left"],
            _SNAPSHOT_CYCLE_MARK.format(path="$['right']['left']"),
        )

    def test_cycle_through_list_truncated(self):
        value = [1]
        value.append(value)
        snap = _snapshot_value(value)
        self.assertEqual(snap[0], 1)
        self.assertEqual(snap[1], _SNAPSHOT_CYCLE_MARK.format(path="$[1]"))

    def test_siblings_after_cycle_are_still_copied(self):
        """环命中的是 continue 而不是 break：其后的兄弟键仍须物化。"""
        value = {"cyc": None, "tail": {"leaf": 1}, "after": [1, 2]}
        value["cyc"] = value
        snap = _snapshot_value(value)
        self.assertEqual(snap["cyc"], _SNAPSHOT_CYCLE_MARK.format(path="$['cyc']"))
        self.assertEqual(snap["tail"], {"leaf": 1})
        self.assertEqual(snap["after"], [1, 2])

    def test_cycle_through_non_root_ancestor_is_truncated(self):
        """祖先集合必须逐个入 seen：非根回指同样截断（而不是一直下推）。"""
        branch = {}
        root = {"branch": branch}
        branch["loop"] = branch
        snap = _snapshot_value(root)
        self.assertEqual(
            snap["branch"]["loop"],
            _SNAPSHOT_CYCLE_MARK.format(path="$['branch']['loop']"),
        )

    def test_cycle_in_checkpoint_export_is_visible(self):
        cyclic = {"x": 1}
        cyclic["loop"] = cyclic
        ctx = ExecutionContext()
        ctx.set_state("$CYC", cyclic)
        data = ctx.to_dict()
        self.assertEqual(
            data["$CYC"]["loop"], _SNAPSHOT_CYCLE_MARK.format(path="$['loop']")
        )

    def test_shared_acyclic_child_is_copied_not_truncated(self):
        """防误伤守卫：DAG（兄弟共享同一子容器）不是环，必须各拷一份真值。"""
        shared = {"x": [1, 2]}
        src = {"a": shared, "b": shared}
        snap = _snapshot_value(src)
        self.assertEqual(snap["a"], {"x": [1, 2]})
        self.assertEqual(snap["b"], {"x": [1, 2]})
        self.assertNotIsInstance(snap["a"], str)
        self.assertIsNot(snap["a"], snap["b"])
        self.assertIsNot(snap["a"]["x"], shared["x"])
        snap["a"]["x"].append(3)
        self.assertEqual(src["a"]["x"], [1, 2])


# ---------------------------------------------------------------------------
# 验收 3：节点结果按值入库——写回自身不再成立、历史结果不再被后续步骤改写
# ---------------------------------------------------------------------------


class TestNodeResultStoredByValue(unittest.TestCase):
    def test_state_table_written_back_to_itself_is_not_self_referential(self):
        state = CheckpointState()
        state.update_node_result("s", None)
        state.update_node_result("a", state["$NODE"])          # output: "$NODE"
        state.update_node_result("b", state["$NODE"]["a"])     # output: "$NODE.a"
        table = state["$NODE"]
        self.assertIsNot(table["a"], table)
        self.assertIsNot(table["b"], table["a"])
        self.assertNotIn("b", table["a"])
        self.assertEqual(sorted(table["a"]), ["s"])

    def test_flow_level_alias_regression(self):
        _result, table = _run_normal_to_end(alias=True)
        self.assertEqual(sorted(table), ["a", "b", "e", "s"])
        self.assertIsNot(table["b"], table["a"])
        self.assertNotIn("b", table["a"])
        self.assertIsNot(table["a"], table)

    def test_handed_out_table_does_not_grow_after_later_updates(self):
        state = CheckpointState()
        state.update_node_result("s", None)
        handed_out = state["$NODE"]
        state.update_node_result("a", {"v": 1})
        self.assertEqual(sorted(handed_out), ["s"])
        self.assertEqual(sorted(state["$NODE"]), ["a", "s"])

    def test_result_payload_is_deep_copied_on_insert(self):
        state = CheckpointState()
        payload = {"v": [1]}
        state.update_node_result("n", payload)
        payload["v"].append(2)
        payload["extra"] = True
        self.assertEqual(state["$NODE"]["n"], {"v": [1]})

    def test_node_result_cycle_is_truncated_on_insert(self):
        state = CheckpointState()
        cyclic = {"x": 1}
        cyclic["loop"] = cyclic
        state.update_node_result("n", cyclic)
        self.assertEqual(
            state["$NODE"]["n"]["loop"],
            _SNAPSHOT_CYCLE_MARK.format(path="$['loop']"),
        )

    def test_existing_entries_are_preserved_by_value(self):
        state = CheckpointState()
        state.update_node_result("n1", {"v": 1})
        state.update_node_result("n2", {"v": 2})
        self.assertEqual(state["$NODE"], {"n1": {"v": 1}, "n2": {"v": 2}})
        state.update_node_result("n1", {"v": 3})
        self.assertEqual(state["$NODE"]["n1"], {"v": 3})
        self.assertEqual(state["$NODE"]["n2"], {"v": 2})


if __name__ == "__main__":
    unittest.main()
