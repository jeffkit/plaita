"""clean 修复包 C1-2/C1-3 — checkpoint/$PARENT 浅拷贝 live 别名 + emit 快照成本。

问题（2026-10 评审）：
- ``CheckpointState.to_checkpoint_dict()`` 浅拷贝：``$NODE`` 等嵌套容器按
  引用共享——对返回值变异直接写穿 live state；分布式每步 emit 的 context
  与后续执行共享同一 dict（flow_worker 落盘/异步序列化时漂移）。
- ``ExecutionContext.setup_flow`` 的 ``$PARENT`` 注释声称快照实为引用：
  子流程写 ``$PARENT`` 污染父运行态；父继续 ``update_node_result`` 让子侧
  "冻结"快照漂移。

修复：``to_checkpoint_dict`` 递归物化（``_snapshot_value``）；``$PARENT``
经同一出口拿真快照。C1-3 消费链路实证：emit context 被 flow_worker 当作
saved_context 回传引擎并逐步持久化（plaita/server/flow_worker.py），所以
快照必须在 emit 边界（引擎内）完成——宿主侧无法补拷贝。选型 (b)：emit
边界快照，单步 O(状态总量)，分布式全程 O(n²) 复制——与宿主每步 JSON
序列化同量级，实测开销见文末计时测试与报告。
"""

import time
import unittest

from plaita.core.context import ExecutionContext
from plaita.core.executor import FlowExecution
from plaita.core.flow import Flow
from plaita.core.state import (
    CheckpointState,
    _LazyRootSnapshot,
    _snapshot_value,
)


def _linear_flow(n_nodes: int) -> Flow:
    """start -> a1 -> ... -> an -> end；每个 assignment 让 $NODE 增长一项。"""
    nodes = [{"type": "start", "id": "start", "next": "a1"}]
    for i in range(1, n_nodes + 1):
        nodes.append({
            "type": "assignment", "id": f"a{i}",
            "output": {"step": i, "payload": "x" * 32},
            "next": f"a{i + 1}" if i < n_nodes else "end",
        })
    nodes.append({"type": "end", "id": "end", "output": {"done": True}})
    return Flow.model_validate({
        "runtime": "python", "inputType": {"dataType": "object"}, "nodes": nodes,
    })


class _FakeFlow:
    flow_id = "child"
    global_context = None
    expose_env = []
    timeout = None


class CheckpointSnapshotTest(unittest.TestCase):
    def test_to_checkpoint_dict_no_write_through(self):
        """对 to_checkpoint_dict() 返回值变异不得写穿 live state。"""
        ctx = ExecutionContext()
        ctx.set_state("$NODE", {"n1": {"v": [1, 2]}})
        cp = ctx.to_dict()
        cp["$NODE"]["n1"]["v"].append(999)
        self.assertEqual(ctx.get_state("$NODE")["n1"]["v"], [1, 2])

    def test_to_checkpoint_dict_repeated_mutation_isolated(self):
        """两次导出互不共享嵌套容器。"""
        ctx = ExecutionContext()
        ctx.set_state("$NODE", {"n1": {"v": 1}})
        a = ctx.to_dict()
        b = ctx.to_dict()
        a["$NODE"]["n1"]["v"] = 2
        self.assertEqual(b["$NODE"]["n1"]["v"], 1)
        self.assertEqual(ctx.get_state("$NODE")["n1"]["v"], 1)

    def test_snapshot_covers_list_tuple_dict_shapes(self):
        from plaita.core.state import _snapshot_value as sv

        src = {"a": [1, {"b": (2, [3])}], "c": ({"d": 4},)}
        snap = sv(src)
        self.assertEqual(snap, src)
        snap["a"][1]["b"][1].append(5)
        snap["c"][0]["d"] = 99
        self.assertEqual(src["a"][1]["b"][1], [3])
        self.assertEqual(src["c"][0]["d"], 4)

    def test_round_trip_equality_kept(self):
        """快照不改变 checkpoint 往返等值性（键集与值深度相等）。"""
        data = {"$NODE": {"n1": {"v": [1, 2]}}, "EXPRESS_PREFIX": "$"}
        state = CheckpointState.from_checkpoint_dict(data)
        self.assertEqual(state, data)
        again = CheckpointState.from_checkpoint_dict(state.to_checkpoint_dict())
        self.assertEqual(state, again)


class ParentSnapshotTest(unittest.TestCase):
    def test_parent_node_not_live_alias(self):
        """$PARENT.NODE 与父 live $NODE 不再是同一对象。"""
        p = ExecutionContext()
        p.clean()
        p.set_state("$NODE", {"n1": {"v": 1}})
        c = p.child()
        c.clean()
        c.setup_flow(_FakeFlow(), (), {})
        snap = c.get_state("$PARENT")
        self.assertIsNot(snap["$NODE"], p.get_state("$NODE"))

    def test_child_write_to_parent_does_not_leak(self):
        """子流程侧写 $PARENT 不再污染父运行态。"""
        p = ExecutionContext()
        p.clean()
        p.set_state("$NODE", {"n1": {"v": 1}})
        c = p.child()
        c.clean()
        c.setup_flow(_FakeFlow(), (), {})
        c.get_state("$PARENT")["$NODE"]["n1"]["v"] = 999
        self.assertEqual(p.get_state("$NODE")["n1"]["v"], 1)

    def test_parent_progress_does_not_drift_into_child_snapshot(self):
        """子侧首读后的 $PARENT 快照不再随父 update_node_result 漂移。

        惰性快照契约（_LazyRootSnapshot）：每个根键在子侧首次读取时物化冻结。
        引擎里父执行在 InlineFlow 子流程存续期被阻塞（非重入），「setup 后
        首读前」父状态不可能变化，故首读物化 ≡ setup 时全量快照；本测试钉
        首读之后的隔离（此前浅拷贝连首读后都不隔离——父原地 mutate 直接
        漂进子侧持有的引用）。
        """
        p = ExecutionContext()
        p.clean()
        p.set_state("$NODE", {"n1": {"v": 1}})
        c = p.child()
        c.clean()
        c.setup_flow(_FakeFlow(), (), {})
        snap = c.get_state("$PARENT")
        # 首读：物化并冻结
        self.assertEqual(snap["$NODE"]["n1"]["v"], 1)

        class _N:
            id = "n2"

        p.update_node_result(_N(), {"v": 2})
        # 子侧快照仍是首读时的值，父侧新增节点结果不漂入
        self.assertNotIn("n2", snap["$NODE"])
        # 父 live state 正常推进
        self.assertIn("n2", p.get_state("$NODE"))

    def test_parent_snapshot_materializes_to_plain_dict_on_checkpoint(self):
        """$PARENT 经 to_checkpoint_dict 完全物化为 plain dict（惰性不出进程）。"""
        p = ExecutionContext()
        p.clean()
        p.set_state("$NODE", {"n1": {"v": 1}})
        c = p.child()
        c.clean()
        c.setup_flow(_FakeFlow(), (), {})
        raw = c.get_state("$PARENT")
        self.assertIsInstance(raw, _LazyRootSnapshot)
        emitted = c.to_dict()
        self.assertIsInstance(emitted["$PARENT"], dict)
        self.assertEqual(emitted["$PARENT"]["$NODE"], {"n1": {"v": 1}})
        # 物化副本与父 live state 不共享容器
        emitted["$PARENT"]["$NODE"]["n1"]["v"] = 999
        self.assertEqual(p.get_state("$NODE")["n1"]["v"], 1)

    def test_parent_snapshot_read_does_not_alias_live(self):
        """首读物化的根值与父 live 值不共享对象。"""
        p = ExecutionContext()
        p.clean()
        p.set_state("$NODE", {"n1": {"v": 1}})
        c = p.child()
        c.clean()
        c.setup_flow(_FakeFlow(), (), {})
        snap_node = c.get_state("$PARENT")["$NODE"]
        self.assertIsNot(snap_node, p.get_state("$NODE"))
        # 等值视图仍成立（dict 断言兼容）
        self.assertEqual(c.get_state("$PARENT"), {"$NODE": {"n1": {"v": 1}},
                                                  "$EXECUTION_ID": p.get_state("$EXECUTION_ID"),
                                                  "$ENV": {}})


class DistributedEmitSnapshotTest(unittest.TestCase):
    """分布式每步 emit 的 context 不再与引擎 live state 共享 $NODE。"""

    def test_emit_context_is_snapshot(self):
        fl = _linear_flow(3)
        execution = FlowExecution()
        execution.mode = "distributed"
        result = execution.run_distributed(fl, {})
        context = result.get("context")
        step1_node = context["$NODE"]
        # 第一步执行 start 节点（写 $NODE.start）+ 解析后继；$NODE 此刻含 start+a1
        self.assertEqual(sorted(step1_node.keys()), ["a1", "start"])
        # 继续推进到终点，回看第一步 emit 的 $NODE 不应增长
        while not result.get("is_end"):
            result = execution.run_distributed(fl, saved_context=context)
            context = result.get("context", context)
        self.assertEqual(sorted(step1_node.keys()), ["a1", "start"])
        self.assertEqual(sorted(context["$NODE"].keys()),
                         ["a1", "a2", "a3", "end", "start"])

    def test_end_output_result_is_flow_result(self):
        fl = _linear_flow(3)
        execution = FlowExecution()
        execution.mode = "distributed"
        result = execution.run_distributed(fl, {})
        context = result.get("context")
        while not result.get("is_end"):
            result = execution.run_distributed(fl, saved_context=context)
            context = result.get("context", context)
        self.assertTrue(result.get("is_end"))
        self.assertEqual(result.get("result"), {"done": True})


class FiftyNodeTimingReportTest(unittest.TestCase):
    """C1-3 计时对比（50 节点，修复后基线；修复前数据见批次报告）。

    只做宽松冒烟上限断言（防意外劣化到秒级），不钉精确数值——CI 机器
    噪音大，精确对比以报告中的 bench 数据为准。
    """

    def test_generator_50_nodes_wall_time_smoke(self):
        fl = _linear_flow(50)
        FlowExecution.run(fl, {}, mode="generator")  # warmup
        t0 = time.perf_counter()
        steps = sum(1 for _ in FlowExecution.run(fl, {}, mode="generator"))
        dt = time.perf_counter() - t0
        self.assertEqual(steps, 52)  # start + 50 assignment + end
        self.assertLess(dt, 5.0, f"generator 50 节点耗时异常: {dt*1000:.0f}ms")

    def test_distributed_50_nodes_wall_time_smoke(self):
        fl = _linear_flow(50)

        def run_full():
            execution = FlowExecution()
            execution.mode = "distributed"
            result = execution.run_distributed(fl, {})
            context = result.get("context")
            while not result.get("is_end"):
                result = execution.run_distributed(fl, saved_context=context)
                context = result.get("context", context)
            return result

        run_full()  # warmup
        t0 = time.perf_counter()
        result = run_full()
        dt = time.perf_counter() - t0
        self.assertTrue(result.get("is_end"))
        self.assertLess(dt, 20.0, f"distributed 50 节点耗时异常: {dt*1000:.0f}ms")


if __name__ == "__main__":
    unittest.main()
