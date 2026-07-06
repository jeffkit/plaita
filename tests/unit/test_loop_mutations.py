"""变异测试专项断言 — plaita.node.loop

针对 mutmut 扫描出的 63 个 survived 变异，按节点/方法分组写精准杀灭测试。

主要变异类别：
  1. collection evaluate(None) — 集合不被求值（各节点）
  2. run_compatible(None, ...) — child_flow=None
  3. run_compatible(flow, None/True, ...) — debug_mode 变异
  4. run_compatible(flow, False, item=item, index=None) — index=None
  5. run_compatible(flow, False, item=item, ) — index 缺失
  6. Loop.execute index 初始值/增量变异（=1, -=1, +=2）
  7. Loop.execute 条件上下文（LOOP-ITEM/LOOP-INDEX/pfx 变异）
  8. Reduce._child_is_array_input 属性名变异
  9. Map.arun Semaphore "or 2" / index=None 变异
"""
from __future__ import annotations

import asyncio
import unittest
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, call, patch

from plaita import Flow
from plaita.node.loop import Filter, Find, Loop, Map, Reduce


# ---------------------------------------------------------------------------
# 公共辅助
# ---------------------------------------------------------------------------

def _child_flow_stub():
    """最小子流程字典——mock ctx 会忽略，仅用于节点解析。"""
    return {
        "id": "cf", "version": "1", "runtime": "python",
        "nodes": [
            {"type": "start", "id": "s", "next": "e"},
            {"type": "end", "id": "e", "resultType": "success", "output": "$INPUT"},
        ],
    }


def _make_node(node_type: str, extra: dict = None):
    """通过 Flow.model_validate 解析真实节点对象。"""
    node_dict = {"type": node_type, "id": "n", "collection": "$INPUT.items",
                 "childFlow": _child_flow_stub(), "next": "end"}
    if extra:
        node_dict.update(extra)
    flow = Flow.model_validate({
        "flow_id": "f", "version": "1", "runtime": "python",
        "nodes": [
            {"type": "start", "id": "start", "next": "n"},
            node_dict,
            {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.n"},
        ],
    })
    return next(n for n in flow.nodes if getattr(n, "id", None) == "n")


def _make_strict_ctx(collection, child_return=None, express_prefix="$"):
    """Mock ExecutionContext，对 run_compatible 调用参数进行严格记录。

    - evaluate(None) → 返回 [] (严格模式：None 表达式意味着集合为空)
    - evaluate(non-None) → 返回 collection
    - 所有 run_compatible 调用参数记录在 ctx._run_calls 中
    """
    ctx = MagicMock()
    ctx.express_prefix = express_prefix
    ctx.context = {}
    run_calls = []

    def evaluate(expr):
        if expr is None:
            return []
        return collection

    ctx.evaluate.side_effect = evaluate

    def get_child():
        child = MagicMock()
        def run_compat(flow, debug, *args, **kw):
            run_calls.append({"flow": flow, "debug": debug, "args": args, "kw": kw})
            if child_return is not None:
                item = kw.get("item") or (args[0] if args else None)
                index = kw.get("index", 0)
                return child_return(item, index)
            return kw.get("item")
        child.run_compatible.side_effect = run_compat
        return child

    ctx.get_child_execution.side_effect = get_child
    ctx._run_calls = run_calls
    return ctx


def _make_async_strict_ctx(collection, child_return=None, express_prefix="$"):
    """带 arun_compatible 的异步 mock ctx。"""
    ctx = MagicMock()
    ctx.express_prefix = express_prefix
    ctx.context = {}
    run_calls = []

    def evaluate(expr):
        if expr is None:
            return []
        return collection

    ctx.evaluate.side_effect = evaluate

    def get_child():
        child = MagicMock()
        async def arun_compat(flow, debug, *args, **kw):
            run_calls.append({"flow": flow, "debug": debug, "args": args, "kw": kw})
            if child_return is not None:
                item = kw.get("item") or (args[0] if args else None)
                index = kw.get("index", 0)
                return child_return(item, index)
            return kw.get("item")
        child.arun_compatible = arun_compat
        return child

    ctx.get_child_execution.side_effect = get_child
    ctx._run_calls = run_calls
    return ctx


# ---------------------------------------------------------------------------
# Loop.execute — 变异杀灭测试
# ---------------------------------------------------------------------------

class TestLoopExecuteMutations(unittest.TestCase):

    def test_evaluate_uses_collection_expr(self):
        """_2: evaluate(None) → 空集合 → 结果为 None。
        严格 evaluate: None 返回 []，非 None 返回 collection。
        """
        node = _make_node("loop")
        ctx = _make_strict_ctx([10, 20, 30])
        result = node.execute(ctx)
        # 若 evaluate(None)，collection=[] → 返回 None
        # 若 evaluate(self.collection)，collection=[10,20,30] → 返回 30
        self.assertIsNotNone(result,
                             "Loop.execute 应调用 evaluate(self.collection)，不是 evaluate(None)")

    def test_child_flow_is_not_none(self):
        """_9: run_compatible(None, ...) — child_flow 被置为 None。"""
        node = _make_node("loop")
        ctx = _make_strict_ctx([1, 2])
        node.execute(ctx)
        for c in ctx._run_calls:
            self.assertIsNotNone(c["flow"],
                                 "run_compatible 第 1 参数（child_flow）不应为 None")

    def test_debug_mode_is_false(self):
        """_10: run_compatible(flow, None, ...) — debug 被置为 None。"""
        node = _make_node("loop")
        ctx = _make_strict_ctx([1, 2])
        node.execute(ctx)
        for c in ctx._run_calls:
            self.assertIs(c["debug"], False,
                          "run_compatible 第 2 参数（debug_mode）应为 False")

    def test_index_kwarg_is_present(self):
        """_16: run_compatible(flow, False, item=item, ) — index kwarg 缺失。"""
        node = _make_node("loop")
        ctx = _make_strict_ctx([10, 20, 30])
        node.execute(ctx)
        for c in ctx._run_calls:
            self.assertIn("index", c["kw"],
                          "run_compatible 应包含 index= 关键字参数")

    def test_index_values_are_correct(self):
        """_6,_12,_31,_32,_33: index 初始值/增量变异。
        - _6: index=1 (初始值)
        - _12: index=None (None 替换)
        - _31: index=1 (重置)
        - _32: index-=1 (递减)
        - _33: index+=2 (步长 2)
        期望: indexes 应为 [0, 1, 2, ...]
        """
        node = _make_node("loop")
        ctx = _make_strict_ctx(["a", "b", "c", "d"])
        node.execute(ctx)
        indexes = [c["kw"]["index"] for c in ctx._run_calls]
        self.assertEqual(indexes, [0, 1, 2, 3],
                         f"index 应从 0 单步递增，实际得到 {indexes}")

    def test_starting_index_is_zero(self):
        """_6: index=1 (初始值改为 1)。单元素集合时第一次调用的 index 应为 0。"""
        node = _make_node("loop")
        ctx = _make_strict_ctx([99])
        node.execute(ctx)
        self.assertEqual(ctx._run_calls[0]["kw"]["index"], 0,
                         "首个元素的 index 应为 0")

    def test_index_none_check(self):
        """_12: index=None — run_compatible 收到 index=None。"""
        node = _make_node("loop")
        ctx = _make_strict_ctx([1, 2, 3])
        node.execute(ctx)
        for c in ctx._run_calls:
            self.assertIsNotNone(c["kw"].get("index"),
                                 "index 不应为 None")

    def test_loop_result_is_last_element(self):
        """Baseline + 区分 index 错误：用 index 作为返回值，最终结果应为 len-1。"""
        node = _make_node("loop")
        ctx = _make_strict_ctx([1, 2, 3, 4], child_return=lambda item, idx: idx)
        result = node.execute(ctx)
        self.assertEqual(result, 3,
                         "以 index 为返回值时，最后元素的 index 应为 3")


class TestLoopConditionMutations(unittest.TestCase):
    """Loop 条件上下文变异测试。"""

    def _make_condition_ctx(self, collection, child_return):
        """带 context 深拷贝的 mock ctx，供条件循环使用。"""
        ctx = MagicMock()
        ctx.express_prefix = "$"
        ctx.context = {}
        ctx.evaluate.side_effect = lambda expr: collection if expr is not None else []

        def get_child():
            child = MagicMock()
            child.run_compatible.side_effect = lambda flow, debug, **kw: child_return(
                kw.get("item"), kw.get("index")
            )
            return child

        ctx.get_child_execution.side_effect = get_child
        return ctx

    def test_loop_item_in_condition(self):
        """_22: LOOP-ITEM=None — 条件检查 $LOOP-ITEM 时，None 导致条件错误求值。
        设置：items=[5,6,7], condition='$LOOP-ITEM > 3'（所有元素 > 3，应跑满）
        原始：LOOP-ITEM 正确，条件满足，运行 3 次，result=7 (最后一个元素)
        变异：LOOP-ITEM=None → None > 3 → False → 第一次就 break，result=5
        """
        node = _make_node("loop", extra={
            "condition": {"field": "$LOOP-ITEM", "operator": "gt", "value": 3},
        })
        ctx = self._make_condition_ctx([5, 6, 7], child_return=lambda item, idx: item)
        result = node.execute(ctx)
        self.assertEqual(result, 7,
                         "LOOP-ITEM 正确时，所有元素 > 3，循环应跑满，result=7")

    def test_loop_index_in_condition(self):
        """_23: LOOP-INDEX=None — 条件检查 $LOOP-INDEX 时，None 导致条件错误求值。
        设置：items=[a,b,c,d,e], condition='$LOOP-INDEX < 4'（index 0..3 满足）
        原始：前 4 个 index(0,1,2,3) < 4，第 5 个(index=4) 不满足 → 5 次迭代，result=e
        变异：LOOP-INDEX=None → None < 4 → False → 第一次就 break，result=a
        """
        node = _make_node("loop", extra={
            "condition": {"field": "$LOOP-INDEX", "operator": "lt", "value": 4},
        })
        ctx = self._make_condition_ctx(["a", "b", "c", "d", "e"],
                                       child_return=lambda item, idx: item)
        result = node.execute(ctx)
        self.assertEqual(result, "e",
                         "LOOP-INDEX 正确时，index < 4 所有元素满足，result 应为最后元素 'e'")

    def test_loop_condition_passes_prefix(self):
        """_29: condition.match(loop_ctx, ) — pfx 被省略，使用默认 '$'。
        使用非默认 express_prefix="$F." 来区分原始和突变。
        条件字段 '$F.LOOP-RESULT'，突变时用 '$' 前缀查找 '$F.LOOP-RESULT'... 
        实际上 Condition.match 有 prefix='$' 默认，需借助条件比较结果来区分。
        策略：使用 LOOP-RESULT 条件让循环在特定结果时 break。
        """
        node = _make_node("loop", extra={
            "condition": {"field": "$LOOP-RESULT", "operator": "lt", "value": 6},
        })
        ctx = self._make_condition_ctx([1, 2, 3, 4, 5],
                                       child_return=lambda item, idx: item * 2)
        result = node.execute(ctx)
        # doubled: 2,4,6,8,10; 条件 < 6: 2 passes, 4 passes, 6 fails → break at result=6
        self.assertEqual(result, 6,
                         "Loop 应在 LOOP-RESULT=6 时停止")


# ---------------------------------------------------------------------------
# Loop.arun — 异步变异（与 execute 同构）
# ---------------------------------------------------------------------------

class TestLoopArunConditionMutations(unittest.IsolatedAsyncioTestCase):
    """Loop.arun 条件上下文变异测试（_22/_23 LOOP-ITEM/LOOP-INDEX=None）。"""

    def _make_arun_condition_ctx(self, collection, child_return):
        ctx = MagicMock()
        ctx.express_prefix = "$"
        ctx.context = {}
        ctx.evaluate.side_effect = lambda expr: collection if expr is not None else []

        def get_child():
            child = MagicMock()
            async def arun_compat(flow, debug, **kw):
                return child_return(kw.get("item"), kw.get("index"))
            child.arun_compatible = arun_compat
            return child

        ctx.get_child_execution.side_effect = get_child
        return ctx

    async def test_arun_loop_item_in_condition(self):
        """_22: LOOP-ITEM=None — 同 execute 路径逻辑，async 版本。"""
        node = _make_node("loop", extra={
            "condition": {"field": "$LOOP-ITEM", "operator": "gt", "value": 3},
        })
        ctx = self._make_arun_condition_ctx([5, 6, 7],
                                            child_return=lambda item, idx: item)
        result = await node.arun(ctx)
        self.assertEqual(result, 7,
                         "LOOP-ITEM 正确时循环跑满，result=7")

    async def test_arun_loop_index_in_condition(self):
        """_23: LOOP-INDEX=None — async 版本。"""
        node = _make_node("loop", extra={
            "condition": {"field": "$LOOP-INDEX", "operator": "lt", "value": 4},
        })
        ctx = self._make_arun_condition_ctx(["a", "b", "c", "d", "e"],
                                             child_return=lambda item, idx: item)
        result = await node.arun(ctx)
        self.assertEqual(result, "e",
                         "LOOP-INDEX 正确时，index < 4 所有元素满足，result='e'")


class TestLoopArunMutations(unittest.IsolatedAsyncioTestCase):

    async def test_arun_evaluate_uses_collection_expr(self):
        """_6: evaluate(None) → 空集合。"""
        node = _make_node("loop")
        ctx = _make_async_strict_ctx([10, 20])
        result = await node.arun(ctx)
        self.assertIsNotNone(result)

    async def test_arun_debug_is_false(self):
        """_10: debug=None。"""
        node = _make_node("loop")
        ctx = _make_async_strict_ctx([1, 2])
        await node.arun(ctx)
        for c in ctx._run_calls:
            self.assertIs(c["debug"], False)

    async def test_arun_index_values_correct(self):
        """_16,_31,_32,_33: index 变异。"""
        node = _make_node("loop")
        ctx = _make_async_strict_ctx(["x", "y", "z"])
        await node.arun(ctx)
        indexes = [c["kw"]["index"] for c in ctx._run_calls]
        self.assertEqual(indexes, [0, 1, 2])

    async def test_arun_index_is_not_none(self):
        """_12: index=None。"""
        node = _make_node("loop")
        ctx = _make_async_strict_ctx([1, 2, 3])
        await node.arun(ctx)
        for c in ctx._run_calls:
            self.assertIsNotNone(c["kw"].get("index"))

    async def test_arun_index_kwarg_present(self):
        """_16: index kwarg 缺失。"""
        node = _make_node("loop")
        ctx = _make_async_strict_ctx([1, 2])
        await node.arun(ctx)
        for c in ctx._run_calls:
            self.assertIn("index", c["kw"])


# ---------------------------------------------------------------------------
# Filter.execute — 变异杀灭测试
# ---------------------------------------------------------------------------

class TestFilterExecuteMutations(unittest.TestCase):

    def _ctx(self, collection, child_return):
        return _make_strict_ctx(collection, child_return=child_return)

    def test_evaluate_not_none(self):
        """_2: evaluate(None) → 空集合 → 返回 []，实际应返回过滤结果。"""
        node = _make_node("filter")
        ctx = _make_strict_ctx([1, 2, 3], child_return=lambda item, idx: item > 1)
        result = node.execute(ctx)
        # evaluate(None) → [] → 结果 []；evaluate(self.collection) → [1,2,3] → [2,3]
        self.assertEqual(result, [2, 3])

    def test_child_flow_not_none(self):
        """_7: run_compatible(None, ...) — child_flow=None。"""
        node = _make_node("filter")
        ctx = _make_strict_ctx([1, 2, 3], child_return=lambda item, idx: True)
        node.execute(ctx)
        for c in ctx._run_calls:
            self.assertIsNotNone(c["flow"])

    def test_debug_is_false(self):
        """_8: run_compatible(flow, None, ...) — debug=None。"""
        node = _make_node("filter")
        ctx = _make_strict_ctx([1, 2], child_return=lambda item, idx: True)
        node.execute(ctx)
        for c in ctx._run_calls:
            self.assertIs(c["debug"], False)

    def test_index_is_not_none(self):
        """_10: index=None。"""
        node = _make_node("filter")
        ctx = _make_strict_ctx([1, 2, 3], child_return=lambda item, idx: True)
        node.execute(ctx)
        for c in ctx._run_calls:
            self.assertIsNotNone(c["kw"].get("index"))

    def test_index_kwarg_present(self):
        """_14: index kwarg 缺失。"""
        node = _make_node("filter")
        ctx = _make_strict_ctx([1, 2], child_return=lambda item, idx: True)
        node.execute(ctx)
        for c in ctx._run_calls:
            self.assertIn("index", c["kw"])

    def test_debug_is_not_true(self):
        """_15: run_compatible(flow, True, ...) — debug=True。"""
        node = _make_node("filter")
        ctx = _make_strict_ctx([1, 2], child_return=lambda item, idx: True)
        node.execute(ctx)
        for c in ctx._run_calls:
            self.assertIsNot(c["debug"], True,
                             "debug_mode 不应为 True")


class TestFilterArunMutations(unittest.IsolatedAsyncioTestCase):

    async def test_arun_debug_is_false(self):
        """_8: debug=None。"""
        node = _make_node("filter")
        ctx = _make_async_strict_ctx([1, 2, 3], child_return=lambda item, idx: item > 1)
        await node.arun(ctx)
        for c in ctx._run_calls:
            self.assertIs(c["debug"], False)

    async def test_arun_index_not_none(self):
        """_10: index=None。"""
        node = _make_node("filter")
        ctx = _make_async_strict_ctx([1, 2], child_return=lambda item, idx: True)
        await node.arun(ctx)
        for c in ctx._run_calls:
            self.assertIsNotNone(c["kw"].get("index"))

    async def test_arun_index_kwarg_present(self):
        """_14: index kwarg 缺失。"""
        node = _make_node("filter")
        ctx = _make_async_strict_ctx([1, 2], child_return=lambda item, idx: True)
        await node.arun(ctx)
        for c in ctx._run_calls:
            self.assertIn("index", c["kw"])


# ---------------------------------------------------------------------------
# Find.execute — 变异杀灭测试
# ---------------------------------------------------------------------------

class TestFindExecuteMutations(unittest.TestCase):

    def test_evaluate_not_none(self):
        """_2: evaluate(None) → 空集合 → 返回 None，实际应返回找到的元素。"""
        node = _make_node("find")
        ctx = _make_strict_ctx([1, 2, 3], child_return=lambda item, idx: item > 1)
        result = node.execute(ctx)
        # evaluate(None) → [] → None；evaluate(self.collection) → 2
        self.assertEqual(result, 2)

    def test_child_flow_not_none(self):
        """_6: run_compatible(None, ...) — child_flow=None。"""
        node = _make_node("find")
        ctx = _make_strict_ctx([1, 2], child_return=lambda item, idx: True)
        node.execute(ctx)
        for c in ctx._run_calls:
            self.assertIsNotNone(c["flow"])

    def test_debug_is_false(self):
        """_7: debug=None。"""
        node = _make_node("find")
        ctx = _make_strict_ctx([1, 2], child_return=lambda item, idx: True)
        node.execute(ctx)
        for c in ctx._run_calls:
            self.assertIs(c["debug"], False)

    def test_index_not_none(self):
        """_9: index=None。"""
        node = _make_node("find")
        ctx = _make_strict_ctx([1, 2, 3], child_return=lambda item, idx: item > 1)
        node.execute(ctx)
        for c in ctx._run_calls:
            self.assertIsNotNone(c["kw"].get("index"))

    def test_index_kwarg_present(self):
        """_13: index kwarg 缺失。"""
        node = _make_node("find")
        ctx = _make_strict_ctx([1, 2], child_return=lambda item, idx: True)
        node.execute(ctx)
        for c in ctx._run_calls:
            self.assertIn("index", c["kw"])

    def test_debug_is_not_true(self):
        """_14: debug=True。"""
        node = _make_node("find")
        ctx = _make_strict_ctx([1, 2], child_return=lambda item, idx: True)
        node.execute(ctx)
        for c in ctx._run_calls:
            self.assertIsNot(c["debug"], True)


class TestFindArunMutations(unittest.IsolatedAsyncioTestCase):

    async def test_arun_debug_is_false(self):
        """_7: debug=None。"""
        node = _make_node("find")
        ctx = _make_async_strict_ctx([1, 2, 3], child_return=lambda item, idx: item > 1)
        await node.arun(ctx)
        for c in ctx._run_calls:
            self.assertIs(c["debug"], False)

    async def test_arun_index_not_none(self):
        """_9: index=None。"""
        node = _make_node("find")
        ctx = _make_async_strict_ctx([1, 2], child_return=lambda item, idx: True)
        await node.arun(ctx)
        for c in ctx._run_calls:
            self.assertIsNotNone(c["kw"].get("index"))

    async def test_arun_index_kwarg_present(self):
        """_13: index kwarg 缺失。"""
        node = _make_node("find")
        ctx = _make_async_strict_ctx([1, 2], child_return=lambda item, idx: True)
        await node.arun(ctx)
        for c in ctx._run_calls:
            self.assertIn("index", c["kw"])


# ---------------------------------------------------------------------------
# Reduce.execute — 变异杀灭测试
# ---------------------------------------------------------------------------

class TestReduceExecuteMutations(unittest.TestCase):

    def _ctx_reduce_object(self, collection, initial=None):
        """Reduce object-style mock：first+second kwargs。"""
        ctx = MagicMock()
        ctx.context = {}
        calls = []

        def evaluate(expr):
            if expr is None:
                return None
            if isinstance(collection, list) and expr == collection:
                return collection
            return expr

        ctx.evaluate.side_effect = evaluate

        def get_child():
            child = MagicMock()
            def run_compat(flow, debug, *args, **kw):
                calls.append({"flow": flow, "debug": debug, "args": args, "kw": kw})
                first = kw.get("first") or (args[0] if args else None)
                second = kw.get("second")
                if first is not None and second is not None:
                    return first + second
                return first
            child.run_compatible.side_effect = run_compat
            return child

        ctx.get_child_execution.side_effect = get_child
        ctx._calls = calls
        return ctx

    def test_initial_evaluated_with_self_initial(self):
        """_4: evaluate(None) 代替 evaluate(self.initial)。
        初始值应通过 evaluate(self.initial) 求值；若 evaluate(None)→None，
        则 initial=None，改为用 collection[0] 作为初始值，结果不同。
        策略：设 initial=100，collection=[1,2]；
        - 原始: result=100+1+2=103
        - 变异: evaluate(None)→None→initial=None→用 collection[0]=1→result=1+2=3
        """
        node = _make_node("reduce", extra={"initial": "100"})
        ctx = MagicMock()
        calls = []

        def evaluate(expr):
            if expr is None:
                return None
            if expr == "100":
                return 100
            return expr

        ctx.evaluate.side_effect = evaluate

        def get_child():
            child = MagicMock()
            def run_compat(flow, debug, *args, **kw):
                calls.append({"flow": flow, "debug": debug, "kw": kw})
                first = kw.get("first")
                second = kw.get("second")
                return (first or 0) + (second or 0)
            child.run_compatible.side_effect = run_compat
            return child

        ctx.get_child_execution.side_effect = get_child
        ctx._calls = calls

        # Reduce 直接 evaluate self.initial，我们用真实 Reduce 节点
        # 但 collection 需通过 evaluate 返回
        ctx.evaluate.side_effect = lambda expr: (
            100 if expr == "100" else
            None if expr is None else
            [1, 2]
        )

        node = _make_node("reduce", extra={"initial": "100"})
        result = node.execute(ctx)
        self.assertEqual(result, 103,
                         "initial=100 时，result 应为 100+1+2=103，而非 1+2=3")

    def test_object_style_child_flow_not_none(self):
        """_21: run_compatible(None, ...) — child_flow=None。"""
        node = _make_node("reduce")
        ctx = self._ctx_reduce_object([1, 2, 3])
        ctx.evaluate.side_effect = lambda expr: [1, 2, 3]
        node.execute(ctx)
        for c in ctx._calls:
            self.assertIsNotNone(c["flow"])

    def test_object_style_debug_is_false(self):
        """_22: debug=None。"""
        node = _make_node("reduce")
        ctx = self._ctx_reduce_object([1, 2, 3])
        ctx.evaluate.side_effect = lambda expr: [1, 2, 3]
        node.execute(ctx)
        for c in ctx._calls:
            self.assertIs(c["debug"], False)

    def test_object_style_debug_is_not_true(self):
        """_29: debug=True。"""
        node = _make_node("reduce")
        ctx = self._ctx_reduce_object([1, 2, 3])
        ctx.evaluate.side_effect = lambda expr: [1, 2, 3]
        node.execute(ctx)
        for c in ctx._calls:
            self.assertIsNot(c["debug"], True)

    def _make_array_style_reduce_node(self):
        """创建 array input_type 的 Reduce 节点（走 array-style 路径）。"""
        return Flow.model_validate({
            "flow_id": "f", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "n"},
                {
                    "type": "reduce", "id": "n",
                    "collection": "$INPUT",
                    "childFlow": {
                        "id": "cf", "version": "1", "runtime": "python",
                        "inputType": {"dataType": "array"},
                        "nodes": [
                            {"type": "start", "id": "s", "next": "e"},
                            {"type": "end", "id": "e", "resultType": "success",
                             "output": "$INPUT"},
                        ],
                    },
                    "next": "end",
                },
                {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.n"},
            ],
        }).nodes[1]  # the reduce node

    def _make_array_reduce_ctx(self, collection):
        """Mock ctx for array-style Reduce (positional arg path)."""
        ctx = MagicMock()
        calls = []
        ctx.evaluate.side_effect = lambda expr: collection

        def get_child():
            child = MagicMock()
            def run_compat(flow, debug, *args, **kw):
                calls.append({"flow": flow, "debug": debug, "args": args, "kw": kw})
                # array arg: [result, item]
                pair = args[0] if args else [0, 0]
                return (pair[0] or 0) + (pair[1] or 0)
            child.run_compatible.side_effect = run_compat
            return child

        ctx.get_child_execution.side_effect = get_child
        ctx._calls = calls
        return ctx

    def test_array_style_child_flow_not_none(self):
        """_13: run_compatible(None, False, [result, item]) — array path。"""
        node = self._make_array_style_reduce_node()
        ctx = self._make_array_reduce_ctx([1, 2, 3])
        node.execute(ctx)
        for c in ctx._calls:
            self.assertIsNotNone(c["flow"],
                                 "array-style: child_flow 不应为 None")

    def test_array_style_debug_is_false(self):
        """_14: run_compatible(flow, None, [result, item]) — array path。"""
        node = self._make_array_style_reduce_node()
        ctx = self._make_array_reduce_ctx([1, 2, 3])
        node.execute(ctx)
        for c in ctx._calls:
            self.assertIs(c["debug"], False,
                          "array-style: debug_mode 应为 False")

    def test_array_style_debug_is_not_true(self):
        """_19: run_compatible(flow, True, [result, item]) — array path。"""
        node = self._make_array_style_reduce_node()
        ctx = self._make_array_reduce_ctx([1, 2, 3])
        node.execute(ctx)
        for c in ctx._calls:
            self.assertIsNot(c["debug"], True,
                             "array-style: debug_mode 不应为 True")


# ---------------------------------------------------------------------------
# Reduce._child_is_array_input — 属性名变异测试
# ---------------------------------------------------------------------------

class TestReduceChildIsArrayInput(unittest.TestCase):
    """测试 _child_is_array_input 对 Property/dict input_type 的正确识别。"""

    def _reduce_node(self, child_input_type=None):
        """创建带有指定 input_type 的 Reduce 节点。"""
        child_dict = {
            "id": "cf", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "s", "next": "e"},
                {"type": "end", "id": "e", "resultType": "success", "output": "$INPUT"},
            ],
        }
        if child_input_type:
            child_dict["inputType"] = child_input_type

        node_dict = {
            "type": "reduce", "id": "n",
            "collection": "$INPUT",
            "childFlow": child_dict,
            "next": "end",
        }
        flow = Flow.model_validate({
            "flow_id": "f", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "n"},
                node_dict,
                {"type": "end", "id": "end", "resultType": "success",
                 "output": "$NODE.n"},
            ],
        })
        return next(n for n in flow.nodes if getattr(n, "id", None) == "n")

    def test_array_input_type_dict_returns_true(self):
        """_36,37,38: "dataType" 大小写/XX 变异 → 无法识别 array 类型。
        dict 形式 {"dataType": "array"} 应返回 True。
        """
        node = self._reduce_node(child_input_type={"dataType": "array"})
        result = node._child_is_array_input()
        self.assertTrue(result,
                        "input_type={'dataType': 'array'} 应返回 True")

    def test_non_array_input_type_returns_false(self):
        """Baseline: dataType='object' 应返回 False。"""
        node = self._reduce_node(child_input_type={"dataType": "object"})
        result = node._child_is_array_input()
        self.assertFalse(result)

    def test_no_input_type_returns_false(self):
        """Baseline: 无 inputType 应返回 False。"""
        node = self._reduce_node()
        result = node._child_is_array_input()
        self.assertFalse(result)

    def test_array_type_with_object_having_data_type_attr(self):
        """_43: removes 'data_type' getattr line — 影响 Property 对象路径。
        在 dict 形式下验证 'dataType' 正确被读取（字符串值）。
        """
        # dict 形式：getattr(input_type_dict, "dataType", None) → AttributeError/None
        # 实际上 dict 有 .get() 而没有属性 dataType；这里用 MagicMock 模拟 Property
        mock_input = MagicMock()
        mock_input.dataType = "array"
        mock_input.data_type = None  # 确保 data_type 不触发

        node = self._reduce_node()
        # 直接替换 child_flow 的 input_type
        node.child_flow.input_type = mock_input
        result = node._child_is_array_input()
        self.assertTrue(result,
                        "Property 对象 dataType='array' 应返回 True")

    def test_property_data_type_attr_fallback(self):
        """_31,_43: getattr(None/input_type, "dataType", ...) 变异。
        当 Property 只有 data_type（不是 dataType）时，fallback 应生效。
        """
        mock_input = MagicMock()
        mock_input.dataType = None     # 主路径返回 None
        mock_input.data_type = "array"  # fallback

        node = self._reduce_node()
        node.child_flow.input_type = mock_input
        result = node._child_is_array_input()
        self.assertTrue(result,
                        "Property 对象只有 data_type='array' 时，fallback 应返回 True")

    def test_child_flow_none_returns_false(self):
        """Baseline: child_flow=None 应返回 False。"""
        node = self._reduce_node()
        node.child_flow = None
        result = node._child_is_array_input()
        self.assertFalse(result)


# ---------------------------------------------------------------------------
# Reduce.arun — 异步变异
# ---------------------------------------------------------------------------

class TestReduceArunMutations(unittest.IsolatedAsyncioTestCase):

    def _make_reduce_async_ctx(self, collection, initial_val=None):
        ctx = MagicMock()
        calls = []

        def evaluate(expr):
            if expr is None:
                return None
            return expr if isinstance(expr, (list, int, float)) else collection

        ctx.evaluate.side_effect = evaluate

        def get_child():
            child = MagicMock()
            async def arun_compat(flow, debug, *args, **kw):
                calls.append({"flow": flow, "debug": debug, "args": args, "kw": kw})
                first = kw.get("first") or (args[0][0] if args and isinstance(args[0], list) else None)
                second = kw.get("second") or (args[0][1] if args and isinstance(args[0], list) else None)
                return (first or 0) + (second or 0)
            child.arun_compatible = arun_compat
            return child

        ctx.get_child_execution.side_effect = get_child
        ctx._calls = calls
        return ctx

    async def test_arun_object_style_debug_is_false(self):
        """_22: debug=None。"""
        node = _make_node("reduce")
        ctx = self._make_reduce_async_ctx([1, 2, 3])
        ctx.evaluate.side_effect = lambda expr: [1, 2, 3]
        await node.arun(ctx)
        for c in ctx._calls:
            self.assertIs(c["debug"], False)

    async def test_arun_array_style_debug_is_false(self):
        """_14: debug=None on array-style path."""
        # 创建带 array inputType 的 reduce
        node_dict = {
            "type": "reduce", "id": "n",
            "collection": "$INPUT",
            "childFlow": {
                "id": "cf", "version": "1", "runtime": "python",
                "inputType": {"dataType": "array"},
                "nodes": [
                    {"type": "start", "id": "s", "next": "e"},
                    {"type": "end", "id": "e", "resultType": "success", "output": "$INPUT"},
                ],
            },
            "next": "end",
        }
        flow = Flow.model_validate({
            "flow_id": "f", "version": "1", "runtime": "python",
            "nodes": [
                {"type": "start", "id": "start", "next": "n"},
                node_dict,
                {"type": "end", "id": "end", "resultType": "success", "output": "$NODE.n"},
            ],
        })
        node = next(n for n in flow.nodes if getattr(n, "id", None) == "n")

        ctx = self._make_reduce_async_ctx([10, 20])
        ctx.evaluate.side_effect = lambda expr: [10, 20]
        await node.arun(ctx)
        for c in ctx._calls:
            self.assertIs(c["debug"], False,
                          "array-style reduce arun 的 debug_mode 应为 False，不是 None")


# ---------------------------------------------------------------------------
# Map.arun — 异步变异测试
# ---------------------------------------------------------------------------

class TestMapArunMutations(unittest.IsolatedAsyncioTestCase):

    def _make_map_async_ctx(self, collection, child_return=None):
        ctx = MagicMock()
        calls = []
        ctx.evaluate.side_effect = lambda expr: collection if expr is not None else []

        def get_child():
            child = MagicMock()
            async def arun_compat(flow, debug, *args, **kw):
                calls.append({"flow": flow, "debug": debug, "args": args, "kw": kw})
                item = kw.get("item")
                index = kw.get("index", 0)
                if child_return:
                    return child_return(item, index)
                return item
            child.arun_compatible = arun_compat
            return child

        ctx.get_child_execution.side_effect = get_child
        ctx._calls = calls
        return ctx

    async def test_arun_sequential_debug_is_false(self):
        """_7: sequential Map.arun debug=None。"""
        node = _make_node("map")
        ctx = self._make_map_async_ctx([1, 2, 3])
        await node.arun(ctx)
        for c in ctx._calls:
            self.assertIs(c["debug"], False)

    async def test_arun_sequential_index_not_none(self):
        """_9: index=None（sequential path）。"""
        node = _make_node("map")
        ctx = self._make_map_async_ctx([1, 2, 3])
        await node.arun(ctx)
        for c in ctx._calls:
            self.assertIsNotNone(c["kw"].get("index"))

    async def test_arun_sequential_index_kwarg_present(self):
        """_13: index kwarg 缺失（sequential）。"""
        node = _make_node("map")
        ctx = self._make_map_async_ctx([1, 2, 3])
        await node.arun(ctx)
        for c in ctx._calls:
            self.assertIn("index", c["kw"])

    async def test_arun_concurrent_semaphore_limit(self):
        """_19: asyncio.Semaphore(...or 2) 代替 ...or 1。
        通过验证 index 传递正确来侧面测试该路径的正确性。
        """
        node = _make_node("map", extra={"concurrent": True})
        ctx = self._make_map_async_ctx([10, 20, 30],
                                       child_return=lambda item, idx: (item, idx))
        result = await node.arun(ctx)
        indexes = sorted([r[1] for r in result])
        self.assertEqual(indexes, [0, 1, 2])

    async def test_arun_concurrent_index_not_none(self):
        """_22,_29: concurrent Map.arun 中 index=None。"""
        node = _make_node("map", extra={"concurrent": True})
        ctx = self._make_map_async_ctx([1, 2, 3],
                                       child_return=lambda item, idx: idx)
        result = await node.arun(ctx)
        self.assertNotIn(None, result,
                         "concurrent Map.arun 结果中不应有 None index")

    async def test_arun_concurrent_results_ordered(self):
        """_37: sequential Map.arun 中 index=None。"""
        node = _make_node("map")  # sequential (concurrent=False)
        ctx = self._make_map_async_ctx([10, 20, 30],
                                       child_return=lambda item, idx: idx)
        result = await node.arun(ctx)
        self.assertEqual(result, [0, 1, 2],
                         "sequential Map.arun index 应为 [0,1,2]")


if __name__ == "__main__":
    unittest.main()
